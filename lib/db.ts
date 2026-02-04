import { randomUUID } from 'crypto'
import { getLogger } from '@logtape/logtape'
import { StorageDriver } from './drivers/interface'
import { CollectionCompactor, CompactResult, VacuumResult } from './compactor'
import { streamToString, streamJsonLines, retry, chunk, parallelLimit, BloomFilter, monotonicTimestamp } from './utils'
import { SizeLimitError, LockActiveError, TransactionError, ValidationError } from './errors'
import {
  DbOptions,
  DbHooks,
  QueryOptions,
  DEFAULT_CONFIG,
  MutationBatch,
  CompactorConfig,
  AutoMaintenanceOptions,
  AutoVacuumOptions,
  VectorDocument,
  VectorCollectionOptions,
  CollectionOptions
} from './types'
import { VectorCollection } from './vector-collection'

/** Index entry mapping ID to byte offset and length in main file */
interface IndexEntry {
  o: number // offset
  l: number // length
}

/** Transaction interface for batching multiple writes */
export interface BatchTransaction<T extends { id: string }> {
  put(data: T): void
  delete(id: string): void
}

export interface TransactionalCollection<T extends { id: string }> {
  put(data: T): Promise<void>
  delete(id: string): Promise<void>
  get(id: string): Promise<T | undefined>
}

export interface TransactionContext {
  collection<T extends { id: string }>(name: string, options?: CollectionOptions): TransactionalCollection<T>
  transaction(fn: (tx: TransactionContext) => Promise<void>): Promise<void>
}

export { DbOptions }

export interface MaintenanceSchedule {
  compactIntervalMs?: number   // How often to run compact (default: 60000 = 1 min)
  vacuumIntervalMs?: number    // How often to run vacuum (default: 300000 = 5 min)
  onError?: (error: Error, operation: 'compact' | 'vacuum') => void
}

interface CollectionConfig {
  maxMutationSize: number
  retryOptions?: {
    maxAttempts?: number
    baseDelayMs?: number
    maxDelayMs?: number
  }
  autoCompact: boolean | AutoMaintenanceOptions
  autoVacuum: boolean | AutoVacuumOptions
  compactorConfig: CompactorConfig
  useIndex: boolean
  useBloomFilter: boolean
  bloomFilterExpectedItems: number
  bloomFilterFalsePositiveRate: number
}

/**
 * Serverless-friendly collection. Each operation reads directly from storage
 * without relying on in-memory state that won't persist between invocations.
 */
export class Collection<T extends { id: string }> {
  private ttlField?: keyof T & string
  private maintenanceTimers: { compact?: NodeJS.Timeout; vacuum?: NodeJS.Timeout } = {}
  private compactor: CollectionCompactor
  private logger = getLogger(['coldbase', 'collection'])

  // Cached index and bloom filter (lazily loaded)
  private cachedIndex?: Record<string, IndexEntry>
  private cachedBloomFilter?: BloomFilter
  private cachedMainFileContent?: string
  private indexValid = false
  private bloomFilterValid = false

  constructor(
    private driver: StorageDriver,
    private name: string,
    private config: CollectionConfig,
    private hooks?: DbHooks,
    options?: CollectionOptions
  ) {
    const bloomConfig = config.useBloomFilter
      ? { expectedItems: config.bloomFilterExpectedItems, falsePositiveRate: config.bloomFilterFalsePositiveRate }
      : undefined
    this.compactor = new CollectionCompactor(driver, config.compactorConfig, bloomConfig)
    if (options?.ttlField) {
      this.ttlField = options.ttlField as keyof T & string
    }
  }

  /**
   * Invalidate cached index, bloom filter, and file content.
   * Called after writes since pending mutations make them stale.
   */
  private invalidateCache(): void {
    this.indexValid = false
    this.bloomFilterValid = false
    this.cachedMainFileContent = undefined
  }

  /**
   * Load the index from storage if enabled and valid.
   * Index is only valid when there are no pending mutations.
   */
  private async loadIndex(): Promise<Record<string, IndexEntry> | undefined> {
    if (!this.config.useIndex) return undefined
    if (this.indexValid && this.cachedIndex) return this.cachedIndex

    // Check if there are pending mutations - if so, index is stale
    const mutations = await this.driver.list(`${this.name}.mutation.`)
    if (mutations.keys.length > 0) {
      this.indexValid = false
      return undefined
    }

    try {
      const resp = await this.driver.get(`${this.name}.idx`)
      if (!resp) return undefined

      const content = await streamToString(resp.stream)
      this.cachedIndex = JSON.parse(content)
      this.indexValid = true
      return this.cachedIndex
    } catch {
      return undefined
    }
  }

  /**
   * Load the bloom filter from storage if enabled.
   * Bloom filter can have false positives, so it's safe to use even with pending mutations.
   */
  private async loadBloomFilter(): Promise<BloomFilter | undefined> {
    if (!this.config.useBloomFilter) return undefined
    if (this.bloomFilterValid && this.cachedBloomFilter) return this.cachedBloomFilter

    try {
      const resp = await this.driver.get(`${this.name}.bloom`)
      if (!resp) return undefined

      const content = await streamToString(resp.stream)
      this.cachedBloomFilter = BloomFilter.deserialize(content)
      this.bloomFilterValid = true
      return this.cachedBloomFilter
    } catch {
      return undefined
    }
  }

  /**
   * Batch multiple writes into a single mutation file.
   * Use this to reduce the number of mutation files and improve performance.
   *
   * @example
   * await collection.batch(async (tx) => {
   *   tx.put('1', { id: '1', name: 'Alice' })
   *   tx.put('2', { id: '2', name: 'Bob' })
   * })
   */
  async batch(fn: (tx: BatchTransaction<T>) => void | Promise<void>): Promise<void> {
    const items: { id: string; data: T | null }[] = []

    const tx: BatchTransaction<T> = {
      put: (data: T) => {
        items.push({ id: data.id, data })
      },
      delete: (id: string) => {
        items.push({ id, data: null })
      }
    }

    await fn(tx)

    if (items.length > 0) {
      await this._writeMutations(items)
    }
  }

  async put(data: T): Promise<void> {
    return this._writeMutations([{ id: data.id, data }])
  }

  async delete(id: string): Promise<void> {
    return this._writeMutations([{ id, data: null }])
  }

  private async _writeMutations(items: { id: string; data: T | null }[]): Promise<void> {
    this.logger.debug('Writing {count} items to collection {name}', { count: items.length, name: this.name })
    const now = monotonicTimestamp()
    const payload: MutationBatch = items.map(({ id, data }) => [id, data, now])
    const json = JSON.stringify(payload)

    if (json.length > this.config.maxMutationSize) {
      this.logger.warn('Mutation size {size} exceeds limit {limit}', { size: json.length, limit: this.config.maxMutationSize })
      throw new SizeLimitError(json.length, this.config.maxMutationSize)
    }

    const write = async () => {
      await this.driver.put(`${this.name}.mutation.${monotonicTimestamp()}-${randomUUID()}`, json)
    }

    try {
      if (this.config.retryOptions) {
        await retry(write, this.config.retryOptions)
      } else {
        await write()
      }
      this.logger.debug('Successfully wrote mutation for {name}', { name: this.name })
    } catch (error) {
      this.logger.error('Failed to write mutation for {name}: {error}', { name: this.name, error })
      throw error
    }

    // Invalidate cache since we have pending mutations now
    this.invalidateCache()

    this.hooks?.onWrite?.(this.name, items.length)
    this.maybeRunMaintenance()
  }

  /**
   * Serverless-friendly automatic maintenance.
   * Uses probabilistic triggers to distribute load across invocations.
   * Includes retry logic with exponential backoff for transient failures.
   */
  private maybeRunMaintenance(): void {
    const { autoCompact, autoVacuum } = this.config

    // Determine if we should attempt compaction
    if (autoCompact) {
      this.shouldTriggerMaintenance(autoCompact).then(shouldCompact => {
        if (!shouldCompact) return

        const options = typeof autoCompact === 'object' ? autoCompact : {}
        this.runAutoCompactWithRetry(options).then((result) => {
          if (result) {
            // Check if we should vacuum after compaction
            if (autoVacuum && typeof autoVacuum === 'object' && autoVacuum.afterCompactProbability) {
              if (Math.random() < autoVacuum.afterCompactProbability) {
                const vacuumOptions = typeof autoVacuum === 'object' ? autoVacuum : {}
                this.runAutoVacuumWithRetry(vacuumOptions)
              }
            }
          }
        })
      })
    }

    // Determine if we should attempt vacuum (independent of compaction)
    if (autoVacuum) {
      this.shouldTriggerMaintenance(autoVacuum).then(shouldVacuum => {
        if (!shouldVacuum) return
        const options = typeof autoVacuum === 'object' ? autoVacuum : {}
        this.runAutoVacuumWithRetry(options)
      })
    }
  }

  /**
   * Run auto-compaction with retry logic.
   * Returns the result on success, or undefined if all attempts failed.
   */
  private async runAutoCompactWithRetry(
    options: AutoMaintenanceOptions
  ): Promise<{ durationMs: number; mutationsProcessed: number } | undefined> {
    const maxRetries = options.maxRetries ?? 2
    const retryDelayMs = options.retryDelayMs ?? 1000
    let lastError: Error | undefined
    let attempts = 0

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      attempts++
      try {
        const result = await this.compactor.compact(this.name)
        this.hooks?.onCompact?.(this.name, result.durationMs, result.mutationsProcessed)
        this.logger.debug('Auto-compaction completed for {name}', { name: this.name })
        return result
      } catch (error) {
        if (error instanceof LockActiveError) {
          this.logger.debug('Auto-compaction skipped for {name} (lock active)', { name: this.name })
          return undefined // Don't retry lock conflicts
        }

        lastError = error as Error
        this.logger.warn('Auto-compaction attempt {attempt}/{max} failed for {name}: {error}', {
          name: this.name,
          attempt: attempt + 1,
          max: maxRetries + 1,
          error
        })

        if (attempt < maxRetries) {
          // Exponential backoff with jitter
          const delay = retryDelayMs * Math.pow(2, attempt) * (0.5 + Math.random() * 0.5)
          await new Promise(resolve => setTimeout(resolve, delay))
        }
      }
    }

    // All retries exhausted - alert via hooks
    if (lastError) {
      this.hooks?.onError?.(lastError, 'compact')
      this.hooks?.onMaintenanceFailure?.(this.name, 'compact', lastError, attempts)
      this.logger.error('Auto-compaction failed after {attempts} attempts for {name}', {
        name: this.name,
        attempts
      })
    }
    return undefined
  }

  /**
   * Run auto-vacuum with retry logic.
   * Returns the result on success, or undefined if all attempts failed.
   */
  private async runAutoVacuumWithRetry(
    options: AutoMaintenanceOptions
  ): Promise<{ durationMs: number; recordsRemoved: number } | undefined> {
    const maxRetries = options.maxRetries ?? 2
    const retryDelayMs = options.retryDelayMs ?? 1000
    let lastError: Error | undefined
    let attempts = 0

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      attempts++
      try {
        const result = await this.compactor.vacuum(this.name)
        this.hooks?.onVacuum?.(this.name, result.durationMs, result.recordsRemoved)
        this.logger.debug('Auto-vacuum completed for {name}', { name: this.name })
        return result
      } catch (error) {
        if (error instanceof LockActiveError) {
          this.logger.debug('Auto-vacuum skipped for {name} (lock active)', { name: this.name })
          return undefined // Don't retry lock conflicts
        }

        lastError = error as Error
        this.logger.warn('Auto-vacuum attempt {attempt}/{max} failed for {name}: {error}', {
          name: this.name,
          attempt: attempt + 1,
          max: maxRetries + 1,
          error
        })

        if (attempt < maxRetries) {
          // Exponential backoff with jitter
          const delay = retryDelayMs * Math.pow(2, attempt) * (0.5 + Math.random() * 0.5)
          await new Promise(resolve => setTimeout(resolve, delay))
        }
      }
    }

    // All retries exhausted - alert via hooks
    if (lastError) {
      this.hooks?.onError?.(lastError, 'vacuum')
      this.hooks?.onMaintenanceFailure?.(this.name, 'vacuum', lastError, attempts)
      this.logger.error('Auto-vacuum failed after {attempts} attempts for {name}', {
        name: this.name,
        attempts
      })
    }
    return undefined
  }

  private async shouldTriggerMaintenance(
    config: boolean | AutoMaintenanceOptions | AutoVacuumOptions
  ): Promise<boolean> {
    // Legacy boolean mode: always trigger
    if (config === true) return true
    if (config === false) return false

    const { probability = 1, mutationThreshold = 0 } = config

    // First check probability
    if (Math.random() >= probability) {
      return false
    }

    // Then check mutation threshold if configured
    if (mutationThreshold > 0) {
      const mutationCount = await this.countMutationFiles()
      if (mutationCount < mutationThreshold) {
        return false
      }
    }

    return true
  }

  /**
   * Count pending mutation files for this collection.
   * Used to determine if maintenance threshold has been reached.
   */
  async countMutationFiles(): Promise<number> {
    let count = 0
    let token: string | undefined

    do {
      const list = await this.driver.list(`${this.name}.mutation.`, token)
      count += list.keys.length
      token = list.continuationToken
    } while (token)

    return count
  }

  /**
   * Get a single record by ID.
   *
   * Performance optimizations:
   * - If bloom filter is enabled and says ID doesn't exist, returns immediately
   * - If index is enabled and no pending mutations, uses direct byte offset lookup
   * - Otherwise falls back to full scan
   */
  async get(id: string, options: { at?: number } = {}): Promise<T | undefined> {
    this.logger.debug('Getting item {id} from {name}', { id, name: this.name })

    // Fast path: Check bloom filter first (only if not doing time travel)
    if (!options.at) {
      const bloom = await this.loadBloomFilter()
      if (bloom && !bloom.mightContain(id)) {
        this.logger.debug('Bloom filter: {id} definitely not in {name}', { id, name: this.name })
        return undefined
      }
    }

    // Fast path: Use index for direct lookup (only if not doing time travel)
    if (!options.at) {
      const index = await this.loadIndex()
      if (index) {
        const entry = index[id]
        if (!entry) {
          this.logger.debug('Index: {id} not found in {name}', { id, name: this.name })
          return undefined
        }

        // Use cached file content or load it once
        if (!this.cachedMainFileContent) {
          const resp = await this.driver.get(`${this.name}.jsonl`)
          if (resp) {
            this.cachedMainFileContent = await streamToString(resp.stream)
          }
        }

        if (this.cachedMainFileContent) {
          const line = this.cachedMainFileContent.substring(entry.o, entry.o + entry.l)
          try {
            const [, data] = JSON.parse(line)
            if (data !== null && !this.isExpired(data)) {
              return data as T
            }
          } catch {
            // Fall through to full scan
          }
        }
      }
    }

    // Slow path: Full scan
    let result: T | null | undefined

    for await (const record of this.read({ at: options.at })) {
      if (record.id === id) {
        result = record.data
      }
    }

    if (result === null || result === undefined) return undefined
    if (this.isExpired(result)) {
      this.logger.debug('Item {id} expired', { id })
      return undefined
    }

    return result
  }


  /**
   * Get multiple records by IDs. Single scan for all requested IDs.
   */
  async getMany(ids: string[], options: { at?: number } = {}): Promise<Map<string, T>> {
    const idSet = new Set(ids)
    const latest = new Map<string, T | null>()

    for await (const { id, data } of this.read({ at: options.at })) {
      if (idSet.has(id)) {
        latest.set(id, data)
      }
    }

    const result = new Map<string, T>()
    for (const [id, data] of latest) {
      if (data !== null && !this.isExpired(data)) {
        result.set(id, data)
      }
    }

    return result
  }

  /**
   * Query records with optional filtering and pagination.
   * Streams through storage, applying filters without loading all into memory.
   */
  async find(options: QueryOptions<T> = {}): Promise<T[]> {
    const { where, limit, offset = 0, at } = options
    const latest = new Map<string, T | null>()

    // Build map of latest values
    for await (const { id, data } of this.read({ at })) {
      latest.set(id, data)
    }

    // Filter and collect results
    let results: T[] = []
    for (const data of latest.values()) {
      if (data === null || this.isExpired(data)) continue

      if (where) {
        if (typeof where === 'function') {
          if (!where(data)) continue
        } else {
          let match = true
          for (const [key, val] of Object.entries(where)) {
            if ((data as Record<string, unknown>)[key] !== val) {
              match = false
              break
            }
          }
          if (!match) continue
        }
      }
      results.push(data)
    }

    if (offset || limit) {
      results = results.slice(offset, limit ? offset + limit : undefined)
    }

    return results
  }

  /**
   * Stream all records from storage (main file + pending mutations).
   * Records are yielded in storage order; later records override earlier ones.
   */
  async *read(options: { at?: number } = {}): AsyncGenerator<{ id: string; data: T | null; timestamp?: number }> {
    const { at } = options

    // Snapshot mutation keys first to avoid race with compaction.
    // If compaction runs after this snapshot, data moves to main file
    // (we'll see it there) and mutation files get deleted (we skip gracefully).
    const snapshotKeys: string[] = []
    let token: string | undefined
    do {
      const list = await this.driver.list(`${this.name}.mutation.`, token)
      snapshotKeys.push(...list.keys)
      token = list.continuationToken
    } while (token)

    // Stream main file
    const resp = await this.driver.get(`${this.name}.jsonl`)
    if (resp) {
      for await (const record of streamJsonLines<[string, T | null, number?]>(resp.stream)) {
        if (Array.isArray(record) && record.length >= 2) {
          const [id, data, ts] = record
          if (at !== undefined && ts !== undefined && ts > at) continue
          yield { id, data, timestamp: ts }
        }
      }
    }

    // Process snapshotted mutation keys
    const keyChunks = chunk(snapshotKeys, 50)

    for (const keyBatch of keyChunks) {
      const results = await parallelLimit(keyBatch, 10, async (key) => {
        // Check timestamp in filename if available to skip early
        // Format: name.mutation.TIMESTAMP-UUID
        if (at !== undefined) {
          const match = key.match(/\.mutation\.(\d+)-/)
          if (match) {
            const fileTs = parseInt(match[1], 10)
            if (fileTs > at) return null
          }
        }

        const mutResp = await this.driver.get(key)
        if (!mutResp) return null
        try {
          return JSON.parse(await streamToString(mutResp.stream)) as MutationBatch
        } catch {
          return null
        }
      })

      for (const batch of results) {
        if (batch && Array.isArray(batch)) {
          for (const record of batch) {
            const [id, data, ts] = record
            // Filter by specific record timestamp if available, fallback to file check
            if (at !== undefined && ts !== undefined && ts > at) continue
            yield { id, data: data as T | null, timestamp: ts }
          }
        }
      }
    }
  }

  /**
   * Count non-deleted, non-expired records.
   */
  async count(options: { at?: number } = {}): Promise<number> {
    const latest = new Map<string, T | null>()

    for await (const { id, data } of this.read({ at: options.at })) {
      latest.set(id, data)
    }

    let count = 0
    for (const data of latest.values()) {
      if (data !== null && !this.isExpired(data)) {
        count++
      }
    }

    return count
  }

  /**
   * Delete all expired records. Should be called periodically (e.g., scheduled function).
   */
  async deleteExpired(): Promise<number> {
    if (!this.ttlField) return 0

    const latest = new Map<string, T | null>()
    for await (const { id, data } of this.read()) {
      latest.set(id, data)
    }

    const toDelete: { id: string; data: null }[] = []
    for (const [id, data] of latest) {
      if (data !== null && this.isExpired(data)) {
        toDelete.push({ id, data: null })
      }
    }

    if (toDelete.length) {
      await this._writeMutations(toDelete)
    }
    return toDelete.length
  }

  async compact(): Promise<void> {
    this.logger.info('Starting manual compaction for {name}', { name: this.name })
    try {
      const result = await this.compactor.compact(this.name)
      this.hooks?.onCompact?.(this.name, result.durationMs, result.mutationsProcessed)
      this.logger.info('Compaction finished for {name} in {duration}ms, processed {count} mutations', { name: this.name, duration: result.durationMs, count: result.mutationsProcessed })
    } catch (e) {
      this.hooks?.onError?.(e as Error, 'compact')
      this.logger.error('Compaction failed for {name}: {error}', { name: this.name, error: e })
      throw e
    }
  }

  async vacuum(): Promise<void> {
    this.logger.info('Starting manual vacuum for {name}', { name: this.name })
    try {
      const result = await this.compactor.vacuum(this.name)
      this.hooks?.onVacuum?.(this.name, result.durationMs, result.recordsRemoved)
      this.logger.info('Vacuum finished for {name} in {duration}ms, removed {count} records', { name: this.name, duration: result.durationMs, count: result.recordsRemoved })
    } catch (e) {
      this.hooks?.onError?.(e as Error, 'vacuum')
      this.logger.error('Vacuum failed for {name}: {error}', { name: this.name, error: e })
      throw e
    }
  }

  startMaintenance(schedule: MaintenanceSchedule): void {
    this.logger.info('Starting maintenance for {name}', { name: this.name })
    this.stopMaintenance()

    if (schedule.compactIntervalMs) {
      this.maintenanceTimers.compact = setInterval(() => {
        this.logger.debug('Running scheduled compaction for {name}', { name: this.name })
        this.compact().catch(err => {
          if (err instanceof LockActiveError) {
            this.logger.debug('Compaction skipped for {name} (lock active)', { name: this.name })
            return
          }
          this.logger.error('Scheduled compaction failed for {name}: {error}', { name: this.name, error: err })
          schedule.onError?.(err, 'compact')
        })
      }, schedule.compactIntervalMs)
    }

    if (schedule.vacuumIntervalMs) {
      this.maintenanceTimers.vacuum = setInterval(() => {
        this.logger.debug('Running scheduled vacuum for {name}', { name: this.name })
        this.vacuum().catch(err => {
          if (err instanceof LockActiveError) {
            this.logger.debug('Vacuum skipped for {name} (lock active)', { name: this.name })
            return
          }
          this.logger.error('Scheduled vacuum failed for {name}: {error}', { name: this.name, error: err })
          schedule.onError?.(err, 'vacuum')
        })
      }, schedule.vacuumIntervalMs)
    }
  }

  stopMaintenance(): void {
    if (this.maintenanceTimers.compact || this.maintenanceTimers.vacuum) {
      this.logger.info('Stopping maintenance for {name}', { name: this.name })
    }
    if (this.maintenanceTimers.compact) clearInterval(this.maintenanceTimers.compact)
    if (this.maintenanceTimers.vacuum) clearInterval(this.maintenanceTimers.vacuum)
    this.maintenanceTimers = {}
  }

  private isExpired(data: T): boolean {
    if (!this.ttlField) return false
    const expiresAt = data[this.ttlField] as unknown as number
    return expiresAt !== undefined && expiresAt < Date.now()
  }
}

const COLLECTION_NAME_RE = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/

export class Db {
  private collections = new Map<string, Collection<any>>()
  private vectorCollections = new Map<string, VectorCollection<any>>()
  private compactorConfig: typeof DEFAULT_CONFIG
  private collectionConfig: CollectionConfig
  private hooks?: DbHooks
  private logger = getLogger(['coldbase', 'db'])

  private validateCollectionName(name: string): void {
    if (!name || name.length > 64 || !COLLECTION_NAME_RE.test(name)) {
      throw new ValidationError(
        `Invalid collection name "${name}": must be 1-64 characters, start with alphanumeric, and contain only alphanumeric, underscore, or hyphen`
      )
    }
  }

  constructor(
    private driver: StorageDriver,
    options: DbOptions = {}
  ) {
    const { autoCompact = false, autoVacuum = false, hooks, retryOptions, ...config } = options
    this.compactorConfig = { ...DEFAULT_CONFIG, ...config }
    this.collectionConfig = {
      maxMutationSize: this.compactorConfig.maxMutationSize,
      retryOptions,
      autoCompact,
      autoVacuum,
      compactorConfig: this.compactorConfig,
      useIndex: this.compactorConfig.useIndex,
      useBloomFilter: this.compactorConfig.useBloomFilter,
      bloomFilterExpectedItems: this.compactorConfig.bloomFilterExpectedItems,
      bloomFilterFalsePositiveRate: this.compactorConfig.bloomFilterFalsePositiveRate
    }
    this.hooks = hooks
    this.logger.debug('Db initialized with options: {options}', { options })
  }

  collection<T extends { id: string }>(name: string, options?: CollectionOptions): Collection<T> {
    let col = this.collections.get(name)
    if (!col) {
      this.validateCollectionName(name)
      this.logger.debug('Creating collection instance {name}', { name })
      col = new Collection<T>(
        this.driver,
        name,
        this.collectionConfig,
        this.hooks,
        options
      )
      this.collections.set(name, col)
    }
    return col as Collection<T>
  }

  vectorCollection<T extends VectorDocument>(
    name: string,
    options: VectorCollectionOptions
  ): VectorCollection<T> {
    let col = this.vectorCollections.get(name)
    if (!col) {
      this.validateCollectionName(name)
      this.logger.debug('Creating vector collection instance {name}', { name })
      col = new VectorCollection<T>(
        this.driver,
        name,
        this.collectionConfig,
        options,
        this.hooks
      )
      this.vectorCollections.set(name, col)
    }
    return col as VectorCollection<T>
  }

  /**
   * Compact pending mutations into the main file.
   * In serverless, call this from a scheduled function rather than inline.
   */
  async compact(name: string): Promise<CompactResult> {
    this.validateCollectionName(name)
    this.logger.info('Starting DB-level compaction for {name}', { name })
    const bloomConfig = this.collectionConfig.useBloomFilter
      ? { expectedItems: this.collectionConfig.bloomFilterExpectedItems, falsePositiveRate: this.collectionConfig.bloomFilterFalsePositiveRate }
      : undefined
    const compactor = new CollectionCompactor(this.driver, this.compactorConfig, bloomConfig)
    try {
      const result = await compactor.compact(name)
      this.hooks?.onCompact?.(name, result.durationMs, result.mutationsProcessed)
      this.logger.info('DB-level compaction finished for {name}', { name })
      return result
    } catch (e) {
      this.hooks?.onError?.(e as Error, 'compact')
      this.logger.error('DB-level compaction failed for {name}: {error}', { name, error: e })
      throw e
    }
  }

  /**
   * Remove duplicates and deleted records from the main file.
   * In serverless, call this from a scheduled function.
   */
  async vacuum(name: string): Promise<VacuumResult> {
    this.validateCollectionName(name)
    this.logger.info('Starting DB-level vacuum for {name}', { name })
    const bloomConfig = this.collectionConfig.useBloomFilter
      ? { expectedItems: this.collectionConfig.bloomFilterExpectedItems, falsePositiveRate: this.collectionConfig.bloomFilterFalsePositiveRate }
      : undefined
    const compactor = new CollectionCompactor(this.driver, this.compactorConfig, bloomConfig)
    try {
      const result = await compactor.vacuum(name)
      this.hooks?.onVacuum?.(name, result.durationMs, result.recordsRemoved)
      this.logger.info('DB-level vacuum finished for {name}', { name })
      return result
    } catch (e) {
      this.hooks?.onError?.(e as Error, 'vacuum')
      this.logger.error('DB-level vacuum failed for {name}: {error}', { name, error: e })
      throw e
    }
  }

  /**
   * Execute a cross-collection transaction using the saga pattern.
   * Each write records a compensation action; if any step fails,
   * compensations run in reverse order to undo previous writes.
   * Supports nesting: inner transactions act as savepoints.
   */
  async transaction(fn: (tx: TransactionContext) => Promise<void>): Promise<void> {
    return this.executeTransaction(fn, null)
  }

  private async executeTransaction(
    fn: (tx: TransactionContext) => Promise<void>,
    parentCompensations: (() => Promise<void>)[] | null
  ): Promise<void> {
    const compensations: (() => Promise<void>)[] = []

    const tx: TransactionContext = {
      collection: <T extends { id: string }>(name: string, options?: CollectionOptions): TransactionalCollection<T> => {
        const col = this.collection<T>(name, options)

        return {
          async put(data: T): Promise<void> {
            const old = await col.get(data.id)
            await col.put(data)
            if (old !== undefined) {
              compensations.push(() => col.put(old))
            } else {
              compensations.push(() => col.delete(data.id))
            }
          },

          async delete(id: string): Promise<void> {
            const old = await col.get(id)
            await col.delete(id)
            if (old !== undefined) {
              compensations.push(() => col.put(old))
            }
          },

          get(id: string): Promise<T | undefined> {
            return col.get(id)
          }
        }
      },

      transaction: (innerFn: (tx: TransactionContext) => Promise<void>): Promise<void> => {
        return this.executeTransaction(innerFn, compensations)
      }
    }

    try {
      await fn(tx)
    } catch (error) {
      const compensationErrors: Error[] = []

      for (let i = compensations.length - 1; i >= 0; i--) {
        try {
          await compensations[i]()
        } catch (compError) {
          compensationErrors.push(compError as Error)
        }
      }

      // If nested, propagate as TransactionError so the parent can handle it
      throw new TransactionError(error as Error, compensationErrors)
    }

    // On success, promote compensations to parent so they roll back
    // if a later step in the outer transaction fails
    if (parentCompensations) {
      parentCompensations.push(...compensations)
    }
  }
}
