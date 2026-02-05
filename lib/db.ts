import { getLogger } from '@logtape/logtape'
import { StorageDriver } from './drivers/interface'
import { CollectionCompactor, CompactResult, VacuumResult } from './compactor'
import { TransactionError, ValidationError } from './errors'
import {
  DbOptions,
  DbHooks,
  DEFAULT_CONFIG,
  VectorDocument,
  VectorCollectionOptions,
  CollectionOptions
} from './types'
import { VectorCollection } from './vector-collection'
import { Collection, CollectionConfig } from './collection'

export { Collection, CollectionConfig, MaintenanceSchedule, BatchTransaction } from './collection'
export { DbOptions }

export interface TransactionalCollection<T extends { id: string }> {
  put(data: T): Promise<void>
  delete(id: string): Promise<void>
  get(id: string): Promise<T | undefined>
}

export interface TransactionContext {
  collection<T extends { id: string }>(name: string, options?: CollectionOptions): TransactionalCollection<T>
  transaction(fn: (tx: TransactionContext) => Promise<void>): Promise<void>
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
    private _driver: StorageDriver,
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

  /** Access the underlying storage driver (for admin operations) */
  get driver(): StorageDriver {
    return this._driver
  }

  collection<T extends { id: string }>(name: string, options?: CollectionOptions): Collection<T> {
    let col = this.collections.get(name)
    if (!col) {
      this.validateCollectionName(name)
      this.logger.debug('Creating collection instance {name}', { name })
      col = new Collection<T>(
        this._driver,
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
        this._driver,
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
    const compactor = new CollectionCompactor(this._driver, this.compactorConfig, bloomConfig)
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
    const compactor = new CollectionCompactor(this._driver, this.compactorConfig, bloomConfig)
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
