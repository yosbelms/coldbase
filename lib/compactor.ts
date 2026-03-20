import { randomUUID } from 'crypto'
import { getLogger } from '@logtape/logtape'
import { StorageDriver } from './drivers/interface'
import { streamToString, streamLines, parallelLimit, chunk, BloomFilter, LRUCache } from './utils'
import { LockActiveError, LockAcquisitionError, PreconditionFailedError } from './errors'
import { CompactorConfig, DEFAULT_CONFIG } from './types'

export { CompactorConfig }

export interface CompactResult {
  mutationsProcessed: number
  durationMs: number
  bloomFilterBuilt?: boolean
}

export interface VacuumResult {
  recordsRemoved: number
  durationMs: number
}

interface LockMeta {
  sessionId: string
  expiresAt: number
}

export class CollectionCompactor {
  private config: Required<CompactorConfig>
  private logger = getLogger(['coldbase', 'compactor'])
  private bloomFilterConfig?: { expectedItems: number; falsePositiveRate: number }

  constructor(
    private driver: StorageDriver,
    config: CompactorConfig = {},
    bloomFilterConfig?: { expectedItems: number; falsePositiveRate: number }
  ) {
    this.config = {
      copyBufferSize: config.copyBufferSize ?? DEFAULT_CONFIG.copyBufferSize,
      parallelism: config.parallelism ?? DEFAULT_CONFIG.parallelism,
      deleteChunkSize: config.deleteChunkSize ?? DEFAULT_CONFIG.deleteChunkSize,
      leaseDurationMs: config.leaseDurationMs ?? DEFAULT_CONFIG.leaseDurationMs,
      vacuumCacheSize: config.vacuumCacheSize ?? DEFAULT_CONFIG.vacuumCacheSize,
      adaptiveLease: config.adaptiveLease ?? DEFAULT_CONFIG.adaptiveLease,
      leasePerByte: config.leasePerByte ?? DEFAULT_CONFIG.leasePerByte,
      leasePerMutation: config.leasePerMutation ?? DEFAULT_CONFIG.leasePerMutation,
      maxLeaseDurationMs: config.maxLeaseDurationMs ?? DEFAULT_CONFIG.maxLeaseDurationMs
    }
    this.bloomFilterConfig = bloomFilterConfig
  }

  /**
   * Create lock metadata with lease-based expiry (no heartbeat needed).
   */
  private lockMeta(sessionId: string, leaseDurationMs: number, expiresAt?: number): string {
    return JSON.stringify({
      sessionId,
      expiresAt: expiresAt ?? (Date.now() + leaseDurationMs)
    } as LockMeta)
  }

  /**
   * Estimate lease duration based on file size and mutation count.
   * Returns the calculated lease duration in milliseconds.
   */
  private async estimateLeaseDuration(collection: string): Promise<number> {
    if (!this.config.adaptiveLease) {
      return this.config.leaseDurationMs
    }

    const mainKey = `${collection}.jsonl`

    // Get file size
    const fileSize = await this.driver.size(mainKey) ?? 0

    // Count mutation files
    let mutationCount = 0
    let token: string | undefined
    do {
      const list = await this.driver.list(`${collection}.mutation.`, token)
      mutationCount += list.keys.length
      token = list.continuationToken
    } while (token)

    // Calculate adaptive lease
    const baseLease = this.config.leaseDurationMs
    const fileSizeLease = fileSize * this.config.leasePerByte
    const mutationLease = mutationCount * this.config.leasePerMutation

    const calculatedLease = baseLease + fileSizeLease + mutationLease
    const finalLease = Math.min(calculatedLease, this.config.maxLeaseDurationMs)

    this.logger.debug(
      'Adaptive lease for {collection}: base={base}ms + file={fileMs}ms ({fileSize} bytes) + mutations={mutMs}ms ({mutCount} files) = {total}ms',
      {
        collection,
        base: baseLease,
        fileMs: Math.round(fileSizeLease),
        fileSize,
        mutMs: Math.round(mutationLease),
        mutCount: mutationCount,
        total: Math.round(finalLease)
      }
    )

    return finalLease
  }

  /**
   * Lease-based locking - simpler and more serverless-friendly than heartbeat.
   * Lock automatically expires after leaseDurationMs without needing background timers.
   */
  private async withLock<T>(collection: string, leaseDurationMs: number, fn: () => Promise<T>): Promise<T> {
    const lockKey = `${collection}.lock`
    const sessionId = randomUUID()

    let lockEtag: string | undefined

    this.logger.debug('Acquiring lock {lockKey} for session {sessionId} with lease {lease}ms', {
      lockKey,
      sessionId,
      lease: Math.round(leaseDurationMs)
    })

    const acquireLock = async (): Promise<string> => {
      try {
        return await this.driver.putIfNoneMatch(lockKey, this.lockMeta(sessionId, leaseDurationMs))
      } catch (err) {
        if (!(err instanceof PreconditionFailedError)) throw new LockAcquisitionError(collection, err as Error)

        // Check if existing lock has expired
        const existing = await this.driver.get(lockKey)
        if (!existing) {
          // Lock was deleted between our attempt and check, retry
          return await this.driver.putIfNoneMatch(lockKey, this.lockMeta(sessionId, leaseDurationMs))
        }

        const meta: LockMeta = JSON.parse((await streamToString(existing.stream)) || '{}')

        if (meta.expiresAt !== undefined && Date.now() > meta.expiresAt) {
          this.logger.warn('Taking over expired lock {lockKey}', { lockKey })
          return await this.driver.putIfMatch(lockKey, this.lockMeta(sessionId, leaseDurationMs), existing.etag)
        }
        throw new LockActiveError(lockKey)
      }
    }

    const releaseLock = async () => {
      if (!lockEtag) return
      try {
        // Set expiresAt to 0 to immediately invalidate the lock
        await this.driver.putIfMatch(lockKey, this.lockMeta(sessionId, leaseDurationMs, 0), lockEtag)
        this.logger.debug('Released lock {lockKey}', { lockKey })
      } catch {
        this.logger.warn('Failed to release lock {lockKey}', { lockKey })
      }
    }

    try {
      lockEtag = await acquireLock()
      return await fn()
    } finally {
      await releaseLock()
    }
  }

  async compact(collection: string): Promise<CompactResult> {
    const startTime = Date.now()
    let totalMutations = 0
    let bloomFilterBuilt = false

    // Estimate lease duration before acquiring lock
    const leaseDurationMs = await this.estimateLeaseDuration(collection)

    await this.withLock(collection, leaseDurationMs, async () => {
      this.logger.debug('Compacting {collection}', { collection })
      const mainKey = `${collection}.jsonl`
      let workDone: boolean

      // Buffer for accumulating writes to reduce "append" calls (S3 optimization)
      let appendBuffer: string[] = []
      let bufferSize = 0

      const flushBuffer = async () => {
        if (appendBuffer.length === 0) return
        await this.driver.append(mainKey, appendBuffer.join('\n'))
        appendBuffer = []
        bufferSize = 0
      }

      do {
        workDone = false
        let token: string | undefined
        const keysToDelete: string[] = []

        do {
          const list = await this.driver.list(`${collection}.mutation.`, token)
          if (list.keys.length) {
            workDone = true
            keysToDelete.push(...list.keys)
          }

          // Process mutations in parallel
          const contents = await parallelLimit(list.keys, this.config.parallelism, async key => {
            const resp = await this.driver.get(key)
            if (!resp) return null
            try {
              const content = await streamToString(resp.stream)
              return JSON.parse(content)
            } catch {
              this.logger.warn('Malformed mutation file {key}', { key })
              return null
            }
          })

          // Append all valid batches
          for (const batch of contents) {
            if (Array.isArray(batch)) {
              // batch is [id, data, ts?]
              const lines = batch.map(item => JSON.stringify(item))

              for (const line of lines) {
                appendBuffer.push(line)
                bufferSize += line.length + 1 // +1 for newline

                if (bufferSize >= this.config.copyBufferSize) {
                  await flushBuffer()
                }
              }

              totalMutations += batch.length
            }
          }

          await flushBuffer() // Flush any remaining items in current batch processing

          token = list.continuationToken
        } while (token)

        // Chunked deletes to avoid overwhelming storage
        if (keysToDelete.length) {
          const chunks = chunk(keysToDelete, this.config.deleteChunkSize)
          for (const batch of chunks) {
            await this.driver.delete(batch)
          }
        }
      } while (workDone)

      // Build bloom filter in single pass after compaction
      bloomFilterBuilt = await this.buildBloomFilter(collection)
    })

    return {
      mutationsProcessed: totalMutations,
      durationMs: Date.now() - startTime,
      bloomFilterBuilt
    }
  }

  /**
   * Build bloom filter in a single pass through the main file.
   * Returns true if a bloom filter was built, false if not configured.
   */
  private async buildBloomFilter(collection: string): Promise<boolean> {
    if (!this.bloomFilterConfig) return false

    const mainKey = `${collection}.jsonl`
    const bloomKey = `${collection}.bloom`

    const mainResp = await this.driver.get(mainKey)
    if (!mainResp) return false

    // Track the last state of each ID (may be deleted)
    const states = new Map<string, boolean>() // id -> deleted

    for await (const { line } of streamLines(mainResp.stream)) {
      try {
        const [id, data] = JSON.parse(line)
        states.set(id, data === null)
      } catch {
        this.logger.warn('Malformed JSON in {collection}.jsonl, skipping', { collection })
      }
    }

    const filter = new BloomFilter(this.bloomFilterConfig.expectedItems, this.bloomFilterConfig.falsePositiveRate)
    for (const [id, deleted] of states) {
      if (!deleted) filter.add(id)
    }

    await this.driver.put(bloomKey, filter.serialize())
    this.logger.debug('Built bloom filter for {collection}', { collection })
    return true
  }

  /**
   * Single-pass streaming vacuum with bounded memory using LRU cache.
   *
   * Pass 1: Stream through file, tracking last occurrence of each ID in LRU cache.
   *         IDs that overflow the cache are added to an "overflow" set.
   * Pass 2: Stream through again, keeping:
   *         - Records at their lastLine position (if tracked and not deleted)
   *         - All records for overflow IDs (couldn't track them, so keep all)
   *
   * This is much faster than the old partition-based approach:
   * - Only 2 passes instead of 2 * numPartitions passes
   * - Most workloads fit entirely in cache, getting full deduplication
   */
  async vacuum(collection: string): Promise<VacuumResult> {
    const startTime = Date.now()
    let removedCount = 0

    // Estimate lease duration before acquiring lock
    // Vacuum does 2 passes, so we multiply by 2 for safety
    const baseLease = await this.estimateLeaseDuration(collection)
    const leaseDurationMs = Math.min(baseLease * 2, this.config.maxLeaseDurationMs)

    await this.withLock(collection, leaseDurationMs, async () => {
      const mainKey = `${collection}.jsonl`

      // Check if main file exists first (use size() to avoid reading entire file)
      const mainSize = await this.driver.size(mainKey)
      if (mainSize === undefined || mainSize === 0) {
        this.logger.debug('Main file not found or empty for {collection}, skipping vacuum', { collection })
        return
      }

      const tempKey = `${collection}.jsonl.tmp`
      const cacheSize = this.config.vacuumCacheSize

      // Clear temp file if exists from previous failed run
      await this.driver.delete([tempKey])
      await this.driver.put(tempKey, '')

      this.logger.debug('Vacuuming {collection} with LRU cache size {cacheSize}', { collection, cacheSize })

      // Pass 1: Build tracking structures with bounded memory
      // LRU cache tracks: id -> { lineNum, deleted }
      // Overflow set contains IDs that were evicted from cache
      const tracker = new LRUCache<string, { lineNum: number; deleted: boolean }>(cacheSize)
      const overflowIds = new Set<string>()
      let totalRecordsScanned = 0

      const mainResp = await this.driver.get(mainKey)
      if (!mainResp) return

      let lineNum = 0
      for await (const { line } of streamLines(mainResp.stream)) {
        lineNum++
        totalRecordsScanned++
        try {
          const [id, data] = JSON.parse(line)

          // Check if this ID will cause an eviction
          if (!tracker.has(id) && tracker.size >= cacheSize) {
            // Get the ID that will be evicted (oldest entry)
            const oldestId = tracker.entries().next().value?.[0]
            if (oldestId) {
              overflowIds.add(oldestId)
            }
          }

          tracker.set(id, { lineNum, deleted: data === null })
        } catch {
          this.logger.warn('Malformed JSON in {collection}.jsonl at line {line}, skipping', { collection, line: lineNum })
        }
      }

      this.logger.debug('Pass 1 complete: {tracked} tracked, {overflow} overflow IDs', {
        tracked: tracker.size,
        overflow: overflowIds.size
      })

      // Pass 2: Write surviving records to temp file
      const mainResp2 = await this.driver.get(mainKey)
      if (!mainResp2) return

      let writeBatch: string[] = []
      let bufferSize = 0
      let totalRecordsKept = 0

      const flush = async () => {
        if (writeBatch.length) {
          await this.driver.append(tempKey, writeBatch.join('\n'))
          writeBatch = []
          bufferSize = 0
        }
      }

      lineNum = 0
      for await (const { line } of streamLines(mainResp2.stream)) {
        lineNum++
        try {
          const [id, data] = JSON.parse(line)

          let shouldKeep = false

          if (overflowIds.has(id)) {
            // For overflow IDs, keep all non-null records (we couldn't track them)
            shouldKeep = data !== null
          } else {
            // For tracked IDs, only keep if this is the last occurrence and not deleted
            const tracked = tracker.get(id)
            if (tracked && tracked.lineNum === lineNum && !tracked.deleted) {
              shouldKeep = true
            }
          }

          if (shouldKeep) {
            writeBatch.push(line)
            bufferSize += line.length + 1
            totalRecordsKept++
            if (bufferSize >= this.config.copyBufferSize) {
              await flush()
            }
          }
        } catch {
          this.logger.warn('Malformed JSON in {collection}.jsonl at line {line}, skipping', { collection, line: lineNum })
        }
      }
      await flush()

      removedCount = totalRecordsScanned - totalRecordsKept
      this.logger.debug('Vacuum {collection}: kept {kept}, removed {removed}', {
        collection,
        kept: totalRecordsKept,
        removed: removedCount
      })

      // Swap temp to main
      const tempResp = await this.driver.get(tempKey)
      if (tempResp) {
        await this.driver.put(mainKey, '')

        let buf = ''
        for await (const chunk of tempResp.stream) {
          buf += chunk
          if (buf.length >= this.config.copyBufferSize) {
            await this.driver.append(mainKey, buf)
            buf = ''
          }
        }
        if (buf) await this.driver.append(mainKey, buf)
        await this.driver.delete([tempKey])
      }

      // Rebuild bloom filter in single pass after vacuum
      await this.buildBloomFilter(collection)
    })

    return {
      recordsRemoved: removedCount,
      durationMs: Date.now() - startTime
    }
  }
}
