export interface Document {
  id: string
  [key: string]: unknown
}

export interface VectorDocument {
  id: string
  vector: number[]
  [key: string]: unknown
}

export type SimilarityMetric = 'cosine' | 'euclidean' | 'dotProduct'

export interface CollectionOptions {
  ttlField?: string
}

export interface VectorCollectionOptions {
  dimension: number
  metric?: SimilarityMetric  // default: 'cosine'
  normalize?: boolean        // default: true for cosine
  ttlField?: string
}

export interface SearchOptions<T> {
  limit?: number
  threshold?: number
  filter?: Partial<T> | ((item: T) => boolean)
  includeVector?: boolean
}

export interface SearchResult<T> {
  id: string
  score: number
  data: T
}

export interface MutationRecord<T = unknown> {
  id: string
  data: T | null
}

export type MutationBatch = Array<[id: string, data: unknown | null, timestamp?: number]>

export interface QueryOptions<T> {
  where?: Partial<T> | ((item: T) => boolean)
  limit?: number
  offset?: number
  at?: number // Snapshot timestamp for time travel
}

export interface DbHooks {
  onWrite?: (collection: string, count: number) => void
  onCompact?: (collection: string, durationMs: number, mutationCount: number) => void
  onVacuum?: (collection: string, durationMs: number, removedCount: number) => void
  onError?: (error: Error, operation: string) => void
  /**
   * Called when auto-maintenance fails after all retry attempts.
   * Use this to alert on persistent maintenance failures that could lead to
   * mutation file accumulation in serverless environments.
   *
   * @param collection - The collection that failed maintenance
   * @param operation - 'compact' or 'vacuum'
   * @param error - The final error after retries
   * @param attemptsMade - Number of attempts made (including retries)
   */
  onMaintenanceFailure?: (collection: string, operation: 'compact' | 'vacuum', error: Error, attemptsMade: number) => void
}

export interface CompactorConfig {
  copyBufferSize?: number
  parallelism?: number
  deleteChunkSize?: number
  /**
   * Base duration of the lock lease in ms. Lock expires after this time without renewal.
   * When adaptiveLease is enabled, this serves as the minimum lease duration.
   * Default: 30000 (30 seconds)
   */
  leaseDurationMs?: number
  /** Maximum items to track in LRU cache during vacuum (memory bound) */
  vacuumCacheSize?: number

  /**
   * Enable adaptive lease duration based on file size and mutation count.
   * When enabled, lease duration is automatically calculated as:
   *   leaseDurationMs + (fileSize * leasePerByte) + (mutationCount * leasePerMutation)
   * Default: true
   */
  adaptiveLease?: boolean

  /**
   * Milliseconds to add per byte of main file size.
   * Default: 0.00003 (~30ms per MB, ~1.8s per 60MB)
   */
  leasePerByte?: number

  /**
   * Milliseconds to add per mutation file.
   * Default: 200 (200ms per mutation file)
   */
  leasePerMutation?: number

  /**
   * Maximum lease duration in ms (cap to prevent runaway locks).
   * Default: 600000 (10 minutes)
   */
  maxLeaseDurationMs?: number
}

/**
 * Options for automatic maintenance operations in serverless environments.
 *
 * Serverless functions have short lifespans and cold starts, so maintenance
 * operations use probabilistic triggers to distribute load across invocations.
 */
export interface AutoMaintenanceOptions {
  /**
   * Probability (0-1) of triggering maintenance after each write.
   * Lower values reduce overhead but delay maintenance.
   * Example: 0.1 = 10% chance per write
   */
  probability?: number

  /**
   * Minimum number of pending mutation files before maintenance triggers.
   * If set, maintenance only runs when mutation count exceeds this threshold.
   * Helps avoid unnecessary maintenance when there's little work to do.
   */
  mutationThreshold?: number

  /**
   * Number of retry attempts for failed maintenance operations.
   * Default: 2 (total 3 attempts including initial)
   * Set to 0 to disable retries.
   */
  maxRetries?: number

  /**
   * Base delay in ms between retry attempts (uses exponential backoff).
   * Default: 1000ms
   */
  retryDelayMs?: number
}

/**
 * Options for automatic vacuum in serverless environments.
 */
export interface AutoVacuumOptions extends AutoMaintenanceOptions {
  /**
   * Probability (0-1) of triggering vacuum after a successful compaction.
   * This is checked independently of the regular probability.
   * Useful since compaction is a good time to also vacuum.
   */
  afterCompactProbability?: number
}

export interface DbOptions extends CompactorConfig {
  /**
   * Enable automatic compaction after writes.
   * - `true`: Always compact after each write (legacy behavior)
   * - `false`: Disable auto-compaction (default)
   * - `AutoMaintenanceOptions`: Serverless-friendly probabilistic compaction
   */
  autoCompact?: boolean | AutoMaintenanceOptions

  /**
   * Enable automatic vacuum to clean up deleted records.
   * - `true`: Always vacuum after each write (not recommended)
   * - `false`: Disable auto-vacuum (default)
   * - `AutoVacuumOptions`: Serverless-friendly probabilistic vacuum
   */
  autoVacuum?: boolean | AutoVacuumOptions

  maxMutationSize?: number
  hooks?: DbHooks
  retryOptions?: {
    maxAttempts?: number
    baseDelayMs?: number
    maxDelayMs?: number
  }

  /**
   * Enable in-memory index for fast lookups.
   * Index is loaded from .idx file on first read and invalidated when mutations exist.
   */
  useIndex?: boolean

  /**
   * Enable bloom filter for fast "not exists" checks.
   * Bloom filter is loaded from .bloom file and rebuilt during compaction.
   */
  useBloomFilter?: boolean

  /**
   * Expected number of items for bloom filter sizing (default: 10000)
   */
  bloomFilterExpectedItems?: number

  /**
   * Bloom filter false positive rate (default: 0.01 = 1%)
   */
  bloomFilterFalsePositiveRate?: number
}

/**
 * Recommended defaults for serverless environments.
 * These values are tuned for AWS Lambda, Vercel, Cloudflare Workers, etc.
 */
export const SERVERLESS_AUTO_COMPACT: Required<AutoMaintenanceOptions> = {
  probability: 0.1,       // 10% chance per write
  mutationThreshold: 5,   // Only compact if >= 5 mutation files
  maxRetries: 2,          // Retry twice on failure
  retryDelayMs: 1000      // 1 second base delay
}

export const SERVERLESS_AUTO_VACUUM: Required<AutoVacuumOptions> = {
  probability: 0.01,           // 1% chance per write
  mutationThreshold: 0,        // No mutation threshold for vacuum
  afterCompactProbability: 0.1, // 10% chance after compaction
  maxRetries: 2,               // Retry twice on failure
  retryDelayMs: 1000           // 1 second base delay
}

export const DEFAULT_CONFIG: Required<Omit<DbOptions, 'hooks' | 'retryOptions'>> = {
  autoCompact: false,
  autoVacuum: false,
  copyBufferSize: 65536,
  parallelism: 5,
  deleteChunkSize: 100,
  maxMutationSize: 1024 * 1024 * 10, // 10MB
  leaseDurationMs: 30000, // 30 second base lease for serverless
  vacuumCacheSize: 100000, // Track up to 100k IDs in memory during vacuum
  useIndex: false,
  useBloomFilter: false,
  bloomFilterExpectedItems: 10000,
  bloomFilterFalsePositiveRate: 0.01,
  // Adaptive lease settings
  adaptiveLease: true,
  leasePerByte: 0.00003, // ~30ms per MB, ~1.8s per 60MB
  leasePerMutation: 200, // 200ms per mutation file
  maxLeaseDurationMs: 600000 // 10 minute cap
}
