// Drivers
export { StorageDriver, ListResult } from './drivers/interface'
export { S3Driver } from './drivers/s3'
export { FileSystemDriver } from './drivers/fs'
export { AzureBlobDriver } from './drivers/azure'

// Core
export { Db, Collection, DbOptions } from './db'
export { VectorCollection } from './vector-collection'
export { CollectionCompactor, CompactorConfig, CompactResult, VacuumResult } from './compactor'

// Types
export {
  DbHooks,
  QueryOptions,
  Document,
  CollectionOptions,
  MutationRecord,
  MutationBatch,
  DEFAULT_CONFIG,
  AutoMaintenanceOptions,
  AutoVacuumOptions,
  SERVERLESS_AUTO_COMPACT,
  SERVERLESS_AUTO_VACUUM,
  VectorDocument,
  VectorCollectionOptions,
  SearchOptions,
  SearchResult,
  SimilarityMetric
} from './types'

// Errors
export {
  MiniDbError,
  PreconditionFailedError,
  LockError,
  LockActiveError,
  LockAcquisitionError,
  ValidationError,
  SizeLimitError,
  CorruptionError,
  VectorDimensionError,
  InvalidVectorError
} from './errors'

// Utilities
export { retry, parallelLimit, chunk, streamToString, streamLines, streamJsonLines } from './utils'

// Vector utilities
export {
  cosineSimilarity,
  euclideanDistance,
  dotProduct,
  normalizeVector,
  validateVector
} from './vector-utils'
