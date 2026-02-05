import type { Context, Next } from 'hono'

export type Role = 'reader' | 'editor' | 'admin'

export interface ApiKey {
  key: string
  role: Role
}

export interface AuthOptions {
  keys?: ApiKey[]
  useEnv?: boolean
}

export interface HttpOptions {
  auth?: AuthOptions
  query?: (docs: any[], queryParam: string) => any[]
  /** Max documents per page (default: 100) */
  maxPageSize?: number
  /** Default documents per page (default: 20) */
  defaultPageSize?: number
  /** Max request body size in bytes (default: 1MB) */
  maxBodySize?: number
  /** Max operations per transaction (default: 50) */
  maxTxOperations?: number
  /** Max vector search results (default: 100) */
  maxVectorResults?: number
}

export interface AuthContext {
  role: Role | null
}

export type AuthMiddleware = (c: Context, next: Next) => Promise<Response | void>

export interface PaginatedResponse<T> {
  data: T[]
  pagination: {
    limit: number
    offset: number
    hasMore: boolean
  }
}

export interface ErrorResponse {
  error: string
}

export interface TransactionOperation {
  type: 'put' | 'delete'
  collection: string
  id: string
  data?: any
}

export interface TransactionRequest {
  operations: TransactionOperation[]
}

export interface TransactionResult {
  type: 'put' | 'delete'
  collection: string
  id: string
  ok: boolean
}

export interface TransactionResponse {
  success: boolean
  results?: TransactionResult[]
  error?: string
  compensated?: boolean
}

export interface VectorSearchRequest {
  vector: number[]
  k?: number
  metric?: 'cosine' | 'euclidean' | 'dotProduct'
}

export interface VectorSearchResult {
  id: string
  score: number
  data: any
}

export interface CollectionStats {
  count: number
  size?: number
}

// Default constants (can be overridden via HttpOptions)
export const DEFAULT_MAX_PAGE_SIZE = 100
export const DEFAULT_PAGE_SIZE = 20
export const DEFAULT_MAX_BODY_SIZE = 1024 * 1024 // 1 MB
export const DEFAULT_MAX_TX_OPERATIONS = 50
export const DEFAULT_MAX_VECTOR_RESULTS = 100

/** Resolve limits with defaults */
export function resolveLimits(options?: HttpOptions) {
  return {
    maxPageSize: options?.maxPageSize ?? DEFAULT_MAX_PAGE_SIZE,
    defaultPageSize: options?.defaultPageSize ?? DEFAULT_PAGE_SIZE,
    maxBodySize: options?.maxBodySize ?? DEFAULT_MAX_BODY_SIZE,
    maxTxOperations: options?.maxTxOperations ?? DEFAULT_MAX_TX_OPERATIONS,
    maxVectorResults: options?.maxVectorResults ?? DEFAULT_MAX_VECTOR_RESULTS
  }
}
