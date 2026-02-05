import { Hono } from 'hono'
import { bodyLimit } from 'hono/body-limit'
import type { Db } from '../db'
import { createAuthMiddleware, createLoggerMiddleware } from './auth'
import { createDataRoutes } from './data'
import { createAdminRoutes } from './admin'
import { createDocsRoutes } from './docs'
import { resolveLimits, type HttpOptions } from './types'

export type { HttpOptions, ApiKey, Role, AuthOptions } from './types'

/**
 * Create a Coldbase HTTP API using Hono
 *
 * @param db - Coldbase database instance
 * @param options - HTTP API options
 * @returns Hono app with /data, /admin, and /docs routes
 *
 * @example
 * ```typescript
 * import { createHttpApi } from 'coldbase/http'
 * import { Db, FileSystemDriver } from 'coldbase'
 *
 * const db = new Db(new FileSystemDriver('./data'))
 * const app = createHttpApi(db)
 *
 * // With query support (jsonquery)
 * import { jsonquery } from '@jsonquerylang/jsonquery'
 * const app = createHttpApi(db, {
 *   query: (docs, q) => jsonquery(docs, q)
 * })
 *
 * // With authentication
 * const app = createHttpApi(db, {
 *   auth: { useEnv: true }
 * })
 * ```
 */
export function createHttpApi(db: Db, options?: HttpOptions): Hono {
  const app = new Hono()
  const limits = resolveLimits(options)

  // Apply body size limit
  app.use('*', bodyLimit({
    maxSize: limits.maxBodySize,
    onError: (c) => c.json({ error: `Body too large (max ${limits.maxBodySize} bytes)` }, 413)
  }))

  // Apply request logging (uses logtape)
  app.use('*', createLoggerMiddleware())

  // Apply auth middleware to all routes
  app.use('*', createAuthMiddleware(options?.auth))

  // Mount route groups
  app.route('/data', createDataRoutes(db, options))
  app.route('/admin', createAdminRoutes(db))
  app.route('/docs', createDocsRoutes())

  return app
}

// Re-export for convenience
export { createDataRoutes } from './data'
export { createAdminRoutes } from './admin'
export { createDocsRoutes } from './docs'
export { createAuthMiddleware, requireRole, createLoggerMiddleware } from './auth'
export { generateOpenApiSpec } from './openapi'
