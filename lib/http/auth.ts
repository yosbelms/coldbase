import { timingSafeEqual } from 'crypto'
import { getLogger } from '@logtape/logtape'
import type { Context, Next } from 'hono'
import type { ApiKey, AuthOptions, Role } from './types'

const logger = getLogger(['coldbase', 'http'])

// Timing-safe string comparison to prevent timing attacks
function safeCompare(a: string, b: string): boolean {
  try {
    const bufA = Buffer.from(a)
    const bufB = Buffer.from(b)
    if (bufA.length !== bufB.length) {
      // Compare with self to maintain constant time even for length mismatch
      timingSafeEqual(bufA, bufA)
      return false
    }
    return timingSafeEqual(bufA, bufB)
  } catch {
    return false
  }
}

const ROLE_HIERARCHY: Record<Role, Role[]> = {
  reader: ['reader'],
  editor: ['reader', 'editor'],
  admin: ['reader', 'editor', 'admin']
}

export function loadKeysFromEnv(): ApiKey[] {
  const keys: ApiKey[] = []

  const readerKey = process.env.COLDBASE_READER_KEY
  const editorKey = process.env.COLDBASE_EDITOR_KEY
  const adminKey = process.env.COLDBASE_ADMIN_KEY

  if (readerKey) keys.push({ key: readerKey, role: 'reader' })
  if (editorKey) keys.push({ key: editorKey, role: 'editor' })
  if (adminKey) keys.push({ key: adminKey, role: 'admin' })

  return keys
}

export function resolveKeys(options?: AuthOptions): ApiKey[] {
  if (!options) return []

  const useEnv = options.useEnv !== false // Default to true
  const explicitKeys = options.keys || []

  if (useEnv) {
    return [...explicitKeys, ...loadKeysFromEnv()]
  }

  return explicitKeys
}

export function hasRole(userRole: Role | null, required: Role): boolean {
  if (!userRole) return false
  return ROLE_HIERARCHY[userRole].includes(required)
}

export function createAuthMiddleware(options?: AuthOptions) {
  const keys = resolveKeys(options)
  const authEnabled = keys.length > 0

  return async (c: Context, next: Next) => {
    // If no keys configured, auth is disabled
    if (!authEnabled) {
      c.set('role', 'admin' as Role) // Full access when auth disabled
      return next()
    }

    // Extract token from Authorization header
    const authHeader = c.req.header('Authorization')
    if (!authHeader) {
      c.set('role', null)
      return next()
    }

    const [scheme, token] = authHeader.split(' ')
    if (scheme?.toLowerCase() !== 'bearer' || !token) {
      c.set('role', null)
      return next()
    }

    // Find matching key (timing-safe comparison)
    const matchedKey = keys.find(k => safeCompare(k.key, token))
    c.set('role', matchedKey?.role || null)

    return next()
  }
}

export function requireRole(required: Role) {
  return async (c: Context, next: Next) => {
    const role = c.get('role') as Role | null

    if (!hasRole(role, required)) {
      if (role === null) {
        return c.json({ error: 'Unauthorized' }, 401)
      }
      return c.json({ error: 'Forbidden' }, 403)
    }

    return next()
  }
}

export function createLoggerMiddleware() {
  return async (c: Context, next: Next) => {
    const start = Date.now()
    await next()
    const duration = Date.now() - start
    const role = c.get('role') as Role | null
    const status = c.res.status

    const logData = {
      method: c.req.method,
      path: c.req.path,
      status,
      duration,
      role
    }

    if (status >= 500) {
      logger.error('{method} {path} {status} {duration}ms', logData)
    } else if (status >= 400) {
      logger.warn('{method} {path} {status} {duration}ms', logData)
    } else {
      logger.info('{method} {path} {status} {duration}ms', logData)
    }
  }
}
