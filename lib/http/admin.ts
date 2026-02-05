import { Hono } from 'hono'
import type { Db } from '../db'
import { ValidationError, LockActiveError } from '../errors'
import { requireRole } from './auth'

// Return appropriate HTTP status based on error type
function getErrorStatus(e: unknown): 400 | 409 | 500 {
  if (e instanceof ValidationError) return 400
  if (e instanceof LockActiveError) return 409
  return 500
}

export function createAdminRoutes(db: Db): Hono {
  const app = new Hono()

  // All admin routes require admin permission
  app.use('*', requireRole('admin'))

  // GET /collections - List all collections
  app.get('/collections', async (c) => {
    try {
      // Get collections from driver by listing top-level prefixes
      const driver = db.driver
      const result = await driver.list('')

      // Extract unique collection names from keys
      const collections = new Set<string>()
      for (const key of result.keys) {
        const match = key.match(/^([^.]+)\./)
        if (match) {
          collections.add(match[1])
        }
      }

      return c.json({ data: Array.from(collections).sort() })
    } catch (e) {
      return c.json({ error: (e as Error).message }, getErrorStatus(e))
    }
  })

  // GET /:collection/stats - Collection stats
  app.get('/:collection/stats', async (c) => {
    try {
      const collectionName = c.req.param('collection')
      const collection = db.collection(collectionName)

      const count = await collection.count()

      // Try to get size from driver
      const driver = db.driver
      const mainKey = `${collectionName}.jsonl`
      const size = await driver.size(mainKey)

      return c.json({
        data: {
          collection: collectionName,
          count,
          size: size || 0
        }
      })
    } catch (e) {
      return c.json({ error: (e as Error).message }, getErrorStatus(e))
    }
  })

  // POST /:collection/compact - Trigger compaction
  app.post('/:collection/compact', async (c) => {
    try {
      const collectionName = c.req.param('collection')
      const collection = db.collection(collectionName)

      await collection.compact()

      return c.json({
        data: {
          collection: collectionName,
          operation: 'compact',
          success: true
        }
      })
    } catch (e) {
      return c.json({ error: (e as Error).message }, getErrorStatus(e))
    }
  })

  // POST /:collection/vacuum - Trigger vacuum
  app.post('/:collection/vacuum', async (c) => {
    try {
      const collectionName = c.req.param('collection')
      const collection = db.collection(collectionName)

      await collection.vacuum()

      return c.json({
        data: {
          collection: collectionName,
          operation: 'vacuum',
          success: true
        }
      })
    } catch (e) {
      return c.json({ error: (e as Error).message }, getErrorStatus(e))
    }
  })

  return app
}
