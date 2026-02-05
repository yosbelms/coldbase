import { Hono } from 'hono'
import type { Db, Collection } from '../db'
import type { VectorCollection } from '../vector-collection'
import type { SimilarityMetric } from '../types'
import { TransactionError, ValidationError, VectorDimensionError } from '../errors'
import { requireRole } from './auth'
import {
  resolveLimits,
  type HttpOptions,
  type TransactionRequest,
  type VectorSearchRequest
} from './types'

// Return appropriate HTTP status based on error type
function getErrorStatus(e: unknown): 400 | 500 {
  if (e instanceof ValidationError) return 400
  if (e instanceof VectorDimensionError) return 400
  return 400 // Default to 400 for data operations (client errors)
}

export function createDataRoutes(db: Db, options?: HttpOptions): Hono {
  const app = new Hono()
  const limits = resolveLimits(options)

  // Helper to get collection
  const getCollection = (name: string): Collection<any> => {
    return db.collection(name)
  }

  // Helper to get vector collection with metric and dimension inferred from query
  const getVectorCollection = (name: string, dimension: number, metric: SimilarityMetric = 'cosine'): VectorCollection<any> => {
    return db.vectorCollection(name, { dimension, metric })
  }

  // Reserved query params (not used for filtering)
  const RESERVED_PARAMS = new Set(['limit', 'offset', 'prefix', 'query', 'q'])

  // Helper to apply query function
  const applyQuery = (docs: any[], queryParam: string | undefined): any[] => {
    if (!queryParam || !options?.query) return docs
    try {
      return options.query(docs, queryParam)
    } catch (e) {
      throw new Error(`Query error: ${(e as Error).message}`)
    }
  }

  // Helper to parse pagination
  const parsePagination = (c: any) => {
    const limitParam = c.req.query('limit')
    const offsetParam = c.req.query('offset')

    let limit = limitParam ? parseInt(limitParam, 10) : limits.defaultPageSize
    let offset = offsetParam ? parseInt(offsetParam, 10) : 0

    // Cap limit to maxPageSize
    limit = Math.min(Math.max(1, limit), limits.maxPageSize)
    offset = Math.max(0, offset)

    return { limit, offset }
  }

  // Helper to parse basic filters from query params (prop=value)
  const parseBasicFilters = (c: any): Record<string, string> => {
    const filters: Record<string, string> = {}
    const queries = c.req.queries()

    for (const [key, values] of Object.entries(queries)) {
      if (!RESERVED_PARAMS.has(key) && values && (values as string[]).length > 0) {
        filters[key] = (values as string[])[0]
      }
    }

    return filters
  }

  // Build a where predicate combining prefix and basic filters
  const buildWherePredicate = (prefix: string | undefined, basicFilters: Record<string, string>) => {
    const hasPrefix = !!prefix
    const hasFilters = Object.keys(basicFilters).length > 0

    if (!hasPrefix && !hasFilters) return undefined

    return (doc: any): boolean => {
      // Check prefix
      if (hasPrefix && !doc.id?.startsWith(prefix)) return false

      // Check basic filters
      for (const [key, value] of Object.entries(basicFilters)) {
        const docValue = doc[key]
        if (docValue === undefined) return false

        // Boolean comparison
        if (value === 'true' && docValue !== true) return false
        if (value === 'false' && docValue !== false) return false

        // Number comparison
        if (!isNaN(Number(value)) && typeof docValue === 'number') {
          if (docValue !== Number(value)) return false
        } else if (value !== 'true' && value !== 'false') {
          // String comparison
          if (String(docValue) !== value) return false
        }
      }
      return true
    }
  }

  // GET /:collection - List/query documents
  app.get('/:collection', requireRole('reader'), async (c) => {
    try {
      const collectionName = c.req.param('collection')
      const collection = getCollection(collectionName)

      const { limit, offset } = parsePagination(c)
      const prefix = c.req.query('prefix')
      const queryParam = c.req.query('query') || c.req.query('q')
      const basicFilters = parseBasicFilters(c)

      // Build combined filter predicate
      const where = buildWherePredicate(prefix, basicFilters)

      // Find docs
      let docs = await collection.find({ where, limit: limit + 1, offset })
      let hasMore = docs.length > limit
      if (hasMore) docs.pop()

      // Custom query param
      if (queryParam && options?.query) {
        docs = applyQuery(docs, queryParam)
        const total = docs.length
        hasMore = offset + limit < total
        docs = docs.slice(offset, offset + limit)
      }

      return c.json({
        data: docs,
        pagination: { limit, offset, hasMore }
      })
    } catch (e) {
      return c.json({ error: (e as Error).message }, getErrorStatus(e))
    }
  })

  // GET /:collection/:id - Get single document
  app.get('/:collection/:id', requireRole('reader'), async (c) => {
    try {
      const collectionName = c.req.param('collection')
      const id = c.req.param('id')
      const collection = getCollection(collectionName)

      const doc = await collection.get(id)
      if (!doc) {
        return c.json({ error: 'Not found' }, 404)
      }

      return c.json({ data: doc })
    } catch (e) {
      return c.json({ error: (e as Error).message }, getErrorStatus(e))
    }
  })

  // PUT /:collection/:id - Create or update document
  app.put('/:collection/:id', requireRole('editor'), async (c) => {
    try {
      const collectionName = c.req.param('collection')
      const id = c.req.param('id')
      const collection = getCollection(collectionName)

      const body = await c.req.json()
      const doc = { id, ...body }
      await collection.put(doc)

      return c.json({ data: doc })
    } catch (e) {
      return c.json({ error: (e as Error).message }, getErrorStatus(e))
    }
  })

  // DELETE /:collection/:id - Delete document
  app.delete('/:collection/:id', requireRole('editor'), async (c) => {
    try {
      const collectionName = c.req.param('collection')
      const id = c.req.param('id')
      const collection = getCollection(collectionName)

      await collection.delete(id)

      return c.json({ data: { id, deleted: true } })
    } catch (e) {
      return c.json({ error: (e as Error).message }, getErrorStatus(e))
    }
  })

  // POST /:collection/search - Vector similarity search
  app.post('/:collection/search', requireRole('reader'), async (c) => {
    try {
      const collectionName = c.req.param('collection')
      const body: VectorSearchRequest = await c.req.json()

      if (!body.vector || !Array.isArray(body.vector)) {
        return c.json({ error: 'Missing or invalid vector' }, 400)
      }

      const k = Math.min(body.k || 10, limits.maxVectorResults)
      const metric = body.metric || 'cosine'
      const dimension = body.vector.length

      const vectorCollection = getVectorCollection(collectionName, dimension, metric)
      const results = await vectorCollection.search(body.vector, { limit: k })

      return c.json({
        data: results.map(r => ({
          id: r.id,
          score: r.score,
          data: r.data
        }))
      })
    } catch (e) {
      return c.json({ error: (e as Error).message }, getErrorStatus(e))
    }
  })

  // POST /tx - Execute transaction
  app.post('/tx', requireRole('editor'), async (c) => {
    try {
      const body: TransactionRequest = await c.req.json()

      if (!body.operations || !Array.isArray(body.operations)) {
        return c.json({ error: 'Missing or invalid operations' }, 400)
      }

      if (body.operations.length > limits.maxTxOperations) {
        return c.json({ error: `Too many operations (max ${limits.maxTxOperations})` }, 400)
      }

      const results: { type: string; collection: string; id: string; ok: boolean }[] = []

      await db.transaction(async (tx) => {
        for (const op of body.operations) {
          const collection = tx.collection(op.collection)

          if (op.type === 'put') {
            if (!op.data) {
              throw new Error(`Missing data for put operation on ${op.collection}/${op.id}`)
            }
            await collection.put({ id: op.id, ...op.data })
          } else if (op.type === 'delete') {
            await collection.delete(op.id)
          } else {
            throw new Error(`Unknown operation type: ${(op as any).type}`)
          }

          results.push({
            type: op.type,
            collection: op.collection,
            id: op.id,
            ok: true
          })
        }
      })

      return c.json({ success: true, results })
    } catch (e) {
      const isTransactionError = e instanceof TransactionError
      return c.json({
        success: false,
        error: (e as Error).message,
        compensated: isTransactionError
      }, 400)
    }
  })

  return app
}
