import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Db, FileSystemDriver } from '../lib'
import { createHttpApi } from '../lib/http'

describe('HTTP API', () => {
  let tmpDir: string
  let db: Db
  let app: ReturnType<typeof createHttpApi>

  beforeAll(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'http-test-'))
    db = new Db(new FileSystemDriver(tmpDir))
  })

  afterAll(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  beforeEach(() => {
    app = createHttpApi(db)
  })

  describe('Data Routes', () => {
    test('PUT and GET document', async () => {
      const id = 'user-' + Date.now()
      const data = { name: 'John', email: 'john@example.com' }

      // PUT
      const putRes = await app.request(`/data/users/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
      })
      expect(putRes.status).toBe(200)
      const putBody = await putRes.json()
      expect(putBody.data.name).toBe('John')

      // GET
      const getRes = await app.request(`/data/users/${id}`)
      expect(getRes.status).toBe(200)
      const getBody = await getRes.json()
      expect(getBody.data.name).toBe('John')
    })

    test('GET non-existent document returns 404', async () => {
      const res = await app.request('/data/users/non-existent-' + Date.now())
      expect(res.status).toBe(404)
    })

    test('DELETE document', async () => {
      const id = 'delete-' + Date.now()
      await app.request(`/data/users/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: 'ToDelete' })
      })

      const deleteRes = await app.request(`/data/users/${id}`, {
        method: 'DELETE'
      })
      expect(deleteRes.status).toBe(200)
      const deleteBody = await deleteRes.json()
      expect(deleteBody.data.deleted).toBe(true)

      const getRes = await app.request(`/data/users/${id}`)
      expect(getRes.status).toBe(404)
    })

    test('GET collection with pagination', async () => {
      const prefix = 'list-' + Date.now() + '-'

      // Create some documents
      for (let i = 0; i < 5; i++) {
        await app.request(`/data/items/${prefix}${i}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ index: i })
        })
      }

      // List with limit
      const res = await app.request(`/data/items?prefix=${prefix}&limit=3`)
      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.data.length).toBe(3)
      expect(body.pagination.limit).toBe(3)
      expect(body.pagination.hasMore).toBe(true)
    })

    test('GET collection respects MAX_LIMIT', async () => {
      const res = await app.request('/data/items?limit=1000')
      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.pagination.limit).toBe(100) // Capped to MAX_LIMIT
    })

    test('GET collection with basic filters (prop=value)', async () => {
      const prefix = 'filter-' + Date.now() + '-'

      // Create documents with different properties
      await app.request(`/data/filtertest/${prefix}1`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role: 'admin', active: true, score: 100 })
      })
      await app.request(`/data/filtertest/${prefix}2`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role: 'user', active: true, score: 50 })
      })
      await app.request(`/data/filtertest/${prefix}3`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role: 'admin', active: false, score: 75 })
      })

      // Filter by role
      const roleRes = await app.request(`/data/filtertest?prefix=${prefix}&role=admin`)
      expect(roleRes.status).toBe(200)
      const roleBody = await roleRes.json()
      expect(roleBody.data.length).toBe(2)
      expect(roleBody.data.every((d: any) => d.role === 'admin')).toBe(true)

      // Filter by boolean
      const activeRes = await app.request(`/data/filtertest?prefix=${prefix}&active=true`)
      expect(activeRes.status).toBe(200)
      const activeBody = await activeRes.json()
      expect(activeBody.data.length).toBe(2)
      expect(activeBody.data.every((d: any) => d.active === true)).toBe(true)

      // Filter by number
      const scoreRes = await app.request(`/data/filtertest?prefix=${prefix}&score=100`)
      expect(scoreRes.status).toBe(200)
      const scoreBody = await scoreRes.json()
      expect(scoreBody.data.length).toBe(1)
      expect(scoreBody.data[0].score).toBe(100)

      // Multiple filters
      const multiRes = await app.request(`/data/filtertest?prefix=${prefix}&role=admin&active=true`)
      expect(multiRes.status).toBe(200)
      const multiBody = await multiRes.json()
      expect(multiBody.data.length).toBe(1)
      expect(multiBody.data[0].role).toBe('admin')
      expect(multiBody.data[0].active).toBe(true)
    })

    test('POST transaction', async () => {
      const id1 = 'tx1-' + Date.now()
      const id2 = 'tx2-' + Date.now()

      const res = await app.request('/data/tx', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          operations: [
            { type: 'put', collection: 'txtest', id: id1, data: { value: 1 } },
            { type: 'put', collection: 'txtest', id: id2, data: { value: 2 } }
          ]
        })
      })

      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.success).toBe(true)
      expect(body.results.length).toBe(2)

      // Verify both documents exist
      const get1 = await app.request(`/data/txtest/${id1}`)
      const get2 = await app.request(`/data/txtest/${id2}`)
      expect(get1.status).toBe(200)
      expect(get2.status).toBe(200)
    })

    test('transaction rejects too many operations', async () => {
      const operations = Array.from({ length: 51 }, (_, i) => ({
        type: 'put' as const,
        collection: 'test',
        id: `id-${i}`,
        data: { value: i }
      }))

      const res = await app.request('/data/tx', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operations })
      })

      expect(res.status).toBe(400)
      const body = await res.json()
      expect(body.error).toContain('Too many operations')
    })
  })

  describe('Admin Routes', () => {
    test('GET /admin/collections lists collections', async () => {
      // Create a document to ensure collection exists
      await app.request('/data/admintest/doc1', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value: 1 })
      })

      const res = await app.request('/admin/collections')
      expect(res.status).toBe(200)
      const body = await res.json()
      expect(Array.isArray(body.data)).toBe(true)
    })

    test('GET /admin/:collection/stats returns stats', async () => {
      const collection = 'statstest-' + Date.now()
      await app.request(`/data/${collection}/doc1`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value: 1 })
      })

      const res = await app.request(`/admin/${collection}/stats`)
      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.data.collection).toBe(collection)
      expect(typeof body.data.count).toBe('number')
    })

    test('POST /admin/:collection/compact triggers compaction', async () => {
      const collection = 'compacttest-' + Date.now()
      await app.request(`/data/${collection}/doc1`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value: 1 })
      })

      const res = await app.request(`/admin/${collection}/compact`, {
        method: 'POST'
      })
      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.data.operation).toBe('compact')
      expect(body.data.success).toBe(true)
    })

    test('POST /admin/:collection/vacuum triggers vacuum', async () => {
      const collection = 'vacuumtest-' + Date.now()
      await app.request(`/data/${collection}/doc1`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ value: 1 })
      })

      const res = await app.request(`/admin/${collection}/vacuum`, {
        method: 'POST'
      })
      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.data.operation).toBe('vacuum')
      expect(body.data.success).toBe(true)
    })
  })

  describe('Docs Routes', () => {
    test('GET /docs/openapi.json returns OpenAPI spec', async () => {
      const res = await app.request('/docs/openapi.json')
      expect(res.status).toBe(200)
      const body = await res.json()
      expect(body.openapi).toBe('3.0.3')
      expect(body.info.title).toBe('Coldbase HTTP API')
      expect(body.paths).toBeDefined()
    })

    test('GET /docs returns Swagger UI', async () => {
      const res = await app.request('/docs')
      expect(res.status).toBe(200)
      const html = await res.text()
      expect(html).toContain('swagger')
    })
  })

  describe('Authentication', () => {
    test('auth disabled by default (no keys)', async () => {
      const appNoAuth = createHttpApi(db)
      const res = await appNoAuth.request('/data/test/doc1')
      // Should work without auth header
      expect(res.status).toBe(404) // Not found, but not 401/403
    })

    test('auth enabled with keys', async () => {
      const appAuth = createHttpApi(db, {
        auth: {
          keys: [
            { key: 'read-key', role: 'reader' },
            { key: 'write-key', role: 'editor' },
            { key: 'admin-key', role: 'admin' }
          ],
          useEnv: false
        }
      })

      // No auth header
      const noAuthRes = await appAuth.request('/data/test/doc1')
      expect(noAuthRes.status).toBe(401)

      // Read key can GET
      const readGetRes = await appAuth.request('/data/test/doc1', {
        headers: { Authorization: 'Bearer read-key' }
      })
      expect(readGetRes.status).toBe(404) // Not found, but auth passed

      // Read key cannot PUT
      const readPutRes = await appAuth.request('/data/test/doc1', {
        method: 'PUT',
        headers: {
          Authorization: 'Bearer read-key',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ value: 1 })
      })
      expect(readPutRes.status).toBe(403)

      // Write key can PUT
      const writePutRes = await appAuth.request('/data/authtest/doc1', {
        method: 'PUT',
        headers: {
          Authorization: 'Bearer write-key',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ value: 1 })
      })
      expect(writePutRes.status).toBe(200)

      // Write key cannot access admin
      const writeAdminRes = await appAuth.request('/admin/collections', {
        headers: { Authorization: 'Bearer write-key' }
      })
      expect(writeAdminRes.status).toBe(403)

      // Admin key can access admin
      const adminRes = await appAuth.request('/admin/collections', {
        headers: { Authorization: 'Bearer admin-key' }
      })
      expect(adminRes.status).toBe(200)
    })
  })

  describe('Query Function', () => {
    test('custom query function is applied', async () => {
      const prefix = 'query-' + Date.now() + '-'

      // Create documents
      for (let i = 0; i < 5; i++) {
        await app.request(`/data/querytest/${prefix}${i}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ index: i, active: i % 2 === 0 })
        })
      }

      // App with custom query function
      const appWithQuery = createHttpApi(db, {
        query: (docs, q) => {
          // Simple filter: "active" returns only active docs
          if (q === 'active') {
            return docs.filter((d: any) => d.active === true)
          }
          return docs
        }
      })

      const res = await appWithQuery.request(`/data/querytest?prefix=${prefix}&q=active`)
      expect(res.status).toBe(200)
      const body = await res.json()

      // Should only return active docs (index 0, 2, 4)
      const activeCount = body.data.filter((d: any) => d.active === true).length
      expect(activeCount).toBe(body.data.length)
    })

    test('query param ignored if no query function', async () => {
      const res = await app.request('/data/test?q=anything')
      expect(res.status).toBe(200)
      // Should not error, just ignore the query param
    })

    test('basic filters + custom query function work together', async () => {
      const prefix = 'combo-' + Date.now() + '-'

      // Create documents
      for (let i = 0; i < 6; i++) {
        await app.request(`/data/combotest/${prefix}${i}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            category: i % 2 === 0 ? 'A' : 'B',
            score: i * 10,
            active: i < 4
          })
        })
      }

      // App with custom query that filters by score > 15
      const appWithQuery = createHttpApi(db, {
        query: (docs, q) => {
          if (q === 'highScore') {
            return docs.filter((d: any) => d.score > 15)
          }
          return docs
        }
      })

      // Basic filter: category=A (indices 0, 2, 4 -> scores 0, 20, 40)
      // Custom query: score > 15 (keeps 20, 40)
      const res = await appWithQuery.request(`/data/combotest?prefix=${prefix}&category=A&q=highScore`)
      expect(res.status).toBe(200)
      const body = await res.json()

      expect(body.data.length).toBe(2)
      expect(body.data.every((d: any) => d.category === 'A' && d.score > 15)).toBe(true)
    })
  })
})
