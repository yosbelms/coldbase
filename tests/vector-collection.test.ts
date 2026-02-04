import { Db } from '../lib/db'
import { VectorCollection } from '../lib/vector-collection'
import { FileSystemDriver } from '../lib/drivers/fs'
import { VectorDimensionError, InvalidVectorError } from '../lib/errors'
import {
  cosineSimilarity,
  euclideanDistance,
  dotProduct,
  normalizeVector,
  validateVector
} from '../lib/vector-utils'
import * as fs from 'fs'
import * as path from 'path'
import * as os from 'os'

describe('Vector Utils', () => {
  describe('dotProduct', () => {
    test('computes dot product correctly', () => {
      expect(dotProduct([1, 0, 0], [1, 0, 0])).toBe(1)
      expect(dotProduct([1, 0, 0], [0, 1, 0])).toBe(0)
      expect(dotProduct([1, 2, 3], [4, 5, 6])).toBe(32) // 1*4 + 2*5 + 3*6 = 32
    })
  })

  describe('cosineSimilarity', () => {
    test('returns 1 for identical vectors', () => {
      expect(cosineSimilarity([1, 0, 0], [1, 0, 0])).toBeCloseTo(1)
      expect(cosineSimilarity([1, 2, 3], [1, 2, 3])).toBeCloseTo(1)
    })

    test('returns 0 for orthogonal vectors', () => {
      expect(cosineSimilarity([1, 0, 0], [0, 1, 0])).toBeCloseTo(0)
    })

    test('returns -1 for opposite vectors', () => {
      expect(cosineSimilarity([1, 0, 0], [-1, 0, 0])).toBeCloseTo(-1)
    })

    test('handles zero vectors', () => {
      expect(cosineSimilarity([0, 0, 0], [1, 0, 0])).toBe(0)
    })
  })

  describe('euclideanDistance', () => {
    test('returns 0 for identical vectors', () => {
      expect(euclideanDistance([1, 0, 0], [1, 0, 0])).toBe(0)
    })

    test('computes distance correctly', () => {
      expect(euclideanDistance([0, 0], [3, 4])).toBe(5)
      expect(euclideanDistance([1, 0, 0], [0, 1, 0])).toBeCloseTo(Math.sqrt(2))
    })
  })

  describe('normalizeVector', () => {
    test('normalizes to unit length', () => {
      const normalized = normalizeVector([3, 4])
      expect(normalized[0]).toBeCloseTo(0.6)
      expect(normalized[1]).toBeCloseTo(0.8)
    })

    test('handles zero vectors', () => {
      const normalized = normalizeVector([0, 0, 0])
      expect(normalized).toEqual([0, 0, 0])
    })
  })

  describe('validateVector', () => {
    test('accepts valid vectors', () => {
      expect(() => validateVector([1, 2, 3], 3)).not.toThrow()
    })

    test('throws on non-array', () => {
      expect(() => validateVector('not an array', 3)).toThrow(InvalidVectorError)
    })

    test('throws on dimension mismatch', () => {
      expect(() => validateVector([1, 2], 3)).toThrow(VectorDimensionError)
    })

    test('throws on non-number elements', () => {
      expect(() => validateVector([1, 'two', 3], 3)).toThrow(InvalidVectorError)
    })

    test('throws on non-finite numbers', () => {
      expect(() => validateVector([1, NaN, 3], 3)).toThrow(InvalidVectorError)
      expect(() => validateVector([1, Infinity, 3], 3)).toThrow(InvalidVectorError)
    })
  })
})

describe('VectorCollection', () => {
  let tmpDir: string
  let driver: FileSystemDriver
  let db: Db

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-vector-test-'))
    driver = new FileSystemDriver(tmpDir)
    db = new Db(driver, { autoCompact: false })
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('creates vector collection with options', () => {
    const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
      dimension: 3,
      metric: 'cosine'
    })

    expect(embeddings).toBeInstanceOf(VectorCollection)
  })

  test('returns same collection instance for same name', () => {
    const col1 = db.vectorCollection('test', { dimension: 3 })
    const col2 = db.vectorCollection('test', { dimension: 3 })
    expect(col1).toBe(col2)
  })

  test('put and get vector documents', async () => {
    const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
      dimension: 3
    })

    await embeddings.put({ id: 'a', vector: [1, 0, 0], text: 'hello' })

    const result = await embeddings.get('a')
    expect(result?.id).toBe('a')
    expect(result?.text).toBe('hello')
    // Vector should be normalized (default for cosine)
    expect(result?.vector).toEqual([1, 0, 0])
  })

  test('validates vector dimension on insert', async () => {
    const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
      dimension: 3
    })

    await expect(
      embeddings.put({ id: 'a', vector: [1, 0] })
    ).rejects.toThrow(VectorDimensionError)
  })

  test('validates vector elements on insert', async () => {
    const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
      dimension: 3
    })

    await expect(
      embeddings.put({ id: 'a', vector: [1, NaN, 0] })
    ).rejects.toThrow(InvalidVectorError)
  })

  test('batch writes multiple vector documents', async () => {
    const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
      dimension: 3
    })

    await embeddings.batch(tx => {
      tx.put({ id: 'a', vector: [1, 0, 0], text: 'right' })
      tx.put({ id: 'b', vector: [0, 1, 0], text: 'up' })
      tx.put({ id: 'c', vector: [0, 0, 1], text: 'forward' })
    })

    const count = await embeddings.count()
    expect(count).toBe(3)
  })

  test('getMany retrieves multiple documents', async () => {
    const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
      dimension: 3
    })

    await embeddings.batch(tx => {
      tx.put({ id: 'a', vector: [1, 0, 0] })
      tx.put({ id: 'b', vector: [0, 1, 0] })
      tx.put({ id: 'c', vector: [0, 0, 1] })
    })

    const results = await embeddings.getMany(['a', 'c', 'nonexistent'])
    expect(results.size).toBe(2)
    expect(results.has('a')).toBe(true)
    expect(results.has('c')).toBe(true)
    expect(results.has('nonexistent')).toBe(false)
  })

  describe('search', () => {
    test('finds similar vectors with cosine similarity', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
        dimension: 3,
        metric: 'cosine'
      })

      await embeddings.batch(tx => {
        tx.put({ id: 'a', vector: [1, 0, 0], text: 'right' })
        tx.put({ id: 'b', vector: [0, 1, 0], text: 'up' })
        tx.put({ id: 'c', vector: [0.9, 0.1, 0], text: 'mostly right' })
      })

      const results = await embeddings.search([1, 0, 0], { limit: 2 })

      expect(results).toHaveLength(2)
      expect(results[0].id).toBe('a')
      expect(results[0].score).toBeCloseTo(1)
      expect(results[1].id).toBe('c')
      expect(results[1].score).toBeGreaterThan(0.9)
    })

    test('finds similar vectors with euclidean distance', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3,
        metric: 'euclidean',
        normalize: false
      })

      await embeddings.batch(tx => {
        tx.put({ id: 'a', vector: [0, 0, 0] })
        tx.put({ id: 'b', vector: [1, 0, 0] })
        tx.put({ id: 'c', vector: [10, 0, 0] })
      })

      const results = await embeddings.search([0, 0, 0], { limit: 2 })

      expect(results).toHaveLength(2)
      expect(results[0].id).toBe('a')
      expect(results[0].score).toBe(0)
      expect(results[1].id).toBe('b')
      expect(results[1].score).toBe(1)
    })

    test('finds similar vectors with dot product', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3,
        metric: 'dotProduct',
        normalize: false
      })

      await embeddings.batch(tx => {
        tx.put({ id: 'a', vector: [1, 0, 0] })
        tx.put({ id: 'b', vector: [2, 0, 0] })
        tx.put({ id: 'c', vector: [0, 1, 0] })
      })

      const results = await embeddings.search([1, 0, 0], { limit: 2 })

      expect(results).toHaveLength(2)
      expect(results[0].id).toBe('b')
      expect(results[0].score).toBe(2)
      expect(results[1].id).toBe('a')
      expect(results[1].score).toBe(1)
    })

    test('applies threshold filter', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3,
        metric: 'cosine'
      })

      await embeddings.batch(tx => {
        tx.put({ id: 'a', vector: [1, 0, 0] })
        tx.put({ id: 'b', vector: [0, 1, 0] })
        tx.put({ id: 'c', vector: [0.7, 0.7, 0] })
      })

      const results = await embeddings.search([1, 0, 0], { threshold: 0.8 })

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe('a')
    })

    test('applies metadata filter as object', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; category: string }>('embeddings', {
        dimension: 3
      })

      await embeddings.batch(tx => {
        tx.put({ id: 'a', vector: [1, 0, 0], category: 'fruit' })
        tx.put({ id: 'b', vector: [0.9, 0.1, 0], category: 'vegetable' })
        tx.put({ id: 'c', vector: [0.8, 0.2, 0], category: 'fruit' })
      })

      const results = await embeddings.search([1, 0, 0], { filter: { category: 'fruit' } })

      expect(results).toHaveLength(2)
      expect(results.every(r => r.data.category === 'fruit')).toBe(true)
    })

    test('applies metadata filter as function', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; score: number }>('embeddings', {
        dimension: 3
      })

      await embeddings.batch(tx => {
        tx.put({ id: 'a', vector: [1, 0, 0], score: 100 })
        tx.put({ id: 'b', vector: [0.9, 0.1, 0], score: 50 })
        tx.put({ id: 'c', vector: [0.8, 0.2, 0], score: 75 })
      })

      const results = await embeddings.search([1, 0, 0], {
        filter: item => item.score >= 75
      })

      expect(results).toHaveLength(2)
      expect(results.every(r => r.data.score >= 75)).toBe(true)
    })

    test('excludes vector from results by default', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0], text: 'hello' })

      const results = await embeddings.search([1, 0, 0])

      expect(results[0].data).not.toHaveProperty('vector')
      expect(results[0].data.text).toBe('hello')
    })

    test('includes vector when requested', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0], text: 'hello' })

      const results = await embeddings.search([1, 0, 0], { includeVector: true })

      expect(results[0].data).toHaveProperty('vector')
    })

    test('validates query vector dimension', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await expect(
        embeddings.search([1, 0])
      ).rejects.toThrow(VectorDimensionError)
    })

    test('handles deleted documents', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0] })
      await embeddings.put({ id: 'b', vector: [0, 1, 0] })
      await embeddings.delete('a') // Delete 'a'

      const results = await embeddings.search([1, 0, 0])

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe('b')
    })

    test('returns empty array for empty collection', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      const results = await embeddings.search([1, 0, 0])
      expect(results).toHaveLength(0)
    })

    test('returns empty array when no results match filter', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; category: string }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0], category: 'fruit' })

      const results = await embeddings.search([1, 0, 0], { filter: { category: 'vegetable' } })
      expect(results).toHaveLength(0)
    })

    test('returns empty array when no results match threshold', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3,
        metric: 'cosine'
      })

      await embeddings.put({ id: 'a', vector: [0, 1, 0] }) // orthogonal to query

      const results = await embeddings.search([1, 0, 0], { threshold: 0.9 })
      expect(results).toHaveLength(0)
    })

    test('applies euclidean threshold as max distance', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3,
        metric: 'euclidean',
        normalize: false
      })

      await embeddings.batch(tx => {
        tx.put({ id: 'close', vector: [0.5, 0, 0] })   // distance 0.5
        tx.put({ id: 'far', vector: [10, 0, 0] })      // distance 10
      })

      const results = await embeddings.search([0, 0, 0], { threshold: 1 })

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe('close')
    })

    test('applies dotProduct threshold as min score', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3,
        metric: 'dotProduct',
        normalize: false
      })

      await embeddings.batch(tx => {
        tx.put({ id: 'high', vector: [2, 0, 0] })   // dot product 2
        tx.put({ id: 'low', vector: [0.1, 0, 0] }) // dot product 0.1
      })

      const results = await embeddings.search([1, 0, 0], { threshold: 1 })

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe('high')
    })

    test('handles updates to existing vectors', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0], text: 'original' })
      await embeddings.put({ id: 'a', vector: [0, 1, 0], text: 'updated' }) // Update vector

      const results = await embeddings.search([0, 1, 0], { limit: 1 })

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe('a')
      expect(results[0].data.text).toBe('updated')
      expect(results[0].score).toBeCloseTo(1) // Should match the updated vector
    })

    test('handles all documents deleted', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0] })
      await embeddings.put({ id: 'b', vector: [0, 1, 0] })
      await embeddings.delete('a')
      await embeddings.delete('b')

      const results = await embeddings.search([1, 0, 0])
      expect(results).toHaveLength(0)
    })

    test('validates query vector for NaN', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await expect(embeddings.search([1, NaN, 0])).rejects.toThrow(InvalidVectorError)
    })

    test('validates query vector for Infinity', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await expect(embeddings.search([1, Infinity, 0])).rejects.toThrow(InvalidVectorError)
    })

    test('respects normalize=false for cosine metric', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3,
        metric: 'cosine',
        normalize: false
      })

      // Insert non-normalized vector
      await embeddings.put({ id: 'a', vector: [3, 4, 0] })

      const doc = await embeddings.get('a')
      // Vector should NOT be normalized (magnitude should be 5, not 1)
      expect(doc?.vector[0]).toBe(3)
      expect(doc?.vector[1]).toBe(4)
    })

    test('normalizes vectors by default for cosine metric', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3,
        metric: 'cosine'
        // normalize defaults to true for cosine
      })

      await embeddings.put({ id: 'a', vector: [3, 4, 0] })

      const doc = await embeddings.get('a')
      // Vector should be normalized (magnitude should be 1)
      expect(doc?.vector[0]).toBeCloseTo(0.6)
      expect(doc?.vector[1]).toBeCloseTo(0.8)
    })

    test('does not normalize by default for euclidean metric', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3,
        metric: 'euclidean'
        // normalize defaults to false for euclidean
      })

      await embeddings.put({ id: 'a', vector: [3, 4, 0] })

      const doc = await embeddings.get('a')
      // Vector should NOT be normalized
      expect(doc?.vector[0]).toBe(3)
      expect(doc?.vector[1]).toBe(4)
    })
  })

  describe('find', () => {
    test('finds documents with where clause', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; category: string }>('embeddings', {
        dimension: 3
      })

      await embeddings.batch(tx => {
        tx.put({ id: 'a', vector: [1, 0, 0], category: 'fruit' })
        tx.put({ id: 'b', vector: [0, 1, 0], category: 'vegetable' })
        tx.put({ id: 'c', vector: [0, 0, 1], category: 'fruit' })
      })

      const results = await embeddings.find({ where: { category: 'fruit' } })

      expect(results).toHaveLength(2)
    })

    test('excludes vector from find results by default', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0], text: 'hello' })

      const results = await embeddings.find()

      expect(results[0]).not.toHaveProperty('vector')
    })

    test('includes vector in find when requested', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0], text: 'hello' })

      const results = await embeddings.find({ includeVector: true })

      expect(results[0]).toHaveProperty('vector')
    })
  })

  describe('TTL', () => {
    test('expired records are not returned', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; expiresAt: number }>('embeddings', {
        dimension: 3,
        ttlField: 'expiresAt'
      })

      const past = Date.now() - 1000
      const future = Date.now() + 100000

      await embeddings.batch(tx => {
        tx.put({ id: 'a', vector: [1, 0, 0], expiresAt: past })
        tx.put({ id: 'b', vector: [0, 1, 0], expiresAt: future })
      })

      const results = await embeddings.search([1, 0, 0])

      expect(results).toHaveLength(1)
      expect(results[0].id).toBe('b')
    })
  })

  describe('compaction', () => {
    test('compact merges mutations', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0] })
      await embeddings.put({ id: 'b', vector: [0, 1, 0] })

      const beforeMutations = await driver.list('embeddings.mutation.')
      expect(beforeMutations.keys.length).toBe(2)

      await embeddings.compact()

      const afterMutations = await driver.list('embeddings.mutation.')
      expect(afterMutations.keys.length).toBe(0)

      // Data should still be accessible
      const results = await embeddings.search([1, 0, 0], { limit: 2 })
      expect(results).toHaveLength(2)
    })

    test('vacuum removes deleted and duplicate records', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      // Create records, update one, delete another
      await embeddings.put({ id: 'a', vector: [1, 0, 0] })
      await embeddings.put({ id: 'b', vector: [0, 1, 0] })
      await embeddings.put({ id: 'a', vector: [0.5, 0.5, 0] }) // Update 'a'
      await embeddings.delete('b') // Delete 'b'

      await embeddings.compact()
      const result = await embeddings.vacuum()

      expect(result.recordsRemoved).toBeGreaterThan(0)

      // Only 'a' should remain with updated vector
      const count = await embeddings.count()
      expect(count).toBe(1)

      const doc = await embeddings.get('a')
      expect(doc?.vector[0]).toBeCloseTo(0.5 / Math.sqrt(0.5))
    })
  })

  describe('get', () => {
    test('returns undefined for non-existent key', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      const result = await embeddings.get('nonexistent')
      expect(result).toBeUndefined()
    })

    test('returns undefined for deleted key', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0] })
      await embeddings.delete('a')

      const result = await embeddings.get('a')
      expect(result).toBeUndefined()
    })

    test('returns latest version after update', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; version: number }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0], version: 1 })
      await embeddings.put({ id: 'a', vector: [0, 1, 0], version: 2 })

      const result = await embeddings.get('a')
      expect(result?.version).toBe(2)
    })
  })

  describe('count', () => {
    test('returns 0 for empty collection', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      const count = await embeddings.count()
      expect(count).toBe(0)
    })

    test('excludes deleted records', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0] })
      await embeddings.put({ id: 'b', vector: [0, 1, 0] })
      await embeddings.delete('a')

      const count = await embeddings.count()
      expect(count).toBe(1)
    })

    test('counts unique records after updates', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0] })
      await embeddings.put({ id: 'a', vector: [0, 1, 0] }) // Update
      await embeddings.put({ id: 'a', vector: [0, 0, 1] }) // Update again

      const count = await embeddings.count()
      expect(count).toBe(1)
    })
  })

  describe('deleteExpired', () => {
    test('deletes expired records and returns count', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[]; expiresAt: number }>('embeddings', {
        dimension: 3,
        ttlField: 'expiresAt'
      })

      const past = Date.now() - 1000
      const future = Date.now() + 100000

      await embeddings.batch(tx => {
        tx.put({ id: 'expired1', vector: [1, 0, 0], expiresAt: past })
        tx.put({ id: 'expired2', vector: [0, 1, 0], expiresAt: past })
        tx.put({ id: 'valid', vector: [0, 0, 1], expiresAt: future })
      })

      const deletedCount = await embeddings.deleteExpired()

      expect(deletedCount).toBe(2)

      const count = await embeddings.count()
      expect(count).toBe(1)
    })

    test('returns 0 when no ttlField configured', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
        // No ttlField
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0] })

      const deletedCount = await embeddings.deleteExpired()
      expect(deletedCount).toBe(0)
    })
  })

  describe('read', () => {
    test('streams all records including mutations', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0] })
      await embeddings.put({ id: 'b', vector: [0, 1, 0] })

      const records: { id: string }[] = []
      for await (const record of embeddings.read()) {
        records.push({ id: record.id })
      }

      expect(records).toHaveLength(2)
    })
  })

  describe('countMutationFiles', () => {
    test('counts pending mutation files', async () => {
      const embeddings = db.vectorCollection<{ id: string; vector: number[] }>('embeddings', {
        dimension: 3
      })

      await embeddings.put({ id: 'a', vector: [1, 0, 0] })
      await embeddings.put({ id: 'b', vector: [0, 1, 0] })
      await embeddings.put({ id: 'c', vector: [0, 0, 1] })

      const count = await embeddings.countMutationFiles()
      expect(count).toBe(3)

      await embeddings.compact()

      const countAfter = await embeddings.countMutationFiles()
      expect(countAfter).toBe(0)
    })
  })
})

describe('Integration test from plan', () => {
  let tmpDir: string
  let driver: FileSystemDriver
  let db: Db

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-vector-integration-'))
    driver = new FileSystemDriver(tmpDir)
    db = new Db(driver, { autoCompact: false })
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('example from plan works correctly', async () => {
    const embeddings = db.vectorCollection<{ id: string; vector: number[]; text: string }>('embeddings', {
      dimension: 3,
      metric: 'cosine'
    })

    await embeddings.put({ id: 'a', vector: [1, 0, 0], text: 'right' })
    await embeddings.put({ id: 'b', vector: [0, 1, 0], text: 'up' })
    await embeddings.put({ id: 'c', vector: [0.9, 0.1, 0], text: 'mostly right' })

    const results = await embeddings.search([1, 0, 0], { limit: 2 })

    // Expected: 'a' (score=1.0), 'c' (score~0.99)
    expect(results).toHaveLength(2)
    expect(results[0].id).toBe('a')
    expect(results[0].score).toBeCloseTo(1.0)
    expect(results[1].id).toBe('c')
    expect(results[1].score).toBeGreaterThan(0.9)
  })
})
