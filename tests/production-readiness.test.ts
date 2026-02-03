import { monotonicTimestamp, TopKHeap } from '../lib/utils'  // internal, not part of public API
import { Db } from '../lib/db'
import { FileSystemDriver } from '../lib/drivers/fs'
import { ValidationError } from '../lib/errors'
import * as fs from 'fs'
import * as path from 'path'
import * as os from 'os'

describe('monotonicTimestamp', () => {
  test('returns strictly increasing values', () => {
    const values: number[] = []
    for (let i = 0; i < 1000; i++) {
      values.push(monotonicTimestamp())
    }
    for (let i = 1; i < values.length; i++) {
      expect(values[i]).toBeGreaterThan(values[i - 1])
    }
  })

  test('returns values close to Date.now()', () => {
    const before = Date.now()
    const ts = monotonicTimestamp()
    const after = Date.now()
    // Allow a small window since rapid calls may increment past Date.now()
    expect(ts).toBeGreaterThanOrEqual(before)
    expect(ts).toBeLessThanOrEqual(after + 1000)
  })
})

describe('TopKHeap', () => {
  test('keeps top-k smallest items with min-heap comparator', () => {
    // comparator: a < b means a is "worse" and gets evicted first
    // For "top-k largest": evict smallest => compare = (a, b) => a.val - b.val
    const heap = new TopKHeap<{ val: number }>(3, (a, b) => a.val - b.val)
    for (const val of [5, 1, 8, 3, 9, 2, 7]) {
      heap.push({ val })
    }
    const result = heap.toSortedArray()
    expect(result.map(r => r.val)).toEqual([9, 8, 7])
  })

  test('keeps top-k largest items with max-heap comparator', () => {
    // For "top-k smallest": evict largest => compare = (a, b) => b.val - a.val
    const heap = new TopKHeap<{ val: number }>(3, (a, b) => b.val - a.val)
    for (const val of [5, 1, 8, 3, 9, 2, 7]) {
      heap.push({ val })
    }
    const result = heap.toSortedArray()
    expect(result.map(r => r.val)).toEqual([1, 2, 3])
  })

  test('works when fewer items than k', () => {
    const heap = new TopKHeap<{ val: number }>(10, (a, b) => a.val - b.val)
    heap.push({ val: 3 })
    heap.push({ val: 1 })
    const result = heap.toSortedArray()
    expect(result.map(r => r.val)).toEqual([3, 1])
    expect(heap.size).toBe(2)
  })

  test('works with k=1', () => {
    const heap = new TopKHeap<{ val: number }>(1, (a, b) => a.val - b.val)
    for (const val of [5, 1, 8, 3]) {
      heap.push({ val })
    }
    const result = heap.toSortedArray()
    expect(result).toEqual([{ val: 8 }])
  })

  test('handles duplicate values', () => {
    const heap = new TopKHeap<{ val: number }>(3, (a, b) => a.val - b.val)
    for (const val of [5, 5, 5, 5, 5]) {
      heap.push({ val })
    }
    expect(heap.size).toBe(3)
    const result = heap.toSortedArray()
    expect(result.every(r => r.val === 5)).toBe(true)
  })
})

describe('Collection name validation', () => {
  let tmpDir: string
  let driver: FileSystemDriver
  let db: Db

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-validation-test-'))
    driver = new FileSystemDriver(tmpDir)
    db = new Db(driver, { autoCompact: false })
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('accepts valid collection names', () => {
    expect(() => db.collection('users')).not.toThrow()
    expect(() => db.collection('my-collection')).not.toThrow()
    expect(() => db.collection('my_collection')).not.toThrow()
    expect(() => db.collection('Collection123')).not.toThrow()
    expect(() => db.collection('a')).not.toThrow()
    expect(() => db.collection('A')).not.toThrow()
    expect(() => db.collection('0test')).not.toThrow()
  })

  test('rejects empty name', () => {
    expect(() => db.collection('')).toThrow(ValidationError)
  })

  test('rejects names starting with non-alphanumeric', () => {
    expect(() => db.collection('_users')).toThrow(ValidationError)
    expect(() => db.collection('-users')).toThrow(ValidationError)
    expect(() => db.collection('.users')).toThrow(ValidationError)
  })

  test('rejects names with special characters', () => {
    expect(() => db.collection('my.collection')).toThrow(ValidationError)
    expect(() => db.collection('my collection')).toThrow(ValidationError)
    expect(() => db.collection('my/collection')).toThrow(ValidationError)
    expect(() => db.collection('test.mutation.123')).toThrow(ValidationError)
    expect(() => db.collection('test.jsonl')).toThrow(ValidationError)
    expect(() => db.collection('test.lock')).toThrow(ValidationError)
  })

  test('rejects names exceeding 64 characters', () => {
    const longName = 'a'.repeat(65)
    expect(() => db.collection(longName)).toThrow(ValidationError)

    const okName = 'a'.repeat(64)
    expect(() => db.collection(okName)).not.toThrow()
  })

  test('validation also applies to vectorCollection', () => {
    expect(() => db.vectorCollection('.bad', { dimension: 3 })).toThrow(ValidationError)
  })

  test('validation applies to compact and vacuum', async () => {
    await expect(db.compact('.bad')).rejects.toThrow(ValidationError)
    await expect(db.vacuum('.bad')).rejects.toThrow(ValidationError)
  })

  test('returns cached collection without re-validating', () => {
    const col1 = db.collection('valid-name')
    const col2 = db.collection('valid-name')
    expect(col1).toBe(col2)
  })
})
