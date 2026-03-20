/**
 * Tests for two improvements:
 * 6. Per-collection bloom filter config overrides
 * 7. onRead observability hook (duration, mutationCount)
 */
import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Db } from '../lib/db'
import { FileSystemDriver } from '../lib/drivers/fs'

async function makeTmpDir() {
  return fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-improve-'))
}
async function cleanup(dir: string) {
  await fs.promises.rm(dir, { recursive: true, force: true })
}

// ─── Improvement 6: Per-collection bloom filter config overrides ─────────────

describe('Improvement 6: per-collection bloom filter overrides', () => {
  let tmpDir: string
  let driver: FileSystemDriver

  beforeEach(async () => {
    tmpDir = await makeTmpDir()
    driver = new FileSystemDriver(tmpDir)
  })

  afterEach(() => cleanup(tmpDir))

  test('collection-level useBloomFilter=true overrides Db-level useBloomFilter=false', async () => {
    const db = new Db(driver, { autoCompact: false, useBloomFilter: false })
    const withBloom = db.collection<{ id: string }>('with-bloom', { useBloomFilter: true })

    await withBloom.put({ id: '1' })
    // Use collection.compact() so the collection's own compactor (with bloom enabled) runs
    await withBloom.compact()

    // Bloom filter file should exist because the collection opted in
    const bloomFile = await driver.get('with-bloom.bloom')
    expect(bloomFile).toBeDefined()
  })

  test('Db-level useBloomFilter=false: collection without override has no bloom filter', async () => {
    const db = new Db(driver, { autoCompact: false, useBloomFilter: false })
    const plain = db.collection<{ id: string }>('no-bloom')

    await plain.put({ id: '1' })
    await db.compact('no-bloom')

    const bloomFile = await driver.get('no-bloom.bloom')
    expect(bloomFile).toBeUndefined()
  })

  test('different collections in same Db can have different bloom filter settings', async () => {
    const db = new Db(driver, { autoCompact: false, useBloomFilter: false })
    const withBloom = db.collection<{ id: string }>('has-bloom', { useBloomFilter: true })
    const noBloom = db.collection<{ id: string }>('no-bloom-2')

    await withBloom.put({ id: '1' })
    await noBloom.put({ id: '1' })
    await withBloom.compact()
    await db.compact('no-bloom-2')

    expect(await driver.get('has-bloom.bloom')).toBeDefined()
    expect(await driver.get('no-bloom-2.bloom')).toBeUndefined()
  })
})

// ─── Improvement 7: onRead observability hook ────────────────────────────────

describe('Improvement 7: onRead hook reports collection, duration, mutationCount', () => {
  let tmpDir: string
  let driver: FileSystemDriver

  beforeEach(async () => {
    tmpDir = await makeTmpDir()
    driver = new FileSystemDriver(tmpDir)
  })

  afterEach(() => cleanup(tmpDir))

  test('get() calls onRead with correct collection name and duration', async () => {
    const onRead = jest.fn()
    const db = new Db(driver, { autoCompact: false, hooks: { onRead } })
    const col = db.collection<{ id: string }>('obs-col')

    await col.put({ id: '1' })
    onRead.mockClear()

    await col.get('1')

    expect(onRead).toHaveBeenCalledTimes(1)
    const [collection, durationMs] = onRead.mock.calls[0]
    expect(collection).toBe('obs-col')
    expect(durationMs).toBeGreaterThanOrEqual(0)
    expect(typeof durationMs).toBe('number')
  })

  test('get() reports mutationCount=0 when no mutations are pending', async () => {
    const onRead = jest.fn()
    const db = new Db(driver, { autoCompact: false, hooks: { onRead } })
    const col = db.collection<{ id: string }>('clean-state')

    await col.put({ id: '1' })
    await col.compact()
    onRead.mockClear()

    await col.get('1')

    expect(onRead.mock.calls[0][2]).toBe(0) // mutationCount
  })

  test('get() reports mutationCount > 0 when mutations are pending', async () => {
    const onRead = jest.fn()
    const db = new Db(driver, { autoCompact: false, hooks: { onRead } })
    const col = db.collection<{ id: string; val: number }>('pending-muts')

    await col.put({ id: 'base', val: 1 })
    await col.compact()
    await col.put({ id: 'new1', val: 2 })
    await col.put({ id: 'new2', val: 3 })
    onRead.mockClear()

    await col.get('base')

    expect(onRead.mock.calls[0][2]).toBe(2) // 2 pending mutation files
  })

  test('find() calls onRead with collection name and duration', async () => {
    const onRead = jest.fn()
    const db = new Db(driver, { autoCompact: false, hooks: { onRead } })
    const col = db.collection<{ id: string; role: string }>('find-obs')

    await col.put({ id: '1', role: 'admin' })
    await col.put({ id: '2', role: 'user' })
    await col.compact()
    onRead.mockClear()

    await col.find({ where: { role: 'admin' } })

    expect(onRead).toHaveBeenCalledTimes(1)
    const [collection, durationMs] = onRead.mock.calls[0]
    expect(collection).toBe('find-obs')
    expect(durationMs).toBeGreaterThanOrEqual(0)
  })

  test('onRead is not called for writes', async () => {
    const onRead = jest.fn()
    const db = new Db(driver, { autoCompact: false, hooks: { onRead } })
    const col = db.collection<{ id: string }>('write-no-read')

    await col.put({ id: '1' })
    await col.delete('1')

    expect(onRead).not.toHaveBeenCalled()
  })

  test('VectorCollection.get() calls onRead', async () => {
    const onRead = jest.fn()
    const db = new Db(driver, { autoCompact: false, hooks: { onRead } })
    const vecs = db.vectorCollection<{ id: string; vector: number[] }>('vec-read', { dimension: 2 })

    await vecs.put({ id: 'v1', vector: [1, 0] })
    onRead.mockClear()

    await vecs.get('v1')

    expect(onRead).toHaveBeenCalledTimes(1)
    expect(onRead.mock.calls[0][0]).toBe('vec-read')
  })

  test('VectorCollection.search() calls onRead', async () => {
    const onRead = jest.fn()
    const db = new Db(driver, { autoCompact: false, hooks: { onRead } })
    const vecs = db.vectorCollection<{ id: string; vector: number[] }>('vec-search', { dimension: 2 })

    await vecs.put({ id: 'v1', vector: [1, 0] })
    onRead.mockClear()

    await vecs.search([1, 0], { limit: 5 })

    expect(onRead).toHaveBeenCalledTimes(1)
    const [collection, durationMs] = onRead.mock.calls[0]
    expect(collection).toBe('vec-search')
    expect(durationMs).toBeGreaterThanOrEqual(0)
  })

  test('onRead is not fired when hook is not configured', async () => {
    // Verify no error is thrown when hooks are absent
    const db = new Db(driver, { autoCompact: false })
    const col = db.collection<{ id: string }>('no-hook')
    await col.put({ id: '1' })
    await expect(col.get('1')).resolves.toBeDefined()
    await expect(col.find()).resolves.toBeDefined()
  })
})
