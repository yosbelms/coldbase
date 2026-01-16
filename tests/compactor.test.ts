import { CollectionCompactor } from '../lib/compactor'
import { FileSystemDriver } from '../lib/drivers/fs'
import { Db } from '../lib/db'
import { LockActiveError } from '../lib/errors'
import { streamToString } from '../lib/utils'
import * as fs from 'fs'
import * as path from 'path'
import * as os from 'os'

describe('CollectionCompactor', () => {
  let tmpDir: string
  let driver: FileSystemDriver
  let compactor: CollectionCompactor
  let db: Db
  const collectionName = 'test-compactor'

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-compactor-test-'))
    driver = new FileSystemDriver(tmpDir)
    compactor = new CollectionCompactor(driver)
    db = new Db(driver, { autoCompact: false })
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('compact merges mutations into main file', async () => {
    const collection = db.collection<{ id: string; val: number }>(collectionName)
    await collection.put('1', { id: '1', val: 1 })
    await collection.put('2', { id: '2', val: 2 })

    const result = await compactor.compact(collectionName)

    expect(result.mutationsProcessed).toBe(2)
    expect(result.durationMs).toBeGreaterThanOrEqual(0)

    const list = await driver.list(`${collectionName}.mutation.`)
    expect(list.keys.length).toBe(0)

    const mainResp = await driver.get(`${collectionName}.jsonl`)
    expect(mainResp).toBeDefined()
    const content = await streamToString(mainResp!.stream)

    const lines = content.trim().split('\n')
    expect(lines.length).toBe(2)

    const parsed = lines.map(l => JSON.parse(l))
    const record1 = parsed.find(p => p[0] === '1')
    const record2 = parsed.find(p => p[0] === '2')
    expect(record1).toBeDefined()
    expect(record1![1]).toEqual({ id: '1', val: 1 })
    expect(typeof record1![2]).toBe('number')
    expect(record2).toBeDefined()
    expect(record2![1]).toEqual({ id: '2', val: 2 })
    expect(typeof record2![2]).toBe('number')
  })

  test('compact handles multiple batches', async () => {
    const batch1 = [['1', { val: 1 }]]
    const batch2 = [['1', { val: 2 }]]

    await driver.put(`${collectionName}.mutation.1`, JSON.stringify(batch1))
    await driver.put(`${collectionName}.mutation.2`, JSON.stringify(batch2))

    await compactor.compact(collectionName)

    const mainResp = await driver.get(`${collectionName}.jsonl`)
    const content = await streamToString(mainResp!.stream)
    const lines = content.trim().split('\n')

    expect(lines.length).toBe(2)
    const parsed = lines.map(l => JSON.parse(l))
    expect(parsed[0]).toEqual(['1', { val: 1 }])
    expect(parsed[1]).toEqual(['1', { val: 2 }])
  })

  test('vacuum deduplicates and removes deletions', async () => {
    const lines = [
      JSON.stringify(['1', { v: 1 }]),
      JSON.stringify(['2', { v: 1 }]),
      JSON.stringify(['1', { v: 2 }]),
      JSON.stringify(['3', { v: 1 }]),
      JSON.stringify(['2', null])
    ]

    await driver.put(`${collectionName}.jsonl`, lines.join('\n') + '\n')

    const result = await compactor.vacuum(collectionName)
    expect(result.recordsRemoved).toBe(3) // 2 duplicates + 1 deletion

    const mainResp = await driver.get(`${collectionName}.jsonl`)
    const content = await streamToString(mainResp!.stream)
    const resultLines = content.trim().split('\n')

    expect(resultLines.length).toBe(2)
    const parsed = resultLines.map(l => JSON.parse(l))

    expect(parsed).toContainEqual(['1', { v: 2 }])
    expect(parsed).toContainEqual(['3', { v: 1 }])
  })

  test('vacuum handles empty or missing file', async () => {
    await compactor.vacuum('non-existent')
    const result = await driver.get('non-existent.jsonl')
    expect(result).toBeUndefined()
  })

  test('vacuum handles file with only deletions', async () => {
    const lines = [JSON.stringify(['1', { v: 1 }]), JSON.stringify(['1', null])]
    await driver.put(`${collectionName}.jsonl`, lines.join('\n'))

    await compactor.vacuum(collectionName)

    const result = await driver.get(`${collectionName}.jsonl`)
    if (result) {
      const content = await streamToString(result.stream)
      expect(content.trim()).toBe('')
    }
  })

  test('lock prevents concurrent access', async () => {
    const lockKey = `${collectionName}.lock`
    const otherSession = 'other-session'
    await driver.put(lockKey, JSON.stringify({ sessionId: otherSession, expiresAt: Date.now() + 30000 }))

    await expect(compactor.compact(collectionName)).rejects.toThrow(LockActiveError)
  })

  test('expired lock is taken over', async () => {
    const lockKey = `${collectionName}.lock`
    const otherSession = 'other-session'
    const expiredTime = Date.now() - 1000 // Already expired
    await driver.put(lockKey, JSON.stringify({ sessionId: otherSession, expiresAt: expiredTime }))

    await expect(compactor.compact(collectionName)).resolves.not.toThrow()
  })

  test('parallel processing with configured parallelism', async () => {
    const parallelCompactor = new CollectionCompactor(driver, { parallelism: 2 })

    // Create multiple mutations
    for (let i = 0; i < 5; i++) {
      await driver.put(
        `${collectionName}.mutation.${i}`,
        JSON.stringify([[`id-${i}`, { val: i }]])
      )
    }

    const result = await parallelCompactor.compact(collectionName)
    expect(result.mutationsProcessed).toBeGreaterThan(0)

    // Verify all mutations were processed (files deleted)
    const remaining = await driver.list(`${collectionName}.mutation.`)
    expect(remaining.keys.length).toBe(0)
  })

  test('chunked deletes with configured chunk size', async () => {
    const chunkedCompactor = new CollectionCompactor(driver, { deleteChunkSize: 2 })

    // Create mutations
    for (let i = 0; i < 5; i++) {
      await driver.put(
        `${collectionName}.mutation.${i}`,
        JSON.stringify([[`id-${i}`, { val: i }]])
      )
    }

    await chunkedCompactor.compact(collectionName)

    const list = await driver.list(`${collectionName}.mutation.`)
    expect(list.keys.length).toBe(0)
  })
})
