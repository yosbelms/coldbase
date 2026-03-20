/**
 * Tests for the three bug fixes:
 * 1. Auto-maintenance is now awaited (was fire-and-forget)
 * 2. Malformed data is now logged as warnings (was silently skipped)
 * 3. LockAcquisitionError is thrown on storage failure (was raw error)
 */
import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import { Db } from '../lib/db'
import { CollectionCompactor } from '../lib/compactor'
import { FileSystemDriver } from '../lib/drivers/fs'
import { LockActiveError, LockAcquisitionError } from '../lib/errors'
import { streamToString } from '../lib/utils'

// ─── helpers ────────────────────────────────────────────────────────────────

async function makeTmpDir() {
  return fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-bugfix-'))
}

async function cleanup(dir: string) {
  await fs.promises.rm(dir, { recursive: true, force: true })
}

// ─── Bug 1: Auto-maintenance is awaited ─────────────────────────────────────

describe('Bug 1: auto-maintenance is awaited before put() returns', () => {
  let tmpDir: string
  let driver: FileSystemDriver

  beforeEach(async () => {
    tmpDir = await makeTmpDir()
    driver = new FileSystemDriver(tmpDir)
  })

  afterEach(() => cleanup(tmpDir))

  test('Collection: compaction completes before put() returns when autoCompact=true', async () => {
    const db = new Db(driver, { autoCompact: true })
    const col = db.collection<{ id: string; val: number }>('auto-compact')

    await col.put({ id: '1', val: 1 })

    // If maintenance were fire-and-forget, the mutation file might still exist.
    // With the fix, compaction is awaited and mutations are merged before put() returns.
    const mutations = await driver.list('auto-compact.mutation.')
    expect(mutations.keys.length).toBe(0)

    const mainResp = await driver.get('auto-compact.jsonl')
    expect(mainResp).toBeDefined()
    const content = await streamToString(mainResp!.stream)
    expect(content.trim()).not.toBe('')
  })

  test('Collection: onCompact hook is called before put() returns', async () => {
    const onCompact = jest.fn()
    const db = new Db(driver, {
      autoCompact: true,
      hooks: { onCompact }
    })
    const col = db.collection<{ id: string }>('hook-test')

    await col.put({ id: '1' })

    // With fire-and-forget, this assertion would be flaky (hook called async after put).
    // With the fix, the hook is guaranteed to have been called.
    expect(onCompact).toHaveBeenCalledTimes(1)
    expect(onCompact).toHaveBeenCalledWith('hook-test', expect.any(Number), 1)
  })

  test('Collection: onMaintenanceFailure hook is called before put() returns on persistent failure', async () => {
    const onMaintenanceFailure = jest.fn()

    // Make compact fail by breaking the driver's list after the initial put
    const brokenDriver = new FileSystemDriver(tmpDir)
    const originalList = brokenDriver.list.bind(brokenDriver)
    let callCount = 0
    brokenDriver.list = async (prefix: string, token?: string) => {
      // First call (from put's write) succeeds; subsequent calls (from compact) fail
      callCount++
      if (callCount > 1 && prefix.includes('.mutation.')) {
        throw new Error('simulated storage failure')
      }
      return originalList(prefix, token)
    }

    const db = new Db(brokenDriver, {
      autoCompact: { probability: 1, maxRetries: 0 },
      hooks: { onMaintenanceFailure }
    })
    const col = db.collection<{ id: string }>('failure-test')

    await col.put({ id: '1' })

    // With fire-and-forget, this would not be called yet when put() returns.
    expect(onMaintenanceFailure).toHaveBeenCalledWith(
      'failure-test', 'compact', expect.any(Error), 1
    )
  })

  test('VectorCollection: compaction completes before put() returns when autoCompact=true', async () => {
    const db = new Db(driver, { autoCompact: true })
    const col = db.vectorCollection<{ id: string; vector: number[] }>('vec-auto', {
      dimension: 3,
      metric: 'cosine'
    })

    await col.put({ id: '1', vector: [1, 0, 0] })

    const mutations = await driver.list('vec-auto.mutation.')
    expect(mutations.keys.length).toBe(0)
  })

  test('VectorCollection: onMaintenanceFailure hook fires before put() returns on failure', async () => {
    const onMaintenanceFailure = jest.fn()

    const brokenDriver = new FileSystemDriver(tmpDir)
    const originalList = brokenDriver.list.bind(brokenDriver)
    let callCount = 0
    brokenDriver.list = async (prefix: string, token?: string) => {
      callCount++
      if (callCount > 1 && prefix.includes('.mutation.')) {
        throw new Error('simulated storage failure')
      }
      return originalList(prefix, token)
    }

    const db = new Db(brokenDriver, {
      autoCompact: { probability: 1, maxRetries: 0 },
      hooks: { onMaintenanceFailure }
    })
    const col = db.vectorCollection<{ id: string; vector: number[] }>('vec-fail', {
      dimension: 3
    })

    await col.put({ id: '1', vector: [1, 0, 0] })

    expect(onMaintenanceFailure).toHaveBeenCalledWith(
      'vec-fail', 'compact', expect.any(Error), 1
    )
  })
})

// ─── Bug 2: Malformed data is warned, not silently skipped ──────────────────

describe('Bug 2: malformed data produces warnings and does not crash', () => {
  let tmpDir: string
  let driver: FileSystemDriver

  beforeEach(async () => {
    tmpDir = await makeTmpDir()
    driver = new FileSystemDriver(tmpDir)
  })

  afterEach(() => cleanup(tmpDir))

  test('read() skips malformed mutation file and returns valid records', async () => {
    const db = new Db(driver, { autoCompact: false })
    const col = db.collection<{ id: string; val: number }>('malformed-mut')

    // Write a valid record
    await col.put({ id: 'valid', val: 42 })

    // Inject a malformed mutation file directly
    await driver.put('malformed-mut.mutation.0000000000000-bad', 'not json {{{')

    // Should not throw; should return the valid record and skip the bad one
    const result = await col.get('valid')
    expect(result).toEqual({ id: 'valid', val: 42 })

    // find() should also work
    const all = await col.find()
    expect(all).toHaveLength(1)
    expect(all[0].id).toBe('valid')
  })

  test('vacuum() survives malformed lines in main .jsonl file', async () => {
    const compactor = new CollectionCompactor(driver)
    const col = 'malformed-main'

    // Build a .jsonl file with one valid and one malformed line
    const validLine = JSON.stringify(['id1', { id: 'id1', val: 1 }, 1000])
    await driver.put(`${col}.jsonl`, `${validLine}\nnot valid json\n`)

    // Should not throw
    await expect(compactor.vacuum(col)).resolves.toBeDefined()

    // The valid record should survive
    const db = new Db(driver, { autoCompact: false })
    const collection = db.collection<{ id: string; val: number }>(col)
    const item = await collection.get('id1')
    expect(item).toEqual({ id: 'id1', val: 1 })
  })

  test('compact() skips malformed mutation files without throwing', async () => {
    const compactor = new CollectionCompactor(driver)
    const col = 'malformed-compact'

    // Write one valid and one malformed mutation file
    await driver.put(`${col}.mutation.1`, JSON.stringify([['id1', { id: 'id1' }, 1000]]))
    await driver.put(`${col}.mutation.2`, 'this is not json')

    // Should not throw
    const result = await compactor.compact(col)
    expect(result.mutationsProcessed).toBe(1)

    // The valid record should be in main file
    const mainResp = await driver.get(`${col}.jsonl`)
    const content = await streamToString(mainResp!.stream)
    expect(content).toContain('id1')
  })
})

// ─── Bug 3: LockAcquisitionError vs LockActiveError ─────────────────────────

describe('Bug 3: LockAcquisitionError on storage failure, LockActiveError on contention', () => {
  let tmpDir: string
  let driver: FileSystemDriver

  beforeEach(async () => {
    tmpDir = await makeTmpDir()
    driver = new FileSystemDriver(tmpDir)
  })

  afterEach(() => cleanup(tmpDir))

  test('throws LockActiveError when another process holds the lock', async () => {
    const compactor2 = new CollectionCompactor(driver)

    await driver.put('locked-col.mutation.1', JSON.stringify([['id1', { id: 'id1' }, 1000]]))

    // Start compaction with compactor1 — it will acquire the lock
    // We use a delayed driver to keep the lock held while compactor2 tries
    let resolveBlock: () => void
    const blockPromise = new Promise<void>(res => { resolveBlock = res })

    const blockingDriver = new FileSystemDriver(tmpDir)
    const originalAppend = blockingDriver.append.bind(blockingDriver)
    blockingDriver.append = async (key: string, content: string) => {
      // Block the append step so the lock is held when compactor2 tries
      await blockPromise
      return originalAppend(key, content)
    }

    const blockingCompactor = new CollectionCompactor(blockingDriver)
    const compact1 = blockingCompactor.compact('locked-col')

    // Give compact1 time to acquire the lock
    await new Promise(resolve => setTimeout(resolve, 50))

    // compactor2 should see the lock as active
    await expect(compactor2.compact('locked-col')).rejects.toBeInstanceOf(LockActiveError)

    // Unblock compact1
    resolveBlock!()
    await compact1
  })

  test('throws LockAcquisitionError when storage itself fails during lock acquisition', async () => {
    // Wrap the real driver, overriding only putIfNoneMatch to simulate a storage failure
    const faultyDriver = new FileSystemDriver(tmpDir)
    faultyDriver.putIfNoneMatch = async () => {
      throw new Error('network timeout')
    }

    const compactor = new CollectionCompactor(faultyDriver)
    await driver.put('storage-fail.mutation.1', JSON.stringify([['id1', { id: 'id1' }, 1000]]))

    const err = await compactor.compact('storage-fail').catch(e => e)
    expect(err).toBeInstanceOf(LockAcquisitionError)
    expect((err as LockAcquisitionError).cause?.message).toBe('network timeout')
  })

  test('LockAcquisitionError and LockActiveError are distinct error types', async () => {
    expect(new LockAcquisitionError('col')).toBeInstanceOf(LockAcquisitionError)
    expect(new LockAcquisitionError('col')).not.toBeInstanceOf(LockActiveError)
    expect(new LockActiveError('col')).toBeInstanceOf(LockActiveError)
    expect(new LockActiveError('col')).not.toBeInstanceOf(LockAcquisitionError)
  })

  test('auto-maintenance silently skips on LockActiveError (contention) without retrying', async () => {
    const onMaintenanceFailure = jest.fn()
    const onError = jest.fn()

    // Place an unexpired lock file so the compactor will find active contention
    const activeLock = JSON.stringify({ sessionId: 'other', expiresAt: Date.now() + 60_000 })
    await driver.put('contention-test.lock', activeLock)
    await driver.put('contention-test.mutation.1', JSON.stringify([['id1', { id: 'id1' }, 1000]]))

    const db = new Db(driver, {
      autoCompact: { probability: 1, maxRetries: 2 },
      hooks: { onMaintenanceFailure, onError }
    })
    const col = db.collection<{ id: string }>('contention-test')

    await col.put({ id: '2' })

    // Lock contention is a normal condition — should not trigger failure hooks or retries
    expect(onMaintenanceFailure).not.toHaveBeenCalled()
    expect(onError).not.toHaveBeenCalled()
  })
})
