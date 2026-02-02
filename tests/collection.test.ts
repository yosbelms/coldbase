import { Db, Collection } from '../lib/db'
import { FileSystemDriver } from '../lib/drivers/fs'
import { TransactionError } from '../lib/errors'
import { streamToString } from '../lib/utils'
import * as fs from 'fs'
import * as path from 'path'
import * as os from 'os'

describe('Db', () => {
  let tmpDir: string
  let driver: FileSystemDriver

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-test-'))
    driver = new FileSystemDriver(tmpDir)
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('manages multiple collections in separate files', async () => {
    const db = new Db(driver, { autoCompact: false })

    const users = db.collection<{ id: string; name: string }>('users')
    const posts = db.collection<{ id: string; title: string }>('posts')

    await users.put({ id: 'u1', name: 'Alice' })
    await posts.put({ id: 'p1', title: 'Hello' })

    const userMutations = await driver.list('users.mutation.')
    const postMutations = await driver.list('posts.mutation.')
    expect(userMutations.keys.length).toBe(1)
    expect(postMutations.keys.length).toBe(1)
  })

  test('returns same collection instance for same name', () => {
    const db = new Db(driver)
    const col1 = db.collection('test')
    const col2 = db.collection('test')
    expect(col1).toBe(col2)
  })

  test('compact and vacuum delegate to compactor', async () => {
    const db = new Db(driver, { autoCompact: false })
    const col = db.collection<{ id: string; val: number }>('test')
    await col.put({ id: '1', val: 1 })

    await db.compact('test')
    const mutations = await driver.list('test.mutation.')
    expect(mutations.keys.length).toBe(0)
  })

  test('hooks are called', async () => {
    const onWrite = jest.fn()
    const onCompact = jest.fn()

    const db = new Db(driver, {
      autoCompact: false,
      hooks: { onWrite, onCompact }
    })

    const col = db.collection<{ id: string }>('hooks-test')
    await col.put({ id: '1' })
    expect(onWrite).toHaveBeenCalledWith('hooks-test', 1)

    await db.compact('hooks-test')
    expect(onCompact).toHaveBeenCalled()
  })
})

describe('Collection', () => {
  let tmpDir: string
  let driver: FileSystemDriver
  let db: Db
  let collection: Collection<any>
  const collectionName = 'test-collection'

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-collection-test-'))
    driver = new FileSystemDriver(tmpDir)
    db = new Db(driver, { autoCompact: false })
    collection = db.collection(collectionName)
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('put writes mutation file', async () => {
    await collection.put({ id: '1', name: 'test' })

    const list = await driver.list(`${collectionName}.mutation.`)
    expect(list.keys.length).toBe(1)

    const resp = await driver.get(list.keys[0])
    const content = await streamToString(resp!.stream)
    const parsed = JSON.parse(content)

    expect(parsed).toHaveLength(1)
    expect(parsed[0]).toHaveLength(3) // [id, data, timestamp]
    expect(parsed[0][0]).toBe('1')
    expect(parsed[0][1]).toEqual({ id: '1', name: 'test' })
    expect(typeof parsed[0][2]).toBe('number')
  })

  test('get returns single record', async () => {
    await collection.put({ id: '1', name: 'test' })

    const item = await collection.get('1')
    expect(item).toEqual({ id: '1', name: 'test' })

    const missing = await collection.get('nonexistent')
    expect(missing).toBeUndefined()
  })

  test('getMany returns multiple records', async () => {
    await collection.batch(tx => {
      tx.put({ id: '1', name: 'one' })
      tx.put({ id: '2', name: 'two' })
      tx.put({ id: '3', name: 'three' })
    })

    const items = await collection.getMany(['1', '3', 'nonexistent'])
    expect(items.size).toBe(2)
    expect(items.get('1')).toEqual({ id: '1', name: 'one' })
    expect(items.get('3')).toEqual({ id: '3', name: 'three' })
  })

  test('find with where clause', async () => {
    await collection.batch(tx => {
      tx.put({ id: '1', name: 'Alice', age: 30 })
      tx.put({ id: '2', name: 'Bob', age: 25 })
      tx.put({ id: '3', name: 'Alice', age: 35 })
    })

    const alices = await collection.find({ where: { name: 'Alice' } })
    expect(alices.length).toBe(2)

    const limited = await collection.find({ where: { name: 'Alice' }, limit: 1 })
    expect(limited.length).toBe(1)
  })

  test('find with function predicate', async () => {
    await collection.batch(tx => {
      tx.put({ id: '1', age: 30 })
      tx.put({ id: '2', age: 25 })
      tx.put({ id: '3', age: 35 })
    })

    const over30 = await collection.find({ where: (item: any) => item.age >= 30 })
    expect(over30.length).toBe(2)
  })

  test('count returns number of records', async () => {
    await collection.batch(tx => {
      tx.put({ id: '1' })
      tx.put({ id: '2' })
    })

    expect(await collection.count()).toBe(2)

    await collection.delete('1')
    expect(await collection.count()).toBe(1)
  })

  test('read yields from snapshot', async () => {
    const snapshotData = [
      ['1', { id: '1', name: 'one' }],
      ['2', { id: '2', name: 'two' }]
    ]
    const ndjson = snapshotData.map(d => JSON.stringify(d)).join('\n')
    await driver.put(`${collectionName}.jsonl`, ndjson)

    const items = []
    for await (const item of collection.read()) {
      items.push(item)
    }

    expect(items).toEqual([
      { id: '1', data: { id: '1', name: 'one' } },
      { id: '2', data: { id: '2', name: 'two' } }
    ])
  })

  test('read yields from pending mutations', async () => {
    await collection.batch(tx => {
      tx.put({ id: '1', name: 'one' })
      tx.put({ id: '2', name: 'two' })
    })

    const items = []
    for await (const item of collection.read()) {
      items.push(item)
    }

    expect(items.length).toBe(2)
    expect(items).toContainEqual(expect.objectContaining({ id: '1', data: { id: '1', name: 'one' } }))
    expect(items).toContainEqual(expect.objectContaining({ id: '2', data: { id: '2', name: 'two' } }))
    expect(typeof items[0].timestamp).toBe('number')
  })

  test('read yields deletions (null data)', async () => {
    await collection.delete('1')

    const items = []
    for await (const item of collection.read()) {
      items.push(item)
    }

    expect(items).toHaveLength(1)
    expect(items[0]).toEqual(expect.objectContaining({ id: '1', data: null }))
  })
})

describe('TTL', () => {
  let tmpDir: string
  let driver: FileSystemDriver
  let db: Db

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-ttl-test-'))
    driver = new FileSystemDriver(tmpDir)
    db = new Db(driver, { autoCompact: false })
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('expired records are not returned', async () => {
    const col = db.collection<{ id: string; expiresAt: number }>('ttl-test', { ttlField: 'expiresAt' })

    const past = Date.now() - 1000
    const future = Date.now() + 100000

    await col.batch(tx => {
      tx.put({ id: '1', expiresAt: past })
      tx.put({ id: '2', expiresAt: future })
    })

    const item1 = await col.get('1')
    const item2 = await col.get('2')

    expect(item1).toBeUndefined()
    expect(item2).toBeDefined()
  })

  test('deleteExpired removes expired records', async () => {
    const col = db.collection<{ id: string; expiresAt: number }>('ttl-delete-test', { ttlField: 'expiresAt' })

    const past = Date.now() - 1000

    await col.batch(tx => {
      tx.put({ id: '1', expiresAt: past })
      tx.put({ id: '2', expiresAt: past })
    })

    const deleted = await col.deleteExpired()
    expect(deleted).toBe(2)
  })
})

describe('Size limits', () => {
  let tmpDir: string
  let driver: FileSystemDriver
  let db: Db

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-size-test-'))
    driver = new FileSystemDriver(tmpDir)
    db = new Db(driver, { autoCompact: false, maxMutationSize: 100 })
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('rejects mutations exceeding size limit', async () => {
    const col = db.collection<{ id: string; data: string }>('size-test')
    const largeData = 'x'.repeat(200)

    await expect(
      col.put({ id: '1', data: largeData })
    ).rejects.toThrow('Payload size')
  })
})

describe('Transaction', () => {
  let tmpDir: string
  let driver: FileSystemDriver
  let db: Db

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-tx-test-'))
    driver = new FileSystemDriver(tmpDir)
    db = new Db(driver, { autoCompact: false })
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('commits writes across multiple collections on success', async () => {
    await db.transaction(async (tx) => {
      const users = tx.collection<{ id: string; name: string }>('users')
      const logs = tx.collection<{ id: string; action: string }>('logs')

      await users.put({ id: '1', name: 'Alice' })
      await logs.put({ id: 'log-1', action: 'user-created' })
    })

    const users = db.collection<{ id: string; name: string }>('users')
    const logs = db.collection<{ id: string; action: string }>('logs')

    expect(await users.get('1')).toEqual({ id: '1', name: 'Alice' })
    expect(await logs.get('log-1')).toEqual({ id: 'log-1', action: 'user-created' })
  })

  test('compensates all writes on failure', async () => {
    await expect(
      db.transaction(async (tx) => {
        const users = tx.collection<{ id: string; name: string }>('users')
        const logs = tx.collection<{ id: string; action: string }>('logs')

        await users.put({ id: '1', name: 'Alice' })
        await logs.put({ id: 'log-1', action: 'user-created' })

        throw new Error('Simulated failure')
      })
    ).rejects.toThrow(TransactionError)

    const users = db.collection<{ id: string; name: string }>('users')
    const logs = db.collection<{ id: string; action: string }>('logs')

    expect(await users.get('1')).toBeUndefined()
    expect(await logs.get('log-1')).toBeUndefined()
  })

  test('compensation restores previous values', async () => {
    const users = db.collection<{ id: string; name: string }>('users')
    await users.put({ id: '1', name: 'Original' })

    await expect(
      db.transaction(async (tx) => {
        const txUsers = tx.collection<{ id: string; name: string }>('users')
        await txUsers.put({ id: '1', name: 'Updated' })
        throw new Error('Simulated failure')
      })
    ).rejects.toThrow(TransactionError)

    expect(await users.get('1')).toEqual({ id: '1', name: 'Original' })
  })

  test('TransactionError contains original error and empty compensation errors on clean rollback', async () => {
    try {
      await db.transaction(async (tx) => {
        const users = tx.collection<{ id: string; name: string }>('users')
        await users.put({ id: '1', name: 'Alice' })
        throw new Error('Original failure')
      })
      fail('Expected TransactionError')
    } catch (err) {
      expect(err).toBeInstanceOf(TransactionError)
      const txErr = err as TransactionError
      expect(txErr.originalError.message).toBe('Original failure')
      expect(txErr.compensationErrors).toHaveLength(0)
      expect(txErr.message).toBe('Transaction failed, all compensations succeeded')
    }
  })

  test('compensation errors are collected in TransactionError.compensationErrors', async () => {
    // Pre-populate so compensation (re-put) will be attempted
    const users = db.collection<{ id: string; name: string }>('users')
    await users.put({ id: '1', name: 'Original' })

    // Sabotage the driver to make compensation fail
    const originalPut = driver.put.bind(driver)
    let callCount = 0

    try {
      await db.transaction(async (tx) => {
        const txUsers = tx.collection<{ id: string; name: string }>('users')
        await txUsers.put({ id: '1', name: 'Updated' })

        // After the successful put, sabotage subsequent puts so compensation fails
        driver.put = async (...args: Parameters<typeof driver.put>) => {
          callCount++
          if (callCount > 0) {
            throw new Error('Storage failure')
          }
          return originalPut(...args)
        }

        throw new Error('Original failure')
      })
      fail('Expected TransactionError')
    } catch (err) {
      expect(err).toBeInstanceOf(TransactionError)
      const txErr = err as TransactionError
      expect(txErr.originalError.message).toBe('Original failure')
      expect(txErr.compensationErrors).toHaveLength(1)
      expect(txErr.compensationErrors[0].message).toBe('Storage failure')
      expect(txErr.message).toContain('1 compensation(s) also failed')
    } finally {
      driver.put = originalPut
    }
  })

  test('nested transaction commits promote compensations to parent', async () => {
    await db.transaction(async (tx) => {
      const users = tx.collection<{ id: string; name: string }>('users')
      await users.put({ id: '1', name: 'Alice' })

      await tx.transaction(async (inner) => {
        const logs = inner.collection<{ id: string; action: string }>('logs')
        await logs.put({ id: 'log-1', action: 'created' })
      })
    })

    const users = db.collection<{ id: string; name: string }>('users')
    const logs = db.collection<{ id: string; action: string }>('logs')
    expect(await users.get('1')).toEqual({ id: '1', name: 'Alice' })
    expect(await logs.get('log-1')).toEqual({ id: 'log-1', action: 'created' })
  })

  test('nested transaction failure only rolls back inner writes', async () => {
    await expect(
      db.transaction(async (tx) => {
        const users = tx.collection<{ id: string; name: string }>('users')
        await users.put({ id: '1', name: 'Alice' })

        try {
          await tx.transaction(async (inner) => {
            const logs = inner.collection<{ id: string; action: string }>('logs')
            await logs.put({ id: 'log-1', action: 'created' })
            throw new Error('Inner failure')
          })
        } catch {
          // Swallow inner failure, outer continues
        }
      })
    ).resolves.toBeUndefined()

    const users = db.collection<{ id: string; name: string }>('users')
    const logs = db.collection<{ id: string; action: string }>('logs')
    expect(await users.get('1')).toEqual({ id: '1', name: 'Alice' })
    expect(await logs.get('log-1')).toBeUndefined()
  })

  test('outer failure rolls back both outer and promoted inner compensations', async () => {
    await expect(
      db.transaction(async (tx) => {
        const users = tx.collection<{ id: string; name: string }>('users')
        await users.put({ id: '1', name: 'Alice' })

        await tx.transaction(async (inner) => {
          const logs = inner.collection<{ id: string; action: string }>('logs')
          await logs.put({ id: 'log-1', action: 'created' })
        })

        throw new Error('Outer failure after nested commit')
      })
    ).rejects.toThrow(TransactionError)

    const users = db.collection<{ id: string; name: string }>('users')
    const logs = db.collection<{ id: string; action: string }>('logs')
    expect(await users.get('1')).toBeUndefined()
    expect(await logs.get('log-1')).toBeUndefined()
  })
})
