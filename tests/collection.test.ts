import { Db, Collection } from '../lib/db'
import { FileSystemDriver } from '../lib/drivers/fs'
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

    await users.put({ id: 'u1', data: { id: 'u1', name: 'Alice' } })
    await posts.put({ id: 'p1', data: { id: 'p1', title: 'Hello' } })

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
    await col.put({ id: '1', data: { id: '1', val: 1 } })

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
    await col.put({ id: '1', data: { id: '1' } })
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
    await collection.put({ id: '1', data: { id: '1', name: 'test' } })

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
    await collection.put({ id: '1', data: { id: '1', name: 'test' } })

    const item = await collection.get('1')
    expect(item).toEqual({ id: '1', name: 'test' })

    const missing = await collection.get('nonexistent')
    expect(missing).toBeUndefined()
  })

  test('getMany returns multiple records', async () => {
    await collection.put(
      { id: '1', data: { id: '1', name: 'one' } },
      { id: '2', data: { id: '2', name: 'two' } },
      { id: '3', data: { id: '3', name: 'three' } }
    )

    const items = await collection.getMany(['1', '3', 'nonexistent'])
    expect(items.size).toBe(2)
    expect(items.get('1')).toEqual({ id: '1', name: 'one' })
    expect(items.get('3')).toEqual({ id: '3', name: 'three' })
  })

  test('find with where clause', async () => {
    await collection.put(
      { id: '1', data: { id: '1', name: 'Alice', age: 30 } },
      { id: '2', data: { id: '2', name: 'Bob', age: 25 } },
      { id: '3', data: { id: '3', name: 'Alice', age: 35 } }
    )

    const alices = await collection.find({ where: { name: 'Alice' } })
    expect(alices.length).toBe(2)

    const limited = await collection.find({ where: { name: 'Alice' }, limit: 1 })
    expect(limited.length).toBe(1)
  })

  test('find with function predicate', async () => {
    await collection.put(
      { id: '1', data: { id: '1', age: 30 } },
      { id: '2', data: { id: '2', age: 25 } },
      { id: '3', data: { id: '3', age: 35 } }
    )

    const over30 = await collection.find({ where: (item: any) => item.age >= 30 })
    expect(over30.length).toBe(2)
  })

  test('count returns number of records', async () => {
    await collection.put(
      { id: '1', data: { id: '1' } },
      { id: '2', data: { id: '2' } }
    )

    expect(await collection.count()).toBe(2)

    await collection.put({ id: '1', data: null })
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
    await collection.put(
      { id: '1', data: { id: '1', name: 'one' } },
      { id: '2', data: { id: '2', name: 'two' } }
    )

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
    await collection.put({ id: '1', data: null })

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
    const col = db.collection<{ id: string; expiresAt: number }>('ttl-test')
    col.defineTTL('expiresAt')

    const past = Date.now() - 1000
    const future = Date.now() + 100000

    await col.put(
      { id: '1', data: { id: '1', expiresAt: past } },
      { id: '2', data: { id: '2', expiresAt: future } }
    )

    const item1 = await col.get('1')
    const item2 = await col.get('2')

    expect(item1).toBeUndefined()
    expect(item2).toBeDefined()
  })

  test('deleteExpired removes expired records', async () => {
    const col = db.collection<{ id: string; expiresAt: number }>('ttl-delete-test')
    col.defineTTL('expiresAt')

    const past = Date.now() - 1000

    await col.put(
      { id: '1', data: { id: '1', expiresAt: past } },
      { id: '2', data: { id: '2', expiresAt: past } }
    )

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
      col.put({ id: '1', data: { id: '1', data: largeData } })
    ).rejects.toThrow('Payload size')
  })
})
