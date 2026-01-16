import { FileSystemDriver } from '../lib/drivers/fs'
import { PreconditionFailedError } from '../lib/errors'
import * as fs from 'fs'
import * as path from 'path'
import * as os from 'os'
import { Readable } from 'stream'

const streamToString = (stream: Readable): Promise<string> => {
  const chunks: any[] = []
  return new Promise((resolve, reject) => {
    stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
    stream.on('error', (err) => reject(err))
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
  })
}

describe('FileSystemDriver', () => {
  let tmpDir: string
  let driver: FileSystemDriver

  beforeEach(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'coldbase-test-'))
    driver = new FileSystemDriver(tmpDir)
  })

  afterEach(async () => {
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  test('put and get', async () => {
    const key = 'test.txt'
    const content = 'hello world'
    await driver.put(key, content)

    const result = await driver.get(key)
    expect(result).toBeDefined()
    expect(result!.etag).toBeDefined()
    
    const readContent = await streamToString(result!.stream)
    expect(readContent).toBe(content)
  })

  test('get non-existent key', async () => {
    const result = await driver.get('non-existent.txt')
    expect(result).toBeUndefined()
  })

  test('putIfNoneMatch success', async () => {
    const key = 'new.txt'
    const content = 'new content'
    const etag = await driver.putIfNoneMatch(key, content)
    
    expect(etag).toBeDefined()
    
    const result = await driver.get(key)
    expect(result).toBeDefined()
    const readContent = await streamToString(result!.stream)
    expect(readContent).toBe(content)
  })

  test('putIfNoneMatch failure (already exists)', async () => {
    const key = 'existing.txt'
    await driver.put(key, 'initial')

    await expect(driver.putIfNoneMatch(key, 'overwrite'))
      .rejects.toThrow(PreconditionFailedError)

    const result = await driver.get(key)
    const readContent = await streamToString(result!.stream)
    expect(readContent).toBe('initial')
  })

  test('putIfMatch success', async () => {
    const key = 'update.txt'
    await driver.put(key, 'v1')
    const result1 = await driver.get(key)
    const etag1 = result1!.etag

    // Wait a bit to ensure mtime changes if fs resolution is low (though mtimeMs usually handles it)
    // But mostly rely on the fact that we are writing.
    
    const newEtag = await driver.putIfMatch(key, 'v2', etag1)
    expect(newEtag).not.toBe(etag1)

    const result2 = await driver.get(key)
    const readContent = await streamToString(result2!.stream)
    expect(readContent).toBe('v2')
  })

  test('putIfMatch failure (etag mismatch)', async () => {
    const key = 'update-fail.txt'
    await driver.put(key, 'v1')

    await expect(driver.putIfMatch(key, 'v2', 'wrong-etag'))
      .rejects.toThrow(PreconditionFailedError)

    const result = await driver.get(key)
    const readContent = await streamToString(result!.stream)
    expect(readContent).toBe('v1')
  })

  test('putIfMatch failure (key does not exist)', async () => {
    await expect(driver.putIfMatch('missing.txt', 'v1', 'some-etag'))
      .rejects.toThrow(PreconditionFailedError)
  })

  test('list keys', async () => {
    await driver.put('a.txt', 'a')
    await driver.put('b.txt', 'b')
    await driver.put('sub/c.txt', 'c')

    const result = await driver.list('')
    expect(result.keys).toEqual(['a.txt', 'b.txt', 'sub/c.txt'])
  })

  test('list with prefix', async () => {
    await driver.put('a.txt', 'a')
    await driver.put('sub/1.txt', '1')
    await driver.put('sub/2.txt', '2')

    const result = await driver.list('sub/')
    expect(result.keys).toEqual(['sub/1.txt', 'sub/2.txt'])
  })

  test('list pagination', async () => {
    // Generate enough keys to test pagination if needed, 
    // but the driver has a hardcoded limit of 1000. 
    // We'll just mock the logic behavior by ensuring list works for small sets.
    // If we want to test pagination logic specifically, we might need to mock readdir or create 1001 files.
    // For now let's verify continuation token is handled if we pass it, 
    // although with < 1000 files it's basically just an offset.
    
    await driver.put('1', '1')
    await driver.put('2', '2')
    await driver.put('3', '3')

    // Using continuation token '1' (index 1) should give us keys starting from index 1
    const result = await driver.list('', '1') 
    expect(result.keys).toEqual(['2', '3'])
  })

  test('delete keys', async () => {
    await driver.put('del1.txt', '1')
    await driver.put('del2.txt', '2')

    await driver.delete(['del1.txt'])

    const res1 = await driver.get('del1.txt')
    const res2 = await driver.get('del2.txt')

    expect(res1).toBeUndefined()
    expect(res2).toBeDefined()
  })

  test('size', async () => {
    const content = '12345'
    await driver.put('size.txt', content)
    
    const size = await driver.size('size.txt')
    expect(size).toBe(content.length)
  })
  
  test('size non-existent', async () => {
    const size = await driver.size('missing-size.txt')
    expect(size).toBeUndefined()
  })

  test('append to existing', async () => {
    await driver.put('log.txt', 'line1')
    await driver.append('log.txt', 'line2')
    
    const result = await driver.get('log.txt')
    const content = await streamToString(result!.stream)
    
    expect(content).toBe('line1\nline2')
  })

  test('append to new', async () => {
    await driver.append('new-log.txt', 'line1')
    
    const result = await driver.get('new-log.txt')
    const content = await streamToString(result!.stream)
    
    expect(content).toBe('line1')
  })

  test('nested directories creation', async () => {
      await driver.put('deep/nested/dir/file.txt', 'content')
      const result = await driver.get('deep/nested/dir/file.txt')
      expect(result).toBeDefined()
  })
})
