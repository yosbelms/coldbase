import { CreateBucketCommand, S3Client } from '@aws-sdk/client-s3'
import * as fs from 'fs'
import * as os from 'os'
import * as path from 'path'
import S3rver from 's3rver'
import { Readable } from 'stream'
import { S3Driver } from '../lib/drivers/s3'
import { PreconditionFailedError } from '../lib/errors'

const streamToString = (stream: Readable): Promise<string> => {
  const chunks: Buffer[] = []
  return new Promise((resolve, reject) => {
    stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
    stream.on('error', (err) => reject(err))
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
  })
}

const TEST_PORT = 4569
const TEST_BUCKET = 'test-bucket'
const TEST_ENDPOINT = `http://localhost:${TEST_PORT}`
const TEST_CREDENTIALS = { accessKeyId: 'S3RVER', secretAccessKey: 'S3RVER' }

describe('S3Driver', () => {
  let s3rverInstance: S3rver
  let tmpDir: string
  let driver: S3Driver

  beforeAll(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 's3-test-'))
    s3rverInstance = new S3rver({
      port: TEST_PORT,
      address: 'localhost',
      silent: true,
      directory: tmpDir,
    })
    await s3rverInstance.run()

    // Create test bucket
    const client = new S3Client({
      region: 'us-east-1',
      endpoint: TEST_ENDPOINT,
      forcePathStyle: true,
      credentials: TEST_CREDENTIALS
    })
    await client.send(new CreateBucketCommand({ Bucket: TEST_BUCKET }))
  })

  afterAll(async () => {
    await s3rverInstance.close()
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  beforeEach(() => {
    driver = new S3Driver(TEST_BUCKET, 'us-east-1', {
      endpoint: TEST_ENDPOINT,
      credentials: TEST_CREDENTIALS,
      forcePathStyle: true
    })
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
    const key = 'new-unique-' + Date.now() + '.txt'
    const content = 'new content'
    const etag = await driver.putIfNoneMatch(key, content)

    expect(etag).toBeDefined()

    const result = await driver.get(key)
    expect(result).toBeDefined()
    const readContent = await streamToString(result!.stream)
    expect(readContent).toBe(content)
  })

  // s3rver doesn't support conditional writes (IfNoneMatch header)
  test.skip('putIfNoneMatch failure (already exists)', async () => {
    const key = 'existing-' + Date.now() + '.txt'
    await driver.put(key, 'initial')

    await expect(driver.putIfNoneMatch(key, 'overwrite'))
      .rejects.toThrow(PreconditionFailedError)

    const result = await driver.get(key)
    const readContent = await streamToString(result!.stream)
    expect(readContent).toBe('initial')
  })

  test('putIfMatch success', async () => {
    const key = 'update-' + Date.now() + '.txt'
    await driver.put(key, 'v1')
    const result1 = await driver.get(key)
    const etag1 = result1!.etag

    const newEtag = await driver.putIfMatch(key, 'v2', etag1)
    expect(newEtag).toBeDefined()
    expect(newEtag).not.toBe(etag1)

    const result2 = await driver.get(key)
    const readContent = await streamToString(result2!.stream)
    expect(readContent).toBe('v2')
  })

  // s3rver doesn't support conditional writes (IfMatch header)
  test.skip('putIfMatch failure (etag mismatch)', async () => {
    const key = 'update-fail-' + Date.now() + '.txt'
    await driver.put(key, 'v1')

    await expect(driver.putIfMatch(key, 'v2', '"wrong-etag"'))
      .rejects.toThrow(PreconditionFailedError)

    const result = await driver.get(key)
    const readContent = await streamToString(result!.stream)
    expect(readContent).toBe('v1')
  })

  test('list keys', async () => {
    const prefix = 'list-test-' + Date.now() + '/'
    await driver.put(prefix + 'a.txt', 'a')
    await driver.put(prefix + 'b.txt', 'b')
    await driver.put(prefix + 'sub/c.txt', 'c')

    const result = await driver.list(prefix)
    expect(result.keys.sort()).toEqual([
      prefix + 'a.txt',
      prefix + 'b.txt',
      prefix + 'sub/c.txt'
    ].sort())
  })

  test('list with prefix', async () => {
    const prefix = 'prefix-test-' + Date.now() + '/'
    await driver.put(prefix + 'a.txt', 'a')
    await driver.put(prefix + 'sub/1.txt', '1')
    await driver.put(prefix + 'sub/2.txt', '2')

    const result = await driver.list(prefix + 'sub/')
    expect(result.keys.sort()).toEqual([
      prefix + 'sub/1.txt',
      prefix + 'sub/2.txt'
    ].sort())
  })

  test('delete keys', async () => {
    const prefix = 'delete-test-' + Date.now() + '/'
    await driver.put(prefix + 'del1.txt', '1')
    await driver.put(prefix + 'del2.txt', '2')

    await driver.delete([prefix + 'del1.txt'])

    const res1 = await driver.get(prefix + 'del1.txt')
    const res2 = await driver.get(prefix + 'del2.txt')

    expect(res1).toBeUndefined()
    expect(res2).toBeDefined()
  })

  test('size', async () => {
    const key = 'size-' + Date.now() + '.txt'
    const content = '12345'
    await driver.put(key, content)

    const size = await driver.size(key)
    expect(size).toBe(content.length)
  })

  test('size non-existent', async () => {
    const size = await driver.size('missing-size-' + Date.now() + '.txt')
    expect(size).toBeUndefined()
  })

  test('append to existing', async () => {
    const key = 'log-' + Date.now() + '.txt'
    await driver.put(key, 'line1')
    await driver.append(key, 'line2')

    const result = await driver.get(key)
    const content = await streamToString(result!.stream)

    expect(content).toBe('line1\nline2')
  })

  test('append to new', async () => {
    const key = 'new-log-' + Date.now() + '.txt'
    await driver.append(key, 'line1')

    const result = await driver.get(key)
    const content = await streamToString(result!.stream)

    expect(content).toBe('line1')
  })

  test('cache hits for repeated get calls', async () => {
    const key = 'cache-test-' + Date.now() + '.txt'
    const content = 'cached content'
    await driver.put(key, content)

    // First get - populates cache
    const result1 = await driver.get(key)
    expect(result1).toBeDefined()

    // Second get - should work (cache is used internally for size)
    const result2 = await driver.get(key)
    expect(result2).toBeDefined()
    const readContent = await streamToString(result2!.stream)
    expect(readContent).toBe(content)
  })

  test('cache hits for size after put', async () => {
    const key = 'cache-size-' + Date.now() + '.txt'
    const content = 'content for size'

    // Put updates cache
    await driver.put(key, content)

    // Size should use cache
    const size = await driver.size(key)
    expect(size).toBe(Buffer.byteLength(content, 'utf-8'))
  })

  test('clearCache invalidates cache', async () => {
    const key = 'clear-cache-' + Date.now() + '.txt'
    const content = 'original'
    await driver.put(key, content)

    // Size uses cached value
    const size1 = await driver.size(key)
    expect(size1).toBe(Buffer.byteLength(content, 'utf-8'))

    // Clear cache
    driver.clearCache()

    // Size should still work (fetches from S3)
    const size2 = await driver.size(key)
    expect(size2).toBe(Buffer.byteLength(content, 'utf-8'))
  })

  test('nested directories', async () => {
    const key = 'deep/nested/dir/file-' + Date.now() + '.txt'
    await driver.put(key, 'content')
    const result = await driver.get(key)
    expect(result).toBeDefined()
    const content = await streamToString(result!.stream)
    expect(content).toBe('content')
  })
})
