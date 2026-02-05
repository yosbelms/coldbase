import { AzureBlobDriver } from '../lib/drivers/azure'
import { PreconditionFailedError } from '../lib/errors'
import { BlobServiceClient } from '@azure/storage-blob'
// @ts-ignore - azurite has no type declarations
import BlobServer from 'azurite/dist/src/blob/BlobServer'
// @ts-ignore - azurite has no type declarations
import BlobConfiguration from 'azurite/dist/src/blob/BlobConfiguration'
import * as fs from 'fs'
import * as path from 'path'
import * as os from 'os'
import { Readable } from 'stream'

const streamToString = (stream: Readable): Promise<string> => {
  const chunks: Buffer[] = []
  return new Promise((resolve, reject) => {
    stream.on('data', (chunk) => chunks.push(Buffer.from(chunk)))
    stream.on('error', (err) => reject(err))
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
  })
}

const TEST_PORT = 10000
const TEST_CONTAINER = 'test-container'
// Azurite default connection string
const TEST_CONNECTION_STRING = `DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:${TEST_PORT}/devstoreaccount1;`

describe('AzureBlobDriver', () => {
  let blobServer: BlobServer
  let tmpDir: string
  let driver: AzureBlobDriver

  beforeAll(async () => {
    tmpDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'azure-test-'))

    // Create persistence location array
    const persistenceArray = [{
      locationId: 'Default',
      locationPath: path.join(tmpDir, '__blobstorage__'),
      maxConcurrency: 50
    }]

    // Create blob configuration
    const config = new BlobConfiguration(
      '127.0.0.1',                             // host
      TEST_PORT,                               // port
      5,                                       // keepAliveTimeout
      path.join(tmpDir, '__azurite_db_blob__.json'),  // metadataDBPath
      path.join(tmpDir, '__azurite_db_blob_extent__.json'),  // extentDBPath
      persistenceArray,                        // persistencePathArray
      false,                                   // enableAccessLog
      undefined,                               // accessLogWriteStream
      false,                                   // enableDebugLog
      undefined,                               // debugLogFilePath
      false,                                   // loose
      true                                     // skipApiVersionCheck
    )

    // Start Azurite blob service
    blobServer = new BlobServer(config)
    await blobServer.start()

    // Create test container
    const blobServiceClient = BlobServiceClient.fromConnectionString(TEST_CONNECTION_STRING)
    const containerClient = blobServiceClient.getContainerClient(TEST_CONTAINER)
    await containerClient.createIfNotExists()
  })

  afterAll(async () => {
    await blobServer.close()
    await fs.promises.rm(tmpDir, { recursive: true, force: true })
  })

  beforeEach(() => {
    driver = new AzureBlobDriver(TEST_CONNECTION_STRING, TEST_CONTAINER)
  })

  test('put and get', async () => {
    const key = 'test-' + Date.now() + '.txt'
    const content = 'hello world'
    await driver.put(key, content)

    const result = await driver.get(key)
    expect(result).toBeDefined()
    expect(result!.etag).toBeDefined()

    const readContent = await streamToString(result!.stream)
    expect(readContent).toBe(content)
  })

  test('get non-existent key', async () => {
    const result = await driver.get('non-existent-' + Date.now() + '.txt')
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

  test('putIfNoneMatch failure (already exists)', async () => {
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

  test('putIfMatch failure (etag mismatch)', async () => {
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

  test('nested directories', async () => {
    const key = 'deep/nested/dir/file-' + Date.now() + '.txt'
    await driver.put(key, 'content')
    const result = await driver.get(key)
    expect(result).toBeDefined()
    const content = await streamToString(result!.stream)
    expect(content).toBe('content')
  })
})
