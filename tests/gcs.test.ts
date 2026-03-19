/**
 * GCSDriver unit tests using Jest mocks.
 *
 * These tests mock @google-cloud/storage to run without external infrastructure.
 * For integration tests against a real emulator, run fake-gcs-server locally:
 *   npx @google-cloud/storage with apiEndpoint pointing to http://localhost:4443
 */
import { GCSDriver } from '../lib/drivers/gcs'
import { PreconditionFailedError } from '../lib/errors'
import { Readable } from 'stream'

// --- Mock @google-cloud/storage ---

const mockSave = jest.fn()
const mockGetMetadata = jest.fn()
const mockCreateReadStream = jest.fn()
const mockDelete = jest.fn()
const mockGetFiles = jest.fn()

const mockFile = {
  save: mockSave,
  getMetadata: mockGetMetadata,
  createReadStream: mockCreateReadStream,
  delete: mockDelete
}

const mockBucket = {
  file: jest.fn(() => mockFile),
  getFiles: mockGetFiles
}

jest.mock('@google-cloud/storage', () => ({
  Storage: jest.fn().mockImplementation(() => ({
    bucket: jest.fn(() => mockBucket)
  }))
}))

// --- Helpers ---

const makeReadable = (content: string): Readable => {
  const stream = new Readable({ read() {} })
  process.nextTick(() => {
    stream.push(content)
    stream.push(null)
  })
  return stream
}

const streamToString = (stream: Readable): Promise<string> =>
  new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    stream.on('data', chunk => chunks.push(Buffer.from(chunk)))
    stream.on('error', reject)
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
  })

// --- Tests ---

describe('GCSDriver', () => {
  let driver: GCSDriver

  beforeEach(() => {
    jest.clearAllMocks()
    driver = new GCSDriver('test-bucket')
  })

  // ---- put ----

  test('put calls file.save', async () => {
    mockSave.mockResolvedValue(undefined)
    await driver.put('file.txt', 'hello')
    expect(mockBucket.file).toHaveBeenCalledWith('file.txt')
    expect(mockSave).toHaveBeenCalledWith('hello', { contentType: 'application/json' })
  })

  // ---- putIfNoneMatch ----

  test('putIfNoneMatch success returns generation', async () => {
    mockSave.mockResolvedValue(undefined)
    mockGetMetadata.mockResolvedValue([{ generation: '42' }])

    const etag = await driver.putIfNoneMatch('file.txt', 'hello')

    expect(mockSave).toHaveBeenCalledWith('hello', expect.objectContaining({
      preconditionOpts: { ifGenerationMatch: 0 }
    }))
    expect(etag).toBe('42')
  })

  test('putIfNoneMatch throws PreconditionFailedError when key exists', async () => {
    const err = Object.assign(new Error('Precondition Failed'), { code: 412 })
    mockSave.mockRejectedValue(err)

    await expect(driver.putIfNoneMatch('file.txt', 'hello'))
      .rejects.toThrow(PreconditionFailedError)
  })

  test('putIfNoneMatch rethrows non-precondition errors', async () => {
    const err = new Error('network error')
    mockSave.mockRejectedValue(err)

    await expect(driver.putIfNoneMatch('file.txt', 'hello'))
      .rejects.toThrow('network error')
  })

  // ---- putIfMatch ----

  test('putIfMatch success passes generation and returns new generation', async () => {
    mockSave.mockResolvedValue(undefined)
    mockGetMetadata.mockResolvedValue([{ generation: '43' }])

    const etag = await driver.putIfMatch('file.txt', 'updated', '42')

    expect(mockSave).toHaveBeenCalledWith('updated', expect.objectContaining({
      preconditionOpts: { ifGenerationMatch: 42 }
    }))
    expect(etag).toBe('43')
  })

  test('putIfMatch throws PreconditionFailedError on mismatch', async () => {
    const err = Object.assign(new Error('Precondition Failed'), { code: 412 })
    mockSave.mockRejectedValue(err)

    await expect(driver.putIfMatch('file.txt', 'updated', '99'))
      .rejects.toThrow(PreconditionFailedError)
  })

  // ---- get ----

  test('get returns stream and etag', async () => {
    mockGetMetadata.mockResolvedValue([{ generation: '10' }])
    const readable = makeReadable('file content')
    mockCreateReadStream.mockReturnValue(readable)

    const result = await driver.get('file.txt')
    expect(result).toBeDefined()
    expect(result!.etag).toBe('10')
    const content = await streamToString(result!.stream)
    expect(content).toBe('file content')
  })

  test('get returns undefined for non-existent key', async () => {
    const err = Object.assign(new Error('Not Found'), { code: 404 })
    mockGetMetadata.mockRejectedValue(err)

    const result = await driver.get('missing.txt')
    expect(result).toBeUndefined()
  })

  test('get rethrows non-404 errors', async () => {
    const err = Object.assign(new Error('Forbidden'), { code: 403 })
    mockGetMetadata.mockRejectedValue(err)

    await expect(driver.get('file.txt')).rejects.toThrow('Forbidden')
  })

  // ---- list ----

  test('list returns keys from bucket.getFiles', async () => {
    mockGetFiles.mockResolvedValue([
      [{ name: 'prefix/a.txt' }, { name: 'prefix/b.txt' }],
      null
    ])

    const result = await driver.list('prefix/')
    expect(result.keys).toEqual(['prefix/a.txt', 'prefix/b.txt'])
    expect(result.continuationToken).toBeUndefined()
  })

  test('list passes continuation token as pageToken', async () => {
    mockGetFiles.mockResolvedValue([[{ name: 'prefix/c.txt' }], null])

    await driver.list('prefix/', 'token123')

    expect(mockGetFiles).toHaveBeenCalledWith(expect.objectContaining({
      pageToken: 'token123'
    }))
  })

  test('list returns continuationToken when more pages exist', async () => {
    mockGetFiles.mockResolvedValue([
      [{ name: 'prefix/a.txt' }],
      { pageToken: 'next-page-token' }
    ])

    const result = await driver.list('prefix/')
    expect(result.continuationToken).toBe('next-page-token')
  })

  test('list returns empty array when no files match', async () => {
    mockGetFiles.mockResolvedValue([[], null])

    const result = await driver.list('no-match/')
    expect(result.keys).toEqual([])
    expect(result.continuationToken).toBeUndefined()
  })

  // ---- delete ----

  test('delete calls file.delete for each key', async () => {
    mockDelete.mockResolvedValue(undefined)

    await driver.delete(['a.txt', 'b.txt'])

    expect(mockBucket.file).toHaveBeenCalledWith('a.txt')
    expect(mockBucket.file).toHaveBeenCalledWith('b.txt')
    expect(mockDelete).toHaveBeenCalledTimes(2)
    expect(mockDelete).toHaveBeenCalledWith({ ignoreNotFound: true })
  })

  test('delete with empty array is a no-op', async () => {
    await driver.delete([])
    expect(mockDelete).not.toHaveBeenCalled()
  })

  // ---- size ----

  test('size returns numeric size from metadata', async () => {
    mockGetMetadata.mockResolvedValue([{ size: '1024', generation: '5' }])

    const size = await driver.size('file.txt')
    expect(size).toBe(1024)
  })

  test('size returns undefined for non-existent key', async () => {
    const err = Object.assign(new Error('Not Found'), { code: 404 })
    mockGetMetadata.mockRejectedValue(err)

    const size = await driver.size('missing.txt')
    expect(size).toBeUndefined()
  })

  test('size rethrows non-404 errors', async () => {
    const err = Object.assign(new Error('Internal Error'), { code: 500 })
    mockGetMetadata.mockRejectedValue(err)

    await expect(driver.size('file.txt')).rejects.toThrow('Internal Error')
  })

  // ---- append ----

  test('append to existing file', async () => {
    // get() call: getMetadata returns generation, createReadStream returns existing content
    mockGetMetadata.mockResolvedValue([{ generation: '1' }])
    mockCreateReadStream.mockReturnValue(makeReadable('line1'))
    mockSave.mockResolvedValue(undefined)

    await driver.append('log.txt', 'line2')

    expect(mockSave).toHaveBeenCalledWith('line1\nline2', expect.any(Object))
  })

  test('append to non-existent file creates it', async () => {
    // get() returns undefined (404)
    const err = Object.assign(new Error('Not Found'), { code: 404 })
    mockGetMetadata.mockRejectedValue(err)
    mockSave.mockResolvedValue(undefined)

    await driver.append('new-log.txt', 'first-line')

    expect(mockSave).toHaveBeenCalledWith('first-line', expect.any(Object))
  })

  // ---- constructor options ----

  test('accepts apiEndpoint for emulator usage', () => {
    const { Storage } = require('@google-cloud/storage')
    new GCSDriver('my-bucket', { apiEndpoint: 'http://localhost:4443' })
    expect(Storage).toHaveBeenCalledWith(expect.objectContaining({
      apiEndpoint: 'http://localhost:4443'
    }))
  })

  test('accepts keyFilename option', () => {
    const { Storage } = require('@google-cloud/storage')
    new GCSDriver('my-bucket', { keyFilename: '/path/to/key.json' })
    expect(Storage).toHaveBeenCalledWith(expect.objectContaining({
      keyFilename: '/path/to/key.json'
    }))
  })

  test('accepts projectId option', () => {
    const { Storage } = require('@google-cloud/storage')
    new GCSDriver('my-bucket', { projectId: 'my-project' })
    expect(Storage).toHaveBeenCalledWith(expect.objectContaining({
      projectId: 'my-project'
    }))
  })
})
