import { BlobServiceClient, ContainerClient, BlobUploadCommonResponse } from '@azure/storage-blob'
import { getLogger } from '@logtape/logtape'
import { StorageDriver, ListResult } from './interface'
import { Readable } from 'stream'
import { PreconditionFailedError } from '../errors'
import { streamToString } from '../utils'

interface AzureError extends Error {
  statusCode?: number
  code?: string
}

const isNotFound = (e: AzureError) => e.statusCode === 404 || e.code === 'BlobNotFound'
const isPreconditionFailed = (e: AzureError) => e.statusCode === 412 || e.statusCode === 409

export class AzureBlobDriver implements StorageDriver {
  private container: ContainerClient
  private logger = getLogger(['coldbase', 'driver', 'azure'])

  constructor(connectionString: string, containerName: string) {
    this.container = BlobServiceClient.fromConnectionString(connectionString).getContainerClient(
      containerName
    )
  }

  private getBlob(key: string) {
    return this.container.getBlockBlobClient(key)
  }

  private async upload(
    key: string,
    body: string,
    conditions?: { ifNoneMatch?: string; ifMatch?: string }
  ): Promise<string> {
    this.logger.debug('Azure Upload {key} conditions={conditions}', { key, conditions })
    const buf = Buffer.from(body, 'utf-8')
    try {
      const res: BlobUploadCommonResponse = await this.getBlob(key).upload(buf, buf.length, {
        conditions
      })
      return res.etag!
    } catch (e) {
      if (conditions && isPreconditionFailed(e as AzureError)) {
        throw new PreconditionFailedError()
      }
      throw e
    }
  }

  async put(key: string, body: string): Promise<void> {
    await this.upload(key, body)
  }

  async putIfNoneMatch(key: string, body: string): Promise<string> {
    return this.upload(key, body, { ifNoneMatch: '*' })
  }

  async putIfMatch(key: string, body: string, etag: string): Promise<string> {
    return this.upload(key, body, { ifMatch: etag })
  }

  async get(key: string): Promise<{ stream: Readable; etag: string } | undefined> {
    try {
      const dl = await this.getBlob(key).download()
      return { stream: dl.readableStreamBody as Readable, etag: dl.etag! }
    } catch (e) {
      if (isNotFound(e as AzureError)) return undefined
      throw e
    }
  }

  async list(prefix: string, continuationToken?: string): Promise<ListResult> {
    const page = await this.container
      .listBlobsFlat({ prefix })
      .byPage({ maxPageSize: 1000, continuationToken })
      .next()

    if (page.done) return { keys: [] }
    return {
      keys: page.value.segment.blobItems.map(b => b.name),
      continuationToken: page.value.continuationToken
    }
  }

  async delete(keys: string[]): Promise<void> {
    await Promise.all(keys.map(k => this.getBlob(k).deleteIfExists()))
  }

  async size(key: string): Promise<number | undefined> {
    try {
      return (await this.getBlob(key).getProperties()).contentLength
    } catch (e) {
      if (isNotFound(e as AzureError)) return undefined
      throw e
    }
  }

  async append(key: string, data: string): Promise<void> {
    const current = await this.get(key)
    let existing = ''
    if (current) {
      existing = await streamToString(current.stream)
    }
    await this.put(key, existing ? `${existing}\n${data}` : data)
  }
}