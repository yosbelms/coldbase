import { Storage } from '@google-cloud/storage'
import { getLogger } from '@logtape/logtape'
import { StorageDriver, ListResult } from './interface'
import { Readable } from 'stream'
import { PreconditionFailedError } from '../errors'
import { streamToString } from '../utils'

interface GCSError extends Error {
  code?: number
}

const isNotFound = (e: GCSError) => e.code === 404
const isPreconditionFailed = (e: GCSError) => e.code === 412

export class GCSDriver implements StorageDriver {
  private storage: Storage
  private logger = getLogger(['coldbase', 'driver', 'gcs'])

  /**
   * @param bucket - GCS bucket name
   * @param options - Optional configuration
   * @param options.projectId - GCP project ID (defaults to GCLOUD_PROJECT / GOOGLE_CLOUD_PROJECT env var)
   * @param options.keyFilename - Path to service account key file
   * @param options.credentials - Service account credentials object
   * @param options.apiEndpoint - Override API endpoint (e.g. for fake-gcs-server emulator)
   */
  constructor(
    private bucket: string,
    options?: {
      projectId?: string
      keyFilename?: string
      credentials?: object
      apiEndpoint?: string
    }
  ) {
    this.storage = new Storage({
      projectId: options?.projectId,
      keyFilename: options?.keyFilename,
      credentials: options?.credentials as any,
      apiEndpoint: options?.apiEndpoint
    })
  }

  private getFile(key: string) {
    return this.storage.bucket(this.bucket).file(key)
  }

  async put(key: string, body: string): Promise<void> {
    this.logger.debug('GCS Put {key}', { key })
    await this.getFile(key).save(body, { contentType: 'application/json' })
  }

  async putIfNoneMatch(key: string, body: string): Promise<string> {
    this.logger.debug('GCS PutIfNoneMatch {key}', { key })
    const file = this.getFile(key)
    try {
      await file.save(body, {
        contentType: 'application/json',
        preconditionOpts: { ifGenerationMatch: 0 }
      })
      const [metadata] = await file.getMetadata()
      return String(metadata.generation)
    } catch (e) {
      if (isPreconditionFailed(e as GCSError)) {
        throw new PreconditionFailedError('Key already exists')
      }
      throw e
    }
  }

  async putIfMatch(key: string, body: string, etag: string): Promise<string> {
    this.logger.debug('GCS PutIfMatch {key} generation={etag}', { key, etag })
    const file = this.getFile(key)
    try {
      await file.save(body, {
        contentType: 'application/json',
        preconditionOpts: { ifGenerationMatch: parseInt(etag, 10) }
      })
      const [metadata] = await file.getMetadata()
      return String(metadata.generation)
    } catch (e) {
      if (isPreconditionFailed(e as GCSError)) {
        throw new PreconditionFailedError('Generation mismatch')
      }
      throw e
    }
  }

  async get(key: string): Promise<{ stream: Readable; etag: string } | undefined> {
    const file = this.getFile(key)
    try {
      const [metadata] = await file.getMetadata()
      const stream = file.createReadStream()
      return { stream, etag: String(metadata.generation) }
    } catch (e) {
      if (isNotFound(e as GCSError)) return undefined
      throw e
    }
  }

  async list(prefix: string, continuationToken?: string): Promise<ListResult> {
    const [files, nextQuery] = await this.storage.bucket(this.bucket).getFiles({
      prefix,
      autoPaginate: false,
      maxResults: 1000,
      pageToken: continuationToken
    } as any)
    return {
      keys: (files as any[]).map((f: any) => f.name),
      continuationToken: (nextQuery as any)?.pageToken
    }
  }

  async delete(keys: string[]): Promise<void> {
    if (keys.length === 0) return
    await Promise.all(keys.map(k => this.getFile(k).delete({ ignoreNotFound: true })))
  }

  async size(key: string): Promise<number | undefined> {
    try {
      const [metadata] = await this.getFile(key).getMetadata()
      const sz = metadata.size
      return sz !== undefined ? Number(sz) : undefined
    } catch (e) {
      if (isNotFound(e as GCSError)) return undefined
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
