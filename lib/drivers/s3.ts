import {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  HeadObjectCommand,
  CreateMultipartUploadCommand,
  UploadPartCopyCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand
} from '@aws-sdk/client-s3'
import { getLogger } from '@logtape/logtape'
import { StorageDriver, ListResult } from './interface'
import { Readable } from 'stream'
import { PreconditionFailedError } from '../errors'
import { streamToString } from '../utils'

interface S3Error extends Error {
  name: string
  $metadata?: { httpStatusCode?: number }
}

const isPreconditionFailed = (e: S3Error) =>
  e.name === 'PreconditionFailed' || e.$metadata?.httpStatusCode === 412

const isNotFound = (e: S3Error) => e.name === 'NoSuchKey' || e.name === 'NotFound'

export class S3Driver implements StorageDriver {
  private client: S3Client
  private logger = getLogger(['coldbase', 'driver', 's3'])

  /**
   * Request-scoped cache for file contents.
   * Avoids re-downloading the same file multiple times within a single request.
   * Key: object key, Value: { content, etag, size }
   */
  private contentCache = new Map<string, { content: string; etag: string; size: number }>()

  constructor(
    private bucket: string,
    region = 'us-east-1',
    options?: {
      endpoint?: string
      credentials?: { accessKeyId: string; secretAccessKey: string }
      forcePathStyle?: boolean
    }
  ) {
    this.client = new S3Client({
      region,
      endpoint: options?.endpoint,
      credentials: options?.credentials,
      forcePathStyle: options?.forcePathStyle
    })
  }

  /**
   * Clear the content cache. Call this at the start of a new request
   * if reusing the driver instance across requests.
   */
  clearCache(): void {
    this.contentCache.clear()
  }

  async put(key: string, body: string): Promise<void> {
    this.logger.debug('S3 Put {key}', { key })
    const res = await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: key,
        Body: body,
        ContentType: 'application/json'
      })
    )

    // Update cache with new content
    this.contentCache.set(key, {
      content: body,
      etag: res.ETag || '',
      size: Buffer.byteLength(body, 'utf-8')
    })
  }

  async putIfNoneMatch(key: string, body: string): Promise<string> {
    this.logger.debug('S3 PutIfNoneMatch {key}', { key })
    try {
      const res = await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Body: body,
          IfNoneMatch: '*'
        })
      )
      return res.ETag!
    } catch (e) {
      if (isPreconditionFailed(e as S3Error)) {
        throw new PreconditionFailedError('Key already exists')
      }
      throw e
    }
  }

  async putIfMatch(key: string, body: string, etag: string): Promise<string> {
    this.logger.debug('S3 PutIfMatch {key} etag={etag}', { key, etag })
    try {
      const res = await this.client.send(
        new PutObjectCommand({
          Bucket: this.bucket,
          Key: key,
          Body: body,
          IfMatch: etag
        })
      )
      return res.ETag!
    } catch (e) {
      if (isPreconditionFailed(e as S3Error)) {
        throw new PreconditionFailedError('ETag mismatch')
      }
      throw e
    }
  }

  async get(key: string): Promise<{ stream: Readable; etag: string } | undefined> {
    try {
      const res = await this.client.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: key
        })
      )
      return { stream: res.Body as Readable, etag: res.ETag! }
    } catch (e) {
      if (isNotFound(e as S3Error)) return undefined
      throw e
    }
  }

  async list(prefix: string, continuationToken?: string): Promise<ListResult> {
    const list = await this.client.send(
      new ListObjectsV2Command({
        Bucket: this.bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken
      })
    )
    return {
      keys: list.Contents?.map(c => c.Key!).filter(Boolean) || [],
      continuationToken: list.NextContinuationToken
    }
  }

  async delete(keys: string[]): Promise<void> {
    if (keys.length === 0) return
    for (let i = 0; i < keys.length; i += 1000) {
      const batch = keys.slice(i, i + 1000)
      await this.client.send(
        new DeleteObjectsCommand({
          Bucket: this.bucket,
          Delete: { Objects: batch.map(k => ({ Key: k })) }
        })
      )
    }
  }

  async size(key: string): Promise<number | undefined> {
    // Check cache first
    const cached = this.contentCache.get(key)
    if (cached) {
      return cached.size
    }

    try {
      const head = await this.client.send(
        new HeadObjectCommand({ Bucket: this.bucket, Key: key })
      )
      return head.ContentLength
    } catch (e) {
      if (isNotFound(e as S3Error)) return undefined
      throw e
    }
  }

  async append(key: string, data: string): Promise<void> {
    // Check cache first for size
    const cached = this.contentCache.get(key)
    const currentSize = cached?.size ?? (await this.size(key)) ?? 0
    const MIN_PART_SIZE = 5 * 1024 * 1024

    if (currentSize > MIN_PART_SIZE) {
      await this.mergeMultipart(key, data)
      // Invalidate cache after multipart merge (we don't track the new content)
      this.contentCache.delete(key)
    } else {
      await this.mergeSimple(key, data)
    }
  }

  private async mergeMultipart(key: string, newData: string): Promise<void> {
    const multipart = await this.client.send(
      new CreateMultipartUploadCommand({
        Bucket: this.bucket,
        Key: key,
        ContentType: 'application/x-jsonlines'
      })
    )
    const uploadId = multipart.UploadId!

    try {
      const copyPart = await this.client.send(
        new UploadPartCopyCommand({
          Bucket: this.bucket,
          Key: key,
          UploadId: uploadId,
          PartNumber: 1,
          CopySource: `${this.bucket}/${key}`
        })
      )

      const newPart = await this.client.send(
        new UploadPartCommand({
          Bucket: this.bucket,
          Key: key,
          UploadId: uploadId,
          PartNumber: 2,
          Body: '\n' + newData
        })
      )

      await this.client.send(
        new CompleteMultipartUploadCommand({
          Bucket: this.bucket,
          Key: key,
          UploadId: uploadId,
          MultipartUpload: {
            Parts: [
              { ETag: copyPart.CopyPartResult?.ETag, PartNumber: 1 },
              { ETag: newPart.ETag, PartNumber: 2 }
            ]
          }
        })
      )
    } catch (e) {
      try {
        await this.client.send(
          new AbortMultipartUploadCommand({
            Bucket: this.bucket,
            Key: key,
            UploadId: uploadId
          })
        )
      } catch (abortErr) {
        this.logger.warn('Failed to abort multipart upload {uploadId} for {key}: {error}',
          { uploadId, key, error: abortErr })
      }
      throw e
    }
  }

  /**
   * Merge using simple download + upload. Uses cache to avoid re-downloading
   * if we already have the content from a previous operation in this request.
   */
  private async mergeSimple(key: string, newData: string): Promise<void> {
    // Check cache first to avoid re-downloading
    const cached = this.contentCache.get(key)
    let existingStr: string

    if (cached) {
      this.logger.debug('S3 mergeSimple using cached content for {key}', { key })
      existingStr = cached.content
    } else {
      const existing = await this.get(key)
      if (!existing) {
        await this.put(key, newData)
        return
      }
      existingStr = await streamToString(existing.stream)

      // Cache the downloaded content for potential future use
      this.contentCache.set(key, {
        content: existingStr,
        etag: existing.etag,
        size: Buffer.byteLength(existingStr, 'utf-8')
      })
    }

    const updated = existingStr ? `${existingStr}\n${newData}` : newData
    await this.put(key, updated)
  }
}
