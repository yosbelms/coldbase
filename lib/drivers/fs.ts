import { promises as fs, createReadStream } from 'fs'
import * as path from 'path'
import { getLogger } from '@logtape/logtape'
import { StorageDriver, ListResult } from './interface'
import { Readable } from 'stream'
import { PreconditionFailedError } from '../errors'

interface FsError extends Error {
  code?: string
}

export class FileSystemDriver implements StorageDriver {
  private logger = getLogger(['coldbase', 'driver', 'fs'])

  constructor(private rootDir: string) {}

  private getPath(key: string): string {
    return path.join(this.rootDir, key)
  }

  private async ensureDir(filePath: string) {
    await fs.mkdir(path.dirname(filePath), { recursive: true })
  }

  async put(key: string, body: string): Promise<void> {
    this.logger.debug('Writing file {key}', { key })
    const filePath = this.getPath(key)
    await this.ensureDir(filePath)
    await fs.writeFile(filePath, body, 'utf-8')
  }

  async putIfNoneMatch(key: string, body: string): Promise<string> {
    this.logger.debug('Writing file {key} if none match', { key })
    const filePath = this.getPath(key)
    await this.ensureDir(filePath)
    try {
      await fs.writeFile(filePath, body, { flag: 'wx', encoding: 'utf-8' })
      const stats = await fs.stat(filePath)
      return stats.mtimeMs.toString()
    } catch (e) {
      if ((e as FsError).code === 'EEXIST') {
        throw new PreconditionFailedError('Key already exists')
      }
      throw e
    }
  }

  async putIfMatch(key: string, body: string, etag: string): Promise<string> {
    this.logger.debug('Writing file {key} if match {etag}', { key, etag })
    const filePath = this.getPath(key)
    try {
      const stats = await fs.stat(filePath)
      if (stats.mtimeMs.toString() !== etag) {
        throw new PreconditionFailedError('ETag mismatch')
      }
      await fs.writeFile(filePath, body, 'utf-8')
      const newStats = await fs.stat(filePath)
      return newStats.mtimeMs.toString()
    } catch (e) {
      if (e instanceof PreconditionFailedError) throw e
      if ((e as FsError).code === 'ENOENT') {
        throw new PreconditionFailedError('Key not found')
      }
      throw e
    }
  }

  async get(key: string): Promise<{ stream: Readable; etag: string } | undefined> {
    const filePath = this.getPath(key)
    try {
      const stats = await fs.stat(filePath)
      const stream = createReadStream(filePath)
      return { stream, etag: stats.mtimeMs.toString() }
    } catch (e) {
      if ((e as FsError).code === 'ENOENT') return undefined
      throw e
    }
  }

  async list(prefix: string, continuationToken?: string): Promise<ListResult> {
    // this.logger.debug`Listing files with prefix ${prefix}` // Can be noisy
    const allKeys: string[] = []

    const walk = async (dir: string, base: string): Promise<void> => {
      try {
        const files = await fs.readdir(dir, { withFileTypes: true })
        for (const file of files) {
          const fullPath = path.join(dir, file.name)
          const relPath = path.join(base, file.name)
          if (file.isDirectory()) {
            await walk(fullPath, relPath)
          } else if (relPath.startsWith(prefix)) {
            allKeys.push(relPath)
          }
        }
      } catch (e) {
        if ((e as FsError).code === 'ENOENT') return
        throw e
      }
    }

    await walk(this.rootDir, '')
    allKeys.sort()

    let start = 0
    if (continuationToken) {
      start = parseInt(continuationToken, 10)
      if (isNaN(start)) start = 0
    }

    const limit = 1000
    const page = allKeys.slice(start, start + limit)
    const nextToken = start + limit < allKeys.length ? (start + limit).toString() : undefined

    return { keys: page, continuationToken: nextToken }
  }

  async delete(keys: string[]): Promise<void> {
    await Promise.all(
      keys.map(async k => {
        try {
          await fs.unlink(this.getPath(k))
        } catch (e) {
          if ((e as FsError).code !== 'ENOENT') throw e
        }
      })
    )
  }

  async size(key: string): Promise<number | undefined> {
    try {
      const stats = await fs.stat(this.getPath(key))
      return stats.size
    } catch (e) {
      if ((e as FsError).code === 'ENOENT') return undefined
      throw e
    }
  }

  async append(key: string, data: string): Promise<void> {
    const filePath = this.getPath(key)
    await this.ensureDir(filePath)
    try {
      const stats = await fs.stat(filePath)
      if (stats.size > 0) {
        await fs.appendFile(filePath, '\n' + data, 'utf-8')
      } else {
        await fs.writeFile(filePath, data, 'utf-8')
      }
    } catch (e) {
      if ((e as FsError).code === 'ENOENT') {
        await fs.writeFile(filePath, data, 'utf-8')
      } else {
        throw e
      }
    }
  }
}