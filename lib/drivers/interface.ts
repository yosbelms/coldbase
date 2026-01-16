import { Readable } from 'stream'

export interface ListResult {
  keys: string[]
  continuationToken?: string
}

export interface StorageDriver {
  /**
   * Writes data to a key. Overwrites if exists.
   */
  put(key: string, body: string): Promise<void>

  /**
   * Atomic write. Only succeeds if the key does not exist.
   * Returns the new ETag (or equivalent version identifier).
   * Throws 'PreconditionFailed' if key exists.
   */
  putIfNoneMatch(key: string, body: string): Promise<string>

  /**
   * Atomic write. Only succeeds if the current version matches `etag`.
   * Returns the new ETag.
   * Throws 'PreconditionFailed' if version mismatch.
   */
  putIfMatch(key: string, body: string, etag: string): Promise<string>

  /**
   * Reads data. Returns undefined if not found.
   * Returns a Readable stream to avoid buffering entire files in memory.
   */
  get(key: string): Promise<{ stream: Readable, etag: string } | undefined>

  /**
   * Lists keys with a given prefix.
   */
  list(prefix: string, continuationToken?: string): Promise<ListResult>

  /**
   * Deletes multiple keys.
   */
  delete(keys: string[]): Promise<void>

  /**
   * Returns the size of the object in bytes, or undefined if not found.
   */
  size(key: string): Promise<number | undefined>

  /**
   * Appends data to the end of the object. 
   */
  append(key: string, data: string): Promise<void>
}