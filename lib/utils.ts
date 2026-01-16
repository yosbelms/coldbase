import { Readable } from 'stream'

export async function streamToString(stream: Readable): Promise<string> {
  const chunks: Buffer[] = []
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  }
  return Buffer.concat(chunks).toString('utf-8')
}

export async function* streamLines(stream: Readable): AsyncGenerator<{ line: string; lineNum: number }> {
  let buffer = ''
  let lineNum = 0
  for await (const chunk of stream) {
    buffer += chunk
    const lines = buffer.split('\n')
    buffer = lines.pop() || ''
    for (const line of lines) {
      if (line.trim()) yield { line, lineNum }
      lineNum++
    }
  }
  if (buffer.trim()) yield { line: buffer, lineNum }
}

export async function* streamJsonLines<T = unknown>(stream: Readable): AsyncGenerator<T> {
  for await (const { line } of streamLines(stream)) {
    try {
      yield JSON.parse(line)
    } catch { /* ignore malformed */ }
  }
}

export async function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms))
}

export async function retry<T>(
  fn: () => Promise<T>,
  options: { maxAttempts?: number; baseDelayMs?: number; maxDelayMs?: number } = {}
): Promise<T> {
  const { maxAttempts = 3, baseDelayMs = 100, maxDelayMs = 5000 } = options

  if (maxAttempts < 1) {
    throw new Error('maxAttempts must be at least 1')
  }

  let lastError: Error | undefined

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      return await fn()
    } catch (err: any) {
      lastError = err
      if (attempt < maxAttempts - 1 && isRetryable(err)) {
        const delay = Math.min(baseDelayMs * Math.pow(2, attempt), maxDelayMs)
        await sleep(delay + Math.random() * delay * 0.1) // jitter
      } else {
        throw err
      }
    }
  }
  throw lastError!
}

function isRetryable(err: any): boolean {
  if (!err) return false
  // Network errors
  if (err.code === 'ECONNRESET' || err.code === 'ETIMEDOUT' || err.code === 'ENOTFOUND') return true
  // HTTP 5xx errors
  if (err.statusCode >= 500 && err.statusCode < 600) return true
  // Rate limiting
  if (err.statusCode === 429) return true
  // S3/Azure specific
  if (err.code === 'SlowDown' || err.code === 'ServiceUnavailable') return true
  return false
}

export async function parallelLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T) => Promise<R>
): Promise<R[]> {
  const results: R[] = new Array(items.length)
  const executing = new Set<Promise<void>>()

  for (let i = 0; i < items.length; i++) {
    const index = i
    const promise = fn(items[i])
      .then(result => {
        results[index] = result
      })
      .finally(() => {
        executing.delete(promise)
      })
    executing.add(promise)

    if (executing.size >= limit) {
      await Promise.race(executing)
    }
  }

  await Promise.all(executing)
  return results
}

export function chunk<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = []
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size))
  }
  return chunks
}

/**
 * Simple bloom filter for fast "definitely not exists" checks.
 * False positives possible, false negatives never.
 */
export class BloomFilter {
  private bits: Uint8Array
  private numHashes: number

  constructor(
    private expectedItems: number = 1000,
    private falsePositiveRate: number = 0.01
  ) {
    // Calculate optimal size and hash count
    const size = Math.ceil(
      (-expectedItems * Math.log(falsePositiveRate)) / (Math.LN2 * Math.LN2)
    )
    this.bits = new Uint8Array(Math.ceil(size / 8))
    this.numHashes = Math.ceil((size / expectedItems) * Math.LN2)
  }

  private hash(str: string, seed: number): number {
    let h = seed
    for (let i = 0; i < str.length; i++) {
      h = Math.imul(h ^ str.charCodeAt(i), 0x5bd1e995)
      h ^= h >>> 15
    }
    return Math.abs(h) % (this.bits.length * 8)
  }

  add(item: string): void {
    for (let i = 0; i < this.numHashes; i++) {
      const pos = this.hash(item, i)
      this.bits[Math.floor(pos / 8)] |= 1 << (pos % 8)
    }
  }

  mightContain(item: string): boolean {
    for (let i = 0; i < this.numHashes; i++) {
      const pos = this.hash(item, i)
      if (!(this.bits[Math.floor(pos / 8)] & (1 << (pos % 8)))) {
        return false
      }
    }
    return true
  }

  serialize(): string {
    return JSON.stringify({
      expectedItems: this.expectedItems,
      falsePositiveRate: this.falsePositiveRate,
      bits: Buffer.from(this.bits).toString('base64')
    })
  }

  static deserialize(data: string): BloomFilter {
    const { expectedItems, falsePositiveRate, bits } = JSON.parse(data)
    const filter = new BloomFilter(expectedItems, falsePositiveRate)
    filter.bits = new Uint8Array(Buffer.from(bits, 'base64'))
    return filter
  }
}

/**
 * LRU Cache for bounded memory usage during vacuum operations.
 */
export class LRUCache<K, V> {
  private cache = new Map<K, V>()

  constructor(private maxSize: number) {}

  get(key: K): V | undefined {
    const value = this.cache.get(key)
    if (value !== undefined) {
      // Move to end (most recently used)
      this.cache.delete(key)
      this.cache.set(key, value)
    }
    return value
  }

  set(key: K, value: V): void {
    if (this.cache.has(key)) {
      this.cache.delete(key)
    } else if (this.cache.size >= this.maxSize) {
      // Delete oldest (first) entry
      const firstKey = this.cache.keys().next().value
      if (firstKey !== undefined) {
        this.cache.delete(firstKey)
      }
    }
    this.cache.set(key, value)
  }

  has(key: K): boolean {
    return this.cache.has(key)
  }

  delete(key: K): boolean {
    return this.cache.delete(key)
  }

  clear(): void {
    this.cache.clear()
  }

  get size(): number {
    return this.cache.size
  }

  entries(): IterableIterator<[K, V]> {
    return this.cache.entries()
  }
}
