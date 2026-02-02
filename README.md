# Coldbase

A lightweight, serverless-first write-ahead log (WAL) database for cloud storage. Supports AWS S3, Azure Blob Storage, and local filesystem. Designed for stateless environments where each operation reads directly from storage.

## Features

- **Serverless-First**: Stateless operations, no in-memory state between invocations
- **Auto-Maintenance**: Probabilistic compaction and vacuum triggers for serverless
- **Multi-Collection Support**: Single `Db` instance manages multiple collections
- **Vector Collections**: Store and search vector embeddings with cosine, euclidean, or dot product similarity
- **Query API**: `find()` with filtering, pagination, and function predicates
- **TTL Support**: Auto-expire records based on a timestamp field
- **Batch Operations**: `getMany()` for efficient multi-key lookups, `batch()` for coalescing writes
- **Transactions**: Cross-collection atomicity via saga pattern with automatic compensation on failure
- **Parallel Processing**: Configurable parallelism for mutation processing
- **Retry Logic**: Exponential backoff with jitter for transient failures
- **Hooks & Metrics**: Monitor writes, compactions, and errors
- **Size Limits**: Configurable mutation size limits
- **Multiple Storage Backends**: S3, Azure Blob, or local filesystem
- **Performance Optimizations**: Bloom filter, in-memory index, lease-based locking

## Installation

```bash
npm install coldbase
```

For cloud storage, install the appropriate SDK:
```bash
npm install @aws-sdk/client-s3        # For S3
npm install @azure/storage-blob       # For Azure
```

## Quick Start

```typescript
import { Db, FileSystemDriver } from 'coldbase'

const db = new Db(new FileSystemDriver('./data'))

interface User {
  id: string
  name: string
  email: string
  role: string
}

const users = db.collection<User>('users')

// Write
await users.put({ id: 'u1', name: 'Alice', email: 'alice@example.com', role: 'admin' })

// Read single
const user = await users.get('u1')

// Query
const admins = await users.find({ where: { role: 'admin' } })

// Delete
await users.delete('u1')
```

## Logging

Coldbase uses [LogTape](https://github.com/doodadjs/logtape) for logging. You can configure it to see internal operations:

```typescript
import { configure, getConsoleSink } from '@logtape/logtape'

await configure({
  sinks: { console: getConsoleSink() },
  filters: {},
  loggers: [
    { category: 'coldbase', level: 'debug', sinks: ['console'] }
  ]
})
```

## Serverless Usage

Coldbase is designed for serverless environments where functions may cold start frequently. It offers probabilistic auto-maintenance that distributes load across invocations:

```typescript
import { Db, S3Driver, SERVERLESS_AUTO_COMPACT, SERVERLESS_AUTO_VACUUM } from 'coldbase'

// Option 1: Use recommended serverless presets
const db = new Db(new S3Driver('my-bucket', 'us-east-1'), {
  autoCompact: SERVERLESS_AUTO_COMPACT,  // 10% chance, min 5 mutations
  autoVacuum: SERVERLESS_AUTO_VACUUM     // 1% chance + 10% after compaction
})

// Option 2: Custom configuration
const db2 = new Db(new S3Driver('my-bucket', 'us-east-1'), {
  autoCompact: {
    probability: 0.05,      // 5% chance per write
    mutationThreshold: 10   // Only if >= 10 pending mutations
  },
  autoVacuum: {
    probability: 0.01,             // 1% chance per write
    afterCompactProbability: 0.2   // 20% chance after compaction
  }
})

// Lambda/Cloud Function handler
export async function handler(event) {
  const users = db.collection<User>('users')

  // Each read scans storage - no stale cache issues
  const user = await users.get(event.userId)

  // Writes may trigger maintenance probabilistically (non-blocking)
  await users.put({ ...user, lastSeen: Date.now() })

  return { user }
}
```

For high-traffic apps, you can also use a separate scheduled function:

```typescript
// Scheduled function for guaranteed maintenance (e.g., every 5 minutes)
export async function maintenanceHandler() {
  const db = new Db(new S3Driver('my-bucket', 'us-east-1'))

  await db.compact('users')
  await db.vacuum('users')
}
```

## API Reference

### `Db`

```typescript
const db = new Db(driver, options?)
```

**Options:**
```typescript
interface DbOptions {
  // Auto-maintenance (serverless-friendly)
  autoCompact?: boolean | AutoMaintenanceOptions  // (default: false)
  autoVacuum?: boolean | AutoVacuumOptions        // (default: false)

  // Lock configuration
  leaseDurationMs?: number     // Lock lease duration (default: 30000)

  // Processing
  copyBufferSize?: number      // Buffer size for file operations (default: 65536)
  parallelism?: number         // Parallel mutation processing (default: 5)
  deleteChunkSize?: number     // Batch size for deletes (default: 100)
  maxMutationSize?: number     // Max mutation payload bytes (default: 10MB)
  vacuumCacheSize?: number     // LRU cache size for vacuum (default: 100000)

  // Performance optimizations
  useIndex?: boolean           // Enable in-memory index for O(1) lookups (default: false)
  useBloomFilter?: boolean     // Enable bloom filter for fast "not exists" (default: false)
  bloomFilterExpectedItems?: number      // Expected items for sizing (default: 10000)
  bloomFilterFalsePositiveRate?: number  // False positive rate (default: 0.01)

  retryOptions?: {
    maxAttempts?: number       // Max retry attempts (default: 3)
    baseDelayMs?: number       // Base delay for backoff (default: 100)
    maxDelayMs?: number        // Max delay cap (default: 5000)
  }
  hooks?: DbHooks
}

// Probabilistic triggers for serverless environments
interface AutoMaintenanceOptions {
  probability?: number         // Chance (0-1) to trigger per write
  mutationThreshold?: number   // Min pending mutations before triggering
}

interface AutoVacuumOptions extends AutoMaintenanceOptions {
  afterCompactProbability?: number  // Chance to vacuum after compaction
}

interface DbHooks {
  onWrite?: (collection: string, count: number) => void
  onCompact?: (collection: string, durationMs: number, mutationCount: number) => void
  onVacuum?: (collection: string, durationMs: number, removedCount: number) => void
  onError?: (error: Error, operation: string) => void
}

interface MaintenanceSchedule {
  compactIntervalMs?: number   // Interval in ms
  vacuumIntervalMs?: number    // Interval in ms
  onError?: (error: Error, operation: 'compact' | 'vacuum') => void
}
```

**Serverless Presets:**
```typescript
import { SERVERLESS_AUTO_COMPACT, SERVERLESS_AUTO_VACUUM } from 'coldbase'

// SERVERLESS_AUTO_COMPACT = { probability: 0.1, mutationThreshold: 5 }
// SERVERLESS_AUTO_VACUUM = { probability: 0.01, mutationThreshold: 0, afterCompactProbability: 0.1 }
```

**Methods:**
```typescript
db.collection<T>(name: string, options?: CollectionOptions): Collection<T>
db.vectorCollection<T>(name: string, options: VectorCollectionOptions): VectorCollection<T>
db.compact(name: string): Promise<CompactResult>
db.vacuum(name: string): Promise<VacuumResult>
```

### `Collection<T>`

**Writing:**
```typescript
// Single item
await collection.put({ id: 'id1', ...fields })

// Delete
await collection.delete('id1')

// Batch writes (coalesces into single mutation file for better performance)
await collection.batch(tx => {
  tx.put({ id: 'id1', name: 'Alice' })
  tx.put({ id: 'id2', name: 'Bob' })
  tx.delete('id3')
})
```

**Reading:**
```typescript
// Single record (scans storage for latest value)
const item = await collection.get('id')

// Multiple records (single scan)
const items = await collection.getMany(['id1', 'id2', 'id3'])

// Query with filter
const results = await collection.find({
  where: { field: 'value' },  // or (item) => item.field > 10
  limit: 10,
  offset: 0
})

// Stream all records
for await (const { id, data } of collection.read()) {
  console.log(id, data)
}

// Count
const count = await collection.count()

// Maintenance
await collection.compact()
await collection.vacuum()
const pendingMutations = await collection.countMutationFiles()
collection.startMaintenance(schedule: MaintenanceSchedule)
collection.stopMaintenance()
```

**TTL:**
```typescript
interface Session {
  id: string
  userId: string
  expiresAt: number  // Unix timestamp ms
}

const sessions = db.collection<Session>('sessions', { ttlField: 'expiresAt' })

// Expired records are automatically filtered from reads
// Clean up expired records (call from scheduled function):
const deleted = await sessions.deleteExpired()
```

**Automatic Maintenance:**
For long-running processes (e.g., background workers), you can schedule maintenance to run automatically on a collection:

```typescript
const users = db.collection<User>('users')

users.startMaintenance({
  compactIntervalMs: 60000,    // Every minute
  vacuumIntervalMs: 300000,   // Every 5 minutes
  onError: (err, op) => console.error(`${op} failed:`, err)
})

// Stop maintenance when closing application
users.stopMaintenance()
```
The maintenance task automatically handles `LockActiveError` by silently skipping the operation if another process is already performing maintenance.

### Transactions

Cross-collection atomicity using the saga pattern. Each write tracks a compensation action; if any step fails, compensations run in reverse order to undo previous writes.

```typescript
await db.transaction(async (tx) => {
  const users = tx.collection<User>('users')
  const logs = tx.collection<Log>('logs')

  await users.put({ id: '1', name: 'Alice' })
  await logs.put({ id: 'log-1', action: 'user-created' })
})
// If logs.put fails → users.put is compensated (deleted)
```

The transactional collection supports `put`, `delete`, and `get` (read-only, no tracking). On failure, a `TransactionError` is thrown containing the original error and any compensation errors:

```typescript
import { TransactionError } from 'coldbase'

try {
  await db.transaction(async (tx) => { /* ... */ })
} catch (e) {
  if (e instanceof TransactionError) {
    console.log(e.originalError)        // The error that caused the rollback
    console.log(e.compensationErrors)   // Any errors during compensation
  }
}
```

**Nested Transactions (Savepoints):**

Transactions can be nested via `tx.transaction()`. A nested transaction acts as a savepoint — if it fails, only its own writes are rolled back while the outer transaction continues. If it succeeds, its compensations are promoted to the parent so they roll back if a later outer step fails.

```typescript
await db.transaction(async (tx) => {
  const users = tx.collection<User>('users')
  await users.put({ id: '1', name: 'Alice' })

  // Nested transaction: if this fails, only inner writes are rolled back
  try {
    await tx.transaction(async (inner) => {
      const logs = inner.collection<Log>('logs')
      await logs.put({ id: 'log-1', action: 'user-created' })
      throw new Error('inner failure')
    })
  } catch {
    // Inner rolled back, outer continues — users.put still committed
  }
})
```

### `VectorCollection<T>`

Vector collections store documents with vector embeddings and support similarity search. Uses brute-force (exact) search, suitable for small to medium datasets (10k-100k vectors).

**Creating a Vector Collection:**
```typescript
interface Embedding {
  id: string
  vector: number[]
  text: string
  category?: string
}

const embeddings = db.vectorCollection<Embedding>('embeddings', {
  dimension: 384,           // Required: vector dimension
  metric: 'cosine',         // 'cosine' | 'euclidean' | 'dotProduct' (default: 'cosine')
  normalize: true           // Auto-normalize on insert (default: true for cosine)
})
```

**Writing:**
```typescript
// Single item
await embeddings.put({
  id: 'doc1',
  vector: [0.1, 0.2, ...],  // Must match dimension
  text: 'Hello world'
})

// Batch writes
await embeddings.batch(tx => {
  tx.put({ id: 'doc1', vector: [...], text: 'First' })
  tx.put({ id: 'doc2', vector: [...], text: 'Second' })
})

// Delete
await embeddings.delete('doc1')
```

**Similarity Search:**
```typescript
const results = await embeddings.search([0.1, 0.2, ...], {
  limit: 10,                           // Max results (default: 10)
  threshold: 0.8,                      // Min similarity (cosine/dot) or max distance (euclidean)
  filter: { category: 'news' },        // Metadata filter (object or function)
  includeVector: false                 // Include vectors in results (default: false)
})

// Results sorted by similarity (descending for cosine/dot, ascending for euclidean)
for (const { id, score, data } of results) {
  console.log(`${id}: ${score} - ${data.text}`)
}

// Filter with function
const results = await embeddings.search(queryVector, {
  filter: (item) => item.category === 'news' && item.text.length > 100
})
```

**Reading:**
```typescript
// Single record
const doc = await embeddings.get('doc1')

// Multiple records
const docs = await embeddings.getMany(['doc1', 'doc2'])

// Query with metadata filter (no vector search)
const news = await embeddings.find({
  where: { category: 'news' },
  limit: 100,
  includeVector: false  // Exclude vectors from results (default: false)
})

// Count and streaming
const count = await embeddings.count()
for await (const { id, data } of embeddings.read()) {
  console.log(id, data)
}
```

**TTL and Maintenance:**
```typescript
// TTL support (pass ttlField in vectorCollection options)
await embeddings.deleteExpired()

// Compaction and vacuum (same as Collection)
await embeddings.compact()
await embeddings.vacuum()
```

**Similarity Metrics:**

| Metric | Range | Best For | Sorting |
|--------|-------|----------|---------|
| `cosine` | -1 to 1 | Normalized embeddings (OpenAI, Cohere) | Descending |
| `euclidean` | 0 to ∞ | Raw feature vectors | Ascending |
| `dotProduct` | -∞ to ∞ | When magnitude matters | Descending |

## Storage Drivers

### FileSystem
```typescript
import { FileSystemDriver } from 'coldbase'
const driver = new FileSystemDriver('./data')
```

### AWS S3
```typescript
import { S3Driver } from 'coldbase'
const driver = new S3Driver('my-bucket', 'us-east-1')
```

### Azure Blob
```typescript
import { AzureBlobDriver } from 'coldbase'
const driver = new AzureBlobDriver(connectionString, 'my-container')
```

## Error Handling

```typescript
import {
  MiniDbError,
  PreconditionFailedError,
  LockActiveError,
  SizeLimitError,
  VectorDimensionError,
  InvalidVectorError
} from 'coldbase'

try {
  await db.compact('users')
} catch (e) {
  if (e instanceof LockActiveError) {
    console.log('Another process is compacting')
  } else if (e instanceof SizeLimitError) {
    console.log('Mutation too large')
  }
}

// Vector-specific errors
try {
  await embeddings.put({ id: 'doc1', vector: [1, 2] })  // Wrong dimension
} catch (e) {
  if (e instanceof VectorDimensionError) {
    console.log('Vector dimension mismatch')
  } else if (e instanceof InvalidVectorError) {
    console.log('Invalid vector data (e.g., NaN, non-number)')
  }
}
```

## Utilities

```typescript
import { retry, parallelLimit, chunk, streamToString, streamLines, streamJsonLines } from 'coldbase'

// Retry with exponential backoff
const result = await retry(
  () => fetchData(),
  { maxAttempts: 5, baseDelayMs: 100, maxDelayMs: 5000 }
)

// Process items in parallel with concurrency limit
const results = await parallelLimit(items, 5, async (item) => {
  return processItem(item)
})

// Split array into chunks
const batches = chunk(items, 100)

// Stream lines from a readable stream
for await (const { line, lineNum } of streamLines(stream)) {
  console.log(`Line ${lineNum}: ${line}`)
}

// Stream and parse NDJSON (newline-delimited JSON)
for await (const record of streamJsonLines<MyType>(stream)) {
  console.log(record)
}
```

### Vector Utilities

```typescript
import {
  cosineSimilarity,
  euclideanDistance,
  dotProduct,
  normalizeVector,
  validateVector
} from 'coldbase'

// Cosine similarity (-1 to 1)
const similarity = cosineSimilarity([1, 0, 0], [0.9, 0.1, 0])  // ~0.99

// Euclidean distance (>= 0)
const distance = euclideanDistance([0, 0], [3, 4])  // 5

// Dot product
const dot = dotProduct([1, 2, 3], [4, 5, 6])  // 32

// Normalize to unit length
const unit = normalizeVector([3, 4])  // [0.6, 0.8]

// Validate vector (throws on invalid)
validateVector([1, 2, 3], 3)  // OK
validateVector([1, 2], 3)     // Throws VectorDimensionError
validateVector([1, NaN], 2)   // Throws InvalidVectorError
```

## Architecture

### Storage Layout

```
users.jsonl              # Compacted user records (NDJSON)
users.lock               # Distributed lock file (lease-based)
users.idx                # Index file for fast lookups (optional)
users.bloom              # Bloom filter for "not exists" checks (optional)
users.mutation.ts-uuid1  # Pending mutations (timestamp prefixed)
users.mutation.ts-uuid2
embeddings.jsonl         # Vector collection (same NDJSON format)
embeddings.mutation.*    # Vector mutations
```

Vector collections use the same storage format as regular collections:
```json
["doc1", {"id":"doc1","vector":[0.1,0.2,...],"text":"Hello"}, 1706400000000]
```

### Write Path

1. `collection.put()` writes `[collection].mutation.[timestamp]-[uuid]`
2. Returns immediately (mutation is durable)
3. If `autoCompact` enabled, may trigger background compaction:
   - `true`: Always trigger (legacy)
   - `{ probability, mutationThreshold }`: Probabilistic trigger
4. If `autoVacuum` enabled, may trigger background vacuum similarly

### Read Path

1. **Bloom filter check** (if enabled): Return `undefined` immediately if ID definitely doesn't exist
2. **Index lookup** (if enabled and no pending mutations): Direct byte-offset read for O(1) lookup
3. **Full scan** (fallback): Stream main `.jsonl` file, then pending mutations
4. Return latest value for requested ID(s)
5. Filter expired records (if TTL defined)

### Compaction Path

1. Acquire distributed lock (lease-based, 30s default - no background heartbeat needed)
2. List and read mutation files in parallel
3. Append to main `.jsonl` file
4. Delete processed mutations in chunks
5. Rebuild index and bloom filter (if enabled)
6. Release lock

### Vacuum Path (Single-Pass with LRU Cache)

1. **Pass 1**: Stream file, track last occurrence of each ID in LRU cache (bounded memory)
   - IDs that overflow the cache are added to an "overflow" set
2. **Pass 2**: Write surviving records to temp file:
   - For tracked IDs: only keep the last occurrence (if not deleted)
   - For overflow IDs: keep all non-deleted records
3. **Swap**: Replace main file, rebuild index and bloom filter

## Performance Tips

1. **Enable bloom filter** - Fast rejection of non-existent keys without scanning
   ```typescript
   const db = new Db(driver, { useBloomFilter: true })
   ```
2. **Enable index** - O(1) lookups when no pending mutations
   ```typescript
   const db = new Db(driver, { useIndex: true })
   ```
3. **Use `batch()` for writes** - Coalesces multiple writes into single mutation file
   ```typescript
   await collection.batch(tx => { tx.put({ id: '1', ... }); tx.put({ id: '2', ... }); })
   ```
4. **Run compaction frequently** - More mutations = slower reads, stale index
5. **Use `getMany()`** - Single scan for multiple IDs
6. **Set appropriate TTLs** - Auto-expire old data
7. **Tune `vacuumCacheSize`** - Larger cache = better deduplication during vacuum
8. **Vector search with filters** - Apply metadata filters to reduce comparisons
   ```typescript
   await embeddings.search(query, { filter: { category: 'news' } })
   ```

## Limitations

- **Read Performance**: Falls back to full scan when mutations are pending or index disabled
- **Eventual Consistency**: Data is durable immediately but appears in main file after compaction
- **Memory**: Vacuum uses LRU cache (default 100k IDs); overflow IDs aren't fully deduplicated
- **Saga-Based Transactions**: Cross-collection transactions use best-effort compensation (not true ACID); compensation failures are reported but cannot guarantee rollback
- **Bloom Filter False Positives**: ~1% false positive rate by default (configurable)
- **Vector Search**: Uses brute-force search (O(n)); suitable for 10k-100k vectors, not millions
