# Coldbase

A lightweight, serverless-first write-ahead log (WAL) database for cloud storage. Supports AWS S3, Azure Blob Storage, and local filesystem. Designed for stateless environments where each operation reads directly from storage.

## Features

- **Serverless-First**: Stateless operations, no in-memory state between invocations
- **Auto-Maintenance**: Probabilistic compaction and vacuum triggers for serverless
- **Multi-Collection Support**: Single `Db` instance manages multiple collections
- **Query API**: `find()` with filtering, pagination, and function predicates
- **TTL Support**: Auto-expire records based on a timestamp field
- **Batch Operations**: `getMany()` for efficient multi-key lookups, `batch()` for coalescing writes
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
await users.put('u1', { name: 'Alice', email: 'alice@example.com', role: 'admin' })

// Read single
const user = await users.get('u1')

// Query
const admins = await users.find({ where: { role: 'admin' } })

// Delete
await users.put('u1', null)
```

## Logging

Mini-DB uses [LogTape](https://github.com/doodadjs/logtape) for logging. You can configure it to see internal operations:

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

Mini-DB is designed for serverless environments where functions may cold start frequently. It offers probabilistic auto-maintenance that distributes load across invocations:

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
  await users.put(event.userId, { ...user, lastSeen: Date.now() })

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
db.collection<T>(name: string): Collection<T>
db.compact(name: string): Promise<CompactResult>
db.vacuum(name: string): Promise<VacuumResult>
```

### `Collection<T>`

**Writing:**
```typescript
// Single item
await collection.put('id1', { ...fields })

// Delete
await collection.put('id1', null)

// Batch writes (coalesces into single mutation file for better performance)
await collection.batch(tx => {
  tx.put('id1', { name: 'Alice' })
  tx.put('id2', { name: 'Bob' })
  tx.put('id3', null)  // Delete
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

const sessions = db.collection<Session>('sessions')
sessions.defineTTL('expiresAt')

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
  SizeLimitError
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

## Architecture

### Storage Layout

```
users.jsonl           # Compacted user records (NDJSON)
users.lock            # Distributed lock file (lease-based)
users.idx             # Index file for fast lookups (optional)
users.bloom           # Bloom filter for "not exists" checks (optional)
posts.jsonl           # Compacted post records
users.mutation.ts-uuid1  # Pending mutations (timestamp prefixed)
users.mutation.ts-uuid2
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
   await collection.batch(tx => { tx.put(...); tx.put(...); })
   ```
4. **Run compaction frequently** - More mutations = slower reads, stale index
5. **Use `getMany()`** - Single scan for multiple IDs
6. **Set appropriate TTLs** - Auto-expire old data
7. **Tune `vacuumCacheSize`** - Larger cache = better deduplication during vacuum

## Limitations

- **Read Performance**: Falls back to full scan when mutations are pending or index disabled
- **Eventual Consistency**: Data is durable immediately but appears in main file after compaction
- **Memory**: Vacuum uses LRU cache (default 100k IDs); overflow IDs aren't fully deduplicated
- **No Transactions**: Operations are atomic per collection, not across collections
- **Bloom Filter False Positives**: ~1% false positive rate by default (configurable)
