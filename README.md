# Coldbase

A lightweight, serverless-first write-ahead log (WAL) database for cloud storage. Supports AWS S3, Azure Blob Storage, and local filesystem. Designed for stateless environments where each operation reads directly from storage.

**5-40x cheaper than DynamoDB or Cosmos DB.** By leveraging S3/Azure Blob's low-cost API pricing ($0.40 per million reads vs $250 for DynamoDB), Coldbase dramatically reduces database costs for serverless applications. A medium-traffic app costs ~$30/month vs $500+ with traditional serverless databases. [See full comparison →](./COMPARISON.md)

## Table of Contents

- [Examples](#examples)
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [HTTP API](#http-api)
- [Logging](#logging)
- [Serverless Usage](#serverless-usage)
- [API Reference](#api-reference)
  - [Db](#db)
  - [Collection](#collectiont)
  - [Transactions](#transactions)
  - [VectorCollection](#vectorcollectiont)
- [Storage Drivers](#storage-drivers)
- [Error Handling](#error-handling)
- [Architecture](#architecture)
- [Performance Tips](#performance-tips)
- [Limitations](#limitations)
- [Adaptive Lease Duration](#adaptive-lease-duration)

## Examples

### Simple Database (Todo App)

```typescript
import { Db, FileSystemDriver } from 'coldbase'

interface Todo {
  id: string
  title: string
  completed: boolean
  createdAt: number
}

const db = new Db(new FileSystemDriver('./data'))
const todos = db.collection<Todo>('todos')

// Create
await todos.put({
  id: crypto.randomUUID(),
  title: 'Learn Coldbase',
  completed: false,
  createdAt: Date.now()
})

// List incomplete todos
const pending = await todos.find({
  where: { completed: false }
})

// Mark as done
await todos.put({ ...pending[0], completed: true })

// Delete
await todos.delete(pending[0].id)
```

### Vector Database (Semantic Search)

```typescript
import { Db, FileSystemDriver } from 'coldbase'

interface Document {
  id: string
  vector: number[]
  title: string
  content: string
}

const db = new Db(new FileSystemDriver('./data'))
const docs = db.vectorCollection<Document>('documents', {
  dimension: 384,  // e.g., all-MiniLM-L6-v2 embeddings
  metric: 'cosine'
})

// Index documents with embeddings
await docs.put({
  id: 'doc1',
  vector: await embed('Introduction to machine learning'),
  title: 'ML Basics',
  content: 'Machine learning is a subset of AI...'
})

await docs.put({
  id: 'doc2',
  vector: await embed('Deep neural networks explained'),
  title: 'Deep Learning',
  content: 'Neural networks with multiple layers...'
})

// Semantic search
const query = await embed('How do neural networks work?')
const results = await docs.search(query, { limit: 5 })

for (const { id, score, data } of results) {
  console.log(`${data.title} (score: ${score.toFixed(3)})`)
}

// Helper: generate embeddings (use your preferred provider)
async function embed(text: string): Promise<number[]> {
  // OpenAI, Cohere, HuggingFace, or local model
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: text
  })
  return response.data[0].embedding
}
```

### HTTP Server (REST API)

```typescript
import { Db, FileSystemDriver } from 'coldbase'
import { createHttpApi } from 'coldbase/http'
import { serve } from '@hono/node-server'

const db = new Db(new FileSystemDriver('./data'))

const app = createHttpApi(db, {
  auth: {
    keys: [
      { key: process.env.API_KEY!, role: 'admin' }
    ]
  },
  maxPageSize: 100,
  maxBodySize: 1024 * 1024  // 1MB
})

// Add custom routes
app.get('/health', (c) => c.json({ status: 'ok' }))

serve({ fetch: app.fetch, port: 3000 })
console.log('Server running at http://localhost:3000')

// API Usage:
// GET    /data/users              - List users
// GET    /data/users/123          - Get user by ID
// PUT    /data/users/123          - Create/update user
// DELETE /data/users/123          - Delete user
// POST   /data/embeddings/search  - Vector search
// GET    /docs                    - Swagger UI
```

## Features

- **Serverless-First**: Stateless operations, no in-memory state between invocations
- **Auto-Maintenance**: Probabilistic compaction and vacuum triggers for serverless
- **Multi-Collection Support**: Single `Db` instance manages multiple collections
- **Vector Collections**: Store and search vector embeddings with cosine, euclidean, or dot product similarity
- **Query API**: `find()` with filtering, pagination, and function predicates
- **TTL Support**: Auto-expire records based on a timestamp field
- **Batch Operations**: `getMany()` for efficient multi-key lookups, `batch()` for coalescing writes
- **Transactions**: Cross-collection consistency via saga pattern with best-effort compensation on failure (not ACID - see Limitations)
- **Parallel Processing**: Configurable parallelism for mutation processing
- **Retry Logic**: Exponential backoff with jitter for transient failures
- **Hooks & Metrics**: Monitor writes, compactions, and errors
- **Size Limits**: Configurable mutation size limits
- **Multiple Storage Backends**: S3, Azure Blob, or local filesystem
- **Performance Optimizations**: Bloom filter, in-memory index, adaptive lease-based locking

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

## HTTP API

Coldbase includes an optional HTTP module built on [Hono](https://hono.dev/) for exposing your database via REST API.

### Installation

```bash
npm install hono @hono/swagger-ui
```

### Quick Start

```typescript
import { Db, FileSystemDriver } from 'coldbase'
import { createHttpApi } from 'coldbase/http'

const db = new Db(new FileSystemDriver('./data'))
const app = createHttpApi(db)

// Serve with your preferred runtime
export default app  // Cloudflare Workers / Bun
// or: serve(app)   // Node.js with @hono/node-server
```

### Routes

| Route | Method | Role | Description |
|-------|--------|------|-------------|
| `/data/:collection` | GET | reader | List/query documents |
| `/data/:collection/:id` | GET | reader | Get document |
| `/data/:collection/:id` | PUT | editor | Create/update document |
| `/data/:collection/:id` | DELETE | editor | Delete document |
| `/data/:collection/search` | POST | reader | Vector similarity search |
| `/data/tx` | POST | editor | Execute transaction |
| `/admin/collections` | GET | admin | List collections |
| `/admin/:collection/stats` | GET | admin | Collection statistics |
| `/admin/:collection/compact` | POST | admin | Trigger compaction |
| `/admin/:collection/vacuum` | POST | admin | Trigger vacuum |
| `/docs` | GET | - | Swagger UI |
| `/docs/openapi.json` | GET | - | OpenAPI spec |

### Authentication

API keys are loaded from environment variables by default:

```bash
COLDBASE_READER_KEY=xxx  # Read-only access
COLDBASE_EDITOR_KEY=xxx  # Read + write access
COLDBASE_ADMIN_KEY=xxx   # Full access including /admin and /docs
```

```typescript
// With explicit keys
const app = createHttpApi(db, {
  auth: {
    keys: [
      { key: 'my-reader-key', role: 'reader' },
      { key: 'my-editor-key', role: 'editor' },
      { key: 'my-admin-key', role: 'admin' }
    ],
    useEnv: false  // Don't load from env
  }
})

// Client usage
fetch('/data/users', {
  headers: { 'Authorization': 'Bearer my-reader-key' }
})
```

If no keys are configured, authentication is disabled (open access).

### Query Parameters

**Basic filtering** - Use any query param as `prop=value` filter:

```
GET /data/users?role=admin&active=true&limit=20&offset=0
```

Supported value types:
- Strings: `?role=admin`
- Booleans: `?active=true` or `?active=false`
- Numbers: `?score=100`

**Reserved params** (not used for filtering):
- `limit`, `offset` - Pagination
- `prefix` - Filter by ID prefix
- `query`, `q` - Custom query string

### Custom Query Function

For advanced queries, provide a custom query handler. It runs **after** basic filters and pagination:

```
Request: GET /data/users?role=admin&q=sort(.name)

Flow:
1. Fetch all docs
2. Apply basic filters (?role=admin)
3. Apply pagination (?limit, ?offset)
4. Apply custom query (?q=sort(.name))
```

```typescript
// Using jsonquery
import { jsonquery } from '@jsonquerylang/jsonquery'

const app = createHttpApi(db, {
  query: (docs, q) => jsonquery(docs, q)
})

// Client: GET /data/users?active=true&q=sort(.name) | pick(.name, .email)
```

```typescript
// Using sift (MongoDB-style queries in q param)
import sift from 'sift'

const app = createHttpApi(db, {
  query: (docs, q) => docs.filter(sift(JSON.parse(q)))
})

// Client: GET /data/users?role=admin&q={"age":{"$gt":25}}
```

### Configurable Limits

```typescript
const app = createHttpApi(db, {
  maxPageSize: 200,        // Max docs per page (default: 100)
  defaultPageSize: 50,     // Default docs per page (default: 20)
  maxBodySize: 5 * 1024 * 1024,  // Max request body (default: 1MB)
  maxTxOperations: 100,    // Max ops per transaction (default: 50)
  maxVectorResults: 200    // Max vector search results (default: 100)
})
```

### Extending the API

Since `createHttpApi` returns a Hono app, extend it naturally:

```typescript
const app = createHttpApi(db)

// Add custom routes
app.get('/api/reports/active-users', async (c) => {
  const users = db.collection('users')
  const active = await users.find({ where: u => u.active })
  return c.json({ count: active.length })
})

// Add middleware
import { cors } from 'hono/cors'
app.use('*', cors())
```

## Logging

Coldbase uses [LogTape](https://github.com/dahlia/logtape) for logging. Configure it to see internal operations:

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

**Log categories:**
- `coldbase.db` - Database operations
- `coldbase.collection` - Collection operations
- `coldbase.compactor` - Compaction and vacuum
- `coldbase.http` - HTTP API requests (method, path, status, duration)
- `coldbase.driver.fs` / `s3` / `azure` - Storage driver operations

**HTTP request logging** is automatic when using the HTTP API. Requests are logged at appropriate levels:
- `info` - Successful requests (2xx, 3xx)
- `warn` - Client errors (4xx)
- `error` - Server errors (5xx)

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

### Handling Maintenance Failures

In serverless environments, failed maintenance can lead to mutation file accumulation over time. Coldbase provides retry logic and alerting hooks to handle this:

```typescript
const db = new Db(new S3Driver('my-bucket', 'us-east-1'), {
  autoCompact: {
    probability: 0.1,
    mutationThreshold: 5,
    maxRetries: 3,        // Retry up to 3 times on failure
    retryDelayMs: 2000    // Start with 2s delay (exponential backoff)
  },
  hooks: {
    // Called when maintenance fails after all retry attempts
    onMaintenanceFailure: (collection, operation, error, attempts) => {
      // Send alert to monitoring system
      console.error(`ALERT: ${operation} failed for ${collection} after ${attempts} attempts:`, error)
      // Example: Send to CloudWatch, Datadog, PagerDuty, etc.
    }
  }
})
```

**Best practices for serverless:**
- Always configure `onMaintenanceFailure` to alert on persistent failures
- Use a separate scheduled function as a fallback for guaranteed maintenance
- Monitor mutation file count via `collection.countMutationFiles()`

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
  leaseDurationMs?: number     // Base lock lease duration (default: 30000)
  adaptiveLease?: boolean      // Auto-calculate lease from file size (default: true)
  leasePerByte?: number        // Ms per byte of file size (default: 0.00003)
  leasePerMutation?: number    // Ms per mutation file (default: 200)
  maxLeaseDurationMs?: number  // Maximum lease cap (default: 600000)

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
  maxRetries?: number          // Retry attempts on failure (default: 2)
  retryDelayMs?: number        // Base delay between retries (default: 1000)
}

interface AutoVacuumOptions extends AutoMaintenanceOptions {
  afterCompactProbability?: number  // Chance to vacuum after compaction
}

interface DbHooks {
  onWrite?: (collection: string, count: number) => void
  onCompact?: (collection: string, durationMs: number, mutationCount: number) => void
  onVacuum?: (collection: string, durationMs: number, removedCount: number) => void
  onError?: (error: Error, operation: string) => void
  // Called when auto-maintenance fails after all retries - use for alerting
  onMaintenanceFailure?: (collection: string, operation: 'compact' | 'vacuum', error: Error, attempts: number) => void
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

// SERVERLESS_AUTO_COMPACT = {
//   probability: 0.1, mutationThreshold: 5, maxRetries: 2, retryDelayMs: 1000
// }
// SERVERLESS_AUTO_VACUUM = {
//   probability: 0.01, mutationThreshold: 0, afterCompactProbability: 0.1,
//   maxRetries: 2, retryDelayMs: 1000
// }
```

**Methods:**
```typescript
db.collection<T>(name: string, options?: CollectionOptions): Collection<T>
db.vectorCollection<T>(name: string, options: VectorCollectionOptions): VectorCollection<T>
db.compact(name: string): Promise<CompactResult>
db.vacuum(name: string): Promise<VacuumResult>
```

**Collection Naming Rules:**

Collection names must match `^[a-zA-Z0-9][a-zA-Z0-9_-]*$` and be at most 64 characters. Names containing `.`, `/`, spaces, or starting with `_` or `-` will throw a `ValidationError`. This prevents collisions with internal storage keys (e.g., `.mutation.`, `.jsonl`, `.lock`).

### `Collection<T>`

**Writing:**
```typescript
// Single item
await collection.put({ id: 'id1', ...fields })

// Delete
await collection.delete('id1')

// Batch writes - ATOMIC within a single collection (all-or-nothing)
// Writes to a single mutation file, so either all succeed or none do
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

Cross-collection consistency using the saga pattern. Each write tracks a compensation action; if any step fails, compensations run in reverse order to undo previous writes.

> **⚠️ Important: Cross-Collection Transactions Are Not ACID**
>
> Coldbase transactions provide **best-effort consistency**, not true ACID guarantees:
> - **No Isolation**: Each write is immediately visible to concurrent readers. Other processes can read partial transaction state.
> - **No Atomicity**: If a transaction fails mid-way, earlier writes are already persisted. Compensation attempts to undo them but may also fail.
> - **Compensation is Best-Effort**: If compensation fails, errors are collected and reported, but the original writes remain.
>
> Use transactions when eventual consistency is acceptable and you need a convenient way to group related writes with automatic rollback attempts.
>
> **For single-collection atomic writes, use `batch()` instead** - it writes all operations to one mutation file, providing true atomicity within that collection.

```typescript
await db.transaction(async (tx) => {
  const users = tx.collection<User>('users')
  const logs = tx.collection<Log>('logs')

  await users.put({ id: '1', name: 'Alice' })  // Immediately visible to other readers!
  await logs.put({ id: 'log-1', action: 'user-created' })
})
// If logs.put fails → compensation attempts to delete users.put
```

The transactional collection supports `put`, `delete`, and `get` (read-only, no tracking). On failure, a `TransactionError` is thrown containing the original error and any compensation errors:

```typescript
import { TransactionError } from 'coldbase'

try {
  await db.transaction(async (tx) => { /* ... */ })
} catch (e) {
  if (e instanceof TransactionError) {
    console.log(e.originalError)        // The error that caused the rollback
    console.log(e.compensationErrors)   // Any errors during compensation (may be non-empty!)
  }
}
```

**Nested Transactions (Savepoints):**

Transactions can be nested via `tx.transaction()`. A nested transaction acts as a savepoint — if it fails, only its own writes are compensated while the outer transaction continues. If it succeeds, its compensations are promoted to the parent so they run if a later outer step fails.

```typescript
await db.transaction(async (tx) => {
  const users = tx.collection<User>('users')
  await users.put({ id: '1', name: 'Alice' })

  // Nested transaction: if this fails, only inner writes are compensated
  try {
    await tx.transaction(async (inner) => {
      const logs = inner.collection<Log>('logs')
      await logs.put({ id: 'log-1', action: 'user-created' })
      throw new Error('inner failure')
    })
  } catch {
    // Inner compensated, outer continues — users.put still committed
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

// With custom endpoint (e.g., MinIO, LocalStack, or S3-compatible storage)
const customDriver = new S3Driver('my-bucket', 'us-east-1', {
  endpoint: 'http://localhost:9000',
  credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' },
  forcePathStyle: true
})
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
  ValidationError,
  SizeLimitError,
  VectorDimensionError,
  InvalidVectorError,
  TransactionError
} from 'coldbase'

try {
  await db.compact('users')
} catch (e) {
  if (e instanceof LockActiveError) {
    console.log('Another process is compacting')
  } else if (e instanceof ValidationError) {
    console.log('Invalid collection name')
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

1. Calculate adaptive lease duration based on file size and mutation count
2. Acquire distributed lock (lease-based, no background heartbeat needed)
3. List and read mutation files in parallel
4. Append to main `.jsonl` file
5. Delete processed mutations in chunks
6. Rebuild index and bloom filter (if enabled)
7. Release lock

### Vacuum Path (Single-Pass with LRU Cache)

1. Calculate adaptive lease duration (2× compaction estimate for two-pass algorithm)
2. Acquire distributed lock
3. **Pass 1**: Stream file, track last occurrence of each ID in LRU cache (bounded memory)
   - IDs that overflow the cache are added to an "overflow" set
4. **Pass 2**: Write surviving records to temp file:
   - For tracked IDs: only keep the last occurrence (if not deleted)
   - For overflow IDs: keep all non-deleted records
5. **Swap**: Replace main file, rebuild index and bloom filter
6. Release lock

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
- **Cross-Collection Transactions Are Not ACID**:
  - **No Isolation**: Writes are immediately visible to concurrent readers mid-transaction
  - **No Atomicity**: Failed transactions leave earlier writes persisted; compensation is best-effort
  - **No Phantom Protection**: Concurrent transactions may see inconsistent reads
  - Use only when eventual consistency is acceptable
- **Single-Collection Atomicity**: `batch()` within one collection IS atomic (all-or-nothing) since it writes to a single mutation file. Use `batch()` when you need atomic writes within a collection.
- **Bloom Filter False Positives**: ~1% false positive rate by default (configurable)
- **Vector Search**: Uses brute-force search (O(n)); suitable for 10k-100k vectors, not millions
- **Lease Duration**: Adaptive lease is enabled by default to handle varying file sizes. For extremely large files (>1GB) or very slow storage, you may need to tune `leasePerByte` or increase `maxLeaseDurationMs`.

## Adaptive Lease Duration

By default, Coldbase automatically calculates lock lease duration based on file size and mutation count. This prevents lease expiration during large operations without requiring manual tuning.

**How it works:**
```
leaseDuration = baseLease + (fileSize × leasePerByte) + (mutationCount × leasePerMutation)
```

**Default values:**
- `leaseDurationMs`: 30,000ms (30s base)
- `leasePerByte`: 0.00003ms (~30ms per MB)
- `leasePerMutation`: 200ms per mutation file
- `maxLeaseDurationMs`: 600,000ms (10 minute cap)

**Example calculations:**
- Empty collection: 30s (base only)
- 10MB file + 5 mutations: 30s + 300ms + 1s = ~31.3s
- 100MB file + 50 mutations: 30s + 3s + 10s = ~43s
- 1GB file + 100 mutations: 30s + 30s + 20s = ~80s

**Customizing adaptive lease:**
```typescript
const db = new Db(driver, {
  // Disable adaptive lease (use fixed duration)
  adaptiveLease: false,
  leaseDurationMs: 60000,

  // Or customize the adaptive parameters
  adaptiveLease: true,
  leaseDurationMs: 30000,      // Base lease
  leasePerByte: 0.00005,       // More time per byte for slow storage
  leasePerMutation: 300,       // More time per mutation
  maxLeaseDurationMs: 300000   // 5 minute cap
})
```

**Monitoring lease usage:**
```typescript
const db = new Db(driver, {
  hooks: {
    onCompact: (collection, durationMs) => {
      // Log actual duration vs estimated lease
      console.log(`Compaction took ${durationMs}ms`)
    }
  }
})
```

The adaptive lease is calculated **before** acquiring the lock, so the estimation adds minimal overhead.
