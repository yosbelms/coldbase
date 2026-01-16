import { Db, Collection } from '../lib/db'
import { FileSystemDriver } from '../lib/drivers/fs'
import * as fs from 'fs'
import * as path from 'path'

interface TestRecord {
  id: string
  name: string
  value: number
  timestamp: number
}

describe('Load Tests', () => {
  const testDir = path.join(__dirname, '../.test-load-data')
  let driver: FileSystemDriver
  let db: Db

  beforeAll(() => {
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true })
    }
    fs.mkdirSync(testDir, { recursive: true })
  })

  beforeEach(() => {
    // Clean up before each test
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true })
    }
    fs.mkdirSync(testDir, { recursive: true })
    driver = new FileSystemDriver(testDir)
    db = new Db(driver)
  })

  afterAll(() => {
    if (fs.existsSync(testDir)) {
      fs.rmSync(testDir, { recursive: true })
    }
  })

  describe('Write Performance', () => {
    test('sequential writes - 1000 records', async () => {
      const collection = db.collection<TestRecord>('write-seq')
      const count = 1000

      const start = performance.now()
      for (let i = 0; i < count; i++) {
        await collection.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
      }
      const duration = performance.now() - start

      console.log(`Sequential writes (${count} records): ${duration.toFixed(2)}ms (${(count / duration * 1000).toFixed(0)} ops/sec)`)
      expect(await collection.count()).toBe(count)
    })

    test('batch writes - 1000 records in batches of 100', async () => {
      const collection = db.collection<TestRecord>('write-batch')
      const count = 1000
      const batchSize = 100

      const start = performance.now()
      for (let batch = 0; batch < count / batchSize; batch++) {
        await collection.batch(tx => {
          for (let i = 0; i < batchSize; i++) {
            const idx = batch * batchSize + i
            tx.put(`record-${idx}`, { id: `record-${idx}`, name: `Name ${idx}`, value: idx, timestamp: Date.now() })
          }
        })
      }
      const duration = performance.now() - start

      console.log(`Batch writes (${count} records, batch=${batchSize}): ${duration.toFixed(2)}ms (${(count / duration * 1000).toFixed(0)} ops/sec)`)
      expect(await collection.count()).toBe(count)
    })

    test('large batch write - 1000 records in single batch', async () => {
      const collection = db.collection<TestRecord>('write-large-batch')
      const count = 1000

      const start = performance.now()
      await collection.batch(tx => {
        for (let i = 0; i < count; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      const duration = performance.now() - start

      console.log(`Large batch write (${count} records): ${duration.toFixed(2)}ms (${(count / duration * 1000).toFixed(0)} ops/sec)`)
      expect(await collection.count()).toBe(count)
    })
  })

  describe('Read Performance', () => {
    const recordCount = 500

    test('get() without index - random lookups', async () => {
      const collection = db.collection<TestRecord>('read-no-index')

      // Pre-populate
      await collection.batch(tx => {
        for (let i = 0; i < recordCount; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      await collection.compact()

      const lookups = 50
      const start = performance.now()
      for (let i = 0; i < lookups; i++) {
        const id = `record-${Math.floor(Math.random() * recordCount)}`
        await collection.get(id)
      }
      const duration = performance.now() - start

      console.log(`Random get() without index (${lookups} lookups): ${duration.toFixed(2)}ms (${(lookups / duration * 1000).toFixed(0)} ops/sec)`)
    })

    test('get() with index enabled - random lookups', async () => {
      const indexedDb = new Db(driver, { useIndex: true })
      const collection = indexedDb.collection<TestRecord>('read-with-index')

      // Pre-populate
      await collection.batch(tx => {
        for (let i = 0; i < recordCount; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      await collection.compact()

      const lookups = 50
      const start = performance.now()
      for (let i = 0; i < lookups; i++) {
        const id = `record-${Math.floor(Math.random() * recordCount)}`
        await collection.get(id)
      }
      const duration = performance.now() - start

      console.log(`Random get() with index (${lookups} lookups): ${duration.toFixed(2)}ms (${(lookups / duration * 1000).toFixed(0)} ops/sec)`)
    })

    test('get() with bloom filter - non-existent keys', async () => {
      const bloomDb = new Db(driver, { useBloomFilter: true })
      const collection = bloomDb.collection<TestRecord>('read-bloom')

      // Pre-populate
      await collection.batch(tx => {
        for (let i = 0; i < recordCount; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      await collection.compact()

      const lookups = 100
      const start = performance.now()
      for (let i = 0; i < lookups; i++) {
        const id = `nonexistent-${i}`
        await collection.get(id)
      }
      const duration = performance.now() - start

      console.log(`Bloom filter miss (${lookups} lookups): ${duration.toFixed(2)}ms (${(lookups / duration * 1000).toFixed(0)} ops/sec)`)
    })

    test('getMany() - batch lookups', async () => {
      const collection = db.collection<TestRecord>('read-getmany')

      // Pre-populate
      await collection.batch(tx => {
        for (let i = 0; i < recordCount; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      await collection.compact()

      const batchSize = 50
      const ids = Array.from({ length: batchSize }, (_, i) => `record-${i}`)

      const start = performance.now()
      const results = await collection.getMany(ids)
      const duration = performance.now() - start

      console.log(`getMany() (${batchSize} IDs): ${duration.toFixed(2)}ms`)
      expect(results.size).toBe(batchSize)
    })

    test('find() with filter - full scan', async () => {
      const collection = db.collection<TestRecord>('read-find-filter')

      // Pre-populate
      await collection.batch(tx => {
        for (let i = 0; i < recordCount; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      await collection.compact()

      const start = performance.now()
      const results = await collection.find({ where: { value: 42 } })
      const duration = performance.now() - start

      console.log(`find() with filter: ${duration.toFixed(2)}ms`)
      expect(results.length).toBe(1)
    })

    test('find() with function predicate', async () => {
      const collection = db.collection<TestRecord>('read-find-predicate')

      // Pre-populate
      await collection.batch(tx => {
        for (let i = 0; i < recordCount; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      await collection.compact()

      const start = performance.now()
      const results = await collection.find({ where: r => r.value > 400 })
      const duration = performance.now() - start

      console.log(`find() with predicate (${results.length} results): ${duration.toFixed(2)}ms`)
      expect(results.length).toBe(99) // 401-499
    })

    test('count() performance', async () => {
      const collection = db.collection<TestRecord>('read-count')

      // Pre-populate
      await collection.batch(tx => {
        for (let i = 0; i < recordCount; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      await collection.compact()

      const start = performance.now()
      const count = await collection.count()
      const duration = performance.now() - start

      console.log(`count(): ${duration.toFixed(2)}ms`)
      expect(count).toBe(recordCount)
    })
  })

  describe('Compaction Performance', () => {
    test('compact 100 mutation files', async () => {
      const collection = db.collection<TestRecord>('compact-perf')

      // Create 100 mutation files
      for (let i = 0; i < 100; i++) {
        await collection.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
      }

      const mutationCount = await collection.countMutationFiles()
      expect(mutationCount).toBe(100)

      const start = performance.now()
      await collection.compact()
      const duration = performance.now() - start

      console.log(`Compact 100 mutation files: ${duration.toFixed(2)}ms`)
      expect(await collection.countMutationFiles()).toBe(0)
    })

    test('compact with index and bloom filter rebuild', async () => {
      const optimizedDb = new Db(driver, { useIndex: true, useBloomFilter: true })
      const collection = optimizedDb.collection<TestRecord>('compact-optimized')

      // Create records
      await collection.batch(tx => {
        for (let i = 0; i < 500; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })

      const start = performance.now()
      await collection.compact()
      const duration = performance.now() - start

      console.log(`Compact with index+bloom rebuild (500 records): ${duration.toFixed(2)}ms`)

      // Verify index and bloom filter exist
      expect(fs.existsSync(path.join(testDir, 'compact-optimized.idx'))).toBe(true)
      expect(fs.existsSync(path.join(testDir, 'compact-optimized.bloom'))).toBe(true)
    })
  })

  describe('Vacuum Performance', () => {
    test('vacuum with duplicates', async () => {
      const collection = db.collection<TestRecord>('vacuum-perf')

      // Create records with updates (duplicates)
      for (let round = 0; round < 5; round++) {
        await collection.batch(tx => {
          for (let i = 0; i < 100; i++) {
            tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i} v${round}`, value: i + round, timestamp: Date.now() })
          }
        })
      }

      await collection.compact()

      const start = performance.now()
      await collection.vacuum()
      const duration = performance.now() - start

      console.log(`Vacuum 500 records (100 unique): ${duration.toFixed(2)}ms`)
      expect(await collection.count()).toBe(100)
    })

    test('vacuum with deletions', async () => {
      const collection = db.collection<TestRecord>('vacuum-delete')

      // Create records
      await collection.batch(tx => {
        for (let i = 0; i < 200; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })

      // Delete half
      await collection.batch(tx => {
        for (let i = 0; i < 100; i++) {
          tx.put(`record-${i}`, null)
        }
      })

      await collection.compact()

      const start = performance.now()
      await collection.vacuum()
      const duration = performance.now() - start

      console.log(`Vacuum with 100 deletions: ${duration.toFixed(2)}ms`)
      expect(await collection.count()).toBe(100)
    })
  })

  describe('Concurrent Access', () => {
    test('parallel writes to different records', async () => {
      const collection = db.collection<TestRecord>('concurrent-write')
      const parallelWrites = 20

      const start = performance.now()
      await Promise.all(
        Array.from({ length: parallelWrites }, (_, i) =>
          collection.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        )
      )
      const duration = performance.now() - start

      console.log(`Parallel writes (${parallelWrites}): ${duration.toFixed(2)}ms`)
      expect(await collection.count()).toBe(parallelWrites)
    })

    test('parallel reads', async () => {
      const collection = db.collection<TestRecord>('concurrent-read')

      // Setup data
      await collection.batch(tx => {
        for (let i = 0; i < 100; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      await collection.compact()

      const parallelReads = 50

      const start = performance.now()
      const results = await Promise.all(
        Array.from({ length: parallelReads }, (_, i) =>
          collection.get(`record-${i % 100}`)
        )
      )
      const duration = performance.now() - start

      console.log(`Parallel reads (${parallelReads}): ${duration.toFixed(2)}ms`)
      expect(results.filter(r => r !== undefined).length).toBe(parallelReads)
    })

    test('mixed read/write workload', async () => {
      const collection = db.collection<TestRecord>('concurrent-mixed')

      // Pre-populate
      await collection.batch(tx => {
        for (let i = 0; i < 50; i++) {
          tx.put(`record-${i}`, { id: `record-${i}`, name: `Name ${i}`, value: i, timestamp: Date.now() })
        }
      })
      await collection.compact()

      const operations = 100

      const start = performance.now()
      await Promise.all(
        Array.from({ length: operations }, (_, i) => {
          if (i % 3 === 0) {
            // Write
            return collection.put(`new-record-${i}`, { id: `new-record-${i}`, name: `New ${i}`, value: i, timestamp: Date.now() })
          } else {
            // Read
            return collection.get(`record-${i % 50}`)
          }
        })
      )
      const duration = performance.now() - start

      console.log(`Mixed workload (${operations} ops, 1:2 write:read): ${duration.toFixed(2)}ms`)
    })
  })

  describe('Large Data', () => {
    test('large record payload', async () => {
      const collection = db.collection<TestRecord & { data: string }>('large-payload')
      const largeData = 'x'.repeat(100000) // 100KB payload

      const start = performance.now()
      await collection.put('large-record', { id: 'large-record', name: 'Large', value: 1, timestamp: Date.now(), data: largeData })
      const writeTime = performance.now() - start

      await collection.compact()

      const readStart = performance.now()
      const record = await collection.get('large-record')
      const readTime = performance.now() - readStart

      console.log(`Large payload (100KB) - Write: ${writeTime.toFixed(2)}ms, Read: ${readTime.toFixed(2)}ms`)
      expect(record?.data).toBe(largeData)
    })

    test('many small records stress test', async () => {
      const collection = db.collection<TestRecord>('stress-small')
      const count = 2000

      const writeStart = performance.now()
      // Write in batches of 200
      for (let batch = 0; batch < count / 200; batch++) {
        await collection.batch(tx => {
          for (let i = 0; i < 200; i++) {
            const idx = batch * 200 + i
            tx.put(`r-${idx}`, { id: `r-${idx}`, name: `N${idx}`, value: idx, timestamp: Date.now() })
          }
        })
      }
      const writeTime = performance.now() - writeStart

      const compactStart = performance.now()
      await collection.compact()
      const compactTime = performance.now() - compactStart

      const readStart = performance.now()
      const count1 = await collection.count()
      const readTime = performance.now() - readStart

      console.log(`Stress test (${count} records):`)
      console.log(`  Write: ${writeTime.toFixed(2)}ms (${(count / writeTime * 1000).toFixed(0)} ops/sec)`)
      console.log(`  Compact: ${compactTime.toFixed(2)}ms`)
      console.log(`  Count: ${readTime.toFixed(2)}ms`)

      expect(count1).toBe(count)
    })
  })
})
