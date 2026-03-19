# A LSM Tree Database - from scratch
![](./assets/wal.webp)
## What is LSM or Log Structured Merge tree database?
A Log-Structured Merge Tree is a write-optimized data structure commonly used in modern databases such as Apache Casandra, LevelDB.

### Layers
- **Write-ahead Logger (WAL)**: Every write operation is appended sequentially to disk. If the system crashes, the database state can be rebuilt from the log.
- **Memtable**: In memory data structure that caches the KV pairs in a sorted order. 
- **Sorted String Table (SSTable)**: Once memtable reaches it's defined limit of some size it is flushed to SSTable which is a On-disk data structure.

- **Bloom Filters**: To ensure low latency search for KV pairs without reading all the file content, we implement a probabilistic data structure known as Bloom Filter.

## Flow (current)
```
Client Write
     ↓
Append to WAL (durable)
     ↓
Insert into MemTable (fast access)
     ↓
MemTable reaches threshold
     ↓
Flush to SSTable (immutable on disk)
```

## Bloom Sidecar Files
- Every SSTable flush now also writes `sstable_<n>.bloom`.
- This is a simple bloom filter (`BitSet`) used only for quick key-presence checks.
- During `get`, the bloom file is checked first and SSTable file scan is skipped when the key is definitely absent.
