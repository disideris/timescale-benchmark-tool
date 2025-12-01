# Design Document

## Architecture

Streaming, concurrent design where N workers share M database connections (N >> M).

```
CSV Reader → Hasher → Workers → Shared Connection Pool → PostgreSQL
                ↓
           Stats Collector → Results
```

## Key Design Choices

### Streaming Processing
Read CSV line-by-line, dispatch queries immediately. Handles any file size.

### Connection Pooling
Workers share connections: acquire before query (not timed), execute (timed), release. This lets X workers share Y connections while respecting Postgres limits.

### Consistent Hashing
Same hostname → same worker. Improves database cache hits.

### Global Stats Collector
One atomic collector instead of per-worker stats. Tested both approaches; global was faster with less memory.

### Backpressure
CSV reader blocks when queues are full. Ensures all queries are processed without dropping any.

## Also experimented with:

**Batch processing**: Read CSV in chunks, assign batches to workers
- Added complexity, no significat benefit
- **Decision**: Stream line-by-line

**Per-worker stats collectors**: Each worker tracks stats, merge at end
- Tested and benchmarked slower than global collector

## Failure Handling

Failures don't abort the benchmark. Retry transient errors (deadlocks, network issues), skip bad CSV lines, report failed query counts in final stats.
