---
source: src/compute-types/src/dyncfgs.rs
revision: 82e054569f
---

# compute-types::dyncfgs

Declares all dynamic configuration (`dyncfg`) constants for the compute layer. Key groups include:

- **Rendering**: `ENABLE_HALF_JOIN2`, `ENABLE_ERROR_DISTINCT`, `ENABLE_MZ_JOIN_CORE`, `LINEAR_JOIN_YIELDING`, `ENABLE_COMPUTE_TEMPORAL_BUCKETING`/`TEMPORAL_BUCKETING_SUMMARY`, `COMPUTE_FLAT_MAP_FUEL`, `ENABLE_COMPUTE_RENDER_FUELED_AS_SPECIFIC_COLLECTION`, `COMPUTE_APPLY_COLUMN_DEMANDS`, `ENABLE_COLUMN_PAGED_BATCHER` (replica-scoped), `ENABLE_COLUMNAR_MERGE_BATCHER` (replica-scoped), `ENABLE_COLUMN_PAGED_BATCHER_SPILL` (replica-scoped), `COLUMN_PAGED_BATCHER_BUDGET_FRACTION` (replica-scoped), `COLUMN_PAGED_BATCHER_SPILL_WORKER_COUNT` (replica-scoped), `COLUMN_PAGED_BATCHER_LZ4` (replica-scoped), `COLUMN_PAGED_BATCHER_SWAP_PAGEOUT` (replica-scoped), `COLUMN_PAGED_BATCHER_EAGER_BACKING` (replica-scoped), `COLUMN_PAGED_BATCHER_POOL_RSS_TARGET_FRACTION` (replica-scoped), `COLUMN_CHUNK_COMPRESS_MIN_DEPTH` (replica-scoped, the youngest chunk generation whose spilled bodies are lz4-compressed; default 1, meaning depth-0 chunks are stored uncompressed).
- **MV sink**: `ENABLE_SYNC_MV_SINK`, `ENABLE_CORRECTION_V2`, `CONSOLIDATING_VEC_GROWTH_DAMPENER`, `CORRECTION_V2_CHAIN_PROPORTIONALITY`, `CORRECTION_V2_CHUNK_SIZE`, `MV_SINK_ADVANCE_PERSIST_FRONTIERS`.
- **Memory management**: `ENABLE_LGALLOC` (replica-scoped), `ENABLE_LGALLOC_EAGER_RECLAMATION`, `LGALLOC_BACKGROUND_INTERVAL`, `LGALLOC_FILE_GROWTH_DAMPENER`, `LGALLOC_LOCAL_BUFFER_BYTES`, `LGALLOC_SLOW_CLEAR_BYTES`, `ENABLE_COLUMNATION_LGALLOC`, `MEMORY_LIMITER_INTERVAL`, `MEMORY_LIMITER_USAGE_BIAS`, `MEMORY_LIMITER_BURST_FACTOR`.
- **Backpressure**: `DATAFLOW_MAX_INFLIGHT_BYTES`, `DATAFLOW_MAX_INFLIGHT_BYTES_CC`, `ENABLE_COMPUTE_LOGICAL_BACKPRESSURE`, `COMPUTE_LOGICAL_BACKPRESSURE_MAX_RETAINED_CAPABILITIES`, `COMPUTE_LOGICAL_BACKPRESSURE_INFLIGHT_SLACK`.
- **Peek stash**: `ENABLE_PEEK_RESPONSE_STASH`, `PEEK_RESPONSE_STASH_THRESHOLD_BYTES`, `PEEK_RESPONSE_STASH_BATCH_BYTES` (replica-scoped, default 1 MiB; the size at which an in-progress stash upload hands accumulated rows to the batch builder after the first threshold batch), `PEEK_RESPONSE_STASH_BATCH_MAX_RUNS`, `PEEK_RESPONSE_STASH_READ_BATCH_SIZE_BYTES`, `PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES`. The former `PEEK_STASH_NUM_BATCHES` and `PEEK_STASH_BATCH_SIZE` constants have been removed.
- **Peek row iteration limit**: `ENABLE_PEEK_ROW_ITERATION_LIMIT` (environment-scoped, default false) gates the feature; `PEEK_ROW_ITERATION_LIMIT` (environment-scoped, default 1000) sets the maximum number of rows a peek may examine per worker. The count spans the peek's whole walk including rows written to the peek stash.
- **Index peek offload**: `ENABLE_INDEX_PEEK_OFFLOAD` (replica-scoped, default false) gates moving a peek's walk off the timely worker for latency; `INDEX_PEEK_INLINE_BUDGET` (replica-scoped, default 1024 cursor positions) is how far one peek may walk on the worker before offloading; `INDEX_PEEK_ACTIVATION_BUDGET` (replica-scoped, default 8 192 cursor positions) caps what all peeks together spend in one activation; `INDEX_PEEK_YIELD_GRANULARITY` (replica-scoped, default 10 000 cursor positions) controls how often an offloaded scan checks for cancellation; `INDEX_PEEK_PERMIT_FRACTION` (replica-scoped, default 1.0) is the maximum number of concurrent offloaded scans as a fraction of the timely workers in one compute runtime, never below one.
- **Other**: `HYDRATION_CONCURRENCY`, `COMPUTE_SERVER_MAINTENANCE_INTERVAL`, `ENABLE_COMPUTE_REPLICA_EXPIRATION`, `COMPUTE_REPLICA_EXPIRATION_OFFSET`, `COPY_TO_S3_*`, `COMPUTE_PROMETHEUS_INTROSPECTION_SCRAPE_INTERVAL`, `SUBSCRIBE_SNAPSHOT_OPTIMIZATION`, `ENABLE_ARRANGEMENT_DICTIONARY_COMPRESSION_ALPHA`.

Constants declare their `ParameterScope` as a required argument to `Config::new`. Constants that carry `ParameterScope::Replica` are eligible for per-replica override through the scoped feature flags mechanism; those with `ParameterScope::Environment` apply environment-wide.

`all_dyncfgs` registers all constants into a `ConfigSet`.
