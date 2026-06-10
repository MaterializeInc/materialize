---
source: src/persist/src/workload.rs
revision: 8d7a46b276
---

# persist::workload

Provides `DataGenerator`, a configurable synthetic data producer for benchmarking.
Generates batches of `ColumnarRecords` with configurable record count, record size, and maximum batch size; defaults are read from environment variables (`MZ_PERSIST_RECORD_SIZE_BYTES`, `MZ_PERSIST_RECORD_COUNT`, `MZ_PERSIST_BATCH_MAX_COUNT`) so benchmarks can be tuned without recompilation.
