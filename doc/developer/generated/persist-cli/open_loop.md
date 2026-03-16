---
source: src/persist-cli/src/open_loop.rs
revision: 82d92a7fad
---

# persistcli::open_loop

Implements an open-loop throughput benchmark for persist, spawning configurable numbers of concurrent writer and reader tasks against a single shard.
Writers use a `DataGenerator` to produce batches at a fixed records-per-second rate, with a separate batch-builder task and an appender task per writer to pipeline the work.
Readers poll `num_records` at each interval and track lag, throughput, and latency, logging progress throughout.
The `BenchmarkWriter` and `BenchmarkReader` traits (defined in the inline `api` submodule) allow the benchmark harness to be reused with different backend implementations; `raw_persist_benchmark` provides the default raw-persist backend.
