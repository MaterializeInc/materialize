---
source: src/storage/src/render/persist_sink.rs
revision: b0fa98e931
---

# mz-storage::render::persist_sink

Implements the `persist_sink` operator that writes source output collections into persist shards.
The operator has three logical stages: `mint_batch_descriptions` (single worker, determines which frontier intervals to write), `write_batches` (all workers, write data into batch blobs in parallel), and `append_batches` (single worker, atomically appends all batches for a frontier interval).
It returns an upper-frontier stream that feeds the resumption frontier calculator and error/token streams for lifecycle management.
