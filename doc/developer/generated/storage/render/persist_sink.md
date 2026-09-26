---
source: src/storage/src/render/persist_sink.rs
revision: d1e62f9f41
---

# mz-storage::render::persist_sink

Implements the `persist_sink` operator that writes source output collections into persist shards.
The operator has three logical stages: `mint_batch_descriptions` (single worker, determines which frontier intervals to write), `write_batches` (all workers, write data into batch blobs in parallel, one `BatchBuilder` per timestamp), and `append_batches` (single worker, atomically appends batches to persist).
`write_batches` records the largest timestamp staged in each batch (`data_max_ts`), which `append_batches` uses during `UpperMismatch` recovery: batches whose data lies entirely below a raised append lower are deleted; batches that straddle it are re-appended under the narrowed description, with persist filtering the already-committed portion on read.
When `validate_part_bounds_on_write` is disabled, `append_batches` may combine multiple consecutive batch descriptions into a single `compare_and_append` call.
It returns an upper-frontier stream that feeds the resumption frontier calculator and error/token streams for lifecycle management.
