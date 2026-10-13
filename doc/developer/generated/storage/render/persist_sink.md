---
source: src/storage/src/render/persist_sink.rs
revision: 80e24400e7
---

# mz-storage::render::persist_sink

Implements the `persist_sink` operator that writes source output collections into persist shards.
The operator has three logical stages: `mint_batch_descriptions` (single worker, determines which frontier intervals to write), `write_batches` (all workers, write data into batch blobs in parallel), and `append_batches` (single worker, atomically appends batches to persist).
`mint_batch_descriptions` also accepts the ingestion's remap upper as a disconnected input. When `snapshot_time` is provided (for a Postgres export that is snapshotting in this incarnation) and the `STORAGE_PERSIST_SINK_DESCRIPTION_LOOKAHEAD` dyncfg is non-zero, it commits a `Commitment` ceiling ahead of the remap upper and broadcasts it to `write_batches` on a second output. The ceiling suppresses normal description minting until the data frontier reaches it, collapsing the entire snapshot and concurrent CDC catch-up into one description. The `description_lookahead` helper converts the raw duration to milliseconds and clamps it to at least the timestamp interval.
`write_batches` holds an `open_builder` for updates that fall inside the outstanding commitment (grouped by bound regardless of timestamp), and `uncovered_builders` (keyed by timestamp) for updates that arrive outside any commitment. When a description arrives that retires the commitment, the open builder is finished under that description.
`write_batches` records the largest timestamp staged in each batch (`data_max_ts`), which `append_batches` uses during `UpperMismatch` recovery: batches whose data lies entirely below a raised append lower are deleted; batches that straddle it are re-appended under the narrowed description, with persist filtering the already-committed portion on read.
When `validate_part_bounds_on_write` is disabled, `append_batches` may combine multiple consecutive batch descriptions into a single `compare_and_append` call.
It returns an upper-frontier stream that feeds the resumption frontier calculator and error/token streams for lifecycle management.
