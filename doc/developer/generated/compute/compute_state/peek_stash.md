---
source: src/compute/src/compute_state/peek_stash.rs
revision: 82e054569f
---

# mz-compute::compute_state::peek_stash

Implements incremental stash upload machinery for writing large peek results to a per-peek persist shard, returning a `PeekResponse::Stashed` handle instead of sending rows inline in `ComputeResponse`.
`StashUpload` owns the IO a stash write needs. It is opened with `StashUpload::open`, which connects to the persist location and creates a batch builder. Rows are pushed incrementally via `push(rows: RowBatch)`, which writes each row to the builder and tracks `num_rows` and whether persist has taken any parts to blob storage (`wrote_parts`). `finish` completes the batch and builds the `PeekResponse::Stashed` response; `abandon` schedules deletion of any parts already written and drops the builder. `Drop for StashUpload` calls `abandon`, so an upload that is cancelled mid-write cleans up after itself.
`StashError` names the failure modes: `OpenLocation`, `WriteRow`, `FinishBatch`, and `LostFinishTask`. The caller reports these as the peek's error.
`DeliveredBatch` wraps a finished `Batch` and deletes it from blob storage on drop if it is never claimed, covering cases where a delivery is sent to a dropped receiver or where the receiver drops between the send and the take.
`StashTarget` is a lightweight descriptor (persist clients, location, peek UUID, relation desc) held by a driver that may or may not reach the stash threshold. Opening an upload is deferred to `StashTarget::open` so that a walk that answers inline opens no shard and writes no byte.
The batch builder is finished in a detached task spawned via `spawn_named`, so a cancellation that arrives while the builder is flushing its buffered part cannot cancel the finish task; whoever holds the resulting `DeliveredBatch` last deletes the batch.
