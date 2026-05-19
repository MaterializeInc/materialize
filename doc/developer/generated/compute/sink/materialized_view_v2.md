---
source: src/compute/src/sink/materialized_view_v2.rs
revision: 9a2394a9a5
---

# mz-compute::sink::materialized_view_v2

Implements an alternative MV sink that replaces async Timely operators with sync Timely operators communicating with Tokio tasks via `mpsc` channels.
Enabled by the `ENABLE_SYNC_MV_SINK` dyncfg flag; see the parent `materialized_view` module for the operator graph and design documentation.

`persist_sink` is the top-level entry point.
It reads back the output persist shard, constructs a `PersistApi` handle, and composes the three sub-operators below before registering the resulting sink frontier with `ComputeState`.

### mint

Runs on a single active worker determined by `sink_id.hashed() % peers`.
Spawns a `persist_watch` Tokio task that polls the write-handle upper frontier directly (bypassing the `persist` stream to avoid snapshot-replay lag) and sends monotonically increasing frontier values back to the Timely thread via an unbounded channel.
A second optional Tokio task wakes the operator when the system transitions out of read-only mode.
On each activation the operator drains the frontier channel (keeping only the most recent value), tracks the desired-input frontiers, and calls `maybe_mint_batch_description` to emit a `BatchDescription` when the persist frontier has advanced past the last emitted lower and the desired frontier is ahead.
Batch descriptions carry a round-robin `append_worker` assignment and are broadcast to all workers via Timely's `Broadcast` operator.

### write

Runs on all workers with desired and persist data exchanged by row hash so cancelling updates land on the same worker.
Owns a `Correction` buffer per worker (managed by a Tokio `batch_writer` task) and coalesces all per-activation input data, frontier advances, and consolidation requests into a single `WriteCommand::Batch` per activation before sending it to the task.
When `MV_SINK_ADVANCE_PERSIST_FRONTIERS` is enabled, both the Timely-side `persist_frontiers` and the Tokio-side `corrections.since` are initialized to `as_of` at startup; this keeps the two sides in lockstep and prevents snapshot-replay updates from slipping into a batch with a `lower` of `as_of`, which would otherwise trip persist's `UpdateNotBeyondLower` invariant.
Once a batch description is ready and both the desired and persist frontiers have passed its bounds, the operator sends a `WriteCommand::WriteBatch` and waits for a `WriteResponse` containing the serialized `ProtoBatch`, which is forwarded downstream.

### append

Runs on all workers; batches are exchange-routed to the designated `append_worker` from the description.
A Tokio append task maintains a `State` machine that absorbs `Description`, `Batch`, and `BatchesFrontier` commands in FIFO order.
`BatchesFrontier` is always sent after its corresponding `Batch` messages within a single activation to preserve the ordering invariant described in the module-level documentation.
When both the batches frontier has advanced past `lower` and a batch description is available, `maybe_append_batches` calls `compare_and_append_batch`; on a lower-mismatch indicating the shard upper lags behind the batch lower, it first bumps the shard upper with an empty append before retrying.
