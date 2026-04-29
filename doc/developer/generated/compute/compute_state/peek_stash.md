---
source: src/compute/src/compute_state/peek_stash.rs
revision: 23772dc84e
---

# mz-compute::compute_state::peek_stash

Implements `StashingPeek`, which uploads peek results to a per-peek persist shard and returns a `PeekResponse::Stashed` handle instead of sending rows inline in `ComputeResponse`.
`start_upload` creates a `PeekResultIterator` from the trace bundle and spawns an async background task connected via an `mpsc` channel. The worker thread's `pump_rows` method feeds batches of `(Row, NonZeroI64)` from the iterator through the channel; the background task writes them into a persist batch builder with a configurable `batch_max_runs` consolidation target.
This mechanism offloads large peek results from the command/response path and allows the controller to retrieve them from persist at its own pace. The `AbortOnDropHandle` ensures the background task is cancelled if the peek is no longer needed.
