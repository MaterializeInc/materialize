---
source: src/compute/src/compute_state/peek_stash.rs
revision: eefc53999d
---

# mz-compute::compute_state::peek_stash

Implements `StashingPeek`, which uploads peek results to a per-peek persist shard and returns a `PeekResponse::Stashed` handle instead of sending rows inline in `ComputeResponse`.
A worker-thread `pump_rows` method feeds batches of `(Row, NonZeroI64)` from a `PeekResultIterator` through an `mpsc` channel to an async background task that writes them into a persist batch builder.
This mechanism offloads large peek results from the command/response path and allows the controller to retrieve them from persist at its own pace.
