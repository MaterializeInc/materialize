---
source: src/compute/src/compute_state.rs
revision: f94584ddef
---

# mz-compute::compute_state

Contains the per-worker `ComputeState` and `ActiveComputeState` types that hold all live dataflow state, pending peeks, pending subscribes, copy-to sinks, and the command history.
`ComputeState` owns collections, the `TraceManager`, subscribe/copy-to response buffers, per-worker dynamic configuration (`worker_config`), suspended collections awaiting scheduling, and replica expiration state.
`ActiveComputeState` is an activated view of `ComputeState` bundled with the Timely worker and response sender; it handles each `ComputeCommand`, processes ready peeks (both index and persist fast-path), and drains subscribe and copy-to response buffers.

## Submodules

- `peek_result_iterator` -- cursor-based row extraction logic for peek processing.
- `peek_stash` -- offloads large peek results to persist blobs via `StashingPeek`.
