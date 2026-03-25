---
source: src/compute/src/compute_state.rs
revision: 22bc981dda
---

# mz-compute::compute_state

Contains the per-worker `ComputeState` and `ActiveComputeState` types that hold all live dataflow state, pending peeks, pending subscribes, copy-to sinks, and the command history.
`ActiveComputeState` is an activated view of `ComputeState` bundled with the Timely worker and response sender; it handles each `ComputeCommand`, processes ready peeks (both index and persist fast-path), and drains subscribe and copy-to response buffers.
The `peek_result_iterator` submodule provides the cursor-based row extraction logic, and `peek_stash` handles offloading large peek results to persist blobs.
