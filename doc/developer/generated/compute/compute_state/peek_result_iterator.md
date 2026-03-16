---
source: src/compute/src/compute_state/peek_result_iterator.rs
revision: 6f6a40feb7
---

# mz-compute::compute_state::peek_result_iterator

Provides `PeekResultIterator`, an `Iterator` that extracts `(Row, NonZeroI64)` pairs from a `TraceReader` while applying a `SafeMfpPlan` and optional literal key constraints.
Literal constraints are sorted and used to seek the trace cursor directly to matching keys, avoiding a full scan; the `Literals` helper manages the cursor seek state.
This iterator is the hot path for index peeks and is used both inline in `ComputeState` and as a row source for the async peek stash upload task.
