---
source: src/compute/src/compute_state/peek_result_iterator.rs
revision: c69fde3d50
---

# mz-compute::compute_state::peek_result_iterator

Provides `PeekResultIterator`, a type that extracts `(Row, NonZeroI64)` pairs from a `TraceReader` while applying a `SafeMfpPlan` and optional literal key constraints.
Literal constraints are sorted and used to seek the trace cursor directly to matching keys, avoiding a full scan; the `Literals` helper manages the cursor seek state.
The iterator tracks `rows_processed`, the total number of arrangement rows (key-value pairs) it has evaluated including those filtered out by the MFP; callers retrieve this via `rows_processed()` after iteration to record row-workload metrics.
The iterator integrates a `PeekRowIterationTracker` (from the parent `compute_state` module). Every cursor position visited charges the tracker before any filtering is applied, because the row has already been read. When the tracker returns `Err(PeekError::RowIterationLimitExceeded)`, the iterator latches `exhausted = true` and returns the error as an `Item`; subsequent calls return `None`.
The `Iterator` implementation drives a fueled inner loop: `step(&mut fuel: usize) -> Step` advances the cursor until it produces a row (`Step::Row`), the cursor is exhausted (`Step::Done`), or fuel runs out (`Step::OutOfFuel`). Fuel is charged per cursor position rather than per returned row, so a selective MFP cannot starve the caller of yield points. The `Iterator::next` wrapper calls `step` with `usize::MAX` fuel to preserve the standard iterator contract.
Item errors use `PeekError` rather than `String`, carrying structured `Dataflow` variants for MFP evaluation failures and `Unstructured` variants for internal consistency errors.
This iterator is the hot path for index peeks and is used both inline in `ComputeState` and as a row source for the async peek stash upload task.
