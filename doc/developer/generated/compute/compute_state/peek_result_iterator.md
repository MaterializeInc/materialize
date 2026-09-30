---
source: src/compute/src/compute_state/peek_result_iterator.rs
revision: 24e1cba0d4
---

# mz-compute::compute_state::peek_result_iterator

Provides `PeekResultIterator`, a type that extracts `(Row, NonZeroI64)` pairs from a `TraceReader` while applying a `SafeMfpPlan` and optional literal key constraints.
Literal constraints are sorted and used to seek the trace cursor directly to matching keys, avoiding a full scan. The `Literals` helper manages seek state via a `LiteralPosition` enum with variants `Seeking` (a seek is outstanding), `At(usize)` (cursor sits on the indexed literal), and `Exhausted` (all literals have been tried). Literal seeks are fueled: `seek_next_literal_key` takes a `fuel: &mut usize` and returns `SeekOutcome::OutOfFuel` when literals remain untried and the budget runs out, so a walk sliced by fuel does not charge more than an unsliced one.
`PeekResultIterator` can be constructed from a `TraceReader` via `new`, or from an already-opened cursor via `from_cursor`, which peek scan drivers use to share a cursor between the error scan and the row walk. `set_row_iteration_limit` and `add_rows_iterated` let a resumed walk adopt both the current limit and the rows a prior walk already examined, so the row-iteration limit bounds the whole peek rather than any single slice.
The iterator tracks `rows_processed`, the total number of arrangement rows (key-value pairs) it has evaluated including those filtered out by the MFP; callers retrieve this via `rows_processed()` after iteration to record row-workload metrics.
The iterator integrates a `PeekRowIterationTracker`. Every cursor position visited charges the tracker before any filtering is applied, because the row has already been read. When the tracker returns `Err(PeekError::RowIterationLimitExceeded)`, the iterator latches `exhausted = true` and returns the error; subsequent calls return `Step::Done`.
`step(&mut fuel: &mut usize) -> Step` advances the cursor until it produces a row (`Step::Row`), the cursor is exhausted (`Step::Done`), or fuel runs out (`Step::OutOfFuel`). Fuel is charged per cursor position, literal seeks included, so a selective MFP or a sparse literal list cannot starve the caller of yield points. The charge is paid after any suspendable advance (literal seek, key step) so that a suspended call pays only for work it kept. An error from MFP evaluation or the row-iteration tracker latches the iterator shut; the error is the peek's whole answer and a caller that steps again gets `Step::Done`. `Iterator::next` calls `step` with `usize::MAX` fuel.
`TraceCursor` and `TraceStorage` type aliases are re-exported as `pub(super)` for use by sibling scan modules.
Item errors use `PeekError` rather than `String`, carrying structured `Dataflow` variants for MFP evaluation failures and `Unstructured` variants for internal consistency errors.
This iterator is the hot path for index peeks and is used both inline in the peek scan and as a row source for offloaded peek drivers.
