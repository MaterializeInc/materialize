---
source: src/compute/src/compute_state/peek_scan.rs
revision: 3fd105cb51
---

# mz-compute::compute_state::peek_scan

An index peek's walk over the two traces that answer it, as one suspendable object.

`PeekScan` owns the error trace cursor (via `ErrorScan`), the ok trace cursor (via `PeekResultIterator`), the rows accumulated so far, and all accounting needed to enforce result size and row limits. It performs no IO and never awaits, so the same scan runs wherever its driver places it, and a driver that stops it between two cursor positions picks it up without repeating work.

`IndexPeekScan` is the concrete instantiation of `PeekScan` for the row-row arrangement type used by index peeks.

## Step protocol

`PeekScan::step` drives whichever phase is active, spending `fuel` cursor positions across both. `row_iteration_limit` is the limit in effect at the time of the call rather than when the scan was opened; the tracker spans both phases so the limit bounds the whole peek.

`ScanOutcome` has two variants:
- `Suspended` — fuel ran out or the accumulated rows have grown into a full batch. The scan retains everything; calling `step` again resumes. A driver must call `take_batch` before stepping again when a batch is ready; a scan holding an untaken batch makes no progress.
- `Finished(Ok(tail))` — the walk is over; `tail` is the rows accumulated since the last batch was taken.
- `Finished(Err(PeekError))` — the peek failed; accumulated rows are dropped.

## Error phase

`ErrorPhase` has three states: `Scanning`, `Clean`, and `Failed`. The error walk transitions `Scanning` to `Clean` (trace has no error at the peek timestamp) or to `Failed` (error found or walk itself failed). Only `Clean` reaches the ok walk. The `Clean` state transfers the error walk's row count to the ok iterator so the iteration limit spans both phases.

## Ok phase

`step_ok_phase` calls `PeekResultIterator::step` in a loop, accumulating `(Row, NonZeroI64)` pairs. Thinning (`thin`) is applied once the accumulated count reaches twice `max_results`, using an unstable partition to keep only the top `max_results` entries by the finishing's ordering. This bounds the scan's memory to a small multiple of the rows the answer needs, rather than accumulating the whole trace. Without an ordering, the scan ends early at `finishing_satisfied`.

## Stash bounds

`StashBounds` controls whether and when accumulated rows are handed to a driver for writing to the peek stash:
- `eligible` — whether this peek may use the stash at all.
- `threshold_bytes` — the byte size past which the first batch is taken, after which the answer is no longer inline.
- `batch_bytes` — the byte size past which later batches are taken.

`take_batch` returns accumulated rows once the current bound is exceeded and sets `stash_bound`, switching from the threshold to the batch size for subsequent cuts.

## Helper functions

`rows_response` converts a `RowBatch` into a `PeekResponse::Rows` by collecting into a `RowCollection`.
`entry_byte_len` computes how many bytes one `(row, count)` entry contributes to the answer as stored in `RowCollection`: row data, offset, and count, but not the `Row` struct itself.
`WalkPhases` aggregates the cumulative time and row counts across both phases for reporting to `PeekWalkMetrics`.
