---
source: src/compute/src/compute_state/error_scan.rs
revision: 24e1cba0d4
---

# mz-compute::compute_state::error_scan

Implements the walk over an index peek's error trace, the phase that runs before `PeekResultIterator` walks the ok trace.

`ErrorScan` holds a cursor into the error trace and a `PeekRowIterationTracker` that counts positions examined across this walk and the ok scan that follows. `scan_time` accumulates wall-clock time spent stepping, summed over every slice the walk was cut into.

`ErrsHandle` is a type alias for the error trace handle as `TraceBundle::errs_mut` hands it out.

## Step protocol

`ErrorScan::step` advances the cursor until one of three things happens: the cursor is exhausted (clean trace), a key's positive multiplicity indicates an error in the trace, or `fuel` is exhausted. Fuel is decremented per key position rather than per error found, so a trace holding many keys that cancel to zero at the peek timestamp does not run to its end within a single call.

`ErrorScanStep` is the outcome:
- `Finished(Ok(rows_iterated))` — the trace is clean; the row count is forwarded to the ok walk so the limit spans both phases.
- `Finished(Err(PeekError))` — the trace holds an error at the peek timestamp, or a negative multiplicity was detected. The error is the peek's answer.
- `OutOfFuel` — fuel ran out; resume from the current cursor position.

`ErrorScan` does not remember that it ended. `PeekScan` remembers instead: it keeps the outcome and drops the walk, so a finished peek stops pinning error batches.

## Construction

`ErrorScan::new` opens a cursor over the error trace, recording the time spent doing so in `scan_time`. `ErrorScan::from_cursor` constructs from an already-opened cursor, used when cursor setup is timed elsewhere.

`set_row_iteration_limit` adopts the limit currently in effect without resetting the count already accumulated.
