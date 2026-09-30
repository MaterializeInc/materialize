---
source: src/compute/src/compute_state/peek_metrics.rs
revision: 3fd105cb51
---

# mz-compute::compute_state::peek_metrics

Defines the per-walk and per-peek metric types for index peek walks.

## PeekWalkMetrics

`PeekWalkMetrics` carries references to all phase-level histograms and walk-substrate counters. It is cloned into each offloaded walk's task so that a walk reports the same metrics regardless of which substrate drove it to completion.

Substrate counters:
- `walks_inline` — incremented once per walk that the timely worker drove to an outcome.
- `walks_offloaded` — incremented once per walk that an offloaded task drove to an outcome.
- `walks_stashed` — incremented alongside `walks_offloaded` for walks that answered via the peek response stash.

Phase histograms (`error_scan_seconds`, `cursor_setup_seconds`, `row_iteration_seconds`, `row_iteration_rows`, `result_sort_seconds`, `result_sort_rows`, `row_collection_seconds`) are observed once per walk by the driver that produces the terminal outcome. A walk that offloads reports nothing at the offload point; the task that finishes reports the cumulative numbers.

`observe_error_phase` reports the error scan and cursor setup times, but only when `error_trace_clean` is true. A peek answered by its error trace reports neither number, since the cursor setup was for an ok cursor that was never used.

`observe_ok_phase` reports the ok walk's iteration time, row count, sort time, and sort row count.

`queued_for_permit` increments `permit_queue_depth` and returns a `PermitWait` guard that decrements it on drop. `PermitWait::admitted` additionally observes `permit_wait_seconds` for waits that ended in a permit; cancelled or aborted walks drop `PermitWait` without calling `admitted` and therefore do not record a wait time.

## IndexPeekMetrics

`IndexPeekMetrics` carries the metrics the timely worker reports from its own handling of a peek — `seek_fulfillment_seconds`, `frontier_check_seconds`, and a reference to the shared `PeekWalkMetrics`. These are single-observer metrics by construction because the worker handles one peek at a time on its own event loop.
