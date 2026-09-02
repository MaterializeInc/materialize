---
source: src/compute/src/compute_state.rs
revision: 844b947128
---

# mz-compute::compute_state

Contains the per-worker `ComputeState` and `ActiveComputeState` types that hold all live dataflow state, pending peeks, pending subscribes, copy-to sinks, and the command history.
`ComputeState` owns collections, the `TraceManager`, subscribe/copy-to response buffers, per-worker dynamic configuration (`worker_config`), suspended collections awaiting scheduling, replica expiration state, and an optional `StorageTimelyLogReader` that is consumed when logging is initialized. When configuration is applied, `ComputeState` also configures the column-paged-batcher by calling `mz_timely_util::column_pager::apply_tiered_config`, deriving a resident-byte budget from `COLUMN_PAGED_BATCHER_BUDGET_FRACTION` and the announced memory limit, selecting a file or swap pager backend depending on whether a scratch directory is available, and forwarding `COLUMN_PAGED_BATCHER_SWAP_PAGEOUT` to control eager RSS eviction of lz4-compressed swap-backend spill chunks. It also applies `COLUMN_CHUNK_COMPRESS_MIN_DEPTH` to `mz_timely_util::columnar::chunk::set_compress_min_depth`, which controls the youngest chunk generation whose spilled bodies are lz4-compressed. Config application also calls `mz_timely_util::columnar::chunk::set_compute_spill_enabled` to set compute's leg of the process-wide chunk spill gate; the gate ORs this leg with storage's so chunks spill while either subsystem's flag is set.
`ActiveComputeState` is an activated view of `ComputeState` bundled with the Timely worker and response sender; it handles each `ComputeCommand`, processes ready peeks (both index and persist fast-path), and drains subscribe and copy-to response buffers.
When `handle_create_instance` is called, it first applies `InstanceConfig::initial_config` to the worker configuration so that create-time setup observes controller-synced dyncfg values rather than defaults, then calls `apply_worker_config` to ensure state consistency before initialization.
When `initialize_logging` is called on `CreateInstance`, the `storage_log_reader` is taken from `ComputeState` and forwarded to the logging setup so the timely logging dataflow can replay storage worker events.
In read-only mode, the output frontier for collections excludes the write frontier (which can't be advanced by the dataflow), preventing stalled progress reporting.
Index peek processing is instrumented via `IndexPeekMetrics`, which carries references to per-phase histograms including `row_iteration_rows` (arrangement rows evaluated by the result iterator, including those filtered by MFP) and `result_sort_rows` (input rows across all thinning sort passes) alongside the existing duration histograms.
`PeekRowIterationConfig` holds cheap `ConfigValHandle<bool>` and `ConfigValHandle<usize>` handles on `ENABLE_PEEK_ROW_ITERATION_LIMIT` and `PEEK_ROW_ITERATION_LIMIT` respectively, so that a limit change delivered via `UpdateConfiguration` is visible to peeks already in flight without re-reading from a captured snapshot. `PeekRowIterationTracker` counts rows examined on this worker and returns `Err(PeekError::RowIterationLimitExceeded { limit })` on the call that would exceed the limit; it can absorb a new limit mid-scan via `set_limit` so that rows counted while the feature was disabled still count after it is enabled. The row iteration limit does not follow a peek into the stash: the stash restarts the scan in bounded bursts and may examine any number of rows.
Dataflow errors are deserialized from the error trace and reported as `PeekError::Dataflow` variants rather than plain strings, preserving SQLSTATE information through the peek response path.

## Submodules

- `peek_result_iterator` -- cursor-based row extraction logic for peek processing.
- `peek_stash` -- offloads large peek results to persist blobs via `StashingPeek`.
