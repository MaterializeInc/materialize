---
source: src/compute/src/render/join/linear_join.rs
revision: b7c30cce49
---

# mz-compute::render::join::linear_join

Renders `JoinPlan::Linear` — a left-deep lookup join that iteratively probes arranged inputs from left to right.
`LinearJoinSpec` controls whether the join uses the standard differential join or a Materialize-specific fueled join core (`mz_join_core`); the spec is derived from dyncfg at render time.
The running accumulator between stages is a `ColCollection` (columnar edge). Each stage arranges the accumulator on the join key via `arrange_join_input`, probes the next input arrangement, and writes its output back onto the columnar edge. The arrange step selects the batcher via `ArrangementBatcher::from_config`: `ColumnarPaged` uses `Col2ValPagedBatcher`/`RowRowColPagedBuilder`, `Columnar` uses `Col2ValColBatcher`/`RowRowColPagedBuilder`, and `Columnation` uses `Col2ValBatcher`/`RowRowBuilder`.
The initial and final closures are applied on the edge via `apply_closure_to_edge`, which calls `flat_map_datums` to decode each row borrowed from the column. Within `differential_join_inner`, the error-capable arm produces `Result` values and then demultiplexes them onto the edge via `demux_join_results` (a single-output operator that splits oks into a `ColCollection` and errs into a `Vec` collection); the two infallible arms write directly to the edge using `ColumnBuilder` (non-terminal stages) or `ConsolidatingColumnBuilder` (terminal stage).
