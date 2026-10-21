---
source: src/compute/src/render.rs
revision: cab902bb0f
---

# mz-compute::render

Translates `RenderPlan` IR nodes into Timely/differential dataflow operators, building the oks and errs parallel computation trees.
The top-level `build_compute_dataflow` function imports source arrangements and index exports, then recursively constructs operators for each `RenderPlan` node via the `Context`; submodules handle joins (`join`), aggregations (`reduce`), top-K (`top_k`), thresholds (`threshold`), table functions (`flat_map`), sinks (`sinks`), columnar dataflow edge types (`columnar`), and error handling utilities (`errors`).
Errors propagate alongside successful rows in a parallel `errs` stream typed as `DataflowErrorSer` (the serialized error type defined in the `errors` submodule), and sinks are expected to observe both streams.
`import_filtered_index_edge` imports a snapshot-excluded arranged index directly onto the columnar collection edge, discarding any batch whose upper is at or below the dataflow's `as_of` frontier (those batches carry only data the snapshot already includes). The `logic` callback packs each `(key, val)` pair into a reused row buffer and pushes it borrowed, so a key-value pair with many timestamps costs one pack rather than an owned `Row` per time.
`EXPLAIN ANALYZE ... CPU` (mz-sql) counts the operators of a span at the smallest address depth as its outermost ones. That holds because every node renders in `self.scope` and closes the regions it opens before `render_plan_expr` returns. A region open across span boundaries would make those queries count nested operators' time twice.
