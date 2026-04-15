---
source: src/compute/src/render.rs
revision: b0fa98e931
---

# mz-compute::render

Translates `RenderPlan` IR nodes into Timely/differential dataflow operators, building the oks and errs parallel computation trees.
The top-level `render` function imports source arrangements and index exports, then recursively constructs operators for each `RenderPlan` node via the `Context`; submodules handle joins (`join`), aggregations (`reduce`), top-K (`top_k`), thresholds (`threshold`), table functions (`flat_map`), sinks (`sinks`), continual tasks (`continual_task`), and error handling utilities (`errors`).
Errors propagate alongside successful rows in a parallel `errs` stream, and sinks are expected to observe both streams.
