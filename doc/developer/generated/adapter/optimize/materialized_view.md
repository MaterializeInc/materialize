---
source: src/adapter/src/optimize/materialized_view.rs
revision: 9d0a7c3c6f
---

# adapter::optimize::materialized_view

Implements the optimizer pipeline for `CREATE MATERIALIZED VIEW` as a two-stage `Optimize` sequence: the first stage produces an optimized `DataflowDescription<OptimizedMirRelationExpr>` (MIR-level), and the second stage lowers to `DataflowDescription<Plan>` (LIR-level).
The pipeline handles refresh schedules, non-null assertions, and compaction windows by encoding them in the sink description attached to the dataflow.
