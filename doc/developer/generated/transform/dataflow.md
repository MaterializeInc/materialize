---
source: src/transform/src/dataflow.rs
revision: ff62f31a52
---

# mz-transform::dataflow

Implements whole-dataflow optimization via the public `optimize_dataflow` function.
After inlining single-use views, the function runs a full logical optimizer pass, propagates filters and demand across view boundaries (`optimize_dataflow_filters`, `optimize_dataflow_demand`), runs a logical cleanup pass, runs the physical optimizer on each relation, calls `MonotonicFlag` to annotate `TopK` and `Reduce` operators with monotonicity information and collect index-usage information, and finally calls `prune_dataflow_source_imports` to restrict the source import list to only those sources actually read by exports. Pruning the import list reclaims read holds and per-worker `persist_source` instances for sources that optimizer passes folded away.
Also defines `DataflowMetainfo`, the container for optimizer notices, index usage metadata, and the transient per-dataflow data accumulated during optimization passes.
