---
source: src/transform/src/dataflow.rs
revision: 4267863081
---

# mz-transform::dataflow

Implements whole-dataflow optimization via the public `optimize_dataflow` function.
After inlining single-use views, the function runs a full logical optimizer pass, propagates filters and demand across view boundaries (`optimize_dataflow_filters`, `optimize_dataflow_demand`), runs a logical cleanup pass, runs the physical optimizer on each relation, and finally calls `MonotonicFlag` to annotate `TopK` and `Reduce` operators with monotonicity information and collects index-usage information.
Also defines `DataflowMetainfo`, the container for optimizer notices, index usage metadata, and the transient per-dataflow data accumulated during optimization passes.
