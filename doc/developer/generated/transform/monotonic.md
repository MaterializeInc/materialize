---
source: src/transform/src/monotonic.rs
revision: ddc1ff8d2d
---

# mz-transform::monotonic

Provides `MonotonicFlag`, a helper (not a `Transform`) that annotates `Reduce` and `TopK` operators with their monotonicity flag by running the `Monotonic` analysis and writing the result into the AST node.
It is called from `dataflow::optimize_dataflow` after the physical optimization pass, using the set of globally-monotonic collection IDs gathered from the dataflow.
