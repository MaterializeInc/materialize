---
source: src/ore/src/graph.rs
revision: 3ea5fbaaff
---

# mz-ore::graph

Provides non-recursive depth-first traversal algorithms for generic graphs, avoiding stack overflows on deep or cyclic structures.

The four public functions — `nonrecursive_dft`, `nonrecursive_dft_mut`, `try_nonrecursive_dft`, and `try_nonrecursive_dft_mut` — share the same iterative algorithm: a `Vec` acts as an explicit stack of entered-but-not-exited nodes, and a `BTreeSet` tracks exited nodes to skip cycles.
Each function accepts caller-supplied `at_enter` and `at_exit` callbacks that return the children to visit and perform any side effects; the `_mut` variants grant mutable access to the graph, and the `try_` variants propagate `Result` errors from callbacks.
