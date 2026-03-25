---
source: src/transform/src/analysis/monotonic.rs
revision: db936d7370
---

# mz-transform::analysis::monotonic

Defines the `Monotonic` analysis, which determines whether a `MirRelationExpr` is logically monotonic — that is, it never introduces retractions.
It implements `Analysis` with a `bool` value and a `BoolLattice`, propagating monotonicity upward through operators according to their semantics (e.g., `Filter` is monotonic only if its input is and no temporal predicates are present; `Reduce` is monotonic only when it is a pure distinct with no aggregates).
The `Monotonic` struct is parameterized by a set of globally-known monotonic `GlobalId`s that are provided at construction time.
