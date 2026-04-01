---
source: src/transform/src/analysis.rs
revision: 5f785f23fd
---

# mz-transform::analysis

Provides a bottom-up analysis framework for `MirRelationExpr` and a collection of concrete analyses that transforms can query.
The `Analysis` trait and its supporting types (`Derived`, `DerivedBuilder`, `DerivedView`) form the infrastructure: each analysis implements `derive` over a post-order traversal, and dependencies between analyses are declared via `announce_dependencies`.
Built-in analyses include `Arity`, `Cardinality`, `ColumnNames`, `NonNegative`, `ReprRelationType`, `SubtreeSize`, `UniqueKeys`, and two directory-based submodules: `equivalences` (equivalence-class tracking) and `monotonic` (monotonicity detection).
