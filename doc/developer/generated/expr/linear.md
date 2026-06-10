---
source: src/expr/src/linear.rs
revision: fc2aaf02e7
---

# mz-expr::linear

Defines `MapFilterProject<E: OptimizableExpr = MirScalarExpr>`, the fused map-filter-project operator that applies a sequence of scalar expressions, guards them with predicates, and projects a subset of columns.
`MapFilterProject` is now generic: the expression type `E` defaults to `MirScalarExpr` but can be any type implementing the `OptimizableExpr` trait; methods that require `MirScalarExpr`-specific operations (e.g. `literal_constraint`, `literal_constraints`, `as_literal_false`, `as_literal_true`, and several extract/optimization helpers) are in a separate `impl MapFilterProject<MirScalarExpr>` block.
Also contains submodules `plan` (which defines `MfpPlan` and `SafeMfpPlan`, the lowered executable forms of an MFP) and `util` (providing `join_permutations` and `permutation_for_arrangement` helpers for arrangement key mapping).
`permutation_for_arrangement` takes `key: &[impl Columns]` (a list of mappings from columns to key indices, a key length, and an input arity) rather than a concrete `&[MirScalarExpr]`, making it generic over any type implementing the `Columns` trait.
The `dataflow::plan::AvailableCollections` reference in `permutation_for_arrangement`'s documentation refers to `compute_types::plan::AvailableCollections`.
Several internal uses of `.iter().max()` and `.iter().min()` on `BTreeSet` results have been replaced with `BTreeSet::last()` and `BTreeSet::first()` for O(log n) rather than O(n) access.
MFP is the central abstraction for pushing computation close to data sources and is referenced throughout the optimizer and dataflow layers.
