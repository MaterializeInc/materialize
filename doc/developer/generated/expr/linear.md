---
source: src/expr/src/linear.rs
revision: 0b82784bb8
---

# mz-expr::linear

Defines `MapFilterProject` (MFP), the fused map-filter-project operator that applies a sequence of scalar expressions, guards them with predicates, and projects a subset of columns.
Also contains submodules `plan` (which defines `MfpPlan` and `SafeMfpPlan`, the lowered executable forms of an MFP) and `util` (providing `join_permutations` and `permutation_for_arrangement` helpers for arrangement key mapping).
`permutation_for_arrangement` takes `key: &[impl Columns]` (a list of mappings from columns to key indices, a key length, and an input arity) rather than a concrete `&[MirScalarExpr]`, making it generic over any type implementing the `Columns` trait.
The `dataflow::plan::AvailableCollections` reference in `permutation_for_arrangement`'s documentation refers to `compute_types::plan::AvailableCollections`.
Several internal uses of `.iter().max()` and `.iter().min()` on `BTreeSet` results have been replaced with `BTreeSet::last()` and `BTreeSet::first()` for O(log n) rather than O(n) access.
MFP is the central abstraction for pushing computation close to data sources and is referenced throughout the optimizer and dataflow layers.
