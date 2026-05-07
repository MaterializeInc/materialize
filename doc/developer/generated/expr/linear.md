---
source: src/expr/src/linear.rs
revision: 07858c9fd7
---

# mz-expr::linear

Defines `MapFilterProject` (MFP), the fused map-filter-project operator that applies a sequence of scalar expressions, guards them with predicates, and projects a subset of columns.
Also contains submodules `plan` (which defines `MfpPlan` and `SafeMfpPlan`, the lowered executable forms of an MFP) and `util` (providing `join_permutations` and `permutation_for_arrangement` helpers for arrangement key mapping).
MFP is the central abstraction for pushing computation close to data sources and is referenced throughout the optimizer and dataflow layers.
