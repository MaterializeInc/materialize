---
source: src/sql/src/plan/lowering.rs
revision: 6168aa99a7
---

# mz-sql::plan::lowering

Implements decorrelation — the translation of `HirRelationExpr` to `MirRelationExpr`.
The core algorithm maintains a `ColumnMap` that tracks outer-relation column positions and an `outer` relation; as it traverses nested subqueries it lifts correlated references by cross-joining the subquery against the outer relation and aggregating appropriately.
The `variadic_left` submodule provides a specialized optimization for stacks of left joins, and `transform_hir` passes are applied before lowering begins.
