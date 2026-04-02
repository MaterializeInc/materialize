---
source: src/expr/src/relation.rs
revision: 2982634c0d
---

# mz-expr::relation

Defines `MirRelationExpr`, the central relational expression type of the MIR, and a rich set of associated types.
`MirRelationExpr` is a large enum whose variants map to relational algebra operators: `Constant`, `Get`, `Let`, `LetRec`, `Project`, `Map`, `FlatMap`, `Filter`, `Join`, `Reduce`, `TopK`, `Negate`, `Threshold`, `Union`, and `ArrangeBy`.
Also defines `MapFilterProject` (MFP), `RowSetFinishing`, `JoinImplementation`, `AccessStrategy`, `FilterCharacteristics`, `ColumnOrder`, `CollectionPlan`, and `RowComparator` (a reusable, column-order-aware comparator that shares `DatumVec` scratch buffers across calls); implements `VisitChildren` for the generic traversal framework.
`could_run_expensive_function` detects potentially expensive expressions by checking for scalar function calls, `FlatMap`/`Reduce` operators, and conservatively returns `true` on `RecursionLimitError`.
Submodules: `canonicalize` (expression normalization), `func` (aggregate and table functions), and `join_input_mapper` (join analysis utilities).
