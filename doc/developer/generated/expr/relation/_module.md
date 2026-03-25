---
source: src/expr/src/relation.rs
revision: d4dc9c7ae5
---

# mz-expr::relation

Defines `MirRelationExpr`, the central relational expression type of the MIR, and a rich set of associated types.
`MirRelationExpr` is a large enum whose variants map to relational algebra operators: `Constant`, `Get`, `Let`, `LetRec`, `Project`, `Map`, `FlatMap`, `Filter`, `Join`, `Reduce`, `TopK`, `Negate`, `Threshold`, `Union`, and `ArrangeBy`.
Also defines `MapFilterProject` (MFP), `RowSetFinishing`, `JoinImplementation`, `AccessStrategy`, `FilterCharacteristics`, `ColumnOrder`, and `CollectionPlan`; implements `VisitChildren` for the generic traversal framework.
Submodules: `canonicalize` (expression normalization), `func` (aggregate and table functions), and `join_input_mapper` (join analysis utilities).
