---
source: src/expr/src/relation.rs
revision: 703a0c27c8
---

# mz-expr::relation

Defines `MirRelationExpr`, the central relational expression type of the MIR, and a rich set of associated types.
`MirRelationExpr` is a large enum whose variants map to relational algebra operators: `Constant`, `Get`, `Let`, `LetRec`, `Project`, `Map`, `FlatMap`, `Filter`, `Join`, `Reduce`, `TopK`, `Negate`, `Threshold`, `Union`, and `ArrangeBy`.
Also defines `MapFilterProject` (MFP), `RowSetFinishing`, `JoinImplementation`, `AccessStrategy`, `FilterCharacteristics`, `ColumnOrder`, and `CollectionPlan`; implements `VisitChildren` for the generic traversal framework.
Submodules `canonicalize`, `func`, and `join_input_mapper` provide supporting analysis and utilities.
