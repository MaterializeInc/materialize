---
source: src/compute-types/src/plan/lowering.rs
revision: a67213e0ae
---

# compute-types::plan::lowering

Implements lowering from `DataflowDescription<MirRelationExpr>` (MIR) to `DataflowDescription<Plan>` (LIR) via `DataflowDescription::lower`.
A `Context` tracks known arrangements keyed by `Id` and assigns monotonically increasing `LirId`s to each node.
`MirRelationExpr` variants (Get, Let, LetRec, Join, Reduce, TopK, Threshold, Negate, Union, ArrangeBy, etc.) are recursively translated to corresponding `Plan` nodes; join implementation selection consults the `JoinImplementation` annotation already placed by the optimizer.
