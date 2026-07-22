---
source: src/compute-types/src/plan/render_plan.rs
revision: e926ec3a86
---

# compute-types::plan::render_plan

Defines `RenderPlan`, a flat, node-ID-indexed representation of a `LirRelationExpr` used in the compute protocol and rendering.
Unlike `LirRelationExpr`, which recurses via `Box`, `RenderPlan` references child nodes by `LirId`, making tree traversal iterative and stack-safe.
A `RenderPlan` is divided into `BindStage`s (each holding non-recursive `LetBind`s and recursive `RecBind`s) followed by a binding-free `LetFreePlan` body.
`LetFreePlan` maintains internal invariants (root in nodes map, all referenced IDs present, valid topological order) and exposes `destruct` to consume it without re-checking invariants.
`Expr` is the node variant enum of `LetFreePlan`, mirroring `LirRelationNode` but without `Let`/`LetRec` and with child references replaced by `LirId`. Fields that formerly held `MirScalarExpr` or `MapFilterProject` now hold `LirScalarExpr` and `MfpPlan<LirScalarExpr>` / `SafeMfpPlan<LirScalarExpr>` respectively.
`RenderPlan::partition_among` partitions `Constant` stages across workers for parallel loading.
Conversion from `LirRelationExpr` is implemented via `TryFrom`; the inverse is not provided.
`RenderPlanExprHumanizer` renders individual `Expr` nodes as single-line human-readable strings for use in `mz_lir_mapping`.
