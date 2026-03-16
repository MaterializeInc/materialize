---
source: src/compute-types/src/plan/render_plan.rs
revision: bd9d3e1ab8
---

# compute-types::plan::render_plan

Defines `RenderPlan<T>`, a flat, node-ID-indexed representation of a `Plan` used in the compute protocol and rendering.
Unlike `Plan`, which recurses via `Box`, `RenderPlan` references child nodes by `LirId`, making tree traversal iterative and stack-safe.
A `RenderPlan` is divided into `BindStage`s (each holding non-recursive `LetBind`s and recursive `RecBind`s) followed by a binding-free `LetFreePlan` body.
Conversion from `Plan` is implemented via `TryFrom`; the inverse is not provided.
