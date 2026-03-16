---
source: src/compute-types/src/plan.rs
revision: 65fb19dabf
---

# compute-types::plan

Defines `Plan<T>` (the LIR plan type) and `PlanNode<T>` (its node enum), along with `AvailableCollections` (which arrangements/raw forms a plan node produces), `GetPlan` (how a Get node reads a collection), and `LirId` (a unique node identifier).
`Plan::finalize_dataflow` is the entry point for post-lowering optimisations: it applies transforms (currently `RelaxMustConsolidate`) and traces the final plan.
Sub-modules: `lowering` (MIR→LIR), `render_plan` (flat node-ID representation for rendering), `join`, `reduce`, `threshold`, `top_k`, `interpret`, and `transform`.
