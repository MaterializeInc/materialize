---
source: src/compute-types/src/plan.rs
revision: b55d3dee25
---

# compute-types::plan

Defines `Plan` (the LIR plan type) and `PlanNode` (its node enum), along with `AvailableCollections` (which arrangements and raw forms a plan node produces), `GetPlan` (how a Get node reads a collection), and `LirId` (a unique node identifier).
`Plan::finalize_dataflow` is the entry point for the full MIR→LIR pipeline: it lowers the dataflow, refines source MFPs, optionally enables union-negate consolidation, upgrades single-time plans to use monotonic operators, and relaxes `must_consolidate` flags via `RelaxMustConsolidate`.
`Plan::pretty` and `Plan::debug_explain` provide text rendering for debugging and tests.
Sub-modules: `lowering` (MIR→LIR), `render_plan` (flat node-ID representation for rendering), `join`, `reduce`, `threshold`, `top_k`, `interpret`, and `transform`.
