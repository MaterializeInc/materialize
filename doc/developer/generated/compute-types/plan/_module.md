---
source: src/compute-types/src/plan.rs
revision: e926ec3a86
---

# compute-types::plan

Defines `LirRelationExpr` (the LIR plan type) and `LirRelationNode` (its node enum), along with `AvailableCollections` (which arrangements and raw forms a plan node produces), `GetPlan` (how a Get node reads a collection), and `LirId` (a unique node identifier). Fields that previously held `MirScalarExpr` or `MapFilterProject` now hold `LirScalarExpr` and `MfpPlan<LirScalarExpr>` / `SafeMfpPlan<LirScalarExpr>` respectively; `AvailableCollections::arranged` stores `Vec<(Vec<LirScalarExpr>, Vec<usize>, Vec<usize>)>`.
`LirRelationExpr::finalize_dataflow` is the entry point for the full MIR→LIR pipeline: it lowers the dataflow, refines source MFPs, optionally enables union-negate consolidation, upgrades single-time plans to use monotonic operators, and relaxes `must_consolidate` flags via `RelaxMustConsolidate`.
`LirRelationExpr::pretty` and `LirRelationExpr::debug_explain` provide text rendering for debugging and tests.
Sub-modules: `lowering` (MIR→LIR), `render_plan` (flat node-ID representation for rendering), `join`, `reduce`, `scalar` (LIR scalar expression types and MIR→LIR conversion helpers), `threshold`, `top_k`, `interpret`, and `transform`.
