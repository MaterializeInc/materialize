---
source: src/compute-types/src/plan.rs
revision: 92047d0776
---

# compute-types::plan

Defines `LirRelationExpr` (the LIR plan type) and `LirRelationNode` (its node enum), along with `AvailableCollections` (which arrangements and raw forms a plan node produces), `GetPlan` (how a Get node reads a collection), `LirId` (a unique node identifier), and `LoweringMetrics` (Prometheus counters collected during MIR-to-LIR lowering). Fields that previously held `MirScalarExpr` or `MapFilterProject` now hold `LirScalarExpr` and `MfpPlan<LirScalarExpr>` / `SafeMfpPlan<LirScalarExpr>` respectively; `AvailableCollections::arranged` stores `Vec<(Vec<LirScalarExpr>, Vec<usize>, Vec<usize>)>`.
`LoweringMetrics` is registered into a `MetricsRegistry` via `LoweringMetrics::register_into`. It exposes `inc_literal_constraints(case: &str)` to record successful `MapFilterProject::literal_constraints` calls by call site. The metric name is `mz_optimizer_lowering_literal_constraints_total`.
`LirRelationExpr::finalize_dataflow` is the entry point for the full MIR→LIR pipeline: it lowers the dataflow (selecting monotonic operator variants for single-time dataflows during lowering itself), refines source MFPs, and for single-time dataflows relaxes `must_consolidate` flags via `RelaxMustConsolidate`. It accepts an `Option<&LoweringMetrics>` that is forwarded to the lowering `Context`.
`LirRelationExpr::pretty` and `LirRelationExpr::debug_explain` provide text rendering for debugging and tests.
Sub-modules: `lowering` (MIR→LIR), `render_plan` (flat node-ID representation for rendering), `join`, `reduce`, `scalar` (LIR scalar expression types and MIR→LIR conversion helpers), `threshold`, `top_k`, `interpret`, and `transform`.
