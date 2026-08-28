---
source: src/compute-types/src/plan.rs
revision: c6be08fe4f
---

# compute-types::plan

Defines `LirRelationExpr` (the LIR plan type) and `LirRelationNode` (its node enum), along with `AvailableCollections` (which arrangements and raw forms a plan node produces), `GetPlan` (how a Get node reads a collection), `ArrangementStrategy` (whether an `ArrangeBy` forms arrangements directly or via temporal bucketing), `LirId` (a unique node identifier), and `LoweringMetrics` (Prometheus counters collected during MIR-to-LIR lowering). Scalar expressions in plan nodes use `LirScalarExpr` and `MfpPlan<LirScalarExpr>` / `SafeMfpPlan<LirScalarExpr>`; `AvailableCollections::arranged` stores `Vec<(Vec<LirScalarExpr>, Vec<usize>, Vec<usize>)>`.
`LIR_VERSION` is a `u64` constant tracking the stable serialization schema version of `LirRelationExpr`. `ConstantRows` is a mirror enum for `LirRelationNode::Constant`'s rows field, used in place of `std::result::Result` so that the stable LIR schema registry can map each container name to a single format without clashing with the `Result` instantiation in `LirScalarExpr::Literal`.
`LoweringMetrics` is registered into a `MetricsRegistry` via `LoweringMetrics::register_into`. It exposes `inc_literal_constraints(case: &str)` to record successful `MapFilterProject::literal_constraints` calls by call site. The metric name is `mz_optimizer_lowering_literal_constraints_total`.
`LirRelationExpr::finalize_dataflow` is the entry point for the full MIR→LIR pipeline: it lowers the dataflow (selecting monotonic operator variants for single-time dataflows during lowering itself), refines source MFPs, and for single-time dataflows relaxes `must_consolidate` flags via `RelaxMustConsolidate` and then applies a delta-join one-shot optimization. The one-shot pass truncates each non-recursive delta join to its first path (the only path that produces updates at a single time) and, where the surviving path's source was backed by a bespoke `ArrangeBy`, converts that input to a raw collection and rewrites the initial closure to address the raw row layout instead of the arranged `(key, value)` layout. It accepts an `Option<&LoweringMetrics>` that is forwarded to the lowering `Context`.
`LirRelationExpr::pretty` and `LirRelationExpr::debug_explain` provide text rendering for debugging and tests.
Sub-modules: `lowering` (MIR→LIR), `render_plan` (flat node-ID representation for rendering), `join`, `reduce`, `scalar` (LIR scalar expression types and MIR→LIR conversion helpers), `threshold`, `top_k`, `interpret`, and `transform`.
