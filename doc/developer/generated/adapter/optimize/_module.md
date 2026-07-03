---
source: src/adapter/src/optimize.rs
revision: e926ec3a86
---

# adapter::optimize

Defines the high-level optimizer interface used throughout the coordinator: the `Optimize<From>` trait (one implementation per statement type and pipeline stage), `OptimizerConfig` (feature flags and execution mode), `OptimizerCatalog` (a minimal catalog view for the optimizer), and `OptimizerError` (aggregated error type for all optimization failures).
Each child module (`peek`, `index`, `materialized_view`, `subscribe`, `copy_to`, `view`) implements the full optimization pipeline for one statement type as a sequence of `Optimize` impls that transform opaque stage-result structs; `dataflows` provides the shared `DataflowBuilder` utility.
`OptimizerError` includes a `RestrictedFunction(UnmaterializableFunc)` variant for unmaterializable functions blocked by the `restrict_to_user_objects` session variable; it carries a hint directing users to contact their administrator.
`OptimizerConfig` can be overridden from `ExplainContext` (for `EXPLAIN ... WITH(...)`) or from `OptimizerFeatureOverrides` (for `CLUSTER ... FEATURES(...)`), keeping the optimizer API stable while allowing ad-hoc experimentation.
`PeekOptimizer` is an enum that carries either a `peek::Optimizer` (for `SELECT` and `EXPLAIN`) or a `copy_to::Optimizer` (for `COPY TO`) through the shared peek sequencing state machine. Both statement types go through identical surrounding stages (timestamp selection, read holds, off-thread optimization), and `PeekOptimizer` lets that shared machinery carry either optimizer without caring which one it is. The companion `PeekGlobalLirPlan` enum tags the optimization result with the same two variants so that downstream stages can recover the concrete plan type. The internal `LirDataflowDescription` type alias resolves to `DataflowDescription<LirRelationExpr>`. The shared optimization pipeline steps (HIR-to-local-MIR, timestamp resolution, global-MIR-to-LIR) are factored into the `pub(crate) optimize_oneshot` helper, which is also used by the frontend peek path in `frontend_peek.rs`.
