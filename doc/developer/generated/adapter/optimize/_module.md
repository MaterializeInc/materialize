---
source: src/adapter/src/optimize.rs
revision: 3df8ae2fd8
---

# adapter::optimize

Defines the high-level optimizer interface used throughout the coordinator: the `Optimize<From>` trait (one implementation per statement type and pipeline stage), `OptimizerConfig` (feature flags and execution mode), `OptimizerCatalog` (a minimal catalog view for the optimizer), and `OptimizerError` (aggregated error type for all optimization failures).
Each child module (`peek`, `index`, `materialized_view`, `subscribe`, `copy_to`, `view`) implements the full optimization pipeline for one statement type as a sequence of `Optimize` impls that transform opaque stage-result structs; `dataflows` provides the shared `DataflowBuilder` utility.
`OptimizerError` includes a `RestrictedFunction(UnmaterializableFunc)` variant for unmaterializable functions blocked by the `restrict_to_user_objects` session variable; it carries a hint directing users to contact their administrator.
`OptimizerConfig` can be overridden from `ExplainContext` (for `EXPLAIN ... WITH(...)`) or from `OptimizerFeatureOverrides` (for `CLUSTER ... FEATURES(...)`), keeping the optimizer API stable while allowing ad-hoc experimentation.
