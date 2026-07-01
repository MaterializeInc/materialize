---
source: src/adapter/src/coord/sequencer/inner/create_materialized_view.rs
revision: 277b33e9c0
---

# adapter::coord::sequencer::inner::create_materialized_view

Implements `sequence_create_materialized_view`, which runs the full materialized-view optimizer pipeline (MIR then LIR), persists the catalog entry with the resulting optimized expression, and installs the dataflow on the target cluster.
Optimizer notices are rendered (via `CatalogState::render_notices_core` with an `ExprHumanizerExt` that resolves the new MV's own `global_id`) and the expression cache is populated before the catalog transaction, so that the durable cache is visible to other processes as soon as the item appears. Raw notices are emitted to the user session only after the catalog transaction succeeds.
Handles `AS OF` clause and refresh schedule validation; also provides `sequence_alter_materialized_view_apply_replacement` for zero-downtime MV replacement.
The optimizer config is built by layering cluster features (`ClusterConfig::features()`), then cluster-coherent scoped overrides (`Coordinator::cluster_scoped_optimizer_overrides`), over the base `OptimizerConfig`. This layering applies at both the initial optimization stage and the replacement apply path.
`PlanValidity` is constructed with the resolved dependency IDs, target cluster, target replica, and role metadata so that concurrent drops (e.g., `ALTER CLUSTER` racing the off-thread optimizer) are caught between stages instead of panicking during catalog application.
