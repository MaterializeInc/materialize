---
source: src/adapter/src/coord/sequencer/inner/subscribe.rs
revision: 070b7cdfb8
---

# adapter::coord::sequencer::inner::subscribe

Implements the `Staged` trait for `SubscribeStage`, which drives the multi-stage subscribe sequencing pipeline (`OptimizeMir`, `LinearizeTimestamp`, `TimestampOptimizeLir`, `Finish`, `Explain`) analogously to peek sequencing.
The `LinearizeTimestamp` stage runs `subscribe_linearize_timestamp` off the coordinator loop via `spawn_linearized_read_ts`, carrying the resulting `oracle_read_ts` forward to `TimestampOptimizeLir`.
Creates an `ActiveSubscribe` entry in the coordinator's sink tracking, then installs the subscribe dataflow (via `implement_subscribe`, which accepts `DataflowDescription<LirRelationExpr>`) on the compute cluster using `try_ship_dataflow`. If shipping fails (for example, because a dependency was dropped between sequencing on the session task and dispatch to the coordinator), `implement_subscribe` removes the registered active compute sink to prevent a panic on connection teardown, and returns a `ConcurrentDependencyDrop` error. On success it returns the `RowBatchStream` receiver to pgwire.
The optimizer config is built by layering cluster features, then cluster-coherent scoped overrides (`Coordinator::cluster_scoped_optimizer_overrides`), over the base `OptimizerConfig`.
`sequence_subscribe` (via `implement_subscribe`) sets `internal: false` on the constructed `ActiveSubscribe`; internal subscribes that should be hidden from `mz_subscriptions` use a separate code path that sets `internal: true`.
