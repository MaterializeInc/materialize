---
source: src/adapter/src/coord/sequencer/inner/subscribe.rs
revision: e926ec3a86
---

# adapter::coord::sequencer::inner::subscribe

Implements the `Staged` trait for `SubscribeStage`, which drives the multi-stage subscribe sequencing pipeline (`OptimizeMir`, `TimestampOptimizeLir`, `Finish`, `Explain`) analogously to peek sequencing.
Creates an `ActiveSubscribe` entry in the coordinator's sink tracking, installs the subscribe dataflow (via `implement_subscribe`, which accepts `DataflowDescription<LirRelationExpr>`) on the compute cluster, and returns the `RowBatchStream` receiver to pgwire.
The optimizer config is built by layering cluster features, then cluster-coherent scoped overrides (`Coordinator::cluster_scoped_optimizer_overrides`), over the base `OptimizerConfig`.
`sequence_subscribe` (via `implement_subscribe`) sets `internal: false` on the constructed `ActiveSubscribe`; internal subscribes that should be hidden from `mz_subscriptions` use a separate code path that sets `internal: true`.
