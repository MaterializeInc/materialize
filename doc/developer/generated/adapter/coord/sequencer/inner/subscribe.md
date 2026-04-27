---
source: src/adapter/src/coord/sequencer/inner/subscribe.rs
revision: f113e73682
---

# adapter::coord::sequencer::inner::subscribe

Implements the `Staged` trait for `SubscribeStage`, which drives the multi-stage subscribe sequencing pipeline (`OptimizeMir`, `TimestampOptimizeLir`, `Finish`, `Explain`) analogously to peek sequencing.
Creates an `ActiveSubscribe` entry in the coordinator's sink tracking, installs the subscribe dataflow on the compute cluster, and returns the `RowBatchStream` receiver to pgwire.
