---
source: src/adapter/src/optimize/subscribe.rs
revision: b9097f8a3d
---

# adapter::optimize::subscribe

Implements the optimizer pipeline for `SUBSCRIBE` statements, following the same two-stage pattern as peek (local then global MIR → LIR) but targeting a subscribe sink description rather than a peek plan.
The optimizer accepts a full `SubscribePlan` and propagates the output row ordering into the `SubscribeSinkConnection`.
The pipeline handles the optional `WITH SNAPSHOT` flag and snapshot optimisation via the `SUBSCRIBE_SNAPSHOT_OPTIMIZATION` dyncfg.
