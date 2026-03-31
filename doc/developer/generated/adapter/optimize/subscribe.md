---
source: src/adapter/src/optimize/subscribe.rs
revision: f7c755e1ed
---

# adapter::optimize::subscribe

Implements the optimizer pipeline for `SUBSCRIBE` statements, following the same two-stage pattern as peek (local then global MIR → LIR) but targeting a subscribe sink description rather than a peek plan.
The pipeline handles the optional `WITH SNAPSHOT` flag and snapshot optimisation via the `SUBSCRIBE_SNAPSHOT_OPTIMIZATION` dyncfg.
