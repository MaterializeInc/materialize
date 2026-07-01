---
source: src/adapter/src/coord/caught_up.rs
revision: 9d0b66c63c
---

# adapter::coord::caught_up

Implements the "caught up" check used during zero-downtime deployments to determine when all collections have hydrated to a sufficient point before allowing the new environment to take over.
`CaughtUpCheckContext` holds per-collection hydration state and a `cluster_stability` map of `ClusterStabilityState` entries, one per cluster that is currently in a caught-up-and-healthy streak.
A point-in-time hydration check is not sufficient: a crash- or OOM-looping replica can momentarily look caught-up, so each tick's caught-up classification is gated by a stability window.
`ClusterStabilityState` tracks the streak across ticks: it records the wall-clock time the current streak began (`stable_since`), the maximum orchestrator-reported status-change timestamp seen on the previous tick (`last_status_change`), and the per-process restart count map from the previous tick (`last_restart_counts`).
`ClusterStabilityState::observe` folds in a `ClusterHealthSnapshot` (which captures `all_healthy`, `max_status_change`, and per-process `restart_counts` from the in-memory orchestrator mirror) and returns a `StabilityObservation` indicating whether the cluster has sustained the required period and, if not, which `StabilityBlocker` (`NotHealthy`, `StatusFlapped`, `Restarted`, or `WithinPeriod`) is holding it back.
Only clusters whose replicas are all `Online`, whose `max_status_change` has not advanced since the last tick, and whose per-process restart counts have not changed accumulate streak time; any disruption resets `stable_since` to `None`.
The `ENABLE_0DT_CAUGHT_UP_STABILITY_CHECK` dyncfg controls whether the stability gate is enforced; `WITH_0DT_CAUGHT_UP_CHECK_STABILITY_PERIOD` sets the required streak duration.
