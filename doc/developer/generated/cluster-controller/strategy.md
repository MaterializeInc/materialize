---
source: src/cluster-controller/src/strategy.rs
revision: 38a95cefe2
---

# cluster-controller::strategy

The pure strategy interface and built-in strategy implementations.

A strategy is a pair of pure functions over `(observed cluster state, live signals, config signals, now)`:

* `Strategy::update_state` returns the durable writes the strategy wants (config cut-overs, record writes/clears). The controller transacts these in the tick's first phase.
* `Strategy::desired_replicas` returns the replica slots the strategy contributes to the cluster's desired set. The controller unions every strategy's contribution in the tick's second phase.

Both are pure: same inputs, same output, no I/O. Strategies never touch `ClusterControllerCtx` directly. They declare the live signals they need via `Strategy::signal_request` (a pure function of durable state and `ConfigSignals`) and the controller fetches those before evaluating them.

Key types:

* `Strategy` (trait) — `signal_request` (default: no signals), `update_state` (default: no write), `desired_replicas`. All methods receive both `&LiveSignals` and `&ConfigSignals`. `Send + Sync` so the controller can hold boxed strategies on its own task.
* `DesiredReplica` — a replica slot a strategy desires this tick, characterized by a `ReplicaShape` and a `CreateReason` (the audit attribution this slot contributes; the highest-precedence reason among all slots for a shape wins).
* `SignalRequest` — declares which live signals a strategy needs for a cluster this tick: `hydration: bool` (probe per-replica hydration), `hydratable_objects: bool` (check whether the cluster has at least one dataflow-backed object), and `refresh_window: bool` (pull bound REFRESH MV frontiers, schedules, and the current read timestamp). The controller unions requests across strategies and fetches only what is needed.
* `ConfigSignals` — environment-wide dyncfg values latched by the kernel once per tick: `burst_enabled` (the break-glass flag for the hydration-burst strategy) and `default_burst_linger` (the system-default linger duration written into new burst records when the policy omits one). Not durable state, so never witness material.
* `LiveSignals` — the fulfilled live signals for one cluster: `hydrated_replicas` is the set of replica IDs (among controller-owned replicas) that are online and have all current collections hydrated; `has_hydratable_objects` is whether the cluster has at least one dataflow-backed object; `refresh_window` is the `RefreshWindowInputs` for a scheduled cluster (`None` when not requested or when the cluster was no longer scheduled at pull time). A signal not requested by any strategy is left at its empty default.
* `BaselineStrategy` — the always-present implicit strategy. Desires `replication_factor` replicas at the cluster's realized shape, but only for MANUAL clusters. On a scheduled cluster the on-refresh strategy owns the replica set and the baseline contributes nothing. With only the baseline engaged on a MANUAL cluster the desired set equals the realized set, so a steady-state managed cluster reconciles to no decisions.
* `GracefulReconfigurationStrategy` — engaged whenever the durable `reconfiguration` record is in progress. Desires `target.replication_factor` replicas at the target shape in addition to the baseline's realized-shape replicas. Once enough target replicas are hydrated, `update_state` cuts over (advances the realized config, marks the record finalized). On a timeout, behavior is governed by the record's `on_timeout`: `Commit` cuts over to the un-hydrated target, `Rollback` (the default) marks the record timed out and stops desiring the target replicas.
* `OnRefreshStrategy` — engaged for clusters with a non-MANUAL `ClusterSchedule`. Contributes one replica at the cluster's realized shape while the cluster is inside a refresh window (determined from `RefreshWindowInputs`), and nothing otherwise. Also normalizes the realized `replication_factor` to `0` via `update_state` so the baseline does not desire replicas on a scheduled cluster.
* `HydrationBurstStrategy` — engaged for clusters whose `AUTO SCALING STRATEGY` includes an `ON HYDRATION` policy, provided the global break-glass dyncfg is on and the cluster is On (`replication_factor > 0`). While at least one hydratable object exists that no steady-state (realized-config) replica has hydrated, it arms a durable `burst` record and desires one extra replica at the policy's `HYDRATION SIZE`. Once the steady set hydrates, the burst replica lingers for `linger_duration` then the record is cleared and the replica dropped. A burst record no longer consistent with the current policy (size changed, policy removed, or cluster turned off) is torn down immediately via `update_state`. There is no TTL: if the steady set never hydrates, the burst replica runs indefinitely. Burst coexists with a graceful reconfiguration without suppression.
