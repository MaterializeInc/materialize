---
source: src/cluster-controller/src/strategy.rs
revision: 8598d82c1c
---

# cluster-controller::strategy

The pure strategy interface and built-in strategy implementations.

A strategy is a pair of pure functions over `(observed cluster state, live signals, now)`:

* `Strategy::update_state` returns the durable writes the strategy wants (config cut-overs, record writes/clears). The controller transacts these in the tick's first phase.
* `Strategy::desired_replicas` returns the replica slots the strategy contributes to the cluster's desired set. The controller unions every strategy's contribution in the tick's second phase.

Both are pure: same inputs, same output, no I/O. Strategies never touch `ClusterControllerCtx` directly. They declare the live signals they need via `Strategy::signal_request` and the controller fetches those before evaluating them.

Key types:

* `Strategy` (trait) — `name() -> &'static str`, `signal_request` (default: no signals), `update_state` (default: no write), `desired_replicas`. `Send + Sync` so the controller can hold boxed strategies on its own task.
* `DesiredReplica` — a replica slot a strategy desires this tick, characterized by a `ReplicaShape`.
* `SignalRequest` — declares which live signals a strategy needs for a cluster this tick (currently only `hydration: bool`). The controller unions requests across strategies and fetches only what is needed.
* `LiveSignals` — the fulfilled live signals for one cluster: `hydrated_replicas` is the set of replica IDs that have all current collections hydrated. A signal not requested by any strategy is left at its empty default.
* `BaselineStrategy` — the always-present implicit strategy. Desires `replication_factor` replicas at the cluster's realized shape. With only the baseline engaged the desired set equals the realized set, so a steady-state managed cluster reconciles to no decisions. The baseline is what makes other strategies purely additive.
* `BASELINE_STRATEGY_NAME` — the audit-attribution string `"baseline"`.
* `GracefulReconfigurationStrategy` — engaged whenever the durable `reconfiguration` record is in progress. Desires `target.replication_factor` replicas at the target shape in addition to the baseline's realized-shape replicas. Once enough target replicas are hydrated, `update_state` cuts over (advances the realized config, marks the record finalized). On a timeout, behavior is governed by the record's `on_timeout`: `Commit` cuts over to the un-hydrated target, `Rollback` (the default) marks the record timed out and stops desiring the target replicas.
* `GRACEFUL_RECONFIGURATION_STRATEGY_NAME` — the audit-attribution string `"graceful-reconfiguration"`.
