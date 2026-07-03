---
source: src/cluster-controller/src/strategy.rs
revision: 73111c3e52
---

# cluster-controller::strategy

The pure strategy interface and built-in strategy implementations.

A strategy is two pure functions over `(observed cluster state, now)`:

* `Strategy::update_state` returns the durable writes the strategy wants (config cut-overs, record writes/clears). The controller transacts these in the tick's first phase.
* `Strategy::desired_replicas` returns the replica slots the strategy contributes to the cluster's desired set. The controller unions every strategy's contribution in the tick's second phase.

Both are pure: same inputs, same output, no I/O. Strategies never touch `ClusterControllerCtx`; the controller assembles their inputs by pulling through it.

Key types:

* `Strategy` (trait) — `name() -> &'static str`, `update_state` (default: no write), `desired_replicas`. `Send + Sync` so the controller can hold boxed strategies on its own task.
* `DesiredReplica` — a replica slot a strategy desires this tick, characterized by a `ReplicaShape`.
* `BaselineStrategy` — the always-present implicit strategy. Desires `replication_factor` replicas at the cluster's realized shape. With only the baseline engaged the desired set equals the realized set, so a steady-state managed cluster reconciles to no decisions. The baseline is what makes other strategies purely additive.
* `BASELINE_STRATEGY_NAME` — the audit-attribution string `"baseline"`.
