---
source: src/cluster-controller/src/lib.rs
revision: 74f18a3354
---

# mz-cluster-controller

Pure, stateless reconciler for the replica set of every managed cluster. The controller is a **reconciler**: each tick it reads desired cluster state and live signals through the [`ClusterControllerCtx`](ctx.md) boundary, runs a set of pure [`Strategy`](strategy.md) implementations, unions their desired contributions, diffs that against the actual replica set, and emits the create/drop and durable-state-write `Decision`s that close the gap. It holds no in-memory state between ticks; the source of truth is always the catalog plus live signals, pulled fresh each tick.

The crate depends only on primitive id/shape types and the `ClusterControllerCtx` trait, never on the adapter or catalog. That boundary makes the controller testable against a fake implementation.

Key modules:

* `ctx` — The `ClusterControllerCtx` trait (the pull/apply boundary), `ClusterState`, `StateWrite`, `Decision`, `ApplyOutcome`, `ObservedReplica`, `ReconfigurationWrite`, `BurstWrite`, and the compare-and-append witness types re-exported from `mz-adapter-types`.
* `strategy` — The `Strategy` trait (three pure functions per tick plus `signal_request`), `DesiredReplica`, `SignalRequest`, `ConfigSignals`, `LiveSignals`, `BaselineStrategy` (the always-present implicit strategy that holds the steady-state replica set), `GracefulReconfigurationStrategy` (engaged while a reconfiguration record is in progress), and `HydrationBurstStrategy` (engaged for clusters with an `ON HYDRATION` auto-scaling policy).

Key types defined in `lib.rs`:

* `ClusterController` — holds the strategy set (baseline, graceful-reconfiguration, hydration-burst) and a `ConfigSet` handle for dyncfgs; drives `reconcile` ticks.
* `reconcile` — two-phase tick: phase 1 (`update_state`) fetches live signals, merges and applies durable writes per cluster; phase 2 (`desired_replicas`) diffs the unified desired set against the actual replicas and emits creates/drops. A `ResourceExhausted` outcome from phase 2 triggers `shed_decision`.
* `merge_state_writes` — disjoint-union join of per-strategy `StateWrite`s under a conflict alarm via `soft_panic_or_log!`.
* `reconcile_replicas` — pure multiset union/diff kernel that closes the replica gap; only controller-owned replicas (those passing `ObservedReplica::owned_shape`) participate in the diff.
* `ReplicaNameGen` — generates deterministic fresh replica names past the highest observed `rNN` index.
* `shed_decision` — emits an `UpdateClusterState` that marks the graceful reconfiguration as `ResourceExhausted` when a phase-2 apply is rejected for exceeding the resource budget.
* `config_signals` — latches `ConfigSignals` from the controller's `ConfigSet` once per tick, so every strategy evaluates against a consistent environment-wide config.

Key dependencies: `mz-adapter-types` (witness types and dyncfgs), `mz-compute-types` (replica logging config), `mz-controller-types` (cluster/replica IDs), `mz-repr` (timestamps), `mz-dyncfg`, `mz-ore`.
