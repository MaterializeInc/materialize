---
source: src/cluster-controller/src/lib.rs
revision: 73111c3e52
---

# mz-cluster-controller

Pure, stateless reconciler for the replica set of every managed cluster. The controller is a **reconciler**: each tick it reads desired cluster state and live signals through the [`ClusterControllerCtx`](ctx.md) boundary, runs a set of pure [`Strategy`](strategy.md) implementations, unions their desired contributions, diffs that against the actual replica set, and emits the create/drop and durable-state-write `Decision`s that close the gap. It holds no in-memory state between ticks; the source of truth is always the catalog plus live signals, pulled fresh each tick.

The crate depends only on primitive id/shape types and the `ClusterControllerCtx` trait, never on the adapter or catalog. That boundary makes the controller testable against a fake implementation.

Key modules:

* `ctx` — The `ClusterControllerCtx` trait (the pull/apply boundary), `ClusterState`, `StateWrite`, `Decision`, `ApplyOutcome`, `ObservedReplica`, and the compare-and-append witness types re-exported from `mz-adapter-types`.
* `strategy` — The `Strategy` trait (two pure functions per tick), `DesiredReplica`, and `BaselineStrategy` (the always-present implicit strategy that holds the steady-state replica set).

Key types defined in `lib.rs`:

* `ClusterController` — holds the strategy set and drives `reconcile` ticks.
* `reconcile` — two-phase tick: phase 1 (`update_state`) merges and applies durable writes per cluster; phase 2 (`desired_replicas`) diffs the unified desired set against the actual replicas and emits creates/drops.
* `merge_state_writes` — disjoint-union join of per-strategy `StateWrite`s under a conflict alarm via `soft_panic_or_log!`.
* `reconcile_replicas` — pure multiset union/diff kernel that closes the replica gap.
* `ReplicaNameGen` — generates deterministic fresh replica names past the highest observed `rNN` index.

Key dependencies: `mz-adapter-types` (witness types), `mz-compute-types` (replica logging config), `mz-controller-types` (cluster/replica IDs), `mz-repr` (timestamps), `mz-ore`.
