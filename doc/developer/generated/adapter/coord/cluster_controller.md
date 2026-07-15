---
source: src/adapter/src/coord/cluster_controller.rs
revision: e68d42a2c4
---

# adapter::coord::cluster_controller

Driver and glue for the `mz-cluster-controller` reconciler.

The controller crate is pure and knows nothing about the Coordinator. This module is the half of the `ClusterControllerCtx` boundary that does: it runs the controller as a separate task and implements the ctx by marshaling each pull/apply to the Coordinator over the internal command channel, because the catalog and live compute/storage signals are reachable only from the coordinator loop. Pulls are batched so the round-trip count per tick is bounded.

Everything here is gated by `ENABLE_CLUSTER_CONTROLLER` (default off). With the gate off the task does not tick, so the legacy scheduling and graceful paths remain the sole writers of the replica set. With the gate on the controller owns the *user* managed-cluster replica set; the legacy entry points no-op. System/builtin clusters are never controller-owned: the catalog's bootstrap migration owns their replicas.

Key types and functions:

* `ClusterControllerRequest` — the enum of requests the controller task marshals to the Coordinator: `ManagedClusterIds`, `ClusterStates` (batched read; reply also carries `now`), `HydratedReplicas` (per-cluster live signal a strategy pulls on demand), `Apply` (tick's decision batch), `TickInterval`. Each variant carries a oneshot for the reply.
* `CoordCtx` — the controller-task side of the `ClusterControllerCtx` boundary. Marshals every ctx call to the Coordinator via `internal_cmd_tx`. Latches `now` from the most recent `ClusterStates` reply so both phases of a tick see a consistent time.
* `Coordinator::spawn_cluster_controller_task` — spawns the controller task. The task loops: reads the tick interval via a `TickInterval` request, sleeps, then calls `ClusterController::reconcile` if the gate is on. Called once at bootstrap.
* `Coordinator::handle_cluster_controller_request` — the Coordinator-side handler for `Message::ClusterControllerRequest`. Dispatches each variant to the appropriate catalog/controller reads or applies the decision batch via `apply_cluster_controller_decisions`. For `HydratedReplicas`, starts per-replica hydration checks on the coordinator loop (via `start_hydration_checks`) then waits for compute's replies off-loop in a spawned task.
* `Coordinator::start_hydration_checks` — checks storage and compute hydration for a set of replicas on a cluster. Returns only replicas that are already storage-hydrated and known to the compute controller. Replica-pinned materialized views (those with `IN CLUSTER ... REPLICA`) are excluded from the hydration check for replicas they are not pinned to, so a graceful reconfiguration does not wait forever for a new replica to hydrate an MV bound to a replica being replaced.
* `Coordinator::apply_cluster_controller_decisions` — transacts a decision batch: for each `Decision`, checks `check_cluster_state` and, if the state still matches, creates or drops the replica or writes the state. Returns `ApplyOutcome::Rejected` if any check fails, `ApplyOutcome::ResourceExhausted` if the batch exceeds the resource budget, and `Applied` otherwise. Create decisions carry a strategy attribution translated to a `ReplicaCreateDropReason` via `reason_from_strategies`: the graceful reconfiguration strategy maps to `GracefulReconfiguration`; all others map to `Manual`. `build_mutation_ops` (called from `apply_cluster_controller_decisions`) checks whether the target replica of a `DropReplica` decision still exists in the catalog before building the drop op. If it has already been removed (e.g. by a concurrent user `DROP CLUSTER`), the function returns `None` to reject the batch, avoiding a panic from resource-limit validation running against a missing replica.
