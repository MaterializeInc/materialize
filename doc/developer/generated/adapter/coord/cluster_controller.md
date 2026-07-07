---
source: src/adapter/src/coord/cluster_controller.rs
revision: 73111c3e52
---

# adapter::coord::cluster_controller

Driver and glue for the `mz-cluster-controller` reconciler.

The controller crate is pure and knows nothing about the Coordinator. This module is the half of the `ClusterControllerCtx` boundary that does: it runs the controller as a separate task and implements the ctx by marshaling each pull/apply to the Coordinator over the internal command channel, because the catalog and live compute/storage signals are reachable only from the coordinator loop. Pulls are batched so the round-trip count per tick is bounded.

Everything here is gated by `ENABLE_CLUSTER_CONTROLLER` (default off). With the gate off the task does not tick, so the legacy scheduling and graceful paths remain the sole writers of the replica set. With the gate on the controller owns the *user* managed-cluster replica set; the legacy entry points no-op. System/builtin clusters are never controller-owned: the catalog's bootstrap migration owns their replicas.

Key types and functions:

* `ClusterControllerRequest` — the enum of requests the controller task marshals to the Coordinator: `ManagedClusterIds`, `ClusterStates` (batched read plus `now`), `Apply` (tick's decision batch), `TickInterval`. Each variant carries a oneshot for the reply.
* `CoordCtx` — the controller-task side of the `ClusterControllerCtx` boundary. Marshals every ctx call to the Coordinator via `internal_cmd_tx`. Latches `now` from the most recent `ClusterStates` reply so both phases of a tick see a consistent time.
* `Coordinator::spawn_cluster_controller_task` — spawns the controller task. The task loops: reads the tick interval via a `TickInterval` request, sleeps, then calls `ClusterController::reconcile` if the gate is on. Called once at bootstrap.
* `Coordinator::handle_cluster_controller_request` — the Coordinator-side handler for `Message::ClusterControllerRequest`. Dispatches each variant to the appropriate catalog/controller reads or applies the decision batch via `apply_cluster_controller_decisions`.
* `Coordinator::apply_cluster_controller_decisions` — transacts a decision batch: for each `Decision`, checks `check_cluster_state` and, if the state still matches, creates or drops the replica or writes the state. Returns `ApplyOutcome::Rejected` if any check fails, `Applied` otherwise.
