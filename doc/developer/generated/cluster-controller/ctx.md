---
source: src/cluster-controller/src/ctx.rs
revision: 8598d82c1c
---

# cluster-controller::ctx

The boundary between the controller and its environment.

`ClusterControllerCtx` is the single, strategy-agnostic interface through which the controller pulls tick signals and applies catalog mutations. The controller crate depends on exactly this trait. Reads are batched and pulled on demand; the single write applies a tick's batch under compare-and-append guards. The Coordinator implements this trait, which makes the controller testable against a fake implementation.

Key types:

* `ClusterControllerCtx` (trait) — five async methods: `now`, `managed_cluster_ids`, `cluster_states`, `hydrated_replicas`, `apply`. Reads are batched to bound round-trips in a separate-task deployment. `hydrated_replicas` returns the subset of given replicas that have all current collections hydrated; it is only called when a strategy declares it needs hydration via `SignalRequest`.
* `ClusterState` — durable config plus observed replicas of one managed cluster for one tick. Carries `cluster_id`, `size`, `replication_factor`, `availability_zones`, `logging`, optional `reconfiguration` and `burst` records, and `replicas`. Unmanaged clusters are not represented.
* `ObservedReplica` — a replica that actually exists: `replica_id`, `name`, `shape`.
* `StateWrite` — the durable mutations a strategy's `update_state` requests: cut-overs (`new_size`, `new_replication_factor`, `new_availability_zones`, `new_logging`) and record writes/clears (`reconfiguration`, `burst`). `None` fields are no-ops.
* `ReconfigurationWrite` — a write to the `reconfiguration` record bundled with the `ReconfigurationAudit` lifecycle intent, so a writer cannot move the record without simultaneously declaring what the audit trail should say.
* `BurstWrite` — analogous bundle for the `burst` record.
* `Decision` — a single command the controller emits: `CreateReplica`, `DropReplica`, or `UpdateClusterState`. Every variant carries an `ExpectedClusterState` for compare-and-append; the apply path rejects the whole batch if any target cluster's state has since diverged.
* `ApplyOutcome` — `Applied`, `Rejected` (at least one compare-and-append guard failed), or `ResourceExhausted` (the batch exceeded the environment's resource budget; nothing was transacted).

The compare-and-append witness types (`ExpectedClusterState`, `ReplicaShape`, `AvailabilityZones`, `BurstRecord`, `BurstAudit`, `BurstFinishCause`, `ReconfigurationRecord`, `ReconfigurationAudit`, `ReconfigurationStatus`, `ReconfigurationTarget`, `OnTimeout`) are re-exported from `mz-adapter-types::cluster_state` so the catalog transaction that applies a decision can share them without depending on this crate.
