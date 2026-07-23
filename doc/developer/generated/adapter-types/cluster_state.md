---
source: src/adapter-types/src/cluster_state.rs
revision: fca741734d
---

# adapter-types::cluster_state

Plain-data mirror of a managed cluster's durable configuration.

These types carry the slices of a managed cluster's config that a caller reasons over without touching the catalog or SQL layers. Keeping them free of catalog and SQL dependencies lets one component reason over the config and another project the live config onto the same types, without either depending on the other.

Key types:

* `ExpectedClusterState` — compare-and-append witness over a managed cluster's durable config. A caller captures it from a config snapshot and pairs it with a conditional write. The applier applies the write only if the cluster's current config still projects to an equal witness. Fields: `size`, `replication_factor`, `availability_zones`, `logging`, `auto_scaling_policy`, `reconfiguration`, `burst`.
* `ReplicaShape` — the config dimensions that distinguish one replica from another. Two replicas with equal shape are interchangeable. `ReplicaShape::matches` compares availability zones as unordered pools so a reorder alone does not force a reprovision.
* `AvailabilityZones` — the availability zones in configured provisioning order. Order is significant (the orchestrator round-robins placement across the list) and is part of the `ExpectedClusterState` witness. `AvailabilityZones::pool` converts to an unordered `AvailabilityZonePool` for interchangeability checks.
* `AvailabilityZonePool` — unordered set of zones produced by `AvailabilityZones::pool`. Used for the one comparison that must ignore order: replica interchangeability.
* `ReconfigurationRecord` — graceful reconfiguration record mirrored from durable state: `target`, `deadline`, `on_timeout`, `status`. `is_in_progress` returns true when `status` is `InProgress`.
* `ReconfigurationStatus` — lifecycle status of a graceful reconfiguration: `InProgress`, `Finalized`, `TimedOut`, `Cancelled`, `ResourceExhausted`.
* `ReconfigurationAudit` — the lifecycle transition a write to the `reconfiguration` record represents, declared by the writer at the decision point: `Started`, `Cancelled`, `Finalized { forced }`, `TimedOut`, `ResourceExhausted`.
* `OnTimeout` — the action a graceful reconfiguration applies once its deadline passes with the target not yet hydrated: `Commit` (cut over anyway) or `Rollback` (revert to pre-reconfiguration shape).
* `ReconfigurationTarget` — the full config shape a reconfiguration is moving to: `size`, `replication_factor`, `availability_zones`, `logging`. `shape()` returns the per-replica `ReplicaShape` (everything but `replication_factor`).
* `BurstRecord` — active hydration-burst record: `burst_size`, `linger_duration`, `steady_hydrated_at`.
* `BurstAudit` — lifecycle transition for a burst record: `Started` or `Finished { cause }`.
* `BurstFinishCause` — why a hydration burst finished: `LingerElapsed` or `NoLongerWarranted`.
* `AutoScalingPolicy` — the user-configured autoscaling policy of a managed cluster, mirrored from durable state. A plain-data mirror of `mz_sql::plan::AutoScalingStrategy`. Extensible: v1 carries only the `ON HYDRATION` burst sub-policy via `on_hydration: Option<OnHydrationPolicy>`.
* `OnHydrationPolicy` — the `ON HYDRATION` burst sub-policy: while some object on the cluster is not hydrated on a steady replica, run one extra replica at `hydration_size` to accelerate hydration, lingering for `linger_duration` after the steady set hydrates. `linger_duration: None` falls back to the system default linger when the burst strategy writes its record.
* `burst_record_warranted(record_size, replication_factor, hydration_size)` — the single shared definition of when an in-flight burst record is warranted: the cluster is on (replication factor nonzero) and the `ON HYDRATION` policy in force bursts at the record's size. Both the catalog and the cluster controller use this function so the two sides of the burst lifecycle cannot drift.
