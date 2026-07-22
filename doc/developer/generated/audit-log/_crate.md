---
source: src/audit-log/src/lib.rs
revision: 8598d82c1c
---

# mz-audit-log

Provides versioned, type-safe data structures for Materialize's audit log, which records user-initiated catalog events for consumption via `mz_catalog.mz_audit_events` and by the cloud management layer.

## Purpose

The crate exists to decouple event producers from consumers across process boundaries.
All event types and their detail payloads are defined here and serialized as JSON, so that schema changes require adding a new version variant rather than modifying existing structs — ensuring backward compatibility with stored events.

## Module structure

The crate is a single `lib.rs` with no submodules.
It defines two independent versioned hierarchies:

* **Audit events** — `VersionedEvent` (currently `V1`) wraps `EventV1`, which carries an `EventType` (Create/Drop/Alter/Grant/Revoke/Comment), an `ObjectType` (Cluster, ClusterReplica, Connection, ContinualTask, Database, Func, Index, MaterializedView, NetworkPolicy, Role, Secret, Schema, Sink, Source, System, Table, Type, View), and an `EventDetails` variant holding the event-specific payload struct.
* **Storage usage snapshots** — `VersionedStorageUsage` (currently `V1`) wraps `StorageUsageV1`, recording per-shard byte sizes at a point in time.

Each `EventDetails` variant is a versioned struct (e.g. `CreateClusterReplicaV1` through `V4`).
The `CreateRoleV1` variant records role creation events with an optional `auto_provision_source` field.
The `AlterAddColumnV1` variant records `ALTER TABLE ADD COLUMN` events, carrying the table `id`, `column` name, `column_type`, and `nullable` flag.
The `AlterClusterReconfigurationV1` variant records lifecycle transitions of background cluster reconfigurations (background `ALTER CLUSTER`), using the `ReconfigurationLifecycleV1` enum (`Started`, `Finalized`, `TimedOut`, `Cancelled`, `ResourceExhausted`). The struct carries `cluster_id`, `cluster_name`, `transition`, optional `forced` flag (set on `Finalized` to indicate a forced `ON TIMEOUT COMMIT` cut-over), `target_size`, `target_replication_factor`, `target_availability_zones`, `target_logging` (`ClusterReplicaLoggingV1`), and optional `deadline`. `ClusterReplicaLoggingV1` mirrors the durable `ReplicaLogging` with `log_logging` and `interval` fields.
The `ClusterHydrationBurstV1` variant records lifecycle transitions of controller-initiated hydration bursts, using the `HydrationBurstLifecycleV1` enum (`Started`, `Finished`). The struct carries `cluster_id`, `cluster_name`, `transition`, optional `finish_cause` (`BurstFinishCauseV1`: `LingerElapsed` or `NoLongerWarranted`), and `burst_size`.
The `CreateOrDropClusterReplicaReasonV1` enum records the reason for replica creation/drop: `Manual`, `Schedule`, `System`, `Reconfiguration`, `HydrationBurst`, or `Retired`.
All types derive `Serialize`/`Deserialize` with stable JSON representations; the test in `lib.rs` hard-codes expected bytes to prevent accidental schema drift.

## Key types

* `VersionedEvent` — top-level event envelope; use `VersionedEvent::new` to create and `serialize`/`deserialize` for I/O.
* `EventDetails` — exhaustive enum of all event payloads; `as_json()` produces the detail object stored in `mz_catalog.mz_audit_events.details`. The proto twin is `mz_catalog_protos::objects::audit_log_event_v1`, which `parse_catalog_audit_log_details` (in `src/expr/src/scalar/func/impls/jsonb.rs`) reshapes back into `as_json`'s output format. The round-trip is covered by a property test in `src/catalog/tests/audit_log_details.rs`.
* `CreateRoleV1` — audit detail for role creation, carrying `id`, `name`, and optional `auto_provision_source`.
* `AlterClusterReconfigurationV1` — audit detail for a background cluster reconfiguration lifecycle transition, carrying `cluster_id`, `cluster_name`, `transition` (`ReconfigurationLifecycleV1`), optional `forced`, target shape fields (`target_size`, `target_replication_factor`, `target_availability_zones`, `target_logging`), and optional `deadline`.
* `ClusterHydrationBurstV1` — audit detail for a hydration burst lifecycle transition, carrying `cluster_id`, `cluster_name`, `transition` (`HydrationBurstLifecycleV1`), optional `finish_cause` (`BurstFinishCauseV1`), and `burst_size`.
* `VersionedStorageUsage` — envelope for periodic storage-usage records.

## Dependencies

* `mz-ore` — timestamp type (`EpochMillis`) and test utilities.
* `serde` / `serde_json` / `serde_plain` — JSON serialization and `Display` derivation for enums.
* `proptest` / `proptest-derive` — property-based test support (`Arbitrary` impls on all types).

## Downstream consumers

Consumed by the catalog implementation (which persists events), the adapter layer (which produces events), and the cloud management layer (billing/introspection).
