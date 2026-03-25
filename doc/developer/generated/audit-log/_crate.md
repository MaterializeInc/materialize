---
source: src/audit-log/src/lib.rs
revision: 4267863081
---

# mz-audit-log

Provides versioned, type-safe data structures for Materialize's audit log, which records user-initiated catalog events for consumption via `mz_catalog.mz_audit_events` and by the cloud management layer.

## Purpose

The crate exists to decouple event producers from consumers across process boundaries.
All event types and their detail payloads are defined here and serialized as JSON, so that schema changes require adding a new version variant rather than modifying existing structs — ensuring backward compatibility with stored events.

## Module structure

The crate is a single `lib.rs` with no submodules.
It defines two independent versioned hierarchies:

* **Audit events** — `VersionedEvent` (currently `V1`) wraps `EventV1`, which carries an `EventType` (Create/Drop/Alter/Grant/Revoke/Comment), an `ObjectType` (Cluster, Table, Role, …), and an `EventDetails` variant holding the event-specific payload struct.
* **Storage usage snapshots** — `VersionedStorageUsage` (currently `V1`) wraps `StorageUsageV1`, recording per-shard byte sizes at a point in time.

Each `EventDetails` variant is a versioned struct (e.g. `CreateClusterReplicaV1` through `V4`).
All types derive `Serialize`/`Deserialize` with stable JSON representations; the test in `lib.rs` hard-codes expected bytes to prevent accidental schema drift.

## Key types

* `VersionedEvent` — top-level event envelope; use `VersionedEvent::new` to create and `serialize`/`deserialize` for I/O.
* `EventDetails` — exhaustive enum of all event payloads; `as_json()` produces the detail object stored in the catalog table.
* `VersionedStorageUsage` — envelope for periodic storage-usage records.

## Dependencies

* `mz-ore` — timestamp type (`EpochMillis`) and test utilities.
* `serde` / `serde_json` / `serde_plain` — JSON serialization and `Display` derivation for enums.
* `proptest` / `proptest-derive` — property-based test support (`Arbitrary` impls on all types).

## Downstream consumers

Consumed by the catalog implementation (which persists events), the adapter layer (which produces events), and the cloud management layer (billing/introspection).
