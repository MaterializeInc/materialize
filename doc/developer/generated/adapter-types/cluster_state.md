---
source: src/adapter-types/src/cluster_state.rs
revision: 73111c3e52
---

# adapter-types::cluster_state

Plain-data mirror of a managed cluster's durable configuration.

These types carry the slices of a managed cluster's config that a caller reasons over without touching the catalog or SQL layers. Keeping them free of catalog and SQL dependencies lets one component reason over the config and another project the live config onto the same types, without either depending on the other.

Key types:

* `ExpectedClusterState` — compare-and-append witness over a managed cluster's durable config. A caller captures it from a config snapshot and pairs it with a conditional write. The applier applies the write only if the cluster's current config still projects to an equal witness. Fields: `size`, `replication_factor`, `availability_zones`, `logging`, `reconfiguration`, `burst`.
* `ReplicaShape` — the config dimensions that distinguish one replica from another. Two replicas with equal shape are interchangeable. `ReplicaShape::matches` compares availability zones as unordered pools so a reorder alone does not force a reprovision.
* `AvailabilityZones` — the availability zones in configured provisioning order. Order is significant (the orchestrator round-robins placement across the list) and is part of the `ExpectedClusterState` witness. `AvailabilityZones::pool` converts to an unordered `AvailabilityZonePool` for interchangeability checks.
* `AvailabilityZonePool` — unordered set of zones produced by `AvailabilityZones::pool`. Used for the one comparison that must ignore order: replica interchangeability.
* `ReconfigurationRecord` — in-flight graceful reconfiguration record: `target` and `deadline`.
* `ReconfigurationTarget` — the full config shape a reconfiguration is moving to: `size`, `replication_factor`, `availability_zones`, `logging`.
* `BurstRecord` — active hydration-burst record: `burst_size`, `linger_duration`, `steady_hydrated_at`.
