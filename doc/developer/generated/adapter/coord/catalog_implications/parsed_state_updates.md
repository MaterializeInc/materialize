---
source: src/adapter/src/coord/catalog_implications/parsed_state_updates.rs
revision: 5d24e1fd58
---

# adapter::coord::catalog_implications::parsed_state_updates

Defines `ParsedStateUpdate` and `ParsedStateUpdateKind`, which enrich raw `StateUpdateKind` diffs with in-memory object representations to make `apply_catalog_implications` easier to work with.
The `ParsedStateUpdateKind` enum covers `Item`, `TemporaryItem`, `Cluster`, `ClusterReplica`, `IntrospectionSourceIndex`, and `ReplicaSystemConfiguration` variants.
The `ReplicaSystemConfiguration` variant carries the raw durable row and is emitted whenever a `StateUpdateKind::ReplicaSystemConfiguration` change is seen; the implication handler re-pushes the complete per-replica dyncfg layer from the catalog working copy rather than acting on individual rows, so the variant exists mainly to make the change visible in tracing. `StateUpdateKind::ClusterSystemConfiguration` changes are not represented because cluster-scoped parameter overrides are read at plan time and require no controller action.
The `parse_state_update` function converts a `StateUpdate` into a `ParsedStateUpdate` by looking up parsed representations from the current `CatalogState`.
