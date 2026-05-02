---
source: src/adapter/src/coord/catalog_implications/parsed_state_updates.rs
revision: eb105c0ce8
---

# adapter::coord::catalog_implications::parsed_state_updates

Defines `ParsedStateUpdate` and `ParsedStateUpdateKind`, which enrich raw `StateUpdateKind` diffs with in-memory object representations to make `apply_catalog_implications` easier to work with.
The `ParsedStateUpdateKind` enum covers `Item`, `TemporaryItem`, `Cluster`, `ClusterReplica`, and `IntrospectionSourceIndex` variants.
The `parse_state_update` function converts a `StateUpdate` into a `ParsedStateUpdate` by looking up parsed representations from the current `CatalogState`.
