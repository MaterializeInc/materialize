---
source: src/adapter/src/coord/catalog_implications.rs
revision: 7258dad07f
---

# adapter::coord::catalog_implications

Derives and applies downstream effects from catalog state changes following the pipeline `StateUpdateKind -> ParsedStateUpdate -> CatalogImplication`.
`ParsedStateUpdate` enriches raw catalog diffs with in-memory object representations; `CatalogImplication` is both a state machine for absorbing multiple updates to the same object and the final command applied to controllers.
`apply_catalog_implications` reacts to consolidated catalog updates by creating or dropping compute dataflows and storage collections, updating read policies, advancing compaction frontiers, managing VPC endpoints, creating and dropping clusters and replicas, applying source alterations, cancelling affected peeks and subscribes, and sending builtin-table updates. When any `ReplicaSystemConfiguration` update is present in the batch, `apply_catalog_implications` calls `push_replica_dyncfg_overrides` after cluster creation but before replica creation, re-pushing the complete per-replica dyncfg override layer from the catalog working copy so new replicas receive their overrides on their first configuration.
The child module `parsed_state_updates` handles the parsing step.
