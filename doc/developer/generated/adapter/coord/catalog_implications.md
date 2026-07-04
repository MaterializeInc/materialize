---
source: src/adapter/src/coord/catalog_implications.rs
revision: 1c17d34993
---

# adapter::coord::catalog_implications

Derives and applies downstream effects from catalog state changes following the pipeline `StateUpdateKind -> ParsedStateUpdate -> CatalogImplication`.
`ParsedStateUpdate` enriches raw catalog diffs with in-memory object representations; `CatalogImplication` is both a state machine for absorbing multiple updates to the same object and the final command applied to controllers.
`apply_catalog_implications` reacts to consolidated catalog updates by creating or dropping compute dataflows and storage collections, updating read policies, advancing compaction frontiers, managing VPC endpoints, creating and dropping clusters and replicas, applying source alterations, cancelling affected peeks and subscribes, and sending builtin-table updates. When any `ReplicaSystemConfiguration` update is present in the batch, `apply_catalog_implications` calls `push_replica_dyncfg_overrides` after cluster creation but before replica creation, re-pushing the complete per-replica dyncfg override layer from the catalog working copy so new replicas receive their overrides on their first configuration.
After applying implications, `apply_catalog_implications` calls `reconcile_now.notify_one()` when `should_reconcile_now` returns true, waking the cluster controller task to reconcile immediately rather than waiting out its tick interval. `should_reconcile_now` returns true when any committed diff contains a `Cluster` or `ClusterReplica` update, keying the wake off the committed diff so it fires regardless of whether this node or another writer applied the change. Environment-wide system-config changes are not represented as catalog implications and do not trigger a wake; the controller picks those up on its next tick.
The child module `parsed_state_updates` handles the parsing step.
