---
source: src/adapter/src/coord/validity.rs
revision: 6a75ad9bb5
---

# adapter::coord::validity

Provides `PlanValidity`, a struct that records the catalog IDs a pending plan depends on: the transient revision (used as a cache marker), dependency `CatalogItemId`s, an optional cluster ID, an optional replica ID, and role metadata.
`check` verifies that all dependency IDs, the cluster, the replica, and the role are still present in the catalog before executing a deferred plan, preventing stale plans from executing after DDL has dropped one of their dependencies. It is called at every off-thread → on-thread hop in `sequence_staged`.
`extend_dependencies` adds additional catalog IDs to the dependency set.
