---
source: src/adapter/src/coord/validity.rs
revision: 277b33e9c0
---

# adapter::coord::validity

Provides `PlanValidity`, a struct that records the catalog IDs a pending plan depends on: the transient revision (used as a cache marker), dependency `CatalogItemId`s, an optional cluster ID, an optional replica ID, and role metadata.
`PlanValidity::new` takes `&Catalog` (not a bare revision integer) so it can snapshot a hash of each dependency's `create_sql` for the optional mutation check.
`check` verifies that all dependency IDs, the cluster, the replica, and the role are still present in the catalog before executing a deferred plan, preventing stale plans from executing after DDL has dropped one of their dependencies. It is called at every off-thread → on-thread hop in `sequence_staged`. When `with_dependency_hash_check` has been called, `check` also re-hashes each dependency's `create_sql` and returns `AdapterError::ConcurrentDependencyMutation` (SQLSTATE 40001) if any hash differs from what was captured at plan time. This catch is opt-in: plans that only read dependencies (peeks, subscribes) do not arm it, because a benign `RENAME` rewrites `create_sql` without affecting those plans' correctness.
`with_dependency_hash_check` arms the mutation check and captures hashes for all current and future dependencies; subsequent calls to `extend_dependencies` also capture hashes when the check is armed.
`extend_dependencies` takes `&Catalog` and adds additional catalog IDs to the dependency set, capturing hashes when the mutation check is armed.
