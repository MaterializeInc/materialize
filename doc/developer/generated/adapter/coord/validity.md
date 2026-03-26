---
source: src/adapter/src/coord/validity.rs
revision: aa7a1afd31
---

# adapter::coord::validity

Provides `PlanValidity`, an enum that records the catalog IDs a pending plan depends on, with two variants: `RequireRevision` (checks for a specific transient catalog revision) and `Checks` (verifies dependency IDs, cluster, replica, and role metadata are still present).
`check` verifies those IDs are still present in the catalog before executing a deferred plan, preventing stale plans from executing after DDL has dropped one of their dependencies.
