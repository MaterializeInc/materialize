---
source: src/adapter/src/coord/validity.rs
revision: c61ef02a01
---

# adapter::coord::validity

Provides `PlanValidity`, a struct that records the catalog IDs a pending plan depends on, and `check_plan_validity`, which verifies those IDs are still present in the catalog before executing a deferred plan.
This prevents stale plans from executing after DDL has dropped one of their dependencies.
