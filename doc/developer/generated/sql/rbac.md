---
source: src/sql/src/rbac.rs
revision: 24e8772efd
---

# mz-sql::rbac

Implements role-based access control (RBAC) checks for SQL statements.
`check_plan_rbac` (and supporting helpers) inspects a fully-formed `Plan` together with the current `SessionCatalog` and `SessionMetadata` to determine whether the active role holds the required privileges; it returns an `UnauthorizedError` on failure.
System users (`mz_system`, `mz_support`) bypass most checks, and individual checks can be disabled via feature flags.
