---
source: src/sql/src/rbac.rs
revision: 3df8ae2fd8
---

# mz-sql::rbac

Implements role-based access control (RBAC) checks for SQL statements.
`check_plan_rbac` (and supporting helpers) inspects a fully-formed `Plan` together with the current `SessionCatalog` and `SessionMetadata` to determine whether the active role holds the required privileges; it returns an `UnauthorizedError` on failure.
System users (`mz_system`, `mz_support`) bypass most checks, and individual checks can be disabled via feature flags.
`check_restrict_to_user_objects` runs when `session.restrict_to_user_objects()` is true: it iterates over `resolved_ids`, and for each system-owned item rejects non-`Func`/non-`Type` catalog items unless their OID appears in `RESTRICT_TO_USER_OBJECTS_ALLOWED_OIDS` (currently `mz_mcp_data_products` and `mz_mcp_data_product_details`). The error variant is `UnauthorizedError::RestrictedSystemObject`.
`ALTER ROLE ... SET restrict_to_user_objects` requires superuser: the `PlannedAlterRoleOption::Variable` match arm in `generate_rbac_requirements` enforces this with a case-insensitive name comparison.
