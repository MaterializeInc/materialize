---
source: src/sql/src/rbac.rs
revision: 37a6a50fa7
---

# mz-sql::rbac

Implements role-based access control (RBAC) checks for SQL statements.
`check_plan` (and supporting helpers) inspects a fully-formed `Plan` together with the current `SessionCatalog` and `SessionMetadata` to determine whether the active role holds the required privileges; it returns an `UnauthorizedError` on failure.
`check_plan` accepts a `target_conn_role: Option<RoleId>` parameter that carries the authenticated role of the connection targeted by a `Plan::SideEffectingFunc` (specifically `PgCancelBackend`); this role is used directly in RBAC membership checks rather than being looked up lazily via a callback.
System users (`mz_system`, `mz_support`) bypass most checks, and individual checks can be disabled via feature flags.
`check_plan` accepts both `resolved_ids` (the statement's main dependency set) and a separate `sql_impl_resolved_ids` (IDs from SQL-implemented function bodies, which are implementation details rather than real statement dependencies). `check_restrict_to_user_objects` is called once for `sql_impl_resolved_ids` before the main RBAC requirements are generated, so that SQL-impl function body IDs are checked for `restrict_to_user_objects` without affecting regular dependency tracking.
`check_restrict_to_user_objects` runs when `session.restrict_to_user_objects()` is true: it iterates over the provided `resolved_ids`, and for each system-owned item rejects non-`Func`/non-`Type` catalog items unless their OID appears in `RESTRICT_TO_USER_OBJECTS_ALLOWED_OIDS` (currently `mz_mcp_data_products`, `mz_mcp_data_product_details`, and `mz_show_my_cluster_privileges`). `mz_show_my_cluster_privileges` is exempted so that `read_data_product` can check cluster USAGE without triggering the restriction. The error variant is `UnauthorizedError::RestrictedSystemObject`.
`generate_read_privileges_inner` uses an iterative worklist traversal rather than recursion; view dependency chains are user-controlled and can be arbitrarily deep, so recursion risks a stack overflow.
`ALTER ROLE ... SET restrict_to_user_objects` requires superuser: the `PlannedAlterRoleOption::Variable` match arm in `generate_rbac_requirements` enforces this with a case-insensitive name comparison.
`Plan::CreateMaterializedView` in `generate_rbac_requirements` requires ownership of both `replace` (the `CREATE OR REPLACE` target, if set) and `materialized_view.replacement_target` (the `FOR <target>` replacement target, if set). Both are independent and optional; ownership of whichever are present is required, mirroring the ownership check for `ALTER ... APPLY REPLACEMENT` and `CREATE INDEX`.
