# `restrict_to_user_objects` session variable for MCP agent isolation

- Associated:
  - Branch: `mcp-restrict-to-user-objects`

## The Problem

Customers want to expose Materialize data products (materialized views, tables) to MCP agents without leaking system metadata. An agent connected as a minimally-privileged role should be able to `SELECT` from granted views but should not be able to discover role names, connection strings, cluster topology, audit events, secret names, DDL history, or version information.

Today, the RBAC privilege system grants `PUBLIC_SELECT` on most objects in `mz_catalog`, `mz_internal`, `pg_catalog`, and `information_schema`. A role with no explicit grants beyond `USAGE` on a schema and `SELECT` on specific objects can still query `mz_roles`, `mz_connections`, `mz_audit_events`, `mz_secrets`, and hundreds of other system tables. Revoking `PUBLIC_SELECT` broadly would break PostgreSQL compatibility and existing tooling.

## Success Criteria

- An agent role with `restrict_to_user_objects = true` can query only user-created objects it has been granted access to.
- The agent cannot read system catalog tables, views, or sources (in any schema: `mz_catalog`, `mz_internal`, `pg_catalog`, `information_schema`).
- The agent cannot use SHOW commands to enumerate system objects.
- The agent cannot use built-in functions that query system tables or expose system metadata.
- The agent cannot disable the restriction itself, even via `ALTER ROLE`, `DISCARD ALL`, or other indirect paths.
- Non-restricted users are unaffected.

## Out of Scope

- Changing `PUBLIC_SELECT` grants on system objects. This is a session-level overlay, not a catalog-level privilege change.
- Row-level filtering within user objects. If a user-created view is defined as `SELECT * FROM mz_catalog.mz_roles`, the restriction blocks it entirely (the view's dependencies include system objects).
- A permanent, production-grade solution. This is a short-term mechanism to ship MCP agent support. A longer-term design (e.g., per-object `PUBLIC` revocation, schema-level access policies) should replace it.

## Solution Proposal

A new boolean session variable `restrict_to_user_objects` causes the RBAC layer to reject queries that reference system catalog objects. It is read-only from the user's perspective and can only be set as a role default by a superuser.

### Variable administration

```sql
-- Superuser sets the restriction as a role default
ALTER ROLE agent SET restrict_to_user_objects = true;

-- Agent reconnects; restriction is active for the session
-- All of these fail:
SET restrict_to_user_objects = false;                   -- ERROR: parameter cannot be changed
ALTER ROLE agent SET restrict_to_user_objects = false;   -- ERROR: requires superuser
ALTER ROLE agent RESET restrict_to_user_objects;         -- ERROR: requires superuser
DISCARD ALL;  -- Resets to role default (true), not the definition default (false)

-- Superuser can change or remove the restriction at any time:
ALTER ROLE agent SET restrict_to_user_objects = false;   -- disables on next connection
ALTER ROLE agent RESET restrict_to_user_objects;         -- removes the default entirely
```

- `SET` and `RESET` are blocked via `check_read_only()`.
- `ALTER ROLE ... SET` goes through `allow_role_default()` which bypasses the read-only check, but the RBAC layer requires superuser privileges for that operation (case-insensitive match on the variable name).
- `DISCARD ALL` resets session variables to `default_value` (the role default applied at session startup), so the restriction survives.

### System object blocking

When `restrict_to_user_objects` is true, the RBAC check in `check_usage` iterates over the query's `ResolvedIds` and rejects any system item that is not a function or type:

```
Allowed: CatalogItemType::Func, CatalogItemType::Type
Blocked: everything else (Table, View, MaterializedView, Source, Sink, Index, Connection, Secret, ...)
```

This is an allow-list. New catalog item types are blocked by default.

### SHOW commands

SHOW commands (`SHOW TABLES`, `SHOW ROLES`, `SHOW CLUSTERS`, etc.) are rewritten during planning into `SELECT ... FROM mz_internal.mz_show_*` queries. The `mz_show_*` views are system objects that appear in `ResolvedIds`, so they are caught by the same check with no special handling.

### Built-in functions

Built-in functions require two separate handling strategies because they have two distinct implementations:

**SQL-implemented functions** (`has_table_privilege`, `pg_get_viewdef`, `obj_description`, `mz_resolve_object_name`, etc.) are defined as static SQL strings that get re-parsed and re-resolved during planning. Their function bodies reference system catalog tables (e.g., `has_table_privilege` queries `mz_relations`; `pg_get_viewdef` queries `mz_views`). Previously, the `ResolvedIds` from the inner `names::resolve()` call inside `sql_impl` were discarded — the system tables they referenced never reached the RBAC check.

Fix: `StatementContext` carries a shared `Arc<Mutex<ResolvedIds>>` accumulator. When `sql_impl` or `sql_impl_table_func_inner` resolves a function body, the resulting IDs are merged into this accumulator. At the end of `plan()`, accumulated IDs are merged into the caller's `resolved_ids`, which flows to `check_plan` -> `check_usage` where the restriction catches them. This follows the same pattern as `ShowSelect::new_from_bare_query`, which already captures and propagates resolved IDs from re-parsed SQL.

**Unmaterializable functions** (`mz_version()`, `mz_role_oid_memberships()`, `mz_environment_id()`, `pg_backend_pid()`, etc.) are evaluated at runtime directly from coordinator/session state. They never reference catalog items by ID, so resolved ID propagation cannot catch them.

Fix: `UnmaterializableFunc` has an `allowed_in_restricted_session()` method with an explicit match — no wildcard arm, so adding a new variant is a compile error until the developer classifies it. The check runs in `eval_unmaterializable_func()` during optimization.

| Classification | Functions |
|---|---|
| **Allowed** (needed for query execution) | `current_database`, `current_schema`, `current_schemas`, `current_timestamp`, `current_user`, `session_user`, `mz_now`, `mz_session_id`, `mz_session_role_memberships`, `is_rbac_enabled`, `viewable_variables` |
| **Excluded** (internal system information, not relevant to data product queries) | `mz_version`, `mz_version_num`, `version`, `mz_environment_id`, `mz_is_superuser`, `mz_role_oid_memberships`, `mz_uptime`, `pg_postmaster_start_time`, `pg_backend_pid` |

### MCP data product discovery

`mz_mcp_data_products` and `mz_mcp_data_product_details` are system views used by the MCP agent endpoint to discover data products the session has access to. They are exempted from the system object block via an OID allowlist in `check_restrict_to_user_objects`.

These views depend on `mz_show_my_object_privileges`, which originally used `pg_has_role(grantee, 'USAGE')`. That function calls `mz_role_oid_memberships()`, which is correctly blocked because it exposes the full system role graph. `mz_show_my_object_privileges` now uses `mz_session_role_memberships()` instead, which returns only the current session's transitive role chain as role names. This is semantically equivalent for the filter purpose and safe to allow in restricted sessions.

### `pg_catalog` and `information_schema`

Views in these schemas (`pg_class`, `pg_namespace`, `information_schema.tables`, etc.) are system catalog objects caught by the same `check_usage` restriction with no special handling.

### Error messages

All restriction errors use the same generic message:

```
ERROR: access to system object <name> is restricted
DETAIL: Access to system catalog objects is restricted for this role.
        Contact your administrator if you need access.
```

The error intentionally does not name the `restrict_to_user_objects` variable or explain how to disable it, to avoid giving a restricted agent information about the restriction mechanism.

## Minimal Viable Prototype

The implementation is on the `mcp-restrict-to-user-objects` branch and includes a comprehensive sqllogictest (`test/sqllogictest/rbac_mcp_agent.slt`) covering: user object access, system table blocking, SHOW command blocking, `pg_catalog`/`information_schema` blocking, SQL-implemented function blocking, unmaterializable function blocking, safe function allow-listing, `DISCARD ALL` non-bypass, and variable administration (SET/RESET/ALTER ROLE).

## Alternatives

**Revoke `PUBLIC_SELECT` on sensitive system tables.** This is the correct long-term solution but is a larger scope change that risks breaking PostgreSQL-compatible tooling (BI tools, ORMs, connection libraries that query `pg_catalog`). The session variable approach lets us ship agent isolation now without disrupting existing users.

**Allowlist approach (only allow access to specific schemas).** Rather than blocking system objects, the agent would only be allowed to access objects in explicitly granted schemas. This was considered but rejected because it requires changes to the schema resolution and search path machinery, and doesn't compose well with the existing RBAC privilege model.

**Check functions at planning time instead of evaluation time.** For unmaterializable functions, we could check during planning (in `func.rs`) rather than at evaluation time (in `dataflows.rs`). This would return a `PlanError` instead of an `OptimizerError`. We chose evaluation-time checking because it keeps the check co-located with the function evaluation logic and avoids threading session state into the planning layer.
