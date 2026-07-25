---
source: src/mz-deploy/src/client.rs
revision: 3f3bdb0535
---

# mz-deploy::client

Database client layer for communicating with a live Materialize region. All database interactions flow through this module.

`Client` (from `connection`) wraps a `tokio_postgres` connection. `SERVER_CLUSTER_NAME` (`"_mz_deploy_server"`) is the dedicated cluster created during `setup`. Every production connection is pinned to this cluster via libpq options; `connect_with_profile_no_pin` bypasses this pin for unit tests and the `setup` command itself.

Scoped sub-clients:
* `introspection` — read-only catalog queries: schema/cluster/object existence checks, dependency lookups, batch metadata.
* `provisioning` — DDL: creates/alters databases, schemas, and clusters to match the project.
* `deployment_ops` — blue/green lifecycle: staging, hydration monitoring, cutover, abort.
* `validation` — pre-deployment checks that the target environment matches expected state.
* `type_info` — `SHOW COLUMNS` queries to generate/refresh the `types.lock` data-contract file.
* `auto_scaling` — helper submodule for converting autoscaling strategy values to/from SQL AST and catalog JSON.

Supporting submodules: `models` (shared data structures), `errors` (`ConnectionError`, `DatabaseValidationError`), `dev_overlays` (`DevOverlaysClient`).

`quote_identifier` double-quotes a SQL identifier, escaping embedded double quotes.
`sql_placeholders(n)` builds a `$1, $2, …, $n` placeholder string for parameterized queries.
`staging_suffix_like_pattern(deploy_id)` builds a `LIKE` pattern (for use with `ESCAPE '\'`) that matches any name ending in the literal staging suffix `_<deploy_id>`. The separating underscore and any LIKE metacharacters in `deploy_id` are escaped so they match literally.
