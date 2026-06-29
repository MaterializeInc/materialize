---
source: src/mz-deploy/src/client.rs
revision: a647094cc4
---

# mz-deploy::client

Database client layer for communicating with a live Materialize region. All database interactions flow through this module.

`Client` (from `connection`) wraps a `tokio_postgres` connection. `SERVER_CLUSTER_NAME` (`"_mz_deploy_server"`) is the dedicated cluster created during `setup`.

Scoped sub-clients:
* `introspection` — read-only catalog queries: schema/cluster/object existence checks, dependency lookups, batch metadata.
* `provisioning` — DDL: creates/alters databases, schemas, and clusters to match the project.
* `deployment_ops` — blue/green lifecycle: staging, hydration monitoring, cutover, abort.
* `validation` — pre-deployment checks that the target environment matches expected state.
* `type_info` — `SHOW COLUMNS` queries to generate/refresh the `types.lock` data-contract file.

Supporting submodules: `models` (shared data structures), `errors` (`ConnectionError`, `DatabaseValidationError`), `dev_overlays` (`DevOverlaysClient`).

`quote_identifier` double-quotes a SQL identifier, escaping embedded double quotes.
