---
source: src/mz-deploy/src/client/type_info.rs
revision: c8a2857de2
---

# mz-deploy::client::type_info

Column-schema introspection for the data-contract and type-checking systems.

`TypeInfoClient` queries the Materialize system catalog to generate or refresh the `types.lock` data-contract file. Plain `CREATE TABLE` objects are excluded — their schemas come from the SQL AST. For all other objects, `query_types_for_objects` retrieves column names, types (via `format_type`), nullability, object kind, and comments in a single query per object.

Columns whose `format_type` output is a pseudo-type token (`record`, `list`, `map`) are refined by `resolve_pseudo_types`, which issues a `pg_typeof` probe on a zero-row scalar subquery for each such column to obtain the full structural type. Probes are batched in chunks of 32 (`PROBE_CHUNK`); if a batch fails, the method retries each column individually and logs a warning for any that still fail rather than aborting the whole lock operation.
