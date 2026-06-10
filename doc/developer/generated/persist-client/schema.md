---
source: src/persist-client/src/schema.rs
revision: 161628c089
---

# persist-client::schema

Manages shard schema evolution: `SchemaCache` caches decoded schemas and schema migrations per handle, while `SchemaCacheMaps` holds the process-wide decoded schema registry.
`CaESchema` is the result type for `compare_and_evolve_schema`, which registers a new schema if it is backward compatible with existing ones.
Migrations (key and value) are cached per handle to amortize the cost of computing the N² set of cross-schema migrations.
`SchemaCache` exposes an `applier()` accessor that returns a reference to the underlying `Applier`, used by callers such as `BatchFetcher::missing_blob_diagnostics` to refresh shard state without requiring a separate handle.
