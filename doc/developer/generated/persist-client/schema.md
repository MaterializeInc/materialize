---
source: src/persist-client/src/schema.rs
revision: a55caae279
---

# persist-client::schema

Manages shard schema evolution: `SchemaCache` caches decoded schemas and schema migrations per handle, while `SchemaCacheMaps` holds the process-wide decoded schema registry.
`CaESchema` is the result type for `compare_and_evolve_schema`, which registers a new schema if it is backward compatible with existing ones.
Migrations (key and value) are cached per handle to amortize the cost of computing the N² set of cross-schema migrations.
