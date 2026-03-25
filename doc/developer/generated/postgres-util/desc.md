---
source: src/postgres-util/src/desc.rs
revision: 4267863081
---

# mz-postgres-util::desc

Defines the descriptor types for PostgreSQL schema objects used by Materialize's Postgres source.
`PostgresSchemaDesc` holds an OID, name, and owner OID for a schema (`pg_namespace`).
`PostgresTableDesc` holds the table OID, namespace, name, an ordered list of `PostgresColumnDesc` values, and a set of `PostgresKeyDesc` values; `determine_compatibility` verifies that a live table schema is a compatible superset of the stored descriptor (columns may be added; existing columns must match name, OID position, and type unless the column number is in the allow-list).
`PostgresColumnDesc` stores name, monotonic column number (`attnum`), type OID, type modifier, and nullability; `is_compatible` permits nullability widening and optionally allows type changes.
`PostgresKeyDesc` stores constraint OID, name, constituent `attnum` values, `is_primary`, and `nulls_not_distinct` (PostgreSQL 15+).
All three descriptor types implement `RustType` for protobuf serialization via generated code included from `OUT_DIR`.
