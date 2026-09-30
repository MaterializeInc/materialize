---
source: src/postgres-util/src/schema_change.rs
revision: 4c45b862e2
---

# mz-postgres-util::schema_change

Defines `SchemaChangeError`, the structured error type returned by `PostgresTableDesc::determine_compatibility` for upstream schema changes that Materialize cannot follow.

`SchemaChangeError` carries the table's namespace, name, and OID alongside a `SchemaChange` variant describing the mismatch. `Display` renders the diagnosis; `SchemaChangeError::hint` renders optional recovery steps as a separate SQL snippet (surfaced as the `HINT` of a SQL error and in the source status). A hint is provided for dropped-constraint variants (`SchemaChange::KeyDropped`), directing the user to recreate the table in a versioned schema with `EXCLUDE CONSTRAINTS`.

`SchemaChange` variants cover: `TableDropped` (the table was dropped and recreated, detected by OID mismatch), `TableRenamed` (namespace or name changed), `ColumnDropped` (a tracked column is absent from the new descriptor), `ColumnMoved` (attnum changed, indicating a drop-and-recreate), `ColumnTypeChanged`, `NotNullDropped` (nullability widened), `KeyDropped` (a tracked constraint is absent), and `KeyAltered` (a constraint was renamed or replaced).

`KeyRef` describes a PRIMARY KEY or UNIQUE constraint by name, kind, and constituent column names; its `Display` matches the format used in error messages.
