---
source: src/mz-deploy/src/types.rs
revision: c8a2857de2
---

# mz-deploy::types

Data-contract system for external dependencies.

When a project references objects it does not own (e.g. tables created by an upstream ingestion pipeline), mz-deploy needs their column schemas to type-check views that depend on them. This module manages that contract through the `types.lock` file.

## Lock file lifecycle

1. **Capture** — Column schemas are queried from the live environment and written to `types.lock`.
2. **Compile** — The lock file is loaded and its schemas are used to resolve external dependency columns during compilation.
3. **Validate** — During incremental typechecking, external dependency schemas are provided to the validation backend when dirty objects reference them.

## Key types

- `Types` — In-memory representation of a `types.lock` file: a map from fully-qualified object names to column schemas, plus optional object-level comments from `COMMENT ON` in the source database.
- `ColumnType` — A single column's type, nullability, and optional `COMMENT ON COLUMN` description.
- `DataType` (from `data_type`) — A column's structural type. Uses `DataType::Named` for ordinary scalar types spelled by `format_type`, and `DataType::Record { fields: Vec<RecordField> }`, `DataType::List`, `DataType::Map`, or `DataType::Array` for composite and container types that have no SQL spelling the grammar accepts. The `stub` submodule turns a recorded schema back into a SQL relation for the in-memory catalog.
- `ObjectKind` — The kind of database object (`Table`, `View`, `MaterializedView`, `Source`, `Sink`, `Secret`, `Connection`). `TableFromSource` is treated as `Table` from a contract perspective.

## Format versioning

`LOCK_VERSION` (currently 2) is the highest format version this binary understands. Version 2 records structural types; a version 1 file still loads, with type names parsed as `DataType::Named`. `TypesError::UnsupportedLockVersion` is returned when a file's version exceeds `LOCK_VERSION`.

## Error handling

`TypesError` covers file read/write/parse failures, unsupported lock versions, invalid column types (`TypesError::InvalidColumnType` with the object and column name and a `TypeLockError` source), directory creation failures, dependency errors, and build-artifact cache errors.
