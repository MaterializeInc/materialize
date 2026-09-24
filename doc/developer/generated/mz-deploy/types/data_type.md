---
source: src/mz-deploy/src/types/data_type.rs
revision: c8a2857de2
---

# mz-deploy::types::data_type

Defines `DataType`, the in-memory structural representation of a column's type in a data contract.

`DataType::Named(String)` covers every type the SQL grammar can spell directly (e.g. `integer`, `numeric(39,2)`, a user-defined type by its fully-qualified name). `DataType::Array`, `DataType::List`, and `DataType::Map` hold a boxed element/value `DataType`. `DataType::Record(Vec<RecordField>)` holds named fields with their own type and nullability; `RecordField` carries nullability separately because `SqlScalarType::Record` uses a full column type per field while container variants hold a bare scalar type.

`TypeLock` and `FieldLock` are the on-disk forms used in `types.lock` and the build artifact. The serde tags `"array"`, `"list"`, `"map"`, and `"record"` identify composite variants. A `DataType::Named` whose string happens to match one of these tags is distinguished by always carrying a dot-qualified name for user-defined types, preventing collisions with composite tags.

`DataType::contains_record` returns whether a record appears anywhere in the type tree. Schemas free of records are expressible as a plain `CREATE TABLE`; schemas containing a record require helper relations (see `stub`).
