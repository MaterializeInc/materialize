---
source: src/mysql-util/src/desc.rs
revision: 011b3b5573
---

# mysql-util::desc

Defines the descriptor types used to represent MySQL table structure within Materialize: `MySqlTableDesc` (schema/name/columns/keys), `MySqlColumnDesc` (name, Materialize column type, optional metadata), `MySqlKeyDesc` (index name, primary flag, column list), and `MySqlColumnMeta` (enum variants for Enum, Json, Year, Date, Timestamp, and Bit columns).
All types implement protobuf serialization via `RustType` using generated `Proto*` types, proptest `Arbitrary`, and `determine_compatibility` / `is_compatible` methods that allow additive upstream schema changes (new columns, wider enum sets) without breaking existing sources.
