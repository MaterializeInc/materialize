---
source: src/avro/src/types.rs
revision: 78cd347f2b
---

Defines the `Value` enum, the in-memory intermediate representation for any valid Avro datum.
Variants cover all standard Avro primitive and complex types (null, boolean, int, long, float, double, bytes, string, fixed, enum, union, array, map, record) plus Materialize extensions: `Date`, `Timestamp`, `Decimal`, `Json` (Debezium-style embedded JSON strings), and `Uuid`.
The `ToAvro` trait converts Rust primitives and standard collections into `Value`; `Record` is a schema-aware builder for the `Value::Record` variant.
`Value::validate` checks a value for conformance with a `SchemaNode`, which is used by the writer before encoding.
`SchemaResolutionError` is also housed here as it is logically tied to the value layer.
