---
source: src/arrow-util/src/builder.rs
revision: 3b99b09758
---

# builder

Converts Materialize `Row` data into Arrow `RecordBatch`es column by column.
`ArrowBuilder` accepts a `RelationDesc` (or a pre-built `Arc<Schema>` for metadata-preserving writes such as Iceberg) and appends rows via `add_row`, finalizing into a `RecordBatch` with `to_record_batch`.
Each SQL scalar type is mapped to a concrete Arrow `DataType`; complex types (arrays, lists, maps, records) are represented as nested Arrow structs, lists, and maps.
Duplicate column names are disambiguated by appending a numeric suffix, and each Arrow field carries a `materialize.v1.<typename>` extension metadata entry.
