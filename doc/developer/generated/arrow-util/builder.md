---
source: src/arrow-util/src/builder.rs
revision: 2f22224ae6
---

# builder

Converts Materialize `Row` data into Arrow `RecordBatch`es column by column.
`ArrowBuilder` is constructed from a `RelationDesc` via `ArrowBuilder::new` or from a pre-built `Arc<Schema>` via `ArrowBuilder::new_with_schema` (the latter preserves schema metadata such as Iceberg field IDs).
Rows are appended via `add_row` and finalized into a `RecordBatch` with `to_record_batch`; when constructed with `new_with_schema`, the original schema is used in the output to preserve metadata.
Each SQL scalar type is mapped to a concrete Arrow `DataType`; complex types (arrays, lists, maps, records, ranges) are represented as nested Arrow structs, lists, and maps. Interval maps to `IntervalUnit::MonthDayNano`. Range types map to a five-field Arrow struct with `lower` (nullable element), `upper` (nullable element), `lower_inclusive` (bool), `upper_inclusive` (bool), and `empty` (bool).
Duplicate column names are disambiguated by appending a numeric suffix, and each Arrow field carries a `materialize.v1.<typename>` extension metadata entry.

Helper functions `desc_to_schema` and `desc_to_schema_with_overrides` convert a `RelationDesc` to an Arrow `Schema`, with the override variant allowing destinations such as Iceberg to substitute custom type mappings (e.g., `UInt64` → `Decimal128`) for unsupported types.
`ArrowBuilder::validate_desc` checks that all columns in a `RelationDesc` have supported Arrow type mappings without constructing a builder.
`ArrowBuilder::validate_desc_for_parquet` extends this check to also reject Arrow types that are not supported by arrow-rs's `ArrowWriter` parquet writer; callers pass the same override function used with `desc_to_schema_with_overrides` so remapped types are not rejected. The private `parquet_incompatible_type` helper recurses into composite types and uses a closed allowlist — anything not explicitly permitted is rejected.
