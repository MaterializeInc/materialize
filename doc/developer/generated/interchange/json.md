---
source: src/interchange/src/json.rs
revision: c317ceee3c
---

# interchange::json

Provides `JsonEncoder` (implements `Encode`), `encode_datums_as_json`, and `build_row_schema_json` for encoding Materialize rows to JSON and generating Avro-compatible row schemas as JSON values.
The `ToJson` trait on `TypedDatum` converts every `SqlScalarType` variant to a `serde_json::Value`, with `JsonNumberPolicy` controlling whether numeric values are kept as JSON numbers or converted to strings.
`SqlScalarType::RegProc` values are encoded as 4-byte unsigned integers (not resolved to function names), matching the schema that `build_row_schema_json` publishes for those columns; name resolution is intentionally left to the pgwire text layer.
The private `Namer` struct ensures unique, valid Avro type names for anonymous records and Materialize-specific logical types (unsigned integers, intervals).
