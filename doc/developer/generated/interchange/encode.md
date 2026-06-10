---
source: src/interchange/src/encode.rs
revision: db271c31b1
---

# interchange::encode

Defines the `Encode` trait (with `encode_unchecked` and `hash` methods) that all format-specific encoders implement.
Also provides `TypedDatum` (a `Datum` paired with its `SqlColumnType`) and `column_names_and_types`, which extracts and deduplicates column names from a `RelationDesc` for use by encoders.
