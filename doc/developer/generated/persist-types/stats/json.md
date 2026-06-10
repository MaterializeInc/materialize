---
source: src/persist-types/src/stats/json.rs
revision: a24c5f8bf6
---

# persist-types::stats::json

Defines `JsonStats`, aggregate statistics for a column of JSON values that may contain a mixture of null, bool, numeric, string, list, and map types.
`JsonMapElementStats` pairs a map-key name with `JsonStats` for its values, enabling per-key pushdown on JSONB columns.
