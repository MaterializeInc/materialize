---
source: src/persist-types/src/stats.rs
revision: e757b4d11b
---

# persist-types::stats

Defines the statistics system used by persist for pushdown filtering.
Key types: `ColumnarStats` (a column's stats including optional null count), `ColumnStatKinds` (enum over primitive, struct, bytes, and none variants), `PartStats` (stats for an entire `Part`, containing a `StructStats` for the key column), and the `DynStats`, `ColumnStats`, `ColumnarStatsBuilder`, and `TrimStats` traits.
`trim_to_budget` and helpers recursively drop or truncate column statistics to fit within a serialization byte budget, with optional force-keep predicates for specific columns.
The four child modules — `primitive`, `bytes`, `json`, and `structured` — implement concrete statistics types for their respective column kinds.
