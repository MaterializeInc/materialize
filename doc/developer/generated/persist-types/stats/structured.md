---
source: src/persist-types/src/stats/structured.rs
revision: e757b4d11b
---

# persist-types::stats::structured

Defines `StructStats`, statistics for a column of structs with a uniform schema: a total row count and a map from column name to `ColumnarStats`.
Persist may prune individual column statistics from `StructStats` to stay within a size budget, so the map may not cover all columns.
