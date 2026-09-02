---
source: src/storage-types/src/stats.rs
revision: 95baa04a85
---

# storage-types::stats

Defines `RelationPartStats`, which bridges persist's `PartStats` to the `ResultSpec`-based filter pushdown used by `mz-expr`.
`may_match_mfp` evaluates a `MapFilterProject` against column-level statistics to decide whether a persist part can be skipped entirely during reads.
Before evaluating column predicates, `may_match_mfp` consults `err_count()`: if the error count is absent (missing or malformed err-column stats) or nonzero, the part is kept unconditionally, matching the storage read path's `filter_result` behavior. Only parts that are confirmed error-free proceed to column-level pushdown.
`err_count` derives the error count from the `err` column's null-count statistics: the null count gives the number of ok rows, and subtracting from the total gives errors. Wrong-shaped err-column stats (`try_as_optional_bytes` fails) or an ok count that exceeds the part length both return `None`, which fails open to keeping the part.
Column stats are accessed through two paths: `col_values` reads scalar bounds directly from the ok struct column, and `col_json` supplements those with JSON-specific range narrowing for `Jsonb` columns. Both paths treat mismatched or corrupt column shapes as unknown range rather than panicking, incrementing the `PartStatsMetrics::mismatched_count` counter and returning `None`.
This is the primary mechanism for predicate pushdown into persist for storage collections.
