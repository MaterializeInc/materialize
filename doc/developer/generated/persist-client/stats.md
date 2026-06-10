---
source: src/persist-client/src/stats.rs
revision: 51f1c1c12c
---

# persist-client::stats

Defines dynamic configuration knobs for per-part statistics collection and filtering (`STATS_COLLECTION_ENABLED`, `STATS_FILTER_ENABLED`, `STATS_BUDGET_BYTES`) and the column-exclusion lists that prevent trimming statistics for columns that are always needed for correctness (e.g., the `err` column).
Also provides `SnapshotStats` and related types for reporting aggregate statistics about a snapshot's parts.
