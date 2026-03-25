---
source: src/storage-types/src/stats.rs
revision: c94586e255
---

# storage-types::stats

Defines `RelationPartStats`, which bridges persist's `PartStats` to the `ResultSpec`-based filter pushdown used by `mz-expr`.
`may_match_mfp` evaluates a `MapFilterProject` against column-level statistics to decide whether a persist part can be skipped entirely during reads.
This is the primary mechanism for predicate pushdown into persist for storage collections.
