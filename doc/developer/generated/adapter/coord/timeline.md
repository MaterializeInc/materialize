---
source: src/adapter/src/coord/timeline.rs
revision: 4a1aeff959
---

# adapter::coord::timeline

Manages per-timeline timestamp oracles and the coordinator's timeline bookkeeping.
`TimelineState` holds the `TimestampOracle` for each timeline and the oracle's current read/write timestamps; `timedomain_for` computes the union of timelines that a query's input collections belong to; the coordinator creates and caches oracle instances here.
`TimelineContext` (re-exported from the catalog) describes whether a collection is timeline-dependent or timestamp-independent.
