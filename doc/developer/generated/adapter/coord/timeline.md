---
source: src/adapter/src/coord/timeline.rs
revision: a60edac7f1
---

# adapter::coord::timeline

Manages per-timeline timestamp oracles and the coordinator's timeline bookkeeping.
`TimelineState` holds the `TimestampOracle<Timestamp>` (wrapped in a `BatchingTimestampOracle`) for each timeline and a set of `ReadHolds` that keep those read timestamps valid; the coordinator creates and caches oracle instances here via `ensure_timeline_state` and `ensure_timeline_state_with_initial_time`.
`TimelineContext` describes whether a collection is timeline-dependent (`TimelineDependent`), timestamp-dependent (`TimestampDependent`), or timestamp-independent (`TimestampIndependent`).
The free-standing `timedomain_for` function computes the `CollectionIdBundle` that covers all objects in the same database schemas as the query's direct references, filtered to the query's timeline, and is used by both the coordinator and frontend peek sequencing to establish read holds for multi-statement transactions.
