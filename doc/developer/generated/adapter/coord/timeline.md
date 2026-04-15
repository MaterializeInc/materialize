---
source: src/adapter/src/coord/timeline.rs
revision: fcc110b5fe
---

# adapter::coord::timeline

Manages per-timeline timestamp oracles and the coordinator's timeline bookkeeping.
`TimelineState` holds the `TimestampOracle<Timestamp>` for each timeline and a set of `ReadHolds` that keep those read timestamps valid; the coordinator creates and caches oracle instances here.
`TimelineContext` describes whether a collection is timeline-dependent (`TimelineDependent`), timestamp-dependent (`TimestampDependent`), or timestamp-independent (`TimestampIndependent`).
