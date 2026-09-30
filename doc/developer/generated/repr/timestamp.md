---
source: src/repr/src/timestamp.rs
revision: 60a8dd3a8f
---

# mz-repr::timestamp

Defines `Timestamp`, Materialize's system-wide timestamp type (a `u64` milliseconds-since-epoch value), implementing `timely::progress::Timestamp`, differential `Lattice`, and `TimestampManipulation` for step-forward operations.
Includes protobuf-generated code for timestamp serialization and the `BucketTimestamp` implementation for temporal bucketing.
Also defines `frontier_within_lag(frontier, reference, allowed_lag) -> bool`, a free function (re-exported from `mz_repr`) that checks whether `frontier` is within `allowed_lag` ticks of `reference`. An empty `reference` (the maximum antichain) is matched only by an empty `frontier`; an empty `frontier` is trivially within any reference. Used by `CollectionReadiness::classify` and the 0dt caught-up check to enforce lag bounds at cut-over.
