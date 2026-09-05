---
source: src/adapter/src/coord/timestamp_selection.rs
revision: 6cef38c018
---

# adapter::coord::timestamp_selection

Implements `determine_timestamp`, which chooses a read timestamp for a query by consulting the appropriate timestamp oracle and verifying that the selected timestamp is readable (i.e. `>= since` for all input collections).
`TimestampContext` carries the selected timeline and timestamp into the optimizer; `TimestampDetermination` records the full decision (oracle response, selected timestamp, reasoning) for use in `EXPLAIN TIMESTAMP`; `TimestampProvider` is the async trait implemented by `Coordinator` that exposes timestamp selection to the sequencer.
`needs_linearized_read_ts` treats `IsolationLevel::BoundedStaleness` as an oracle-anchoring level alongside `StrictSerializable` and `StrongSessionSerializable`. For bounded staleness queries, `determine_timestamp_via_constraints` anchors a freshness floor at `oracle_read_ts - D` and clamps the upper at `largest_not_in_advance_of_upper`; if no valid timestamp exists in that window, `AdapterError::BoundedStalenessExceeded` is returned. Bounded staleness is restricted to the `EpochMilliseconds` timeline; touching any other timeline returns `AdapterError::BoundedStalenessTimelineUnsupported`. After selecting a bounded-staleness timestamp, `determine_timestamp_for` records a `timestamp_difference_for_bounded_staleness_ms` metric (via `metrics.by_cluster.timestamp_difference_for_bounded_staleness_ms`) comparing the chosen timestamp against what serializable would have produced. The `determine_timestamp` counter is also recorded via `metrics.by_cluster.determine_timestamp`. No equivalent metric is recorded for strict-serializable queries.
