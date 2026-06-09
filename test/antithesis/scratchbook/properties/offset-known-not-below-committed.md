# offset-known-not-below-committed

## Summary

For every Kafka source, the statistics view always reports `offset_known >= offset_committed`. Causally, what the broker has told us is available cannot lag what Materialize has durably ingested.

## Code

- `src/storage/src/statistics.rs` (around line 56-71) — the statistics update path that previously allowed regression. Commit `3e32df1f69` introduced clamping so that on a restart where `offset_known` would be loaded from the broker watermark while `offset_committed` is restored from persist, the metric does not flip into the wrong order.

## How to check it

Workload-side polling:

```sql
SELECT id, offset_known, offset_committed
FROM mz_internal.mz_source_statistics_per_worker
WHERE id = ?
```

`assert_always!(offset_known >= offset_committed, "kafka source statistics: offset_known < offset_committed")`.

SUT-side: mirror as an `assert_always!` inside the statistics update path itself, immediately after both fields are computed but before the value is published.

## What goes wrong on violation

The lag metric `offset_known - offset_committed` becomes a small negative number that wraps to a huge positive number in dashboards (commonly displayed as `u64` or with `MAX(0, …)` clamping that hides the actual bug). Operational tooling that drives autoscaling or alerting off lag becomes unreliable.

## Antithesis angle

The most interesting timing is the very first sample after a clusterd restart. The order in which the source restores `offset_committed` (from the persist shard upper) and learns `offset_known` (from rdkafka's first metadata response) determines whether the invariant holds during the window where one is set and the other is zero. The fix in commit `3e32df1f69` clamps; Antithesis should verify the clamp covers every interleaving.

## Existing instrumentation

None. Pure workload-side polling assertion, optionally mirrored SUT-side.

## Provenance

Surfaced by: Data Integrity (metrics correctness). Direct regression target for commit `3e32df1f69`.
