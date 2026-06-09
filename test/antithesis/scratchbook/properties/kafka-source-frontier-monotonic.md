# kafka-source-frontier-monotonic

## Summary

The `upper` frontier of the source's data persist shard never regresses across the source's lifetime, including across clusterd restarts and `compare_and_append` retries.

## Code paths

- `src/storage/src/render/persist_sink.rs` — `append_batches` calls `WriteHandle::compare_and_append`. Cached upper is the failure-prone spot (commit `505dc96aaa`: cached upper went stale under concurrent writers; fix uses `fetch_recent_upper`).
- `src/storage/src/source/reclock.rs` — `ReclockOperator::sync`: must not let the operator's `upper` field regress across `compare_and_append` retries.
- `src/storage/src/source/reclock/compat.rs:306` — `panic!("compare_and_append failed: {invalid_use}")`: this is the assertion that catches genuinely invalid persist calls (vs. legitimate `UpperMismatch` which is retried).

## How to check it

- Workload polls `mz_internal.mz_source_statistics_per_worker.offset_committed` (or equivalent shard upper view) on a tight cadence and `assert_always!(upper_monotonic, "kafka: source shard upper non-monotonic")` whenever a new sample is `< previous sample`.
- SUT-side: in `append_batches`, immediately before `compare_and_append`, capture the previous upper from the local cached state and `assert_always!(new_upper >= prev_upper, "persist sink: upper regression on append")`. Distinct messages on the reclock side.

## What goes wrong on violation

Downstream operators panic when `as_of > upper` (the reclock-`as_of` race in commit `e3805ad790`, database-issues#8698, was exactly this shape). `AS OF` SQL queries return wrong results.

## Antithesis angle

- Kill clusterd mid-`compare_and_append`. On restart, the cached upper must be refreshed before the next append.
- Concurrent reclock writers (two storage workers racing during a transient split-brain): both attempt CaS; only one wins; the other's local upper must catch up before it tries again.
- Inject persist consensus latency to widen the cache-staleness window.

## Open question (resolved)

Q: Does the reclock retry loop in `ReclockOperator::mint` (reclock.rs:160-166) protect against this, or is the bug in code that doesn't go through `sync`?

A: The retry loop does protect — but only if `sync()` is called *before* the local upper is used in subsequent code. The historical bug (`e3805ad790`) was in the `as_of` computation path which ran *outside* `mint` and used a cached upper from the read handle. Workload-level monotonicity assertion is sufficient to catch both paths.

## Existing instrumentation

None. The persist-side `panic!("compare_and_append failed: …")` in `reclock/compat.rs:306` is informational, not a property. Wrap with `assert_unreachable!` for the genuinely-invalid case and add an `assert_always!` for the workload-observable monotonicity.

## Implementation status

Implemented 2026-05-11 (workload-side) as `test/antithesis/workload/test/anytime_kafka_frontier_monotonic.py`. The `anytime_` driver runs throughout the timeline alongside other drivers while faults are active. Each poll iteration:

1. Lists every source in `SOURCES = ["upsert_text_src", "none_text_src"]` that currently exists in the catalog (so an early-timeline poll before sources are created doesn't fire false negatives).
2. For each source, calls `helper_source_stats.offset_committed()` (a `MAX(offset_committed)` over `mz_internal.mz_source_statistics` joined to `mz_sources` by name).
3. Compares against the previous observation for that source in `last_seen`. The assertion `always("kafka: source offset_committed non-monotonic", details)` fires only when both observations succeeded — partition/clusterd unavailable is expected under faults and not an assertion target.

`details` carries `source`, `previous`, `observed`, and `regression` (`previous - observed`).

The SUT-side `assert_always!` in `append_batches` and the `reclock/compat.rs` `compare_and_append` paths (commit `e3805ad790`'s and `505dc96aaa`'s code paths) are deferred — the workload signal is sufficient to catch any externally-visible regression. Add SUT instrumentation later if Antithesis surfaces failures that need internal localization.

The complementary `offset-known-not-below-committed` property is similar shape and could be added to this same driver with minimal cost; that's deliberately deferred to keep this commit scoped to the user-requested three properties.

## Provenance

Surfaced by: Data Integrity, Distributed Coordination. Direct regression target for commits `e3805ad790` and `505dc96aaa`.
