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

## Provenance

Surfaced by: Data Integrity, Distributed Coordination. Direct regression target for commits `e3805ad790` and `505dc96aaa`.
