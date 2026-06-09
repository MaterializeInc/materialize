# reclock-mint-eventually-succeeds

## Summary

Under transient persist outages and competing writers, the reclock mint loop (`compare_and_append` with `UpperMismatch` retry, `src/storage/src/source/reclock.rs:160-166`) eventually completes for every source-frontier advance that has data to bind.

## Code

```rust
// src/storage/src/source/reclock.rs (around line 150-170)
loop {
    match handle.compare_and_append(updates, prev_upper, new_into_upper).await {
        Ok(()) => break,
        Err(UpperMismatch { current, .. }) => {
            self.sync(&current).await;
            // recompute updates and retry
        }
    }
}
```

There is no upper bound on this loop. It depends on the persist backend eventually being responsive and on competing writers not livelocking the source.

## Why this is a liveness property

Antithesis's job is to assert that the loop terminates in adversarial schedules. The catalog entry asserts both:

1. The retry path is *exercised* (the loop runs more than once at least once during a run): `Sometimes(saw_cas_retry)`.
2. The source frontier eventually advances past the contention point: a workload-observable liveness check.

## How to check it

SUT-side anchor:
- Add an `assert_sometimes!(reclock_cas_retry_succeeded, "reclock: mint compare_and_append retry succeeded")` immediately after a successful `compare_and_append` that was preceded by at least one `UpperMismatch`. The local counter is reset on each `mint()` invocation.

Workload-side liveness check:
- After injecting persist consensus latency or a competing-writer scenario, observe the source's `offset_committed` advancing in `mz_internal.mz_source_statistics_per_worker`. `assert_sometimes!(source_advanced_post_contention, …)`.

## What goes wrong on violation

The source's frontier stops advancing without any external signal that something is wrong. Health reports `Running`. The reclock operator is in an infinite `compare_and_append` → `UpperMismatch` → `sync` → `compare_and_append` cycle. To an operator looking from outside it looks like Kafka is the problem.

## Antithesis angle

- Inject high persist consensus latency. With many concurrent storage workers (or restart-induced competing writers), the CaS contention rate climbs and the retry loop runs many times. Antithesis tests that progress still happens.
- Race the metadata fetcher's partition-add against an in-flight mint. The mint is now reckoning with an extended `source_upper`; the CaS retry must recompute updates correctly.
- Concurrent kill+restart cycles that create competing-writer scenarios.

## Open question (resolved)

Q: Is there any input under which `compare_and_append` returns a non-retryable error and the loop should exit?

A: Yes — `InvalidUsage` errors (handled by `panic!("compare_and_append failed: {invalid_use}")` at `reclock/compat.rs:306`). Those terminate the source. The retry loop only handles `UpperMismatch`. Antithesis fault injection should not produce `InvalidUsage` under correct code; if it does, that is a separate property (`reclock-cas-no-invalid-usage`) but it falls under the broader `kafka-source-no-internal-panic` property already cataloged.

## Existing instrumentation

None. The retry loop is silent.

## Provenance

Surfaced by: Failure Recovery, Distributed Coordination.
