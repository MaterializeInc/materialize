# REFRESH EVERY MV first-refresh anchor: surprising interaction with read-hold slack

## The observed symptom

In the `adapter-read-holds-slack` branch, the `Coordinator::advance_timelines`
loop downgrades global timeline read holds to `oracle.read_ts() - 30s` instead
of `oracle.read_ts()`. The intent is to keep some history available for reads.
Most cluster-level tests are unaffected, but
`test/sqllogictest/materialized_views.slt` fails the `mv_aligned_to_past` case:
a fresh MV with `REFRESH EVERY '10s' ALIGNED TO mz_now() - 100s + 3s`, queried
right after creation, returns a snapshot from before two earlier-in-the-test
inserts and returns immediately instead of blocking ~3s for the next refresh.

## Where the behavior comes from

`Coordinator::select_timestamps`
(`src/adapter/src/coord/sequencer/inner/create_materialized_view.rs:840-908`)
computes the new MV's `storage_as_of` for refresh-schedule MVs as:

```rust
let least_valid_read = read_holds.least_valid_read();
// ...
let first_refresh_ts = refresh_schedule.round_up_timestamp(*least_valid_read_ts);
storage_as_of = Antichain::from_elem(first_refresh_ts);
```

That is: **the MV's first refresh time is the next schedule point at or after
the least valid read of its inputs.**

Pre-slack, `read_holds.least_valid_read()` for inputs in a global timeline
coincided with `oracle.read_ts()` (because the timeline's read holds pinned
input sinces tightly to `oracle.read_ts()`). So `round_up(least_valid_read_ts)`
was effectively `round_up(now)` — always the next schedule point in the future.

With 30s of slack, `least_valid_read_ts` is now `oracle.read_ts() - 30s`. For
a 10s-period schedule, `round_up(now - 30s)` lands ~27 seconds in the past.
The MV is created with a `storage_as_of` in the past, the past schedule points
process immediately as the dataflow catches up, and the MV ends up tracking
the snapshot of its inputs as of the most recent past schedule point — not as
of MV creation. Strict-serializable queries against the MV at
`oracle.read_ts() ≈ now` answer immediately (upper has already raced past
`now`), but the contents they see are an older snapshot.

The query timestamp picker is unaffected — under StrictSerializable it still
uses `oracle.read_ts()` as a lower bound, regardless of slack. The bug is
entirely in the MV's *creation-time anchor* for the first refresh.

## Is the current contract what we want?

I think no. Two arguments.

**The user-facing expectation for REFRESH EVERY is that the MV is at least as
fresh as the moment it was created.** When someone writes `CREATE MATERIALIZED
VIEW … WITH (REFRESH EVERY '10s' ALIGNED TO …)`, they expect that immediately
after creation the MV reflects the current state of its inputs (up to the next
refresh boundary). `ALIGNED TO` is meant to pin the *phase* of the schedule,
not to resurrect past schedule points and have the MV materialize against
them. Using `ALIGNED TO mz_now() - 100s` to phase-align a 10s schedule is a
perfectly reasonable pattern; under the current contract, that pattern
silently makes the MV start arbitrarily far in the past whenever any read
hold is held back from `oracle.read_ts()`.

**The current behavior was not a deliberate choice — it was load-bearing on
an implicit invariant.** The code uses `least_valid_read` because that's what
the dataflow's `dataflow_as_of` legitimately needs (the dataflow can't read
its inputs from before their since). But for `storage_as_of` — the visible
"MV first refresh" — anchoring to `least_valid_read` was only correct because
`least_valid_read` happened to equal "now." Once we let collection sinces lag
(which is exactly what the slack is for), the two diverge and the wrong
anchor produces surprising results. This is the classic shape of a contract
that was never written down: you can't tell from the code that it depended on
`least_valid_read == oracle.read_ts()`, and any change that breaks that
equality (slack, RETAIN HISTORY interactions, paused clusters holding sinces
back, etc.) silently shifts the MV's effective creation point into the past.

**What the contract should be:**

> The first refresh of a REFRESH EVERY MV is the smallest scheduled time
> strictly greater than wall-clock now (equivalently, `oracle.read_ts()`) at
> MV creation. The dataflow's `dataflow_as_of` may be earlier (so warmup can
> begin), but the MV's *visible* contents do not regress before creation.

Concretely: split the two anchors. `dataflow_as_of` keeps using
`least_valid_read` (it must — the dataflow can't read earlier than that).
`storage_as_of` uses `round_up(oracle.read_ts())` instead. That keeps the
MV's first visible refresh in the future regardless of what slack, RETAIN
HISTORY, or paused clusters do to input sinces.

There's a small subtlety: if `oracle.read_ts() > least_valid_read`, you also
need `dataflow_as_of ≤ storage_as_of`, which the existing
`dataflow_as_of.join_assign(...)` line already arranges. So the change is
roughly "replace the round_up anchor; keep the dataflow_as_of join."

## Implications for the failing test

If we adopt the contract above, the SLT does not need to change —
`mv_aligned_to_past` will go back to blocking ~3 seconds for the next-future
refresh and returning all 7 rows. The test is doing its job: it caught a real
semantic regression, not a side-effect.

If we *don't* adopt that contract — i.e., we explicitly decide "MVs created
in a slack window may materialize in the past" — then the test becomes wrong:
querying a freshly-created MV would no longer be guaranteed to reflect the
inputs as of creation, and we'd need to update the SLT (and probably document
the new semantics for users). I'd push back on this option; the
surprise/sharp-edge cost seems much higher than the benefit.

## Suggested next step

Make the two-anchor change in `select_timestamps`, run the materialized-views
SLT, and confirm the test passes unchanged. That removes one of the test
failures from the slack PR's blast radius and resolves the design question
without weakening any existing user-visible guarantee.
