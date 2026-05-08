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

## What we tried, and what actually worked

The first attempt was to anchor `storage_as_of` directly on "now" — first
`self.now()`, then `oracle.read_ts()`. Both broke other cases:

* `REFRESH AT mz_now()` schedules: the AT point captured at plan time is at
  `T_plan`, but `self.now()` (and sometimes `oracle.read_ts()` at
  `select_timestamps` time) is strictly greater than `T_plan` because of the
  time spent between planning and timestamp selection. `round_up_timestamp`
  is "smallest tick ≥ x" — pushing `x` past the only AT point makes it
  return `None`, which surfaces as
  `MaterializedViewWouldNeverRefresh`.
* `REFRESH EVERY 1d` (no `ALIGNED TO`, defaulting to plan-time `mz_now()`):
  same problem — pushing the anchor past the alignment point makes
  `round_up` skip the at-creation tick and return the *next* tick, one full
  period (a day) in the future. Subsequent SELECTs block waiting for an
  upper that won't advance for 24h.

The version that actually works (and is what's in the DNM stack today) is
conditional. Keep `least_valid_read` as the default round-up anchor — that
preserves the pre-slack behavior and continues to honor captured plan-time
`mz_now()` points. Only for schedules with periodic refreshes (`!everies.is_empty()`),
if the resulting `first_refresh_ts` is strictly less than `oracle.read_ts()`,
bump it forward to the next tick at-or-after `oracle.read_ts()`. AT-only
schedules are left untouched. Concretely:

```rust
let initial = refresh_schedule.round_up_timestamp(*least_valid_read_ts);
let first_refresh_ts = if !refresh_schedule.everies.is_empty() {
    let oracle_read_ts = self.get_local_read_ts().await;
    match initial {
        Some(t) if t < oracle_read_ts => refresh_schedule
            .round_up_timestamp(oracle_read_ts)
            .or(Some(t)),
        other => other,
    }
} else {
    initial
};
```

CI on the slack PR confirms this fixes `mv_aligned_to_past` without
regressing the `REFRESH AT mz_now()` / `REFRESH AT CREATION` /
`REFRESH EVERY 1d` cases that the simpler anchor-on-now version had broken.

## Known remaining corner case

A periodic schedule whose first scheduled refresh is genuinely in the recent
past *because of* a planner→`select_timestamps` gap (e.g., an MV body
containing `mz_unsafe.mz_sleep(3)` makes the optimizer take a few seconds,
during which the EpochMilliseconds oracle advances on background writes)
will still be bumped one period forward instead of honoring the at-creation
tick. The conditional `t < oracle_read_ts` check can't tell that case apart
from the slack-induced past anchor case.

The principled fix is to thread the captured plan-time `mz_now()` from
`resolve_mz_now_for_create_materialized_view` (in
`src/adapter/src/coord/command_handler.rs`) all the way through to
`select_timestamps`, and use *that* value as the anchor (max'd with
`least_valid_read` so `dataflow_as_of <= storage_as_of` continues to hold).
That's invasive — it requires plumbing the captured timestamp through the
sequencing stages or stashing it alongside `txn_read_holds` — so the DNM
goes with the conditional bump. If we decide to ship the slack work for
real, the threaded version is the right cleanup.

## Implications for the failing test

`mv_aligned_to_past` does not need to be changed. With the conditional
bump the test goes back to blocking ~3 seconds for the next future refresh
and returning all 7 rows. The test was doing its job: it caught a real
semantic regression introduced by the slack, not a side-effect of the
test being too strict.
