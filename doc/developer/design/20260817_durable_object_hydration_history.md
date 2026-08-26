# Durable Object Hydration History

## Context

Materialize exposes current hydration state, but that state disappears when a
dataflow or a replica restarts. A user can tell whether an object is hydrated
now, but not how long the last hydration took, or whether today's is unusually
slow.

This records completed compute-object hydration episodes in a durable table. It
is the first stage of broader hydration history. Replica-wide episodes, resource
peaks, failed episodes, and storage objects need signals that do not exist yet.

## Goals

- Record successful hydration of indexes and materialized views per replica.
- Survive environmentd and replica restarts.
- Stay idempotent across concurrent environmentd processes.
- Leave the values of existing hydration relations untouched.
- Bound storage with configurable retention.

## Non-Goals

Each of these is excluded because the signal to do it correctly is missing, not
because it is unwanted:

- **Failed or canceled episodes.** A replica cannot report its own crash, and the
  current-state log retracts an object without recording why.
- **Replica-wide episodes.** Correct boundaries need the object set present at the
  transition from hydrated to hydrating, and explicit transitions between those
  states.
- **Resource peaks.** The metrics history holds samples, not high-water values.
  Presenting samples as peaks would mislead.
- **Sources and sinks.** Storage publishes no equivalent lifecycle timestamps.
- **Failed replicas.** A replica that never finishes hydrating an object records
  nothing for it, since only completions are stamped.

## Design

Compute already stamps three replica-side timestamps per export and worker in
`mz_introspection.mz_compute_hydration_times_per_worker`: `installed_at` when the
dataflow is installed, `started_at` when it is unsuspended, and `hydrated_at` when
the output frontier passes the as-of. See
`doc/developer/design/20260817_compute_hydration_timestamps.md`. This design adds
the history table and the sweep that writes those timestamps down.

A coordinator task visits one user replica per interval, in a rotation, skipping
replicas with introspection disabled, whose logs are installed but never populated.
It installs an internal subscribe on that replica which aggregates every worker's
completed rows, anti-joins them against the history table, and writes the missing
ones through the timestamped OCC read-then-write path.

Collection has no explicit batch bound. It returns at most one row per
not-yet-recorded dataflow, and the OCC path rejects a result that exceeds
`max_result_size` or `max_query_result_size`. At their 1 GiB defaults that ceiling
only matters at millions of dataflows per replica.

Two dyncfgs control it. `hydration_history_collection_interval` sets the sweep
cadence and disables collection at zero, which is the production default.
`hydration_history_retention_period` bounds how long rows live, defaulting to 30
days.

An *episode* throughout this document is one such row: one dataflow's hydration on
one replica, from installation to hydrated, recorded once.

## History Table

```text
mz_internal.mz_object_hydration_history
  object_id     text         not null
  cluster_id    text         not null
  replica_id    text         not null
  installed_at  timestamptz  not null
  started_at    timestamptz  null
  hydrated_at   timestamptz  null
  status        text         not null
```

An episode is identified by `(object_id, replica_id, installed_at)`. The
installation time is replica-stamped and stable across an environmentd restart. The
nullability of each column matches the compute log the values come from, so
`installed_at` is not null there and here, while the two later stamps are nullable.

`object_id` is named the way the other hydration relations name it, because to a
user these are objects that exist somewhere rather than exports of a dataflow. The
value is the id compute reports for the object's dataflow, and one catalog item can
own several dataflows at once, so consumers reach a catalog object through
`mz_internal.mz_object_global_ids` rather than joining `mz_objects` directly.

Only terminal `hydrated` rows are written today, so every row has a finish time.
The nullable columns and `status` reserve a compatible shape for canceled or
unfinished episodes once those become observable. Retention keys off `hydrated_at`,
so an unfinished episode needs a second age basis before it can be recorded at all.

The table lives in `mz_internal` to mark it unstable while it settles.

No index initially, and not a retained-metrics object either, both because of size.
The collector does anti-join by `(object_id, replica_id, installed_at)`, but its
subscribe runs on the selected user replica. An index arranged on the catalog
server cannot serve that dataflow, so it would pin the whole table without removing
the collector's recurring import and arrangement cost. That cost grows with retained
history and is accepted by this sampling design. Adding an index later is a
migration we can do when a query that can use it needs one. The retained-metrics
flag would independently pin a 30 day compaction window, so it stays off too.

## Why concurrent writers converge

The subscribe reads the history table it is about to write. That is what makes the
write idempotent across processes, and it is the reason the read expression looks
redundant.

Two environmentd processes computing the same missing row at frontier `T` both
submit a timestamped write for `T`. One commits and advances the table upper. The
other is told the timestamp passed, waits for its own subscribe to progress, sees
the committed row appear, and its anti-join retracts the candidate. There is
nothing left to write.

The losing writer never retries stale diffs at the next eligible timestamp. It
only retries with state it has observed to be valid at the frontier it is writing
at. That same property is what makes a trailing replica record nothing, which the
last section covers.

## Aggregating a replica's workers

Collection aggregates every worker's row for an export and records nothing until all
of them have hydrated, taking `max(hydrated_at)` and `min` of the two start stamps.

One worker would be enough for an index, whose output frontier is the arrangement
upper and therefore moves only as timely's dataflow-wide progress allows. It is not
enough for a materialized view. Its output frontier also depends on the sink write
frontier, and only the sink's single active worker, `hash(sink_id) % workers`,
tracks the shard upper. Every other worker stamps at compute completion, before the
snapshot is durable, and the sink buffers that snapshot until computation finishes,
so the gap is the whole write. Reading one worker would mean `hydrated_at` said
"durable" for some objects and "computed" for others, decided by a hash. The
aggregate gives one meaning for every object, and it is what
`mz_compute_hydration_times` already does, so the durable history and the live
signal agree.

Completeness uses no separately configured worker count. The replica owns this
introspection collection and recreates it as a unit on restart, so a snapshot cannot
combine rows from before and after a restart. Installation adds one row per
`(export_id, worker_id)` with a null `hydrated_at`, and
`count(*) = count(hydrated_at)` means every row visible at the read timestamp has
finished. An object still hydrating is skipped and picked up by a later sweep. The
cutoff and the anti-join apply to the aggregate's output, since as `WHERE` clauses
either one would drop visible unfinished rows and make that check trivially true.

Each process's logging clock also determines the logical timestamps of its updates.
A process whose clock is ahead can therefore place its row beyond the read timestamp
while the other rows are complete below it. The collector deliberately accepts this
sampling race rather than depending on replica configuration for an expected worker
count. In that case the durable finish can precede the latest worker's finish. This
is separate from restart behavior, which discards the replica collection as a unit.

The missing worker cannot later change the episode key. Its logging clock stamps
both the Differential update and `installed_at`, so appearing after the read means
its installation stamp is later than the visible minimum. A later sweep therefore
keeps the same `min(installed_at)` and the anti-join matches the row already written.
The worker can raise `max(hydrated_at)`, but the durable row is not repaired after
its episode key has been recorded.

Compute is adding an append-only lifecycle log for the same stages, currently
proposed in #38403: one row per export, worker and event, with a reason and the
dataflow's as-of. It leaves `mz_compute_hydration_times_per_worker` alone, so
everything above keeps its meaning and nothing recorded here is relabelled.

That log is the signal this design says is missing, and moving onto it later buys
three things at once: stages that mean the same thing on every worker, a reason
that distinguishes a replacement waiting for cutover from an index that will never
write, and the as-of, without which an interval says nothing about how much work
was done, since a replacement with a far behind as-of does far more of it in the
same wall-clock time. Recording should keep gating on hydration when that happens.
A replacement runs read-only and does not write until cutover, so gating on a write
stage would never record the hydration a deployment most wants to measure, and
would wait forever on a rollback.

Two consequences. An interval can span workers in different processes, each
anchoring its logging clock at its own `SystemTime`, so skew inflates a duration.
Nothing rejects a row for looking inconsistent, deliberately, because a guard on
cross-worker stamps rejects complete episodes permanently once the log values
settle. And `started_at` equal to `installed_at` means no start was observed rather
than an immediate start, since the log substitutes the installation time. An
import-free dataflow is never suspended, so that is the common case.

## What is and is not recorded

Collection samples current state rather than consuming events, so it writes down
whatever the replica collection holds when that replica's turn comes around. The
log retracts an export's row when the dataflow goes away, so a short-lived object,
or one dropped before its turn, leaves no trace. Nothing wrong is recorded, it is
simply absent.

Making these durable requires compute to emit hydration transitions into an
append-only collection that survives until an observer acknowledges them. Until
then, best effort is the honest description of a sampler.

## Retention

Retention is another OCC mutation: it subscribes to rows older than the cutoff and
writes their retractions at the observed frontier. Collection applies the same
cutoff, so a still-live log row cannot resurrect an episode retention just
retracted.

Retention deletes one bounded batch per sweep. The fixed cutoff makes the eligible
set finite, and collection refuses to insert rows behind that cutoff, so later
sweeps continue draining the same backlog without starving collection. The batch
bound is not a nicety. The OCC path refuses a selection larger than
`max_result_size` before submitting any write, so one unbounded delete over a large
backlog would fail identically forever and never shrink the table. The bound has to
sit inside a derived table, because a top-level `LIMIT` lands in the plan's
`RowSetFinishing`, which the OCC path deliberately discards, and the delete would be
silently unbounded again.

`mz_hydration_history_retention_batch_full_total` increments when the batch deletes
all 1,000 rows. Repeated increments mean retention may not be keeping up. An operator
can lower `hydration_history_collection_interval` to schedule sweeps more often,
then compare appended and deleted row rates to confirm the backlog is shrinking.

Retention runs on the catalog server, so it keeps working when there are no user
replicas at all, and it runs even when that sweep's collection failed. A
crash-looping replica must not be able to stop the table from shrinking. The
dependency does not run the other way: a catalog server without a replica skips
retention and leaves collection running.

Disabling collection also suspends retention. The alternative is an always-on
subscribe in the default configuration, where the table is empty and there is
nothing to retain. Rows already collected are therefore kept while collection is
off.

## Durability is best effort

Builtin tables are reset at bootstrap and re-sharded on a forced schema migration.
This table is exempt from both, because a sampled history cannot be rebuilt from
anything else. Those two exemptions are the whole promise.

We do not promise to carry the contents across every future version. A shard
replacement clears the table, and that is an acceptable trade: what has value here
is the distribution the table accumulates, and starting over costs one retention
period, while freezing the schema to protect it costs every later improvement.
Schema *evolution* keeps the rows, so an additive column costs nothing.

Giving the exemption up is meant to be deliberate. An assert fires if a replacement
step ever names this table, and the comment there says to remove the assert and the
exemption together and to note in the release notes that the history restarts.

## Scheduling and isolation

Sweeps never overlap and each visits one replica, which bounds compute load and
keeps the collector from contending with itself in the serialized timestamped-write
path. Fires align to interval boundaries and each sleep is capped, so lowering a
long interval takes effect within the cap rather than after the old interval
elapses. The grid is offset by an amount seeded from the full environment id, since
the interval is one fleet-wide setting and an unshifted grid would have every
environment sweep at the same instant. Using the full id also separates regions and
ordinals belonging to one organization.

One replica per interval means an environment with `N` eligible replicas revisits
each one approximately every `N * interval`. Freshness therefore degrades linearly
with replica count. Lowering the interval improves freshness at the cost of more
replica dataflow installs.

**Background mutations take no OCC write permit.** The permits are one semaphore
shared by every read-then-write in the process, not one per table. A session's wait
is bounded by its statement timeout, a sweep's is not, and a sweep's subscribe has
to hydrate a dataflow on a user replica first, so holding a permit would let
background sampling stall user DML on any table for as long as that takes. Being
single-flight, skipping the permit adds at most one concurrent read-then-write.

**The sweep is aborted when the coordinator is dropped.** It holds no `Client`, so
nothing else stops it outliving the coordinator, and the teardown that follows drops
the timestamp oracle's worker task. A sweep still running would then read a
timestamp from a dead oracle and panic, so the coordinator owns the task handle.

**Each mutation's timeout is deliberately generous.** Subscribe installation,
replica-side progress, OCC conflict retries, and the external commit can all wait
indefinitely. The bound keeps one unavailable replica or service from starving
retention and every later replica in the single-flight rotation.

Replica drop, cluster drop, dependency replacement, replica failure, and timeout all
skip the attempt, and a later sweep recomputes from current state. Read-only
generations do nothing, which the next section covers.

## Upgrades record late, not never

A read-only generation writes nothing, and a 0dt upgrade hydrates the incoming
replicas while it is still read-only. That looks like it drops exactly the window
worth measuring, since that is when most objects hydrate at once.

It does not, because collection samples current state rather than events. The
stamps live in the replica's own log, the replicas keep running across promotion,
and the first sweep after promotion writes those episodes with the replica's
original `installed_at` and `hydrated_at`. The history is written late, the
timestamps are not.

What is genuinely lost is an episode whose dataflow goes away before that first
post-promotion sweep, which is the general sampling limit rather than anything
specific to upgrades.

## Known limitation: a trailing replica records nothing

The write timestamp comes from the timeline's oracle, and the subscribe's frontier
certifies that the loop has a complete view below it. The write happens once that
frontier reaches the target. One of the subscribe's inputs is a replica-local
introspection log whose frontier the *replica* advances from its own clock, rounded
up to its introspection interval.

For a non-empty selection, a replica whose clock trails environmentd by more than
one introspection interval repeatedly loses the same race. Its frontier eventually
certifies the current target, but by then keepalives or other writes have advanced
the oracle past it. The committer refuses the stale target, the OCC loop adopts a
new one, and the replica has to catch up again. The sweep's timeout normally ends
that loop. A lower `max_occ_retries` can end it first with a contention error.

Nothing wrong is recorded in either case. That replica records nothing at all, and
the sweep stretches while it retries, which delays others in the rotation. Empty
selections exit without entering the conflict loop. The condition resolves itself
when the clocks converge.

Accepted for now: skew between processes is normally milliseconds while the
tolerance is a whole introspection interval, and the failure mode is retrying rather
than wrong data. Because the symptom is otherwise hard to attribute, both a timeout
and retry-budget exhaustion log that the replica's introspection frontier may be
trailing.

## Rollout

Collection is disabled by default, and the interval is runtime configurable, so
enabling it needs no restart. The plan is to enable it in CI when this merges, then
in staging, then in production, a week apart, so each step has a week of real
traffic behind it before the next.

CI enables it at a 60 second interval through the mzcompose configuration, which is
what exercises the hydration, restart, retention, and catalog tests. It stays off in
the sqllogictest runner defaults, against the usual preference for enabling new
paths in tests: the collector installs subscribes and writes a builtin table, while
those runs assert on catalog contents and plans.

Background collection always uses frontend OCC, independently of the session
`frontend_read_then_write` rollout flag. This is safe while the lock path remains
available because the target is a system table, which user DML can neither read nor
write. The background OCC entry point enforces that target contract rather than
relying on each maintenance caller to remember the rollout constraint.

## Future Work

- Record installation and start before completion, then finalize canceled and
  failed episodes by joining replica lifecycle events.
- Define a replica episode state machine from the object set present at the
  hydrated-to-hydrating transition.
- Publish resettable per-process high-water values for RAM, swap, and scratch disk,
  and define replica aggregation without pretending sampled maxima are simultaneous
  peaks.
- Give storage objects equivalent lifecycle timestamps.
- Build replica history and progress views on those signals.
