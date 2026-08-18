# Durable Object Hydration History

## Context

Materialize exposes current hydration state, but that state disappears when a
dataflow or replica restarts. A user can tell whether an object is hydrated now,
but cannot tell how long the previous hydration took.

This design records completed compute-object hydration episodes in a durable
catalog table. It is the first stage of broader hydration history. Replica-wide
episodes, resource peaks, failed episodes, and storage objects require signals
that do not exist yet and are described under Future Work.

## Goals

- Record successful hydration of indexes and materialized views per replica.
- Preserve history across environmentd and replica restarts.
- Make writes idempotent across concurrent environmentd processes.
- Preserve the shape and values of existing hydration relations.
- Bound storage with configurable retention.
- Keep collection disabled by default in production while exercising it in CI.

## Non-Goals

- Failed or canceled hydration. A replica cannot report its own crash, and the
  current-state compute log retracts an object without recording why.
- Replica-wide episodes. Correct episode boundaries require the initial object
  set and explicit transitions between fully hydrated and hydrating states.
- Resource peaks. The existing metrics history contains samples, not true
  high-water values. Reporting those samples as peaks would be misleading.
- Source and sink hydration. Storage does not publish equivalent lifecycle
  timestamps.
- Unmanaged replicas. Their Timely worker count is unknown, so the collector
  cannot prove that a cross-worker aggregate is complete.

## Overview

Compute publishes three replica-stamped timestamps for every export and worker:

- `installed_at`, when the dataflow is installed
- `started_at`, when the dataflow is unsuspended
- `hydrated_at`, when the output frontier passes the as-of

The three columns are added to the existing
`mz_introspection.mz_compute_hydration_times_per_worker` log, next to the
existing `time_ns`. The relation keeps its name, OID, and object kind, so its
auto-generated per-replica index is unchanged and every aggregate relation built
on it keeps reading the same columns with the same values. The change is additive
rather than byte-identical: a consumer doing `SELECT *` sees three new columns.

A rename plus a compatibility view was considered, since that is what the compute
half of this project proposes. It is not done here. It would move OID 16977 to a
differently shaped relation, flip the old name from a log to a view, and rename
the generated per-replica index. Naming that relation is the compute team's
decision on their own change, and this one does not need it.

A background task visits one managed user replica per interval. It installs an
internal subscribe on that replica. The subscribe aggregates complete worker
rows, maps runtime export IDs to catalog object IDs, and anti-joins the result
against the history table. The task writes the resulting rows at the subscribe
frontier using the timestamped OCC write path.

## What is and is not recorded

Collection samples current state. It is not an event log, and the compute log it
reads retracts an object's row when the export goes away. So an episode is
recorded only if its row is still live when its replica's turn comes around:

- An object dropped before the next sweep of its replica is not recorded.
- A replica process that restarts before the next sweep loses the episode that
  preceded the restart.
- An object that hydrates without the replica ever reporting a completion time,
  such as a constant materialized view whose frontier jumps straight to empty, is
  never recorded. The collector requires every worker to report one.
  `test/testdrive/hydration-status.td` queries such an object by name and expects
  no row, so the limit is asserted rather than surprising.

Making these cases durable requires compute to emit hydration transitions into an
append-only collection that survives until an observer acknowledges them. That is
compute-side work and is listed under Future Work. Until then the table is
documented as best effort, which is the honest description of a sampler.

## History Table

`mz_internal.mz_object_hydration_history` has this shape:

```text
object_id     text         not null
cluster_id    text         not null
replica_id    text         not null
installed_at  timestamptz  not null
started_at    timestamptz  null
finished_at   timestamptz  null
status        text         not null
key (object_id, replica_id, installed_at)
```

The initial implementation writes only terminal `hydrated` rows, so every row
has a finish timestamp. A start timestamp is present only when it was observed,
see Compute Timestamps below. Nullable columns reserve a compatible
representation for an episode that is canceled before it starts or never
finishes once those events become observable. Retention keys off `finished_at`,
so an unfinished episode needs a second age basis before it can be recorded.

The table lives in `mz_internal` rather than `mz_catalog` because its contents
are best effort, and its `status` column and nullable timestamps will gain
meanings as more hydration events become observable. `mz_internal` carries no
stability commitment, which is the same reason its closest sibling
`mz_internal.mz_object_arrangement_size_history` lives there.

`installed_at` is the episode identity. It is stamped when the replica creates
the export and remains stable across an environmentd restart. `started_at` is
not suitable as the identity because it is null while the object waits to run.

One index, on `object_id`, is maintained on the catalog server. It serves the
question users ask of this table, how long a given object took to hydrate. The
collector's anti-join has the same shape but cannot use it: that subscribe runs
on the targeted user replica, where the index does not exist. Retention scans by
`finished_at` and is deliberately left unindexed, since it runs once per sweep
over a table bounded by retention, and a second arrangement on the catalog
server costs every environment memory.

## Compute Timestamps

Compute logging event times use a Unix epoch anchor advanced by a monotonic
clock. Timestamp values come from the event time, not the rounded differential
timestamp used to publish the update. Logging cadence can delay visibility but
does not reduce timestamp accuracy.

The row state moves through these forms:

```text
(installed_at, null, null)
(installed_at, started_at, null)
(installed_at, started_at, hydrated_at)
```

A start is only recorded if the replica observed one before reporting completion.
An import-free dataflow is never suspended, so its `Schedule` can arrive after it
has already hydrated, and recording that late arrival as the start would invent an
interval nobody observed. In practice index exports report a start and
materialized view exports frequently do not, so consumers must treat `started_at`
as optional. What always holds is that `installed_at <= finished_at`, and that
`installed_at <= started_at <= finished_at` for a non-NULL start.

Why materialized view exports so often report no start is worth running down
before `started_at` is presented to users as a hydration-queueing signal. It is
recorded honestly today rather than filled in with a value nobody measured.

Worker rows are grouped per dataflow, not per catalog item. A materialized view
can own two `GlobalId`s at once, because a replacement installs the new
dataflow's id while the old one still serves reads, and grouping by item would
mix both dataflows' worker rows and record neither.

Each worker has its own wall-clock anchor. A completed object row uses the
minimum installation and start times and the maximum completion time across
workers. A start is reported only when every worker observed one, since a
partially observed start is not the episode's start. The collector requires exactly the configured worker count and requires
every worker to have reported completion. It additionally requires
`max(installed_at) <= min(hydrated_at)`, which rejects worker rows that cannot
belong to one episode: a replica whose processes restarted at different times can
otherwise present one worker from before the restart and one from after, satisfy
the count check, and produce a row whose duration includes the downtime between
two episodes. The resulting interval can still include clock skew between worker
processes.

Multi-export dataflows would need one more step. All exports of a dataflow share
a suspension token, so the dataflow only starts once every export is scheduled,
while the stamp is per export. Every compute dataflow has exactly one export
today and `sequential_hydration.rs` asserts it, so the two coincide. The code
records what has to change if that stops holding.

## OCC Collector

The subscribe reads both the replica-local timestamp log and the physical
history table. Including the target table in the read expression is required for
distributed idempotence.

Assume two environmentd processes compute the same missing row at frontier `T`:

1. Both submit a timestamped write for `T`.
2. One write commits and advances the table upper and timestamp oracle.
3. The other write receives `TimestampPassed`.
4. Its subscribe observes the committed history row.
5. The anti-join retracts the candidate, leaving no row to write.

The losing writer never retries stale diffs at the next eligible timestamp. It
waits for subscribe progress and retries only with state known to be valid at
the observed frontier.

Background subscribes use the ordinary active-compute-sink lifecycle but have a
background owner rather than a synthetic SQL session. They do not write an
`mz_subscriptions` row. Dropping the handle retires the compute dataflow on
success, error, timeout, or replica failure.

## Scheduling and Failure Handling

`hydration_history_collection_interval` controls cadence. A zero duration
disables collection. The task waits for each replica attempt to finish before
scheduling the next one, which bounds compute load and avoids self-contention in
the globally serialized timestamped-write path.

Fires are aligned to interval boundaries, and each scheduler sleep is capped so
that a configuration change takes effect within the cap rather than after the
previous interval has elapsed. Without the cap, lowering a long interval at
runtime, which tests do, would appear to do nothing. A disabled collector polls
much more coarsely, since that is the cadence of every environment in the default
configuration and nothing is waiting on it.

Background mutations do not take an OCC write permit. The permit bounds how many
read-then-writes run at once, and a session's wait for one is bounded by its
statement timeout. A sweep has no statement timeout, and its subscribe has to
hydrate a dataflow on a user replica first, so holding a permit would let a
background sampler stall user DML for as long as that takes. The sweep is
single-flight by construction, one replica and one mutation at a time, so it adds
at most one concurrent read-then-write rather than an unbounded number.

Each mutation is bounded by its own timeout. The bound has to be generous: a
mutation that finds nothing to write still waits for its read to linearize, which
can take a full `default_timestamp_interval`, and that parameter has no upper
bound. A shared, tighter bound would let a large timestamp interval starve
retention permanently.

The replica list is refreshed before every attempt. Replica drop, cluster drop,
dependency replacement, replica failure, and timeout skip the attempt. A later
cycle recomputes from current state. Read-only environmentd generations do not
install subscribes or buffer writes.

Managed replicas expose their total worker count. Unmanaged replicas are skipped,
because without that count the collector cannot distinguish a complete
cross-worker aggregate from a partial one, and the public documentation says so.
Replicas with introspection disabled are skipped too: their log arrangements are
installed but never populated, so a subscribe would read a sealed, empty
collection on every sweep.

## Retention and Restart

`hydration_history_retention_period` defaults to 30 days. Retention is another
OCC mutation. It subscribes to rows with `finished_at` before the cutoff and
writes their retractions at the observed frontier. The insertion query applies
the same cutoff so a current-state log cannot resurrect a row just removed by
retention.

Retention deletes a bounded batch per sweep, and converges over as many sweeps as
it takes. It has to be bounded: the OCC path refuses a selection larger than
`max_result_size` before submitting any write, so one unbounded delete over a
large backlog would fail identically on every sweep and never shrink the table.
The bound lives inside a derived table, because a top-level `LIMIT` lands in the
plan's `RowSetFinishing`, which the OCC path deliberately discards.

Retention runs on the catalog server cluster, so it keeps working when there are
no user replicas at all, and it runs even when that sweep's replica collection
failed. A crash-looping replica must not be able to stop the table from shrinking
back to its bound. The dependency does not run the other way: a catalog server
without a replica skips retention for that sweep and leaves collection running.

Disabling collection also suspends retention. The alternative is an always-on
background subscribe in the default configuration, where the table is empty and
there is nothing to retain. The consequence is that rows already collected are
kept while collection is off, which the user-facing documentation states.

Builtin tables are normally reset during environmentd bootstrap. The history
table is explicitly retained and is protected from replacement schema
migrations, which would otherwise allocate a fresh shard and discard its data.

### Durability is best effort

Those two exemptions are the extent of the promise. We do not commit to carrying
this table's contents across every future version. A schema change to a builtin
table is implemented by allocating a fresh shard, so a change to these columns
clears the accumulated history, and there is no other source to rebuild it from.
That is an acceptable trade rather than a bug to design around: the value of the
table is in the distribution it accumulates, and starting that distribution over
costs one retention period, whereas permanently freezing the schema to protect it
costs every improvement we might want to make.

So the position is that we try not to break it and we do not promise not to. The
exemption from forced migration exists to stop an incidental migration from
wiping the table as a side effect of unrelated work, not to make the data
untouchable. Giving it up is a deliberate act: an assert in `plan_migration`
fails if a migration step ever names this table, and the comment there says to
remove both the assert and the exemption, and to note in the release notes that
the history restarts. The user-facing documentation says the same thing in the
other direction, that this is a diagnostic record to look at and not a data
source to build on, so that nobody outside builds something that a schema change
would break.

## Rollout

The collection interval defaults to zero in production. Test and CI defaults
enable collection so hydration, restart, retention, and catalog tests exercise
the path. Runtime configuration can enable the collector without restarting
environmentd.

## Known Limitation: Frontier Skew

The write timestamp is the subscribe's observed frontier, not a fresh oracle
timestamp. That is deliberate, the diffs are only known to be correct as of a
frontier we actually observed, so a conflict waits for subscribe progress instead
of jumping ahead. The frontier is the minimum over the subscribe's inputs, and one
of those inputs is a replica-local introspection log whose frontier the replica
advances from its own clock, rounded up to its introspection interval.

So a replica whose clock trails `environmentd` by more than that interval produces
a frontier that never gets ahead of the timestamp oracle. Every attempt loses its
timestamp race, and the step exhausts its retries and gives up. The consequence is
bounded: nothing wrong is recorded, that replica records nothing at all, and the
sweep also stretches while it retries, which delays the other replicas in the
rotation. It resolves itself as soon as the clocks converge.

This is accepted for now. Skew between processes is normally in the milliseconds
while the tolerance here is a whole introspection interval, and the failure is
silent retry rather than wrong data. Because the symptom is otherwise hard to
attribute, exhausting retries logs that the replica's introspection frontier may
be trailing the write frontier. Removing the limitation means deriving the write
timestamp from the target table's own frontier, which needs a correctness argument
for applying replica-derived diffs at a timestamp the replica never observed.

## Test Enablement

Collection is on in the mzcompose configuration at a 60 second interval, and off
in the sqllogictest runner's defaults. Enabling it there too would exercise the
path more broadly, but the collector installs subscribes and writes to a builtin
table, and those runs assert on catalog contents and plans, so it risks plan churn
and timing flakiness in files that have nothing to do with hydration.

## Future Work

The full hydration visibility surface needs additional durable events:

- Record installation and start observations before completion, then finalize
  canceled and failed episodes by joining replica lifecycle events.
- Define a replica episode state machine using the object set present when the
  replica transitions from fully hydrated to hydrating.
- Publish resettable per-process high-water values for RAM, swap, and scratch
  disk. Define replica aggregation without pretending that sampled maxima are
  simultaneous peaks.
- Give storage objects equivalent lifecycle timestamps.
- Build replica history and progress views from those authoritative signals.
