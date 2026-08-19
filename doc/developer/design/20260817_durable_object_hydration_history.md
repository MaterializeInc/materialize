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
- Default off in production, on in CI.

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
- **Unmanaged replicas.** Their worker count is unknown, and without it a
  cross-worker aggregate cannot be shown to be complete.

## Design

Compute stamps three replica-side timestamps per export and worker: `installed_at`
when the dataflow is installed, `started_at` when it is unsuspended, and
`hydrated_at` when the output frontier passes the as-of. They are added to the
existing `mz_introspection.mz_compute_hydration_times_per_worker` log, which keeps
its name, OID, and object kind, so its generated per-replica index and every
relation built on it are unaffected. A consumer doing `SELECT *` sees three new
columns.

Renaming the log and leaving a compatibility view behind was considered, since the
compute half of this project proposes it. Not done here. It would move OID 16977 to
a differently shaped relation and flip the old name from a log to a view. Naming
that relation is the compute team's call on their own change.

A coordinator task visits one managed user replica per interval. It installs an
internal subscribe on that replica which aggregates complete worker rows, maps
runtime export ids to catalog objects, anti-joins against the history table, and
writes the missing rows through the timestamped OCC read-then-write path.

## History Table

```text
mz_internal.mz_object_hydration_history
  object_id     text         not null
  cluster_id    text         not null
  replica_id    text         not null
  installed_at  timestamptz  not null
  started_at    timestamptz  null
  finished_at   timestamptz  null
  status        text         not null
  key (object_id, replica_id, installed_at)
```

`installed_at` is part of the episode identity because it is replica-stamped and
stable across an environmentd restart. `started_at` cannot serve that role, it is
null while an object waits to run.

Only terminal `hydrated` rows are written today, so every row has a finish time.
The nullable columns and the `status` column reserve a compatible shape for
episodes that are canceled or unfinished, once those become observable. Retention
keys off `finished_at`, so an unfinished episode needs a second age basis before it
can be recorded at all.

The table is in `mz_internal`, not `mz_catalog`, because its contents are best
effort and `status` will gain values. `mz_internal` carries no stability
commitment. Its closest sibling, `mz_internal.mz_object_arrangement_size_history`,
sits there for the same reason.

One index on `object_id`, on the catalog server, serves the question users ask:
how long did this object take to hydrate. The collector's anti-join has the same
shape but cannot use it, because that subscribe runs on the targeted *user*
replica where the index does not exist. Retention scans by `finished_at` and is
deliberately left unindexed. It runs once per sweep over a table bounded by
retention, and a second arrangement costs every environment memory.

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
at. That property is also the source of the frontier-skew limitation below.

## What the aggregation has to guard against

Three conditions in the collection query each prevent a specific wrong row. None
of them are obvious from reading the query.

**Group by dataflow, not by catalog item.** A materialized view can own two
`GlobalId`s at once, because a replacement installs the new dataflow's id while the
old one still serves reads. Grouping by item mixes both dataflows' worker rows,
presents twice the expected worker count, and records neither episode.

**Require `max(installed_at) <= min(hydrated_at)`.** A replica whose processes
restarted at different times can otherwise present worker 0 from the old process
and worker 1 from the new one, satisfy the worker-count check, and yield one row
whose duration silently includes the downtime between two episodes.

**Report `started_at` only when every worker observed one.** A partially observed
start is not the episode's start. An import-free dataflow is never suspended, so
its `Schedule` can arrive after it has already hydrated, and recording that late
arrival would invent an interval nobody measured.

In practice index exports report a start and materialized view exports frequently
do not, so `started_at` is optional for consumers. What always holds is
`installed_at <= finished_at`, and `installed_at <= started_at <= finished_at` for
a non-null start. Why materialized view exports so often observe no start is worth
running down before `started_at` is presented as a queueing signal.

An episode's interval spans workers, using the minimum installation and start and
the maximum completion, so it still includes clock skew between worker processes.

Multi-export dataflows would need one more step. All exports of a dataflow share a
suspension token, so the dataflow starts only once every export is scheduled, while
the stamp is per export. Every compute dataflow has exactly one export today and
`sequential_hydration.rs` asserts it, so the two coincide.

## What is and is not recorded

Collection samples current state. It is not an event log, and the log it reads
retracts an object's row when the export goes away. An episode is recorded only if
its row is still live when its replica's turn comes around, so an object dropped
before then, or a replica process that restarts before then, leaves no trace. An
object that never reports a completion time, such as a constant materialized view
whose frontier jumps straight to empty, is never recorded.
`test/testdrive/hydration-status.td` asserts that absence by name, so the limit is
pinned rather than merely tolerated.

Making these durable requires compute to emit hydration transitions into an
append-only collection that survives until an observer acknowledges them. Until
then, best effort is the honest description of a sampler.

## Retention

`hydration_history_retention_period` defaults to 30 days. Retention is another OCC
mutation: it subscribes to rows older than the cutoff and writes their retractions
at the observed frontier. Collection applies the same cutoff, so a still-live log
row cannot resurrect an episode retention just retracted.

Retention deletes a bounded batch per sweep and converges over as many sweeps as it
takes. The bound is not a nicety. The OCC path refuses a selection larger than
`max_result_size` before submitting any write, so one unbounded delete over a large
backlog would fail identically forever and never shrink the table. The bound has to
sit inside a derived table, because a top-level `LIMIT` lands in the plan's
`RowSetFinishing`, which the OCC path deliberately discards, and the delete would
be silently unbounded again.

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

Builtin tables are reset at bootstrap and re-shard on a forced schema migration.
The history table is exempt from both, because a sampled history cannot be rebuilt
from anything else once it is gone. Those two exemptions are the whole promise.

We do not commit to carrying the contents across every future version. A schema
change to a builtin table allocates a fresh shard, so changing these columns clears
the history. That is an acceptable trade: the value here is the distribution the
table accumulates, and starting it over costs one retention period, while freezing
the schema to protect it costs every later improvement.

So we try not to break it and we do not promise not to. The exemption exists to
stop an incidental migration from wiping the table as a side effect of unrelated
work, not to make it untouchable. Giving it up is deliberate: an assert in
`plan_migration` fires if a migration step ever names this table, and the comment
there says to remove the assert and the exemption together and to note in the
release notes that the history restarts. The user-facing documentation says the
matching thing, that this is a record to look at and not a data source to build on.

## Scheduling and isolation

`hydration_history_collection_interval` sets the cadence and disables collection at
zero. Sweeps never overlap, which bounds compute load and keeps the collector from
contending with itself in the serialized timestamped-write path.

Fires align to interval boundaries, and each sleep is capped, so that lowering a
long interval at runtime takes effect within the cap rather than after the old
interval elapses. Tests depend on that. A disabled collector polls far more
coarsely, since that is the cadence of every environment in the default
configuration.

Two isolation decisions are worth stating outright:

**Background mutations take no OCC write permit.** A session's wait for a permit is
bounded by its statement timeout. A sweep has no statement timeout, and its
subscribe must first hydrate a dataflow on a user replica, so holding a permit
would let a background sampler stall user DML for as long as that takes. The sweep
is single-flight, so skipping the permit adds at most one concurrent
read-then-write.

**The sweep is aborted when the coordinator is dropped.** Unlike a session it holds
no `Client`, so nothing otherwise stops it from outliving the coordinator, and the
runtime teardown that follows drops the timestamp oracle's worker task. A sweep
still running then reads a timestamp from a dead oracle and panics. The coordinator
owns the task handle so that dropping it cancels the sweep first.

Each mutation has its own deliberately generous timeout. Even a mutation with
nothing to write waits for its read to linearize, which can take a full
`default_timestamp_interval`, a parameter with no upper bound, so a tighter bound
would let a large timestamp interval starve retention permanently.

Replica drop, cluster drop, dependency replacement, replica failure, and timeout all
skip the attempt, and a later sweep recomputes from current state. Read-only
generations do nothing. Replicas with introspection disabled are skipped, since
their log arrangements exist but are never populated, so a subscribe would read a
sealed, empty collection on every sweep forever.

## Known limitation: frontier skew

The write timestamp is the subscribe's observed frontier rather than a fresh oracle
timestamp, for the reason given above. That frontier is the minimum over the
subscribe's inputs, and one input is a replica-local introspection log whose
frontier the *replica* advances from its own clock, rounded up to its introspection
interval.

So a replica whose clock trails environmentd by more than one introspection
interval produces a frontier that never gets ahead of the timestamp oracle. Every
attempt loses its timestamp race and the step exhausts its retries. The consequence
is bounded. Nothing wrong is recorded, that replica records nothing at all, and the
sweep stretches while it retries, delaying others in the rotation. It resolves
itself when the clocks converge.

Accepted for now: skew between processes is normally milliseconds while the
tolerance is a whole introspection interval, and the failure mode is silent retry
rather than wrong data. Because the symptom is otherwise hard to attribute,
exhausting retries logs that the replica's introspection frontier may be trailing.
Removing the limitation means deriving the write timestamp from the target table's
own frontier, which needs a correctness argument for applying replica-derived diffs
at a timestamp the replica never observed.

## Notes on the catalog plumbing

Two things here surprise on first contact and cost a CI round trip each.

Adding a builtin index changes the fingerprint of `mz_catalog.mz_indexes`, which
inlines the builtin index set as a VALUES list precisely so that this happens. The
fingerprint check then panics at catalog open unless an explicit
`MigrationStep::replacement` for `mz_indexes` is declared at the current dev
version. This is deliberate, it guarantees stale data is never served, and it means
adding an index is never only an index.

The `test/sqllogictest/autogenerated/*.slt` goldens are generated from the
user-facing docs markdown, but the sqllogictest run compares them against the live
catalog. Editing a column comment in the markdown without editing it in the Rust
builtin definition produces a golden that passes the docs lint locally and fails in
CI.

## Rollout

The interval is zero, and so disabled, in production, and runtime configuration can
enable it without restarting environmentd. The mzcompose configuration enables it at
60 seconds so hydration, restart, retention, and catalog tests exercise the path.

It stays off in the sqllogictest runner defaults, against the usual preference for
enabling new paths in tests. The collector installs subscribes and writes a builtin
table, while those runs assert on catalog contents and plans, so enabling it risks
churn and timing flakiness in files that have nothing to do with hydration.

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
