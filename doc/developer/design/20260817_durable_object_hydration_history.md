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
- **Failed replicas.** A replica that never finishes hydrating an object records
  nothing for it, since only completions are stamped.

## Design

Compute stamps three replica-side timestamps per export and worker: `installed_at`
when the dataflow is installed, `started_at` when it is unsuspended, and
`hydrated_at` when the output frontier passes the as-of. They are added to the
existing `mz_introspection.mz_compute_hydration_times_per_worker` log, which keeps
its name, OID, and object kind, so its generated per-replica index and every
relation built on it are unaffected. A consumer doing `SELECT *` sees three new
columns.

Renaming the log was considered and not done, which is why the columns are appended
rather than reorganized. A rename would move OID 16977 to a differently shaped
relation and turn the old name into a view over the new one, and naming that
relation is the compute team's call on their own change.

A coordinator task visits one user replica per interval, in a rotation. The only
replicas it skips are those with introspection disabled, whose logs are installed
but never populated. It installs an internal subscribe on that
replica which aggregates every worker's completed rows, anti-joins them against the
history table, and writes the missing ones through the timestamped OCC
read-then-write path.

An *episode* throughout this document is one such row: one dataflow's hydration on
one replica, from installation to hydrated, recorded once.

## History Table

```text
mz_internal.mz_object_hydration_history
  export_id     text         not null
  cluster_id    text         not null
  replica_id    text         not null
  installed_at  timestamptz  not null
  started_at    timestamptz  null
  hydrated_at   timestamptz  null
  status        text         not null
```

An episode is identified by `(export_id, replica_id, installed_at)`. The
installation time is replica-stamped and stable across an environmentd restart,
where `started_at` cannot serve that role because it is null while an object waits
to run.

The id is the dataflow export's, not the catalog item's, because that is the grain of the
data we read: the log holds one row per export, and one item can own several
exports at once. Resolving to an item id would force us to merge those into one
row and invent its timestamps. The cost is that a superseded generation stops
resolving through `mz_object_global_ids` once its mapping row is retracted, and
consumers reach a catalog object through that view rather than joining `mz_objects`
directly.

That identity is deliberately not declared as a key on the relation. A key is a
promise to the optimizer, which may then elide a `DISTINCT` or assume a join
cardinality, so a single duplicate from a best-effort sampler becomes a silently
wrong answer rather than a duplicate row. The collector's anti-join is what keeps
the identity unique, and none of the comparable history tables declare a key
either. Adding one later is also expensive: it changes the relation's descriptor,
which needs a migration, which for this table means giving up the exemptions that
protect its contents.

Only terminal `hydrated` rows are written today, so every row has a finish time.
The nullable columns and the `status` column reserve a compatible shape for
episodes that are canceled or unfinished, once those become observable. Retention
keys off `hydrated_at`, so an unfinished episode needs a second age basis before it
can be recorded at all.

The table is in `mz_internal`, not `mz_catalog`, because its contents are best
effort and `status` will gain values. `mz_internal` carries no stability
commitment. Its closest sibling, `mz_internal.mz_object_arrangement_size_history`,
sits there for the same reason.

No index. An arrangement on the catalog server would hold the whole table, which
grows with objects times replicas times re-hydrations, and nothing queries this
table by key yet. A user query instead scans a table bounded by retention. Note
that an index would not help the collector anyway, since its anti-join runs on the
targeted user replica, where a catalog-server arrangement does not exist.

For the same reason the table is not a retained-metrics object. That flag pins a
30 day logical compaction window, which keeps 30 days of update history rather
than current state. Our history is in the rows, which retention retracts on its
own schedule, and nothing reads this table at an old timestamp.

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
at. That same property is what makes a trailing replica record nothing, in the limitation section below.

## Reading every worker

Collection aggregates a replica's workers for an export and records nothing until
all of them have hydrated. The reason is narrower than it looks, and it is worth
writing down because the obvious argument for reading one worker is almost right.

For an index it *is* right. A worker stamps `hydrated_at` when its reported output
frontier passes the as_of, an index's output frontier is the arrangement upper, and
that moves only as timely's dataflow-wide progress tracking allows. No worker sees
it advance while another still holds capabilities below it, so any one worker's
stamp already accounts for the slowest.

A materialized view breaks it. Its output frontier is the meet of the compute
frontier and the *sink write* frontier, and the sink write frontier is not a timely
progress quantity. The persist sink picks one active worker, `hash(sink_id) %
workers`. Only that worker's frontier tracks the shard upper. Every other worker
clears its own, so its output frontier is the compute frontier alone and it stamps
at compute completion, before the snapshot is durable. The sink buffers the snapshot
until the desired frontier passes, so that write follows the computation rather than
overlapping it, which makes the gap the whole write rather than a flush.

Reading one worker would therefore give a finish that means "durable" for the
`1/workers` of objects whose hash picks worker 0 and "computed" for the rest, with
nothing in the data to say which. `max` over a complete set of workers gives one
meaning for every object, and matches `mz_compute_hydration_times`, so the durable
history and the live hydration signal agree.

Completeness needs no worker count. The log carries a row per
`(export_id, worker_id)` from installation with a null `hydrated_at`, so
`count(*) = count(hydrated_at)` says every worker that has the dataflow has
finished. An object still hydrating is skipped rather than recorded with a finish
that precedes its write, and a later sweep picks it up. The cutoff and the anti-join
have to apply to the aggregate's output rather than its input, since as `WHERE`
clauses either one drops the not-yet-hydrated rows and makes the check trivially
true.

The cost is that an interval can span two process clocks. Each process anchors its
logging clock at its own `SystemTime`, so a duration carries whatever skew there is
between the ends. That inflates it. Nothing rejects a row for looking inconsistent,
which is deliberate: a guard comparing cross-worker stamps rejects complete episodes
permanently, because the log values never change. Skew is normally milliseconds
against durations of seconds, so inflation is the right failure to accept and
rejection is not.

`started_at` is the earliest across workers. Note that the compute log does not
leave it unset when no start was observed, it reports the installation time instead,
which keeps `installed_at <= started_at <= hydrated_at` total. So a queueing interval
of exactly zero means "no start was observed" rather than "the dataflow started
immediately", and the two are not distinguishable here. An import-free dataflow is
never suspended, so it is the common case for the former.

The aggregate is per export, not per catalog item. A materialized view being
replaced has two global ids live at once, and a replica running both dataflows logs
a row for each. We record both, as two rows with different `export_id`s, which is
what actually happened. Keying by item id instead would have collapsed them into one
row per item and forced us to pick timestamps across two separate hydrations.

## What is and is not recorded

Collection samples current state. It is not an event log, and the log it reads
retracts an object's row when the export goes away. An episode is recorded only if
its row is still live when its replica's turn comes around, so an object dropped
before then, or a replica process that restarts before then, leaves no trace.

A short-lived dataflow makes this concrete. A constant materialized view hydrates
immediately, its log row briefly carries a complete episode, and then the
controller drops the collection and the row is retracted for good. Whether a sweep
lands inside that window is a race, so such an object is recorded on some runs and
not others. Nothing wrong is recorded either way, and no test can pin the outcome,
which is why `test/testdrive/hydration-status.td` asserts nothing about it.

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
release notes that the history restarts. The user-facing documentation states the
same limit, that a schema change in a future release may clear the contents.

## Scheduling and isolation

`hydration_history_collection_interval` sets the cadence and disables collection at
zero. One sweep visits one replica, and sweeps never overlap, which bounds compute
load and keeps the collector from contending with itself in the serialized
timestamped-write path.

Fires align to interval boundaries, and each sleep is capped, so that lowering a
long interval at runtime takes effect within the cap rather than after the old
interval elapses. Tests depend on that. The cap is much coarser while collection is
disabled, because a disabled collector has nothing to do but notice that it has been
enabled, and disabled is what every environment runs by default.

Two isolation decisions are worth stating outright:

**Background mutations take no OCC write permit.** The permits are one semaphore
shared by every read-then-write in the process, not one per table. A session's wait for a permit is
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
generations do nothing, which is discussed under upgrades below. Replicas with introspection disabled are skipped, since
their log arrangements exist but are never populated, so a subscribe would read a
sealed, empty collection on every sweep forever.

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

So a replica whose clock trails environmentd by more than one introspection
interval produces a frontier that keeps sitting below the target the oracle hands
out. The mutation waits, and the sweep's own timeout ends it. The consequence is
bounded. Nothing wrong is recorded, that replica records nothing at all, and the
sweep stretches while it waits, which delays others in the rotation. It resolves
itself when the clocks converge.

Accepted for now: skew between processes is normally milliseconds while the
tolerance is a whole introspection interval, and the failure mode is waiting rather
than wrong data. Because the symptom is otherwise hard to attribute, a step that
times out logs that the replica's introspection frontier may be trailing.

## Notes on the catalog plumbing

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
