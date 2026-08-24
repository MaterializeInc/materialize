# Compute Hydration Timestamps

- Associated:
  [SQL-632](https://linear.app/materializeinc/issue/SQL-632/write-design-doc),
  [Improved Hydration Visibility](https://linear.app/materializeinc/project/improved-hydration-visibility-f8cb205ddd89/overview),
  [PRD](https://app.notion.com/p/materialize/Improved-hydration-visibility-3a513f48d37b806cb3a5e7f5b5a9c1e6)

Scope. This covers the compute half of the PRD, and within that only the
per-worker introspection relation. Compute's job is to observe the hydration
lifecycle accurately and publish it. Rolling the per-worker rows up into a
per-replica relation, and persisting episodes durably, are follow-up work
described at the end and not built here.

## The Problem

The PRD asks for per-object and per-replica hydration history: when hydration
started, when it finished, and whether it finished at all. Compute observes none
of that today. It observes a single duration, and only for objects that finished
hydrating.

`mz_introspection.mz_compute_hydration_times_per_worker` has columns
`(export_id, worker_id, time_ns)`. `time_ns` is NULL until the object is
hydrated, at which point `handle_hydration` in
`src/compute/src/logging/compute.rs` fills it with an `Instant` based elapsed
measurement taken inside the replica process. That has three consequences.

**No timestamps at all.** There is no start and no finish instant, only a
duration between two unnamed points. Nothing downstream can roll per-object rows
up into a replica or cluster hydration episode, because that requires absolute
times to take a min and a max over. A replica has no notion of being globally
hydrated, so that rollup is the only way to obtain one.

**In-flight hydration is opaque.** While an object hydrates, `time_ns` is NULL,
which is indistinguishable from an object waiting on its inputs, an object
queued behind `HYDRATION_CONCURRENCY`, and an object whose dataflow was never
installed. "Is this object hydrating, and since when" is unanswerable.

**The reported duration measures the wrong interval.**
`CollectionLogging::new` emits the `Export` event, which stamps
`ExportState::created_at`, and `handle_create_dataflow` in
`src/compute/src/compute_state.rs` calls it while the dataflow is still suspended
by its `StartSignal` token. That token is released only when `Schedule` arrives at
`handle_schedule`. So `time_ns` spans time spent waiting for input frontiers to
advance, time spent queued behind the sequential hydration limit in
`src/compute-client/src/controller/sequential_hydration.rs`, and the hydration
work itself, with no way to separate them.

## Success Criteria

1. For every maintained compute object on every worker, compute publishes the
   wallclock instant at which its dataflow was installed, the instant hydration
   began, and the instant it completed, or NULL where the event has not happened.
2. Time spent hydrating is distinguishable from time spent waiting to start.
3. An object currently hydrating is distinguishable from one waiting to start and
   from one never installed, from the moment its row appears rather than only
   once it completes.
4. The published instants are accurate to the event, not quantized to the
   introspection logging interval.
5. Values are stable for as long as the replica process lives, including across
   an environmentd restart and reconnection.
6. Every row satisfies `installed_at <= started_at <= hydrated_at` where the
   values are non-NULL, and no row presents a later stage without an earlier one.
7. No existing relation changes semantics or values.
   `mz_compute_hydration_times` and `mz_compute_hydration_statuses` keep
   returning exactly what they return today. The per-worker log keeps its
   existing columns and their values, and gains three, so a consumer doing
   `SELECT *` against it sees a wider row.

## Out of Scope

- **The per-replica relation.** Aggregating the per-worker rows into a
  `mz_compute_hydration_timestamps` storage-managed collection, and the
  cross-worker rollup questions that come with it, are follow-up work. See
  "Follow-up work" below.
- **Persistence, retention, and history relations.** Writing episodes into
  durable append-only relations, deduplicating them, and enforcing retention is
  adapter work.
- **Peak memory and disk.** Being built separately, sourced from the
  process-global memory limiter in `src/compute/src/memory_limiter.rs` and the
  usage collector in `src/clusterd/src/usage_metrics.rs`. It shares this design's
  notion of an episode, since the point of a peak is to attribute it to one, but
  nothing here depends on it landing first.
- **Prometheus metrics.** A later cloud-team requirement per the PRD.
- **Per-operator progress.** `mz_compute_operator_hydration_statuses` already
  provides per-LIR-node booleans, which are the numerator and denominator the
  PRD's progress view needs.
- **Storage hydration.** Sources report `rehydration_latency` through their own
  statistics path, which is a duration and not a pair of timestamps. Giving
  storage a timestamp equivalent so that `mz_hydration_statuses` could carry
  timestamps across both halves is a known pre-existing gap and not one this
  design closes.
- **Subscribes, peeks, and other transient dataflows.** The per-worker relation
  covers whatever the logging dataflows cover, and consumers filter transient IDs
  as they do today.

## Solution Proposal

Add three wallclock timestamps to the compute hydration log, stamped by the
replica from the logging event time, and retain `time_ns` unchanged alongside
them.

| Column | Stamped when | Meaning |
| --- | --- | --- |
| `installed_at` | the `Export` event, from `CreateDataflow` | the dataflow exists on this worker, suspended, so this is also the start of queueing |
| `started_at` | the dataflow is unsuspended | hydration is actually running |
| `hydrated_at` | the reported output frontier passes the as-of | the output is readable, which for a collection that writes means durable |

`hydrated_at - started_at` is hydration time as users mean it, and
`started_at - installed_at` is the queueing interval. Today's `time_ns` conflates
the two.

The lifecycle is wider than these three stages, and timestamp columns cannot carry
all of it. Two things get in the way. The stages do not share a grain: `installed`,
`started` and `hydrated` are per-worker facts, since each worker hydrates its own
fragment of the dataflow, while whether the output is durable is a property of the
sink as a whole. And a timestamp column cannot say *why* the next stage has not
happened, so a NULL cannot tell a replacement materialized view waiting for a
cutover apart from an index that will never write.

So the lifecycle proper is recorded as an append-only event log, described under
"The lifecycle event log", and `mz_compute_hydration_times_per_worker` keeps
exactly the shape and meaning above. The two are complementary: the timestamps are
the compact per-worker summary a rollup aggregates, and the log is where causes and
the write stages live.

Two choices shape everything else, each argued in its own section below. The
timestamps are stamped by the replica rather than by the compute controller, for
the reasons in "Why the replica and not the compute controller". And `time_ns` is
kept rather than replaced, for the reasons in "Preserving the existing relations".

The per-replica episode, once the rollup in "Follow-up work" exists, is
`min(started_at)` to `max(hydrated_at)` over an object set. An object hydrating
right now has a non-NULL `started_at` and a NULL `hydrated_at`. An object waiting
to start has a NULL `started_at`.

Queueing is made of two distinct waits, and the replica cannot tell them apart.
A collection waits first for its input frontiers to advance, which the controller
enforces by not sending `Schedule` at all from `maybe_schedule_collection`, and
then for a hydration concurrency slot, which the `SequentialHydration` interceptor
enforces by withholding a `Schedule` it has already received. Both appear to the
replica as `Schedule` arriving late. Separating them is possible, because the
interceptor knows when it enqueued the command, but that observation lives in
environmentd and would reset on restart, which is the property this design exists
to preserve. If the distinction is wanted it belongs in a live-only surface or a
Prometheus metric.

### The replica can already stamp wallclock times

Compute log event times are Unix-epoch based. `initialize` in
`src/compute/src/logging/initialize.rs` captures
`start_offset = SystemTime::now() - UNIX_EPOCH` alongside an `Instant` and passes
both to timely's `Logger::new`, which computes each event's time as
`offset + now.elapsed()`. So every event time is a `Duration` since the epoch,
advanced monotonically. The comment there states the intent: to let logging
sources be joined against tables and other real-time sources.

So the replica already knows the wallclock instant of every hydration event.
`handle_hydration` discards it in favour of an elapsed measurement.

**Stamp the event time, not the update timestamp.** The demux has the event
`Duration` in hand per event, and separately computes `DemuxHandler::ts`, which
rounds up to the next logging interval and is used as the differential timestamp
of the update. The values must come from the event time. The rounding then affects
only when an update becomes visible and whether two transitions in the same
interval consolidate, never the accuracy of a recorded instant. This is
criterion 4, and it is easy to get wrong by reaching for the value already being
computed.

Two properties of this clock need documenting on the relation. It is monotone,
because it advances off an `Instant` rather than re-reading the system clock, so
it does not jump if NTP steps the wall clock. And `initialize` runs per timely
worker, so the epoch anchor is sampled per worker, and a replica with `scale > 1`
spans processes on different machines. Within a row all three timestamps share one
anchor and are therefore mutually consistent. Any comparison across workers
absorbs anchor skew. That skew is pre-existing across everything derived from
compute logging and has not been observed to be severe, but it is a real risk and
this design is the first to invite direct comparison of absolute times, so it is
acknowledged rather than designed around.

### The lifecycle event log

One append-only log relation, per replica, in memory:

```
export_id    text        not null
worker_id    uint8       not null
event        text        not null
occurred_at  timestamptz not null
reason       text        nullable
details      jsonb       nullable
```

| event | grain | `reason` |
| --- | --- | --- |
| `installed` | per worker | none |
| `started` | per worker | none |
| `hydrated` | per worker | none |
| `write_blocked` | per object | `read_only` |
| `write_unblocked` | per object | none |
| `written` | per object | none |

An index emits the first three and stops, which is the index degeneracy of the
lifecycle falling out of the model rather than being special-cased. Subscribes and
`COPY TO` stop early for the same reason, and a metric sink folds its output into
the metrics registry rather than into a shard, so it has no write stages either.

**Grain.** `worker_id` is the worker that observed the event and is never NULL. The
per-object events are observed by the single worker that maintains the sink's
shared write frontier, elected as `hashed(sink_id) % peers`, so they appear once
per object rather than once per worker, and the row records which worker was
elected for free. Nothing else may read that shared frontier as a measure of
writing: `mint` clears it on every non-elected worker, where it is then the empty
antichain and would report having written everything immediately. The election has
one definition, `sink::materialized_view::frontier_owner`, called both by `mint`
and by the code that records ownership.

**`installed` is the denominator.** A consumer asking whether every worker has reached
a stage cannot count rows and compare against a worker count it does not have, and it
should not have to join the catalog to find one. The relation is append-only, so a
worker that has not hydrated has no `hydrated` row at all rather than a NULL, which is
the shape `mz_compute_hydration_times_per_worker` relies on for its
`count(*) = count(time_ns)` check. What replaces it is `installed`, which every worker
logs unconditionally when the export is created: the count of `installed` events is the
number of workers reporting on that export, so

```sql
count(*) FILTER (WHERE event = 'hydrated') = count(*) FILTER (WHERE event = 'installed')
```

is the all-workers-reported test, and the per-object stages are expected exactly once
(or not at all, for an export that never writes). Which stages have which grain is a
fixed property of the vocabulary, not of the object or the cluster, so a consumer
encodes it once rather than deriving it per query. This is a contract the relation owes
its readers, not an incidental property.

**Which frontier each stage reads.** `hydrated` reads the dataflow's own progress
frontier, the compute probe, not the reported output frontier. The output frontier
is the meet of write and compute frontier, which makes it a measure of durability
rather than of computation, and for a sink-backed collection it is not even uniform
across workers, for the reason just given. A collection with no compute probe
produces its output *by* writing it, an index into its own trace, so there the
write frontier is the progress and `hydrated` coincides with durability.

`written` reads the sink's write frontier passing the as-of, held back until
`hydrated` has been reported and until writes are permitted. Without the first
clamp the two can invert, for the reason under "Refresh schedules do not block
writing".

The second clamp is there because the frontier is the output shard's upper, which
is a property of the shard rather than of this replica. The as-of is bounded to one
step below that upper for a non-empty storage export, in
`as_of_selection::apply_downstream_storage_constraints`, so for a shard that
already holds data the predicate is true from the moment the dataflow is installed.
A replica that may not write cannot be the one that advanced it, so reporting the
stage there would attribute another writer's progress to this replica and put
`written` ahead of `write_unblocked`. What `written` promises is therefore that the
output is durable at the as-of and that this replica was permitted to write, not
that this replica performed the write. On a restarted or scaled-out replica the
output was already durable, and `written` lands with `hydrated`.

**`write_blocked` is logged on entry, not on exit.** Carrying the cause on
`write_unblocked` reads more naturally, but then the cause is only observable once
the block has ended. If it never ends, which is the state an operator is debugging,
there is no row at all. Logging entry makes "which objects are hydrated but not
writing, and why" a query over present rows rather than an inference from absence.

Entry means entry into a state where the block matters, which is after hydration.
Before it, the sink has produced nothing and read-only mode is holding nothing
back. Every collection starts read-only and is released by the controller, so
reporting a block from installation would put a `write_blocked` and a
`write_unblocked` on essentially every materialized view, both before `hydrated`,
making `write_unblocked - hydrated` negative rather than zero. Gating on hydration
means the pair appears only when something really was held back, and the intervals
in the list above are all non-negative by construction.

**`write_unblocked`, not `write_started`.** `mint` produces a batch description as
soon as the desired frontier advances past the persist frontier, and the persist
frontier is initialized to the as-of, so for a plain read-write materialized view
the first write is minted at hydration and a separate "write started" stamp would
carry no information. What does vary is when writing became *permitted*. Naming it
that way gives each interval exactly one cause: `started - installed` is queueing,
`hydrated - started` is compute, and `write_unblocked - hydrated` is blocked time.
In the common case the blocked pair is absent rather than zero: the controller
allows writes in the same turn it ships the dataflow, so a collection is normally
already permitted to write by the time it hydrates, and neither event is logged.

**`reason` is typed, `details` is not.** This follows `mz_source_statuses` and
`mz_sink_statuses`, which pair a typed status with a nullable `details jsonb`
documented by example rather than by schema. What people filter and group on stays
typed, and only the look-at-one-row detail goes in the json. What `details` carries
is the dataflow's as-of, which every stage is defined relative to: without it
`hydrated - started` cannot distinguish a genuinely fast hydration from one whose
as-of was already recent, and a replacement materialized view with a far behind
as-of is a completely different amount of work at the same duration. That does not
earn a column and is worth having in the row. Invariant tests must assert on
`event`, `reason` and `occurred_at` and never on `details`, or the first test that
pins a field removes the extensibility it exists for.

**Bounds.** At most six rows per object, times workers for the first three events,
all retracted when the object is dropped. This is in-memory introspection, so
there is no durable growth to reason about.

**Only `read_only` is attributed.** It is the one cause of a write block that
compute can observe. Two further attributions would be useful and are not
available, so they are follow-up work rather than part of this design.
Distinguishing a `started` that waited on the hydration limiter from one that
waited on its inputs needs `SequentialHydration` to report which, since both appear
to the replica as `Schedule` arriving late. And distinguishing a dataflow installed
by a fresh `CreateDataflow` from one retained across reconciliation is not
observable here at all, because a retained dataflow emits no new `installed` event.

### Refresh schedules do not block writing

`apply_refresh` rounds a `REFRESH` materialized view's frontier *up* to the next
refresh time, and it does so off its input frontier, before the dataflow has
computed anything. The sink therefore sees a desired frontier ahead of the as-of
immediately, mints a description for the pre-refresh window, and appends an empty
batch, advancing the shard's upper. A refresh schedule brings writing forward
rather than holding it back.

`test/testdrive/materialized-view-refresh-options.td` shows this from the outside:
a materialized view whose first refresh is far in the future reports
`mz_hydration_statuses.hydrated = true`, and that flag is `time_ns IS NOT NULL`,
which requires the write frontier to have passed the as-of.

Two consequences. There is no `refresh` cause for `write_blocked` to report,
because there is no such state. And the shard's upper can pass the as-of while the
dataflow is still hydrating, which is why `written` is clamped to `hydrated`.

What a refresh schedule does still distort is any rollup reading
`mz_compute_hydration_statuses.hydration_time` as hydration work, since for a
refresh materialized view that interval can include waiting on the schedule. That
is a property of the retained `time_ns` column, not of the log.

### A new hydration start event

There is no event for hydration start today. Add
`ComputeEvent::HydrationStart { export_id }`, logged from the three places a
dataflow's computation becomes unblocked. A fourth site fills the column in for
dataflows whose start was never separately observable.

**From `handle_schedule`**, which drops the suspension token. Guard it in the
demux on `started_at` already being set, mirroring the existing guard on
`hydration_time_ns`. That makes it idempotent under reconciliation, where
`reconcile` retains matching dataflows but still forwards `Schedule`, reaching a
`suspended_collections.remove` that returns `None`.

**From `handle_create_dataflow`, for dataflows with nothing to gate.**
`StartSignal` is attached only to imported sources and imported indexes, and
`Dataflow::import_ids` is exactly the imported index ids chained with the imported
source ids. So a dataflow with no imports, for example a default index on
`SELECT 1` or a constant materialized view, has nothing suspended and begins
computing at creation. `handle_create_dataflow` can test this before rendering and
stamp `started_at` alongside `installed_at`.

This matters beyond bookkeeping. Such a dataflow can reach hydration before its
`Schedule` ever arrives, and the interceptor anticipates exactly that: *"it is
possible to observe hydration even for collections for which we never sent a
`Schedule` command, if the replica decided to not suspend the dataflow after
creation"*. It then sends the `Schedule` anyway to keep protocol communication
predictable, so a `started_at` stamped only from `handle_schedule` would land
after `hydrated_at` and violate criterion 6. Stamping at creation also means the
row is correct from the moment it appears: a consumer sampling mid-hydration sees
`(installed_at, started_at, NULL)`, which is the truth, rather than
`(installed_at, NULL, NULL)`, which would report the object as queued when nothing
is queueing it. That is criterion 3.

**From `initialize_logging`, for log collections.** These emit an `Export` event
like any other collection, so they get an `installed_at`, but the controller marks
them scheduled implicitly and never sends `Schedule`, per
`CollectionState::new_log_collection`. They then hydrate immediately at an as-of of
`Timestamp::MIN`. Without a start event they would sit permanently in the illegal
state `(installed_at, NULL, hydrated_at)`, on every replica, for a dozen or so
objects. Emitting the event where they are created is simpler than filtering them
and keeps the state machine total. Nobody should read hydration timings for log
collections, and the relation's documentation should say so.

**And in `handle_hydration`, when the event arrives with `started_at` unset.**
It is tempting to treat this as a repair for something unexpected, on the
reasoning that `StartSignal` gates only imports, so a dataflow with no imports is
the only one that can run before its `Schedule`. That reasoning is wrong. Having
no imports is sufficient to start immediately, but it is not necessary in order to
hydrate early: a dataflow that *does* import can still see its output frontier
pass the as-of while suspended, when the arrangement it imports is already
hydrated. A handful of `mz_catalog_server` indexes do exactly that on every
bootstrap.

So this is a third normal path, not an exotic one, and it needs no diagnostic.
Stamp `started_at` from `installed_at`, which keeps the invariant total and reports
the queueing interval as zero. Stamping from `hydrated_at` would invert it,
charging the whole life of the dataflow to queueing and reporting zero hydration
time for a dataflow that only ever hydrated.

One consequence for consumers. A `started_at` stamped here is exactly equal to
`installed_at`, where one stamped at creation is a separate event a few
microseconds later. That difference is an artifact of how the two are stamped, not
a contract, so nothing should read a zero queueing interval as distinguishing
"never queued" from "queued immeasurably briefly".

### Log relation shape

`ComputeLog::HydrationTime` currently describes `(export_id, worker_id, time_ns)`
in `src/compute-client/src/logging.rs`. Widen it:

```
export_id     text        not null
worker_id     uint8       not null
time_ns       uint8       nullable
installed_at  timestamptz not null
started_at    timestamptz nullable
hydrated_at   timestamptz nullable
key: (export_id, worker_id)
```

`ExportState` in the demux gains the three instants next to `created_at` and
`hydration_time_ns`, which both stay so that `time_ns` keeps its current
derivation. Row updates follow the existing retract-and-reinsert pattern in
`handle_hydration`, so an object moves through `(installed_at, NULL, NULL)` or
`(installed_at, started_at, NULL)` depending on whether it is gated, and then to
the complete row.

Since the durable log id is keyed on the `LogVariant`, in
`src/catalog/src/durable/transaction.rs`, widening in place rather than adding a
variant means no new id and no second log collection producing overlapping data.

### Preserving the existing relations

The three columns are appended to the existing builtin log
`mz_compute_hydration_times_per_worker`, in
`src/catalog/src/builtin/mz_introspection.rs`, which keeps its name, its OID, and
its object kind. A consumer doing `SELECT *` sees three new columns.

Renaming it and leaving a projecting view behind the old name was considered and
rejected. It buys only the `SELECT *` width on an unstable `mz_introspection`
relation, and costs a new OID, a view, and the golden churn that comes with both.
Every consumer of this relation selects columns by name: the introspection
subscribe, `mz-debug`, and the goldens, which churn either way. Nothing reads it
positionally. The consumer that *is* positional,
`arrangement_sizes_snapshot`, decodes the aggregate
`mz_internal.mz_compute_hydration_times` from a persist snapshot, and neither this
change nor the rename would have touched that relation.

**`time_ns` is kept rather than replaced.** It is the reason the existing columns
keep their exact values: retained rather than derived, so nothing is recomputed,
no precision is lost, and no cross-worker arithmetic is introduced.
It could not be derived from the timestamps in any case. `time_ns` and
`hydrated_at` fire on the same crossing, but deriving the duration would move it to
microsecond precision, since `timestamptz` caps there, where today it is true
nanoseconds. It would also change what the interval is measured from: `time_ns`
runs off a single `Instant` taken when the export state is created, where
`hydrated_at - installed_at` is a difference of two rounded event times. Deriving
it after aggregation would additionally have absorbed cross-worker install skew and
the per-worker anchor skew described above. So
`time_ns` remains the authoritative per-worker duration, measured from a single
`Instant` inside one worker, and the timestamps carry episode identity, which
requires absolute times a duration cannot provide. Two columns with two documented
jobs rather than two sources of truth.

Everything downstream is therefore untouched. The introspection subscribe in
`src/adapter/src/coord/introspection.rs` keeps its current SQL, since it names the
columns it reads. `mz_internal.mz_compute_hydration_times` and
`mz_internal.mz_compute_hydration_statuses` are not modified at all, so they keep
their shapes, semantics, values, retained-metrics properties, indexes and shards.
`arrangement_sizes_snapshot` in `src/adapter/src/coord/message_handler.rs` needs
no change, and neither does the console query that joins by name.

So the change adds columns and changes no value that anything currently reads. The
one thing it is not is invisible: the per-worker log is wider than it was.

### Sequence of events

Two cases matter, and they differ in exactly the way the timestamps are meant to
expose.

**An index created on a running, already-hydrated cluster.** Everything happens
in one controller turn and one replica turn.

| Step | Where | What | Stamp |
| --- | --- | --- | --- |
| 1 | controller | `create_dataflow` downgrades input read holds to the as-of and calls `add_collection` per export | |
| 2 | controller | sends `ComputeCommand::CreateDataflow` | |
| 3 | controller | calls `maybe_schedule_collection` immediately. Inputs are already available, so the frontier check passes and `Schedule` goes out in the same turn | |
| 4 | interceptor | forwards `CreateDataflow` and tracks the collection. Withholds `Schedule`, enqueuing it, then `hydrate_collections` re-emits it because the cluster is below `HYDRATION_CONCURRENCY` | |
| 5 | replica | `handle_create_dataflow` calls `CollectionLogging::new`, which logs `Export` | **`installed_at`**, and `started_at` too if the dataflow has no imports |
| 6 | replica | inserts the suspension token and renders the dataflow, whose operators park on the `StartSignal` | |
| 7 | replica | `handle_schedule` drops the token and the operators start | **`started_at`** |
| 8 | replica | the dataflow reads its inputs from the as-of forward and builds arrangements. Nothing is stamped here, this interval is the hydration | |
| 9 | replica | the reported output frontier passes the as-of and `set_reported_output_frontier` calls `set_hydrated`. Separately, `observe_hydration` sees the dataflow's own progress frontier pass the as-of and logs the `hydrated` stage | **`hydrated_at`**, and the `hydrated` event |
| 10 | replica | the demux writes the retract and insert pair, so the per-worker relation carries all three | |
| 11 | controller | separately, a `Frontiers` response arrives and `update_output_frontier` flips the controller's own hydration view, which is what the 0dt caught-up check and the autoscaling signal read. One round trip later, and it stamps nothing | |

Because steps 2 through 7 collapse into one turn, `installed_at` and `started_at`
are within milliseconds of each other and the queueing interval is approximately
zero. This is the case today's `time_ns` happens to measure correctly.

**A replica joining a cluster that already hosts N objects.** This is the
rehydration case the PRD is about.

1. `Instance::add_replica` replays the reduced command history into the new client
   and then installs per-replica collection state. The replay is what delivers the
   commands, there is no separate trigger.
2. `ComputeCommandHistory::reduce` emits every `CreateDataflow` before any
   `Schedule`, so the replica stamps N `installed_at` values in a burst. This is
   the moment the replica learned its whole workload.
3. Every `Schedule` is then withheld by the interceptor and released
   `HYDRATION_CONCURRENCY` at a time.
4. Objects therefore acquire `started_at` in waves. An object at the back of the
   queue has `installed_at` set and `started_at` NULL, and is correctly reported
   as not yet started rather than as hydrating slowly.
5. Each completion releases the next queued `Schedule`, so the queue drains at
   the rate dataflows hydrate.

For an object at the back of that queue, today's `time_ns` is mostly queueing
time. Separating the two intervals is the point of stamping `installed_at`.

### What the timestamps promise, and what they do not

This is the contract handed to whatever consumes the relation.

**Episode identity.** `installed_at` is generated by the replica and is stable
across an environmentd restart, a reconnection, and reconciliation, because the
logging dataflows and the demux state holding it are not recreated. It is
`NOT NULL` by construction and stamped at the inception of the episode, before any
waiting. So `(worker_id, export_id, installed_at)`, and its per-replica equivalent
once the rollup exists, identifies an episode without needing an epoch or any
controller-side bookkeeping. `started_at` is deliberately not the key: it is NULL
for the entire pre-start phase of a gated dataflow, which is precisely the state a
consumer most needs to track and re-recognise.

**What resets it.** A replica process restart recreates the logging dataflows and
yields entirely fresh timestamps for every object, which is a genuinely new
episode. Reconciliation preserves them only for dataflows it actually retains:
`reconcile` requires that a candidate be compatible, uncompacted, free of
subscribes and copy-tos, and have its dependencies retained, and that last
condition cascades, so one base index failing to reconcile replaces every dataflow
downstream of it. Those objects get fresh timestamps on a replica that has been up
for a long time. That is arguably correct, since the dataflows really were
rebuilt, but it is not obvious from the outside.

**Very short episodes may not be observable at all.** The demux assigns updates a
timestamp rounded up to the logging interval, and the introspection write path
consolidates: the subscribe handler discards the subscribe timestamp and flattens
a batch into one append, and the storage write task consolidates on its own batch
interval. So an object whose transitions all fall inside one such window presents
only its final state, and an object that dies before its rows reach persist leaves
no record. In the limit this is unavoidable, and stamping event time rather than
the rounded update timestamp is what keeps it to a visibility limit rather than an
accuracy one: an episode that is recorded is recorded accurately.

**A crash is not a compute event and is not reported here.** A replica that dies
cannot report its own death. What the relation shows is that a dropped object's
row is retracted individually while its replica's other rows persist, whereas a
replica going away removes all of its rows at once via
`drop_introspection_subscribes`. Replica lifecycle is recorded in
`mz_cluster_replica_status_history`, which the PRD already wants joined against,
so a consumer distinguishes the two by joining rather than by anything compute
adds.

**A restarting replica briefly serves stale rows.** When a replica restarts under
the same ID, the subscribe is reinstalled with `deferred_write` set and
`first_data_at` cleared, and until the new subscribe reports, the collection
serves the previous subscribe's data, which the replica may have invalidated by
restarting. Consumers must gate on introspection freshness, as
`mz_object_arrangement_size_history` already does via
`fresh_introspection_replicas`.

**`REFRESH` materialized views hydrate on their computation.** The compute probe
is attached before the `apply_refresh` operator, deliberately, with the comment in
`src/compute/src/sink/materialized_view.rs` explaining that rounding frontiers up
"makes it impossible to accurately track the progress of the computation". So the
log's `hydrated` stage reads the pre-rounding frontier. `hydrated_at` agrees, even
though it reads the meet: a refresh schedule pushes the write frontier ahead of the
as-of, so the meet is bounded by the compute frontier and crosses when the
computation does. Both report when the computation caught up rather than anything
derived from the schedule. What the schedule does affect is writing, and it
advances it rather than delaying it. See "Refresh schedules do not block writing".

### Why the replica and not the compute controller

The compute controller evaluates a very similar hydration predicate already in
`ReplicaCollectionState::hydrated`, has a real clock, and has an existing
append-only introspection path it uses for `WallclockLagHistory`. Stamping there
would be less code.

It is nonetheless wrong here. Controller state is rebuilt from scratch on every
environmentd restart, so every timestamp it stamped would reset on a restart that
did not disturb the replica at all, and every episode would look new.
Controller-stamped values also cannot be made idempotent: `ReplicaState::epoch`
resets to 1 on restart, so `(replica_id, epoch)` collides across restarts. And the
controller learns of hydration one `Frontiers` round trip late, so its durations
run systematically long against the replica's own measurement.

Two qualifications, so the argument is not overstated. The controller-side and
replica-side predicates are close but not identical: the controller's is
`as_of.is_empty() || as_of < output_frontier`, while `CollectionState::hydrated` on
the replica omits the empty-as-of clause and additionally requires the frontier to
have been reported. The divergence is unobservable, because an empty as-of never
produces a `CreateDataflow` at all. Separately, an ordinary version upgrade reaps
clusterd processes, so timestamps reset on upgrade wherever they are stamped. The
benefit of replica stamping is for same-version environmentd restarts,
reconnections and generation changes, which is still the common case, rather than
for upgrades.

### Implementation touch points

The timestamps:

- `src/compute/src/logging/compute.rs`: `ComputeEvent::HydrationStart`, the three
  `ExportState` fields, the packer, `handle_export`, `handle_export_dropped`,
  `handle_hydration` including the `started_at` backfill, and a new
  `handle_hydration_start`. Also a `CollectionLogging` method alongside
  `set_hydrated`.
- `src/compute/src/compute_state.rs`: log the start event from `handle_schedule`,
  from `handle_create_dataflow` when `import_ids` is empty, and from
  `initialize_logging`.
- `src/compute-client/src/logging.rs`: the widened `RelationDesc`.
  `LogVariant::desc` is the only exhaustive match a shape change touches, since
  the variant itself is unchanged.
- `src/catalog/src/builtin/mz_introspection.rs`: the appended columns on the
  existing builtin log. No rename, so no new OID and no `BUILTINS_STATIC` entry.

The lifecycle log:

- `src/compute-client/src/logging.rs`: a `ComputeLog::LifecycleEvent` variant and
  its `RelationDesc`, unkeyed, so `index_by` arranges by the whole row. Declaring
  `(export_id, worker_id, event)` as a key would be true today but is a uniqueness
  claim the optimizer would act on, and a false key is a correctness hazard rather
  than a missed optimization.
- `src/catalog/src/durable/transaction.rs`: a new log id. Existing ids must not be
  renumbered. Doing so panics on restart with a negative capability on
  `IntrospectionSourceIndex`.
- `src/catalog/src/builtin/mz_introspection.rs` and `src/catalog/src/builtin.rs`: a
  `BuiltinLog` with a fresh OID and an ontology entry, plus its `BUILTINS_STATIC`
  registration.
- `src/compute/src/logging/compute.rs`: the `Lifecycle` event and the
  `LifecycleStage` vocabulary, an as-of field on `Export`, a demux output and
  packer including the `jsonb` column, and the emitted rows kept on `ExportState`
  so that they can be retracted verbatim when the export is dropped.
- `src/compute/src/compute_state.rs`: the stage bookkeeping on `CollectionState`
  and the observation of both frontiers in `report_frontiers`.
- `src/compute/src/sink/materialized_view.rs` and `materialized_view_v2.rs`: the
  shared `frontier_owner` election, and recording on the collection whether this
  worker owns the sink frontier.

- `src/adapter/src/catalog/open/builtin_schema_migration.rs`: `Replacement` steps for
  `mz_catalog.mz_indexes` and `mz_catalog.mz_sources` at the workspace's current dev
  version. Adding a builtin log moves two generated materialized views. `make_mz_indexes`
  inlines one `VALUES` row per log, naming the log and its `index_by` columns, and
  `make_mz_sources` inlines one per log alongside the builtin sources. Either fingerprint
  moving without a step reaches `update_fingerprints` with a mismatch for a builtin that is
  neither migrated nor ephemeral, which panics and blocks catalog open on upgrade.

Goldens that hardcode a log relation's identity, columns, OIDs or indexes:
`test/sqllogictest/oid.slt`, `information_schema_tables.slt`,
`mz_catalog_server_index_accounting.slt`, `cluster.slt`,
`cockroach/srfs.slt`, the autogenerated
`test/sqllogictest/autogenerated/mz_introspection.slt`,
`test/testdrive/indexes.td`, `test/testdrive/catalog.td`, and
`test/workload-replay/system_catalog_identifiers.txt` and `objects.txt`. Docs: the
`mz_introspection` system catalog reference page.

`catalog_server_explain.slt` and `test/cluster/mzcompose.py` need no change. The
former's query filters `o.id NOT LIKE 'si%'`, which excludes per-replica
introspection log indexes, and the latter queries named relations.

Not touched, and deliberately so: the introspection subscribe,
`mz_internal.mz_compute_hydration_times`,
`mz_internal.mz_compute_hydration_statuses`,
`src/adapter/src/coord/message_handler.rs`, and
`src/mz-debug/src/system_catalog_dumper.rs`.

New testdrive coverage worth adding:

- `installed_at` set and `started_at` NULL for an object gated by
  `HYDRATION_CONCURRENCY`.
- `started_at` NULL for an object waiting on an unavailable input.
- An import-free dataflow carrying `started_at` from creation, equal to its
  `installed_at`, and satisfying the ordering invariant.
- Log collections having all three timestamps set.
- All timestamps surviving an environmentd restart unchanged.
- A replica restart yielding entirely fresh values.

The most important tests are compatibility ones: that
`mz_compute_hydration_times_per_worker`, `mz_compute_hydration_times` and
`mz_compute_hydration_statuses` return identical values before and after. The
existing assertions in `test/testdrive/hydration-status.td` and the blue-green
tests are the contract and should be left alone rather than adjusted to fit.

## Follow-up work

### `mz_compute_hydration_timestamps`, the per-replica relation

The per-worker relation is per replica and per worker, and lives in
`mz_introspection`, so it is only queryable against a targeted replica. Making the
timestamps generally useful needs the same treatment
`mz_compute_hydration_times` has: an introspection `SUBSCRIBE` rolling the
per-worker rows up per object and writing them to a storage-managed collection in
`mz_internal`. That is deferred, and it is where the genuinely open questions
live.

The rollup has to answer questions the per-worker relation does not raise:

- **Which aggregate for which column.** `hydrated_at` presumably keeps today's
  all-or-nothing rule, `max` gated on every worker having reported, since an
  object is readable only once all workers have advanced. `installed_at` and
  `started_at` are less obvious. An ungated `min` avoids transient NULLs but is
  non-monotone, because a min over a growing set of reporting workers can move
  earlier as slow workers report, which would make an episode key appear to
  change. Gating all three the same way is the conservative choice and needs
  checking against how the existing gate behaves in practice.
- **Internally contradictory rows.** Mixing gated and ungated aggregates can
  publish a row whose `hydrated_at` is set while `started_at` is NULL, or whose
  timestamps cross. Whatever aggregation is chosen has to preserve criterion 6 at
  the rolled-up level too.
- **Cross-worker clock anchors.** Aggregating min and max across workers crosses
  per-worker epoch anchors, and for multi-process replicas crosses machines. Any
  derived interval absorbs that skew, which is one more reason to keep `time_ns`
  as the authoritative duration rather than recomputing it from aggregated
  timestamps.
- **Object-set scoping.** A replica that gains an object after hydrating reports a
  later `max(hydrated_at)`, so a naive rollup conflates an initial hydration
  episode with later incremental ones. The information needed to scope an episode,
  which objects belonged to the replica's initial set, lives in the controller,
  and the PRD states the intended rule: adding an object starts a new replica
  episode only if the replica transitions from fully hydrated to not. REFRESH
  materialized views and log collections need excluding from the same object set.
- **Freshness.** Rows from a replica that has restarted are briefly stale and must
  be gated on `fresh_introspection_replicas`.
- **Naming and compatibility.** If the aggregate relation supersedes
  `mz_compute_hydration_times`, the positional persist decode in
  `arrangement_sizes_snapshot` and its use of
  `resolve_builtin_storage_collection`, which is typed `&'static BuiltinSource`,
  both need repointing. Simplest is to add the new relation alongside and leave
  the old one alone.

### Durable history

Persisting episodes into append-only relations with retention, per the PRD, is
adapter work. What compute provides for it is a stable episode key
(`installed_at`), event-accurate instants, and values that survive an environmentd
restart. What it does not provide is any record of an episode whose rows never
reached persist, or any signal that a replica crashed.

## Minimal Viable Prototype

The prototype is the compute and catalog change itself, exercised through
testdrive against a targeted replica. The validating query selects from
`mz_compute_hydration_times_per_worker` on a cluster with a hydration
concurrency limit and a handful of indexes, showing objects moving from waiting,
to hydrating, to hydrated, with the queueing interval visible separately from the
hydration interval. A second run after an environmentd restart shows identical
values, which is the property the design turns on. A third check confirms
`mz_compute_hydration_times` and `mz_compute_hydration_statuses` are byte for byte
unchanged.

No SQL syntax changes and no new user-facing commands, so there is nothing to
wireframe.

## Alternatives

**Stamp in the compute controller.** Rejected for the reasons in "Why the replica
and not the compute controller".

**Replace `time_ns` with the timestamps rather than keeping both.** Rejected for
the reasons in "Preserving the existing relations".

**Backfill `started_at` at the hydration event only,** instead of also stamping
it at creation for dataflows with no imports. Rejected because the row would be
wrong in the interim, reporting an object as queued while it hydrates, and because
it defers a fact the replica already knows at creation. The backfill is kept as
well, since it covers a case creation-time stamping cannot: see "A new hydration
start event".

**Narrow `mz_compute_hydration_statuses.hydration_time` to the interval its name
claims.** Rejected. Monitoring and analytics depend on the current values, and the
failure mode is a query that keeps working while silently returning different
numbers. A column whose name overstates what it measures is the cheaper problem.
For the same reason no `queue_time` column is added to it: every interval is
derivable from the new relation, and growing the old surface would attract new
consumers to it.

**Add a variant instead of widening `ComputeLog::HydrationTime`.** Rejected. It
would need a new durable log id and would leave two log collections producing
overlapping data, which is the two-sources-of-truth problem the design otherwise
avoids.

**Emit per-transition events into an append-only relation** rather than
maintaining current state, in the shape of `mz_cluster_replica_status_history`.
This is a real alternative, and `WallclockLagHistory` shows that append-only
compute introspection works. It would make short episodes observable, which the
current-state relation cannot do. Rejected for two reasons. Current state is what
consumers actually need in order to answer "is this hydrating now", so an event
log would be in addition to this relation rather than instead of it. And the
compute-side cost is unbounded demux state, because the demux would have to retain
per-object history rather than a fixed-size `ExportState`, on a path that runs
inside every replica.

**Raise the resolution of existing metrics sampling.** Fails because sampling
cannot observe a transition, only its aftermath, so a hydration that starts and
finishes between two samples is invisible.

## Settled during review

Recorded so the reasoning is not relitigated. Each item is argued in the section
named.

- **Stamping location.** The replica, not the compute controller. See "Why the
  replica and not the compute controller".
- **`time_ns` is kept.** See "Preserving the existing relations".
- **`started_at` for dataflows with no imports is stamped at creation,** with the
  backfill at hydration kept for the cases creation-time stamping cannot see. See
  "A new hydration start event".
- **The per-worker log is widened in place,** keeping its name and OID, rather than
  renamed behind a projecting view. See "Preserving the existing relations".
- **`hydration_time` is left exactly as it is,** and no `queue_time` column is
  added. Future consumers read the timestamps. See "Alternatives".
- **Clock skew.** Accepted and documented rather than designed around.
  Pre-existing across everything derived from compute logging.
- **Per-export rather than per-dataflow stamping.** Correct for today's
  single-export dataflows, which the interceptor asserts outright. If multi-output
  dataflows land, the fix is to stamp per dataflow and fan out to exports in a
  view.
- **No fourth stamp** for the hydration-slot boundary. The interceptor knows it
  but runs in environmentd.
- **Crash detection is not compute's job.** A replica cannot report its own death,
  and replica lifecycle is already in `mz_cluster_replica_status_history`.
- **Sub-window episodes are unavoidable in the limit.** Mitigated by stamping
  event time so that recorded episodes are accurate, and documented as a
  visibility limit.
- **Log collections** get a start event rather than a filter.
- **The per-replica relation is follow-up work,** not part of this design.
- **The lifecycle is an event log, not more timestamp columns.** The stages do not
  share a grain and a timestamp cannot carry a cause. See "The lifecycle event
  log".
- **`mz_compute_hydration_times_per_worker` keeps its meaning.** `hydrated_at`
  reads the output frontier, as it always has, so nothing built on it changes
  value. The log carries the dataflow reading under `hydrated`.
- **Refresh schedules advance writing rather than blocking it,** established
  against the refresh tests. So `write_blocked` has no `refresh` cause, and
  `written` is clamped to `hydrated` to keep the stages ordered. See "Refresh
  schedules do not block writing".
- **`worker_id` is not nullable.** A NULL would make the per-object grain visible
  in the row, at the cost of introducing the only NULL `worker_id` in the logging
  framework. The grain is documented instead, and the column records which worker
  was elected.

## Open questions

None outstanding for this design. Two attributions the `reason` vocabulary would
benefit from are not observable today and are enumerated under "The lifecycle event
log". The rest of the open questions belong to the per-replica rollup and are
enumerated under "Follow-up work".
