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
hydrated, at which point it holds an `Instant` based elapsed measurement taken
inside the replica process (`src/compute/src/logging/compute.rs:964-990`). That
has three consequences.

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
`ExportState::created_at` (`src/compute/src/logging/compute.rs:734`), from
`handle_create_dataflow` (`src/compute/src/compute_state.rs:682-690`). That
happens while the dataflow is still suspended by its `StartSignal` token, which
is released only when `Schedule` arrives
(`src/compute/src/compute_state.rs:722-730`). So `time_ns` spans time spent
waiting for input frontiers to advance, time spent queued behind the sequential
hydration limit (`src/compute-client/src/controller/sequential_hydration.rs`),
and the hydration work itself, with no way to separate them.

## Success Criteria

1. For every maintained compute object on every worker, compute publishes the
   wallclock instant at which its dataflow was installed, the instant hydration
   began, and the instant it completed, or NULL where the event has not happened.
2. Time spent hydrating is distinguishable from time spent waiting to start.
3. An object currently hydrating is distinguishable from one waiting to start and
   from one never installed.
4. The published instants are accurate to the event, not quantized to the
   introspection logging interval.
5. Values are stable for as long as the replica process lives, including across
   an environmentd restart and reconnection.
6. Every row satisfies `installed_at <= started_at <= hydrated_at` where the
   values are non-NULL, and no row presents a later stage without an earlier one.
7. No existing relation changes shape, semantics, or values.
   `mz_compute_hydration_times`, `mz_compute_hydration_statuses`, and the
   per-worker relation all keep returning exactly what they return today.

## Out of Scope

- **The per-replica relation.** Aggregating the per-worker rows into a
  `mz_compute_hydration_timestamps` storage-managed collection, and the
  cross-worker rollup questions that come with it, are follow-up work. See
  "Follow-up work" below.
- **Persistence, retention, and history relations.** Writing episodes into
  durable append-only relations, deduplicating them, and enforcing retention is
  adapter work.
- **Peak memory and disk.** Orthogonal. The natural source is the process-global
  memory limiter (`src/compute/src/memory_limiter.rs`), which already samples
  physical plus swap usage on `MEMORY_LIMITER_INTERVAL`. A separate introspection
  relation carrying current and running-maximum usage would address the PRD's
  sharp-spike problem without raising the orchestrator's sampling rate.
- **Prometheus metrics.** A later cloud-team requirement per the PRD.
- **Per-operator progress.** `mz_compute_operator_hydration_statuses` already
  provides per-LIR-node booleans, which are the numerator and denominator the
  PRD's progress view needs.
- **Storage hydration.** Sources report `rehydration_latency`
  (`src/catalog/src/builtin/mz_internal.rs:8423-8425`), which is a duration and
  not a pair of timestamps. Giving storage a timestamp equivalent so that
  `mz_hydration_statuses` could carry timestamps across both halves is a known
  pre-existing gap and not one this design closes.
- **Subscribes, peeks, and other transient dataflows.** The per-worker relation
  covers whatever the logging dataflows cover, and consumers filter transient IDs
  as they do today.

## Solution Proposal

### Overview

Add three wallclock timestamps to the compute hydration log, stamped by the
replica from the logging event time, and retain `time_ns` unchanged alongside
them.

| Column | Stamped when | Meaning |
| --- | --- | --- |
| `installed_at` | the `Export` event, from `CreateDataflow` | the dataflow exists on this worker, suspended, so this is also the start of queueing |
| `started_at` | the dataflow is unsuspended | hydration is actually running |
| `hydrated_at` | the output frontier passes the as-of | hydration is complete |

`hydrated_at - started_at` is hydration time as users mean it.
`started_at - installed_at` is the queueing interval. Today's `time_ns` conflates
the two, and keeping it lets every existing relation continue to report exactly
what it reports today.

`time_ns` and the timestamps have distinct jobs, which is why both are kept.
`time_ns` remains the authoritative per-worker duration, measured from a single
`Instant` inside one worker at nanosecond precision. The timestamps exist to
identify and bound episodes, which requires absolute times a duration cannot
provide. Documented that way they are two columns with two purposes rather than
two sources of truth.

Queueing is made of two distinct waits, and the replica cannot tell them apart.
A collection waits first for its input frontiers to advance, which the controller
enforces by not sending `Schedule` at all
(`src/compute-client/src/controller/instance.rs:1662-1696`), and then for a
hydration concurrency slot, which the `SequentialHydration` interceptor enforces
by withholding a `Schedule` it has already received
(`src/compute-client/src/controller/sequential_hydration.rs:128-135`). Both
appear to the replica as `Schedule` arriving late. Separating them is possible,
because the interceptor knows when it enqueued the command, but that observation
lives in environmentd and would reset on restart, which is the property this
design exists to preserve. If the distinction is wanted it belongs in a live-only
surface or a Prometheus metric.

### The replica can already stamp wallclock times

Compute log event times are Unix-epoch based.
`src/compute/src/logging/initialize.rs:54-60` captures
`start_offset = SystemTime::now() - UNIX_EPOCH` alongside an `Instant`, and
passes both to `Logger::new(self.now, self.start_offset, ...)` (`:249`). Timely
computes each event's time as `offset + now.elapsed()`, so it is a `Duration`
since the epoch advanced monotonically. The comment there states the intent: to
let logging sources be joined against tables and other real-time sources.

So the replica already knows the wallclock instant of every hydration event.
`handle_hydration` discards it in favour of an elapsed measurement.

**Stamp the event time, not the update timestamp.** The demux has the event
`Duration` in hand per event, and separately computes `ts()`
(`src/compute/src/logging/compute.rs:795-800`), which rounds up to the next
logging interval and is used as the differential timestamp of the update. The
values must come from the event time. The rounding then affects only when an
update becomes visible and whether two transitions in the same interval
consolidate, never the accuracy of a recorded instant. This is criterion 4, and
it is easy to get wrong by reaching for the value already being computed.

Two properties of this clock need documenting on the relation. It is monotone,
because it advances off an `Instant` rather than re-reading the system clock, so
it does not jump if NTP steps the wall clock. And `initialize` runs per timely
worker (`src/compute/src/logging/initialize.rs:44-60`), so the epoch anchor is
sampled per worker, and a replica with `scale > 1` spans processes on different
machines. Within a row all three timestamps share one anchor and are therefore
mutually consistent. Any comparison across workers absorbs anchor skew. That skew
is pre-existing across everything derived from compute logging and has not been
observed to be severe, but it is a real risk and this design is the first to
invite direct comparison of absolute times, so it is acknowledged rather than
designed around.

### A new hydration start event

There is no event for hydration start today. Add
`ComputeEvent::HydrationStart { export_id }`, and log it from the two places a
dataflow becomes unsuspended.

**From `handle_schedule`** (`src/compute/src/compute_state.rs:722-730`), which
drops the suspension token. Guard it in the demux on `started_at` already being
set, mirroring the existing guard on `hydration_time_ns.is_some()`
(`src/compute/src/logging/compute.rs:972-976`). That makes it idempotent under
reconciliation, where `reconcile` retains matching dataflows
(`src/compute/src/server.rs:594-606`) but still forwards `Schedule` (`:655-658`),
reaching a `suspended_collections.remove` that returns `None`.

**From `initialize_logging`** (`src/compute/src/compute_state.rs:830-845`), for
log collections. These emit an `Export` event like any other collection, so they
get an `installed_at`, but the controller marks them scheduled implicitly and
never sends `Schedule` (`CollectionState::new_log_collection`,
`src/compute-client/src/controller/instance.rs:2617`, `state.scheduled = true`).
They then hydrate immediately at an as-of of `Timestamp::MIN`. Without a start
event they would sit permanently in the illegal state
`(installed_at, NULL, hydrated_at)`, on every replica, for a dozen or so objects.
Emitting the event where they are created is simpler than filtering them, and
keeps the state machine total. Nobody should read hydration timings for log
collections, and the relation's documentation should say so.

### The ordering invariant

Criterion 6 requires `installed_at <= started_at <= hydrated_at`. That is not
free, because a dataflow can hydrate before it is scheduled.

`StartSignal` is attached only to imported sources and imported indexes
(`src/compute/src/render.rs:303,401,458,501,549`). A dataflow with no imports,
for example a default index on `SELECT 1` or a constant materialized view, has
nothing gated, so its output frontier advances past the as-of regardless of the
suspension token. The interceptor anticipates exactly this and says so
(`src/compute-client/src/controller/sequential_hydration.rs:173-178`): *"it is
possible to observe hydration even for collections for which we never sent a
`Schedule` command, if the replica decided to not suspend the dataflow after
creation"*. It then sends the `Schedule` anyway to keep protocol communication
predictable, so `handle_schedule` fires after hydration and would stamp
`started_at` later than `hydrated_at`.

Resolve it in the demux: when handling the hydration event, if `started_at` is
NULL, stamp it to the same instant as `hydrated_at`. A dataflow that hydrated
without being scheduled did not queue, so a zero queueing interval is the honest
reading, and it keeps the invariant total rather than asking every consumer to
handle a negative interval. The same handler already tolerates repeated hydration
events, so this is a small addition to logic that exists.

### Log relation shape

`ComputeLog::HydrationTime` currently describes `(export_id, worker_id, time_ns)`
(`src/compute-client/src/logging.rs:354-359`). Widen it:

```
export_id     text        not null
worker_id     uint8       not null
time_ns       uint8       nullable
installed_at  timestamptz not null
started_at    timestamptz nullable
hydrated_at   timestamptz nullable
key: (export_id, worker_id)
```

`ExportState` (`src/compute/src/logging/compute.rs:724-751`) gains the three
instants next to `created_at` and `hydration_time_ns`, which both stay so that
`time_ns` keeps its current derivation. Row updates follow the existing
retract-and-reinsert pattern in `handle_hydration`, so an object moves through
`(installed_at, NULL, NULL)`, then `(installed_at, started_at, NULL)`, then the
complete row.

Since the durable log id is keyed on the `LogVariant`
(`src/catalog/src/durable/transaction.rs:947`, `HydrationTime => 29`), widening
in place rather than adding a variant means no new id and no second log
collection producing overlapping data.

### Preserving the existing relations

The builtin log currently named `mz_compute_hydration_times_per_worker`
(`src/catalog/src/builtin/mz_introspection.rs:318-335`) is renamed to
`mz_compute_hydration_timestamps_per_worker` and exposes all six columns. A new
builtin view takes the old name and projects the old three columns:

```sql
SELECT export_id, worker_id, time_ns
FROM mz_introspection.mz_compute_hydration_timestamps_per_worker
```

Because `time_ns` is retained rather than derived from the timestamps, that
projection is exact. No value changes, no precision is lost, and no cross-worker
arithmetic is introduced. This matters more than it might appear: deriving
`time_ns` as `hydrated_at - installed_at` would have moved to microsecond
precision, since `timestamptz` caps there
(`src/repr/src/adt/timestamp.rs:60`), and deriving it after aggregation would
additionally have absorbed cross-worker install skew and anchor skew.

Everything downstream is therefore untouched. The introspection subscribe
(`src/adapter/src/coord/introspection.rs:599-613`) keeps reading the old name and
keeps its current SQL. `mz_internal.mz_compute_hydration_times` and
`mz_internal.mz_compute_hydration_statuses` are not modified at all, so they keep
their shapes, semantics, values, retained-metrics properties, indexes and shards.
`arrangement_sizes_snapshot`, which decodes that collection positionally from a
persist snapshot (`src/adapter/src/coord/message_handler.rs:1228-1253`), needs no
change. Neither does the console query that joins it by name
(`console/src/api/materialize/cluster/largestClusterReplica.ts:26`).

This is what makes the design purely additive: it adds columns and a relation,
and changes nothing that anything currently reads.

### Sequence of events

Two cases matter, and they differ in exactly the way the timestamps are meant to
expose.

**An index created on a running, already-hydrated cluster.** Everything happens
in one controller turn and one replica turn.

| Step | Where | What | Stamp |
| --- | --- | --- | --- |
| 1 | controller | `create_dataflow` downgrades input read holds to the as-of and calls `add_collection` per export (`instance.rs:1467`) | |
| 2 | controller | sends `ComputeCommand::CreateDataflow` (`instance.rs:1616`) | |
| 3 | controller | calls `maybe_schedule_collection` immediately (`:1618-1620`). Inputs are already available, so `frontiers_ready` holds (`:1686`) and `Schedule` goes out in the same turn (`:1693`) | |
| 4 | interceptor | forwards `CreateDataflow` and tracks the collection (`sequential_hydration.rs:120-127`). Withholds `Schedule`, enqueuing it (`:128-135`), then `hydrate_collections` re-emits it because the cluster is below `HYDRATION_CONCURRENCY` | |
| 5 | replica | `handle_create_dataflow` calls `CollectionLogging::new`, which logs `Export` (`compute_state.rs:682-690`, `logging/compute.rs:1320-1323`) | **`installed_at`** |
| 6 | replica | inserts the suspension token (`compute_state.rs:705-710`) and renders the dataflow, whose operators park on the `StartSignal` | |
| 7 | replica | `handle_schedule` drops the token and the operators start (`compute_state.rs:722-730`) | **`started_at`** |
| 8 | replica | the dataflow reads its inputs from the as-of forward and builds arrangements. Nothing is stamped here, this interval is the hydration | |
| 9 | replica | the output frontier passes the as-of and `set_reported_output_frontier` calls `set_hydrated` (`compute_state.rs:2059-2070`, `logging/compute.rs:1398`) | **`hydrated_at`** |
| 10 | replica | the demux writes the retract and insert pair, so the per-worker relation carries all three | |
| 11 | controller | separately, a `Frontiers` response arrives and `update_output_frontier` (`instance.rs:3317`) flips the controller's own hydration view, which is what the 0dt caught-up check and the autoscaling signal read. One round trip later, and it stamps nothing | |

Because steps 2 through 7 collapse into one turn, `installed_at` and `started_at`
are within milliseconds of each other and the queueing interval is approximately
zero. This is the case today's `time_ns` happens to measure correctly.

**A replica joining a cluster that already hosts N objects.** This is the
rehydration case the PRD is about.

1. `Instance::add_replica` replays the reduced command history into the new
   client (`instance.rs:1237-1266`) and then installs per-replica collection
   state (`:1269`, reaching `replica.add_collection` at `:416`). The replay is
   what delivers the commands, there is no separate trigger.
2. `ComputeCommandHistory::reduce` emits every `CreateDataflow` before any
   `Schedule` (`src/compute-client/src/protocol/history.rs:214-225`), so the
   replica stamps N `installed_at` values in a burst. This is the moment the
   replica learned its whole workload.
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
`NOT NULL` by construction and stamped at the inception of the episode, before
any waiting. So `(worker_id, export_id, installed_at)`, and its per-replica
equivalent once the rollup exists, identifies an episode without needing an epoch
or any controller-side bookkeeping. `started_at` is deliberately not the key: it
is NULL for the entire pre-start phase, which is precisely the state a consumer
most needs to track and re-recognise.

**What resets it.** A replica process restart recreates the logging dataflows and
yields entirely fresh timestamps for every object, which is a genuinely new
episode. Reconciliation preserves them only for dataflows it actually retains:
`reconcile` requires `compatible && uncompacted && subscribe_free &&
copy_to_free && dependencies_retained` (`src/compute/src/server.rs:594-599`), and
`dependencies_retained` cascades, so one base index failing to reconcile replaces
every dataflow downstream of it. Those objects get fresh timestamps on a replica
that has been up for a long time. That is arguably correct, since the dataflows
really were rebuilt, but it is not obvious from the outside.

**Very short episodes may not be observable at all.** The demux assigns updates a
timestamp rounded up to the logging interval, and the introspection write path
consolidates: the subscribe handler discards the subscribe timestamp and flattens
a batch into one append (`src/adapter/src/coord/introspection.rs:468-473`), and
the storage write task consolidates on its own batch interval. So an object whose
transitions all fall inside one such window presents only its final state, and an
object that dies before its rows reach persist leaves no record. In the limit this
is unavoidable, and stamping event time rather than the rounded update timestamp
is what keeps it to a visibility limit rather than an accuracy one: an episode
that is recorded is recorded accurately.

**A crash is not a compute event and is not reported here.** A replica that dies
cannot report its own death. What the relation shows is that a dropped object's
row is retracted individually while its replica's other rows persist, whereas a
replica going away removes all of its rows at once
(`drop_introspection_subscribes`, `src/adapter/src/coord/introspection.rs:338-355`).
Replica lifecycle is recorded in `mz_cluster_replica_status_history`, which the
PRD already wants joined against, so a consumer distinguishes the two by joining
rather than by anything compute adds.

**A restarting replica briefly serves stale rows.** When a replica restarts under
the same ID, the subscribe is reinstalled with `deferred_write` set and
`first_data_at` cleared, and until the new subscribe reports, "the collection
serves the previous subscribe's data, which the replica may have invalidated by
restarting" (`src/adapter/src/coord/introspection.rs:423-426`). Consumers must
gate on introspection freshness, as `mz_object_arrangement_size_history` already
does via `fresh_introspection_replicas`
(`src/adapter/src/coord/introspection.rs:519-530`).

**`REFRESH` materialized views report a refresh interval, not hydration work.**
The reported output frontier is the meet of write and compute frontier
(`src/compute/src/compute_state.rs:929-950`), and a REFRESH MV's write frontier
sits at the as-of until the first refresh lands, so hydration is not considered
complete until then. For `REFRESH EVERY '1 day'`, `hydrated_at - started_at` can
be most of a day, nearly all of it idle. The per-object stamps are still
internally consistent, so this is not a defect in the relation, but any rollup
must exclude these objects or it will never close an episode. The controller
already receives `refresh_schedule` in `add_collection`, so it can mark them.

**Read-only mode changes what the output frontier means.** In read-only mode the
write frontier is deliberately excluded from the reported output frontier
(`src/compute/src/compute_state.rs:929-950`), because a read-only dataflow cannot
push it forward. So `hydrated_at` during a 0dt read-only window reflects compute
progress only, which is the intended reading but differs from the steady-state
one.

### Why the replica and not the compute controller

The compute controller evaluates a very similar hydration predicate already, has
a real clock, and has an existing append-only introspection path it uses for
`WallclockLagHistory`
(`src/compute-client/src/controller/introspection.rs:73-74`,
`instance.rs:635-680`). Stamping there would be less code.

It is nonetheless wrong here. Controller state is rebuilt from scratch on every
environmentd restart, so every timestamp it stamped would reset on a restart that
did not disturb the replica at all, and every episode would look new.
Controller-stamped values also cannot be made idempotent: `ReplicaState::epoch`
(`instance.rs:3097`, incremented in `rehydrate_replica` at `:1367`) resets to 1
on restart, so `(replica_id, epoch)` collides across restarts. And the controller
learns of hydration one `Frontiers` round trip late, so its durations run
systematically long against the replica's own measurement.

Two qualifications, so the argument is not overstated. The controller-side and
replica-side predicates are close but not identical: the controller's is
`as_of.is_empty() || as_of < output_frontier` (`instance.rs:3256-3274`), while the
replica's omits the empty-as-of clause and additionally requires the frontier to
have been reported (`compute_state.rs:2072-2078`). The divergence is unobservable,
because an empty as-of never produces a `CreateDataflow` at all
(`instance.rs:1608-1614`). Separately, an ordinary version upgrade reaps clusterd
processes, so timestamps reset on upgrade wherever they are stamped. The benefit
of replica stamping is for same-version environmentd restarts, reconnections and
generation changes, which is still the common case, rather than for upgrades.

### Implementation touch points

- `src/compute/src/logging/compute.rs`: `ComputeEvent::HydrationStart`, the three
  `ExportState` fields, the packer, `handle_export` (`:828-851`),
  `handle_export_dropped`, `handle_hydration` including the `started_at` backfill,
  and a new `handle_hydration_start`. Also `CollectionLogging` alongside
  `set_hydrated` (`:1398`).
- `src/compute/src/compute_state.rs`: log the start event from `handle_schedule`
  (`:722-730`) and from `initialize_logging` (`:830-845`).
- `src/compute-client/src/logging.rs`: the widened `RelationDesc` (`:354-359`).
  `LogVariant::desc` (`:210+`) is the only exhaustive match a shape change
  touches, since the variant itself is unchanged.
- `src/catalog/src/builtin/mz_introspection.rs`: rename the builtin log
  (`:318-335`) and add the compat view.
- `src/catalog/src/builtin.rs`: `BUILTINS_STATIC` registration for the new view
  (compare `:1108` for logs).
- `src/pgrepr-consts/src/oid.rs`: an OID for the new view (existing entries at
  `:697-698`).
- Goldens that hardcode this relation's identity, columns, OIDs or indexes:
  `test/sqllogictest/oid.slt`, `information_schema_tables.slt`,
  `mz_catalog_server_index_accounting.slt`, `cluster.slt`,
  `catalog_server_explain.slt`, `test/cluster/mzcompose.py`, and the autogenerated
  `test/sqllogictest/autogenerated/mz_introspection.slt`.
- Docs: the `mz_introspection` system catalog reference page.

Not touched, and deliberately so: the introspection subscribe,
`mz_internal.mz_compute_hydration_times`,
`mz_internal.mz_compute_hydration_statuses`,
`src/adapter/src/coord/message_handler.rs`, and
`src/mz-debug/src/system_catalog_dumper.rs`.

New testdrive coverage worth adding:

- `installed_at` set and `started_at` NULL for an object gated by
  `HYDRATION_CONCURRENCY`.
- `started_at` NULL for an object waiting on an unavailable input.
- An import-free dataflow satisfying the ordering invariant.
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
`mz_introspection`, so it is only queryable against a targeted replica. Making
the timestamps generally useful needs the same treatment
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
- **Object-set scoping.** A replica that gains an object after hydrating reports
  a later `max(hydrated_at)`, so a naive rollup conflates an initial hydration
  episode with later incremental ones. The information needed to scope an episode,
  which objects belonged to the replica's initial set, lives in the controller,
  and the PRD states the intended rule: adding an object starts a new replica
  episode only if the replica transitions from fully hydrated to not. REFRESH
  materialized views and log collections need excluding from the same object set.
- **Freshness.** Rows from a replica that has restarted are briefly stale and
  must be gated on `fresh_introspection_replicas`.
- **Naming and compatibility.** If the aggregate relation supersedes
  `mz_compute_hydration_times`, the positional persist decode in
  `arrangement_sizes_snapshot` (`src/adapter/src/coord/message_handler.rs:1228-1253`)
  and its use of `resolve_builtin_storage_collection`, which is typed
  `&'static BuiltinSource` (`src/adapter/src/catalog.rs:972-977`), both need
  repointing. Simplest is to add the new relation alongside and leave the old one
  alone.

### Durable history

Persisting episodes into append-only relations with retention, per the PRD, is
adapter work. What compute provides for it is a stable episode key
(`installed_at`), event-accurate instants, and values that survive an
environmentd restart. What it does not provide is any record of an episode whose
rows never reached persist, or any signal that a replica crashed.

## Minimal Viable Prototype

The prototype is the compute and catalog change itself, exercised through
testdrive against a targeted replica. The validating query selects from
`mz_compute_hydration_timestamps_per_worker` on a cluster with a hydration
concurrency limit and a handful of indexes, showing objects moving from waiting,
to hydrating, to hydrated, with the queueing interval visible separately from the
hydration interval. A second run after an environmentd restart shows identical
values, which is the property the design turns on. A third check confirms
`mz_compute_hydration_times` and `mz_compute_hydration_statuses` are byte for byte
unchanged.

No SQL syntax changes and no new user-facing commands, so there is nothing to
wireframe.

## Alternatives

**Stamp in the compute controller.** Covered above. Less code, but it resets on
every environmentd restart and cannot produce a stable episode key.

**Replace `time_ns` with the timestamps rather than keeping both.** Rejected.
Deriving `time_ns` from the timestamps would lose nanosecond precision, since
`timestamptz` caps at microseconds, and deriving it after aggregation would also
absorb cross-worker install and anchor skew. Keeping it makes every existing
relation an exact projection and costs two `ExportState` fields.

**Narrow `mz_compute_hydration_statuses.hydration_time` to the interval its name
claims.** Rejected. Monitoring and analytics depend on the current values, and
the failure mode is a query that keeps working while silently returning different
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
This is a real alternative and `WallclockLagHistory` shows that append-only
compute introspection works
(`src/compute-client/src/controller/introspection.rs:73-74`). It would make short
episodes observable, which the current-state relation cannot do. Rejected for two
reasons. Current state is what consumers actually need in order to answer "is this
hydrating now", so an event log would be in addition to this relation rather than
instead of it. And the compute-side cost is unbounded demux state, because the
demux would have to retain per-object history rather than a fixed-size
`ExportState`, on a path that already runs inside every replica. Note that the
earlier rejection rationale, that a consumer needs current state to notice a
non-completing episode, was wrong: current state alone does not distinguish a drop
from a disappearance, and the distinction comes from joining replica status
history instead.

**Raise the resolution of existing metrics sampling.** Fails because sampling
cannot observe a transition, only its aftermath, so a hydration that starts and
finishes between two samples is invisible.

## Settled during review

Recorded so the reasoning is not relitigated.

- **Stamping location.** The replica, not the compute controller.
- **`time_ns` is kept.** Both existing per-worker and aggregate relations become
  or stay exact projections, so no value anywhere changes.
- **`hydration_time` is left exactly as it is,** and no `queue_time` column is
  added. Future consumers read the timestamps.
- **Clock skew.** Accepted and documented rather than designed around.
  Pre-existing across everything derived from compute logging.
- **Per-export rather than per-dataflow stamping.** Correct for today's
  single-export dataflows, which `sequential_hydration.rs:122` asserts outright.
  If multi-output dataflows land, the fix is to stamp per dataflow and fan out to
  exports in a view.
- **No fourth stamp** for the hydration-slot boundary. The interceptor knows it
  but runs in environmentd.
- **Crash detection is not compute's job.** A replica cannot report its own
  death, and replica lifecycle is already in
  `mz_cluster_replica_status_history`.
- **Sub-window episodes are unavoidable in the limit.** Mitigated by stamping
  event time so that recorded episodes are accurate, and documented as a
  visibility limit.
- **Log collections** get a start event rather than a filter.
- **The per-replica relation is follow-up work,** not part of this design.

## Open questions

None outstanding for this design. The open questions all belong to the
per-replica rollup and are enumerated under "Follow-up work".
