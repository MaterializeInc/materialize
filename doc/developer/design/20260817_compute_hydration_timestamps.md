# Compute Hydration Timestamps

- Associated:
  [SQL-632](https://linear.app/materializeinc/issue/SQL-632/write-design-doc),
  [Improved Hydration Visibility](https://linear.app/materializeinc/project/improved-hydration-visibility-f8cb205ddd89/overview),
  [PRD](https://app.notion.com/p/materialize/Improved-hydration-visibility-3a513f48d37b806cb3a5e7f5b5a9c1e6)

This design covers the compute half of the PRD only. Compute's job is to surface
the hydration signal through introspection. Persisting it into durable history
relations is adapter work and is out of scope here.

## The Problem

The PRD asks for per-object and per-replica hydration history: when hydration
started, when it finished, and whether it finished at all. Compute reports none
of that today. It reports a single duration, and only for objects that finished
hydrating.

`mz_internal.mz_compute_hydration_times` has columns
`(replica_id, object_id, time_ns)`. `time_ns` is NULL until the object is
hydrated, at which point it holds an `Instant` based elapsed measurement taken
inside the replica process
(`src/compute/src/logging/compute.rs:964-990`). That has four consequences.

**No timestamps at all.** There is no `started_at` and no `finished_at`, so
nothing can roll per-object rows up into a replica or cluster hydration episode.
A replica has no notion of being globally hydrated, so that rollup is the only
way to obtain one, and it needs absolute times to take a min and a max over.

**In-flight hydration is opaque.** While an object hydrates, `time_ns` is NULL,
which is indistinguishable from an object waiting on its inputs, an object
queued behind `HYDRATION_CONCURRENCY`, and an object whose dataflow was never
installed. "Is this replica hydrating, and since when" is unanswerable.

**Failed hydration leaves no trace.** The collection is differential and
reflects current state only. If a replica is OOM killed mid-hydration, its rows
vanish. There is nothing for a layer above compute to write down, so the PRD's
headline case, telling a user whether an incident was a hydration OOM, cannot be
answered at any layer.

**The reported duration measures the wrong interval.**
`CollectionLogging::new` emits the `Export` event, which stamps
`ExportState::created_at`, from `handle_create_dataflow`
(`src/compute/src/compute_state.rs:682-690`). That happens while the dataflow is
still suspended by its `StartSignal` token, which is only released when
`Schedule` arrives (`src/compute/src/compute_state.rs:722-730`). So `time_ns`
includes time the dataflow spent waiting for input frontiers to advance and time
it spent queued behind the sequential hydration limit
(`src/compute-client/src/controller/sequential_hydration.rs`). On a cluster with
a hydration concurrency limit, that waiting can dominate the measurement.

## Success Criteria

1. For every maintained compute object on every replica, compute publishes the
   wallclock time at which hydration started and the wallclock time at which it
   completed, or NULL where the corresponding event has not happened.
2. Grouping those columns by replica or cluster and taking `min` and `max`
   yields a well defined hydration episode for a given set of objects. Choosing
   that set is the consumer's job, see "Scoping an episode to an object set".
3. An object that is currently hydrating is distinguishable from one waiting on
   its inputs, one queued behind the hydration concurrency limit, and one whose
   dataflow was never installed.
4. Time spent hydrating is distinguishable from time spent waiting to start.
5. Values are stable across an environmentd restart, including a 0dt cutover,
   for as long as the replica process lives.
6. An episode that never completes is observable while it is in flight. A
   replica crash resets all of its introspection, so there is no
   after-the-fact recovery path and a consumer can only record such an episode
   if it observed the in-flight state first.
7. Each row carries a key that identifies its hydration episode, so a consumer
   reading the relation repeatedly can tell a re-observation of a known episode
   from a new one.
8. `mz_internal.mz_compute_hydration_statuses` keeps its current column shape.

## Out of Scope

- **Persistence, retention, and history relations.** Compute surfaces the
  signal. Writing it into durable append-only relations, deduplicating episodes
  across repeated observations, and enforcing retention is adapter work.
- **Peak memory and disk.** Orthogonal to timestamps. The natural source is the
  process-global memory limiter (`src/compute/src/memory_limiter.rs`), which
  already samples physical plus swap usage on `MEMORY_LIMITER_INTERVAL`. A
  separate introspection relation carrying current and running-maximum usage
  would solve the PRD's sharp-spike problem without raising the orchestrator's
  sampling rate. Deliberately not bundled into this design.
- **Prometheus metrics.** The PRD lists these as a later cloud-team
  requirement.
- **Per-operator progress.** `mz_compute_operator_hydration_statuses` already
  provides per-LIR-node booleans, which are the numerator and denominator the
  PRD's progress view needs. No compute change required.
- **Storage hydration.** Sources report `rehydration_latency` through their own
  statistics path, and `mz_hydration_statuses` unions the two halves. This
  design does not touch the storage half, but it keeps the compute half shaped
  so that union still works.
- **Subscribes, peeks, and other transient dataflows.** Excluded today by the
  `export_id NOT LIKE 't%'` filter in the introspection subscribe, and staying
  excluded.

## Solution Proposal

### Overview

Replace the single duration in the compute hydration log with three wallclock
timestamps, all stamped by the replica.

| Column | Stamped when | Meaning |
| --- | --- | --- |
| `installed_at` | `CreateDataflow` is applied | the dataflow exists on the replica, suspended, so this is also the start of queueing |
| `started_at` | `Schedule` unsuspends the dataflow | hydration is actually running |
| `hydrated_at` | the output frontier passes the as-of | hydration is complete |

`hydrated_at - started_at` is hydration time as users mean it.
`started_at - installed_at` is the queueing interval, which answers whether a
slow cluster is slow at hydrating or merely gated on upstream frontiers or the
hydration concurrency limit. Today's `time_ns` conflates the two.

Storing the start of queueing as its own timestamp rather than only reporting a
corrected duration is deliberate. It keeps both intervals derivable, so
narrowing `hydration_time` to the interval users mean does not destroy the
number they see today. From the replica's vantage point the two waits that make
up queueing, waiting for input frontiers to advance and waiting for a hydration
concurrency slot, are indistinguishable: both simply delay the arrival of
`Schedule`. Splitting them would require the controller to report when it
considered the collection ready, which is not worth a protocol change here.

Everything else follows from those three columns. The per-replica episode is
`min(started_at)` to `max(hydrated_at)` over the objects on the replica. An
object hydrating right now has a non-NULL `started_at` and a NULL
`hydrated_at`. An object waiting to start has a NULL `started_at`. An episode
that never completes is one whose rows disappear while `hydrated_at` is still
NULL, which a consumer can only notice because the in-flight row was visible
first.

### The replica can already stamp wallclock times

Compute log event times are Unix-epoch based.
`src/compute/src/logging/initialize.rs:54-60` captures
`start_offset = SystemTime::now() - UNIX_EPOCH` when the logging dataflows are
initialized, and passes it to `Logger::new(self.now, self.start_offset, ...)`
(`:249`), so every event's time is a `Duration` since the epoch, advanced
monotonically off an `Instant`. The comment there states the intent: to let
logging sources be joined against tables and other real-time sources.

So the replica already knows the wallclock instant of every hydration event.
`handle_hydration` discards it in favour of an elapsed measurement. Surfacing
timestamps is mostly a matter of packing the event time that is already in hand.

Two properties of this clock are worth recording. It is monotone, because it
advances off an `Instant` rather than re-reading the system clock, so it does
not jump if NTP steps the wall clock. And its epoch anchor is sampled once per
replica process, so two replicas of the same cluster can disagree by whatever
their clock skew is. That is harmless for durations, which are computed within
one process, and it introduces skew into absolute cross-replica comparisons,
including the cluster-level rollup that takes a min and a max across replicas.

We accept that skew rather than design around it. It is pre-existing, since
every relation derived from compute logging already carries it, and it has not
been observed to be severe in practice. It is nonetheless a real risk, because
this design is the first to invite direct comparison of absolute times from
different replicas, so it is acknowledged here and must be documented on the
relation rather than left implicit.

### A new hydration start event

There is no event for hydration start today. The hook is `handle_schedule`
(`src/compute/src/compute_state.rs:722-730`), which drops the suspension token
and is therefore the exact moment computation begins.

Add a `ComputeEvent::HydrationStart { export_id }`, logged from
`handle_schedule`, and guard it in the demux on the export's `started_at`
already being set. The guard mirrors the existing one on
`hydration_time_ns.is_some()` (`src/compute/src/logging/compute.rs:972-976`) and
makes the event idempotent under reconciliation: `reconcile`
(`src/compute/src/server.rs:496`) retains matching dataflows, and the re-sent
`Schedule` reaches a `suspended_collections.remove` that returns `None`.

A dataflow may export several collections sharing one suspension token, so
computation actually starts only once every export has been scheduled. We log
per export anyway. Today Materialize only produces single-export dataflows, an
assumption `sequential_hydration.rs` already depends on, so the two are
equivalent in practice.

If multi-output dataflows ever land, the accurate shape is to stamp per dataflow,
keyed by the `dataflow_index` the demux already tracks on `ExportState`, and
recover the per-export relation as a view that fans the dataflow's timestamps out
across its exports. That bridges the gap cleanly. It is not worth building now:
the gap is unobservable with single-export dataflows, and there is no scheduled
work on multi-output dataflows to build against. Recorded here so the fix is
known rather than rediscovered.

### Log relation shape

`ComputeLog::HydrationTime` currently describes
`(export_id, worker_id, time_ns)`
(`src/compute-client/src/logging.rs:354-359`). Every column of that relation is
changing meaning, so rather than widening it in place we introduce
`ComputeLog::HydrationTimestamps`:

```
export_id     text        not null
worker_id     uint8       not null
installed_at  timestamptz not null
started_at    timestamptz nullable
hydrated_at   timestamptz nullable
key: (export_id, worker_id)
```

`time_ns` is dropped rather than kept alongside. Keeping it would leave two
sources of truth that disagree, because it measures from `installed_at` while
the timestamps measure from `started_at`.

Retracting and re-inserting the row on each transition follows the existing
pattern in `handle_hydration`, so an object moves through three states,
`(installed_at, NULL, NULL)`, then `(installed_at, started_at, NULL)`, then
`(installed_at, started_at, hydrated_at)`.

The `ExportState` in the demux (`src/compute/src/logging/compute.rs:735-748`)
holds the three values in place of `created_at` and `hydration_time_ns`.

### Cross-worker rollup

The introspection subscribe in
`src/adapter/src/coord/introspection.rs:599-613` becomes:

```sql
SUBSCRIBE (
    SELECT
        export_id,
        min(installed_at) AS installed_at,
        min(started_at) AS started_at,
        CASE count(*) = count(hydrated_at)
            WHEN true THEN max(hydrated_at)
            ELSE NULL
        END AS hydrated_at
    FROM mz_introspection.mz_compute_hydration_timestamps_per_worker
    WHERE export_id NOT LIKE 't%'
    GROUP BY export_id
    OPTIONS (AGGREGATE INPUT GROUP SIZE = 1)
)
```

`hydrated_at` keeps today's all-or-nothing rule: an object is hydrated only once
every worker reports, and the reported time is the last worker to get there.

`installed_at` and `started_at` use an ungated `min`, meaning "the first worker
reached this point". For `started_at` that is deliberately the definition of
"hydration has started on this replica", and it avoids a transient NULL while
some workers have reported and others have not. The skew is small by
construction: the sequential hydration interceptor sits ahead of the
`PartitionedState` client specifically so that all workers observe `Schedule` in
the same order (`sequential_hydration.rs:28-39`).

### The resulting catalog surface

`mz_internal.mz_compute_hydration_times` is replaced by
`mz_internal.mz_compute_hydration_timestamps`:

```
replica_id    text        not null
object_id     text        not null
installed_at  timestamptz not null
started_at    timestamptz nullable
hydrated_at   timestamptz nullable
```

It stays a differential storage-managed collection
(`CollectionManagerKind::Differential`,
`src/storage-controller/src/lib.rs:3841`) fed by the per-replica introspection
subscribe, and keeps its `replica_id` index and its
`is_retained_metrics_object` setting.

`mz_internal.mz_compute_hydration_times` is not deleted. It becomes a view over
the new relation, preserving its exact current shape and semantics:

```sql
SELECT
    replica_id,
    object_id,
    (extract(epoch FROM hydrated_at - installed_at) * 1000000000)::uint8 AS time_ns
FROM mz_internal.mz_compute_hydration_timestamps
```

Note that the compat view reproduces the *old* measurement, from install to
hydrated, rather than the corrected one. That is the point: it is a compatibility
shim, so existing consumers see no change at all. This is what makes the rename
cheap. `src/mz-debug/src/system_catalog_dumper.rs`, the freshness check in
`src/adapter/src/coord/message_handler.rs:520-523`, and any external query keep
working untouched, and the blast radius shrinks to the new builtins plus the
autogenerated catalog slt files and the reference pages.

Because the old number stays available, the same trick applies to
`mz_internal.mz_compute_hydration_statuses`. It keeps its
`(object_id, replica_id, hydrated, hydration_time)` shape, with `hydrated`
becoming `hydrated_at IS NOT NULL`, and gains a `queue_time` column. The
question is what `hydration_time` should mean:

- Keep it as `hydrated_at - installed_at`, matching today exactly, and let
  `queue_time` plus arithmetic recover the corrected interval.
- Narrow it to `hydrated_at - started_at` and let `queue_time` recover the old
  number.

This design proposes the second, because the narrow reading is the one the
column name claims and the one the PRD's history table wants, and because
`queue_time` makes the change non-destructive. It remains a user-visible
behavior change and needs a release note. The `complete_mvs` UNION branch for
materialized views that have advanced to the empty frontier stays as it is, with
NULL timestamps, since those objects have no dataflow installed.

The per-replica and per-cluster rollups the PRD asks for are then SQL over this
relation joined to `mz_cluster_replicas`, with no new compute state:

```sql
SELECT
    r.cluster_id,
    h.replica_id,
    min(h.started_at) AS started_at,
    CASE count(*) = count(h.hydrated_at)
        WHEN true THEN max(h.hydrated_at)
        ELSE NULL
    END AS hydrated_at,
    count(*) AS object_count,
    count(*) - count(h.hydrated_at) AS objects_unhydrated
FROM mz_internal.mz_compute_hydration_timestamps h
JOIN mz_cluster_replicas r ON r.id = h.replica_id
GROUP BY r.cluster_id, h.replica_id
```

### Scoping an episode to an object set

The rollup query above is over whichever objects are on the replica right now,
which is not the same as a hydration episode. A cluster that has fully hydrated
and then gains one new index will report a `max(hydrated_at)` that moves forward
to the new object, so the replica appears to have finished hydrating at the
moment the newest object did.

This is not a defect in the relation, and it is not compute's problem to solve.
The information needed to scope an episode, which objects belonged to the
replica's initial object set, lives in the controller, and the PRD already
states the intended rule: adding an object starts a new replica episode only if
the replica transitions from fully hydrated to not fully hydrated. A consumer
holding both the object set and these timestamps can apply that rule. A relation
of per-object timestamps cannot, because it does not know which objects arrived
together.

What compute owes here is that the per-object rows are individually correct and
carry a stable episode key, so any scoping rule a consumer chooses is
expressible. Restating the rollup as an illustration rather than a finished
answer: it is correct for a replica whose object set has not changed, and it is
the join with the controller's notion of the initial set that makes it correct
in general.

### Episode identity

`started_at` is generated by the replica and is stable across an environmentd
restart, a reconnection, and reconciliation, because the logging dataflows and
the demux state that holds it are not recreated. A replica process restart does
recreate them, yielding a fresh `started_at`.

That makes `(replica_id, object_id, started_at)` a natural idempotency key for
whatever writes this down. A consumer that snapshots the relation repeatedly
sees the same key for the same episode and a new key for a genuinely new one,
without needing an epoch or any controller-side bookkeeping. This is the reason
the timestamps have to come from the replica, and it is what success criterion 7
asks for.

Behavior across the events that matter:

| Event | Effect on the relation |
| --- | --- |
| environmentd restart, replica reconnects and reconciles | values unchanged, the subscribe re-snapshots and reports the same keys |
| 0dt cutover | same as above, for replicas the new generation inherits |
| replica process restart, including after an OOM kill | all introspection restarts from scratch, every object gets a fresh `installed_at` and `started_at`, all previous episodes are gone |
| `CREATE INDEX` on a running cluster | one new row, joining the replica's current object set, see "Scoping an episode to an object set" |
| materialized view reaches the empty frontier | dataflow dropped, row disappears, `complete_mvs` branch covers it |

The replica restart row is the one to design against. A crash does not surgically
remove the rows of objects that were mid-hydration, it resets the entire
introspection state of that replica, so the whole object set reappears with new
timestamps. There is therefore no way to reconstruct a failed episode after the
fact from this relation, at any layer. The only record that can exist is one a
consumer captured while the episode was in flight, which is why publishing the
in-flight row matters more than publishing the completed one.

### Why the replica and not the compute controller

The compute controller evaluates an equivalent hydration predicate already.
`ReplicaCollectionState::hydrated()`
(`src/compute-client/src/controller/instance.rs:3256-3274`) is
`as_of.is_empty() || as_of < output_frontier`, the same test the replica applies
in `CollectionState::hydrated()`
(`src/compute/src/compute_state.rs:2072-2078`). The controller also has a real
clock and an existing append-only introspection path it already uses for
`WallclockLagHistory`
(`src/compute-client/src/controller/introspection.rs:73-74`,
`instance.rs:635-680`). Stamping there would be less code.

It is nonetheless wrong for this purpose. Controller state is rebuilt from
scratch on every environmentd restart, including a 0dt cutover, so every
timestamp it stamps would reset at exactly the moment hydration visibility
matters most, and every episode would look new. Compute-controller-stamped
timestamps also cannot be made idempotent: `ReplicaState::epoch`
(`instance.rs:3096`, incremented in `rehydrate_replica` at `:1367`) resets to 1
on environmentd restart, so `(replica_id, epoch)` collides across restarts and
cannot serve as an episode key. Finally, the controller learns about hydration
one `Frontiers` response round trip late, so its durations run systematically
long against the replica's own measurement.

The replica has none of these problems. Its logging state outlives environmentd,
its clock is already epoch-anchored, and it observes hydration directly.

### Implementation touch points

- `src/compute/src/logging/compute.rs`: `ComputeEvent::HydrationStart`,
  `ExportState` fields, the packer, `handle_export`, `handle_export_dropped`,
  `handle_hydration`, and a new `handle_hydration_start`.
- `src/compute/src/compute_state.rs`: log the start event from
  `handle_schedule`, and add the corresponding `CollectionLogging` method
  alongside `set_hydrated` (`:1397`).
- `src/compute-client/src/logging.rs`: the `ComputeLog` variant and its
  `RelationDesc`.
- `src/catalog/src/durable/transaction.rs:947`: a durable log id for the new
  variant.
- `src/catalog/src/builtin/mz_introspection.rs:323`: the per-worker builtin.
- `src/catalog/src/builtin/mz_internal.rs`: the new storage-managed collection
  and its index, `mz_compute_hydration_times` recast as a compat view, and the
  `mz_compute_hydration_statuses` view definition.
- `src/adapter/src/coord/introspection.rs`: the subscribe rollup.
- Tests: `test/testdrive/hydration-status.td`, `test/testdrive/indexes.td`,
  `test/testdrive/catalog.td`,
  `test/testdrive/materialized-view-replica-targeted.td`,
  `test/cluster/blue-green-deployment/`, and the autogenerated
  `test/sqllogictest/autogenerated/mz_{internal,introspection}.slt`.
- Docs: the `mz_internal` and `mz_introspection` system catalog reference pages,
  plus a release note for the `hydration_time` semantic change.

Two places need no change, because the compat view keeps them working:
`src/mz-debug/src/system_catalog_dumper.rs` and the `ComputeHydrationTimes`
freshness check in `src/adapter/src/coord/message_handler.rs:520-523`. The
freshness check keys off the `IntrospectionType`, which the new relation
inherits, so its behaviour is unchanged.

New testdrive coverage worth adding: that `started_at` is non-NULL and
`hydrated_at` NULL for an object gated by `HYDRATION_CONCURRENCY`, that
`started_at` is NULL for an object waiting on an unavailable input, that all
three timestamps survive an environmentd restart unchanged, that a replica
restart yields entirely fresh timestamps for every object on it, and that
`mz_compute_hydration_times` reports the same numbers before and after the
change.

## Minimal Viable Prototype

The prototype is the compute and catalog change itself, exercised through
testdrive rather than through a UI. The validating query is the per-replica
rollup above, run against a cluster with a hydration concurrency limit and a
handful of indexes, showing objects moving from waiting, to hydrating, to
hydrated, with the queueing interval visible separately from the hydration
interval. A second run after an environmentd restart shows identical values,
which is the property the whole design turns on.

No SQL syntax changes and no new user-facing commands, so there is nothing to
wireframe.

## Alternatives

**Keep `time_ns` as a stored column alongside the timestamps.** Rejected because
two stored measurements of the same thing drift apart and neither can be called
authoritative. Deriving the old number in a compat view, as proposed above,
gets the compatibility without the second source of truth.

**Widen `ComputeLog::HydrationTime` in place instead of adding a variant.**
Fewer moving parts, no new durable log id. Rejected because every column of that
relation changes meaning, and a relation keeping its name while all its columns
change is the worst outcome for anyone with an existing query. The compat view
makes the rename nearly free anyway.

**Stamp in the compute controller.** Covered above. Less code, but it resets on
every environmentd restart and cannot produce a stable episode key.

**Have compute report a duration and let adapter compute timestamps from its own
clock at observation time.** Requires no compute change beyond the start event.
Rejected because the observation time is unrelated to the event time, so the
error is as large as the interval between snapshots, and the values would move
every time adapter re-observed them, making episode identity impossible.

**Emit a per-transition event log rather than a current-state relation.** A
relation of `(object_id, replica_id, occurred_at, event)` rows, matching the
shape of `mz_cluster_replica_status_history`, would pivot into
`started_at`/`hydrated_at` in a view. Attractive for the durable history
relation, and adapter may well choose exactly that shape when it writes this
down. Rejected for the compute surface because a differential current-state
relation is what compute introspection produces naturally, and because a
consumer needs current state anyway in order to notice an episode that ends
without completing.

**Raise the resolution of the existing metrics sampling instead.** This is the
option the PRD considers and rejects for peak resources, and it fails here for a
different reason: sampling cannot observe a transition, only its aftermath, so a
hydration that starts and finishes between two samples is invisible.

## Settled during review

Recorded so the reasoning is not relitigated.

- **Relation naming.** Add the new relation and turn
  `mz_compute_hydration_times` into a view over it, preserving the old shape and
  the old measurement exactly. This makes the rename cheap and leaves existing
  consumers untouched.
- **The queueing interval.** Store the start of queueing as a timestamp rather
  than only reporting a corrected duration, so both intervals stay derivable and
  narrowing `hydration_time` is non-destructive.
- **Read-only mode.** Not a compute concern. Introspection relations are not
  durable state, and `CollectionManager` already handles read-only mode itself by
  buffering batches and appending them on exit
  (`src/storage-controller/src/collection_mgmt.rs:23-25,45-52`). Durable
  recording is entirely an adapter question.
- **Clock skew.** Accepted. Pre-existing across everything derived from compute
  logging, not observed to be severe, acknowledged and documented on the
  relation rather than designed around.
- **Multi-export dataflows.** Stamp per export for now. The per-dataflow stamp
  plus a fan-out view is the known fix if multi-output dataflows ever land.
- **Incremental hydration.** A replica that gains an object after hydrating will
  report a later `max(hydrated_at)`. Not a defect. Scoping an episode to the
  initial object set requires the controller's knowledge of that set, not a
  richer introspection relation.

## Open questions

1. **Deprecating the compat view.** `mz_compute_hydration_times` as a view is
   cheap to keep, but it reports the measurement this design argues is wrong. Do
   we keep it indefinitely, or announce a removal once consumers have moved to
   the timestamps?
2. **`hydration_time` narrow or wide.** The design proposes narrowing
   `mz_compute_hydration_statuses.hydration_time` to
   `hydrated_at - started_at` and adding `queue_time`. The alternative is to
   leave `hydration_time` matching today and let consumers subtract. Both are
   non-destructive now that `queue_time` exists, so this is a judgment call about
   which number deserves the better name.
3. **The unified view.** `mz_hydration_statuses` unions compute and storage.
   Compute would now have timestamps and storage would not, since sources report
   `rehydration_latency` through their own statistics path. Do we expose
   timestamps only on the compute relation, or does the storage half need a
   counterpart before the unified view can carry them?
