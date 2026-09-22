# EXPLAIN ANALYZE for storage objects

- Associated:
  - Unified cluster design: [20260915_unified_cluster.md](./20260915_unified_cluster.md)
  - Existing `EXPLAIN ANALYZE`: [20230802_explain_running_dataflows.md](./20230802_explain_running_dataflows.md)
  - [CPU-259](https://linear.app/materializeinc/issue/CPU-259)

## The Problem

`EXPLAIN ANALYZE` answers "where is this object spending CPU and memory" for
indexes and materialized views. It has no answer for sources, tables created
from sources, or sinks. Users debugging a slow or memory-hungry source today
read raw `mz_dataflow_operators` output, guess which operators belong to which
subsource, and cross-reference `mz_source_statistics` by hand.

On a unified-cluster replica, storage dataflows run on the compute Timely
cluster, so their operators already appear in `mz_dataflows`,
`mz_dataflow_operators`, `mz_scheduling_elapsed`, and `mz_arrangement_sizes`.
The per-operator data exists. What is missing is the mapping from operators
back to catalog objects:

* `mz_dataflow_global_ids`, `mz_compute_exports`, `mz_lir_mapping`, and
  therefore `mz_mappable_objects`, are written only by compute's
  `materialize/compute` logger. Storage never logs to it and cannot, since
  `mz-compute` depends on `mz-storage` and not the reverse.
* The only link from a storage dataflow to an object is its name,
  `Source dataflow: {primary_id}`. Sinks use the same prefix. Export
  (subsource, table) ids never appear in the dataflow name.
* Some operators carry an export id in their name (`SourceGenericStats`,
  `upsert_rehydration_backpressure`, `persist_sink`). Others do not
  (`Reclock`, decode, the `NONE` envelope maps, `Partition`).
* One export's operators occupy about three disjoint operator-id ranges,
  because `create_raw_source`, `render_source`, and `build_ingestion_dataflow`
  render reclock, decode and envelope, and `persist_sink` in separate loops.

There is also a correctness gap in the unified topology. Compute registers its
Timely loggers in `initialize_logging`, on the process's first
`CreateInstance`. The storage and compute controllers connect independently,
so a storage-internal command can render an ingestion before that first
`CreateInstance`. That dataflow captures no logger and stays invisible to
introspection for its lifetime.

## Success Criteria

* A user can run one statement against a source, a table from source, or a
  sink and get a per-stage CPU and memory breakdown in the same shape as
  `EXPLAIN ANALYZE` for an index.
* A user can see, per export, ingestion progress: status, snapshot progress,
  upstream offset lag, received and committed counts, rehydration latency,
  and wallclock lag.
* Every storage dataflow rendered on a unified replica appears in
  introspection, regardless of the order in which the controllers connect.
* `EXPLAIN ANALYZE CLUSTER` lists storage objects alongside compute objects.
* No change to the storage or compute controller protocols.

## Out of Scope

* Two-cluster replicas. The storage log bridge is gone, so storage operators
  are not visible there. `CPU` and `MEMORY` return no rows on such a replica.
  `INGESTION` does not depend on operator introspection and works on both
  topologies.
* Operator frontiers for storage dataflows. Reachability loggers are
  registered for compute's timestamp types only, not for the source-native
  `FromTime` scope. A `FRONTIERS` property is follow-up work.
* Per-worker source statistics. `mz_source_statistics` is aggregated on
  worker 0. Skew for storage objects is computed from operator introspection
  only.
* Oneshot ingestions (`COPY FROM`). They are short-lived and not catalog
  objects.
* `HINTS`. Storage dataflows have no hierarchical reductions to advise on.

## Solution Proposal

### Syntax

The existing grammar is extended with new explainee kinds and one new
property:

```
EXPLAIN ANALYZE { CPU | MEMORY | CPU, MEMORY } [WITH SKEW]
    FOR { SOURCE | TABLE | SINK } <name> [AS SQL]

EXPLAIN ANALYZE INGESTION FOR { SOURCE | TABLE } <name> [AS SQL]
```

* `FOR SOURCE` covers the whole ingestion dataflow: the stages it shares
  across exports, plus every export's stages.
* `FOR TABLE` accepts a table created `FROM SOURCE` or a subsource. It shows
  that export's stages plus the shared stages it depends on. Shared stages are
  labelled `(shared)` so their cost is not read as belonging to the table
  alone. The tree has no root row, because the dataflow total includes the
  sibling exports, so the shared stages and the table's `Export` stage are
  the top level. A plain table (no source) is rejected.
* `FOR SINK` covers the sink's export dataflow.
* `INGESTION` is valid for sources and tables from source only.

As with the existing statement, the planner rewrites each form into SQL over
introspection relations, and `AS SQL` returns that SQL.

### Output: `CPU` and `MEMORY`

The output is a tree of pipeline stages, indented by nesting, ordered top-down
in rendering order. For a PostgreSQL source with two tables, one of them using
an upsert envelope:

```
              operator               | total_elapsed | total_memory | total_records
-------------------------------------+---------------+--------------+---------------
 Source pg_src (u12)                 | 00:04:11.2    | 1204 MB      | 8912331
   Remap                             | 00:00:01.8    |              |
   Reader::Snapshot                  | 00:00:00.0    |              |
   Reader::Replication               | 00:01:02.4    |              |
   Partition                         | 00:00:03.1    |              |
   Healthcheck                       | 00:00:00.1    |              |
   Export public.orders (u13)        | 00:00:41.0    |              |
     Reclock                         | 00:00:09.7    |              |
     Envelope NONE                   | 00:00:02.2    |              |
     PersistSink                     | 00:00:29.1    |              |
   Export public.items (u14)         | 00:02:23.0    | 1204 MB      | 8912331
     Reclock                         | 00:00:08.9    |              |
     Decode                          | 00:00:31.5    |              |
     Envelope UPSERT                 | 00:01:18.3    | 1204 MB      | 8912331
     PersistSink                     | 00:00:24.3    |              |
```

Parent rows aggregate their children, so a stage's cost is attributed exactly
once and the root row is the dataflow total. CPU counts leaf operators only,
because a region's scheduling time includes the time of the operators inside
it. `EXPLAIN ANALYZE CLUSTER` applies the same rule to storage objects.

A restarted ingestion is rendered as a new dataflow while its predecessor may
still be shutting down. Stage ids are positions in each dataflow's own export
order, so the same id can name different exports in the two. The tree shows
the newest dataflow that renders the explainee, which is the one with the
highest dataflow id.

`WITH SKEW` adds `worker_id` and the `*_ratio`, `worker_*`, and `avg_*`
columns, with the same definitions as the index form. It also adds
`active_workers`, the number of workers that scheduled the stage at all.
Remap and most readers run on a single active worker by design. Without this
column that shows up as extreme skew on every source.

The memory columns report arrangement memory only, from
`mz_arrangement_sizes`. For the upsert v2 envelope that is the feedback
arrangement. The upsert v1 envelope keeps its state in RocksDB, which is not
heap and is not an arrangement, so it does not appear here. It is reported by
`INGESTION` as `state_bytes` and `state_records`. Summing RocksDB bytes with
arrangement bytes would produce a number that matches neither memory nor disk.

### Output: `INGESTION`

One row per export, plus a row for the source itself:

```
     object     | status  | snapshot_progress | offset_lag | messages_received | bytes_received | updates_committed | state_bytes | state_records | rehydration_latency | wallclock_lag
----------------+---------+-------------------+------------+-------------------+----------------+-------------------+-------------+---------------+---------------------+---------------
 public.pg_src  | running | 1.00              |       1824 |          91822103 | 38 GB          |          91820011 | 1204 MB     |       8912331 | 00:00:41            | 00:00:02.3
 public.orders  | running | 1.00              |            |          41020077 | 12 GB          |          41019980 | 0 bytes     |             0 | 00:00:12            | 00:00:01.9
 public.items   | stalled | 1.00              |            |          50802026 | 26 GB          |          50800031 | 1204 MB     |       8912331 | 00:00:41            | 00:03:10.0
```

It is built entirely from existing relations:

* `status` from `mz_source_statuses`.
* `snapshot_progress` is 1.00 once `snapshot_committed` is true, and
  otherwise `snapshot_records_staged / snapshot_records_known`, NULL while
  the total is unknown. A known total of 0 is treated as unknown, because
  the snapshot operators write 0 before they have sized the upstream table.
* `offset_lag` is `offset_known - offset_committed`. Offsets belong to the
  upstream connection, so only the source row reports it.
* `state_bytes` and `state_records` come from `bytes_indexed` and
  `records_indexed`.
* `wallclock_lag` comes from `mz_wallclock_global_lag`.
* The remaining counters come from `mz_source_statistics`. They are
  cumulative, not rates.

`mz_source_statistics` has one row per object and replica that has run it.
Each output row reports the replica with the highest `updates_committed`,
which is the one furthest ahead. A lagging replica's counters are not shown.
Summing across replicas would double-count the same upstream data.

Object names are schema-qualified. The source row uses
`mz_source_statistics`'s roll-up of exports into their parent. `FOR TABLE`
returns only that export's row.

### Mapping storage operators to objects

Storage gains its own logger, `materialize/storage`, with an event type
defined in `mz-storage`. Compute's `register_loggers` registers it alongside
the others, and compute's logging dataflow turns its events into two new
introspection collections. Defining the events in `mz-storage` works with the
existing crate edge: compute already depends on storage to host it. The
alternative is for compute's `process_storage_guest` to log on storage's
behalf. That is less code, but it couples the mapping to the guest loop, and
the guest loop can't see stage boundaries inside a render call.

Events:

* `DataflowGlobal { dataflow_index, global_id }`, logged for the primary id
  and every export id when `build_ingestion_dataflow` or
  `build_export_dataflow` constructs a dataflow.
* `StageMapping { dataflow_index, global_id, stage_id, parent_stage_id,
  nesting, stage, operator_id_start, operator_id_end }`, logged by bracketing
  each rendering stage with `peek_identifier()`, the same mechanism compute's
  `render_plan` uses for LIR nodes.
* `DataflowShutdown { dataflow_index }`, logged when every operator of the
  dataflow has shut down.

Storage never retracts what it logs, and it does not observe operator
shutdown. Compute's Timely logging fragment does, so it logs
`DataflowShutdown` into the storage logger when it sees a dataflow's last
operator shut down. The storage logging fragment then retracts every
`DataflowGlobal` and `StageMapping` row of that dataflow index. Keying
retractions by dataflow index keeps them correct when a restarted ingestion
renders the same ids in a new dataflow before the old one has shut down. This
means compute emits an event type owned by `mz-storage`, which is the
existing crate edge. Any other host of storage dataflows that consumes
`materialize/storage` must also log `DataflowShutdown`, or the rows of dropped
dataflows stay for the replica's lifetime.

New relations:

* `mz_introspection.mz_storage_dataflow_global_ids_per_worker` and its
  worker-0 view `mz_storage_dataflow_global_ids(id, global_id)`.
* `mz_introspection.mz_storage_stage_mapping_per_worker` and its worker-0
  view `mz_storage_stage_mapping(dataflow_id, global_id, stage_id,
  parent_stage_id, nesting, stage, operator_id_start, operator_id_end)`.
  Stage ids are unique only within a dataflow, so consumers scope them by
  `dataflow_id`.

A stage's operators can span several disjoint id ranges, so a
`(dataflow_id, global_id, stage_id)` triple may appear in several rows, one
per range. The
SQL rewrite already expands each range with `generate_series`, so multiple
ranges per stage need no extra handling. This avoids reordering storage
rendering to make each export contiguous, which would touch every connector's
render path for no user-visible gain.

The stage set is fixed and connector-independent: `Remap`, `Reader::Snapshot`,
`Reader::Replication` (a single `Reader` for connectors that don't separate
the two), `Partition`, `Healthcheck`, `Export`, `Reclock`, `Decode`,
`Envelope <kind>`, and `PersistSink`. Sinks use `Export`, `Arrange`, `Encode`,
and `Sink`. Shared stages carry the primary id, and per-export stages carry
the export id. Operators rendered between brackets are not attributed to any
stage. The root row still counts them, so the tree is never silently
incomplete. `mz_dataflow_global_ids` and `mz_mappable_objects` are extended
with `UNION ALL` over the storage relations. Consumers that join them by id
see additional rows but no changed ones.

### Logger registration ordering

The unified host buffers the storage-internal commands that build, restart, or
drop ingestion and sink dataflows when they arrive on the command lane before
the first `CreateInstance` has initialized compute logging, and dispatches
them in lane order immediately after. Every worker sees the same lane order,
so all workers buffer and replay the same commands at the same position, and
dataflow construction stays deterministic. Compute logging is initialized once
per process and reused across compute reconnects, so the buffer only ever
holds commands from process start. The cost is that a replica does not render
ingestions or sinks until its compute controller has connected. Every replica
has both controllers connected in normal operation.

Oneshot ingestions and configuration updates are not buffered. A
`CancelOneshotIngestion` is an external storage command that only drops an
already-rendered oneshot ingestion, and storage reconciliation treats a
oneshot ingestion as running only once it has rendered. Buffering the render
would let a `COPY FROM` cancelled during the buffering window render anyway,
and would let a storage reconnect in that window render it twice. Oneshot
ingestions are out of scope for introspection mapping, so rendering them
without a logger loses nothing. Configuration updates pass through so an
unbuffered oneshot ingestion renders with them. A buffered dataflow then
renders with the configuration current at release, as if its command had
arrived at that position.

The frontiers inside a buffered `CreateIngestionDataflow` are not refreshed.
Its `as_of` and resume uppers are the ones the async storage worker computed
when it produced the command, and that worker holds the remap shard's since
at the `as_of` for a bounded time only. If the buffering window outlasts that
hold and another replica advances the since, the remap operator rejects the
stale `as_of` and the ingestion recovers through `SuspendAndRestart`, which
recomputes the frontiers. This needs the compute controller to connect
minutes after the storage controller, so the design accepts the restart
instead of recomputing frontiers at release.

### Planning and cluster targeting

Log reads resolve against the session's cluster and, on a multi-replica
cluster, require `cluster_replica`. A source runs on its own cluster, and an
ingestion that prefers a single replica runs on one replica only. When the
explainee's cluster differs from the session's cluster, the planner returns an
error with a hint to `SET cluster = <name>`, instead of silently returning an
empty tree. The existing index form returns an empty result in the same
situation. This design does not change that.

On a two-cluster replica, `CPU` and `MEMORY` return an empty result. The
planner cannot see a replica's topology, and the statement is a SQL rewrite
with no hook to raise a notice from query results. The user-facing docs state
the unified-cluster requirement.

### Privileges

Same rule as the existing statement: with `restrict_to_user_objects`, the
caller must own the explainee.

## Minimal Viable Prototype

The `INGESTION` property needs no storage changes and is the first slice.
`CPU` and `MEMORY` follow once the storage logger and mapping relations land.
Validation for each slice:

* sqllogictest golden output for `AS SQL` of every form.
* A testdrive file on a unified cluster that creates a load generator source
  with multiple exports, an upsert source, and a Kafka sink, and asserts a
  non-empty tree with every expected stage name for each.
* A testdrive section that starts a replica whose storage controller connects
  before its compute controller, and asserts the source still appears in
  `mz_storage_dataflow_global_ids`.

## Alternatives

* **Parse dataflow and operator names.** `Source dataflow: {id}` and the
  export ids in some operator names are enough for a rough tree today with no
  Rust changes. It can't attribute the unlabelled operators, it breaks
  whenever an operator is renamed, and it can't distinguish sources from sinks
  without also joining the catalog. It is fine as a stopgap view, but not as
  the basis for a stable statement.
* **Reuse `mz_lir_mapping`.** Storage stages are not LIR nodes, and
  `lir_id` ordering encodes the LIR tree's post-order. Mixing the two would
  give that column two meanings.
* **Reorder storage rendering for contiguous export ranges.** This would give
  one range per stage, but it rewrites every connector's render loop, and it
  can't cover operators that are genuinely shared.
* **Register compute loggers at worker start, not at `CreateInstance`.** The
  logging configuration (interval, log index ids) arrives in `CreateInstance`,
  so there is nothing to register with before it.

## Open questions

* Should `MEMORY` gain the heap-size columns for storage arrangements? That
  needs compute's `ArrangementSize` logger attached to the upsert v2 feedback
  arrangement and the sink's arrangement. It is the same work as for compute
  arrangements, but it touches storage render paths.
* The upsert v2 stash batcher is built without a logger, so its memory is
  invisible. Giving it one is small, but it changes the batcher's
  construction. Should that be part of this work, or come later?
