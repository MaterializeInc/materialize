# Background: how a compute replica serves maintenance and reads today

This is a status-quo companion to the [two-runtime compute design](./README.md).
It records how a Materialize compute replica is structured today, how it stores
and compacts arrangements, how it serves peeks, and how the controller drives
it. The design proposes splitting maintenance and reads across two timely
runtimes. This document is the baseline it departs from.

Nothing here is a proposal. References point at `/home/user/materialize/src` at
the time of writing. Line numbers drift, so treat them as pointers to the right
function.

## A clusterd process already runs two timely runtimes

The most important status-quo fact for this design: a `clusterd` process today
already hosts two independent timely runtimes, one for storage and one for
compute. `clusterd::run` starts both, `mz_storage::serve` and
`mz_compute::server::serve`, from one process (`clusterd/src/lib.rs:441`,
`:471`), each with its own `TimelyConfig`. There is an explicit
`// TODO: unify storage and compute servers to use one timely cluster.`
(`clusterd/src/lib.rs:502`).

So "two timely runtimes in one process, sharing per-process resources" is not
new infrastructure. What is new in this design is that both runtimes would be
*compute* and would *share arrangements*, which storage and compute do not.

Two invariants the co-resident runtimes already rely on carry directly into this
design:

- **Equal worker counts are asserted across runtimes.** `clusterd` refuses to
  start unless storage and compute have equal workers per process
  (`clusterd/src/lib.rs:426-429`, "storage and compute must have equal
  workers-per-process"). This is the precondition a pairwise per-worker
  arrangement import needs.
- **Key routing is a deterministic `hash % peers`.** The arrangement exchange
  routes by `key.hashed()` (`compute/src/extensions/arrange.rs:133`), so a given
  key lands on the same worker ordinal in any runtime with the same worker
  count. This is what makes worker-local, non-networked pairwise import feasible.

### The lifecycle seam

Timely threads are spawned in exactly one place, `ClusterSpec::build_cluster`
(`cluster/src/client.rs:182-279`), which calls timely's `execute_from`
(`client.rs:260`) and stores the `WorkerGuards` in a `TimelyContainer`
(`client.rs:50-61`). Compute's `serve` (`compute/src/server.rs:85-131`) builds a
`Config` that `impl ClusterSpec` and calls `build_cluster` once
(`server.rs:121`), producing one compute runtime.

Resources group into three scopes, which is the map for where a second runtime
would attach:

- **Per process (shared today):** `Arc<PersistClientCache>` (created once in
  `clusterd/src/lib.rs:394`, passed to both servers, "intentionally shared
  between workers" `compute_state.rs:114-116`), `TxnsContext`,
  `ConnectionContext`, `Arc<TracingHandle>`, `MetricsRegistry`. Also several
  process-global statics that assume a single compute instance:
  `mz_row_spine::DICTIONARY_COMPRESSION` (`compute_state.rs:497`), lgalloc and
  pager config (`compute_state.rs:255-361`), documented as "a replica process
  hosts a single instance" (`compute_state.rs:495-496`).
- **Per runtime (one per `ClusterSpec`/`execute_from`):** the `TimelyContainer`
  (`cluster/src/client.rs:50-61`), the networking mesh from
  `initialize_networking` (`cluster/src/communication.rs:100`), and the
  `WorkerConfig`.
- **Per worker (per thread):** `ComputeState` including the `TraceManager`
  (`compute_state.rs:86`, `:97`), the command channel, and the `&mut
  TimelyWorker`.

## Per-worker state and the command loop

Each worker owns a `ComputeState` (`compute/src/compute_state.rs:86`,
"Worker-local state that is maintained across dataflows"), constructed lazily on
the first `CreateInstance` command (`compute/src/server.rs:431-443`). It holds
the collections, the `traces: TraceManager` (`compute_state.rs:97`), the
`pending_peeks` map (`compute_state.rs:108-109`), and the command history.

The worker loop is `Worker::run_client` (`compute/src/server.rs:372-422`). Each
iteration runs periodic maintenance (`traces.maintenance()`,
`report_frontiers()`, `server.rs:394-400`), steps timely
(`step_or_park`, `server.rs:411`), applies pending commands
(`handle_pending_commands`, `server.rs:414`), then services reads:

```rust
compute_state.process_peeks();
compute_state.process_subscribes();
compute_state.process_copy_tos();
```
(`server.rs:416-420`). Command dispatch is
`ActiveComputeState::handle_compute_command`
(`compute_state.rs:446-478`), a match over `ComputeCommand` variants.

Commands reach workers through a dataflow-based command channel: the controller
sends only to worker 0, which broadcasts over the timely fabric so every worker
sees the same ordered stream (`compute/src/command_channel.rs:10-23`).
`CreateDataflow` is split per worker by `partition_among`
(`command_channel.rs:156-199`), everything else is replicated
(`command_channel.rs:201-204`). Each runtime has its own worker 0.

## Arrangements: storage, rendering, import, compaction

### Storage

Arrangements live in `TraceManager`, a `BTreeMap<GlobalId, TraceBundle>`
(`compute/src/arrangement/manager.rs:33-36`), held on `ComputeState`
(`compute_state.rs:97`). A `TraceBundle` (`manager.rs:234-239`) bundles an ok
trace, an err trace, and lifetime tokens:

```rust
pub struct TraceBundle {
    oks: PaddedTrace<RowRowAgent<Timestamp, Diff>>,
    errs: PaddedTrace<ErrAgent<Timestamp, Diff>>,
    to_drop: Option<Rc<dyn Any>>,
}
```

The handle type is a `TraceAgent` over the spine (`RowRowAgent = TraceAgent<
RowRowSpine>`, `typedefs.rs:98`). The ok arrangements are backed by
`RowRowSpine` and errors by `ErrSpine`. On `main` these spines wrap batches in
`Rc` (`row-spine/src/lib.rs:51`, `typedefs.rs:107`). The `Arc`-batches migration
(#37743) changes the wrapper to `Arc` and adds `Send + Sync` assertions over the
batch contents. Either way the coupling that keeps a trace single-runtime is
*above* the batch layer: the `TraceAgent` handle and the `to_drop: Rc<dyn Any>`
token (`manager.rs:238`) are `Rc`-based and `!Send`.

### Rendering and registration

`CollectionBundle::arrange_collection` arranges the ok stream into a
`RowRowSpine` (`compute/src/render/context.rs:1129-1212`) and the err stream into
an `ErrSpine` (`context.rs:1104-1111`). `Context::export_index`
(`compute/src/render.rs:688-758`) registers the resulting handles into the trace
manager:

```rust
compute_state.traces.set(idx_id, TraceBundle::new(oks.trace, errs.trace).with_drop(needed_tokens));
```
(`render.rs:734-737`).

### Import (the single-runtime analog of this design)

A new dataflow that reads an existing index imports it worker-locally.
`Context::import_index` (`render.rs:591-682`) looks the trace up in the same
worker's `TraceManager` (`render.rs:603`), asserts the import is legal against
compaction ("Index has been allowed to compact beyond the dataflow as_of",
`render.rs:604-607`), and calls `import_frontier_core` to bring the trace into
the new dataflow scope restricted to `[as_of, until)` (`render.rs:611-625`). No
exchange happens. This is a worker-local trace-handle handoff. It is the exact
operation a second runtime needs a cross-runtime analog for.

### Compaction and read holds

Two frontiers per trace:

- **Logical compaction** (the `since`) is driven by the controller.
  `AllowCompaction` (`compute_state.rs:466`) calls
  `TraceManager::allow_compaction` (`manager.rs:79-84`), which sets
  `set_logical_compaction` on both traces. An empty frontier means drop the
  collection (`compute_state.rs:662-671`).
- **Physical compaction** is driven by periodic maintenance:
  `TraceManager::maintenance` sets `set_physical_compaction(upper)` on each trace
  (`manager.rs:54-71`), called from the worker loop (`server.rs:396`).

There is no separate read-hold object on the replica. The read hold *is* the
trace handle plus its logical-compaction frontier. A peek pins a hold by cloning
the `TraceBundle` and calling `set_logical_compaction(timestamp)` on it
(`compute_state.rs:1231-1252`). The cloned handle keeps data readable until the
peek finishes and drops. The authority for how far a trace may compact lives in
the controller (next section), which sends `AllowCompaction`.

## The peek path

Where the fast-path versus slow-path decision happens: in the adapter or
coordinator, not in compute. `PeekPlan` is `FastPath` or `SlowPath`
(`adapter/src/coord/peek.rs:442-446`). A fast-path `PeekExisting` reads an
existing arrangement (`peek.rs:799-820`). A slow-path plan ships a transient
index dataflow first via `create_dataflow`, then peeks it and drops it
(`peek.rs:855-926`, teardown at `:1002-1004`). Both paths send compute the *same*
`Peek` command with `PeekTarget::Index { id }` (`compute-client/src/protocol/
command.rs:411-425`, `:449-474`).

On the replica, `Peek` is handled by `handle_peek` (`compute_state.rs:673-699`),
which clones the `TraceBundle` out of the manager (`compute_state.rs:678`),
builds a `PendingPeek::Index`, and calls `process_peek`.

The actual read. `IndexPeek::seek_fulfillment` (`compute_state.rs:1504-1544`)
first gates on frontiers, then walks the trace. The gate contract
(`compute_state.rs:1505-1516`):

> To produce output at `peek.timestamp`, we must be certain that it is no longer
> changing. A trace guarantees that all future changes will be greater than or
> equal to an element of `upper`. If an element of `upper` is less or equal to
> `peek.timestamp`, then there can be further updates that would change the
> output.

So the peek is not ready until `upper` passes `timestamp`
(`compute_state.rs:1527-1534`), and it errors if `since` is beyond `timestamp`
(`compute_state.rs:1536-1544`). The serving window is `since <= timestamp <
upper`. The cursor walk is `PeekResultIterator`
(`compute/src/compute_state/peek_result_iterator.rs`), which walks a `CursorList`
over the trace's batch cursors and accumulates diffs only for times
`<= peek_timestamp` (`peek_result_iterator.rs:257-262`). Literal constraints use
`cursor.seek_key` for point lookups (`peek_result_iterator.rs:91-105`).

**Peek reads run on the timely worker thread.** `process_peeks`
(`compute_state.rs:1058-1065`) is called from the worker loop
(`server.rs:417`) and calls `seek_fulfillment` inline (`compute_state.rs:982-988`),
so a large cursor walk blocks the timely step loop and contends with
maintenance. The one exception is the Persist peek (`PendingPeek::persist`,
`compute_state.rs:1254-1325`), which spawns an async task and re-activates the
worker via a `SyncActivator` when done. Index arrangement reads do not offload.
This offload is the precedent the design's stage 1 builds on.

## Ephemeral dataflows

There is no compute-layer "ephemeral query" concept. A slow-path peek's one-off
dataflow is an ordinary `CreateDataflow` whose exports are all transient.
`DataflowDescription::is_transient()` is "all export ids are transient"
(`compute-types/src/dataflows.rs:380-383`). A maintained index or MV exports a
persistent `GlobalId::User`, kept alive by controller read holds. A transient
peek dataflow exports only `GlobalId::Transient` and is created then dropped
around one peek. The only rendering consequence is that compute-event logging is
skipped for transient dataflows (`render/context.rs:116-123`). Subscribes are the
other transient class, maintained continuously rather than read once.

## The controller protocol

The protocol is documented at `compute-client/src/protocol.rs:10-135`
(asynchronous, in-order, three stages). `ComputeCommand`
(`compute-client/src/protocol/command.rs:38`) carries the lifecycle:
`CreateInstance`, `CreateDataflow` (`command.rs:144`), `Schedule`
(`command.rs:154`), `AllowCompaction { id, frontier }` (`command.rs:209-214`),
`Peek` (`command.rs:244`), `CancelPeek` (`command.rs:259`). `ComputeResponse`
(`compute-client/src/protocol/response.rs:29`) carries `Frontiers`
(`response.rs:61`), `PeekResponse` (`response.rs:79`, exactly one per peek),
`SubscribeResponse`, and `CopyToResponse`.

### Replica addressing

The controller talks to a replica as one logical client, internally partitioned
across the replica's processes. Each replica has one async `ReplicaTask`
(`compute-client/src/controller/replica.rs:159`) and a `Partitioned` client with
one partition per control address (process) (`replica.rs:42`, `:184-191`). The
partitioning state `PartitionedComputeState` (`compute-client/src/service.rs:79`)
exists both on the controller (dispatching between processes) and inside each
process (dispatching between workers) (`service.rs:66-70`). `split_command`
broadcasts `Hello`/`UpdateConfiguration` to all parts and sends everything else
to part 0 (`service.rs:365-382`). `absorb_response` merges: frontiers by meet
across parts (`service.rs:179`, `:417`), peek responses by union once all parts
report (`service.rs:219`).

**There is no protocol-level notion of two runtimes or sub-replicas inside one
process.** The controller's model of a replica is `ReplicaState`
(`instance.rs:3082-3097`): one client, one config, one epoch, a flat
`BTreeMap<GlobalId, ReplicaCollectionState>`. The only multiplexing it knows is
processes (partition axis) and, below that, workers (in-process command channel).

### Frontier reporting and compaction control

The replica reports frontiers from `ComputeState::report_frontiers`
(`compute_state.rs:825-918`): write frontier from `traces.oks_mut().read_upper`
(`:842-847`), emitted as `ComputeResponse::Frontiers`. The controller consumes
them in `handle_frontiers_response` (`instance.rs:2042-2080`), advancing the
global write frontier and relaxing read holds per read policy
(`maybe_update_global_write_frontier`, `instance.rs:1870-1919`).

Compaction is decided by controller read holds, not by the replica.
`apply_read_hold_change` (`instance.rs:1922-1981`) recomputes `new_since` from
the capability frontier, checks it does not regress, propagates it to compute and
storage dependencies, and emits `ComputeCommand::AllowCompaction { id, frontier:
new_since }`. The `since <= upper` relationship holds because the since is
derived from the write frontier through the read policy
(`instance.rs:1840-1860`). A peek's read hold is what keeps `AllowCompaction`
from advancing `since` past the peek timestamp, which is exactly the guarantee
the peek's frontier gate relies on.

## Load-bearing facts this design inherits

- Two timely runtimes per process is already how `clusterd` works, with equal
  worker counts asserted and `hash % peers` routing. The preconditions for
  pairwise import exist.
- The arrangement-sharing boundary is `ComputeState::traces` / `TraceManager`,
  accessed worker-locally today via `export_index` / `import_index`. Batches are
  already `Arc` and `Send + Sync`. The `TraceAgent` handle and `to_drop` token
  are still `Rc`.
- Index peek reads run on the worker thread and contend with maintenance.
  Persist peeks already offload to an async task, which is the pattern a
  worker-offloaded index peek would follow.
- The controller drives compaction through its own read holds, addresses a
  replica at process granularity, and has no notion of sub-runtimes. A split can
  be made transparent to it, or modeled explicitly, and the compaction authority
  is the controller either way.
- Several process-global statics assume one compute instance per process. Two
  compute runtimes in one process must reconcile them.
