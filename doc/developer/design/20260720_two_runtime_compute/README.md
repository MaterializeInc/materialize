# Two-runtime compute: isolating reads from maintenance

- Associated:
  - [background.md](./background.md): how a compute replica serves maintenance
    and reads today (the status-quo baseline this design departs from).
  - [Shared arrangements across timely runtimes](../20260719_shared_arrangements_across_runtimes/README.md):
    the differential-level primitive this design consumes.
  - Implementation of the `Arc`-batches prerequisite: differential
    TimelyDataflow/differential-dataflow#807, materialize
    MaterializeInc/materialize#37743.

## The Problem

A compute replica does two very different kinds of work on the same timely
worker threads. It maintains indexes and materialized views, which is CPU-bound
and bursty (arrangement merges, operator work proportional to input volume). And
it serves reads: peeks against arrangements, and the short-lived dataflows
behind ad-hoc queries.

These contend. An index peek runs its cursor walk synchronously on the worker
thread (`compute/src/compute_state.rs:982-988`, driven from the loop at
`compute/src/server.rs:417`), so it waits for the worker that owns the relevant
shard to come around to it. That worker may be deep in a join or a large merge.
The result is read tail latency proportional to how busy maintenance is, which
is exactly when users are watching. Persist peeks already sidestep this by
offloading to an async task (`compute_state.rs:1254-1325`). Index peeks, the
common fast path, do not.

The root cause is that reads and maintenance share worker threads, and an
arrangement is readable only from the worker that maintains it. The
[`Arc`-batches work](../20260719_shared_arrangements_across_runtimes/README.md)
removes the second half of that restriction: arrangement batches are now `Arc`'d
and their contents are `Send + Sync` (`row-spine/src/lib.rs:175-177`), so a batch
can be read from another thread. This design uses that to move reads off the
maintenance threads.

## Success Criteria

- A read (a peek, and later an ad-hoc query dataflow) does not wait behind
  maintenance work on a worker thread. Read tail latency is decoupled from
  maintenance load.
- Correctness is unchanged. A read observes exactly the collection at its
  timestamp, gated by the same `since <= timestamp < upper` window as today.
- The controller remains the single authority on compaction. No arrangement
  compacts past a time a read still needs.
- Maintenance throughput is not regressed. Publishing an arrangement for reading
  costs the maintenance path only constant per-batch overhead.
- The change is stageable: an early increment ships read isolation without the
  full second-runtime architecture.

## Out of Scope

- Cross-process sharing. Everything here is threads within one replica process.
  Cross-process is persist's job.
- Changing the fast-path versus slow-path peek decision, which lives in the
  coordinator (`adapter/src/coord/peek.rs`). This design changes only where the
  read executes on the replica, not how the coordinator plans it.
- Scheduling policy between the two runtimes (thread counts, pinning,
  priorities). Discussed under open questions, not decided here.
- Multi-replica or cross-replica concerns. This is about one replica's internal
  structure.

## Solution overview: two stages

The design is a full arc, delivered in two stages. Stage 1 gets most of the
latency win with a small, self-contained change and no second runtime. Stage 2
is the full two-runtime architecture that also isolates ad-hoc query dataflows.

The staging matters because the two stages need very different amounts of the
[shared-arrangements primitive](../20260719_shared_arrangements_across_runtimes/README.md).
Stage 1 needs only `Arc` batches and a `Send` snapshot. Stage 2 needs the
publish, import, and cross-thread hold machinery.

## Stage 1: off-worker peek reads

**Idea.** Keep one timely runtime. On an index peek, the worker does only the
cheap, gated part on-thread, then hands the expensive cursor walk to a reader
thread pool. This mirrors what Persist peeks already do
(`compute_state.rs:1254-1325`).

**Mechanism.** When a `Peek { target: Index { id }, timestamp, .. }` arrives, the
worker:

1. Gates as today: it waits until the trace `upper` passes `timestamp` and
   checks `since <= timestamp` (`compute_state.rs:1517-1544`). This stays
   on-thread because it is a frontier comparison, not a data scan.
2. Takes a `Send` snapshot of its own trace. A snapshot is a clone of the
   current batch chain (a handful of `Arc` clones) plus the `since` and `upper`
   frontiers. This is cheap and does not block on maintenance.
3. Hands the snapshot, the `SafeMfpPlan`, the `RowSetFinishing`, and the peek
   `uuid` to a reader pool. The pool runs the existing `PeekResultIterator`
   cursor walk (`compute/src/compute_state/peek_result_iterator.rs`) against the
   snapshot and produces the `PeekResponse`.
4. The pool delivers the response back through the existing response path,
   re-activating the worker via a `SyncActivator` exactly as the Persist peek
   task does.

**Why this is correct and cheap.**

- The snapshot's `Arc`'d batches are immutable. Once taken, the maintenance
  worker may merge and compact freely. The reader pool reads the pinned
  pre-merge batches. No torn read, no lock held across the walk.
- The controller's per-peek read hold already keeps the trace `since <=
  timestamp` until the `PeekResponse` is received
  (`compute-client/src/controller/instance.rs:1922-1981`), so the snapshot is
  accurate at `timestamp`. The existing `since` gate stays as the safety net.
- `PeekResultIterator` already operates over a `CursorList` and a `Vec` of
  batches, which become `Send` once batches are `Send + Sync`. The walk moves
  off-thread almost unchanged.

**What stage 1 needs from the primitive.** Only the `Arc` batches (done in #807
and #37743) and a `Send` snapshot type. It does *not* need a publication point,
import, or cross-thread hold registration. The worker snapshots its own trace on
demand. There is no second runtime and no cross-runtime compaction to reconcile.

**What stage 1 does not solve.** Slow-path peeks still ship a transient dataflow
onto the maintenance runtime, and ad-hoc query dataflows still run there. Their
rendering and operator work still contends with maintenance. Stage 1 isolates
the fast-path read, which is the common case and the sharpest latency problem,
but not query dataflow rendering.

### Cancellation and lifecycle (stage 1)

`CancelPeek` must cancel an in-flight offloaded read. The reader pool task holds
a cancellation flag keyed by `uuid`, checked between output batches, mirroring
how `pending_peeks` is drained today (`compute_state.rs:701-705`). A snapshot
held by a running task keeps its batches alive independently of the trace, so
cancellation is about not sending a response, not about memory safety.

## Stage 2: the query runtime

**Idea.** Stand up a second compute timely runtime in the same process, the
*query runtime*, alongside the *maintenance runtime*. The maintenance runtime
owns indexes and MVs as today and publishes their arrangements. The query
runtime serves slow-path peeks and renders ad-hoc query dataflows by importing
those arrangements, so query rendering never competes with maintenance for
threads.

This is where the
[shared-arrangements primitive](../20260719_shared_arrangements_across_runtimes/README.md)
is used in full: `Arranged::publish` on the maintenance side, `import` and
`snapshot` on the query side, with cross-thread hold aggregation driving
compaction.

**Topology.** One replica process, two compute `serve` instances (two
`ClusterSpec` runtimes, `compute/src/server.rs:85-131`), equal worker counts.
Query worker `i` imports maintenance worker `i`'s publication point. Because both
runtimes route keys by the same `hash % peers`
(`compute/src/extensions/arrange.rs:133`), a key lives on the same worker ordinal
in both, so an imported arrangement is co-located with any query-side operator
on the same key, with no cross-worker exchange. This reuses the equal-worker
invariant `clusterd` already asserts between storage and compute
(`clusterd/src/lib.rs:426-429`).

**Publication.** When the maintenance runtime renders an index
(`export_index`, `compute/src/render.rs:688`), it also publishes the arrangement
and records the resulting `SharedTraceHandle`s in a per-process, per-worker
registry keyed by `GlobalId`, so the query runtime's worker `i` can find worker
`i`'s handle. Publication cost is one `Arc` clone plus a mutex push per batch, so
maintenance throughput is unaffected.

**Query-side reads.**

- A slow-path peek is served by the query runtime: instead of shipping a
  transient index dataflow onto the maintenance runtime, the query runtime
  renders it, importing the base arrangements it needs, and peeks its own result.
- An ad-hoc query dataflow (the general form) is rendered entirely on the query
  runtime, importing maintenance arrangements as ordinary `Arranged` collections
  through `SharedTraceHandle::import`.

**Command routing.** The replica process demultiplexes the controller's command
stream between the two runtimes: `Peek`, `CancelPeek`, and transient query
dataflows go to the query runtime; `CreateDataflow` for maintained
indexes/MVs, `Schedule`, `AllowCompaction`, and `AllowWrites` go to the
maintenance runtime. Both runtimes see the lifecycle commands (`CreateInstance`,
`UpdateConfiguration`, `InitializationComplete`). Each runtime keeps its own
worker-0 broadcast (`compute/src/command_channel.rs`), so ordering within a
runtime is preserved.

**Response merging.** The process merges responses from both runtimes into the
single stream the controller expects. `PeekResponse`s originate in the query
runtime, `Frontiers` for maintained collections in the maintenance runtime. This
extends the existing per-process `PartitionedComputeState` merge
(`compute-client/src/service.rs:79`) to span two runtimes rather than only the
workers of one.

## Compaction reconciliation

This is the subtlest correctness point, and it differs by stage.

**Stage 1.** No reconciliation. There is one runtime and one trace. The
controller drives `since` through `AllowCompaction` exactly as today. The
offloaded read is protected by the controller's per-peek read hold plus the
snapshot's immutable batches.

**Stage 2.** Two things now hold a maintenance arrangement back from compaction:

1. The controller, through `AllowCompaction` to the maintenance runtime, derived
   from its read holds (`instance.rs:1922-1981`).
2. The query runtime, through the primitive's hold aggregation: an imported
   arrangement registers logical and physical holds that the publisher forwards
   to the maintenance `TraceAgent`.

These must not fight. The controller remains the authority: it already holds a
read capability for the query being served (a peek hold, or the read hold behind
a slow-path dataflow's `as_of`). The query runtime's holds are a *finer-grained,
in-process realization* of that same requirement. The design keeps them
consistent by construction:

- The query runtime never needs a hold *below* what the controller has already
  guaranteed. It imports at an `as_of` the controller vouched for, so its holds
  sit at or above the controller's `since`.
- The maintenance runtime's `TraceAgent` receives the *meet* of the controller's
  `AllowCompaction` frontier and the query runtime's forwarded holds. Because
  compaction only advances, the effective `since` is the lower of the two, which
  is always `<=` any time either party still needs.

The open risk is a query-runtime hold that lags the controller: if the query
runtime wedges while holding a hold, it pins maintenance compaction. That is the
lease-expiry question the primitive design already flags, surfaced here as a
concrete operational concern (see open questions).

## Controller transparency

Two options for how much the controller knows about the split.

- **Transparent.** The controller still sees one replica and one response
  stream. The process routes commands and merges responses internally. Minimal
  protocol change. This is the recommended starting point, and it is enough for
  both stages: the split is an internal replica detail, and the controller's
  compaction authority is unchanged because `AllowCompaction` still targets the
  maintenance runtime's traces.
- **Explicit.** The controller models two runtimes and routes to them directly,
  enabling smarter placement and per-runtime introspection. This is a real
  protocol and controller change (`ReplicaState` would grow a sub-runtime axis,
  `instance.rs:3082-3097`). Deferred until transparency proves insufficient.

The recommendation is transparent for both stages. The cost is that peek routing
and per-runtime memory attribution are hidden from the controller, which
introspection will eventually want (open question).

## Resource sharing and the single-instance assumption

Two compute runtimes in one process share the per-process resources the
storage/compute split already shares: the persist client cache, tracing handle,
and metrics registry (`compute_state.rs:114-116`, `background.md`). Three
process-global assumptions must be reconciled, since they are written for "a
replica process hosts a single instance" (`compute_state.rs:495-496`):

- `mz_row_spine::DICTIONARY_COMPRESSION` (`compute_state.rs:497`): a process
  global. Both runtimes must agree on it, or it must move to per-runtime state.
- lgalloc and pager configuration (`compute_state.rs:255-361`): initialized once
  per process. The second runtime must not re-initialize it.
- Metrics (`compute_state.rs:538-540`): already shared with storage, so the
  pattern exists, but per-runtime labels are needed to tell maintenance and query
  metrics apart.

## Thread allocation

Two runtimes of `N` workers each put `2N` worker threads on the same cores. The
query runtime is mostly blocked waiting on reads and imports, so it is not `N`
cores of steady load, but the design must not silently double the thread count
and oversubscribe. Options (thread split, smaller query pool, cooperative
yielding) are a scheduling decision left to the open questions, not fixed here.
Stage 1 avoids this entirely (one runtime plus a bounded reader pool).

## Open questions

- **Thread and core allocation between runtimes.** How many worker threads does
  the query runtime get, and how are cores shared with maintenance? Needs
  measurement.
- **Lease expiry for query-runtime holds (stage 2).** A wedged query runtime
  pins maintenance compaction through its forwarded holds. Do we want a
  time-based lease with snapshot invalidation, or is same-process trust enough?
  The primitive design assumes trust for now.
- **Import queue backpressure (stage 2).** The primitive's replay queue is
  bounded today only because producer and consumer share a worker step. Across
  runtimes they are independently scheduled, so a lagging query runtime can grow
  the queue without bound. A bound plus publisher backpressure is likely needed
  before long-lived query dataflows are safe. Captured in the primitive design.
- **Introspection and memory attribution.** Under transparency the controller
  cannot see which runtime holds what memory. Shared arrangements are counted by
  whoever drops last. Introspection needs a per-runtime view eventually.
- **Slow-path versus stage 1.** Stage 1 offloads fast-path index peeks but not
  slow-path peeks. Is it worth teaching the coordinator to prefer the fast path
  more aggressively once stage 1 lands, deferring stage 2?
- **Failure and restart.** If the query runtime dies, peeks in flight fail and
  should be retried on maintenance as a fallback. If maintenance dies, the whole
  replica restarts as today. The fallback path needs specifying.

## Alternatives

- **Priority scheduling inside one runtime.** Yield more finely so peeks can
  interleave with maintenance. Timely scheduling is cooperative per worker, so a
  peek behind a long operator step cannot preempt it, and this couples
  maintenance throughput to read latency permanently. It also does nothing for
  query dataflow rendering. Stage 1's offload achieves read isolation without
  this coupling.
- **Serve all peeks from Persist.** Peeks can read persist shards directly
  (`PeekTarget::Persist`), bypassing arrangements. This already exists for some
  peeks, but it loses the in-memory arrangement's latency and freshness and does
  not help index-backed reads that must reflect the maintained arrangement.
- **A second process instead of a second runtime.** Isolates reads fully but
  cannot share arrangements in memory, so it would copy data across the process
  boundary, which is what persist already offers. In-process sharing is the whole
  point.
- **Do only stage 1, never stage 2.** Viable if fast-path peek isolation turns
  out to cover the observed latency problem. Stage 2 is justified only if ad-hoc
  query dataflow rendering is shown to contend materially with maintenance.
