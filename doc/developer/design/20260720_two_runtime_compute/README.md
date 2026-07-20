# Two-runtime compute: isolating reads from maintenance

- Associated:
  - [background.md](./background.md): how a compute replica serves maintenance
    and reads today (the status-quo baseline this design departs from).
  - [Shared arrangements across timely runtimes](../20260719_shared_arrangements_across_runtimes/README.md):
    the differential-level primitive this design consumes.
  - Implementation of the `Arc`-batches prerequisite: differential
    TimelyDataflow/differential-dataflow#807, materialize
    MaterializeInc/materialize#37743. File and line references to the `Arc`
    spines below are to the state on those branches, not to `main`.

## The Problem

A compute replica does two very different kinds of work on the same timely
worker threads. It maintains indexes and materialized views, which is CPU-bound
and bursty (arrangement merges, operator work proportional to input volume). And
it serves reads: peeks against arrangements, and the short-lived dataflows
behind ad-hoc queries.

These contend. An index peek runs its cursor walk synchronously on the worker
thread (`compute/src/compute_state.rs:982-988`, driven from the loop at
`compute/src/server.rs:417`). The worker only reaches peek servicing after it
returns from `step_or_park`, so a peek waits for the worker that owns the shard
to come back around its loop. That worker may be deep in a join or a large
merge, and timely operators run to completion. The result is read tail latency
proportional to how busy maintenance is, which is exactly when users are
watching. Persist peeks already sidestep this by offloading to an async task
(`compute_state.rs:1254-1325`). Index peeks, the common fast path, do not.

The root cause is that reads and maintenance share worker threads, and an
arrangement is readable only from the worker that maintains it. The `Arc`-batches
work removes the second half of that restriction: arrangement batches become
`Arc`'d and their contents are asserted `Send + Sync` in `mz-row-spine` (added
by the Arc migration, #37743), so a batch can be read from another thread. This
design uses that to serve reads from a different set of threads than the ones
doing maintenance.

## Success Criteria

- A read is served by threads other than the maintenance worker threads. Its
  latency floor is CPU contention with maintenance, not waiting for a
  maintenance worker to return to its command loop. The read no longer queues
  behind a long maintenance operator step.
- Correctness is unchanged. A read observes exactly the collection at its
  timestamp, gated by the same `since <= timestamp < upper` window as today.
- The controller remains the single authority on compaction. No arrangement
  compacts past a time a read still needs, and the read path never pins an
  arrangement back beyond what the controller already holds.
- Maintenance throughput is not regressed. Publishing an arrangement for reading
  costs the maintenance path only constant per-batch overhead, and the read side
  does not hold maintenance compaction back.

Non-criterion, stated to head off a misreading: this does not make an individual
peek's cursor walk faster. It removes the walk, and the wait to be serviced,
from the maintenance worker's critical path. A tiny point-lookup that was
microseconds inline is still microseconds, now paid on an interactive thread
that was free to run it immediately instead of after the maintenance worker's
current step.

## Out of Scope

- Cross-process sharing. Everything here is threads within one replica process.
  Cross-process is persist's job.
- Changing the fast-path versus slow-path peek decision, which lives in the
  coordinator (`adapter/src/coord/peek.rs`). This design changes only where the
  read executes on the replica, not how the coordinator plans it.
- Core-sharing policy between the two runtimes (pinning, priorities,
  oversubscription). Worker counts are fixed by this design. How the resulting
  threads share cores is deferred to measurement (see open questions).
- Multi-replica or cross-replica concerns. This is about one replica's internal
  structure.

## Architecture

One replica process runs two compute timely runtimes, two `serve`/`ClusterSpec`
instances (`compute/src/server.rs:85-131`) sharing per-process resources exactly
as storage and compute already do today (`clusterd/src/lib.rs`, `background.md`):

- The **maintenance runtime** owns the durable, maintained work: indexes,
  materialized views, and subscribes. It renders and maintains their
  arrangements as today, and additionally *publishes* each index arrangement so
  other threads can read it.
- The **interactive runtime** owns the ephemeral, read-oriented work: peeks and
  temporary dataflows. It reads maintenance arrangements through the published
  handles, never touching a maintenance worker thread.

Reads are served on the interactive side by threads that hold the published
arrangement directly and only contend for CPU with maintenance. That is the
whole point: a peek does not wait for a maintenance worker to come back around
its loop, because the thread serving it is not a maintenance worker.

### Serving a peek

A fast-path index peek is served by taking a consistent snapshot of the
published arrangement and running the existing cursor walk against it, on an
interactive thread:

1. The routing layer delivers the `Peek` to the interactive side without going
   through a maintenance worker's command loop.
2. The interactive side looks up the published handle for the target
   `GlobalId`, gates on the published `upper`/`since`, and takes a `Send`
   snapshot: a clone of the current batch chain (a handful of `Arc` clones) plus
   the frontiers.
3. It runs `PeekResultIterator` (`compute/src/compute_state/peek_result_iterator.rs`)
   over the snapshot and produces the `PeekResponse`.

The snapshot's `Arc`'d batches are immutable, so once taken, the maintenance
runtime may merge and compact freely while the walk proceeds. No lock is held
across the walk, and there is no torn read.

Whether the walk runs on the interactive runtime's timely workers or on a
dedicated reader pool alongside it is an implementation choice. A dedicated pool
isolates peeks even from the interactive runtime's own temporary-dataflow
rendering, which matches the "a thread that just grabs the trace and runs the
peek" model most directly. Either way the walk is off the maintenance threads.

### Serving a temporary dataflow

A slow-path peek or other ad-hoc query is rendered as a temporary dataflow on
the interactive runtime. It imports the maintenance arrangements it needs as
ordinary `Arranged` collections through `SharedTraceHandle::import`, then runs
joins, reduces, and the rest with no maintenance-thread involvement. Its result
is peeked and the dataflow is dropped, as transient dataflows are today.

## What decouples, and what does not

The latency win is real and comes from the serving thread not being a
maintenance worker. A peek at a timestamp at or below the arrangement's
published `upper`, which is the common case, is served immediately, bounded only
by CPU availability. It does not queue behind a maintenance operator step.

The one residual dependency is frontier freshness, and it is not new in kind. A
peek at a timestamp *beyond* the published `upper` must wait for the maintenance
runtime to publish a newer `upper`. Publication happens on the maintenance
worker's activation and is cheap (a chain refresh plus a frontier update, no
per-record work), so it advances whenever the worker steps, independently of
whether the worker is free to do a full walk. This is the same kind of wait a
read at the frontier edge already incurs today (waiting for the trace `upper` to
advance), not a wait for the maintenance worker to be free to *serve* the read.
So fresh-edge reads are gated on maintenance frontier progress as before, and
everything at or behind the published frontier is fully decoupled.

## Memory

A snapshot pins the specific batches it captured until the read finishes. While
it is held, the maintenance runtime may merge those batches into a new one, so
the pre-merge inputs (pinned by the reader) and the merged output (in the live
trace) coexist for the read's duration. This is bounded by the arrangement size
and is transient, and it is the intended cost of letting a reader hold a
consistent view while the writer compacts.

It is not a new doubling. A fueled merge already holds both its input and output
batches in memory while the merge is in progress, independent of any reader. A
held snapshot extends the lifetime of its captured batches, it does not create a
second copy of the whole arrangement.

## Routing and response merging

The controller sees one replica behind one protocol endpoint. A **new
process-level multiplexer** sits between the controller connection and the two
runtimes. It is a genuinely new component, not a reuse of the existing
`PartitionedComputeState` (`compute-client/src/service.rs:79`), which merges
*homogeneous* partitions (all workers of one runtime, waiting for every part and
taking a meet). The two runtimes are *heterogeneous*: each collection id lives on
exactly one runtime, and a peek is answered by exactly one runtime. The
multiplexer therefore does ownership-based demux and pass-through, not a meet or
an all-parts union.

**Routing by target id.** The multiplexer dispatches on the id a command
targets:

- `Peek`, `CancelPeek`, and `CreateDataflow` for a temporary dataflow go to the
  interactive side. A temporary dataflow exports only `GlobalId::Transient`
  (`compute-types/src/dataflows.rs:380-383`).
- `CreateDataflow` for maintained work (indexes, MVs, subscribes), `Schedule`,
  `AllowWrites`, and `AllowCompaction` for a maintained id go to the maintenance
  runtime.
- `AllowCompaction` (including the empty-frontier drop) for a transient id goes
  to the interactive side that owns that temporary dataflow.
- Lifecycle commands (`CreateInstance`, `UpdateConfiguration`,
  `InitializationComplete`) go to both.

Note that routing is not literally "by the target id" for every command. A
fast-path `Peek` names a maintenance-owned index yet must be served on the
interactive side, and `CancelPeek` keys by `uuid`. So the multiplexer learns
transient-id ownership by observing `CreateDataflow`, and treats peeks as
interactive by command type. The one subtlety is that subscribes also export
transient ids but must stay on the maintenance runtime, since they are
continuously maintained rather than read once. A subscribe is distinguished by
carrying a subscribe sink in its `DataflowDescription`. The predicate is a
bespoke policy, "maintained work to maintenance, one-shot reads to interactive",
not a one-line id check.

**Response merging.** The multiplexer merges both runtimes' responses into the
one stream the controller expects. `PeekResponse`s originate on the interactive
side, `Frontiers` for maintained collections on the maintenance runtime. Its
combining contract is its own (pass a peek response through from whichever side
produced it, report maintained frontiers from maintenance), which is why it
cannot simply reuse `PartitionedComputeState`.

## Publication and the publish-versus-read handshake

When the maintenance runtime renders an index (`export_index`,
`compute/src/render.rs:688`), it publishes the arrangement and records the
resulting `SharedTraceHandle`s in a **per-process publication registry** keyed by
`GlobalId`, one entry per worker ordinal. This registry is a new first-class
per-process object, shared into both runtimes the same way the persist client
cache is shared today (created once, handed to both `serve` calls,
`compute_state.rs:114-116`). Interactive worker `i` finds maintenance worker
`i`'s handle. Publication cost is one `Arc` clone plus a mutex push per batch.

The two runtimes step independently, so there is no happens-before between
"maintenance renders and publishes index X" and "interactive is asked to read
X". In steady state the controller only issues a peek after a `Frontiers`
response proves the index rendered, which implies it published. But on
reconciliation or replica restart the controller replays `CreateDataflow` and
`Peek` from history, and a peek can reach the interactive side before the
maintenance runtime has re-rendered and re-published. The design therefore
requires an explicit readiness rule: a read for an unregistered `GlobalId`
**blocks** until the publication point appears, woken when it registers, rather
than erroring or reading an empty snapshot as if valid. This block-until-
published handshake is new machinery the primitive does not provide today.

## Compaction: controller authority and required primitive changes

The controller stays the sole compaction authority. The maintenance runtime
drives an arrangement's `since` from `AllowCompaction` exactly as today, derived
from the controller's read holds (`instance.rs:1922-1981`). The read side must
never pin a maintenance arrangement back below that frontier.

Correctness of a read rests on three things, and only the first exists today:

1. The controller already holds a maintenance arrangement back for as long as a
   read needs it. A peek carries a read hold at its timestamp, and a temporary
   dataflow is created behind a read hold at its `as_of`. Command reordering
   across the two runtimes cannot defeat this, because the controller gates the
   *emission* of `AllowCompaction`, not its delivery: it never emits a frontier
   past a time it is still holding. So while the read is outstanding, the
   maintenance `since` cannot advance past the read timestamp.
2. The read's snapshot pins immutable batches, so even after the controller
   releases and maintenance compacts, an in-progress walk is unaffected.
3. A `since <= timestamp` gate on the read path, so a read at a time the
   arrangement has already compacted past returns the same error it does today
   (`compute_state.rs:1536-1544`) rather than a silently coalesced result.

**Two changes to the shared-arrangements primitive are required for this to
hold, and the primitive does not do them yet.** Both were found by adversarial
review of `sharing.rs` and are the gating work for this design:

- **The publisher must not pin compaction.** As written, the publisher holds a
  `TraceAgent` clone whose logical/physical hold is sourced from its own
  `get_logical_compaction` and never advances (`sharing.rs:412,462`), so merely
  publishing an index freezes its compaction at publish-time `since` for the life
  of the index. The publisher's liveness hold must instead track the maintenance
  runtime's controller-driven compaction frontier, so publishing does not hold
  the trace back at all.
- **Reads must register no independent hold.** `import` and handle registration
  install real holds that the publisher forwards to the maintenance trace
  (`sharing.rs:169-170,229-230,435-463`), so a wedged interactive reader would
  pin maintenance compaction. The integration needs a read mode that registers
  no hold and relies solely on the controller's hold (criterion 1 above). Only
  then is it true that a wedged interactive runtime cannot pin maintenance.

- **`snapshot_at` must enforce the `since <= timestamp` gate** (criterion 3). It
  currently waits only for `upper` to pass the time and returns whatever `since`
  the snapshot has (`sharing.rs:185-203`), so it could serve stale rows once
  compaction actually moves. The gate must be added on the read path.

With those changes, the read side is purely a reader: it holds nothing back, the
controller remains the sole authority, and a stuck interactive thread can fall
behind on its own reads but cannot wedge maintenance compaction. Without them,
the "interactive never pins maintenance" property this design depends on is
false.

## Internal to compute, one protocol endpoint

The split is invisible to the controller, by choice. Teaching the controller
about two runtimes (an explicit sub-runtime axis in `ReplicaState`,
`instance.rs:3082-3097`) would enable smarter placement and per-runtime
introspection, but it is a real protocol and controller change and is not needed
for the mechanism to work. The cost of staying internal is that peek routing and
per-runtime memory attribution are not visible to the controller, which
introspection will eventually want (see open questions).

## Equal peers is a hard requirement

Co-location of an imported arrangement with interactive-side operators relies on
both runtimes routing a key to the same worker ordinal, which holds only if they
have the same total peer count. Peer count is `workers_per_process *
num_processes` (`compute/src/extensions/arrange.rs`), so both factors must match
between the two compute runtimes, not just workers-per-process. This is stronger
than the storage-versus-compute worker-count assertion that exists today
(`clusterd/src/lib.rs:426-429`), which does not cover it. The design requires a
hard assertion of equal peers at replica configuration, and the primitive's
`import` should assert matching peers at import time (it does not today, contrary
to an earlier claim in the primitive design).

## Resource sharing and the single-instance assumption

Two compute runtimes in one process share the per-process resources the
storage/compute split already shares: the persist client cache, tracing handle,
and metrics registry (`compute_state.rs:114-116`). Three process-global
assumptions must be reconciled, since they are written for "a replica process
hosts a single instance" (`compute_state.rs:495-496`):

- `mz_row_spine::DICTIONARY_COMPRESSION` (`compute_state.rs:497`): a process
  global both runtimes would write. They must agree, or it must move to
  per-runtime state.
- lgalloc and pager configuration (`compute_state.rs:255-361`): initialized once
  per process. The second runtime must not re-initialize it.
- Metrics (`compute_state.rs:538-540`): already shared with storage, so the
  pattern exists, but per-runtime labels are needed to tell maintenance and
  interactive metrics apart.

## Failure model

The two runtimes share fate, exactly as the replica process behaves today. They
live in one process, so a fatal error in either brings the process down and the
replica restarts as a whole. There is no partial-failure path, no fallback that
reroutes interactive reads onto the maintenance runtime, and no independent
restart of one runtime. A read in flight when the process dies fails with the
process and is retried by the controller against the restarted replica, which is
the existing behavior.

Shared fate is a requirement, not just a convenience: it is what lets the read
side hold nothing and rely on the controller. For it to hold, a panic on any
worker or reader thread of either runtime must abort the whole process. The
implementation must confirm the panic path is abort-equivalent for both
runtimes. If a single thread could panic without taking the process down, a
wedged reader's state and a half-served peek would leak, and that fallback path
would need its own correctness argument.

## New components this design requires

Called out explicitly, because the mechanism does not reduce to existing objects:

- The process-level command/response multiplexer over two runtimes (not
  `PartitionedComputeState`).
- The per-process publication registry keyed by `GlobalId`, plus the
  block-until-published handshake.
- An interactive read path that gates and walks a `SharedTraceHandle` snapshot
  (a new `PendingPeek` variant or reader-pool equivalent), since today's
  `handle_peek` reads only the local `TraceManager` (`compute_state.rs:673-699`).
- The three primitive changes in "Compaction": publisher-does-not-pin, no-hold
  reads, and the `snapshot_at` since gate.
- A compile-time `Send` assertion over the peek walk. `peek_stash.rs:44-48`
  documents that `PeekResultIterator` is `!Send` today because the trace reader
  is `Rc`. The `Arc` migration should flip it, but the design depends on it, so
  it must be asserted, including the batch cursor and the columnation containers,
  not just the batch wrapper.

## Open questions

- **Core sharing between runtimes.** Worker counts are fixed (equal peers). How
  the resulting threads share cores, and whether oversubscription is acceptable
  given the interactive side is often blocked on reads, is deferred to
  measurement.
- **Import queue backpressure.** The primitive's replay queue is bounded today
  only because producer and consumer share a worker step. Across runtimes they
  are independently scheduled, so a lagging interactive runtime can grow the
  queue without bound. A bound plus publisher backpressure is likely needed.
  This is a liveness and memory concern, not a compaction one: with the no-hold
  read mode, a slow reader falls behind on its own imports but cannot wedge
  maintenance compaction. Captured in the primitive design.
- **Introspection and memory attribution.** Under one endpoint the controller
  cannot see which runtime holds what memory, and a shared arrangement reachable
  from both runtimes' arrangement-size loggers can be double-counted or
  misattributed depending on drop order. Per-replica memory relations
  (`mz_arrangement_sizes` and friends) need a per-runtime view eventually.
- **Cancellation and exactly-one response.** With the walk off the maintenance
  thread, a `CancelPeek` can race a completing read. The merge point on the
  interactive side must guarantee exactly one `PeekResponse`, including for
  point-lookups that produce no intermediate batch at which to observe the
  cancel.
- **`batches_through` straddle handling.** The primitive's `batches_through`
  includes a batch that straddles the requested cut rather than panicking as the
  spine does (`sharing.rs:258-276`). Confirm every `cursor_through` frontier that
  reaches a `SharedTraceHandle` is batch-aligned, or restore a fail-stop, so an
  import cannot silently return updates past the cut.

## Alternatives

- **Priority scheduling inside one runtime.** Yield more finely so peeks
  interleave with maintenance. Timely scheduling is cooperative per worker, so a
  peek behind a long operator step cannot preempt it, and this couples
  maintenance throughput to read latency permanently. It also does nothing for
  temporary-dataflow rendering.
- **Offload only the walk, keeping the snapshot on the maintenance worker.** A
  smaller change: the maintenance worker gates and snapshots on its own loop,
  then hands the walk to a pool. Rejected, because the worker must still come
  around its loop to take the snapshot, so the read still queues behind a long
  maintenance step. It removes the walk cost but not the wait to be serviced,
  which is the dominant tail. Serving the read from an interactive thread that
  holds the published handle avoids both.
- **Serve all peeks from Persist.** Peeks can read persist shards directly
  (`PeekTarget::Persist`), bypassing arrangements. This exists for some peeks,
  but it loses the in-memory arrangement's latency and freshness and does not
  help index-backed reads that must reflect the maintained arrangement.
- **A second process instead of a second runtime.** Isolates reads fully but
  cannot share arrangements in memory, so it copies data across the process
  boundary, which is what persist already offers. In-process sharing is the
  point.
