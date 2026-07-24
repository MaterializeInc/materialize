# Two-runtime read isolation

## Summary

A compute replica can run a second, in-process "interactive" timely runtime that
serves latency-sensitive reads directly off the arrangements the "maintenance"
runtime builds. A read no longer waits behind a long, run-to-completion
maintenance operator step, and introspection stays answerable while the
maintenance runtime is hydrating. The two runtimes share the process and its
memory. The interactive runtime reads the maintained arrangements zero-copy
through a per-process sharing registry, so nothing is re-materialized.

The feature is gated by the `ENABLE_TWO_RUNTIME_COMPUTE` dyncfg, off in
production and on by default in CI. With the dyncfg off, a replica runs a single
`Solo` runtime that is behaviorally identical to a deployment from before this
work existed.

This document is the single design of record. It supersedes the four planning
and design documents that preceded it in this directory (see [Implementation
history](#implementation-history)).

## Motivation

Reads and index maintenance compete inside a single timely runtime. Timely does
not preempt a running operator, so a maintenance operator that runs to
completion over a large input blocks any read interleaved on the same worker.
The read waits, not because the machine is out of CPU, but because the one run
loop is busy and cannot be interrupted.

The sharpest form of the problem is introspection. `mz_introspection` and the
logging dataflows describe a replica's own dataflow state, so they cannot be
served from any other replica. They are exactly what an operator reaches for
during hydration or a burst of batchy work, which is precisely when the
maintenance runtime is pinned and the introspection read blocks. Today we fly
dark at the moment we most need to see.

## Why this architecture

This is a deliberate architectural commitment, not an isolated feature. It is
close to a one-way door (see [The commitment](#the-commitment)), so the rationale
matters as much as the mechanism.

### The thesis is separation of concerns

Two runtimes do not add CPU and do not magically isolate reads. Both runtimes
share the same cores. The second runtime buys one precise thing, a separate,
OS-preemptible run loop, so an interactive read is not trapped behind a
run-to-completion maintenance step.

The right frame is separation of concerns:

* The maintenance runtime stays a pure run-to-completion batch engine. Operators
  consume their inputs fully. The only sanctioned yield is before an exchange
  edge, to let downstream operators reduce memory. Yielding for interactivity is
  an anti-pattern we do not want in that runtime.
* The interactive runtime is a pure, preemptible, low-latency reader over the
  maintained arrangements.

A single runtime cannot be both without compromising one of them. Two runtimes
let each be pure.

### Why not a read replica

Introspection cannot be offloaded. A replica's introspection describes that
replica, so a second replica cannot answer the first replica's introspection
reads. Only an in-process second runtime can keep introspection answerable while
maintenance is busy.

Separately, replicas in the fleet mostly redline on memory, not CPU. A second
replica doubles the binding resource, because it maintains its own copy of every
arrangement. The sharing approach here duplicates no arrangement memory. And
because those replicas are memory-bound, they usually have spare CPU, which is
exactly the headroom the interactive runtime needs. The CPU-saturated case,
where a second runtime helps least, is not the common one.

### Why not yield for interactivity in one runtime

Making the maintenance runtime yield finely so reads interleave would violate its
core contract. Run-to-completion is what lets an operator consume its inputs and
consolidate. The only yield we want in maintenance is the pre-exchange,
memory-reduction yield. Yielding for interactivity would degrade the maintenance
runtime to buy latency it should not be responsible for.

### What it does not buy

It does not add CPU. On a CPU-saturated box the interactive runtime cannot get a
core either, and reads back up just as they would in a single runtime. The
benefit is real but conditional on CPU headroom. A benchmark that pins every
core with synthetic churn models a CPU-bound box and understates the feature,
because the fleet is memory-bound with spare CPU.

It also does not touch the control plane. Peeks still serialize behind DDL on the
single coordinator thread. For non-introspection reads under load that control
plane can be the first-order bottleneck, and this work is necessary but not
sufficient there. See [Known limitations](#known-limitations-and-follow-ups).

### The commitment

Two properties make this close to irreversible.

* The off switch is not a clean exit. `Solo` keeps single-runtime deployments
  byte-identical, but once users depend on the isolated low-latency reads,
  turning the feature off is a visible query-latency regression, not a no-op.
* The capability we lean on will atrophy. Once reads live in the interactive
  runtime, the maintenance runtime no longer needs to accommodate interactivity
  at all, and it will be built to be maximally batchy because it was freed to be.
  Single-runtime interactive-read behavior rots from disuse and hardened
  assumptions. Recovering it later is a rebuild, not a revert.

That irreversibility is acceptable only because the end-state, maintenance as a
pure batch runtime and interactive as a pure reader, is the architecture we would
choose deliberately given the points above. The bar for adopting this is
therefore "we would design it this way on purpose," not "we can back out."

## Design principles

Three principles govern the protocol between the runtimes.

1. **Build on a correct protocol, and panic outside it.** Compute trusts the
   controller's read-hold discipline. A maintained arrangement is dropped only
   after every reader has completed, so an import never outlives the arrangement
   it reads. There is no cross-runtime lease and no refcount. A panic on any
   worker or reader thread of either runtime takes down the whole process, which
   is correct: the two runtimes share fate, and there is nothing to isolate.
2. **The multiplexer splits one endpoint into two well-defined sub-protocols.**
   The maintenance sub-protocol is the ordinary compute protocol. The interactive
   sub-protocol is a variant whose index imports are shared registry imports, a
   self-describing import kind that references a maintained id without a prior
   local `CreateDataflow`.
3. **Deterministic construction.** Timely allocates exchange-channel identifiers
   from a per-worker, construction-order counter, so every worker must build
   dataflows in the same order. We render in command arrival order and never
   reorder or defer a build. An import that depends on a not-yet-published
   arrangement binds a real but empty publication point at construction time and
   is filled in place later.

A fourth, structural fact underlies the whole design: **sharing is per-process.**
The shared batches are `Arc`-backed in memory, so the interactive runtime reads
only the maintenance arrangements published in that same process.

## The bounded-read boundary

The interactive runtime serves a read only when it is bounded, meaning its
`until` is a finite frontier, it is not a `SUBSCRIBE`, and it is not a
`COPY TO`. Everything else runs on the maintenance runtime.

The routing predicate keys on `until`-finiteness, not on whether the target id is
transient. A maintained index has an unbounded `until` and so always lands on
maintenance regardless of catalog transience. `COPY TO` is bounded but still
excluded, for reconciliation and S3-sink reasons rather than frontier reasons. A
mixed or non-homogeneous dataflow is treated as maintained by construction, which
is the safe default.

## The sharing primitive

The cross-runtime sharing primitive lives entirely in Materialize. It builds
against a released differential-dataflow, with no fork and no `[patch.crates-io]`.

* `mz_row_spine::ArcBatch` is a local newtype around `Arc<B>` that carries the
  differential batch traits, so a batch whose contents are `Send + Sync` can be
  read from a thread other than the one maintaining the trace. The orphan rule
  forbids the blanket `impl Trait for Arc<B>` in Materialize, which is why the
  newtype exists rather than a bare `Arc<B>`.
* `mz_compute::shared_trace` holds the primitive proper: `Published`,
  `SharedTraceHandle`, `SharedTrace`, the `PublishArrangement` extension trait,
  and `import_snapshot_at`.

An earlier prototype consumed these from a differential-dataflow fork. The only
capability that kept the fork alive was reading `agent.trace_box_unstable()`, an
upstream API documented as unstable and undefined behavior to mutate, to compute
a compaction floor. Materialize already has authoritative sources for that
information, so the read was replaced (see [Compaction](#compaction)) and the fork
was dropped.

### Placeholder and adopt

A publication point is an `Arc<SharedTrace>`. It can be created empty as a
placeholder and later adopted by a publisher in place, filling the same `Arc`.
This is what makes arrival-order construction work. A differential import captures
its input trace by value at construction time, so the import must have a real
trace to hold even before the arrangement it reads exists. `Published::placeholder`
gives it one. A later `PublishArrangement::adopt` installs the real publisher into
that same point, and the by-value handle observes the fill because it is a live
proxy into the shared state, not a snapshot.

The `TraceAgent` that writes the arrangement lives in the publisher's sink
closure, not in `SharedTrace`, so the writer is decoupled from the shared state.

Placeholder frontiers are `Antichain::from_elem(Timestamp::minimum())`, never
`Antichain::new()`. The empty antichain reads as sealed through the end of time,
which would make every snapshot wait vacuously true and return empty results.

### Single-sourced replay feed

The publisher replays batches and frontiers to importers from a single
authoritative source. The hazard it avoids: the trace's `map_batches` upper can
run ahead of the arrangement stream within a worker step, so splicing batches
from the stream with frontiers from the trace can enqueue a `Frontier(upper)`
ahead of a `Batch` whose time is below it, which makes the importer's delayed
capability panic. Feeding a batch and the frontier that closes it from one source
keeps them mutually ordered. For the same reason the replay is incremental rather
than a one-shot dump of the whole chain under a single capability, which would be
the record-doubling bug.

### Compaction

Publishing carries no independent compaction floor. In Materialize the controller
drives `since` through the maintained trace's own handle. Only a live importer's
registered hold may hold the shared view back, and it releases on drop. The
publisher keeps a holding agent solely so importer holds have somewhere to forward
to, so that hold must follow the writer rather than pin the trace.

The publisher takes its writer-driven floors from sources Materialize already has:

* Logical compaction comes from the controller's `AllowCompaction` frontier,
  forwarded into the published slot by
  `ArrangementSharingRegistry::note_allow_compaction`, which
  `compute_state::handle_allow_compaction` calls alongside the local
  `TraceManager` update. `SharedTraceState.writer_logical` holds it, seeded
  `None` so the publisher falls back to its own current hold, the dataflow
  `as_of`, before the first command arrives.
* Physical compaction follows the stream `upper`, mirroring
  `TraceManager::maintenance`, which sets physical compaction to the trace upper
  to enable batch merging.

With no reader hold on a dimension, the target follows that writer floor, so with
zero readers compaction follows the writer. The published `since` is the meet of
the publisher's post-forward hold and the writer floor, which keeps a registering
reader from latching an anti-conservative `since` that claims accuracy at
already-merged times. An index publishes two independent arrangements, so
readiness and `since` gating operate on `meet(oks, errs)`.

### Bounded import, not live replay

`SharedTraceHandle::import_snapshot_at` imports the shared arrangement as a static
snapshot at `as_of`, bounded by `until`. The interactive runtime only ever answers
bounded reads, so a live-following import would track the source's live frontier,
gain nothing over the maintenance seal rate, and still consume interactive-lane
resources. The unbounded live `import` was dead code and was removed.

Import is pairwise: importer worker `i` reads publisher worker `i`. That is sound
only when both sides shard keys the same way, `key.hashed() % peers`, with equal
total peers, so `import_snapshot_at` asserts equal peers and panics otherwise. It
also asserts `since <= as_of`: a published slot whose `since` already sits above
the requested `as_of` means the controller offered an unreadable `as_of`, a
protocol error, and the import must panic rather than silently read coalesced
data.

## The registry

`ArrangementSharingRegistry` (`src/compute/src/sharing.rs`) maps a `GlobalId` to a
per-worker slot holding the published `oks` and `errs` points.

* **Get-or-create is symmetric.** Whichever runtime touches an id first creates
  its publication point. Maintenance-first creates the point and adopts it.
  Interactive-first creates a placeholder and builds a live import over it, which
  a later maintenance adopt fills in place rather than overwriting.
* **Notification-driven, no polling.** Each interactive worker registers a
  coalescing `SyncActivator` and a dirty-id inbox. A read whose dependency is not
  yet published or sealed is enqueued, not blocked. Publication (`insert`) and
  seal (`note_frontier`, fired from the maintenance export's frontier probe on
  both the `oks` and `errs` streams) mark the id dirty and wake the worker, which
  re-examines only the affected pending work. The `map` and `wakers` locks are
  independent, and the lost-wakeup argument that lets them stay separate is a
  map-lock total order plus drain-before-reread plus a sticky activation token.
  The per-step `process_peeks` scan is removed on the interactive runtime.
* **Two closes, no withdrawal command.** An adopted point closes when its
  publisher drops. A never-adopted placeholder is evicted when its last reader
  leaves. So no explicit withdrawal command is needed. Eviction reads the slot's
  adoption flag and its `Arc` strong count under one lock so an adopter in flight
  is always observed as either adopted or still-referenced.
* **Re-exports.** A `Trace` re-export, where one index aliases another's
  arrangement, shares the existing `Arc` under the new id rather than
  republishing. The source's seal signal wakes the re-export transitively.

## The interactive serving path

The interactive runtime serves everything through the registry.

* **Fast-path index peeks** read the published arrangement directly, served inline
  on the interactive runtime's own worker step (`PendingPeek::IndexShared`). An
  earlier plan offloaded the arrangement walk to a tokio task off a maintenance
  worker. That was abandoned: once reads live in a separate runtime, the runtime
  itself is the isolation mechanism, and no async hand-off is needed.
* **Slow-path query dataflows** import the maintenance arrangements as real
  `ArrangementFlavor::SharedTrace` arrangements and render joins and reduces over
  them. Importing as a real arrangement, not a substituted collection, is
  required for correctness. Downstream operators, delta joins especially, are
  rendered assuming the arrangement, its key, and its permutation exist.
  Substituting a collection loses that contract.
* **Transient outputs are republished.** A query's own transient output is
  published into the registry, so its result peek is served the same way as any
  other read.
* **Deferred dataflows.** A query dataflow whose imported dependencies are not yet
  published is deferred, not built, and built via the unchanged path once all
  dependencies publish.

## The multiplexer

`src/compute-client/src/multiplex.rs` presents one controller endpoint over the
two runtimes.

* It routes peeks and one-shot work to interactive, maintained work to
  maintenance, and lifecycle commands to both.
* It emits exactly one `PeekResponse` per uuid. A cancel and a completion can race
  for the same peek, so the first terminal response wins and any later duplicate
  is dropped. State: `live_peeks` tracks in-flight uuids.
* It forwards each collection's `Frontiers` only from the runtime that owns the
  collection. Both runtimes install the internal logging dataflows, so without
  this rule the interactive runtime's empty copies would regress the controller's
  per-collection frontier. State: `transient_owner` maps a `GlobalId` to its
  owning runtime.

## Roles and process globals

`ComputeRuntimeRole` distinguishes the runtimes.

* `Solo` is the sole runtime of a single-runtime process. It owns maintenance and
  the process globals, and it is behaviorally identical to a deployment from
  before this work. Its metric and log label is `None`, so a single-runtime
  registration collides with nothing and looks unchanged. Process-global
  initializers guard on role, so `Solo` still runs them exactly once.
* `Maintenance` owns index maintenance and the process globals in a two-runtime
  process. It publishes its maintained indexes, and its logging and introspection
  indexes, into the registry. Publication into the registry is gated on the role,
  not on the dyncfg, so a disabled dyncfg cannot strand an interactive read that
  would otherwise block until it times out.
* `Interactive` shares the process globals owned by `Maintenance` and reads only
  from the registry. It runs with logging disabled and serves introspection peeks
  from maintenance's published copies, so introspection during hydration returns
  promptly, possibly stale, instead of blocking. It publishes its own transient
  query outputs so their result peeks route through the registry.

The interactive runtime distinguishes itself in tracing with the span name
`compute-interactive`. Note that this is a span name, not an OS thread name (see
[Known limitations](#known-limitations-and-follow-ups)).

## Failure model

There is no cross-runtime lease. A process-global panic hook
(`mz_ore::panic::install_enhanced_handler`) is installed in `clusterd::main`
before either `serve` call, so a panic on any worker or reader thread of either
runtime aborts the whole process. A stuck or torn read hold can never outlive the
process, which is what makes the import hold safe without a lease-expiry
mechanism. Dropping a still-deferred interactive dataflow cancels cleanly rather
than panicking.

## Configuration

* `ENABLE_TWO_RUNTIME_COMPUTE` (dyncfg, `mz-controller-types`) is off in
  production and on by default in the variable CI system parameters, so the suite
  exercises the two-runtime path broadly. The compiled default stays off, so
  production is unaffected.
* When enabled, the controller launches replicas with a second interactive
  runtime configured by the `--interactive-compute-timely-config` CLI argument
  (its own worker ports). The dyncfg controls whether the controller passes that
  argument.

## Non-goals

* `SUBSCRIBE` is out of scope. All interactive work is single-time, so the shared
  import applies no `until` or `as_of` coalescing. A future subscribe migration
  must add it.
* Cross-process and replica-to-replica sharing are out of scope. Sharing is
  per-process because the batches are `Arc`-backed in memory.
* The import and replay queue is unbounded, with no overflow handling, in this
  first cut. This is deliberate: maintenance progress must never be coupled to a
  slow interactive-side reader. The cost, unbounded memory growth for a
  pathological long-lived importer, is accepted for now and recorded as deferred
  work, not silently ignored.

## Known limitations and follow-ups

* **The coordinator control plane is a parallel, unsolved bottleneck.** Peeks
  serialize behind DDL on the single coordinator thread, upstream of compute.
  Two-runtime fixes the data plane and does not touch this. For non-introspection
  reads under load the coordinator can dominate, so this must stay on the roadmap.
* **The interactive runtime is a single step loop.** Its read throughput has a
  ceiling, and a heavy scan can clog light point reads sharing the loop. A future
  admission or lane policy would protect a cheap-read lane and decide where
  expensive reads spill.
* **Peek placement is static.** All peeks go to interactive today. Whether some
  should be admitted to maintenance (work-stealing when interactive saturates and
  maintenance idles) is a deferrable optimization. It is purely additive and
  changes no correctness. The signal to build it is a measured workload that
  saturates the interactive loop while maintenance sits idle. Routing a read to
  maintenance trades against the isolation guarantee, so the policy is
  workload-dependent, isolation-first for latency-SLA reads and
  placement-for-throughput for bulk reads.
* **Thread names collide across runtimes.** Timely names worker threads
  `timely:work-{index}` by a per-instance-local index, so the maintenance and
  interactive runtimes (and storage) emit identical OS thread names in one
  process. Profilers cannot tell them apart by name. A per-worker rename in the
  worker entrypoint would fix it, and would fix the pre-existing storage and
  compute collision as a side effect.
* **Per-runtime memory attribution.** Arrangement-size introspection does not yet
  attribute memory per runtime.

## Testing strategy

* A `clusterd-test-driver` workflow drives an interactive query dataflow that
  imports an unpublished maintenance index, scheduled before the index publishes,
  and asserts its result peek resolves correctly only after publication. This
  proves the defer, build, resolve read path is served off the maintenance
  worker.
* A shared-fate subprocess test verifies a panic in either runtime aborts the
  process.
* Unit tests cover the sharing primitive and registry, including the single-source
  feed, placeholder-adopted-late joins, cross-thread reads, and the compaction and
  eviction invariants.
* The `TwoRuntimeReadIsolation` parallel-benchmark scenario measures read latency
  while the maintenance runtime is saturated by hydration churn. On a box with CPU
  headroom, two-runtime holds the point-read p50 flat while a single-runtime
  baseline backlogs. On a CPU-saturated runner both backlog, which is the expected
  degradation without spare cores.

## Implementation history

The feature was first built against a differential-dataflow fork that supplied
Arc-backed batches and a `sharing` module, stacked on an Arc-batches base branch.
The sharing primitive was then reimplemented natively in `mz_compute::shared_trace`
plus `mz_row_spine::ArcBatch`, and a lifecycle correctness redesign fixed a set of
concurrency bugs through a single-source publisher feed and placeholder-plus-adopt
construction. Finally the fork was dropped entirely: the publisher's compaction
floor moved from the fork's `trace_box_unstable` read to the controller's
`AllowCompaction` and the stream upper, so the build now depends only on released
differential-dataflow.

The prior planning documents in this directory (`implementation-plan.md`,
`stage2-detailed-plan.md`, `arrangement-sharing-lifecycle-design.md`,
`arrangement-sharing-lifecycle-plan.md`) are superseded by this document. Their
task checklists are fully executed and their fork-era mechanics no longer reflect
the code.
