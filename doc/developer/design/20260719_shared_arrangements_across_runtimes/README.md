# Shared arrangements across timely runtimes

- Associated:
  - [Two-runtime compute](../20260720_two_runtime_compute/README.md): the
    compute-layer design this primitive exists to serve.
  - [background.md](./background.md): how a single-runtime arrangement works
    today (the status-quo baseline this design departs from)
  - differential-dataflow branch `claude/spines-differential-arc-j93mho`
    (Arc'd batches, shared-trace primitive)
  - materialize branch `claude/spines-differential-arc-j93mho`
    (Send + Sync batch assertions)

## The Problem

A replica runs one timely runtime. That runtime does two very different kinds
of work on the same worker threads:

1. CPU-bound maintenance: keeping indexes and materialized views up to date,
   including arrangement merges and operator work proportional to input volume.
2. Latency-sensitive reads: serving peeks against arrangements and rendering
   short-lived dataflows for ad-hoc queries.

These collide. A peek against an index must wait for the worker that owns the
relevant arrangement shard to come around to it, and that worker may be deep
in a join or a large merge. The result is tail latency on reads that is
proportional to how busy maintenance is, which is exactly when users notice.

The underlying restriction is that arrangements are worker-local. Batches are
reference counted with `Rc`, trace handles are `Rc<RefCell<...>>`, and both
are pinned to the worker thread that built them. Nothing else in the process
can look at an arrangement, so all reads must be scheduled onto the owning
worker.

The Arc'd-batches work on the associated branches removes the first half of
that restriction for the stock differential spines: their batches are now
`Arc`'d, so a batch can be handed to and read from any thread. Materialize's
batch *contents* (lgalloc regions, `CompactBytes`, columnation stacks) are
verified `Send + Sync`, though Materialize's own spine typedefs still wrap
those contents in `Rc` and must migrate to `Arc` before they can be shared
(tracked as step 4 of the prototype). What is missing beyond the wrapper is
the machinery to hand over entire arrangements, with correct frontiers and
compaction, to code running outside the owning worker. That machinery is the
subject of this design.

## Success Criteria

- A second timely runtime ("query runtime") in the same process can import an
  arrangement maintained by the main runtime and use it as an ordinary
  `Arranged` collection: render joins, reduces, and other operators against it
  without copying or re-arranging the data.
- A non-timely thread (or the query runtime) can serve a point or full-scan
  read against a consistent snapshot of an arrangement at a given time,
  without scheduling work on the main runtime's workers.
- Readers hold back compaction only as far as necessary: the main runtime's
  arrangement compacts to the meet of all registered read holds (their greatest
  lower bound, so it never compacts past the least reader's hold), and no
  further than the published `since` when there are none. Holds release
  deterministically when readers drop.
- The main runtime's write path pays only constant overhead per batch
  (an `Arc` clone and a mutex push), independent of batch size.
- No unsafe code in the sharing layer, and no changes to the `Trace`/
  `TraceReader` contracts that existing operators depend on.

## Out of Scope

- Cross-process sharing. Everything here is threads within one process.
  Cross-process sharing is persist's job.
- Sharing the chunked columnar traces (`trace::chunk`, `columnar` in
  differential). Their batch internals (`Rc`'d `ColChunk`, `OnceCell` page
  cache) are not yet thread-safe. They keep `Rc` batches for now and can adopt
  the same machinery once their internals move to `Arc`/`OnceLock`.
- Materialize integration (compute controller owning two runtimes, peek
  routing policy, introspection of shared traces). This design describes the
  differential-level primitive and the architecture it enables. Wiring it into
  `mz-compute` is follow-up work.
- Scheduling policy inside either runtime (thread counts, pinning, priorities).

## Solution Proposal

Two adjacent timely runtimes in one process. The main runtime maintains
indexes and materialized views exactly as today. The query runtime renders
ephemeral dataflows and serves peeks. Arrangements cross the boundary through
a per-arrangement, per-worker *publication point*: a small mutex-guarded
structure through which the owning worker publishes its trace's batches and
frontiers, and from which readers on other threads import them.

The primitive lives in differential dataflow as a new module,
`operators::arrange::sharing`, built entirely on public trace APIs.

### The unit of sharing is the batch chain, not the spine

A `Spine` cannot cross threads: it holds a timely `Logger` (`Rc`-based) and
in-progress mergers. It also should not cross threads, because it is the
mutable half of the arrangement and has exactly one writer.

What crosses the boundary is the spine's *contents*: a chain of `Arc`'d
batches with contiguous descriptions, together with the trace's `since`
(logical compaction) and `upper` (seal) frontiers. Batches are immutable, so
a chain plus frontiers is a consistent, self-describing view of the
arrangement. When the main runtime later merges batches, readers holding the
old chain are unaffected: their `Arc`s keep the pre-merge batches alive, and
they simply hold that memory until they drop.

### Publisher: an operator on the main runtime

Sharing is initiated on the owning worker, from an existing arrangement:

```rust
let arranged: Arranged<G, TraceAgent<Tr>> = collection.arrange();
let published: Published<TraceAgent<Tr>> = arranged.publish();
let handle: SharedTraceHandle<Tr> = published.handle();  // Clone + Send
```

`publish` attaches a sink-like operator to `arranged.stream`. That stream
carries the non-empty batches the arrange operator emits, each under a
capability whose time lower-bounds the batch's updates. On each activation the
publisher, under the shared lock:

- appends each newly received batch to every registered reader's replay queue
  as `TraceReplayInstruction::Batch(batch, Some(cap_time))`, recording the
  message's capability time as the replay hint, then a
  `TraceReplayInstruction::Frontier(batch.upper())`, mirroring
  `TraceWriter::insert`. It wakes each reader (see wakeup below).
- refreshes the published chain from `TraceReader::map_batches` on its trace
  handle. This is the authoritative source of the published `chain`, `since`,
  and `upper`, for three reasons the arrangement stream cannot satisfy:
  `map_batches` includes the seal-only empty batches that `arrange_core`
  inserts into the trace but never sends downstream (an idle arrangement with
  a ticking frontier produces only these). It reflects the main runtime's
  compaction so *new* readers start from the already-merged state rather than
  replaying history. And its last batch's `upper` is the trace's true seal
  frontier. The published `upper` is taken from the chain (the join of batch
  uppers via `read_upper`), never from the operator's input frontier, which
  lags the trace by at least one progress round.
- forwards compaction to its own `TraceAgent` (details in "Compaction
  forwarding" below).

The publisher owns a `TraceAgent` clone, so the trace cannot compact or be
dropped out from under remote readers: the publisher's read capability *is*
the aggregate lease for all of them. Dropping the publisher marks the state
`closed` and enqueues a final `Frontier(empty)` at the end of each reader's
queue, so readers drain everything already published before shutting down.

Cost on the write path: one `Arc` clone and one `VecDeque` push per batch per
reader, plus a mutex acquisition per activation. No per-record work.

#### Compaction forwarding

Readers register holds. The publisher aggregates them and advances its
`TraceAgent`, which is the only writer of the underlying trace's compaction
frontiers. Two hazards shape how:

- *`set_physical_compaction` is not cheap.* On the spine it calls
  `consider_merges`, which can complete an in-progress merge with unbounded
  fuel synchronously. It must therefore run *outside* the shared lock, or a
  large merge would stall every concurrent peek and import. The publisher
  computes the target frontiers under the lock (updating `state.since` first,
  so a reader registering concurrently cannot latch a stale lower `since`),
  releases the lock, then calls `set_logical_compaction` /
  `set_physical_compaction` on the agent.
- *Compaction is irreversible and the empty meet is destructive.* The meet of
  an empty hold registry is the empty frontier, i.e. "compact everything".
  Forwarding it releases the publisher's read capability permanently, after
  which `batches_through` panics (it asserts a non-empty logical frontier) and
  no future reader can re-establish a floor. So the publisher never forwards a
  bare meet. With no readers it holds compaction at the published `since` and
  `upper` and advances only the join of holds that actually exist. Logical and
  physical holds are tracked and forwarded independently, because consumers
  like `join` legitimately advance one far ahead of the other.

### Shared state

```rust
struct SharedTraceState<Tr: TraceReader> {
    /// The published chain, sourced from `map_batches`: contiguous
    /// descriptions including seal-only empty batches. Its coverage is at
    /// least `upper` (it may run ahead within a worker step, see below).
    chain: Vec<Tr::Batch>,
    /// Logical compaction frontier of the published view. Reads at times not
    /// beyond `since` are not accurate; peeks pick a time at or beyond it.
    since: Antichain<Tr::Time>,
    /// Seal frontier: the join of the chain's batch uppers. Batches strictly
    /// below `upper` are complete and readable.
    upper: Antichain<Tr::Time>,
    /// Per-registration logical holds. The publisher forwards their meet,
    /// falling back to `since` when empty (never the destructive empty meet).
    logical_holds: BTreeMap<usize, Antichain<Tr::Time>>,
    /// Per-registration physical holds, forwarded independently of logical.
    physical_holds: BTreeMap<usize, Antichain<Tr::Time>>,
    /// Per-registration replay queues and wakeups. Keyed by registration id,
    /// not by handle: one handle may back several registrations.
    readers: BTreeMap<usize, ReaderQueue<Tr>>,
    /// Set when the publisher drops. Terminal `Frontier(empty)` is enqueued
    /// per reader. Readers act on that queue entry, not on this flag directly.
    closed: bool,
}

/// Wakeup for a registered reader. Import registrations carry a timely
/// `SyncActivator`; peek waiters carry a `Condvar`/thread handle, since a
/// non-timely thread has no operator to activate.
enum ReaderWakeup {
    Operator(SyncActivator),
    Thread(Arc<Condvar>),
}

type SharedTraceRef<Tr> = Arc<Mutex<SharedTraceState<Tr>>>;
```

One instance per (arrangement, worker). Every operation under the lock is
short: push a batch, clone the chain, update an antichain, read holds. The one
expensive step, applying compaction to the `TraceAgent`, is deliberately done
outside the lock (see "Compaction forwarding"). Contention is bounded by
publish and read frequency, both per-batch, not per-record.

Ids are assigned by a counter in the shared state. Distinguishing *reader ids*
(one hold-and-queue registration) from *handles* matters because holds must be
independent per consumer, addressed next.

### Reader: handle, snapshot, and import

`SharedTraceHandle<Tr>` is `Clone + Send` and holds a `SharedTraceRef<Tr>`
plus its own registration id, minted from the shared counter. It implements
`TraceReader` directly:

- `batches_through(upper)` locks, finds a clean cut of the published chain at
  `upper` (skipping empty batches, as `Spine::batches_through` does), and
  returns `Arc` clones of the covering batches. The lock is released before
  any cursor work. Reading never blocks the publisher longer than a chain
  clone.
- `set_logical_compaction(frontier)` writes this registration's entry in
  `logical_holds`. The publisher applies the join of all entries on its next
  activation, so compaction is asynchronous but conservative: the trace never
  compacts beyond a registered hold.
- `set_physical_compaction(frontier)` writes `physical_holds` likewise.
  Logical and physical are independent: `join` advances a trace's logical
  compaction to its *opposing* input's frontier while keeping its physical
  hold at its *own* acknowledged batch boundary, and the two routinely
  diverge. The "physical equals logical" default applies only to readers that
  never call `set_physical_compaction`.

`clone()` is where independence is enforced. It cannot share a registration
id, because `import` returns `Arranged { trace: handle.clone() }` and
`Arranged` is itself `Clone`, so two downstream operators (say a join and a
reduce) each drive compaction on their own clone. If they shared one hold
slot, the faster one would release the slower one's hold and the trace would
compact under it, corrupting the slower reader. So `clone()` mints a fresh
registration id under the lock and copies the source's current holds into it,
exactly as `TraceAgent::clone` registers an independent counted hold in
`TraceBox`. Dropping a handle removes only its own registration's holds and
queue.

A subtlety compaction inherits from the trace: a reader registering late gets
a *logical* hold at the current published `since`, but its effective *physical*
floor is whatever physical frontier the publisher has already forwarded (near
`upper`), because physical compaction cannot retreat. This is harmless for
import-based readers, since every `cursor_through` frontier they use is derived
from chain state at or after their registration.

Two read paths sit on top:

1. **Peeks.** `handle.snapshot()` returns a `TraceSnapshot<Tr>`: an owned
   `(chain, since, upper)` triple. It exposes `cursor()` over a `CursorList`
   of the batches' cursors (`Arc<B>` implements `Navigable`). A peek picks a
   time `t` with `since ≤ t` and `upper` not `≤ t`, so all updates at `t` are
   complete. If `upper` has not yet passed `t`, the peek registers a
   `ReaderWakeup::Thread(Condvar)` and waits on it under the shared lock,
   which closes the lost-wakeup window: the publisher signals the condvar
   under the same lock whenever it advances `upper`. This works from any
   thread, timely or not.

2. **Imports.** `handle.import(scope)` on a query-runtime worker builds a
   source operator, mirroring `TraceAgent::import_core`:
   - registration, under one lock acquisition, mints a registration id,
     installs a replay queue keyed by that id and a
     `ReaderWakeup::Operator(worker.sync_activator_for(...))`, and seeds the
     queue with the current chain as `Batch` instructions (hint
     `Time::minimum()`, as `new_listener` does for historical batches)
     followed by `Frontier(upper)`. Taking the chain and installing the queue
     atomically is what guarantees no batch is missed or duplicated: later
     batches land in the queue, earlier ones are in the seed.
   - on activation the source drains its queue. It emits each non-empty batch
     under a capability *delayed to that batch's hint* (a lower bound on the
     batch's times), and applies each `Frontier` by downgrading its
     capabilities, exactly as `TraceAgent` import replay does. Empty batches
     carry no hint and are not emitted, only their frontier is applied. Note
     the hint is a lower bound, never the batch's `upper`: emitting at `upper`
     would let the query runtime's frontier advance past updates still inside
     the batch and break `join`.
   - the queue is owned by the source operator, not the handle. The
     registration is a guard captured in the source closure, so dropping the
     import dataflow deregisters it and releases its holds even while the
     query worker and other handle clones live on. Tying queue lifetime to the
     handle instead would leak: a `SyncActivator` for a live worker keeps
     succeeding, so a dropped-dataflow-but-live-worker reader would never be
     detected, its queue would grow without bound, and its holds would wedge
     compaction. `SyncActivator` failure detects only a fully-gone worker, so
     it is a backstop, not the primary deregistration path.
   - the result is `Arranged { stream, trace: handle.clone() }`, so every
     existing arrangement-consuming operator works unchanged against the
     shared trace.

#### Initial state

The shared state exists at `publish()` time so `handle()` can hand out
`Clone + Send` handles that peek or import before the publisher's first
activation. It initializes to `chain = []`, `since =` the publisher agent's
`get_logical_compaction()` at publish time, and `upper =
Antichain::from_elem(Time::minimum())`. The last point is a trap worth naming:
the `Antichain::new()` default is the *empty* frontier, which reads as
"complete through the end of time", so every peek's wait condition would be
vacuously satisfied and a peek would read the empty chain as a valid snapshot
and return wrong, empty results instead of blocking.

### Partitioning: pairwise import

Arrangements are sharded across workers by a deterministic exchange of the
key. A shared arrangement is therefore N publication points, one per
main-runtime worker. For rendered dataflows the query runtime must preserve
that sharding: it runs with the same number of workers, and query worker `i`
imports main worker `i`'s publication point. Keys then live on the same query
worker that any downstream `arrange` of the same key would route them to, so
joins between two imported arrangements of the same key are co-located without
any exchange.

The soundness of pairwise import rests on the exchange being a deterministic
function of the key and worker count alone. Confirming that Materialize's
arrangement exchange is exactly that function (and not one that also depends
on, say, dataflow-local state) is part of the Materialize integration work,
and equal worker counts between runtimes is a hard requirement the import path
asserts.

This pairing is a correctness requirement for imports, not a preference. The
implementation asserts `peers` match at import time. Peeks are exempt: a
peek may read any or all publication points from any thread, since it does
not feed exchanged operators.

### Consistency and correctness argument

- *No torn reads.* The chain, `since`, and `upper` are only ever updated
  together under the lock, and readers copy all three under the same lock.
  Every snapshot is a frontier-consistent view.
- *No missed or duplicated batches on import.* Registration atomically takes
  the current chain and installs the queue. Batches arriving after
  registration land in the queue. Batches before it are in the chain. The
  publisher appends to queues under the same lock it uses to refresh the
  chain.
- *Compaction safety.* The publisher's `TraceAgent` capability lower-bounds
  everything it has published, and it advances only to the meet of registered
  holds (never the empty meet, never below `since`). `state.since` is updated
  under the lock before compaction is applied to the agent outside the lock,
  so a concurrently registering reader cannot latch a `since` the trace has
  already passed. A reader reading at a time not beyond `since` therefore
  always sees accurate times.
- *Progress.* `upper` is the join of the published chain's batch uppers, which
  is the trace's true seal frontier (the operator's input frontier only lags
  it). Within a worker step the chain may briefly cover *more* than the last
  published `upper`, never less, so the invariant is "chain coverage ≥
  `upper`", and peeks that bound times by `t < upper` stay correct. Import
  capabilities downgrade only on `Frontier` instructions, so query-runtime
  frontiers are always justified by the main runtime's progress.
- *Shutdown.* Publisher drop marks `closed` and enqueues a terminal
  `Frontier(empty)` at the tail of each reader's queue. Readers act on that
  queue entry, after draining every batch ahead of it, so nothing published is
  dropped. Import registrations deregister via their source-closure guard when
  the import dataflow drops, which releases holds even while the query worker
  lives. `SyncActivator` failure (whole query worker gone) is only a backstop.

### Memory considerations

A snapshot pins its chain until dropped. A long-running peek over a large
arrangement holds pre-merge batches alive while the main runtime replaces
them. The cost is bounded by chain size at snapshot time, and is exactly the
cost the main runtime would pay if it had `cursor_through` open locally.
lgalloc supports freeing from foreign threads (its `Handle` is
`Send + Sync`), so batches dropped on the query side release correctly.
Accounting attribution moves to whoever drops last, which introspection needs
to be aware of eventually (out of scope here).

## Minimal Viable Prototype

The prototype is the differential branch itself, in four steps, each with
tests:

1. Arc'd batches (done): the stock `ord_neu` spines (`OrdValSpine`,
   `OrdKeySpine` over the `Vector` layout) use `Arc`, the full test suite
   passes, and a test reads a batch from a foreign thread.
2. Materialize batch contents verified `Send + Sync` (done): compile-time
   assertions in `mz-row-spine` over the production `Row`/`Timestamp`/`Diff`
   layouts. Note this covers the *contents*, not the `Rc` wrapper.
3. The `sharing` module (this design): publisher, handle, snapshot, import,
   built and tested against the stock Arc'd spines. Tests run a main runtime
   and a query runtime as two `timely::execute` instances in one process and
   verify each of:
   - a peek snapshot sees exactly the published collection at a chosen time.
   - an imported arrangement renders a correct dataflow in the query runtime
     and tracks ongoing updates.
   - two consumers of one import hold compaction independently.
   - read holds keep the main trace from compacting, and release on drop.
   - publisher shutdown drains queued batches, then closes importers.
4. Materialize adoption of Arc spines (done): Materialize's production spines
   now wrap batches in `Arc` with `ArcBuilder`. `RowRowSpine`, `RowValSpine`,
   `RowSpine`, `ValRowSpine` in `mz-row-spine` and `ColValSpine`, `ColKeySpine`
   in `mz-compute` typedefs migrated from `Rc`/`RcBuilder`. `ErrSpine` and the
   other error and generic spines are aliases of these, so they follow. The
   only consumers that needed touching were those that named the batch wrapper
   directly: the arrangement-size logger (`Rc::downgrade`/`Weak` over batches)
   and the storage sink's stock `OrdValSpine` builder alias. The step-2
   assertions guaranteed the contents were ready, so only the wrapper changed.
   Materialize depends on `differential-dataflow` through a `[patch.crates-io]`
   entry pointing at the fork until an upstream release carries
   `arc_blanket_impls`.

## Alternatives

- **Share the whole spine behind a mutex.** Rejected: `Spine` is `!Send`
  (timely `Logger`), holds in-progress mergers, and has a single-writer
  ownership story that a shared mutex muddles. It would also serialize
  maintenance merge work against reads. The chain-of-batches view gives
  readers everything they can use.
- **Ship batches over a channel and rebuild a spine on the query side.**
  Rejected: the query runtime would pay merge CPU and hold a second copy of
  merged batches, and it could not benefit from the main runtime's
  compaction. It also duplicates `since`/`upper` management badly.
- **Serialize batches across the boundary.** Strictly worse in-process:
  copies all data, loses the columnar in-memory layout, and equals what
  persist already offers across processes.
- **Priority scheduling inside one runtime.** Timely scheduling is
  cooperative per worker, so a peek behind a long-running operator step cannot
  preempt it. Yielding more finely helps tail latency but couples maintenance
  throughput and read latency forever, and does nothing to isolate ephemeral
  dataflow rendering.
- **`TraceRc`-style read-counted wrappers extended across threads.** The old
  `TraceRc` machinery solved hold accounting worker-locally with
  `Rc<RefCell<...>>`. Generalizing it directly to `Arc<Mutex<...>>` puts a
  lock on the owning worker's hot read path too. The publisher/handle split
  keeps the owning worker lock-free except at publication points.

## Open questions

- **Physical compaction policy.** Logical and physical holds are independent
  (a reader that never sets a physical hold defaults it to its logical hold).
  Whether the default keeps more batch boundaries alive than join actually
  needs is a question to measure before refining.
- **Publisher wakeup latency for hold changes.** Reader hold changes take
  effect on the publisher's next activation. Applying compaction outside the
  lock (see "Compaction forwarding") means an idle arrangement with an
  unscheduled publisher applies hold *releases* lazily, which delays freeing
  memory but is never a correctness problem. If it matters, readers can wake
  the publisher through a main-runtime `SyncActivator` registered alongside
  the publication point.
- **Lease expiry.** RAII holds are correct in-process, but a query thread
  stuck in a long computation holds compaction back. Do we want time-based
  lease expiry with snapshot invalidation, or is same-process trust enough?
  This design assumes the latter for now.
- **Wakeup of the publisher.** Reader hold changes take effect on the
  publisher's next activation, which today means the next batch or frontier
  movement. An idle arrangement applies hold releases lazily. If that
  matters, the publisher can also be woken by readers through a main-runtime
  `SyncActivator`.
- **How Materialize adopts it.** One replica process running two runtimes
  needs controller work: which dataflows land where, how peeks choose a
  runtime, how introspection reports shared traces and their memory. Separate
  design.
- **Logging.** Shared-trace activity (publications, imports, holds) is
  invisible to timely/differential logging today. Decide what to log and
  where once the mz integration design exists.
