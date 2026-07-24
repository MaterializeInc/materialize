# Background: how a single-runtime arrangement works today

This is a status-quo companion to the [shared arrangements design](./README.md).
It records how differential dataflow builds, merges, compacts, reads, and
imports an arrangement inside one timely runtime. The design proposes crossing
a thread boundary. This document is the baseline it departs from, so that the
design reads as a delta and its assumptions are visible rather than implicit.

Nothing here is a proposal. It is a description of existing behavior, grounded
in source. References point at `differential-dataflow/src/...` on the branch
behind TimelyDataflow/differential-dataflow#807 (the Arc-batches fork). Line
numbers drift, so treat them as pointers to the right function, not addresses.

## The data model: immutable batches with a description

An arrangement is a collection of updates `(data, time, diff)` organized into
*batches*. A batch is immutable once built. The `Batch` trait is documented as
"An immutable collection of updates" (`trace/mod.rs:236`), and the spine is
"based on collection and merging immutable batches of updates"
(`trace/implementations/spine_fueled.rs:9-11`). Merging never mutates its
inputs. `Merger::done` produces a new output batch (`trace/mod.rs:321-323`) and
hands the source batches back to the caller to drop.

Every batch carries a `Description` (`trace/description.rs:68-97`) with three
frontiers:

- `lower`: the inclusive lower bound of the times the batch describes.
- `upper`: the exclusive upper bound. Together `[lower, upper)` is the batch's
  slice of time. `upper` is also the seal frontier: updates strictly below it
  are complete.
- `since`: the compaction frontier at which the batch's times are observed.
  When `since <= lower` the batch contents are exact. When `since` is in advance
  of `lower`, a consumer must advance observed times by `since` before comparing
  them (`trace/description.rs:50-56`).

`lower`, `upper`, and `since` are the vocabulary for everything below.
Immutability is what makes sharing conceivable at all. A batch plus its
description is a self-contained, consistent fact that stays valid no matter what
the writer does next.

## The arrange operator: one write path, two consumers

`arrange_core` (`operators/arrange/arrangement.rs:352-520`) turns a stream of
updates into an arrangement. It feeds a `Batcher` and, as the input frontier
advances, seals batches whose bounds are the successive frontiers
(`arrangement.rs:361-374`). Each sealed batch goes to two places:

1. Into the trace, via `writer.insert(batch.clone(), Some(cap_time))`
   (`arrangement.rs:479`).
2. Onto the operator's output stream, via `output.session(...).give(batch)`
   (`arrangement.rs:481-482`).

These are the same batches. The stream doc states it plainly: "This stream
contains the same batches of updates the trace itself accepts"
(`arrangement.rs:48-49`). Downstream operators consume the stream for the
*incremental* batches they need (a join needs each side's deltas), and reach the
*accumulated* state through a trace handle.

### Seal-only empty batches

When the input frontier ticks but the batcher holds no data in advance of it,
the operator takes a different branch (`arrangement.rs:503-509`). It calls
`writer.seal(frontier)`, which builds an empty batch `[old_upper, new_upper)`
and inserts it into the trace but does **not** send it downstream. The comment
is explicit: an empty input batch with the new upper is fed "to the trace agent
(but not along the timely output)" (`arrangement.rs:449-452`).

So an idle arrangement with a ticking frontier produces empty batches that enter
the trace (and, as the next section shows, the replay queues) but never travel
the output stream. This is why anything reconstructing the seal frontier must
read it from the trace, not from the stream. The stream lags the trace by these
empty batches, and by at least one progress round.

## The spine: fueled, amortized, internal merging

The trace is a `Spine` (`trace/implementations/spine_fueled.rs`). Inserting a
batch (`spine_fueled.rs:270-286`) appends to a pending list and calls
`consider_merges` (`spine_fueled.rs:399-437`), which places batches into
size-tiered layers and begins merges when a layer fills.

Merging is *fueled*. `introduce_batch` grants `fuel = (8 << level) * effort`
(`spine_fueled.rs:474-477`) and applies it to every in-progress merge via
`apply_fuel` (`spine_fueled.rs:563-589`). A merge advances a little on each
insert rather than all at once, which is how the spine amortizes merge cost
against ingestion. The eight-units figure is justified in the source as four
units per real record plus four per virtual record from promoting smaller
batches (`spine_fueled.rs:468-473`). When a merge completes it cascades into the
next larger layer (`spine_fueled.rs:584-587`).

The load-bearing fact for anything downstream: **merges are entirely internal to
the spine.** They are driven by `insert`, `exert`, and
`set_physical_compaction` → `consider_merges` (`spine_fueled.rs:205`). None of
them call `TraceWriter::insert`, and none of them touch the arrange operator's
output stream. A merge changes the physical layout of the trace. It produces no
event that leaves the spine.

## Compaction: logical versus physical

A trace exposes two independent compaction frontiers, and they mean different
things (`trace/mod.rs:82-129`).

**Logical compaction** (`set_logical_compaction`) is permission to change update
times. From the trait doc (`trace/mod.rs:82-97`):

> Logical compaction is the ability of the trace to change the times of the
> updates it contains. Update times may be changed as long as their comparison
> to all query times beyond the logical compaction frontier remains unchanged.
> Practically, this means that groups of timestamps not beyond the frontier can
> be coalesced into fewer representative times.
> By advancing the logical compaction frontier, the caller unblocks merging of
> otherwise equivalent updates, but loses the ability to observe historical
> detail that is not beyond `frontier`.

**Physical compaction** (`set_physical_compaction`) is permission to merge
batches (`trace/mod.rs:107-121`):

> Physical compaction is the ability of the trace to merge the batches of
> updates it maintains. Physical compaction does not change the updates or their
> timestamps, although it is also the moment at which logical compaction is most
> likely to happen.
> By advancing the physical compaction frontier, the caller unblocks the merging
> of batches of updates, but loses the ability to create a cursor through any
> frontier not beyond `frontier`.

In the spine, `set_logical_compaction` only records the frontier
(`spine_fueled.rs:193-196`). `set_physical_compaction` records its frontier and
calls `consider_merges` (`spine_fueled.rs:200-206`). The actual coalescing
happens *when a merge begins*: `insert_at` passes the logical frontier into
`begin_merge` (`spine_fueled.rs:617-618`), the new batch's `since` becomes the
join of its inputs' `since` with that frontier (`ord_neu.rs:383-384`), and each
update time is advanced by the merged `since` before it is consolidated
(`ord_neu.rs:571-572`). Advancing `since` is what collapses distinct times at or
below it into equal representatives, so their diffs cancel or sum.

Two consequences worth stating. Compaction only ever advances, and it is
irreversible: once times are coalesced the detail is gone. And logical
compaction does its actual work through physical merges. A trace whose physical
compaction is held back accumulates uncompacted detail even if its logical
frontier has advanced, because the merge that would coalesce has not run.

## Reading a trace

`TraceReader` (`trace/mod.rs`) is the read interface. Two methods matter for
sharing:

- `batches_through(upper)` returns a clean cut of the batch chain: the non-empty
  batches whose descriptions are covered by `upper`. It skips empty batches
  (`spine_fueled.rs:139,146,155,166`) and panics if a non-empty batch straddles
  the cut (`spine_fueled.rs:176-186`). The frontiers of the chain "form a total
  order" (`spine_fueled.rs:116-117`).
- `map_batches(f)` visits the entire current batch chain, including empty
  batches (`spine_fueled.rs:211-223`). It reflects merges: after the spine rolls
  two batches into one, `map_batches` yields the merged batch. It is the
  authoritative view of what the trace currently holds.

A reader turns batches into a cursor. Because `map_batches` reflects merges, any
reader that re-reads the trace sees the current, compacted, merged state. A
reader that captured an older set of batch handles keeps those exact batches
alive and unchanged, because batches are immutable and reference counted.

## Sharing within a worker: TraceAgent and TraceBox

Within one worker, an arrangement is shared through `TraceAgent`
(`operators/arrange/agent.rs`), a handle onto a shared `TraceBox`
(`agent.rs:529-587`). This is the existing, single-thread analog of the
cross-thread sharing the design adds, and it is worth understanding as the model
being generalized.

`TraceBox` wraps the trace plus two `MutableAntichain`s, one for logical and one
for physical holds, described as "accumulated holds on times for advancement"
and "for distinction" (`agent.rs:540-547`). A `MutableAntichain` counts
multiplicities of held times, and its `frontier()` is the meet, the greatest
lower bound, of all outstanding holds. Each live `TraceAgent` contributes its
own hold as a counted entry.

Holds move through `adjust_logical_compaction` (`agent.rs:572-577`), which
replaces the old hold with the new one (add the new times, remove the old) and
pushes the resulting meet into the trace. Cloning an agent adds its holds
(`agent.rs:494-495`), dropping one removes them (`agent.rs:520-521`). The trace's
effective compaction frontier is therefore always the meet of holds across all
live handles. No single handle can compact the trace past another handle's hold.

Notably, `TraceAgent::set_logical_compaction` does not require monotonic
advance. It joins the request with the agent's current frontier
(`agent.rs:43-50`) and moves forward with the consequence. The trace only ever
sees advancing frontiers even when a caller is careless.

## Import and replay

A downstream dataflow consumes an arrangement it did not build by *importing*
it. `import_core` (`agent.rs:270-318`) builds a timely `source` operator that
registers a *listener queue* against the trace and replays instructions from it.

The queue speaks `TraceReplayInstruction`, a stream of `Batch(batch, hint)` and
`Frontier(frontier)` items. Two producers feed it:

1. **Seeding.** When a listener registers, `new_listener` (`agent.rs:110-137`)
   walks the current trace with `map_batches` and enqueues each existing batch
   as `Batch(batch, Some(minimum))`, followed by one `Frontier(upper)` for the
   last batch's upper. The hint for historical batches is the minimum time, not
   each batch's lower. Crucially the seed comes from `map_batches`, so a new
   importer starts from the *already merged and compacted* batch set, not from a
   replay of history.

2. **Ongoing inserts.** `TraceWriter::insert` (`writer.rs:50-82`) pushes, for
   each newly sealed batch, the pair `Batch(batch, hint)` then
   `Frontier(batch.upper())` to every registered queue (`writer.rs:66-75`), then
   activates the listener. This runs once per new batch, including the empty
   seal batches from an idle arrangement. The `hint` is "either `None` in the
   case of an empty batch, or is `Some(time)` for a time less or equal to all
   updates in the batch and which is suitable for use as a capability"
   (`writer.rs:50-54`).

The replay loop (`agent.rs:291-313`) drains the queue and translates it:
`Frontier(f)` downgrades the source's capability set, and `Batch(batch, hint)`
emits the batch under a capability *delayed to the hint* when the hint is
`Some` and the batch is non-empty. The hint being a lower bound, never the
batch's `upper`, is what keeps the imported frontier from advancing past updates
still inside a batch. Empty batches produce no output but their following
frontier still moves progress.

There is no third producer. **Merges are never enqueued.** `TraceWriter` has no
merge entry point. Its only merge-adjacent method, `exert` (`writer.rs:43-48`),
drives spine merges but pushes nothing to any queue. An importer therefore never
receives a merge event. It receives new batches as they are sealed and rebuilds
equivalent state locally, and a *late* importer instead seeds from the current
merged chain. Both arrive at the same logical collection, differing only in
physical layout.

## What any cross-thread sharing inherits from this baseline

The design in [README.md](./README.md) generalizes `TraceAgent` and its listener
queue across a thread boundary. These status-quo properties are the ones it
must carry, work around, or consciously change. They are stated here as facts,
not decisions.

- **Merges and compaction never leave the spine.** No event is produced when
  the writer merges or coalesces. Readers that re-read the trace (via
  `map_batches`) see the merged state. Readers replaying the insert stream do
  not, and rebuild equivalent state on their own. Any sharing mechanism that
  wants a remote reader to *benefit* from the writer's merges must route that
  reader through a fresh read of the batch chain, not through the replay stream.

- **The replay queue is producer and consumer coupled on one worker today.**
  `insert` pushes to the queue and the same worker's scheduler drains it in the
  same run of steps. The queue self-limits because production and consumption
  advance together. Nothing in `writer.rs` or `agent.rs` bounds the queue
  length. Decoupling the producer and consumer onto independently scheduled
  runtimes removes the coupling that kept it bounded.

- **Compaction is monotone and irreversible, and the empty meet is
  destructive.** The trace only advances. A hold set that empties to the empty
  frontier means "compact everything", which is unrecoverable. Hold accounting
  that today lives in `TraceBox` as counted antichains has to be reconstructed
  wherever the holds now live.

- **Logical compaction realizes through physical merges.** Holding physical
  compaction back keeps uncompacted detail resident even when the logical
  frontier has advanced. A remote reader that holds physical compaction affects
  the writer's ability to shrink its own footprint.

- **Immutability plus reference counting is the whole basis for sharing.** A
  batch handed out stays valid and unchanged for as long as a handle to it
  lives, wherever that handle lives. The only thing pinning batches to the
  writing thread is the reference-count wrapper. Today that is `Rc`
  (`trace/mod.rs:327-439`, `rc_blanket_impls`). The Arc-batches work adds an
  identical `arc_blanket_impls` (`trace/mod.rs:447-558`) whose sole difference
  is `Arc` for `Rc`, so a batch whose contents are `Send + Sync` can be read
  from any thread.
