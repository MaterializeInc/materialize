# Broadcast compaction

Status: implemented, steps 1 to 4. Only the remodelling of `protocol-holds` (step 5) is
left. Supersedes the mechanism in [read-holds.md](read-holds.md), whose invariant (I1) and
gap analysis still stand and are not restated here.

## The conclusion that motivates this

`read-holds.md` diagnoses the problem correctly. A single-runtime replica satisfies I1
twice over, and splitting compute keeps I1a (the controller's global floor) while losing
I1b (one totally ordered command stream). Every mechanism built on top of that diagnosis
has been a way to reconstruct I1b: cap the frontier at the multiplexer, or synthesize an
`AcquireHolds` onto the owning runtime's stream and reclaim it later.

Reconstructing I1b is the wrong move. I1b was lost because of a routing decision, not
because two runtimes cannot have it. `CreateDataflow` goes only to the rendering runtime
and `AllowCompaction` only to the owning one, so the two commands that must be ordered
are placed on different streams. Send compaction to both runtimes and each runtime has
them on one ordered stream again, which is I1b restored rather than simulated.

## The design

Two changes.

**Broadcast compaction.** The multiplexer forwards `AllowCompaction` to both runtimes
instead of routing it to the owning one. It keeps routing `CreateDataflow` by ownership.

**A standing hold per shared collection.** The rendering runtime holds every shared
collection it may import at the last compaction frontier *it* has applied. The publisher
bounds the owning runtime's compaction by that hold, alongside the reader registrations
it already meets.

The resulting invariant:

> **I1c.** A shared arrangement compacts only as fast as the slowest runtime's stream
> position.

That is what makes an individual read correct, and it is derived from the importing
runtime rather than from the controller's per-dataflow bookkeeping. It satisfies the
requirement `read-holds.md` states: the direct dependency is the mechanism.

### Why broadcast alone is not enough

Restoring the ordering *within* each stream does not restore it *between* them. The
runtimes still drain at their own rates, so the owning runtime can realize a frontier the
rendering runtime has not applied.

TLC refutes it (`protocol-holds/HoldsBroadcast.cfg`, I1 violated in five states):

1. the controller creates a dataflow at `as_of = 0`, queued to the rendering runtime
2. the controller drops it immediately, as a cancelled peek does, releasing its own read
   hold before the rendering runtime has applied either command
3. `AllowCompaction(1)` is broadcast
4. the owning runtime applies it, reaching `since = 1`

The create is still queued. When the rendering runtime gets to it, it builds a dataflow
at `as_of = 0` over a collection compacted to 1. The controller did nothing wrong: from
its point of view the dataflow is gone.

The standing hold closes exactly this window. In step 4 the bound is the rendering
runtime's applied frontier, still 0, and it advances only when that runtime applies the
broadcast compaction, which is queued *behind* the create.
`protocol-holds/HoldsBroadcastStanding.cfg` holds over 19940 distinct states.

### Why the standing hold costs nothing

It pins each collection at the controller's own compaction frontier, which the controller
already guarantees is readable, and which the publisher's writer-driven fallback already
targets when no reader is registered. So it adds no pin that was not there. What it
removes is the publisher's freedom to run ahead of the rendering runtime.

### What it needs from the runtimes

* The rendering runtime must accept `AllowCompaction` for collections it does not host,
  and treat it as advancing the standing hold rather than as compacting a local trace.
* It must keep not reporting frontiers for shared collections, which
  `ComputeState::report_frontiers` already handles via its transient-only filter.
* A collection's standing hold is installed when the collection first appears and removed
  when its compaction reaches the empty frontier, so it needs no per-dataflow identity.

## What this deleted

Steps 0 through 4 of the sequence in `read-holds.md`:

* `ComputeCommand::AcquireHolds` / `ReleaseHolds` and `HoldRequest`
* `compute_state/command_hold.rs`, and `command_holds` in `SharedTraceState` with its
  half of `refresh_since`
* `ArrangementSharingRegistry::release_holder` / `reclaim_holder` / `released_holders`
* the multiplexer's `held_exports` and its hold synthesis, and with it the
  acquire-on-the-owner / release-on-the-renderer asymmetry
* the `AcquireHolds` / `ReleaseHolds` arms in `ComputeCommandHistory::reduce`

**Step 5 dissolves rather than being solved.** G2, the epoch boundary, is a problem about
holder identity surviving a reconnection: a replayed dataflow gets a fresh transient id,
so one holder shared across epochs conflates two dataflows. A standing hold is per
collection and carries no dataflow identity, so there is nothing to go stale. The
conservative discard at reconnection goes away with it.

## What survives, and is independent

Neither of these is about admitting a read, so neither is affected:

* **Physical compaction follows the readers' cut floors** (`shared_trace.rs`,
  `physical_holds`). About which batches may merge, not which times stay
  distinguishable.
* **Padding the imported trace.** After a reconnection the controller re-reads logging
  collections from the minimum time, and the shared chain cannot serve that whatever the
  compaction routing is. Still open.

## Costs and open questions

**Compaction is coupled to the rendering runtime's drain rate.** A stalled rendering
runtime stalls compaction on every shared arrangement. This is the price of I1c and it is
the intended behaviour, but it is new: today a slow interactive runtime cannot hold
maintenance back. It wants a metric on the gap between the two runtimes' applied
frontiers, so the coupling is observable before it becomes a memory incident.

**Publishing every arrangement is unmeasured.** "Share all traces" means every index pays
the publisher operator and the chain bookkeeping whether anything imports it or not. With
physical compaction now following reader floors the merge behaviour is at parity with an
unshared index, so what remains is the per-round chain refresh. Measure before committing
to publishing unconditionally.

**The model does not cover G2.** `MaxEpochs = 0`, as before. The claim that a standing
hold makes the epoch boundary vacuous is an argument, not a checked property, and it is
cheap to check once holder identity is out of the model.

## Implementation sequence

1. **Assert I1c first.** Add the standing hold and the broadcast, and a test that a
   compaction the rendering runtime has not applied does not advance the published
   `since`. The deletions below are then verified rather than hoped. **Done.**
2. Broadcast `AllowCompaction` in the multiplexer, and accept it for non-hosted
   collections in the rendering runtime. **Done.**
3. Install and downgrade the standing hold, and bound the publisher's forward by it.
   **Done.**
4. Delete the acquisition layer in one commit, keeping its findings in
   `read-holds.md` as rejected alternatives. **Done.**
5. Remodel `protocol-holds` around stream positions, and drop the mechanisms that no
   longer exist in the code once nothing references them.

## As implemented

The standing hold is one frontier per publication point (`SharedTraceState::standing_hold`),
joined so it only rises, and the publisher's logical target is met against it. The published
`since` is then bounded by it without further work, because `since` is derived from the
publisher's own agent hold and that hold is the join of targets it has already forwarded. A
`debug_assert` in the publisher states that, so an edit that lets the target escape the bound
fails there rather than admitting a reader below what the trace holds.

Two things were not obvious from the design.

**The hold must be seeded at the adoption floor, not at the minimum time.** An arrangement whose
importing runtime has not yet applied any compaction for it would otherwise be pinned at the
minimum time for as long as that lasts, which for a collection the controller never compacts again
is forever. The publisher's own compaction frontier at adoption is the right seed: the controller
does not offer an `as_of` below a collection's `since`, so no importer can need a frontier below
it. That also makes the no-broadcast-yet behaviour identical to what the writer-driven fallback
did before.

**The rendering runtime tells its own publications apart by transience, not by whether it hosts
the collection.** It holds empty local copies of the maintenance runtime's introspection indexes,
so "do I have a collection for this id" answers yes for ids whose *publication* is the peer's. A
non-transient id there is the peer's, a transient one is its own. It must also not apply the
broadcast frontier as the writer-driven floor, which is the peer's to drive, and it must not run
`drop_collection` for a broadcast drop, which would remove the peer's slot from the registry.

Its own transient publications keep their adoption floor as their standing hold, since nothing
notes one for them. They are single-`as_of` dataflows with a bounded `until`, so there is no
history for that to retain.

## What the deletion also removed

Two things fell out that the list above does not name, both of which existed only because a
command hold was a second writer of the published `since`.

`SharedTraceState::writer_since` and `refresh_since`. A command hold moved from the
publisher's own thread but outside its dataflow, so `since` had to be recomputable without
the publisher's inputs, which is why it was split in two and derived. With one writer left,
the publisher assigns `since` directly.

The four `Published` methods that recorded a hold the point did not own
(`acquire_command_hold`, `downgrade_command_hold`, `release_command_hold`,
`reader_hold_meet`). The distinction between a hold that is a *request* forwarded through the
publisher's agent and a hold that is a *grant* backed by someone else's handle goes with
them. Every hold is a request again.

The replica no longer clears anything at a reconnection. A standing hold is per collection,
carries no dataflow identity, and only rises, so clearing it would only drop the bound to the
minimum time until the replayed compactions raised it again. That is G2 being vacuous rather
than handled.
