# Two-runtime read-hold protocol model

TLA+, checked with TLC. Design: [`../broadcast-compaction.md`](../broadcast-compaction.md).
Invariant and gap analysis: [`../read-holds.md`](../read-holds.md).

Run it:

```
ci/test/tla-holds.sh
```

That builds a TLC image (headless JRE plus a digest-pinned `tla2tools.jar`) and
runs the manifest in that script. Pass arguments to run something else in the
image, for example a single config with more workers:

```
ci/test/tla-holds.sh tlc2.TLC -deadlock -workers 4 -config Holds.cfg Holds.tla
```

The spec models **what the code does**, not the design as first drafted. It is
brought back into line whenever the implementation changes, because a model that
describes a system nobody built is worse than no model: it reads as assurance.

## What is checked

`Holds.cfg` runs the shipped design, `Mechanism = "broadcast-standing"`: compaction
goes to both runtimes, and the rendering runtime holds every shared collection at
the last frontier it has applied. Invariants `TypeOK`, `I1`, `I1c` and
`NoRegression`, plus the liveness property `CompactionNotStalled` against
`FairSpec`. Two processes, three timestamps, no reconnection. 7845 distinct states,
depth 19.

Two configs are **expected to violate `I1`**, and `ci/test/tla-holds.sh` fails if
either stops doing so. Without them a green run of `Holds.cfg` proves nothing: a
model that can only express the shipped design cannot tell you the design fixed
anything.

* `HoldsRouted.cfg`, `Mechanism = "routed"`: compaction routed to the owning
  runtime alone. The raw defect the runtime split introduced, and the reason this
  work exists.
* `HoldsBroadcast.cfg`, `Mechanism = "broadcast"`: compaction broadcast to both
  runtimes with no hold added. The near miss.

Both fail in the same six states, which is the point of checking both. The
controller creates a dataflow at `as_of = 0`, drops it at once as a cancelled peek
does, then allows compaction to 1. The owning runtime applies that and publishes it
while the create is still queued on the rendering runtime, so a dataflow that has
not been built yet will read a collection compacted past its `as_of`. Broadcasting
the command restores the ordering *within* each stream and buys nothing at all here,
because the two runtimes still drain at their own rates. The controller did nothing
wrong: from its point of view the dataflow is gone.

## What the model earned

**It refuted broadcast-alone before it was implemented.** The counterexample above
is the whole reason the standing hold exists. Adding two `Mechanism` values and two
configs to an existing spec cost about thirty minutes and settled a claim that would
otherwise have been implemented on reasoning alone. Reach for the model first on a
two-runtime ordering question.

**It refuted an earlier design's routing.** Before the standing hold, the mechanism
was a synthesized `AcquireHolds` on the owning runtime's stream with a matching
release. Run one of that model found in nine steps that the release can overtake a
create the rendering runtime has not processed, which is why the release had to
travel on the rendering runtime's stream. That mechanism is gone from the code and
from this spec, and `../read-holds.md` keeps the reasoning as a rejected
alternative.

**Restating `I1` for a moving hold.** A hold that follows its reader rather than
sitting at the `as_of` means the old `I1` would have flagged a *correct* downgrade:
after the reader advances, the collection may legitimately compact past the original
`as_of`. `I1` is two windows, protected at the `as_of` before the dataflow is built
on a process and at the reader's current hold afterwards.

**A liveness dependency nobody had written down.** Under a standing hold the owning
runtime's compaction is bounded by the rendering runtime's stream position, so a
rendering runtime that stops draining stalls compaction on every shared arrangement.
`CompactionNotStalled` holds only because `Fairness` states that the runtimes drain.
That coupling is the design's price and stating it as fairness is what records it.

`I1c` is implied by `I1` under this mechanism, and is checked separately because it
is the mechanism stated directly rather than through its consequence. A
counterexample then distinguishes "the bound was not applied" from "the bound was
applied and was not enough". It is the same claim the publisher asserts at runtime.

## What is NOT checked

**G2, the epoch boundary.** `MaxEpochs = 0`, so `Hello` never fires. It is no longer
the largest gap: a standing hold is per collection and carries no dataflow identity,
so a replayed dataflow getting a fresh transient id cannot conflate two holders, and
the replica keeps its standing holds across a reconnection deliberately. That is an
argument rather than a checked property, and it is now cheap to check because holder
identity is out of the model.

Also unmodelled: more than one interactive dataflow, more than one collection, and
the arrangement's physical compaction, which is about which batches may merge rather
than which times stay distinguishable and is independent of everything here.

The publisher's ratcheting agent *is* modelled now, as the monotonicity of `since`
in `MaintRefresh`, and the reader's own registration is a bound. It could not be one
under the acquisition mechanism, because a registration is forwarded through a single
agent whose setter joins and so cannot represent a frontier below where that agent
already sits. Bounding the agent by the standing hold is what removes the ratchet, so
the registration became representable and the acquisition became unnecessary, both
for the same reason.

G3 needs nothing: a real hold is keyed by arrangement rather than by id, so a
re-export that shares one `TraceBundle` under two ids is covered by a hold on either.
G4 dissolved with the cap, since nothing depends on a frontier report any more.

Sizes are what make the run exhaustive, so they are the confidence dial. Two
processes is the smallest that can express G1; raising `Procs` and `Times` buys more
and costs state space.

## Notes for editing the spec

* Every controller action needs a progress guard, or its queue grows without bound
  and the state space stops being finite. `CtlCompact` is strictly increasing and
  `CtlDrop` requires a live hold for exactly this reason. `QueuesBounded` is a
  backstop so that a future action missing a guard fails loudly rather than running
  forever.
* Sets must stay homogeneous. TLC canonicalizes a set value by sorting its elements,
  so a set mixing a string sentinel with numbers fails to compare. `NoTime` is
  therefore a configured number outside `Times`.
* `NoTime` is deliberately larger than every element of `Times`, which reads as
  "permits all compaction". `Target` relies on it: an absent bound takes part in the
  minimum without a special case.
* Keep applying a compaction and publishing one as separate actions. Recomputing
  `since` only when a command arrives would model a publisher that never notices its
  standing hold catching up, and would hide the whole liveness question.
* Liveness is checked under the `QueuesBounded` state constraint, which TLC applies
  by pruning states. A constraint can in principle hide a counterexample from a
  temporal property. The progress guards are what keep the queues short, so the
  constraint should never bite; if it starts to, the guards are what to fix, not the
  bound.
