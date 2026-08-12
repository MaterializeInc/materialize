# Two-runtime read-hold protocol model

TLA+, checked with TLC. Design: [`../read-holds.md`](../read-holds.md).

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

The spec models **what the code does**, not the design as first drafted. It was
brought back into line after the implementation landed, because a model that
describes a system nobody built is worse than no model: it reads as assurance.

## What is checked

`Holds.cfg` runs the shipped design, `Mechanism = "acquire"`. Invariants `TypeOK`,
`I1`, `HoldNeverPassesReader`, `NoRegression`, `LagNeverExposes`, plus the liveness
property `NoPermanentPin` against `FairSpec`. Two processes, three timestamps, no
reconnection. 25581 distinct states, depth 23.

Two configs are **expected to violate `I1`**, and `ci/test/tla-holds.sh` fails if
either stops doing so. Without them a green run of `Holds.cfg` proves nothing: a
model that can only express the shipped design cannot tell you the design fixed
anything.

* `HoldsCap.cfg`, `Mechanism = "cap"`: the mechanism this work deleted. The
  multiplexer capped `AllowCompaction` and retired the cap when interactive
  reported a frontier.
* `HoldsReleaseOnMaint.cfg`, `Mechanism = "release-on-maint"`: the release
  travelling on the owning runtime's stream instead of the rendering runtime's.
  The asymmetry in the code exists for this reason alone, so it is checked rather
  than asserted in a comment.

## What the model earned

**Run one refuted the design as drafted.** The release was to travel on
maintenance's stream, and TLC found in nine steps that it can overtake a create
interactive has not processed: maintenance applies acquire, release and compaction
while the dataflow is still queued, and the dataflow then renders against compacted
data. That is why the release is on the rendering runtime's stream, and
`HoldsReleaseOnMaint.cfg` now keeps that refutation live.

**Restating `I1` for the downgrade.** The shipped hold follows its reader rather
than sitting at the `as_of`, so the old `I1` would have flagged a *correct*
downgrade: after the reader advances, the collection may legitimately compact past
the original `as_of`. `I1` is now two windows, protected at the `as_of` before the
dataflow is built here and at the reader's current hold afterwards. Sabotaging the
downgrade to overshoot its reader violates it, so it discriminates.

**A liveness dependency nobody had written down.** `NoPermanentPin` failed until
`Fairness` included the runtimes draining their command queues. The release reaches
the owning runtime only through the rendering runtime applying a command, so a
runtime that stops draining strands the hold forever. That is a real dependency of
the design, and stating it as fairness is what records it.

`HoldNeverPassesReader` is implied by `I1` (a hold above its reader lets compaction
pass the reader) but is kept because it fires immediately, without waiting for a
compaction to be queued behind it.

## What is NOT checked

**G2, the epoch boundary.** `MaxEpochs = 0`, so `Hello` never fires. Modelling a
reconnection faithfully needs the holder identity to be epoch-scoped, because a
replayed dataflow gets a fresh transient id and one holder shared across epochs
conflates two different dataflows. An earlier attempt did fire `Hello` and produced
a counterexample that was an artifact of the action resetting replica state that a
real `Hello` does not touch. This is the largest remaining gap and G2 is a
confirmed defect in the current implementation, so it should be modelled before
the design is built.

Also unmodelled: more than one interactive dataflow, more than one collection, and
the publisher's ratcheting agent. That last one is handled by assumption rather than
by modelling: `MaintStep` treats **only** the command-acquired hold as a bound and
deliberately ignores the reader's own registration, because that registration is
forwarded through a single agent whose setter joins and so cannot represent a
frontier below where it already sits. Treating the registration as a bound would
assume away the very ratchet the acquired hold exists for.

G3 needs nothing: a real hold is keyed by arrangement rather than by id, so a
re-export that shares one `TraceBundle` under two ids is covered by a hold on
either. G4 dissolved with the cap, since nothing depends on a frontier report any
more.

Sizes are what make the run exhaustive, so they are the confidence dial. Two
processes is the smallest that can express G1; raising `Procs` and `Times` buys
more and costs state space.

## Notes for editing the spec

* Every controller action needs a progress guard, or its queue grows without
  bound and the state space stops being finite. `CtlCompact` is strictly
  increasing and `CtlDrop` requires a live hold for exactly this reason.
  `QueuesBounded` is a backstop so that a future action missing a guard fails
  loudly rather than running forever.
* Sets must stay homogeneous. TLC canonicalizes a set value by sorting its
  elements, so a set mixing a string sentinel with numbers fails to compare.
  `NoTime` is therefore a configured number outside `Times`.
* `NoTime` is deliberately larger than every element of `Times`, which reads as
  "permits all compaction". Every comparison against a frontier guards on
  `# NoTime` first regardless. The liveness property leans on the ordering too: a
  reclaimed hold satisfies `acquired >= readerHold` the same way a downgraded one
  does, and both are ways of no longer pinning.
* Liveness is checked under the `QueuesBounded` state constraint, which TLC
  applies by pruning states. A constraint can in principle hide a counterexample
  from a temporal property. The progress guards are what keep the queues short, so
  the constraint should never bite; if it starts to, the guards are what to fix,
  not the bound.
* A safety invariant cannot express "eventually reclaimed". Reclaim and downgrade
  are separate steps, so there is always a legal state where the release has been
  applied and the hold is still there. A first attempt stated `NoPermanentPin` as
  an invariant and it failed on exactly that state.
