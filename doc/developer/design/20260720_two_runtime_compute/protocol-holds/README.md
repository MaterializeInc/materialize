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

## What is checked

`Holds.cfg` runs the proposed design, `Mechanism = "acquire"`: the multiplexer
emits `AcquireHolds` to maintenance's ordered stream, and the release travels on
interactive's stream so it cannot overtake the create. Invariants: `TypeOK`, `I1`,
`NoRegression`, `LagNeverExposes`. Two processes, three timestamps, no
reconnection. 7659 distinct states, depth 17.

`HoldsCap.cfg` runs the retired design, `Mechanism = "cap"`: the multiplexer caps
`AllowCompaction` and retires the cap when interactive reports a frontier, which
is what the code does today. It is **expected to violate `I1`**, and
`ci/test/tla-holds.sh` fails if it stops doing so. Without that, a green run of
`Holds.cfg` would prove nothing: a model that can only express the proposed design
cannot tell you the design fixed anything.

## What the model already earned

The first run refuted the design as originally written. The release was to travel
on maintenance's stream, and TLC found in nine steps that it can then overtake a
create interactive has not processed: maintenance applies acquire, release and
compaction while the dataflow is still queued, and the dataflow then renders
against compacted data. Hence the release is on interactive's stream, and
maintenance reclaims its hold by observing the registration disappear. That needs
`everRegistered`, because otherwise "no registration" is ambiguous between "the
create has not been processed yet" and "the reader is finished", and reclaiming in
the first case is the same defect again.

## What is NOT checked

**G2, the epoch boundary.** `MaxEpochs = 0`, so `Hello` never fires. Modelling a
reconnection faithfully needs the holder identity to be epoch-scoped, because a
replayed dataflow gets a fresh transient id and one holder shared across epochs
conflates two different dataflows. An earlier attempt did fire `Hello` and produced
a counterexample that was an artifact of the action resetting replica state that a
real `Hello` does not touch. This is the largest remaining gap and G2 is a
confirmed defect in the current implementation, so it should be modelled before
the design is built.

Also unmodelled: more than one interactive dataflow, more than one collection, the
re-export aliasing that lets one publication point answer to two ids (G3), the
retirement signal that subscribe collections never emit (G4), and any liveness
property. `NoPermanentPin` is defined in the spec but not in either config, since
it needs the release path to be fair and fairness is not stated.

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
  `# NoTime` first regardless.
