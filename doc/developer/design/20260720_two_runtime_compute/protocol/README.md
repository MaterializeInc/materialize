# Two-runtime compute protocol model

A Lean 4 model of the command protocol between the compute controller and a
`clusterd` process hosting two compute runtimes. `Protocol/TwoRuntime.lean` is
the whole model, about 400 lines including its commentary.

Run it:

```
ci/test/lean-protocol.sh
```

That builds a Lean toolchain image and checks every theorem. The library sets
`warningAsError`, so an unproved goal fails the build rather than passing with a
warning. Pass a command to poke around inside the image instead, for example
`ci/test/lean-protocol.sh lake env lean Protocol/TwoRuntime.lean`.

## What it proves

* `since_le_as_of`: an index is never compacted past the `as_of` of a dataflow
  that has been created and has not yet rendered. This is the invariant the
  runtime split loses, because `CreateDataflow` reaches only the interactive
  runtime while `AllowCompaction` reaches only maintenance.
* `physical_le_since`: the publisher never forwards a physical compaction
  frontier beyond the published `since`, which is what keeps a cut available for
  every reader the controller may admit.
* `no_regression`: a compaction frontier in flight never regresses what
  maintenance has already applied.

All three are corollaries of one inductive invariant, `Inv`, proved preserved by
every step in `inv_step`.

## What it refutes

`Step` takes two booleans selecting behaviours the implementation used to have,
and each gets a counterexample:

* `release_on_drop_violates_invariant`: retiring the multiplexer's hold when the
  controller enqueues the dataflow's drop, rather than when the interactive
  runtime confirms it rendered. Four steps: create at 2, drop, allow compaction
  to 3, apply it.
* `physical_from_upper_violates_invariant`: forwarding the stream `upper` as the
  publisher's physical compaction target.

A model that can only express the fixed system cannot tell you it fixed
anything. These two are the reason the parameters exist rather than the fixed
behaviour being hardcoded.

## Why Lean rather than TLA+

This replaces a TLA+ model that stated the same central invariant. That model was
never checked: the repository has no TLC runner and no CI job, and it turned out
to contain a four-step counterexample to its own stated property, the same one
`release_on_drop_violates_invariant` now records. A stated-but-unchecked property
is worse than none, because it reads as assurance.

In Lean an unproved goal is a build failure, so the failure mode that produced
that situation is structurally impossible. The proof is also unbounded in time
rather than checked over a small finite `Times` set.

The cost is real and worth stating. TLC finds counterexamples automatically,
which is how the original bug would have been caught had anyone run it. Lean
requires the counterexample to be constructed by hand, so the two theorems above
are only as good as the imagination that produced them. This model certifies a
fix. It does not search for the next bug.

## Scope

Modelled: one index, one interactive dataflow, the controller's read hold, the
multiplexer's cap, two independent command queues, and the point at which each
runtime realizes a command.

Not modelled: data, rendering cost, failure and reconnection, more than one
dataflow or index, and liveness. In particular `muxFlush` is an action that *may*
fire, so nothing here says a deferred compaction is eventually forwarded, and
`Multiplexer::reset` is out of scope because it concerns a second connection.

This is a model of the protocol, not of the Rust. It does not verify
`mz_compute_client::multiplex` or `mz_compute::shared_trace`. It establishes that
the algorithm they implement is sound, and that two algorithms they used to
implement were not.

## Infrastructure

The `Dockerfile` follows the pattern in `doc/developer/semantics/Dockerfile`
(elan installed system-wide so any uid works under `--user`, the Lean version
passed as a build arg from `lean-toolchain`, `safe.directory` set so lake does
not re-resolve). It is simpler in one respect: this model depends on core Lean
only, so there is no Mathlib olean cache to bake.

Do not bind-mount `/workspace` itself. That masks the resolved `.lake` directory.
Mount `Protocol/` and `Protocol.lean` only, as the CI script does.
