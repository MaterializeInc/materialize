# Cutting the work into landable pieces

Four pieces, in dependency order. The shape follows from two things measurement settled:
the peek-placement question is orthogonal to the interactive runtime and is
[parked pending an experiment](peek-placement.md), and `Arc`-backed batches are what both
of the others need.

```
design docs (this PR)          no dependencies, lands first

#37881, Arc-backed spines      gates both of the below
       |
       +---- #37884 role and metrics ---- interactive runtime
       |
       +---- peek placement (draft, parked)
```

## The dependency that is easy to miss

The peek work is independent of the interactive runtime but **not** of the `Arc` migration.
`LocalSnapshot` hands a cursor to a blocking task, and it can only be `Send` because it owns
`Arc<Batch>` cursors outright: the traces this crate maintains are read through an `Rc`-based
reader, and owning `Rc` batches would be no more `Send` than borrowing them. So both children
of #37881 need it, and neither needs the other.

## The pieces

**Design docs.** This directory plus the two model checks (`ci/test/tla-holds.sh`,
`ci/test/lean-protocol.sh`) and their pipeline entries. The models travel with the docs
because a model nobody runs is not a check. Lands first: no code, no conflict surface, and
the other three cite it.

**#37881, `Arc`-backed production spines.** About 470 lines across 7 files, including the
`relations.slt` golden, which prints batch type names and therefore churns. Gates everything
else. Note it also ports the Iceberg sink's batch stash, since that sink consumes the sink
trace and appeared upstream after the branch was cut.

**#37884, `ComputeRuntimeRole` and role-labeled metrics.** About 230 lines. `Interactive` is
`#[cfg(test)]` until the runtime constructs it, because the label's whole purpose is that two
named roles coexist in one process registry, and `Solo` cannot stand in: it registers the same
metric names without a `role` label, so prometheus rejects the pair for differing label
dimensions rather than recording a second series.

**The interactive runtime.** The bulk. Arrangement sharing (`sharing.rs`, `shared_trace.rs`),
rendering index imports from the registry, the second `serve`, the command multiplexer with
broadcast compaction and the standing hold, and the test harness. Its case is E7 and E9,
temporary dataflows under maintenance load and a replica that stays introspectable while it
hydrates. Not E1 or E11: E2 showed those belong to the walk substrate.

**Peek placement, as a draft.** `local_snapshot.rs`, `PendingPeek::IndexOffload`, the
substrate parameter and its metric. Opened as a draft rather than for review, because the
[deciding experiment](peek-placement.md) may retire it, and a merged mechanism is harder to
delete than an unmerged one. The code has to exist for the experiment to run, which is why it
is a branch rather than nothing.

## Notes on order

Each piece is one semantic change, and the repository squash-merges, so intra-branch commit
structure never reaches `main`. What matters is that each piece compiles and tests on its own.

GitHub's stacked-PR support does not apply here. These branches live on a fork while the base
repository is upstream, so a PR's base cannot be retargeted at another PR's branch. In
practice that means sequential landing: a prerequisite's commits stay visible in the
dependent's diff until it merges, then the dependent rebases.

Land the low-conflict pieces early. The longer a tail branch lives, the more likely an
unrelated change touches the same files and turns a clean rebase into a real conflict, which
is how the Iceberg port became necessary in the first place.
