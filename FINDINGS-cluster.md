# Findings from building the compute surface suite

Observations from building the generated compute-workload suite
(`src/clusterd-test-driver`, `src/transform/src/mirgen.rs`,
`test/clusterd-test-driver/workloads`).

**No product bugs found.** The suite has not yet completed a full end-to-end run:
the runner stalls waiting on the index export's frontier on its first workload,
and that is a bug in the new harness, not in compute. The same `clusterd` and
persist setup runs the hand-written `index.spec` scenario green, which is what
rules out the environment and points at `runner.rs`. So nothing below was found by
the suite *running*; all four were found by *building* it.

Each entry is labelled with what it actually is, because three of the four are not
product defects and it would be misleading to file them as though they were.

---

## 1. `CollationPlan` is dead code

**Kind:** dead code. Not a bug, no behavioural impact.
**Where:** `src/compute-types/src/plan/reduce.rs:389` (struct) and `:404` (its
`impl`).

`CollationPlan`, and its `as_monotonic` method, are never referenced anywhere in
the tree. `ReducePlan` has four variants (`Distinct`, `Accumulable`,
`Hierarchical`, `Basic`) and none of them carries a collation, so nothing can
construct one.

The history is visible in the design: collation is how a reduce over *mixed*
reduction types used to be executed inside the renderer. That job now belongs to
`ReduceReduction` in the optimizer (see finding 2), which splits such a reduce
into one reduce per reduction type and joins the results. The renderer no longer
needs to collate, and the type was left behind.

Found because the surface classifier tried to give collation a coverage cell and
would not compile: there is no `ReducePlan` variant to match on.

**Status:** deleted in its own commit, "compute-types: remove dead
CollationPlan".

---

## 2. A mixed-reduction-type `Reduce` panics the LIR lowering

**Kind:** a landmine, but **SQL-unreachable**. Not a product bug on a correct
build.
**Where:** `src/compute-types/src/plan/reduce.rs:433`,
`assert_eq!(..., "Multiple reduction types detected")` in
`ReducePlan::create_from`.

Lowering a `Reduce` whose aggregates span more than one `ReductionType` (for
example `max(a)`, which is `Hierarchical`, alongside `sum(b)`, which is
`Accumulable`) aborts the process on an `assert_eq!`. The precondition is that
`ReduceReduction` has already split the reduce.

**Why this is not a product bug:** `ReduceReduction` sits unconditionally in
`Optimizer::logical_optimizer`'s `fixpoint_logical_02`
(`src/transform/src/lib.rs:796`), behind no feature flag. Every SQL path through
the optimizer therefore splits a mixed reduce before lowering sees it. I checked
this specifically because if the transform *were* flag-gated, the same assert
would be a panic reachable from a plain `SELECT max(a), sum(b) FROM t GROUP BY k`,
which would be serious. It is not.

**What is a real (if minor) defect** is the consequence for
`src/clusterd-test-driver`, which is pre-existing checked-in code. Its
`DataflowBuilder::finish` documents:

> Returns an error rather than panicking on a malformed plan (e.g. a key column
> out of range, or an unbalanced object graph), so a caller driving this from
> external input — notably the script reader — can surface a clean error instead
> of crashing the process.

That contract does not hold. A `.spec` script whose `define` block contains a
mixed-type reduce, without `optimize`, aborts the driver process instead of
reporting `error: ...` in its golden block. The panic is inside `compute-types`
lowering, so `finish` cannot catch it.

**Suggested action:** either have `ReducePlan::create_from` return a `Result` (a
wider change, it is on the optimizer's hot path), or screen for the shape in the
driver before lowering. The suite's generator currently screens for it
(`needs_optimizer` in `src/clusterd-test-driver/src/generate.rs`), which fixes the
generated corpus but not the hand-written script path.

---

## 3. `fold_to_multiset` conflated "folded to an error" with "did not fold"

**Kind:** a latent weakness in test tooling that I introduced a fix for. Affected
the three existing `mz-transform` fuzz targets.
**Where:** was `src/transform/fuzz/src/lib.rs`, now
`src/transform/src/mirgen.rs`.

The `FoldConstants` result-equivalence oracle shared by
`full_optimizer_equiv`, `optimizer_symbolic_equiv`, and
`mir_relation_transforms` returned `Option<multiset>`, mapping both "the plan
reduced to an `EvalError`" and "the plan did not reduce to a constant" to `None`.
Both cases were then skipped.

The error case has a perfectly good expected result, namely the same error, so
skipping it silently removes every error-propagating plan from the oracle's
reach. Given that `gen_scalar` deliberately emits `DivisionByZero` poison
literals to exercise error propagation, a meaningful fraction of generated plans
were landing in the skipped bucket.

**Action taken:** added `FoldOutcome { Rows, Error, Unfoldable }` and
`fold_outcome`, keeping `fold_to_multiset` as a wrapper so the existing fuzz
targets are unchanged in behaviour. The three targets could now distinguish the
error case and assert on it; they do not yet.

---

## 4. Error-propagating plans are now result-checked

**Kind:** a gap in the new suite, since closed.

A plan whose scalars evaluate to an error folds to an `EvalError`, and the
renderer routes it to the `err` collection so the read reports it. Both sides
agree, but the runner originally treated a failed read as a harness failure, so
those workloads could not carry the fold oracle.

`Driver::peek_result` and `Driver::await_subscribe_result` now report a collection
error as a value (`ReadResult::Error`) while leaving genuine failures (timeouts,
dropped connections, a cancelled peek) in the error channel. The fold oracle
requires an erroring plan to produce that same error rather than rows, and reports
a message difference between the folder and the renderer as its own case, since
the renderer wraps an `EvalError` in a `DataflowError` and the two spellings can
legitimately differ. All 16 corpus workloads now carry the fold oracle, up from
15.

The remaining gaps are the nine surface cells random MIR cannot reach, listed with
causes in `KNOWN_GAPS` in `src/clusterd-test-driver/src/generate.rs`.
