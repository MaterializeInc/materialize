# Architecture review — `compute::render`

Scope: `src/compute/src/render/` (8,425 LOC).

## 1. `Correction` enum — parallel dispatch without a trait

**Files**
- `src/compute/src/sink/correction.rs:46-120` — `Correction<D>` enum with
  variants `V1(CorrectionV1<D>)` and `V2(CorrectionV2<D>)`; every method is a
  two-arm `match self` that delegates identically to whichever variant is live.

**Problem**
`CorrectionV1` (695 LOC) and `CorrectionV2` (1,378 LOC) expose the same five
methods (`insert`, `insert_negated`, `updates_before`, `advance_since`,
`consolidate_at_since`) but there is no shared trait. The `Correction` wrapper
is a manual dispatch shim: each method repeats the same `match self { V1(c) =>
c.method(..), V2(c) => c.method(..) }` pattern five times. Adding a new method
or changing a signature requires touching all three structs in lockstep.

The comment at `correction.rs:43-45` confirms this is intentional migration
scaffolding: "remove V1 eventually." The Deletion Test therefore passes only if
the V1-only capabilities are confirmed absent in V2 before removal (spill-to-
disk is V2 only; the question is whether V2 covers all edge cases V1's
`BTreeMap`-per-time storage handles).

**Solution sketch**
Extract a `CorrectionBuffer<D>` trait with the five methods and implement it on
both `CorrectionV1` and `CorrectionV2`. Replace `Correction<D>` with
`Box<dyn CorrectionBuffer<D>>` or an `enum_dispatch` derive. Each new method
lives in one place per implementation; the shim collapses to zero lines.

**Risk**
`updates_before` returns `Box<dyn Iterator<...> + '_>` — the lifetime bound
already forces heap allocation, so a trait object adds no regression. Verify
that `D: Data` is object-safe before committing to `dyn CorrectionBuffer`.

**Actionable pre-deletion checklist (add as TODO at `correction.rs:46`)**
1. `STORAGE_UPSERT_PREVENT_SNAPSHOT_BUFFERING`-equivalent for MV sink — present
   in V2?
2. Metrics parity: `ArrangementHeapSize` logging in `CorrectionV1::drop` — does
   V2 log equivalently?
3. `ConsolidatingVec` (re-exported from `correction.rs`) — consumers use it
   outside the sink; ensure they don't rely on V1 internals.

## 2. (Honest skip) `render_plan_expr` match dispatch in `render.rs`

`render_plan_expr` (line 1164) is a single `match` on `render_plan::Expr`
variants that delegates to submodule functions. Each variant maps to exactly one
file/function. The Deletion Test fails: collapsing this into a trait would be
relocation, not deepening — the match is the *correct* seam between the plan IR
and the operator renderers. Keep.

## 3. (Honest skip) `reduce.rs` at 2,460 LOC

`ReducePlan` has four genuinely distinct strategies (accumulable, hierarchical,
basic, collation). Each strategy interacts with different differential operators
and cannot share logic. The file is large by count but not by complexity:
splitting it would require cross-file references for the internal
`ReduceCollation` join. No deepening available.

## What this review did not reach

- `continual_task.rs` (1,109 LOC) — the delta-source + output-reader + persist-
  sink wiring for continual tasks is non-trivial; worth checking whether it
  duplicates logic from `sink/materialized_view.rs`'s persist write path.
- `join/mz_join_core.rs` (934 LOC) — a custom join operator; should be compared
  against differential's built-in join core to validate why the replacement was
  needed and whether it is still differentiated.
