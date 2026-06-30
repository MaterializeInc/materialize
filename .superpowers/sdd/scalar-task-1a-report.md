# Task 1a report: scalar e-class analyses

Status: DONE
Commit: 280d6432f1
Test summary: 22/22 pass (10 new analysis tests, 12 existing round-trip tests)

## What was implemented

Created `src/transform/src/eqsat/scalar/analysis.rs` with:
- `ClassAnalysis { could_error: bool, literal: Option<(Result<Row, EvalError>, ReprColumnType)> }`
  with `#[derive(Debug)]`.
- `make(node, store, find) -> ClassAnalysis`: computes a node's contribution,
  reading children from `store` via the `find` closure. Mirrors
  `MirScalarExpr::could_error` at scalar.rs:1226 exactly.
- `merge(a, b) -> ClassAnalysis`: OR for `could_error`, `a.literal.or(b.literal)`
  for `literal`.

Modified `src/transform/src/eqsat/scalar/egraph.rs`:
- Added `analysis: HashMap<Id, ClassAnalysis>` field (no longer derives `Debug`
  since `ClassAnalysis` has its own `Debug`; verified no consumers used
  `ScalarEGraph`'s `Debug`).
- `add`: computes and inserts analysis for the new class immediately.
- `union`: merges and updates analyses when folding rb into ra.
- `rebuild`: after the congruence fixpoint and hashcons rebuild, clears and
  recomputes all analyses via a fixpoint loop (handles unknown topological
  order).
- Added `pub fn analysis(&self, id: Id) -> &ClassAnalysis` accessor (panics on
  unknown id; documented).

Modified `src/transform/src/eqsat/scalar.rs`: added `pub mod analysis;`.

## Decisions made

- `arity` omitted from `ClassAnalysis` per brief direction; comment in struct
  explains why.
- `ScalarEGraph` no longer derives `Debug` (the derive required all fields to
  impl `Debug`; `ClassAnalysis` derives `Debug` independently). No callers used
  `ScalarEGraph`'s `Debug`.
- The `rebuild` analysis recompute uses a fixpoint loop (simple, correct). An
  alternative topological sort was considered but skipped per brief's "keep it
  simple and correct over clever" guidance.
- `BitAndInt64` chosen as the known non-erroring binary func in tests (`a & b`
  on i64 is infallible). `AddInt64` chosen as the known erroring func (can
  overflow). Both verified via the func's `could_error()` method.

## Concerns

None.
The lint failure in `check-whitespace.sh` is pre-existing (trailing newlines
in `.superpowers/sdd/task-*.md` files committed before this task).
