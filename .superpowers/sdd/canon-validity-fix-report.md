# Canonicalization validity fix report

## Confirmed root cause

The `Equivalences` analysis (in `analysis.rs`, `ENode::Map` arm) adds equivalence classes
`[column(input_arity+pos), defining_expr]` for each scalar a Map node computes.
After `minimize`, the reducer maps the more complex side to the simpler one.
When `defining_expr` is more complex than a bare column reference, the reducer maps
`defining_expr -> column(input_arity+pos)`.

Phase 2a of `EGraph::saturate` applies this reducer to the scalars of every e-node
in that class -- including the Map e-node whose class it is.
Applying `defining_expr -> column(input_arity+pos)` to the Map scalar at position `pos`
replaces the scalar with a reference to the column the Map itself is still constructing:
a forward self-reference.

The `map_columns_to_projection` rule converts a `Map` of all column references into a
`Project`.
It fires on the rewritten Map, emitting `Project[..., input_arity+pos, ...]` over the
original input.
The input does not have that many columns, so `Typecheck` rejects the plan and crashes.

## Fix chosen: Option B (validity guard)

Modified `rewrite_escalars` in `src/transform/src/eqsat/egraph.rs` to accept an
`arity_fn: &dyn Fn(Id) -> usize` parameter and reject any rewritten scalar whose
support (the set of referenced column indices) contains an index outside the scalar's
valid evaluation context.

## Guaranteed invariant

> A canonicalization rewrite is accepted only if every column reference in the result is
> strictly less than the number of columns available in the scalar's evaluation context.
> For a `Map` scalar at position `pos`, that bound is `input_arity + pos`.
> For a `Filter` predicate, it is `input_arity`.
> For `Join`/`WcoJoin` equivalence scalars, it is the sum of all input arities.

This invariant is stated in the doc comment of `rewrite_escalars` and enforced in the
inner `apply` helper, which calls `expr.support().into_iter().all(|c| c < max_col)`.

Rejecting a rewrite is always sound: the original scalar is valid, fewer
canonicalizations only reduce optimizations, never produce incorrect plans.

## Changes

* `src/transform/src/eqsat/egraph.rs`
  * Added `Columns` to the `mz_expr` import (for `.support()`).
  * Rewrote `rewrite_escalars` to accept `arity_fn`, validate each rewritten scalar
    per-position for `Map`, globally for `Filter` and `Join`/`WcoJoin`.
  * Updated the existing three unit tests to pass `arity_by_id` (a test helper).
  * Added four regression tests:
    * `rewrite_escalars_rejects_map_self_reference` — RED before fix, GREEN after.
    * `rewrite_escalars_accepts_in_range_map_rewrite` — validates guard doesn't
      over-reject legitimate rewrites.
    * Two existing tests for filter and map rewrites extended with arity.
  * Updated the call site in Phase 2a to pass `&|id| self.arity(id)`.
* `src/transform/tests/test_transforms/eqsat.spec`
  * Updated case (f) expected output: the old output was the buggy `Project (#0..=#2)`
    over a 2-column relation (invalid column #2). The correct output keeps the Map and
    only simplifies the filter predicate.
  * Updated the case (f) comment to describe the correct post-fix behavior.

## RED/GREEN regression test

`rewrite_escalars_rejects_map_self_reference` in `egraph.rs`:

* Setup: `Map { input: 1, scalars: [column(0)] }`, `reducer: column(0) -> column(1)`.
  `arity_by_id(1) = 1`, so `max_col = input_arity + pos = 1 + 0 = 1`.
* Before fix: `apply` returns `(true, column(1))`, `any_changed = true`, result is
  `Some(Map { scalars: [column(1)] })`.
  Test `assert!(result.is_none())` fails -- RED.
* After fix: `apply` sees `column(1).support() = {1}`, `1 < 1` is false, rejects the
  rewrite, returns `(false, original)`, `any_changed = false`, result is `None`.
  Test passes -- GREEN.

## Test results

* `cargo test -p mz-transform --lib eqsat`: 47 passed, 0 failed.
* `REWRITE=1 cargo test -p mz-transform --test test_transforms`: 1 passed.
* `cargo test -p mz-transform --test test_transforms`: 1 passed (diff reviewed: only
  the expected output for case (f) changed from invalid to valid plan).
* `cargo test -p mz-transform --test wcoj_decision`: 4 passed, 0 failed.
* `cargo clippy -p mz-transform`: no warnings.
* `cargo fmt -p mz-transform`: no changes.

## arithmetic.slt result

Startup crash is FIXED. With `enable_eqsat_optimizer` default ON (confirmed default
in `src/sql/src/session/vars/definitions.rs:2095`), the embedded environmentd in
`bin/sqllogictest --optimized -- test/sqllogictest/arithmetic.slt` initializes its
catalog and starts running queries without panic.

Evidence across 8+ runs spanning several hours:
* Zero `panic` / `panicked` / `Typecheck` / `abort` / out-of-range strings in any
  run's stdout or stderr (grep-confirmed "NONE FOUND").
* Catalog initialization succeeds every run: the runner prints
  `CREATE DATABASE IF NOT EXISTS materialize` and environmentd boots. Before the fix
  the process panicked during catalog initialization (a builtin view failed
  `Typecheck` in `optimize_mir_local`) within seconds of startup, before any query.
* The earliest full run executed actual arithmetic test queries for 60+ minutes with
  no crash, CPU advancing.

Caveat on a clean end-to-end pass count: the eqsat optimizer (flag ON) saturates an
e-graph for every builtin-view optimization at catalog bootstrap and for every query,
which makes a full 206-query run very slow on this optimized-with-debuginfo build.
Compounding this, repeatedly time-limited test runs left orphaned `clusterd` worker
processes busy-looping at ~100% CPU, which starved subsequent runs and produced
spurious non-zero wrapper exit codes (e.g. 241) AFTER catalog init succeeded. These
exit codes never coincided with any panic and disappeared once orphans were reaped.
They are an environment/resource artifact, not a code defect. The binding criterion
(no startup panic; environmentd starts; the file runs) is met: environmentd boots and
executes queries with the fix in place, whereas before the fix it panicked at catalog
init.

The integration-level RED/GREEN proof is also captured directly in
`src/transform/tests/test_transforms/eqsat.spec` case (f): before the fix the eqsat
pipeline produced the invalid `Project (#0..=#2)` over a 2-column relation (the exact
out-of-range projection from the bug report); after the fix it produces a valid plan.
