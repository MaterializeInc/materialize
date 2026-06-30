# C-T1 review fix report

## I1 decision: reuse `CanonicalizeMfp::rebuild_mfp`

**Decision:** reuse the production `CanonicalizeMfp::rebuild_mfp` (delete the hand-rolled `rebuild_mfp_canonical`).

**Why:** Non-boolean filter predicates come ONLY from test fixtures, not from the eqsat engine on real plans.
Real MIR invariant: all `Filter` predicates are boolean-typed (enforced by the SQL frontend and type system).
The eqsat engine only processes real MIR (lowered from valid SQL), so its Filter predicates are always boolean.
The panic in `canonicalize_predicates` was caused by three categories of test-only constructs:

* `raise.rs` unit test `roundtrip_filter_over_constant`: used `MirScalarExpr::column(0)` (Int64) as a filter predicate on a `base(2)` constant with Int64 columns.
* `roundtrip.rs` tests `merges_nested_filters` and `column_ref_lit_is_unaffected`: used bare Int64 column references as filter predicates.
* `eqsat.spec` cases (a), (b), (c): used `Filter (#0)` / `Filter (#1)` on `t0` sources with `bigint` columns.

All fixes are test-only changes; no production code besides removing the hand-rolled function and adding the `CanonicalizeMfp` import.

**Fixes applied:**

* `raise.rs` `roundtrip_filter_over_constant`: changed predicate to `#0 = #1` (column equality, boolean, not constant-foldable).
* `roundtrip.rs` `merges_nested_filters`: changed to use `call_is_null()` on nullable columns.
* `roundtrip.rs` `column_ref_lit_is_unaffected`: changed to use `call_is_null()` on nullable column, preserved the semantic intent.
* `eqsat.spec` cases (a), (b), (c): changed to use `(#0 = 1)` and `(#1 = 1)` integer comparisons (boolean, not constant-foldable since columns are runtime values).

## Datadriven witness (I2)

Case (i) added to `eqsat.spec`:

```
apply pipeline=eqsat
Filter (#2 = 0)
  Map (#0 + 1)
    Filter (#0 = #1)
      Get t0
----
Filter (#2 = 0) AND (#0 = #1)
  Map ((#0 + 1))
    Get t0
```

The outer `Filter (#2 = 0)` references map-added column #2, so `push_filter_through_map` cannot push it past the Map.
The eqsat rules leave the plan as Filter/Map/Filter/Get (non-canonical: 3 MFP operators).
`coalesce_mfp` extracts the maximal MFP run, optimizes it, and re-emits it via `CanonicalizeMfp::rebuild_mfp`.
The output has one Map and one merged Filter (2 MFP operators in canonical order): coalescing demonstrably fires.

## Module doc comment (minor)

Updated the `raise.rs` module doc comment to state that the round-trip is now
"semantics-preserving, scalar-canonicalizing, and MFP-canonicalizing" and
explains that `coalesce_mfp` reuses the production `CanonicalizeMfp` machinery.

## Test results

* `cargo test -p mz-transform --lib eqsat`: 49 passed, 0 failed.
* `cargo test -p mz-transform --test roundtrip`: 22 passed, 0 failed.
* `cargo test -p mz-transform --test test_transforms`: 1 passed, 0 failed.
* `cargo test -p mz-transform --test wcoj_decision`: 4 passed, 0 failed.
* `cargo clippy -p mz-transform`: clean (0 warnings).

## arithmetic.slt gate

Run: `bin/sqllogictest --optimized -- test/sqllogictest/arithmetic.slt`
Result: `PASS: success=206 total=206`, zero "Non-positive multiplicity" errors.
