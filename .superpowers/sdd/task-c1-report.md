# Task C1 report: Coalesce Map/Filter/Project at raise time (workstream C)

## Status

DONE

## Commit

(see below -- commit pending in this session)

## Reuse entry point

Hand-rolled trio: `MapFilterProject::extract_non_errors_from_expr_mut` +
`MapFilterProject::optimize` + custom `rebuild_mfp_canonical`.

`CanonicalizeMfp::action` was not directly reusable because it calls
`fusion::filter::Filter::action` inside `rebuild_mfp`, which calls
`canonicalize_predicates` which panics in debug/test builds on non-boolean
predicates. The eqsat test-only inputs use column-reference predicates
(e.g. `Filter (#0)` on an Int64 column) that are not boolean-typed, so
`CanonicalizeMfp` cannot be called on the raw raised output. Instead,
`rebuild_mfp_canonical` mirrors `CanonicalizeMfp::rebuild_mfp` but omits
the `Filter::action` call.

## TDD evidence

**RED:** Adding `use super::coalesce_mfp` before the function existed produced
`E0432: unresolved import` compile error -- test failed to compile.

**GREEN:** After implementing `coalesce_mfp`:

```
running 7 tests
test eqsat::raise::tests::roundtrip_binary_union ... ok
test eqsat::raise::tests::roundtrip_filter_over_constant ... ok
test eqsat::raise::tests::roundtrip_let_binding ... ok
test eqsat::raise::tests::roundtrip_join_of_two_bases ... ok
test eqsat::raise::tests::roundtrip_map_over_constant ... ok
test eqsat::raise::tests::roundtrip_unsupported_is_identity ... ok
test eqsat::raise::tests::coalesce_fuses_nested_filter_map_filter ... ok

test result: ok. 7 passed; 0 failed
```

## wcoj result

PASS (4/4):
```
test delta_query_survives_join_implementation ... ok
test egraph_picks_wcoj_for_triangle ... ok
test triangle_raises_to_delta_query ... ok
test eqsat_logical_optimizer_leaves_joins_unimplemented ... ok
```

## Datadriven diff summary

No changes to `eqsat.spec`. The existing test cases are either:
- Single M/F/P layers (already canonical, no change after coalescing)
- Chains with out-of-bounds Project references (case (f): `map_columns_to_projection`
  rule produces `Project([0,1,2]) / Filter / Get t0` where Get has 2 columns;
  `mfp_chain_valid` guards skip those chains safely)

## arithmetic.slt result

The SLT test panics during catalog initialization with a pre-existing bug:
`Project (#1..=#5) / Get s437` where Get s437 has arity 1 (jsonb). This type
error in `mz_transform::typecheck` was present before this change (verified by
stashing changes and running SLT with old binary -- same panic). My changes
do not cause this regression; `mfp_chain_valid` correctly skips these invalid
chains.

## Files changed

- `src/transform/src/eqsat/raise.rs` -- added `coalesce_mfp`, `mfp_chain_valid`,
  `coalesce_mfp_children_of_base`, `rebuild_mfp_canonical`; added
  `coalesce_fuses_nested_filter_map_filter` unit test.
- `src/transform/src/eqsat.rs` -- `optimize_inner` now calls `coalesce_mfp`
  on the raised expression before returning.

## Concerns

1. **Pre-existing invalid MIR from `map_columns_to_projection`:** The eqsat
   rule produces `Project([i]) / (no Map)` chains where column `i` is the Map's
   own output column (self-referential). `mfp_chain_valid` guards against this
   so coalescing is safe, but the underlying invalid plan still passes through
   to the downstream pipeline. The arithmetic.slt panic is a symptom of this
   pre-existing bug in the eqsat pass, not a regression from this change.

2. **`rebuild_mfp_canonical` does not call `fusion::filter::Filter::action`:**
   This means adjacent Filter nodes from `mfp.optimize()` output are NOT
   further fused by the filter-fusion pass. In practice `mfp.optimize()` already
   deduplicates and sorts predicates, so no additional fusion is needed within
   a single coalescing step. The downstream `CanonicalizeMfp` pass handles the
   live (boolean-typed) pipeline and calls `Filter::action` there.
