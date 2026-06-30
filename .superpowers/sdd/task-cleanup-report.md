# WMR-lift doc/test cleanup report

## Change 1: doc wording in `cse.rs` (~line 312)

Old: "appear in two or more binding value subtrees"

New: "appear two or more times among the binding values (repeats within a single binding value count)"

The original text implied the candidate must appear across at least two distinct bindings.
`count_letrec_candidates` counts total occurrences, so two occurrences within a single binding
also qualify.
The new text matches the implementation without overstating the requirement.

## Change 2: add Typecheck to `loop_invariant_subterm_hoisted_out` test

File: `src/transform/tests/eqsat_wmr_lift.rs`

Added `use mz_transform::eqsat::raise;` import.

Updated `versioned_local` to populate `get: Some(Box::new(local_get(...)))` so that raise can
reconstruct the node verbatim (matching what `lower` produces from a real `MirRelationExpr::LetRec`).
Without this, raise panics when it encounters a `LocalGet { get: None }` node that has no scope
entry.

After the structural assertion in `loop_invariant_subterm_hoisted_out`, added:
```rust
let mut checked = raise::raise(&result, false, &BTreeMap::new());
Typecheck::new(std::sync::Arc::clone(&ctx))
    .transform(&mut checked, &mut transform_ctx)
    .expect("raised plan must pass strict Typecheck");
```
This mirrors the sibling test `cross_binding_subexpression_is_shared` exactly.

## Change 3: reconcile contradictory comments in `cse.rs` (~lines 467-476)

Old (lines 467-471): claimed the `.rev()` is needed so "each outer Let's value can reference
inner ones via their LocalGet placeholders" — describing a case that cannot occur, since all
invariant bindings are closed (as the NOTE immediately below correctly stated).

New: trimmed to "Reversing puts the largest invariant binding innermost (closest to the LetRec),
consistent with the outer CSE's binding order." The NOTE is preserved unchanged, explaining why
scope correctness does not depend on order. The two comments are now consistent.

## Test output

```
Nextest run ID ebc17f34-8aa8-4b3d-b75f-12de585b4d03 with nextest profile: default
    Starting 5 tests across 1 binary
        PASS [   0.013s] mz-transform::eqsat_wmr_lift wmr_lift_reduces_physical_arrangement_count
        PASS [   0.013s] mz-transform::eqsat_wmr_lift wmr_lift_does_not_increase_arrangement_count
        PASS [   0.013s] mz-transform::eqsat_wmr_lift versioned_localgets_get_distinct_eclasses
        PASS [   0.014s] mz-transform::eqsat_wmr_lift loop_invariant_subterm_hoisted_out
        PASS [   0.015s] mz-transform::eqsat_wmr_lift cross_binding_subexpression_is_shared
    Summary [   0.016s] 5 tests run: 5 passed, 0 skipped
```
