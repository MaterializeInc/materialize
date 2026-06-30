# Task 2 Report: Part A wiring — emit hints from `commit_differential` (introduces `NativeJoinFlags`)

## Files Changed

### `src/transform/src/eqsat/raise.rs`
- Added `NativeJoinFlags` struct (pub, with `commit: bool` and `prioritize_arranged: bool` fields and `none()` constructor).
- Changed `raise` and `raise_inner` from taking `native_join_commit: bool` to taking `flags: NativeJoinFlags`.
- Updated the inner closure in `raise_inner` to thread `flags` through recursion.
- Changed the guard `!native_join_commit` to `!flags.commit`.
- Updated `commit_differential` call to pass `&raised_inputs` and `flags.prioritize_arranged`.
- Updated all `raise` call sites in the test module to use `NativeJoinFlags::none()`.

### `src/transform/src/eqsat/join_commit.rs`
- Widened `commit_differential` signature: added `inputs: &[MirRelationExpr]` and `prioritize_arranged: bool` params.
- Replaced the `(s.input, key, None)` order-tuple construction with a call to `step_characteristics` for each step.
- Added start-key recomputation (after the start-key alignment block) to reflect the aligned key in the characteristics.
- Updated both `commit_differential` unit tests to clone `inputs` before `join_scalars` consumes it, and pass `false` for `prioritize_arranged`.
- Added the missing `unique_key: true` assertion to `step_characteristics_reports_arranged_unique_and_len`.

### `src/transform/src/eqsat.rs`
- Changed `optimize_with_availability` param from `native_join_commit: bool` to `flags: crate::eqsat::raise::NativeJoinFlags`.
- Changed `optimize_inner` param from `native_join_commit: bool` to `flags: crate::eqsat::raise::NativeJoinFlags`.
- Updated `optimize`, `optimize_without_let_union`, `optimize_logical`, `optimize_with_wmr_lift` to pass `raise::NativeJoinFlags::none()`.
- Updated `optimize_inner` to pass `flags` to `raise::raise`.

### `src/transform/src/eqsat/transform.rs`
- Updated the real call site (PhysicalEqSatTransform) to build `NativeJoinFlags { commit: ..., prioritize_arranged: ... }` from `ctx.features`.
- Updated all 5 test call sites to use `crate::eqsat::raise::NativeJoinFlags::none()`.

### `src/transform/src/eqsat/validation.rs` (incidentally fixed)
- Updated `raise` call to use `NativeJoinFlags::none()` (required by signature change).

### `src/transform/tests/eqsat_wmr_lift.rs` (incidentally fixed)
- Updated `raise::raise` call to use `raise::NativeJoinFlags::none()`.

### `test/sqllogictest/transform/` (14 goldens regenerated, see below)

## NativeJoinFlags Definition

```rust
/// Flags controlling the physical join-commit path. Replaces the former lone
/// `native_join_commit` bool so Part A (`prioritize_arranged`) and Part B
/// (`eager_delta`, added later) thread without growing the positional arg list.
#[derive(Clone, Copy, Debug)]
pub struct NativeJoinFlags {
    /// Commit acyclic joins to a cost-model Differential/DeltaQuery so
    /// `JoinImplementation` no-ops on them.
    pub commit: bool,
    /// Select the V2 `JoinInputCharacteristics` layout (`enable_join_prioritize_arranged`).
    pub prioritize_arranged: bool,
}

impl NativeJoinFlags {
    /// All-off: no native commit. Used by logical/offline entry points and tests.
    pub fn none() -> Self {
        NativeJoinFlags { commit: false, prioritize_arranged: false }
    }
}
```

Note: Made `pub` (not `pub(crate)`) to avoid `private_interfaces` warning since it appears in the signatures of `pub fn optimize_with_availability` and `pub fn raise`.

## Consumer Audit

Run: `grep -rn "JoinInputCharacteristics" src/ | grep -v "src/transform/src/join_implementation.rs"`

| file:line | reader | verdict |
|---|---|---|
| `src/transform/src/eqsat/join_commit.rs:17,23,35,45,81,87` | definition site and `step_characteristics` return type | IN-SCOPE |
| `src/compute-types/src/plan/join/linear_join.rs:113` | `for (_, _, _characteristics) in join_order.iter()` — explicitly ignored | SAFE: display-only |
| `src/compute-types/src/plan/join/delta_join.rs:100` | `for (_, _, _characteristics) in &join_orders[source_relation]` — explicitly ignored | SAFE: display-only |
| `src/expr/src/explain/text.rs:657,671,674,678,683` | EXPLAIN rendering calls `.explain()` string method | SAFE: display-only |
| `src/expr/src/lib.rs:45` | re-export | IN-SCOPE: type definition |
| `src/expr/src/relation.rs:3227-3483` | type definition | IN-SCOPE: type definition |

**Conclusion: DISPLAY-ONLY.** Neither `linear_join.rs` nor `delta_join.rs` makes any planning decision based on characteristics — they explicitly pattern-match the third tuple element as `_characteristics`. The only non-definition reader that acts on the value is `explain/text.rs`, which calls `.explain()` for EXPLAIN string rendering. Emitting `Some(chars)` with `cardinality=None, filters=none()` is safe: planners ignore it, EXPLAIN renders only K/UK/A markers from populated fields. No blocking consumers found.

## Goldens Regenerated

14 files (each regenerated per-file in isolation with `bin/sqllogictest -- --rewrite-results`):

1. `aggregation_nullability.slt`
2. `column_knowledge.slt`
3. `eqsat_delta_join_cost.slt`
4. `filter_index.slt`
5. `is_null_propagation.slt`
6. `join_fusion.slt`
7. `join_index.slt`
8. `literal_constraints.slt`
9. `literal_lifting.slt`
10. `predicate_pushdown.slt`
11. `reduce_elision.slt`
12. `reduction_pushdown.slt`
13. `relation_cse.slt`
14. `union.slt`

Excluded (pre-existing failures): `demand.slt`, `ldbc_bi.slt`, `tpch_select.slt`.

## Result-Row Gate

```
git diff test/sqllogictest/transform/ | grep -E '^[+-]' | grep -vE '^[+-]{3} |EXPLAIN|» |\[|type=|Join|Map|Filter|Project|Get|ArrangeBy|%[0-9]'
```

Output: (empty) — zero result-row changes.

All diff lines are join-implementation order lines with only K/UK/A marker additions. Sample:
```
-            %1:t2[#0{f1}] » %0:t1[#0]
+            %1:t2[#0{f1}]K » %0:t1[#0]K
-                      %1[#0] » %0:l0[#0]
+                      %1[#0]UKA » %0:l0[#0]K
```

## Test Results

`cargo nextest run -p mz-transform eqsat::join_commit`: 3/3 PASS
- `step_characteristics_reports_arranged_unique_and_len`
- `commit_differential_builds_expected_shape`
- `commit_differential_aligns_hub_start_key_to_first_lookup`

Full golden suite: `bin/sqllogictest --optimized -- $(ls test/sqllogictest/transform/*.slt | ...)` → PASS: success=1181 total=1181

## Cargo Check

`cargo check -p mz-transform`: clean (0 errors, 0 warnings).

## Self-Review Notes

- The Task-1 `dead_code` warning for `step_characteristics` is gone: `commit_differential` now calls it.
- `NativeJoinFlags` made `pub` (not `pub(crate)`) to resolve `private_interfaces` warning from public function signatures.
- The `inputs` parameter in `commit_differential` and the local `inputs` from the `Join` destructuring shadow correctly: function param used for `step_characteristics` before the destructuring, join's internal `inputs` used for `implement_arrangements` after.
