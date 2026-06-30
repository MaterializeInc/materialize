# Task 2 Report: Differential Emitter (`eqsat/join_commit.rs`)

## Status: DONE

## Commit

(see git log after commit)

---

## What Was Done

### Step 1: Made three JI helpers `pub(crate)`
In `src/transform/src/join_implementation.rs`:
- Line 944: `fn implement_arrangements<'a>(` → `pub(crate) fn implement_arrangements<'a>(`
- Line 1048: `fn install_lifted_mfp(` → `pub(crate) fn install_lifted_mfp(`
- Line 1099: `fn permute_order(` → `pub(crate) fn permute_order(`

### Step 2: Registered the module in `eqsat.rs`
Added `pub(crate) mod join_commit;` next to `pub(crate) mod join_spelling;` in `src/transform/src/eqsat.rs`.

### Step 3 + 5: Created `src/transform/src/eqsat/join_commit.rs`
Implemented `commit_differential` per the brief, followed by the test module.

### Deviation: Test type names
The brief's test used `mz_repr::{ColumnType, RelationType, ScalarType}` — these names don't exist in
this repo's `mz_repr`. Adapted to `mz_repr::{ReprRelationType, ReprScalarType}` and used
`ReprScalarType::Int32.nullable(true)` instead of the struct literal form `ColumnType { ... }`.

### Deviation: Missing `Columns` trait import
The `permute` call on `MirScalarExpr` requires `use mz_expr::Columns;` in scope. Added to the
non-test import block.

## Test Output

### `bin/cargo-test -p mz-transform commit_differential`
```
PASS [0.005s] (1/1) mz-transform eqsat::join_commit::tests::commit_differential_builds_expected_shape
Summary: 1 test run: 1 passed
```

### `bin/cargo-test -p mz-transform join_implementation`
```
PASS [0.007s] (1/2) mz-transform::wcoj_decision physical_eqsat_differential_survives_join_implementation
PASS [0.010s] (2/2) mz-transform::wcoj_decision committed_differential_survives_join_implementation
Summary: 2 tests run: 2 passed
```

## Warnings (expected, not errors)
Six `dead_code` warnings from `cost.rs` (`binary_join_order`, `JoinStep`, `JoinOrder`,
`frontier_key_cols`, `best_left_deep_sequence`) and one from `join_commit.rs`
(`commit_differential`) — all items are `pub(crate)` awaiting Task 3's routing which will
call them. These will resolve once Task 3 wires up the caller.

## Concerns
None. NLL pattern compiled cleanly as specified (the `&mut join` borrow ends before
`install_lifted_mfp(&mut join, ..)` is called, matching `differential::plan`'s structure).
