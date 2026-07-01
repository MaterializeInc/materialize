# SP2a Task 3 Report: Migrate scalar engine onto generic core

## Status: DONE

## Commit
`e24e5a914f` — eqsat/scalar: run the scalar engine on the generic core

## Baseline (pre-migration)
`bin/cargo-test -p mz-transform eqsat::scalar`:
- **90 tests run: 90 passed** (90 = 12 analysis + 66 rules + 10 scalar module + 2 lang — the brief said 88, but Tasks 1 and 2 added 2 new lang tests)

## Post-migration
`bin/cargo-test -p mz-transform eqsat::scalar`: **90/90 passed, 207 skipped**
`bin/cargo-test -p mz-transform eqsat`: **231/231 passed, 66 skipped** (relational 141 + scalar 90)
`bin/cargo-test -p mz-transform`: **296/296 passed, 1 skipped**

Key tests confirmed passing:
- `eqsat::scalar::analysis::tests::test_merge_could_error_survives_union` ✓
- `eqsat::scalar::analysis::tests::test_merge_preserves_literal` ✓
- `eqsat::scalar::rules::tests::test_fold_cycle_could_error_is_conservative` ✓ (self-cycle via recompute_analysis)
- `eqsat::scalar::rules::tests::test_fold_terminates` ✓

## Sanity grep
`git grep -n "ScalarEGraph::with_col_types\|\.saturate()\|struct ScalarEGraph\|hashcons" src/transform/src/eqsat/scalar/`

Result: two comment-only matches for the word "hashcons" in `rules.rs` (documentation text explaining hash-cons stability). No code-level references to the deleted struct, `with_col_types`, or the `.saturate()` method. These comments predated the migration and are harmless.

## Files changed
- `src/transform/src/eqsat/scalar/egraph.rs` — complete rewrite: deleted `ScalarEGraph` struct + substrate; replaced with `pub type ScalarEGraph = EGraph<ScalarLang>`, `pub use crate::eqsat::core::Id`, inherent `impl EGraph<ScalarLang>` providing `analysis` and `col_types`, free functions `recompute_analysis` and `saturate`.
- `src/transform/src/eqsat/scalar.rs` — `canonicalize`: renamed local to `eg`, uses `eg.data_mut().col_types = ...` + `egraph::saturate(&mut eg)`; `assert_round_trip` test helper: same rename + free-fn call.
- `src/transform/src/eqsat/scalar/rules.rs` — two test `saturate` calls changed from `eg.saturate()` to `crate::eqsat::scalar::egraph::saturate(&mut eg)`.

## Borrow-checker note on recompute_analysis
The brief's borrow pattern works without special tricks: `eg.data().analysis` and `eg.find(c)` are both immutable borrows, so they coexist. The block `{ let store = ...; let find = ...; analysis::make(...) }` ensures the immutable borrows end before the `eg.data_mut().analysis.insert(...)` call below it.

## Behavior-neutral confirmation
- No `REWRITE=1`, no `cargo insta accept`, no `--rewrite-results` used.
- The relational engine is untouched (all 141 relational eqsat tests pass).
- No new dependencies introduced.
