# SP4d Task 11b: eqsat-local outermost-first reduction

## Summary

Fixed a silent no-op in `reduce_escalar` caused by a short-circuit ordering bug in the
shared `ExpressionReducer::reduce_expr` trait method. Added a new eqsat-local
`reduce_outermost` helper that tries the whole-expression replacement FIRST, then recurses
into children — the correct strategy for contextual reductions like tautology folding.

## The `reduce_outermost` helper

Location: `src/transform/src/eqsat/egraph/saturate.rs`, immediately above `reduce_escalar`.

```rust
fn reduce_outermost(
    reducer: &BTreeMap<MirScalarExpr, MirScalarExpr>,
    expr: &mut MirScalarExpr,
) -> bool {
    let mut changed = false;
    // Phase 1: try whole-expr replacement to a fixpoint first.
    while reducer.replace(expr) {
        changed = true;
    }
    // Phase 2: recurse into children (mirroring `reduce_child` arms exactly).
    let child_changed = match expr {
        MirScalarExpr::CallBinary { expr1, expr2, .. } => {
            let c1 = reduce_outermost(reducer, expr1);
            let c2 = reduce_outermost(reducer, expr2);
            c1 || c2
        }
        MirScalarExpr::CallUnary { expr: inner, .. } => reduce_outermost(reducer, inner),
        MirScalarExpr::CallVariadic { exprs, .. } => {
            exprs.iter_mut().fold(false, |acc, e| reduce_outermost(reducer, e) || acc)
        }
        MirScalarExpr::If { cond: _, then, els } => {
            let c1 = reduce_outermost(reducer, then);
            let c2 = reduce_outermost(reducer, els);
            c1 || c2
        }
        _ => false,
    };
    if child_changed {
        changed = true;
        // Phase 3: after children changed, re-try the parent-level replacement to a fixpoint.
        while reducer.replace(expr) {}
    }
    changed
}
```

`reduce_escalar` now calls `reduce_outermost(reducer, &mut expr)` instead of
`reducer.reduce_expr(&mut expr)`.

## Shared trait NOT modified

`ExpressionReducer::reduce_expr`/`reduce_child`/`replace` in
`src/transform/src/analysis/equivalences.rs` are unchanged. Confirmed by diffing the file:
no modifications to `equivalences.rs`.

## Unit tests (all pass)

Four tests added to `saturate.rs`'s `#[cfg(test)] mod tests`:

| Test | What it checks |
|------|---------------|
| `reduce_outermost_composite_replace_fires_before_children` | `Eq(#0,#1)` under `{Eq(#0,#1)→true, #1→#0}` yields `true`, not `Eq(#0,#0)` — the key regression case |
| `reduce_outermost_plain_column_substitution` | `#1` under `{#1→#0}` yields `#0` — common path unbroken |
| `reduce_outermost_no_match_unchanged` | Empty reducer leaves expression unchanged, returns `false` |
| `reduce_escalar_tautology_folds_to_true` | Integration: `reduce_escalar` on `Eq(#0,#1)` with tautology reducer produces `(true, EScalar{lit: Some(true)})` |

## Golden file changes

`git diff --stat src/transform/tests/`:

```
src/transform/tests/test_transforms/eqsat.spec | 14 +--
1 file changed, 9 insertions(+), 5 deletions(-)
```

**1 spec file changed. 0 slt files changed.**

### Change analysis

**Case (f) in `eqsat.spec`** — only change:

```diff
 Filter (#0 = #1)
-  Map ((#0 + 1))
+  Map ((#1 + 1))
     Get t0
```

**Soundness justification:** The Map scalar `#1 + 1` is NOT canonicalized to `#0 + 1` anymore. The outermost-first reducer now tries the composite entry `#1+1 → #2` (the Map's own output column) first; this fires, but `#2` is out of range (`max_col=2`, forward self-reference guard), so `reduce_escalar` rejects the entire reduction. The original `#1 + 1` is kept. Under the Filter's constraint `#0 = #1`, both `#0 + 1` and `#1 + 1` are semantically equivalent, so keeping `#1 + 1` is **correct**. Updated the spec comment to describe the new behavior.

**Category:** No unsound changes. The 1-line spec change is a less-canonicalized-but-correct spelling. No tautology-folding cases exist in the current spec (the new behavior would show up as `true` replacing `Eq(#0,#1)` predicates — but no such test exists in the existing suite yet).

**No uncertain changes.** The single change is clearly equivalent.

## Clippy and suite status

- `cargo clippy -p mz-transform --all-targets -- -D warnings`: **CLEAN** (no warnings, no errors)
- `bin/cargo-test -p mz-transform`: **364/364 PASS** (3 skipped)
- All 4 new unit tests: **PASS**

## Fix (T11b-2)

### Problem

`reduce_outermost` had no `max_col` parameter. The `max_col` guard was applied only
at the `reduce_escalar` level, after `reduce_outermost` returned. This made the guard
all-or-nothing: if the fully-reduced expression referenced an out-of-range column,
the ENTIRE result was discarded and the original was kept — including any valid
child-level rewrites that had already been applied.

Concretely: a Map scalar `#1+1` with reducer `{#1+1→#2, #1→#0}` and `max_col=2`
produced `col(2)` (out of range), so `reduce_escalar` rejected it and emitted the
original `#1+1` instead of `#0+1` (which is both valid and more canonical).

### Fix

Threaded `max_col: usize` into `reduce_outermost`. Each whole-expression replace
step is now guarded individually: the fix looks up `reducer.get(expr)` directly,
checks that the candidate's column support is entirely `< max_col` before accepting
it, and breaks the whole-expr loop (falls through to child recursion) if the
candidate is out of range. The three whole-expr loops (Phase 1, Phase 3) both use
this per-step guard.

`reduce_escalar`'s final `max_col` check is kept as a defensive backstop; with the
per-step guard, it should always pass, but leaving it is belt-and-suspenders.

`ExpressionReducer::replace` (from `analysis/equivalences.rs`) is no longer called
in `reduce_outermost` (replaced by direct `BTreeMap::get` + `clone_from`), so its
import was removed from `saturate.rs`.

### Regression test added

`reduce_escalar_per_step_max_col_guard_allows_child_rewrite` in
`src/transform/src/eqsat/egraph/saturate.rs`:

- Reducer: `{add(#1,1)→col(2), #1→#0}`, `max_col=2`
- Input: `add(#1, 1)`
- Before fix: returns `(false, add(#1,1))` — whole-expr rewrite rejected, child rewrite lost
- After fix: returns `(true, add(#0,1))` — whole-expr step rejected, child rewrite `#1→#0` accepted

### Existing tests updated

The three `reduce_outermost_*` tests that call `reduce_outermost` directly were
updated to pass `max_col=usize::MAX` (guard irrelevant, behavior preserved).

### Golden diff (`git diff --stat src/transform/tests/`)

```
src/transform/tests/test_transforms/eqsat.spec | 2 +-
1 file changed, 1 insertion(+), 1 deletion(-)
```

The previously-moved golden (case (f), `Map ((#1 + 1))`) reverts back to
`Map ((#0 + 1))`. This is the T11b regression undone: the net diff vs the
pre-Task-11b baseline is **0 goldens moved** (empty).

### Suite and clippy

- `cargo clippy -p mz-transform --all-targets -- -D warnings`: **CLEAN**
- `bin/cargo-test -p mz-transform`: **365/365 PASS** (3 skipped)
- All 5 unit tests in `eqsat::egraph::saturate::tests`: **PASS**
