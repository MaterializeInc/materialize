# Empty-propagation rules: implementation report

## Summary

Added five equality-preserving rewrite rules that propagate empty (zero-row)
collections upward through relational operators.
All 11 roundtrip tests pass.
Harness result: **3 wins / 6 losses / 11 ties** — unchanged from baseline.

## Mechanism

Chose `Cond::IsRelEmpty` / `Cond::NotRelEmpty` side conditions over a new
`Pat::Empty` pattern variant.
A `PatRelVar` binds the input in the usual way; the side condition then checks
whether the bound e-class already contains an `ENode::Constant { card: 0, .. }`.
`Tmpl::Empty(name)` (pre-existing) handles the RHS.
This reuses all existing binding and compilation machinery; no changes to the
e-matching pipeline were needed.

## Rules added (`src/transform-egraph/rules/relational.rewrite`)

```
rule threshold_empty   Threshold e => Empty(e)      where is_rel_empty(e)
rule negate_empty      Negate e    => Empty(e)      where is_rel_empty(e)
rule filter_empty      Filter[p] e => Empty(e)      where is_rel_empty(e)
rule union_drop_empty_left   Union(e, b) => b       where is_rel_empty(e)  where not_rel_empty(b)
rule union_drop_empty_right  Union(a, e) => a       where is_rel_empty(e)  where not_rel_empty(a)
```

## Termination

`threshold_empty`, `negate_empty`, `filter_empty`: fire only when the input is
already `Constant{card:0}`.
The RHS is the same constant (just merged into the parent's class), so the
effective e-graph shrinks by one node per firing.
No cycle risk.

`union_drop_empty_left/right` without guard: **discovered a non-termination
bug**.
When both branches of a `Union` are empty, the rule `union_drop_empty_left`
merged the Union's e-class with the kept branch (also empty).
That class also contained `Filter` nodes installed by `empty_false_filter`.
The outer `Filter` then had itself as input (`C0 -> Filter[p] C0`), making
`merge_filters` generate unboundedly long predicate lists
(`Filter[p++p]`, `Filter[p++p++p]`, …).

Fix: `not_rel_empty(b)` / `not_rel_empty(a)` guard prevents the rule firing
when the kept branch is also empty.
Union-of-two-empties is handled transitively: `threshold_empty` or another
consumer will see the Union's class once `union_cancel` or `empty_false_filter`
adds `Constant{0}` to it.

## Tests (`src/transform-egraph/tests/roundtrip.rs`)

Two new tests:

* `threshold_over_empty_filter_collapses`: `Threshold(Filter(false, r))` →
  `Constant{0,2}`.
* `union_drops_empty_branch`: `Union(Filter(false,r1), r2)` → `r2` (non-empty).

Both passed immediately — not because the new rules fired, but because the
existing rules already handled these cases:

* `empty_false_filter` turns `Filter(false,r)` into `Constant{0,2}`.
* `non_negative` analysis marks `Constant{card:0}` vacuously non-negative (no
  children → `all()` holds).
* `threshold_elision` (`Threshold r => r where non_negative(r)`) then merges
  the Threshold class with the Constant class.
* Extraction picks `Constant{0,2}` as cheapest.

The new rules add a shorter path to the same conclusion and cover more complex
cases (e.g. `Negate(empty)`, `Filter(p, empty)`) where `threshold_elision`
does not apply.

## Differential harness

**Before: 3 wins / 6 losses / 11 ties.**
**After:  3 wins / 6 losses / 11 ties.** (no change)

### Why no improvement

The 6 losses are dominated by `union_cancel`-related cases
(`union_cancel`, `union_cancel_under_filter_map`,
`threshold_over_union_cancel`, etc.).

Root cause: `intern_leaf` does NOT deduplicate.
When `Union(a, Negate(a))` is lowered, both occurrences of `a` are interned
separately, yielding distinct leaf IDs (`leaf:0`, `leaf:1`).
The `union_cancel` rule requires `Union(a, Negate a)` — the *same* e-class in
both positions.
With distinct leaves, the rule never fires because the two leaves are in
different e-classes and nothing merges them.

Empty-propagation cannot fix this: even if `union_cancel` fired, the new rules
add at most one extra hop (Threshold/Negate/Filter above the cancelled union).
The structural deduplication gap must be fixed first.

### Remaining losses

| Case | Root cause |
|---|---|
| `union_cancel` | `intern_leaf` no dedup — same subtree → distinct leaves |
| `union_cancel_under_filter_map` | same |
| `threshold_over_union_cancel` | same |
| `threshold_over_union_filtered_inputs` | same |
| `filter_union_branch_filters_map_project` | cost model: cheaper path exists in e-class but extractor still picks original |
| `filter_then_project` | pushdown + projection fusion not fully closing the gap |

## TODO: non-convergence detection

A debug assertion that bans self-referential e-nodes (a node whose child
`find`s to its own class) would **not** be sound: `threshold_elision` unions
`Threshold r` into `r`'s class on purpose, so self-cycles are an intended part
of saturation (see the `arity` doc in `egraph.rs`, which uses a visited-guard
precisely because classes can be cyclic).

The bug this work surfaced was not a cycle but unbounded *payload* growth
(`merge_filters` concatenating ever-longer predicate lists). The existing
`max_iters: 100` cap in `saturate` already prevents a true infinite loop, so
the symptom was slowness, not a hang. A sound, useful follow-up would be a
debug-only growth detector: track the max scalar-list length per iteration and
assert it does not grow on every one of the `max_iters` rounds, which flags
non-converging payload-concatenation rules without false-positives on
legitimate structural cycles. Deferred — the `not_rel_empty` guard fixes the
concrete case, and a general detector needs care to avoid flagging inputs that
legitimately need many iterations.

## Files changed

* `src/transform-egraph/src/dsl.rs` — `Cond::IsRelEmpty`, `Cond::NotRelEmpty`
* `src/transform-egraph/src/parser.rs` — `is_rel_empty`, `not_rel_empty` parse arms
* `src/transform-egraph/src/matcher.rs` — `check_conds` arms for both
* `src/transform-egraph/src/egraph.rs` — `check_conds` arms for both
* `src/transform-egraph/rules/relational.rewrite` — five new rules
* `src/transform-egraph/tests/roundtrip.rs` — two new tests
