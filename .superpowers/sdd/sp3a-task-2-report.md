# SP3a Task 2 Report

## What Was Done

Implemented Task 2 of SP3a in `src/transform/src/eqsat/colored.rs`, following TDD order:

### Step 1: Failing tests added
Added to the existing `#[cfg(test)] mod tests` block (after Task 1's tests):
- `close_color_by_copy<L>` — differential oracle: all-pairs structural comparison over the base id space, O(n² * iters).
- `same_partition` — helper that checks two `ColoredUf`s induce the same equivalence on a slice of ids.
- `close_color_no_equalities_is_base_partition` — empty equalities ⇒ colored partition == base partition, all metrics zero.
- `close_color_leaf_equality_no_cascade` — leaf-only equality: 1 applied, 0 induced.
- `close_color_propagates_congruence` — a≡b ⇒ Op(0,[a]) ≡ Op(0,[b]) via congruence cascade.
- `close_color_matches_oracle_fixed` — 4 equality-set cases validated against the oracle.

### Step 2: Confirmed compile failure
`bin/cargo-test -p mz-transform eqsat::colored` produced 4 `E0425: cannot find function close_color` errors, as expected.

### Step 3: Implementation added
Before the `#[cfg(test)]` block:
- `use std::collections::HashMap;` — added at top of file alongside existing `use crate::eqsat::core::...`.
- `pub(crate) struct ColorMetrics` with all 5 `pub` fields: `applied_equalities`, `induced_merges`, `delta_classes`, `delta_nodes`, `iters`.
- `pub(crate) fn close_color<L: Language>` — seeds `ColoredUf` from base, applies input equalities, runs hash-cons congruence closure to fixpoint, computes delta metrics by comparing colored and base canonical forms.

### Step 4: All 6 tests pass

```
────────────
 Nextest run ID 83cdcdde-18ab-4132-9784-6ce0137b4bea with nextest profile: default
    Starting 6 tests across 10 binaries (297 tests skipped)
        PASS [   0.005s] (1/6) mz-transform eqsat::colored::tests::colored_uf_starts_equal_to_base
        PASS [   0.006s] (2/6) mz-transform eqsat::colored::tests::close_color_no_equalities_is_base_partition
        PASS [   0.006s] (3/6) mz-transform eqsat::colored::tests::close_color_matches_oracle_fixed
        PASS [   0.006s] (4/6) mz-transform eqsat::colored::tests::close_color_propagates_congruence
        PASS [   0.006s] (5/6) mz-transform eqsat::colored::tests::close_color_leaf_equality_no_cascade
        PASS [   0.006s) (6/6) mz-transform eqsat::colored::tests::colored_uf_union_merges_classes
────────────
     Summary [   0.008s] 6 tests run: 6 passed, 297 skipped
```

### Step 5: Committed
Commit: `8ca390dee8` — "eqsat: SP3a close_color congruence kernel + ColorMetrics + oracle"

## Deviations

None. Code was transcribed verbatim from the brief. The `use std::collections::HashMap;` was placed at the top of the file alongside the existing `use crate::eqsat::core::...` import, as recommended in the brief's note.

## Notes for Task 3

- `ColorMetrics` and `close_color` are `pub(crate)` and ready for consumption.
- The `#![allow(dead_code)]` and SP3b note in the module header remain intact.
- `close_color` takes `&EGraph<L>` and `&[(Id, Id)]` and returns `(ColoredUf, ColorMetrics)` — no mutation of base.
- The oracle (`close_color_by_copy`) is `#[cfg(test)]`-only and lives inside the `tests` mod.
- Task 3 (generators + harness) can call `close_color` directly and use `ColorMetrics` to measure explosion.
- The fixed base graph has 5 nodes: a=Leaf(0), b=Leaf(1), x=Leaf(2), fa=Op(0,[a]), fb=Op(0,[b]); ids are returned in insertion order [a,b,x,fa,fb].
