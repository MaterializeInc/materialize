# Task 3 Report: Join-spelling selector

## What was implemented

Created `src/transform/src/eqsat/join_spelling.rs` with:
- `select_join_spelling` (pub(crate)): enumerates column-pure remaps from the
  Equivalences analysis classes, scores each with `delta_join_terms`, returns
  the spelling with fewest forced crosses (then lowest degree), or base
  unchanged if already cross-free or caps exceeded.
- `advance`: odometer helper for the cartesian-product enumeration.
- `CAP_CLASSES = 4`, `CAP_PRODUCT = 64` safety caps.

Registered in `src/transform/src/eqsat.rs` as `mod join_spelling;`.

## TDD: RED

```
COCKROACH_URL=... bin/cargo-test -p mz-transform -- eqsat::join_spelling
error[E0425]: cannot find function `select_join_spelling` in this scope
  --> src/transform/src/eqsat/join_spelling.rs:56:22
```

Expected compile error — confirmed RED.

## TDD: GREEN

```
COCKROACH_URL=... bin/cargo-test -p mz-transform -- eqsat::join_spelling
        PASS [0.005s] mz-transform eqsat::join_spelling::tests::selector_keeps_base_when_no_cross
        PASS [0.005s] mz-transform eqsat::join_spelling::tests::selector_picks_local_spelling_to_avoid_cross
Summary [0.008s] 2 tests run: 2 passed, 377 skipped
```

## Files changed

- `src/transform/src/eqsat.rs`: added `mod join_spelling;`
- `src/transform/src/eqsat/join_spelling.rs`: new file (190 lines)

## Deviations from the brief

### Selector: remap expression keys only, not bare-column members

The brief's original candidate construction applied the column remap uniformly
to every equivalence member. That has a real bug, exposed by the genuine VOJ
shape where the cross-forcing expression key (`#0 = #4`, the null-pad key)
references the SAME column `#0` that the connector class `{#0, #2}` shares:
a global `#0 -> #2` remap collapses `{#0, #2}` into `{#2}`, severing the
t1-t2 connection and orphaning t1 (crosses go 1 -> 3, worse than base).

This is not a test-data quirk; it is the actual case the selector exists for.
The surgical fix (from coordinator review) is to remap ONLY expression members
(`e.is_col().is_none()`) and pass bare-column members through unchanged. The
plain class `{#0, #2}` is the connectivity backbone proving `t1.f0 = t2.f2`;
re-spelling the expression key `#0=#4 -> #2=#4` makes it local while `{#0, #2}`
survives, so the join stays connected and crosses = 0. Sound because `#0 = #2`
is still asserted by the surviving class — no extra equivalences unioned, no
degenerate `[#2, #2]` classes produced.

`selector_picks_local_spelling_to_avoid_cross` therefore uses the real
overlapping fixture: base `[[#5, #0=#4], [#0, #2]]`, analysis class `{#0, #2}`,
and asserts the chosen spelling has crosses = 0 and strictly beats base.

### Import fix

Added `use mz_expr::Columns;` — the `as_column()` / `is_col()` methods on
`MirScalarExpr`/`EScalar` are gated behind the `Columns` trait.

## Self-review

The algorithm is correct. The `advance` helper is a standard odometer.
`permute_cols` returns `Result<EScalar, String>` and the remap only maps to
non-negative targets, so the `expect` is sound. Bare-column members are
preserved verbatim so connector classes survive re-spelling.

## Re-run after coordinator review (expression-only remap)

```
COCKROACH_URL=postgres://root@localhost:26257 METADATA_BACKEND_URL=postgres://root@localhost:26257 \
  bin/cargo-test -p mz-transform -- eqsat::join_spelling

        PASS [0.006s] (1/2) mz-transform eqsat::join_spelling::tests::selector_keeps_base_when_no_cross
        PASS [0.007s] (2/2) mz-transform eqsat::join_spelling::tests::selector_picks_local_spelling_to_avoid_cross
     Summary [0.009s] 2 tests run: 2 passed, 377 skipped
```

## Concerns

None.
