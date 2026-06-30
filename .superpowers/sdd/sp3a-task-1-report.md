# Task 1 Report: SP3a colored module skeleton (uf_len, ToyLang, ColoredUf)

## Status: DONE

## Commit

`893798266a` — "eqsat: SP3a colored module skeleton (uf_len, ToyLang, ColoredUf)"

## What Was Done

### Step 1: `uf_len` accessor in `core.rs`

Added `uf_len` immediately after `node_count` (line ~215) in `impl<L: Language> EGraph<L>`:

```rust
/// The size of the union-find id space (count of ever-created e-classes,
/// including ids since merged away). Read-only; lets a colored layer seed a
/// union-find over the full id space.
#[allow(dead_code)] // SP3a colored.rs consumes this; SP3b uses it in production.
pub(crate) fn uf_len(&self) -> usize {
    self.uf.len()
}
```

`self.uf` is `Vec<Id>` (confirmed from struct definition at line ~70), so `.len()` works directly.

### Step 2: `mod colored;` in `eqsat.rs`

Added `mod colored;` in `eqsat.rs`. It was placed before `mod core;` (alphabetically `colored` < `core`), which is a minor cosmetic deviation from the brief (which said "after `mod core;`") but produces identical semantics — Rust does not require declaration order for `mod` statements.

### Step 3 & 4: `colored.rs` created with tests

Created `src/transform/src/eqsat/colored.rs` verbatim from the brief:
- `#![allow(dead_code)]` with SP3b note at module scope
- `ToyNode { Leaf(u32), Op(u16, Vec<Id>) }`, `ToySym { Leaf, Op(u16) }`, `ToyLang` implementing `Language`
- `ColoredUf { parent: Vec<Id> }` with `over_base`, `find` (non-compressing), `union`
- Two `#[mz_ore::test]` tests in `mod tests`

## Test Run

Command: `bin/cargo-test -p mz-transform eqsat::colored`

Output:
```
Compiling mz-transform v0.0.0 (...) [26.94s]
PASS [   0.005s] (1/2) mz-transform eqsat::colored::tests::colored_uf_starts_equal_to_base
PASS [   0.006s] (2/2) mz-transform eqsat::colored::tests::colored_uf_union_merges_classes
Summary [   0.007s] 2 tests run: 2 passed, 297 skipped
```

## Deviations from the Brief

1. **`mod colored;` placement**: Brief says "after `mod core;`"; placed before it (alphabetical). Semantically identical.
2. **Report file name**: Brief says write to `task-1-report.md`, but that file already contains the SP1 task-1 report. Written to `sp3a-task-1-report.md` following the SP2a naming convention (`sp2a-task-1-report.md`) to preserve history.

## Notes for Task 2

- `ColoredUf::find` is non-compressing (follows parent chain without path compression), as specified. Task 2's `close_color` will use it for the congruence closure kernel.
- `ColoredUf::union` merges `rb → ra` (no rank heuristic), as specified.
- `uf_len` returns the full id space size including merged-away ids — Task 2 can use `(0..base.uf_len())` to iterate over all ids when building the colored partition.
- `ToyLang` is fully implemented with `children`, `map_children`, `symbol` — ready for Task 2's congruence closure to use `Language::children` to traverse e-nodes.
- `EGraph<ToyLang>::index()` (if it exists in core) is also available for Task 2's congruence closure scan.
- All dead-code allows are in place; the module compiles with zero warnings.
