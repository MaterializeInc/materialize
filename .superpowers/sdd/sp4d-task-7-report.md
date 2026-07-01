# SP4d Task 7 — Hierarchical Color Forest + Colored Congruence Report

## Summary

Task 7 makes the colored layer **hierarchical**: instead of creating every color
as a flat child of black, `build_colored_layer` now builds an inclusion forest
(parents = maximal proper subsets) and then calls `close_all` to run congruence
over the seed equalities.

---

## 1. Forest Derivation (`build_colored_layer`)

**File:** `src/transform/src/eqsat/colored_derive.rs`

Replaced the 9-line flat loop with the hierarchical forest derivation specified
in the brief:

1. **Sort** scope indices by `(|unions|, unions)` ascending so smaller sets
   (parents) are always processed before larger sets (children).
2. **Parent search:** for each scope `i`, iterate already-placed scopes in the
   same ascending order and keep the last proper-subset as the parent (last =
   most equalities = maximal proper subset). Tie-break is deterministic via the
   sorted `Vec<(Id,Id)>` ordering.
3. **Color creation:** `ceg.new_color(Some(parent_color))` (or `None` if no
   proper-subset scope exists → direct child of black).
4. **Delta equalities:** for each color, collect only equalities not in the
   immediate parent's set (the parent chain is inherited automatically by the
   layered find). Re-canonicalize through `base.find` as before.
5. **`close_all`:** collect `per_color: Vec<(ColorId, Vec<(Id,Id)>)>` and call
   `ceg.close_all(&per_color)` once. This applies deltas **and** runs congruence
   to fixpoint, parent-first. The old per-iteration `ceg.union` calls are gone;
   the unions are now applied exclusively inside `close_all → close`.

Moved `BTreeSet` into the existing `std::collections` import line.

---

## 2. `close_all` Integration

`close_all` (in `colored/congruence.rs:149`) sorts its input by `ColorId.0`
ascending before closing each color. Because the forest creates parents before
children (step 1–3 above), `ColorId`s are already in parent-first order, so
`close_all`'s own sort is redundant but harmless — it guarantees the invariant
holds even if callers build the `per_color` vec in an arbitrary order.

The `close` method (line 85) applies the equalities via `union`, then iterates
all visible e-nodes to a congruence fixpoint. This is what enables "seed
congruence propagation": once `#0 ≅ #1` is applied, `Eq(#0,#1)` and
`Eq(#0,#0)` both canonicalize to `Eq(rep,rep)` and are merged.

---

## 3. Parent-first ColorId Invariant

**Invariant:** a color's `ColorId.0` is always strictly less than any of its
descendants' `ColorId.0` values.

**Verification:** the sort in step 1 puts smaller sets first. A parent scope
must have strictly fewer equalities than any of its children (proper subset), so
it always appears earlier in `order`. `new_color` assigns `ColorId(colors.len())`
at call time, so earlier-processed scopes get lower `ColorId`s. QED.

This is documented both in the `build_colored_layer` doc-comment ("Parent-first
`ColorId` invariant" paragraph) and in the inline comment "Parents are created
before children → pcolor.0 < cid.0 always holds".

The T6 driver (`colored_saturate`) iterates `0..num_colors()` and requires
parent-first processing. Because the invariant holds by construction, the driver
needs no change. The doc-comment in `colored_saturate` already says "flat
declaration order, which is parent-first because colors are created parent-first"
— this remains accurate.

---

## 4. `parent_of` Accessor

Added `pub(crate) fn parent_of(&self, c: ColorId) -> Option<ColorId>` to
`ColoredEGraph` in `colored.rs`. It exposes the `ColorData::parent` field for
test assertions and hierarchy navigation.

---

## 5. Tests

### `color_forest_is_inclusion_ordered` (colored_derive.rs)

Constructs `DerivedScopes` manually with two scopes given **out of order**:

- `scopes[0]` = large `{(a,b),(a,c)}` — 2 equalities (superset)
- `scopes[1]` = small `{(a,b)}` — 1 equality (proper subset)

After `build_colored_layer`, asserts:
1. `ceg.parent_of(color_large) == Some(color_small)` — the superset color is a
   child of the subset color (forest structure correct).
2. `ceg.parent_of(color_small) == None` — the smaller scope is a direct child
   of black.
3. `color_small.0 < color_large.0` — parent-first `ColorId` invariant.

**Proves:** the sort-and-parent-search algorithm correctly identifies inclusion
relationships regardless of input order.

### `colored_congruence_propagates_from_seeds` (colored_derive.rs)

Builds a base graph with scalar nodes `#0`, `#1`, `Eq(#0,#1)`, `Eq(#0,#0)` and
a single scope asserting `#0 ≅ #1`. After `build_colored_layer`, asserts:

```
ceg.find(color, id_eq01) == ceg.find(color, id_eq00)
```

**Proves:** `close_all` runs congruence over the seeds — both `Eq(#0,#1)` and
`Eq(#0,#0)` canonicalize to `Eq(rep,rep)` under the color, so they are merged.

---

## 6. T6 Driver Test

`colored_saturate_simplifies_under_context` (in `colored/saturate.rs`) builds
its `ColoredLayer` manually (not via `build_colored_layer`) and directly calls
`ceg.union` before `colored_saturate`. It is **not affected** by this change;
no adjustment was needed.

---

## 7. Gate Results

```
bin/cargo-test -p mz-transform colored   → 52/52 pass (all SP3b + T6 tests pass)
bin/cargo-test -p mz-transform           → 355/355 pass, 3 skipped; 0 golden diffs
cargo clippy -p mz-transform --all-targets -- -D warnings  → clean (0 warnings)
```

---

## 8. Deviations from Brief

None. The implementation follows the brief's derivation code exactly. The only
additions beyond the brief:
- `parent_of` accessor documented as needed for tests.
- `BTreeSet` added to imports (required by the derivation).
- The `per_color` accumulation is interleaved with the loop (not a second pass)
  for clarity; semantics are identical.
