# Task 6 report: Color-aware Filter/Map resolution + empty-fold (`EquivMode::Colored`)

## Status: DONE

Color-aware extraction now does real work under `EquivMode::Colored`; `Phase2a`
(the default) is byte-identical to before.

## What landed

### `colored_derive.rs`
- `scalar_cost(&MirScalarExpr) -> usize` — node count via `visit_pre`.
- `resolve_scalar_colored(base, layer, color, id) -> EScalar` — exactly as the
  brief: cheapest member of `colored_class_members(color, base.find(id))` by
  `(scalar_cost(expr), name_key())`, falling back to `base.data().escalar(...)`.
  (No memoization added — `colored_class_members` already does the class scan and
  the call counts per fragment are small; a cache wasn't worth the borrow churn.)
- Tests: `scalar_cost_counts_nodes`, `resolve_colored_picks_cheaper_member`
  (plus a new `redundant_compute_fixture_mut` that returns the mutable e-graph,
  the Map's `f(#1)` payload id, and the Map class).

### `egraph/build.rs` (where `extract_with`/`build_rel` actually live — the brief
said `egraph.rs`, but the recursive Rel builder is in `egraph/build.rs`)
- `build_rel` now takes the node's canonical `class: Id` and
  `colored: Option<&mut ColoredLayer>`.
- **Empty-fold:** at the top of `build_rel` (before the `Negate`/`Nonneg` guard,
  since an empty relation is non-negative), if the layer marks `self.find(class)`
  empty, it returns `Rel::Constant { card: 0, arity: self.arity(class),
  col_types: self.column_types(class) }`. This is the same empty-Constant
  construction `empty_false_filter`/`union_cancel` synthesize and that raise
  consumes (`card: 0` + captured `col_types`); `column_types()` is precisely the
  SP4a "synthesis-time" helper for this (it had no other in-tree caller).
- **Filter/Map resolution:** a new `resolve_scalars_in_color(ids, class, colored)`
  resolves each scalar to its cheapest congruent spelling under
  `color_of[class]`, falling back to the plain `resolve_scalars` cache path when
  there's no layer or no color. Only `Filter.predicates` and `Map.scalars` use
  it; Join/Reduce/ArrangeBy/FlatMap/IndexedFilter keep the plain path.

### `engine.rs`
- Removed the Task-5 placeholder `colored_layer()`.
- New `colored_scopes(&self, eg: &mut EGraph) -> Option<DerivedScopes>` runs the
  CRITICAL ordering: `derive(eg)` (mutates: interns reduced spellings) → `eg.rebuild()`
  → return owned scopes. Under `Phase2a` returns `None`.
- All four extracting arms (`optimize_node_with_alt`, `optimize_node`,
  `optimize_body_with_let_union`, `optimize_around_scopes`) now, after
  `seed_indexed_filters`: `let scopes = self.colored_scopes(&mut eg); let mut
  layer = scopes.map(|s| build_colored_layer(&eg, s));` then pass `layer.as_mut()`
  to extraction. In `optimize_node_with_alt` the TimeFirst `extract_with` reuses
  the same layer (`layer.as_mut()` a second time).

## How the `&mut ColoredLayer` reborrow was threaded
The real `build_rel` is **not** recursive — it's a bottom-up DP that pulls
children from `best_any`/`best_nonneg`. So there is no recursive `&mut` thread;
the loop in `extract_with` passes `colored.as_deref_mut()` to each `build_rel`
call (each a fresh reborrow). Inside `resolve_scalars_in_color` the per-id loop
reborrows with `&mut *layer` (can't move the `&mut` each iteration).

The layer-lifetime hazard was at the engine level, not in `build_rel`: a helper
returning `ColoredLayer<'a>` from a `&'a mut EGraph` would pin `eg` as
exclusively borrowed for the layer's whole life, blocking the shared `&eg`
borrows extraction needs. Fixed by splitting: `colored_scopes` does the
mutation and returns **owned** `DerivedScopes`; the layer is then built from a
plain shared `&eg`, so it coexists with the extraction borrows.

## DEVIATION from the brief (important)
The brief step 4 says resolve Filter predicates / Map scalars under
`color_of[base.find(input)]` (the **input** class). That is incorrect given how
`derive` records: the predicate/scalar reduction unions are keyed by the **node's
own class** (`canon` in `derive`'s loop = the Filter/Map class), so
`build_colored_layer` populates `color_of[own_class]`, not `color_of[input]`.
Using the input's class would (almost always) find no color and silently fall
back to plain — the optimization would never fire. The brief's own test confirms
own-class: `resolve_colored_picks_cheaper_member` looks up `color_of[map_class]`
(the Map class itself). I implemented **own-class** (`color_of[class]`); the
empty-fold is also keyed on the node's own class.

## Empty-Constant construction reused
`Rel::Constant { card: 0, arity: self.arity(class), col_types: self.column_types(class) }`
— mirrors `empty_false_filter`/`union_cancel` (`Constant { card: 0, .. }`) and
the raise empty path; `column_types()` is the SP4a synthesis helper documented
for exactly this.

## Visibility narrowing (Task-5 cleanup)
- `Extractor` trait: `pub` → `pub(crate)`; removed all three
  `#[allow(private_interfaces)]` (trait method + `GreedyExtractor`/`IlpExtractor`
  impls).
- `EGraph::extract_with`: `pub` → `pub(crate)`; removed its `#[allow(private_interfaces)]`.
- `EGraph::extract` (2-arg `PeakDegree` convenience): `pub` → `pub(crate)` and
  gated `#[cfg(test)]` — its only callers are in-crate tests, so without the gate
  it tripped `dead_code` in the non-test lib build.
- `Optimizer::with_extractor`: `pub` → `pub(crate)`. Required: narrowing
  `Extractor` to `pub(crate)` makes this pub builder expose a crate-private type
  (`private_interfaces`). Its only caller (`eqsat::optimize_inner`) is in-crate.
No external use broke (repo grep confirmed nothing outside `mz-transform` uses
`Extractor`/these methods).

## Tests
- New: `scalar_cost_counts_nodes`, `resolve_colored_picks_cheaper_member`
  (colored_derive), `colored_mode_rewrites_redundant_map_compute` (engine, with
  `optimize_with_mode` + `rel_node_count` helpers, asserting colored
  `rel_node_count <=` Phase2a).
- `cargo nextest run -p mz-transform`: **342 passed, 3 skipped** (the +3 are
  exactly the new tests). Phase2a behavior unchanged.
- `cargo clippy -p mz-transform --all-targets -- -D warnings`: clean.
- `bin/sqllogictest -- test/sqllogictest/filter-pushdown.slt`: 34/34 PASS
  (representative end-to-end check; production uses default Phase2a, which is
  byte-identical, so the slt suite is unaffected by construction).

## Concerns
- `resolve_scalar_colored` calls `base.data().escalar(m)` on every colored-class
  member; this is safe only because `derive` unions **scalar** ids exclusively
  (no relational class can join a scalar's colored class). Held today; a future
  rule that unions relational into scalar classes would need a guard.
- The engine smoke test asserts `<=` only; for the redundant-map fixture Phase-2a's
  `rewrite_escalars` already reduces the Map scalar during saturation, so both
  plans coincide. The colored resolution path is proven in isolation by
  `resolve_colored_picks_cheaper_member`. Empty-fold has no dedicated unit test
  (not in the brief's test list); it's covered structurally + by the full suite.

## Fix (review I1/I2)

Two TEST-COVERAGE gaps closed. Shipping logic unchanged; tests only (in
`colored_derive.rs`, where the fixtures/`derive`/`build_colored_layer` live).
Both tests drive `EGraph::extract_with` directly on a graph built WITHOUT
saturation, so the Phase-2a `rewrite_escalars` / `empty_false_filter` rules
never fire and the only thing that can do the work is the Colored `build_rel`
path. Each asserts a `None`-layer baseline (no substitution / Filter kept) AND
the colored result, making them inherently fail-on-no-op.

### Tests added (`src/transform/src/eqsat/colored_derive.rs`)
- `colored_extraction_substitutes_redundant_map_scalar` (I1): on
  `Map[#1+1](Filter[#0=#1+1](Get(2)))`, the no-layer extraction keeps the
  recomputed scalar (`is_col() == None`); the colored extraction substitutes the
  bare column (`scalars[0].is_col() == Some(0)`).
- `colored_extraction_empty_folds_contradiction` (I2): on
  `Filter[#0=2](Filter[#0=1](Get(1)))`, the no-layer extraction keeps the
  `Filter` tree; the colored extraction empty-folds to
  `Constant{card:0, arity:1}` — exercising `build_rel`'s empty-fold path, not
  just `derive`'s `empty_classes` marking.

### Why engine-level (`EquivMode::Colored`) couldn't be fail-on-no-op
Verified empirically (temporary dump test, since reverted): under the full
engine both fixtures coincide between `Phase2a` and `Colored` — Phase-2a's
saturation already rewrites the redundant Map scalar (to `Project[0,1,0]
Filter…`) and already folds the contradiction to `Constant rows=0` via
`empty_false_filter`. So an engine-level structural assert would pass even if
the Colored branch were a no-op. Testing at `extract_with` on an unsaturated
graph isolates the Colored path, which is what makes the asserts discriminating.

### Fail-on-no-op verification
Temporarily disabled BOTH Colored branches in `build_rel` (gated the empty-fold
`if` to `false`, and forced `resolve_scalars_in_color` to take its plain
fallback). Re-ran `bin/cargo-test -p mz-transform colored_extraction`: both new
tests FAILED — `colored_extraction_empty_folds_contradiction` got the
`Filter[(#0=2)] Filter[(#0=1)] Get` tree instead of `Constant`, and
`colored_extraction_substitutes_redundant_map_scalar` kept `#1+1`. Reverted the
shipping edits (build.rs back to a clean `git diff`); both pass again.

### Commands run
- `bin/cargo-test -p mz-transform colored_extraction`: 2 passed.
- `bin/cargo-test -p mz-transform colored`: 41 passed, 306 skipped.
- `bin/cargo-test -p mz-transform`: 344 passed, 3 skipped (was 342; +2 new).
- `cargo clippy -p mz-transform --all-targets -- -D warnings`: clean.

## Fix (Task 7 gate: coalesce_mfp panic)

### Root cause (confirmed)
`resolve_scalar_colored` (colored_derive.rs) picked `min_by_key((scalar_cost,
name_key))` over ALL colored-class members with no column-range guard. A colored
class merges spellings transitively within one color, so it can contain a
spelling whose column support is valid for a *different* node but out of range
for the payload position being emitted here. That out-of-range column ref then
crashed `coalesce_mfp`'s `MapFilterProject::map` → `permute` at
`src/expr/src/scalar/columns.rs:46`. The recording step (`reduce_escalar`)
already applies a `max_col` guard; resolution did not re-check it.

Confirmed empirically BEFORE the fix (Colored default, my two files stashed):
- `joins.slt`: `index out of bounds: the len is 4 but the index is 4` at
  `columns.rs:46:37`, stack `permute ← MapFilterProject::map ←
  extract_non_errors_from_expr_mut ← coalesce_mfp ← optimize_inner`.
- `explain/physical_plan_as_text.slt`: same panic (2 occurrences).

### The guard added
In `resolve_scalar_colored` (now takes `max_col: usize`), among colored-class
members consider only in-range spellings before the cost min:
```rust
.filter(|e| e.expr.support().into_iter().all(|c| c < max_col))
.min_by_key(|e| (scalar_cost(&e.expr), e.name_key()))
.unwrap_or_else(|| base.data().escalar(base.find(id)).clone())
```
The fallback (the original payload's `escalar`) is always in range, so a valid
candidate always exists. This matches `reduce_escalar`'s recording guard exactly
(`expr.support().all(|c| c < max_col)`).

### Per-position max_col mapping (build.rs)
`resolve_scalars_in_color` now takes `max_cols: &[usize]` parallel to `ids`,
computed per kind at the two `build_rel` call sites — identical to the bounds
`derive`/`reduce_escalar` use when recording:
- **Filter** predicates: `max_col = self.arity(input)` for every predicate.
- **Map** scalar at position `pos`: `max_col = self.arity(input) + pos`.
`self.arity(input)` is valid: the input is a relational class (SP4a M1.1 only
forbids `arity` on scalar classes).

### Regression test
Unit test `resolve_colored_rejects_out_of_range_member` in colored_derive.rs:
builds a colored class unioning the in-range payload `#1 + 1` (cost 3) with a
*cheaper* out-of-range bare column `#5` (cost 1) under one color, then resolves
with `max_col = 2`. Asserts the result is NOT a column (the out-of-range `#5` is
filtered despite being cheapest) and equals the in-range payload `#1 + 1`. A
sanity assert confirms `#5` IS a colored-class member, so an unguarded
`min_by_key` would have selected it. Chose a unit test (faithful, fast, directly
targets the guard). Also updated `resolve_colored_picks_cheaper_member` to pass
`max_col = 2`.

### Before/after
- `joins.slt`: BEFORE → panic at columns.rs:46; AFTER → 0 panic lines.
- `explain/physical_plan_as_text.slt`: BEFORE → 2 panics; AFTER → 0.

### 10-file no-panic confirmation (Colored default, with fix)
aggregates, joins, not-null-propagation, order_by, regex, types,
explain/physical_plan_as_text, explain/physical_plan_as_json,
explain/physical_plan_as_text_redacted, transform/relax_must_consolidate —
all report 0 panic/internal-error lines. (Plan/golden DIFFs remain and are
expected; goldens not regenerated.)

### Full suite
`bin/cargo-test -p mz-transform`: 344 passed, 1 failed, 3 skipped. The single
failure is `test_transforms::run_tests` (`eqsat.spec:159`): a golden expecting
the colored substitution `#0 + 1` but the Colored path emits `#1 + 1`. Verified
PRE-EXISTING and independent of this fix: it fails IDENTICALLY with my two files
stashed (same `#1 + 1` actual), so it stems from the in-progress Colored flip
(engine.rs/extract.rs), not the column-range guard. Goldens not regenerated per
instructions. `cargo clippy -p mz-transform --all-targets -- -D warnings`: clean.
The new colored_derive unit tests pass (13/13 in module).

## Fix (Task 7 gate: reducer-parity tie-break)

### Key change
`resolve_scalar_colored` in `colored_derive.rs`: inserted `e.expr.support().into_iter().collect::<Vec<_>>()` between `scalar_cost` and `name_key()` in the `min_by_key` key:
```rust
.min_by_key(|e| (scalar_cost(&e.expr), e.expr.support().into_iter().collect::<Vec<_>>(), e.name_key()))
```
`support()` returns `BTreeSet<usize>` (sorted ascending); collecting to `Vec<usize>` gives a deterministic `Ord` key that prefers the lexicographically smallest sorted column support — i.e., lower column indices win ties, matching the Phase-2a reducer's canonicalization.

### New test
`resolve_colored_equal_cost_prefers_lower_column_index` in `colored_derive.rs`: interns `#1 + 1` (payload) and `#0 + 1` (canonical), confirms equal `scalar_cost` (3), unions them under one color, resolves with `max_col = 2` (both in range), asserts result is `#0 + 1`. Fails without the support tie-break.

### Verification
- `bin/cargo-test -p mz-transform colored`: **43 passed** (includes new test + all prior colored tests).
- `bin/cargo-test -p mz-transform run_tests`: **PASSES UNCHANGED** — colored now emits `#0 + 1` matching the committed eqsat.spec golden (no REWRITE needed).
- `bin/cargo-test -p mz-transform`: **346 passed, 3 skipped, 0 failed**.
- `cargo clippy -p mz-transform --all-targets -- -D warnings`: clean.
- `bin/sqllogictest --optimized -- test/sqllogictest/joins.slt`: **136/136 PASS**, no panics.

### Commit
`231e9efa86` — only `src/transform/src/eqsat/colored_derive.rs` committed.
