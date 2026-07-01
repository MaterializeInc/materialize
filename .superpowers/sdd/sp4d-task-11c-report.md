# SP4d Task 11c — Resolve colored scalar payloads under input context

**Status: DONE**

Fixes a confirmed, shipped soundness bug in the eqsat optimizer's colored scalar
resolution (`mz-transform`). With `COLORED_SATURATION = false` (production) and
`enable_eqsat_optimizer = true` (default on), nested identical filters were
collapsed to a tautology filter that returned **all** rows.

Switch stays OFF — this is purely the soundness fix to the always-on colored
resolution.

## The bug (trigger case before/after)

`Filter(#0=1)(Filter(#0=1)(Get t0))`:
- **Before:** `Filter[1=1, 1=1](Get t0)` — the predicate `#0=1` was folded to the
  tautology `1=1` (which `coalesce_mfp` strips → `Get`), returning ALL rows.
- **After:** `Filter(#0=1)(Get t0)` — sound, still filters `#0 = 1`.

### Root cause

SP4b colored scalar resolution gave each relational e-class ONE color and
resolved every e-node's payload under that class color. After `merge_filters`
fuses nested filters, one filter e-class can hold SIBLING e-nodes with DIFFERENT
inputs:
- node A: `Filter[pred](F2)` where `F2 = Filter[#0=1](Get)` — F2 proves `#0=1`, so
  under F2's context `pred` correctly reduces to `true`.
- node B: `Filter[pred, pred](Get)` — the merged node; its input is the bare
  `Get`, which proves nothing.

`derive()` computed the reduction `pred → true` with node A's INPUT reducer
(correct) but stored it in the FILTER CLASS's color; extraction then resolved
node B's predicates under that SAME class color, wrongly folding node B's `pred`
to `true`. The same applies to `fuse_maps` siblings.

## The fix: key payload resolution by the node's INPUT context

The contextual equivalences valid for a Filter predicate / Map scalar are the
node's INPUT's output-equivalences (what holds of the rows BEFORE the operator).
So both recording (`derive`) and resolution (`build_rel`) are now keyed by the
node's **input** class, never the enclosing class.

### `derive` (`src/transform/src/eqsat/colored_derive.rs`)

- Reductions are accumulated into `by_context: HashMap<Id, Vec<(Id,Id)>>` keyed by
  the **canonical input class** (`eg.find(input)`), not the enclosing class. The
  flattened result is sorted by context-class id for determinism, then fed
  through the unchanged scope-dedup, so `class_scope`/`color_of` are now keyed by
  input classes.
- Filter and Map are unified: both reduce a payload against the **input class's**
  reducer (`fresh_reducer(input_ec)`) with `max_col = arity(input)`.
  - This changes Map, which previously used the **Map class's own** reducer with
    `max_col = arity(input) + pos`. The own-reducer additionally knows map-output
    definitions, so it could rewrite a scalar to an earlier *map-output* column.
    Such a spelling is local to a single Map node and must never be shared via
    the input context, so the `max_col = arity(input)` guard now rejects any
    out-of-input-range rewrite. The result: `by_context[input]` only ever asserts
    congruences that genuinely hold over the input's output rows — the clean,
    sound invariant `color_of[X] = equivalences valid over X's output rows`.
  - For the existing Map fixtures this is behavior-preserving: the redundant-
    recompute case (`#1+1` over `Filter[#0=#1+1]`, input arity 2) still reduces
    `#1+1 → #0` (in range `< 2`); the self-reference case (`#1+1` over `Get`) still
    records nothing (empty input reducer). The own-reducer's `own_reducer` local
    is removed (no longer used).

### Extraction (`src/transform/src/eqsat/egraph/build.rs`)

- `resolve_scalars_in_color` now takes a `context: Id` parameter and looks up
  `color_of[find(context)]`.
- `build_rel`'s Filter and Map arms pass `*input` (the node's input class) as the
  context, not the enclosing `class`. The `max_cols` for the extraction-time
  range guard are unchanged (`arity(input)` for Filter, `arity(input)+pos` for
  Map) — all colored members are `< arity(input)` so the more-permissive Map
  bound selects no out-of-range spelling.
- The empty-fold still uses the node's own `class` (a class proven empty folds
  regardless of input), unchanged.

### Interaction with the hierarchical color forest (T7)

`build_colored_layer` is unchanged. It builds the inclusion-ordered color forest
and `color_of` from `DerivedScopes`; the only difference is the keys of
`class_scope`/`color_of` are now input (context) classes. Because the
input-keyed reductions only ever reference columns `< arity(input)`, every
seeded equality is a valid output-row congruence of that context class, so the
parent/child inclusion lattice and `close_all` congruence closure operate over a
well-formed (and now sound) set of per-color equalities. The input-keyed lookup
is an ordinary `color_of` lookup, so it composes with the forest exactly as
before.

## Tests

### Primary soundness regression test (RED-then-GREEN)

`eqsat::engine::tests::nested_identical_filters_retain_predicate` drives the full
eqsat optimizer (`Optimizer::optimize`, the default colored path) on
`Filter[#0=1](Filter[#0=1](Get t0))` and asserts the result (a) is not a bare
`Get` and (b) still contains the `#0 = 1` filter predicate.

- **RED proof (pre-fix):** failed with
  `got Filter { input: Get { name: "t0", arity: 2 }, predicates: [Eq(1,1), Eq(1,1)] }`
  — the predicate `#0=1` folded to the tautology `1=1`.
- **GREEN (post-fix):** passes.

### Datadriven goldens (`eqsat.spec`)

Added two cases (no existing golden moved):
- **Case (a2):** the same-predicate nested-filter trigger → golden
  `Filter (#0 = 1) / Get t0` (sound, retains the predicate).
- **Case (a3):** single `Filter (#0 = 1) (Get t0)` → unchanged (sound baseline).
- Case (a) (different predicates `#0=1`, `#1=1`) is **unchanged**: `#1=1` does not
  imply `#0=1`, so no fold ever occurred — confirming the fix is inert outside
  the buggy regime.

### Existing colored unit tests re-keyed (convention change, not behavior)

Three `colored_derive` tests asserted the old enclosing-class keying of
`class_scope`/`color_of` for a Map fixture; they now look up via the Map node's
input class (new `map_input` helper). The behavior they verify (a reduction is
recorded; the union is applied under the color; resolution picks the cheaper
member) is preserved:
- `map_scalar_reduced_to_column_is_interned_and_unioned`
- `build_colored_layer_applies_unions`
- `resolve_colored_picks_cheaper_member`

## `git diff --stat` and per-area soundness justification

```
 src/transform/src/eqsat/colored_derive.rs      | 168 +++++++++++-----------
 src/transform/src/eqsat/egraph/build.rs        |  31 +++--
 src/transform/src/eqsat/engine.rs              |  57 +++++++++
 src/transform/tests/test_transforms/eqsat.spec |  27 ++++
```

`src/transform/tests`:
- `eqsat.spec` (+27): **case (a2)** is the soundness regression — sound, retains
  `#0=1`. **case (a3)** is identity — sound. No pre-existing case moved.
- `engine.rs` test module (+57): added the primary regression test + helpers; no
  golden, pure assertion on a sound property.

`git diff --stat src/transform/tests` (test files only):
```
 src/transform/tests/test_transforms/eqsat.spec | 27 +++
```
(The engine.rs regression test lives in `src/transform/src/...`, alongside the
code under test.)

## Gates

- **Primary regression test:** RED (pre-fix `Filter[1=1,1=1](Get)`) → GREEN.
- **Datadriven `run_tests`:** PASS with no rewrite (0 spec goldens moved beyond
  the 2 added cases).
- **Colored oracle differential tests** (`eqsat::colored::oracle`, incl.
  `close_matches_oracle_random`): 6/6 PASS.
- **All colored tests** (`eqsat::colored*`, `eqsat::colored_derive`,
  `eqsat::egraph::build`): PASS (21/21 + colored suite).
- **Full `mz-transform` suite:** 366/366 PASS (incl. `compare_real` differential
  harness, `validation`).
- **slt goldens:** `test/sqllogictest/transform/*.slt` — **0 goldens moved**.
  The one in-suite failure (`case_literal.slt:246`, a `WITH(raw)` JSON EXPLAIN) is
  a **pre-existing flake**: it reproduces identically on the base branch with my
  changes stashed, passes 5/5 when run alone, and `--rewrite-results` produces a
  byte-identical file. Non-buggy plans are byte-identical before/after (single
  filters, case (a)), so the only goldens that *could* move are ones that
  captured the unsound plan — there are none.
- **Clippy** `-D warnings` on `mz-transform --all-targets`: clean (exit 0).

## Cases I was unsure about / notes

- **Map reducer change (own → input):** I switched Map to the input reducer with
  `max_col = arity(input)` rather than preserving the own-reducer +
  `arity(input)+pos`. This is required for soundness once Map reductions are
  shared via the input context (a map-output column reference is node-local and
  would conflate distinct sibling Maps). It can in principle lose a rare
  earlier-map-output CSE, but no golden (spec or transform slt) moved, so no such
  case is exercised in the corpus. The redundant-map-compute engine test and the
  Map derive fixtures all still pass.
