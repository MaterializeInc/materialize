# Task 4 report: `colored_derive::derive`

**Status:** DONE.

## What was implemented

- Made `reduce_escalar` `pub(crate)` in `src/transform/src/eqsat/egraph/saturate.rs`
  (body unchanged) and re-exported it `pub(crate) use self::saturate::reduce_escalar;`
  from the `egraph` module root (`src/transform/src/eqsat/egraph.rs`).
- Added `ScopeEqualities`, `DerivedScopes`, and `pub(crate) fn derive(eg: &mut EGraph)`
  to `src/transform/src/eqsat/colored_derive.rs`, plus 4 new fixtures and the 4
  tests from the brief.

## Reducer freshness

`derive_facts` runs the analysis through `run_analysis_bounded` →
`minimize_bounded`, which populates each fact's reducer (`remap`). The pre-existing
Task-3 test `filter_eq_derives_column_equivalence` already asserts the reducer is
non-empty after `derive_facts`, so freshness holds in practice. As belt-and-suspenders
and to honor the "read the reducer only after minimize" contract, `derive` reads
reducers through a `fresh_reducer` helper that clones + `minimize(None)` only if the
reducer is unexpectedly empty. Purely additive: a genuinely equivalence-free class
still yields an empty reducer (no reduction).

## Filter-input vs Map-own reducer choice

Mirrors Phase-2a `rewrite_escalars` exactly:
- **Filter** predicates reduce against the **input class's** reducer
  (`facts[&eg.find(input)]`), `max_col = eg.arity(input)` — avoids the circular
  "drop the filter" pathology.
- **Map** scalars at position `pos` reduce against the **Map class's own** reducer,
  `max_col = eg.arity(input) + pos` — the `max_col` guard inside `reduce_escalar`
  rejects a rewrite to the scalar's own (forward) output column.
- All other node kinds skipped.

Empty/`None` facts are recorded in `empty_classes` and not processed for scopes.
Interning (`intern_scalar`) mutates `eg` but never unions relational classes, so
canonical ids from `facts` stay valid through the pass; recorded ids are the
pre-rebuild ids the caller re-canonicalizes later (per the brief).

## Dedup

Per-class union lists are sorted + deduped and used as a `HashMap` key so classes
with byte-identical equality-sets share one `scopes` index. The
`two_filters_same_equality_fixture` (two differently-named `Get`s under identical
`Filter[NOT(#1)]/Filter[#0=#1]` stacks) exercises this: both reduce `NOT(#1)` to the
same interned `NOT(#0)` → 2 classes, 1 scope.

## Deviations

- The brief's `map_self_reference` test used `.get(...).is_none()`; clippy
  (`unnecessary_get_then_check`, `-D warnings`) required `!...contains_key(...)`.
  Changed accordingly.
- `contradictory_filter_fixture` already existed from Task 3; reused it rather than
  rewriting.

## Verification

- `bin/cargo-test -p mz-transform colored_derive`: 7 passed (4 new).
- `bin/cargo-test -p mz-transform`: 338 passed, 3 skipped.
- `cargo clippy -p mz-transform --all-targets -- -D warnings`: clean.

## Concerns

None. `derive` has no production caller yet (T5/T6 wire it); change is additive and
behavior-neutral.

---

# Task 4 (M6 + M7) addendum — SP4c hardening nits

**Status:** DONE  
**Commit:** `3d46b046f8` — `SP4c: bound fresh_reducer fallback (M6); doc empty-unsat divergence (M7)`

## Changes

File: `src/transform/src/eqsat/colored_derive.rs`

**M6** (`fresh_reducer`): replaced `clone.minimize(None)` with
`clone.minimize_bounded(None, 100)` and added a 5-line comment explaining the
fallback is not expected to fire, and that matching the analysis-merge bound
ensures the fallback can never over-reduce relative to production.

**M7** (`derive`, `if fact_is_empty(fact)` branch): added a 5-line comment
marking the empty-but-unsat routing as an intentional, sound divergence from
Phase-2a — extraction empty-folds the class regardless of predicate spelling, so
rewriting those predicates is unnecessary work.

No other files touched. No golden files moved.

## Tests

```
bin/cargo-test -p mz-transform colored_derive   → 15/15 passed
bin/cargo-test -p mz-transform                  → 345/345 passed, 3 skipped
cargo clippy -p mz-transform --all-targets -- -D warnings  → clean
```

## Golden changes

Zero. The `fresh_reducer` fallback is not reached by any test fixture, so
swapping `minimize(None)` → `minimize_bounded(None, 100)` produced no output
change. `run_tests` and `test_runner` both passed without regeneration.

## Concerns

None.

---

# Task 4 (delta join): `commit_delta_query` + `delta_new_arrangements`

**Status:** DONE

## Functions Added

### `src/transform/src/eqsat/join_commit.rs`

**`pub(crate) fn delta_new_arrangements`**: counts distinct `(input, key)` lookup
arrangements across all paths not already in `available`. Uses a `BTreeSet` to
deduplicate. Mirrors `delta_queries::plan` (join_implementation.rs:727-739).

**`pub(crate) fn commit_delta_query`**: lowers a bare `Unimplemented` `Join` to
`JoinImplementation::DeltaQuery(orders)`. Each inner `Vec` is a lookup sequence
for one driver (driver excluded from path — lookups only, no start key stored).
Characteristics computed via `step_characteristics`. Reuses
`implement_arrangements` / `permute_order` / `install_lifted_mfp`.

### `src/transform/src/eqsat/cost.rs` (guard)

Added `n >= 32` early return in `delta_join_order` immediately after the `n == 1`
return. Prevents a `1u32 << driver` overflow panic on wide joins. Mirrors
`delta_join_terms`.

## `permute_order` Signature Confirmed

```rust
pub(crate) fn permute_order(
    order: &mut Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>,
    lifted_projections: &Vec<Option<Vec<usize>>>,
)
```

DeltaQuery tuple shape matches exactly.

## Test Command and Output

```
cargo nextest run -p mz-transform "eqsat::join_commit" "eqsat::cost::tests::delta_join_order"
```

7 tests run: 7 passed (2 new: `delta_new_arrangements_counts_distinct_missing`,
`commit_delta_query_builds_deltaquery_shape`), 383 skipped.

## Cargo Check

`cargo check -p mz-transform` clean, only expected `dead_code` warnings on the 3
new/existing functions (`delta_join_order`, `delta_new_arrangements`,
`commit_delta_query`) — all wired in Task 5.

## Concerns

None. `commit_delta_query` and `delta_new_arrangements` have no production caller
yet (Task 5 wires them); change is additive and behavior-neutral.
