# Task D2: Index-aware cost model report

## How index availability is represented

`CostModel` gains a `available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>` field.
Each entry maps a global relation id to the list of index keys available on it (each key is the ordered `MirScalarExpr` slice from `IndexOracle::indexes_on`).
`CostModel::new()` / `CostModel::default()` produce an empty map — index-blind, backward-compatible.
`CostModel::with_available(available)` seeds it with real availability.

## How availability is threaded

* `eqsat.rs` adds `optimize_with_availability(expr, BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>)` which builds a `CostModel::with_available(available)` and passes it to the optimizer.
  Existing `optimize` and `optimize_logical` pass an empty map — unchanged behavior.
* `transform.rs` `PhysicalEqSatTransform::actually_perform_transform` now uses `ctx` (no longer `_ctx`).
  It calls `build_availability(relation, ctx.indexes)` to walk the plan for global `Get` nodes and query `oracle.indexes_on` for each, then passes the result to `optimize_with_availability`.
  The logical `EqSatTransform` is unchanged (still calls `optimize_logical`, which uses empty availability).

## The cost change

In `collect_memory` for `Rel::WcoJoin`, each input's arrangement term is now conditionally skipped via `input_already_arranged(input, offset, equivalences)`:

1. If `available` is empty, return false immediately (index-blind path, zero overhead).
2. If the input is not a direct `Rel::Opaque(MirRelationExpr::Get { Id::Global(gid) })`, return false (conservatively, no match for wrapped inputs).
3. Compute the join key for this input: the set of local column indices (relative to the input's offset) that appear in equivalence classes shared with at least one other input (`join_key_cols_for_input`).
4. Check if any available index on `gid` has a key whose column set (plain `Column(k)` references) equals the join key set.
5. If yes, skip the memory term — the arrangement is already present.

Only plain-column index keys are matched; expression-valued index keys are ignored.

## New tests (cost.rs `#[mz_ore::test]`)

* `index_aware_no_arrangement_term_for_indexed_input`: triangle WcoJoin with an index on R covering exactly its join-key columns `[0,1]`. Blind model: 3 memory terms. Index-aware model: 2 memory terms (R's term suppressed).
* `index_aware_wrong_key_still_charges_arrangement`: index on R with key `[0]` only (partial). All 3 terms still charged.
* `index_blind_triangle_still_prefers_wcoj`: with `CostModel::new()` and opaque global-get inputs, WcoJoin still dominates binary join (regression guard).

## Test results

```
cargo test -p mz-transform --lib eqsat::cost   8 passed, 0 failed
cargo test -p mz-transform --lib eqsat         52 passed, 0 failed
cargo test -p mz-transform --test wcoj_decision 6 passed, 0 failed
cargo clippy -p mz-transform                   0 warnings
```

No-index triangle: still WcoJoin (covered by `egraph_picks_wcoj_for_triangle` in wcoj_decision and by `index_blind_triangle_still_prefers_wcoj` in cost).

## Plumbing shortcuts

Extraction stays untouched: the `CostModel` is passed into `engine::Optimizer::new` which stores it; extraction calls `model.cost(&rel)` on the built `Rel`.
No changes to `egraph.rs`, `engine.rs`, or the e-graph extraction path were needed — the availability is an opaque field on `CostModel` that the existing extraction loop picks up automatically.

## Commit

`2ecd1956b1` eqsat: index-aware cost for the physical placement
