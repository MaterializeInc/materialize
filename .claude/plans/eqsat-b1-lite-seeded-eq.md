# B1-lite: cross-round seeded Equivalences analysis

## Goal

Recompute the Equivalences analysis EXACTLY every changed saturation round (no
staleness), but cheaply, by seeding the Kleene fixpoint with the prior round's
result remapped through the new union-find. Replaces B2's `EQ_REFRESH_EVERY`
staleness, which produced equivalent-but-less-canonical plans (EXPLAIN golden
churn, e.g. delta-join key reordering in catalog_server_explain).

## Soundness

Analyses are monotone: the e-graph only grows (nodes added, classes merged,
never removed), so the old fixpoint pointwise <= the new fixpoint after
remapping through `find()`. A bounded (partial) prior result is <= old
fixpoint <= new fixpoint, hence a sound under-approximation = a valid Kleene
seed. Iterating up from it converges to the exact new fixpoint, in ~1 inner
iteration instead of ~4 from bottom.

## Files

* Modify `src/transform/src/eqsat/egraph.rs`:
  1. Add `run_analysis_seeded<A>(&self, a, max_iters, seed)` — like
     `run_analysis_bounded` but inits `m` by remapping `seed` through `find()`
     (merging collisions via `a.merge`), bottom for unseeded classes.
  2. Make `run_analysis_bounded` delegate to it with an empty seed (= all
     bottom), preserving current behaviour for nn/keys/mono callers.
  3. In `saturate`: drop `EQ_REFRESH_EVERY` / `rounds_since_eq`. Recompute eq
     every round, seeded from the prior `cached_eq` (remapped). Keep
     `cached_eq` to carry the seed across rounds.

## Verify

* `cargo run --release -p mz-transform --example eqsat_bench 2000`:
  filter_over_union / mixed should stay near B2 numbers (~2.4s / ~1.4s) or
  better, and not regress to pre-B2 (3.66s / 2.32s).
* `cargo test -p mz-transform eqsat` 117/117.
* Regenerate `eqsat.spec` — expect it to become MORE canonical (revert some
  B2 churn), exact fixpoint.
* Re-run the churned EXPLAIN SLTs locally; expect less churn than B2.
