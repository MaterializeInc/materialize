---
source: src/timely-util/src/columnar/builder_input.rs
revision: 241f928bf3
---

# timely-util::columnar::builder_input

Implements `BuilderInput<KBC, VBC>` for both `ColumnBody<((K, V), T, R)>` and `Column<((K, V), T, R)>`, allowing differential dataflow `Builder`s to drain a batcher's chunks directly without an intermediate container conversion. The `ColumnBody` impl serves the chunk batchers whose chains are bodies; the `Column` impl serves the column pager's batcher whose chains are still edge containers.

Both implementations mirror the one for `ColumnationStack` in `columnation.rs`, but the associated `Item<'a>` is a columnar `Ref` tuple rather than a borrowed owned tuple. As a result:

- `Key<'a>` and `Val<'a>` are `columnar::Ref<'a, K>` and `columnar::Ref<'a, V>` — keys and values are read directly from the columnar layout with no owned round-trip.
- `Time` and `Diff` materialize as owned values via `into_owned` in `into_parts`, as the `BuilderInput` contract requires.

`key_val_upd_counts` tallies distinct keys, values, and updates per chunk and sums across the chain. Cross-chunk equality is not checked, so the counts may over-count by at most `chain.len()`. Downstream consumers treat these as capacity hints, so a small over-estimate is cheaper than snapshotting `K::Owned` / `V::Owned` across chunk boundaries.
