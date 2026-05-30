---
source: src/timely-util/src/columnar/builder_input.rs
revision: 3506b9aee8
---

# timely-util::columnar::builder_input

Implements differential dataflow's `BuilderInput` trait for `Column<((K, V), T, R)>` so differential dataflow `Builder`s can drain the paged batcher's output without an extra container conversion.

## Trait implementation

The impl is generic over `K`, `V`, `T`, `R`, `KBC`, and `VBC` with the following associated types:

- `Key<'a>` = `columnar::Ref<'a, K>` — a zero-copy columnar reference, not an owned value.
- `Val<'a>` = `columnar::Ref<'a, V>` — same.
- `Time` = `T` (owned).
- `Diff` = `R` (owned).

This differs from the analogous `ColumnationStack` implementation: because `Column<C>` stores data in columnar form, reads yield `Ref` types without materializing owned `K`/`V` values.

## Methods

`into_parts` destructs a single item `((key, val), time, diff)` and returns `(key, val, T::into_owned(time), R::into_owned(diff))`. Keys and values pass through as columnar references; times and diffs are converted to owned values per the trait contract.

`key_eq` and `val_eq` compare a stored `columnar::Ref` against the batch container's `ReadItem` using `BatchContainer::reborrow` for lifetime alignment.

`key_val_upd_counts` tallies distinct keys, values, and updates per chunk and sums across the chain. Cross-chunk boundaries are not checked: if the last entry of one chunk equals the first of the next, both are counted separately, giving at most `chain.len()` over-counts. The downstream consumers use these counts as capacity hints, so a small over-estimate is cheaper than snapshotting `K::Owned`/`V::Owned` values across chunk boundaries.
