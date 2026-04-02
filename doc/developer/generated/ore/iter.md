---
source: src/ore/src/iter.rs
revision: 2982634c0d
---

# mz-ore::iter

Extends the standard `Iterator` trait with convenience methods via `IteratorExt`.
Provides `chain_one` (append a single item), `all_equal` (check all elements are identical), `exact_size` (wrap an iterator with a caller-supplied exact length), and `repeat_clone` (pair each element with a cloned extra value, moving the last copy instead of cloning).
The concrete adaptor types `ExactSize<I>` and `RepeatClone<I, A>` are also exported.
Also provides `consolidate_iter` and `consolidate_update_iter` (available with the `differential-dataflow` feature) for proactively combining adjacent equal elements in an iterator, and `merge_iters_by` for merging a collection of sorted iterators into a single sorted iterator using a binary heap.
