---
source: src/ore/src/iter.rs
revision: 319ec72398
---

# mz-ore::iter

Extends the standard `Iterator` trait with convenience methods via `IteratorExt`.
Provides `chain_one` (append a single item), `all_equal` (check all elements are identical), `exact_size` (wrap an iterator with a caller-supplied exact length), and `repeat_clone` (pair each element with a cloned extra value, moving the last copy instead of cloning).
The concrete adaptor types `ExactSize<I>` and `RepeatClone<I, A>` are also exported.
