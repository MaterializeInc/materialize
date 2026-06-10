---
source: src/storage/src/source/generator/counter.rs
revision: 6c50c23bea
---

# mz-storage::source::generator::counter

Implements the `Counter` load-generator, which emits an incrementing integer stream optionally bounded by a maximum cardinality, retracting old values once the bound is reached.
