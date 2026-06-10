---
source: src/ore/src/permutations.rs
revision: 36f26c2c23
---

# mz-ore::permutations

Provides three small utilities for working with index permutations: `invert` (produces the inverse of a permutation as an iterator of `(target, source)` pairs), `argsort` (returns the indices that would sort a slice), and `inverse_argsort` (returns the permutation that maps the sorted order back to the original order).
