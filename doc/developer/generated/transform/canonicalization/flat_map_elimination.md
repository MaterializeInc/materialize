---
source: src/transform/src/canonicalization/flat_map_elimination.rs
revision: 703a0c27c8
---

# mz-transform::canonicalization::flat_map_elimination

Implements `FlatMapElimination`, which removes `FlatMap` operators that can be reduced to something simpler when the table function's arguments are all constants.
When the function evaluates to zero rows the subtree is replaced with an empty constant; when it evaluates to exactly one row the `FlatMap` is replaced with a `Map`.
It also handles the `Wrap` table function specially: if `Wrap`'s width exceeds its argument count the `FlatMap` is eliminated unconditionally.
