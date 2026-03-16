---
source: src/expr/src/scalar/func/impls/array.rs
revision: 703a0c27c8
---

# mz-expr::scalar::func::impls::array

Provides scalar function implementations operating on array datums, such as casting arrays to lists, subscripting, slicing, computing array length/lower bound, and array-to-string conversion.
Functions are defined using `#[sqlfunc]` and are re-exported into `UnaryFunc`/`BinaryFunc`/`VariadicFunc` via the parent `impls` module.
