---
source: src/expr/src/scalar/reduce/unary.rs
revision: 5d046b3ab6
---

# mz-expr::scalar::reduce::unary

Post-order rewrites for `CallUnary` nodes, called from `reduce::reduce_post`.

## Entry point

`reduce_call_unary(e, column_types, temp_storage)` requires `e` to be a `CallUnary` node. It applies two rewrites:

- **Constant folding** — if the operand is a literal and the function is not `Panic`, evaluate the call immediately and replace it with a typed literal. `Panic` is excluded because it should retain its error-producing behavior rather than be folded away at planning time.

- **`RecordGet` / `RecordCreate` cancellation** — `RecordGet(i)(RecordCreate([e0, e1, ..., en]))` is simplified to `ei` directly, eliminating both the record construction and the field projection.
