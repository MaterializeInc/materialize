---
source: src/expr/src/scalar/reduce/unary.rs
revision: ed05cf7584
---

# mz-expr::scalar::reduce::unary

Post-order rewrites for `CallUnary` nodes, called from `reduce::reduce_post`.

## Entry point

`reduce_call_unary(e, column_types, temp_storage)` requires `e` to be a `CallUnary` node. It applies two rewrites:

- **Constant folding** — if the operand is a literal and the function is not `Panic`, evaluate the call immediately and replace it with a typed literal. `Panic` is excluded because it should retain its error-producing behavior rather than be folded away at planning time.

- **Inverse-function cancellation** — `f(g(x))` is simplified to `x` when `g` declares `f` as its inverse (via `g.inverse()`), `g` preserves uniqueness, and both `f` and `g` are infallible and propagate nulls. This admits involutions such as `NOT(NOT(x))`, `~(~x)`, `reverse(reverse(x))`, and numeric negation, as well as infallible bijective cast round-trips. Fallible functions (e.g., integer negation, narrowing casts) are excluded because eliding them would suppress errors.

- **`RecordGet` / `RecordCreate` cancellation** — `RecordGet(i)(RecordCreate([e0, e1, ..., en]))` is simplified to `ei` directly, eliminating both the record construction and the field projection.
