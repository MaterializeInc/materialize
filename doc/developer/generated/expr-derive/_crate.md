---
source: src/expr-derive/src/lib.rs
revision: 9c1e2767b0
---

# mz-expr-derive

Proc-macro crate that exposes the `#[sqlfunc]` attribute macro for deriving SQL function traits.
The macro generates implementations of `EagerUnaryFunc`, `EagerBinaryFunc`, or `EagerVariadicFunc` depending on the function arity, accepting parameters such as `sqlname`, `is_monotone`, `propagates_nulls`, `introduces_nulls`, and `output_type`.
The implementation is delegated entirely to `mz-expr-derive-impl`; this crate serves as the public proc-macro entry point required by the Rust proc-macro ABI.
