---
source: src/expr-derive-impl/src/lib.rs
revision: 9c1e2767b0
---

# mz-expr-derive-impl

Contains the implementation of the `#[sqlfunc]` proc-macro, split out from the proc-macro crate so the logic can be exported and tested independently.
Re-exports `sqlfunc` (the expansion entry point) and, under the `test` feature, exposes `test_sqlfunc` / `test_sqlfunc_str` helpers used by snapshot tests in the `test` module.
Depends on `darling`, `syn`, `quote`, and `proc-macro2`; consumed by `mz-expr-derive` and test harnesses.

## Module structure

* `sqlfunc` — arity detection, code generation, and modifier parsing
