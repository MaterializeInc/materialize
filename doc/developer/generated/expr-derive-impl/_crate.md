---
source: src/expr-derive-impl/src/lib.rs
revision: fe91a762d1
---

# mz-expr-derive-impl

Contains the implementation of the `#[sqlfunc]` proc-macro, split out from the proc-macro crate so the logic can be exported and tested independently.
Re-exports `sqlfunc` (the expansion entry point) and, under the `test` feature, exposes `test_sqlfunc` / `test_sqlfunc_str` helpers used by snapshot tests.
The `lib.rs` test module contains `insta` snapshot tests covering all arity patterns: unary (plain, ref, arena), binary (plain, arena, generic), and variadic (tuple, `Variadic<T>`, arena, `&self`, modifiers, generics).

## Module structure

* `sqlfunc` -- arity detection, code generation, and modifier parsing

## Key dependencies

* `darling`, `syn`, `quote`, `proc-macro2` -- proc-macro infrastructure
* `prettyplease` -- Rust code formatting for test snapshots
* `insta` -- snapshot testing (dev/test only)

## Downstream consumers

* `mz-expr-derive` -- the proc-macro crate that re-exports the `#[sqlfunc]` attribute
* `mz-expr` -- uses `#[sqlfunc]` throughout `scalar::func` to define scalar functions
