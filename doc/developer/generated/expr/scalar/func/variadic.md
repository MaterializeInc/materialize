---
source: src/expr/src/scalar/func/variadic.rs
revision: 9c1e2767b0
---

# mz-expr::scalar::func::variadic

Implements variadic scalar functions (those taking a variable number of arguments) using `#[sqlfunc]`-annotated functions, and defines the `LazyVariadicFunc` trait.
Includes functions such as `coalesce`, `greatest`/`least`, `concat`, `make_timestamp`, array/list/map constructors, cryptographic digest functions (`md5`, `sha256`, etc.), `jsonb_build_object`, `regexp_match`, and date/time formatting.
