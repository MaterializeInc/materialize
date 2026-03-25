---
source: src/expr/src/scalar/func/impls.rs
revision: de1872534e
---

# mz-expr::scalar::func::impls

Re-exports all type-specific scalar function implementations, organized into one submodule per SQL type.
Each submodule uses `#[sqlfunc]`-annotated functions or manual `LazyUnaryFunc`/`EagerUnaryFunc`/`EagerBinaryFunc` implementations that are collected into the `UnaryFunc`, `BinaryFunc`, and `VariadicFunc` enums by `macros.rs`.

Submodules: `array`, `boolean`, `byte`, `case_literal`, `char`, `date`, `datum`, `float32`, `float64`, `int16`, `int2vector`, `int32`, `int64`, `interval`, `jsonb`, `list`, `map`, `mz_acl_item`, `mz_timestamp`, `numeric`, `oid`, `pg_legacy_char`, `range`, `record`, `regproc`, `string`, `time`, `timestamp`, `uint16`, `uint32`, `uint64`, `uuid`, `varchar`.
