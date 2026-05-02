---
source: src/ore-build/src/lib.rs
revision: 44c64a9e5a
---

# ore-build

A lightweight utility crate for Materialize build scripts, kept separate from `mz-ore` to avoid compiling `ore`'s large dependency tree when cross-compiling build scripts for the host.

## Module structure

* `codegen` — provides `CodegenBuf`, an indentation-aware string buffer for generating Rust code at build time.

## Dependencies

The crate has no substantive runtime dependencies; it is intentionally minimal so it compiles quickly as a build-script dependency.

## Consumers

Any Materialize `build.rs` script that needs to emit generated Rust code should depend on this crate instead of `mz-ore`.
