---
source: src/walkabout/src/parse.rs
revision: 7c2004f36c
---

# mz-walkabout::parse

Provides `parse_mod`, which reads a Rust source file at a given path and recursively collects all `struct`, `enum`, and `union` items as `syn::DeriveInput` values.
Handles inline module declarations by resolving submodule paths relative to the parent file (`mod.rs` / `lib.rs` convention), matching the file layout used in Materialize.
The result feeds directly into `ir::analyze` to build the walkabout IR.
