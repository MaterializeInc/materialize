---
source: src/walkabout/src/lib.rs
revision: e757b4d11b
---

# mz-walkabout

Generates visitor and fold traversal code for Rust ASTs from their source definitions, used in Materialize to avoid manually maintaining `Visit`/`VisitMut`/`Fold` impls for the SQL parser AST.
The public API is the `load` function (which parses a Rust module into an `ir::Ir`) and the three generators `gen_fold`, `gen_visit`, `gen_visit_mut` (re-exported from `generated`).
`parse` reads and recursively collects type definitions; `ir` performs lightweight semantic analysis; `generated` emits the final Rust source strings.
Key dependencies are `syn` and `quote` for parsing/generating Rust; `mz-ore-build` provides the `CodegenBuf` writer; the crate is used at build time by `mz-sql-parser`.
