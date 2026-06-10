---
source: src/sql-parser/src/lib.rs
revision: e757b4d11b
---

# mz-sql-parser

Provides an SQL lexer and parser for Materialize's SQL dialect, producing an unresolved `AST<Raw>` from SQL text.

Key modules:
* `parser` — recursive-descent parser; entry point is `parse_statements`
* `ast` — all AST node types, re-exported flat; includes `display`, `fold`, `visit`, `visit_mut`
* `ident` (private, re-exported via macro) — compile-time `Ident` construction

Key dependencies: `mz-sql-lexer` (tokenization), `mz-walkabout` (build-time visitor generation), `mz-ore`.
The `test` feature enables `datadriven_testcase` for parser roundtrip tests.
Downstream consumers include `mz-sql` (planning), `mz-sql-pretty` (formatting), `mz-lsp-server`, and `mz-expr-parser`.
