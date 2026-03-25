---
source: src/sql-parser/src/ast/display.rs
revision: 326b2e7ebb
---

# mz-sql-parser::ast::display

Defines the `AstDisplay` trait and `AstFormatter<W>`, which drive SQL-to-string rendering of all AST nodes.
`AstFormatter` carries a `FormatMode` (`Simple`, `Stable`, or `Redacted`) that controls whether literals are redacted, enabling privacy-safe display of queries.
Also provides `DisplaySeparated`, a helper for rendering comma- or space-separated lists of `AstDisplay` items, and macros such as `impl_display_for_ast_display!`.
