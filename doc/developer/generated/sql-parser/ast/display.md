---
source: src/sql-parser/src/ast/display.rs
revision: bb08b11a38
---

# mz-sql-parser::ast::display

Defines the `AstDisplay` trait and `AstFormatter<W>`, which drive SQL-to-string rendering of all AST nodes.
`AstFormatter` carries a `FormatMode` (`Simple`, `Stable`, or `Redacted`) that controls whether literals are redacted, enabling privacy-safe display of queries.
Also provides `DisplaySeparated`, a helper for rendering comma- or space-separated lists of `AstDisplay` items, and macros such as `impl_display_for_ast_display!`.
`WithOptionName` is a trait implemented by option-name enums (e.g. `TableOptionName`, `MaterializedViewOptionName`). Its `redact_value() -> bool` method controls whether the associated option value is redacted under `FormatMode::Redacted`. Returning `false` disables redaction for the value and all of its descendants: the `impl_display_for_with_option!` macro fully un-redacts the formatter before rendering the value, so every nested literal, sub-option, and expression underneath it prints verbatim. Returning `true` delegates to `WithOptionValue`'s per-type logic, which redacts scalar literals but leaves identifiers (e.g. column lists) verbatim.
