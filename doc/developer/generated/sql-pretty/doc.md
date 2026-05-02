---
source: src/sql-pretty/src/doc.rs
revision: 5680493e7d
---

# mz-sql-pretty::doc

Implements the `Pretty` struct's methods that convert individual SQL AST node types to `RcDoc` values.
Each `doc_*` method corresponds to a specific statement or clause kind (e.g., `doc_select_statement`, `doc_create_view`, `doc_create_source`), using the utility combinators from `util` to build indented, comma-separated, and bracket-wrapped layouts.
`doc_display` / `doc_display_pass` serve as a fallback for node types that lack a dedicated pretty-printing path, delegating to the `AstDisplay` trait.
