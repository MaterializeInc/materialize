---
source: src/sql-pretty/src/doc.rs
revision: 8f05f1b28d
---

# mz-sql-pretty::doc

Implements the `Pretty` struct's methods that convert individual SQL AST node types to `RcDoc` values.
Each `doc_*` method corresponds to a specific statement or clause kind (e.g., `doc_select_statement`, `doc_create_view`, `doc_create_source`), using the utility combinators from `util` to build indented, comma-separated, and bracket-wrapped layouts.
`doc_display` / `doc_display_pass` serve as a fallback for node types that lack a dedicated pretty-printing path, delegating to the `AstDisplay` trait.
`doc_create_role` and `doc_alter_role` handle `CREATE ROLE` and `ALTER ROLE` statements; the private `doc_role_attribute` helper preserves `PASSWORD` values verbatim (rather than redacting them as the `AstDisplay` safety net does) so the pretty-printer can round-trip user SQL.
`doc_create_materialized_view` emits the `AS OF` clause when present.
