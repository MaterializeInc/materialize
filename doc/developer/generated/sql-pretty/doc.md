---
source: src/sql-pretty/src/doc.rs
revision: 72277f8ac9
---

# mz-sql-pretty::doc

Implements the `Pretty` struct's methods that convert individual SQL AST node types to `RcDoc` values.
Each `doc_*` method corresponds to a specific statement or clause kind (e.g., `doc_select_statement`, `doc_create_view`, `doc_create_source`), using the utility combinators from `util` to build indented, comma-separated, and bracket-wrapped layouts.
`doc_display` / `doc_display_pass` serve as a fallback for node types that lack a dedicated pretty-printing path, delegating to the `AstDisplay` trait.
`doc_create_role` and `doc_alter_role` handle `CREATE ROLE` and `ALTER ROLE` statements; the private `doc_role_attribute` helper preserves `PASSWORD` values verbatim (rather than redacting them as the `AstDisplay` safety net does) so the pretty-printer can round-trip user SQL.
`doc_create_materialized_view` emits the `AS OF` clause when present.

`doc_create_index` force-quotes an index name whose text equals `in` (case-insensitive), matching the `CreateIndexStatement` `AstDisplay` impl, to avoid the name being consumed as the start of the optional `IN CLUSTER` clause on reparse.

`doc_subscribe` emits `SUBSCRIBE TO` instead of `SUBSCRIBE` when `SubscribeRelation::needs_explicit_to` returns true (i.e. simple/simple-redacted mode and the relation name starts with the bare identifier `to`), preventing the relation name from being consumed as the optional `TO` keyword.

`doc_declare` and `doc_prepare` pretty-print `DECLARE <name> CURSOR FOR <stmt>` and `PREPARE <name> AS <stmt>` respectively, recursing into the inner statement via `to_doc` rather than the redacting `AstDisplay` fallback, so secrets carried by the inner statement (e.g. a role password) survive the round trip losslessly.

`doc_select_statement` wraps the query in parentheses when `query.body.starts_with_show()` is true, mirroring the `AstDisplay for SelectStatement` behavior.

`doc_expr` for `Expr::Op` (prefix form) mirrors the `prefix_operand_needs_parens` logic from `AstDisplay`: it peels tight postfix forms (`Cast`, `Subscript`) and wraps the operand in a `bracket("(", …, ")")` call when the chain bottoms out at a numeric literal or a non-self-delimiting expression.

`doc_function` mirrors the function-name quoting logic from `AstDisplay for Function`: names that clash with special-grammar keywords (`array`, `coalesce`, `exists`, `extract`, `greatest`, `least`, `list`, `map`, `normalize`, `nullif`, `position`, `row`, `substring`, `trim`) are emitted in the always-quoted stable form. The `extract` and `position` special forms delegate to `doc_display` rather than being pretty-printed inline.
