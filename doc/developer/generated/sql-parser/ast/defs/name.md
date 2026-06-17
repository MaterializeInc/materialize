---
source: src/sql-parser/src/ast/defs/name.rs
revision: 72277f8ac9
---

# mz-sql-parser::ast::defs::name

Defines identifier and name types used throughout the SQL AST.
`Ident` is a validated SQL identifier (max 255 bytes, no forbidden characters) with quoting/escaping support via `AstDisplay`.
Also defines multi-part name types: `UnresolvedItemName`, `UnresolvedSchemaName`, `UnresolvedDatabaseName`, `UnresolvedObjectName`, and `QualifiedReplica`.

`Ident` exposes two related methods for controlling quoting:
- `has_only_bare_chars() -> bool`: returns true when the identifier is composed solely of characters matching `[a-z_][a-z0-9_]*`. This is the character-level check only; it does not consider whether the name is a keyword. Contexts that need legible output without SQL round-trip requirements (e.g. `HumanizedExplain::humanize_ident`) use this instead of `can_be_printed_bare`.
- `can_be_printed_bare() -> bool`: returns true when the identifier passes `has_only_bare_chars` and also is not a keyword that requires quoting for round-trip safety. Beyond the `is_sometimes_reserved` and `begins_query_body` checks, identifiers are also force-quoted when they are: `AS` (consumed as the `AS OF` timestamp keyword in SELECT items), `ANY`/`ALL`/`SOME` (consumed as quantifier keywords after a comparison operator), `DISTINCT` (consumed as a projection quantifier right after `SELECT`), `LIST` (re-lexes as a `LIST[...]` literal when subscripted), `PREPARE` (consumed as the optional keyword in `DEALLOCATE [PREPARE]`), or `WHEN` (consumed as the start of a searched `CASE` arm when used as the `CASE` operand).
