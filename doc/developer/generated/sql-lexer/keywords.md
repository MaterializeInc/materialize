---
source: src/sql-lexer/src/keywords.rs
revision: 72277f8ac9
---

# mz-sql-lexer::keywords

Defines the `Keyword` enum, per-keyword constants, and a compile-time perfect-hash lookup map; the enum body and the `KEYWORDS: phf::Map` are generated from `keywords.txt` by the crate's build script.

Hand-written methods classify each keyword by reservation level: `is_always_reserved` (top-level SELECT clause starters and set operators), `is_reserved_in_scalar_expression`, `is_reserved_in_table_alias`, and `is_reserved_in_column_alias`; the `is_sometimes_reserved` method is the union of all four.
`FromStr` uses `KEYWORDS` for case-insensitive lookup and `Display` delegates to `as_str()`.

Two additional classification methods support round-trip-safe `AstDisplay` quoting decisions:
- `begins_query_body() -> bool`: returns true for keywords that begin a query body (`WITH`, `SELECT`, `VALUES`, `SHOW`, `TABLE`). When `AstDisplay` wraps an expression in parentheses (e.g. for a field access), an identifier with one of these names would be reparsed as a subquery clause rather than an identifier; such identifiers must be quoted.
- `is_context_sensitive_keyword() -> bool`: returns true for keywords that have a special parser-dispatch form triggered by the next token (e.g. `POSITION(expr IN expr)`, `MAP[K => V]`). Emitting these unquoted in expression position causes the reparser to enter the special grammar rather than parsing a plain identifier. The set includes: `ALL`, `ANY`, `COALESCE`, `EXISTS`, `EXTRACT`, `GREATEST`, `LEAST`, `MAP`, `NORMALIZE`, `NULLIF`, `POSITION`, `ROW`, `SOME`, `SUBSTRING`, `TRIM`.
