---
source: src/sql-lexer/src/keywords.rs
revision: 4267863081
---

# mz-sql-lexer::keywords

Defines the `Keyword` enum, per-keyword constants, and a compile-time perfect-hash lookup map; the enum body and the `KEYWORDS: phf::Map` are generated from `keywords.txt` by the crate's build script.

Hand-written methods classify each keyword by reservation level: `is_always_reserved` (top-level SELECT clause starters and set operators), `is_reserved_in_scalar_expression`, `is_reserved_in_table_alias`, and `is_reserved_in_column_alias`; the `is_sometimes_reserved` method is the union of all four.
`FromStr` uses `KEYWORDS` for case-insensitive lookup and `Display` delegates to `as_str()`.
