---
source: src/sql-lexer/src/lib.rs
revision: 30d929249e
---

# mz-sql-lexer

The lexer for Materialize's SQL dialect, providing tokenization as the first stage of SQL parsing.

The crate root re-exports the two modules `keywords` and `lexer` with no additional logic.
`keywords` defines the `Keyword` enum and a compile-time perfect-hash map generated from `keywords.txt` by `build.rs`.
`lexer` implements the full tokenizer, producing `Vec<PosToken>` from a SQL query string.

Key dependencies: `mz-ore` (stack/lex utilities), `phf`/`uncased` (compile-time keyword map), `serde`.
Consumed by `mz-sql-parser` and any component that needs to tokenize SQL before parsing.
