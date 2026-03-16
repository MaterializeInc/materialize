---
source: src/sql-lexer/src/lexer.rs
revision: 04d7dfaa0c
---

# mz-sql-lexer::lexer

Implements the SQL lexer following the PostgreSQL lexical-structure rules, exposing the `lex` function that converts a SQL query string into a `Vec<PosToken>`.

`Token` enumerates all token kinds: keywords, identifiers (`IdentString`, limited to 255 bytes), string and hex-string literals, numbers, `$n` parameters, operators, and single-character punctuation tokens.
`PosToken` pairs a `Token` with its byte offset in the original input; `LexerError` carries a message and position.
Internal helpers cover every lexical category: quoted and dollar-quoted strings, extended E-string escapes, identifiers, numbers (with optional decimal and exponent), PROXY-v2-style operators, and PROXY v2-style multi-character operator trimming.
The `=>` sequence is special-cased as `Token::Arrow` and `!=` is normalized to `<>`.
