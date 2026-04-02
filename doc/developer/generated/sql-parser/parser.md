---
source: src/sql-parser/src/parser.rs
revision: d9b6a942b5
---

# mz-sql-parser::parser

Implements Materialize's recursive-descent SQL parser.
`parse_statements` is the main entry point, tokenizing input via `mz-sql-lexer` and parsing a sequence of semicolon-separated statements into `Vec<StatementParseResult>`.
Also exposes `parse_expr` for parsing a single expression, `parse_datatype` for type expressions, `parse_item_name` for parsing a qualified item name (e.g. `"db"."schema"."table"`), and `ParserError` with position information for error reporting.
The parser enforces a recursion limit to guard against stack overflow on deeply nested queries.
