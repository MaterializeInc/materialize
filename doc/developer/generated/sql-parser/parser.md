---
source: src/sql-parser/src/parser.rs
revision: b1959edbc1
---

# mz-sql-parser::parser

Implements Materialize's recursive-descent SQL parser.
`parse_statements` is the main entry point, tokenizing input via `mz-sql-lexer` and parsing a sequence of semicolon-separated statements into `Vec<StatementParseResult>`.
Also exposes `parse_expr` for parsing a single expression, `parse_datatype` for type expressions, `parse_item_name` for parsing a qualified item name (e.g. `"db"."schema"."table"`), and `ParserError` with position information for error reporting.
The parser enforces a recursion limit to guard against stack overflow on deeply nested queries.
Iceberg sink mode parsing accepts `UPSERT` or `APPEND` as valid values.
`EXECUTE UNIT TEST <name> FOR <target> [AT TIME <expr>] [MOCK <view_def>, ...] EXPECTED <result_def>` is parsed by `parse_execute_unit_test`; individual mock clauses are parsed by `parse_mock_view_def`. Both methods are called from `parse_execute` after the leading `EXECUTE UNIT TEST` tokens are consumed.
The private method `parse_list_value<T, F>` optionally consumes `=`, then parses a comma-separated list enclosed in parentheses or brackets using a provided closure, returning `Vec<T>`.
`CREATE CONNECTION ... TO AWS` dispatches on the next keyword: `PRIVATELINK` yields `CreateConnectionType::AwsPrivatelink`, `GLUE` (followed by `SCHEMA REGISTRY`) yields `CreateConnectionType::GlueSchemaRegistry`, and no keyword yields `CreateConnectionType::Aws`.
