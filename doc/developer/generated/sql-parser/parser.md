---
source: src/sql-parser/src/parser.rs
revision: a86b11d757
---

# mz-sql-parser::parser

Implements Materialize's recursive-descent SQL parser.
`parse_statements` is the main entry point, tokenizing input via `mz-sql-lexer` and parsing a sequence of semicolon-separated statements into `Vec<StatementParseResult>`.
Also exposes `parse_expr` for parsing a single expression, `parse_datatype` for type expressions, `parse_item_name` for parsing a qualified item name (e.g. `"db"."schema"."table"`), and `ParserError` with position information for error reporting.
The parser enforces a recursion limit (`RECURSION_LIMIT = 128`) to guard against stack overflow on deeply nested queries. Iterative expression chains (`a + b + c`, field-access chains) are bounded separately by `EXPR_CHAIN_LIMIT = 1024` to allow wide but valid predicates while preventing stack overflows in later recursive passes (display, drop, visit). `maybe_parse` speculative failures are capped at `SPECULATIVE_FAILURES_PER_TOKEN × token_count` to prevent exponential backtracking on pathological input.
`parse_table_factor` guards the nested table-factor recursion via `checked_recur_mut`; the actual work is delegated to `parse_table_factor_inner`.
The right-hand side of `IS [NOT] DISTINCT FROM` is parsed at the precedence of the surrounding `IS` operator (via `parse_subexpr(precedence)`), not at `Precedence::Zero`. This ensures that `AND`/`OR` following the RHS are left for the enclosing expression rather than absorbed into it, matching PostgreSQL precedence.
`parse_cast_expr` wraps the inner expression in `Expr::Nested` only for expressions that are unsafe to place directly left of a `::` cast (i.e. those with an exposed operator spine that would re-associate on reparse); self-delimiting expressions (identifiers, function calls, nested, values, etc.) are left unwrapped.
`parse_raw_ident_str` rejects empty identifiers so that resolved names like `[""]` (which display as `[]` and fail to reparse) are caught at parse time.
A parenthesized `(SHOW …)` query at statement level is unwrapped to a bare `Statement::Show` when it carries no CTEs, ORDER BY, LIMIT, or OFFSET, keeping the AST independent of redundant outer parens.
Iceberg sink mode parsing accepts `UPSERT` or `APPEND` as valid values.
`EXECUTE UNIT TEST <name> FOR <target> [AT TIME <expr>] [MOCK <view_def>, ...] EXPECTED <result_def>` is parsed by `parse_execute_unit_test`; individual mock clauses are parsed by `parse_mock_view_def`. Both methods are called from `parse_execute` after the leading `EXECUTE UNIT TEST` tokens are consumed.
The private method `parse_list_value<T, F>` optionally consumes `=`, then parses a comma-separated list enclosed in parentheses or brackets using a provided closure, returning `Vec<T>`.
`CREATE CONNECTION ... TO AWS` dispatches on the next keyword: `PRIVATELINK` yields `CreateConnectionType::AwsPrivatelink`, `GLUE` (followed by `SCHEMA REGISTRY`) yields `CreateConnectionType::GlueSchemaRegistry`, and no keyword yields `CreateConnectionType::Aws`. `CREATE CONNECTION ... TO GCP` yields `CreateConnectionType::Gcp`.
In connection option parsing, `GCP CONNECTION` is parsed as `ConnectionOptionName::GcpConnection` (with `parse_object_option_value`), and `SERVICE ACCOUNT KEY` is parsed as `ConnectionOptionName::ServiceAccountKey`.
Iceberg sink parsing reads the catalog connection name, then optionally parses `USING AWS CONNECTION <name>` — if the keywords are absent, `aws_connection` is `None`.
`parse_rows_from` uses `parse_windowless_function` (a private method) for each function inside `ROWS FROM (...)`. `parse_windowless_function` parses a function name and argument list without consuming `DISTINCT`, `FILTER`, or `OVER`, ensuring that table functions in `ROWS FROM` never carry those clauses.
