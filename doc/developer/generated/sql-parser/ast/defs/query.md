---
source: src/sql-parser/src/ast/defs/query.rs
revision: 72277f8ac9
---

# mz-sql-parser::ast::defs::query

Defines the AST types for SELECT queries and related constructs: `Query` (the top-level query with CTEs, body, ORDER BY, LIMIT, OFFSET), `SetExpr` (SELECT / set operations), `Select` (the body of a SELECT), `SelectItem`, `TableWithJoins`, `TableFactor`, `Join`, `JoinOperator`, and CTE types.

`SetExpr<T>` exposes `starts_with_show() -> bool`, which returns true when the leftmost leaf of the body is a `SetExpr::Show` node (either directly or as the left operand of a set operation). This is used by `SelectStatement`'s `AstDisplay` impl to decide when to wrap the query in parentheses so it reparses as a query rather than a bare `SHOW` statement.

`TableFactor`'s `AstDisplay` impl calls `function.fmt_table_call(f)` (rather than `f.write_node(function)`) for `Function` and `RowsFrom` variants, forcing the plain comma form and quoting the function name to prevent `extract`/`position` special-form grammar from firing in table-function position.
