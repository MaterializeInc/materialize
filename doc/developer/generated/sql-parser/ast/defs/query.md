---
source: src/sql-parser/src/ast/defs/query.rs
revision: 1b41713262
---

# mz-sql-parser::ast::defs::query

Defines the AST types for SELECT queries and related constructs: `Query` (the top-level query with CTEs, body, ORDER BY, LIMIT, OFFSET), `SetExpr` (SELECT / set operations), `Select` (the body of a SELECT), `SelectItem`, `TableWithJoins`, `TableFactor`, `Join`, `JoinOperator`, and CTE types.
