---
source: src/sql-parser/src/ast/defs/expr.rs
revision: ddc1ff8d2d
---

# mz-sql-parser::ast::defs::expr

Defines `Expr<T>`, the main SQL expression enum covering all expression forms: identifiers, literals, binary and unary operators, function calls, subqueries, casts, case expressions, window functions, row constructors, and more.
The parser makes no type distinctions between expression types; semantic type-checking is left to callers.
Also defines closely related types such as `Function`, `WindowSpec`, `OrderByExpr`, `HomogenizingFunction`, and various function-argument and operator types.
