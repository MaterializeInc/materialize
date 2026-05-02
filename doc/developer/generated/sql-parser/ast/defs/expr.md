---
source: src/sql-parser/src/ast/defs/expr.rs
revision: 721951ce66
---

# mz-sql-parser::ast::defs::expr

Defines `Expr<T>`, the main SQL expression enum covering all expression forms: identifiers, literals, binary and unary operators, function calls, subqueries, casts, case expressions, window functions, row constructors, and more.
The parser makes no type distinctions between expression types; semantic type-checking is left to callers.
`Expr::Identifier(Vec<Ident>)` holds a qualified identifier; the parser always constructs it with a non-empty `Vec`.
Also defines closely related types such as `Function`, `WindowSpec`, `OrderByExpr`, `HomogenizingFunction`, and various function-argument and operator types.
