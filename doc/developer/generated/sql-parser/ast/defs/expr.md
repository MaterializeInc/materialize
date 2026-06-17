---
source: src/sql-parser/src/ast/defs/expr.rs
revision: 72277f8ac9
---

# mz-sql-parser::ast::defs::expr

Defines `Expr<T>`, the main SQL expression enum covering all expression forms: identifiers, literals, binary and unary operators, function calls, subqueries, casts, case expressions, window functions, row constructors, and more.
The parser makes no type distinctions between expression types; semantic type-checking is left to callers.
`Expr::Identifier(Vec<Ident>)` holds a qualified identifier; the parser always constructs it with a non-empty `Vec`.
Also defines closely related types such as `Function`, `WindowSpec`, `OrderByExpr`, `HomogenizingFunction`, and various function-argument and operator types.

The `AstDisplay` implementation for `Expr` uses several private helpers to guarantee print→reparse round-trips:
- `write_dot_receiver` parenthesizes the receiver of `.` (field access, wildcard access) for expressions that do not print in a self-terminating form (e.g. numeric literals, casts, binary ops), preventing the dot from re-associating on reparse.
- `write_quantified_left` parenthesizes the LHS of `ANY`/`ALL` quantified comparisons when it is a low-precedence expression (`Like`, `Between`, `IsExpr`, `In*`, `And`, `Or`, `Not`, nested quantified comparisons) that the quantifier's infix operator would otherwise absorb.
- `write_subscript_receiver` parenthesizes an `Expr::Identifier` subscript receiver when the last identifier component is a context-sensitive keyword (e.g. `map`) that would otherwise re-lex as a map-literal opener.
- `prints_self_delimiting` reports whether an expression is atomic or wrapped in its own brackets/parens (values, identifiers, function calls, `CASE … END`, `ARRAY[…]`, etc.), so postfix operators (`::`, `COLLATE`, `position`'s `IN` delimiter) can safely be appended without re-association. Postfix chains (`Cast`, `Collate`, `Subscript`) are self-delimiting only when their own inner operand is.
- `prefix_operand_needs_parens` determines whether the operand of a prefix operator (`-`/`+`/`~`) must be parenthesized: it peels tight postfix forms and parenthesizes when the chain bottoms out at a numeric literal (to prevent sign-folding) or at a non-self-delimiting operand (to prevent re-association).
- `Collate` parenthesizes its operand unless it is self-delimiting.
- `AnySubquery`, `AnyExpr`, `AllSubquery`, `AllExpr` delegate their LHS to `write_quantified_left`.
- `FieldAccess` and `WildcardAccess` delegate to `write_dot_receiver`.
- `Subscript` delegates to `write_subscript_receiver`.

`Function` exposes `fmt_table_call` for use in table-function positions (`FROM f(...)`, `ROWS FROM (...)`), which forces the plain comma form and quotes the name to prevent the `extract`/`position` special-form grammar from firing.
`Function`'s `AstDisplay` impl quotes function names that clash with keywords dispatched as special grammar forms (`array`, `coalesce`, `exists`, `extract`, `greatest`, `least`, `list`, `map`, `normalize`, `nullif`, `position`, `row`, `substring`, `trim`) so that reparsing goes through the regular function-call path.
The `extract(field FROM source)` special form is only emitted when `field` (the first argument) is a `Value::String`; otherwise the plain quoted-call form is used. The `position(needle IN haystack)` special form is only emitted when the needle is self-delimiting (per `prints_self_delimiting`).
`FunctionArgs` exposes a `first()` method returning the first positional argument, if any.
