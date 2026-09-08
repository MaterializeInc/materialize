---
source: src/expr-parser/src/parser.rs
revision: 94ee2d5448
---

# mz-expr-parser::parser

Implements a `syn`-based recursive-descent parser for the `MirRelationExpr` text format used in datadriven tests.
`try_parse_mir` drives parsing via a `syn` `ParseStream` and a `Ctx` holding the `TestCatalog`; it handles all MIR relation variants (`Constant`, `Get`, `Let`/`LetRec`, `Project`, `Map`, `FlatMap`, `Filter`, `CrossJoin`, `Join`, `Distinct`, `Reduce`, `TopK`, `Negate`, `Threshold`, `Union`, `ArrangeBy`) and performs a post-processing `fix_types` pass on local let bindings.
`try_parse_scalar` and `try_parse_scalars` parse a single `MirScalarExpr` or a comma-separated list of them from a string. `try_parse_column_types` parses a parenthesized, comma-separated column type list such as `(bigint, text?)`.
`try_parse_def` handles `source` definitions.
`// { ... }` annotation comments are re-encoded as `:: { ... }` before lexing to avoid confusing the `syn` tokenizer.
Scalar function application is resolved in two ways. Named functions listed explicitly in the match (e.g. `isnull`, `greatest`, `coalesce`) are handled directly. All other names are dispatched by `parse_apply_variant`, which calls `UnaryFunc::from_variant_name`, `BinaryFunc::from_variant_name`, or `VariadicFunc::from_variant_name` (provided by the `FuncName` trait) based on argument count. Parameterized function variants (e.g. `cast_int32_to_numeric[127]`, `record_get[1]`, `record_create["a","b"]`, `list_create[integer]`) are handled by `parse_apply_parameterized`, dispatched when a `[` follows the function name.
Literal scalars support an optional type annotation using `::` syntax (e.g. `2000.5::numeric`, `"{}"::jsonb`), which coerces the literal to the named type. Row parsing in `Constant` nodes is type-aware: datums are coerced to the declared column type where possible.
The `analyses` submodule parses `// { types: "...", keys: "..." }` annotation comments; `keys` annotations are parsed as a parenthesized list of bracket-enclosed index lists, e.g. `([0], [1, 2])`. Recognized scalar types include `bigint`, `boolean`, `double precision`, `integer`, `jsonb`, `numeric`, `smallint`, and `text`.
Internal submodules: `relation` (relation expression parsing), `scalar` (scalar expression parsing), `row` (type-aware row parsing), `aggregate` (aggregate function parsing), `def` (source definitions), `analyses` (type/key annotations), and `util` (parsing helpers).
