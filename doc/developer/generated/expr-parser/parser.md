---
source: src/expr-parser/src/parser.rs
revision: 52af3ba2a1
---

# mz-expr-parser::parser

Implements a `syn`-based recursive-descent parser for the `MirRelationExpr` text format used in datadriven tests.
`try_parse_mir` drives parsing via a `syn` `ParseStream` and a `Ctx` holding the `TestCatalog`; `try_parse_def` handles `source` definitions.
`// { ... }` annotation comments are re-encoded as `:: { ... }` before lexing to avoid confusing the `syn` tokenizer.
