---
source: src/expr-parser/src/parser.rs
revision: de1872534e
---

# mz-expr-parser::parser

Implements a `syn`-based recursive-descent parser for the `MirRelationExpr` text format used in datadriven tests.
`try_parse_mir` drives parsing via a `syn` `ParseStream` and a `Ctx` holding the `TestCatalog`; it handles all MIR relation variants (`Constant`, `Get`, `Let`/`LetRec`, `Project`, `Map`, `FlatMap`, `Filter`, `CrossJoin`, `Join`, `Distinct`, `Reduce`, `TopK`, `Negate`, `Threshold`, `Union`, `ArrangeBy`) and performs a post-processing `fix_types` pass on local let bindings.
`try_parse_def` handles `source` definitions.
`// { ... }` annotation comments are re-encoded as `:: { ... }` before lexing to avoid confusing the `syn` tokenizer.
Internal submodules: `relation` (relation expression parsing), `scalar` (scalar expression parsing), `def` (source definitions), `analyses` (type/key annotations), and `util` (parsing helpers).
