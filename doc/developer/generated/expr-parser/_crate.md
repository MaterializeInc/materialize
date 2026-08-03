---
source: src/expr-parser/src/lib.rs
revision: 94ee2d5448
---

# mz-expr-parser

Provides utilities for constructing and verifying `mz-expr` objects in tests, specifically a text-format parser for `MirRelationExpr` and an in-memory `TestCatalog`.
It is used exclusively in datadriven tests to write and round-trip MIR expressions in a human-readable notation without requiring a full SQL planning stack.

## Module structure

* `catalog` — `TestCatalog` (in-memory relation registry implementing `ExprHumanizer`).
* `command` — `handle_define` and `handle_roundtrip` datadriven command handlers.
* `parser` — `syn`-based `MirRelationExpr` text parser and `Def` type.
* `printer` — `print_scalar` for rendering a `MirScalarExpr` back to text.

## Key exports

`try_parse_mir`, `try_parse_scalar`, `try_parse_scalars`, `try_parse_column_types`, `try_parse_def`, `Def`, `TestCatalog`, `handle_define`, `handle_roundtrip`, `print_scalar`.

## Key dependencies

* `mz-expr` — provides `MirRelationExpr` and related IR types.
* `mz-repr` — `GlobalId`, `SqlRelationType`, `ExprHumanizer`.
* `syn` / `proc-macro2` — tokenization and parsing of the MIR text syntax.
