---
source: src/expr-parser/src/lib.rs
revision: e757b4d11b
---

# mz-expr-parser

Provides utilities for constructing and verifying `mz-expr` objects in tests, specifically a text-format parser for `MirRelationExpr` and an in-memory `TestCatalog`.
It is used exclusively in datadriven tests to write and round-trip MIR expressions in a human-readable notation without requiring a full SQL planning stack.

## Module structure

* `catalog` — `TestCatalog` (in-memory relation registry implementing `ExprHumanizer`).
* `command` — `handle_define` and `handle_roundtrip` datadriven command handlers.
* `parser` — `syn`-based `MirRelationExpr` text parser and `Def` type.

## Key dependencies

* `mz-expr` — provides `MirRelationExpr` and related IR types.
* `mz-repr` — `GlobalId`, `SqlRelationType`, `ExprHumanizer`.
* `syn` / `proc-macro2` — tokenization and parsing of the MIR text syntax.
