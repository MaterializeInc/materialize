---
source: src/sql-pretty/src/lib.rs
revision: 72277f8ac9
---

# mz-sql-pretty

Pretty-prints Materialize's SQL dialect by parsing statements with `mz-sql-parser` and rendering them via the `pretty` crate at a configurable line width (default 100).
The main entry points are `pretty_str` / `pretty_strs` (which return results) and `to_pretty` (which operates on an already-parsed `Statement`); `Simple` and `Stable` format modes are forwarded to the underlying AST display logic.
`to_pretty` is bounded on `T: AstInfo<NestedStatement = Statement<Raw>>` (satisfied by both `Raw` and `Aug`) so that `DECLARE`/`PREPARE` can recurse into their inner statement's pretty doc rather than redacting it through the `AstDisplay` fallback.
The `to_doc` dispatcher handles `CreateRole`, `AlterRole`, `Declare`, and `Prepare` via dedicated `doc` methods; `Declare` and `Prepare` recurse into the inner statement so secrets they carry (e.g. a role password) are printed losslessly.

## Module structure

* `doc` — per-statement and per-clause `RcDoc` builders on the `Pretty` struct.
* `util` — shared `RcDoc` combinators (`nest`, `bracket`, `comma_separate`, …).

## Key dependencies

* `mz-sql-parser` — provides the AST types and `parse_statements`.
* `pretty` — Wadler/Lindig document algebra used for layout.
