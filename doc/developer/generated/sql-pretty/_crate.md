---
source: src/sql-pretty/src/lib.rs
revision: 8f05f1b28d
---

# mz-sql-pretty

Pretty-prints Materialize's SQL dialect by parsing statements with `mz-sql-parser` and rendering them via the `pretty` crate at a configurable line width (default 100).
The main entry points are `pretty_str` / `pretty_strs` (which return results) and `to_pretty` (which operates on an already-parsed `Statement`); `Simple` and `Stable` format modes are forwarded to the underlying AST display logic.
The `to_doc` dispatcher handles `CreateRole` and `AlterRole` via dedicated `doc` methods in addition to the other supported statement types.

## Module structure

* `doc` — per-statement and per-clause `RcDoc` builders on the `Pretty` struct.
* `util` — shared `RcDoc` combinators (`nest`, `bracket`, `comma_separate`, …).

## Key dependencies

* `mz-sql-parser` — provides the AST types and `parse_statements`.
* `pretty` — Wadler/Lindig document algebra used for layout.
