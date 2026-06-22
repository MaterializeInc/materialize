---
source: src/sqllogictest/src/ast.rs
revision: 80d40a68c8
---

# sqllogictest::ast

Defines the abstract syntax tree for sqllogictest files: `Record` (a single directive such as `statement`, `query`, `simple`, `halt`, `copy`, or `replace`), `Output` (expected results as literal values or an MD5 hash), `Sort` (no-sort, rowsort, or valuesort), `Type` (column type hint), and `Mode` (Standard vs Cockroach output format).
`QueryOutput` bundles sort order, column types, mode, and expected output for a `query` directive.
`Record::Replace` carries a `pattern: String` (a validated regex) and a `replacement: String`; it registers a substitution applied to the actual output of every subsequent `query` before comparison or rewriting, used to mask non-deterministic tokens such as a folded `mz_now()` timestamp in an `EXPLAIN` plan.
These types are produced by the parser and consumed by the runner.
