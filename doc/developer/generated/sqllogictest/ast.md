---
source: src/sqllogictest/src/ast.rs
revision: 5e6d96e124
---

# sqllogictest::ast

Defines the abstract syntax tree for sqllogictest files: `Record` (a single directive such as `statement`, `query`, `simple`, `halt`, or `copy`), `Output` (expected results as literal values or an MD5 hash), `Sort` (no-sort, rowsort, or valuesort), `Type` (column type hint), and `Mode` (Standard vs Cockroach output format).
`QueryOutput` bundles sort order, column types, mode, and expected output for a `query` directive.
These types are produced by the parser and consumed by the runner.
