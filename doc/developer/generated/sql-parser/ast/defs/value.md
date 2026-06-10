---
source: src/sql-parser/src/ast/defs/value.rs
revision: 4267863081
---

# mz-sql-parser::ast::defs::value

Defines `Value`, the enum of primitive SQL literal types: numbers, strings, hex strings, booleans, intervals, and null.
Also defines `IntervalValue` (a string value with optional precision qualifiers) and `ValueError` for parse failures.
