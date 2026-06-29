---
source: src/sql-parser/src/ast/defs/value.rs
revision: 72277f8ac9
---

# mz-sql-parser::ast::defs::value

Defines `Value`, the enum of primitive SQL literal types: numbers, strings, hex strings, booleans, intervals, and null.
Also defines `IntervalValue` (a string value with optional precision qualifiers) and `ValueError` for parse failures.
`Value::HexString`'s `AstDisplay` impl escapes single-quote characters in the hex string body (via `display::escape_single_quote_string`) so that a hex string value containing `'` round-trips through the `X'...'` literal form without prematurely closing the string.
