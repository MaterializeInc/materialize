---
source: src/sqllogictest/src/parser.rs
revision: 5e6d96e124
---

# sqllogictest::parser

Implements `Parser<'a>`, a line-oriented parser that converts raw sqllogictest file contents into an iterator of `Record` values defined in `ast`.
Tracks current file name and line number for error reporting via `Location`.
Supports both Standard and Cockroach dialect modes, switching when it encounters a `mode cockroach` directive.
