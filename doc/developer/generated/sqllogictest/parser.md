---
source: src/sqllogictest/src/parser.rs
revision: 80d40a68c8
---

# sqllogictest::parser

Implements `Parser<'a>`, a line-oriented parser that converts raw sqllogictest file contents into an iterator of `Record` values defined in `ast`.
Tracks current file name and line number for error reporting via `Location`.
Supports both Standard and Cockroach dialect modes, switching when it encounters a `mode cockroach` directive.
Parses `replace <regex>  <replacement>` directives (regex and replacement separated by two-or-more spaces) into `Record::Replace`; the regex is validated at parse time so errors are reported with file/line context.
