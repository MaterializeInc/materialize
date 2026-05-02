---
source: src/testdrive/src/parser.rs
revision: 0a1a898ad2
---

# testdrive::parser

Parses testdrive script text into a sequence of `PosCommand` values.
The four sigil characters define command types: `$` for builtin commands, `>` for SQL queries, `?` for explain queries, and `!` for expected-failure SQL.
`LineReader` is the line-at-a-time iterator that tracks byte positions for error reporting and handles line-continuation folding.
`ArgMap` wraps a `BTreeMap<String, String>` and provides typed accessors (`string`, `parse`, `opt_bool`, `done`) for consuming builtin command arguments.
`VersionConstraint` allows commands to be guarded by a Materialize version range, and `SqlExpectedError` represents the four match modes for `!`-commands.
