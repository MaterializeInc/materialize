---
source: src/expr-parser/src/command.rs
revision: 65fb19dabf
---

# mz-expr-parser::command

Provides two datadriven test command handlers: `handle_define` and `handle_roundtrip`.
`handle_define` parses a source definition and registers it in the `TestCatalog`, returning the assigned `GlobalId`.
`handle_roundtrip` parses a `MirRelationExpr`, pretty-prints it with `debug_explain`, and reports whether the output matches the stripped input, enabling parse/print round-trip tests.
