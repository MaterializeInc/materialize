---
source: src/testdrive/src/action/skip_if.rs
revision: e757b4d11b
---

# testdrive::action::skip_if

Implements the `skip-if` builtin command.
Executes a single-column boolean SQL query against Materialize; if the result is `true`, returns `ControlFlow::SkipBegin` so the driver skips all subsequent commands until a matching `skip-end` or end-of-file.
