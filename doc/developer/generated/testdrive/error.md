---
source: src/testdrive/src/error.rs
revision: 887cfdaaf0
---

# testdrive::error

Defines the error types used throughout the testdrive library.
`PosError` carries a byte-offset position alongside an `anyhow::Error` as errors propagate up the call stack;
`Error` is the public-facing type that converts a `PosError` into a human-readable message with filename, line/column, and a source-code snippet.
`ErrorLocation` encapsulates the snippet rendering logic, redacting lines that look like they may contain credentials.
