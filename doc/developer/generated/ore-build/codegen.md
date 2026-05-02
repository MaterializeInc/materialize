---
source: src/ore-build/src/codegen.rs
revision: ffaf116293
---

# ore-build::codegen

Provides `CodegenBuf`, a string buffer with automatic indentation management for generating Rust source code.
`CodegenBuf` tracks an indentation level and exposes methods to write lines, open/close braces, and transition between blocks, allowing build scripts to emit well-formatted code without manual whitespace arithmetic.
It is the sole consumer-facing type in this module and is re-exported through the crate root.
