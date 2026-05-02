---
source: src/lsp-server/src/lib.rs
revision: e757b4d11b
---

# mz_lsp_server

Materialize Language Server Protocol (LSP) implementation, providing SQL editing support (parsing, formatting, completion) for IDEs.

## Module structure

- `backend` -- Core LSP backend implementing the `LanguageServer` trait with SQL parsing, formatting, completion, and command execution.

## Key types

- **`BUILD_INFO`** / **`PKG_VERSION`** / **`PKG_NAME`** -- Build metadata and package identity constants used in server initialization responses.

## Key dependencies

- `mz_sql_parser` -- SQL parsing and AST types.
- `mz_sql_pretty` -- SQL formatting/pretty-printing.
- `mz_sql_lexer` -- Lexer and keyword definitions for completion.
- `tower_lsp` -- LSP protocol transport and trait definitions.
- `mz_build_info` -- Build metadata generation.

## Downstream consumers

- The `mz-lsp-server` binary (`main.rs`) -- Wires up stdin/stdout transport and launches the server.
- VS Code / IDE extensions -- Connect to this server over stdio for Materialize SQL support.
