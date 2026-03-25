---
source: src/lsp-server/src/main.rs
revision: 30d929249e
---

# mz_lsp_server (binary entry point)

Binary entry point for the Materialize LSP server. Launches a `tower_lsp::Server` over stdin/stdout using the Tokio async runtime.

## Behavior

Constructs a `Backend` instance with empty initial state (no parse results, default formatting width of 100, no schema, no completions) and wires it into an `LspService`. The server then listens on stdin and writes responses to stdout, following the LSP stdio transport convention.
