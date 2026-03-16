---
source: src/lsp-server/src/
revision: f38003ddc8
---

# mz-lsp-server

`mz-lsp-server` is Materialize's Language Server Protocol (LSP) server for SQL files.
It parses SQL using `mz_sql_parser`, publishes diagnostics over the LSP protocol, and supports formatting, completions, and a custom `parse` execute-command.

## Modules

### `lib` (root)

Exposes build metadata (`BUILD_INFO`, `PKG_VERSION`, `PKG_NAME`) and re-exports the `backend` module.

### `backend`

Implements the `LanguageServer` trait via the `Backend` struct, which holds per-file parse results and document content in `tokio::sync::Mutex` maps.
Key capabilities:

* **Document sync** (`did_open`, `did_change`): parses the full document text on every change using `mz_sql_parser`; publishes parse-error diagnostics at the error position, or clears them on success.
  Jinja template syntax (`{{ }}`, `{% %}`, `{# #}`) is detected via regex and suppressed rather than reported as errors.
* **Formatting** (`textDocument/formatting`): pretty-prints all AST statements using `mz_sql_pretty` at a configurable line width (default 100).
* **Completions** (`textDocument/completion`): context-aware suggestions driven by a client-supplied schema (tables, views, sources, sinks and their columns); completion lists are pre-built at initialization and keyed on the last `SELECT` or `FROM` keyword before the cursor.
* **Execute commands**: `parse` splits raw SQL text into individual statements and returns their AST kind labels; `optionsUpdate` allows the client to update formatting width and schema after initialization.

Supporting types include `Schema` / `SchemaObject` / `SchemaObjectColumn` (schema introspection from the client), `ParseResult` (rope + AST pair per file), and `InitializeOptions` (JSON initialization parameters).

### `main`

Entry point: constructs a `Backend` instance and serves it over stdio using `tower_lsp::Server`.

## Key relationships

`backend::Backend` depends on `mz_sql_parser` for parsing, `mz_sql_lexer` for token-level completion context, and `mz_sql_pretty` for formatting.
The `lib` root provides version metadata that the server advertises to the client during the `initialize` handshake.
