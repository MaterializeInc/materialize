---
source: src/lsp-server/src/backend.rs
revision: a3b3b15bd9
---

# mz_lsp_server::backend

Core LSP backend implementing the `LanguageServer` trait for Materialize SQL files.

## Key types

- **`Backend`** -- Main server struct holding the `tower_lsp::Client`, per-file `ParseResult` and `Rope` content caches, formatting width, schema state, and precomputed `Completions`. Implements `LanguageServer` with support for:
  - **Parsing** -- On `did_open`/`did_change`, parses SQL via `mz_sql_parser` and publishes diagnostics. Detects Jinja (dbt) template syntax to suppress false-positive parse errors.
  - **Formatting** -- `textDocument/formatting` using `mz_sql_pretty` with a configurable width (default 100 columns).
  - **Completion** -- Context-aware suggestions after `SELECT` (column names) and `FROM` (object names) tokens, driven by a `Schema` provided at initialization or via `optionsUpdate`.
  - **Commands** -- `parse` splits SQL text into individual typed statements; `optionsUpdate` refreshes formatting width and schema at runtime.
- **`ParseResult`** -- Cached ASTs and rope for an open file.
- **`Schema`** / **`SchemaObject`** / **`SchemaObjectColumn`** / **`ObjectType`** -- Client-provided schema model describing databases, objects (tables, views, sources, materialized views, sinks), and their columns.
- **`ExecuteCommandParseStatement`** / **`ExecuteCommandParseResponse`** -- Wire types for the `parse` command response.
- **`InitializeOptions`** -- Deserialized client initialization options (formatting width, schema).
- **`Completions`** -- Precomputed completion item lists for `SELECT` and `FROM` contexts.

## Helper functions

- `position_to_offset` / `offset_to_position` -- Convert between LSP `(line, column)` positions and rope character offsets.
- `build_error` -- Constructs a `tower_lsp::jsonrpc::Error` with `InternalError` code.
