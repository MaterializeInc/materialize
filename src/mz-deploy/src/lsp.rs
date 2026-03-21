//! LSP server for mz-deploy projects.
//!
//! Provides IDE integration for `.sql` files in mz-deploy projects via the
//! Language Server Protocol (LSP). The server runs over stdio and supports
//! three capabilities:
//!
//! ## Go-to-definition
//!
//! Navigates from a SQL identifier (e.g., `foo` in `SELECT * FROM foo`) to the
//! `.sql` file that defines it. Resolution uses two phases:
//!
//! 1. **Lexer-based identifier extraction** — Uses [`mz_sql_lexer::lexer::lex()`]
//!    to tokenize the file and find the identifier (possibly dot-qualified) at the
//!    cursor position.
//! 2. **Project model lookup** — Resolves the identifier against the planned
//!    project model using [`ObjectId::from_item_name()`] conventions (1-part uses
//!    default db+schema from file path, 2-part uses default db, 3-part as-is).
//!
//! ## Hover
//!
//! Shows the output schema (column names, types, nullability) for a referenced
//! object when hovering over its identifier. Column data comes from
//! `types.cache.bin` (internal views) merged with `types.lock` (external
//! dependencies). If no column data is available, the hover shows the object
//! kind and source file path.
//!
//! ## Parse error diagnostics
//!
//! On every `didOpen` and `didChange`, the file is parsed with
//! [`mz_sql_parser::parser::parse_statements()`]. Parse errors are converted to
//! LSP diagnostics with correct line/column positions via a ropey `Rope`.
//!
//! ## Architecture
//!
//! ```text
//! Editor ──stdio──▶ tower-lsp Server ──▶ Backend
//!                                          ├─ documents: per-file Rope (updated on change)
//!                                          ├─ project: planned::Project (rebuilt on save)
//!                                          ├─ types_cache: Types (rebuilt on save)
//!                                          └─ root: project root directory
//! ```
//!
//! The project model rebuilds fully on `did_save` via [`project::plan_sync()`].
//! Parse diagnostics run per-file on every keystroke. Incremental updates may
//! come later; the pipeline is fast enough for typical project sizes.

pub mod diagnostics;
pub mod goto_definition;
pub mod hover;
mod run;
mod server;

pub use run::run;
