// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! LSP server for mz-deploy projects.
//!
//! Provides IDE integration for `.sql` files in mz-deploy projects via the
//! Language Server Protocol (LSP). The server runs over stdio and supports
//! nine capabilities:
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
//!    project model using `ObjectId::from_item_name()` conventions (1-part uses
//!    default db+schema from file path, 2-part uses default db, 3-part as-is).
//!
//! ## Find References
//!
//! Finds all project objects that depend on a given identifier. This is the
//! inverse of go-to-definition: it answers "who uses this table/view/source?"
//! by building the reverse dependency graph from `graph::Project`.
//!
//! ## Hover
//!
//! Shows the output schema (column names, types, nullability) for a referenced
//! object when hovering over its identifier. Column data comes from the
//! build artifact database (internal views) merged with `types.lock` (external
//! dependencies). If no column data is available, the hover shows the object
//! kind and source file path.
//!
//! ## Completion
//!
//! Two kinds of completions are provided:
//!
//! - **Keywords** — Static list from the lexer's keyword map, computed once at
//!   startup. Excluded when the typed prefix contains dots (qualified context).
//! - **Object names** — Dynamic completions from the project model and external
//!   dependencies, computed per-request. Qualification level adapts to the
//!   dot-qualified prefix the user has already typed (0 dots = minimum, 1 dot =
//!   schema.object, 2+ dots = db.schema.object).
//!
//! ## Semantic Tokens
//!
//! Drives editor syntax coloring via `textDocument/semanticTokens/full`. The
//! document is lexed with `mz_sql_lexer` and each token is mapped to a standard
//! LSP token type (KEYWORD, STRING, NUMBER, OPERATOR, VARIABLE, PARAMETER,
//! COMMENT). This covers Materialize-specific keywords that VS Code's built-in
//! SQL grammar doesn't know about, so the editor's theme colors apply uniformly
//! across the full Materialize keyword set.
//!
//! ## Document Symbols
//!
//! Returns the structural outline of a `.sql` file: the main CREATE statement
//! as the root symbol, with supporting statements (indexes, grants, comments,
//! unit tests) as children. Powers the editor's "Outline" view and breadcrumb
//! navigation.
//!
//! ## Workspace Symbols
//!
//! Searches all objects in the project by name, enabling fuzzy-find across the
//! entire workspace. Results include the object kind, file location, and a
//! `database.schema` container label for grouping.
//!
//! ## Parse error diagnostics
//!
//! On every `didOpen` and `didChange`, the file is parsed with
//! [`mz_sql_parser::parser::parse_statements()`]. Parse errors are converted to
//! LSP diagnostics with correct line/column positions via a ropey `Rope`.
//!
//! ## Code Lens
//!
//! - **"Run Test"** above each `EXECUTE UNIT TEST` statement.
//! - **"Explain"** above `CREATE MATERIALIZED VIEW` and named `CREATE INDEX`.
//!
//! ## Architecture
//!
//! ```text
//! Editor ──stdio──▶ tower-lsp Server ──▶ Backend
//!                                          ├─ documents: per-file Rope (updated on change)
//!                                          ├─ project: graph::Project (rebuilt on save)
//!                                          ├─ project_cache: ProjectCache (rebuilt on save)
//!                                          ├─ profile_name: active connection profile
//!                                          └─ root: project root directory
//! ```
//!
//! The project model rebuilds fully on `did_save` via `project::plan_sync()`.
//! Parse diagnostics run per-file on every keystroke. Incremental updates may
//! come later; the pipeline is fast enough for typical project sizes.

mod code_action;
mod code_lens;
mod completion;
pub mod diagnostics;
mod document_symbol;
pub mod functions;
pub mod goto_definition;
pub mod hover;
mod references;
mod run;
mod semantic_tokens;
mod server;
mod symbol_kind;
mod workspace_symbol;

pub use run::run;
