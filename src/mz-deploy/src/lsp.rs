//! LSP server for mz-deploy projects.
//!
//! Provides IDE integration for `.sql` files in mz-deploy projects via the
//! Language Server Protocol (LSP). The server runs over stdio and supports
//! five capabilities:
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
//! ## Completion
//!
//! Two kinds of completions are provided:
//!
//! - **Keywords** — Static list from the lexer's keyword map, computed once at
//!   startup. Excluded when the typed prefix contains dots (qualified context).
//! - **Object names** — Dynamic completions from the project model and external
//!   dependencies, computed per-request. Qualification level adapts to the
//!   dot-qualified prefix the user has already typed (0 dots = minimum, 1 dot =
//!   schema.object, 2+ dots = db.schema.object). Each item carries a
//!   `text_edit` that replaces the entire typed prefix, so the editor
//!   substitutes rather than appends.
//!
//! ## Parse error diagnostics
//!
//! On every `didOpen` and `didChange`, the file is parsed with
//! [`mz_sql_parser::parser::parse_statements()`]. Parse errors are converted to
//! LSP diagnostics with correct line/column positions via a ropey `Rope`.
//!
//! ## Code Lens
//!
//! Adds a clickable "Run Test" link above each `EXECUTE UNIT TEST` statement.
//! When clicked, the editor dispatches a `mz-deploy.runTest` command with the
//! test filter string (e.g., `database.schema.object#test_name`) as argument.
//!
//! ## Custom Endpoints
//!
//! - **`mz-deploy/dag`** — Returns the project's dependency graph as JSON
//!   ([`dag::DagResponse`]) for rendering in the VS Code workspace webview.
//!   The response contains lightweight node metadata and edges; see the
//!   [`dag`] module for the data model.
//!
//! - **`mz-deploy/catalog`** — Returns the project's data catalog as JSON
//!   ([`catalog::CatalogResponse`]) for the VS Code workspace catalog view.
//!   The response contains a database/schema tree for sidebar navigation and
//!   full object metadata (columns, constraints, grants, indexes) for the
//!   detail panel. See the [`catalog`] module for the data model.
//!
//! - **`mz-deploy/keywords`** — Returns the full list of Materialize SQL
//!   keywords as a JSON array of uppercase strings. Used by the VS Code
//!   extension to apply keyword syntax highlighting via editor decorations.
//!
//! ## Architecture
//!
//! ```text
//! Editor ──stdio──▶ tower-lsp Server ──▶ Backend
//!                                          ├─ documents: per-file Rope (updated on change)
//!                                          ├─ project: planned::Project (rebuilt on save)
//!                                          ├─ types_cache: Types (rebuilt on save)
//!                                          ├─ completions: keyword CompletionItems (static)
//!                                          └─ root: project root directory
//! ```
//!
//! The project model rebuilds fully on `did_save` via [`project::plan_sync()`].
//! Parse diagnostics run per-file on every keystroke. Incremental updates may
//! come later; the pipeline is fast enough for typical project sizes.

mod catalog;
mod code_lens;
mod completion;
mod dag;
pub mod diagnostics;
pub mod goto_definition;
pub mod hover;
mod run;
mod server;

pub use run::run;
