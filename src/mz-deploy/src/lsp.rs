//! LSP server for mz-deploy projects.
//!
//! Provides IDE integration for `.sql` files in mz-deploy projects via the
//! Language Server Protocol (LSP). The server runs over stdio and supports
//! eight capabilities:
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
//! by building the reverse dependency graph from `planned::Project`.
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
//! ## Document Symbols
//!
//! Returns the structural outline of a `.sql` file: the main CREATE statement
//! as the root symbol, with supporting statements (indexes, constraints, grants,
//! comments, unit tests) as children. Powers the editor's "Outline" view and
//! breadcrumb navigation.
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
//! - **"Execute" / "Run"** above each statement in `worksheets/` files.
//!   Queries get "Execute"; DML/DDL get "Run". Both include the cluster
//!   name when available (e.g., "▶ Execute on quickstart").
//!
//! ## Custom Endpoints
//!
//! - **`mz-deploy/dag`** — Returns the project's dependency graph as JSON
//!   (`dag::DagResponse`) for rendering in the VS Code workspace webview.
//!   The response contains lightweight node metadata and edges; see the
//!   `dag` module for the data model.
//!
//! - **`mz-deploy/catalog`** — Returns the project's data catalog as JSON
//!   (`catalog::CatalogResponse`) for the VS Code workspace catalog view.
//!   The response contains a database/schema tree for sidebar navigation and
//!   full object metadata (columns, constraints, grants, indexes) for the
//!   detail panel. See the `catalog` module for the data model.
//!
//! - **`mz-deploy/keywords`** — Returns the full list of Materialize SQL
//!   keywords as a JSON array of uppercase strings. Used by the VS Code
//!   extension to apply keyword syntax highlighting via editor decorations.
//!
//! - **`mz-deploy/connection-info`** — Returns the resolved profile
//!   connection details (host, port, user, profile name) without making
//!   a connection. Used by the worksheet panel to show connection status.
//!
//! - **`mz-deploy/execute-query`** — Executes a SQL statement against the
//!   configured Materialize instance. For SELECTs, injects LIMIT and returns
//!   columnar results. For EXPLAIN, returns raw text. For DML/DDL, returns
//!   an affected row count. See the `worksheet` module.
//!
//! - **`mz-deploy/cancel-query`** — Cancels the in-flight worksheet
//!   query or active SUBSCRIBE using the PostgreSQL cancel protocol.
//!
//! - **`mz-deploy/subscribe`** — Starts a SUBSCRIBE query on a dedicated
//!   connection. Returns immediately with a `subscribe_id`. Results stream
//!   to the client via `mz-deploy/subscribeBatch` notifications (one per
//!   timestamp group). Ends with a `mz-deploy/subscribeComplete` notification.
//!   See the `worksheet` module for the full data flow.
//!
//! - **`mz-deploy/worksheet-context`** — Returns available databases (with
//!   their schemas), clusters, and current session values for the worksheet
//!   dropdowns.
//!
//! - **`mz-deploy/set-session`** — Applies `SET database/search_path/cluster`
//!   on the worksheet connection. Returns refreshed context so schema
//!   dropdowns update after a database change.
//!
//! - **`mz-deploy/set-profile`** — Switches the active connection profile.
//!   Drops the current worksheet connection, cancels any active SUBSCRIBE,
//!   and stores the new profile name for the next lazy connect.
//!
//! ## Architecture
//!
//! ```text
//! Editor ──stdio──▶ tower-lsp Server ──▶ Backend
//!                                          ├─ documents: per-file Rope (updated on change)
//!                                          ├─ project: planned::Project (rebuilt on save)
//!                                          ├─ types_cache: Types (rebuilt on save)
//!                                          ├─ worksheet_connection: lazy DB connection
//!                                          ├─ subscribe_task: background SUBSCRIBE loop
//!                                          ├─ profile_name: active connection profile
//!                                          └─ root: project root directory
//! ```
//!
//! The project model rebuilds fully on `did_save` via `project::plan_sync()`.
//! Parse diagnostics run per-file on every keystroke. Incremental updates may
//! come later; the pipeline is fast enough for typical project sizes.

mod catalog;
mod code_lens;
mod completion;
mod dag;
pub mod diagnostics;
mod document_symbol;
pub mod functions;
pub mod goto_definition;
mod helpers;
pub mod hover;
mod references;
mod run;
mod server;
mod subscribe;
mod symbol_kind;
mod worksheet;
mod workspace_symbol;

pub use run::run;
