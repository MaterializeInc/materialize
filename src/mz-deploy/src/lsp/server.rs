//! LSP backend and `LanguageServer` trait implementation.
//!
//! [`Backend`] holds per-session state: the document ropes (for offset
//! conversion), the latest successfully built project model (for
//! go-to-definition, hover, completion, and code lens), the types cache (for
//! hover column schemas and column completions), and the project root path.
//!
//! ## State Management
//!
//! - **`documents`** — Updated on every `didOpen` / `didChange`. Each entry is
//!   a [`Rope`] built from the full document text.
//! - **`project`** — Rebuilt on every `didSave` via [`project::plan_sync()`].
//!   On build failure the last good model is kept so go-to-definition degrades
//!   gracefully rather than disappearing entirely.
//! - **`types_cache`** — Rebuilt on every `didSave` from `types.cache.bin`
//!   merged with `types.lock`. On load failure the last good cache is kept.
//! - **`project_diagnostic_uris`** — Tracks which file URIs currently have
//!   project-level validation diagnostics. On each rebuild, diagnostics are
//!   diffed via [`compute_diagnostic_actions`]: old URIs not in the new set
//!   are cleared, new diagnostics are published.
//! - **`root`** — Set during `initialize` from `InitializeParams::root_uri`.
//! - **`settings`** / **`variables`** — Cached from `project.toml` via
//!   [`load_settings()`]. Reloaded at startup and on every save.
//!
//! ## Worksheet Connections
//!
//! The worksheet feature uses **two separate database connections**:
//!
//! - **`worksheet_connection`** — Lazy, long-lived connection for one-shot
//!   queries (`execute-query`), session management (`set-session`), and
//!   catalog introspection (`worksheet-context`). Created on first use,
//!   cached across requests, reconnected if the profile changes.
//!
//! - **SUBSCRIBE connection** — A dedicated short-lived connection opened
//!   for each SUBSCRIBE. Required because SUBSCRIBE holds a transaction
//!   open for its entire lifetime, blocking the connection for other use.
//!   Managed by [`subscribe::SubscribeManager`]; see the [`subscribe`]
//!   module for the full lifecycle.
//!
//! ## Typecheck on Save
//!
//! After every `rebuild_project()`, the server runs [`run_typecheck()`] which
//! performs incremental typechecking via the local Docker container. If AST
//! hashes are unchanged since the last typecheck, Docker is skipped entirely.
//! If Docker is unavailable, typechecking is silently skipped — the catalog
//! retains its last known column data.
//!
//! ## Custom Notifications
//!
//! - **`mz-deploy/projectRebuilt`** — Sent to the client after
//!   `rebuild_project()` and again after `run_typecheck()` if column data
//!   changed. The VS Code extension uses this to refresh the catalog sidebar
//!   and DAG panel with fresh data.
//! - **`mz-deploy/subscribeBatch`** — Pushed for each timestamp group during
//!   a SUBSCRIBE. Contains diff rows or a progress-only marker.
//! - **`mz-deploy/subscribeComplete`** — Pushed when a SUBSCRIBE ends
//!   (cancelled, errored, or connection closed).

use crate::config::{DEFAULT_DOCKER_IMAGE, ProfilesConfig, ProjectSettings};
use crate::lsp::{
    catalog, code_lens, completion, dag, diagnostics, document_symbol, goto_definition, hover,
    references, subscribe, worksheet, workspace_symbol,
};
use crate::project;
use crate::project::error::{ProjectError, ValidationErrors};
use crate::project::planned;
use crate::types::docker_runtime::DockerRuntime;
use crate::types::{self, Types};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use ropey::Rope;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use tokio_postgres::NoTls;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

/// Custom notification sent to the client after a project rebuild completes.
///
/// The extension listens for this to refresh catalog and DAG data, replacing
/// the old timer-based approach that was prone to race conditions.
struct ProjectRebuilt;

impl tower_lsp::lsp_types::notification::Notification for ProjectRebuilt {
    type Params = ();
    const METHOD: &'static str = "mz-deploy/projectRebuilt";
}

/// Actions to take after a project rebuild, expressed as pure data.
///
/// Separates the *decision* of which diagnostics to publish/clear from the
/// *execution* of those actions (which requires async I/O via the LSP client).
/// The [`compute_diagnostic_actions`] function produces this from validation
/// diagnostics and the set of previously tracked diagnostic URIs.
struct DiagnosticActions {
    /// New diagnostics to publish, keyed by file URI.
    diagnostics_to_publish: Vec<(Url, Vec<Diagnostic>)>,
    /// URIs that had diagnostics before but should now be cleared.
    uris_to_clear: Vec<Url>,
    /// The new set of URIs that have diagnostics (replaces `project_diagnostic_uris`).
    new_tracked_uris: Vec<Url>,
}

/// Compute which diagnostics to publish and which URIs to clear.
///
/// `new_diagnostics` is the set of validation diagnostics from the current
/// build (empty on success or non-validation errors). `old_diagnostic_uris`
/// is the set of URIs that had diagnostics before this build.
///
/// URIs in the old set that are not in the new set are scheduled for clearing.
fn compute_diagnostic_actions(
    new_diagnostics: BTreeMap<PathBuf, Vec<Diagnostic>>,
    old_diagnostic_uris: &[Url],
) -> DiagnosticActions {
    let new_uris: Vec<Url> = new_diagnostics
        .keys()
        .filter_map(|path| Url::from_file_path(path).ok())
        .collect();

    let uris_to_clear: Vec<Url> = old_diagnostic_uris
        .iter()
        .filter(|uri| !new_uris.contains(uri))
        .cloned()
        .collect();

    let diagnostics_to_publish: Vec<(Url, Vec<Diagnostic>)> = new_diagnostics
        .into_iter()
        .filter_map(|(path, diags)| Url::from_file_path(path).ok().map(|uri| (uri, diags)))
        .collect();

    DiagnosticActions {
        diagnostics_to_publish,
        uris_to_clear,
        new_tracked_uris: new_uris,
    }
}

/// Load and merge types from `types.lock` and `types.cache.bin`.
///
/// Returns `Some` if any types were found, `None` otherwise. The two sources
/// are merged so hover covers both external dependencies (from the lock file)
/// and internal views (from the cache).
fn load_merged_types(root: &Path) -> Option<Types> {
    let mut merged = types::load_types_lock(root).unwrap_or_default();
    if let Ok(cache) = types::load_types_cache(root) {
        merged.merge(&cache);
    }
    if merged.tables.is_empty() {
        None
    } else {
        Some(merged)
    }
}

/// Cached worksheet database connection.
///
/// Wraps the raw `tokio_postgres::Client` plus the profile name that was
/// used to establish it, so the handler can detect when the active profile
/// has changed and reconnect.
struct WorksheetConnection {
    pg_client: Arc<tokio_postgres::Client>,
    profile_name: String,
    host: String,
    /// Current database (from `SHOW database` or last `SET database`).
    database: Option<String>,
    /// Current schema/search_path (from `SHOW search_path` or last `SET search_path`).
    schema: Option<String>,
    /// Current cluster (from `SHOW cluster` or last `SET cluster`).
    cluster: Option<String>,
}

/// LSP backend holding session state.
pub struct Backend {
    /// Client handle for sending notifications (e.g., diagnostics).
    client: Client,
    /// Per-file text ropes, keyed by document URI.
    documents: Mutex<BTreeMap<Url, Rope>>,
    /// The latest successfully built project model (wrapped in Arc for
    /// cheap sharing across async typecheck operations).
    project: RwLock<Option<Arc<planned::Project>>>,
    /// Types cache for hover column schemas (types.cache.bin merged with types.lock).
    types_cache: RwLock<Option<Types>>,
    /// File URIs that currently have project-level validation diagnostics.
    project_diagnostic_uris: Mutex<Vec<Url>>,
    /// Project root directory.
    root: RwLock<PathBuf>,
    /// Cached project settings loaded from `project.toml`.
    settings: RwLock<Option<ProjectSettings>>,
    /// Cached variables from the active profile config.
    variables: RwLock<BTreeMap<String, String>>,
    /// Name of the active profile (for hover display).
    profile_name: RwLock<String>,
    /// Lazy worksheet database connection, created on first `execute-query`.
    /// Uses `tokio::sync::Mutex` because the lock is held across `.await`.
    worksheet_connection: tokio::sync::Mutex<Option<WorksheetConnection>>,
    /// Cancel token for the in-flight worksheet query.
    worksheet_cancel: tokio::sync::Mutex<Option<tokio_postgres::CancelToken>>,
    /// Manages the active SUBSCRIBE background task lifecycle.
    subscribe: subscribe::SubscribeManager,
    /// Last project build error, if the most recent build failed.
    /// Used by the catalog endpoint to report errors to the sidebar.
    last_build_error: RwLock<Option<String>>,
}

impl Backend {
    /// Create a new backend with the given LSP client handle and project root.
    pub fn new_with_root(client: Client, root: PathBuf) -> Self {
        Self {
            client,
            documents: Mutex::new(BTreeMap::new()),
            project: RwLock::new(None),
            types_cache: RwLock::new(None),
            project_diagnostic_uris: Mutex::new(Vec::new()),
            root: RwLock::new(root),
            settings: RwLock::new(None),
            variables: RwLock::new(BTreeMap::new()),
            profile_name: RwLock::new("default".to_string()),
            worksheet_connection: tokio::sync::Mutex::new(None),
            worksheet_cancel: tokio::sync::Mutex::new(None),
            subscribe: subscribe::SubscribeManager::new(),
            last_build_error: RwLock::new(None),
        }
    }

    /// Load project settings and variables from `project.toml`.
    ///
    /// Silently defaults when `project.toml` is missing (no config is valid).
    /// Called during `initialized` and at the start of each `rebuild_project`.
    fn load_settings(&self) {
        let root = self.root.read().unwrap().clone();
        match ProjectSettings::load(&root) {
            Ok(ps) => {
                let name = ps.profile.clone();
                let config = ps.config_for_profile(&name);
                *self.variables.write().unwrap() = config.variables.clone();
                *self.profile_name.write().unwrap() = name;
                *self.settings.write().unwrap() = Some(ps);
            }
            Err(_) => {
                // No project.toml or parse error — use defaults.
                *self.settings.write().unwrap() = None;
                *self.variables.write().unwrap() = BTreeMap::new();
                *self.profile_name.write().unwrap() = "default".to_string();
            }
        }
    }

    /// Publish parse diagnostics for a single document.
    async fn publish_diagnostics(&self, uri: Url, text: &str) {
        let rope = Rope::from_str(text);
        let variables = self.variables.read().unwrap().clone();
        let profile = self.profile_name.read().unwrap().clone();
        let diags = diagnostics::diagnose(text, &rope, &variables, &profile);

        // Store the rope for later offset conversions (go-to-definition).
        if let Ok(mut docs) = self.documents.lock() {
            docs.insert(uri.clone(), rope);
        }

        self.client.publish_diagnostics(uri, diags, None).await;
    }

    /// Snapshot document text and cursor context for a given position.
    ///
    /// Acquires the documents lock once and returns the full document text,
    /// char offset, and optional dot-qualified identifier parts at the cursor.
    /// Returns `None` if the document is not open.
    fn snapshot_at_position(
        &self,
        uri: &Url,
        position: Position,
    ) -> Option<(String, usize, Option<Vec<String>>)> {
        let (byte_offset, text) = {
            let docs = self.documents.lock().unwrap();
            let rope = docs.get(uri)?;
            let line_start = rope
                .try_line_to_char(usize::try_from(position.line).unwrap_or(0))
                .ok()?;
            let offset = line_start + usize::try_from(position.character).unwrap_or(0);
            (offset, rope.to_string())
        };

        let parts = goto_definition::find_reference_at_position(&text, byte_offset);
        Some((text, byte_offset, parts))
    }

    /// Handle the `mz-deploy/dag` custom request.
    ///
    /// Returns the project's dependency graph as JSON, or `null` if no project
    /// has been successfully built yet.
    #[allow(clippy::unused_async)] // async required by tower-lsp custom_method
    pub async fn dag(&self) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        let root = self.root.read().unwrap().clone();
        let project_guard = self.project.read().unwrap();
        match project_guard.as_ref() {
            Some(project) => Ok(
                serde_json::to_value(dag::build_dag_response(project, &root))
                    .unwrap_or(serde_json::Value::Null),
            ),
            None => Ok(serde_json::Value::Null),
        }
    }

    /// Handle the `mz-deploy/keywords` custom request.
    ///
    /// Returns the full list of Materialize SQL keywords as a JSON array of
    /// uppercase strings (e.g., `["ABORT", "ACCESS", ...]`). The keyword list
    /// is static and derived from [`mz_sql_lexer::keywords::KEYWORDS`].
    #[allow(clippy::unused_async)] // async required by tower-lsp custom_method
    pub async fn keywords(&self) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        let kws: Vec<&str> = mz_sql_lexer::keywords::KEYWORDS
            .entries()
            .map(|(_, kw)| kw.as_str())
            .collect();
        Ok(serde_json::to_value(kws).unwrap_or(serde_json::Value::Null))
    }

    /// Handle the `mz-deploy/catalog` custom request.
    ///
    /// Returns the project's data catalog as JSON, or `null` if no project
    /// has been successfully built yet.
    #[allow(clippy::unused_async)] // async required by tower-lsp custom_method
    pub async fn catalog(&self) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        let root = self.root.read().unwrap().clone();
        let project_guard = self.project.read().unwrap();
        match project_guard.as_ref() {
            Some(project) => {
                let types_guard = self.types_cache.read().unwrap();
                Ok(serde_json::to_value(catalog::build_catalog_response(
                    project,
                    types_guard.as_ref(),
                    &root,
                ))
                .unwrap_or(serde_json::Value::Null))
            }
            None => {
                let error = self.last_build_error.read().unwrap().clone();
                Ok(serde_json::to_value(catalog::build_error_response(
                    error.as_deref(),
                ))
                .unwrap_or(serde_json::Value::Null))
            }
        }
    }

    // =========================================================================
    // Worksheet endpoints
    // =========================================================================

    /// Handle the `mz-deploy/connection-info` custom request.
    ///
    /// Returns the resolved profile connection details (host, port, user,
    /// profile name) without making a connection. Reports `connected: false`
    /// with a message if the profile can't be loaded.
    #[allow(clippy::unused_async)] // async required by tower-lsp custom_method
    pub async fn connection_info(&self) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        let profile_name = self.profile_name.read().unwrap().clone();
        let resp = match ProfilesConfig::load_profile(None, None, &profile_name) {
            Ok(profile) => worksheet::ConnectionInfoResponse {
                connected: true,
                host: Some(profile.host),
                port: Some(profile.port),
                user: Some(profile.username),
                profile: Some(profile.name),
                message: None,
            },
            Err(e) => worksheet::ConnectionInfoResponse {
                connected: false,
                host: None,
                port: None,
                user: None,
                profile: None,
                message: Some(e.to_string()),
            },
        };
        Ok(serde_json::to_value(resp).unwrap_or(serde_json::Value::Null))
    }

    /// Handle the `mz-deploy/execute-query` custom request.
    ///
    /// Validates and classifies the SQL, injects LIMIT if needed, connects
    /// lazily to Materialize, executes the query with a timeout, and
    /// returns the result as JSON.
    pub async fn execute_query(&self, params: serde_json::Value) -> Result<serde_json::Value> {
        let params: worksheet::ExecuteQueryParams =
            serde_json::from_value(params).map_err(|e| {
                let ws_err = worksheet::WorksheetError {
                    code: "invalid_params",
                    message: e.to_string(),
                    hint: None,
                };
                worksheet_error_to_jsonrpc(&ws_err, tower_lsp::jsonrpc::ErrorCode::InvalidParams)
            })?;

        // Validate the query (pure).
        let validated = worksheet::validate_worksheet_query(&params.query).map_err(|e| {
            worksheet_error_to_jsonrpc(&e, tower_lsp::jsonrpc::ErrorCode::InvalidRequest)
        })?;

        // Ensure connection, extract client, release mutex.
        let pg_client = {
            let mut conn_guard = self.worksheet_connection.lock().await;
            let current_profile = self.profile_name.read().unwrap().clone();

            if let Err(e) = self
                .ensure_worksheet_connection(&mut conn_guard, &current_profile)
                .await
            {
                return Err(worksheet_error_to_jsonrpc(
                    &e,
                    tower_lsp::jsonrpc::ErrorCode::InternalError,
                ));
            }

            let wc = conn_guard.as_ref().unwrap();

            // Store cancel token while we have the guard.
            {
                let mut cancel_guard = self.worksheet_cancel.lock().await;
                *cancel_guard = Some(wc.pg_client.cancel_token());
            }

            Arc::clone(&wc.pg_client)
            // conn_guard dropped here — mutex released before query execution.
        };

        // Execute with timeout (mutex NOT held).
        let timeout = std::time::Duration::from_millis(params.timeout_ms);
        let start = std::time::Instant::now();

        let (sql, kind) = match &validated {
            worksheet::ValidatedQuery::Tabular {
                sql,
                limit_injected,
            } => (
                sql.as_str(),
                QueryKind::Tabular {
                    limit_injected: *limit_injected,
                },
            ),
            worksheet::ValidatedQuery::RawText { sql } => (sql.as_str(), QueryKind::RawText),
            worksheet::ValidatedQuery::DML { sql } => (sql.as_str(), QueryKind::DML),
        };

        let query_result = tokio::time::timeout(timeout, pg_client.simple_query(sql)).await;
        let elapsed_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

        // Clear cancel token.
        {
            let mut cancel_guard = self.worksheet_cancel.lock().await;
            *cancel_guard = None;
        }

        // Format response.
        format_query_response(query_result, kind, elapsed_ms, params.timeout_ms)
    }

    /// Ensure the worksheet connection is live and matches the current profile.
    ///
    /// If the connection is missing, stale (wrong profile), or closed, opens
    /// a new one and stores it in the guard. Returns `Ok(())` on success or
    /// the worksheet error on failure.
    async fn ensure_worksheet_connection(
        &self,
        conn_guard: &mut Option<WorksheetConnection>,
        profile_name: &str,
    ) -> std::result::Result<(), worksheet::WorksheetError> {
        let needs_connect = match conn_guard.as_ref() {
            None => true,
            Some(wc) => wc.profile_name != profile_name || wc.pg_client.is_closed(),
        };
        if needs_connect {
            let wc = self.connect_worksheet(profile_name).await?;
            *conn_guard = Some(wc);
        }
        Ok(())
    }

    /// Handle the `mz-deploy/cancel-query` custom request.
    ///
    /// Cancels both the in-flight one-shot worksheet query (via the stored
    /// cancel token) and any active SUBSCRIBE (via its dedicated cancel token
    /// and task handle). Returns `{ "cancelled": true }` if either was
    /// cancelled, or `{ "cancelled": false }` if nothing was in flight.
    pub async fn cancel_query(&self) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        // Cancel any in-flight one-shot query.
        let token = {
            let mut guard = self.worksheet_cancel.lock().await;
            guard.take()
        };

        let mut cancelled = false;

        if let Some(cancel_token) = token {
            let cancel_result = cancel_with_tls(&self.worksheet_connection, cancel_token).await;
            if cancel_result.is_ok() {
                cancelled = true;
            }
        }

        // Cancel any active SUBSCRIBE.
        if self.subscribe.cancel().await {
            cancelled = true;
        }

        let resp = if cancelled {
            serde_json::json!({ "cancelled": true })
        } else {
            serde_json::json!({ "cancelled": false, "message": "no query in flight" })
        };
        Ok(resp)
    }

    /// Handle the `mz-deploy/subscribe` custom request.
    ///
    /// Validates the SQL, opens a dedicated connection, and delegates to
    /// [`subscribe::SubscribeManager::start`] which spawns the background
    /// FETCH loop and returns immediately with a `subscribe_id`.
    pub async fn subscribe(
        &self,
        params: serde_json::Value,
    ) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        let params: worksheet::SubscribeParams =
            serde_json::from_value(params).map_err(|e| tower_lsp::jsonrpc::Error {
                code: tower_lsp::jsonrpc::ErrorCode::InvalidParams,
                message: e.to_string().into(),
                data: None,
            })?;

        // Validate and inject WITH (PROGRESS).
        let subscribe_sql = worksheet::validate_subscribe_query(&params.query).map_err(|e| {
            worksheet_error_to_jsonrpc(&e, tower_lsp::jsonrpc::ErrorCode::InvalidRequest)
        })?;

        // Open a dedicated connection for the subscribe.
        let current_profile = self.profile_name.read().unwrap().clone();
        let sub_conn = self
            .connect_worksheet(&current_profile)
            .await
            .map_err(|e| {
                worksheet_error_to_jsonrpc(&e, tower_lsp::jsonrpc::ErrorCode::InternalError)
            })?;

        // Snapshot session from the main worksheet connection.
        let session = {
            let conn = self.worksheet_connection.lock().await;
            match conn.as_ref() {
                Some(wc) => subscribe::SessionSnapshot {
                    database: wc.database.clone(),
                    schema: wc.schema.clone(),
                    cluster: wc.cluster.clone(),
                },
                None => subscribe::SessionSnapshot {
                    database: None,
                    schema: None,
                    cluster: None,
                },
            }
        };

        let started = self
            .subscribe
            .start(
                subscribe_sql,
                session,
                sub_conn.pg_client,
                sub_conn.host,
                self.client.clone(),
            )
            .await
            .map_err(|e| {
                worksheet_error_to_jsonrpc(&e, tower_lsp::jsonrpc::ErrorCode::InternalError)
            })?;

        Ok(serde_json::to_value(started).unwrap_or(serde_json::Value::Null))
    }

    /// Handle the `mz-deploy/worksheet-context` custom request.
    ///
    /// Returns all databases with their schemas, all clusters, and the
    /// current session values. If no connection is established, returns
    /// empty collections.
    pub async fn worksheet_context(&self) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        let mut conn_guard = self.worksheet_connection.lock().await;
        let current_profile = self.profile_name.read().unwrap().clone();

        // Lazily establish connection if needed (same as execute_query).
        if let Err(_) = self
            .ensure_worksheet_connection(&mut conn_guard, &current_profile)
            .await
        {
            // Can't connect — return context with profiles only.
            let profiles = load_profile_names();
            return Ok(serde_json::to_value(worksheet::WorksheetContextResponse {
                profiles,
                current_profile: Some(current_profile),
                database_schemas: std::collections::BTreeMap::new(),
                clusters: Vec::new(),
                current_database: None,
                current_schema: None,
                current_cluster: None,
            })
            .unwrap_or(serde_json::Value::Null));
        }

        let profiles = load_profile_names();
        let resp =
            query_worksheet_context(conn_guard.as_ref().unwrap(), profiles, &current_profile).await;
        Ok(serde_json::to_value(resp).unwrap_or(serde_json::Value::Null))
    }

    /// Handle the `mz-deploy/set-session` custom request.
    ///
    /// Applies SET commands to the worksheet connection and returns a fresh
    /// [`WorksheetContextResponse`] with updated current values. The response
    /// includes the full `database_schemas` map so the client can update
    /// schema dropdowns without an additional round-trip.
    pub async fn set_session(
        &self,
        params: serde_json::Value,
    ) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        let params: worksheet::SetSessionParams =
            serde_json::from_value(params).map_err(|e| tower_lsp::jsonrpc::Error {
                code: tower_lsp::jsonrpc::ErrorCode::InvalidParams,
                message: e.to_string().into(),
                data: None,
            })?;

        let mut conn_guard = self.worksheet_connection.lock().await;
        let wc = match conn_guard.as_mut() {
            Some(wc) => wc,
            None => {
                return Err(tower_lsp::jsonrpc::Error {
                    code: tower_lsp::jsonrpc::ErrorCode::InternalError,
                    message: "no worksheet connection".into(),
                    data: None,
                });
            }
        };

        // Run SET commands.
        let sql = worksheet::build_set_commands(&params);
        if !sql.is_empty() {
            wc.pg_client
                .simple_query(&sql)
                .await
                .map_err(|e| tower_lsp::jsonrpc::Error {
                    code: tower_lsp::jsonrpc::ErrorCode::InternalError,
                    message: e.to_string().into(),
                    data: None,
                })?;
        }

        // Refresh cached values from the server.
        wc.database = query_show_value(&wc.pg_client, "SHOW database").await;
        wc.schema = query_show_value(&wc.pg_client, "SHOW search_path").await;
        wc.cluster = query_show_value(&wc.pg_client, "SHOW cluster").await;

        let profiles = load_profile_names();
        let current_profile = self.profile_name.read().unwrap().clone();
        let resp = query_worksheet_context(wc, profiles, &current_profile).await;

        // Tell the client to refresh code lenses so "Execute on <cluster>" updates.
        let _ = self.client.code_lens_refresh().await;

        Ok(serde_json::to_value(resp).unwrap_or(serde_json::Value::Null))
    }

    /// Handle the `mz-deploy/set-profile` custom request.
    ///
    /// Switches the worksheet to a different connection profile. Drops the
    /// current worksheet connection (if any), cancels any active subscribe,
    /// and updates the profile name. The next operation will lazily reconnect
    /// to the new profile. Returns a fresh [`WorksheetContextResponse`].
    pub async fn set_profile(
        &self,
        params: serde_json::Value,
    ) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        let params: worksheet::SetProfileParams =
            serde_json::from_value(params).map_err(|e| tower_lsp::jsonrpc::Error {
                code: tower_lsp::jsonrpc::ErrorCode::InvalidParams,
                message: e.to_string().into(),
                data: None,
            })?;

        // Validate the profile exists.
        let profiles_config =
            ProfilesConfig::load(None).map_err(|e| tower_lsp::jsonrpc::Error {
                code: tower_lsp::jsonrpc::ErrorCode::InternalError,
                message: e.to_string().into(),
                data: None,
            })?;
        let _ = profiles_config.get_profile(&params.profile).map_err(|e| {
            tower_lsp::jsonrpc::Error {
                code: tower_lsp::jsonrpc::ErrorCode::InvalidParams,
                message: e.to_string().into(),
                data: None,
            }
        })?;

        // Update the profile name.
        *self.profile_name.write().unwrap() = params.profile;

        // Drop the worksheet connection — next operation reconnects lazily.
        *self.worksheet_connection.lock().await = None;

        // Cancel any active subscribe.
        self.subscribe.cancel().await;

        // Return a fresh context (triggers lazy reconnect).
        // Delegate to worksheet_context which handles the connect + query.
        self.worksheet_context().await
    }

    /// Establish a new worksheet connection using the given profile name.
    async fn connect_worksheet(
        &self,
        profile_name: &str,
    ) -> std::result::Result<WorksheetConnection, worksheet::WorksheetError> {
        let profile = ProfilesConfig::load_profile(None, None, profile_name).map_err(|e| {
            worksheet::WorksheetError {
                code: "connection_failed",
                message: e.to_string(),
                hint: Some("Check your profiles.toml configuration.".to_string()),
            }
        })?;

        let mut conn_str = format!("host={} port={}", profile.host, profile.port);
        conn_str.push_str(&format!(
            " user='{}'",
            profile.username.replace('\\', "\\\\").replace('\'', "\\'")
        ));
        if let Some(ref password) = profile.password {
            conn_str.push_str(&format!(
                " password='{}'",
                password.replace('\\', "\\\\").replace('\'', "\\'")
            ));
        }

        let connect_err = |e: tokio_postgres::Error| worksheet::WorksheetError {
            code: "connection_failed",
            message: e.to_string(),
            hint: Some(format!(
                "Could not connect to {}:{}",
                profile.host, profile.port
            )),
        };

        let pg_client = if is_local_host(&profile.host) {
            let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
                .await
                .map_err(connect_err)?;
            mz_ore::task::spawn(|| "mz-deploy-worksheet-connection", async move {
                let _ = connection.await;
            });
            client
        } else {
            let connector = build_tls_connector().map_err(|e| worksheet::WorksheetError {
                code: "connection_failed",
                message: e,
                hint: None,
            })?;
            let (client, connection) = tokio_postgres::connect(&conn_str, connector)
                .await
                .map_err(connect_err)?;
            mz_ore::task::spawn(|| "mz-deploy-worksheet-connection", async move {
                let _ = connection.await;
            });
            client
        };

        // Query server defaults for session state.
        let database = query_show_value(&pg_client, "SHOW database").await;
        let schema = query_show_value(&pg_client, "SHOW search_path").await;
        let cluster = query_show_value(&pg_client, "SHOW cluster").await;

        Ok(WorksheetConnection {
            pg_client: Arc::new(pg_client),
            profile_name: profile.name,
            host: profile.host,
            database,
            schema,
            cluster,
        })
    }

    /// Rebuild the project model and types cache from disk.
    ///
    /// Delegates to [`compute_diagnostic_actions`] for the pure diagnostic
    /// diffing logic, then applies the resulting actions via the LSP client.
    /// Types are loaded via [`load_merged_types`].
    async fn rebuild_project(&self) {
        self.load_settings();
        let root = self.root.read().unwrap().clone();
        let (profile, profile_suffix, variables) = {
            let settings_guard = self.settings.read().unwrap();
            match settings_guard.as_ref() {
                Some(ps) => {
                    let profile = ps.profile.clone();
                    let config = ps.config_for_profile(&profile);
                    (
                        profile,
                        config.profile_suffix.clone(),
                        config.variables.clone(),
                    )
                }
                None => ("default".to_string(), None, BTreeMap::new()),
            }
        };

        let build_result =
            project::plan_sync(&root, &profile, profile_suffix.as_deref(), &variables);

        // Extract validation diagnostics from the build result (pure).
        let new_diagnostics = match &build_result {
            Err(ProjectError::Validation(ValidationErrors { errors })) => {
                diagnostics::validation_diagnostics(errors)
            }
            _ => BTreeMap::new(),
        };

        // Compute diagnostic actions (pure).
        let old_uris = self.project_diagnostic_uris.lock().unwrap().clone();
        let actions = compute_diagnostic_actions(new_diagnostics, &old_uris);

        // Store the new project on success, log and record error on failure.
        match build_result {
            Ok(p) => {
                *self.project.write().unwrap() = Some(Arc::new(p));
                *self.last_build_error.write().unwrap() = None;
            }
            Err(ref e) => {
                self.client
                    .log_message(
                        MessageType::ERROR,
                        format!("Project build failed: {e}"),
                    )
                    .await;
                *self.last_build_error.write().unwrap() = Some(format!("{e}"));
            }
        }

        // Apply diagnostic actions (I/O).
        for uri in &actions.uris_to_clear {
            self.client
                .publish_diagnostics(uri.clone(), Vec::new(), None)
                .await;
        }
        for (uri, diags) in actions.diagnostics_to_publish {
            self.client.publish_diagnostics(uri, diags, None).await;
        }
        *self.project_diagnostic_uris.lock().unwrap() = actions.new_tracked_uris;

        // Load types.lock + types.cache.bin so hover covers all objects.
        if let Some(merged) = load_merged_types(&root) {
            *self.types_cache.write().unwrap() = Some(merged);
        }

        // Notify the client that the project has been rebuilt so it can
        // refresh catalog/DAG data.
        self.client.send_notification::<ProjectRebuilt>(()).await;
    }

    /// Run incremental typechecking via Docker after a project rebuild.
    ///
    /// Checks if any SQL AST hashes have changed since the last typecheck. If
    /// so, connects to the local Docker container and type-checks only the dirty
    /// views. On success, writes `types.cache.bin` and `typecheck.snapshot.bin`,
    /// then reloads the types cache and sends a `projectRebuilt` notification so
    /// the extension can refresh column data in the catalog.
    ///
    /// Silently returns on any failure (no Docker, typecheck errors) — the
    /// catalog simply won't have updated column data until the next successful
    /// typecheck.
    async fn run_typecheck(&self) {
        let root = self.root.read().unwrap().clone();

        let project = self.project.read().unwrap().as_ref().map(Arc::clone);
        let project = match project {
            Some(p) => p,
            None => return,
        };

        let plan = match types::plan_typecheck(&root, &project) {
            Ok(p) => p,
            Err(_) => return,
        };

        if plan.is_up_to_date() {
            return;
        }

        let docker_image = self
            .settings
            .read()
            .unwrap()
            .as_ref()
            .map(|s| s.docker_image())
            .unwrap_or_else(|| DEFAULT_DOCKER_IMAGE.to_string());

        let types_lock = types::load_types_lock(&root).unwrap_or_default();

        let runtime = DockerRuntime::new().with_image(&docker_image);
        let mut client = match runtime.get_client(&types_lock).await {
            Ok(c) => c,
            Err(_) => return,
        };

        let staged_fqns: BTreeSet<String> = types_lock.tables.keys().cloned().collect();

        if let Ok(completed_plan) =
            types::typecheck_with_client(&mut client, &project, &root, &staged_fqns, plan).await
        {
            let _ = types::write_snapshot_from_plan(&root, &completed_plan);
        }

        // Reload types cache and notify client
        if let Some(merged) = load_merged_types(&root) {
            *self.types_cache.write().unwrap() = Some(merged);
        }
        self.client.send_notification::<ProjectRebuilt>(()).await;
    }
}

/// Extract the first column of the first row from a `SHOW` command.
///
/// Returns `None` if the query fails or returns no rows.
async fn query_show_value(client: &tokio_postgres::Client, query: &str) -> Option<String> {
    let msgs = client.simple_query(query).await.ok()?;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_string());
        }
    }
    None
}

/// Query the worksheet context from a live connection.
///
/// Returns all databases mapped to their schemas, plus clusters and
/// current session values.
async fn query_worksheet_context(
    wc: &WorksheetConnection,
    profiles: Vec<String>,
    current_profile: &str,
) -> worksheet::WorksheetContextResponse {
    // Query all databases and their schemas in one pass.
    let database_schemas = query_database_schemas(&wc.pg_client).await;

    let clusters = query_name_list(
        &wc.pg_client,
        "SELECT name FROM mz_catalog.mz_clusters ORDER BY name",
    )
    .await;

    worksheet::WorksheetContextResponse {
        profiles,
        current_profile: Some(current_profile.to_string()),
        database_schemas,
        clusters,
        current_database: wc.database.clone(),
        current_schema: wc.schema.clone(),
        current_cluster: wc.cluster.clone(),
    }
}

/// Load the list of available profile names from `profiles.toml`.
fn load_profile_names() -> Vec<String> {
    ProfilesConfig::load(None)
        .map(|c| {
            c.profile_names()
                .into_iter()
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Query all databases and their schemas as a map.
async fn query_database_schemas(
    client: &tokio_postgres::Client,
) -> std::collections::BTreeMap<String, Vec<String>> {
    let mut map: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    let query = "SELECT d.name AS database, s.name AS schema \
                 FROM mz_catalog.mz_schemas s \
                 JOIN mz_catalog.mz_databases d ON s.database_id = d.id \
                 ORDER BY d.name, s.name";
    if let Ok(msgs) = client.simple_query(query).await {
        for msg in msgs {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                if let (Some(db), Some(schema)) = (row.get(0), row.get(1)) {
                    map.entry(db.to_string())
                        .or_default()
                        .push(schema.to_string());
                }
            }
        }
    }
    map
}

/// Run a query and collect the first column of each row as a string list.
async fn query_name_list(client: &tokio_postgres::Client, query: &str) -> Vec<String> {
    let mut names = Vec::new();
    if let Ok(msgs) = client.simple_query(query).await {
        for msg in msgs {
            if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
                if let Some(val) = row.get(0) {
                    names.push(val.to_string());
                }
            }
        }
    }
    names
}

/// Build a TLS connector for Materialize cloud connections.
///
/// Shared between [`Backend::connect_worksheet`] and [`cancel_with_tls`].
fn build_tls_connector() -> std::result::Result<MakeTlsConnector, String> {
    let mut builder =
        SslConnector::builder(SslMethod::tls()).map_err(|e| format!("TLS builder: {e}"))?;

    let ca_paths = [
        "/etc/ssl/cert.pem",
        "/opt/homebrew/etc/openssl@3/cert.pem",
        "/usr/local/etc/openssl@3/cert.pem",
        "/opt/homebrew/etc/openssl/cert.pem",
        "/usr/local/etc/openssl/cert.pem",
        "/etc/ssl/certs/ca-certificates.crt",
        "/etc/pki/tls/certs/ca-bundle.crt",
        "/etc/ssl/ca-bundle.pem",
    ];

    let mut ca_loaded = false;
    for path in &ca_paths {
        if std::path::Path::new(path).exists() && builder.set_ca_file(path).is_ok() {
            ca_loaded = true;
            break;
        }
    }
    if !ca_loaded {
        let _ = builder.set_default_verify_paths();
    }

    builder.set_verify(SslVerifyMode::PEER);
    Ok(MakeTlsConnector::new(builder.build()))
}

/// Returns `true` if the host looks like a local or private-network address.
///
/// Used to decide TLS policy: local connections use `NoTls`, remote ones
/// use OpenSSL. The heuristic covers loopback and RFC 1918 prefixes.
fn is_local_host(host: &str) -> bool {
    host == "localhost"
        || host == "127.0.0.1"
        || host.starts_with("192.168.")
        || host.starts_with("10.")
        || host.starts_with("172.")
}

/// Send a cancel request, choosing TLS policy based on the host.
pub(super) async fn cancel_query_with_tls_policy(
    cancel_token: &tokio_postgres::CancelToken,
    host: &str,
) -> std::result::Result<(), String> {
    if is_local_host(host) {
        cancel_token
            .cancel_query(NoTls)
            .await
            .map_err(|e| e.to_string())
    } else {
        let connector = build_tls_connector()?;
        cancel_token
            .cancel_query(connector)
            .await
            .map_err(|e| e.to_string())
    }
}

/// Send a cancel message for a one-shot query using the worksheet connection's host.
async fn cancel_with_tls(
    conn: &tokio::sync::Mutex<Option<WorksheetConnection>>,
    cancel_token: tokio_postgres::CancelToken,
) -> std::result::Result<(), String> {
    let host = {
        let guard = conn.lock().await;
        match guard.as_ref() {
            Some(wc) => wc.host.clone(),
            None => return Err("no active connection".to_string()),
        }
    };
    cancel_query_with_tls_policy(&cancel_token, &host).await
}

/// Classification of a validated worksheet query for response formatting.
enum QueryKind {
    Tabular { limit_injected: bool },
    RawText,
    DML,
}

/// Format the result of a worksheet query execution into a JSON-RPC response.
///
/// Handles success, query error, and timeout cases. On success, delegates
/// to the appropriate worksheet response builder based on the [`QueryKind`].
fn format_query_response(
    query_result: std::result::Result<
        std::result::Result<Vec<tokio_postgres::SimpleQueryMessage>, tokio_postgres::Error>,
        tokio::time::error::Elapsed,
    >,
    kind: QueryKind,
    elapsed_ms: u64,
    timeout_ms: u64,
) -> Result<serde_json::Value> {
    match query_result {
        Ok(Ok(messages)) => {
            let resp = match kind {
                QueryKind::Tabular { limit_injected } => {
                    let extracted = worksheet::extract_rows_from_messages(messages);
                    worksheet::build_tabular_response(extracted, limit_injected, elapsed_ms)
                }
                QueryKind::RawText => {
                    let lines = worksheet::extract_text_from_messages(messages);
                    worksheet::build_raw_text_response(lines, elapsed_ms)
                }
                QueryKind::DML => worksheet::build_dml_response(messages, elapsed_ms),
            };
            Ok(serde_json::to_value(resp).unwrap_or(serde_json::Value::Null))
        }
        Ok(Err(e)) => {
            let ws_err = worksheet::WorksheetError {
                code: "query_error",
                message: e.to_string(),
                hint: None,
            };
            Err(worksheet_error_to_jsonrpc(
                &ws_err,
                tower_lsp::jsonrpc::ErrorCode::InternalError,
            ))
        }
        Err(_) => {
            let ws_err = worksheet::WorksheetError {
                code: "timeout",
                message: format!("query timed out after {}ms", timeout_ms),
                hint: Some("Try a simpler query or increase the timeout.".to_string()),
            };
            Err(worksheet_error_to_jsonrpc(
                &ws_err,
                tower_lsp::jsonrpc::ErrorCode::InternalError,
            ))
        }
    }
}

/// Convert a [`WorksheetError`] into a JSON-RPC error response.
fn worksheet_error_to_jsonrpc(
    e: &worksheet::WorksheetError,
    code: tower_lsp::jsonrpc::ErrorCode,
) -> tower_lsp::jsonrpc::Error {
    tower_lsp::jsonrpc::Error {
        code,
        message: e.message.clone().into(),
        data: serde_json::to_value(e).ok(),
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        if let Some(root_uri) = params.root_uri {
            if let Ok(path) = root_uri.to_file_path() {
                if let Ok(mut root) = self.root.write() {
                    *root = path;
                }
            }
        }

        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Options(
                    TextDocumentSyncOptions {
                        open_close: Some(true),
                        change: Some(TextDocumentSyncKind::FULL),
                        save: Some(TextDocumentSyncSaveOptions::SaveOptions(SaveOptions {
                            include_text: Some(false),
                        })),
                        ..Default::default()
                    },
                )),
                completion_provider: Some(CompletionOptions::default()),
                definition_provider: Some(OneOf::Left(true)),
                references_provider: Some(OneOf::Left(true)),
                document_symbol_provider: Some(OneOf::Left(true)),
                workspace_symbol_provider: Some(OneOf::Left(true)),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                code_lens_provider: Some(CodeLensOptions {
                    resolve_provider: Some(false),
                }),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.load_settings();
        self.rebuild_project().await;
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        self.publish_diagnostics(params.text_document.uri, &params.text_document.text)
            .await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        if let Some(change) = params.content_changes.into_iter().last() {
            self.publish_diagnostics(params.text_document.uri, &change.text)
                .await;
        }
    }

    async fn did_save(&self, _params: DidSaveTextDocumentParams) {
        self.rebuild_project().await;
        self.run_typecheck().await;
    }

    async fn did_change_watched_files(&self, _params: DidChangeWatchedFilesParams) {
        self.rebuild_project().await;
        self.run_typecheck().await;
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let uri = params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let (_, _, parts) = match self.snapshot_at_position(&uri, position) {
            Some(s) => s,
            None => return Ok(None),
        };
        let parts = match parts {
            Some(p) => p,
            None => return Ok(None),
        };

        let root = self.root.read().unwrap().clone();
        let project_guard = self.project.read().unwrap();
        let project = match project_guard.as_ref() {
            Some(p) => p,
            None => return Ok(None),
        };

        let location = goto_definition::resolve_reference(&parts, &uri, &root, project);
        Ok(location.map(GotoDefinitionResponse::Scalar))
    }

    async fn references(&self, params: ReferenceParams) -> Result<Option<Vec<Location>>> {
        let uri = params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;

        let (_, _, parts) = match self.snapshot_at_position(&uri, position) {
            Some(s) => s,
            None => return Ok(None),
        };
        let parts = match parts {
            Some(p) => p,
            None => return Ok(None),
        };

        let root = self.root.read().unwrap().clone();
        let project_guard = self.project.read().unwrap();
        let project = match project_guard.as_ref() {
            Some(p) => p,
            None => return Ok(None),
        };

        let locations = references::find_references(
            &parts,
            &uri,
            &root,
            project,
            params.context.include_declaration,
        );
        if locations.is_empty() {
            Ok(None)
        } else {
            Ok(Some(locations))
        }
    }

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        let file_uri = params.text_document.uri;
        let root = self.root.read().unwrap().clone();

        let project_guard = self.project.read().unwrap();
        let project = match project_guard.as_ref() {
            Some(p) => p,
            None => return Ok(None),
        };

        let symbols = document_symbol::document_symbols(&file_uri, &root, project);
        if symbols.is_empty() {
            Ok(None)
        } else {
            Ok(Some(DocumentSymbolResponse::Nested(symbols)))
        }
    }

    async fn symbol(
        &self,
        params: WorkspaceSymbolParams,
    ) -> Result<Option<Vec<SymbolInformation>>> {
        let root = self.root.read().unwrap().clone();
        let project_guard = self.project.read().unwrap();
        let project = match project_guard.as_ref() {
            Some(p) => p,
            None => return Ok(None),
        };

        let symbols = workspace_symbol::workspace_symbols(&params.query, project, &root);
        if symbols.is_empty() {
            Ok(None)
        } else {
            Ok(Some(symbols))
        }
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let (text, byte_offset, parts) = match self.snapshot_at_position(&uri, position) {
            Some(s) => s,
            None => return Ok(None),
        };

        // Try variable hover first (pure).
        let variables = self.variables.read().unwrap();
        let profile = self.profile_name.read().unwrap();
        if let Some(h) = hover::resolve_variable_hover(&text, byte_offset, &variables, &profile) {
            return Ok(Some(h));
        }
        drop(variables);
        drop(profile);

        // Then object hover (pure).
        let parts = match parts {
            Some(p) => p,
            None => return Ok(None),
        };

        let root = self.root.read().unwrap().clone();
        let project_guard = self.project.read().unwrap();
        let project = match project_guard.as_ref() {
            Some(p) => p,
            None => return Ok(None),
        };

        let cache_guard = self.types_cache.read().unwrap();
        let empty = Types::default();
        let types_cache = cache_guard.as_ref().unwrap_or(&empty);

        Ok(hover::resolve_hover(
            &parts,
            &uri,
            &root,
            project,
            types_cache,
        ))
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let file_uri = params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;
        let root = self.root.read().unwrap().clone();

        let doc_text = {
            let docs = self.documents.lock().unwrap();
            docs.get(&file_uri).map(|rope| rope.to_string())
        };
        let text = doc_text.as_deref().unwrap_or("");
        let prefix = completion::prefix_context(text, position);

        let project_guard = self.project.read().unwrap();
        let cache_guard = self.types_cache.read().unwrap();
        let items = completion::complete(
            project_guard.as_deref(),
            cache_guard.as_ref(),
            &file_uri,
            &root,
            &prefix,
        );

        Ok(Some(CompletionResponse::Array(items)))
    }

    async fn code_lens(&self, params: CodeLensParams) -> Result<Option<Vec<CodeLens>>> {
        let file_uri = params.text_document.uri;
        let root = self.root.read().unwrap().clone();

        let doc_text = {
            let docs = self.documents.lock().unwrap();
            docs.get(&file_uri).map(|rope| rope.to_string())
        };
        let text = match doc_text.as_deref() {
            Some(t) => t,
            None => return Ok(None),
        };

        // Get current cluster name for worksheet code lenses.
        let cluster = self
            .worksheet_connection
            .try_lock()
            .ok()
            .and_then(|guard| guard.as_ref().and_then(|wc| wc.cluster.clone()));

        let project_guard = self.project.read().unwrap();
        let lenses = code_lens::code_lenses(
            &file_uri,
            text,
            &root,
            project_guard.as_deref(),
            cluster.as_deref(),
        );
        Ok(Some(lenses))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_url(path: &str) -> Url {
        Url::from_file_path(path).unwrap()
    }

    fn make_diagnostic(msg: &str) -> Diagnostic {
        Diagnostic {
            message: msg.to_string(),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn diagnostic_actions_success_clears_all() {
        let old = vec![file_url("/a.sql"), file_url("/b.sql")];
        let actions = compute_diagnostic_actions(BTreeMap::new(), &old);

        assert!(actions.diagnostics_to_publish.is_empty());
        assert_eq!(actions.uris_to_clear.len(), 2);
        assert!(actions.new_tracked_uris.is_empty());
    }

    #[test]
    fn diagnostic_actions_validation_errors() {
        let old = vec![file_url("/a.sql"), file_url("/b.sql")];
        let mut new_diags = BTreeMap::new();
        new_diags.insert(PathBuf::from("/b.sql"), vec![make_diagnostic("error in b")]);
        new_diags.insert(PathBuf::from("/c.sql"), vec![make_diagnostic("error in c")]);

        let actions = compute_diagnostic_actions(new_diags, &old);

        // /a.sql should be cleared (was in old, not in new).
        assert_eq!(actions.uris_to_clear, vec![file_url("/a.sql")]);
        // /b.sql and /c.sql should be published.
        assert_eq!(actions.diagnostics_to_publish.len(), 2);
        // Tracked URIs should be the new set.
        assert_eq!(actions.new_tracked_uris.len(), 2);
    }

    #[test]
    fn diagnostic_actions_no_previous() {
        let mut new_diags = BTreeMap::new();
        new_diags.insert(PathBuf::from("/a.sql"), vec![make_diagnostic("error")]);

        let actions = compute_diagnostic_actions(new_diags, &[]);

        assert!(actions.uris_to_clear.is_empty());
        assert_eq!(actions.diagnostics_to_publish.len(), 1);
        assert_eq!(actions.new_tracked_uris.len(), 1);
    }

    #[test]
    fn load_merged_types_returns_none_for_missing_files() {
        let result = load_merged_types(Path::new("/nonexistent/path"));
        assert!(result.is_none());
    }
}
