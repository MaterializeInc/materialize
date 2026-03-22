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

use crate::config::ProjectSettings;
use crate::lsp::{catalog, code_lens, completion, dag, diagnostics, goto_definition, hover};
use crate::project;
use crate::project::error::{ProjectError, ValidationErrors};
use crate::project::planned;
use crate::types::{self, Types};
use ropey::Rope;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

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

/// LSP backend holding session state.
pub struct Backend {
    /// Client handle for sending notifications (e.g., diagnostics).
    client: Client,
    /// Per-file text ropes, keyed by document URI.
    documents: Mutex<BTreeMap<Url, Rope>>,
    /// The latest successfully built project model.
    project: RwLock<Option<planned::Project>>,
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
    /// byte offset, and optional dot-qualified identifier parts at the cursor.
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
            None => Ok(serde_json::Value::Null),
        }
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

        // Store the new project on success.
        if let Ok(p) = build_result {
            *self.project.write().unwrap() = Some(p);
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
            project_guard.as_ref(),
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

        let project_guard = self.project.read().unwrap();
        let project = match project_guard.as_ref() {
            Some(p) => p,
            None => return Ok(None),
        };

        let lenses = code_lens::code_lenses(&file_uri, text, &root, project);
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
