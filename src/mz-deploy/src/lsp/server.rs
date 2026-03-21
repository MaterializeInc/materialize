//! LSP backend and `LanguageServer` trait implementation.
//!
//! [`Backend`] holds per-session state: the document ropes (for offset
//! conversion), the latest successfully built project model (for
//! go-to-definition and hover), the types cache (for hover column schemas),
//! and the project root path.
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
//!   project-level validation diagnostics. On each rebuild, old diagnostics
//!   are cleared (empty `[]` published) for URIs no longer in the error set,
//!   then new diagnostics are published. On a successful build all tracked
//!   URIs are cleared.
//! - **`root`** — Set during `initialize` from `InitializeParams::root_uri`.
//! - **`settings`** / **`variables`** — Cached from `project.toml` via
//!   [`load_settings()`]. Reloaded at startup and on every save. Variables are
//!   passed to per-keystroke diagnostics and the on-save project rebuild.

use crate::config::ProjectSettings;
use crate::lsp::{completion, diagnostics, goto_definition, hover};
use crate::project;
use crate::project::error::{ProjectError, ValidationErrors};
use crate::project::planned;
use crate::types::{self, Types};
use ropey::Rope;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::RwLock;
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

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
    /// Pre-computed keyword completion items (static; object completions are
    /// computed per-request since they depend on the file URI and project state).
    completions: Vec<CompletionItem>,
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
            completions: completion::keyword_completions(),
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

    /// Find the dot-qualified identifier parts at the given cursor position.
    ///
    /// Converts the LSP line/character position to a byte offset using the
    /// stored rope, reads the document text, and extracts the identifier chain.
    fn find_parts_at_position(&self, uri: &Url, position: Position) -> Option<Vec<String>> {
        let (byte_offset, text) = {
            let docs = self.documents.lock().unwrap();
            let rope = docs.get(uri)?;
            let line_start = rope
                .try_line_to_char(usize::try_from(position.line).unwrap_or(0))
                .ok()?;
            let offset = line_start + usize::try_from(position.character).unwrap_or(0);
            (offset, rope.to_string())
        };

        goto_definition::find_reference_at_position(&text, byte_offset)
    }

    /// Clear project diagnostics for all previously tracked URIs.
    async fn clear_project_diagnostics(&self) {
        let old_uris = {
            let mut uris = self.project_diagnostic_uris.lock().unwrap();
            std::mem::take(&mut *uris)
        };
        for uri in old_uris {
            self.client.publish_diagnostics(uri, Vec::new(), None).await;
        }
    }

    /// Rebuild the project model and types cache from disk.
    ///
    /// On validation errors, publishes diagnostics to the affected files and
    /// clears diagnostics from files that no longer have errors. On success,
    /// clears all project diagnostics.
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
        match project::plan_sync(&root, &profile, profile_suffix.as_deref(), &variables) {
            Ok(p) => {
                self.clear_project_diagnostics().await;
                if let Ok(mut proj) = self.project.write() {
                    *proj = Some(p);
                }
            }
            Err(ProjectError::Validation(ValidationErrors { errors })) => {
                let new_diags = diagnostics::validation_diagnostics(&errors);

                // Collect new URIs and publish diagnostics.
                let new_uris: Vec<Url> = new_diags
                    .iter()
                    .filter_map(|(path, _)| Url::from_file_path(path).ok())
                    .collect();

                // Clear old URIs that aren't in the new set.
                {
                    let old_uris = self.project_diagnostic_uris.lock().unwrap().clone();
                    for uri in &old_uris {
                        if !new_uris.contains(uri) {
                            self.client
                                .publish_diagnostics(uri.clone(), Vec::new(), None)
                                .await;
                        }
                    }
                }

                // Publish new diagnostics.
                for (path, diags) in &new_diags {
                    if let Ok(uri) = Url::from_file_path(path) {
                        self.client
                            .publish_diagnostics(uri, diags.clone(), None)
                            .await;
                    }
                }

                // Store the new set of URIs.
                *self.project_diagnostic_uris.lock().unwrap() = new_uris;
            }
            Err(_) => {}
        }

        // Load types.lock (external deps) and types.cache.bin (internal views),
        // merging them so hover covers all objects.
        let mut merged = types::load_types_lock(&root).unwrap_or_default();
        if let Ok(cache) = types::load_types_cache(&root) {
            merged.merge(&cache);
        }
        if !merged.tables.is_empty() {
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

        let parts = match self.find_parts_at_position(&uri, position) {
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

        {
            let (byte_offset, text) = {
                let docs = self.documents.lock().unwrap();
                match docs.get(&uri) {
                    Some(rope) => {
                        let line_start = rope
                            .try_line_to_char(usize::try_from(position.line).unwrap_or(0))
                            .ok();
                        let offset = line_start
                            .map(|ls| ls + usize::try_from(position.character).unwrap_or(0));
                        (offset, Some(rope.to_string()))
                    }
                    None => (None, None),
                }
            };
            if let (Some(offset), Some(text)) = (byte_offset, text) {
                let variables = self.variables.read().unwrap();
                let profile = self.profile_name.read().unwrap();
                if let Some(h) = hover::resolve_variable_hover(&text, offset, &variables, &profile)
                {
                    return Ok(Some(h));
                }
            }
        }

        let parts = match self.find_parts_at_position(&uri, position) {
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

        // If the user is typing a qualified name (dots >= 1), keywords are
        // not valid — only return object completions.
        let mut items = if prefix.dots == 0 {
            self.completions.clone()
        } else {
            Vec::new()
        };

        if let Some(project) = self.project.read().unwrap().as_ref() {
            let cache_guard = self.types_cache.read().unwrap();
            items.extend(completion::object_completions(
                project,
                cache_guard.as_ref(),
                &file_uri,
                &root,
                &prefix,
            ));
        }

        Ok(Some(CompletionResponse::Array(items)))
    }
}
