// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! LSP backend and `LanguageServer` trait implementation.
//!
//! [`Backend`] holds per-session state: open documents, compiled project
//! metadata (for go-to-definition, hover, completion, and code lens),
//! and workspace configuration.
//!
//! ## State Management
//!
//! - **`documents`** — Open document contents, updated on every `didOpen` /
//!   `didChange`.
//! - **`project_cache`** — Compiled project metadata. Opened lazily on the
//!   first successful build; the same handle is reused across rebuilds.
//! - **`parse_diagnostics`** — Per-keystroke parse-level diagnostics, keyed by
//!   URI. Updated on every `didOpen` / `didChange`.
//! - **`project_diagnostics`** — Project-level (validation + typecheck)
//!   diagnostics, keyed by URI. Updated on every `rebuild_project`.
//! - **`root`** — The workspace root directory.
//! - **`settings`** / **`variables`** — Project and profile configuration,
//!   reloaded at startup and on every save.
//!
//! ## Diagnostic Publishing
//!
//! LSP `publishDiagnostics` is full-replacement per URI, so the two sources
//! must be merged before publishing or one will overwrite the other. Both
//! diagnostic flows route through [`Backend::publish_merged`], which reads
//! both maps and emits the union.
//!
//! `rebuild_project()` runs validation and (on a successful build)
//! typechecking inline. Both are merged into the new project-diagnostic map;
//! every URI that was previously tracked or is newly tracked is republished
//! through [`Backend::publish_merged`] so stale project diagnostics clear
//! while parse diagnostics for open documents survive.

use crate::config::{ProjectSettings, read_mzprofile};
use crate::lsp::{
    code_action, code_lens, completion, diagnostics, document_symbol, goto_definition, hover,
    references, semantic_tokens, workspace_symbol,
};
use crate::project;
use crate::project::compiler::cache::ProjectCache;
use crate::project::compiler::typecheck::TypeCheckError;
use crate::project::error::{ProjectError, ValidationErrors};
use crate::project::ir::graph;
use crate::types;
use ropey::Rope;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

/// How long to wait after the last keystroke before kicking off an idle
/// rebuild. Long enough that rapid typing doesn't run repeated rebuilds,
/// short enough that the user sees diagnostics promptly after pausing.
const IDLE_REBUILD_DEBOUNCE: Duration = Duration::from_millis(100);

/// Resolve the active profile for LSP operations.
///
/// The language server has no CLI flags; it reads the project root's
/// `.mzprofile`. Returns `None` if no profile is set — variable resolution,
/// suffix lookup, and cluster normalization all degrade gracefully (no
/// variables, no suffix), and SQL with `:variables` will surface
/// undefined-variable diagnostics as usual.
fn resolve_lsp_profile_name(project_root: &Path) -> Option<String> {
    read_mzprofile(project_root).ok().flatten()
}

/// Try to open a long-lived [`ProjectCache`] (read-only SQLite connection).
///
/// Returns `None` if the database file doesn't exist yet or can't be opened.
fn try_open_project_cache(
    root: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> Option<ProjectCache> {
    ProjectCache::open(root, profile, profile_suffix, variables)
        .ok()
        .flatten()
}

/// LSP backend holding session state.
///
/// `Backend` is a cheap-to-clone handle around an [`Arc<BackendInner>`].
/// Cloning is the standard way to capture state in spawned tokio tasks (e.g.
/// the debounced rebuild scheduler). Field access goes through `Deref` so
/// existing `self.documents.lock()` patterns work unchanged.
#[derive(Clone)]
pub(super) struct Backend {
    inner: Arc<BackendInner>,
}

pub(super) struct BackendInner {
    /// Client handle for sending notifications (e.g., diagnostics).
    client: Client,
    /// Per-file text ropes, keyed by document URI.
    documents: Mutex<BTreeMap<Url, Rope>>,
    /// Compiled project metadata for go-to-definition, hover, completion, and code lens.
    project_cache: Mutex<Option<ProjectCache>>,
    /// Latest per-keystroke parse-level diagnostics, keyed by URI.
    parse_diagnostics: Mutex<BTreeMap<Url, Vec<Diagnostic>>>,
    /// Latest project-level (validation + typecheck) diagnostics, keyed by URI.
    project_diagnostics: Mutex<BTreeMap<Url, Vec<Diagnostic>>>,
    /// Project root directory.
    root: RwLock<PathBuf>,
    /// Cached project settings loaded from `project.toml`.
    settings: RwLock<Option<ProjectSettings>>,
    /// Cached variables from the active profile config.
    variables: RwLock<BTreeMap<String, String>>,
    /// Name of the active profile (for hover display). `None` when no profile
    /// is set in the project — variable references will surface as undefined.
    profile_name: RwLock<Option<String>>,
    /// Monotonic edit version. Bumps on every signal that should cause a
    /// rebuild (didChange, didSave, didChangeWatchedFiles, initialized).
    edit_version: AtomicU64,
    /// Highest edit version a published rebuild was based on. A rebuild only
    /// runs if `edit_version > rebuilt_through`.
    rebuilt_through: AtomicU64,
    /// Serializes rebuild execution: at most one rebuild runs at a time.
    rebuild_lock: Mutex<()>,
}

impl Deref for Backend {
    type Target = BackendInner;
    fn deref(&self) -> &BackendInner {
        &self.inner
    }
}

impl Backend {
    /// Create a new backend with the given LSP client handle and project root.
    pub(super) fn new_with_root(client: Client, root: PathBuf) -> Self {
        Self {
            inner: Arc::new(BackendInner {
                client,
                documents: Mutex::new(BTreeMap::new()),
                project_cache: Mutex::new(None),
                parse_diagnostics: Mutex::new(BTreeMap::new()),
                project_diagnostics: Mutex::new(BTreeMap::new()),
                root: RwLock::new(root),
                settings: RwLock::new(None),
                variables: RwLock::new(BTreeMap::new()),
                profile_name: RwLock::new(None),
                edit_version: AtomicU64::new(0),
                rebuilt_through: AtomicU64::new(0),
                rebuild_lock: Mutex::new(()),
            }),
        }
    }

    /// Load project settings and variables from `project.toml`.
    ///
    /// Silently defaults when `project.toml` is missing (no config is valid).
    /// Called during `initialized` and at the start of each `rebuild_project`.
    async fn load_settings(&self) {
        let root = self.root.read().await.clone();
        match ProjectSettings::load(&root) {
            Ok(ps) => {
                let name = resolve_lsp_profile_name(&root);
                let config = match &name {
                    Some(n) => ps.config_for_profile(n),
                    None => Default::default(),
                };
                *self.variables.write().await = config.variables.clone();
                *self.profile_name.write().await = name;
                *self.settings.write().await = Some(ps);
            }
            Err(_) => {
                // No project.toml or parse error — use defaults.
                *self.settings.write().await = None;
                *self.variables.write().await = BTreeMap::new();
                *self.profile_name.write().await = None;
            }
        }
    }

    /// Publish parse diagnostics for a single document.
    ///
    /// Updates the parse-diagnostic cache for `uri` and republishes the
    /// merged set (parse + project) so prior project-level diagnostics for
    /// this URI survive the per-keystroke refresh.
    async fn publish_diagnostics(&self, uri: Url, text: &str) {
        let rope = Rope::from_str(text);
        let path = uri.to_file_path().ok();
        let diags = match path.as_deref() {
            Some(p) if p.extension().and_then(|e| e.to_str()) == Some("toml") => Vec::new(),
            _ => {
                let variables = self.variables.read().await.clone();
                let profile = self.profile_name.read().await.clone();
                diagnostics::diagnose(text, &rope, &variables, profile.as_deref())
            }
        };

        // Store the rope for later offset conversions (go-to-definition).
        let mut docs = self.documents.lock().await;
        docs.insert(uri.clone(), rope);
        drop(docs); // release before .await on client

        self.parse_diagnostics
            .lock()
            .await
            .insert(uri.clone(), diags);
        self.publish_merged(uri).await;
    }

    /// Publish the union of parse and project diagnostics for `uri`.
    ///
    /// LSP `publishDiagnostics` is a full-replacement per URI, so we have to
    /// resend both streams together every time either changes. Empty union →
    /// publish an empty list (which clears any stale diagnostics on the
    /// client).
    async fn publish_merged(&self, uri: Url) {
        let parse = self
            .parse_diagnostics
            .lock()
            .await
            .get(&uri)
            .cloned()
            .unwrap_or_default();
        let project = self
            .project_diagnostics
            .lock()
            .await
            .get(&uri)
            .cloned()
            .unwrap_or_default();
        let mut merged = parse;
        merged.extend(project);
        self.client.publish_diagnostics(uri, merged, None).await;
    }

    /// Snapshot the open-document map into a `FileSystem` overlay.
    ///
    /// Each open document's URI is converted to an absolute filesystem path
    /// and its rope is stringified into the overlay. URIs that don't resolve
    /// to a `file://` path (e.g. `untitled:`) are skipped.
    ///
    /// At save time the overlay matches disk byte-for-byte (the save just
    /// flushed). With idle rebuilds (Phase 3), the overlay carries unsaved
    /// edits forward into the compiler.
    async fn build_overlay(&self) -> crate::fs::FileSystem {
        let docs = self.documents.lock().await;
        let mut overlay = BTreeMap::new();
        for (uri, rope) in docs.iter() {
            if let Ok(path) = uri.to_file_path() {
                overlay.insert(path, rope.to_string());
            }
        }
        crate::fs::FileSystem::with_overlay(overlay)
    }

    /// Snapshot document text and cursor context for a given position.
    ///
    /// Acquires the documents lock once and returns the full document text,
    /// char offset, and optional dot-qualified identifier parts at the cursor.
    /// Returns `None` if the document is not open.
    async fn snapshot_at_position(
        &self,
        uri: &Url,
        position: Position,
    ) -> Option<(String, usize, Option<Vec<String>>)> {
        let (byte_offset, text) = {
            let docs = self.documents.lock().await;
            let rope = docs.get(uri)?;
            let offset = diagnostics::position_to_offset(position, rope)?;
            (offset, rope.to_string())
        };

        let parts = goto_definition::find_reference_at_position(&text, byte_offset);
        Some((text, byte_offset, parts))
    }

    /// Schedule a debounced rebuild after the next idle window.
    ///
    /// Each call bumps `edit_version` and spawns a task that, after
    /// [`IDLE_REBUILD_DEBOUNCE`], calls [`maybe_rebuild`](Self::maybe_rebuild)
    /// only if its captured version is still the latest — i.e. the user has
    /// stopped typing. Sibling tasks from earlier keystrokes self-bail.
    fn schedule_rebuild_after_idle(&self) {
        let target = self.edit_version.fetch_add(1, Ordering::SeqCst) + 1;
        let backend = self.clone();
        mz_ore::task::spawn(|| "debounce", async move {
            tokio::time::sleep(IDLE_REBUILD_DEBOUNCE).await;
            if backend.edit_version.load(Ordering::SeqCst) != target {
                // A newer keystroke has scheduled (or will schedule) its own task.
                return;
            }
            backend.maybe_rebuild().await;
        });
    }

    /// Run a rebuild if the buffer has moved past the last published version,
    /// serializing concurrent triggers through `rebuild_lock`.
    ///
    /// Concurrency invariants:
    /// - At most one rebuild executes at a time (mutex).
    /// - Each rebuild captures `edit_version` under the lock, so its overlay
    ///   snapshot is consistent with `started_against`.
    /// - If the buffer changes during the rebuild, the publish step is
    ///   skipped and `rebuilt_through` is *not* advanced — the next trigger
    ///   will rebuild against the newer version.
    /// - Redundant triggers (no edits since the last published rebuild)
    ///   skip the pipeline via the `started_against <= rebuilt_through`
    ///   check.
    ///
    /// Runs validation and (on success) typechecking, merges their diagnostics,
    /// and applies the resulting publish/clear actions via the LSP client.
    /// Opens the [`ProjectCache`] lazily on the first rebuild where the
    /// SQLite DB file exists, regardless of whether the build itself
    /// succeeded — this keeps hover/goto/find-references usable against the
    /// last-known-good state even while the user has a temporary error in
    /// their working tree.
    async fn maybe_rebuild(&self) {
        if self.edit_version.load(Ordering::SeqCst) <= self.rebuilt_through.load(Ordering::SeqCst) {
            return;
        }
        let _guard = self.rebuild_lock.lock().await;
        let started_against = self.edit_version.load(Ordering::SeqCst);
        if started_against <= self.rebuilt_through.load(Ordering::SeqCst) {
            // Another rebuild covered this version while we waited for the lock.
            return;
        }

        self.load_settings().await;
        let root = self.root.read().await.clone();
        let (profile, profile_suffix, variables) = {
            let settings_guard = self.settings.read().await;
            match settings_guard.as_ref() {
                Some(ps) => {
                    let profile = resolve_lsp_profile_name(&root);
                    let config = match &profile {
                        Some(name) => ps.config_for_profile(name),
                        None => Default::default(),
                    };
                    (
                        profile,
                        config.profile_suffix.clone(),
                        config.variables.clone(),
                    )
                }
                None => (None, None, BTreeMap::new()),
            }
        };

        let fs = self.build_overlay().await;
        let build_result = project::plan_sync(
            &fs,
            &root,
            profile.as_deref(),
            profile_suffix.as_deref(),
            &variables,
        );

        // Extract validation diagnostics from the build result (pure).
        let mut new_diagnostics = match &build_result {
            Err(ProjectError::Validation(ValidationErrors { errors })) => {
                diagnostics::validation_diagnostics(&fs, errors)
            }
            _ => BTreeMap::new(),
        };

        let project = match build_result {
            Ok(p) => Some(Arc::new(p)),
            Err(ref e) => {
                // Validation errors already flow through `new_diagnostics`;
                // surface non-validation build failures as a client log.
                if !matches!(e, ProjectError::Validation(_)) {
                    self.client
                        .log_message(MessageType::ERROR, format!("Project build failed: {e}"))
                        .await;
                }
                None
            }
        };

        // Run typecheck only when the project compiled. Merge typecheck errors
        // into the diagnostic map so they flow through the same publish/clear
        // pipeline as validation errors.
        if let Some(ref project) = project {
            if let Some(tc_err) = self
                .run_typecheck(
                    Arc::clone(project),
                    profile.as_deref().unwrap_or(""),
                    profile_suffix.as_deref(),
                    &variables,
                )
                .await
            {
                let candidates = {
                    let guard = self.project_cache.lock().await;
                    code_action::harvest_candidates(guard.as_ref())
                };
                let tc_diags = diagnostics::typecheck_diagnostics(&fs, &tc_err, &candidates);
                if tc_diags.is_empty() {
                    self.client
                        .log_message(MessageType::ERROR, format!("Typecheck failed: {tc_err}"))
                        .await;
                } else {
                    for (path, diags) in tc_diags {
                        new_diagnostics.entry(path).or_default().extend(diags);
                    }
                }
            }
        }

        // Open the long-lived ProjectCache SQLite connection the first time
        // the DB file is present. `try_open_project_cache` returns `None` when
        // the file doesn't exist, so it's safe to attempt this even when the
        // build failed — we still want hover/goto to work against any
        // previously-written rows. Done unconditionally because it's
        // idempotent and useful even for stale rebuilds.
        {
            let mut guard = self.project_cache.lock().await;
            if guard.is_none() {
                *guard = try_open_project_cache(
                    &root,
                    profile.as_deref().unwrap_or(""),
                    profile_suffix.as_deref(),
                    &variables,
                );
            }
        }

        // Generation guard: drop stale results if the buffer changed during
        // the rebuild. The next trigger will rebuild against the newer
        // version; do not advance `rebuilt_through`.
        if self.edit_version.load(Ordering::SeqCst) != started_against {
            return;
        }

        // Convert path-keyed map to URI-keyed map; drop entries whose paths
        // can't be expressed as `file://` URIs (relative paths, etc.).
        let new_project_diags: BTreeMap<Url, Vec<Diagnostic>> = new_diagnostics
            .into_iter()
            .filter_map(|(path, diags)| Url::from_file_path(path).ok().map(|uri| (uri, diags)))
            .collect();

        // Swap in the new map and compute the union of old ∪ new URIs to
        // republish: both old-only (so stale project diagnostics clear) and
        // new (so fresh project diagnostics appear).
        let to_republish: BTreeSet<Url> = {
            let mut guard = self.project_diagnostics.lock().await;
            let union: BTreeSet<Url> = guard
                .keys()
                .chain(new_project_diags.keys())
                .cloned()
                .collect();
            *guard = new_project_diags;
            union
        };

        for uri in to_republish {
            self.publish_merged(uri).await;
        }

        self.rebuilt_through
            .store(started_against, Ordering::SeqCst);
    }

    /// Run compiler-owned typechecking.
    ///
    /// Returns `Some(err)` on typecheck failure so the caller can convert it
    /// into LSP diagnostics, `None` on success.
    async fn run_typecheck(
        &self,
        project: Arc<graph::Project>,
        profile: &str,
        profile_suffix: Option<&str>,
        variables: &BTreeMap<String, String>,
    ) -> Option<TypeCheckError> {
        let root = self.root.read().await.clone();
        let types_lock = types::load_types_lock(&root).unwrap_or_default();
        match project::compiler::typecheck::run(
            &root,
            profile,
            profile_suffix,
            variables,
            &project,
            types_lock,
        ) {
            Ok((_, _stats)) => None,
            Err(e) => Some(e),
        }
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        if let Some(root_uri) = params.root_uri {
            if let Ok(path) = root_uri.to_file_path() {
                let mut root = self.root.write().await;
                *root = path;
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
                code_action_provider: Some(CodeActionProviderCapability::Simple(true)),
                semantic_tokens_provider: Some(
                    SemanticTokensServerCapabilities::SemanticTokensOptions(
                        SemanticTokensOptions {
                            legend: SemanticTokensLegend {
                                token_types: semantic_tokens::legend_token_types(),
                                token_modifiers: vec![],
                            },
                            full: Some(SemanticTokensFullOptions::Bool(true)),
                            ..Default::default()
                        },
                    ),
                ),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.edit_version.fetch_add(1, Ordering::SeqCst);
        self.maybe_rebuild().await;
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
            self.schedule_rebuild_after_idle();
        }
    }

    async fn did_close(&self, params: DidCloseTextDocumentParams) {
        let uri = params.text_document.uri;
        self.documents.lock().await.remove(&uri);
        self.parse_diagnostics.lock().await.remove(&uri);
        self.publish_merged(uri).await;
        self.edit_version.fetch_add(1, Ordering::SeqCst);
        self.maybe_rebuild().await;
    }

    async fn did_save(&self, _params: DidSaveTextDocumentParams) {
        self.edit_version.fetch_add(1, Ordering::SeqCst);
        self.maybe_rebuild().await;
    }

    async fn did_change_watched_files(&self, _params: DidChangeWatchedFilesParams) {
        self.edit_version.fetch_add(1, Ordering::SeqCst);
        self.maybe_rebuild().await;
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let uri = params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let (_, _, parts) = match self.snapshot_at_position(&uri, position).await {
            Some(s) => s,
            None => return Ok(None),
        };
        let parts = match parts {
            Some(p) => p,
            None => return Ok(None),
        };

        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let location = goto_definition::resolve_reference(&parts, &uri, &root, cache);
        Ok(location.map(GotoDefinitionResponse::Scalar))
    }

    async fn references(&self, params: ReferenceParams) -> Result<Option<Vec<Location>>> {
        let uri = params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;

        let (_, _, parts) = match self.snapshot_at_position(&uri, position).await {
            Some(s) => s,
            None => return Ok(None),
        };
        let parts = match parts {
            Some(p) => p,
            None => return Ok(None),
        };

        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let locations = references::find_references(
            &parts,
            &uri,
            &root,
            cache,
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
        let root = self.root.read().await.clone();

        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let symbols = document_symbol::document_symbols(&file_uri, &root, cache);
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
        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let symbols = workspace_symbol::workspace_symbols(&params.query, cache, &root);
        if symbols.is_empty() {
            Ok(None)
        } else {
            Ok(Some(symbols))
        }
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let (text, byte_offset, parts) = match self.snapshot_at_position(&uri, position).await {
            Some(s) => s,
            None => return Ok(None),
        };

        // Try variable hover first (pure).
        let variables = self.variables.read().await;
        let profile = self.profile_name.read().await;
        if let Some(h) = hover::resolve_variable_hover(&text, byte_offset, &variables) {
            return Ok(Some(h));
        }
        drop(variables);
        drop(profile);

        // Then object hover (pure).
        let parts = match parts {
            Some(p) => p,
            None => return Ok(None),
        };

        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let types_lock = types::load_types_lock(&root).unwrap_or_default();

        Ok(hover::resolve_hover(
            &parts,
            &uri,
            &root,
            cache,
            &types_lock,
        ))
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let file_uri = params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;
        let root = self.root.read().await.clone();

        let doc_text = {
            let docs = self.documents.lock().await;
            docs.get(&file_uri).map(|rope| rope.to_string())
        };
        let text = doc_text.as_deref().unwrap_or("");
        let prefix = completion::prefix_context(text, position);

        let cache_guard = self.project_cache.lock().await;
        let types_lock = types::load_types_lock(&root).unwrap_or_default();
        let items =
            completion::complete(cache_guard.as_ref(), &types_lock, &file_uri, &root, &prefix);

        Ok(Some(CompletionResponse::Array(items)))
    }

    async fn code_lens(&self, params: CodeLensParams) -> Result<Option<Vec<CodeLens>>> {
        let file_uri = params.text_document.uri;
        let root = self.root.read().await.clone();

        let doc_text = {
            let docs = self.documents.lock().await;
            docs.get(&file_uri).map(|rope| rope.to_string())
        };
        let text = match doc_text.as_deref() {
            Some(t) => t,
            None => return Ok(None),
        };

        let cache_guard = self.project_cache.lock().await;
        let lenses = code_lens::code_lenses(&file_uri, text, &root, cache_guard.as_ref());
        Ok(Some(lenses))
    }

    async fn code_action(&self, params: CodeActionParams) -> Result<Option<CodeActionResponse>> {
        let actions = code_action::build_code_actions(&params);
        if actions.is_empty() {
            Ok(None)
        } else {
            Ok(Some(actions))
        }
    }

    async fn semantic_tokens_full(
        &self,
        params: SemanticTokensParams,
    ) -> Result<Option<SemanticTokensResult>> {
        let file_uri = params.text_document.uri;

        let doc_text = {
            let docs = self.documents.lock().await;
            docs.get(&file_uri).map(|rope| rope.to_string())
        };
        let text = match doc_text.as_deref() {
            Some(t) => t,
            None => return Ok(None),
        };

        let data = semantic_tokens::compute_semantic_tokens(text);
        Ok(Some(SemanticTokensResult::Tokens(SemanticTokens {
            result_id: None,
            data,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    fn capture_client_with_root(root: PathBuf) -> (Client, tower_lsp::LspService<Backend>) {
        let captured_client: Arc<StdMutex<Option<Client>>> = Arc::new(StdMutex::new(None));
        let captured_client_clone = Arc::clone(&captured_client);
        let (service, _socket) = tower_lsp::LspService::new(move |client| {
            *captured_client_clone.lock().unwrap() = Some(client.clone());
            Backend::new_with_root(client, root.clone())
        });
        let client = captured_client.lock().unwrap().take().unwrap();
        (client, service)
    }

    fn write_project_toml(root: &Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }

    #[mz_ore::test]
    fn try_open_project_cache_returns_none_for_missing_db() {
        let result = try_open_project_cache(
            Path::new("/nonexistent/path"),
            "default",
            None,
            &BTreeMap::new(),
        );
        assert!(result.is_none());
    }

    /// Regression test for the tokio::sync lock conversion.
    ///
    /// `publish_diagnostics` acquires `documents.lock()` and then holds the
    /// lock across an `.await` on `client.publish_diagnostics(...)`. With the
    /// previous `std::sync::Mutex` a second concurrent call from another task
    /// would deadlock, because the std guard is not `Send` and blocks the
    /// worker thread. With `tokio::sync::Mutex` the second task yields
    /// correctly and both calls complete.
    ///
    /// The multi-thread runtime with 2 workers is required so the two spawned
    /// tasks can actually make progress concurrently.
    #[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn concurrent_publish_diagnostics_do_not_deadlock() {
        let (client, _service) = capture_client_with_root(std::env::temp_dir());

        let backend = Arc::new(Backend::new_with_root(client, std::env::temp_dir()));

        let b1 = Arc::clone(&backend);
        let b2 = Arc::clone(&backend);
        let t1 = mz_ore::task::spawn(|| "lsp-test-publish-a", async move {
            b1.publish_diagnostics(
                Url::from_file_path(std::env::temp_dir().join("a.sql")).unwrap(),
                "SELECT 1;",
            )
            .await;
        });
        let t2 = mz_ore::task::spawn(|| "lsp-test-publish-b", async move {
            b2.publish_diagnostics(
                Url::from_file_path(std::env::temp_dir().join("b.sql")).unwrap(),
                "SELECT 2;",
            )
            .await;
        });
        let result = tokio::time::timeout(Duration::from_secs(2), async {
            let _ = tokio::join!(t1, t2);
        })
        .await;
        assert!(result.is_ok(), "concurrent publish_diagnostics timed out");
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn code_action_returns_quickfix_for_unknown_column_diagnostic() {
        use crate::lsp::code_action::QuickFixData;
        use tower_lsp::lsp_types::{
            CodeActionContext, CodeActionKind, CodeActionOrCommand, CodeActionParams, Diagnostic,
            DiagnosticSeverity, PartialResultParams, Position, Range, TextDocumentIdentifier,
            WorkDoneProgressParams,
        };

        let (client, _service) = capture_client_with_root(std::env::temp_dir());
        let backend = Backend::new_with_root(client, std::env::temp_dir());

        let uri = Url::from_file_path(std::env::temp_dir().join("qf.sql")).unwrap();
        let qf = QuickFixData {
            suggestions: vec![code_action::SuggestionData {
                label: "did you mean `customer_name`?".to_string(),
                alternatives: vec![code_action::ReplacementData {
                    range: Range::new(Position::new(0, 7), Position::new(0, 20)),
                    new_text: "customer_name".to_string(),
                }],
            }],
        };
        let diag = Diagnostic {
            range: Range::new(Position::new(0, 7), Position::new(0, 20)),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            message: "column custoser_name does not exist".to_string(),
            data: Some(serde_json::to_value(qf).unwrap()),
            ..Default::default()
        };

        let params = CodeActionParams {
            text_document: TextDocumentIdentifier { uri: uri.clone() },
            range: diag.range,
            context: CodeActionContext {
                diagnostics: vec![diag],
                only: None,
                trigger_kind: None,
            },
            work_done_progress_params: WorkDoneProgressParams::default(),
            partial_result_params: PartialResultParams::default(),
        };

        let response = backend.code_action(params).await.unwrap();
        let actions = response.expect("code_action should return Some");
        assert_eq!(actions.len(), 1);
        let CodeActionOrCommand::CodeAction(ca) = &actions[0] else {
            panic!("expected CodeAction");
        };
        assert_eq!(ca.kind.as_ref(), Some(&CodeActionKind::QUICKFIX));
        assert_eq!(ca.is_preferred, Some(true));
        let edits = ca
            .edit
            .as_ref()
            .unwrap()
            .changes
            .as_ref()
            .unwrap()
            .get(&uri)
            .unwrap();
        assert_eq!(edits[0].new_text, "customer_name");
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
    async fn did_close_rebuilds_immediately_against_disk_state() {
        let root = tempfile::tempdir().unwrap();
        let models = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        write_project_toml(root.path());

        let (client, _service) = capture_client_with_root(root.path().to_path_buf());
        let backend = Backend::new_with_root(client, root.path().to_path_buf());
        let uri = Url::from_file_path(models.join("foo.sql")).unwrap();

        backend
            .publish_diagnostics(uri.clone(), "CREATE VIEW foo AS SELECT * FROM missing;")
            .await;
        backend.edit_version.fetch_add(1, Ordering::SeqCst);
        backend.maybe_rebuild().await;

        assert!(
            backend.project_diagnostics.lock().await.contains_key(&uri),
            "unsaved overlay should have produced project diagnostics"
        );

        backend
            .did_close(DidCloseTextDocumentParams {
                text_document: TextDocumentIdentifier { uri: uri.clone() },
            })
            .await;

        assert!(!backend.documents.lock().await.contains_key(&uri));
        assert!(!backend.parse_diagnostics.lock().await.contains_key(&uri));
        assert!(!backend.project_diagnostics.lock().await.contains_key(&uri));
        assert_eq!(
            backend.edit_version.load(Ordering::SeqCst),
            backend.rebuilt_through.load(Ordering::SeqCst),
            "close should rebuild immediately instead of leaving stale overlay state behind"
        );
    }
}
