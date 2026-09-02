// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Context-aware completion for the LSP server.
//!
//! Completions are produced by a 3-phase pipeline:
//!
//! ```text
//! Phase 1: RESOLVE CONTEXT    → CompletionContext
//! Phase 2: GATHER CANDIDATES  → Vec<CompletionCandidate>
//! Phase 3: FORMAT ITEMS       → Vec<CompletionItem>
//! ```
//!
//! ## Phase 1: Resolve Context ([`resolve_context`])
//!
//! Builds a [`CompletionContext`] from the file URI, project cache, and prefix.
//! Determines the default database/schema from the file path, and resolves
//! the current file's dependencies and alias map (for column completions).
//! All downstream logic operates on this resolved context — no further URI
//! parsing or project lookups needed.
//!
//! ## Phase 2: Gather Candidates
//!
//! Four independent gatherers produce [`CompletionCandidate`]s:
//!
//! ### Functions ([`gather_functions`])
//!
//! Sourced from [`super::functions::FUNCTIONS`] (built from `mz_sql::func`
//! registries). Only offered when `dots == 0`. Label is the function name,
//! detail is the first overload's signature.
//! Kind: `FUNCTION`. Sort: `4_`.
//!
//! ### Keywords ([`gather_keywords`])
//!
//! Static list from [`mz_sql_lexer::keywords::KEYWORDS`]. Only offered when
//! `dots == 0`. Label is the uppercase keyword. Kind: `KEYWORD`.
//!
//! ### Object names ([`gather_objects`])
//!
//! Dynamic per-request from project objects and external dependencies.
//!
//! - **`dots == 0`:** all objects with minimum qualification — bare if
//!   same-schema, `schema.object` if cross-schema, `db.schema.object` if
//!   cross-database. No filtering; the editor handles fuzzy matching.
//! - **`dots >= 1`:** filtered by prefix match with disambiguation. Each
//!   object is matched against candidates `schema.object` then
//!   `db.schema.object`. First case-insensitive prefix match wins. Label is
//!   the remainder after the last dot in the prefix.
//!
//! Sort: `1_` same-schema, `2_` cross-schema, `3_` cross-database.
//!
//! ### Column names ([`gather_columns`])
//!
//! Dynamic per-request from the types cache. **Only offered for objects that
//! are dependencies of the current file's object.**
//!
//! - **Unqualified** (`dots == 0`, [`gather_unqualified_columns`]): columns
//!   from all dependencies, filtered by prefix.
//! - **Qualified** (`dots >= 1`, [`gather_qualified_columns`]): resolves the
//!   object prefix to an [`ObjectId`] (with alias map support), checks it is
//!   a dependency, and returns that object's columns.
//!
//! Sort: `0_` (before object names).
//!
//! #### Alias Resolution
//!
//! When a qualified column prefix has a 1-part object (e.g., `o.col`), the
//! alias map is checked before falling back to default db/schema resolution.
//! Aliases are extracted from `FROM` clauses in views/materialized views.
//!
//! ## Phase 3: Format Items ([`format_candidate`])
//!
//! Converts each [`CompletionCandidate`] into an LSP [`CompletionItem`] with
//! appropriate kind, detail, and sort text.

use crate::project::compiler::cache::ProjectCache;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, ObjectKind, Types};
use mz_sql_lexer::keywords::KEYWORDS;
use ropey::Rope;
use std::collections::BTreeMap;
use std::path::Path;
use tower_lsp::lsp_types::{CompletionItem, CompletionItemKind, Position, Url};

/// Describes the dot-qualified prefix at the cursor position.
pub(super) struct PrefixContext<'a> {
    /// Number of dots in the typed prefix (0, 1, or 2+).
    pub dots: usize,
    /// The raw prefix text the user has typed (e.g., `"public.f"`).
    pub text: &'a str,
}

/// Everything the completion engine needs about the cursor position and file.
///
/// Built once by [`resolve_context`] and consumed by all candidate-gathering
/// functions. No further URI parsing or project lookups are needed downstream.
struct CompletionContext<'a> {
    /// The default database derived from the file path.
    default_db: String,
    /// The default schema derived from the file path.
    default_schema: String,
    /// The parsed prefix at the cursor.
    prefix: &'a PrefixContext<'a>,
    /// The current file's dependencies and alias map (None if file not in project).
    file_object: Option<FileObject>,
}

/// The current file's resolved context for column completions.
struct FileObject {
    /// Objects this file depends on.
    dependencies: Vec<ObjectId>,
    /// Alias/bare-table-name → target FQN map from the SQL AST.
    alias_map: BTreeMap<String, String>,
}

/// Build a [`CompletionContext`] from the file URI, project cache, and prefix.
///
/// Returns `None` if the file is not under `models/<database>/<schema>/`
/// (i.e., the default database/schema cannot be determined from the path).
fn resolve_context<'a>(
    file_uri: &Url,
    root: &Path,
    project_cache: &ProjectCache,
    prefix: &'a PrefixContext<'a>,
) -> Option<CompletionContext<'a>> {
    let (default_db, default_schema) = ObjectId::default_db_schema_from_uri(file_uri, root)?;

    // Try to resolve the current file's object for column completions.
    let file_object = file_uri
        .to_file_path()
        .ok()
        .and_then(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .and_then(|object_name| {
            let file_object_id =
                ObjectId::new(default_db.clone(), default_schema.clone(), object_name);
            project_cache
                .get_object(&file_object_id)
                .map(|obj| FileObject {
                    dependencies: project_cache.get_dependencies(&file_object_id),
                    alias_map: obj.aliases.clone(),
                })
        });

    Some(CompletionContext {
        default_db,
        default_schema,
        prefix,
        file_object,
    })
}

/// Find the dot-qualified identifier prefix at the cursor position.
///
/// Scans backward from `position` through identifier characters (alphanumeric,
/// underscore) and dots to determine what the user has typed so far.
pub(super) fn prefix_context(text: &str, position: Position) -> PrefixContext<'_> {
    let rope = Rope::from_str(text);
    let byte_offset = crate::lsp::diagnostics::position_to_offset(position, &rope)
        .unwrap_or(text.len())
        .min(text.len());

    let prefix_bytes = &text.as_bytes()[..byte_offset];
    let mut start = prefix_bytes.len();
    while start > 0 {
        let ch = char::from(prefix_bytes[start - 1]);
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            start -= 1;
        } else {
            break;
        }
    }

    let prefix = &text[start..byte_offset];
    let dots = prefix.chars().filter(|&c| c == '.').count();

    PrefixContext { dots, text: prefix }
}

/// A completion candidate before final formatting.
///
/// Intermediate representation produced by the gather phase. Captures what to
/// complete and how to sort it, without LSP-specific formatting concerns.
enum CompletionCandidate<'a> {
    Keyword {
        label: String,
    },
    Object {
        label: String,
        sort_key: String,
        kind: ObjectKind,
        is_external: bool,
    },
    Column {
        name: String,
        col_type: ColumnType,
    },
    Function {
        info: &'a super::functions::FunctionInfo,
    },
}

/// Gather keyword candidates. Only offered when `dots == 0`.
fn gather_keywords(ctx: &CompletionContext<'_>) -> Vec<CompletionCandidate<'static>> {
    if ctx.prefix.dots > 0 {
        return Vec::new();
    }
    KEYWORDS
        .entries()
        .map(|(_, kw)| CompletionCandidate::Keyword {
            label: kw.as_str().to_string(),
        })
        .collect()
}

/// Gather function candidates from the static function registry.
/// Only offered when `dots == 0` (unqualified context).
fn gather_functions(prefix: &PrefixContext<'_>) -> Vec<CompletionCandidate<'static>> {
    if prefix.dots > 0 {
        return Vec::new();
    }
    super::functions::search_prefix(prefix.text)
        .map(|info| CompletionCandidate::Function { info })
        .collect()
}

/// Gather object candidates from project objects and external dependencies.
fn gather_objects<'a>(
    ctx: &CompletionContext<'a>,
    project_cache: &ProjectCache,
    types_lock: &Types,
) -> Vec<CompletionCandidate<'a>> {
    let mut candidates = Vec::new();

    for summary in project_cache.list_objects() {
        let id = ObjectId::new(
            summary.database.clone(),
            summary.schema.clone(),
            summary.name.clone(),
        );
        if let Some((label, sort_key)) =
            qualify_and_filter(&id, &ctx.default_db, &ctx.default_schema, ctx.prefix)
        {
            candidates.push(CompletionCandidate::Object {
                label,
                sort_key,
                kind: summary.kind,
                is_external: false,
            });
        }
    }

    for id in project_cache.list_external_dependencies() {
        let kind = project_cache
            .get_kind(&id)
            .or_else(|| types_lock.kinds.get(&id).copied())
            .unwrap_or(ObjectKind::Table);
        if let Some((label, sort_key)) =
            qualify_and_filter(&id, &ctx.default_db, &ctx.default_schema, ctx.prefix)
        {
            candidates.push(CompletionCandidate::Object {
                label,
                sort_key,
                kind,
                is_external: true,
            });
        }
    }

    candidates
}

/// Gather column candidates from dependency objects via the types cache.
fn gather_columns<'a>(
    ctx: &CompletionContext<'a>,
    project_cache: Option<&ProjectCache>,
    types_lock: &Types,
) -> Vec<CompletionCandidate<'a>> {
    let file_obj = match &ctx.file_object {
        Some(fo) => fo,
        None => return Vec::new(),
    };

    if ctx.prefix.dots == 0 {
        gather_unqualified_columns(
            &file_obj.dependencies,
            project_cache,
            types_lock,
            ctx.prefix.text,
        )
    } else {
        gather_qualified_columns(
            ctx.prefix.text,
            file_obj,
            project_cache,
            types_lock,
            &ctx.default_db,
            &ctx.default_schema,
        )
    }
}

/// Gather columns from all dependencies, filtered by prefix (case-insensitive).
fn gather_unqualified_columns<'a>(
    dependencies: &[ObjectId],
    project_cache: Option<&ProjectCache>,
    types_lock: &Types,
    filter_text: &str,
) -> Vec<CompletionCandidate<'a>> {
    let filter = filter_text.to_lowercase();
    let mut candidates = Vec::new();
    for id in dependencies {
        let columns = project_cache
            .and_then(|tc| tc.get_columns(id))
            .or_else(|| types_lock.get_table(id).cloned());
        if let Some(columns) = columns {
            for (col_name, col_type) in columns {
                if filter.is_empty() || col_name.to_lowercase().starts_with(&filter) {
                    candidates.push(CompletionCandidate::Column {
                        name: col_name.clone(),
                        col_type: col_type.clone(),
                    });
                }
            }
        }
    }
    candidates
}

/// Gather columns from a specific qualified object reference.
///
/// Splits the prefix at the last `.` into `(object_text, col_filter)`,
/// resolves `object_text` to an [`ObjectId`] via [`resolve_qualified_object`],
/// checks it is a dependency, and returns its columns filtered by `col_filter`.
fn gather_qualified_columns<'a>(
    prefix_text: &str,
    file_object: &FileObject,
    project_cache: Option<&ProjectCache>,
    types_lock: &Types,
    default_db: &str,
    default_schema: &str,
) -> Vec<CompletionCandidate<'a>> {
    let last_dot = match prefix_text.rfind('.') {
        Some(pos) => pos,
        None => return Vec::new(),
    };
    let object_text = &prefix_text[..last_dot];
    let col_filter = prefix_text[last_dot + 1..].to_lowercase();

    let object_id = match resolve_qualified_object(
        object_text,
        &file_object.alias_map,
        default_db,
        default_schema,
    ) {
        Some(id) => id,
        None => return Vec::new(),
    };

    if !file_object.dependencies.contains(&object_id) {
        return Vec::new();
    }

    let columns = project_cache
        .and_then(|tc| tc.get_columns(&object_id))
        .or_else(|| types_lock.get_table(&object_id).cloned());

    match columns {
        Some(columns) => columns
            .into_iter()
            .filter(|(name, _)| {
                col_filter.is_empty() || name.to_lowercase().starts_with(&col_filter)
            })
            .map(|(name, col_type)| CompletionCandidate::Column { name, col_type })
            .collect(),
        None => Vec::new(),
    }
}

/// Resolve a dot-qualified object prefix to an [`ObjectId`].
///
/// - 1 part: alias map lookup (case-insensitive), then fallback to
///   `default_db.default_schema.name`
/// - 2 parts: `default_db.part0.part1`
/// - 3 parts: `part0.part1.part2`
/// - 4+ parts: `None`
fn resolve_qualified_object(
    object_text: &str,
    alias_map: &BTreeMap<String, String>,
    default_db: &str,
    default_schema: &str,
) -> Option<ObjectId> {
    let parts: Vec<&str> = object_text.split('.').collect();
    match parts.len() {
        1 => {
            if let Some(fqn) = alias_map.get(&parts[0].to_lowercase()) {
                fqn.parse::<ObjectId>().ok()
            } else {
                Some(ObjectId::new(
                    default_db.to_string(),
                    default_schema.to_string(),
                    parts[0].to_string(),
                ))
            }
        }
        2 => Some(ObjectId::new(
            default_db.to_string(),
            parts[0].to_string(),
            parts[1].to_string(),
        )),
        3 => Some(ObjectId::new(
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
        )),
        _ => None,
    }
}

/// Convert a [`CompletionCandidate`] into an LSP [`CompletionItem`].
fn format_candidate(candidate: &CompletionCandidate<'_>) -> CompletionItem {
    match candidate {
        CompletionCandidate::Keyword { label } => CompletionItem {
            label: label.clone(),
            kind: Some(CompletionItemKind::KEYWORD),
            ..Default::default()
        },
        CompletionCandidate::Object {
            label,
            sort_key,
            kind,
            is_external,
        } => CompletionItem {
            label: label.clone(),
            kind: Some(object_kind_to_completion_kind(*kind)),
            detail: Some(if *is_external {
                format!("{} (external)", kind)
            } else {
                kind.to_string()
            }),
            sort_text: Some(sort_key.clone()),
            ..Default::default()
        },
        CompletionCandidate::Column { name, col_type } => CompletionItem {
            label: name.to_string(),
            kind: Some(CompletionItemKind::FIELD),
            detail: Some(format_column_detail(col_type)),
            sort_text: Some(format!("0_{}", name)),
            ..Default::default()
        },
        CompletionCandidate::Function { info } => CompletionItem {
            label: info.name.to_string(),
            kind: Some(CompletionItemKind::FUNCTION),
            detail: info.signatures.first().cloned(),
            sort_text: Some(format!("4_{}", info.name)),
            ..Default::default()
        },
    }
}

/// Run the 3-phase completion pipeline.
///
/// 1. **Resolve context** — determine default db/schema, file dependencies,
///    and alias map from the file URI and project cache.
/// 2. **Gather candidates** — collect keywords, objects, and columns that
///    match the prefix.
/// 3. **Format items** — convert candidates to LSP completion items.
///
/// When `project_cache` is `None` (no successful build yet), only keyword
/// completions are returned. Keywords are included only when `dots == 0`
/// (the module decides this, not the caller).
pub(super) fn complete(
    project_cache: Option<&ProjectCache>,
    types_lock: &Types,
    file_uri: &Url,
    root: &Path,
    prefix: &PrefixContext<'_>,
) -> Vec<CompletionItem> {
    let ctx = project_cache.and_then(|pc| resolve_context(file_uri, root, pc, prefix));

    let mut candidates: Vec<CompletionCandidate<'_>> = Vec::new();
    // Functions are always available (static registry, no project needed)
    candidates.extend(gather_functions(prefix));
    if let Some(ctx) = &ctx {
        candidates.extend(gather_keywords(ctx));
        candidates.extend(gather_objects(ctx, project_cache.unwrap(), types_lock));
        candidates.extend(gather_columns(ctx, project_cache, types_lock));
    } else if prefix.dots == 0 {
        candidates.extend(
            KEYWORDS
                .entries()
                .map(|(_, kw)| CompletionCandidate::Keyword {
                    label: kw.as_str().to_string(),
                }),
        );
    }

    candidates.iter().map(format_candidate).collect()
}

/// Compute the label and sort prefix for an object, filtered by the typed prefix.
///
/// For `dots == 0`, returns minimum qualification (bare name if same-schema,
/// `schema.object` if cross-schema, `db.schema.object` if cross-database).
///
/// For `dots >= 1`, tries matching against `schema.object` and
/// `db.schema.object` candidates. Returns `None` if neither matches.
pub(crate) fn qualify_and_filter(
    id: &ObjectId,
    default_db: &str,
    default_schema: &str,
    prefix: &PrefixContext<'_>,
) -> Option<(String, String)> {
    let in_default_db = id.database() == Some(default_db);
    let sort_key = if in_default_db && id.schema() == default_schema {
        "1"
    } else if in_default_db {
        "2"
    } else {
        "3"
    };

    if prefix.dots == 0 {
        let label = if in_default_db && id.schema() == default_schema {
            id.object().to_string()
        } else if in_default_db || id.database().is_none() {
            format!("{}.{}", id.schema(), id.object())
        } else {
            id.to_string()
        };
        return Some((label.clone(), format!("{}_{}", sort_key, label)));
    }

    let candidates = [format!("{}.{}", id.schema(), id.object()), id.to_string()];

    let prefix_lower = prefix.text.to_lowercase();
    for candidate in &candidates {
        if candidate.to_lowercase().starts_with(&prefix_lower) {
            let last_dot = prefix.text.rfind('.').expect("dots >= 1 guarantees a dot");
            let label = candidate[last_dot + 1..].to_string();
            return Some((label, format!("{}_{}", sort_key, candidate)));
        }
    }

    None
}

/// Map an [`ObjectKind`] to the corresponding LSP [`CompletionItemKind`].
fn object_kind_to_completion_kind(kind: ObjectKind) -> CompletionItemKind {
    match kind {
        ObjectKind::Table | ObjectKind::View | ObjectKind::MaterializedView => {
            CompletionItemKind::STRUCT
        }
        ObjectKind::Source | ObjectKind::Sink => CompletionItemKind::EVENT,
        ObjectKind::Secret => CompletionItemKind::CONSTANT,
        ObjectKind::Connection => CompletionItemKind::INTERFACE,
    }
}

/// Format a column type for the completion item detail field.
fn format_column_detail(col_type: &ColumnType) -> String {
    if col_type.nullable {
        format!("{} (nullable)", col_type.r#type)
    } else {
        col_type.r#type.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    /// An empty prefix with no dots — the default for tests that don't care
    /// about prefix context.
    fn no_prefix() -> PrefixContext<'static> {
        PrefixContext { dots: 0, text: "" }
    }

    fn write_project_toml(root: &Path) {
        std::fs::write(root.join("project.toml"), "[project]\nname = \"test\"\n").unwrap();
    }

    fn build_cache(root: &tempfile::TempDir) -> ProjectCache {
        write_project_toml(root.path());
        let _project = crate::project::plan_sync(
            &crate::fs::FileSystem::new(),
            root.path(),
            None,
            None,
            &Default::default(),
        )
        .expect("project should compile");
        ProjectCache::open(root.path(), "", None, &Default::default())
            .expect("cache should open")
            .expect("cache DB should exist")
    }

    /// Test helper: run only the object-gathering phase and format results.
    fn object_completions(
        cache: &ProjectCache,
        types_lock: Option<&Types>,
        file_uri: &Url,
        root: &Path,
        prefix: &PrefixContext<'_>,
    ) -> Vec<CompletionItem> {
        let empty = Types::default();
        let types_lock = types_lock.unwrap_or(&empty);
        let ctx = match resolve_context(file_uri, root, cache, prefix) {
            Some(ctx) => ctx,
            None => return Vec::new(),
        };
        gather_objects(&ctx, cache, types_lock)
            .iter()
            .map(format_candidate)
            .collect()
    }

    /// Test helper: run only the column-gathering phase and format results.
    fn column_completions(
        cache: &ProjectCache,
        types_lock: Option<&Types>,
        file_uri: &Url,
        root: &Path,
        prefix: &PrefixContext<'_>,
    ) -> Vec<CompletionItem> {
        let empty = Types::default();
        let types_lock = types_lock.unwrap_or(&empty);
        let ctx = match resolve_context(file_uri, root, cache, prefix) {
            Some(ctx) => ctx,
            None => return Vec::new(),
        };
        gather_columns(&ctx, Some(cache), types_lock)
            .iter()
            .map(format_candidate)
            .collect()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn prefix_no_prefix() {
        let text = "SELECT ";
        let ctx = prefix_context(text, Position::new(0, 7));
        assert_eq!(ctx.dots, 0);
        assert_eq!(ctx.text, "");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn prefix_bare_ident() {
        let text = "SELECT foo";
        let ctx = prefix_context(text, Position::new(0, 10));
        assert_eq!(ctx.dots, 0);
        assert_eq!(ctx.text, "foo");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn prefix_one_dot() {
        let text = "SELECT schema.foo";
        let ctx = prefix_context(text, Position::new(0, 17));
        assert_eq!(ctx.dots, 1);
        assert_eq!(ctx.text, "schema.foo");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn prefix_two_dots() {
        let text = "SELECT db.schema.foo";
        let ctx = prefix_context(text, Position::new(0, 20));
        assert_eq!(ctx.dots, 2);
        assert_eq!(ctx.text, "db.schema.foo");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn prefix_mid_line() {
        let text = "SELECT * FROM schema.f";
        let ctx = prefix_context(text, Position::new(0, 22));
        assert_eq!(ctx.dots, 1);
        assert_eq!(ctx.text, "schema.f");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `llvm.aarch64.neon.uaddlv.i32.v16i8` on OS `linux`
    fn prefix_text_stored() {
        let text = "SELECT public.f";
        let ctx = prefix_context(text, Position::new(0, 15));
        assert_eq!(ctx.dots, 1);
        assert_eq!(ctx.text, "public.f");
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn same_schema_bare_name() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        std::fs::write(dir.join("bar.sql"), "CREATE VIEW bar AS SELECT * FROM foo;").unwrap();
        let cache = build_cache(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();
        let items = object_completions(&cache, None, &uri, root.path(), &no_prefix());

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"foo"),
            "expected bare 'foo', got: {:?}",
            labels
        );
        assert!(
            labels.contains(&"bar"),
            "expected bare 'bar', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn cross_schema_qualified() {
        let root = tempfile::tempdir().unwrap();
        let public = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&public).unwrap();
        std::fs::write(public.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let other = root.path().join("models/mydb/other");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(
            other.join("baz.sql"),
            "CREATE VIEW baz AS SELECT * FROM mydb.public.foo;",
        )
        .unwrap();
        let cache = build_cache(&root);

        // URI is in "other" schema, so "foo" from "public" should be schema-qualified.
        let uri = Url::from_file_path(root.path().join("models/mydb/other/baz.sql")).unwrap();
        let items = object_completions(&cache, None, &uri, root.path(), &no_prefix());

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"public.foo"),
            "expected 'public.foo', got: {:?}",
            labels
        );
        // "baz" is same-schema, so bare.
        assert!(
            labels.contains(&"baz"),
            "expected bare 'baz', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn cross_database_fully_qualified() {
        let root = tempfile::tempdir().unwrap();
        let db1 = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&db1).unwrap();
        std::fs::write(db1.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();

        let db2 = root.path().join("models/otherdb/public");
        std::fs::create_dir_all(&db2).unwrap();
        std::fs::write(db2.join("bar.sql"), "CREATE VIEW bar AS SELECT 1 AS id;").unwrap();
        let cache = build_cache(&root);

        // URI is in otherdb, so "foo" from mydb should be fully qualified.
        let uri = Url::from_file_path(root.path().join("models/otherdb/public/bar.sql")).unwrap();
        let items = object_completions(&cache, None, &uri, root.path(), &no_prefix());

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"mydb.public.foo"),
            "expected 'mydb.public.foo', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn external_deps_included() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("foo.sql"),
            "CREATE VIEW foo AS SELECT * FROM mydb.ext.src;",
        )
        .unwrap();

        // Write a types.lock (TOML) that declares the external dep.
        std::fs::write(
            root.path().join("types.lock"),
            "version = 1\n\n\
             [[source]]\n\
             name = \"mydb.ext.src\"\n\
             \n\
             [[source.columns]]\n\
             name = \"id\"\n\
             type = \"integer\"\n\
             nullable = false\n",
        )
        .unwrap();
        let cache = build_cache(&root);

        let types_cache = crate::types::load_types_lock(root.path()).unwrap();
        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let items = object_completions(&cache, Some(&types_cache), &uri, root.path(), &no_prefix());

        let ext_items: Vec<_> = items
            .iter()
            .filter(|i| i.detail.as_deref() == Some("source (external)"))
            .collect();
        assert_eq!(ext_items.len(), 1, "expected one external source");
        assert_eq!(ext_items[0].label, "ext.src");
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn kind_mapping() {
        let root = tempfile::tempdir().unwrap();
        // Storage and computation objects must be in separate schemas.
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("t.sql"), "CREATE TABLE t (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.t;",
        )
        .unwrap();
        let cache = build_cache(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/storage/t.sql")).unwrap();
        let items = object_completions(&cache, None, &uri, root.path(), &no_prefix());

        let table_item = items.iter().find(|i| i.label == "t").unwrap();
        assert_eq!(table_item.detail.as_deref(), Some("table"));
        assert_eq!(table_item.kind, Some(CompletionItemKind::STRUCT));

        let view_item = items.iter().find(|i| i.label.ends_with("v")).unwrap();
        assert_eq!(view_item.detail.as_deref(), Some("view"));
        assert_eq!(view_item.kind, Some(CompletionItemKind::STRUCT));
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn file_outside_models_returns_empty() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        let cache = build_cache(&root);

        // URI is outside models/
        let uri = Url::from_file_path(root.path().join("random/file.sql")).unwrap();
        let items = object_completions(&cache, None, &uri, root.path(), &no_prefix());
        assert!(items.is_empty());
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn schema_prefix_strips_label_to_bare_name() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        std::fs::write(dir.join("bar.sql"), "CREATE VIEW bar AS SELECT * FROM foo;").unwrap();
        let cache = build_cache(&root);

        let prefix = PrefixContext {
            dots: 1,
            text: "public.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/public/bar.sql")).unwrap();
        let items = object_completions(&cache, None, &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        // Labels should be bare names since "public." prefix is stripped.
        assert!(
            labels.contains(&"foo"),
            "expected bare 'foo', got: {:?}",
            labels
        );
        assert!(
            labels.contains(&"bar"),
            "expected bare 'bar', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn db_prefix_disambiguates_to_schema_dot_object() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        let cache = build_cache(&root);

        // User typed "mydb." — a database prefix, not a schema prefix.
        let prefix = PrefixContext {
            dots: 1,
            text: "mydb.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let items = object_completions(&cache, None, &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        // Label should be "public.foo" — the remainder after "mydb.".
        assert!(
            labels.contains(&"public.foo"),
            "expected 'public.foo', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn full_qualification_strips_to_bare_name() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("foo.sql"), "CREATE VIEW foo AS SELECT 1 AS id;").unwrap();
        let cache = build_cache(&root);

        let prefix = PrefixContext {
            dots: 2,
            text: "mydb.public.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/public/foo.sql")).unwrap();
        let items = object_completions(&cache, None, &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"foo"),
            "expected bare 'foo', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn prefix_filters_non_matching_objects() {
        let root = tempfile::tempdir().unwrap();
        let public = root.path().join("models/mydb/public");
        std::fs::create_dir_all(&public).unwrap();
        std::fs::write(public.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let other = root.path().join("models/mydb/other");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(
            other.join("baz.sql"),
            "CREATE VIEW baz AS SELECT * FROM mydb.public.foo;",
        )
        .unwrap();
        let cache = build_cache(&root);

        // Prefix "other." should only match objects in the "other" schema.
        let prefix = PrefixContext {
            dots: 1,
            text: "other.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/other/baz.sql")).unwrap();
        let items = object_completions(&cache, None, &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"baz"), "expected 'baz', got: {:?}", labels);
        // "foo" is in "public" schema — should be filtered out.
        assert!(
            !labels.iter().any(|l| l.contains("foo")),
            "expected no 'foo' items, got: {:?}",
            labels
        );
    }

    /// Helper: write a types.lock with the given tables and columns.
    fn write_types_lock(root: &Path, tables: &[(&str, &str, &str, &str, &[(&str, &str, bool)])]) {
        let mut toml = String::from("version = 1\n\n");
        for (db, schema, name, kind, columns) in tables {
            toml.push_str(&format!(
                "[[{}]]\nname = \"{}.{}.{}\"\n\n",
                kind, db, schema, name
            ));
            for (col_name, col_type, nullable) in *columns {
                toml.push_str(&format!(
                    "[[{}.columns]]\nname = \"{}\"\ntype = \"{}\"\nnullable = {}\n\n",
                    kind, col_name, col_type, nullable
                ));
            }
        }
        std::fs::write(root.join("types.lock"), toml).unwrap();
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_deps_at_zero_dots() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "foo",
                "table",
                &[("id", "integer", false), ("bar", "text", true)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &no_prefix());

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"id"), "expected 'id', got: {:?}", labels);
        assert!(labels.contains(&"bar"), "expected 'bar', got: {:?}", labels);
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_deps_filtered_by_prefix() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "foo",
                "table",
                &[("id", "integer", false), ("name", "text", false)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        let prefix = PrefixContext { dots: 0, text: "i" };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"id"), "expected 'id', got: {:?}", labels);
        assert!(
            !labels.contains(&"name"),
            "should not contain 'name', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_deps_no_types_cache() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        let cache = build_cache(&root);

        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, None, &uri, root.path(), &no_prefix());
        assert!(items.is_empty(), "expected empty without types cache");
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_deps_multiple_dependencies() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("t1.sql"), "CREATE TABLE t1 (a INT);").unwrap();
        std::fs::write(storage.join("t2.sql"), "CREATE TABLE t2 (b INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.t1, mydb.storage.t2;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[
                ("mydb", "storage", "t1", "table", &[("a", "integer", false)]),
                ("mydb", "storage", "t2", "table", &[("b", "integer", false)]),
            ],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &no_prefix());

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"a"), "expected 'a', got: {:?}", labels);
        assert!(labels.contains(&"b"), "expected 'b', got: {:?}", labels);
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_qualified_bare_object() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "foo",
                "table",
                &[("id", "integer", false), ("name", "text", false)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        // "storage.foo" qualified with schema — 2 parts resolves to (default_db, schema, object).
        let prefix = PrefixContext {
            dots: 2,
            text: "storage.foo.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"id"), "expected 'id', got: {:?}", labels);
        assert!(
            labels.contains(&"name"),
            "expected 'name', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_qualified_schema_object() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "foo",
                "table",
                &[("id", "integer", false), ("name", "text", false)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        let prefix = PrefixContext {
            dots: 2,
            text: "storage.foo.i",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"id"), "expected 'id', got: {:?}", labels);
        assert!(
            !labels.contains(&"name"),
            "should not contain 'name', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_qualified_fully_qualified() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "foo",
                "table",
                &[("id", "integer", false)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        let prefix = PrefixContext {
            dots: 3,
            text: "mydb.storage.foo.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"id"), "expected 'id', got: {:?}", labels);
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_qualified_non_dependency_excluded() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();
        std::fs::write(storage.join("other.sql"), "CREATE TABLE other (x INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[
                (
                    "mydb",
                    "storage",
                    "foo",
                    "table",
                    &[("id", "integer", false)],
                ),
                (
                    "mydb",
                    "storage",
                    "other",
                    "table",
                    &[("x", "integer", false)],
                ),
            ],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        // "other" is not a dependency of "v" — qualified as schema.object.
        let prefix = PrefixContext {
            dots: 2,
            text: "storage.other.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        assert!(
            items.is_empty(),
            "expected empty for non-dependency, got: {:?}",
            items.iter().map(|i| &i.label).collect::<Vec<_>>()
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_qualified_object_not_in_cache() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        // types.lock exists but has no columns for foo.
        write_types_lock(root.path(), &[]);
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        // Must use schema-qualified since foo is in storage schema, not compute.
        let prefix = PrefixContext {
            dots: 2,
            text: "storage.foo.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        assert!(items.is_empty(), "expected empty when object not in cache");
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_qualified_filter_case_insensitive() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "foo",
                "table",
                &[("id", "integer", false), ("name", "text", false)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        // Uppercase "I" should match "id".
        let prefix = PrefixContext {
            dots: 2,
            text: "storage.foo.I",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"id"),
            "expected 'id' with case-insensitive match, got: {:?}",
            labels
        );
        assert!(
            !labels.contains(&"name"),
            "should not contain 'name', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_kind_and_detail() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "foo",
                "table",
                &[("id", "integer", false), ("name", "text", true)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &no_prefix());

        let id_item = items.iter().find(|i| i.label == "id").unwrap();
        assert_eq!(id_item.kind, Some(CompletionItemKind::FIELD));
        assert_eq!(id_item.detail.as_deref(), Some("integer"));

        let name_item = items.iter().find(|i| i.label == "name").unwrap();
        assert_eq!(name_item.kind, Some(CompletionItemKind::FIELD));
        assert_eq!(name_item.detail.as_deref(), Some("text (nullable)"));
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_sort_before_objects() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("foo.sql"), "CREATE TABLE foo (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.foo;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "foo",
                "table",
                &[("id", "integer", false)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let col_items =
            column_completions(&cache, Some(&types_cache), &uri, root.path(), &no_prefix());
        let obj_items =
            object_completions(&cache, Some(&types_cache), &uri, root.path(), &no_prefix());

        // Column sort_text starts with "0_", object sort_text starts with "1_" or higher.
        for item in &col_items {
            assert!(
                item.sort_text.as_ref().unwrap().starts_with("0_"),
                "column sort_text should start with '0_', got: {:?}",
                item.sort_text
            );
        }
        for item in &obj_items {
            let sort = item.sort_text.as_ref().unwrap();
            assert!(
                sort.starts_with("1_") || sort.starts_with("2_") || sort.starts_with("3_"),
                "object sort_text should start with '1_'/'2_'/'3_', got: {:?}",
                sort
            );
        }
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_alias_explicit() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("orders.sql"), "CREATE TABLE orders (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT o.id FROM mydb.storage.orders o;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "orders",
                "table",
                &[("id", "integer", false), ("name", "text", false)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        // Typing "o." should resolve via alias to orders.
        let prefix = PrefixContext {
            dots: 1,
            text: "o.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"id"), "expected 'id', got: {:?}", labels);
        assert!(
            labels.contains(&"name"),
            "expected 'name', got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_alias_bare_table_name() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("orders.sql"), "CREATE TABLE orders (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT * FROM mydb.storage.orders;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "orders",
                "table",
                &[("id", "integer", false)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        // Typing "orders." should resolve via bare table name.
        let prefix = PrefixContext {
            dots: 1,
            text: "orders.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"id"), "expected 'id', got: {:?}", labels);
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_alias_non_dependency_empty() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("orders.sql"), "CREATE TABLE orders (id INT);").unwrap();
        std::fs::write(storage.join("other.sql"), "CREATE TABLE other (x INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        // v depends on orders but NOT other. The alias "o" maps to orders.
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT o.id FROM mydb.storage.orders o;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[
                (
                    "mydb",
                    "storage",
                    "orders",
                    "table",
                    &[("id", "integer", false)],
                ),
                (
                    "mydb",
                    "storage",
                    "other",
                    "table",
                    &[("x", "integer", false)],
                ),
            ],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        // "other" is not a dependency — alias resolves to it but dep check fails.
        let prefix = PrefixContext {
            dots: 1,
            text: "other.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        assert!(
            items.is_empty(),
            "expected empty for non-dependency alias, got: {:?}",
            items.iter().map(|i| &i.label).collect::<Vec<_>>()
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_alias_multiple_joins() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("t1.sql"), "CREATE TABLE t1 (a INT);").unwrap();
        std::fs::write(storage.join("t2.sql"), "CREATE TABLE t2 (b INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT x.a, y.b FROM mydb.storage.t1 x JOIN mydb.storage.t2 y ON x.a = y.b;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[
                ("mydb", "storage", "t1", "table", &[("a", "integer", false)]),
                ("mydb", "storage", "t2", "table", &[("b", "integer", false)]),
            ],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();

        // "x." should resolve to t1.
        let prefix_x = PrefixContext {
            dots: 1,
            text: "x.",
        };
        let items_x = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix_x);
        let labels_x: Vec<&str> = items_x.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels_x.contains(&"a"),
            "expected 'a' for x., got: {:?}",
            labels_x
        );

        // "y." should resolve to t2.
        let prefix_y = PrefixContext {
            dots: 1,
            text: "y.",
        };
        let items_y = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix_y);
        let labels_y: Vec<&str> = items_y.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels_y.contains(&"b"),
            "expected 'b' for y., got: {:?}",
            labels_y
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_alias_case_insensitive() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("orders.sql"), "CREATE TABLE orders (id INT);").unwrap();

        let compute = root.path().join("models/mydb/compute");
        std::fs::create_dir_all(&compute).unwrap();
        std::fs::write(
            compute.join("v.sql"),
            "CREATE VIEW v AS SELECT O.id FROM mydb.storage.orders O;",
        )
        .unwrap();
        write_types_lock(
            root.path(),
            &[(
                "mydb",
                "storage",
                "orders",
                "table",
                &[("id", "integer", false)],
            )],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        // Lowercase "o." should match uppercase alias "O".
        let prefix = PrefixContext {
            dots: 1,
            text: "o.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/compute/v.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(
            labels.contains(&"id"),
            "expected 'id' with case-insensitive alias, got: {:?}",
            labels
        );
    }

    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    #[mz_ore::test]
    fn column_alias_non_query_stmt_empty_map() {
        let root = tempfile::tempdir().unwrap();
        let storage = root.path().join("models/mydb/storage");
        std::fs::create_dir_all(&storage).unwrap();
        std::fs::write(storage.join("t.sql"), "CREATE TABLE t (id INT);").unwrap();
        write_types_lock(
            root.path(),
            &[("mydb", "storage", "t", "table", &[("id", "integer", false)])],
        );
        let cache = build_cache(&root);
        let types_cache = crate::types::load_types_lock(root.path()).unwrap();

        // CREATE TABLE has no query — alias map is empty, falls back to normal behavior.
        // "t." with 1 dot resolves to ObjectId(mydb, storage, t) via fallback.
        let prefix = PrefixContext {
            dots: 1,
            text: "t.",
        };
        let uri = Url::from_file_path(root.path().join("models/mydb/storage/t.sql")).unwrap();
        let items = column_completions(&cache, Some(&types_cache), &uri, root.path(), &prefix);

        // t is itself, not a dependency of itself, so empty.
        assert!(
            items.is_empty(),
            "expected empty for non-query statement self-reference, got: {:?}",
            items.iter().map(|i| &i.label).collect::<Vec<_>>()
        );
    }

    #[mz_ore::test]
    fn prefix_context_uses_utf16_cursor_positions() {
        let text = "SELECT 😀foo";
        let ctx = prefix_context(text, Position::new(0, 12));
        assert_eq!(ctx.dots, 0);
        assert_eq!(ctx.text, "foo");
    }

    #[mz_ore::test]
    fn qualify_same_schema_bare_label() {
        let id = ObjectId::new("mydb".to_string(), "public".to_string(), "foo".to_string());
        let prefix = no_prefix();
        let result = qualify_and_filter(&id, "mydb", "public", &prefix);
        assert_eq!(result, Some(("foo".to_string(), "1_foo".to_string())));
    }

    #[mz_ore::test]
    fn qualify_cross_schema_qualified() {
        let id = ObjectId::new("mydb".to_string(), "other".to_string(), "bar".to_string());
        let prefix = no_prefix();
        let result = qualify_and_filter(&id, "mydb", "public", &prefix);
        assert_eq!(
            result,
            Some(("other.bar".to_string(), "2_other.bar".to_string()))
        );
    }

    #[mz_ore::test]
    fn qualify_cross_database_fully_qualified() {
        let id = ObjectId::new("otherdb".to_string(), "s".to_string(), "x".to_string());
        let prefix = no_prefix();
        let result = qualify_and_filter(&id, "mydb", "public", &prefix);
        assert_eq!(
            result,
            Some(("otherdb.s.x".to_string(), "3_otherdb.s.x".to_string()))
        );
    }

    #[mz_ore::test]
    fn qualify_dotted_prefix_matches_schema_qualified() {
        let id = ObjectId::new("mydb".to_string(), "public".to_string(), "foo".to_string());
        let prefix = PrefixContext {
            dots: 1,
            text: "public.",
        };
        let result = qualify_and_filter(&id, "mydb", "public", &prefix);
        assert_eq!(
            result,
            Some(("foo".to_string(), "1_public.foo".to_string()))
        );
    }

    #[mz_ore::test]
    fn qualify_dotted_prefix_no_match() {
        let id = ObjectId::new("mydb".to_string(), "public".to_string(), "foo".to_string());
        let prefix = PrefixContext {
            dots: 1,
            text: "other.",
        };
        let result = qualify_and_filter(&id, "mydb", "public", &prefix);
        assert_eq!(result, None);
    }

    #[mz_ore::test]
    fn qualify_case_insensitive() {
        let id = ObjectId::new("mydb".to_string(), "public".to_string(), "foo".to_string());
        let prefix = PrefixContext {
            dots: 1,
            text: "PUBLIC.F",
        };
        let result = qualify_and_filter(&id, "mydb", "public", &prefix);
        assert_eq!(
            result,
            Some(("foo".to_string(), "1_public.foo".to_string()))
        );
    }

    #[mz_ore::test]
    fn resolve_qualified_object_alias_hit() {
        let mut aliases = BTreeMap::new();
        aliases.insert("o".to_string(), "mydb.storage.orders".to_string());
        let result = resolve_qualified_object("o", &aliases, "mydb", "public");
        assert_eq!(
            result,
            Some(ObjectId::new(
                "mydb".to_string(),
                "storage".to_string(),
                "orders".to_string()
            ))
        );
    }

    #[mz_ore::test]
    fn resolve_qualified_object_bare_fallback() {
        let aliases = BTreeMap::new();
        let result = resolve_qualified_object("foo", &aliases, "mydb", "public");
        assert_eq!(
            result,
            Some(ObjectId::new(
                "mydb".to_string(),
                "public".to_string(),
                "foo".to_string()
            ))
        );
    }

    #[mz_ore::test]
    fn resolve_qualified_object_two_parts() {
        let aliases = BTreeMap::new();
        let result = resolve_qualified_object("storage.orders", &aliases, "mydb", "public");
        assert_eq!(
            result,
            Some(ObjectId::new(
                "mydb".to_string(),
                "storage".to_string(),
                "orders".to_string()
            ))
        );
    }

    #[mz_ore::test]
    fn resolve_qualified_object_three_parts() {
        let aliases = BTreeMap::new();
        let result = resolve_qualified_object("otherdb.s.x", &aliases, "mydb", "public");
        assert_eq!(
            result,
            Some(ObjectId::new(
                "otherdb".to_string(),
                "s".to_string(),
                "x".to_string()
            ))
        );
    }

    #[mz_ore::test]
    fn resolve_qualified_object_four_parts_none() {
        let aliases = BTreeMap::new();
        let result = resolve_qualified_object("a.b.c.d", &aliases, "mydb", "public");
        assert_eq!(result, None);
    }
}
