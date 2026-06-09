// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Incremental project compiler.
//!
//! This module is the canonical implementation of `project::plan_sync()`'s
//! compile contract:
//!
//! - The result of compilation is a
//!   [`crate::project::ir::graph::Project`].
//! - The **unit of incremental reuse** is a logical database object
//!   (`database.schema.object`), not an entire project.
//! - Object-local work is evaluated independently and may run in parallel.
//! - Cross-object validation remains deterministic and is performed after all
//!   object-local results for the invocation are available.
//!
//! ## Build Artifacts
//!
//! Compiler state is scoped to the active configuration (profile name,
//! optional suffix, and compile-time variable bindings). Each configuration
//! gets an isolated namespace so caches never leak across profiles.
//!
//! Within a namespace the compiler persists:
//!
//! - file metadata and content hashes to avoid rereading unchanged files
//! - cached object artifacts for incremental reuse across invocations
//! - cached runtime typecheck artifacts for incremental dirty detection
//!
//! All cached state is advisory. Missing, corrupt, or schema-incompatible
//! entries are treated as cache misses and rebuilt from source.
//!
//! ## Invalidation Rules
//!
//! An object cache entry is reusable only when its fingerprint matches the
//! current compile inputs for that object.
//! The fingerprint includes:
//!
//! - the logical object key
//! - every file variant that can affect active-variant resolution
//! - the full path of every file variant
//! - the cached content hash of those variants
//! - the compile-time variable map
//!
//! As a result:
//!
//! - editing any variant for an object invalidates that object's cache entry
//! - changing variables invalidates every object whose fingerprint includes
//!   those variables
//! - changing the active profile or suffix moves compilation to a different
//!   namespace, isolating caches across profiles
//! - moving the same checkout to a different directory invalidates the cache
//!   because file paths are part of the fingerprint contract
//!
//! This module does **not** currently perform dependency-directed invalidation.
//! Downstream project-graph work is recomputed from the object set produced for
//! the current invocation.
//!
//! ## Correctness Guarantees
//!
//! Cached object artifacts store a validated object payload. A cache hit must
//! therefore produce the same object facts that object-local parsing and
//! validation would produce from source while skipping revalidation.
//!
//! Compilation must preserve these invariants:
//!
//! - all object-local validation errors are reported exactly as if the object
//!   had been freshly compiled
//! - database- and schema-level mod statements are validated on every
//!   invocation; they are not cached independently
//! - schema-level checks (e.g., storage/computation separation, replacement
//!   schemas) are enforced after object artifacts are assembled, so they see
//!   the full current project
//! - final dependency extraction operates on a complete compiled project
//!   assembled for the current invocation

pub(crate) mod cache;
mod cache_io;
mod mod_statements;
mod object_validation;
pub(crate) mod typecheck;

use super::error::{LoadError, ProjectError, ValidationError, ValidationErrors};
use crate::project::ir::{compiled, graph};
use crate::project::syntax::input;
use crate::project::syntax::parser::parse_statements_with_context;
use crate::project::syntax::profile_files::collect_all_sql_files;
use crate::verbose;
use cache::BuildArtifact;
use cache::build_artifact::{CompiledObjectArtifact, CompiledObjectArtifactData, ObjectStateRow};
use cache_io::hex_digest;
use mz_sql_parser::ast::{
    CommentStatement, CreateIndexStatement, ExecuteUnitTestStatement, GrantPrivilegesStatement,
    Raw, Statement,
};
use rayon::prelude::*;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) const COMPILER_DIR: &str = "compiler";

/// Counters for cache behavior during a single compilation run.
///
/// Used by the compile orchestrator to report how many objects were served
/// from cached artifacts (`cache_hits`) versus recompiled from source
/// (`cache_misses`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompileStats {
    pub cache_hits: usize,
    pub cache_misses: usize,
}

/// Output of the discovery phase: everything needed to plan and compile objects.
///
/// Produced by [`discover_project`], which walks the `models/` directory tree
/// and collects:
///
/// - `db_metas` — database and schema metadata (names, mod statements) used
///   by [`object_validation::assemble_project`] to build the compiled project.
/// - `object_descriptors` — one entry per logical object with its file variants,
///   used to fingerprint and compile individual objects.
/// - `db_name_map` — when a profile suffix is active, maps original database
///   names to their suffixed forms (e.g., `"app"` → `"app_dev"`), used later
///   to rewrite cross-database references in compiled object SQL.
#[derive(Debug)]
struct Discovery {
    db_metas: Vec<object_validation::DatabaseBuildMeta>,
    object_descriptors: Vec<ObjectDescriptor>,
    db_name_map: BTreeMap<String, String>,
}

/// A logical database object discovered on disk, not yet compiled.
///
/// Identifies an object by its fully qualified triple (`db_name.schema_name.object_name`)
/// and lists every file variant (default + profile overrides) that could contribute
/// to the active variant. This is the unit of parallelism for fingerprinting and
/// compilation: each descriptor is processed independently.
#[derive(Debug, Clone)]
struct ObjectDescriptor {
    /// Database name after profile suffix has been applied (matches the
    /// database name that will be deployed). Used for grouping, caching,
    /// fingerprinting, and as the canonical name in the compiled project.
    db_name: String,
    /// Original directory name without profile suffix. Used to build the FQN
    /// for per-object validation so that the user's SQL — which references
    /// the unsuffixed name they wrote in their files — matches the directory.
    original_db_name: String,
    schema_name: String,
    object_name: String,
    variants: Vec<VariantDescriptor>,
}

/// A single file variant contributing to an [`ObjectDescriptor`].
///
/// - `path` — absolute path to the `.sql` file on disk.
/// - `profile` — `None` for the default variant (`object.sql`), `Some("prod")`
///   for a profile override (`object#prod.sql`). Used during active-variant
///   resolution to select which file to compile for the current profile.
#[derive(Debug, Clone)]
struct VariantDescriptor {
    path: PathBuf,
    profile: Option<String>,
}

/// In-memory representation of a compiled object together with its location.
///
/// Carries the fully validated [`compiled::DatabaseObject`] alongside the
/// database and schema names needed to slot it into the assembled project.
/// Produced by both cache hits (via `CachedTypedObjectArtifact::into_compiled_object`)
/// and fresh compilation (via [`compile_object_uncached`]).
#[derive(Debug, Clone)]
struct CachedTypedObject {
    db_name: String,
    schema_name: String,
    typed_object: compiled::DatabaseObject,
}

/// Result of compiling a single object from source.
///
/// - `Ok` — compilation succeeded. `compiled` is `None` when the object was
///   skipped (e.g., no matching profile variant). `state_row` carries the
///   artifact to persist in the cache.
/// - `ValidationErr` — the object has user-facing validation errors. These are
///   collected and reported after all objects are compiled.
/// - `ProjectErr` — an internal error (I/O failure, parse crash) that should
///   abort compilation immediately.
enum ObjectCompileResult {
    Ok {
        compiled: Option<CachedTypedObject>,
        state_row: Option<ObjectStateRow>,
        stats: CompileStats,
    },
    ValidationErr(Vec<ValidationError>),
    ProjectErr(ProjectError),
}

/// Result of the planning phase for a single object.
///
/// Planning determines whether a cached artifact can be reused (`Hit`) or the
/// object must be recompiled from source (`Miss`). This runs in parallel across
/// all discovered objects before any compilation begins.
///
/// - `Hit` — the cached fingerprint matches; `compiled` contains the
///   deserialized object (or `None` if the cached artifact was `Skipped`).
/// - `Miss` — the object needs fresh compilation. Carries the descriptor and
///   current fingerprint so the compile phase can produce and persist a new
///   artifact.
/// - `ProjectErr` — fingerprinting failed (e.g., missing file hash).
enum ObjectPlanResult {
    Hit {
        object_key: String,
        compiled: Option<CachedTypedObject>,
        stats: CompileStats,
    },
    Miss {
        object_key: String,
        fingerprint: String,
        descriptor: ObjectDescriptor,
    },
    ProjectErr(ProjectError),
}

/// Compile a project directory into a dependency-aware [`graph::Project`].
///
/// This is the canonical synchronous compiler entrypoint. It parses and
/// validates every object for the active profile, reusing cached artifacts
/// when fingerprints still match, and returns a fully-linked project graph
/// with dependency and cross-object validation applied.
///
/// See [`compile_sync_with_stats`] for the detailed pipeline and cache
/// behavior.
pub(crate) fn compile_sync<P: AsRef<Path>>(
    fs: &crate::fs::FileSystem,
    root: P,
    profile: Option<&str>,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> Result<graph::Project, ProjectError> {
    compile_sync_with_stats(fs, root, profile, profile_suffix, variables)
        .map(|(project, _)| project)
}

/// Internal entry point that returns compile statistics alongside the project.
///
/// Runs the full incremental pipeline:
///
/// 1. **Discover** — walk `models/` to find databases, schemas, objects, and
///    mod files. Build the [`Discovery`] containing all descriptors and the
///    database name map.
/// 2. **Plan** — fingerprint every object against the cached artifact store.
///    Partition objects into cache hits and cache misses (parallel via rayon).
/// 3. **Compile misses** — parse, validate, and normalize each miss from
///    source. Persist new artifacts back to the cache (parallel via rayon).
/// 4. **Assemble** — combine database/schema metadata with validated objects
///    into a [`compiled::Project`]. Apply cross-database and cluster name
///    rewrites if a profile suffix is active.
/// 5. **Build graph** — run cross-object validation, dependency extraction,
///    and topological analysis to produce the final [`graph::Project`].
fn compile_sync_with_stats<P: AsRef<Path>>(
    fs: &crate::fs::FileSystem,
    root: P,
    profile: Option<&str>,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> Result<(graph::Project, CompileStats), ProjectError> {
    // Internally we keep the profile name as a plain `&str` (empty when no
    // profile is selected) and carry the "is a profile set?" bit separately
    // for error display. The empty string can't collide with a real profile
    // name (validation rejects it) and produces a stable cache namespace.
    let profile_set = profile.is_some();
    let profile = profile.unwrap_or("");
    let root = root.as_ref();
    let mut db =
        BuildArtifact::open(root, profile, profile_suffix, variables).map_err(LoadError::from)?;
    let discovery = discover_project(fs, root, profile_suffix, variables, profile_set, &mut db)?;

    let variant_paths: BTreeSet<PathBuf> = discovery
        .object_descriptors
        .iter()
        .flat_map(|descriptor| {
            descriptor
                .variants
                .iter()
                .map(|variant| variant.path.clone())
        })
        .collect();
    let file_hashes = db
        .load_file_hashes(fs, &variant_paths)
        .map_err(LoadError::from)?;

    let existing_fingerprints = db.load_object_fingerprints().map_err(LoadError::from)?;

    // Phase 1: classify each descriptor as a fingerprint-level Hit/Miss in parallel.
    let stages: Vec<ObjectPlanStage> = discovery
        .object_descriptors
        .clone()
        .into_par_iter()
        .map(|descriptor| stage_object(descriptor, &existing_fingerprints, &file_hashes, variables))
        .collect();

    // Phase 2: load full artifacts only for fingerprint-level hits — selective.
    let hit_keys: BTreeSet<String> = stages
        .iter()
        .filter_map(|stage| match stage {
            ObjectPlanStage::Hit { object_key, .. } => Some(object_key.clone()),
            _ => None,
        })
        .collect();
    let hit_artifacts = db
        .load_object_artifacts(&hit_keys)
        .map_err(LoadError::from)?;

    // Phase 3: finalize each stage. Hits parse their cached SQL into AST; if any
    // fragment fails to parse the entry is demoted to a Miss for fresh compilation.
    let plans: Vec<ObjectPlanResult> = stages
        .into_par_iter()
        .map(|stage| finalize_stage(stage, &hit_artifacts))
        .collect();

    let mut all_validation_errors = Vec::new();
    let mut validated_objects = Vec::new();
    let mut stats = CompileStats::default();
    let mut current_keys = BTreeSet::new();
    let mut miss_keys: BTreeSet<String> = BTreeSet::new();
    let mut misses = Vec::new();

    for plan in plans {
        match plan {
            ObjectPlanResult::Hit {
                object_key,
                compiled,
                stats: object_stats,
            } => {
                current_keys.insert(object_key);
                if let Some(compiled) = compiled {
                    validated_objects.push((
                        compiled.db_name,
                        compiled.schema_name,
                        compiled.typed_object,
                    ));
                }
                stats.cache_hits += object_stats.cache_hits;
                stats.cache_misses += object_stats.cache_misses;
            }
            ObjectPlanResult::Miss {
                object_key,
                fingerprint,
                descriptor,
            } => {
                current_keys.insert(object_key.clone());
                miss_keys.insert(object_key.clone());
                misses.push((object_key, fingerprint, descriptor));
            }
            ObjectPlanResult::ProjectErr(err) => return Err(err),
        }
    }

    if !misses.is_empty() {
        let miss_paths: BTreeSet<PathBuf> = misses
            .iter()
            .flat_map(|(_, _, descriptor)| {
                descriptor
                    .variants
                    .iter()
                    .map(|variant| variant.path.clone())
            })
            .collect();
        let miss_file_entries = db
            .load_file_contents(fs, &miss_paths)
            .map_err(LoadError::from)?;
        let results: Vec<ObjectCompileResult> = misses
            .into_par_iter()
            .map(|(object_key, fingerprint, descriptor)| {
                compile_object(
                    descriptor,
                    object_key,
                    fingerprint,
                    profile,
                    variables,
                    profile_set,
                    &miss_file_entries,
                )
            })
            .collect();

        let mut updated_rows = Vec::new();
        for result in results {
            match result {
                ObjectCompileResult::Ok {
                    compiled,
                    state_row,
                    stats: object_stats,
                } => {
                    if let Some(compiled) = compiled {
                        validated_objects.push((
                            compiled.db_name,
                            compiled.schema_name,
                            compiled.typed_object,
                        ));
                    }
                    if let Some(row) = state_row {
                        updated_rows.push(row);
                    }
                    stats.cache_hits += object_stats.cache_hits;
                    stats.cache_misses += object_stats.cache_misses;
                }
                ObjectCompileResult::ValidationErr(errs) => all_validation_errors.extend(errs),
                ObjectCompileResult::ProjectErr(err) => return Err(err),
            }
        }
        db.upsert_object_rows(&updated_rows)
            .map_err(LoadError::from)?;
    }

    if !all_validation_errors.is_empty() {
        return Err(ValidationErrors::new(all_validation_errors).into());
    }
    db.prune_object_rows(&current_keys)
        .map_err(LoadError::from)?;

    let mut compiled_project =
        object_validation::assemble_project(discovery.db_metas, validated_objects)?;
    if !discovery.db_name_map.is_empty() {
        compiled_project.rewrite_database_references(&discovery.db_name_map);
    }
    if let Some(ps) = profile_suffix {
        let cluster_name_map = build_cluster_name_map(&compiled_project, ps);
        if !cluster_name_map.is_empty() {
            compiled_project.rewrite_cluster_references(&cluster_name_map);
        }
    }

    let mut project = graph::Project::from(compiled_project);
    project.compile_dirty = miss_keys.iter().filter_map(|k| k.parse().ok()).collect();

    // Advisory persist for LSP consumption — failure is logged, not fatal.
    let deleted_keys: BTreeSet<String> = existing_fingerprints
        .keys()
        .filter(|k| !current_keys.contains(*k))
        .cloned()
        .collect();
    if let Err(e) = db.write_project(&project, &miss_keys, &deleted_keys, root) {
        verbose!("Failed to persist project to SQLite: {}", e);
    }

    Ok((project, stats))
}

/// Build a map from original cluster name to the suffixed cluster name for all
/// clusters referenced by the compiled project.
fn build_cluster_name_map(
    project: &compiled::Project,
    cluster_suffix: &str,
) -> BTreeMap<String, String> {
    let mut names = BTreeSet::new();
    for db in &project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                names.extend(obj.clusters());
            }
        }
    }
    names
        .into_iter()
        .map(|name| {
            let suffixed = format!("{}{}", name, cluster_suffix);
            (name, suffixed)
        })
        .collect()
}

/// Walk the `models/` directory tree and collect everything needed for compilation.
///
/// The directory structure follows the convention:
///
/// ```text
/// models/
///   <database>/               ← directory name = database name
///     <database>.sql          ← optional database-level mod file (grants, comments)
///     <schema>/               ← directory name = schema name
///       <schema>.sql          ← optional schema-level mod file
///       <object>.sql          ← one file per database object
///       <object>#<profile>.sql ← optional profile variant override
/// ```
///
/// For each database directory:
/// - Computes the effective database name (original + profile suffix if active).
/// - Parses and validates database and schema mod files.
/// - Collects all object file variants into [`ObjectDescriptor`]s.
/// - Builds the `db_name_map` for cross-database reference rewriting.
///
/// Returns a [`Discovery`] or fails with accumulated validation errors.
fn discover_project(
    fs: &crate::fs::FileSystem,
    root: &Path,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    profile_set: bool,
    db: &mut BuildArtifact,
) -> Result<Discovery, ProjectError> {
    if !root.exists() {
        return Err(LoadError::RootNotFound {
            path: root.to_path_buf(),
        }
        .into());
    }
    if !root.is_dir() {
        return Err(LoadError::RootNotDirectory {
            path: root.to_path_buf(),
        }
        .into());
    }

    let models_dir = root.join("models");
    if !models_dir.is_dir() {
        return Err(LoadError::ModelsNotFound { path: models_dir }.into());
    }

    let mut db_name_map = BTreeMap::new();
    let mut db_metas = Vec::new();
    let mut object_descriptors = Vec::new();
    let mut validation_errors = Vec::new();

    for db_entry in fs::read_dir(&models_dir).map_err(|source| LoadError::DirectoryReadFailed {
        path: models_dir.clone(),
        source,
    })? {
        let db_entry = db_entry.map_err(|source| LoadError::EntryReadFailed {
            directory: models_dir.clone(),
            source,
        })?;
        let db_path = db_entry.path();
        if !db_path.is_dir() || db_entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }

        let original_db_name = db_entry.file_name().to_string_lossy().to_string();
        let db_name = match profile_suffix {
            Some(suffix) => format!("{}{}", original_db_name, suffix),
            None => original_db_name.clone(),
        };
        if profile_suffix.is_some() {
            db_name_map.insert(original_db_name.clone(), db_name.clone());
        }

        let db_mod_path = models_dir.join(format!("{}.sql", original_db_name));
        let db_mod_statements = parse_mod_statements(
            fs,
            &db_mod_path,
            &original_db_name,
            profile_suffix,
            variables,
            profile_set,
            db,
        )?;
        if let Some(ref stmts) = db_mod_statements {
            mod_statements::validate_database_mod_statements(
                &db_name,
                &db_mod_path,
                stmts,
                &mut validation_errors,
            );
        }

        let mut schema_metas = Vec::new();
        for schema_entry in
            fs::read_dir(&db_path).map_err(|source| LoadError::DirectoryReadFailed {
                path: db_path.clone(),
                source,
            })?
        {
            let schema_entry = schema_entry.map_err(|source| LoadError::EntryReadFailed {
                directory: db_path.clone(),
                source,
            })?;
            let schema_path = schema_entry.path();
            if !schema_path.is_dir() || schema_entry.file_name().to_string_lossy().starts_with('.')
            {
                continue;
            }

            let schema_name = schema_entry.file_name().to_string_lossy().to_string();
            let schema_mod_path = db_path.join(format!("{}.sql", schema_name));
            let mut schema_mod_statements = parse_mod_statements(
                fs,
                &schema_mod_path,
                &original_db_name,
                profile_suffix,
                variables,
                profile_set,
                db,
            )?;
            if let Some(ref mut stmts) = schema_mod_statements {
                mod_statements::validate_schema_mod_statements(
                    &db_name,
                    &schema_name,
                    &schema_mod_path,
                    stmts,
                    &mut validation_errors,
                );
            }

            let object_files = collect_all_sql_files(&schema_path)?;
            for object_files in object_files {
                let mut variants = Vec::new();
                if let Some(path) = object_files.default {
                    variants.push(VariantDescriptor {
                        path,
                        profile: None,
                    });
                }
                for (variant_profile, path) in object_files.overrides {
                    variants.push(VariantDescriptor {
                        path,
                        profile: Some(variant_profile),
                    });
                }
                object_descriptors.push(ObjectDescriptor {
                    db_name: db_name.clone(),
                    original_db_name: original_db_name.clone(),
                    schema_name: schema_name.clone(),
                    object_name: object_files.name,
                    variants,
                });
            }

            schema_metas.push(object_validation::SchemaBuildMeta {
                name: schema_name,
                mod_statements: schema_mod_statements,
            });
        }

        db_metas.push(object_validation::DatabaseBuildMeta {
            name: db_name,
            mod_statements: db_mod_statements,
            schemas: schema_metas,
        });
    }

    if !validation_errors.is_empty() {
        return Err(ValidationErrors::new(validation_errors).into());
    }

    Ok(Discovery {
        db_metas,
        object_descriptors,
        db_name_map,
    })
}

/// Parse mod statements from a SQL file, optionally rewriting database names.
///
/// If `profile_suffix` is `Some`, all `UnresolvedDatabaseName` nodes matching
/// `original_db_name` are rewritten at the AST level by appending the suffix.
/// This is safer than raw text substitution because it only touches identifier
/// nodes, not string literals or comments.
fn parse_mod_statements(
    fs: &crate::fs::FileSystem,
    path: &Path,
    original_db_name: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    profile_set: bool,
    db: &mut BuildArtifact,
) -> Result<Option<Vec<Statement<Raw>>>, ProjectError> {
    if !path.exists() {
        return Ok(None);
    }

    let mut entries = db
        .load_file_contents(fs, &BTreeSet::from([path.to_path_buf()]))
        .map_err(LoadError::from)?;
    let sql = entries
        .remove(path)
        .ok_or_else(|| LoadError::InvalidFileName {
            path: path.to_path_buf(),
        })?;
    let mut statements: Vec<Statement<Raw>> =
        parse_statements_with_context(&sql, path.to_path_buf(), variables, profile_set)?
            .into_iter()
            .map(|stmt| stmt.ast)
            .collect();
    if let Some(suffix) = profile_suffix {
        crate::project::resolve::normalize::rewrite_database_names(
            &mut statements,
            original_db_name,
            suffix,
        );
    }
    Ok(Some(statements))
}

/// Intermediate planner classification before cached SQL fragments are loaded.
///
/// Phase 1 of the planner produces a stage by comparing the current and stored
/// fingerprints. Hits become candidates for cache reuse; their SQL fragments
/// are loaded and parsed in a later phase (see [`finalize_stage`]).
enum ObjectPlanStage {
    Hit {
        object_key: String,
        fingerprint: String,
        descriptor: ObjectDescriptor,
    },
    Miss {
        object_key: String,
        fingerprint: String,
        descriptor: ObjectDescriptor,
    },
    ProjectErr(ProjectError),
}

/// Classify a single object as a fingerprint-level Hit or Miss against the
/// stored cache, without loading the cached SQL fragments.
fn stage_object(
    descriptor: ObjectDescriptor,
    existing_fingerprints: &BTreeMap<String, String>,
    file_hashes: &BTreeMap<PathBuf, String>,
    variables: &BTreeMap<String, String>,
) -> ObjectPlanStage {
    let object_key = object_key(
        &descriptor.db_name,
        &descriptor.schema_name,
        &descriptor.object_name,
    );
    let fingerprint = match object_fingerprint(&descriptor, file_hashes, variables) {
        Ok(fingerprint) => fingerprint,
        Err(err) => return ObjectPlanStage::ProjectErr(err),
    };

    if existing_fingerprints.get(&object_key) == Some(&fingerprint) {
        ObjectPlanStage::Hit {
            object_key,
            fingerprint,
            descriptor,
        }
    } else {
        ObjectPlanStage::Miss {
            object_key,
            fingerprint,
            descriptor,
        }
    }
}

/// Turn a fingerprint-level stage into a final [`ObjectPlanResult`].
///
/// For a fingerprint Hit, attempts to reconstruct the typed object by
/// re-parsing the cached SQL fragments. If any fragment fails to parse the
/// entry is treated as a Miss for fresh compilation.
fn finalize_stage(
    stage: ObjectPlanStage,
    hit_artifacts: &BTreeMap<String, CompiledObjectArtifact>,
) -> ObjectPlanResult {
    match stage {
        ObjectPlanStage::Hit {
            object_key,
            fingerprint,
            descriptor,
        } => {
            let Some(artifact) = hit_artifacts.get(&object_key) else {
                verbose!(
                    "recompiling {} after cached object row was missing during artifact load",
                    object_key
                );
                return ObjectPlanResult::Miss {
                    object_key,
                    fingerprint,
                    descriptor,
                };
            };
            match artifact_to_compiled_object(artifact) {
                Ok(compiled) => ObjectPlanResult::Hit {
                    object_key,
                    compiled,
                    stats: CompileStats {
                        cache_hits: 1,
                        cache_misses: 0,
                    },
                },
                Err(()) => {
                    verbose!(
                        "recompiling {} after cached object payload could not be reconstructed",
                        object_key
                    );
                    ObjectPlanResult::Miss {
                        object_key,
                        fingerprint,
                        descriptor,
                    }
                }
            }
        }
        ObjectPlanStage::Miss {
            object_key,
            fingerprint,
            descriptor,
        } => ObjectPlanResult::Miss {
            object_key,
            fingerprint,
            descriptor,
        },
        ObjectPlanStage::ProjectErr(err) => ObjectPlanResult::ProjectErr(err),
    }
}

/// Internal error type for [`compile_object_uncached`].
///
/// Separates user-facing validation errors (which should be collected and
/// reported together) from internal project errors (which abort compilation).
enum ObjectCompileFailure {
    Validation(Vec<ValidationError>),
    Project(ProjectError),
}

/// Compile a single object from source files without consulting the cache.
///
/// Reads the SQL content for each file variant from the pre-loaded
/// `file_entries` map, parses them into AST, builds an [`input::DatabaseObject`],
/// and runs object-level validation via [`compiled::DatabaseObject::validate`].
///
/// Returns `Ok(Some(...))` for a successfully compiled object, `Ok(None)` if
/// the object was skipped (no matching profile variant), or an error for
/// validation failures or I/O problems.
///
/// This function is pure (no database writes) and runs in parallel via rayon.
fn compile_object_uncached(
    descriptor: ObjectDescriptor,
    profile: &str,
    variables: &BTreeMap<String, String>,
    profile_set: bool,
    file_entries: &BTreeMap<PathBuf, String>,
) -> Result<Option<CachedTypedObject>, ObjectCompileFailure> {
    let mut variants = Vec::new();
    for variant in descriptor.variants {
        let sql = file_entries.get(&variant.path).cloned().ok_or_else(|| {
            ObjectCompileFailure::Project(
                LoadError::InvalidFileName {
                    path: variant.path.clone(),
                }
                .into(),
            )
        })?;
        let statements =
            parse_statements_with_context(&sql, variant.path.clone(), variables, profile_set)
                .map_err(|err| ObjectCompileFailure::Project(err.into()))?;
        variants.push(input::ObjectVariant {
            path: variant.path,
            profile: variant.profile,
            statements,
        });
    }

    // Build the input with the *original* (directory-derived) database name so
    // that per-object validation compares the user's declared database against
    // the directory they wrote it under. The suffixed name is reapplied to
    // dependencies and the statement's own name post-assembly via
    // `Project::rewrite_database_references`.
    let raw_object = input::DatabaseObject {
        name: descriptor.object_name,
        database: descriptor.original_db_name.clone(),
        schema: descriptor.schema_name.clone(),
        variants,
    };

    match compiled::DatabaseObject::validate(raw_object, profile) {
        Ok(Some(typed_object)) => Ok(Some(CachedTypedObject {
            db_name: descriptor.db_name,
            schema_name: descriptor.schema_name,
            typed_object,
        })),
        Ok(None) => Ok(None),
        Err(errs) => Err(ObjectCompileFailure::Validation(errs.errors)),
    }
}

/// Compile a single object and wrap the result for cache persistence.
///
/// Delegates to [`compile_object_uncached`] for the actual compilation, then
/// serializes the result into a [`CompiledObjectArtifact`] and packages it
/// with the object key and fingerprint into an [`ObjectStateRow`] ready to
/// be upserted into the SQLite cache.
fn compile_object(
    descriptor: ObjectDescriptor,
    object_key: String,
    fingerprint: String,
    profile: &str,
    variables: &BTreeMap<String, String>,
    profile_set: bool,
    file_entries: &BTreeMap<PathBuf, String>,
) -> ObjectCompileResult {
    let compiled =
        match compile_object_uncached(descriptor, profile, variables, profile_set, file_entries) {
            Ok(compiled) => compiled,
            Err(ObjectCompileFailure::Validation(errs)) => {
                return ObjectCompileResult::ValidationErr(errs);
            }
            Err(ObjectCompileFailure::Project(err)) => return ObjectCompileResult::ProjectErr(err),
        };

    let artifact = match &compiled {
        Some(object) => CompiledObjectArtifact::Object(compiled_object_to_artifact_data(object)),
        None => CompiledObjectArtifact::Skipped,
    };

    ObjectCompileResult::Ok {
        compiled,
        state_row: Some(ObjectStateRow {
            object_key,
            fingerprint,
            artifact,
        }),
        stats: CompileStats {
            cache_hits: 0,
            cache_misses: 1,
        },
    }
}

/// Compute a SHA-256 fingerprint for an object's current compile inputs.
///
/// The fingerprint is a hex-encoded hash of:
///
/// - the object's logical key (`db_name`, `schema_name`, `object_name`)
/// - every compile-time variable binding (name and value)
/// - every file variant's path, profile tag, and content hash
///
/// Two invocations produce the same fingerprint if and only if the object's
/// identity, variables, file paths, and file contents are all identical.
/// This is the cache key: a matching fingerprint means the cached artifact
/// is safe to reuse without recompilation.
fn object_fingerprint(
    descriptor: &ObjectDescriptor,
    file_hashes: &BTreeMap<PathBuf, String>,
    variables: &BTreeMap<String, String>,
) -> Result<String, ProjectError> {
    let mut hasher = Sha256::new();
    hasher.update(descriptor.db_name.as_bytes());
    hasher.update([0]);
    hasher.update(descriptor.schema_name.as_bytes());
    hasher.update([0]);
    hasher.update(descriptor.object_name.as_bytes());
    hasher.update([0]);
    for (name, value) in variables {
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update(value.as_bytes());
        hasher.update([0xff]);
    }
    for variant in &descriptor.variants {
        hasher.update(variant.path.to_string_lossy().as_bytes());
        hasher.update([0]);
        hasher.update(variant.profile.as_deref().unwrap_or("").as_bytes());
        hasher.update([0]);
        let content_hash =
            file_hashes
                .get(&variant.path)
                .ok_or_else(|| LoadError::InvalidFileName {
                    path: variant.path.clone(),
                })?;
        hasher.update(content_hash.as_bytes());
        hasher.update([0xfe]);
    }
    Ok(hex_digest(hasher.finalize()))
}

/// Build the canonical cache key for a logical object: `"db.schema.object"`.
fn object_key(db_name: &str, schema_name: &str, object_name: &str) -> String {
    format!("{db_name}.{schema_name}.{object_name}")
}

/// Compute the cache namespace for a profile configuration.
///
/// Returns a hex-encoded SHA-256 hash of the profile name, optional suffix,
/// and variable bindings. This ensures that different profile/suffix/variable
/// combinations use isolated SQLite databases under `target/compiler/`,
/// preventing cross-contamination of cached artifacts.
pub(crate) fn profile_namespace(
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(profile.as_bytes());
    hasher.update([0]);
    hasher.update(profile_suffix.unwrap_or("").as_bytes());
    hasher.update([0]);
    for (name, value) in variables {
        hasher.update(name.as_bytes());
        hasher.update([0]);
        hasher.update(value.as_bytes());
        hasher.update([0xff]);
    }
    hex_digest(hasher.finalize())
}

/// Convert a freshly compiled object into the SQL-text shape persisted in the
/// cache. Each AST node is rendered with a trailing semicolon so the inverse
/// parse in [`artifact_to_compiled_object`] sees a self-contained statement.
fn compiled_object_to_artifact_data(object: &CachedTypedObject) -> CompiledObjectArtifactData {
    CompiledObjectArtifactData {
        db_name: object.db_name.clone(),
        schema_name: object.schema_name.clone(),
        file_path: object.typed_object.path.clone(),
        stmt_sql: format!("{};", object.typed_object.stmt),
        indexes_sql: object
            .typed_object
            .indexes
            .iter()
            .map(|stmt| format!("{};", stmt))
            .collect(),
        grants_sql: object
            .typed_object
            .grants
            .iter()
            .map(|stmt| format!("{};", stmt))
            .collect(),
        comments_sql: object
            .typed_object
            .comments
            .iter()
            .map(|stmt| format!("{};", stmt))
            .collect(),
        tests_sql: object
            .typed_object
            .tests
            .iter()
            .map(|stmt| format!("{};", stmt))
            .collect(),
    }
}

/// Reconstruct a [`CachedTypedObject`] by re-parsing the cached SQL fragments.
///
/// Returns `Ok(None)` for `Skipped` entries. Returns `Err(())` if any fragment
/// fails to parse — the caller should treat this as a cache miss and recompile
/// the object from source.
fn artifact_to_compiled_object(
    artifact: &CompiledObjectArtifact,
) -> Result<Option<CachedTypedObject>, ()> {
    match artifact {
        CompiledObjectArtifact::Skipped => Ok(None),
        CompiledObjectArtifact::Object(data) => Ok(Some(CachedTypedObject {
            db_name: data.db_name.clone(),
            schema_name: data.schema_name.clone(),
            typed_object: compiled::DatabaseObject {
                path: data.file_path.clone(),
                stmt: parse_main_statement(&data.stmt_sql)?,
                indexes: parse_statement_list(&data.indexes_sql, expect_index)?,
                grants: parse_statement_list(&data.grants_sql, expect_grant)?,
                comments: parse_statement_list(&data.comments_sql, expect_comment)?,
                tests: parse_statement_list(&data.tests_sql, expect_test)?,
            },
        })),
    }
}

/// Parse a SQL string into a list of raw AST statements.
///
/// Returns `Err(())` on any parse failure. Used only for cache reconstruction
/// where detailed error reporting is unnecessary — a parse failure simply
/// means the cache entry is stale.
fn parse_sql(sql: &str) -> Result<Vec<Statement<Raw>>, ()> {
    mz_sql_parser::parser::parse_statements_with_limit(sql)
        .map_err(|_| ())?
        .map(|stmts| stmts.into_iter().map(|stmt| stmt.ast).collect())
        .map_err(|_| ())
}

/// Parse a SQL string that must contain exactly one statement.
///
/// Returns `Err(())` if parsing fails or the string contains zero or
/// multiple statements.
fn parse_one_statement(sql: &str) -> Result<Statement<Raw>, ()> {
    let mut statements = parse_sql(sql)?;
    if statements.len() != 1 {
        return Err(());
    }
    Ok(statements.remove(0))
}

/// Parse a list of SQL strings and downcast each to a specific statement type.
///
/// Each string is parsed via [`parse_one_statement`], then passed through
/// `parser` to extract the expected AST variant (e.g., `CreateIndexStatement`).
/// Returns `Err(())` if any string fails to parse or has the wrong statement type.
fn parse_statement_list<T>(
    sql_statements: &[String],
    parser: fn(Statement<Raw>) -> Result<T, ()>,
) -> Result<Vec<T>, ()> {
    sql_statements
        .iter()
        .map(|sql| parse_one_statement(sql).and_then(parser))
        .collect()
}

/// Parse a cached main statement SQL string into the project's [`Statement`](crate::project::ast::Statement) enum.
///
/// Only the statement types that mz-deploy manages as database objects are
/// accepted: views, materialized views, tables, table-from-source, sources,
/// sinks, secrets, and connections. Any other statement type returns `Err(())`.
fn parse_main_statement(sql: &str) -> Result<crate::project::ast::Statement, ()> {
    match parse_one_statement(sql)? {
        Statement::CreateSink(stmt) => Ok(crate::project::ast::Statement::CreateSink(stmt)),
        Statement::CreateView(stmt) => Ok(crate::project::ast::Statement::CreateView(stmt)),
        Statement::CreateMaterializedView(stmt) => {
            Ok(crate::project::ast::Statement::CreateMaterializedView(stmt))
        }
        Statement::CreateTable(stmt) => Ok(crate::project::ast::Statement::CreateTable(stmt)),
        Statement::CreateTableFromSource(stmt) => {
            Ok(crate::project::ast::Statement::CreateTableFromSource(stmt))
        }
        Statement::CreateSource(stmt) => Ok(crate::project::ast::Statement::CreateSource(stmt)),
        Statement::CreateSecret(stmt) => Ok(crate::project::ast::Statement::CreateSecret(stmt)),
        Statement::CreateConnection(stmt) => {
            Ok(crate::project::ast::Statement::CreateConnection(stmt))
        }
        _ => Err(()),
    }
}

/// Extract a [`CreateIndexStatement`] from a generic `Statement`, or `Err(())`.
fn expect_index(stmt: Statement<Raw>) -> Result<CreateIndexStatement<Raw>, ()> {
    match stmt {
        Statement::CreateIndex(stmt) => Ok(stmt),
        _ => Err(()),
    }
}

/// Extract a [`GrantPrivilegesStatement`] from a generic `Statement`, or `Err(())`.
fn expect_grant(stmt: Statement<Raw>) -> Result<GrantPrivilegesStatement<Raw>, ()> {
    match stmt {
        Statement::GrantPrivileges(stmt) => Ok(stmt),
        _ => Err(()),
    }
}

/// Extract a [`CommentStatement`] from a generic `Statement`, or `Err(())`.
fn expect_comment(stmt: Statement<Raw>) -> Result<CommentStatement<Raw>, ()> {
    match stmt {
        Statement::Comment(stmt) => Ok(stmt),
        _ => Err(()),
    }
}

/// Extract an [`ExecuteUnitTestStatement`] from a generic `Statement`, or `Err(())`.
fn expect_test(stmt: Statement<Raw>) -> Result<ExecuteUnitTestStatement<Raw>, ()> {
    match stmt {
        Statement::ExecuteUnitTest(stmt) => Ok(stmt),
        _ => Err(()),
    }
}
