// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runtime typechecking integrated with the project compiler.
//!
//! Validation runs against an `mz-deploy` in-memory catalog using `mz-sql`
//! directly (see [`catalog`]). See [`run`] for the algorithm.

use super::cache::BuildArtifact;
use crate::project::ast::Statement;
use crate::project::ir::compiled::FullyQualifiedName;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::types::{ColumnType, ObjectKind, Types, TypesError};
use crate::verbose;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

mod bootstrap;
mod catalog;
mod convert;
mod error;
mod executor;

pub(crate) use error::{ObjectTypeCheckError, ObjectTypeCheckErrorKind, TypeCheckError};

/// Counts of incremental typecheck behavior during a single `run` call.
///
/// `ran + skipped` partitions all typecheck-eligible nodes;
/// `schema_stable + schema_changed` partitions `ran`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TypecheckStats {
    pub ran: usize,
    pub skipped: usize,
    pub schema_stable: usize,
    pub schema_changed: usize,
}

/// `schema_stable` is true when `columns` matches the cached result; dependents
/// only re-typecheck when at least one dep is not schema-stable.
#[derive(Debug, Clone)]
struct NodeValue {
    columns: BTreeMap<String, ColumnType>,
    schema_stable: bool,
}

/// Full-typecheck entrypoint with incremental reuse.
///
/// Runs three phases:
///
/// 1. Build the base catalog (serial): seeds builtins, namespaces, external
///    types, and all non-typechecked project objects.
/// 2. Run the DAG executor (parallel): each view/MV is a node. A node either
///    re-typechecks (when its file or any upstream output changed) or returns
///    its cached column schema directly. Dependents only re-typecheck when at
///    least one upstream dep was schema-changed, which keeps a leaf edit that
///    doesn't change the leaf's output schema from cascading.
/// 3. Persist newly-validated columns to SQLite. Failed and blocked objects
///    keep their last successful row in the cache.
///
/// Returns the merged `Types` covering validated columns, base columns
/// (tables/sources/etc.), and external `types.lock` entries, plus stats
/// describing how much work the incremental layer skipped.
pub(crate) fn run(
    directory: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
    project: &Project,
    external_types: Types,
) -> Result<(Types, TypecheckStats), TypeCheckError> {
    let sorted = project.get_sorted_objects()?;
    let typed_objects: BTreeMap<ObjectId, &crate::project::ir::compiled::DatabaseObject> = sorted
        .iter()
        .filter(|(_, db_obj)| {
            matches!(
                db_obj.stmt,
                Statement::CreateView(_) | Statement::CreateMaterializedView(_)
            )
        })
        .map(|(id, db_obj)| (id.clone(), *db_obj))
        .collect();

    // Open the build artifact db now so we can use it for incremental reads
    // (cached columns, prior external-type digests) and the final upserts.
    let mut db = BuildArtifact::open(directory, profile, profile_suffix, variables)
        .map_err(TypesError::from)?;

    // Snapshot all cached typecheck columns up front. Reading inside the DAG
    // would require a Sync SQLite handle, which rusqlite::Connection isn't.
    // The map is keyed by `ObjectId.to_string()` (matches sqlite layout).
    let cached_columns_by_key: BTreeMap<String, BTreeMap<String, ColumnType>> =
        db.load_typecheck_columns().map_err(TypesError::from)?;

    // Diff per-external-table digests against the cached set. Any project
    // object whose `external_dependencies` intersects the changed set is added
    // to the dirty set on top of `project.compile_dirty`.
    let current_ext_digests = compute_external_digests(&external_types);
    let cached_ext_digests = db.load_external_type_digests().map_err(TypesError::from)?;
    let changed_externals: BTreeSet<ObjectId> = current_ext_digests
        .iter()
        .filter(|(k, v)| cached_ext_digests.get(*k) != Some(*v))
        .filter_map(|(k, _)| k.parse().ok())
        .chain(
            cached_ext_digests
                .keys()
                .filter(|k| !current_ext_digests.contains_key(*k))
                .filter_map(|k| k.parse().ok()),
        )
        .collect();

    let reverse_graph = project.build_reverse_dependency_graph();

    let initial_dirty: BTreeSet<ObjectId> = typed_objects
        .keys()
        .filter(|id| {
            // 1. The view's own source changed.
            if project.compile_dirty.contains(id) {
                return true;
            }
            // 2. The view has no cached typecheck row — it was either never
            //    validated or its previous run failed. Either way, retry.
            if !cached_columns_by_key.contains_key(&id.to_string()) {
                return true;
            }
            // 3. A non-view direct dep was recompiled, or an external schema
            //    changed. View deps are deliberately ignored here — the DAG's
            //    schema-stability propagation handles those.
            let Some(deps) = project.dependency_graph.get(id) else {
                return false;
            };
            let external_schema_changed = |d: &ObjectId| changed_externals.contains(d);
            let non_view_dep_recompiled =
                |d: &ObjectId| project.compile_dirty.contains(d) && !typed_objects.contains_key(d);
            deps.iter()
                .any(|d| external_schema_changed(d) || non_view_dep_recompiled(d))
        })
        .cloned()
        .collect();

    // `pessimistic_dirty` = `initial_dirty` plus every transitive view
    // dependent — a schema change in an upstream view may cascade.
    let mut pessimistic_dirty: BTreeSet<ObjectId> = BTreeSet::new();
    let mut stack: Vec<ObjectId> = initial_dirty.iter().cloned().collect();
    while let Some(id) = stack.pop() {
        if !pessimistic_dirty.insert(id.clone()) {
            continue;
        }
        if let Some(downs) = reverse_graph.get(&id) {
            for d in downs {
                if typed_objects.contains_key(d) {
                    stack.push(d.clone());
                }
            }
        }
    }

    // Direct deps of `pessimistic_dirty`: view deps join the DAG; non-view
    // deps go into the bootstrap set. Clean DAG nodes skip-return their
    // cached columns, so transitive deps don't need expansion.
    let mut dag_nodes: BTreeSet<ObjectId> = pessimistic_dirty.clone();
    let mut bootstrap_set: BTreeSet<ObjectId> = BTreeSet::new();
    for id in &pessimistic_dirty {
        let Some(deps) = project.dependency_graph.get(id) else {
            continue;
        };
        for d in deps {
            if typed_objects.contains_key(d) {
                dag_nodes.insert(d.clone());
            } else {
                bootstrap_set.insert(d.clone());
            }
        }
    }

    let (base_catalog, base_columns) =
        bootstrap::bootstrap_catalog(project, &external_types, Some(&bootstrap_set))?;

    // Build the DAG only over `dag_nodes`. Direct-dep edges are filtered to
    // node IDs actually present in the DAG (other deps are already stubbed
    // into the base catalog above).
    let dag_node_ids: Vec<ObjectId> = typed_objects
        .keys()
        .filter(|id| dag_nodes.contains(id))
        .cloned()
        .collect();
    let mut direct_deps: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    let mut dependents: BTreeMap<ObjectId, Vec<ObjectId>> = BTreeMap::new();
    for node_id in &dag_node_ids {
        let node_deps = project
            .dependency_graph
            .get(node_id)
            .into_iter()
            .flatten()
            .filter(|d| dag_nodes.contains(d))
            .cloned()
            .collect();
        direct_deps.insert(node_id.clone(), node_deps);

        let node_dependents = reverse_graph
            .get(node_id)
            .into_iter()
            .flatten()
            .filter(|d| dag_nodes.contains(d))
            .cloned()
            .collect();
        dependents.insert(node_id.clone(), node_dependents);
    }

    let stats_counter = Arc::new(StatsCounter::default());

    let typed_objects = Arc::new(typed_objects);
    let cached_columns_by_key = Arc::new(cached_columns_by_key);
    let outcomes = {
        let typed_objects = Arc::clone(&typed_objects);
        let base_catalog = Arc::clone(&base_catalog);
        let initial_dirty = Arc::new(initial_dirty);
        let cached_columns_by_key = Arc::clone(&cached_columns_by_key);
        let stats_counter = Arc::clone(&stats_counter);
        executor::run::<NodeValue, _>(
            dag_node_ids.clone(),
            direct_deps,
            dependents,
            move |node_id, dep_results| {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for every scheduled node");

                let cached_columns = cached_columns_by_key.get(&node_id.to_string()).cloned();
                let any_dep_changed = dep_results.values().any(|v| !v.schema_stable);
                let must_typecheck =
                    initial_dirty.contains(node_id) || any_dep_changed || cached_columns.is_none();

                if !must_typecheck {
                    let columns = cached_columns.expect("must_typecheck guards None");
                    return Ok(NodeValue {
                        columns,
                        schema_stable: true,
                    });
                }

                let value = typecheck_node(
                    node_id,
                    db_obj,
                    Arc::clone(&base_catalog),
                    dep_results,
                    cached_columns.as_ref(),
                )?;
                stats_counter.record(value.schema_stable);
                Ok(value)
            },
        )
    };

    let mut errors: Vec<ObjectTypeCheckError> = Vec::new();
    let mut upsert_rows: Vec<(String, String, BTreeMap<String, ColumnType>)> = Vec::new();
    let mut merged_tables: BTreeMap<ObjectId, BTreeMap<String, ColumnType>> = BTreeMap::new();
    let mut merged_kinds: BTreeMap<ObjectId, ObjectKind> = BTreeMap::new();

    let project_kinds: BTreeMap<&ObjectId, ObjectKind> = project
        .iter_objects()
        .map(|obj| (&obj.id, obj.typed_object.stmt.kind()))
        .collect();
    for (id, columns) in base_columns.iter() {
        merged_tables.insert(id.clone(), columns.clone());
        if let Some(kind) = project_kinds.get(id) {
            merged_kinds.insert(id.clone(), *kind);
        }
    }
    for (id, columns) in &external_types.tables {
        merged_tables.insert(id.clone(), columns.clone());
        if let Some(kind) = external_types.kinds.get(id) {
            merged_kinds.insert(id.clone(), *kind);
        }
    }

    let mut unhealthy: BTreeSet<String> = BTreeSet::new();
    for node_id in typed_objects.keys() {
        let Some(outcome) = outcomes.get(node_id) else {
            continue;
        };
        match outcome {
            executor::NodeOutcome::Ok(value) => {
                let db_obj = typed_objects
                    .get(node_id)
                    .expect("typed_object exists for outcome");
                let kind = db_obj.stmt.kind();
                merged_tables.insert(node_id.clone(), value.columns.clone());
                merged_kinds.insert(node_id.clone(), kind);
                // Only persist nodes whose schema actually changed (or are
                // brand new). Skipped and schema-stable nodes already have a
                // matching row in the cache.
                if !value.schema_stable {
                    upsert_rows.push((
                        node_id.to_string(),
                        kind.as_str().to_string(),
                        value.columns.clone(),
                    ));
                }
            }
            executor::NodeOutcome::Failed(err) => {
                // Replace the catalog's synthesized placeholder path with the
                // real source path so diagnostics point at the user's file.
                let mut err = err.clone();
                if let Some(db_obj) = typed_objects.get(node_id) {
                    err.file_path = directory.join(&db_obj.path);
                }
                errors.push(err);
                unhealthy.insert(node_id.to_string());
            }
            executor::NodeOutcome::Blocked(blocker) => {
                verbose!(
                    "Skipping {}: blocked by upstream error in {}",
                    node_id,
                    blocker
                );
                unhealthy.insert(node_id.to_string());
            }
        }
    }

    // Drop cache rows for failed/blocked nodes. Without this, a previously
    // successful row would let a now-broken view skip typecheck on the next
    // run and silently report success.
    db.upsert_typecheck_results(&upsert_rows)
        .map_err(TypesError::from)?;
    let keep: BTreeSet<String> = typed_objects
        .keys()
        .map(|id| id.to_string())
        .filter(|key| !unhealthy.contains(key))
        .collect();
    db.prune_typecheck_results(&keep)
        .map_err(TypesError::from)?;
    db.replace_external_type_digests(&current_ext_digests)
        .map_err(TypesError::from)?;

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(errors));
    }

    let stats = stats_counter.snapshot(typed_objects.len());

    Ok((
        Types {
            version: 1,
            tables: merged_tables,
            kinds: merged_kinds,
            comments: BTreeMap::new(),
        },
        stats,
    ))
}

/// Lock-free per-node decision counters aggregated across the parallel executor.
#[derive(Default)]
struct StatsCounter {
    schema_stable: std::sync::atomic::AtomicUsize,
    schema_changed: std::sync::atomic::AtomicUsize,
}

impl StatsCounter {
    fn record(&self, schema_stable: bool) {
        let counter = if schema_stable {
            &self.schema_stable
        } else {
            &self.schema_changed
        };
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn snapshot(&self, total_nodes: usize) -> TypecheckStats {
        use std::sync::atomic::Ordering::Relaxed;
        let schema_stable = self.schema_stable.load(Relaxed);
        let schema_changed = self.schema_changed.load(Relaxed);
        let ran = schema_stable + schema_changed;
        TypecheckStats {
            ran,
            skipped: total_nodes.saturating_sub(ran),
            schema_stable,
            schema_changed,
        }
    }
}

fn typecheck_node(
    node_id: &ObjectId,
    db_obj: &crate::project::ir::compiled::DatabaseObject,
    base_catalog: Arc<catalog::CatalogRuntime>,
    dep_results: &BTreeMap<ObjectId, Arc<NodeValue>>,
    cached_columns: Option<&BTreeMap<String, ColumnType>>,
) -> Result<NodeValue, ObjectTypeCheckError> {
    let mut runtime = catalog::TaskCatalog::new(base_catalog);
    for (dep_id, dep_value) in dep_results {
        runtime
            .create_stub_table(dep_id, &dep_value.columns)
            .map_err(|err| {
                ObjectTypeCheckError::internal(
                    dep_id.clone(),
                    db_obj.path.clone(),
                    format!("failed to stub dependency: {err}"),
                )
            })?;
    }
    let fqn: FullyQualifiedName = node_id.clone().into();
    let ast = convert::create_catalog_item_ast(&db_obj.stmt, &fqn).ok_or_else(|| {
        ObjectTypeCheckError::internal(
            node_id.clone(),
            db_obj.path.clone(),
            "internal: failed to build catalog AST".into(),
        )
    })?;
    let desc = runtime.create_item_from_ast(node_id, ast)?;
    let columns = convert::relation_desc_to_columns(&desc);
    let schema_stable = cached_columns.is_some_and(|cached| cached == &columns);
    Ok(NodeValue {
        columns,
        schema_stable,
    })
}

/// SHA-256 digest of a column map, deterministic across runs because the
/// underlying `BTreeMap` iterates in sorted key order.
fn digest_columns(cols: &BTreeMap<String, ColumnType>) -> String {
    let mut hasher = Sha256::new();
    for (name, t) in cols {
        hasher.update(name.as_bytes());
        hasher.update(b"\0");
        hasher.update(t.r#type.as_bytes());
        hasher.update(b"\0");
        hasher.update([u8::from(t.nullable)]);
        hasher.update(b"\0");
        hasher.update(u64::try_from(t.position).unwrap_or(u64::MAX).to_le_bytes());
        hasher.update(b"\0");
    }
    format!("{:x}", hasher.finalize())
}

/// Per-external-table digests keyed by `ObjectId.to_string()`.
fn compute_external_digests(external_types: &Types) -> BTreeMap<String, String> {
    external_types
        .tables
        .iter()
        .map(|(id, cols)| (id.to_string(), digest_columns(cols)))
        .collect()
}

#[cfg(test)]
mod run_tests {
    use super::*;
    use crate::project::compiler::compile_sync;
    use std::collections::BTreeMap;
    use std::fs;
    use tempfile::tempdir;

    fn write_sql(root: &Path, rel: &str, sql: &str) {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, sql).unwrap();
    }

    #[test]
    fn run_typechecks_simple_view_and_persists_columns() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        // Tables (storage) and views (computation) must be in separate schemas.
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );

        let fs = crate::fs::FileSystem::new();
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (merged, _stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        assert!(
            merged
                .tables
                .contains_key(&"materialize.public.v1".parse::<ObjectId>().unwrap())
        );
        assert!(
            merged
                .tables
                .contains_key(&"materialize.storage.t1".parse::<ObjectId>().unwrap())
        );
    }

    /// A second `run` after no source change should typecheck zero nodes.
    #[test]
    fn second_run_skips_all_nodes_when_nothing_changed() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );
        write_sql(
            root,
            "models/materialize/public/v2.sql",
            "CREATE VIEW v2 AS SELECT a FROM materialize.public.v1",
        );

        let fs = crate::fs::FileSystem::new();
        // First run: prime the cache.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, first) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();
        assert_eq!(first.ran, 2, "first run should typecheck v1 and v2");
        assert_eq!(first.skipped, 0);

        // Second run: nothing changed, both views should be skipped.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, second) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();
        assert_eq!(second.ran, 0, "second run should skip everything");
        assert_eq!(second.skipped, 2);
    }

    /// Editing a leaf view in a way that doesn't change its output schema
    /// should re-typecheck the leaf but skip its dependents.
    #[test]
    fn schema_stable_edit_does_not_dirty_dependents() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );
        write_sql(
            root,
            "models/materialize/public/v2.sql",
            "CREATE VIEW v2 AS SELECT a FROM materialize.public.v1",
        );

        let fs = crate::fs::FileSystem::new();
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let _ = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        // Rewrite v1 in a way that produces the same column schema.
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM (SELECT * FROM materialize.storage.t1)",
        );
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        assert_eq!(stats.ran, 1, "only v1 should re-typecheck");
        assert_eq!(stats.schema_stable, 1, "v1 output unchanged");
        assert_eq!(stats.schema_changed, 0);
        assert_eq!(stats.skipped, 1, "v2 should skip on stable upstream");
    }

    /// Changing one external table's schema only dirties objects that depend
    /// on that specific table. Unrelated objects keep their cached results.
    #[test]
    fn external_type_change_dirties_only_consumers() {
        use crate::types::ObjectKind;

        let temp = tempdir().unwrap();
        let root = temp.path();
        // v_ext_a depends on ext.public.t_a; v_ext_b depends on ext.public.t_b.
        // Both are in storage so the project itself has no internal deps to
        // muddy the test.
        write_sql(
            root,
            "models/materialize/public/v_ext_a.sql",
            "CREATE VIEW v_ext_a AS SELECT a FROM ext.public.t_a",
        );
        write_sql(
            root,
            "models/materialize/public/v_ext_b.sql",
            "CREATE VIEW v_ext_b AS SELECT a FROM ext.public.t_b",
        );

        let mk_types = |a_type: &str, b_type: &str| {
            let mut tables: BTreeMap<ObjectId, BTreeMap<String, ColumnType>> = BTreeMap::new();
            let mut kinds: BTreeMap<ObjectId, ObjectKind> = BTreeMap::new();
            let t_a: ObjectId = "ext.public.t_a".parse().unwrap();
            let t_b: ObjectId = "ext.public.t_b".parse().unwrap();
            tables.insert(
                t_a.clone(),
                BTreeMap::from([(
                    "a".to_string(),
                    ColumnType {
                        r#type: a_type.to_string(),
                        nullable: true,
                        position: 0,
                        comment: None,
                    },
                )]),
            );
            tables.insert(
                t_b.clone(),
                BTreeMap::from([(
                    "a".to_string(),
                    ColumnType {
                        r#type: b_type.to_string(),
                        nullable: true,
                        position: 0,
                        comment: None,
                    },
                )]),
            );
            kinds.insert(t_a, ObjectKind::Table);
            kinds.insert(t_b, ObjectKind::Table);
            Types {
                version: 1,
                tables,
                kinds,
                comments: BTreeMap::new(),
            }
        };

        let fs = crate::fs::FileSystem::new();
        // Prime.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let _ = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            mk_types("integer", "integer"),
        )
        .unwrap();

        // Same externals → both views skip.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            mk_types("integer", "integer"),
        )
        .unwrap();
        assert_eq!(stats.skipped, 2, "no external change → both skip");
        assert_eq!(stats.ran, 0);

        // Change t_a's column type → only v_ext_a dirties.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            mk_types("text", "integer"),
        )
        .unwrap();
        assert_eq!(stats.ran, 1, "only v_ext_a should re-run");
        assert_eq!(stats.skipped, 1, "v_ext_b should skip");
    }

    /// A leaf edit that changes the output schema must cascade to dependents.
    #[test]
    fn schema_change_dirties_dependents() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int, b int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );
        write_sql(
            root,
            "models/materialize/public/v2.sql",
            "CREATE VIEW v2 AS SELECT * FROM materialize.public.v1",
        );

        let fs = crate::fs::FileSystem::new();
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let _ = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        // Add a column to v1's projection — its schema changes.
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a, b FROM materialize.storage.t1",
        );
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let (_, stats) = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        assert_eq!(stats.ran, 2, "v1 changed, v2 must re-run");
        assert_eq!(stats.schema_changed, 2);
        assert_eq!(stats.skipped, 0);
    }

    /// A view whose typecheck failed must be re-run on the next invocation,
    /// even if no source files changed. Otherwise an unfixed broken project
    /// would silently start passing on the second compile.
    #[test]
    fn previous_typecheck_failure_re_runs_next_invocation() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        // v1 references a column that doesn't exist on t1 — typecheck fails.
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT no_such_column FROM materialize.storage.t1",
        );

        let fs = crate::fs::FileSystem::new();
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let first = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        );
        assert!(first.is_err(), "first run should fail typechecking v1");

        // Second run: identical project, identical files. The failed view
        // must run again and surface the same error — not be skipped.
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let second = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        );
        assert!(
            second.is_err(),
            "second run must also fail — typecheck cache must not mask an unfixed error"
        );
    }

    /// Editing a previously-successful view to introduce a typecheck error must
    /// surface that error on every subsequent run — not just the first one.
    #[test]
    fn typecheck_failure_after_successful_run_persists() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );

        let fs = crate::fs::FileSystem::new();
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .expect("first run typechecks cleanly");

        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT no_such_column FROM materialize.storage.t1",
        );
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let second = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        );
        assert!(second.is_err(), "edit should surface typecheck error");

        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let third = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        );
        assert!(
            third.is_err(),
            "stale cache row from before the edit must not let a broken view skip typecheck"
        );
    }

    /// Editing a non-view object (e.g. a table) must invalidate dependent
    /// views' cached typecheck results, because the table's column schema
    /// flows into the catalog views are validated against.
    #[test]
    fn table_edit_dirties_dependent_view() {
        let temp = tempdir().unwrap();
        let root = temp.path();
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (a int)",
        );
        write_sql(
            root,
            "models/materialize/public/v1.sql",
            "CREATE VIEW v1 AS SELECT a FROM materialize.storage.t1",
        );

        let fs = crate::fs::FileSystem::new();
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let _ = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        )
        .unwrap();

        // Edit the table to remove the column the view depends on.
        write_sql(
            root,
            "models/materialize/storage/t1.sql",
            "CREATE TABLE t1 (b int)",
        );
        let project = compile_sync(&fs, root, None, None, &BTreeMap::new()).unwrap();
        let result = run(
            root,
            "default",
            None,
            &BTreeMap::new(),
            &project,
            Types::default(),
        );
        assert!(
            result.is_err(),
            "v1 references column `a` which no longer exists on t1; \
             dependent view must re-typecheck and surface the error"
        );
    }
}
