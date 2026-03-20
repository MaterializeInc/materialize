//! Incremental type checking: plan computation, dirty propagation, and snapshot management.
//!
//! Before starting a Docker container (expensive), callers use [`plan_typecheck`]
//! to compare current AST hashes against the stored `typecheck.snapshot`. The
//! returned [`TypecheckPlan`] encapsulates whether any objects need re-checking
//! and carries cached column types for the "nothing changed" fast path.
//!
//! ## Plan computation
//!
//! [`plan_typecheck`] loads two artifacts from the build directory:
//! - `typecheck.snapshot` — maps fully-qualified object names to AST content hashes
//!   from the last successful typecheck.
//! - `types.cache` — column schemas for internal views from the last successful
//!   typecheck.
//!
//! It then computes current AST hashes for all views/MVs and diffs them against
//! the snapshot. If nothing changed, the plan is "up to date" and callers can
//! skip Docker entirely. Otherwise, the plan carries the dirty set for
//! [`typecheck_with_client`](super::typecheck_with_client).
//!
//! ## Dirty propagation
//!
//! [`DirtyPropagator`] implements the core incremental algorithm as a pure
//! function: track which objects are dirty, propagate dirtiness when type
//! hashes change, and merge final column caches. This is the "functional
//! core" — all I/O happens in the async caller loop in `typechecker.rs`.
//!
//! **Key insight:** Type-hash short-circuiting stops dirty propagation when a
//! view's output columns are unchanged despite an AST change. Editing a view's
//! internal logic without altering its output schema won't cascade re-checks to
//! downstream consumers.
//!
//! ## Lazy dependency creation
//!
//! The incremental path creates dependencies **lazily** — only when a dirty view
//! actually needs them. Instead of eagerly creating stubs for every clean view,
//! `typecheck_incremental` calls `ensure_dep_exists` for each direct dependency
//! of a dirty view just before type-checking it. Stubs are self-contained
//! (`CREATE TEMPORARY TABLE` with hardcoded columns), so they do NOT require
//! their own dependencies to exist. This reduces the number of SQL statements
//! from O(total objects) to O(dirty views + their direct deps).
//!
//! ## Snapshot management
//!
//! After a successful typecheck, [`write_snapshot`] writes the current AST
//! hashes to `typecheck.snapshot` so the next run can diff against them.

use crate::project::ast::Statement;
use crate::project::deployment_snapshot::compute_typed_hash;
use crate::project::object_id::ObjectId;
use crate::project::planned::Project;
use crate::types::{ColumnType, Types, TypesError, is_types_cache_stale, type_hash};
use crate::{timing, verbose};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// The result of comparing current project state against the typecheck snapshot.
///
/// Callers inspect [`is_up_to_date()`](TypecheckPlan::is_up_to_date) to decide
/// whether to start Docker. If up-to-date, [`into_cached_types()`](TypecheckPlan::into_cached_types)
/// returns the cached column schemas. Otherwise, the plan is passed to
/// [`typecheck_with_client`](super::typecheck_with_client) for execution.
pub struct TypecheckPlan {
    /// None = everything up to date; Some = needs checking.
    pub(super) state: Option<IncrementalState>,
    /// Cached types loaded from types.cache.
    pub(super) cached_types: Types,
    /// Precomputed AST hashes for all views/MVs, keyed by FQN.
    ///
    /// Populated during [`plan_typecheck`] so that [`write_snapshot_from_plan`]
    /// can write the snapshot without recomputing hashes.
    pub(super) current_hashes: BTreeMap<String, String>,
}

impl TypecheckPlan {
    /// Returns true if no objects need re-typechecking.
    pub fn is_up_to_date(&self) -> bool {
        self.state.is_none()
    }

    /// Consume the plan and return the cached types.
    ///
    /// Useful when `is_up_to_date()` is true and the caller just needs the types.
    pub fn into_cached_types(self) -> Types {
        self.cached_types
    }
}

/// The set of objects that need re-typechecking.
///
/// Extracted from the plan computation after diffing AST hashes against the
/// snapshot. Passed to [`typecheck_with_client`](super::typecheck_with_client)
/// alongside the `cached_types` from [`TypecheckPlan`].
pub(super) struct IncrementalState {
    /// Objects whose AST hash differs from the snapshot (need re-typechecking).
    pub(super) dirty: BTreeSet<ObjectId>,
}

/// Compare current AST hashes against the snapshot to build a typecheck plan.
///
/// Loads `typecheck.snapshot` and `types.cache` from the build directory,
/// computes current AST hashes for all views/MVs, and diffs them.
///
/// Returns a [`TypecheckPlan`] that is either up-to-date (nothing changed)
/// or contains the dirty set for incremental re-checking.
pub fn plan_typecheck(
    directory: &Path,
    planned_project: &Project,
) -> Result<TypecheckPlan, TypesError> {
    let cache_start = std::time::Instant::now();
    let cached_types = crate::types::load_types_cache(directory).unwrap_or_default();
    timing!("  plan: load_types_cache", cache_start.elapsed());

    let snap_start = std::time::Instant::now();
    let old_snapshot = crate::types::load_typecheck_snapshot(directory)?;
    timing!("  plan: load_snapshot", snap_start.elapsed());

    match old_snapshot {
        Some(old_hashes) => plan_from_snapshot(planned_project, cached_types, old_hashes),
        None => plan_without_snapshot(directory, planned_project, cached_types),
    }
}

/// Build a plan by diffing current AST hashes against an existing snapshot.
fn plan_from_snapshot(
    planned_project: &Project,
    cached_types: Types,
    old_hashes: BTreeMap<String, String>,
) -> Result<TypecheckPlan, TypesError> {
    let sort_start = std::time::Instant::now();
    let sorted = planned_project
        .get_sorted_objects()
        .map_err(TypesError::DependencyError)?;
    timing!("  plan: get_sorted", sort_start.elapsed());

    let hash_start = std::time::Instant::now();
    let mut current_hashes = BTreeMap::new();
    let mut dirty = BTreeSet::new();

    for (oid, typed_obj) in &sorted {
        if matches!(
            typed_obj.stmt,
            Statement::CreateView(_) | Statement::CreateMaterializedView(_)
        ) {
            let hash = compute_typed_hash(typed_obj);
            let fqn = oid.to_string();
            match old_hashes.get(&fqn) {
                Some(old_hash) if *old_hash == hash => {} // unchanged
                _ => {
                    dirty.insert(oid.clone());
                }
            }
            current_hashes.insert(fqn, hash);
        }
    }

    timing!(
        &format!("  plan: hash_views ({})", current_hashes.len()),
        hash_start.elapsed()
    );

    // Check for removed objects
    let current_fqns: BTreeSet<&String> = current_hashes.keys().collect();
    let old_fqns: BTreeSet<&String> = old_hashes.keys().collect();
    let has_removals = old_fqns.difference(&current_fqns).next().is_some();

    if dirty.is_empty() && !has_removals && !cached_types.tables.is_empty() {
        return Ok(TypecheckPlan {
            state: None,
            cached_types,
            current_hashes,
        });
    }

    verbose!(
        "Incremental typecheck: {} dirty object(s), {} removed",
        dirty.len(),
        old_fqns.difference(&current_fqns).count()
    );

    Ok(TypecheckPlan {
        state: Some(IncrementalState { dirty }),
        cached_types,
        current_hashes,
    })
}

/// Build a plan when no snapshot exists.
///
/// If a fresh `types.cache` exists (checked via filesystem mtime), the plan is
/// considered up-to-date — this handles the migration case where `types.cache`
/// was written before snapshot support was added.
///
/// Otherwise, all views/MVs are marked dirty for a full typecheck.
fn plan_without_snapshot(
    directory: &Path,
    planned_project: &Project,
    cached_types: Types,
) -> Result<TypecheckPlan, TypesError> {
    // Compute hashes for all views/MVs — needed for snapshot write after typecheck.
    let sort_start = std::time::Instant::now();
    let sorted = planned_project
        .get_sorted_objects()
        .map_err(TypesError::DependencyError)?;
    timing!("  plan: get_sorted", sort_start.elapsed());

    let hash_start = std::time::Instant::now();
    let mut current_hashes = BTreeMap::new();
    let mut dirty = BTreeSet::new();
    for (oid, typed_obj) in &sorted {
        if matches!(
            typed_obj.stmt,
            Statement::CreateView(_) | Statement::CreateMaterializedView(_)
        ) {
            current_hashes.insert(oid.to_string(), compute_typed_hash(typed_obj));
            dirty.insert(oid.clone());
        }
    }
    timing!(
        &format!("  plan: hash_views ({})", current_hashes.len()),
        hash_start.elapsed()
    );

    // If cache exists and is fresh, use it
    if !cached_types.tables.is_empty() && !is_types_cache_stale(directory) {
        return Ok(TypecheckPlan {
            state: None,
            cached_types,
            current_hashes,
        });
    }

    Ok(TypecheckPlan {
        state: Some(IncrementalState { dirty }),
        cached_types,
        current_hashes,
    })
}

/// Write the typecheck snapshot using precomputed hashes from a [`TypecheckPlan`].
///
/// This avoids recomputing AST hashes — they were already computed during
/// [`plan_typecheck`] and carried through the plan.
pub fn write_snapshot_from_plan(
    directory: &Path,
    plan: &TypecheckPlan,
) -> Result<(), TypesError> {
    crate::types::write_typecheck_snapshot(directory, &plan.current_hashes)
}

/// Write the typecheck snapshot after a successful typecheck.
///
/// Computes AST hashes for all views/MVs in the project and writes them to
/// `typecheck.snapshot` in the build directory.
pub fn write_snapshot(directory: &Path, planned_project: &Project) -> Result<(), TypesError> {
    let sorted = planned_project
        .get_sorted_objects()
        .map_err(TypesError::DependencyError)?;

    let mut hashes = BTreeMap::new();
    for (oid, typed_obj) in &sorted {
        if matches!(
            typed_obj.stmt,
            Statement::CreateView(_) | Statement::CreateMaterializedView(_)
        ) {
            hashes.insert(oid.to_string(), compute_typed_hash(typed_obj));
        }
    }

    crate::types::write_typecheck_snapshot(directory, &hashes)
}

/// Tracks dirty state and propagation during incremental type checking.
///
/// Encapsulates the pure logic of the incremental algorithm:
/// tracking dirty objects, propagating dirtiness when type hashes
/// change, and merging final column caches.
pub(super) struct DirtyPropagator {
    dirty: BTreeSet<ObjectId>,
    cached_types: Types,
    reverse_deps: BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    rechecked_columns: BTreeMap<String, BTreeMap<String, ColumnType>>,
}

impl DirtyPropagator {
    /// Create a new propagator from the dirty set, cached types, and reverse dependency graph.
    pub(super) fn new(
        dirty: BTreeSet<ObjectId>,
        cached_types: Types,
        reverse_deps: BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    ) -> Self {
        Self {
            dirty,
            cached_types,
            reverse_deps,
            rechecked_columns: BTreeMap::new(),
        }
    }

    /// Returns true if the object needs re-typechecking.
    pub(super) fn is_dirty(&self, object_id: &ObjectId) -> bool {
        self.dirty.contains(object_id)
    }

    /// Report the column types after successfully typechecking a dirty object.
    ///
    /// Compares type hash to cached value; if changed, marks immediate
    /// dependents dirty. Returns true if propagation occurred.
    pub(super) fn report_columns(
        &mut self,
        object_id: &ObjectId,
        columns: BTreeMap<String, ColumnType>,
    ) -> bool {
        let fqn_str = object_id.to_string();
        let new_type_hash = type_hash(&columns);
        let old_type_hash = self.cached_types.get_table(&fqn_str).map(type_hash);

        let propagated = if old_type_hash.as_ref() != Some(&new_type_hash) {
            if let Some(dependents) = self.reverse_deps.get(object_id) {
                for dep in dependents {
                    self.dirty.insert(dep.clone());
                }
            }
            true
        } else {
            false
        };

        self.rechecked_columns.insert(fqn_str, columns);
        propagated
    }

    /// Merge rechecked columns with cached columns to produce the final types.cache.
    ///
    /// Walks `sorted_view_ids` in order. For each object, rechecked columns take
    /// priority over cached columns. Objects present in neither are omitted.
    pub(super) fn into_merged_cache(mut self, sorted_view_ids: &[ObjectId]) -> Types {
        let mut merged_tables = BTreeMap::new();
        for object_id in sorted_view_ids {
            let fqn_str = object_id.to_string();
            if let Some(cols) = self.rechecked_columns.remove(&fqn_str) {
                merged_tables.insert(fqn_str, cols);
            } else if let Some(cols) = self.cached_types.get_table(&fqn_str) {
                merged_tables.insert(fqn_str, cols.clone());
            }
        }

        Types {
            version: 1,
            tables: merged_tables,
            kinds: BTreeMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnType, Types};

    fn oid(db: &str, schema: &str, obj: &str) -> ObjectId {
        ObjectId::new(db.to_string(), schema.to_string(), obj.to_string())
    }

    fn columns(pairs: &[(&str, &str, bool)]) -> BTreeMap<String, ColumnType> {
        pairs
            .iter()
            .map(|(name, typ, nullable)| {
                (
                    name.to_string(),
                    ColumnType {
                        r#type: typ.to_string(),
                        nullable: *nullable,
                    },
                )
            })
            .collect()
    }

    fn make_cached_types(entries: &[(&ObjectId, &BTreeMap<String, ColumnType>)]) -> Types {
        let mut tables = BTreeMap::new();
        for (oid, cols) in entries {
            tables.insert(oid.to_string(), (*cols).clone());
        }
        Types {
            version: 1,
            tables,
            kinds: BTreeMap::new(),
        }
    }

    fn make_reverse_deps(edges: &[(ObjectId, ObjectId)]) -> BTreeMap<ObjectId, BTreeSet<ObjectId>> {
        let mut map: BTreeMap<ObjectId, BTreeSet<ObjectId>> = BTreeMap::new();
        for (from, to) in edges {
            map.entry(from.clone()).or_default().insert(to.clone());
        }
        map
    }

    fn make_propagator(
        dirty: &[ObjectId],
        cached: &[(&ObjectId, &BTreeMap<String, ColumnType>)],
        edges: &[(ObjectId, ObjectId)],
    ) -> DirtyPropagator {
        DirtyPropagator::new(
            dirty.iter().cloned().collect(),
            make_cached_types(cached),
            make_reverse_deps(edges),
        )
    }

    // --- Propagation tests ---

    #[test]
    fn propagate_type_changed() {
        let a = oid("db", "sc", "a");
        let b = oid("db", "sc", "b");
        let old_cols = columns(&[("id", "integer", false)]);
        let new_cols = columns(&[("id", "bigint", false)]);

        let mut prop = make_propagator(&[a.clone()], &[(&a, &old_cols)], &[(a.clone(), b.clone())]);

        let propagated = prop.report_columns(&a, new_cols);
        assert!(propagated);
        assert!(prop.is_dirty(&b));
    }

    #[test]
    fn no_propagate_type_unchanged() {
        let a = oid("db", "sc", "a");
        let b = oid("db", "sc", "b");
        let cols = columns(&[("id", "integer", false)]);

        let mut prop = make_propagator(&[a.clone()], &[(&a, &cols)], &[(a.clone(), b.clone())]);

        let propagated = prop.report_columns(&a, cols.clone());
        assert!(!propagated);
        // b should still be clean
        assert!(!prop.is_dirty(&b));
    }

    #[test]
    fn propagate_cascades_through_chain() {
        let a = oid("db", "sc", "a");
        let b = oid("db", "sc", "b");
        let c = oid("db", "sc", "c");
        let old_cols = columns(&[("id", "integer", false)]);
        let new_cols_a = columns(&[("id", "bigint", false)]);
        let new_cols_b = columns(&[("id", "text", false)]);

        let mut prop = make_propagator(
            &[a.clone()],
            &[(&a, &old_cols), (&b, &old_cols), (&c, &old_cols)],
            &[(a.clone(), b.clone()), (b.clone(), c.clone())],
        );

        // Process A: type changes -> B becomes dirty
        assert!(prop.report_columns(&a, new_cols_a));
        assert!(prop.is_dirty(&b));

        // Process B: type changes -> C becomes dirty
        assert!(prop.report_columns(&b, new_cols_b));
        assert!(prop.is_dirty(&c));
    }

    #[test]
    fn propagate_stops_when_hash_stable() {
        let a = oid("db", "sc", "a");
        let b = oid("db", "sc", "b");
        let c = oid("db", "sc", "c");
        let old_cols = columns(&[("id", "integer", false)]);
        let new_cols_a = columns(&[("id", "bigint", false)]);

        let mut prop = make_propagator(
            &[a.clone()],
            &[(&a, &old_cols), (&b, &old_cols), (&c, &old_cols)],
            &[(a.clone(), b.clone()), (b.clone(), c.clone())],
        );

        // A type changes -> B dirty
        assert!(prop.report_columns(&a, new_cols_a));
        assert!(prop.is_dirty(&b));

        // B type unchanged -> C stays clean
        assert!(!prop.report_columns(&b, old_cols.clone()));
        assert!(!prop.is_dirty(&c));
    }

    #[test]
    fn propagate_diamond_dependency() {
        let a = oid("db", "sc", "a");
        let b = oid("db", "sc", "b");
        let c = oid("db", "sc", "c");
        let d = oid("db", "sc", "d");
        let old_cols = columns(&[("id", "integer", false)]);
        let new_cols = columns(&[("id", "bigint", false)]);

        let mut prop = make_propagator(
            &[a.clone()],
            &[
                (&a, &old_cols),
                (&b, &old_cols),
                (&c, &old_cols),
                (&d, &old_cols),
            ],
            &[(a.clone(), b.clone()), (a.clone(), c.clone())],
        );

        assert!(prop.report_columns(&a, new_cols));
        assert!(prop.is_dirty(&b));
        assert!(prop.is_dirty(&c));
        // D is not a dependent of A
        assert!(!prop.is_dirty(&d));
    }

    #[test]
    fn propagate_no_cached_type_always_propagates() {
        let a = oid("db", "sc", "a");
        let b = oid("db", "sc", "b");
        let new_cols = columns(&[("id", "integer", false)]);

        // No cached types for A at all
        let mut prop = make_propagator(&[a.clone()], &[], &[(a.clone(), b.clone())]);

        let propagated = prop.report_columns(&a, new_cols);
        assert!(propagated, "should propagate when no cached type exists");
        assert!(prop.is_dirty(&b));
    }

    #[test]
    fn propagate_multiple_dirty_roots() {
        let a = oid("db", "sc", "a");
        let b = oid("db", "sc", "b");
        let c = oid("db", "sc", "c");
        let d = oid("db", "sc", "d");
        let old_cols = columns(&[("id", "integer", false)]);
        let new_cols = columns(&[("id", "bigint", false)]);

        let mut prop = make_propagator(
            &[a.clone(), c.clone()],
            &[
                (&a, &old_cols),
                (&b, &old_cols),
                (&c, &old_cols),
                (&d, &old_cols),
            ],
            &[(a.clone(), b.clone()), (c.clone(), d.clone())],
        );

        // A type changes -> B dirty
        assert!(prop.report_columns(&a, new_cols));
        assert!(prop.is_dirty(&b));

        // C type unchanged -> D stays clean
        assert!(!prop.report_columns(&c, old_cols.clone()));
        assert!(!prop.is_dirty(&d));
    }

    // --- Merge tests ---

    #[test]
    fn merge_rechecked_overrides_cached() {
        let a = oid("db", "sc", "a");
        let cached_cols = columns(&[("id", "integer", false)]);
        let new_cols = columns(&[("id", "bigint", false)]);

        let mut prop = make_propagator(&[a.clone()], &[(&a, &cached_cols)], &[]);
        prop.report_columns(&a, new_cols.clone());

        let merged = prop.into_merged_cache(&[a.clone()]);
        assert_eq!(merged.get_table(&a.to_string()), Some(&new_cols));
    }

    #[test]
    fn merge_cached_used_for_clean() {
        let a = oid("db", "sc", "a");
        let cached_cols = columns(&[("id", "integer", false)]);

        let prop = make_propagator(&[], &[(&a, &cached_cols)], &[]);

        let merged = prop.into_merged_cache(&[a.clone()]);
        assert_eq!(merged.get_table(&a.to_string()), Some(&cached_cols));
    }

    #[test]
    fn merge_missing_from_both() {
        let a = oid("db", "sc", "a");
        let prop = make_propagator(&[], &[], &[]);

        let merged = prop.into_merged_cache(&[a.clone()]);
        assert_eq!(merged.get_table(&a.to_string()), None);
    }
}
