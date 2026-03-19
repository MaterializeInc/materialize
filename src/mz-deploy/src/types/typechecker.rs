//! Type checking trait and Docker-based validation for Materialize projects.
//!
//! The [`TypeChecker`] trait defines a single `typecheck` method that validates
//! every object in a planned project. The core implementation lives in
//! [`typecheck_with_client`], which supports two paths:
//!
//! ## Full type check (`incremental = None`)
//!
//! 1. Creates temporary tables for external dependencies using schemas from
//!    `types.lock`.
//! 2. Creates temporary views for each project object in topological order.
//! 3. On success, queries column types for all views/MVs and writes
//!    `types.cache`.
//! 4. Collects per-object errors into [`TypeCheckErrors`] with rustc-style
//!    formatting.
//!
//! ## Incremental type check (`incremental = Some(IncrementalState)`)
//!
//! Only re-validates objects whose AST changed since the last successful
//! typecheck, using dirty propagation to minimize work:
//!
//! 1. Walks objects in topological order. For each view/MV:
//!    - **Dirty** — Creates a temporary view, queries its output columns, and
//!      computes a [`type_hash`](crate::types::type_hash). If the type hash
//!      differs from the cached value, immediate downstream dependents are
//!      marked dirty (propagation).
//!    - **Clean with cached types** — Stubbed as a temporary table from
//!      `types.cache`, providing the correct column schema for downstream
//!      views without running the SQL.
//!    - **Clean without cache** — Falls back to full validation (shouldn't
//!      normally happen).
//! 2. On success, merges rechecked column types with cached types and writes
//!    the updated `types.cache`.
//!
//! **Key insight:** Type-hash short-circuiting stops dirty propagation when a
//! view's output columns are unchanged despite an AST change. This means
//! editing a view's internal logic without altering its output schema won't
//! cascade re-checks to downstream consumers.

use crate::client::Client;
use crate::project::ast::Statement;
use crate::project::normalize::NormalizingVisitor;
use crate::project::object_id::ObjectId;
use crate::project::planned::Project;
use crate::project::typed::FullyQualifiedName;
use crate::types::{ColumnType, Types, type_hash};
use crate::verbose;
use mz_sql_parser::ast::{
    CreateViewStatement, Ident, IfExistsBehavior, UnresolvedItemName, ViewDefinition,
};
use owo_colors::OwoColorize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Errors that can occur during type checking
#[derive(Debug, Error)]
pub enum TypeCheckError {
    /// Failed to start Docker container
    #[error("failed to start Materialize container: {0}")]
    ContainerStartFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Failed to connect to Materialize
    #[error("failed to connect to Materialize: {0}")]
    ConnectionFailed(#[from] crate::client::ConnectionError),

    /// Failed to create temporary tables for external dependencies
    #[error("failed to create temporary table for external dependency {object}: {source}")]
    ExternalDependencyFailed {
        object: ObjectId,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Type checking failed for an object
    #[error(transparent)]
    TypeCheckFailed(#[from] ObjectTypeCheckError),

    /// Multiple type check errors occurred
    #[error(transparent)]
    Multiple(#[from] TypeCheckErrors),

    /// Database error during setup
    #[error("database error during setup: {0}")]
    DatabaseSetupError(String),

    /// Failed to get sorted objects
    #[error("failed to get sorted objects: {0}")]
    SortError(#[from] crate::project::error::DependencyError),

    /// Failed to write types cache
    #[error("failed to write types cache: {0}")]
    TypesCacheWriteFailed(#[from] crate::types::TypesError),
}

/// A single type check error for a specific object
#[derive(Debug)]
pub struct ObjectTypeCheckError {
    /// The object that failed type checking
    pub object_id: ObjectId,
    /// The file path of the object
    pub file_path: PathBuf,
    /// The SQL statement that failed
    pub sql_statement: String,
    /// The database error message
    pub error_message: String,
    /// Optional detail from the database
    pub detail: Option<String>,
    /// Optional hint from the database
    pub hint: Option<String>,
}

impl fmt::Display for ObjectTypeCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Extract database/schema/file for path display
        let path_components: Vec<_> = self.file_path.components().collect();
        let len = path_components.len();

        let relative_path = if len >= 3 {
            format!(
                "{}/{}/{}",
                path_components[len - 3].as_os_str().to_string_lossy(),
                path_components[len - 2].as_os_str().to_string_lossy(),
                path_components[len - 1].as_os_str().to_string_lossy()
            )
        } else {
            self.file_path.display().to_string()
        };

        // Format like rustc errors
        writeln!(
            f,
            "{}: type check failed for '{}'",
            "error".bright_red().bold(),
            self.object_id
        )?;
        writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;
        writeln!(f)?;

        // Show the SQL statement (first few lines if long)
        let lines: Vec<_> = self.sql_statement.lines().collect();
        writeln!(f, "  {}", "|".bright_blue().bold())?;
        for (idx, line) in lines.iter().take(10).enumerate() {
            writeln!(f, "  {} {}", "|".bright_blue().bold(), line)?;
            if idx == 9 && lines.len() > 10 {
                writeln!(
                    f,
                    "  {} ... ({} more lines)",
                    "|".bright_blue().bold(),
                    lines.len() - 10
                )?;
                break;
            }
        }
        writeln!(f, "  {}", "|".bright_blue().bold())?;
        writeln!(f)?;

        // Show error message
        writeln!(
            f,
            "  {}: {}",
            "error".bright_red().bold(),
            self.error_message
        )?;

        if let Some(ref detail) = self.detail {
            writeln!(f, "  {}: {}", "detail".bright_cyan().bold(), detail)?;
        }

        if let Some(ref hint) = self.hint {
            writeln!(
                f,
                "  {} {}",
                "=".bright_blue().bold(),
                format!("hint: {}", hint).bold()
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for ObjectTypeCheckError {}

/// Collection of type check errors
#[derive(Debug)]
pub struct TypeCheckErrors {
    pub errors: Vec<ObjectTypeCheckError>,
}

impl fmt::Display for TypeCheckErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, error) in self.errors.iter().enumerate() {
            if idx > 0 {
                writeln!(f)?;
            }
            write!(f, "{}", error)?;
        }

        writeln!(f)?;
        writeln!(
            f,
            "{}: could not type check due to {} previous error{}",
            "error".bright_red().bold(),
            self.errors.len(),
            if self.errors.len() == 1 { "" } else { "s" }
        )?;

        Ok(())
    }
}

impl std::error::Error for TypeCheckErrors {}

/// Trait for type checking Materialize projects
#[async_trait::async_trait]
pub trait TypeChecker: Send + Sync {
    /// Type check a project, validating all object definitions
    ///
    /// This method:
    /// 1. Creates temporary tables/views for external dependencies (from types.lock)
    /// 2. Creates temporary views for all project objects in topological order
    /// 3. Reports any type errors found
    ///
    /// Returns Ok(()) if all objects pass type checking, or Err with detailed errors
    async fn typecheck(&self, project: &Project) -> Result<(), TypeCheckError>;
}

/// Bundles cached state for incremental type checking.
///
/// When provided to [`typecheck_with_client`], only objects in `dirty` are
/// actually type-checked against Materialize. Clean objects with cached column
/// types are stubbed as temporary tables (cheap). After each dirty object is
/// checked, its output columns are compared to the cached type hash; if the
/// type hash changed, immediate downstream dependents are marked dirty.
pub struct IncrementalState {
    /// Previously cached column types (from `types.cache`).
    pub cached_types: Types,
    /// Objects whose AST hash differs from the snapshot (need re-typechecking).
    pub dirty: BTreeSet<ObjectId>,
}

/// Type check a project using a pre-configured client.
///
/// Performs type checking by creating temporary views/tables for all project
/// objects in topological order. The client should already be connected and
/// have external dependencies staged as temporary tables.
///
/// When `incremental` is `Some`, only dirty objects are validated against
/// Materialize; clean objects are stubbed from cached column types. After
/// each dirty object is checked, its output columns are compared to the
/// cached type hash — if unchanged, downstream propagation stops.
///
/// When `incremental` is `None`, all objects are fully type-checked (the
/// original behavior).
///
/// # Arguments
/// * `client` - A connected client with external dependencies already staged
/// * `project` - The project to type check
/// * `project_root` - Root directory of the project (for error reporting)
/// * `incremental` - Optional incremental state for dirty propagation
///
/// # Returns
/// Ok(()) if all objects pass type checking, or Err with detailed errors
pub async fn typecheck_with_client(
    client: &mut Client,
    project: &Project,
    project_root: &Path,
    incremental: Option<IncrementalState>,
) -> Result<(), TypeCheckError> {
    let object_paths = build_object_paths(project, project_root);
    let sorted_objects = project.get_sorted_objects()?;

    verbose!(
        "Type checking {} objects in topological order",
        sorted_objects.len()
    );

    match incremental {
        Some(state) => {
            typecheck_incremental(
                client,
                project,
                project_root,
                &object_paths,
                &sorted_objects,
                state,
            )
            .await
        }
        None => typecheck_full(client, project_root, &object_paths, &sorted_objects).await,
    }
}

/// Full type check: validate every view/MV, then query and cache all column types.
async fn typecheck_full(
    client: &Client,
    project_root: &Path,
    object_paths: &BTreeMap<ObjectId, PathBuf>,
    sorted_objects: &[(ObjectId, &crate::project::typed::DatabaseObject)],
) -> Result<(), TypeCheckError> {
    let mut errors = Vec::new();

    for (object_id, typed_object) in sorted_objects {
        verbose!("Type checking: {}", object_id);

        let fqn = fqn_from_object_id(object_id);

        if let Some(statement) = create_temporary_view_sql(&typed_object.stmt, &fqn) {
            let sql = statement.to_string();
            match client.execute(&sql, &[]).await {
                Ok(_) => {
                    verbose!("  ✓ Type check passed");
                }
                Err(e) => {
                    let error =
                        build_typecheck_error(object_id, &sql, &e, object_paths, project_root);
                    verbose!("  ✗ Type check failed: {}", error.error_message);
                    errors.push(error);
                }
            }
        } else {
            verbose!("  - Skipping non-view/table object");
        }
    }

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(TypeCheckErrors { errors }));
    }

    verbose!("All type checks passed!");

    // Query types for all views/MVs and write cache
    let view_object_ids: Vec<&ObjectId> = sorted_objects
        .iter()
        .filter(|(_, typed_obj)| is_view_or_materialized_view(&typed_obj.stmt))
        .map(|(oid, _)| oid)
        .collect();

    if !view_object_ids.is_empty() {
        verbose!(
            "Caching types for {} view(s) to types.cache",
            view_object_ids.len()
        );

        let internal_types = client
            .types()
            .query_internal_types(&view_object_ids, true)
            .await?;

        internal_types.write_types_cache(project_root)?;
        verbose!("Successfully wrote types.cache");
    }

    Ok(())
}

/// What the async caller should do for a given object.
pub(crate) enum ObjectAction {
    /// Object is dirty — typecheck it, then call `report_columns`.
    CheckDirty,
    /// Object is clean with cached types — stub it as a temporary table.
    StubFromCache(BTreeMap<String, ColumnType>),
    /// Object is clean but has no cached types — full typecheck, then call `report_columns`.
    CheckFallback,
}

/// Tracks dirty state and propagation during incremental type checking.
///
/// Encapsulates the pure logic of the incremental algorithm:
/// classifying objects as dirty/clean/fallback, propagating dirtiness
/// when type hashes change, and merging final column caches.
pub(crate) struct DirtyPropagator {
    dirty: BTreeSet<ObjectId>,
    cached_types: Types,
    reverse_deps: BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    rechecked_columns: BTreeMap<String, BTreeMap<String, ColumnType>>,
}

impl DirtyPropagator {
    /// Create a new propagator from incremental state and reverse dependency graph.
    pub(crate) fn new(
        state: IncrementalState,
        reverse_deps: BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    ) -> Self {
        Self {
            dirty: state.dirty,
            cached_types: state.cached_types,
            reverse_deps,
            rechecked_columns: BTreeMap::new(),
        }
    }

    /// Classify what action to take for an object.
    pub(crate) fn classify(&self, object_id: &ObjectId) -> ObjectAction {
        if self.dirty.contains(object_id) {
            ObjectAction::CheckDirty
        } else if let Some(cached_cols) = self.cached_types.get_table(&object_id.to_string()) {
            ObjectAction::StubFromCache(cached_cols.clone())
        } else {
            ObjectAction::CheckFallback
        }
    }

    /// Report the column types after successfully typechecking a dirty object.
    ///
    /// Compares type hash to cached value; if changed, marks immediate
    /// dependents dirty. Returns true if propagation occurred.
    pub(crate) fn report_columns(
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

    /// Report columns for a fallback-checked object (no propagation needed).
    pub(crate) fn report_fallback_columns(
        &mut self,
        object_id: &ObjectId,
        columns: BTreeMap<String, ColumnType>,
    ) {
        self.rechecked_columns
            .insert(object_id.to_string(), columns);
    }

    /// Merge rechecked columns with cached columns to produce the final types.cache.
    ///
    /// Walks `sorted_view_ids` in order. For each object, rechecked columns take
    /// priority over cached columns. Objects present in neither are omitted.
    pub(crate) fn into_merged_cache(mut self, sorted_view_ids: &[ObjectId]) -> Types {
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

/// Incremental type check with dirty propagation and short-circuiting.
///
/// Walks objects in topological order. Dirty objects are validated via
/// `CREATE TEMPORARY VIEW` and their output columns are queried inline.
/// If the type hash changed compared to the cache, immediate downstream
/// dependents are marked dirty. Clean objects with cached types are
/// stubbed as temporary tables.
async fn typecheck_incremental(
    client: &Client,
    project: &Project,
    project_root: &Path,
    object_paths: &BTreeMap<ObjectId, PathBuf>,
    sorted_objects: &[(ObjectId, &crate::project::typed::DatabaseObject)],
    state: IncrementalState,
) -> Result<(), TypeCheckError> {
    let reverse_deps = project.build_reverse_dependency_graph();
    let mut propagator = DirtyPropagator::new(state, reverse_deps);

    let mut errors = Vec::new();

    for (object_id, typed_object) in sorted_objects {
        if !is_view_or_materialized_view(&typed_object.stmt) {
            verbose!("  - Skipping non-view/MV object: {}", object_id);
            continue;
        }

        let fqn = fqn_from_object_id(object_id);

        match propagator.classify(object_id) {
            ObjectAction::CheckDirty => {
                verbose!("Type checking (dirty): {}", object_id);

                if let Some(statement) = create_temporary_view_sql(&typed_object.stmt, &fqn) {
                    let sql = statement.to_string();
                    match client.execute(&sql, &[]).await {
                        Ok(_) => {
                            verbose!("  ✓ Type check passed");

                            let new_columns =
                                client.types().query_object_columns(object_id, true).await?;

                            if propagator.report_columns(object_id, new_columns) {
                                verbose!(
                                    "  ⚡ Type hash changed for {}, propagating to dependents",
                                    object_id
                                );
                            } else {
                                verbose!(
                                    "  ◈ Type hash unchanged for {}, skipping downstream",
                                    object_id
                                );
                            }
                        }
                        Err(e) => {
                            let error = build_typecheck_error(
                                object_id,
                                &sql,
                                &e,
                                object_paths,
                                project_root,
                            );
                            verbose!("  ✗ Type check failed: {}", error.error_message);
                            errors.push(error);
                        }
                    }
                }
            }
            ObjectAction::StubFromCache(cached_cols) => {
                verbose!("Stubbing (clean, cached): {}", object_id);
                create_stub_table(client, &object_id.to_string(), &cached_cols).await?;
            }
            ObjectAction::CheckFallback => {
                verbose!("Type checking (clean, no cache): {}", object_id);

                if let Some(statement) = create_temporary_view_sql(&typed_object.stmt, &fqn) {
                    let sql = statement.to_string();
                    match client.execute(&sql, &[]).await {
                        Ok(_) => {
                            verbose!("  ✓ Type check passed");
                            let new_columns =
                                client.types().query_object_columns(object_id, true).await?;
                            propagator.report_fallback_columns(object_id, new_columns);
                        }
                        Err(e) => {
                            let error = build_typecheck_error(
                                object_id,
                                &sql,
                                &e,
                                object_paths,
                                project_root,
                            );
                            verbose!("  ✗ Type check failed: {}", error.error_message);
                            errors.push(error);
                        }
                    }
                }
            }
        }
    }

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(TypeCheckErrors { errors }));
    }

    verbose!("All type checks passed!");

    let view_ids: Vec<ObjectId> = sorted_objects
        .iter()
        .filter(|(_, typed_obj)| is_view_or_materialized_view(&typed_obj.stmt))
        .map(|(oid, _)| oid.clone())
        .collect();

    let merged_types = propagator.into_merged_cache(&view_ids);

    verbose!(
        "Caching types for {} view(s) to types.cache",
        merged_types.tables.len()
    );
    merged_types.write_types_cache(project_root)?;
    verbose!("Successfully wrote types.cache");

    Ok(())
}

/// Build the FQN AST node from an ObjectId.
fn fqn_from_object_id(object_id: &ObjectId) -> FullyQualifiedName {
    FullyQualifiedName::from(UnresolvedItemName(vec![
        Ident::new(&object_id.database).expect("valid database"),
        Ident::new(&object_id.schema).expect("valid schema"),
        Ident::new(&object_id.object).expect("valid object"),
    ]))
}

/// Extract error details from a connection error and build an `ObjectTypeCheckError`.
fn build_typecheck_error(
    object_id: &ObjectId,
    sql: &str,
    e: &crate::client::ConnectionError,
    object_paths: &BTreeMap<ObjectId, PathBuf>,
    project_root: &Path,
) -> ObjectTypeCheckError {
    let (error_message, detail, hint) = match e {
        crate::client::ConnectionError::Query(pg_err) => {
            if let Some(db_err) = pg_err.as_db_error() {
                (
                    db_err.message().to_string(),
                    db_err.detail().map(|s| s.to_string()),
                    db_err.hint().map(|s| s.to_string()),
                )
            } else {
                (pg_err.to_string(), None, None)
            }
        }
        _ => (e.to_string(), None, None),
    };

    let path = object_paths.get(object_id).cloned().unwrap_or_else(|| {
        project_root
            .join(&object_id.database)
            .join(&object_id.schema)
            .join(format!("{}.sql", object_id.object))
    });

    ObjectTypeCheckError {
        object_id: object_id.clone(),
        file_path: path,
        sql_statement: sql.to_string(),
        error_message,
        detail,
        hint,
    }
}

/// Create a temporary table stub from cached column types.
///
/// This is used for clean objects during incremental type checking — the stub
/// provides the correct column schema so downstream views can reference it.
async fn create_stub_table(
    client: &Client,
    fqn: &str,
    columns: &BTreeMap<String, ColumnType>,
) -> Result<(), TypeCheckError> {
    let mut col_defs = Vec::new();
    for (col_name, col_type) in columns {
        let nullable = if col_type.nullable { "" } else { " NOT NULL" };
        col_defs.push(format!("{} {}{}", col_name, col_type.r#type, nullable));
    }

    let create_sql = format!(
        "CREATE TEMPORARY TABLE \"{}\" ({})",
        fqn,
        col_defs.join(", ")
    );

    client.execute(&create_sql, &[]).await.map_err(|e| {
        TypeCheckError::DatabaseSetupError(format!(
            "failed to create stub table for '{}': {}",
            fqn, e
        ))
    })?;

    Ok(())
}

/// Check if a statement is a view or materialized view
fn is_view_or_materialized_view(stmt: &Statement) -> bool {
    matches!(
        stmt,
        Statement::CreateView(_) | Statement::CreateMaterializedView(_)
    )
}

/// Build object path mapping for error reporting
fn build_object_paths(project: &Project, project_root: &Path) -> BTreeMap<ObjectId, PathBuf> {
    let mut paths = BTreeMap::new();
    for obj in project.iter_objects() {
        let path = project_root
            .join(&obj.id.database)
            .join(&obj.id.schema)
            .join(format!("{}.sql", obj.id.object));
        paths.insert(obj.id.clone(), path);
    }
    paths
}

/// Create temporary view for a project object
fn create_temporary_view_sql(stmt: &Statement, fqn: &FullyQualifiedName) -> Option<Statement> {
    let visitor = NormalizingVisitor::flattening(fqn);

    match stmt {
        Statement::CreateView(view) => {
            let mut view = view.clone();
            view.temporary = true;

            let normalized = Statement::CreateView(view)
                .normalize_name_with(&visitor, &fqn.to_item_name())
                .normalize_dependencies_with(&visitor);

            Some(normalized)
        }
        Statement::CreateMaterializedView(mv) => {
            let view_stmt = CreateViewStatement {
                if_exists: IfExistsBehavior::Error,
                temporary: true,
                definition: ViewDefinition {
                    name: mv.name.clone(),
                    columns: mv.columns.clone(),
                    query: mv.query.clone(),
                },
            };
            let normalized = Statement::CreateView(view_stmt)
                .normalize_name_with(&visitor, &fqn.to_item_name())
                .normalize_dependencies_with(&visitor);

            Some(normalized)
        }
        Statement::CreateTable(_) | Statement::CreateTableFromSource(_) => {
            // loaded from types.lock
            None
        }
        _ => {
            // Other statement types (sources, sinks, connections, secrets)
            // cannot be type-checked in this way
            None
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
        let state = IncrementalState {
            cached_types: make_cached_types(cached),
            dirty: dirty.iter().cloned().collect(),
        };
        DirtyPropagator::new(state, make_reverse_deps(edges))
    }

    // --- Classification tests ---

    #[test]
    fn classify_dirty_object() {
        let a = oid("db", "sc", "a");
        let cols_a = columns(&[("id", "integer", false)]);
        let prop = make_propagator(&[a.clone()], &[(&a, &cols_a)], &[]);

        assert!(matches!(prop.classify(&a), ObjectAction::CheckDirty));
    }

    #[test]
    fn classify_clean_with_cache() {
        let a = oid("db", "sc", "a");
        let cols_a = columns(&[("id", "integer", false)]);
        let prop = make_propagator(&[], &[(&a, &cols_a)], &[]);

        match prop.classify(&a) {
            ObjectAction::StubFromCache(cols) => assert_eq!(cols, cols_a),
            other => panic!("expected StubFromCache, got {:?}", action_name(&other)),
        }
    }

    #[test]
    fn classify_clean_no_cache() {
        let a = oid("db", "sc", "a");
        let prop = make_propagator(&[], &[], &[]);

        assert!(matches!(prop.classify(&a), ObjectAction::CheckFallback));
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
        assert!(matches!(prop.classify(&b), ObjectAction::CheckDirty));
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
        assert!(!matches!(prop.classify(&b), ObjectAction::CheckDirty));
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

        // Process A: type changes → B becomes dirty
        assert!(prop.report_columns(&a, new_cols_a));
        assert!(matches!(prop.classify(&b), ObjectAction::CheckDirty));

        // Process B: type changes → C becomes dirty
        assert!(prop.report_columns(&b, new_cols_b));
        assert!(matches!(prop.classify(&c), ObjectAction::CheckDirty));
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

        // A type changes → B dirty
        assert!(prop.report_columns(&a, new_cols_a));
        assert!(matches!(prop.classify(&b), ObjectAction::CheckDirty));

        // B type unchanged → C stays clean
        assert!(!prop.report_columns(&b, old_cols.clone()));
        assert!(!matches!(prop.classify(&c), ObjectAction::CheckDirty));
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
        assert!(matches!(prop.classify(&b), ObjectAction::CheckDirty));
        assert!(matches!(prop.classify(&c), ObjectAction::CheckDirty));
        // D is not a dependent of A
        assert!(!matches!(prop.classify(&d), ObjectAction::CheckDirty));
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
        assert!(matches!(prop.classify(&b), ObjectAction::CheckDirty));
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

        // A type changes → B dirty
        assert!(prop.report_columns(&a, new_cols));
        assert!(matches!(prop.classify(&b), ObjectAction::CheckDirty));

        // C type unchanged → D stays clean
        assert!(!prop.report_columns(&c, old_cols.clone()));
        assert!(!matches!(prop.classify(&d), ObjectAction::CheckDirty));
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

    /// Helper to name ObjectAction variants for debug output.
    fn action_name(action: &ObjectAction) -> &'static str {
        match action {
            ObjectAction::CheckDirty => "CheckDirty",
            ObjectAction::StubFromCache(_) => "StubFromCache",
            ObjectAction::CheckFallback => "CheckFallback",
        }
    }
}
