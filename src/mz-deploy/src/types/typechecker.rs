//! Type checking trait and Docker-based validation for Materialize projects.
//!
//! The [`TypeChecker`] trait defines a single `typecheck` method that validates
//! every object in a planned project. The core implementation lives in
//! [`typecheck_with_client`], which supports two paths:
//!
//! ## Table handling
//!
//! `CREATE TABLE` column schemas are derived from the SQL AST during type
//! checking — the table is created as a `CREATE TEMPORARY TABLE` in the
//! Docker container. This allows downstream views to reference project tables
//! without requiring them in `types.lock`.
//!
//! `CREATE TABLE ... FROM SOURCE` columns always come from `types.lock`,
//! because their schemas depend on the external source and cannot be derived
//! from the AST alone.
//!
//! A `CREATE TABLE` is created from the AST **if and only if** its FQN is not
//! already staged from `types.lock` (the `staged_fqns` parameter). This lets
//! `types.lock` override project table schemas when present.
//!
//! ## Full type check
//!
//! When no incremental state is available (first run or no snapshot), all
//! objects are validated:
//!
//! 1. Creates temporary tables/views for each project object in topological
//!    order (`CREATE TABLE` from AST, views/MVs via `CREATE TEMPORARY VIEW`).
//! 2. On success, queries column types for all views/MVs and writes
//!    `types.cache`.
//! 3. Collects per-object errors into [`TypeCheckErrors`] with rustc-style
//!    formatting.
//!
//! ## Incremental type check
//!
//! When a [`TypecheckPlan`](super::TypecheckPlan) carries a dirty set, only
//! changed objects are re-validated. Dirty propagation logic lives in the
//! sibling [`incremental`](super::incremental) module.

use super::incremental::{DirtyPropagator, IncrementalState, TypecheckPlan};
use crate::client::Client;
use crate::project::ast::Statement;
use crate::project::normalize::NormalizingVisitor;
use crate::project::object_id::ObjectId;
use crate::project::planned::Project;
use crate::project::typed::FullyQualifiedName;
use crate::types::ColumnType;
use crate::{timing, verbose};
use mz_sql_parser::ast::{
    ColumnOption, CreateViewStatement, Ident, IfExistsBehavior, UnresolvedItemName, ViewDefinition,
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

/// Type check a project using a pre-configured client.
///
/// Performs type checking by creating temporary views/tables for all project
/// objects in topological order. The client should already be connected and
/// have external dependencies staged as temporary tables.
///
/// `CreateTable` columns are derived from the SQL AST during type checking,
/// unless the table's FQN is already present in `types.lock` (passed via
/// `staged_fqns`). `CreateTableFromSource` tables always come from
/// `types.lock` because their columns depend on the external source.
///
/// The `plan` determines whether to use the incremental or full path. When
/// the plan carries a dirty set, only changed objects are re-validated;
/// otherwise all objects are fully type-checked.
///
/// # Arguments
/// * `client` - A connected client with external dependencies already staged
/// * `project` - The project to type check
/// * `project_root` - Root directory of the project (for error reporting)
/// * `staged_fqns` - FQNs already staged from `types.lock` (tables to skip)
/// * `plan` - The typecheck plan from [`plan_typecheck`](super::plan_typecheck)
///
/// # Returns
/// The [`TypecheckPlan`] on success (carries precomputed hashes for snapshot
/// writing), or Err with detailed errors.
pub async fn typecheck_with_client(
    client: &mut Client,
    project: &Project,
    project_root: &Path,
    staged_fqns: &BTreeSet<String>,
    plan: TypecheckPlan,
) -> Result<TypecheckPlan, TypeCheckError> {
    let object_paths = build_object_paths(project, project_root);
    let sorted_objects = project.get_sorted_objects()?;

    verbose!(
        "Type checking {} objects in topological order",
        sorted_objects.len()
    );

    match plan.state {
        Some(state) => {
            typecheck_incremental(
                client,
                project,
                project_root,
                &object_paths,
                &sorted_objects,
                staged_fqns,
                &plan.cached_types,
                state,
            )
            .await?;
        }
        None => {
            typecheck_full(
                client,
                project_root,
                &object_paths,
                &sorted_objects,
                staged_fqns,
            )
            .await?;
        }
    }

    Ok(TypecheckPlan {
        state: None,
        cached_types: plan.cached_types,
        current_hashes: plan.current_hashes,
    })
}

/// Full type check: validate every view/MV, then query and cache all column types.
///
/// `CreateTable` objects not in `staged_fqns` are created as temporary tables
/// from their AST, making their columns available for downstream views.
async fn typecheck_full(
    client: &Client,
    project_root: &Path,
    object_paths: &BTreeMap<ObjectId, PathBuf>,
    sorted_objects: &[(ObjectId, &crate::project::typed::DatabaseObject)],
    staged_fqns: &BTreeSet<String>,
) -> Result<(), TypeCheckError> {
    let mut errors = Vec::new();

    let create_start = std::time::Instant::now();
    for (object_id, typed_object) in sorted_objects {
        verbose!("Type checking: {}", object_id);

        // Skip CreateTable if already staged from types.lock
        if matches!(typed_object.stmt, Statement::CreateTable(_))
            && staged_fqns.contains(&object_id.to_string())
        {
            verbose!("  - Skipping table (staged from types.lock): {}", object_id);
            continue;
        }

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

    let _ = client.execute("DISCARD ALL;", &[]).await;

    timing!("  tc: create_objects", create_start.elapsed());

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

        let query_start = std::time::Instant::now();
        let internal_types = client
            .types()
            .query_internal_types(&view_object_ids, true)
            .await?;
        timing!("  tc: query_types", query_start.elapsed());

        let write_start = std::time::Instant::now();
        internal_types.write_types_cache(project_root)?;
        timing!("  tc: write_cache", write_start.elapsed());
        verbose!("Successfully wrote types.cache");
    }

    Ok(())
}

/// Incremental type check with lazy dependency creation and dirty propagation.
///
/// Only dirty views are type-checked. Dependencies are created **lazily** via
/// [`ensure_dep_exists`] — only when a dirty view actually needs them. Stubs
/// are self-contained (`CREATE TEMPORARY TABLE` with hardcoded columns), so
/// they don't require their own dependencies to exist. This reduces the number
/// of SQL statements from O(total objects) to O(dirty views + their direct deps).
///
/// After type-checking each dirty view, its output columns are compared to the
/// cached type hash. If the hash changed, immediate downstream dependents are
/// marked dirty via [`DirtyPropagator::report_columns`].
///
/// `CreateTable` objects not in `staged_fqns` are created lazily from their AST
/// when needed as a dependency.
async fn typecheck_incremental(
    client: &Client,
    project: &Project,
    project_root: &Path,
    object_paths: &BTreeMap<ObjectId, PathBuf>,
    sorted_objects: &[(ObjectId, &crate::project::typed::DatabaseObject)],
    staged_fqns: &BTreeSet<String>,
    cached_types: &super::Types,
    state: IncrementalState,
) -> Result<(), TypeCheckError> {
    let rev_start = std::time::Instant::now();
    let reverse_deps = project.build_reverse_dependency_graph();
    timing!("  tc: reverse_deps", rev_start.elapsed());
    let mut propagator = DirtyPropagator::new(state.dirty, cached_types.clone(), reverse_deps);
    let mut created: BTreeSet<String> = staged_fqns.clone();

    // Build object map for O(1) lookup by ObjectId
    let object_map: BTreeMap<ObjectId, &crate::project::typed::DatabaseObject> = sorted_objects
        .iter()
        .map(|(oid, obj)| (oid.clone(), *obj))
        .collect();

    let ctx = DepContext {
        cached_types,
        object_map: &object_map,
        dependency_graph: &project.dependency_graph,
        external_deps: &project.external_dependencies,
        staged_fqns,
        object_paths,
        project_root,
    };

    let mut errors = Vec::new();
    let mut dirty_count: usize = 0;
    let mut skip_count: usize = 0;
    let mut deps_time = std::time::Duration::ZERO;
    let mut check_time = std::time::Duration::ZERO;
    let mut columns_time = std::time::Duration::ZERO;

    for (object_id, typed_object) in sorted_objects {
        if !is_view_or_materialized_view(&typed_object.stmt) {
            continue; // Tables created lazily via ensure_dep_exists
        }

        if !propagator.is_dirty(object_id) {
            skip_count += 1;
            continue; // Clean objects created lazily when needed
        }

        // Lazily ensure all dependencies exist
        let dep_start = std::time::Instant::now();
        if let Some(deps) = project.dependency_graph.get(object_id) {
            for dep in deps {
                ensure_dep_exists(dep, &mut created, client, &ctx).await?;
            }
        }
        deps_time += dep_start.elapsed();

        // Type-check the dirty view
        verbose!("Type checking (dirty): {}", object_id);
        dirty_count += 1;

        let fqn = fqn_from_object_id(object_id);
        if let Some(statement) = create_temporary_view_sql(&typed_object.stmt, &fqn) {
            let sql = statement.to_string();
            let check_start = std::time::Instant::now();
            match client.execute(&sql, &[]).await {
                Ok(_) => {
                    check_time += check_start.elapsed();
                    verbose!("  ✓ Type check passed");

                    let col_start = std::time::Instant::now();
                    let new_columns = client.types().query_object_columns(object_id, true).await?;
                    columns_time += col_start.elapsed();

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
                    check_time += check_start.elapsed();
                    let error =
                        build_typecheck_error(object_id, &sql, &e, object_paths, project_root);
                    verbose!("  ✗ Type check failed: {}", error.error_message);
                    errors.push(error);
                }
            }
        }

        created.insert(object_id.to_string());
    }

    timing!("  tc: create_deps", deps_time);
    timing!("  tc: check_dirty", check_time);
    timing!("  tc: query_columns", columns_time);

    if !errors.is_empty() {
        return Err(TypeCheckError::Multiple(TypeCheckErrors { errors }));
    }

    verbose!("All type checks passed!");

    let stubs_created = created
        .len()
        .saturating_sub(dirty_count + staged_fqns.len());
    verbose!(
        "Incremental typecheck: {} type-checked, {} stubs created, {} objects skipped",
        dirty_count,
        stubs_created,
        skip_count
    );

    let view_ids: Vec<ObjectId> = sorted_objects
        .iter()
        .filter(|(_, typed_obj)| is_view_or_materialized_view(&typed_obj.stmt))
        .map(|(oid, _)| oid.clone())
        .collect();

    let merge_start = std::time::Instant::now();
    let merged_types = propagator.into_merged_cache(&view_ids);

    verbose!(
        "Caching types for {} view(s) to types.cache",
        merged_types.tables.len()
    );
    merged_types.write_types_cache(project_root)?;
    timing!("  tc: merge+write_cache", merge_start.elapsed());
    verbose!("Successfully wrote types.cache");

    Ok(())
}

/// Read-only context shared by lazy dependency creation functions.
///
/// Bundles the lookup tables and configuration that `ensure_dep_exists` and
/// `collect_deps_to_create` need, avoiding 7+ parameters on each call.
struct DepContext<'a> {
    cached_types: &'a super::Types,
    object_map: &'a BTreeMap<ObjectId, &'a crate::project::typed::DatabaseObject>,
    dependency_graph: &'a BTreeMap<ObjectId, BTreeSet<ObjectId>>,
    external_deps: &'a BTreeSet<ObjectId>,
    staged_fqns: &'a BTreeSet<String>,
    object_paths: &'a BTreeMap<ObjectId, PathBuf>,
    project_root: &'a Path,
}

/// Lazily create a dependency in Docker if it doesn't already exist.
///
/// - Tables: created from AST (self-contained)
/// - Views with cached columns: created as stub temp tables (self-contained)
/// - Views without cache (rare fallback): created as real views, which
///   requires recursively ensuring THEIR dependencies exist first
///
/// Stubs are self-contained (`CREATE TEMPORARY TABLE` with hardcoded columns),
/// so they do NOT require their own dependencies to exist.
async fn ensure_dep_exists(
    dep_id: &ObjectId,
    created: &mut BTreeSet<String>,
    client: &Client,
    ctx: &DepContext<'_>,
) -> Result<(), TypeCheckError> {
    let dep_fqn = dep_id.to_string();

    // Already created, staged from types.lock, or an external dependency
    if created.contains(&dep_fqn)
        || ctx.staged_fqns.contains(&dep_fqn)
        || ctx.external_deps.contains(dep_id)
    {
        return Ok(());
    }

    let typed_object = match ctx.object_map.get(dep_id) {
        Some(obj) => obj,
        None => return Ok(()), // External or unknown dep — skip
    };

    match &typed_object.stmt {
        Statement::CreateTable(_) => {
            // Tables are created from AST (self-contained)
            let fqn = fqn_from_object_id(dep_id);
            if let Some(statement) = create_temporary_view_sql(&typed_object.stmt, &fqn) {
                let sql = statement.to_string();
                client.execute(&sql, &[]).await.map_err(|e| {
                    let error =
                        build_typecheck_error(dep_id, &sql, &e, ctx.object_paths, ctx.project_root);
                    TypeCheckError::TypeCheckFailed(error)
                })?;
                verbose!("  ✓ Created temp table from AST (lazy): {}", dep_id);
            }
            created.insert(dep_fqn);
        }
        stmt if is_view_or_materialized_view(stmt) => {
            if let Some(cached_cols) = ctx.cached_types.get_table(&dep_fqn) {
                // View with cached columns — create as stub temp table (self-contained)
                create_stub_table(client, &dep_fqn, cached_cols).await?;
                verbose!("  ✓ Stubbed (lazy, cached): {}", dep_id);
                created.insert(dep_fqn);
            } else {
                // View without cache (rare fallback) — need to create as real view,
                // which requires its own dependencies to exist first.
                let deps_to_create = collect_deps_to_create(dep_id, created, ctx);

                // Create collected dependencies in dependency order
                for needed_id in &deps_to_create {
                    let needed_fqn = needed_id.to_string();
                    if created.contains(&needed_fqn) {
                        continue;
                    }
                    if let Some(needed_obj) = ctx.object_map.get(needed_id) {
                        if let Some(cached_cols) = ctx.cached_types.get_table(&needed_fqn) {
                            // Stub from cache
                            create_stub_table(client, &needed_fqn, cached_cols).await?;
                            verbose!("  ✓ Stubbed (lazy, cached): {}", needed_id);
                        } else {
                            // Create as real view/table
                            let fqn = fqn_from_object_id(needed_id);
                            if let Some(statement) =
                                create_temporary_view_sql(&needed_obj.stmt, &fqn)
                            {
                                let sql = statement.to_string();
                                client.execute(&sql, &[]).await.map_err(|e| {
                                    let error = build_typecheck_error(
                                        needed_id,
                                        &sql,
                                        &e,
                                        ctx.object_paths,
                                        ctx.project_root,
                                    );
                                    TypeCheckError::TypeCheckFailed(error)
                                })?;
                                verbose!("  ✓ Created (lazy, fallback): {}", needed_id);
                            }
                        }
                    }
                    created.insert(needed_fqn);
                }

                // Now create the dep itself as a real view
                let fqn = fqn_from_object_id(dep_id);
                if let Some(statement) = create_temporary_view_sql(&typed_object.stmt, &fqn) {
                    let sql = statement.to_string();
                    client.execute(&sql, &[]).await.map_err(|e| {
                        let error = build_typecheck_error(
                            dep_id,
                            &sql,
                            &e,
                            ctx.object_paths,
                            ctx.project_root,
                        );
                        TypeCheckError::TypeCheckFailed(error)
                    })?;
                    verbose!("  ✓ Created (lazy, fallback): {}", dep_id);
                }
                created.insert(dep_fqn);
            }
        }
        _ => {
            // Other statement types (sources, sinks, etc.) — skip
        }
    }

    Ok(())
}

/// Collect transitive dependencies that need to be created for a fallback view.
///
/// Performs a sync DFS walk. For cached views (stub-able), adds them to the result
/// but does **not** recurse into their deps (stubs are self-contained). For
/// non-cached views, recurses into their deps first (since they need real views).
/// Tables are added without recursion.
///
/// Returns objects in dependency order (post-order DFS).
fn collect_deps_to_create(
    dep_id: &ObjectId,
    created: &BTreeSet<String>,
    ctx: &DepContext<'_>,
) -> Vec<ObjectId> {
    let mut result = Vec::new();
    let mut visited = BTreeSet::new();
    collect_deps_dfs(dep_id, &mut result, &mut visited, created, ctx);
    result
}

/// Inner DFS for [`collect_deps_to_create`].
fn collect_deps_dfs(
    dep_id: &ObjectId,
    result: &mut Vec<ObjectId>,
    visited: &mut BTreeSet<String>,
    created: &BTreeSet<String>,
    ctx: &DepContext<'_>,
) {
    let dep_fqn = dep_id.to_string();

    if visited.contains(&dep_fqn)
        || created.contains(&dep_fqn)
        || ctx.staged_fqns.contains(&dep_fqn)
        || ctx.external_deps.contains(dep_id)
    {
        return;
    }
    visited.insert(dep_fqn);

    let typed_object = match ctx.object_map.get(dep_id) {
        Some(obj) => obj,
        None => return,
    };

    match &typed_object.stmt {
        Statement::CreateTable(_) => {
            // Tables are self-contained — add without recursion
            result.push(dep_id.clone());
        }
        stmt if is_view_or_materialized_view(stmt) => {
            if ctx.cached_types.get_table(&dep_id.to_string()).is_some() {
                // Cached view — stub is self-contained, no recursion needed
                result.push(dep_id.clone());
            } else {
                // Non-cached view — recurse into its deps first
                if let Some(deps) = ctx.dependency_graph.get(dep_id) {
                    for sub_dep in deps {
                        collect_deps_dfs(sub_dep, result, visited, created, ctx);
                    }
                }
                result.push(dep_id.clone());
            }
        }
        _ => {}
    }
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
        Statement::CreateTable(table) => {
            let mut table = table.clone();
            table.temporary = true;
            table.constraints.clear();
            table.with_options.clear();
            // Strip column-level FK/CHECK options (keep NOT NULL, DEFAULT, etc.)
            for col in &mut table.columns {
                col.options.retain(|opt| {
                    !matches!(
                        opt.option,
                        ColumnOption::ForeignKey { .. } | ColumnOption::Check(_)
                    )
                });
            }
            let normalized =
                Statement::CreateTable(table).normalize_name_with(&visitor, &fqn.to_item_name());
            Some(normalized)
        }
        Statement::CreateTableFromSource(_) => {
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
