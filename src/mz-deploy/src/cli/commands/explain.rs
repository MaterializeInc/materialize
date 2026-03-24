//! Explain command — show the EXPLAIN plan for a materialized view or index.
//!
//! This command compiles the project, stages the target object's dependencies
//! in a dedicated schema on the live Materialize instance, creates the target,
//! and runs `EXPLAIN` to show the query plan.
//!
//! ## Target Format
//!
//! `database.schema.object` — explain a materialized view
//! `database.schema.object#index_name` — explain a specific index
//!
//! ## Dependency Staging Algorithm
//!
//! For each dependency of the target object:
//!
//! 1. If the dependency has indexes on the **same cluster** as the target →
//!    stub as TABLE + create those matching indexes on `quickstart`.
//! 2. Else if the dependency is a materialized view, table, or table-from-source →
//!    stub as TABLE only.
//! 3. Else (plain view) → recursively stage its dependencies, then create it.
//!
//! All `IN CLUSTER` clauses are rewritten to `quickstart` via the
//! [`ExplainTransformer`](crate::project::normalize::ExplainTransformer).
//!
//! ## Schema Lifecycle
//!
//! A dedicated schema `_mz_explain_<uuid>` is created before staging and
//! dropped with `CASCADE` after completion (even on error).

use crate::cli::CliError;
use crate::cli::commands::compile;
use crate::client::Client;
use crate::client::quote_identifier;
use crate::config::Settings;
use crate::project::ast::Statement;
use crate::project::normalize::NormalizingVisitor;
use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::project::typed::FullyQualifiedName;
use crate::types::{ColumnType, Types};
use crate::verbose;
use mz_sql_parser::ast::*;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use tokio_postgres::SimpleQueryMessage;

/// The parsed explain target: an object and optional index name.
struct ExplainTarget {
    object_id: ObjectId,
    index_name: Option<String>,
}

/// Actions to stage dependencies before running EXPLAIN.
enum StagingAction {
    /// Create a stub TABLE from column schemas.
    StubTable {
        object_id: ObjectId,
        columns: BTreeMap<String, ColumnType>,
    },
    /// Create an index on a previously stubbed table.
    CreateIndex {
        index: CreateIndexStatement<Raw>,
        on_object: ObjectId,
    },
    /// Create the actual view (for plain views in the "else" case).
    CreateView {
        object_id: ObjectId,
        stmt: Statement,
    },
}

/// Output of the explain command.
#[derive(Serialize)]
struct ExplainOutput {
    object: String,
    explain_output: String,
}

impl fmt::Display for ExplainOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.explain_output)
    }
}

/// Run the explain command.
///
/// Compiles the project, stages dependencies in a temporary schema on the live
/// Materialize instance, creates the target object, runs EXPLAIN, and cleans up.
pub async fn run(settings: &Settings, target: &str) -> Result<(), CliError> {
    let target = parse_target(target)?;

    // Compile with type checking to populate types.cache
    let project = compile::run(settings, false, false).await?;

    // Find the target object in the planned project
    let planned_obj = project.find_object(&target.object_id).ok_or_else(|| {
        CliError::Message(format!(
            "object '{}' not found in project",
            target.object_id
        ))
    })?;

    // Validate target type and find the target cluster
    let target_cluster = validate_target(planned_obj, &target)?;

    // Load column schemas for stub tables
    let types = load_types(&settings.directory)?;

    // Connect to live Materialize
    let client = Client::connect_with_profile(settings.connection().clone())
        .await
        .map_err(CliError::Connection)?;

    // Build the staging plan (pure core)
    let actions = plan_staging(&project, &target, &target_cluster, &types)?;

    // Generate a unique schema name using timestamp + random suffix
    let explain_schema = format!(
        "_mz_explain_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
    );
    let explain_db = &target.object_id.database;

    // Execute the explain plan with cleanup
    let result = execute_explain(
        &client,
        explain_db,
        &explain_schema,
        &actions,
        &target,
        &planned_obj.typed_object,
        &target_cluster,
    )
    .await;

    // Always drop the schema (best effort)
    let drop_sql = format!(
        "DROP SCHEMA IF EXISTS {}.{} CASCADE",
        quote_identifier(explain_db),
        quote_identifier(&explain_schema),
    );
    verbose!("Cleanup: {}", drop_sql);
    let _ = client.execute(&drop_sql, &[]).await;

    let explain_text = result?;

    let output = ExplainOutput {
        object: target.object_id.to_string(),
        explain_output: explain_text,
    };
    crate::log::output(&output);

    Ok(())
}

/// Parse a target string like `database.schema.object` or `database.schema.object#index`.
fn parse_target(target: &str) -> Result<ExplainTarget, CliError> {
    let (object_part, index_name) = match target.split_once('#') {
        Some((obj, idx)) => (obj, Some(idx.to_string())),
        None => (target, None),
    };

    let parts: Vec<&str> = object_part.split('.').collect();
    if parts.len() != 3 {
        return Err(CliError::Message(format!(
            "expected fully qualified name 'database.schema.object', got '{}'",
            object_part
        )));
    }

    Ok(ExplainTarget {
        object_id: ObjectId {
            database: parts[0].to_string(),
            schema: parts[1].to_string(),
            object: parts[2].to_string(),
        },
        index_name,
    })
}

/// Validate the target is an MV or index and return the target cluster name.
fn validate_target(
    planned_obj: &planned::DatabaseObject,
    target: &ExplainTarget,
) -> Result<String, CliError> {
    match &target.index_name {
        None => {
            // Must be a materialized view
            match &planned_obj.typed_object.stmt {
                Statement::CreateMaterializedView(mv) => {
                    let cluster = mv
                        .in_cluster
                        .as_ref()
                        .expect("materialized view must have IN CLUSTER")
                        .to_string();
                    Ok(cluster)
                }
                other => Err(CliError::Message(format!(
                    "'{}' is a {}, but explain without #index only supports materialized views",
                    target.object_id,
                    other.kind()
                ))),
            }
        }
        Some(index_name) => {
            // Find the named index
            let index = planned_obj
                .typed_object
                .indexes
                .iter()
                .find(|idx| {
                    idx.name
                        .as_ref()
                        .map(|n| n.to_string() == *index_name)
                        .unwrap_or(false)
                })
                .ok_or_else(|| {
                    let available: Vec<String> = planned_obj
                        .typed_object
                        .indexes
                        .iter()
                        .filter_map(|idx| idx.name.as_ref().map(|n| n.to_string()))
                        .collect();
                    CliError::Message(format!(
                        "index '{}' not found on '{}'. Available indexes: {}",
                        index_name,
                        target.object_id,
                        if available.is_empty() {
                            "(none)".to_string()
                        } else {
                            available.join(", ")
                        }
                    ))
                })?;

            let cluster = index
                .in_cluster
                .as_ref()
                .expect("index must have IN CLUSTER")
                .to_string();
            Ok(cluster)
        }
    }
}

/// Load and merge types.lock + types.cache for stub table column schemas.
fn load_types(directory: &std::path::Path) -> Result<Types, CliError> {
    let mut types = crate::types::load_types_lock(directory).unwrap_or_default();
    match crate::types::load_types_cache(directory) {
        Ok(cache) => types.merge(&cache),
        Err(_) => {
            verbose!("No types.cache found; stub tables will use types.lock and AST only");
        }
    }
    Ok(types)
}

/// Build the staging actions for all transitive dependencies of the target.
///
/// This is a pure function (no I/O). It walks the dependency graph and
/// classifies each dependency according to the staging algorithm:
///
/// 1. Has indexes on the target's cluster → stub TABLE + create those indexes
/// 2. Is MV/table/table-from-source → stub TABLE only
/// 3. Is a plain view → recursively stage deps, then create the view
fn plan_staging(
    project: &planned::Project,
    target: &ExplainTarget,
    target_cluster: &str,
    types: &Types,
) -> Result<Vec<StagingAction>, CliError> {
    let mut actions = Vec::new();
    let mut visited = BTreeSet::new();

    // Get the target's direct dependencies
    let target_deps = project
        .dependency_graph
        .get(&target.object_id)
        .cloned()
        .unwrap_or_default();

    for dep_id in &target_deps {
        plan_dep(
            project,
            dep_id,
            target_cluster,
            types,
            &mut actions,
            &mut visited,
        )?;
    }

    Ok(actions)
}

/// Recursively plan staging for a single dependency.
fn plan_dep(
    project: &planned::Project,
    dep_id: &ObjectId,
    target_cluster: &str,
    types: &Types,
    actions: &mut Vec<StagingAction>,
    visited: &mut BTreeSet<ObjectId>,
) -> Result<(), CliError> {
    if visited.contains(dep_id) {
        return Ok(());
    }
    visited.insert(dep_id.clone());

    // External dependencies get stubbed if we have their types
    if project.external_dependencies.contains(dep_id) {
        let columns = get_columns_for_stub(dep_id, None, types)?;
        actions.push(StagingAction::StubTable {
            object_id: dep_id.clone(),
            columns,
        });
        return Ok(());
    }

    let planned_obj = project.find_object(dep_id).ok_or_else(|| {
        CliError::Message(format!("dependency '{}' not found in project", dep_id))
    })?;

    // Check if the dependency has indexes on the target's cluster
    let matching_indexes: Vec<_> = planned_obj
        .typed_object
        .indexes
        .iter()
        .filter(|idx| {
            idx.in_cluster
                .as_ref()
                .map(|c| c.to_string() == target_cluster)
                .unwrap_or(false)
        })
        .cloned()
        .collect();

    if !matching_indexes.is_empty() {
        // Case 1: Stub as table + create matching indexes
        let columns = get_columns_for_stub(dep_id, Some(&planned_obj.typed_object.stmt), types)?;
        actions.push(StagingAction::StubTable {
            object_id: dep_id.clone(),
            columns,
        });
        for index in matching_indexes {
            actions.push(StagingAction::CreateIndex {
                index,
                on_object: dep_id.clone(),
            });
        }
    } else {
        match planned_obj.typed_object.stmt.kind() {
            crate::types::ObjectKind::MaterializedView | crate::types::ObjectKind::Table => {
                // Case 2: Stub as table only
                let columns =
                    get_columns_for_stub(dep_id, Some(&planned_obj.typed_object.stmt), types)?;
                actions.push(StagingAction::StubTable {
                    object_id: dep_id.clone(),
                    columns,
                });
            }
            crate::types::ObjectKind::View => {
                // Case 3: Recursively stage this view's dependencies, then create it
                let view_deps = project
                    .dependency_graph
                    .get(dep_id)
                    .cloned()
                    .unwrap_or_default();
                for sub_dep_id in &view_deps {
                    plan_dep(project, sub_dep_id, target_cluster, types, actions, visited)?;
                }
                actions.push(StagingAction::CreateView {
                    object_id: dep_id.clone(),
                    stmt: planned_obj.typed_object.stmt.clone(),
                });
            }
            kind => {
                return Err(CliError::Message(format!(
                    "dependency '{}' is a {} which cannot be staged for explain",
                    dep_id, kind
                )));
            }
        }
    }

    Ok(())
}

/// Get column schemas for a stub table.
///
/// Tries, in order:
/// 1. `types` (types.lock + types.cache merged)
/// 2. `CREATE TABLE` AST columns (if the statement is a CreateTable)
fn get_columns_for_stub(
    object_id: &ObjectId,
    stmt: Option<&Statement>,
    types: &Types,
) -> Result<BTreeMap<String, ColumnType>, CliError> {
    let fqn = object_id.to_string();

    // Try types.lock / types.cache first
    if let Some(columns) = types.get_table(&fqn) {
        return Ok(columns.clone());
    }

    // Try deriving from CREATE TABLE AST
    if let Some(Statement::CreateTable(table)) = stmt {
        let mut columns = BTreeMap::new();
        for (position, col) in table.columns.iter().enumerate() {
            let nullable = !col
                .options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull));
            columns.insert(
                col.name.to_string(),
                ColumnType {
                    r#type: col.data_type.to_string(),
                    nullable,
                    position,
                },
            );
        }
        return Ok(columns);
    }

    Err(CliError::Message(format!(
        "no column schema available for '{}'. Run 'mz-deploy compile' to populate types.cache",
        object_id
    )))
}

/// Execute the staging actions, create the target, and run EXPLAIN.
async fn execute_explain(
    client: &Client,
    explain_db: &str,
    explain_schema: &str,
    actions: &[StagingAction],
    target: &ExplainTarget,
    target_typed_obj: &crate::project::typed::DatabaseObject,
    target_cluster: &str,
) -> Result<String, CliError> {
    // Create the explain schema
    let create_schema_sql = format!(
        "CREATE SCHEMA {}.{}",
        quote_identifier(explain_db),
        quote_identifier(explain_schema),
    );
    verbose!("Creating explain schema: {}", create_schema_sql);
    client
        .execute(&create_schema_sql, &[])
        .await
        .map_err(|e| CliError::Message(format!("failed to create explain schema: {}", e)))?;

    // Execute staging actions
    for action in actions {
        match action {
            StagingAction::StubTable { object_id, columns } => {
                let fqn = object_id.to_string();
                let mut col_defs = Vec::new();
                for (col_name, col_type) in columns {
                    let nullable = if col_type.nullable { "" } else { " NOT NULL" };
                    col_defs.push(format!(
                        "{} {}{}",
                        quote_identifier(col_name),
                        col_type.r#type,
                        nullable
                    ));
                }
                let sql = format!(
                    "CREATE TABLE {}.{}.{} ({})",
                    quote_identifier(explain_db),
                    quote_identifier(explain_schema),
                    quote_identifier(&fqn),
                    col_defs.join(", ")
                );
                verbose!("Stub table: {}", sql);
                client.execute(&sql, &[]).await.map_err(|e| {
                    CliError::Message(format!(
                        "failed to create stub table for '{}': {}",
                        object_id, e
                    ))
                })?;
            }
            StagingAction::CreateIndex { index, on_object } => {
                let sql = build_index_sql(index, on_object, explain_db, explain_schema);
                verbose!("Create index: {}", sql);
                client
                    .execute(&sql, &[])
                    .await
                    .map_err(|e| CliError::Message(format!("failed to create index: {}", e)))?;
            }
            StagingAction::CreateView { object_id, stmt } => {
                let sql = build_view_sql(stmt, object_id, explain_db, explain_schema);
                verbose!("Create view: {}", sql);
                client.execute(&sql, &[]).await.map_err(|e| {
                    CliError::Message(format!("failed to create view '{}': {}", object_id, e))
                })?;
            }
        }
    }

    // Create the target object
    create_target(client, explain_db, explain_schema, target, target_typed_obj).await?;

    // Run EXPLAIN
    let explain_sql = build_explain_sql(target, explain_db, explain_schema);
    verbose!("Running: {}", explain_sql);
    let messages = client
        .simple_query(&explain_sql)
        .await
        .map_err(|e| CliError::Message(format!("EXPLAIN failed: {}", e)))?;

    let lines = extract_text_from_messages(messages);
    let text = lines.join("\n");

    // Strip the temporary schema prefix from the output so users see clean object names.
    // Materialize's EXPLAIN output uses unquoted identifiers, so match both forms.
    let quoted_prefix = format!(
        "{}.{}.",
        quote_identifier(explain_db),
        quote_identifier(explain_schema),
    );
    let unquoted_prefix = format!("{}.{}.", explain_db, explain_schema);
    let text = text
        .replace(&quoted_prefix, "")
        .replace(&unquoted_prefix, "")
        .replace(
            "Target cluster: quickstart",
            &format!("Target cluster: {}", target_cluster),
        );
    Ok(text)
}

/// Create the target object (MV + indexes if explaining an index).
async fn create_target(
    client: &Client,
    explain_db: &str,
    explain_schema: &str,
    target: &ExplainTarget,
    typed_obj: &crate::project::typed::DatabaseObject,
) -> Result<(), CliError> {
    let fqn = fqn_from_object_id(&target.object_id);
    let mut visitor =
        NormalizingVisitor::explain(&fqn, explain_db.to_string(), explain_schema.to_string());

    // Create the main statement
    match &typed_obj.stmt {
        Statement::CreateMaterializedView(_) => {
            let normalized = typed_obj
                .stmt
                .clone()
                .normalize_name_with(&visitor, &fqn.to_item_name())
                .normalize_dependencies_with(&mut visitor)
                .normalize_cluster_with(&visitor);
            let sql = normalized.to_string();
            verbose!("Create target MV: {}", sql);
            client.execute(&sql, &[]).await.map_err(|e| {
                CliError::Message(format!(
                    "failed to create target '{}': {}",
                    target.object_id, e
                ))
            })?;
        }
        other => {
            // For index targets, the parent object might not be an MV — stub it
            // and we only create indexes below
            if target.index_name.is_some() {
                // The parent object was already handled by the staging actions
                // (it's a dependency of itself in a sense, but actually the indexes
                // are ON this object). We need to make sure it exists in the explain
                // schema. If it's an MV/table, it was stubbed. If it's something
                // else, create it.
                match other.kind() {
                    crate::types::ObjectKind::MaterializedView
                    | crate::types::ObjectKind::Table => {
                        // Already stubbed as a table by the caller — nothing to do
                    }
                    crate::types::ObjectKind::View => {
                        let normalized = other
                            .clone()
                            .normalize_name_with(&visitor, &fqn.to_item_name())
                            .normalize_dependencies_with(&mut visitor);
                        let sql = normalized.to_string();
                        verbose!("Create target view: {}", sql);
                        client.execute(&sql, &[]).await.map_err(|e| {
                            CliError::Message(format!(
                                "failed to create target '{}': {}",
                                target.object_id, e
                            ))
                        })?;
                    }
                    kind => {
                        return Err(CliError::Message(format!(
                            "'{}' is a {} — cannot create in explain schema",
                            target.object_id, kind
                        )));
                    }
                }
            } else {
                return Err(CliError::Message(format!(
                    "'{}' is a {} — explain only supports materialized views",
                    target.object_id,
                    other.kind()
                )));
            }
        }
    }

    // If explaining an index, create all indexes on the target
    if target.index_name.is_some() {
        let mut indexes = typed_obj.indexes.clone();
        visitor.normalize_index_references(&mut indexes);
        visitor.normalize_index_clusters(&mut indexes);
        for index in &indexes {
            let sql = index.to_string();
            verbose!("Create target index: {}", sql);
            client
                .execute(&sql, &[])
                .await
                .map_err(|e| CliError::Message(format!("failed to create index: {}", e)))?;
        }
    }

    Ok(())
}

/// Build SQL for creating an index in the explain schema.
fn build_index_sql(
    index: &CreateIndexStatement<Raw>,
    on_object: &ObjectId,
    explain_db: &str,
    explain_schema: &str,
) -> String {
    let fqn = fqn_from_object_id(on_object);
    let visitor =
        NormalizingVisitor::explain(&fqn, explain_db.to_string(), explain_schema.to_string());

    let mut indexes = vec![index.clone()];
    visitor.normalize_index_references(&mut indexes);
    visitor.normalize_index_clusters(&mut indexes);
    indexes.into_iter().next().unwrap().to_string()
}

/// Build SQL for creating a view in the explain schema.
fn build_view_sql(
    stmt: &Statement,
    object_id: &ObjectId,
    explain_db: &str,
    explain_schema: &str,
) -> String {
    let fqn = fqn_from_object_id(object_id);
    let mut visitor =
        NormalizingVisitor::explain(&fqn, explain_db.to_string(), explain_schema.to_string());

    let normalized = stmt
        .clone()
        .normalize_name_with(&visitor, &fqn.to_item_name())
        .normalize_dependencies_with(&mut visitor);

    normalized.to_string()
}

/// Build the EXPLAIN SQL statement.
fn build_explain_sql(target: &ExplainTarget, explain_db: &str, explain_schema: &str) -> String {
    let flattened_obj = target.object_id.to_string();
    let qualified_name = format!(
        "{}.{}.{}",
        quote_identifier(explain_db),
        quote_identifier(explain_schema),
        quote_identifier(&flattened_obj),
    );

    match &target.index_name {
        None => {
            format!("EXPLAIN MATERIALIZED VIEW {}", qualified_name)
        }
        Some(index_name) => {
            // Index names in the explain schema are normalized by the visitor
            // The index name itself is not flattened — it stays as-is
            let qualified_index = format!(
                "{}.{}.{}",
                quote_identifier(explain_db),
                quote_identifier(explain_schema),
                quote_identifier(index_name),
            );
            format!("EXPLAIN INDEX {}", qualified_index)
        }
    }
}

/// Build a FullyQualifiedName from an ObjectId.
fn fqn_from_object_id(object_id: &ObjectId) -> FullyQualifiedName {
    FullyQualifiedName::from(UnresolvedItemName(vec![
        Ident::new(&object_id.database).expect("valid database"),
        Ident::new(&object_id.schema).expect("valid schema"),
        Ident::new(&object_id.object).expect("valid object"),
    ]))
}

/// Extract raw text lines from `SimpleQueryMessage` results.
///
/// Concatenates all cell values — the right shape for EXPLAIN output
/// (a series of single-column rows).
fn extract_text_from_messages(messages: Vec<SimpleQueryMessage>) -> Vec<String> {
    let mut lines = Vec::new();
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            for i in 0..row.columns().len() {
                let text: Option<&str> = row.get(i);
                if let Some(t) = text {
                    lines.push(t.to_string());
                }
            }
        }
    }
    lines
}
