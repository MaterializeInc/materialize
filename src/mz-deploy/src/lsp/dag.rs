//! DAG response builder for the `mz-deploy/dag` custom LSP endpoint.
//!
//! Builds a lightweight JSON representation of the project's dependency graph
//! for rendering in the VS Code workspace webview. The response contains two
//! collections:
//!
//! - **`objects`** — One [`DagNode`] per project object plus one per external
//!   dependency. Each node carries enough metadata for rendering (type, schema,
//!   file path) but intentionally omits SQL, columns, and constraints to keep
//!   the payload small.
//!
//! - **`edges`** — One [`DagEdge`] per dependency relationship. Edge kinds are
//!   inferred from the target object's type using the same heuristic as the
//!   explore manifest ([`infer_edge_kind`]).
//!
//! ## Differences from `DocsManifest`
//!
//! The explore manifest (`manifest.rs`) includes columns, SQL text, grants,
//! constraints, cluster info, and test results. This endpoint returns only the
//! graph topology and minimal node metadata needed for DAG visualization.

use crate::project::planned;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;

/// Complete DAG response returned by the `mz-deploy/dag` endpoint.
#[derive(Debug, Serialize)]
pub struct DagResponse {
    /// All objects in the project, including external dependencies.
    pub objects: Vec<DagNode>,
    /// Dependency edges between objects.
    pub edges: Vec<DagEdge>,
}

/// A single node in the DAG.
#[derive(Debug, Serialize)]
pub struct DagNode {
    /// Fully-qualified object ID (e.g., `"db.schema.name"`).
    pub id: String,
    /// Unqualified object name.
    pub name: String,
    /// Object type (e.g., `"view"`, `"materialized-view"`, `"table"`).
    pub object_type: String,
    /// Schema the object belongs to.
    pub schema: String,
    /// Whether this is an external dependency (not defined in the project).
    pub is_external: bool,
    /// Relative file path to the `.sql` source file, or `null` for external deps.
    pub file_path: Option<String>,
}

/// A dependency edge between two objects.
#[derive(Debug, Serialize)]
pub struct DagEdge {
    /// Fully-qualified ID of the upstream (dependency) object.
    pub source: String,
    /// Fully-qualified ID of the downstream (dependent) object.
    pub target: String,
    /// Semantic kind of this dependency relationship.
    pub kind: EdgeKind,
}

/// The semantic kind of a dependency edge.
///
/// Mirrors `EdgeKind` from `manifest.rs` but lives here to avoid coupling the
/// LSP module to the CLI explore command.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeKind {
    /// Secret consumed by a connection.
    UsesCredential,
    /// Connection consumed by a source.
    UsesConnection,
    /// Source materialized into a table.
    MaterializesFrom,
    /// Data dependency via SQL query.
    TransformsFrom,
    /// Dependency on an object not defined in this project.
    External,
}

/// Infer the semantic edge kind from the target object's type.
fn infer_edge_kind(target_type: &str) -> EdgeKind {
    match target_type {
        "connection" => EdgeKind::UsesCredential,
        "source" => EdgeKind::UsesConnection,
        "table-from-source" => EdgeKind::MaterializesFrom,
        _ => EdgeKind::TransformsFrom,
    }
}

/// Map an AST statement to its display type name.
fn object_type_name(stmt: &crate::project::ast::Statement) -> &'static str {
    match stmt {
        crate::project::ast::Statement::CreateView(_) => "view",
        crate::project::ast::Statement::CreateMaterializedView(_) => "materialized-view",
        crate::project::ast::Statement::CreateTable(_) => "table",
        crate::project::ast::Statement::CreateTableFromSource(_) => "table-from-source",
        crate::project::ast::Statement::CreateSource(_) => "source",
        crate::project::ast::Statement::CreateSink(_) => "sink",
        crate::project::ast::Statement::CreateSecret(_) => "secret",
        crate::project::ast::Statement::CreateConnection(_) => "connection",
    }
}

/// Build the DAG response from a planned project.
///
/// Walks all project objects to create nodes and edges, then adds nodes for
/// external dependencies (with `is_external: true` and no `file_path`).
/// File paths are made relative to `root`.
pub fn build_dag_response(project: &planned::Project, root: &Path) -> DagResponse {
    let mut objects = Vec::new();
    let mut edges = Vec::new();
    // Track which object IDs we've seen so we can add external deps after.
    let mut seen_ids = BTreeMap::new();

    for db in &project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                // Skip constraint MVs — they're implementation details.
                if obj.is_constraint_mv {
                    continue;
                }

                let id_str = obj.id.to_string();
                let obj_type = object_type_name(&obj.typed_object.stmt);

                let file_path = obj
                    .typed_object
                    .path
                    .strip_prefix(root)
                    .ok()
                    .map(|p| p.to_string_lossy().to_string());

                seen_ids.insert(obj.id.clone(), obj_type);

                objects.push(DagNode {
                    id: id_str.clone(),
                    name: obj.id.object().to_string(),
                    object_type: obj_type.to_string(),
                    schema: schema.name.clone(),
                    is_external: false,
                    file_path,
                });

                // Build edges from this object's dependencies.
                for dep_id in &obj.dependencies {
                    let kind = if project.external_dependencies.contains(dep_id) {
                        EdgeKind::External
                    } else {
                        infer_edge_kind(obj_type)
                    };
                    edges.push(DagEdge {
                        source: dep_id.to_string(),
                        target: id_str.clone(),
                        kind,
                    });
                }
            }
        }
    }

    // Add external dependency nodes.
    for ext_id in &project.external_dependencies {
        if !seen_ids.contains_key(ext_id) {
            objects.push(DagNode {
                id: ext_id.to_string(),
                name: ext_id.object().to_string(),
                object_type: "external".to_string(),
                schema: ext_id.schema().to_string(),
                is_external: true,
                file_path: None,
            });
        }
    }

    DagResponse { objects, edges }
}
