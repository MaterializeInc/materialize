//! Base fact extraction from a planned project.
//!
//! Translates the project's object graph into relational facts consumed by
//! the Datalog fixed-point computation in [`super::datalog`].

use super::super::ast::Statement;
use super::super::object_id::ObjectId;
use super::super::planned::{self, Project};
use crate::verbose;
use owo_colors::OwoColorize;
use std::collections::BTreeSet;

/// Base facts extracted from the project for Datalog computation.
///
/// Each field corresponds to a Datalog relation:
#[derive(Debug)]
pub(super) struct BaseFacts {
    /// `ObjectInSchema(object, database, schema)` -- every object and the
    /// schema it belongs to.  Used by schema-level propagation rules.
    pub object_in_schema: Vec<(ObjectId, String, String)>,

    /// `DependsOn(child, parent)` -- child depends on parent.  Derived from
    /// the project's dependency graph so dirtiness propagates downstream.
    pub depends_on: Vec<(ObjectId, ObjectId)>,

    /// `StmtUsesCluster(object, cluster_name)` -- the cluster referenced in
    /// the object's CREATE statement.  A dirty cluster dirties the object.
    pub stmt_uses_cluster: Vec<(ObjectId, String)>,

    /// `IndexUsesCluster(object, index_name, cluster_name)` -- clusters used
    /// by indexes on an object.  Index clusters dirty only the cluster set,
    /// **not** the parent object itself.
    pub index_uses_cluster: Vec<(ObjectId, String, String)>,

    /// `IsSink(object)` -- objects that are sinks.  Sinks do not propagate
    /// dirtiness to clusters or schemas because they write to external systems
    /// and are created after the swap.
    pub is_sink: BTreeSet<ObjectId>,

    /// `IsReplacement(object)` -- objects in replacement schemas.  Changed
    /// replacement MVs use the in-place replacement protocol so dirtiness
    /// should not propagate through them to downstream objects.
    pub is_replacement: BTreeSet<ObjectId>,
}

/// Extract all base facts from the project for Datalog computation.
pub(super) fn extract_base_facts(project: &Project) -> BaseFacts {
    verbose!(
        "{} {}",
        "▶".cyan(),
        "Extracting base facts from project...".cyan().bold()
    );
    let mut object_in_schema = Vec::new();
    let mut depends_on = Vec::new();
    let mut stmt_uses_cluster = Vec::new();
    let mut index_uses_cluster = Vec::new();
    let mut is_sink = BTreeSet::new();
    let mut is_replacement = BTreeSet::new();

    // Extract facts from each object in the project
    for db in &project.databases {
        for schema in &db.schemas {
            // Check if this schema is a replacement schema
            let is_replacement_schema = project
                .replacement_schemas
                .iter()
                .any(|sq| sq.database == db.name && sq.schema == schema.name);

            for obj in &schema.objects {
                let obj_id = obj.id.clone();

                // ObjectInSchema fact
                object_in_schema.push((obj_id.clone(), db.name.clone(), schema.name.clone()));

                // IsSink fact - sinks should not propagate dirtiness to clusters/schemas
                if matches!(obj.typed_object.stmt, Statement::CreateSink(_)) {
                    verbose!("  ├─ {}: {}", "IsSink".yellow(), obj_id.to_string().cyan());
                    is_sink.insert(obj_id.clone());
                }

                // IsReplacement fact - replacement MVs should not propagate dirtiness to schemas
                if is_replacement_schema {
                    verbose!(
                        "  ├─ {}: {}",
                        "IsReplacement".yellow(),
                        obj_id.to_string().cyan()
                    );
                    is_replacement.insert(obj_id.clone());
                }

                // DependsOn facts from dependency graph
                if let Some(deps) = project.dependency_graph.get(&obj_id) {
                    for parent in deps {
                        depends_on.push((obj_id.clone(), parent.clone()));
                    }
                }

                // Extract cluster usage from statement
                let (_, clusters) =
                    planned::extract_dependencies(&obj.typed_object.stmt, &db.name, &schema.name);

                // StmtUsesCluster facts
                for cluster in clusters {
                    stmt_uses_cluster.push((obj_id.clone(), cluster.name.clone()));
                }

                // IndexUsesCluster facts - extract from indexes
                for index in &obj.typed_object.indexes {
                    // Extract cluster directly from CreateIndexStatement
                    if let Some(cluster_name) = &index.in_cluster {
                        let index_name = index
                            .name
                            .as_ref()
                            .map(|n| n.to_string())
                            .unwrap_or_else(|| "unnamed_index".to_string());

                        // Convert cluster name to string
                        let cluster_str = cluster_name.to_string();

                        index_uses_cluster.push((obj_id.clone(), index_name, cluster_str));
                    }
                }
            }
        }
    }

    verbose!(
        "  └─ Base facts: {} objects, {} dependencies, {} stmt→cluster, {} index→cluster, {} sinks, {} replacements",
        object_in_schema.len().to_string().bold(),
        depends_on.len().to_string().bold(),
        stmt_uses_cluster.len().to_string().bold(),
        index_uses_cluster.len().to_string().bold(),
        is_sink.len().to_string().bold(),
        is_replacement.len().to_string().bold()
    );

    BaseFacts {
        object_in_schema,
        depends_on,
        stmt_uses_cluster,
        index_uses_cluster,
        is_sink,
        is_replacement,
    }
}
