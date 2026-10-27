// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Base fact extraction from a planned project.
//!
//! Translates the project's object graph into relational facts consumed by
//! the Datalog fixed-point computation in [`super::datalog`].
//!
//! ## Base Facts
//!
//! Each base fact corresponds to a Datalog relation used by the propagation
//! rules in [`super::datalog`]:
//!
//! | Relation | Source | Meaning |
//! |----------|--------|---------|
//! | `ObjectInSchema(obj, db, sch)` | Project hierarchy | Object `obj` lives in `db.sch` |
//! | `DependsOn(child, parent)` | `project.dependency_graph` | `child` references `parent` in its query |
//! | `StmtUsesCluster(obj, cluster)` | `IN CLUSTER` clause on main CREATE | Object's statement runs on `cluster` |
//! | `IndexUsesCluster(obj, idx, cluster)` | `IN CLUSTER` clause on CREATE INDEX | Index `idx` on `obj` runs on `cluster` |
//! | `ClusterBoundary(cluster)` | Evaluator-derived from cluster usage facts | The set of clusters eligible to become `DirtyCluster` |
//! | `IsSink(obj)` | `Statement::CreateSink` | Object writes to an external system |
//! | `IsReplacement(obj)` | Schema is in `project.replacement_schemas` | Object uses in-place replacement protocol |
//!
//! `ClusterBoundary` is derived as the set of all clusters referenced by
//! statements or indexes in the project.

use crate::project::analysis::deps::extract_dependencies;
use crate::project::ast::Cluster;
use crate::project::ast::Statement;
use crate::project::ir::graph::Project;
use crate::project::ir::object_id::ObjectId;
use crate::verbose;
use owo_colors::{OwoColorize, Stream, Style};
use std::collections::BTreeSet;

/// Base facts extracted from the project for Datalog computation.
///
/// Each field corresponds to a stored extensional relation. Helper relations
/// such as `ClusterBoundary` are materialized from these facts in the
/// evaluator layer.
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
    pub stmt_uses_cluster: Vec<(ObjectId, Cluster)>,

    /// `IndexUsesCluster(object, index_name, cluster_name)` -- clusters used
    /// by indexes on an object.  Index clusters dirty only the cluster set,
    /// **not** the parent object itself.
    pub index_uses_cluster: Vec<(ObjectId, String, Cluster)>,

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
    let header_style = Style::new().cyan().bold();
    verbose!(
        "{} {}",
        "▶".if_supports_color(Stream::Stderr, |t| t.cyan()),
        "Extracting base facts from project..."
            .if_supports_color(Stream::Stderr, |t| header_style.style(t))
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
                    verbose!(
                        "  ├─ {}: {}",
                        "IsSink".if_supports_color(Stream::Stderr, |t| t.yellow()),
                        obj_id
                            .to_string()
                            .if_supports_color(Stream::Stderr, |t| t.cyan())
                    );
                    is_sink.insert(obj_id.clone());
                }

                // IsReplacement fact - replacement MVs should not propagate dirtiness to schemas
                if is_replacement_schema {
                    verbose!(
                        "  ├─ {}: {}",
                        "IsReplacement".if_supports_color(Stream::Stderr, |t| t.yellow()),
                        obj_id
                            .to_string()
                            .if_supports_color(Stream::Stderr, |t| t.cyan())
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
                    extract_dependencies(&obj.typed_object.stmt, &db.name, &schema.name);

                // StmtUsesCluster facts
                for cluster in clusters {
                    stmt_uses_cluster.push((obj_id.clone(), cluster));
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
                        index_uses_cluster.push((
                            obj_id.clone(),
                            index_name,
                            Cluster::new(cluster_name.to_string()),
                        ));
                    }
                }
            }
        }
    }

    verbose!(
        "  └─ Base facts: {} objects, {} dependencies, {} stmt→cluster, {} index→cluster, {} sinks, {} replacements",
        object_in_schema
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        depends_on
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        stmt_uses_cluster
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        index_uses_cluster
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        is_sink
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold()),
        is_replacement
            .len()
            .to_string()
            .if_supports_color(Stream::Stderr, |t| t.bold())
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
