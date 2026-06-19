// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster validation for object statements.
//!
//! Validates that indexes, materialized views, sinks, and sources specify
//! which cluster they run on using the `IN CLUSTER` clause.

use super::identifiers::validate_cluster_name;
use crate::project::ast::Statement;
use crate::project::error::{ValidationError, ValidationErrorKind};
use crate::project::ir::compiled::FullyQualifiedName;
use mz_sql_parser::ast::*;

/// Validates that all indexes specify a cluster.
///
/// Indexes in Materialize must specify which cluster they run on using the IN CLUSTER clause.
/// This ensures deterministic deployment and avoids implicit cluster selection.
///
/// # Example
///
/// Valid:
/// ```sql
/// CREATE INDEX idx ON table (col) IN CLUSTER quickstart;
/// ```
///
/// Invalid:
/// ```sql
/// CREATE INDEX idx ON table (col);  -- missing cluster
/// ```
pub(super) fn validate_index_clusters(
    fqn: &FullyQualifiedName,
    indexes: &[CreateIndexStatement<Raw>],
    offsets: &[usize],
    errors: &mut Vec<ValidationError>,
) {
    for (i, index) in indexes.iter().enumerate() {
        let offset = offsets[i];
        match &index.in_cluster {
            None => {
                let index_sql = format!("{};", index);
                let index_name = index
                    .name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "<unnamed>".to_string());

                errors.push(ValidationError::with_file_sql_and_offset(
                    ValidationErrorKind::IndexMissingCluster { index_name },
                    fqn.path.clone(),
                    index_sql,
                    offset,
                ));
            }
            Some(cluster) => {
                // Validate cluster name format
                let cluster_name = cluster.to_string();
                if let Err(e) = validate_cluster_name(&cluster_name, &fqn.path, offset) {
                    errors.push(e);
                }
            }
        }
    }
}

/// Validates that indexes are only defined on views and materialized views.
///
/// Tables and sources are storage objects whose lifecycle `mz-deploy` manages
/// through `apply`; an index on one is not supported.
pub(super) fn validate_indexes_supported(
    fqn: &FullyQualifiedName,
    stmt: &Statement,
    indexes: &[CreateIndexStatement<Raw>],
    offsets: &[usize],
    errors: &mut Vec<ValidationError>,
) {
    let object_type = match stmt {
        Statement::CreateTable(_) | Statement::CreateTableFromSource(_) => "table",
        Statement::CreateSource(_) => "source",
        _ => return,
    };

    for (i, index) in indexes.iter().enumerate() {
        let index_name = index
            .name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<unnamed>".to_string());
        let index_sql = format!("{};", index);

        errors.push(ValidationError::with_file_sql_and_offset(
            ValidationErrorKind::IndexOnStorageObject {
                object_type: object_type.to_string(),
                object_name: fqn.object().to_string(),
                index_name,
            },
            fqn.path.clone(),
            index_sql,
            offsets[i],
        ));
    }
}

/// Validates that a materialized view specifies a cluster.
///
/// Materialized views in Materialize must specify which cluster they run on using the IN CLUSTER clause.
/// This ensures deterministic deployment and avoids implicit cluster selection.
///
/// # Example
///
/// Valid:
/// ```sql
/// CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT ...;
/// ```
///
/// Invalid:
/// ```sql
/// CREATE MATERIALIZED VIEW mv AS SELECT ...;  -- missing cluster
/// ```
pub(super) fn validate_mv_cluster(
    fqn: &FullyQualifiedName,
    stmt: &Statement,
    main_offset: usize,
    errors: &mut Vec<ValidationError>,
) {
    if let Statement::CreateMaterializedView(mv) = stmt {
        match &mv.in_cluster {
            None => {
                let mv_sql = format!("{};", mv);
                let view_name = mv.name.to_string();

                errors.push(ValidationError::with_file_sql_and_offset(
                    ValidationErrorKind::MaterializedViewMissingCluster { view_name },
                    fqn.path.clone(),
                    mv_sql,
                    main_offset,
                ));
            }
            Some(cluster) => {
                // Validate cluster name format
                let cluster_name = cluster.to_string();
                if let Err(e) = validate_cluster_name(&cluster_name, &fqn.path, main_offset) {
                    errors.push(e);
                }
            }
        }
    }
}

/// Validates that a sink specifies a cluster.
///
/// Sinks in Materialize must specify which cluster they run on using the IN CLUSTER clause.
/// This ensures deterministic deployment and avoids implicit cluster selection.
///
/// # Example
///
/// Valid:
/// ```sql
/// CREATE SINK sink IN CLUSTER quickstart FROM table INTO ...;
/// ```
///
/// Invalid:
/// ```sql
/// CREATE SINK sink FROM table INTO ...;  -- missing cluster
/// ```
pub(super) fn validate_sink_cluster(
    fqn: &FullyQualifiedName,
    stmt: &Statement,
    main_offset: usize,
    errors: &mut Vec<ValidationError>,
) {
    if let Statement::CreateSink(sink) = stmt {
        match &sink.in_cluster {
            None => {
                let sink_sql = format!("{};", sink);
                let sink_name = sink
                    .name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "<unnamed>".to_string());

                errors.push(ValidationError::with_file_sql_and_offset(
                    ValidationErrorKind::SinkMissingCluster { sink_name },
                    fqn.path.clone(),
                    sink_sql,
                    main_offset,
                ));
            }
            Some(cluster) => {
                // Validate cluster name format
                let cluster_name = cluster.to_string();
                if let Err(e) = validate_cluster_name(&cluster_name, &fqn.path, main_offset) {
                    errors.push(e);
                }
            }
        }
    }
}

/// Validates that a CREATE SOURCE statement has a required IN CLUSTER clause.
pub(super) fn validate_source_cluster(
    fqn: &FullyQualifiedName,
    stmt: &Statement,
    main_offset: usize,
    errors: &mut Vec<ValidationError>,
) {
    if let Statement::CreateSource(source) = stmt {
        match &source.in_cluster {
            None => {
                let source_sql = format!("{};", source);
                errors.push(ValidationError::with_file_sql_and_offset(
                    ValidationErrorKind::SourceMissingCluster {
                        source_name: source.name.to_string(),
                    },
                    fqn.path.clone(),
                    source_sql,
                    main_offset,
                ));
            }
            Some(cluster) => {
                let cluster_name = cluster.to_string();
                if let Err(e) = validate_cluster_name(&cluster_name, &fqn.path, main_offset) {
                    errors.push(e);
                }
            }
        }

        if source.external_references.is_some() {
            let source_sql = format!("{};", source);
            errors.push(ValidationError::with_file_sql_and_offset(
                ValidationErrorKind::SourceExternalReferences {
                    source_name: source.name.to_string(),
                },
                fqn.path.clone(),
                source_sql,
                main_offset,
            ));
        }
    }
}
