//! Cluster validation for object statements.
//!
//! Validates that indexes, materialized views, sinks, and sources specify
//! which cluster they run on using the `IN CLUSTER` clause.

use super::super::super::ast::Statement;
use super::super::types::FullyQualifiedName;
use super::identifiers::validate_cluster_name;
use crate::project::error::{ValidationError, ValidationErrorKind};
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
pub(in super::super) fn validate_index_clusters(
    fqn: &FullyQualifiedName,
    indexes: &[CreateIndexStatement<Raw>],
    errors: &mut Vec<ValidationError>,
) {
    for index in indexes.iter() {
        match &index.in_cluster {
            None => {
                let index_sql = format!("{};", index);
                let index_name = index
                    .name
                    .as_ref()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| "<unnamed>".to_string());

                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::IndexMissingCluster { index_name },
                    fqn.path.clone(),
                    index_sql,
                ));
            }
            Some(cluster) => {
                // Validate cluster name format
                let cluster_name = cluster.to_string();
                if let Err(e) = validate_cluster_name(&cluster_name, &fqn.path) {
                    errors.push(e);
                }
            }
        }
    }
}

/// Validates cluster rules for constraints.
///
/// - **Enforced constraints** (`enforced: true`): `in_cluster` is **required**. Validate format.
/// - **Not-enforced constraints** (`enforced: false`): `in_cluster` must **not** be set.
pub(in super::super) fn validate_constraint_clusters(
    fqn: &FullyQualifiedName,
    constraints: &[CreateConstraintStatement<Raw>],
    errors: &mut Vec<ValidationError>,
) {
    for constraint in constraints.iter() {
        let constraint_name = constraint
            .name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<unnamed>".to_string());

        if constraint.enforced {
            // Enforced constraints require IN CLUSTER
            match &constraint.in_cluster {
                None => {
                    let constraint_sql = format!("{};", constraint);
                    errors.push(ValidationError::with_file_and_sql(
                        ValidationErrorKind::EnforcedConstraintMissingCluster { constraint_name },
                        fqn.path.clone(),
                        constraint_sql,
                    ));
                }
                Some(cluster) => {
                    let cluster_name = cluster.to_string();
                    if let Err(e) = validate_cluster_name(&cluster_name, &fqn.path) {
                        errors.push(e);
                    }
                }
            }
        } else {
            // Not-enforced constraints must NOT have IN CLUSTER
            if constraint.in_cluster.is_some() {
                let constraint_sql = format!("{};", constraint);
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::NotEnforcedConstraintHasCluster { constraint_name },
                    fqn.path.clone(),
                    constraint_sql,
                ));
            }
        }
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
pub(in super::super) fn validate_mv_cluster(
    fqn: &FullyQualifiedName,
    stmt: &Statement,
    errors: &mut Vec<ValidationError>,
) {
    if let Statement::CreateMaterializedView(mv) = stmt {
        match &mv.in_cluster {
            None => {
                let mv_sql = format!("{};", mv);
                let view_name = mv.name.to_string();

                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::MaterializedViewMissingCluster { view_name },
                    fqn.path.clone(),
                    mv_sql,
                ));
            }
            Some(cluster) => {
                // Validate cluster name format
                let cluster_name = cluster.to_string();
                if let Err(e) = validate_cluster_name(&cluster_name, &fqn.path) {
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
pub(in super::super) fn validate_sink_cluster(
    fqn: &FullyQualifiedName,
    stmt: &Statement,
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

                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::SinkMissingCluster { sink_name },
                    fqn.path.clone(),
                    sink_sql,
                ));
            }
            Some(cluster) => {
                // Validate cluster name format
                let cluster_name = cluster.to_string();
                if let Err(e) = validate_cluster_name(&cluster_name, &fqn.path) {
                    errors.push(e);
                }
            }
        }
    }
}

/// Validates that a CREATE SOURCE statement has a required IN CLUSTER clause.
pub(in super::super) fn validate_source_cluster(
    fqn: &FullyQualifiedName,
    stmt: &Statement,
    errors: &mut Vec<ValidationError>,
) {
    if let Statement::CreateSource(source) = stmt {
        match &source.in_cluster {
            None => {
                let source_sql = format!("{};", source);
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::SourceMissingCluster {
                        source_name: source.name.to_string(),
                    },
                    fqn.path.clone(),
                    source_sql,
                ));
            }
            Some(cluster) => {
                let cluster_name = cluster.to_string();
                if let Err(e) = validate_cluster_name(&cluster_name, &fqn.path) {
                    errors.push(e);
                }
            }
        }

        if source.external_references.is_some() {
            let source_sql = format!("{};", source);
            errors.push(ValidationError::with_file_and_sql(
                ValidationErrorKind::SourceExternalReferences {
                    source_name: source.name.to_string(),
                },
                fqn.path.clone(),
                source_sql,
            ));
        }
    }
}
