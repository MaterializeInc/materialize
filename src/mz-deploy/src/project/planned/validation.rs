//! Deployment-time validation for planned projects.
//!
//! These validations check runtime constraints that require the full planned
//! representation (dependency graph, cluster assignments, etc.) rather than
//! the per-object checks performed during the typed phase.
//!
//! ## Validations
//!
//! - **Cluster isolation** ([`validate_cluster_isolation`]): Ensures that
//!   sources/sinks do not share a cluster with materialized views or indexes.
//!   During a blue/green swap, **all** objects on a cluster are affected. If
//!   storage and compute objects share a cluster, an MV update would force
//!   source recreation — an expensive and disruptive operation.

use super::super::ast::Statement;
use super::types::Project;
use std::collections::{BTreeMap, BTreeSet};

/// Validate that sources and sinks don't share clusters with indexes or materialized views.
///
/// This validation prevents accidentally recreating sources/sinks when updating compute objects.
/// During a blue/green swap, all objects on a cluster are affected — mixing storage and compute
/// objects on the same cluster means a view update could trigger source recreation.
///
/// # Arguments
/// * `project` - The planned project to validate
/// * `sources_by_cluster` - Map of cluster name to list of source FQNs from the database
///
/// # Returns
/// * `Ok(())` if no conflicts found
/// * `Err((cluster_name, compute_objects, storage_objects))` if conflicts detected
pub fn validate_cluster_isolation(
    project: &Project,
    sources_by_cluster: &BTreeMap<String, Vec<String>>,
) -> Result<(), (String, Vec<String>, Vec<String>)> {
    // Build a map of cluster -> compute objects (indexes, MVs)
    let mut cluster_compute_objects: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for db in &project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                // Check for materialized views
                if let Statement::CreateMaterializedView(mv) = &obj.typed_object.stmt {
                    if let Some(cluster_name) = &mv.in_cluster {
                        cluster_compute_objects
                            .entry(cluster_name.to_string())
                            .or_default()
                            .push(obj.id.to_string());
                    }
                }

                // Check for indexes
                for index in &obj.typed_object.indexes {
                    if let Some(cluster_name) = &index.in_cluster {
                        let index_name = index
                            .name
                            .as_ref()
                            .map(|n| format!(" (index: {})", n))
                            .unwrap_or_default();
                        cluster_compute_objects
                            .entry(cluster_name.to_string())
                            .or_default()
                            .push(format!("{}{}", obj.id, index_name));
                    }
                }
            }
        }
    }

    // Build a map of cluster -> sinks
    let mut cluster_sinks: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for db in &project.databases {
        for schema in &db.schemas {
            for obj in &schema.objects {
                if let Statement::CreateSink(sink) = &obj.typed_object.stmt {
                    if let Some(cluster_name) = &sink.in_cluster {
                        cluster_sinks
                            .entry(cluster_name.to_string())
                            .or_default()
                            .push(obj.id.to_string());
                    }
                }
            }
        }
    }

    // Get all clusters that have compute objects or sinks
    let mut all_clusters: BTreeSet<String> = BTreeSet::new();
    all_clusters.extend(cluster_compute_objects.keys().cloned());
    all_clusters.extend(cluster_sinks.keys().cloned());

    // Check for conflicts: cluster has both compute objects AND (sources OR sinks)
    for cluster_name in all_clusters {
        let compute_objects = cluster_compute_objects.get(&cluster_name);
        let sources = sources_by_cluster.get(&cluster_name);
        let sinks = cluster_sinks.get(&cluster_name);

        let has_compute = compute_objects.is_some() && !compute_objects.unwrap().is_empty();
        let has_sources = sources.is_some() && !sources.unwrap().is_empty();
        let has_sinks = sinks.is_some() && !sinks.unwrap().is_empty();

        if has_compute && (has_sources || has_sinks) {
            let mut storage_objects = Vec::new();
            if let Some(sources) = sources {
                storage_objects.extend(sources.iter().cloned());
            }
            if let Some(sinks) = sinks {
                storage_objects.extend(sinks.iter().cloned());
            }

            return Err((
                cluster_name,
                compute_objects.unwrap().clone(),
                storage_objects,
            ));
        }
    }

    Ok(())
}
