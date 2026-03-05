//! Core `ChangeSet` type and its display formatting.
//!
//! A `ChangeSet` captures the full set of objects, schemas, and clusters that
//! need redeployment after comparing two deployment snapshots.

use super::super::SchemaQualifier;
use super::super::ast::Cluster;
use super::super::object_id::ObjectId;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

/// Represents the set of changes between two project states.
///
/// Used to determine which objects need redeployment based on snapshot comparison.
#[derive(Debug, Clone)]
pub struct ChangeSet {
    /// Objects that exist in changed files
    pub changed_objects: BTreeSet<ObjectId>,

    /// Schemas where ANY file changed (entire schema is dirty)
    pub dirty_schemas: BTreeSet<SchemaQualifier>,

    /// Clusters used by objects in dirty schemas
    pub dirty_clusters: BTreeSet<Cluster>,

    /// All objects that need redeployment (includes transitive dependencies)
    pub objects_to_deploy: BTreeSet<ObjectId>,

    /// New replacement MVs (in replacement schemas but NOT in old snapshot).
    /// These are deployed via normal blue-green schema swap.
    pub new_replacement_objects: BTreeSet<ObjectId>,

    /// Changed replacement MVs (in replacement schemas AND in old snapshot with different hash).
    /// These are deployed via CREATE REPLACEMENT MV protocol.
    pub changed_replacement_objects: BTreeSet<ObjectId>,
}

impl ChangeSet {
    /// Check if any changes were detected.
    pub fn is_empty(&self) -> bool {
        self.objects_to_deploy.is_empty()
    }

    /// Get the number of objects that need deployment.
    pub fn deployment_count(&self) -> usize {
        self.objects_to_deploy.len()
    }
}

impl Display for ChangeSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Incremental deployment: {} objects need redeployment",
            self.deployment_count()
        )?;

        if !self.changed_objects.is_empty() {
            writeln!(f, "Changed objects:")?;
            for obj in &self.changed_objects {
                writeln!(f, "  - {}.{}.{}", obj.database, obj.schema, obj.object)?;
            }
        }

        if !self.dirty_schemas.is_empty() {
            writeln!(f, "Dirty schemas:")?;
            for sq in &self.dirty_schemas {
                writeln!(f, "  - {}.{}", sq.database, sq.schema)?;
            }
        }

        if !self.dirty_clusters.is_empty() {
            writeln!(f, "Dirty clusters:")?;
            for cluster in &self.dirty_clusters {
                writeln!(f, "  - {}", cluster.name)?;
            }
        }

        if !self.objects_to_deploy.is_empty() {
            writeln!(f, "Objects to deploy:")?;
            for obj in &self.objects_to_deploy {
                writeln!(f, "  - {}.{}.{}", obj.database, obj.schema, obj.object)?;
            }
        }

        Ok(())
    }
}
