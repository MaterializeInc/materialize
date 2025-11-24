//! Domain models for Materialize catalog objects.
//!
//! These types represent objects in the Materialize system catalog and provide
//! a type-safe interface over raw database rows.

/// A schema in a Materialize database.
///
/// Schemas contain database objects like tables, views, and materialized views.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// Materialize's unique identifier for the schema
    pub id: String,
    /// Schema name (e.g., "public")
    pub name: String,
    /// Database ID that contains this schema
    pub database_id: String,
    /// Role ID that owns this schema
    pub owner_id: String,
}

/// A compute cluster in Materialize.
///
/// Clusters provide the compute resources for materialized views, indexes, and sinks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cluster {
    /// Materialize's unique identifier for the cluster
    pub id: String,
    /// Cluster name (e.g., "quickstart")
    pub name: String,
    /// Cluster size (e.g., "M.1-large"), None for unmanaged clusters
    pub size: Option<String>,
    /// Number of replicas for fault tolerance (stored as i64 to handle postgres uint4 type)
    pub replication_factor: Option<i64>,
}

/// Options for creating a new cluster.
///
/// Only size and replication factor are configurable - all other settings
/// use Materialize defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterOptions {
    /// Cluster size (e.g., "M.1-large", "M.1-small")
    pub size: String,
    /// Number of replicas (default: 1)
    pub replication_factor: u32,
}

impl ClusterOptions {
    /// Create cluster options from a production cluster configuration.
    pub fn from_cluster(cluster: &Cluster) -> Result<Self, String> {
        let size = cluster.size.clone().ok_or_else(|| {
            format!(
                "Cluster '{}' has no size (unmanaged cluster?)",
                cluster.name
            )
        })?;

        let replication_factor = cluster
            .replication_factor
            .unwrap_or(1)
            .try_into()
            .map_err(|_| format!("Invalid replication_factor for cluster '{}'", cluster.name))?;

        Ok(Self {
            size,
            replication_factor,
        })
    }
}

/// A schema deployment record tracking when and how a schema was deployed.
///
/// Stored in the `deploy.deployments` table. Schemas are deployed
/// atomically - all objects in a dirty schema are redeployed together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDeploymentRecord {
    /// Environment name (e.g., "<init>" for direct deploy, "staging" for staged deploy)
    pub environment: String,
    /// Database name (e.g., "materialize")
    pub database: String,
    /// Schema name (e.g., "public")
    pub schema: String,
    /// When this schema was deployed
    pub deployed_at: std::time::SystemTime,
    /// Which Materialize user/role deployed this schema
    pub deployed_by: String,
    /// When this schema was promoted to production (NULL for staging, set on promotion)
    pub promoted_at: Option<std::time::SystemTime>,
    /// Git commit hash if available
    pub git_commit: Option<String>,
}

/// An object deployment record tracking object-level deployment history.
///
/// Stored in the `deploy.objects` table (append-only).
/// Each row records that an object with a specific hash was deployed
/// to an environment at a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentObjectRecord {
    /// Environment name (e.g., "<init>" for direct deploy, "staging" for staged deploy)
    pub environment: String,
    /// Database name (e.g., "materialize")
    pub database: String,
    /// Schema name (e.g., "public")
    pub schema: String,
    /// Object name (e.g., "my_view")
    pub object: String,
    /// Hash of the HIR DatabaseObject (semantic content hash)
    pub object_hash: String,
    /// When this object was deployed
    pub deployed_at: std::time::SystemTime,
}

/// Legacy deployment record for backwards compatibility.
/// @deprecated Use SchemaDeploymentRecord and DeploymentObjectRecord instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentRecord {
    /// Fully qualified object name ("database.schema.object")
    pub object_fqn: String,
    /// Hash of the HIR DatabaseObject (semantic content hash)
    pub object_hash: String,
    /// Environment name (None for production, Some("staging") for staging)
    pub environment: Option<String>,
    /// When this object was deployed
    pub deployed_at: std::time::SystemTime,
    /// Which Materialize user/role deployed this object
    pub deployed_by: String,
    /// Git commit hash if available
    pub git_commit: Option<String>,
}

/// Metadata about a deployment environment.
///
/// Used for validation before operations like apply or abort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentMetadata {
    /// Environment name
    pub environment: String,
    /// When this deployment was promoted (NULL if not promoted)
    pub promoted_at: Option<std::time::SystemTime>,
    /// List of (database, schema) tuples in this deployment
    pub schemas: Vec<(String, String)>,
}

/// A conflict record indicating a schema was updated after deployment started.
///
/// Used for git-merge-style conflict detection when promoting deployments.
/// Returned by conflict detection queries that check if production schemas
/// were modified since the staging deployment began.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictRecord {
    /// Database name containing the conflicting schema
    pub database: String,
    /// Schema name that has a conflict
    pub schema: String,
    /// Environment that last promoted this schema
    pub environment: String,
    /// When the schema was last promoted to production
    pub promoted_at: std::time::SystemTime,
}
