//! Domain models for Materialize catalog objects.
//!
//! These types represent objects in the Materialize system catalog and provide
//! a type-safe interface over raw database rows.

use chrono::{DateTime, Utc};
use std::fmt;
use std::str::FromStr;

/// The type of deployment - either tables-only or full objects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeploymentKind {
    /// Table creation deployment (create-tables command)
    Tables,
    /// Full object deployment (stage, apply commands)
    Objects,
    /// Contains sinks
    Sinks
}

impl fmt::Display for DeploymentKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeploymentKind::Tables => write!(f, "tables"),
            DeploymentKind::Objects => write!(f, "objects"),
            DeploymentKind::Sinks => write!(f, "sinks"),
        }
    }
}

impl FromStr for DeploymentKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "tables" => Ok(DeploymentKind::Tables),
            "objects" => Ok(DeploymentKind::Objects),
            "sinks" => Ok(DeploymentKind::Sinks),
            _ => Err(format!("Invalid deployment kind: {}", s)),
        }
    }
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

/// Configuration for a cluster replica (used for unmanaged clusters).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterReplica {
    /// Replica name (e.g., "r1", "r2")
    pub name: String,
    /// Replica size (e.g., "25cc")
    pub size: String,
    /// Optional availability zone
    pub availability_zone: Option<String>,
}

/// A privilege grant on a cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterGrant {
    /// Role name that receives the grant
    pub grantee: String,
    /// Privilege type (e.g., "USAGE", "CREATE")
    pub privilege_type: String,
}

/// Configuration for creating a cluster (managed or unmanaged).
///
/// This captures all the information needed to clone a cluster's configuration
/// including its replicas (for unmanaged clusters) and privilege grants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterConfig {
    /// Managed cluster with SIZE and REPLICATION FACTOR
    Managed {
        /// Cluster options (size, replication factor)
        options: ClusterOptions,
        /// Privilege grants on the cluster
        grants: Vec<ClusterGrant>,
    },
    /// Unmanaged cluster with explicit replicas
    Unmanaged {
        /// Replica configurations
        replicas: Vec<ClusterReplica>,
        /// Privilege grants on the cluster
        grants: Vec<ClusterGrant>,
    },
}

impl ClusterConfig {
    /// Get the grants for this cluster configuration.
    pub fn grants(&self) -> &[ClusterGrant] {
        match self {
            ClusterConfig::Managed { grants, .. } => grants,
            ClusterConfig::Unmanaged { grants, .. } => grants,
        }
    }
}

/// A schema deployment record tracking when and how a schema was deployed.
///
/// Stored in the `deploy.deployments` table. Schemas are deployed
/// atomically - all objects in a dirty schema are redeployed together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaDeploymentRecord {
    /// Deploy ID (e.g., "<init>" for direct deploy, "staging" for staged deploy)
    pub deploy_id: String,
    /// Database name (e.g., "materialize")
    pub database: String,
    /// Schema name (e.g., "public")
    pub schema: String,
    /// When this schema was deployed
    pub deployed_at: DateTime<Utc>,
    /// Which Materialize user/role deployed this schema
    pub deployed_by: String,
    /// When this schema was promoted to production (NULL for staging, set on promotion)
    pub promoted_at: Option<DateTime<Utc>>,
    /// Git commit hash if available
    pub git_commit: Option<String>,
    /// Type of deployment (tables or objects)
    pub kind: DeploymentKind,
}

/// An object deployment record tracking object-level deployment history.
///
/// Stored in the `deploy.objects` table (append-only).
/// Each row records that an object with a specific hash was deployed
/// to a deployment at a point in time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentObjectRecord {
    /// Deploy ID (e.g., "<init>" for direct deploy, "staging" for staged deploy)
    pub deploy_id: String,
    /// Database name (e.g., "materialize")
    pub database: String,
    /// Schema name (e.g., "public")
    pub schema: String,
    /// Object name (e.g., "my_view")
    pub object: String,
    /// Hash of the HIR DatabaseObject (semantic content hash)
    pub object_hash: String,
    /// When this object was deployed
    pub deployed_at: DateTime<Utc>,
}

/// Metadata about a deployment.
///
/// Used for validation before operations like apply or abort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentMetadata {
    /// Deploy ID
    pub deploy_id: String,
    /// When this deployment was promoted (NULL if not promoted)
    pub promoted_at: Option<DateTime<Utc>>,
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
    /// Deploy ID that last promoted this schema
    pub deploy_id: String,
    /// When the schema was last promoted to production
    pub promoted_at: DateTime<Utc>,
}

/// Details about a specific deployment.
///
/// Returned by `get_deployment_details()` for the describe command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentDetails {
    /// When this deployment was created
    pub deployed_at: DateTime<Utc>,
    /// When this deployment was promoted (None if still staging)
    pub promoted_at: Option<DateTime<Utc>>,
    /// Which Materialize user/role deployed this
    pub deployed_by: String,
    /// Git commit hash if available
    pub git_commit: Option<String>,
    /// Type of deployment (tables or objects)
    pub kind: DeploymentKind,
    /// List of (database, schema) tuples in this deployment
    pub schemas: Vec<(String, String)>,
}

/// Summary of a staging deployment.
///
/// Used by `list_staging_deployments()` for the deployments command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingDeployment {
    /// When this deployment was created
    pub deployed_at: DateTime<Utc>,
    /// Which Materialize user/role deployed this
    pub deployed_by: String,
    /// Git commit hash if available
    pub git_commit: Option<String>,
    /// Type of deployment (tables or objects)
    pub kind: DeploymentKind,
    /// List of (database, schema) tuples in this deployment
    pub schemas: Vec<(String, String)>,
}

/// A promoted deployment in history.
///
/// Returned by `list_deployment_history()` for the history command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeploymentHistoryEntry {
    /// Deploy ID for this deployment
    pub deploy_id: String,
    /// When this deployment was promoted
    pub promoted_at: DateTime<Utc>,
    /// Which Materialize user/role deployed this
    pub deployed_by: String,
    /// Git commit hash if available
    pub git_commit: Option<String>,
    /// Type of deployment (tables or objects)
    pub kind: DeploymentKind,
    /// List of (database, schema) tuples in this deployment
    pub schemas: Vec<(String, String)>,
}

/// State of an apply operation for resumable apply.
///
/// This is determined by checking the existence and comments of the
/// `_mz_deploy.apply_<deploy_id>_pre` and `_mz_deploy.apply_<deploy_id>_post` schemas.
/// Comments are set when creating the schemas; the swap transaction exchanges which
/// schema has which comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyState {
    /// No apply state schemas exist - fresh apply or completed.
    NotStarted,
    /// State schemas exist but swap hasn't happened yet.
    /// The `_pre` schema has comment 'swapped=false'.
    PreSwap,
    /// Swap has completed.
    /// After the swap, `_pre` schema has comment 'swapped=true' (it was `_post` before).
    PostSwap,
}

/// A pending statement to be executed after the swap.
///
/// Used for deferred execution of statements like sinks that cannot
/// be created in staging (they write to external systems immediately).
/// Stored in `_mz_deploy.public.pending_statements` table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingStatement {
    /// Deploy ID this statement belongs to
    pub deploy_id: String,
    /// Sequence number for ordering execution
    pub sequence_num: i32,
    /// Database containing the object
    pub database: String,
    /// Schema containing the object
    pub schema: String,
    /// Object name
    pub object: String,
    /// Hash of the object definition
    pub object_hash: String,
    /// SQL statement to execute
    pub statement_sql: String,
    /// Kind of statement (e.g., "sink")
    pub statement_kind: String,
    /// When this statement was executed (None if not yet executed)
    pub executed_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployment_kind_display() {
        assert_eq!(DeploymentKind::Tables.to_string(), "tables");
        assert_eq!(DeploymentKind::Objects.to_string(), "objects");
    }

    #[test]
    fn test_deployment_kind_from_str_valid() {
        assert_eq!(
            "tables".parse::<DeploymentKind>().unwrap(),
            DeploymentKind::Tables
        );
        assert_eq!(
            "objects".parse::<DeploymentKind>().unwrap(),
            DeploymentKind::Objects
        );
    }

    #[test]
    fn test_deployment_kind_from_str_invalid() {
        let result = "invalid".parse::<DeploymentKind>();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Invalid deployment kind: invalid");
    }

    #[test]
    fn test_deployment_kind_roundtrip() {
        // Verify that Display and FromStr are consistent
        for kind in [DeploymentKind::Tables, DeploymentKind::Objects] {
            let s = kind.to_string();
            let parsed: DeploymentKind = s.parse().unwrap();
            assert_eq!(kind, parsed);
        }
    }

    #[test]
    fn test_cluster_options_from_cluster_success() {
        let cluster = Cluster {
            id: "u1".to_string(),
            name: "quickstart".to_string(),
            size: Some("25cc".to_string()),
            replication_factor: Some(2),
        };

        let options = ClusterOptions::from_cluster(&cluster).unwrap();
        assert_eq!(options.size, "25cc");
        assert_eq!(options.replication_factor, 2);
    }

    #[test]
    fn test_cluster_options_from_cluster_default_replication() {
        let cluster = Cluster {
            id: "u1".to_string(),
            name: "quickstart".to_string(),
            size: Some("25cc".to_string()),
            replication_factor: None, // Should default to 1
        };

        let options = ClusterOptions::from_cluster(&cluster).unwrap();
        assert_eq!(options.size, "25cc");
        assert_eq!(options.replication_factor, 1);
    }

    #[test]
    fn test_cluster_options_from_cluster_no_size() {
        let cluster = Cluster {
            id: "u1".to_string(),
            name: "unmanaged".to_string(),
            size: None, // Unmanaged cluster
            replication_factor: Some(1),
        };

        let result = ClusterOptions::from_cluster(&cluster);
        assert!(result.is_err());
        let err_msg = result.unwrap_err();
        assert!(err_msg.contains("unmanaged"));
        assert!(err_msg.contains("has no size"));
    }

    #[test]
    fn test_cluster_options_from_cluster_negative_replication() {
        let cluster = Cluster {
            id: "u1".to_string(),
            name: "test".to_string(),
            size: Some("25cc".to_string()),
            replication_factor: Some(-1), // Invalid negative value
        };

        let result = ClusterOptions::from_cluster(&cluster);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid replication_factor"));
    }

    #[test]
    fn test_cluster_equality() {
        let cluster1 = Cluster {
            id: "u1".to_string(),
            name: "test".to_string(),
            size: Some("25cc".to_string()),
            replication_factor: Some(1),
        };

        let cluster2 = Cluster {
            id: "u1".to_string(),
            name: "test".to_string(),
            size: Some("25cc".to_string()),
            replication_factor: Some(1),
        };

        let cluster3 = Cluster {
            id: "u2".to_string(), // Different ID
            name: "test".to_string(),
            size: Some("25cc".to_string()),
            replication_factor: Some(1),
        };

        assert_eq!(cluster1, cluster2);
        assert_ne!(cluster1, cluster3);
    }

    #[test]
    fn test_cluster_options_equality() {
        let opts1 = ClusterOptions {
            size: "25cc".to_string(),
            replication_factor: 2,
        };

        let opts2 = ClusterOptions {
            size: "25cc".to_string(),
            replication_factor: 2,
        };

        let opts3 = ClusterOptions {
            size: "50cc".to_string(),
            replication_factor: 2,
        };

        assert_eq!(opts1, opts2);
        assert_ne!(opts1, opts3);
    }

    #[test]
    fn test_cluster_replica_equality() {
        let r1 = ClusterReplica {
            name: "r1".to_string(),
            size: "25cc".to_string(),
            availability_zone: Some("use1-az1".to_string()),
        };

        let r2 = ClusterReplica {
            name: "r1".to_string(),
            size: "25cc".to_string(),
            availability_zone: Some("use1-az1".to_string()),
        };

        let r3 = ClusterReplica {
            name: "r2".to_string(),
            size: "25cc".to_string(),
            availability_zone: None,
        };

        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    #[test]
    fn test_cluster_grant_equality() {
        let g1 = ClusterGrant {
            grantee: "reader".to_string(),
            privilege_type: "USAGE".to_string(),
        };

        let g2 = ClusterGrant {
            grantee: "reader".to_string(),
            privilege_type: "USAGE".to_string(),
        };

        let g3 = ClusterGrant {
            grantee: "writer".to_string(),
            privilege_type: "CREATE".to_string(),
        };

        assert_eq!(g1, g2);
        assert_ne!(g1, g3);
    }

    #[test]
    fn test_cluster_config_managed() {
        let config = ClusterConfig::Managed {
            options: ClusterOptions {
                size: "25cc".to_string(),
                replication_factor: 2,
            },
            grants: vec![ClusterGrant {
                grantee: "reader".to_string(),
                privilege_type: "USAGE".to_string(),
            }],
        };

        assert_eq!(config.grants().len(), 1);
        assert_eq!(config.grants()[0].grantee, "reader");
    }

    #[test]
    fn test_cluster_config_unmanaged() {
        let config = ClusterConfig::Unmanaged {
            replicas: vec![
                ClusterReplica {
                    name: "r1".to_string(),
                    size: "25cc".to_string(),
                    availability_zone: None,
                },
                ClusterReplica {
                    name: "r2".to_string(),
                    size: "50cc".to_string(),
                    availability_zone: Some("use1-az1".to_string()),
                },
            ],
            grants: vec![],
        };

        if let ClusterConfig::Unmanaged { replicas, grants } = &config {
            assert_eq!(replicas.len(), 2);
            assert_eq!(replicas[0].name, "r1");
            assert_eq!(replicas[1].availability_zone, Some("use1-az1".to_string()));
            assert!(grants.is_empty());
        } else {
            panic!("Expected Unmanaged config");
        }
    }

    #[test]
    fn test_cluster_config_unmanaged_empty_replicas() {
        // Unmanaged clusters with 0 replicas are valid
        let config = ClusterConfig::Unmanaged {
            replicas: vec![],
            grants: vec![ClusterGrant {
                grantee: "admin".to_string(),
                privilege_type: "CREATE".to_string(),
            }],
        };

        if let ClusterConfig::Unmanaged { replicas, grants } = &config {
            assert!(replicas.is_empty());
            assert_eq!(grants.len(), 1);
        } else {
            panic!("Expected Unmanaged config");
        }
    }
}
