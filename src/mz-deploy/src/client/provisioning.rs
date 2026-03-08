//! DDL provisioning operations.
//!
//! Methods on [`ProvisioningClient`] issue `CREATE … IF NOT EXISTS` and
//! `ALTER` statements to ensure that the target region's databases, schemas,
//! and clusters match the project definition. These are idempotent and run
//! before any object-level deployment.

use crate::client::connection::ProvisioningClient;
use crate::client::errors::ConnectionError;
use crate::client::models::{ClusterConfig, ClusterOptions};
use crate::client::quote_identifier;

impl ProvisioningClient<'_> {
    /// Create a database if it does not already exist.
    pub async fn create_database(&self, database: &str) -> Result<(), ConnectionError> {
        let sql = format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            quote_identifier(database)
        );

        self.client.execute(&sql, &[]).await.map_err(|e| {
            ConnectionError::DatabaseCreationFailed {
                database: database.to_string(),
                source: Box::new(e),
            }
        })?;

        Ok(())
    }

    /// Create a schema in the specified database if it does not already exist.
    pub async fn create_schema(&self, database: &str, schema: &str) -> Result<(), ConnectionError> {
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{}",
            quote_identifier(database),
            quote_identifier(schema)
        );

        self.client.execute(&sql, &[]).await.map_err(|e| {
            ConnectionError::SchemaCreationFailed {
                database: database.to_string(),
                schema: schema.to_string(),
                source: Box::new(e),
            }
        })?;

        Ok(())
    }

    /// Create a managed cluster with the requested size and replication factor.
    pub async fn create_cluster(
        &self,
        name: &str,
        options: &ClusterOptions,
    ) -> Result<(), ConnectionError> {
        let sql = format!(
            "CREATE CLUSTER {} (SIZE = '{}', REPLICATION FACTOR = {})",
            quote_identifier(name),
            options.size,
            options.replication_factor
        );

        self.client.execute(&sql, &[]).await.map_err(|e| {
            if e.to_string().contains("already exists") {
                ConnectionError::ClusterAlreadyExists {
                    name: name.to_string(),
                }
            } else {
                ConnectionError::ClusterCreationFailed {
                    name: name.to_string(),
                    source: Box::new(e),
                }
            }
        })?;

        Ok(())
    }

    /// Create a cluster from a captured cluster configuration.
    pub async fn create_cluster_with_config(
        &self,
        name: &str,
        config: &ClusterConfig,
    ) -> Result<(), ConnectionError> {
        let grants = match config {
            ClusterConfig::Managed { options, grants } => {
                self.create_cluster(name, options).await?;
                grants
            }
            ClusterConfig::Unmanaged { replicas, grants } => {
                let create_cluster_sql =
                    format!("CREATE CLUSTER {} REPLICAS ()", quote_identifier(name));

                self.client
                    .execute(&create_cluster_sql, &[])
                    .await
                    .map_err(|e| {
                        if e.to_string().contains("already exists") {
                            ConnectionError::ClusterAlreadyExists {
                                name: name.to_string(),
                            }
                        } else {
                            ConnectionError::ClusterCreationFailed {
                                name: name.to_string(),
                                source: Box::new(e),
                            }
                        }
                    })?;

                for replica in replicas {
                    let mut options_parts = vec![format!("SIZE = '{}'", replica.size)];

                    if let Some(ref az) = replica.availability_zone {
                        options_parts.push(format!("AVAILABILITY ZONE '{}'", az));
                    }

                    let create_replica_sql = format!(
                        "CREATE CLUSTER REPLICA {}.{} ({})",
                        quote_identifier(name),
                        quote_identifier(&replica.name),
                        options_parts.join(", ")
                    );

                    self.client
                        .execute(&create_replica_sql, &[])
                        .await
                        .map_err(|e| ConnectionError::ClusterCreationFailed {
                            name: format!("{}.{}", name, replica.name),
                            source: Box::new(e),
                        })?;
                }

                grants
            }
        };

        for grant in grants {
            let sql = format!(
                "GRANT {} ON CLUSTER {} TO {}",
                grant.privilege_type,
                quote_identifier(name),
                quote_identifier(&grant.grantee)
            );
            self.client.execute(&sql, &[]).await.map_err(|e| {
                ConnectionError::Message(format!(
                    "Failed to grant {} to {} on cluster '{}': {}",
                    grant.privilege_type, grant.grantee, name, e
                ))
            })?;
        }

        Ok(())
    }

    /// Update options for an existing managed cluster.
    pub async fn alter_cluster(
        &self,
        name: &str,
        options: &ClusterOptions,
    ) -> Result<(), ConnectionError> {
        let sql = format!(
            "ALTER CLUSTER {} SET (SIZE = '{}', REPLICATION FACTOR = {})",
            quote_identifier(name),
            options.size,
            options.replication_factor
        );

        self.client.execute(&sql, &[]).await.map_err(|e| {
            ConnectionError::Message(format!("Failed to alter cluster '{}': {}", name, e))
        })?;

        Ok(())
    }
}
