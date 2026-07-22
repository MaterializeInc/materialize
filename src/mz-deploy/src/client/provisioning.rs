// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! DDL provisioning operations.
//!
//! Methods on [`ProvisioningClient`] issue `CREATE … IF NOT EXISTS` and
//! `ALTER` statements to ensure that the target region's databases, schemas,
//! and clusters match the project definition. These are idempotent and run
//! before any object-level deployment.
//!
//! ## Ordering
//!
//! Provisioning must follow referential order: **databases → schemas → clusters**.
//! A schema cannot be created until its parent database exists, so callers
//! (e.g., [`super::super::cli::executor::DeploymentExecutor`]) are responsible
//! for invoking provisioning methods in the correct order.
//!
//! ## Idempotency
//!
//! All `create_*` methods use `IF NOT EXISTS` (or catch "already exists" errors)
//! so that re-running provisioning on an already-provisioned environment is a
//! no-op.

use crate::client::auto_scaling::strategy_to_cluster_option;
use crate::client::connection::ProvisioningClient;
use crate::client::errors::ConnectionError;
use crate::client::models::{ClusterConfig, ClusterOptions};
use crate::client::quote_identifier;
use mz_sql_parser::ast::display::AstDisplay;

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

    /// Create a managed cluster with the requested size, replication factor,
    /// and autoscaling policy.
    pub async fn create_cluster(
        &self,
        name: &str,
        options: &ClusterOptions,
    ) -> Result<(), ConnectionError> {
        let mut sql = format!(
            "CREATE CLUSTER {} (SIZE = '{}', REPLICATION FACTOR = {}",
            quote_identifier(name),
            options.size,
            options.replication_factor
        );
        if let Some(strategy) = &options.auto_scaling_strategy {
            sql.push_str(", ");
            sql.push_str(&strategy_to_cluster_option(strategy).to_ast_string_simple());
        }
        sql.push(')');

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
}
