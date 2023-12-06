// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator functionality to sequence linked-cluster-related plans

use mz_catalog::memory::objects::{ClusterConfig, ClusterVariant};
use mz_catalog::LINKED_CLUSTER_REPLICA_NAME;
use mz_compute_client::controller::ComputeReplicaConfig;
use mz_controller::clusters::{ReplicaAllocation, ReplicaConfig, ReplicaLogging};
use mz_controller_types::{ClusterId, DEFAULT_REPLICA_LOGGING_INTERVAL};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::CatalogCluster;
use mz_sql::names::QualifiedItemName;
use mz_sql::plan::SourceSinkClusterConfig;

use crate::catalog::{self};
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::session::Session;

impl Coordinator {
    /// Generates the catalog operations to create a linked cluster for the
    /// source or sink with the given name.
    ///
    /// The operations are written to the provided `ops` vector. The ID
    /// allocated for the linked cluster is returned.
    pub(super) async fn create_linked_cluster_ops(
        &mut self,
        linked_object_id: GlobalId,
        name: &QualifiedItemName,
        config: &SourceSinkClusterConfig,
        ops: &mut Vec<catalog::Op>,
        session: &Session,
    ) -> Result<ClusterId, AdapterError> {
        let size = match config {
            SourceSinkClusterConfig::Linked { size } => size.clone(),
            SourceSinkClusterConfig::Undefined => self.default_linked_cluster_size()?,
            SourceSinkClusterConfig::Existing { id } => return Ok(*id),
        };
        let disk = self
            .catalog()
            .system_config()
            .disk_cluster_replicas_default();
        let id = self.catalog().allocate_user_cluster_id().await?;
        let name = self.catalog().resolve_full_name(name, None);
        let name = format!("{}_{}_{}", name.database, name.schema, name.item);
        let name = self.catalog().find_available_cluster_name(&name);
        let introspection_sources = self.catalog().allocate_introspection_sources().await;
        ops.push(catalog::Op::CreateCluster {
            id,
            name: name.clone(),
            linked_object_id: Some(linked_object_id),
            introspection_sources,
            owner_id: *session.current_role_id(),
            config: ClusterConfig {
                variant: ClusterVariant::Unmanaged,
            },
        });
        self.create_linked_cluster_replica_op(id, size, disk, ops, *session.current_role_id())
            .await?;
        Ok(id)
    }

    /// Generates the catalog operation to create a replica of the given linked
    /// cluster for the given storage cluster configuration.
    async fn create_linked_cluster_replica_op(
        &mut self,
        cluster_id: ClusterId,
        size: String,
        disk: bool,
        ops: &mut Vec<catalog::Op>,
        owner_id: RoleId,
    ) -> Result<(), AdapterError> {
        let location = mz_catalog::durable::ReplicaLocation::Managed {
            size: size.to_string(),
            availability_zone: None,
            disk,
            billed_as: None,
            internal: false,
        };
        let location = self.catalog().concretize_replica_location(
            location,
            &self
                .catalog()
                .system_config()
                .allowed_cluster_replica_sizes(),
            None,
        )?;
        let logging = {
            ReplicaLogging {
                log_logging: false,
                interval: Some(DEFAULT_REPLICA_LOGGING_INTERVAL),
            }
        };
        ops.push(catalog::Op::CreateClusterReplica {
            cluster_id,
            id: self.catalog().allocate_user_replica_id().await?,
            name: LINKED_CLUSTER_REPLICA_NAME.into(),
            config: ReplicaConfig {
                location,
                compute: ComputeReplicaConfig {
                    logging,
                    idle_arrangement_merge_effort: None,
                },
            },
            owner_id,
        });
        Ok(())
    }

    /// Generates the catalog operations to alter the linked cluster for the
    /// source or sink with the given ID, if such a cluster exists.
    pub(super) async fn alter_linked_cluster_ops(
        &mut self,
        linked_object_id: GlobalId,
        config: &SourceSinkClusterConfig,
    ) -> Result<Vec<catalog::Op>, AdapterError> {
        let mut ops = vec![];
        match self.catalog().get_linked_cluster(linked_object_id) {
            None => {
                coord_bail!("cannot change the size of a source or sink created with IN CLUSTER");
            }
            Some(linked_cluster) => {
                for replica in linked_cluster.replicas() {
                    ops.extend(
                        self.catalog()
                            .cluster_replica_dependents(linked_cluster.id(), replica.replica_id)
                            .into_iter()
                            .map(catalog::Op::DropObject),
                    );
                }
                let size = match config {
                    SourceSinkClusterConfig::Linked { size } => size.clone(),
                    SourceSinkClusterConfig::Undefined => self.default_linked_cluster_size()?,
                    SourceSinkClusterConfig::Existing { .. } => {
                        coord_bail!("cannot change the cluster of a source or sink")
                    }
                };
                let disk = self
                    .catalog()
                    .system_config()
                    .disk_cluster_replicas_default();
                self.create_linked_cluster_replica_op(
                    linked_cluster.id,
                    size,
                    disk,
                    &mut ops,
                    linked_cluster.owner_id,
                )
                .await?;
            }
        }
        Ok(ops)
    }

    fn default_linked_cluster_size(&self) -> Result<String, AdapterError> {
        if !self.catalog().system_config().allow_unsafe() {
            let mut entries = self
                .catalog()
                .cluster_replica_sizes()
                .enabled_allocations()
                .collect::<Vec<_>>();
            entries.sort_by_key(
                |(
                    _name,
                    ReplicaAllocation {
                        scale,
                        workers,
                        memory_limit,
                        ..
                    },
                )| (scale, workers, memory_limit),
            );
            let expected = entries.into_iter().map(|(name, _)| name.clone()).collect();
            return Err(AdapterError::SourceOrSinkSizeRequired { expected });
        }
        Ok(self.catalog().default_linked_cluster_size())
    }

    /// Creates the cluster linked to the specified object after a create
    /// operation, if such a linked cluster exists.
    pub(super) async fn maybe_create_linked_cluster(&mut self, linked_object_id: GlobalId) {
        if let Some(cluster) = self.catalog().get_linked_cluster(linked_object_id) {
            self.create_cluster(cluster.id).await;
        }
    }

    /// Updates the replicas of the cluster linked to the specified object after
    /// an alter operation, if such a linked cluster exists.
    pub(super) async fn maybe_alter_linked_cluster(&mut self, linked_object_id: GlobalId) {
        if let Some(cluster) = self.catalog().get_linked_cluster(linked_object_id) {
            // The old replicas of the linked cluster will have been dropped by
            // `catalog_transact`, both from the catalog state and from the
            // controller. The new replicas will be in the catalog state, and
            // need to be recreated in the controller.
            let replicas: Vec<_> = cluster
                .replicas()
                .map(|r| (r.cluster_id, r.replica_id))
                .collect();
            self.create_cluster_replicas(&replicas).await;
        }
    }
}
