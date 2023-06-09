// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator functionality to sequence cluster-related plans

use std::collections::BTreeMap;

use mz_compute_client::controller::ComputeReplicaConfig;
use mz_controller::clusters::{
    ClusterConfig, ClusterId, CreateReplicaConfig, ReplicaConfig, ReplicaId, ReplicaLogging,
};
use mz_sql::catalog::{CatalogCluster, CatalogItem as SqlCatalogItem, CatalogItemType, ObjectType};
use mz_sql::plan::{
    AlterClusterRenamePlan, AlterClusterReplicaRenamePlan, CreateClusterPlan,
    CreateClusterReplicaPlan,
};

use crate::catalog::{self, Op, SerializedReplicaLocation};
use crate::command::ExecuteResponse;
use crate::coord::{Coordinator, DEFAULT_LOGICAL_COMPACTION_WINDOW_TS};
use crate::error::AdapterError;
use crate::session::Session;

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_cluster(
        &mut self,
        session: &Session,
        CreateClusterPlan { name, replicas }: CreateClusterPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_cluster");

        let id = self.catalog_mut().allocate_user_cluster_id().await?;
        // The catalog items for the introspection sources are shared between all replicas
        // of a compute instance, so we create them unconditionally during instance creation.
        // Whether a replica actually maintains introspection arrangements is determined by the
        // per-replica introspection configuration.
        let introspection_sources = self.catalog_mut().allocate_introspection_sources().await;
        let mut ops = vec![catalog::Op::CreateCluster {
            id,
            name: name.clone(),
            linked_object_id: None,
            introspection_sources,
            owner_id: *session.current_role_id(),
        }];

        let azs = self.catalog().state().availability_zones();
        let mut n_replicas_per_az = azs
            .iter()
            .map(|s| (s.clone(), 0))
            .collect::<BTreeMap<_, _>>();
        for (_name, r) in replicas.iter() {
            if let mz_sql::plan::ReplicaConfig::Managed {
                availability_zone: Some(az),
                ..
            } = r
            {
                let ct: &mut usize = n_replicas_per_az.get_mut(az).ok_or_else(|| {
                    AdapterError::InvalidClusterReplicaAz {
                        az: az.to_string(),
                        expected: azs.to_vec(),
                    }
                })?;
                *ct += 1
            }
        }

        for (replica_name, replica_config) in replicas {
            // If the AZ was not specified, choose one, round-robin, from the ones with
            // the lowest number of configured replicas for this cluster.
            let (compute, location) = match replica_config {
                mz_sql::plan::ReplicaConfig::Unmanaged {
                    storagectl_addrs,
                    storage_addrs,
                    computectl_addrs,
                    compute_addrs,
                    workers,
                    compute,
                } => {
                    let location = SerializedReplicaLocation::Unmanaged {
                        storagectl_addrs,
                        storage_addrs,
                        computectl_addrs,
                        compute_addrs,
                        workers,
                    };
                    (compute, location)
                }
                mz_sql::plan::ReplicaConfig::Managed {
                    size,
                    availability_zone,
                    compute,
                } => {
                    let (availability_zone, user_specified) =
                        availability_zone.map(|az| (az, true)).unwrap_or_else(|| {
                            let az = Self::choose_az(&n_replicas_per_az);
                            *n_replicas_per_az
                                .get_mut(&az)
                                .expect("availability zone does not exist") += 1;
                            (az, false)
                        });
                    let location = SerializedReplicaLocation::Managed {
                        size: size.clone(),
                        availability_zone,
                        az_user_specified: user_specified,
                    };
                    (compute, location)
                }
            };

            let logging = if let Some(config) = compute.introspection {
                ReplicaLogging {
                    log_logging: config.debugging,
                    interval: Some(config.interval),
                }
            } else {
                ReplicaLogging::default()
            };

            let config = ReplicaConfig {
                location: self.catalog().concretize_replica_location(
                    location,
                    &self
                        .catalog()
                        .system_config()
                        .allowed_cluster_replica_sizes(),
                )?,
                compute: ComputeReplicaConfig {
                    logging,
                    idle_arrangement_merge_effort: compute.idle_arrangement_merge_effort,
                },
            };

            ops.push(catalog::Op::CreateClusterReplica {
                cluster_id: id,
                id: self.catalog_mut().allocate_replica_id().await?,
                name: replica_name.clone(),
                config,
                owner_id: *session.current_role_id(),
            });
        }

        self.catalog_transact(Some(session), ops).await?;

        self.create_cluster(id).await;

        Ok(ExecuteResponse::CreatedCluster)
    }

    pub(crate) async fn create_cluster(&mut self, cluster_id: ClusterId) {
        let (catalog, controller) = self.catalog_and_controller_mut();
        let cluster = catalog.get_cluster(cluster_id);
        let cluster_id = cluster.id;
        let introspection_source_ids: Vec<_> =
            cluster.log_indexes.iter().map(|(_, id)| *id).collect();

        controller
            .create_cluster(
                cluster_id,
                ClusterConfig {
                    arranged_logs: cluster.log_indexes.clone(),
                },
            )
            .expect("creating cluster must not fail");

        let replicas: Vec<_> = cluster
            .replicas_by_id
            .keys()
            .copied()
            .map(|r| (cluster_id, r))
            .collect();
        self.create_cluster_replicas(&replicas).await;

        if !introspection_source_ids.is_empty() {
            self.initialize_compute_read_policies(
                introspection_source_ids,
                cluster_id,
                Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_TS),
            )
            .await;
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_cluster_replica(
        &mut self,
        session: &Session,
        CreateClusterReplicaPlan {
            name,
            cluster_id,
            config,
        }: CreateClusterReplicaPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        // Choose default AZ if necessary
        let (compute, location) = match config {
            mz_sql::plan::ReplicaConfig::Unmanaged {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
                compute,
            } => {
                let location = SerializedReplicaLocation::Unmanaged {
                    storagectl_addrs,
                    storage_addrs,
                    computectl_addrs,
                    compute_addrs,
                    workers,
                };
                (compute, location)
            }
            mz_sql::plan::ReplicaConfig::Managed {
                size,
                availability_zone,
                compute,
            } => {
                let (availability_zone, user_specified) = match availability_zone {
                    Some(az) => {
                        let azs = self.catalog().state().availability_zones();
                        if !azs.contains(&az) {
                            return Err(AdapterError::InvalidClusterReplicaAz {
                                az,
                                expected: azs.to_vec(),
                            });
                        }
                        (az, true)
                    }
                    None => {
                        // Choose the least popular AZ among all replicas of this cluster as the default
                        // if none was specified. If there is a tie for "least popular", pick the first one.
                        // That is globally unbiased (for Materialize, not necessarily for this customer)
                        // because we shuffle the AZs on boot in `crate::serve`.
                        let cluster = self.catalog().get_cluster(cluster_id);
                        let azs = self.catalog().state().availability_zones();
                        let mut n_replicas_per_az = azs
                            .iter()
                            .map(|s| (s.clone(), 0))
                            .collect::<BTreeMap<_, _>>();
                        for r in cluster.replicas_by_id.values() {
                            if let Some(az) = r.config.location.availability_zone() {
                                *n_replicas_per_az.get_mut(az).expect("unknown AZ") += 1;
                            }
                        }
                        let az = Self::choose_az(&n_replicas_per_az);
                        (az, false)
                    }
                };
                let location = SerializedReplicaLocation::Managed {
                    size,
                    availability_zone,
                    az_user_specified: user_specified,
                };
                (compute, location)
            }
        };

        let logging = if let Some(config) = compute.introspection {
            ReplicaLogging {
                log_logging: config.debugging,
                interval: Some(config.interval),
            }
        } else {
            ReplicaLogging::default()
        };

        let config = ReplicaConfig {
            location: self.catalog().concretize_replica_location(
                location,
                &self
                    .catalog()
                    .system_config()
                    .allowed_cluster_replica_sizes(),
            )?,
            compute: ComputeReplicaConfig {
                logging,
                idle_arrangement_merge_effort: compute.idle_arrangement_merge_effort,
            },
        };

        let id = self.catalog_mut().allocate_replica_id().await?;
        let op = catalog::Op::CreateClusterReplica {
            cluster_id,
            id,
            name: name.clone(),
            config,
            owner_id: *session.current_role_id(),
        };

        self.catalog_transact(Some(session), vec![op]).await?;

        self.create_cluster_replicas(&[(cluster_id, id)]).await;

        Ok(ExecuteResponse::CreatedClusterReplica)
    }

    pub(super) async fn create_cluster_replicas(&mut self, replicas: &[(ClusterId, ReplicaId)]) {
        let mut replicas_to_start = Vec::new();

        for (cluster_id, replica_id) in replicas.iter().copied() {
            let cluster = self.catalog().get_cluster(cluster_id);
            let role = cluster.role();
            let replica_config = cluster.replicas_by_id[&replica_id].config.clone();

            replicas_to_start.push(CreateReplicaConfig {
                cluster_id,
                replica_id,
                role,
                config: replica_config,
            });
        }

        self.controller
            .create_replicas(replicas_to_start)
            .await
            .expect("creating replicas must not fail");
    }

    pub(super) async fn sequence_alter_cluster_rename(
        &mut self,
        session: &Session,
        AlterClusterRenamePlan { id, name, to_name }: AlterClusterRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = Op::RenameCluster { id, name, to_name };
        match self.catalog_transact(Some(session), vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(ObjectType::Cluster)),
            Err(err) => Err(err),
        }
    }

    pub(super) async fn sequence_alter_cluster_replica_rename(
        &mut self,
        session: &Session,
        AlterClusterReplicaRenamePlan {
            cluster_id,
            replica_id,
            name,
            to_name,
        }: AlterClusterReplicaRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = catalog::Op::RenameClusterReplica {
            cluster_id,
            replica_id,
            name,
            to_name,
        };
        match self.catalog_transact(Some(session), vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(ObjectType::ClusterReplica)),
            Err(err) => Err(err),
        }
    }

    /// Returns whether the given cluster exclusively maintains items
    /// that were formerly maintained on `computed`.
    pub(super) fn is_compute_cluster(&self, id: ClusterId) -> bool {
        let cluster = self.catalog().get_cluster(id);
        cluster.bound_objects().iter().all(|id| {
            matches!(
                self.catalog().get_entry(id).item_type(),
                CatalogItemType::Index | CatalogItemType::MaterializedView
            )
        })
    }
}
