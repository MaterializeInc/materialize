// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator functionality to sequence cluster-related plans

use std::collections::BTreeSet;
use std::time::Duration;

use mz_compute_client::controller::ComputeReplicaConfig;
use mz_controller::clusters::{
    ClusterId, CreateReplicaConfig, ManagedReplicaAvailabilityZones, ReplicaConfig, ReplicaId,
    ReplicaLocation, ReplicaLogging, DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS,
};
use mz_ore::cast::CastFrom;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{CatalogCluster, CatalogItem, CatalogItemType, ObjectType};
use mz_sql::names::ObjectId;
use mz_sql::plan::{
    AlterClusterItemRenamePlan, AlterClusterPlan, AlterClusterRenamePlan, AlterOptionParameter,
    AlterReplicaSetPlan, ComputeReplicaIntrospectionConfig, CreateClusterManagedPlan,
    CreateClusterPlan, CreateClusterReplicaPlan, CreateClusterUnmanagedPlan, CreateClusterVariant,
    CreateReplicaSetPlan, PlanClusterOption,
};
use mz_sql::session::vars::{SystemVars, Var, MAX_REPLICAS_PER_CLUSTER};

use crate::catalog::{
    ClusterConfig, ClusterVariant, ClusterVariantManaged, Op, SerializedReplicaLocation,
};
use crate::coord::{Coordinator, DEFAULT_LOGICAL_COMPACTION_WINDOW_TS};
use crate::session::Session;
use crate::{catalog, AdapterError, ExecuteResponse};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_cluster(
        &mut self,
        session: &Session,
        CreateClusterPlan { name, variant }: CreateClusterPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_cluster");

        let id = self.catalog_mut().allocate_user_cluster_id().await?;
        // The catalog items for the introspection sources are shared between all replicas
        // of a compute instance, so we create them unconditionally during instance creation.
        // Whether a replica actually maintains introspection arrangements is determined by the
        // per-replica introspection configuration.
        let introspection_sources = self.catalog_mut().allocate_introspection_sources().await;
        let cluster_variant = match &variant {
            CreateClusterVariant::Managed(plan) => {
                let logging = if let Some(config) = plan.compute.introspection {
                    ReplicaLogging {
                        log_logging: config.debugging,
                        interval: Some(config.interval),
                    }
                } else {
                    ReplicaLogging::default()
                };
                ClusterVariant::Managed(ClusterVariantManaged {
                    size: plan.size.clone(),
                    availability_zones: plan.availability_zones.clone(),
                    logging: logging.into(),
                    idle_arrangement_merge_effort: plan.compute.idle_arrangement_merge_effort,
                    replication_factor: plan.replication_factor,
                    disk: plan.disk,
                })
            }
            CreateClusterVariant::Unmanaged(_) => ClusterVariant::Unmanaged,
        };
        let config = ClusterConfig {
            variant: cluster_variant,
        };
        let ops = vec![catalog::Op::CreateCluster {
            id,
            name: name.clone(),
            linked_object_id: None,
            introspection_sources,
            owner_id: *session.current_role_id(),
            config,
        }];

        match variant {
            CreateClusterVariant::Managed(plan) => {
                self.sequence_create_managed_cluster(session, plan, id, ops)
                    .await
            }
            CreateClusterVariant::Unmanaged(plan) => {
                self.sequence_create_unmanaged_cluster(session, plan, id, ops)
                    .await
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_managed_cluster(
        &mut self,
        session: &Session,
        CreateClusterManagedPlan {
            availability_zones,
            compute,
            replication_factor,
            size,
            disk,
        }: CreateClusterManagedPlan,
        cluster_id: ClusterId,
        mut ops: Vec<catalog::Op>,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_managed_cluster");

        self.sequence_create_managed_replica_set(
            session,
            availability_zones,
            &compute,
            replication_factor,
            &size,
            disk,
            cluster_id,
            None,
            &mut ops,
        )
        .await?;

        self.catalog_transact(Some(session), ops).await?;

        self.create_cluster(cluster_id).await;

        Ok(ExecuteResponse::CreatedCluster)
    }

    async fn sequence_create_managed_replica_set(
        &mut self,
        session: &Session,
        availability_zones: Vec<String>,
        compute: &mz_sql::plan::ComputeReplicaConfig,
        replication_factor: u32,
        size: &String,
        disk: bool,
        cluster_id: ClusterId,
        replica_set_id: Option<ReplicaId>,
        ops: &mut Vec<Op>,
    ) -> Result<(), AdapterError> {
        self.ensure_valid_azs(availability_zones.iter())?;

        let allowed_replica_sizes = &self
            .catalog()
            .system_config()
            .allowed_cluster_replica_sizes();
        self.catalog
            .ensure_valid_replica_size(allowed_replica_sizes, size)?;

        // Eagerly validate the `max_replicas_per_cluster` limit.
        // `catalog_transact` will do this validation too, but allocating
        // replica IDs is expensive enough that we need to do this validation
        // before allocating replica IDs. See #20195.
        self.validate_resource_limit(
            0,
            i64::from(replication_factor),
            SystemVars::max_replicas_per_cluster,
            "cluster replica",
            MAX_REPLICAS_PER_CLUSTER.name(),
        )?;

        for replica_name in
            (0..replication_factor).map(|index| managed_cluster_replica_name(index, replica_set_id))
        {
            let id = self.catalog_mut().allocate_user_replica_id().await?;
            self.create_managed_cluster_replica_op(
                cluster_id,
                id,
                replica_name,
                compute,
                size,
                ops,
                if availability_zones.is_empty() {
                    None
                } else {
                    Some(availability_zones.as_ref())
                },
                disk,
                *session.current_role_id(),
                replica_set_id,
            )?;
        }
        Ok(())
    }

    fn create_managed_cluster_replica_op(
        &mut self,
        cluster_id: ClusterId,
        id: ReplicaId,
        name: String,
        compute: &mz_sql::plan::ComputeReplicaConfig,
        size: &String,
        ops: &mut Vec<Op>,
        azs: Option<&[String]>,
        disk: bool,
        owner_id: RoleId,
        replica_set_id: Option<ReplicaId>,
    ) -> Result<(), AdapterError> {
        let location = SerializedReplicaLocation::Managed {
            size: size.clone(),
            availability_zone: None,
            disk,
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
                azs,
            )?,
            compute: ComputeReplicaConfig {
                logging,
                idle_arrangement_merge_effort: compute.idle_arrangement_merge_effort,
            },
        };

        ops.push(catalog::Op::CreateClusterReplica {
            cluster_id,
            id,
            name,
            config,
            owner_id,
            replica_set_id,
        });
        Ok(())
    }

    fn ensure_valid_azs<'a, I: IntoIterator<Item = &'a String>>(
        &self,
        azs: I,
    ) -> Result<(), AdapterError> {
        let cat_azs = self.catalog().state().availability_zones();
        for az in azs.into_iter() {
            if !cat_azs.contains(az) {
                return Err(AdapterError::InvalidClusterReplicaAz {
                    az: az.to_string(),
                    expected: cat_azs.to_vec(),
                });
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_unmanaged_cluster(
        &mut self,
        session: &Session,
        CreateClusterUnmanagedPlan { replicas }: CreateClusterUnmanagedPlan,
        id: ClusterId,
        mut ops: Vec<catalog::Op>,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_unmanaged_cluster");

        self.sequence_create_unmanaged_replica_set(
            id,
            *session.current_role_id(),
            None,
            replicas,
            &mut ops,
        )
        .await?;

        self.catalog_transact(Some(session), ops).await?;

        self.create_cluster(id).await;

        Ok(ExecuteResponse::CreatedCluster)
    }

    async fn sequence_create_unmanaged_replica_set(
        &mut self,
        cluster_id: ClusterId,
        owner_id: RoleId,
        replica_set_id: Option<ReplicaId>,
        replicas: Vec<(String, mz_sql::plan::ReplicaConfig)>,
        ops: &mut Vec<Op>,
    ) -> Result<(), AdapterError> {
        self.ensure_valid_azs(replicas.iter().filter_map(|(_, r)| {
            if let mz_sql::plan::ReplicaConfig::Managed {
                availability_zone: Some(az),
                ..
            } = &r
            {
                Some(az)
            } else {
                None
            }
        }))?;

        // Eagerly validate the `max_replicas_per_cluster` limit.
        // `catalog_transact` will do this validation too, but allocating
        // replica IDs is expensive enough that we need to do this validation
        // before allocating replica IDs. See #20195.
        self.validate_resource_limit(
            0,
            i64::try_from(replicas.len()).unwrap_or(i64::MAX),
            SystemVars::max_replicas_per_cluster,
            "cluster replica",
            MAX_REPLICAS_PER_CLUSTER.name(),
        )?;

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
                    disk,
                } => {
                    let location = SerializedReplicaLocation::Managed {
                        size: size.clone(),
                        availability_zone,
                        disk,
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
                    None,
                )?,
                compute: ComputeReplicaConfig {
                    logging,
                    idle_arrangement_merge_effort: compute.idle_arrangement_merge_effort,
                },
            };

            ops.push(catalog::Op::CreateClusterReplica {
                cluster_id,
                id: self.catalog_mut().allocate_user_replica_id().await?,
                name: replica_name.clone(),
                config,
                owner_id,
                replica_set_id,
            });
        }
        Ok(())
    }

    async fn create_replica_set(&mut self, cluster_id: ClusterId, replica_set: Option<ReplicaId>) {
        let cluster = self.catalog.get_cluster(cluster_id);
        let cluster_id = cluster.id;
        let introspection_source_ids: Vec<_> =
            cluster.log_indexes.iter().map(|(_, id)| *id).collect();

        let replicas: Vec<_> = cluster
            .iter_replicas()
            .filter(|entry| entry.replica().replica_set_id == replica_set)
            .map(|r| (cluster_id, r.item_id))
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

    pub(super) async fn create_cluster(&mut self, cluster_id: ClusterId) {
        let Coordinator {
            catalog,
            controller,
            ..
        } = self;
        let cluster = catalog.get_cluster(cluster_id);
        let cluster_id = cluster.id;
        let introspection_source_ids: Vec<_> =
            cluster.log_indexes.iter().map(|(_, id)| *id).collect();

        controller
            .create_cluster(
                cluster_id,
                mz_controller::clusters::ClusterConfig {
                    arranged_logs: cluster.log_indexes.clone(),
                },
            )
            .expect("creating cluster must not fail");

        let replicas: Vec<_> = cluster
            .iter_replicas()
            .map(|r| (cluster_id, r.item_id))
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
    pub(super) async fn sequence_create_replica_set(
        &mut self,
        session: &Session,
        CreateReplicaSetPlan {
            name,
            cluster_id,
            variant,
        }: CreateReplicaSetPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let id = self.catalog_mut().allocate_user_replica_id().await?;
        // Replicas have the same owner as their cluster.
        let owner_id = self.catalog().get_cluster(cluster_id).owner_id();
        let cluster_variant = match &variant {
            CreateClusterVariant::Managed(plan) => {
                let logging = if let Some(config) = plan.compute.introspection {
                    ReplicaLogging {
                        log_logging: config.debugging,
                        interval: Some(config.interval),
                    }
                } else {
                    ReplicaLogging::default()
                };
                ClusterVariant::Managed(ClusterVariantManaged {
                    size: plan.size.clone(),
                    availability_zones: plan.availability_zones.clone(),
                    logging: logging.into(),
                    idle_arrangement_merge_effort: plan.compute.idle_arrangement_merge_effort,
                    replication_factor: plan.replication_factor,
                    disk: plan.disk,
                })
            }
            CreateClusterVariant::Unmanaged(_) => ClusterVariant::Unmanaged,
        };
        let config = ClusterConfig {
            variant: cluster_variant,
        };
        let mut ops = vec![catalog::Op::CreateReplicaSet {
            cluster_id,
            id,
            name: name.clone(),
            owner_id,
            config,
        }];

        match variant {
            CreateClusterVariant::Managed(CreateClusterManagedPlan {
                replication_factor,
                size,
                availability_zones,
                compute,
                disk,
            }) => {
                self.sequence_create_managed_replica_set(
                    session,
                    availability_zones,
                    &compute,
                    replication_factor,
                    &size,
                    disk,
                    cluster_id,
                    Some(id),
                    &mut ops,
                )
                .await?;
            }
            CreateClusterVariant::Unmanaged(plan) => {
                self.sequence_create_unmanaged_replica_set(
                    cluster_id,
                    *session.current_role_id(),
                    Some(id),
                    plan.replicas,
                    &mut ops,
                )
                .await?;
            }
        }

        self.catalog_transact(Some(session), ops).await?;

        self.create_replica_set(cluster_id, Some(id)).await;

        Ok(ExecuteResponse::CreatedReplicaSet)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn sequence_create_cluster_replica(
        &mut self,
        session: &Session,
        CreateClusterReplicaPlan {
            name,
            cluster_id,
            config,
            replica_set_id,
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
                disk,
            } => {
                let availability_zone = match availability_zone {
                    Some(az) => {
                        self.ensure_valid_azs([&az])?;
                        Some(az)
                    }
                    None => None,
                };
                let location = SerializedReplicaLocation::Managed {
                    size,
                    availability_zone,
                    disk,
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
                // Planning ensures all replicas in this codepath
                // are unmanaged.
                None,
            )?,
            compute: ComputeReplicaConfig {
                logging,
                idle_arrangement_merge_effort: compute.idle_arrangement_merge_effort,
            },
        };

        let id = self.catalog_mut().allocate_user_replica_id().await?;
        // Replicas have the same owner as their cluster.
        let owner_id = self.catalog().get_cluster(cluster_id).owner_id();
        let op = catalog::Op::CreateClusterReplica {
            cluster_id,
            id,
            name: name.clone(),
            config,
            owner_id,
            replica_set_id,
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
            let replica_config = cluster.get_item(replica_id).replica().config.clone();

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

    pub(super) async fn sequence_alter_cluster(
        &mut self,
        session: &Session,
        AlterClusterPlan {
            id: cluster_id,
            name: _,
            options,
        }: AlterClusterPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let config = self.catalog.get_cluster(cluster_id).config.clone();
        self.sequence_alter_cluster_profile_inner(
            session,
            cluster_id,
            None,
            options,
            config,
            ObjectType::Cluster,
        )
        .await
    }

    pub(super) async fn sequence_alter_replica_set(
        &mut self,
        session: &Session,
        AlterReplicaSetPlan {
            id: cluster_id,
            replica_set_id,
            name: _,
            options,
        }: AlterReplicaSetPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let cluster = self.catalog.get_cluster(cluster_id);
        let config = cluster
            .get_replica_set(replica_set_id)
            .replica_config
            .clone();

        self.sequence_alter_cluster_profile_inner(
            session,
            cluster_id,
            Some(replica_set_id),
            options,
            config,
            ObjectType::ReplicaSet,
        )
        .await
    }

    async fn sequence_alter_cluster_profile_inner(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        replica_set_id: Option<ReplicaId>,
        options: PlanClusterOption,
        config: ClusterConfig,
        object_type: ObjectType,
    ) -> Result<ExecuteResponse, AdapterError> {
        use catalog::ClusterVariant::*;

        let mut new_config = config.clone();

        match (&new_config.variant, &options.managed) {
            (Managed(_), AlterOptionParameter::Reset)
            | (Managed(_), AlterOptionParameter::Unchanged)
            | (Managed(_), AlterOptionParameter::Set(true)) => {}
            (Managed(_), AlterOptionParameter::Set(false)) => new_config.variant = Unmanaged,
            (Unmanaged, AlterOptionParameter::Unchanged)
            | (Unmanaged, AlterOptionParameter::Set(false)) => {}
            (Unmanaged, AlterOptionParameter::Reset)
            | (Unmanaged, AlterOptionParameter::Set(true)) => {
                // Generate a minimal correct configuration

                // Size and disk adjusted later when sequencing the actual configuration change.
                let size = "".to_string();
                let disk = false;
                let logging = ReplicaLogging {
                    log_logging: false,
                    interval: Some(Duration::from_micros(
                        DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS.into(),
                    )),
                };
                new_config.variant = Managed(ClusterVariantManaged {
                    size,
                    availability_zones: Default::default(),
                    logging: logging.into(),
                    idle_arrangement_merge_effort: None,
                    replication_factor: 1,
                    disk,
                });
            }
        }

        match &mut new_config.variant {
            Managed(ClusterVariantManaged {
                size,
                availability_zones,
                logging,
                idle_arrangement_merge_effort,
                replication_factor,
                disk,
            }) => {
                use AlterOptionParameter::*;
                match &options.size {
                    Set(s) => *size = s.clone(),
                    Reset => coord_bail!("SIZE has no default value"),
                    Unchanged => {}
                }
                match &options.disk {
                    Set(d) => *disk = *d,
                    Reset => *disk = self.catalog.system_config().disk_cluster_replicas_default(),
                    Unchanged => {}
                }
                match &options.availability_zones {
                    Set(az) => *availability_zones = az.clone(),
                    Reset => *availability_zones = Default::default(),
                    Unchanged => {}
                }
                match &options.introspection_debugging {
                    Set(id) => logging.log_logging = *id,
                    Reset => logging.log_logging = false,
                    Unchanged => {}
                }
                match &options.introspection_interval {
                    Set(ii) => {
                        logging.interval =
                            ii.0.map(|ii| ii.duration())
                                .transpose()
                                .map_err(AdapterError::Unstructured)?
                    }
                    Reset => {
                        logging.interval = Some(Duration::from_micros(
                            DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS.into(),
                        ))
                    }
                    Unchanged => {}
                }
                match &options.idle_arrangement_merge_effort {
                    Set(effort) => *idle_arrangement_merge_effort = Some(*effort),
                    Reset => *idle_arrangement_merge_effort = None,
                    Unchanged => {}
                }
                match &options.replication_factor {
                    Set(rf) => *replication_factor = *rf,
                    Reset => *replication_factor = 1,
                    Unchanged => {}
                }
                if !matches!(options.replicas, Unchanged) {
                    coord_bail!("Cannot change REPLICAS of a managed {object_type}");
                }
            }
            Unmanaged => {
                use AlterOptionParameter::*;
                if !matches!(options.size, Unchanged) {
                    coord_bail!("Cannot change SIZE of an unmanaged {object_type}");
                }
                if !matches!(options.availability_zones, Unchanged) {
                    coord_bail!("Cannot change AVAILABILITY ZONES of an unmanaged {object_type}");
                }
                if !matches!(options.introspection_debugging, Unchanged) {
                    coord_bail!(
                        "Cannot change INTROSPECTION DEBUGGING of an unmanaged {object_type}"
                    );
                }
                if !matches!(options.introspection_interval, Unchanged) {
                    coord_bail!(
                        "Cannot change INTROSPECTION INTERVAL of an unmanaged {object_type}"
                    );
                }
                if !matches!(options.idle_arrangement_merge_effort, Unchanged) {
                    coord_bail!(
                        "Cannot change IDLE ARRANGEMENT MERGE EFFORT of an unmanaged {object_type}"
                    );
                }
                if !matches!(options.replication_factor, Unchanged) {
                    coord_bail!("Cannot change REPLICATION FACTOR of an unmanaged {object_type}");
                }
            }
        }

        if new_config == config {
            return Ok(ExecuteResponse::AlteredObject(object_type));
        }

        match (&config.variant, new_config.variant) {
            (Managed(config), Managed(new_config)) => {
                self.sequence_alter_cluster_managed_to_managed(
                    session,
                    cluster_id,
                    replica_set_id,
                    config,
                    new_config,
                )
                .await?;
            }
            (Unmanaged, Managed(new_config)) => {
                self.sequence_alter_cluster_unmanaged_to_managed(
                    session,
                    cluster_id,
                    new_config,
                    options,
                    replica_set_id,
                )
                .await?;
            }
            (Managed(_), Unmanaged) => {
                self.sequence_alter_cluster_managed_to_unmanaged(
                    session,
                    cluster_id,
                    replica_set_id,
                )
                .await?;
            }
            (Unmanaged, Unmanaged) => {
                self.sequence_alter_cluster_unmanaged_to_unmanaged(
                    session,
                    cluster_id,
                    replica_set_id,
                    options.replicas,
                    object_type,
                )?;
            }
        }

        Ok(ExecuteResponse::AlteredObject(object_type))
    }

    async fn sequence_alter_cluster_managed_to_managed(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        replica_set_id: Option<ReplicaId>,
        config: &ClusterVariantManaged,
        new_config: ClusterVariantManaged,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog.get_cluster(cluster_id);
        let name = cluster.name().to_string();
        let owner_id = cluster.owner_id();
        let mut ops = vec![];

        let (
            ClusterVariantManaged {
                size,
                replication_factor,
                availability_zones,
                logging,
                idle_arrangement_merge_effort,
                disk,
            },
            ClusterVariantManaged {
                size: new_size,
                replication_factor: new_replication_factor,
                availability_zones: new_availability_zones,
                logging: new_logging,
                idle_arrangement_merge_effort: new_idle_arrangement_merge_effort,
                disk: new_disk,
            },
        ) = (&config, &new_config);

        let allowed_replica_sizes = &self
            .catalog()
            .system_config()
            .allowed_cluster_replica_sizes();
        self.catalog
            .ensure_valid_replica_size(allowed_replica_sizes, new_size)?;

        let mut create_cluster_replicas = vec![];

        let compute = mz_sql::plan::ComputeReplicaConfig {
            idle_arrangement_merge_effort: new_idle_arrangement_merge_effort.clone(),
            introspection: new_logging
                .interval
                .map(|interval| ComputeReplicaIntrospectionConfig {
                    debugging: new_logging.log_logging,
                    interval,
                }),
        };

        // Eagerly validate the `max_replicas_per_cluster` limit.
        // `catalog_transact` will do this validation too, but allocating
        // replica IDs is expensive enough that we need to do this validation
        // before allocating replica IDs. See #20195.
        if new_replication_factor > replication_factor {
            self.validate_resource_limit(
                usize::cast_from(*replication_factor),
                i64::from(*new_replication_factor) - i64::from(*replication_factor),
                SystemVars::max_replicas_per_cluster,
                "cluster replica",
                MAX_REPLICAS_PER_CLUSTER.name(),
            )?;
        }

        if new_size != size
            || new_availability_zones != availability_zones
            || new_idle_arrangement_merge_effort != idle_arrangement_merge_effort
            || new_logging != logging
            || new_disk != disk
        {
            self.ensure_valid_azs(new_availability_zones.iter())?;

            // tear down all replicas, create new ones
            for name in (0..*replication_factor)
                .map(|index| managed_cluster_replica_name(index, replica_set_id))
            {
                let replica = cluster.resolve_item(&name);
                if let Ok(replica) = replica {
                    replica.replica()?;
                    ops.push(catalog::Op::DropObject(ObjectId::ClusterItem((
                        cluster.id(),
                        replica.item_id(),
                    ))))
                }
            }
            for name in (0..*new_replication_factor)
                .map(|index| managed_cluster_replica_name(index, replica_set_id))
            {
                let id = self.catalog_mut().allocate_user_replica_id().await?;
                self.create_managed_cluster_replica_op(
                    cluster_id,
                    id,
                    name,
                    &compute,
                    new_size,
                    &mut ops,
                    Some(new_availability_zones.as_ref()),
                    *new_disk,
                    owner_id,
                    replica_set_id,
                )?;
                create_cluster_replicas.push((cluster_id, id))
            }
        } else if new_replication_factor < replication_factor {
            // Adjust size down
            for name in (*new_replication_factor..*replication_factor)
                .map(|index| managed_cluster_replica_name(index, replica_set_id))
            {
                let item = cluster.resolve_item(&name);
                if let Ok(item) = item {
                    item.replica()?;
                    ops.push(catalog::Op::DropObject(ObjectId::ClusterItem((
                        cluster.id(),
                        item.item_id(),
                    ))))
                }
            }
        } else if new_replication_factor > replication_factor {
            // Adjust size up
            for name in (*replication_factor..*new_replication_factor)
                .map(|index| managed_cluster_replica_name(index, replica_set_id))
            {
                let id = self.catalog_mut().allocate_user_replica_id().await?;
                self.create_managed_cluster_replica_op(
                    cluster_id,
                    id,
                    name,
                    &compute,
                    new_size,
                    &mut ops,
                    // AVAILABILITY ZONES hasn't changed, so existing replicas don't need to be
                    // rescheduled.
                    Some(new_availability_zones.as_ref()),
                    *new_disk,
                    owner_id,
                    replica_set_id,
                )?;
                create_cluster_replicas.push((cluster_id, id))
            }
        }

        let variant = ClusterVariant::Managed(new_config);
        match replica_set_id {
            None => {
                ops.push(catalog::Op::UpdateClusterConfig {
                    id: cluster_id,
                    name,
                    config: ClusterConfig { variant },
                });
            }
            Some(replica_set_id) => {
                ops.push(catalog::Op::UpdateReplicaSetConfig {
                    id: cluster_id,
                    replica_set_id,
                    name,
                    config: ClusterConfig { variant },
                });
            }
        }

        self.catalog_transact(Some(session), ops).await?;
        self.create_cluster_replicas(&create_cluster_replicas).await;
        Ok(())
    }

    async fn sequence_alter_cluster_unmanaged_to_managed(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        mut new_config: ClusterVariantManaged,
        options: PlanClusterOption,
        replica_set_id: Option<ReplicaId>,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog.get_cluster(cluster_id);
        let cluster_name = cluster.name().to_string();

        let ClusterVariantManaged {
            size: new_size,
            replication_factor: new_replication_factor,
            availability_zones: new_availability_zones,
            logging: _,
            idle_arrangement_merge_effort: _,
            disk: new_disk,
        } = &mut new_config;

        // Validate replication factor parameter
        let current_replica_count = cluster.iter_replicas_in_replica_set(replica_set_id).count();
        match options.replication_factor {
            AlterOptionParameter::Set(_) => {
                // Validate that the replication factor matches the current length only if specified.
                if u32::try_from(current_replica_count).expect("must fit")
                    != *new_replication_factor
                {
                    coord_bail!("REPLICATION FACTOR {new_replication_factor} does not match number of replicas ({current_replica_count})");
                }
            }
            _ => *new_replication_factor = current_replica_count.try_into().expect("must fit"),
        }

        let mut names = BTreeSet::new();
        let mut sizes = BTreeSet::new();
        let mut disks = BTreeSet::new();

        self.ensure_valid_azs(new_availability_zones.iter())?;

        // Validate per-replica configuration
        for item in cluster.iter_replicas_in_replica_set(replica_set_id) {
            names.insert(item.name.clone());
            let replica = item.try_replica()?;
            match &replica.config.location {
                ReplicaLocation::Unmanaged(_) => coord_bail!(
                    "Cannot convert unmanaged cluster with unmanaged replicas to managed cluster"
                ),
                ReplicaLocation::Managed(location) => {
                    sizes.insert(location.size.clone());
                    disks.insert(location.disk);

                    if let ManagedReplicaAvailabilityZones::FromReplica(Some(az)) =
                        &location.availability_zones
                    {
                        if !new_availability_zones.contains(az) {
                            coord_bail!(
                                "unmanaged replica has availability zone {az} which is not \
                                in managed {new_availability_zones:?}"
                            )
                        }
                    }
                }
            }
        }

        if sizes.is_empty() {
            assert!(
                cluster
                    .iter_replicas_in_replica_set(replica_set_id)
                    .next()
                    .is_none(),
                "Cluster should not have replicas"
            );
            // We didn't collect any size, so the user has to name it.
            match &options.size {
                AlterOptionParameter::Reset | AlterOptionParameter::Unchanged => {
                    coord_bail!("Missing SIZE for empty cluster")
                }
                _ => {} // Was set within the calling function.
            }
        } else if sizes.len() == 1 {
            let size = sizes.into_iter().next().expect("must exist");
            match &options.size {
                AlterOptionParameter::Set(sz) if *sz != size => {
                    coord_bail!("Cluster replicas of size {size} do not match expected SIZE {sz}");
                }
                _ => *new_size = size,
            }
        } else {
            let formatted = sizes
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            coord_bail!(
                "Cannot convert unmanaged cluster to managed, non-unique replica sizes: {formatted}"
            );
        }

        for i in 0..*new_replication_factor {
            let name = managed_cluster_replica_name(i, replica_set_id);
            names.remove(&name);
        }
        if !names.is_empty() {
            let formatted = names
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            coord_bail!(
                "Cannot convert unmanaged cluster to managed, invalid replica names: {formatted}"
            );
        }

        if disks.len() == 1 {
            let disk = disks.into_iter().next().expect("must exist");
            match &options.disk {
                AlterOptionParameter::Set(ds) if *ds != disk => {
                    coord_bail!(
                        "Cluster replicas with DISK {disk} do not match expected DISK {ds}"
                    );
                }
                _ => *new_disk = disk,
            }
        } else if !disks.is_empty() {
            coord_bail!(
                "Cannot convert unmanaged cluster to managed, non-unique replica DISK options"
            );
        }

        let mut ops = vec![];

        let variant = ClusterVariant::Managed(new_config);
        match replica_set_id {
            None => {
                ops.push(catalog::Op::UpdateClusterConfig {
                    id: cluster_id,
                    name: cluster_name,
                    config: ClusterConfig { variant },
                });
            }
            Some(replica_set_id) => {
                ops.push(catalog::Op::UpdateReplicaSetConfig {
                    id: cluster_id,
                    replica_set_id,
                    name: cluster_name,
                    config: ClusterConfig { variant },
                });
            }
        }

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    async fn sequence_alter_cluster_managed_to_unmanaged(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        replica_set_id: Option<ReplicaId>,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog().get_cluster(cluster_id);
        let mut ops = vec![];

        let variant = ClusterVariant::Unmanaged;
        let name = cluster.name().to_string();
        match replica_set_id {
            None => {
                ops.push(catalog::Op::UpdateClusterConfig {
                    id: cluster_id,
                    name,
                    config: ClusterConfig { variant },
                });
            }
            Some(replica_set_id) => {
                ops.push(catalog::Op::UpdateReplicaSetConfig {
                    id: cluster_id,
                    replica_set_id,
                    name,
                    config: ClusterConfig { variant },
                });
            }
        }

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    fn sequence_alter_cluster_unmanaged_to_unmanaged(
        &self,
        _session: &Session,
        _cluster_id: ClusterId,
        _replica_set_id: Option<ReplicaId>,
        _replicas: AlterOptionParameter<Vec<(String, mz_sql::plan::ReplicaConfig)>>,
        object_type: ObjectType,
    ) -> Result<(), AdapterError> {
        coord_bail!("Cannot alter an unmanaged {object_type}");
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
        AlterClusterItemRenamePlan {
            cluster_id,
            item_id: replica_id,
            name,
            to_name,
        }: AlterClusterItemRenamePlan,
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
    pub(crate) fn is_compute_cluster(&self, id: ClusterId) -> bool {
        let cluster = self.catalog().get_cluster(id);
        cluster.bound_objects().iter().all(|id| {
            matches!(
                self.catalog().get_entry(id).item_type(),
                CatalogItemType::Index | CatalogItemType::MaterializedView
            )
        })
    }
}

fn managed_cluster_replica_name(index: u32, replica_set_id: Option<ReplicaId>) -> String {
    if let Some(replica_set_id) = replica_set_id {
        // TODO: This encodes the profile ID and not a human-readable name.
        format!("p{}_r{}", replica_set_id, index + 1)
    } else {
        format!("r{}", index + 1)
    }
}
