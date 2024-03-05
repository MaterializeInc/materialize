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

use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{ClusterConfig, ClusterVariant, ClusterVariantManaged};
use mz_compute_client::controller::ComputeReplicaConfig;
use mz_controller::clusters::{
    CreateReplicaConfig, ManagedReplicaAvailabilityZones, ManagedReplicaLocation, ReplicaConfig,
    ReplicaLocation, ReplicaLogging,
};
use mz_controller_types::{ClusterId, ReplicaId, DEFAULT_REPLICA_LOGGING_INTERVAL};
use mz_ore::cast::CastFrom;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{CatalogCluster, ObjectType};
use mz_sql::names::ObjectId;
use mz_sql::plan::{
    AlterClusterPlan, AlterClusterRenamePlan, AlterClusterReplicaRenamePlan, AlterClusterSwapPlan,
    AlterOptionParameter, ComputeReplicaIntrospectionConfig, CreateClusterManagedPlan,
    CreateClusterPlan, CreateClusterReplicaPlan, CreateClusterUnmanagedPlan, CreateClusterVariant,
    PlanClusterOption,
};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::{SystemVars, Var, MAX_REPLICAS_PER_CLUSTER};

use crate::catalog::Op;
use crate::coord::Coordinator;
use crate::session::Session;
use crate::{catalog, AdapterError, ExecuteResponse};

impl Coordinator {
    #[mz_ore::instrument(level = "debug")]
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
                    logging,
                    idle_arrangement_merge_effort: plan.compute.idle_arrangement_merge_effort,
                    replication_factor: plan.replication_factor,
                    disk: plan.disk,
                    optimizer_feature_overrides: plan.optimizer_feature_overrides.clone(),
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

    #[mz_ore::instrument(level = "debug")]
    pub(super) async fn sequence_create_managed_cluster(
        &mut self,
        session: &Session,
        CreateClusterManagedPlan {
            availability_zones,
            compute,
            replication_factor,
            size,
            disk,
            optimizer_feature_overrides: _,
        }: CreateClusterManagedPlan,
        cluster_id: ClusterId,
        mut ops: Vec<catalog::Op>,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_managed_cluster");

        self.ensure_valid_azs(availability_zones.iter())?;

        let allowed_replica_sizes = &self
            .catalog()
            .system_config()
            .allowed_cluster_replica_sizes();
        self.catalog
            .ensure_valid_replica_size(allowed_replica_sizes, &size)?;

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

        for replica_name in (0..replication_factor).map(managed_cluster_replica_name) {
            let id = self.catalog_mut().allocate_user_replica_id().await?;
            self.create_managed_cluster_replica_op(
                cluster_id,
                id,
                replica_name,
                &compute,
                &size,
                &mut ops,
                if availability_zones.is_empty() {
                    None
                } else {
                    Some(availability_zones.as_ref())
                },
                disk,
                *session.current_role_id(),
            )?;
        }

        self.catalog_transact(Some(session), ops).await?;

        self.create_cluster(cluster_id).await;

        Ok(ExecuteResponse::CreatedCluster)
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
    ) -> Result<(), AdapterError> {
        let location = mz_catalog::durable::ReplicaLocation::Managed {
            availability_zone: None,
            billed_as: None,
            disk,
            internal: false,
            size: size.clone(),
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

    #[mz_ore::instrument(level = "debug")]
    pub(super) async fn sequence_create_unmanaged_cluster(
        &mut self,
        session: &Session,
        CreateClusterUnmanagedPlan { replicas }: CreateClusterUnmanagedPlan,
        id: ClusterId,
        mut ops: Vec<catalog::Op>,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_unmanaged_cluster");

        self.ensure_valid_azs(replicas.iter().filter_map(|(_, r)| {
            if let mz_sql::plan::ReplicaConfig::Orchestrated {
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
                mz_sql::plan::ReplicaConfig::Unorchestrated {
                    storagectl_addrs,
                    storage_addrs,
                    computectl_addrs,
                    compute_addrs,
                    workers,
                    compute,
                } => {
                    let location = mz_catalog::durable::ReplicaLocation::Unmanaged {
                        storagectl_addrs,
                        storage_addrs,
                        computectl_addrs,
                        compute_addrs,
                        workers,
                    };
                    (compute, location)
                }
                mz_sql::plan::ReplicaConfig::Orchestrated {
                    availability_zone,
                    billed_as,
                    compute,
                    disk,
                    internal,
                    size,
                } => {
                    // Only internal users have access to INTERNAL and BILLED AS
                    if !session.user().is_internal() && (internal || billed_as.is_some()) {
                        coord_bail!("cannot specify INTERNAL or BILLED AS as non-internal user")
                    }
                    // BILLED AS implies the INTERNAL flag.
                    if billed_as.is_some() && !internal {
                        coord_bail!("must specify INTERNAL when specifying BILLED AS");
                    }

                    let location = mz_catalog::durable::ReplicaLocation::Managed {
                        availability_zone,
                        billed_as,
                        disk,
                        internal,
                        size: size.clone(),
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
                cluster_id: id,
                id: self.catalog_mut().allocate_user_replica_id().await?,
                name: replica_name.clone(),
                config,
                owner_id: *session.current_role_id(),
            });
        }

        self.catalog_transact(Some(session), ops).await?;

        self.create_cluster(id).await;

        Ok(ExecuteResponse::CreatedCluster)
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
            .replicas()
            .map(|r| (cluster_id, r.replica_id))
            .collect();
        self.create_cluster_replicas(&replicas).await;

        if !introspection_source_ids.is_empty() {
            self.initialize_compute_read_policies(
                introspection_source_ids,
                cluster_id,
                CompactionWindow::Default,
            )
            .await;
        }
    }

    #[mz_ore::instrument(level = "debug")]
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
            mz_sql::plan::ReplicaConfig::Unorchestrated {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
                compute,
            } => {
                let location = mz_catalog::durable::ReplicaLocation::Unmanaged {
                    storagectl_addrs,
                    storage_addrs,
                    computectl_addrs,
                    compute_addrs,
                    workers,
                };
                (compute, location)
            }
            mz_sql::plan::ReplicaConfig::Orchestrated {
                availability_zone,
                billed_as,
                compute,
                disk,
                internal,
                size,
            } => {
                let availability_zone = match availability_zone {
                    Some(az) => {
                        self.ensure_valid_azs([&az])?;
                        Some(az)
                    }
                    None => None,
                };
                let location = mz_catalog::durable::ReplicaLocation::Managed {
                    availability_zone,
                    billed_as,
                    disk,
                    internal,
                    size,
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

        let cluster = self.catalog().get_cluster(cluster_id);

        if let ReplicaLocation::Managed(ManagedReplicaLocation {
            internal,
            billed_as,
            ..
        }) = &config.location
        {
            // Only internal users have access to INTERNAL and BILLED AS
            if !session.user().is_internal() && (*internal || billed_as.is_some()) {
                coord_bail!("cannot specify INTERNAL or BILLED AS as non-internal user")
            }
            // Managed clusters require the INTERNAL flag.
            if cluster.is_managed() && !*internal {
                coord_bail!("must specify INTERNAL when creating a replica in a managed cluster");
            }
            // BILLED AS implies the INTERNAL flag.
            if billed_as.is_some() && !*internal {
                coord_bail!("must specify INTERNAL when specifying BILLED AS");
            }
        }

        // Replicas have the same owner as their cluster.
        let owner_id = cluster.owner_id();
        let id = self.catalog_mut().allocate_user_replica_id().await?;
        let op = catalog::Op::CreateClusterReplica {
            cluster_id,
            id,
            name: name.clone(),
            config,
            owner_id,
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
            let replica_config = cluster
                .replica(replica_id)
                .expect("known to exist")
                .config
                .clone();

            replicas_to_start.push(CreateReplicaConfig {
                cluster_id,
                replica_id,
                role,
                config: replica_config,
            });
        }

        let enable_worker_core_affinity =
            self.catalog().system_config().enable_worker_core_affinity();
        self.controller
            .create_replicas(replicas_to_start, enable_worker_core_affinity)
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
        use mz_catalog::memory::objects::ClusterVariant::*;

        let config = self.catalog.get_cluster(cluster_id).config.clone();
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
                    interval: Some(DEFAULT_REPLICA_LOGGING_INTERVAL),
                };
                new_config.variant = Managed(ClusterVariantManaged {
                    size,
                    availability_zones: Default::default(),
                    logging,
                    idle_arrangement_merge_effort: None,
                    replication_factor: 1,
                    disk,
                    optimizer_feature_overrides: Default::default(),
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
                optimizer_feature_overrides: _,
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
                    Set(ii) => logging.interval = ii.0,
                    Reset => logging.interval = Some(DEFAULT_REPLICA_LOGGING_INTERVAL),
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
                    coord_bail!("Cannot change REPLICAS of managed clusters");
                }
            }
            Unmanaged => {
                use AlterOptionParameter::*;
                if !matches!(options.size, Unchanged) {
                    coord_bail!("Cannot change SIZE of unmanaged clusters");
                }
                if !matches!(options.availability_zones, Unchanged) {
                    coord_bail!("Cannot change AVAILABILITY ZONES of unmanaged clusters");
                }
                if !matches!(options.introspection_debugging, Unchanged) {
                    coord_bail!("Cannot change INTROSPECTION DEGUBBING of unmanaged clusters");
                }
                if !matches!(options.introspection_interval, Unchanged) {
                    coord_bail!("Cannot change INTROSPECTION INTERVAL of unmanaged clusters");
                }
                if !matches!(options.idle_arrangement_merge_effort, Unchanged) {
                    coord_bail!(
                        "Cannot change IDLE ARRANGEMENT MERGE EFFORT of unmanaged clusters"
                    );
                }
                if !matches!(options.replication_factor, Unchanged) {
                    coord_bail!("Cannot change REPLICATION FACTOR of unmanaged clusters");
                }
            }
        }

        if new_config == config {
            return Ok(ExecuteResponse::AlteredObject(ObjectType::Cluster));
        }

        match (&config.variant, new_config.variant) {
            (Managed(config), Managed(new_config)) => {
                self.sequence_alter_cluster_managed_to_managed(
                    session, cluster_id, config, new_config,
                )
                .await?;
            }
            (Unmanaged, Managed(new_config)) => {
                self.sequence_alter_cluster_unmanaged_to_managed(
                    session, cluster_id, new_config, options,
                )
                .await?;
            }
            (Managed(_), Unmanaged) => {
                self.sequence_alter_cluster_managed_to_unmanaged(session, cluster_id)
                    .await?;
            }
            (Unmanaged, Unmanaged) => {
                self.sequence_alter_cluster_unmanaged_to_unmanaged(
                    session,
                    cluster_id,
                    options.replicas,
                )?;
            }
        }

        Ok(ExecuteResponse::AlteredObject(ObjectType::Cluster))
    }

    async fn sequence_alter_cluster_managed_to_managed(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
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
                optimizer_feature_overrides: _,
            },
            ClusterVariantManaged {
                size: new_size,
                replication_factor: new_replication_factor,
                availability_zones: new_availability_zones,
                logging: new_logging,
                idle_arrangement_merge_effort: new_idle_arrangement_merge_effort,
                disk: new_disk,
                optimizer_feature_overrides: _,
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
            for name in (0..*replication_factor).map(managed_cluster_replica_name) {
                let replica = cluster.replica_id(&name);
                if let Some(replica) = replica {
                    ops.push(catalog::Op::DropObject(ObjectId::ClusterReplica((
                        cluster.id(),
                        replica,
                    ))))
                }
            }
            for name in (0..*new_replication_factor).map(managed_cluster_replica_name) {
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
                )?;
                create_cluster_replicas.push((cluster_id, id))
            }
        } else if new_replication_factor < replication_factor {
            // Adjust size down
            for name in
                (*new_replication_factor..*replication_factor).map(managed_cluster_replica_name)
            {
                let replica = cluster.replica_id(&name);
                if let Some(replica) = replica {
                    ops.push(catalog::Op::DropObject(ObjectId::ClusterReplica((
                        cluster.id(),
                        replica,
                    ))))
                }
            }
        } else if new_replication_factor > replication_factor {
            // Adjust size up
            for name in
                (*replication_factor..*new_replication_factor).map(managed_cluster_replica_name)
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
                )?;
                create_cluster_replicas.push((cluster_id, id))
            }
        }

        let variant = ClusterVariant::Managed(new_config);
        ops.push(catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name,
            config: ClusterConfig { variant },
        });

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
            optimizer_feature_overrides: _,
        } = &mut new_config;

        // Validate replication factor parameter
        let user_replica_count = cluster
            .user_replicas()
            .count()
            .try_into()
            .expect("must_fit");
        match options.replication_factor {
            AlterOptionParameter::Set(_) => {
                // Validate that the replication factor matches the current length only if specified.
                if user_replica_count != *new_replication_factor {
                    coord_bail!("REPLICATION FACTOR {new_replication_factor} does not match number of replicas ({user_replica_count})");
                }
            }
            _ => {
                *new_replication_factor = user_replica_count;
            }
        }

        let mut names = BTreeSet::new();
        let mut sizes = BTreeSet::new();
        let mut disks = BTreeSet::new();

        self.ensure_valid_azs(new_availability_zones.iter())?;

        // Validate per-replica configuration
        for replica in cluster.user_replicas() {
            names.insert(replica.name.clone());
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
                cluster.user_replicas().next().is_none(),
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
            let name = managed_cluster_replica_name(i);
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
        ops.push(catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster_name,
            config: ClusterConfig { variant },
        });

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    async fn sequence_alter_cluster_managed_to_unmanaged(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog().get_cluster(cluster_id);
        let mut ops = vec![];

        let variant = ClusterVariant::Unmanaged;
        ops.push(catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster.name().to_string(),
            config: ClusterConfig { variant },
        });

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    fn sequence_alter_cluster_unmanaged_to_unmanaged(
        &self,
        _session: &Session,
        _cluster_id: ClusterId,
        _replicas: AlterOptionParameter<Vec<(String, mz_sql::plan::ReplicaConfig)>>,
    ) -> Result<(), AdapterError> {
        coord_bail!("Cannot alter unmanaged cluster");
    }

    pub(super) async fn sequence_alter_cluster_rename(
        &mut self,
        session: &mut Session,
        AlterClusterRenamePlan { id, name, to_name }: AlterClusterRenamePlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op = Op::RenameCluster {
            id,
            name,
            to_name,
            check_reserved_names: true,
        };
        match self
            .catalog_transact_with_ddl_transaction(session, vec![op])
            .await
        {
            Ok(()) => Ok(ExecuteResponse::AlteredObject(ObjectType::Cluster)),
            Err(err) => Err(err),
        }
    }

    pub(super) async fn sequence_alter_cluster_swap(
        &mut self,
        session: &mut Session,
        AlterClusterSwapPlan {
            id_a,
            id_b,
            name_a,
            name_b,
            name_temp,
        }: AlterClusterSwapPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let op_a = Op::RenameCluster {
            id: id_a,
            name: name_a.clone(),
            to_name: name_temp.clone(),
            check_reserved_names: false,
        };
        let op_b = Op::RenameCluster {
            id: id_b,
            name: name_b.clone(),
            to_name: name_a,
            check_reserved_names: false,
        };
        let op_temp = Op::RenameCluster {
            id: id_a,
            name: name_temp,
            to_name: name_b,
            check_reserved_names: false,
        };

        match self
            .catalog_transact_with_ddl_transaction(session, vec![op_a, op_b, op_temp])
            .await
        {
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
}

fn managed_cluster_replica_name(index: u32) -> String {
    format!("r{}", index + 1)
}
