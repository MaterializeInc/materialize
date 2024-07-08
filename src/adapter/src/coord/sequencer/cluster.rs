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

use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{ClusterConfig, ClusterVariant, ClusterVariantManaged};
use mz_compute_client::controller::ComputeReplicaConfig;
use mz_controller::clusters::{
    ManagedReplicaAvailabilityZones, ManagedReplicaLocation, ReplicaConfig, ReplicaLocation,
    ReplicaLogging,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_repr::role_id::RoleId;
use mz_sql::catalog::{CatalogCluster, ObjectType};
use mz_sql::plan::{
    AlterClusterPlanStrategy, AlterClusterRenamePlan, AlterClusterReplicaRenamePlan,
    AlterClusterSwapPlan, AlterOptionParameter, ComputeReplicaIntrospectionConfig,
    CreateClusterManagedPlan, CreateClusterPlan, CreateClusterReplicaPlan,
    CreateClusterUnmanagedPlan, CreateClusterVariant, PlanClusterOption,
};
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::{SystemVars, Var, MAX_REPLICAS_PER_CLUSTER};

use crate::catalog::{Op, ReplicaCreateDropReason};
use crate::coord::Coordinator;
use crate::session::Session;
use crate::{catalog, AdapterError, ExecuteResponse};

pub(crate) const PENDING_REPLICA_SUFFIX: &str = "-pending";

impl Coordinator {
    #[mz_ore::instrument(level = "debug")]
    pub(super) async fn sequence_create_cluster(
        &mut self,
        session: &Session,
        CreateClusterPlan {
            name,
            variant,
            workload_class,
        }: CreateClusterPlan,
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
                    replication_factor: plan.replication_factor,
                    disk: plan.disk,
                    optimizer_feature_overrides: plan.optimizer_feature_overrides.clone(),
                    schedule: plan.schedule.clone(),
                })
            }
            CreateClusterVariant::Unmanaged(_) => ClusterVariant::Unmanaged,
        };
        let config = ClusterConfig {
            variant: cluster_variant,
            workload_class,
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
            schedule: _,
        }: CreateClusterManagedPlan,
        cluster_id: ClusterId,
        mut ops: Vec<catalog::Op>,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_managed_cluster");

        self.ensure_valid_azs(availability_zones.iter())?;

        let role_id = session.role_metadata().current_role;
        self.catalog.ensure_valid_replica_size(
            &self
                .catalog()
                .get_role_allowed_cluster_sizes(&Some(role_id)),
            &size,
        )?;

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
            let id = self.catalog_mut().allocate_replica_id(&cluster_id).await?;
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
                false,
                *session.current_role_id(),
                ReplicaCreateDropReason::Manual,
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
        pending: bool,
        owner_id: RoleId,
        reason: ReplicaCreateDropReason,
    ) -> Result<(), AdapterError> {
        let location = mz_catalog::durable::ReplicaLocation::Managed {
            availability_zone: None,
            billed_as: None,
            disk,
            internal: false,
            size: size.clone(),
            pending,
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
                    .get_role_allowed_cluster_sizes(&Some(owner_id)),
                azs,
            )?,
            compute: ComputeReplicaConfig { logging },
        };

        ops.push(catalog::Op::CreateClusterReplica {
            cluster_id,
            id,
            name,
            config,
            owner_id,
            reason,
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
                        pending: false,
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

            let role_id = session.role_metadata().current_role;
            let config = ReplicaConfig {
                location: self.catalog().concretize_replica_location(
                    location,
                    &self
                        .catalog()
                        .get_role_allowed_cluster_sizes(&Some(role_id)),
                    None,
                )?,
                compute: ComputeReplicaConfig { logging },
            };

            let replica_id = self.catalog_mut().allocate_replica_id(&id).await?;
            ops.push(catalog::Op::CreateClusterReplica {
                cluster_id: id,
                id: replica_id,
                name: replica_name.clone(),
                config,
                owner_id: *session.current_role_id(),
                reason: ReplicaCreateDropReason::Manual,
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
                    workload_class: cluster.config.workload_class.clone(),
                },
            )
            .expect("creating cluster must not fail");

        let replica_ids: Vec<_> = cluster.replicas().map(|r| r.replica_id).collect();
        for replica_id in replica_ids {
            self.create_cluster_replica(cluster_id, replica_id).await;
        }

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
                    pending: false,
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

        let role_id = session.role_metadata().current_role;
        let config = ReplicaConfig {
            location: self.catalog().concretize_replica_location(
                location,
                &self
                    .catalog()
                    .get_role_allowed_cluster_sizes(&Some(role_id)),
                // Planning ensures all replicas in this codepath
                // are unmanaged.
                None,
            )?,
            compute: ComputeReplicaConfig { logging },
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
        let id = self.catalog_mut().allocate_replica_id(&cluster_id).await?;
        let op = catalog::Op::CreateClusterReplica {
            cluster_id,
            id,
            name: name.clone(),
            config,
            owner_id,
            reason: ReplicaCreateDropReason::Manual,
        };

        self.catalog_transact(Some(session), vec![op]).await?;

        self.create_cluster_replica(cluster_id, id).await;

        Ok(ExecuteResponse::CreatedClusterReplica)
    }

    pub(super) async fn create_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) {
        let cluster = self.catalog().get_cluster(cluster_id);
        let role = cluster.role();
        let replica_config = cluster
            .replica(replica_id)
            .expect("known to exist")
            .config
            .clone();

        let enable_worker_core_affinity =
            self.catalog().system_config().enable_worker_core_affinity();

        self.controller
            .create_replica(
                cluster_id,
                replica_id,
                role,
                replica_config,
                enable_worker_core_affinity,
            )
            .expect("creating replicas must not fail");

        self.install_introspection_subscribes(cluster_id, replica_id)
            .await;
    }

    /// When this is called by the automated cluster scheduling, `scheduling_decision_reason` should
    /// contain information on why is a cluster being turned On/Off. It will be forwarded to the
    /// `details` field of the audit log event that records creating or dropping replicas.
    ///
    /// # Panics
    ///
    /// Panics if the identified cluster is not a managed cluster.
    /// Panics if `new_config` is not a configuration for a managed cluster.
    pub async fn sequence_alter_cluster_managed_to_managed(
        &mut self,
        session: Option<&Session>,
        cluster_id: ClusterId,
        new_config: ClusterConfig,
        reason: ReplicaCreateDropReason,
        strategy: AlterClusterPlanStrategy,
    ) -> Result<FinalizationNeeded, AdapterError> {
        let cluster = self.catalog.get_cluster(cluster_id);
        let name = cluster.name().to_string();
        let owner_id = cluster.owner_id();

        let mut ops = vec![];
        let mut create_cluster_replicas = vec![];
        let mut finalization_needed = FinalizationNeeded::False;

        let ClusterVariant::Managed(ClusterVariantManaged {
            size,
            availability_zones,
            logging,
            replication_factor,
            disk,
            optimizer_feature_overrides: _,
            schedule: _,
        }) = &cluster.config.variant
        else {
            panic!("expected existing managed cluster config");
        };
        let ClusterVariant::Managed(ClusterVariantManaged {
            size: new_size,
            replication_factor: new_replication_factor,
            availability_zones: new_availability_zones,
            logging: new_logging,
            disk: new_disk,
            optimizer_feature_overrides: _,
            schedule: _,
        }) = &new_config.variant
        else {
            panic!("expected new managed cluster config");
        };

        let role_id = session.map(|s| s.role_metadata().current_role);
        self.catalog.ensure_valid_replica_size(
            &self.catalog().get_role_allowed_cluster_sizes(&role_id),
            new_size,
        )?;

        // check for active updates
        if cluster.replicas().any(|r| r.config.location.pending()) {
            coord_bail!("Cannot Alter clusters with pending updates.")
        }

        let compute = mz_sql::plan::ComputeReplicaConfig {
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
            || new_logging != logging
            || new_disk != disk
        {
            self.ensure_valid_azs(new_availability_zones.iter())?;
            // If we're not doing a graceful reconfig
            // tear down all replicas, create new ones
            // else create the pending replicas and return
            // early asking for finalization
            match strategy {
                AlterClusterPlanStrategy::None => {
                    let replica_ids_and_reasons = (0..*replication_factor)
                        .map(managed_cluster_replica_name)
                        .filter_map(|name| cluster.replica_id(&name))
                        .map(|replica_id| {
                            catalog::DropObjectInfo::ClusterReplica((
                                cluster.id(),
                                replica_id,
                                reason.clone(),
                            ))
                        })
                        .collect();
                    ops.push(catalog::Op::DropObjects(replica_ids_and_reasons));
                    for name in (0..*new_replication_factor).map(managed_cluster_replica_name) {
                        let id = self.catalog_mut().allocate_replica_id(&cluster_id).await?;
                        self.create_managed_cluster_replica_op(
                            cluster_id,
                            id,
                            name,
                            &compute,
                            new_size,
                            &mut ops,
                            Some(new_availability_zones.as_ref()),
                            *new_disk,
                            false,
                            owner_id,
                            reason.clone(),
                        )?;
                        create_cluster_replicas.push((cluster_id, id));
                    }
                }
                AlterClusterPlanStrategy::For(duration) => {
                    for name in (0..*new_replication_factor).map(managed_cluster_replica_name) {
                        let id = self.catalog_mut().allocate_replica_id(&cluster_id).await?;
                        self.create_managed_cluster_replica_op(
                            cluster_id,
                            id,
                            format!("{name}{PENDING_REPLICA_SUFFIX}"),
                            &compute,
                            new_size,
                            &mut ops,
                            Some(new_availability_zones.as_ref()),
                            *new_disk,
                            true,
                            owner_id,
                            reason.clone(),
                        )?;
                        create_cluster_replicas.push((cluster_id, id));
                    }
                    finalization_needed = FinalizationNeeded::In(duration);
                }
                AlterClusterPlanStrategy::UntilReady { .. } => coord_bail!("Unimplemented"),
            }
        } else if new_replication_factor < replication_factor {
            // Adjust replica count down
            let replica_ids = (*new_replication_factor..*replication_factor)
                .map(managed_cluster_replica_name)
                .filter_map(|name| cluster.replica_id(&name))
                .map(|replica_id| {
                    catalog::DropObjectInfo::ClusterReplica((
                        cluster.id(),
                        replica_id,
                        reason.clone(),
                    ))
                })
                .collect();
            ops.push(catalog::Op::DropObjects(replica_ids));
        } else if new_replication_factor > replication_factor {
            // Adjust replica count up
            for name in
                (*replication_factor..*new_replication_factor).map(managed_cluster_replica_name)
            {
                let id = self.catalog_mut().allocate_replica_id(&cluster_id).await?;
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
                    false,
                    owner_id,
                    reason.clone(),
                )?;
                create_cluster_replicas.push((cluster_id, id))
            }
        }

        // If finalization is needed, finalization should update the cluster
        // config.
        match finalization_needed {
            FinalizationNeeded::False => {
                ops.push(catalog::Op::UpdateClusterConfig {
                    id: cluster_id,
                    name,
                    config: new_config,
                });
            }
            _ => {}
        }
        self.catalog_transact(session, ops.clone()).await?;
        for (cluster_id, replica_id) in create_cluster_replicas {
            self.create_cluster_replica(cluster_id, replica_id).await;
        }
        Ok(finalization_needed)
    }

    /// # Panics
    ///
    /// Panics if `new_config` is not a configuration for a managed cluster.
    pub(crate) async fn sequence_alter_cluster_unmanaged_to_managed(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        mut new_config: ClusterConfig,
        options: PlanClusterOption,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog.get_cluster(cluster_id);
        let cluster_name = cluster.name().to_string();

        let ClusterVariant::Managed(ClusterVariantManaged {
            size: new_size,
            replication_factor: new_replication_factor,
            availability_zones: new_availability_zones,
            logging: _,
            disk: new_disk,
            optimizer_feature_overrides: _,
            schedule: _,
        }) = &mut new_config.variant
        else {
            panic!("expected new managed cluster config");
        };

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

        let ops = vec![catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster_name,
            config: new_config,
        }];

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    pub(crate) async fn sequence_alter_cluster_managed_to_unmanaged(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        new_config: ClusterConfig,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog().get_cluster(cluster_id);

        let ops = vec![catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster.name().to_string(),
            config: new_config,
        }];

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
    }

    pub(crate) async fn sequence_alter_cluster_unmanaged_to_unmanaged(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        new_config: ClusterConfig,
        replicas: AlterOptionParameter<Vec<(String, mz_sql::plan::ReplicaConfig)>>,
    ) -> Result<(), AdapterError> {
        if !matches!(replicas, AlterOptionParameter::Unchanged) {
            coord_bail!("Cannot alter replicas in unmanaged cluster");
        }

        let cluster = self.catalog().get_cluster(cluster_id);

        let ops = vec![catalog::Op::UpdateClusterConfig {
            id: cluster_id,
            name: cluster.name().to_string(),
            config: new_config,
        }];

        self.catalog_transact(Some(session), ops).await?;
        Ok(())
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

/// The type of finalization needed after an
/// operation such as alter_cluster_managed_to_managed.
pub enum FinalizationNeeded {
    /// Wait for the provided duration before finalizing
    In(Duration),
    /// Finalization has already occurred
    False,
}
