// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator functionality to sequence cluster-related plans

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use mz_compute_client::controller::ComputeReplicaConfig;
use mz_controller::clusters::{
    ClusterId, CreateReplicaConfig, ReplicaConfig, ReplicaId, ReplicaLocation, ReplicaLogging,
    DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS,
};
use mz_sql::catalog::{CatalogCluster, CatalogItem, CatalogItemType, ObjectType};
use mz_sql::names::ObjectId;
use mz_sql::plan::{
    AlterClusterPlan, AlterClusterRenamePlan, AlterClusterReplicaRenamePlan, AlterOptionParameter,
    ComputeReplicaIntrospectionConfig, CreateClusterManagedPlan, CreateClusterPlan,
    CreateClusterReplicaPlan, CreateClusterUnmanagedPlan, CreateClusterVariant, PlanClusterOption,
};
use rand::seq::SliceRandom;

use crate::catalog::{
    ClusterConfig, ClusterVariant, ClusterVariantManaged, Op, SerializedReplicaLocation,
};
use crate::coord::{Coordinator, DEFAULT_LOGICAL_COMPACTION_WINDOW_TS};
use crate::session::Session;
use crate::{catalog, AdapterError, ExecuteResponse};

/// Helper to select availability zones based on the least-populated zone.
pub struct AzHelper {
    n_replicas_per_az: BTreeMap<String, usize>,
}

impl AzHelper {
    pub(crate) fn new(availability_zones: impl IntoIterator<Item = impl ToString>) -> Self {
        let n_replicas_per_az = availability_zones
            .into_iter()
            .map(|s| (s.to_string(), 0))
            .collect::<BTreeMap<_, _>>();
        Self { n_replicas_per_az }
    }

    // Utility function used by both `sequence_create_cluster` and
    // `sequence_create_cluster_replica`. Chooses the availability zone for a
    // replica arbitrarily based on some state (currently: the number of
    // replicas of the given cluster per AZ).
    pub(crate) fn choose_az(&self) -> String {
        let min = *self
            .n_replicas_per_az
            .values()
            .min()
            .expect("Must have at least one availability zone");
        let argmins = self
            .n_replicas_per_az
            .iter()
            .filter_map(|(k, v)| (*v == min).then_some(k))
            .collect::<Vec<_>>();
        let arbitrary_argmin = argmins
            .choose(&mut rand::thread_rng())
            .expect("Must have at least one value corresponding to `min`");

        (*arbitrary_argmin).clone()
    }

    /// Select an availability zone and increment its count.
    fn choose_az_and_increment(&mut self) -> String {
        let az = self.choose_az();
        self.increment(&az);
        az
    }

    /// Increments the count for a specific availability zone.
    ///
    /// Panics if the availability zone does not exist.
    fn increment(&mut self, az: &str) {
        self.try_increment(az)
            .expect("Availability zone must exist");
    }

    /// Try to increment the count for an availability zone. Returns `None` if it doesn't exist, and
    /// the count after incrementing otherwise.
    fn try_increment(&mut self, az: &str) -> Option<usize> {
        let count = self.n_replicas_per_az.get_mut(az)?;
        *count += 1;
        Some(*count)
    }
}

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
        }: CreateClusterManagedPlan,
        cluster_id: ClusterId,
        mut ops: Vec<catalog::Op>,
    ) -> Result<ExecuteResponse, AdapterError> {
        tracing::debug!("sequence_create_managed_cluster");

        let (azs, az_user_specified) = if availability_zones.is_empty() {
            (self.catalog().state().availability_zones(), false)
        } else {
            (&*availability_zones, true)
        };
        let mut az_helper = AzHelper::new(azs);

        let allowed_replica_sizes = &self
            .catalog()
            .system_config()
            .allowed_cluster_replica_sizes();
        self.catalog
            .ensure_valid_replica_size(allowed_replica_sizes, &size)?;

        for replica_name in (0..replication_factor).map(managed_cluster_replica_name) {
            let id = self.catalog_mut().allocate_replica_id().await?;
            self.create_managed_cluster_replica_op(
                session,
                cluster_id,
                id,
                replica_name,
                az_user_specified,
                &compute,
                &size,
                &mut ops,
                &mut az_helper,
            )?;
        }

        self.catalog_transact(Some(session), ops).await?;

        self.create_cluster(cluster_id).await;

        Ok(ExecuteResponse::CreatedCluster)
    }

    fn create_managed_cluster_replica_op(
        &mut self,
        session: &Session,
        cluster_id: ClusterId,
        id: ReplicaId,
        name: String,
        az_user_specified: bool,
        compute: &mz_sql::plan::ComputeReplicaConfig,
        size: &String,
        ops: &mut Vec<Op>,
        az_helper: &mut AzHelper,
    ) -> Result<(), AdapterError> {
        let availability_zone = az_helper.choose_az_and_increment();
        let location = SerializedReplicaLocation::Managed {
            size: size.clone(),
            availability_zone,
            az_user_specified,
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
            cluster_id,
            id,
            name,
            config,
            owner_id: *session.current_role_id(),
        });
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

        let azs = self.catalog().state().availability_zones();
        let mut az_helper = AzHelper::new(azs);
        for (_name, r) in replicas.iter() {
            if let mz_sql::plan::ReplicaConfig::Managed {
                availability_zone: Some(az),
                ..
            } = r
            {
                az_helper.try_increment(az).ok_or_else(|| {
                    AdapterError::InvalidClusterReplicaAz {
                        az: az.to_string(),
                        expected: azs.to_vec(),
                    }
                })?;
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
                    let (availability_zone, user_specified) = availability_zone
                        .map(|az| (az, true))
                        .unwrap_or_else(|| (az_helper.choose_az_and_increment(), false));
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

    pub(super) async fn create_cluster(&mut self, cluster_id: ClusterId) {
        let (catalog, controller) = self.catalog_and_controller_mut();
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
                        let mut az_helper = AzHelper::new(azs);
                        for r in cluster.replicas_by_id.values() {
                            if let Some(az) = r.config.location.availability_zone() {
                                az_helper.increment(az);
                            }
                        }
                        (az_helper.choose_az(), false)
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

    pub(super) async fn sequence_alter_cluster(
        &mut self,
        session: &Session,
        AlterClusterPlan {
            id: cluster_id,
            name: _,
            options,
        }: AlterClusterPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        use catalog::ClusterVariant::*;

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

                // Size adjusted later when sequencing the actual configuration change.
                let size = "".to_string();
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
            }) => {
                use AlterOptionParameter::*;
                match &options.size {
                    Set(s) => *size = s.clone(),
                    Reset => coord_bail!("SIZE has no default value"),
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
        let mut ops = vec![];

        let (
            ClusterVariantManaged {
                size,
                replication_factor,
                availability_zones,
                logging,
                idle_arrangement_merge_effort,
            },
            ClusterVariantManaged {
                size: new_size,
                replication_factor: new_replication_factor,
                availability_zones: new_availability_zones,
                logging: new_logging,
                idle_arrangement_merge_effort: new_idle_arrangement_merge_effort,
            },
        ) = (&config, &new_config);

        let allowed_replica_sizes = &self
            .catalog()
            .system_config()
            .allowed_cluster_replica_sizes();
        self.catalog
            .ensure_valid_replica_size(allowed_replica_sizes, new_size)?;

        let (azs, az_user_specified) = if new_availability_zones.is_empty() {
            (self.catalog().state().availability_zones(), false)
        } else {
            (&**new_availability_zones, true)
        };
        let mut az_helper = AzHelper::new(azs);
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

        if new_size != size
            || new_availability_zones != availability_zones
            || new_idle_arrangement_merge_effort != idle_arrangement_merge_effort
            || new_logging != logging
        {
            // tear down all replicas, create new ones
            for name in (0..*replication_factor).map(managed_cluster_replica_name) {
                let replica = cluster.replica_id_by_name.get(&name);
                if let Some(replica) = replica {
                    ops.push(catalog::Op::DropObject(ObjectId::ClusterReplica((
                        cluster.id(),
                        *replica,
                    ))))
                }
            }
            for name in (0..*new_replication_factor).map(managed_cluster_replica_name) {
                let id = self.catalog_mut().allocate_replica_id().await?;
                self.create_managed_cluster_replica_op(
                    session,
                    cluster_id,
                    id,
                    name,
                    az_user_specified,
                    &compute,
                    new_size,
                    &mut ops,
                    &mut az_helper,
                )?;
                create_cluster_replicas.push((cluster_id, id))
            }
        } else if new_replication_factor < replication_factor {
            // Adjust size down
            for name in
                (*new_replication_factor..*replication_factor).map(managed_cluster_replica_name)
            {
                let replica = cluster.replica_id_by_name.get(&name);
                if let Some(replica) = replica {
                    ops.push(catalog::Op::DropObject(ObjectId::ClusterReplica((
                        cluster.id(),
                        *replica,
                    ))))
                }
            }
        } else if new_replication_factor > replication_factor {
            // Adjust size up

            // Update availability zone counts based on current configuration.
            for az in cluster
                .replicas_by_id
                .values()
                .flat_map(|r| r.config.location.availability_zone())
            {
                az_helper.increment(az);
            }
            for name in
                (*replication_factor..*new_replication_factor).map(managed_cluster_replica_name)
            {
                let id = self.catalog_mut().allocate_replica_id().await?;
                self.create_managed_cluster_replica_op(
                    session,
                    cluster_id,
                    id,
                    name,
                    az_user_specified,
                    &compute,
                    new_size,
                    &mut ops,
                    &mut az_helper,
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
            availability_zones: _,
            logging: _,
            idle_arrangement_merge_effort: _,
        } = &mut new_config;

        // Validate replication factor parameter
        match options.replication_factor {
            AlterOptionParameter::Set(_) => {
                // Validate that the replication factor matches the current length only if specified.
                if u32::try_from(cluster.replicas_by_id.len()).expect("must fit")
                    != *new_replication_factor
                {
                    coord_bail!("REPLICATION FACTOR {new_replication_factor} does not match number of replicas ({})", cluster.replicas_by_id.len());
                }
            }
            _ => {
                *new_replication_factor = cluster.replicas_by_id.len().try_into().expect("must fit")
            }
        }

        let mut names = BTreeSet::new();
        let mut sizes = BTreeSet::new();

        // Validate per-replica configuration
        for replica in cluster.replicas_by_id.values() {
            names.insert(replica.name.clone());
            match &replica.config.location {
                ReplicaLocation::Unmanaged(_) => coord_bail!(
                    "Cannot convert unmanaged cluster with unmanaged replicas to managed cluster"
                ),
                ReplicaLocation::Managed(location) => {
                    sizes.insert(location.size.clone());
                }
            }
        }

        if sizes.is_empty() {
            assert!(
                cluster.replicas_by_id.is_empty(),
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

fn managed_cluster_replica_name(index: u32) -> String {
    format!("r{}", index + 1)
}
