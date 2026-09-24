// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster management.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::anyhow;
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, StreamExt};
use mz_cluster_client::client::{ClusterReplicaLocation, TimelyConfig};
use mz_compute_client::logging::LogVariant;
pub use mz_controller_types::clusters::{
    ClusterRole, ClusterStatus, ManagedReplicaLocation, ReplicaAllocation, ReplicaConfig,
    ReplicaLocation, ReplicaLogging, UnmanagedReplicaLocation,
};
use mz_controller_types::dyncfgs::{
    ARRANGEMENT_EXERT_PROPORTIONALITY, CONTROLLER_PAST_GENERATION_REPLICA_CLEANUP_RETRY_INTERVAL,
    ENABLE_TIMELY_ZERO_COPY, ENABLE_TIMELY_ZERO_COPY_LGALLOC, ENABLE_UNIFIED_CLUSTER,
    TIMELY_ZERO_COPY_LIMIT,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_orchestrator::NamespacedOrchestrator;
use mz_orchestrator::{
    DiskLimit, LabelSelectionLogic, LabelSelector, MemoryLimit, Service, ServiceConfig,
    ServiceEvent, ServicePort,
};
use mz_ore::task::{self, AbortOnDropHandle};
use mz_ore::{halt, instrument};
use mz_repr::GlobalId;
use regex::Regex;
use serde::Serialize;
use tokio::time;
use tracing::{error, info, warn};

use crate::Controller;

/// Configures a cluster.
pub struct ClusterConfig {
    /// The logging variants to enable on the compute instance.
    ///
    /// Each logging variant is mapped to the identifier under which to register
    /// the arrangement storing the log's data.
    pub arranged_logs: BTreeMap<LogVariant, GlobalId>,
    /// An optional arbitrary string that describes the class of the workload
    /// this cluster is running (e.g., `production` or `staging`).
    pub workload_class: Option<String>,
}

/// Identifier of a process within a replica.
pub type ProcessId = u64;

/// An event describing a change in status of a cluster replica process.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterEvent {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub process_id: ProcessId,
    pub status: ClusterStatus,
    /// Cumulative restart count of the process, propagated from the orchestrator.
    /// See [`mz_orchestrator::ServiceEvent::restart_count`].
    pub restart_count: u64,
    pub time: DateTime<Utc>,
}

impl Controller {
    /// Creates a cluster with the specified identifier and configuration.
    ///
    /// A cluster is a combination of a storage instance and a compute instance.
    /// A cluster has zero or more replicas; each replica colocates the storage
    /// and compute layers on the same physical resources.
    pub fn create_cluster(
        &mut self,
        id: ClusterId,
        config: ClusterConfig,
    ) -> Result<(), anyhow::Error> {
        self.storage
            .create_instance(id, config.workload_class.clone());
        if !self.replica_owned_compute() {
            self.compute
                .create_instance(id, config.arranged_logs, config.workload_class)?;
        }
        Ok(())
    }

    /// Updates the workload class for a cluster.
    ///
    /// # Panics
    ///
    /// Panics if the instance does not exist in the StorageController or the ComputeController.
    pub fn update_cluster_workload_class(&mut self, id: ClusterId, workload_class: Option<String>) {
        self.storage
            .update_instance_workload_class(id, workload_class.clone());
        if !self.replica_owned_compute() {
            self.compute
                .update_instance_workload_class(id, workload_class)
                .expect("instance exists");
        }
    }

    /// Drops the specified cluster.
    ///
    /// # Panics
    ///
    /// Panics if the cluster still has replicas.
    pub fn drop_cluster(&mut self, id: ClusterId) {
        self.storage.drop_instance(id);
        if !self.replica_owned_compute() {
            self.compute.drop_instance(id);
        }
    }

    /// Creates a replica of the specified cluster with the specified identifier
    /// and configuration.
    pub fn create_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        cluster_name: String,
        replica_name: String,
        role: ClusterRole,
        config: ReplicaConfig,
        enable_worker_core_affinity: bool,
    ) -> Result<(), anyhow::Error> {
        let storage_location: ClusterReplicaLocation;
        let compute_location: ClusterReplicaLocation;
        let metrics_task: Option<AbortOnDropHandle<()>>;

        match config.location {
            ReplicaLocation::Unmanaged(UnmanagedReplicaLocation {
                storagectl_addrs,
                computectl_addrs,
            }) => {
                compute_location = ClusterReplicaLocation {
                    ctl_addrs: computectl_addrs,
                };
                storage_location = ClusterReplicaLocation {
                    ctl_addrs: storagectl_addrs,
                };
                metrics_task = None;
            }
            ReplicaLocation::Managed(m) => {
                let (service, metrics_task_join_handle) = self.provision_replica(
                    cluster_id,
                    replica_id,
                    cluster_name,
                    replica_name,
                    role,
                    m,
                    enable_worker_core_affinity,
                )?;
                storage_location = ClusterReplicaLocation {
                    ctl_addrs: service.addresses("storagectl"),
                };
                compute_location = ClusterReplicaLocation {
                    ctl_addrs: service.addresses("computectl"),
                };
                metrics_task = Some(metrics_task_join_handle);

                // Register the replica for HTTP proxying.
                let http_addresses = service.addresses("internal-http");
                self.replica_http_locator
                    .register_replica(cluster_id, replica_id, http_addresses);
            }
        }

        if self.replica_owned_compute() {
            self.storage.register_replica(cluster_id, replica_id);
        } else {
            self.storage
                .connect_replica(cluster_id, replica_id, storage_location);
            self.compute.add_replica_to_instance(
                cluster_id,
                replica_id,
                compute_location,
                config.compute,
            )?;
        }

        if let Some(task) = metrics_task {
            self.metrics_tasks.insert(replica_id, task);
        }

        Ok(())
    }

    /// Drops the specified replica of the specified cluster.
    pub fn drop_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> Result<(), anyhow::Error> {
        // We unconditionally deprovision even for unmanaged replicas to avoid
        // needing to keep track of which replicas are managed and which are
        // unmanaged. Deprovisioning is a no-op if the replica ID was never
        // provisioned.
        self.deprovision_replica(cluster_id, replica_id, self.deploy_generation)?;
        self.metrics_tasks.remove(&replica_id);

        // Remove HTTP addresses from the locator.
        self.replica_http_locator
            .remove_replica(cluster_id, replica_id);

        // The coordinator only re-pushes the override map when the scoped
        // configuration itself changes, so a dropped replica's entry would
        // otherwise be retained until the next such change.
        self.replica_dyncfg_overrides.remove(&replica_id);

        if !self.replica_owned_compute() {
            self.compute.drop_replica(cluster_id, replica_id)?;
        }
        self.storage.drop_replica(cluster_id, replica_id);
        Ok(())
    }

    /// Removes replicas from past generations in a background task.
    pub(crate) fn remove_past_generation_replicas_in_background(&self) {
        let deploy_generation = self.deploy_generation;
        let dyncfg = Arc::clone(self.compute.dyncfg());
        let orchestrator = Arc::clone(&self.orchestrator);
        task::spawn(
            || "controller_remove_past_generation_replicas",
            async move {
                info!("attempting to remove past generation replicas");
                loop {
                    match try_remove_past_generation_replicas(&*orchestrator, deploy_generation)
                        .await
                    {
                        Ok(()) => {
                            info!("successfully removed past generation replicas");
                            return;
                        }
                        Err(e) => {
                            let interval =
                                CONTROLLER_PAST_GENERATION_REPLICA_CLEANUP_RETRY_INTERVAL
                                    .get(&dyncfg);
                            warn!(%e, "failed to remove past generation replicas; will retry in {interval:?}");
                            time::sleep(interval).await;
                        }
                    }
                }
            },
        );
    }

    /// Lists actual replica services across all generations.
    ///
    /// For snapshot-based orphan cleanup, await this list before reading
    /// authoritative durable catalog replica membership.
    pub async fn list_replica_services(&self) -> Result<Vec<ReplicaServiceName>, anyhow::Error> {
        self.orchestrator
            .list_services()
            .await?
            .iter()
            .map(|s| s.parse())
            .collect()
    }

    /// Removes only observed, current-generation services absent from `live`.
    ///
    /// The caller must obtain `observed` from `list_replica_services` BEFORE
    /// fetching `live` from the authoritative durable catalog, not a bootstrap
    /// or controller-local snapshot. Service creation must follow catalog commit
    /// and replica IDs must never be reused. Thus an observed service absent from
    /// the later catalog snapshot cannot belong to an in-flight allocation.
    /// Services created after the list are left for a subsequent cleanup pass.
    pub fn remove_orphaned_replicas_from_snapshot(
        &self,
        observed: Vec<ReplicaServiceName>,
        live: BTreeSet<(ClusterId, ReplicaId)>,
    ) -> Result<(), anyhow::Error> {
        remove_orphaned_replica_services(observed, &live, self.deploy_generation, |name| {
            self.deprovision_replica(name.cluster_id, name.replica_id, name.generation)
        })
    }

    /// Remove replicas that are orphaned in the current generation using local
    /// inventory and allocator bounds, for unprotected upgrade/prewarming.
    #[instrument]
    pub async fn remove_orphaned_replicas(
        &mut self,
        next_user_replica_id: u64,
        next_system_replica_id: u64,
    ) -> Result<(), anyhow::Error> {
        let desired: BTreeSet<_> = self.metrics_tasks.keys().copied().collect();

        let actual: BTreeSet<_> = self
            .orchestrator
            .list_services()
            .await?
            .iter()
            .map(|s| ReplicaServiceName::from_str(s))
            .collect::<Result<_, _>>()?;

        for ReplicaServiceName {
            cluster_id,
            replica_id,
            generation,
        } in actual
        {
            // We limit our attention here to replicas from the current deploy
            // generation. Replicas from past generations are cleaned up during
            // `Controller::allow_writes`.
            if generation != self.deploy_generation {
                continue;
            }

            let smaller_next = match replica_id {
                ReplicaId::User(id) if id >= next_user_replica_id => {
                    Some(ReplicaId::User(next_user_replica_id))
                }
                ReplicaId::System(id) if id >= next_system_replica_id => {
                    Some(ReplicaId::System(next_system_replica_id))
                }
                _ => None,
            };
            if let Some(next) = smaller_next {
                // Found a replica in the orchestrator with a higher replica ID
                // than what we are aware of. This must have been created by an
                // environmentd that's competing for control of this generation.
                // Abort to let the other process have full control.
                halt!("found replica ID ({replica_id}) in orchestrator >= next ID ({next})");
            }
            if !desired.contains(&replica_id) {
                self.deprovision_replica(cluster_id, replica_id, generation)?;
            }
        }

        Ok(())
    }

    pub fn events_stream(&self) -> BoxStream<'static, ClusterEvent> {
        let deploy_generation = self.deploy_generation;

        fn translate_event(event: ServiceEvent) -> Result<(ClusterEvent, u64), anyhow::Error> {
            let ReplicaServiceName {
                cluster_id,
                replica_id,
                generation: replica_generation,
                ..
            } = event.service_id.parse()?;

            let event = ClusterEvent {
                cluster_id,
                replica_id,
                process_id: event.process_id,
                status: event.status,
                restart_count: event.restart_count,
                time: event.time,
            };

            Ok((event, replica_generation))
        }

        let stream = self
            .orchestrator
            .watch_services()
            .map(|event| event.and_then(translate_event))
            .filter_map(move |event| async move {
                match event {
                    Ok((event, replica_generation)) => {
                        if replica_generation == deploy_generation {
                            Some(event)
                        } else {
                            None
                        }
                    }
                    Err(error) => {
                        error!("service watch error: {error}");
                        None
                    }
                }
            });

        Box::pin(stream)
    }

    /// Provisions a replica with the service orchestrator.
    fn provision_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        cluster_name: String,
        replica_name: String,
        role: ClusterRole,
        location: ManagedReplicaLocation,
        enable_worker_core_affinity: bool,
    ) -> Result<(Box<dyn Service>, AbortOnDropHandle<()>), anyhow::Error> {
        let service_name = ReplicaServiceName {
            cluster_id,
            replica_id,
            generation: self.deploy_generation,
        }
        .to_string();
        let role_label = match role {
            ClusterRole::SystemCritical => "system-critical",
            ClusterRole::System => "system",
            ClusterRole::User => "user",
        };
        let environment_id = self.connection_context().environment_id.clone();
        let aws_external_id_prefix = self.connection_context().aws_external_id_prefix.clone();
        let aws_connection_role_arn = self.connection_context().aws_connection_role_arn.clone();
        let persist_pubsub_url = self.persist_pubsub_url.clone();
        let catalog_persist_location = self.catalog_persist_location.clone();
        let catalog_follower_config = self.catalog_follower_config.clone();
        if catalog_persist_location.is_some() && catalog_follower_config.is_none() {
            anyhow::bail!("catalog follower config must be set before provisioning replicas");
        }
        let deploy_generation = self.deploy_generation;
        let secrets_args = self.secrets_args.to_flags();

        // These configure the replica's process rather than environmentd's, so
        // they are `ParameterScope::Replica` and must be read through this
        // replica's scoped overrides. They are baked into the process
        // configuration at provisioning time, so a later change to either the
        // environment-wide value or the override reaches the replica only when
        // it is next provisioned.
        let overrides = self.replica_dyncfg_overrides.get(&replica_id);
        // Storage and compute arrangements share one maintenance policy, so a
        // unified replica runs both kinds of arrangement under the same reach.
        let arrangement_exert_proportionality =
            ARRANGEMENT_EXERT_PROPORTIONALITY.get_with_overrides(&self.dyncfg, overrides);
        let storage_proto_timely_config = TimelyConfig {
            arrangement_exert_proportionality,
            ..Default::default()
        };
        let compute_proto_timely_config = TimelyConfig {
            arrangement_exert_proportionality,
            enable_zero_copy: ENABLE_TIMELY_ZERO_COPY.get_with_overrides(&self.dyncfg, overrides),
            enable_zero_copy_lgalloc: ENABLE_TIMELY_ZERO_COPY_LGALLOC
                .get_with_overrides(&self.dyncfg, overrides),
            zero_copy_limit: TIMELY_ZERO_COPY_LIMIT.get_with_overrides(&self.dyncfg, overrides),
            ..Default::default()
        };
        let unified_cluster = ENABLE_UNIFIED_CLUSTER.get_with_overrides(&self.dyncfg, overrides);

        let mut disk_limit = location.allocation.disk_limit;
        let memory_limit = location.allocation.memory_limit;
        let mut memory_request = None;

        if location.allocation.swap_enabled {
            // The disk limit we specify in the service config decides whether or not the replica
            // gets a scratch disk attached. We want to avoid attaching disks to swap replicas, so
            // make sure to set the disk limit accordingly.
            disk_limit = Some(DiskLimit::ZERO);

            // We want to keep the memory request equal to the memory limit, to avoid
            // over-provisioning and ensure replicas have predictable performance. However, to
            // enable swap, Kubernetes currently requires that request and limit are different.
            memory_request = memory_limit.map(|MemoryLimit(limit)| {
                let request = ByteSize::b(limit.as_u64() - 1);
                MemoryLimit(request)
            });
        }

        let service = self.orchestrator.ensure_service(
            &service_name,
            ServiceConfig {
                app_name: "clusterd".into(),
                image: self.clusterd_image.clone(),
                init_container_image: self.init_container_image.clone(),
                args: Box::new(move |assigned| {
                    let storage_timely_config = TimelyConfig {
                        workers: location.allocation.workers.get(),
                        addresses: assigned.peer_addresses("storage"),
                        ..storage_proto_timely_config
                    };
                    let compute_timely_config = TimelyConfig {
                        workers: location.allocation.workers.get(),
                        addresses: assigned.peer_addresses("compute"),
                        ..compute_proto_timely_config
                    };

                    let mut args = vec![
                        format!(
                            "--storage-controller-listen-addr={}",
                            assigned.listen_addrs["storagectl"]
                        ),
                        format!(
                            "--compute-controller-listen-addr={}",
                            assigned.listen_addrs["computectl"]
                        ),
                        format!(
                            "--internal-http-listen-addr={}",
                            assigned.listen_addrs["internal-http"]
                        ),
                        format!("--opentelemetry-resource=cluster_id={}", cluster_id),
                        format!("--opentelemetry-resource=replica_id={}", replica_id),
                        format!("--persist-pubsub-url={}", persist_pubsub_url),
                        format!("--environment-id={}", environment_id),
                        format!(
                            "--storage-timely-config={}",
                            storage_timely_config.to_string(),
                        ),
                        format!(
                            "--compute-timely-config={}",
                            compute_timely_config.to_string(),
                        ),
                    ];
                    if let Some(location) = &catalog_persist_location {
                        args.extend([
                            format!("--catalog-cluster-id={cluster_id}"),
                            format!("--catalog-replica-id={replica_id}"),
                            format!("--catalog-deploy-generation={deploy_generation}"),
                            format!(
                                "--catalog-config={}",
                                catalog_follower_config
                                    .as_ref()
                                    .expect("checked before provisioning")
                            ),
                            format!(
                                "--catalog-persist-blob-url={}",
                                location.blob_uri.to_string_unredacted()
                            ),
                            format!(
                                "--catalog-persist-consensus-url={}",
                                location.consensus_uri.to_string_unredacted()
                            ),
                        ]);
                    }
                    if let Some(aws_external_id_prefix) = &aws_external_id_prefix {
                        args.push(format!(
                            "--aws-external-id-prefix={}",
                            aws_external_id_prefix
                        ));
                    }
                    if let Some(aws_connection_role_arn) = &aws_connection_role_arn {
                        args.push(format!(
                            "--aws-connection-role-arn={}",
                            aws_connection_role_arn
                        ));
                    }
                    if let Some(memory_limit) = location.allocation.memory_limit {
                        args.push(format!(
                            "--announce-memory-limit={}",
                            memory_limit.0.as_u64()
                        ));
                    }
                    if location.allocation.cpu_exclusive && enable_worker_core_affinity {
                        args.push("--worker-core-affinity".into());
                    }
                    if unified_cluster {
                        args.push("--unified-cluster".into());
                    }
                    if location.allocation.is_cc {
                        args.push("--is-cc".into());
                    }

                    // If swap is enabled, make the replica limit its own heap usage based on the
                    // configured memory and disk limits.
                    if location.allocation.swap_enabled
                        && let Some(memory_limit) = location.allocation.memory_limit
                        && let Some(disk_limit) = location.allocation.disk_limit
                        // Currently, the way for replica sizes to request unlimited swap is to
                        // specify a `disk_limit` of 0. Ideally we'd change this to make them
                        // specify no disk limit instead, but for now we need to special-case here.
                        && disk_limit != DiskLimit::ZERO
                    {
                        let heap_limit = memory_limit.0 + disk_limit.0;
                        args.push(format!("--heap-limit={}", heap_limit.as_u64()));
                    }

                    args.extend(secrets_args.clone());
                    args
                }),
                ports: vec![
                    ServicePort {
                        name: "storagectl".into(),
                        port_hint: 2100,
                    },
                    // To simplify the changes to tests, the port
                    // chosen here is _after_ the compute ones.
                    // TODO(petrosagg): fix the numerical ordering here
                    ServicePort {
                        name: "storage".into(),
                        port_hint: 2103,
                    },
                    ServicePort {
                        name: "computectl".into(),
                        port_hint: 2101,
                    },
                    ServicePort {
                        name: "compute".into(),
                        port_hint: 2102,
                    },
                    ServicePort {
                        name: "internal-http".into(),
                        port_hint: 6878,
                    },
                ],
                cpu_limit: location.allocation.cpu_limit,
                cpu_request: location.allocation.cpu_request,
                memory_limit,
                memory_request,
                scale: location.allocation.scale,
                labels: BTreeMap::from([
                    ("replica-id".into(), replica_id.to_string()),
                    ("cluster-id".into(), cluster_id.to_string()),
                    ("generation".into(), self.deploy_generation.to_string()),
                    ("type".into(), "cluster".into()),
                    ("replica-role".into(), role_label.into()),
                    ("workers".into(), location.allocation.workers.to_string()),
                    (
                        "size".into(),
                        location
                            .size
                            .to_string()
                            .replace("=", "-")
                            .replace(",", "_"),
                    ),
                ]),
                annotations: BTreeMap::from([
                    (
                        "replica-name".into(),
                        format!("{cluster_name}.{replica_name}"),
                    ),
                    ("cluster-name".into(), cluster_name),
                ]),
                // An empty list means no AZ constraint; a non-empty one pins
                // placement to those zones.
                availability_zones: Some(location.availability_zones).filter(|azs| !azs.is_empty()),
                // This provides the orchestrator with some label selectors that
                // are used to constraint the scheduling of replicas, based on
                // its internal configuration.
                //
                // Selectors include `generation` so that scheduling constraints
                // (anti-affinity, topology spread) only consider pods of the same
                // deploy generation. Otherwise, during a generation rollout, the
                // new-generation pods would be constrained by the placement of
                // old-generation pods that are about to be torn down, which can
                // prevent the new pods from scheduling (e.g., when only one AZ
                // has capacity but it is already occupied by an old-generation
                // pod).
                other_replicas_selector: vec![
                    LabelSelector {
                        label_name: "cluster-id".to_string(),
                        logic: LabelSelectionLogic::Eq {
                            value: cluster_id.to_string(),
                        },
                    },
                    // Select other replicas (but not oneself)
                    LabelSelector {
                        label_name: "replica-id".into(),
                        logic: LabelSelectionLogic::NotEq {
                            value: replica_id.to_string(),
                        },
                    },
                    LabelSelector {
                        label_name: "generation".into(),
                        logic: LabelSelectionLogic::Eq {
                            value: self.deploy_generation.to_string(),
                        },
                    },
                ],
                replicas_selector: vec![
                    LabelSelector {
                        label_name: "cluster-id".to_string(),
                        // Select ALL replicas.
                        logic: LabelSelectionLogic::Eq {
                            value: cluster_id.to_string(),
                        },
                    },
                    LabelSelector {
                        label_name: "generation".into(),
                        logic: LabelSelectionLogic::Eq {
                            value: self.deploy_generation.to_string(),
                        },
                    },
                ],
                disk_limit,
                node_selector: location.allocation.selectors,
            },
        )?;

        let metrics_task = mz_ore::task::spawn(|| format!("replica-metrics-{replica_id}"), {
            let tx = self.metrics_tx.clone();
            let orchestrator = Arc::clone(&self.orchestrator);
            let service_name = service_name.clone();
            async move {
                const METRICS_INTERVAL: Duration = Duration::from_secs(60);

                // TODO[btv] -- I tried implementing a `watch_metrics` function,
                // similar to `watch_services`, but it crashed due to
                // https://github.com/kube-rs/kube/issues/1092 .
                //
                // If `metrics-server` can be made to fill in `resourceVersion`,
                // or if that bug is fixed, we can try that again rather than using this inelegant
                // loop.
                let mut interval = tokio::time::interval(METRICS_INTERVAL);
                loop {
                    interval.tick().await;
                    match orchestrator.fetch_service_metrics(&service_name).await {
                        Ok(metrics) => {
                            let _ = tx.send((replica_id, metrics));
                        }
                        Err(e) => {
                            warn!("failed to get metrics for replica {replica_id}: {e}");
                        }
                    }
                }
            }
        });

        Ok((service, metrics_task.abort_on_drop()))
    }

    /// Deprovisions a replica with the service orchestrator.
    fn deprovision_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        generation: u64,
    ) -> Result<(), anyhow::Error> {
        let service_name = ReplicaServiceName {
            cluster_id,
            replica_id,
            generation,
        }
        .to_string();
        self.orchestrator.drop_service(&service_name)
    }
}

fn remove_orphaned_replica_services(
    observed: Vec<ReplicaServiceName>,
    live: &BTreeSet<(ClusterId, ReplicaId)>,
    deploy_generation: u64,
    mut drop_service: impl FnMut(ReplicaServiceName) -> Result<(), anyhow::Error>,
) -> Result<(), anyhow::Error> {
    for name in observed {
        if name.generation == deploy_generation
            && !live.contains(&(name.cluster_id, name.replica_id))
        {
            drop_service(name)?;
        }
    }
    Ok(())
}

/// Remove all replicas from past generations.
async fn try_remove_past_generation_replicas(
    orchestrator: &dyn NamespacedOrchestrator,
    deploy_generation: u64,
) -> Result<(), anyhow::Error> {
    let services: BTreeSet<_> = orchestrator.list_services().await?.into_iter().collect();

    for service in services {
        let name: ReplicaServiceName = service.parse()?;
        if name.generation < deploy_generation {
            info!(
                cluster_id = %name.cluster_id,
                replica_id = %name.replica_id,
                "removing past generation replica",
            );
            orchestrator.drop_service(&service)?;
        }
    }

    Ok(())
}

/// Represents the name of a cluster replica service in the orchestrator.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplicaServiceName {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub generation: u64,
}

impl fmt::Display for ReplicaServiceName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ReplicaServiceName {
            cluster_id,
            replica_id,
            generation,
        } = self;
        write!(f, "{cluster_id}-replica-{replica_id}-gen-{generation}")
    }
}

impl FromStr for ReplicaServiceName {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        static SERVICE_NAME_RE: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"(?-u)^([us]\d+)-replica-([us]\d+)(?:-gen-(\d+))?$").unwrap()
        });

        let caps = SERVICE_NAME_RE
            .captures(s)
            .ok_or_else(|| anyhow!("invalid service name: {s}"))?;

        Ok(ReplicaServiceName {
            cluster_id: caps.get(1).unwrap().as_str().parse().unwrap(),
            replica_id: caps.get(2).unwrap().as_str().parse().unwrap(),
            // Old versions of Materialize did not include generations in
            // replica service names. Synthesize generation 0 if absent.
            // TODO: remove this in the next version of Materialize.
            generation: caps.get(3).map_or("0", |m| m.as_str()).parse().unwrap(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn snapshot_orphan_cleanup() {
        // The actuator inventory includes a committed replica missing from the
        // caller's bootstrap inventory, an orphan, and other generations.
        let mut services: BTreeSet<String> = [
            "u1-replica-u1-gen-2",
            "u1-replica-u2-gen-2",
            "u1-replica-u3-gen-1",
            "u1-replica-u4-gen-3",
            "s1-replica-s1-gen-2",
        ]
        .map(String::from)
        .into_iter()
        .collect();
        let observed = services.iter().map(|s| s.parse().unwrap()).collect();

        // Read authoritative membership after listing, including the newly
        // committed replica. A service arriving after the list is out of scope,
        // even if it is absent from this catalog snapshot.
        let live = BTreeSet::from([
            (ClusterId::User(1), ReplicaId::User(1)),
            (ClusterId::System(1), ReplicaId::System(1)),
        ]);
        services.insert("u1-replica-u5-gen-2".into());
        remove_orphaned_replica_services(observed, &live, 2, |name| {
            assert!(services.remove(&name.to_string()));
            Ok(())
        })
        .unwrap();

        assert_eq!(
            services,
            [
                "u1-replica-u1-gen-2",
                "u1-replica-u3-gen-1",
                "u1-replica-u4-gen-3",
                "u1-replica-u5-gen-2",
                "s1-replica-s1-gen-2",
            ]
            .map(String::from)
            .into_iter()
            .collect()
        );
    }

    #[mz_ore::test]
    fn snapshot_orphan_cleanup_propagates_drop_failure() {
        let observed = vec!["u1-replica-u1-gen-2".parse().unwrap()];
        let result = remove_orphaned_replica_services(observed, &BTreeSet::new(), 2, |_| {
            Err(anyhow!("drop failed"))
        });
        assert_eq!(result.unwrap_err().to_string(), "drop failed");
    }
}
