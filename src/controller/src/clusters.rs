// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster management.

pub mod provisioning;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, StreamExt};
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_compute_client::logging::LogVariant;
pub use mz_controller_types::clusters::{
    ClusterRole, ClusterStatus, ManagedReplicaLocation, ReplicaAllocation, ReplicaConfig,
    ReplicaLocation, ReplicaLogging, UnmanagedReplicaLocation,
};
use mz_controller_types::dyncfgs::CONTROLLER_PAST_GENERATION_REPLICA_CLEANUP_RETRY_INTERVAL;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_orchestrator::NamespacedOrchestrator;
use mz_orchestrator::{Service, ServiceEvent, ServicePort};
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
            self.compute.create_instance(
                id,
                config.arranged_logs,
                config.workload_class.clone(),
            )?;
        }
        self.metrics
            .set_workload_class(id.to_string(), config.workload_class);
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
                .update_instance_workload_class(id, workload_class.clone())
                .expect("instance exists");
        }
        self.metrics
            .set_workload_class(id.to_string(), workload_class);
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
        self.metrics.set_workload_class(id.to_string(), None);
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
            ReplicaLocation::Managed(m) if !self.manage_replica_processes => {
                let service = ReplicaServiceName {
                    cluster_id,
                    replica_id,
                    generation: self.deploy_generation,
                }
                .to_string();
                let addresses = |name: &str, port_hint| {
                    self.orchestrator.service_addresses(
                        &service,
                        m.allocation.scale,
                        &ServicePort {
                            name: name.into(),
                            port_hint,
                            requires_tcp: false,
                        },
                    )
                };
                storage_location = ClusterReplicaLocation {
                    ctl_addrs: addresses("storagectl", 2100)?,
                };
                compute_location = ClusterReplicaLocation {
                    ctl_addrs: addresses("computectl", 2101)?,
                };
                self.replica_http_locator.register_replica(
                    cluster_id,
                    replica_id,
                    addresses("internal-http", 6878)?,
                );
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
        publish_status_history: bool,
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
        self.storage
            .drop_replica(cluster_id, replica_id, publish_status_history);
        Ok(())
    }

    /// Removes replicas from past generations in a background task.
    pub(crate) fn remove_past_generation_replicas_in_background(&self) {
        if !self.manage_replica_processes {
            return;
        }
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

        self.orchestrator.flush().await?;
        Ok(())
    }

    pub fn events_stream(&self) -> BoxStream<'static, ClusterEvent> {
        if !self.manage_replica_processes {
            return futures::stream::pending().boxed();
        }
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
        let config = provisioning::ReplicaProvisioningConfig {
            deploy_generation: self.deploy_generation,
            connection_context: self.connection_context(),
            persist_pubsub_url: &self.persist_pubsub_url,
            catalog_persist_location: self.catalog_persist_location.as_ref(),
            catalog_timestamp_oracle_url: self.catalog_timestamp_oracle_url.as_ref(),
            timestamp_oracle_clock_file: self.timestamp_oracle_clock_file.as_deref(),
            catalog_follower_config: self.catalog_follower_config.as_deref(),
            secrets_args: &self.secrets_args,
            dyncfg: &self.dyncfg,
            replica_dyncfg_overrides: self.replica_dyncfg_overrides.get(&replica_id),
            clusterd_image: &self.clusterd_image,
            init_container_image: self.init_container_image.as_deref(),
        }
        .service_config(
            cluster_id,
            replica_id,
            cluster_name,
            replica_name,
            role,
            location,
            enable_worker_core_affinity,
        )?;
        let service = self.orchestrator.ensure_service(&service_name, config)?;

        let tx = self.metrics_tx.clone();
        let metrics_task = spawn_service_metrics_task(
            Arc::clone(&self.orchestrator),
            service_name,
            move |metrics| {
                let _ = tx.send((replica_id, metrics));
            },
        );

        Ok((service, metrics_task))
    }

    /// Deprovisions a replica with the service orchestrator.
    fn deprovision_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        generation: u64,
    ) -> Result<(), anyhow::Error> {
        if !self.manage_replica_processes {
            return Ok(());
        }
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

/// Samples a provisioned service every minute until the returned handle is dropped.
/// Backend errors skip a sample rather than terminating observation.
pub fn spawn_service_metrics_task(
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    service_name: String,
    on_metrics: impl Fn(Vec<mz_orchestrator::ServiceProcessMetrics>) + Send + 'static,
) -> AbortOnDropHandle<()> {
    let task_name = format!("service-metrics-{service_name}");
    mz_ore::task::spawn(|| task_name, async move {
        // metrics-server does not supply resourceVersion for a reliable watch.
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            interval.tick().await;
            match orchestrator.fetch_service_metrics(&service_name).await {
                Ok(metrics) => on_metrics(metrics),
                Err(error) => warn!(%service_name, %error, "failed to fetch service metrics"),
            }
        }
    })
    .abort_on_drop()
}
