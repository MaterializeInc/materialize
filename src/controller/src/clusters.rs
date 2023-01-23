// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster management.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use futures::stream::{BoxStream, StreamExt};
use mz_ore::halt;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use tracing::{error, warn};

use mz_compute_client::controller::{
    ComputeInstanceId, ComputeReplicaConfig, ComputeReplicaLocation, ComputeReplicaLogging,
};
use mz_compute_client::logging::LogVariant;
use mz_compute_client::service::{ComputeClient, ComputeGrpcClient};
use mz_orchestrator::{
    CpuLimit, LabelSelectionLogic, LabelSelector, MemoryLimit, Service, ServiceConfig,
    ServiceEvent, ServicePort,
};
use mz_ore::task::JoinHandleExt;
use mz_repr::GlobalId;

use crate::Controller;

pub use mz_compute_client::controller::DEFAULT_COMPUTE_REPLICA_LOGGING_INTERVAL_MICROS as DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS;

/// Identifies a cluster.
pub type ClusterId = ComputeInstanceId;

/// Configures a cluster.
pub struct ClusterConfig {
    /// The logging variants to enable on the compute instance.
    ///
    /// Each logging variant is mapped to the identifier under which to register
    /// the arrangement storing the log's data.
    pub arranged_logs: BTreeMap<LogVariant, GlobalId>,
}

/// The status of a cluster.
pub type ClusterStatus = mz_orchestrator::ServiceStatus;

/// Identifies a cluster replica.
pub type ReplicaId = mz_compute_client::controller::ReplicaId;

/// Configures a cluster replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaConfig {
    /// The location of the replica.
    pub location: ReplicaLocation,
    /// Configuration for the compute half of the replica.
    pub compute: ComputeReplicaConfig,
}

/// Configures the resource allocation for a cluster replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaAllocation {
    /// The memory limit for each process in the replica.
    pub memory_limit: Option<MemoryLimit>,
    /// The CPU limit for each process in the replica.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of processes in the replica.
    pub scale: u16,
    /// The number of worker threads in the replica.
    pub workers: usize,
}

/// Configures the location of a cluster replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplicaLocation {
    /// An unmanaged replica.
    Unmanaged(UnmanagedReplicaLocation),
    /// A managed replica.
    Managed(ManagedReplicaLocation),
}

impl ReplicaLocation {
    /// Returns the availability zone specified by this replica location, if
    /// any.
    pub fn availability_zone(&self) -> Option<&str> {
        match self {
            ReplicaLocation::Unmanaged(_) => None,
            ReplicaLocation::Managed(m) => Some(&m.availability_zone),
        }
    }

    /// Returns the number of processes specified by this replica location.
    pub fn num_processes(&self) -> usize {
        match self {
            ReplicaLocation::Unmanaged(UnmanagedReplicaLocation {
                computectl_addrs, ..
            }) => computectl_addrs.len(),
            ReplicaLocation::Managed(ManagedReplicaLocation { allocation, .. }) => {
                allocation.scale.into()
            }
        }
    }
}

/// The location of an unmanaged replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnmanagedReplicaLocation {
    /// The network address of the storagectl endpoints for the first process in
    /// the replica.
    pub storagectl_addr: String,
    /// The network addresses of the computectl endpoints for each process in
    /// the replica.
    pub computectl_addrs: Vec<String>,
    /// The network addresses of the compute (Timely) endpoints for
    /// each process in the replica.
    pub compute_addrs: Vec<String>,
    /// The workers per process in the replica.
    pub workers: usize,
}

/// The location of a managed replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManagedReplicaLocation {
    /// The resource allocation for the replica.
    pub allocation: ReplicaAllocation,
    /// SQL size parameter used for allocation
    pub size: String,
    /// The replica's availability zone
    pub availability_zone: String,
    /// `true` if the AZ was specified by the user and must be respected;
    /// `false` if it was picked arbitrarily by Materialize.
    pub az_user_specified: bool,
}

/// Configures logging for a cluster replica.
pub type ReplicaLogging = ComputeReplicaLogging;

/// Identifier of a process within a replica.
pub type ProcessId = u64;

/// An event describing a change in status of a cluster replica process.
#[derive(Debug, Clone, Serialize)]
pub struct ClusterEvent {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub process_id: ProcessId,
    pub status: ClusterStatus,
    pub time: DateTime<Utc>,
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
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
        self.storage.create_instance(id);
        self.compute.create_instance(id, config.arranged_logs)?;
        Ok(())
    }

    /// Drops the specified cluster.
    ///
    /// # Panics
    ///
    /// Panics if the cluster still has replicas.
    pub fn drop_cluster(&mut self, id: ClusterId) {
        self.storage.drop_instance(id);
        self.compute.drop_instance(id);
    }

    /// Creates a replica of the specified cluster with the specified identifier
    /// and configuration.
    pub async fn create_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        config: ReplicaConfig,
    ) -> Result<(), anyhow::Error> {
        let (storage_addr, compute_location) = match config.location {
            ReplicaLocation::Unmanaged(u) => {
                let storage_addr = u.storagectl_addr;
                let compute_location = ComputeReplicaLocation {
                    computectl_addrs: u.computectl_addrs,
                    compute_addrs: u.compute_addrs,
                    workers: u.workers,
                };
                (storage_addr, compute_location)
            }
            ReplicaLocation::Managed(m) => {
                let workers = m.allocation.workers;
                let service = self.provision_replica(cluster_id, replica_id, m).await?;
                let storage_addr = service
                    .addresses("storagectl")
                    .into_iter()
                    .next()
                    .expect("should be at least one process");
                let compute_location = ComputeReplicaLocation {
                    computectl_addrs: service.addresses("computectl"),
                    compute_addrs: service.addresses("compute"),
                    workers,
                };
                (storage_addr, compute_location)
            }
        };

        self.storage.connect_replica(cluster_id, storage_addr);
        self.active_compute().add_replica_to_instance(
            cluster_id,
            replica_id,
            compute_location,
            config.compute,
        )?;
        Ok(())
    }

    /// Drops the specified replica of the specified cluster.
    pub async fn drop_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> Result<(), anyhow::Error> {
        // We unconditionally deprovision even for unmanaged replicas to avoid
        // needing to keep track of which replicas are managed and which are
        // unmanaged. Deprovisioning is a no-op if the replica ID was never
        // provisioned.
        self.deprovision_replica(cluster_id, replica_id).await?;
        self.metrics_tasks.remove(&replica_id);

        // Storage does not support active-active replication and so does not
        // have an API for dropping replicas.
        self.active_compute().drop_replica(cluster_id, replica_id)?;
        Ok(())
    }

    /// Remove orphaned replicas.
    pub async fn remove_orphaned_replicas(
        &mut self,
        next_replica_id: ReplicaId,
    ) -> Result<(), anyhow::Error> {
        let desired: HashSet<_> = self.metrics_tasks.keys().copied().collect();

        let actual: HashSet<_> = self
            .orchestrator
            .list_services()
            .await?
            .iter()
            .map(|s| parse_replica_service_name(s))
            .collect::<Result<_, _>>()?;

        for (cluster_id, replica_id) in actual {
            if replica_id >= next_replica_id {
                // Found a replica in kubernetes with a higher replica ID than
                // what we are aware of. This must have been created by an
                // environmentd with higher epoch number.
                halt!(
                    "found replica id ({}) in orchestrator >= next id ({})",
                    replica_id,
                    next_replica_id
                );
            }

            if !desired.contains(&replica_id) {
                self.deprovision_replica(cluster_id, replica_id).await?;
            }
        }

        Ok(())
    }

    pub fn events_stream(&self) -> BoxStream<'static, ClusterEvent> {
        fn translate_event(event: ServiceEvent) -> Result<ClusterEvent, anyhow::Error> {
            let (cluster_id, replica_id) = parse_replica_service_name(&event.service_id)?;
            Ok(ClusterEvent {
                cluster_id,
                replica_id,
                process_id: event.process_id,
                status: event.status,
                time: event.time,
            })
        }

        let stream = self
            .orchestrator
            .watch_services()
            .map(|event| event.and_then(translate_event))
            .filter_map(|event| async {
                match event {
                    Ok(event) => Some(event),
                    Err(error) => {
                        error!("service watch error: {error}");
                        None
                    }
                }
            });

        Box::pin(stream)
    }

    /// Provisions a replica with the service orchestrator.
    async fn provision_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        location: ManagedReplicaLocation,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let service_name = generate_replica_service_name(cluster_id, replica_id);

        let service = self
            .orchestrator
            .ensure_service(
                &service_name,
                ServiceConfig {
                    image: self.clusterd_image.clone(),
                    init_container_image: self.init_container_image.clone(),
                    args: &|assigned| {
                        vec![
                            format!("--storage-workers={}", location.allocation.workers),
                            format!(
                                "--storage-controller-listen-addr={}",
                                assigned["storagectl"]
                            ),
                            format!(
                                "--compute-controller-listen-addr={}",
                                assigned["computectl"]
                            ),
                            format!("--internal-http-listen-addr={}", assigned["internal-http"]),
                            format!("--opentelemetry-resource=cluster_id={}", cluster_id),
                            format!("--opentelemetry-resource=replica_id={}", replica_id),
                        ]
                    },
                    ports: vec![
                        ServicePort {
                            name: "storagectl".into(),
                            port_hint: 2100,
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
                    memory_limit: location.allocation.memory_limit,
                    scale: location.allocation.scale,
                    labels: BTreeMap::from([
                        ("replica-id".into(), replica_id.to_string()),
                        ("cluster-id".into(), cluster_id.to_string()),
                        ("type".into(), "cluster".into()),
                    ]),
                    availability_zone: Some(location.availability_zone),
                    // This constrains the orchestrator (for those orchestrators that support
                    // anti-affinity, today just k8s) to never schedule pods for different replicas
                    // of the same cluster on the same node. Pods from the _same_ replica are fine;
                    // pods from different clusters are also fine.
                    //
                    // The point is that if pods of two replicas are on the same node, that node
                    // going down would kill both replicas, and so the replication factor of the
                    // cluster in question is illusory.
                    anti_affinity: Some(vec![
                        LabelSelector {
                            label_name: "cluster-id".to_string(),
                            logic: LabelSelectionLogic::Eq {
                                value: cluster_id.to_string(),
                            },
                        },
                        LabelSelector {
                            label_name: "replica-id".into(),
                            logic: LabelSelectionLogic::NotEq {
                                value: replica_id.to_string(),
                            },
                        },
                    ]),
                },
            )
            .await?;

        let metrics_task = mz_ore::task::spawn(|| format!("replica-metrics-{replica_id}"), {
            let tx = self.metrics_tx.clone();
            let orchestrator = Arc::clone(&self.orchestrator);
            let service_name = service_name.clone();
            async move {
                const METRICS_INTERVAL: Duration = Duration::from_secs(10);

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
        self.metrics_tasks
            .insert(replica_id, metrics_task.abort_on_drop());

        Ok(service)
    }

    /// Deprovisions a replica with the service orchestrator.
    async fn deprovision_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> Result<(), anyhow::Error> {
        let service_name = generate_replica_service_name(cluster_id, replica_id);
        self.orchestrator.drop_service(&service_name).await
    }
}

/// Deterministically generates replica names based on inputs.
fn generate_replica_service_name(cluster_id: ClusterId, replica_id: ReplicaId) -> String {
    format!("{cluster_id}-replica-{replica_id}")
}

/// Parses a name generated by `generate_replica_service_name`, to extract the
/// replica's cluster ID and replica ID.
fn parse_replica_service_name(
    service_name: &str,
) -> Result<(ComputeInstanceId, ReplicaId), anyhow::Error> {
    static SERVICE_NAME_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?-u)^([us]\d+)-replica-(\d+)$").unwrap());

    let caps = SERVICE_NAME_RE
        .captures(service_name)
        .ok_or_else(|| anyhow!("invalid service name: {service_name}"))?;

    let cluster_id = caps.get(1).unwrap().as_str().parse().unwrap();
    let replica_id = caps.get(2).unwrap().as_str().parse().unwrap();
    Ok((cluster_id, replica_id))
}
