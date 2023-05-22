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
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use futures::stream::{BoxStream, StreamExt};
use mz_cluster_client::client::ClusterReplicaLocation;
pub use mz_compute_client::controller::DEFAULT_COMPUTE_REPLICA_LOGGING_INTERVAL_MICROS as DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS;
use mz_compute_client::controller::{
    ComputeInstanceId, ComputeReplicaConfig, ComputeReplicaLogging,
};
use mz_compute_client::logging::LogVariant;
use mz_compute_client::service::{ComputeClient, ComputeGrpcClient};
use mz_orchestrator::{
    CpuLimit, LabelSelectionLogic, LabelSelector, MemoryLimit, Service, ServiceConfig,
    ServiceEvent, ServicePort,
};
use mz_ore::halt;
use mz_ore::task::{AbortOnDropHandle, JoinHandleExt};
use mz_repr::adt::numeric::Numeric;
use mz_repr::GlobalId;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use tracing::{error, warn};

use crate::Controller;

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
pub type ReplicaId = mz_cluster_client::ReplicaId;

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
    /// The number of credits per hour that the replica consumes.
    #[serde(deserialize_with = "mz_repr::adt::numeric::str_serde::deserialize")]
    pub credits_per_hour: Numeric,
}

#[test]
#[cfg_attr(miri, ignore)]
// We test this particularly because we deserialize values from strings.
fn test_replica_allocation_deserialization() {
    let data = r#"
        {
            "cpu_limit": 1.0,
            "memory_limit": "10GiB",
            "scale": 16,
            "workers": 1,
            "credits_per_hour": "16"
        }"#;

    let _: ReplicaAllocation = serde_json::from_str(data)
        .expect("deserialization from JSON succeeds for ReplicaAllocation");
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

/// The "role" of a cluster, which is currently used to determine the
/// severity of alerts for problems with its replicas.
pub enum ClusterRole {
    /// The existence and proper functioning of the cluster's replicas is
    /// business-critical for Materialize.
    SystemCritical,
    /// Assuming no bugs, the cluster's replicas should always exist and function
    /// properly. If it doesn't, however, that is less urgent than
    /// would be the case for a `SystemCritical` replica.
    System,
    /// The cluster is controlled by the user, and might go down for
    /// reasons outside our control (e.g., OOMs).
    User,
}

/// The location of an unmanaged replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnmanagedReplicaLocation {
    /// The network addresses of the storagectl endpoints for each process in
    /// the replica.
    pub storagectl_addrs: Vec<String>,
    /// The network addresses of the storage (Timely) endpoints for
    /// each process in the replica.
    pub storage_addrs: Vec<String>,
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

/// A struct describing a replica that needs to be created,
/// using `Controller::create_replicas`.
pub struct CreateReplicaConfig {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub role: ClusterRole,
    pub config: ReplicaConfig,
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
    ///
    /// This method is NOT idempotent; It can fail between processing of different
    /// replicas and leave the controller in an inconsistent state. It is almost
    /// always wrong to do anything but abort the process on `Err`.
    pub async fn create_replicas(
        &mut self,
        replicas: Vec<CreateReplicaConfig>,
    ) -> Result<(), anyhow::Error> {
        /// A intermediate struct to hold info about a replica, to avoid
        /// a large tuple.
        struct ReplicaInfo {
            replica_id: ReplicaId,
            compute_config: ComputeReplicaConfig,
            storage_location: ClusterReplicaLocation,
            compute_location: ClusterReplicaLocation,
            metrics_task_join_handle: Option<AbortOnDropHandle<()>>,
        }

        // Reborrow the `&mut self` as immutable, as all the concurrent work to be processed in
        // this stream cannot all have exclusive access.
        let this = &*self;
        let mut replica_stream = futures::stream::iter(replicas)
            .map(|config| async move {
                let CreateReplicaConfig {
                    cluster_id,
                    replica_id,
                    role,
                    config,
                } = config;

                match config.location {
                    // This branch doesn't do any async work, so there is a slight performance
                    // opportunity to serially process it, but it makes the code worse to read.
                    ReplicaLocation::Unmanaged(UnmanagedReplicaLocation {
                        storagectl_addrs,
                        storage_addrs,
                        computectl_addrs,
                        compute_addrs,
                        workers,
                    }) => {
                        let compute_location = ClusterReplicaLocation {
                            ctl_addrs: computectl_addrs,
                            dataflow_addrs: compute_addrs,
                            workers,
                        };
                        let storage_location = ClusterReplicaLocation {
                            ctl_addrs: storagectl_addrs,
                            dataflow_addrs: storage_addrs,
                            // Storage and compute on the same replica have linked sizes.
                            workers,
                        };

                        Ok::<_, anyhow::Error>((
                            cluster_id,
                            ReplicaInfo {
                                replica_id,
                                compute_config: config.compute,
                                storage_location,
                                compute_location,
                                metrics_task_join_handle: None,
                            },
                        ))
                    }
                    ReplicaLocation::Managed(m) => {
                        let workers = m.allocation.workers;
                        let (service, metrics_task_join_handle) = this
                            .provision_replica(cluster_id, replica_id, role, m)
                            .await?;
                        let storage_location = ClusterReplicaLocation {
                            ctl_addrs: service.addresses("storagectl"),
                            dataflow_addrs: service.addresses("storage"),
                            workers,
                        };
                        let compute_location = ClusterReplicaLocation {
                            ctl_addrs: service.addresses("computectl"),
                            dataflow_addrs: service.addresses("compute"),
                            workers,
                        };
                        Ok((
                            cluster_id,
                            ReplicaInfo {
                                replica_id,
                                compute_config: config.compute,
                                storage_location,
                                compute_location,
                                metrics_task_join_handle: Some(metrics_task_join_handle),
                            },
                        ))
                    }
                }
            })
            // TODO(guswynn): make this configurable.
            .buffer_unordered(50);

        // _Usually_ `try_collect` and `collect` are the only safe ways to process a
        // `buffer_unordered`, but if we ensure we don't do
        // any async work in this loop, we are fine.
        //
        // If we do do async work in the loop, we could starve the stream itself of
        // polls, which can cause errors.
        // See the docs in `mz_storage_client::controller` for more info.
        let mut replicas: BTreeMap<_, Vec<_>> = BTreeMap::new();
        while let Some(res) = replica_stream.next().await {
            let (cluster_id, replica_info) = res?;

            replicas.entry(cluster_id).or_default().push(replica_info);
        }
        drop(replica_stream);

        for (cluster_id, replicas) in replicas {
            // We only connect to the last replica (chosen arbitrarily)
            // for storage, until we support multi-replica storage objects
            self.storage.connect_replica(
                cluster_id,
                replicas.last().unwrap().storage_location.clone(),
            );

            for ReplicaInfo {
                replica_id,
                compute_config,
                storage_location: _,
                compute_location,
                metrics_task_join_handle,
            } in replicas
            {
                if let Some(jh) = metrics_task_join_handle {
                    self.metrics_tasks.insert(replica_id, jh);
                }
                self.active_compute().add_replica_to_instance(
                    cluster_id,
                    replica_id,
                    compute_location,
                    compute_config,
                )?;
            }
        }

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

        self.active_compute().drop_replica(cluster_id, replica_id)?;
        self.storage.drop_replica(cluster_id, replica_id);
        Ok(())
    }

    /// Remove orphaned replicas.
    pub async fn remove_orphaned_replicas(
        &mut self,
        next_replica_id: ReplicaId,
    ) -> Result<(), anyhow::Error> {
        let desired: BTreeSet<_> = self.metrics_tasks.keys().copied().collect();

        let actual: BTreeSet<_> = self
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
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        role: ClusterRole,
        location: ManagedReplicaLocation,
    ) -> Result<(Box<dyn Service>, AbortOnDropHandle<()>), anyhow::Error> {
        let service_name = generate_replica_service_name(cluster_id, replica_id);
        let role_label = match role {
            ClusterRole::SystemCritical => "system-critical",
            ClusterRole::System => "system",
            ClusterRole::User => "user",
        };
        let persist_pubsub_url = self.persist_pubsub_url.clone();
        let service = self
            .orchestrator
            .ensure_service(
                &service_name,
                ServiceConfig {
                    image: self.clusterd_image.clone(),
                    init_container_image: self.init_container_image.clone(),
                    args: &|assigned| {
                        vec![
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
                            format!("--persist-pubsub-url={}", persist_pubsub_url),
                        ]
                    },
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
                    memory_limit: location.allocation.memory_limit,
                    scale: location.allocation.scale,
                    labels: BTreeMap::from([
                        ("replica-id".into(), replica_id.to_string()),
                        ("cluster-id".into(), cluster_id.to_string()),
                        ("type".into(), "cluster".into()),
                        ("replica-role".into(), role_label.into()),
                        ("scale".into(), location.allocation.scale.to_string()),
                        ("workers".into(), location.allocation.workers.to_string()),
                        ("size".into(), location.size.to_string()),
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
