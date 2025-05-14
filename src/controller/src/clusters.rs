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
use chrono::{DateTime, Utc};
use futures::stream::{BoxStream, StreamExt};
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_compute_client::controller::ComputeControllerTimestamp;
use mz_compute_client::logging::LogVariant;
use mz_compute_client::service::{ComputeClient, ComputeGrpcClient};
use mz_compute_types::config::{ComputeReplicaConfig, ComputeReplicaLogging};
use mz_controller_types::dyncfgs::CONTROLLER_PAST_GENERATION_REPLICA_CLEANUP_RETRY_INTERVAL;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_orchestrator::NamespacedOrchestrator;
use mz_orchestrator::{
    CpuLimit, DiskLimit, LabelSelectionLogic, LabelSelector, MemoryLimit, Service, ServiceConfig,
    ServiceEvent, ServicePort,
};
use mz_ore::halt;
use mz_ore::instrument;
use mz_ore::task::{self, AbortOnDropHandle};
use mz_repr::GlobalId;
use mz_repr::adt::numeric::Numeric;
use regex::Regex;
use serde::{Deserialize, Serialize};
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

/// The status of a cluster.
pub type ClusterStatus = mz_orchestrator::ServiceStatus;

/// Configures a cluster replica.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ReplicaConfig {
    /// The location of the replica.
    pub location: ReplicaLocation,
    /// Configuration for the compute half of the replica.
    pub compute: ComputeReplicaConfig,
}

/// Configures the resource allocation for a cluster replica.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplicaAllocation {
    /// The memory limit for each process in the replica.
    pub memory_limit: Option<MemoryLimit>,
    /// The CPU limit for each process in the replica.
    pub cpu_limit: Option<CpuLimit>,
    /// The disk limit for each process in the replica.
    pub disk_limit: Option<DiskLimit>,
    /// The number of processes in the replica.
    pub scale: u16,
    /// The number of worker threads in the replica.
    pub workers: usize,
    /// The number of credits per hour that the replica consumes.
    #[serde(deserialize_with = "mz_repr::adt::numeric::str_serde::deserialize")]
    pub credits_per_hour: Numeric,
    /// Whether each process has exclusive access to its CPU cores.
    #[serde(default)]
    pub cpu_exclusive: bool,
    /// Whether this size represents a modern "cc" size rather than a legacy
    /// T-shirt size.
    #[serde(default = "default_true")]
    pub is_cc: bool,
    /// Whether instances of this type can be created.
    #[serde(default)]
    pub disabled: bool,
    /// Additional node selectors.
    #[serde(default)]
    pub selectors: BTreeMap<String, String>,
}

fn default_true() -> bool {
    true
}

#[mz_ore::test]
// We test this particularly because we deserialize values from strings.
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
fn test_replica_allocation_deserialization() {
    use bytesize::ByteSize;

    let data = r#"
        {
            "cpu_limit": 1.0,
            "memory_limit": "10GiB",
            "disk_limit": "100MiB",
            "scale": 16,
            "workers": 1,
            "credits_per_hour": "16",
            "selectors": {
                "key1": "value1",
                "key2": "value2"
            }
        }"#;

    let replica_allocation: ReplicaAllocation = serde_json::from_str(data)
        .expect("deserialization from JSON succeeds for ReplicaAllocation");

    assert_eq!(
        replica_allocation,
        ReplicaAllocation {
            credits_per_hour: 16.into(),
            disk_limit: Some(DiskLimit(ByteSize::mib(100))),
            disabled: false,
            memory_limit: Some(MemoryLimit(ByteSize::gib(10))),
            cpu_limit: Some(CpuLimit::from_millicpus(1000)),
            cpu_exclusive: false,
            is_cc: true,
            scale: 16,
            workers: 1,
            selectors: BTreeMap::from([
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string())
            ]),
        }
    );

    let data = r#"
        {
            "cpu_limit": 0,
            "memory_limit": "0GiB",
            "disk_limit": "0MiB",
            "scale": 0,
            "workers": 0,
            "credits_per_hour": "0",
            "cpu_exclusive": true,
            "disabled": true
        }"#;

    let replica_allocation: ReplicaAllocation = serde_json::from_str(data)
        .expect("deserialization from JSON succeeds for ReplicaAllocation");

    assert_eq!(
        replica_allocation,
        ReplicaAllocation {
            credits_per_hour: 0.into(),
            disk_limit: Some(DiskLimit(ByteSize::mib(0))),
            disabled: true,
            memory_limit: Some(MemoryLimit(ByteSize::gib(0))),
            cpu_limit: Some(CpuLimit::from_millicpus(0)),
            cpu_exclusive: true,
            is_cc: true,
            scale: 0,
            workers: 0,
            selectors: Default::default(),
        }
    );
}

/// Configures the location of a cluster replica.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub enum ReplicaLocation {
    /// An unmanaged replica.
    Unmanaged(UnmanagedReplicaLocation),
    /// A managed replica.
    Managed(ManagedReplicaLocation),
}

impl ReplicaLocation {
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

    pub fn billed_as(&self) -> Option<&str> {
        match self {
            ReplicaLocation::Managed(ManagedReplicaLocation { billed_as, .. }) => {
                billed_as.as_deref()
            }
            _ => None,
        }
    }

    pub fn internal(&self) -> bool {
        match self {
            ReplicaLocation::Managed(ManagedReplicaLocation { internal, .. }) => *internal,
            ReplicaLocation::Unmanaged(_) => false,
        }
    }

    pub fn workers(&self) -> usize {
        let workers_per_process = match self {
            ReplicaLocation::Managed(ManagedReplicaLocation { allocation, .. }) => {
                allocation.workers
            }
            ReplicaLocation::Unmanaged(UnmanagedReplicaLocation { workers, .. }) => *workers,
        };
        workers_per_process * self.num_processes()
    }

    /// A pending replica is created as part of an alter cluster of an managed
    /// cluster. the configuration of a pending replica will not match that of
    /// the clusters until the alter has been finalized promoting the pending
    /// replicas and setting this value to false.
    pub fn pending(&self) -> bool {
        match self {
            ReplicaLocation::Managed(ManagedReplicaLocation { pending, .. }) => *pending,
            _ => false,
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
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

/// Information about availability zone constraints for replicas.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManagedReplicaAvailabilityZones {
    /// Specified if the `Replica` is from `MANAGED` cluster,
    /// and specifies if there is an `AVAILABILITY ZONES`
    /// constraint. Empty lists are represented as `None`.
    FromCluster(Option<Vec<String>>),
    /// Specified if the `Replica` is from a non-`MANAGED` cluster,
    /// and specifies if there is a specific `AVAILABILITY ZONE`.
    FromReplica(Option<String>),
}

/// The location of a managed replica.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ManagedReplicaLocation {
    /// The resource allocation for the replica.
    pub allocation: ReplicaAllocation,
    /// SQL size parameter used for allocation
    pub size: String,
    /// If `true`, Materialize support owns this replica.
    pub internal: bool,
    /// Optional SQL size parameter used for billing.
    pub billed_as: Option<String>,
    /// The replica's availability zones, if specified.
    ///
    /// This is either the replica's specific `AVAILABILITY ZONE`,
    /// or the zones placed here during replica concretization
    /// from the `MANAGED` cluster config.
    ///
    /// We skip serialization (which is used for some validation
    /// in tests) as the latter case is a "virtual" piece of information,
    /// that exists only at runtime.
    ///
    /// An empty list of availability zones is concretized as `None`,
    /// as the on-disk serialization of `MANAGED CLUSTER AVAILABILITY ZONES`
    /// is an empty list if none are specified
    #[serde(skip)]
    pub availability_zones: ManagedReplicaAvailabilityZones,
    /// Whether the replica needs scratch disk space.
    pub disk: bool,
    /// Whether the repelica is pending reconfiguration
    pub pending: bool,
}

impl ManagedReplicaLocation {
    /// Return the size which should be used to determine billing-related information.
    pub fn size_for_billing(&self) -> &str {
        self.billed_as.as_deref().unwrap_or(&self.size)
    }
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
    T: ComputeControllerTimestamp,
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
        self.storage
            .create_instance(id, config.workload_class.clone());
        self.compute
            .create_instance(id, config.arranged_logs, config.workload_class)?;
        Ok(())
    }

    /// Updates the workload class for a cluster.
    pub fn update_cluster_workload_class(
        &mut self,
        id: ClusterId,
        workload_class: Option<String>,
    ) -> Result<(), anyhow::Error> {
        self.storage
            .update_instance_workload_class(id, workload_class.clone());
        self.compute
            .update_instance_workload_class(id, workload_class)?;
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
    pub fn create_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
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
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
            }) => {
                compute_location = ClusterReplicaLocation {
                    ctl_addrs: computectl_addrs,
                    dataflow_addrs: compute_addrs,
                    workers,
                };
                storage_location = ClusterReplicaLocation {
                    ctl_addrs: storagectl_addrs,
                    dataflow_addrs: storage_addrs,
                    // Storage and compute on the same replica have linked sizes.
                    workers,
                };
                metrics_task = None;
            }
            ReplicaLocation::Managed(m) => {
                let workers = m.allocation.workers;
                let (service, metrics_task_join_handle) = self.provision_replica(
                    cluster_id,
                    replica_id,
                    role,
                    m,
                    enable_worker_core_affinity,
                )?;
                storage_location = ClusterReplicaLocation {
                    ctl_addrs: service.addresses("storagectl"),
                    dataflow_addrs: service.addresses("storage"),
                    workers,
                };
                compute_location = ClusterReplicaLocation {
                    ctl_addrs: service.addresses("computectl"),
                    dataflow_addrs: service.addresses("compute"),
                    workers,
                };
                metrics_task = Some(metrics_task_join_handle);
            }
        }

        self.storage
            .connect_replica(cluster_id, replica_id, storage_location);
        self.compute.add_replica_to_instance(
            cluster_id,
            replica_id,
            compute_location,
            config.compute,
        )?;

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

        self.compute.drop_replica(cluster_id, replica_id)?;
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

    /// Remove replicas that are orphaned in the current generation.
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
        let secrets_args = self.secrets_args.to_flags();
        let service = self.orchestrator.ensure_service(
            &service_name,
            ServiceConfig {
                image: self.clusterd_image.clone(),
                init_container_image: self.init_container_image.clone(),
                args: Box::new(move |assigned| {
                    let mut args = vec![
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
                        format!("--environment-id={}", environment_id),
                    ];
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
                    if location.allocation.is_cc {
                        args.push("--is-cc".into());
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
                memory_limit: location.allocation.memory_limit,
                scale: location.allocation.scale,
                labels: BTreeMap::from([
                    ("replica-id".into(), replica_id.to_string()),
                    ("cluster-id".into(), cluster_id.to_string()),
                    ("type".into(), "cluster".into()),
                    ("replica-role".into(), role_label.into()),
                    ("workers".into(), location.allocation.workers.to_string()),
                    ("size".into(), location.size.to_string()),
                ]),
                availability_zones: match location.availability_zones {
                    ManagedReplicaAvailabilityZones::FromCluster(azs) => azs,
                    ManagedReplicaAvailabilityZones::FromReplica(az) => az.map(|z| vec![z]),
                },
                // This provides the orchestrator with some label selectors that
                // are used to constraint the scheduling of replicas, based on
                // its internal configuration.
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
                ],
                replicas_selector: vec![LabelSelector {
                    label_name: "cluster-id".to_string(),
                    // Select ALL replicas.
                    logic: LabelSelectionLogic::Eq {
                        value: cluster_id.to_string(),
                    },
                }],
                disk_limit: location.allocation.disk_limit,
                disk: location.disk,
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
