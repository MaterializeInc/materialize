// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A representative of STORAGE and COMPUTE that maintains summaries of the involved objects.
//!
//! The `Controller` provides the ability to create and manipulate storage and compute instances.
//! Each of Storage and Compute provide their own controllers, accessed through the `storage()`
//! and `compute(instance_id)` methods. It is an error to access a compute instance before it has
//! been created; a single storage instance is always available.
//!
//! The controller also provides a `recv()` method that returns responses from the storage and
//! compute layers, which may remain of value to the interested user. With time, these responses
//! may be thinned down in an effort to make the controller more self contained.
//!
//! Consult the `StorageController` and `ComputeController` documentation for more information
//! about each of these interfaces.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::mem;
use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use futures::future::{self, FutureExt};
use futures::stream::{BoxStream, StreamExt};
use maplit::hashmap;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use tokio::sync::Mutex;
use uuid::Uuid;

use mz_compute_client::command::{ComputeCommand, ProcessId, ProtoComputeCommand, ReplicaId};
use mz_compute_client::controller::{
    ComputeController, ComputeControllerMut, ComputeControllerResponse, ComputeControllerState,
    ComputeInstanceId,
};
use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::response::{
    ComputeResponse, PeekResponse, ProtoComputeResponse, TailResponse,
};
use mz_compute_client::service::ComputeGrpcClient;
use mz_orchestrator::{
    CpuLimit, MemoryLimit, NamespacedOrchestrator, Orchestrator, ServiceConfig, ServiceEvent,
    ServicePort,
};
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::PersistLocation;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::GlobalId;
use mz_storage::client::controller::StorageController;
use mz_storage::client::{
    ProtoStorageCommand, ProtoStorageResponse, StorageCommand, StorageResponse,
};

pub use mz_orchestrator::ServiceStatus as ComputeInstanceStatus;

/// Configures a controller.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// The orchestrator implementation to use.
    pub orchestrator: Arc<dyn Orchestrator>,
    /// The persist location where all storage collections will be written to.
    pub persist_location: PersistLocation,
    /// A process-global cache of (blob_uri, consensus_uri) ->
    /// PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<Mutex<PersistClientCache>>,
    /// The stash URL for the storage controller.
    pub storage_stash_url: String,
    /// The storaged image to use when starting new storage processes.
    pub storaged_image: String,
    /// The computed image to use when starting new compute processes.
    pub computed_image: String,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClusterReplicaSizeConfig {
    pub memory_limit: Option<MemoryLimit>,
    pub cpu_limit: Option<CpuLimit>,
    pub scale: NonZeroUsize,
    pub workers: NonZeroUsize,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ClusterReplicaSizeMap(pub HashMap<String, ClusterReplicaSizeConfig>);

impl Default for ClusterReplicaSizeMap {
    fn default() -> Self {
        // {
        //     "1": {"scale": 1, "workers": 1},
        //     "2": {"scale": 1, "workers": 2},
        //     "4": {"scale": 1, "workers": 4},
        //     /// ...
        //     "32": {"scale": 1, "workers": 32}
        //     /// Testing with multiple processes on a single machine is a novelty, so
        //     /// we don't bother providing many options.
        //     "2-1": {"scale": 2, "workers": 1},
        //     "2-2": {"scale": 2, "workers": 2},
        //     "2-4": {"scale": 2, "workers": 4},
        // }
        let mut inner = (0..=5)
            .map(|i| {
                let workers = 1 << i;
                (
                    workers.to_string(),
                    ClusterReplicaSizeConfig {
                        memory_limit: None,
                        cpu_limit: None,
                        scale: NonZeroUsize::new(1).unwrap(),
                        workers: NonZeroUsize::new(workers).unwrap(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        inner.insert(
            "2-1".to_string(),
            ClusterReplicaSizeConfig {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(1).unwrap(),
            },
        );
        inner.insert(
            "2-2".to_string(),
            ClusterReplicaSizeConfig {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(2).unwrap(),
            },
        );
        inner.insert(
            "2-4".to_string(),
            ClusterReplicaSizeConfig {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(4).unwrap(),
            },
        );
        Self(inner)
    }
}

/// Replica configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConcreteComputeInstanceReplicaConfig {
    /// Out-of-process replica
    Remote {
        /// A map from replica name to hostnames.
        replicas: BTreeSet<String>,
    },
    /// A remote but managed replica
    Managed {
        /// The size configuration of the replica.
        size_config: ClusterReplicaSizeConfig,
        /// A readable name for the replica size.
        size_name: String,
        /// The replica's availability zone, if `Some`.
        availability_zone: Option<String>,
    },
}

/// Deterministically generates replica names based on inputs.
fn generate_replica_service_name(instance_id: ComputeInstanceId, replica_id: ReplicaId) -> String {
    format!("cluster-{instance_id}-replica-{replica_id}")
}

/// Parse a name generated by `generate_replica_service_name`, to extract the
/// replica's compute instance ID and replica ID values.
fn parse_replica_service_name(
    service_name: &str,
) -> Result<(ComputeInstanceId, ReplicaId), anyhow::Error> {
    static SERVICE_NAME_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?-u)^cluster-(\d+)-replica-(\d+)$").unwrap());

    let caps = SERVICE_NAME_RE
        .captures(service_name)
        .ok_or_else(|| anyhow!("invalid service name: {service_name}"))?;

    let instance_id = caps.get(1).unwrap().as_str().parse().unwrap();
    let replica_id = caps.get(2).unwrap().as_str().parse().unwrap();
    Ok((instance_id, replica_id))
}

/// An event describing a change in status of a compute process.
#[derive(Debug, Clone, Serialize)]
pub struct ComputeInstanceEvent {
    pub instance_id: ComputeInstanceId,
    pub replica_id: ReplicaId,
    pub process_id: ProcessId,
    pub status: ComputeInstanceStatus,
    pub time: DateTime<Utc>,
}

/// Responses that [`Controller`] can produce.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControllerResponse<T = mz_repr::Timestamp> {
    /// The worker's response to a specified (by connection id) peek.
    ///
    /// Additionally, an `OpenTelemetryContext` to forward trace information
    /// back into coord. This allows coord traces to be children of work
    /// done in compute!
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// The worker's next response to a specified tail.
    TailResponse(GlobalId, TailResponse<T>),
    /// Notification that we have received a message from the given compute replica
    /// at the given time.
    ComputeReplicaHeartbeat(ReplicaId, DateTime<Utc>),
}

impl<T> From<ComputeControllerResponse<T>> for ControllerResponse<T> {
    fn from(r: ComputeControllerResponse<T>) -> ControllerResponse<T> {
        match r {
            ComputeControllerResponse::PeekResponse(uuid, peek, otel_ctx) => {
                ControllerResponse::PeekResponse(uuid, peek, otel_ctx)
            }
            ComputeControllerResponse::TailResponse(id, tail) => {
                ControllerResponse::TailResponse(id, tail)
            }
            ComputeControllerResponse::ReplicaHeartbeat(id, when) => {
                ControllerResponse::ComputeReplicaHeartbeat(id, when)
            }
        }
    }
}

/// Whether one of the underlying controllers is ready for their `process`
/// method to be called.
#[derive(Default)]
enum Readiness {
    /// No underlying controllers are ready.
    #[default]
    NotReady,
    /// The storage controller is ready.
    Storage,
    /// The compute controller is ready.
    Compute(ComputeInstanceId),
}

/// A client that maintains soft state and validates commands, in addition to forwarding them.
///
/// NOTE(benesch): I find the fact that this type is called `Controller` but is
/// referred to as the `dataflow_client` in the coordinator to be very
/// confusing. We should find the one correct name, and use it everywhere!
pub struct Controller<T = mz_repr::Timestamp> {
    storage_controller: Box<dyn StorageController<Timestamp = T>>,
    compute_orchestrator: Arc<dyn NamespacedOrchestrator>,
    computed_image: String,
    compute: BTreeMap<ComputeInstanceId, ComputeControllerState<T>>,
    readiness: Readiness,
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + Codec64 + Copy + Unpin,
    ComputeCommand<T>: RustType<ProtoComputeCommand>,
    ComputeResponse<T>: RustType<ProtoComputeResponse>,
{
    pub async fn create_instance(
        &mut self,
        instance: ComputeInstanceId,
        logging: Option<LoggingConfig>,
    ) {
        // Insert a new compute instance controller.
        self.compute
            .insert(instance, ComputeControllerState::new(&logging).await);
    }

    /// Adds replicas of an instance.
    ///
    /// # Panics
    /// - If the identified `instance` has not yet been created via
    ///   [`Self::create_instance`].
    pub async fn add_replica_to_instance(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        config: ConcreteComputeInstanceReplicaConfig,
    ) -> Result<(), anyhow::Error> {
        assert!(
            self.compute.contains_key(&instance_id),
            "call Controller::create_instance before calling add_replica_to_instance"
        );

        // Add replicas backing that instance.
        match config {
            ConcreteComputeInstanceReplicaConfig::Remote { replicas } => {
                let mut compute_instance = self.compute_mut(instance_id).unwrap();
                let client = ComputeGrpcClient::new_partitioned(replicas.into_iter().collect());
                compute_instance.add_replica(replica_id, client);
            }
            ConcreteComputeInstanceReplicaConfig::Managed {
                size_config,
                size_name: _,
                availability_zone,
            } => {
                let service_name = generate_replica_service_name(instance_id, replica_id);

                let service = self
                    .compute_orchestrator
                    .ensure_service(
                        &service_name,
                        ServiceConfig {
                            image: self.computed_image.clone(),
                            args: &|assigned| {
                                let mut compute_opts = vec![
                                    format!(
                                        "--listen-addr={}:{}",
                                        assigned.listen_host, assigned.ports["controller"]
                                    ),
                                    format!(
                                        "--internal-http-listen-addr={}:{}",
                                        assigned.listen_host, assigned.ports["internal-http"]
                                    ),
                                    format!("--workers={}", size_config.workers),
                                    format!("--opentelemetry-resource=instance_id={}", instance_id),
                                    format!("--opentelemetry-resource=replica_id={}", replica_id),
                                ];
                                compute_opts.extend(
                                    assigned.peers.iter().map(|(host, ports)| {
                                        format!("{host}:{}", ports["compute"])
                                    }),
                                );
                                if let Some(index) = assigned.index {
                                    compute_opts.push(format!("--process={index}"));
                                    compute_opts.push(format!(
                                        "--opentelemetry-resource=replica_index={}",
                                        index
                                    ));
                                }
                                compute_opts
                            },
                            ports: vec![
                                ServicePort {
                                    name: "controller".into(),
                                    port_hint: 2100,
                                },
                                ServicePort {
                                    name: "compute".into(),
                                    port_hint: 2102,
                                },
                                ServicePort {
                                    name: "internal-http".into(),
                                    port_hint: 6875,
                                },
                            ],
                            cpu_limit: size_config.cpu_limit,
                            memory_limit: size_config.memory_limit,
                            scale: size_config.scale,
                            labels: hashmap! {
                                "cluster-id".into() => instance_id.to_string(),
                                "type".into() => "cluster".into(),
                            },
                            availability_zone,
                        },
                    )
                    .await?;
                let client = ComputeGrpcClient::new_partitioned(service.addresses("controller"));
                self.compute_mut(instance_id)
                    .unwrap()
                    .add_replica(replica_id, client);
            }
        }

        Ok(())
    }

    /// Removes a replica from an instance, including its service in the
    /// orchestrator.
    pub async fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        config: ConcreteComputeInstanceReplicaConfig,
    ) -> Result<(), anyhow::Error> {
        if let ConcreteComputeInstanceReplicaConfig::Managed {
            size_config: _,
            size_name: _,
            availability_zone: _,
        } = config
        {
            let service_name = generate_replica_service_name(instance_id, replica_id);
            self.compute_orchestrator
                .drop_service(&service_name)
                .await?;
        }
        let mut compute = self.compute_mut(instance_id).unwrap();
        compute.remove_replica(replica_id);
        Ok(())
    }

    /// Removes an instance from the orchestrator.
    ///
    /// # Panics
    /// - If the identified `instance` still has active replicas.
    pub async fn drop_instance(
        &mut self,
        instance: ComputeInstanceId,
    ) -> Result<(), anyhow::Error> {
        if let Some(mut compute) = self.compute.remove(&instance) {
            assert!(
                compute.client.get_replica_ids().next().is_none(),
                "cannot drop instances with provisioned replicas; call `drop_replica` first"
            );
            self.compute_orchestrator
                .drop_service(&format!("cluster-{instance}"))
                .await?;
            compute.client.send(ComputeCommand::DropInstance);
        }
        Ok(())
    }

    /// Listen for changes to compute services reported by the orchestrator.
    pub fn watch_compute_services(&self) -> BoxStream<'static, ComputeInstanceEvent> {
        fn translate_event(event: ServiceEvent) -> Result<ComputeInstanceEvent, anyhow::Error> {
            let (instance_id, replica_id) = parse_replica_service_name(&event.service_id)?;
            Ok(ComputeInstanceEvent {
                instance_id,
                replica_id,
                process_id: event.process_id,
                status: event.status,
                time: event.time,
            })
        }

        let stream = self
            .compute_orchestrator
            .watch_services()
            .map(|event| event.and_then(translate_event))
            .filter_map(|event| async {
                match event {
                    Ok(event) => Some(event),
                    Err(error) => {
                        tracing::error!("service watch error: {error}");
                        None
                    }
                }
            });

        Box::pin(stream)
    }
}

impl<T> Controller<T> {
    /// Acquires an immutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage(&self) -> &dyn StorageController<Timestamp = T> {
        &*self.storage_controller
    }

    /// Acquires a mutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage_mut(&mut self) -> &mut dyn StorageController<Timestamp = T> {
        &mut *self.storage_controller
    }

    /// Acquires an immutable handle to a controller for the indicated compute instance, if it exists.
    #[inline]
    pub fn compute(&self, instance: ComputeInstanceId) -> Option<ComputeController<T>> {
        let compute = self.compute.get(&instance)?;
        Some(ComputeController {
            instance,
            compute,
            storage_controller: self.storage(),
        })
    }

    /// Acquires a mutable handle to a controller for the indicated compute instance, if it exists.
    #[inline]
    pub fn compute_mut(&mut self, instance: ComputeInstanceId) -> Option<ComputeControllerMut<T>> {
        let compute = self.compute.get_mut(&instance)?;
        Some(ComputeControllerMut {
            instance,
            compute,
            storage_controller: &mut *self.storage_controller,
        })
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + Codec64,
{
    /// Waits until the controller is ready to process a response.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the owner should call [`Controller::ready`] to
    /// process the ready message.
    ///
    /// This method is cancellation safe.
    pub async fn ready(&mut self) {
        if let Readiness::NotReady = self.readiness {
            // The underlying `ready` methods are cancellation safe, so it is
            // safe to construct this `select!`.
            let computes = future::select_all(
                self.compute
                    .iter_mut()
                    .map(|(id, compute)| Box::pin(compute.ready().map(|()| *id))),
            );
            tokio::select! {
                (id, _index, _remaining) = computes => {
                    self.readiness = Readiness::Compute(id);
                }
                () = self.storage_controller.ready() => {
                    self.readiness = Readiness::Storage;
                }
            }
        }
    }

    /// Processes the work queued by [`Controller::ready`].
    ///
    /// This method is guaranteed to return "quickly" unless doing so would
    /// compromise the correctness of the system.
    ///
    /// This method is **not** guaranteed to be cancellation safe. It **must**
    /// be awaited to completion.
    pub async fn process(&mut self) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        match mem::take(&mut self.readiness) {
            Readiness::NotReady => Ok(None),
            Readiness::Storage => {
                self.storage_mut().process().await?;
                Ok(None)
            }
            Readiness::Compute(id) => {
                let response = self
                    .compute_mut(id)
                    .expect("reference to absent compute instance")
                    .process()
                    .await?;
                Ok(response.map(|r| r.into()))
            }
        }
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + TotalOrder + TryInto<i64> + TryFrom<i64> + Codec64 + Unpin,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    /// Creates a new controller.
    pub async fn new(config: ControllerConfig) -> Self {
        let storage_controller = mz_storage::client::controller::Controller::new(
            config.storage_stash_url,
            config.persist_location,
            config.persist_clients,
            config.orchestrator.namespace("storage"),
            config.storaged_image,
        )
        .await;
        Self {
            storage_controller: Box::new(storage_controller),
            compute_orchestrator: config.orchestrator.namespace("compute"),
            computed_image: config.computed_image,
            compute: BTreeMap::default(),
            readiness: Readiness::NotReady,
        }
    }
}
