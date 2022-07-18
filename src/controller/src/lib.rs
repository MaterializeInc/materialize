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

use std::collections::{BTreeMap, BTreeSet};
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

use mz_build_info::BuildInfo;
use mz_compute_client::command::{ComputeCommand, ProcessId, ProtoComputeCommand, ReplicaId};
use mz_compute_client::controller::{
    ComputeController, ComputeControllerMut, ComputeControllerResponse, ComputeControllerState,
    ComputeInstanceId,
};
use mz_compute_client::logging::LoggingConfig;
use mz_compute_client::response::{
    ComputeResponse, PeekResponse, ProtoComputeResponse, TailResponse,
};
use mz_compute_client::service::{ComputeClient, ComputeGrpcClient};
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
use mz_storage::controller::StorageController;
use mz_storage::protocol::client::{
    ProtoStorageCommand, ProtoStorageResponse, StorageCommand, StorageResponse,
};

pub use mz_orchestrator::ServiceStatus as ComputeInstanceStatus;

/// Configures a controller.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
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

/// Resource allocations for a replica of a compute instance.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComputeInstanceReplicaAllocation {
    /// The memory limit for each process in the replica.
    pub memory_limit: Option<MemoryLimit>,
    /// The CPU limit for each process in the replica.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of processes in the replica.
    pub scale: NonZeroUsize,
    /// The number of worker threads in the replica.
    pub workers: NonZeroUsize,
}

/// Configuration for a replica of a compute instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConcreteComputeInstanceReplicaConfig {
    /// Out-of-process replica
    Remote {
        /// The network addresses of the processes in the replica.
        addrs: BTreeSet<String>,
    },
    /// A remote but managed replica
    Managed {
        /// The resource allocation for the replica.
        allocation: ComputeInstanceReplicaAllocation,
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
pub struct Controller<T = mz_repr::Timestamp> {
    build_info: &'static BuildInfo,
    storage_controller: Box<dyn StorageController<Timestamp = T>>,
    compute_orchestrator: Arc<dyn NamespacedOrchestrator>,
    computed_image: String,
    compute: BTreeMap<ComputeInstanceId, ComputeControllerState<T>>,
    readiness: Readiness,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
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
        self.compute.insert(
            instance,
            ComputeControllerState::new(self.build_info, &logging).await,
        );
        if self.initialized {
            self.compute_mut(instance)
                .expect("Instance just initialized")
                .initialization_complete();
        }
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
            ConcreteComputeInstanceReplicaConfig::Remote { addrs } => {
                let mut compute_instance = self.compute_mut(instance_id).unwrap();
                compute_instance.add_replica(replica_id, addrs.into_iter().collect());
            }
            ConcreteComputeInstanceReplicaConfig::Managed {
                allocation,
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
                                        "--controller-listen-addr={}:{}",
                                        assigned.listen_host, assigned.ports["controller"]
                                    ),
                                    format!(
                                        "--internal-http-listen-addr={}:{}",
                                        assigned.listen_host, assigned.ports["internal-http"]
                                    ),
                                    format!("--workers={}", allocation.workers),
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
                                    port_hint: 6878,
                                },
                            ],
                            cpu_limit: allocation.cpu_limit,
                            memory_limit: allocation.memory_limit,
                            scale: allocation.scale,
                            labels: hashmap! {
                                "cluster-id".into() => instance_id.to_string(),
                                "type".into() => "cluster".into(),
                            },
                            availability_zone,
                        },
                    )
                    .await?;
                self.compute_mut(instance_id)
                    .unwrap()
                    .add_replica(replica_id, service.addresses("controller"));
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
        if let ConcreteComputeInstanceReplicaConfig::Managed { .. } = config {
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
                compute.replicas.get_replica_ids().next().is_none(),
                "cannot drop instances with provisioned replicas; call `drop_replica` first"
            );
            self.compute_orchestrator
                .drop_service(&format!("cluster-{instance}"))
                .await?;
            compute.replicas.send(ComputeCommand::DropInstance);
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
    T: Timestamp + Lattice + Codec64 + Copy,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    pub fn initialization_complete(&mut self) {
        self.initialized = true;
        for (instance, compute) in self.compute.iter_mut() {
            ComputeControllerMut {
                instance: *instance,
                compute,
                storage_controller: &mut *self.storage_controller,
            }
            .initialization_complete();
        }
        self.storage_mut().initialization_complete();
    }

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
            if self.compute.is_empty() {
                // If there are no clients, block forever. This signals that
                // there may be more work to do (e.g., if a compute instance
                // is created). Calling `select_all` with an empty list of
                // futures will panic.
                future::pending().await
            }
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
        let storage_controller = mz_storage::controller::Controller::new(
            config.build_info,
            config.storage_stash_url,
            config.persist_location,
            config.persist_clients,
            config.orchestrator.namespace("storage"),
            config.storaged_image,
        )
        .await;
        Self {
            build_info: config.build_info,
            storage_controller: Box::new(storage_controller),
            compute_orchestrator: config.orchestrator.namespace("compute"),
            computed_image: config.computed_image,
            compute: BTreeMap::default(),
            readiness: Readiness::NotReady,
            initialized: false,
        }
    }
}
