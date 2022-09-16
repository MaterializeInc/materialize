// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller that provides an interface to the compute layer, and the storage layer below it.
//!
//! The compute controller manages the creation, maintenance, and removal of compute instances.
//! This involves ensuring the intended service state with the orchestrator, as well as maintaining
//! a dedicated compute instance controller for each active compute instance.
//!
//! An instance controller curates the creation of indexes and sinks installed on its instance, the
//! progress of readers through these collections, and their eventual dropping and resource
//! reclamation.
//!
//! An instance controller can be viewed as a partial map from `GlobalId` to collection. It is an error to
//! use an identifier before it has been "created" with `create_dataflows()`. Once created, the controller holds
//! a read capability for each output collection of a dataflow, which is manipulated with `allow_compaction()`.
//! Eventually, a collection is dropped with either `drop_sources()` or by allowing compaction to the empty frontier.
//!
//! Created dataflows will prevent the compaction of their inputs, including other compute collections but also
//! collections managed by the storage layer. Each dataflow input is prevented from compacting beyond the allowed
//! compaction of each of its outputs, ensuring that we can recover each dataflow to its current state in case of
//! failure or other reconfiguration.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use futures::stream::BoxStream;
use futures::{future, FutureExt};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_expr::RowSetFinishing;
use mz_orchestrator::{CpuLimit, MemoryLimit, NamespacedOrchestrator};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Row};
use mz_storage::controller::{ReadPolicy, StorageController, StorageError};

use crate::command::{
    ComputeCommand, DataflowDescription, InstanceConfig, Peek, ProcessId, ReplicaId,
    SourceInstanceDesc,
};
use crate::logging::{LogVariant, LogView, LoggingConfig};
use crate::response::{ComputeResponse, PeekResponse, TailBatch, TailResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};
use crate::sinks::{ComputeSinkConnection, ComputeSinkDesc, PersistSinkConnection};

use self::orchestrator::ComputeOrchestrator;
use self::replicated::{ActiveReplication, ActiveReplicationResponse};

mod orchestrator;
mod replicated;

pub use mz_orchestrator::ServiceStatus as ComputeInstanceStatus;

/// An abstraction allowing us to name different compute instances.
pub type ComputeInstanceId = u64;

/// An event describing a change in status of a compute process.
#[derive(Debug, Clone, Serialize)]
pub struct ComputeInstanceEvent {
    pub instance_id: ComputeInstanceId,
    pub replica_id: ReplicaId,
    pub process_id: ProcessId,
    pub status: ComputeInstanceStatus,
    pub time: DateTime<Utc>,
}

/// Responses from the compute controller.
pub enum ComputeControllerResponse<T> {
    /// See [`ComputeResponse::PeekResponse`].
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// See [`ComputeResponse::TailResponse`].
    TailResponse(GlobalId, TailResponse<T>),
    /// A notification that we heard a response from the given replica at the
    /// given time.
    ReplicaHeartbeat(ReplicaId, DateTime<Utc>),
}

/// Errors arising from compute commands.
#[derive(Debug)]
pub enum ComputeError {
    /// Command referenced an instance that was not present.
    InstanceMissing(ComputeInstanceId),
    /// Command referenced an identifier that was not present.
    IdentifierMissing(GlobalId),
    /// The identified instance exists already.
    InstanceExists(ComputeInstanceId),
    /// Dataflow was malformed (e.g. missing `as_of`).
    DataflowMalformed,
    /// The dataflow `as_of` was not greater than the `since` of the identifier.
    DataflowSinceViolation(GlobalId),
    /// The peek `timestamp` was not greater than the `since` of the identifier.
    PeekSinceViolation(GlobalId),
    /// An error from the underlying client.
    ClientError(anyhow::Error),
    /// An error during an interaction with Storage
    StorageError(StorageError),
}

impl Error for ComputeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InstanceMissing(_)
            | Self::IdentifierMissing(_)
            | Self::InstanceExists(_)
            | Self::DataflowMalformed
            | Self::DataflowSinceViolation(_)
            | Self::PeekSinceViolation(_) => None,
            Self::ClientError(err) => Some(err.root_cause()),
            Self::StorageError(err) => err.source(),
        }
    }
}

impl fmt::Display for ComputeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("compute error: ")?;
        match self {
            Self::InstanceMissing(id) => write!(
                f,
                "command referenced an instance that was not present: {id}"
            ),
            Self::IdentifierMissing(id) => write!(
                f,
                "command referenced an identifier that was not present: {id}"
            ),
            Self::InstanceExists(id) => write!(f, "an instance with this ID exists already: {id}"),
            Self::DataflowMalformed => write!(f, "dataflow was malformed"),
            Self::DataflowSinceViolation(id) => write!(
                f,
                "dataflow as_of was not greater than the `since` of the identifier: {id}"
            ),
            Self::PeekSinceViolation(id) => write!(
                f,
                "peek timestamp was not greater than the `since` of the identifier: {id}"
            ),
            Self::ClientError(err) => write!(f, "underlying client error: {err}"),
            Self::StorageError(err) => write!(f, "storage interaction error: {err}"),
        }
    }
}

impl From<StorageError> for ComputeError {
    fn from(error: StorageError) -> Self {
        Self::StorageError(error)
    }
}

impl From<anyhow::Error> for ComputeError {
    fn from(error: anyhow::Error) -> Self {
        Self::ClientError(error)
    }
}

/// Replica configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ConcreteComputeInstanceReplicaConfig {
    pub location: ConcreteComputeInstanceReplicaLocation,
    pub persisted_logs: ConcreteComputeInstanceReplicaLogging,
}

/// Size or location of a replica
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ConcreteComputeInstanceReplicaLocation {
    /// Out-of-process replica
    Remote {
        /// The network addresses of the processes in the replica.
        addrs: BTreeSet<String>,
    },
    /// A remote but managed replica
    Managed {
        /// The resource allocation for the replica.
        allocation: ComputeInstanceReplicaAllocation,
        /// SQL size parameter used for allocation
        size: String,
        /// The replica's availability zone
        availability_zone: String,
        /// `true` if the AZ was specified by the user and must be respected;
        /// `false` if it was picked arbitrarily by Materialize.
        az_user_specified: bool,
    },
}

impl ConcreteComputeInstanceReplicaLocation {
    pub fn get_az(&self) -> Option<&str> {
        match self {
            ConcreteComputeInstanceReplicaLocation::Remote { .. } => None,
            ConcreteComputeInstanceReplicaLocation::Managed {
                availability_zone, ..
            } => Some(availability_zone),
        }
    }
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

impl ComputeInstanceReplicaAllocation {
    pub fn workers(&self) -> NonZeroUsize {
        self.workers
    }
}

/// Logging configuration of a replica.
/// Changing this type requires a catalog storage migration!
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ConcreteComputeInstanceReplicaLogging {
    /// Instantiate default logging configuration upon system start.
    /// To configure a replica without logging, ConcreteViews(vec![],vec![]) should be used.
    Default,
    /// Logging sources and views have been built for this replica.
    ConcreteViews(Vec<(LogVariant, GlobalId)>, Vec<(LogView, GlobalId)>),
}

impl ConcreteComputeInstanceReplicaLogging {
    /// Return all persisted introspection sources contained.
    pub fn get_sources(&self) -> &[(LogVariant, GlobalId)] {
        match self {
            ConcreteComputeInstanceReplicaLogging::Default => &[],
            ConcreteComputeInstanceReplicaLogging::ConcreteViews(logs, _) => logs,
        }
    }

    /// Return all persisted introspection views contained.
    pub fn get_views(&self) -> &[(LogView, GlobalId)] {
        match self {
            ConcreteComputeInstanceReplicaLogging::Default => &[],
            ConcreteComputeInstanceReplicaLogging::ConcreteViews(_, views) => views,
        }
    }

    /// Return all ids of the persisted introspection views contained.
    pub fn get_view_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.get_views().into_iter().map(|(_, id)| *id)
    }

    /// Return all ids of the persisted introspection sources contained.
    pub fn get_source_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.get_sources().into_iter().map(|(_, id)| *id)
    }

    /// Return all ids of the persisted introspection sources and logs contained.
    pub fn get_source_and_view_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.get_source_ids().chain(self.get_view_ids())
    }
}

/// A controller for the compute layer.
pub struct ComputeController<T> {
    instances: BTreeMap<ComputeInstanceId, Instance<T>>,
    build_info: &'static BuildInfo,
    orchestrator: ComputeOrchestrator,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
    /// A response to handle on the next call to `ActiveComputeController::process`.
    stashed_response: Option<(ComputeInstanceId, ActiveReplicationResponse<T>)>,
}

impl<T> ComputeController<T> {
    /// Construct a new [`ComputeController`].
    pub fn new(
        build_info: &'static BuildInfo,
        orchestrator: Arc<dyn NamespacedOrchestrator>,
        computed_image: String,
    ) -> Self {
        Self {
            instances: BTreeMap::new(),
            build_info,
            orchestrator: ComputeOrchestrator::new(orchestrator, computed_image),
            initialized: false,
            stashed_response: None,
        }
    }

    /// Return a handle to the indicated compute instance.
    pub fn instance(&self, id: ComputeInstanceId) -> Result<&Instance<T>, ComputeError> {
        self.instances
            .get(&id)
            .ok_or(ComputeError::InstanceMissing(id))
    }

    /// Acquire an [`ActiveComputeController`] by supplying a storage connection.
    pub fn activate<'a>(
        &'a mut self,
        storage: &'a mut dyn StorageController<Timestamp = T>,
    ) -> ActiveComputeController<'a, T> {
        ActiveComputeController {
            compute: self,
            storage,
        }
    }
}

impl<T> ComputeController<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Create a compute instance.
    pub fn create_instance(
        &mut self,
        id: ComputeInstanceId,
        logging: Option<LoggingConfig>,
        max_result_size: u32,
    ) -> Result<(), ComputeError> {
        if self.instances.contains_key(&id) {
            return Err(ComputeError::InstanceExists(id));
        }

        self.instances.insert(
            id,
            Instance::new(id, self.build_info, &logging, max_result_size),
        );

        if self.initialized {
            self.instances
                .get_mut(&id)
                .expect("instance just added")
                .initialization_complete();
        }

        Ok(())
    }

    /// Remove a compute instance.
    ///
    /// # Panics
    /// - If the identified `instance` still has active replicas.
    pub fn drop_instance(&mut self, id: ComputeInstanceId) {
        if let Some(compute_state) = self.instances.remove(&id) {
            compute_state.drop();
        }
    }

    /// Mark the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    pub fn initialization_complete(&mut self) {
        self.initialized = true;
        for instance in self.instances.values_mut() {
            instance.initialization_complete();
        }
    }

    /// Wait until the controller is ready to process a response.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the caller should call [`ActiveComputeController::process`] to
    /// process the ready message.
    ///
    /// This method is cancellation safe.
    pub async fn ready(&mut self) {
        if self.stashed_response.is_some() {
            // We still have a response stashed, which we are immediately ready to process.
            return;
        }

        if self.instances.is_empty() {
            // If there are no clients, block forever. This signals that there may be more work to
            // do (e.g., if a compute instance is created). Calling `select_all` with an empty list
            // of futures will panic.
            future::pending().await
        }

        // `ActiveReplication::recv` is cancellation safe, so it is safe to construct this
        // `select_all`.
        let receives = self
            .instances
            .iter_mut()
            .map(|(id, instance)| Box::pin(instance.replicas.recv().map(|resp| (*id, resp))));
        let (resp, _index, _remaining) = future::select_all(receives).await;

        self.stashed_response = Some(resp);
    }

    /// Listen for changes to compute services reported by the orchestrator.
    pub fn watch_services(&self) -> BoxStream<'static, ComputeInstanceEvent> {
        self.orchestrator.watch_services()
    }
}

/// A wrapper around a [`ComputeController`] with a live connection to a storage controller.
pub struct ActiveComputeController<'a, T> {
    compute: &'a mut ComputeController<T>,
    storage: &'a mut dyn StorageController<Timestamp = T>,
}

impl<T> ActiveComputeController<'_, T> {
    /// Return a handle to the indicated compute instance.
    pub fn instance(&mut self, id: ComputeInstanceId) -> Result<ActiveInstance<T>, ComputeError> {
        self.compute
            .instances
            .get_mut(&id)
            .ok_or(ComputeError::InstanceMissing(id))
            .map(|c| c.activate(self.storage))
    }
}

impl<T> ActiveComputeController<'_, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Adds replicas of an instance.
    pub async fn add_replica_to_instance(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        config: ConcreteComputeInstanceReplicaConfig,
    ) -> Result<(), ComputeError> {
        let persisted_logs = config
            .persisted_logs
            .get_sources()
            .iter()
            .cloned()
            .collect();

        // Add replicas backing that instance.
        match config.location {
            ConcreteComputeInstanceReplicaLocation::Remote { addrs } => {
                self.instance(instance_id)?.add_replica(
                    replica_id,
                    addrs.into_iter().collect(),
                    persisted_logs,
                );
            }
            ConcreteComputeInstanceReplicaLocation::Managed {
                allocation,
                availability_zone,
                ..
            } => {
                let replica_addrs = self
                    .compute
                    .orchestrator
                    .ensure_replica(instance_id, replica_id, allocation, availability_zone)
                    .await?;

                self.instance(instance_id)?
                    .add_replica(replica_id, replica_addrs, persisted_logs);
            }
        }

        Ok(())
    }

    /// Removes a replica from an instance, including its service in the orchestrator.
    pub async fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        config: ConcreteComputeInstanceReplicaConfig,
    ) -> Result<(), ComputeError> {
        if let ConcreteComputeInstanceReplicaLocation::Managed { .. } = config.location {
            self.compute
                .orchestrator
                .drop_replica(instance_id, replica_id)
                .await?;
        }

        self.instance(instance_id)
            .unwrap()
            .remove_replica(replica_id);
        Ok(())
    }

    /// Processes the work queued by [`ComputeController::ready`].
    ///
    /// This method is guaranteed to return "quickly" unless doing so would
    /// compromise the correctness of the system.
    ///
    /// This method is **not** guaranteed to be cancellation safe. It **must**
    /// be awaited to completion.
    pub async fn process(&mut self) -> Result<Option<ComputeControllerResponse<T>>, ComputeError> {
        let (instance_id, ar_response) = match self.compute.stashed_response.take() {
            Some(resp) => resp,
            None => return Ok(None),
        };

        let response = match ar_response {
            ActiveReplicationResponse::ComputeResponse(resp) => resp,
            ActiveReplicationResponse::ReplicaHeartbeat(replica_id, when) => {
                return Ok(Some(ComputeControllerResponse::ReplicaHeartbeat(
                    replica_id, when,
                )))
            }
        };

        let mut instance = self.instance(instance_id)?;

        match response {
            ComputeResponse::FrontierUppers(updates) => {
                instance.update_write_frontiers(&updates).await?;
                Ok(None)
            }
            ComputeResponse::PeekResponse(uuid, peek_response, otel_ctx) => {
                instance.remove_peeks(std::iter::once(uuid)).await?;
                Ok(Some(ComputeControllerResponse::PeekResponse(
                    uuid,
                    peek_response,
                    otel_ctx,
                )))
            }
            ComputeResponse::TailResponse(global_id, response) => {
                let new_upper = match &response {
                    TailResponse::Batch(TailBatch { lower, upper, .. }) => {
                        // Ensure there are no gaps in the tail stream we receive.
                        assert_eq!(
                            lower,
                            &instance.compute.collections[&global_id].write_frontier
                        );

                        upper.clone()
                    }
                    // The tail will not be written to again, but we should not confuse that
                    // with the source of the TAIL being complete through this time.
                    TailResponse::DroppedAt(_) => Antichain::new(),
                };
                instance
                    .update_write_frontiers(&[(global_id, new_upper)])
                    .await?;
                Ok(Some(ComputeControllerResponse::TailResponse(
                    global_id, response,
                )))
            }
        }
    }
}

/// A controller for a compute instance.
#[derive(Debug)]
pub struct Instance<T> {
    /// The ID of the compute instance.
    instance_id: ComputeInstanceId,
    /// The replicas of this compute instance.
    replicas: ActiveReplication<T>,
    /// Tracks expressed `since` and received `upper` frontiers for indexes and sinks.
    collections: BTreeMap<GlobalId, CollectionState<T>>,
    /// Currently outstanding peeks: identifiers and timestamps.
    peeks: BTreeMap<uuid::Uuid, (GlobalId, T)>,
}

/// A wrapper around [`Instance`] with a live storage controller.
#[derive(Debug)]
pub struct ActiveInstance<'a, T> {
    compute: &'a mut Instance<T>,
    storage_controller: &'a mut dyn StorageController<Timestamp = T>,
}

// Public interface

impl<T> Instance<T> {
    /// Returns this controller's compute instance ID.
    pub fn instance_id(&self) -> ComputeInstanceId {
        self.instance_id
    }

    /// Acquire a handle to the collection state associated with `id`.
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
        self.collections
            .get(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }

    /// Acquire an [`ActiveInstance`] by providing a storage controller.
    pub fn activate<'a>(
        &'a mut self,
        storage_controller: &'a mut dyn StorageController<Timestamp = T>,
    ) -> ActiveInstance<'a, T> {
        ActiveInstance {
            compute: self,
            storage_controller,
        }
    }
}

impl<T> Instance<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub fn new(
        instance_id: ComputeInstanceId,
        build_info: &'static BuildInfo,
        logging: &Option<LoggingConfig>,
        max_result_size: u32,
    ) -> Self {
        let mut collections = BTreeMap::default();
        if let Some(logging_config) = logging.as_ref() {
            for id in logging_config.log_identifiers() {
                collections.insert(
                    id,
                    CollectionState::new(
                        Antichain::from_elem(T::minimum()),
                        Vec::new(),
                        Vec::new(),
                    ),
                );
            }
        }
        let mut replicas = ActiveReplication::new(build_info);
        replicas.send(ComputeCommand::CreateInstance(InstanceConfig {
            replica_id: Default::default(),
            logging: logging.clone(),
            max_result_size,
        }));

        Self {
            instance_id,
            replicas,
            collections,
            peeks: Default::default(),
        }
    }

    /// Marks the end of any initialization commands.
    ///
    /// Intended to be called by `Controller`, rather than by other code (to avoid repeated calls).
    pub fn initialization_complete(&mut self) {
        self.replicas.send(ComputeCommand::InitializationComplete);
    }

    /// Drop this compute instance.
    ///
    /// # Panics
    /// - If the compute instance still has active replicas.
    pub fn drop(mut self) {
        assert!(
            self.replicas.get_replica_ids().next().is_none(),
            "cannot drop instances with provisioned replicas"
        );
        self.replicas.send(ComputeCommand::DropInstance);
    }
}

impl<'a, T> ActiveInstance<'a, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Acquire a handle to the collection state associated with `id`.
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
        self.compute.collection(id)
    }

    /// Adds a new instance replica, by name.
    pub fn add_replica(
        &mut self,
        id: ReplicaId,
        addrs: Vec<String>,
        persisted_logs: HashMap<LogVariant, GlobalId>,
    ) {
        // Create ComputeState entries in Instance
        for id in persisted_logs.values() {
            self.compute.collections.insert(
                *id,
                CollectionState::new(Antichain::from_elem(T::minimum()), Vec::new(), Vec::new()),
            );
        }

        // Enrich log collections with metadata such that they can be sent over to computed
        let persisted_logs = persisted_logs
            .into_iter()
            .map(|(variant, id)| {
                let meta = self
                    .storage_controller
                    .collection(id)
                    .expect("cannot get collection metadata")
                    .collection_metadata
                    .clone();
                (variant, (id, meta))
            })
            .collect();

        // Add the replica
        self.compute.replicas.add_replica(id, addrs, persisted_logs);
    }

    /// Removes an existing instance replica, by name.
    pub fn remove_replica(&mut self, id: ReplicaId) {
        self.compute.replicas.remove_replica(id);
    }

    /// Creates and maintains the described dataflows, and initializes state for their output.
    ///
    /// This method creates dataflows whose inputs are still readable at the dataflow `as_of`
    /// frontier, and initializes the outputs as readable from that frontier onward.
    /// It installs read dependencies from the outputs to the inputs, so that the input read
    /// capabilities will be held back to the output read capabilities, ensuring that we are
    /// always able to return to a state that can serve the output read capabilities.
    pub async fn create_dataflows(
        &mut self,
        dataflows: Vec<DataflowDescription<crate::plan::Plan<T>, (), T>>,
    ) -> Result<(), ComputeError> {
        // Validate dataflows as having inputs whose `since` is less or equal to the dataflow's `as_of`.
        // Start tracking frontiers for each dataflow, using its `as_of` for each index and sink.
        for dataflow in dataflows.iter() {
            let as_of = dataflow
                .as_of
                .as_ref()
                .ok_or(ComputeError::DataflowMalformed)?;

            // Record all transitive dependencies of the outputs.
            let mut storage_dependencies = Vec::new();
            let mut compute_dependencies = Vec::new();

            // Validate sources have `since.less_equal(as_of)`.
            for source_id in dataflow.source_imports.keys() {
                let since = &self
                    .storage_controller
                    .collection(*source_id)
                    .or(Err(ComputeError::IdentifierMissing(*source_id)))?
                    .read_capabilities
                    .frontier();
                if !(timely::order::PartialOrder::less_equal(since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*source_id))?;
                }

                storage_dependencies.push(*source_id);
            }

            // Validate indexes have `since.less_equal(as_of)`.
            // TODO(mcsherry): Instead, return an error from the constructing method.
            for index_id in dataflow.index_imports.keys() {
                let collection = self.compute.collection(*index_id)?;
                let since = collection.read_capabilities.frontier();
                if !(timely::order::PartialOrder::less_equal(&since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*index_id))?;
                } else {
                    compute_dependencies.push(*index_id);
                }
            }

            // Canonicalize dependencies.
            // Probably redundant based on key structure, but doing for sanity.
            storage_dependencies.sort();
            storage_dependencies.dedup();
            compute_dependencies.sort();
            compute_dependencies.dedup();

            // We will bump the internals of each input by the number of dependents (outputs).
            let outputs = dataflow.sink_exports.len() + dataflow.index_exports.len();
            let mut changes = ChangeBatch::new();
            for time in as_of.iter() {
                changes.update(time.clone(), outputs as i64);
            }
            // Update storage read capabilities for inputs.
            let mut storage_read_updates = storage_dependencies
                .iter()
                .map(|id| (*id, changes.clone()))
                .collect();
            self.storage_controller
                .update_read_capabilities(&mut storage_read_updates)
                .await?;
            // Update compute read capabilities for inputs.
            let mut compute_read_updates = compute_dependencies
                .iter()
                .map(|id| (*id, changes.clone()))
                .collect();
            self.update_read_capabilities(&mut compute_read_updates)
                .await?;

            // Install collection state for each of the exports.
            for sink_id in dataflow.sink_exports.keys() {
                self.compute.collections.insert(
                    *sink_id,
                    CollectionState::new(
                        as_of.clone(),
                        storage_dependencies.clone(),
                        compute_dependencies.clone(),
                    ),
                );
            }
            for index_id in dataflow.index_exports.keys() {
                self.compute.collections.insert(
                    *index_id,
                    CollectionState::new(
                        as_of.clone(),
                        storage_dependencies.clone(),
                        compute_dependencies.clone(),
                    ),
                );
            }
        }

        // Here we augment all imported sources and all exported sinks with with the appropriate
        // storage metadata needed by the compute instance.
        let mut augmented_dataflows = Vec::with_capacity(dataflows.len());
        for d in dataflows {
            let mut source_imports = BTreeMap::new();
            for (id, (si, monotonic)) in d.source_imports {
                let collection = self.storage_controller.collection(id)?;
                let desc = SourceInstanceDesc {
                    storage_metadata: collection.collection_metadata.clone(),
                    arguments: si.arguments,
                    typ: collection.description.desc.typ().clone(),
                };
                source_imports.insert(id, (desc, monotonic));
            }

            let mut sink_exports = BTreeMap::new();
            for (id, se) in d.sink_exports {
                let connection = match se.connection {
                    ComputeSinkConnection::Persist(conn) => {
                        let metadata = self
                            .storage_controller
                            .collection(id)?
                            .collection_metadata
                            .clone();
                        let conn = PersistSinkConnection {
                            value_desc: conn.value_desc,
                            storage_metadata: metadata,
                        };
                        ComputeSinkConnection::Persist(conn)
                    }
                    ComputeSinkConnection::Tail(conn) => ComputeSinkConnection::Tail(conn),
                };
                let desc = ComputeSinkDesc {
                    from: se.from,
                    from_desc: se.from_desc,
                    connection,
                    as_of: se.as_of,
                };
                sink_exports.insert(id, desc);
            }

            augmented_dataflows.push(DataflowDescription {
                source_imports,
                sink_exports,
                // The rest of the fields are identical
                index_imports: d.index_imports,
                objects_to_build: d.objects_to_build,
                index_exports: d.index_exports,
                as_of: d.as_of,
                until: d.until,
                debug_name: d.debug_name,
            });
        }

        self.compute
            .replicas
            .send(ComputeCommand::CreateDataflows(augmented_dataflows));

        Ok(())
    }

    /// Drops the read capability for the given collections and allows their resources to be
    /// reclaimed.
    pub async fn drop_collections(&mut self, ids: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.validate_ids(ids.iter().cloned())?;

        let compaction_commands = ids.into_iter().map(|id| (id, Antichain::new())).collect();
        self.allow_compaction(compaction_commands).await?;
        Ok(())
    }

    /// Initiate a peek request for the contents of `id` at `timestamp`.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn peek(
        &mut self,
        id: GlobalId,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<ReplicaId>,
    ) -> Result<(), ComputeError> {
        let since = self.compute.collection(id)?.read_capabilities.frontier();

        if !since.less_equal(&timestamp) {
            Err(ComputeError::PeekSinceViolation(id))?;
        }

        // Install a compaction hold on `id` at `timestamp`.
        let mut updates = BTreeMap::new();
        updates.insert(id, ChangeBatch::new_from(timestamp.clone(), 1));
        self.update_read_capabilities(&mut updates).await?;
        self.compute.peeks.insert(uuid, (id, timestamp.clone()));

        self.compute.replicas.send(ComputeCommand::Peek(Peek {
            id,
            literal_constraints,
            uuid,
            timestamp,
            finishing,
            map_filter_project,
            target_replica,
            // Obtain an `OpenTelemetryContext` from the thread-local tracing
            // tree to forward it on to the compute worker.
            otel_ctx: OpenTelemetryContext::obtain(),
        }));

        Ok(())
    }

    /// Cancels existing peek requests.
    ///
    /// Canceling a peek is best effort. The caller may see any of the following
    /// after canceling a peek request:
    ///
    ///   * A `PeekResponse::Rows` indicating that the cancellation request did
    ///    not take effect in time and the query succeeded.
    ///
    ///   * A `PeekResponse::Canceled` affirming that the peek was canceled.
    ///
    ///   * No `PeekResponse` at all.
    pub async fn cancel_peeks(&mut self, uuids: &BTreeSet<Uuid>) -> Result<(), ComputeError> {
        self.remove_peeks(uuids.iter().cloned()).await?;
        self.compute.replicas.send(ComputeCommand::CancelPeeks {
            uuids: uuids.clone(),
        });
        Ok(())
    }

    /// Assigns a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated identifiers should
    /// conclude with the last policy. Changing a policy will immediately downgrade the read
    /// capability if appropriate, but it will not "recover" the read capability if the prior
    /// capability is already ahead of it.
    ///
    /// Identifiers not present in `policies` retain their existing read policies.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn set_read_policy(
        &mut self,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), ComputeError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, policy) in policies.into_iter() {
            if let Ok(collection) = self.compute.collection_mut(id) {
                let mut new_read_capability = policy.frontier(collection.write_frontier.borrow());

                if timely::order::PartialOrder::less_equal(
                    &collection.implied_capability,
                    &new_read_capability,
                ) {
                    let mut update = ChangeBatch::new();
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                    std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                    update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
                    if !update.is_empty() {
                        read_capability_changes.insert(id, update);
                    }
                }

                collection.read_policy = policy;
            } else {
                tracing::error!("Reference to unregistered id: {:?}", id);
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes)
                .await?;
        }
        Ok(())
    }

    /// Update the max size in bytes of any result.
    pub async fn update_max_result_size(&mut self, max_result_size: u32) {
        self.compute
            .replicas
            .send(ComputeCommand::UpdateMaxResultSize(max_result_size))
    }
}

// Internal interface

impl<T> Instance<T> {
    /// Acquire a mutable handle to the collection state associated with `id`.
    fn collection_mut(&mut self, id: GlobalId) -> Result<&mut CollectionState<T>, ComputeError> {
        self.collections
            .get_mut(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }
}

impl<'a, T> ActiveInstance<'a, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), ComputeError> {
        for id in ids {
            self.compute.collection(id)?;
        }
        Ok(())
    }

    /// Accept write frontier updates from the compute layer.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn update_write_frontiers(
        &mut self,
        updates: &[(GlobalId, Antichain<T>)],
    ) -> Result<(), ComputeError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, new_upper) in updates.iter() {
            let collection = self
                .compute
                .collection_mut(*id)
                .expect("Reference to absent collection");

            collection.write_frontier.join_assign(new_upper);

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier.borrow());
            if timely::order::PartialOrder::less_equal(
                &collection.implied_capability,
                &new_read_capability,
            ) {
                let mut update = ChangeBatch::new();
                update.extend(new_read_capability.iter().map(|time| (time.clone(), 1)));
                std::mem::swap(&mut collection.implied_capability, &mut new_read_capability);
                update.extend(new_read_capability.iter().map(|time| (time.clone(), -1)));
                if !update.is_empty() {
                    read_capability_changes.insert(*id, update);
                }
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes)
                .await?;
        }

        // Tell the storage controller about new write frontiers for storage
        // collections that are advanced by compute sinks.
        // TODO(teskje): The storage controller should have a task to directly
        // keep track of the frontiers of storage collections, instead of
        // relying on others for that information.
        let storage_updates: Vec<_> = updates
            .iter()
            .filter(|(id, _)| self.storage_controller.collection(*id).is_ok())
            .cloned()
            .collect();
        self.storage_controller
            .update_write_frontiers(&storage_updates)
            .await?;

        Ok(())
    }

    /// Applies `updates`, propagates consequences through other read capabilities, and sends an appropriate compaction command.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) -> Result<(), ComputeError> {
        // Locations to record consequences that we need to act on.
        let mut storage_todo = BTreeMap::default();
        let mut compute_net = Vec::default();
        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.compute.collection_mut(key) {
                let changes = collection.read_capabilities.update_iter(update.drain());
                update.extend(changes);
                for id in collection.storage_dependencies.iter() {
                    storage_todo
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }
                for id in collection.compute_dependencies.iter() {
                    updates
                        .entry(*id)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.iter().cloned());
                }
                compute_net.push((key, update));
            } else {
                // Storage presumably, but verify.
                storage_todo
                    .entry(key)
                    .or_insert_with(ChangeBatch::new)
                    .extend(update.drain())
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands.
        let mut compaction_commands = Vec::new();
        for (id, change) in compute_net.iter_mut() {
            if !change.is_empty() {
                let frontier = self
                    .compute
                    .collection(*id)
                    .unwrap()
                    .read_capabilities
                    .frontier()
                    .to_owned();
                compaction_commands.push((*id, frontier));
            }
        }
        if !compaction_commands.is_empty() {
            self.compute
                .replicas
                .send(ComputeCommand::AllowCompaction(compaction_commands));
        }

        // We may have storage consequences to process.
        if !storage_todo.is_empty() {
            self.storage_controller
                .update_read_capabilities(&mut storage_todo)
                .await?;
        }
        Ok(())
    }

    /// Removes a registered peek, unblocking compaction that might have waited on it.
    async fn remove_peeks(
        &mut self,
        peek_ids: impl IntoIterator<Item = uuid::Uuid>,
    ) -> Result<(), ComputeError> {
        let mut updates = peek_ids
            .into_iter()
            .flat_map(|uuid| {
                self.compute
                    .peeks
                    .remove(&uuid)
                    .map(|(id, time)| (id, ChangeBatch::new_from(time, -1)))
            })
            .collect();
        self.update_read_capabilities(&mut updates).await?;
        Ok(())
    }

    /// Downgrade the read capabilities of specific identifiers to specific frontiers.
    ///
    /// Downgrading any read capability to the empty frontier will drop the item and eventually reclaim its resources.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn allow_compaction(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.validate_ids(frontiers.iter().map(|(id, _)| *id))?;
        let policies = frontiers
            .into_iter()
            .map(|(id, frontier)| (id, ReadPolicy::ValidFrom(frontier)));
        self.set_read_policy(policies.collect()).await?;
        Ok(())
    }

    /// Downgrade the read capabilities of specific identifiers to specific frontiers.
    ///
    /// Downgrading any read capability to the empty frontier will drop the item and eventually reclaim its resources.
    /// TODO(jkosh44): This method does not validate the provided identifiers. Currently when the
    ///     controller starts/restarts it has no durable state. That means that it has no way of
    ///     remembering any past commands sent. In the future we plan on persisting state for the
    ///     controller so that it is aware of past commands.
    ///     Therefore this method is for allowing compaction on objects that we know to have been
    ///     previously created, but have been forgotten by the controller due to a restart.
    ///     Once command history becomes durable we can remove this method and use the normal
    ///     `allow_compaction`.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn allow_compaction_unvalidated(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), ComputeError> {
        let policies = frontiers
            .into_iter()
            .map(|(id, frontier)| (id, ReadPolicy::ValidFrom(frontier)));
        self.set_read_policy(policies.collect()).await?;
        Ok(())
    }
}

/// State maintained about individual collections.
#[derive(Debug)]
pub struct CollectionState<T> {
    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation will always contain `self.implied_capability`, but may also contain
    /// capabilities held by others who have read dependencies on this collection.
    read_capabilities: MutableAntichain<T>,
    /// The implicit capability associated with collection creation.
    implied_capability: Antichain<T>,
    /// The policy to use to downgrade `self.implied_capability`.
    read_policy: ReadPolicy<T>,

    /// Storage identifiers on which this collection depends.
    storage_dependencies: Vec<GlobalId>,
    /// Compute identifiers on which this collection depends.
    compute_dependencies: Vec<GlobalId>,

    /// Reported write frontier.
    write_frontier: Antichain<T>,
}

impl<T: Timestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from `since`.
    pub fn new(
        since: Antichain<T>,
        storage_dependencies: Vec<GlobalId>,
        compute_dependencies: Vec<GlobalId>,
    ) -> Self {
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));
        Self {
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::ValidFrom(since),
            storage_dependencies,
            compute_dependencies,
            write_frontier: Antichain::from_elem(Timestamp::minimum()),
        }
    }

    /// Reports the current read capability.
    pub fn read_capability(&self) -> &Antichain<T> {
        &self.implied_capability
    }

    /// Reports the current read frontier.
    pub fn read_frontier(&self) -> AntichainRef<T> {
        self.read_capabilities.frontier()
    }

    /// Reports the current write frontier.
    pub fn write_frontier(&self) -> AntichainRef<T> {
        self.write_frontier.borrow()
    }
}
