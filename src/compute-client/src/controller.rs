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
//! For each compute instance, the compute controller curates the creation of indexes and sinks
//! installed on the instance, the progress of readers through these collections, and their
//! eventual dropping and resource reclamation.
//!
//! The state maintained for a compute instance can be viewed as a partial map from `GlobalId` to
//! collection. It is an error to use an identifier before it has been "created" with
//! `create_dataflows()`. Once created, the controller holds a read capability for each output
//! collection of a dataflow, which is manipulated with `set_read_policy()`. Eventually, a
//! collection is dropped with either `drop_collections()` or by allowing compaction to the empty
//! frontier.
//!
//! Created dataflows will prevent the compaction of their inputs, including other compute
//! collections but also collections managed by the storage layer. Each dataflow input is prevented
//! from compacting beyond the allowed compaction of each of its outputs, ensuring that we can
//! recover each dataflow to its current state in case of failure or other reconfiguration.

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use futures::stream::BoxStream;
use futures::{future, FutureExt};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_expr::RowSetFinishing;
use mz_orchestrator::{CpuLimit, MemoryLimit, NamespacedOrchestrator};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Row};
use mz_storage::controller::{ReadPolicy, StorageController, StorageError};

use crate::command::{DataflowDescription, ProcessId, ReplicaId};
use crate::logging::{LogVariant, LogView, LoggingConfig};
use crate::response::{ComputeResponse, PeekResponse, SubscribeResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};

use self::instance::{ActiveInstance, Instance};
use self::orchestrator::ComputeOrchestrator;

mod instance;
mod orchestrator;
mod replica;

pub use mz_orchestrator::ServiceStatus as ComputeInstanceStatus;

/// An abstraction allowing us to name different compute instances.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum ComputeInstanceId {
    System(u64),
    User(u64),
}

impl ComputeInstanceId {
    pub fn inner_id(&self) -> u64 {
        match self {
            ComputeInstanceId::System(id) | ComputeInstanceId::User(id) => *id,
        }
    }

    pub fn is_user(&self) -> bool {
        matches!(self, Self::User(_))
    }

    pub fn is_system(&self) -> bool {
        matches!(self, Self::System(_))
    }
}

impl FromStr for ComputeInstanceId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() < 2 {
            return Err(anyhow!("couldn't parse compute instance id {}", s));
        }
        let val: u64 = s[1..].parse()?;
        match s.chars().next().unwrap() {
            's' => Ok(Self::System(val)),
            'u' => Ok(Self::User(val)),
            _ => Err(anyhow!("couldn't parse role id {}", s)),
        }
    }
}

impl fmt::Display for ComputeInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::System(id) => write!(f, "s{}", id),
            Self::User(id) => write!(f, "u{}", id),
        }
    }
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

/// Responses from the compute controller.
#[derive(Debug)]
pub enum ComputeControllerResponse<T> {
    /// See [`ComputeResponse::PeekResponse`](crate::response::ComputeResponse::PeekResponse).
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// See [`ComputeResponse::SubscribeResponse`](crate::response::ComputeResponse::SubscribeResponse).
    SubscribeResponse(GlobalId, SubscribeResponse<T>),
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
pub struct ComputeReplicaConfig {
    pub location: ComputeReplicaLocation,
    pub logging: ComputeReplicaLogging,
}

/// Size or location of a replica
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ComputeReplicaLocation {
    /// Out-of-process replica
    Remote {
        /// The network addresses of the processes in the replica.
        addrs: BTreeSet<String>,
        /// The network addresses of the Timely endpoints of the processes in the replica.
        compute_addrs: BTreeSet<String>,
        /// The workers per process in the replica.
        workers: NonZeroUsize,
    },
    /// Out-of-process replica
    /// A remote but managed replica
    Managed {
        /// The resource allocation for the replica.
        allocation: ComputeReplicaAllocation,
        /// SQL size parameter used for allocation
        size: String,
        /// The replica's availability zone
        availability_zone: String,
        /// `true` if the AZ was specified by the user and must be respected;
        /// `false` if it was picked arbitrarily by Materialize.
        az_user_specified: bool,
    },
}

impl ComputeReplicaLocation {
    pub fn get_az(&self) -> Option<&str> {
        match self {
            ComputeReplicaLocation::Remote { .. } => None,
            ComputeReplicaLocation::Managed {
                availability_zone, ..
            } => Some(availability_zone),
        }
    }
}

/// Resource allocations for a replica of a compute instance.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct ComputeReplicaAllocation {
    /// The memory limit for each process in the replica.
    pub memory_limit: Option<MemoryLimit>,
    /// The CPU limit for each process in the replica.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of processes in the replica.
    pub scale: NonZeroUsize,
    /// The number of worker threads in the replica.
    pub workers: NonZeroUsize,
}

impl ComputeReplicaAllocation {
    pub fn workers(&self) -> NonZeroUsize {
        self.workers
    }
}

/// Logging configuration of a replica.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComputeReplicaLogging {
    /// Whether to enable logging for the logging dataflows.
    pub log_logging: bool,
    /// The interval at which to log.
    ///
    /// A `None` value indicates that logging is disabled.
    pub interval: Option<Duration>,
    /// Log sources of this replica.
    pub sources: Vec<(LogVariant, GlobalId)>,
    /// Log views of this replica.
    pub views: Vec<(LogView, GlobalId)>,
}

impl ComputeReplicaLogging {
    /// Return whether logging is enabled.
    pub fn enabled(&self) -> bool {
        self.interval.is_some()
    }

    /// Return all ids of the persisted introspection views contained.
    pub fn view_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.views.iter().map(|(_, id)| *id)
    }

    /// Return all ids of the persisted introspection sources contained.
    pub fn source_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.sources.iter().map(|(_, id)| *id)
    }

    /// Return all ids of the persisted introspection sources and logs contained.
    pub fn source_and_view_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.source_ids().chain(self.view_ids())
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
    stashed_response: Option<(ComputeInstanceId, ReplicaId, ComputeResponse<T>)>,
    /// Times we have last received responses from replicas.
    replica_heartbeats: BTreeMap<ReplicaId, DateTime<Utc>>,
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
            replica_heartbeats: BTreeMap::new(),
        }
    }

    pub fn instance_exists(&self, id: ComputeInstanceId) -> bool {
        self.instances.contains_key(&id)
    }

    /// Return a reference to the indicated compute instance.
    fn instance(&self, id: ComputeInstanceId) -> Result<&Instance<T>, ComputeError> {
        self.instances
            .get(&id)
            .ok_or(ComputeError::InstanceMissing(id))
    }

    /// Return a mutable reference to the indicated compute instance.
    fn instance_mut(&mut self, id: ComputeInstanceId) -> Result<&mut Instance<T>, ComputeError> {
        self.instances
            .get_mut(&id)
            .ok_or(ComputeError::InstanceMissing(id))
    }

    /// Return a read-only handle to the indicated compute instance.
    pub fn instance_ref(
        &self,
        id: ComputeInstanceId,
    ) -> Result<ComputeInstanceRef<T>, ComputeError> {
        self.instance(id).map(|instance| ComputeInstanceRef {
            instance_id: id,
            instance,
        })
    }

    /// Return a read-only handle to the indicated collection.
    pub fn collection(
        &self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
    ) -> Result<&CollectionState<T>, ComputeError> {
        self.instance(instance_id)?.collection(collection_id)
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
        arranged_logs: BTreeMap<LogVariant, GlobalId>,
        max_result_size: u32,
    ) -> Result<(), ComputeError> {
        if self.instances.contains_key(&id) {
            return Err(ComputeError::InstanceExists(id));
        }

        self.instances.insert(
            id,
            Instance::new(
                id,
                self.build_info,
                arranged_logs,
                max_result_size,
                self.orchestrator.clone(),
            ),
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

    /// Wait until the controller is ready to do some processing.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the caller should call [`ActiveComputeController::process`].
    ///
    /// This method is cancellation safe.
    pub async fn ready(&mut self) {
        if self.stashed_response.is_some() {
            // We still have a response stashed, which we are immediately ready to process.
            return;
        }
        if !self.replica_heartbeats.is_empty() {
            // We have replica heartbeats waiting to be processes.
            return;
        }
        if self.instances.values().any(|i| i.wants_processing()) {
            // An instance requires processing.
            return;
        }

        if self.instances.is_empty() {
            // If there are no clients, block forever. This signals that there may be more work to
            // do (e.g., if a compute instance is created). Calling `select_all` with an empty list
            // of futures will panic.
            future::pending().await
        }

        // `Instance::recv` is cancellation safe, so it is safe to construct this `select_all`.
        let receives = self
            .instances
            .iter_mut()
            .map(|(id, instance)| Box::pin(instance.recv().map(|result| (*id, result))));
        let ((instance_id, result), _index, _remaining) = future::select_all(receives).await;

        if let Ok((replica_id, resp)) = result {
            self.replica_heartbeats.insert(replica_id, Utc::now());
            self.stashed_response = Some((instance_id, replica_id, resp));
        } else {
            // There is nothing to do here. `recv` has already added the failed replica to
            // `instance.failed_replicas`, so it will be rehydrated in the next call to
            // `ActiveComputeController::process`.
        }
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
    pub fn instance_exists(&self, id: ComputeInstanceId) -> bool {
        self.compute.instance_exists(id)
    }

    /// Return a read-only handle to the indicated collection.
    pub fn collection(
        &self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
    ) -> Result<&CollectionState<T>, ComputeError> {
        self.compute.collection(instance_id, collection_id)
    }

    /// Return a handle to the indicated compute instance.
    fn instance(&mut self, id: ComputeInstanceId) -> Result<ActiveInstance<T>, ComputeError> {
        self.compute
            .instance_mut(id)
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
        config: ComputeReplicaConfig,
    ) -> Result<(), ComputeError> {
        let logging_config = if let Some(interval) = config.logging.interval {
            let mut sink_logs = BTreeMap::new();
            for (variant, id) in config.logging.sources {
                let storage_meta = self.storage.collection(id)?.collection_metadata.clone();
                sink_logs.insert(variant, (id, storage_meta));
            }

            Some(LoggingConfig {
                interval_ns: interval.as_nanos(),
                active_logs: Default::default(),
                log_logging: config.logging.log_logging,
                sink_logs,
            })
        } else {
            None
        };

        self.instance(instance_id)?
            .add_replica(replica_id, config.location, logging_config)
            .await
    }

    /// Removes a replica from an instance, including its service in the orchestrator.
    pub async fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
    ) -> Result<(), ComputeError> {
        self.instance(instance_id)?.remove_replica(replica_id).await
    }

    /// Create and maintain the described dataflows, and initialize state for their output.
    ///
    /// This method creates dataflows whose inputs are still readable at the dataflow `as_of`
    /// frontier, and initializes the outputs as readable from that frontier onward.
    /// It installs read dependencies from the outputs to the inputs, so that the input read
    /// capabilities will be held back to the output read capabilities, ensuring that we are
    /// always able to return to a state that can serve the output read capabilities.
    pub async fn create_dataflows(
        &mut self,
        instance_id: ComputeInstanceId,
        dataflows: Vec<DataflowDescription<crate::plan::Plan<T>, (), T>>,
    ) -> Result<(), ComputeError> {
        self.instance(instance_id)?
            .create_dataflows(dataflows)
            .await
    }

    /// Drop the read capability for the given collections and allow their resources to be
    /// reclaimed.
    pub async fn drop_collections(
        &mut self,
        instance_id: ComputeInstanceId,
        collection_ids: Vec<GlobalId>,
    ) -> Result<(), ComputeError> {
        self.instance(instance_id)?
            .drop_collections(collection_ids)
            .await
    }

    /// Initiate a peek request for the contents of the given collection at `timestamp`.
    pub async fn peek(
        &mut self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<ReplicaId>,
    ) -> Result<(), ComputeError> {
        self.instance(instance_id)?
            .peek(
                collection_id,
                literal_constraints,
                uuid,
                timestamp,
                finishing,
                map_filter_project,
                target_replica,
            )
            .await
    }

    /// Cancel existing peek requests.
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
    pub fn cancel_peeks(
        &mut self,
        instance_id: ComputeInstanceId,
        uuids: BTreeSet<Uuid>,
    ) -> Result<(), ComputeError> {
        self.instance(instance_id)?.cancel_peeks(uuids);
        Ok(())
    }

    /// Assign a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated identifiers should
    /// conclude with the last policy. Changing a policy will immediately downgrade the read
    /// capability if appropriate, but it will not "recover" the read capability if the prior
    /// capability is already ahead of it.
    ///
    /// Identifiers not present in `policies` retain their existing read policies.
    pub async fn set_read_policy(
        &mut self,
        instance_id: ComputeInstanceId,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), ComputeError> {
        self.instance(instance_id)?.set_read_policy(policies).await
    }

    /// Update the max size in bytes of any result.
    pub fn update_max_result_size(
        &mut self,
        instance_id: ComputeInstanceId,
        max_result_size: u32,
    ) -> Result<(), ComputeError> {
        self.instance(instance_id)?
            .update_max_result_size(max_result_size);
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
        // Rehydrate any failed replicas.
        for instance in self.compute.instances.values_mut() {
            instance
                .activate(self.storage)
                .rehydrate_failed_replicas()
                .await
                .expect("error rehydrating failed replicas");
        }

        // Process pending ready responses.
        for instance in self.compute.instances.values_mut() {
            if let Some(response) = instance.ready_responses.pop_front() {
                return Ok(Some(response));
            }
        }

        // Process pending replica heartbeats.
        // TODO(teskje): Use `BTreeMap::pop_first`, once stable.
        if let Some(replica_id) = self.compute.replica_heartbeats.keys().next().copied() {
            let when = self.compute.replica_heartbeats.remove(&replica_id).unwrap();
            return Ok(Some(ComputeControllerResponse::ReplicaHeartbeat(
                replica_id, when,
            )));
        }

        // Process pending responses from replicas.
        if let Some((instance_id, replica_id, response)) = self.compute.stashed_response.take() {
            let mut instance = self.instance(instance_id)?;
            instance.handle_response(response, replica_id).await
        } else {
            Ok(None)
        }
    }
}

/// A read-only handle to a compute instance.
#[derive(Debug, Clone, Copy)]
pub struct ComputeInstanceRef<'a, T> {
    instance_id: ComputeInstanceId,
    instance: &'a Instance<T>,
}

impl<T> ComputeInstanceRef<'_, T> {
    /// Return the ID of this compute instance.
    pub fn instance_id(&self) -> ComputeInstanceId {
        self.instance_id
    }

    /// Return a read-only handle to the indicated collection.
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
        self.instance.collection(id)
    }
}

/// State maintained about individual collections.
#[derive(Debug)]
pub struct CollectionState<T> {
    /// Whether this collection is a log collection.
    ///
    /// Log collections are special in that they are only maintained by a subset of all replicas.
    log_collection: bool,

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

    /// The write frontier of this collection.
    write_frontier: Antichain<T>,
    /// The write frontiers reported by individual replicas.
    replica_write_frontiers: BTreeMap<ReplicaId, Antichain<T>>,
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
            log_collection: false,
            read_capabilities,
            implied_capability: since.clone(),
            read_policy: ReadPolicy::ValidFrom(since),
            storage_dependencies,
            compute_dependencies,
            write_frontier: Antichain::from_elem(Timestamp::minimum()),
            replica_write_frontiers: BTreeMap::new(),
        }
    }

    pub fn new_log_collection() -> Self {
        let since = Antichain::from_elem(Timestamp::minimum());
        let mut state = Self::new(since, Vec::new(), Vec::new());
        state.log_collection = true;
        state
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
