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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{future, FutureExt, StreamExt};
use serde::{Deserialize, Serialize};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use timely::PartialOrder;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_expr::RowSetFinishing;
use mz_orchestrator::{CpuLimit, MemoryLimit, NamespacedOrchestrator};
use mz_ore::retry::Retry;
use mz_ore::task::{AbortOnDropHandle, JoinHandleExt};
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Row};
use mz_service::client::GenericClient;
use mz_storage::controller::{ReadPolicy, StorageController, StorageError};

use crate::command::{
    CommunicationConfig, ComputeCommand, ComputeCommandHistory, DataflowDescription,
    InstanceConfig, Peek, ProcessId, ReplicaId, SourceInstanceDesc,
};
use crate::logging::{LogVariant, LogView, LoggingConfig};
use crate::response::{ComputeResponse, PeekResponse, SubscribeBatch, SubscribeResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};
use crate::sinks::{ComputeSinkConnection, ComputeSinkDesc, PersistSinkConnection};

use self::orchestrator::ComputeOrchestrator;

mod orchestrator;

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
            Instance::new(self.build_info, arranged_logs, max_result_size),
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
            .map(|(id, instance)| Box::pin(instance.recv().map(|resp| (*id, resp))));
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
        let (addrs, communication_config) = match config.location {
            ComputeReplicaLocation::Remote {
                addrs,
                compute_addrs,
                workers,
            } => {
                let addrs = addrs.into_iter().collect();
                let comm = CommunicationConfig {
                    workers: workers.get(),
                    process: 0,
                    addresses: compute_addrs.into_iter().collect(),
                };
                (addrs, comm)
            }
            ComputeReplicaLocation::Managed {
                allocation,
                availability_zone,
                ..
            } => {
                let service = self
                    .compute
                    .orchestrator
                    .ensure_replica(instance_id, replica_id, allocation, availability_zone)
                    .await?;

                let addrs = service.addresses("controller");
                let comm = CommunicationConfig {
                    workers: allocation.workers.get(),
                    process: 0,
                    addresses: service.addresses("compute"),
                };
                (addrs, comm)
            }
        };

        self.instance(instance_id)?.add_replica(
            replica_id,
            addrs,
            config.logging,
            communication_config,
        )
    }

    /// Removes a replica from an instance, including its service in the orchestrator.
    pub async fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        config: ComputeReplicaConfig,
    ) -> Result<(), ComputeError> {
        if let ComputeReplicaLocation::Managed { .. } = config.location {
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
    pub async fn cancel_peeks(
        &mut self,
        instance_id: ComputeInstanceId,
        uuids: BTreeSet<Uuid>,
    ) -> Result<(), ComputeError> {
        self.instance(instance_id)?.cancel_peeks(uuids).await
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
        let (instance_id, response) = match self.compute.stashed_response.take() {
            Some(resp) => resp,
            None => return Ok(None),
        };

        let mut instance = self.instance(instance_id)?;

        match response {
            ActiveReplicationResponse::FrontierUppers(updates) => {
                instance.update_write_frontiers(&updates).await?;
                Ok(None)
            }
            ActiveReplicationResponse::PeekResponse(uuid, peek_response, otel_ctx) => Ok(Some(
                ComputeControllerResponse::PeekResponse(uuid, peek_response, otel_ctx),
            )),
            ActiveReplicationResponse::PeekFinished(uuid) => {
                instance.remove_peeks(&[uuid].into()).await?;
                Ok(None)
            }
            ActiveReplicationResponse::SubscribeResponse(global_id, response) => {
                if let SubscribeResponse::Batch(SubscribeBatch { lower, .. }) = &response {
                    // Ensure there are no gaps in the subscribe stream we receive.
                    assert_eq!(
                        lower,
                        &instance.compute.collections[&global_id].write_frontier_upper,
                    );
                };

                Ok(Some(ComputeControllerResponse::SubscribeResponse(
                    global_id, response,
                )))
            }
            ActiveReplicationResponse::ReplicaHeartbeat(replica_id, when) => Ok(Some(
                ComputeControllerResponse::ReplicaHeartbeat(replica_id, when),
            )),
        }
    }
}

/// The state we keep for a compute instance.
#[derive(Debug)]
struct Instance<T> {
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// The replicas of this compute instance.
    replicas: HashMap<ReplicaId, ReplicaState<T>>,
    /// Tracks expressed `since` and received `upper` frontiers for indexes and sinks.
    collections: BTreeMap<GlobalId, CollectionState<T>>,
    /// IDs of arranged log sources maintained by this compute instance.
    arranged_logs: BTreeMap<LogVariant, GlobalId>,
    /// Currently outstanding peeks.
    peeks: HashMap<Uuid, PendingPeek<T>>,
    /// IDs of in-progress subscribes, to guide responses (and which to suppress).
    subscribes: BTreeSet<GlobalId>,
    /// Reported upper frontiers for replicated collections and in-progress subscribes.
    uppers: HashMap<GlobalId, ReportedUppers<T>>,
    /// Reported upper frontiers for arranged log collections.
    ///
    /// Arranged log collections are special in that their IDs are shared between replicas, but
    /// only exist on replicas that have introspection enabled.
    index_log_uppers: HashMap<GlobalId, ReportedUppers<T>>,
    /// Reported upper frontiers for persisted log collections.
    ///
    /// Persisted log collections are special in that they are replica-specific.
    sink_log_uppers: HashMap<GlobalId, Antichain<T>>,
    /// The command history, used when introducing new replicas or restarting existing replicas.
    history: ComputeCommandHistory<T>,
    /// Responses that should be emitted on the next `recv` call.
    pending_response: VecDeque<ActiveReplicationResponse<T>>,
}

impl<T> Instance<T> {
    /// Acquire a handle to the collection state associated with `id`.
    fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
        self.collections
            .get(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }

    /// Acquire a mutable handle to the collection state associated with `id`.
    fn collection_mut(&mut self, id: GlobalId) -> Result<&mut CollectionState<T>, ComputeError> {
        self.collections
            .get_mut(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }

    /// Acquire an [`ActiveInstance`] by providing a storage controller.
    fn activate<'a>(
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
    fn new(
        build_info: &'static BuildInfo,
        arranged_logs: BTreeMap<LogVariant, GlobalId>,
        max_result_size: u32,
    ) -> Self {
        let collections = arranged_logs
            .iter()
            .map(|(_, id)| {
                let state = CollectionState::new(
                    Antichain::from_elem(T::minimum()),
                    Vec::new(),
                    Vec::new(),
                );
                (*id, state)
            })
            .collect();

        let mut instance = Self {
            build_info,
            replicas: Default::default(),
            collections,
            arranged_logs,
            peeks: Default::default(),
            subscribes: Default::default(),
            uppers: Default::default(),
            index_log_uppers: Default::default(),
            sink_log_uppers: Default::default(),
            history: Default::default(),
            pending_response: Default::default(),
        };

        instance.send(ComputeCommand::CreateTimely(Default::default()));
        instance.send(ComputeCommand::CreateInstance(InstanceConfig {
            replica_id: Default::default(),
            logging: None,
            max_result_size,
        }));

        instance
    }

    /// Marks the end of any initialization commands.
    ///
    /// Intended to be called by `Controller`, rather than by other code (to avoid repeated calls).
    fn initialization_complete(&mut self) {
        self.send(ComputeCommand::InitializationComplete);
    }

    /// Drop this compute instance.
    ///
    /// # Panics
    /// - If the compute instance still has active replicas.
    fn drop(mut self) {
        assert!(
            self.replicas.is_empty(),
            "cannot drop instances with provisioned replicas"
        );
        self.send(ComputeCommand::DropInstance);
    }

    /// Introduce a new replica, and catch it up to the commands of other replicas.
    ///
    /// It is not yet clear under which circumstances a replica can be removed.
    fn add_replica(
        &mut self,
        id: ReplicaId,
        addrs: Vec<String>,
        logging_config: Option<LoggingConfig>,
        communication_config: CommunicationConfig,
    ) {
        // Launch a task to handle communication with the replica
        // asynchronously. This isolates the main controller thread from
        // the replica.
        let (command_tx, command_rx) = unbounded_channel();
        let (response_tx, response_rx) = unbounded_channel();
        let task = mz_ore::task::spawn(
            || format!("active-replication-replica-{id}"),
            replica_task(ReplicaTaskConfig {
                replica_id: id,
                build_info: self.build_info,
                addrs: addrs.clone(),
                command_rx,
                response_tx,
            }),
        );

        // Take this opportunity to clean up the history we should present.
        self.history.retain_peeks(&self.peeks);
        self.history.reduce();

        let replica_state = ReplicaState {
            command_tx,
            response_rx,
            _task: task.abort_on_drop(),
            addrs,
            logging_config,
            communication_config,
        };

        // Replay the commands at the client, creating new dataflow identifiers.
        for command in self.history.iter() {
            let mut command = command.clone();
            replica_state.specialize_command(&mut command, id);
            if replica_state.command_tx.send(command).is_err() {
                // We swallow the error here. On the next send, we will fail again, and
                // restart the connection as well as this rehydration.
                tracing::warn!("Replica {:?} connection terminated during rehydration", id);
                break;
            }
        }

        if let Some(logging) = &replica_state.logging_config {
            // Start tracking frontiers of persisted log collections.
            for (collection_id, _) in logging.sink_logs.values() {
                let frontier = Antichain::from_elem(Timestamp::minimum());
                let previous = self.sink_log_uppers.insert(*collection_id, frontier);
                assert!(previous.is_none());
            }

            // Start tracking frontiers of arranged log collections.
            for collection_id in logging.active_logs.values() {
                self.index_log_uppers
                    .entry(*collection_id)
                    .and_modify(|reported| reported.add_replica(id))
                    .or_insert_with(|| ReportedUppers::new([id]));
            }
        }

        // Add replica to tracked state.
        self.replicas.insert(id, replica_state);
        for uppers in self.uppers.values_mut() {
            uppers.add_replica(id);
        }
        for peek in self.peeks.values_mut() {
            peek.unfinished.insert(id);
        }
    }

    /// Sends a command to all replicas.
    #[tracing::instrument(level = "debug", skip(self))]
    fn send(&mut self, cmd: ComputeCommand<T>) {
        self.handle_command(&cmd);

        // Clone the command for each active replica.
        let mut failed_replicas = vec![];
        for (id, replica) in self.replicas.iter_mut() {
            let mut command = cmd.clone();
            replica.specialize_command(&mut command, *id);
            // If sending the command fails, the replica requires rehydration.
            if replica.command_tx.send(command).is_err() {
                failed_replicas.push(*id);
            }
        }
        for id in failed_replicas {
            self.rehydrate_replica(id);
        }
    }

    /// Receives the next response from any replica.
    ///
    /// This method is cancellation safe.
    async fn recv(&mut self) -> ActiveReplicationResponse<T> {
        // If we have a pending response, we should send it immediately.
        if let Some(response) = self.pending_response.pop_front() {
            return response;
        }

        // Receive responses from any of the replicas, and take appropriate
        // action.
        loop {
            let response = self
                .replicas
                .iter_mut()
                .map(|(id, replica)| async { (*id, replica.response_rx.recv().await) })
                .collect::<FuturesUnordered<_>>()
                .next()
                .await;

            match response {
                None => {
                    // There were no replicas in the set. Block forever to
                    // communicate that no response is ready.
                    future::pending().await
                }
                Some((replica_id, None)) => {
                    // A replica has failed and requires rehydration.
                    self.rehydrate_replica(replica_id)
                }
                Some((replica_id, Some(response))) => {
                    // A replica has produced a response. Absorb it, possibly
                    // returning a response up the stack.
                    match self.handle_response(response, replica_id) {
                        Some(response) => return response,
                        None => { /* continue */ }
                    }
                }
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    fn handle_command(&mut self, cmd: &ComputeCommand<T>) {
        // Update our tracking of peek and subscribe commands.
        match &cmd {
            ComputeCommand::CancelPeeks { uuids } => {
                // Enqueue the response to the cancelation.
                for uuid in uuids {
                    let otel_ctx = self
                        .peeks
                        .get_mut(uuid)
                        // Canceled peeks should not be further responded to.
                        .map(|pending| pending.otel_ctx.take())
                        .unwrap_or_else(|| {
                            tracing::warn!("did not find pending peek for {}", uuid);
                            None
                        });
                    if let Some(ctx) = otel_ctx {
                        self.pending_response
                            .push_back(ActiveReplicationResponse::PeekResponse(
                                *uuid,
                                PeekResponse::Canceled,
                                ctx,
                            ));
                    }
                }
            }
            ComputeCommand::CreateDataflows(dataflows) => {
                let subscribe_ids = dataflows.iter().flat_map(|df| df.subscribe_ids());
                self.subscribes.extend(subscribe_ids);
            }
            _ => {}
        }

        // Initialize any necessary frontier tracking.
        let mut start = Vec::new();
        let mut cease = Vec::new();
        cmd.frontier_tracking(&mut start, &mut cease);
        for id in start.into_iter() {
            self.start_frontier_tracking(id);
        }
        for id in cease.into_iter() {
            self.cease_frontier_tracking(id);
        }

        // Record the command so that new replicas can be brought up to speed.
        self.history.push(cmd.clone());
    }

    fn start_frontier_tracking(&mut self, id: GlobalId) {
        let uppers = ReportedUppers::new(self.replicas.keys().copied());
        let previous = self.uppers.insert(id, uppers);
        assert!(previous.is_none());
    }

    fn cease_frontier_tracking(&mut self, id: GlobalId) {
        let previous = self.uppers.remove(&id).expect("untracked frontier");

        // If we cease tracking an in-progress subscribe, we should emit a `DroppedAt` response.
        if self.subscribes.remove(&id) {
            self.pending_response
                .push_back(ActiveReplicationResponse::SubscribeResponse(
                    id,
                    SubscribeResponse::DroppedAt(previous.bounds.upper),
                ));
        }
    }

    fn handle_response(
        &mut self,
        message: ComputeResponse<T>,
        replica_id: ReplicaId,
    ) -> Option<ActiveReplicationResponse<T>> {
        self.pending_response
            .push_front(ActiveReplicationResponse::ReplicaHeartbeat(
                replica_id,
                Utc::now(),
            ));
        match message {
            ComputeResponse::PeekResponse(uuid, response, otel_ctx) => {
                self.handle_peek_response(uuid, response, otel_ctx, replica_id)
            }
            ComputeResponse::FrontierUppers(list) => self.handle_frontier_uppers(list, replica_id),
            ComputeResponse::SubscribeResponse(id, response) => {
                self.handle_subscribe_response(id, response, replica_id)
            }
        }
    }
}

/// A wrapper around [`Instance`] with a live storage controller.
#[derive(Debug)]
struct ActiveInstance<'a, T> {
    compute: &'a mut Instance<T>,
    storage_controller: &'a mut dyn StorageController<Timestamp = T>,
}

impl<'a, T> ActiveInstance<'a, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Add a new instance replica, by ID.
    fn add_replica(
        &mut self,
        id: ReplicaId,
        addrs: Vec<String>,
        logging: ComputeReplicaLogging,
        communication_config: CommunicationConfig,
    ) -> Result<(), ComputeError> {
        let logging_config = if let Some(interval) = logging.interval {
            // Initialize state for per-replica log sources.
            let mut sink_logs = BTreeMap::new();
            for (variant, id) in logging.sources {
                self.compute.collections.insert(
                    id,
                    CollectionState::new(
                        Antichain::from_elem(T::minimum()),
                        Vec::new(),
                        Vec::new(),
                    ),
                );

                let storage_meta = self
                    .storage_controller
                    .collection(id)?
                    .collection_metadata
                    .clone();
                sink_logs.insert(variant, (id, storage_meta));
            }

            Some(LoggingConfig {
                interval_ns: interval.as_nanos(),
                active_logs: self.compute.arranged_logs.clone(),
                log_logging: logging.log_logging,
                sink_logs,
            })
        } else {
            None
        };

        // Add the replica
        self.compute
            .add_replica(id, addrs, logging_config, communication_config);
        Ok(())
    }

    /// Remove an existing instance replica, by ID.
    fn remove_replica(&mut self, id: ReplicaId) {
        // Removing a replica might elicit changes to collection frontiers, which we must
        // report up the chain.
        let mut new_uppers = Vec::new();
        for (collection_id, uppers) in self.uppers.iter_mut() {
            if uppers.remove_replica(id) {
                new_uppers.push((*collection_id, uppers.bounds.clone()));
            }
        }
        for (collection_id, uppers) in self.index_log_uppers.iter_mut() {
            if uppers.tracks_replica(id) && uppers.remove_replica(id) {
                new_uppers.push((*collection_id, uppers.bounds.clone()));
            }
        }
        if !new_uppers.is_empty() {
            self.pending_response
                .push_back(ActiveReplicationResponse::FrontierUppers(new_uppers));
        }

        // Removing a replica might implicitly finish a peek, which we must report up the chain.
        for (uuid, peek) in &mut self.peeks {
            peek.unfinished.remove(&id);
            if peek.is_finished() {
                self.pending_response
                    .push_back(ActiveReplicationResponse::PeekFinished(uuid.clone()));
            }
        }

        let replica_state = self.replicas.remove(&id).expect("replica not found");

        // Cease tracking frontiers of persisted log collections.
        if let Some(logging) = &replica_state.logging_config {
            for (collection_id, _) in logging.sink_logs.values() {
                let previous = self.sink_log_uppers.remove(collection_id);
                assert!(previous.is_some());
            }
        }
    }

    fn rehydrate_replica(&mut self, id: ReplicaId) {
        let addrs = self.replicas[&id].addrs.clone();
        let logging_config = self.replicas[&id].logging_config.clone();
        let communication_config = self.replicas[&id].communication_config.clone();
        self.remove_replica(id);
        self.add_replica(id, addrs, logging_config, communication_config);
    }

    /// Create the described dataflows and initializes state for their output.
    async fn create_dataflows(
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
                    ComputeSinkConnection::Subscribe(conn) => {
                        ComputeSinkConnection::Subscribe(conn)
                    }
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
            .send(ComputeCommand::CreateDataflows(augmented_dataflows));

        Ok(())
    }

    /// Drops the read capability for the given collections and allows their resources to be
    /// reclaimed.
    async fn drop_collections(&mut self, ids: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.validate_ids(ids.iter().cloned())?;

        let compaction_commands = ids.into_iter().map(|id| (id, Antichain::new())).collect();
        self.allow_compaction(compaction_commands).await?;
        Ok(())
    }

    /// Initiate a peek request for the contents of `id` at `timestamp`.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn peek(
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

        let unfinished = match &target_replica {
            Some(target) => [*target].into(),
            None => self.compute.replicas.keys().copied().collect(),
        };
        let otel_ctx = OpenTelemetryContext::obtain();
        self.compute.peeks.insert(
            uuid,
            PendingPeek {
                target: id,
                time: timestamp.clone(),
                unfinished,
                // TODO(guswynn): can we just hold the `tracing::Span` here instead?
                otel_ctx: Some(otel_ctx.clone()),
            },
        );

        self.compute.send(ComputeCommand::Peek(Peek {
            id,
            literal_constraints,
            uuid,
            timestamp,
            finishing,
            map_filter_project,
            target_replica,
            // Obtain an `OpenTelemetryContext` from the thread-local tracing
            // tree to forward it on to the compute worker.
            otel_ctx: otel_ctx.clone(),
        }));

        Ok(())
    }

    /// Cancels existing peek requests.
    async fn cancel_peeks(&mut self, uuids: BTreeSet<Uuid>) -> Result<(), ComputeError> {
        self.remove_peeks(&uuids).await?;
        self.compute.send(ComputeCommand::CancelPeeks { uuids });
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
    async fn set_read_policy(
        &mut self,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), ComputeError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, policy) in policies.into_iter() {
            if let Ok(collection) = self.compute.collection_mut(id) {
                let mut new_read_capability =
                    policy.frontier(collection.write_frontier_lower.borrow());

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
    fn update_max_result_size(&mut self, max_result_size: u32) {
        self.compute
            .send(ComputeCommand::UpdateMaxResultSize(max_result_size))
    }

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
        updates: &[(GlobalId, FrontierBounds<T>)],
    ) -> Result<(), ComputeError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, new_upper) in updates.iter() {
            let collection = self
                .compute
                .collection_mut(*id)
                .expect("Reference to absent collection");

            collection
                .write_frontier_upper
                .join_assign(&new_upper.upper);
            collection
                .write_frontier_lower
                .join_assign(&new_upper.lower);

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier_lower.borrow());
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
            .map(|(id, bounds)| (*id, bounds.upper.clone()))
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
    async fn remove_peeks(&mut self, peek_ids: &BTreeSet<Uuid>) -> Result<(), ComputeError> {
        let mut updates = peek_ids
            .into_iter()
            .flat_map(|uuid| {
                self.compute
                    .peeks
                    .remove(uuid)
                    .map(|peek| (peek.target, ChangeBatch::new_from(peek.time, -1)))
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

    fn handle_frontier_uppers(
        &mut self,
        list: Vec<(GlobalId, Antichain<T>)>,
        replica_id: ReplicaId,
    ) -> Option<ActiveReplicationResponse<T>> {
        let mut new_uppers = Vec::new();

        for (id, new_upper) in list {
            if let Some(reported) = self.uppers.get_mut(&id) {
                if reported.update(replica_id, new_upper) {
                    new_uppers.push((id, reported.bounds.clone()));
                }
            } else if let Some(reported) = self.index_log_uppers.get_mut(&id) {
                if reported.update(replica_id, new_upper) {
                    new_uppers.push((id, reported.bounds.clone()));
                }
            } else if let Some(reported) = self.sink_log_uppers.get_mut(&id) {
                if PartialOrder::less_than(reported, &new_upper) {
                    reported.clone_from(&new_upper);
                    new_uppers.push((
                        id,
                        FrontierBounds {
                            lower: new_upper.clone(),
                            upper: new_upper,
                        },
                    ));
                }
            }
        }

        if !new_uppers.is_empty() {
            Some(ActiveReplicationResponse::FrontierUppers(new_uppers))
        } else {
            None
        }
    }

    fn handle_peek_response(
        &mut self,
        uuid: Uuid,
        response: PeekResponse,
        otel_ctx: OpenTelemetryContext,
        replica_id: ReplicaId,
    ) -> Option<ActiveReplicationResponse<T>> {
        let peek = match self.peeks.get_mut(&uuid) {
            Some(peek) => peek,
            None => {
                tracing::warn!("did not find pending peek for {}", uuid);
                return None;
            }
        };

        // If this is the first response, forward it; otherwise do not.
        // TODO: we could collect the other responses to assert equivalence?
        // Trades resources (memory) for reassurances; idk which is best.
        //
        // NOTE: we use the `otel_ctx` from the response, not the
        // pending peek, because we currently want the parent
        // to be whatever the compute worker did with this peek. We
        // still `take` the pending peek's `otel_ctx` to mark it as
        // served.
        //
        // Additionally, we just use the `otel_ctx` from the first worker to
        // respond.
        if peek.otel_ctx.take().is_some() {
            self.pending_response
                .push_back(ActiveReplicationResponse::PeekResponse(
                    uuid, response, otel_ctx,
                ));
        }

        // Update the per-replica tracking and draw appropriate consequences.
        peek.unfinished.remove(&replica_id);
        if peek.is_finished() {
            self.pending_response
                .push_back(ActiveReplicationResponse::PeekFinished(uuid));
        }

        self.pending_response.pop_front()
    }

    fn handle_subscribe_response(
        &mut self,
        subscribe_id: GlobalId,
        response: SubscribeResponse<T>,
        replica_id: ReplicaId,
    ) -> Option<ActiveReplicationResponse<T>> {
        let entry = self.uppers.get_mut(&subscribe_id)?;

        match response {
            SubscribeResponse::Batch(SubscribeBatch {
                lower: _,
                upper,
                mut updates,
            }) => {
                // We track both the upper and the lower bound of all upper frontiers
                // reported by all replicas.
                //  * If the upper bound advances, we can emit all updates at times greater
                //    or equal to the last reported upper bound (to avoid emitting duplicate
                //    updates) as a `SubscribeResponse`.
                //  * If either the upper or the lower bound advances, we emit this
                //    information as a `FrontierUppers` response.

                let old_upper_bound = entry.bounds.upper.clone();
                if !entry.update(replica_id, upper.clone()) {
                    // There are no new updates to report.
                    return None;
                }

                if PartialOrder::less_than(&old_upper_bound, &entry.bounds.upper) {
                    // When we get here, the subscribe must still be in progress.
                    assert!(self.subscribes.get(&subscribe_id).is_some());

                    let new_lower = old_upper_bound;
                    updates.retain(|(time, _data, _diff)| new_lower.less_equal(time));
                    self.pending_response
                        .push_back(ActiveReplicationResponse::SubscribeResponse(
                            subscribe_id,
                            SubscribeResponse::Batch(SubscribeBatch {
                                lower: new_lower,
                                upper: entry.bounds.upper.clone(),
                                updates,
                            }),
                        ))
                }

                let frontier_updates = vec![(subscribe_id, entry.bounds.clone())];
                self.pending_response
                    .push_back(ActiveReplicationResponse::FrontierUppers(frontier_updates));

                if upper.is_empty() {
                    // This subscribe has finished producing all its data. Remove it from the
                    // in-progress subscribes, so we don't emit a `DroppedAt` for it.
                    self.subscribes.remove(&subscribe_id);
                }
            }
            SubscribeResponse::DroppedAt(_) => {
                // We should never get here. A replica emits `DroppedAt` only in response to a
                // subscribe being dropped by its client (via `AllowCompaction`). When we handle
                // the `AllowCompaction` command, we cease tracking the subscribe's frontier. And
                // without a tracked frontier, we return immediately at the beginning of this
                // method.
                tracing::error!("unexpected `DroppedAt` received for subscribe {subscribe_id}");
            }
        }

        self.pending_response.pop_front()
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

    /// Upper bound of write frontiers reported by all replicas.
    ///
    /// Used to determine valid times at which the collection can be read.
    write_frontier_upper: Antichain<T>,
    /// Lower bound of write frontiers reported by all replicas.
    ///
    /// Used to determine times that can be compacted.
    write_frontier_lower: Antichain<T>,
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
            write_frontier_upper: Antichain::from_elem(Timestamp::minimum()),
            write_frontier_lower: Antichain::from_elem(Timestamp::minimum()),
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
        self.write_frontier_upper.borrow()
    }
}

#[derive(Debug)]
struct PendingPeek<T> {
    /// ID of the collected targeted by this peek.
    target: GlobalId,
    /// The peek time.
    time: T,
    /// Replicas that have yet to respond to this peek.
    unfinished: BTreeSet<ReplicaId>,
    /// The OpenTelemetry context for this peek.
    ///
    /// This value is `Some` as long as we have not yet passed a response up the chain, and `None`
    /// afterwards.
    otel_ctx: Option<OpenTelemetryContext>,
}

impl<T> PendingPeek<T> {
    /// Return whether this peek is finished and can be cleaned up.
    fn is_finished(&self) -> bool {
        // If we have not yet emitted a response for the peek, the peek is not finished, even if
        // the set of replicas we are waiting for is currently empty. It might be that the cluster
        // has no replicas or all replicas have been temporarily removed for re-hydration. In this
        // case, we wait for new replicas to be added to eventually serve the peek.
        self.otel_ctx.is_none() && self.unfinished.is_empty()
    }
}

/// Configuration for `replica_task`.
struct ReplicaTaskConfig<T> {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// The network addresses of the processes in the replica.
    addrs: Vec<String>,
    /// The build information for this process.
    build_info: &'static BuildInfo,
    /// A channel upon which commands intended for the replica are delivered.
    command_rx: UnboundedReceiver<ComputeCommand<T>>,
    /// A channel upon which responses from the replica are delivered.
    response_tx: UnboundedSender<ComputeResponse<T>>,
}

/// Asynchronously forwards commands to and responses from a single replica.
async fn replica_task<T>(config: ReplicaTaskConfig<T>)
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    let replica_id = config.replica_id;
    tracing::info!("starting replica task for {replica_id}");
    match run_replica_core(config).await {
        Ok(()) => tracing::info!("gracefully stopping replica task for {replica_id}"),
        Err(e) => tracing::warn!("replica task for {replica_id} failed: {e}"),
    }
}

async fn run_replica_core<T>(
    ReplicaTaskConfig {
        replica_id,
        addrs,
        build_info,
        mut command_rx,
        response_tx,
    }: ReplicaTaskConfig<T>,
) -> Result<(), anyhow::Error>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    let mut client = Retry::default()
        .clamp_backoff(Duration::from_secs(32))
        .retry_async(|state| {
            let addrs = addrs.clone();
            let version = build_info.semver_version();
            async move {
                match ComputeGrpcClient::connect_partitioned(addrs, version).await {
                    Ok(client) => Ok(client),
                    Err(e) => {
                        tracing::warn!(
                            "error connecting to replica {replica_id}, retrying in {:?}: {e}",
                            state.next_backoff.unwrap()
                        );
                        Err(e)
                    }
                }
            }
        })
        .await
        .expect("retry retries forever");

    loop {
        select! {
            // Command from controller to forward to replica.
            command = command_rx.recv() => match command {
                None => {
                    // Controller is no longer interested in this replica. Shut
                    // down.
                    return Ok(())
                }
                Some(command) => client.send(command).await?,
            },
            // Response from replica to forward to controller.
            response = client.recv() => {
                let response = match response? {
                    None => bail!("replica unexpectedly gracefully terminated connection"),
                    Some(response) => response,
                };
                if response_tx.send(response).is_err() {
                    // Controller is no longer interested in this replica. Shut
                    // down.
                    return Ok(());
                }
            }
        }
    }
}

/// Reported upper frontiers for a single compute collection.
///
/// The type maintains the following invariants:
///   * replica frontiers only advance
///   * frontier bounds only advance
///   * `bounds.lower` <= `bounds.upper`
///   * `bounds.lower` is the lower bound of the frontiers of all active replicas
///   * `bounds.upper` is the upper bound of the frontiers of all replicas
#[derive(Debug)]
struct ReportedUppers<T> {
    /// The reported uppers per replica.
    per_replica: HashMap<ReplicaId, Antichain<T>>,
    /// The lower and upper bound of all reported uppers.
    bounds: FrontierBounds<T>,
}

impl<T> ReportedUppers<T>
where
    T: Timestamp + Lattice,
{
    /// Construct a [`ReportedUppers`] that tracks frontiers of the given replicas.
    fn new<I>(replica_ids: I) -> Self
    where
        I: IntoIterator<Item = ReplicaId>,
    {
        let per_replica = replica_ids
            .into_iter()
            .map(|id| (id, Antichain::from_elem(T::minimum())))
            .collect();

        Self {
            per_replica,
            bounds: FrontierBounds {
                lower: Antichain::from_elem(T::minimum()),
                upper: Antichain::from_elem(T::minimum()),
            },
        }
    }

    /// Start tracking the given replica.
    ///
    /// # Panics
    /// - If the given `replica_id` is already tracked.
    fn add_replica(&mut self, id: ReplicaId) {
        let previous = self.per_replica.insert(id, self.bounds.lower.clone());
        assert!(previous.is_none(), "replica already tracked");
    }

    /// Stop tracking the given replica.
    ///
    /// Returns `true` iff the update caused a change in any of the two bounds.
    ///
    /// # Panics
    /// - If the given `replica_id` is not tracked.
    fn remove_replica(&mut self, id: ReplicaId) -> bool {
        self.per_replica.remove(&id).expect("replica not tracked");

        self.update_lower_bound()
    }

    /// Return whether the given replica's frontiers are tracked.
    fn tracks_replica(&self, id: ReplicaId) -> bool {
        self.per_replica.contains_key(&id)
    }

    /// Apply a frontier update from a single replica.
    ///
    /// Returns `true` iff the update caused a change in any of the two bounds.
    ///
    /// # Panics
    /// - If the given `replica_id` is not tracked.
    fn update(&mut self, replica_id: ReplicaId, new_upper: Antichain<T>) -> bool {
        let replica_upper = self
            .per_replica
            .get_mut(&replica_id)
            .expect("replica not tracked");

        // Replica frontiers only advance.
        if PartialOrder::less_than(&new_upper, replica_upper) {
            return false;
        }

        replica_upper.clone_from(&new_upper);

        let upper_bound_changed = PartialOrder::less_than(&self.bounds.upper, &new_upper);
        if upper_bound_changed {
            self.bounds.upper = new_upper;
        }

        let lower_bound_changed = self.update_lower_bound();

        upper_bound_changed || lower_bound_changed
    }

    /// Update `bounds.lower` to restore its invariants.
    ///
    /// Returns `true` iff the update caused a change in the lower bound.
    fn update_lower_bound(&mut self) -> bool {
        // This operation is linear in the number of replicas. We could do better, but since the
        // number of replicas is expected to be small, this is fine.
        let mut new_lower_bound = self.bounds.upper.clone();
        for frontier in self.per_replica.values() {
            new_lower_bound.meet_assign(frontier);
        }

        let lower_bound_changed = PartialOrder::less_than(&self.bounds.lower, &new_lower_bound);
        if lower_bound_changed {
            self.bounds.lower = new_lower_bound;
        }

        lower_bound_changed
    }
}

/// State for a single replica.
#[derive(Debug)]
struct ReplicaState<T> {
    /// A sender for commands for the replica.
    ///
    /// If sending to this channel fails, the replica has failed and requires
    /// rehydration.
    command_tx: UnboundedSender<ComputeCommand<T>>,
    /// A receiver for responses from the replica.
    ///
    /// If receiving from the channel returns `None`, the replica has failed
    /// and requires rehydration.
    response_rx: UnboundedReceiver<ComputeResponse<T>>,
    /// A handle to the task that aborts it when the replica is dropped.
    _task: AbortOnDropHandle<()>,
    /// The network addresses of the processes that make up the replica.
    addrs: Vec<String>,
    /// The logging config specific to this replica.
    logging_config: Option<LoggingConfig>,
    /// The communication config specific to this replica.
    communication_config: CommunicationConfig,
}

impl<T> ReplicaState<T> {
    /// Specialize a command for the given `Replica` and `ReplicaId`.
    ///
    /// Most `ComputeCommand`s are independent of the target replica, but some
    /// contain replica-specific fields that must be adjusted before sending.
    fn specialize_command(&self, command: &mut ComputeCommand<T>, replica_id: ReplicaId) {
        // Set new replica ID and obtain set the sinked logs specific to this replica
        if let ComputeCommand::CreateInstance(config) = command {
            config.replica_id = replica_id;
            config.logging = self.logging_config.clone();
        }

        if let ComputeCommand::CreateTimely(comm_config) = command {
            *comm_config = self.communication_config.clone();
        }
    }
}

/// A response from the ActiveReplication client.
#[derive(Debug, Clone)]
enum ActiveReplicationResponse<T = mz_repr::Timestamp> {
    /// A list of identifiers of traces, with new lower and upper bounds of upper frontiers.
    FrontierUppers(Vec<(GlobalId, FrontierBounds<T>)>),
    /// The compute instance's response to the specified peek.
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// A notification that all replicas have finished processing the specified peek.
    ///
    /// This is different from `PeekResponse`, because we respond to a peek immediately upon seeing
    /// the first response for it. `PeekFinished` reports that it is now allowed to release any
    /// read holds installed for the peek.
    PeekFinished(Uuid),
    /// The compute instance's next response to the specified subscribe.
    SubscribeResponse(GlobalId, SubscribeResponse<T>),
    /// A notification that we heard a response from the given replica at the
    /// given time.
    ReplicaHeartbeat(ReplicaId, DateTime<Utc>),
}

#[derive(Debug, Clone)]
struct FrontierBounds<T> {
    lower: Antichain<T>,
    upper: Antichain<T>,
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! assert_bounds {
        ($uppers:expr, ($lower:expr, $upper:expr)) => {
            assert_eq!(
                $uppers.bounds.lower,
                Antichain::from_elem($lower),
                "lower mismatch"
            );
            assert_eq!(
                $uppers.bounds.upper,
                Antichain::from_elem($upper),
                "upper mismatch"
            );
        };
    }

    #[test]
    fn reported_uppers() {
        let mut uppers = ReportedUppers::<u64>::new([1, 2]);
        assert_bounds!(uppers, (0, 0));

        let changed = uppers.update(1, Antichain::from_elem(1));
        assert!(changed);
        assert_bounds!(uppers, (0, 1));

        let changed = uppers.update(2, Antichain::from_elem(2));
        assert!(changed);
        assert_bounds!(uppers, (1, 2));

        // Frontiers can only advance.
        let changed = uppers.update(2, Antichain::from_elem(1));
        assert!(!changed);
        assert_bounds!(uppers, (1, 2));
        assert_eq!(uppers.per_replica[&2], Antichain::from_elem(2));

        // Adding a replica doesn't affect current bounds.
        uppers.add_replica(3);
        assert_bounds!(uppers, (1, 2));

        let changed = uppers.update(3, Antichain::from_elem(3));
        assert!(changed);
        assert_bounds!(uppers, (1, 3));

        // Removing the slowest replica advances the lower bound.
        let changed = uppers.remove_replica(1);
        assert!(changed);
        assert_bounds!(uppers, (2, 3));

        // Removing the fastest replica doesn't affect bounds.
        let changed = uppers.remove_replica(3);
        assert!(!changed);
        assert_bounds!(uppers, (2, 3));

        // Removing the last replica advances the lower bound to the upper.
        let changed = uppers.remove_replica(2);
        assert!(changed);
        assert_bounds!(uppers, (3, 3));

        // Bounds tracking resumes correctly with new replicas.
        uppers.add_replica(4);
        uppers.add_replica(5);
        uppers.update(5, Antichain::from_elem(5));
        uppers.update(4, Antichain::from_elem(4));
        assert_bounds!(uppers, (4, 5));
    }
}
