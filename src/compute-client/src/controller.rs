// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller that provides an interface to a compute instance, and the storage layer below it.
//!
//! The compute controller curates the creation of indexes and sinks, the progress of readers through
//! these collections, and their eventual dropping and resource reclamation.
//!
//! The compute controller can be viewed as a partial map from `GlobalId` to collection. It is an error to
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
use std::fmt::{self, Debug};

use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_expr::RowSetFinishing;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_types::Codec64;
use mz_repr::{GlobalId, Row};
use mz_storage::controller::{ReadPolicy, StorageController, StorageError};
use mz_storage::types::sinks::{PersistSinkConnection, SinkConnection, SinkDesc};

use crate::command::{
    ComputeCommand, DataflowDescription, InstanceConfig, Peek, ReplicaId, SourceInstanceDesc,
};
use crate::controller::replicated::{ActiveReplication, ActiveReplicationResponse};
use crate::logging::{LogVariant, LoggingConfig};
use crate::response::{ComputeResponse, PeekResponse, TailBatch, TailResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};

pub mod replicated;

/// An abstraction allowing us to name different compute instances.
pub type ComputeInstanceId = u64;

/// Controller state maintained for each compute instance.
#[derive(Debug)]
pub struct ComputeControllerState<T> {
    /// The replicas of this compute instance.
    pub replicas: ActiveReplication<T>,
    /// Tracks expressed `since` and received `upper` frontiers for indexes and sinks.
    pub collections: BTreeMap<GlobalId, CollectionState<T>>,
    /// Currently outstanding peeks: identifiers and timestamps.
    pub peeks: BTreeMap<uuid::Uuid, (GlobalId, T)>,
    /// A response to handle on the next call to `ComputeControllerMut::process`.
    stashed_response: Option<ActiveReplicationResponse<T>>,
}

/// An immutable controller for a compute instance.
#[derive(Debug, Copy, Clone)]
pub struct ComputeController<'a, T> {
    pub instance: ComputeInstanceId,
    pub compute: &'a ComputeControllerState<T>,
    pub storage_controller: &'a dyn StorageController<Timestamp = T>,
}

/// A mutable controller for a compute instance.
#[derive(Debug)]
pub struct ComputeControllerMut<'a, T> {
    pub instance: ComputeInstanceId,
    pub compute: &'a mut ComputeControllerState<T>,
    pub storage_controller: &'a mut dyn StorageController<Timestamp = T>,
}

/// Responses from a compute instance controller.
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

impl<T> ComputeControllerState<T>
where
    T: Timestamp + Lattice + Debug + Copy,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub async fn new(build_info: &'static BuildInfo, logging: &Option<LoggingConfig>) -> Self {
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
        }));

        Self {
            replicas,
            collections,
            peeks: Default::default(),
            stashed_response: None,
        }
    }

    /// Waits until the controller is ready to process a response.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the owner should call
    /// [`ComputeControllerMut::process`] to process the ready message.
    ///
    /// This method is cancellation safe.
    pub async fn ready(&mut self) {
        if self.stashed_response.is_none() {
            self.stashed_response = Some(self.replicas.recv().await);
        }
    }
}

// Public interface
impl<'a, T> ComputeController<'a, T>
where
    T: Timestamp + Lattice,
{
    /// Returns this controller's compute instance ID.
    pub fn instance_id(&self) -> ComputeInstanceId {
        self.instance
    }

    /// Acquires an immutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage(&self) -> &dyn StorageController<Timestamp = T> {
        self.storage_controller
    }

    /// Acquire a handle to the collection state associated with `id`.
    pub fn collection(&self, id: GlobalId) -> Result<&'a CollectionState<T>, ComputeError> {
        self.compute
            .collections
            .get(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }
}

impl<'a, T> ComputeControllerMut<'a, T>
where
    T: Timestamp + Lattice + Codec64 + Debug,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Constructs an immutable handle from this mutable handle.
    pub fn as_ref<'b>(&'b self) -> ComputeController<'b, T> {
        ComputeController {
            instance: self.instance,
            storage_controller: self.storage_controller,
            compute: &self.compute,
        }
    }

    /// Acquires a mutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage_mut(&mut self) -> &mut dyn StorageController<Timestamp = T> {
        self.storage_controller
    }

    /// Adds a new instance replica, by name.
    pub fn add_replica(
        &mut self,
        id: ReplicaId,
        addrs: Vec<String>,
        persisted_logs: HashMap<LogVariant, GlobalId>,
    ) {
        // Create ComputeState entries in ComputeController
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

    pub fn get_replica_ids(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.compute.replicas.get_replica_ids()
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
                if !(<_ as timely::order::PartialOrder>::less_equal(since, &as_of.borrow())) {
                    Err(ComputeError::DataflowSinceViolation(*source_id))?;
                }

                storage_dependencies.push(*source_id);
            }

            // Validate indexes have `since.less_equal(as_of)`.
            // TODO(mcsherry): Instead, return an error from the constructing method.
            for index_id in dataflow.index_imports.keys() {
                let collection = self.as_ref().collection(*index_id)?;
                let since = collection.read_capabilities.frontier();
                if !(<_ as timely::order::PartialOrder>::less_equal(&since, &as_of.borrow())) {
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
                    SinkConnection::Persist(conn) => {
                        let metadata = self
                            .storage_controller
                            .collection(id)?
                            .collection_metadata
                            .clone();
                        let conn = PersistSinkConnection {
                            value_desc: conn.value_desc,
                            storage_metadata: metadata,
                        };
                        SinkConnection::Persist(conn)
                    }
                    SinkConnection::Kafka(conn) => SinkConnection::Kafka(conn),
                    SinkConnection::Tail(conn) => SinkConnection::Tail(conn),
                };
                let desc = SinkDesc {
                    from: se.from,
                    from_desc: se.from_desc,
                    connection,
                    envelope: se.envelope,
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
                debug_name: d.debug_name,
                id: d.id,
            });
        }

        self.compute
            .replicas
            .send(ComputeCommand::CreateDataflows(augmented_dataflows));

        Ok(())
    }
    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    pub async fn drop_sinks(&mut self, identifiers: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.as_ref().validate_ids(identifiers.iter().cloned())?;

        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction(compaction_commands).await?;
        Ok(())
    }
    /// Drops the read capability for the sinks and allows their resources to be reclaimed.
    /// TODO(jkosh44): This method does not validate the provided identifiers. Currently when the
    ///     controller starts/restarts it has no durable state. That means that it has no way of
    ///     remembering any past commands sent. In the future we plan on persisting state for the
    ///     controller so that it is aware of past commands.
    ///     Therefore this method is for dropping sinks that we know to have been previously
    ///     created, but have been forgotten by the controller due to a restart.
    ///     Once command history becomes durable we can remove this method and use the normal
    ///     `drop_sinks`.
    pub async fn drop_sinks_unvalidated(
        &mut self,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), ComputeError> {
        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction_unvalidated(compaction_commands)
            .await?;
        Ok(())
    }
    /// Drops the read capability for the indexes and allows their resources to be reclaimed.
    pub async fn drop_indexes(&mut self, identifiers: Vec<GlobalId>) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.as_ref().validate_ids(identifiers.iter().cloned())?;

        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction(compaction_commands).await?;
        Ok(())
    }
    /// Drops the read capability for the indexes and allows their resources to be reclaimed.
    /// TODO(jkosh44): This method does not validate the provided identifiers. Currently when the
    ///     controller starts/restarts it has no durable state. That means that it has no way of
    ///     remembering any past commands sent. In the future we plan on persisting state for the
    ///     controller so that it is aware of past commands.
    ///     Therefore this method is for dropping indexes that we know to have been previously
    ///     created, but have been forgotten by the controller due to a restart.
    ///     Once command history becomes durable we can remove this method and use the normal
    ///     `drop_indexes`.
    pub async fn drop_indexes_unvalidated(
        &mut self,
        identifiers: Vec<GlobalId>,
    ) -> Result<(), ComputeError> {
        let compaction_commands = identifiers
            .into_iter()
            .map(|id| (id, Antichain::new()))
            .collect();
        self.allow_compaction_unvalidated(compaction_commands)
            .await?;
        Ok(())
    }
    /// Initiate a peek request for the contents of `id` at `timestamp`.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn peek(
        &mut self,
        id: GlobalId,
        key: Option<Row>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<ReplicaId>,
    ) -> Result<(), ComputeError> {
        let since = self.as_ref().collection(id)?.read_capabilities.frontier();

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
            key,
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

    /// Downgrade the read capabilities of specific identifiers to specific frontiers.
    ///
    /// Downgrading any read capability to the empty frontier will drop the item and eventually reclaim its resources.
    #[tracing::instrument(level = "debug", skip(self))]
    async fn allow_compaction(
        &mut self,
        frontiers: Vec<(GlobalId, Antichain<T>)>,
    ) -> Result<(), ComputeError> {
        // Validate that the ids exist.
        self.as_ref()
            .validate_ids(frontiers.iter().map(|(id, _)| *id))?;
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
            if let Ok(collection) = self.collection_mut(id) {
                let mut new_read_capability = policy.frontier(collection.write_frontier.frontier());

                if <_ as timely::order::PartialOrder>::less_equal(
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

    /// Processes the work queued by [`ComputeControllerState::ready`].
    ///
    /// This method is guaranteed to return "quickly" unless doing so would
    /// compromise the correctness of the system.
    ///
    /// This method is **not** guaranteed to be cancellation safe. It **must**
    /// be awaited to completion.
    pub async fn process(&mut self) -> Result<Option<ComputeControllerResponse<T>>, anyhow::Error> {
        match self.compute.stashed_response.take() {
            None => Ok(None),
            Some(ActiveReplicationResponse::ComputeResponse(response)) => match response {
                ComputeResponse::FrontierUppers(updates) => {
                    self.update_write_frontiers(&updates).await?;
                    Ok(None)
                }
                ComputeResponse::PeekResponse(uuid, peek_response, otel_ctx) => {
                    self.remove_peeks(std::iter::once(uuid)).await?;
                    Ok(Some(ComputeControllerResponse::PeekResponse(
                        uuid,
                        peek_response,
                        otel_ctx,
                    )))
                }
                ComputeResponse::TailResponse(global_id, response) => {
                    let mut changes = timely::progress::ChangeBatch::new();
                    match &response {
                        TailResponse::Batch(TailBatch { lower, upper, .. }) => {
                            changes.extend(upper.iter().map(|time| (time.clone(), 1)));
                            changes.extend(lower.iter().map(|time| (time.clone(), -1)));
                        }
                        TailResponse::DroppedAt(frontier) => {
                            // The tail will not be written to again, but we should not confuse that
                            // with the source of the TAIL being complete through this time.
                            changes.extend(frontier.iter().map(|time| (time.clone(), -1)));
                        }
                    }
                    self.update_write_frontiers(&[(global_id, changes)]).await?;
                    Ok(Some(ComputeControllerResponse::TailResponse(
                        global_id, response,
                    )))
                }
            },
            Some(ActiveReplicationResponse::ReplicaHeartbeat(replica_id, when)) => Ok(Some(
                ComputeControllerResponse::ReplicaHeartbeat(replica_id, when),
            )),
        }
    }
}

// Internal interface
impl<'a, T> ComputeController<'a, T>
where
    T: Timestamp + Lattice,
{
    /// Validate that a collection exists for all identifiers, and error if any do not.
    pub fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), ComputeError> {
        for id in ids {
            self.collection(id)?;
        }
        Ok(())
    }
}

impl<'a, T> ComputeControllerMut<'a, T>
where
    T: Timestamp + Lattice + Codec64,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Marks the end of any initialization commands.
    ///
    /// Intended to be called by `Controller`, rather than by other code (to avoid repeated calls).
    pub fn initialization_complete(&mut self) {
        self.compute
            .replicas
            .send(ComputeCommand::InitializationComplete);
    }

    /// Acquire a mutable reference to the collection state, should it exist.
    pub fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<T>, ComputeError> {
        self.compute
            .collections
            .get_mut(&id)
            .ok_or(ComputeError::IdentifierMissing(id))
    }

    /// Accept write frontier updates from the compute layer.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn update_write_frontiers(
        &mut self,
        updates: &[(GlobalId, ChangeBatch<T>)],
    ) -> Result<(), ComputeError> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, changes) in updates.iter() {
            let collection = self
                .collection_mut(*id)
                .expect("Reference to absent collection");

            collection
                .write_frontier
                .update_iter(changes.clone().drain());

            let mut new_read_capability = collection
                .read_policy
                .frontier(collection.write_frontier.frontier());
            if <_ as timely::order::PartialOrder>::less_equal(
                &collection.implied_capability,
                &new_read_capability,
            ) {
                // TODO: reuse change batch above?
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
            .filter(|(id, _)| self.storage_mut().collection(*id).is_ok())
            .cloned()
            .collect();
        self.storage_mut()
            .update_write_frontiers(&storage_updates)
            .await?;

        Ok(())
    }

    /// Applies `updates`, propagates consequences through other read capabilities, and sends an appropriate compaction command.
    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn update_read_capabilities(
        &mut self,
        updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>,
    ) -> Result<(), ComputeError> {
        // Locations to record consequences that we need to act on.
        let mut storage_todo = BTreeMap::default();
        let mut compute_net = Vec::default();
        // Repeatedly extract the maximum id, and updates for it.
        while let Some(key) = updates.keys().rev().next().cloned() {
            let mut update = updates.remove(&key).unwrap();
            if let Ok(collection) = self.collection_mut(key) {
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
                    .as_ref()
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
    pub async fn remove_peeks(
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
}

/// State maintained about individual collections.
#[derive(Debug)]
pub struct CollectionState<T> {
    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation will always contain `self.implied_capability`, but may also contain
    /// capabilities held by others who have read dependencies on this collection.
    pub read_capabilities: MutableAntichain<T>,
    /// The implicit capability associated with collection creation.
    pub implied_capability: Antichain<T>,
    /// The policy to use to downgrade `self.implied_capability`.
    pub read_policy: ReadPolicy<T>,

    /// Storage identifiers on which this collection depends.
    pub storage_dependencies: Vec<GlobalId>,
    /// Compute identifiers on which this collection depends.
    pub compute_dependencies: Vec<GlobalId>,

    /// Reported progress in the write capabilities.
    ///
    /// Importantly, this is not a write capability, but what we have heard about the
    /// write capabilities of others. All future writes will have times greater than or
    /// equal to `write_frontier.frontier()`.
    pub write_frontier: MutableAntichain<T>,
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
            write_frontier: MutableAntichain::new_bottom(Timestamp::minimum()),
        }
    }

    /// Reports the current read capability.
    pub fn read_capability(&self) -> &Antichain<T> {
        &self.implied_capability
    }
}
