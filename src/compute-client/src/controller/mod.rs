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

use std::collections::{BTreeSet, HashMap};
use std::error::Error;
use std::fmt::{self, Debug};

use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_expr::RowSetFinishing;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Row};
use mz_storage::controller::{ReadPolicy, StorageController, StorageError};

use crate::command::{DataflowDescription, ReplicaId};
use crate::logging::{LogVariant, LoggingConfig};
use crate::response::{ComputeResponse, PeekResponse, TailBatch, TailResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};

use self::durable::{ActiveDurableState, DurableState};
use self::replicated::ActiveReplicationResponse;

mod durable;
mod replicated;

/// An abstraction allowing us to name different compute instances.
pub type ComputeInstanceId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ComputeSinkId {
    pub compute_instance: ComputeInstanceId,
    pub global_id: GlobalId,
}

/// A controller for a compute instance.
#[derive(Debug)]
pub struct ComputeController<T> {
    /// The ID of the compute instance.
    instance_id: ComputeInstanceId,
    /// Durable compute controller state.
    durable: DurableState<T>,
    /// A response to handle on the next call to `ActiveComputeController::process`.
    stashed_response: Option<ActiveReplicationResponse<T>>,
}

/// A wrapper around [`ComputeController`] with a live storage controller.
#[derive(Debug)]
pub struct ActiveComputeController<'a, T> {
    compute: &'a mut ComputeController<T>,
    storage_controller: &'a mut dyn StorageController<Timestamp = T>,
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

// Public interface

impl<T> ComputeController<T> {
    /// Returns this controller's compute instance ID.
    pub fn instance_id(&self) -> ComputeInstanceId {
        self.instance_id
    }

    /// Acquire a handle to the collection state associated with `id`.
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
        self.durable.collection(id)
    }

    /// Acquire an [`ActiveComputeController`] by providing a storage controller.
    pub fn activate<'a>(
        &'a mut self,
        storage_controller: &'a mut dyn StorageController<Timestamp = T>,
    ) -> ActiveComputeController<'a, T> {
        ActiveComputeController {
            compute: self,
            storage_controller,
        }
    }
}

impl<T> ComputeController<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub fn new(
        instance_id: ComputeInstanceId,
        build_info: &'static BuildInfo,
        logging: &Option<LoggingConfig>,
    ) -> Self {
        Self {
            instance_id,
            durable: DurableState::new(build_info, logging),
            stashed_response: None,
        }
    }

    /// Marks the end of any initialization commands.
    ///
    /// Intended to be called by `Controller`, rather than by other code (to avoid repeated calls).
    pub fn initialization_complete(&mut self) {
        self.durable.initialization_complete();
    }

    /// Waits until the controller is ready to process a response.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the owner should call
    /// [`ActiveComputeController::process`] to process the ready message.
    ///
    /// This method is cancellation safe.
    pub async fn ready(&mut self) {
        if self.stashed_response.is_none() {
            self.stashed_response = Some(self.durable.receive_response().await);
        }
    }

    /// Drop this compute instance.
    ///
    /// # Panics
    /// - If the compute instance still has active replicas.
    pub fn drop(self) {
        self.durable.drop();
    }
}

impl<'a, T> ActiveComputeController<'a, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Acquire a handle to the collection state associated with `id`.
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
        self.compute.collection(id)
    }

    /// Adds a new instance replica, by ID.
    pub fn add_replica(
        &mut self,
        id: ReplicaId,
        addrs: Vec<String>,
        persisted_logs: HashMap<LogVariant, GlobalId>,
    ) {
        self.active_durable_state()
            .add_replica(id, addrs, persisted_logs)
    }

    /// Removes an existing instance replica, by ID.
    pub fn remove_replica(&mut self, id: ReplicaId) {
        self.active_durable_state().remove_replica(id)
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
        self.active_durable_state()
            .create_dataflows(dataflows)
            .await
    }

    /// Drops the read capability for the given collections and allows their resources to be
    /// reclaimed.
    pub async fn drop_collections(&mut self, ids: Vec<GlobalId>) -> Result<(), ComputeError> {
        self.active_durable_state().drop_collections(ids).await
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
        self.active_durable_state()
            .peek(
                id,
                literal_constraints,
                uuid,
                timestamp,
                finishing,
                map_filter_project,
                target_replica,
            )
            .await
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
        self.active_durable_state().cancel_peeks(uuids).await
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
        self.active_durable_state().set_read_policy(policies).await
    }

    /// Processes the work queued by [`ComputeController::ready`].
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
                    self.active_durable_state()
                        .update_write_frontiers(&updates)
                        .await?;
                    Ok(None)
                }
                ComputeResponse::PeekResponse(uuid, peek_response, otel_ctx) => {
                    self.active_durable_state()
                        .remove_peeks(std::iter::once(uuid))
                        .await?;
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
                            assert_eq!(lower, &self.compute.collection(global_id)?.write_frontier);

                            upper.clone()
                        }
                        // The tail will not be written to again, but we should not confuse that
                        // with the source of the TAIL being complete through this time.
                        TailResponse::DroppedAt(_) => Antichain::new(),
                    };

                    self.active_durable_state()
                        .update_write_frontiers(&[(global_id, new_upper)])
                        .await?;

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

impl<T> ActiveComputeController<'_, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    fn active_durable_state(&mut self) -> ActiveDurableState<T> {
        self.compute.durable.activate(self.storage_controller)
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
