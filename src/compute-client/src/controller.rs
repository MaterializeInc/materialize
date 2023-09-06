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
//! `create_dataflow()`. Once created, the controller holds a read capability for each output
//! collection of a dataflow, which is manipulated with `set_read_policy()`. Eventually, a
//! collection is dropped with either `drop_collections()` or by allowing compaction to the empty
//! frontier.
//!
//! Created dataflows will prevent the compaction of their inputs, including other compute
//! collections but also collections managed by the storage layer. Each dataflow input is prevented
//! from compacting beyond the allowed compaction of each of its outputs, ensuring that we can
//! recover each dataflow to its current state in case of failure or other reconfiguration.

use std::collections::BTreeMap;
use std::num::NonZeroI64;
use std::time::Duration;

use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use futures::{future, FutureExt};
use mz_build_info::BuildInfo;
use mz_cluster_client::client::ClusterReplicaLocation;
use mz_expr::RowSetFinishing;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::OpenTelemetryContext;
use mz_proto::{ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, Row};
use mz_stash::objects::proto;
use mz_storage_client::controller::{ReadPolicy, StorageController};
use mz_storage_client::types::instances::StorageInstanceId;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::{Antichain, Timestamp};
use tracing::warn;
use uuid::Uuid;

use crate::controller::error::{
    CollectionLookupError, CollectionMissing, CollectionUpdateError, DataflowCreationError,
    InstanceExists, InstanceMissing, PeekError, ReplicaCreationError, ReplicaDropError,
    SubscribeTargetError,
};
use crate::controller::instance::{ActiveInstance, Instance};
use crate::controller::replica::ReplicaConfig;
use crate::logging::{LogVariant, LoggingConfig};
use crate::metrics::ComputeControllerMetrics;
use crate::protocol::command::ComputeParameters;
use crate::protocol::response::{ComputeResponse, PeekResponse, SubscribeResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};
use crate::types::dataflows::DataflowDescription;

mod instance;
mod replica;

pub mod error;

/// Identifier of a compute instance.
pub type ComputeInstanceId = StorageInstanceId;

/// Identifier of a replica.
pub type ReplicaId = mz_cluster_client::ReplicaId;

/// Responses from the compute controller.
#[derive(Debug)]
pub enum ComputeControllerResponse<T> {
    /// See [`ComputeResponse::PeekResponse`](crate::protocol::response::ComputeResponse::PeekResponse).
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// See [`ComputeResponse::SubscribeResponse`](crate::protocol::response::ComputeResponse::SubscribeResponse).
    SubscribeResponse(GlobalId, SubscribeResponse<T>),
    /// A notification that we heard a response from the given replica at the
    /// given time.
    ReplicaHeartbeat(ReplicaId, DateTime<Utc>),
    /// A notification about dependency updates.
    DependencyUpdate {
        id: GlobalId,
        dependencies: Vec<GlobalId>,
        diff: i64,
    },
}

/// Replica configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComputeReplicaConfig {
    pub logging: ComputeReplicaLogging,
    /// The amount of effort to be spent on arrangement compaction during idle times.
    ///
    /// See [`differential_dataflow::Config::idle_merge_effort`].
    pub idle_arrangement_merge_effort: Option<u32>,
}

/// The default logging interval for [`ComputeReplicaLogging`], in number
/// of microseconds.
pub const DEFAULT_COMPUTE_REPLICA_LOGGING_INTERVAL_MICROS: u32 = 1_000_000;

/// Logging configuration of a replica.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct ComputeReplicaLogging {
    /// Whether to enable logging for the logging dataflows.
    pub log_logging: bool,
    /// The interval at which to log.
    ///
    /// A `None` value indicates that logging is disabled.
    pub interval: Option<Duration>,
}

impl ComputeReplicaLogging {
    /// Return whether logging is enabled.
    pub fn enabled(&self) -> bool {
        self.interval.is_some()
    }
}

impl RustType<proto::ReplicaLogging> for ComputeReplicaLogging {
    fn into_proto(&self) -> proto::ReplicaLogging {
        proto::ReplicaLogging {
            log_logging: self.log_logging,
            interval: self.interval.into_proto(),
        }
    }

    fn from_proto(proto: proto::ReplicaLogging) -> Result<Self, TryFromProtoError> {
        Ok(ComputeReplicaLogging {
            log_logging: proto.log_logging,
            interval: proto.interval.into_rust()?,
        })
    }
}

/// A controller for the compute layer.
pub struct ComputeController<T> {
    instances: BTreeMap<ComputeInstanceId, Instance<T>>,
    build_info: &'static BuildInfo,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
    /// Compute configuration to apply to new instances.
    config: ComputeParameters,
    /// A response to handle on the next call to `ActiveComputeController::process`.
    stashed_response: Option<(ComputeInstanceId, ReplicaId, ComputeResponse<T>)>,
    /// Times we have last received responses from replicas.
    replica_heartbeats: BTreeMap<ReplicaId, DateTime<Utc>>,
    /// A number that increases on every `environmentd` restart.
    envd_epoch: NonZeroI64,
    /// The compute controller metrics
    metrics: ComputeControllerMetrics,
}

impl<T> ComputeController<T> {
    /// Construct a new [`ComputeController`].
    pub fn new(
        build_info: &'static BuildInfo,
        envd_epoch: NonZeroI64,
        metrics_registry: MetricsRegistry,
    ) -> Self {
        Self {
            instances: BTreeMap::new(),
            build_info,
            initialized: false,
            config: Default::default(),
            stashed_response: None,
            replica_heartbeats: BTreeMap::new(),
            envd_epoch,
            metrics: ComputeControllerMetrics::new(metrics_registry),
        }
    }

    pub fn instance_exists(&self, id: ComputeInstanceId) -> bool {
        self.instances.contains_key(&id)
    }

    /// Return a reference to the indicated compute instance.
    fn instance(&self, id: ComputeInstanceId) -> Result<&Instance<T>, InstanceMissing> {
        self.instances.get(&id).ok_or(InstanceMissing(id))
    }

    /// Return a mutable reference to the indicated compute instance.
    fn instance_mut(&mut self, id: ComputeInstanceId) -> Result<&mut Instance<T>, InstanceMissing> {
        self.instances.get_mut(&id).ok_or(InstanceMissing(id))
    }

    /// Return a read-only handle to the indicated compute instance.
    pub fn instance_ref(
        &self,
        id: ComputeInstanceId,
    ) -> Result<ComputeInstanceRef<T>, InstanceMissing> {
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
    ) -> Result<&CollectionState<T>, CollectionLookupError> {
        let collection = self.instance(instance_id)?.collection(collection_id)?;
        Ok(collection)
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
    T: Clone,
{
    /// Returns the write frontier for each collection installed on each replica.
    pub fn replica_write_frontiers(&self) -> BTreeMap<(GlobalId, ReplicaId), Antichain<T>> {
        let mut result = BTreeMap::new();
        let collections = self.instances.values().flat_map(|i| i.collections_iter());
        for (&collection_id, collection) in collections {
            for (&replica_id, frontier) in &collection.replica_write_frontiers {
                result.insert((collection_id, replica_id), frontier.clone());
            }
        }
        result
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
        variable_length_row_encoding: bool,
    ) -> Result<(), InstanceExists> {
        if self.instances.contains_key(&id) {
            return Err(InstanceExists(id));
        }

        self.instances.insert(
            id,
            Instance::new(
                self.build_info,
                arranged_logs,
                self.envd_epoch,
                self.metrics.for_instance(id),
                variable_length_row_encoding,
            ),
        );

        let instance = self.instances.get_mut(&id).expect("instance just added");
        if self.initialized {
            instance.initialization_complete();
        }

        let config_params = self.config.clone();
        instance.update_configuration(config_params);

        Ok(())
    }

    /// Remove a compute instance.
    ///
    /// # Panics
    ///
    /// Panics if the identified `instance` still has active replicas.
    pub fn drop_instance(&mut self, id: ComputeInstanceId) {
        if let Some(compute_state) = self.instances.remove(&id) {
            compute_state.drop();
        }
    }

    /// Update compute configuration.
    pub fn update_configuration(&mut self, config_params: ComputeParameters) {
        for instance in self.instances.values_mut() {
            instance.update_configuration(config_params.clone());
        }

        self.config.update(config_params);
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
        match result {
            Ok((replica_id, resp)) => {
                self.replica_heartbeats.insert(replica_id, Utc::now());
                self.stashed_response = Some((instance_id, replica_id, resp));
            }
            Err(_) => {
                // There is nothing to do here. `recv` has already added the failed replica to
                // `instance.failed_replicas`, so it will be rehydrated in the next call to
                // `ActiveComputeController::process`.
            }
        }
    }

    /// Assign a target replica to the identified subscribe.
    ///
    /// If a subscribe has a target replica assigned, only subscribe responses
    /// sent by that replica are considered.
    pub fn set_subscribe_target_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        subscribe_id: GlobalId,
        target_replica: ReplicaId,
    ) -> Result<(), SubscribeTargetError> {
        self.instance_mut(instance_id)?
            .set_subscribe_target_replica(subscribe_id, target_replica)?;
        Ok(())
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
    ) -> Result<&CollectionState<T>, CollectionLookupError> {
        self.compute.collection(instance_id, collection_id)
    }

    /// Return a handle to the indicated compute instance.
    fn instance(&mut self, id: ComputeInstanceId) -> Result<ActiveInstance<T>, InstanceMissing> {
        self.compute
            .instance_mut(id)
            .map(|c| c.activate(self.storage))
    }
}

/// Default value for `idle_arrangement_merge_effort` if none is supplied.
// TODO(#16906): Test if 1000 is a good default.
const DEFAULT_IDLE_ARRANGEMENT_MERGE_EFFORT: u32 = 1000;

impl<T> ActiveComputeController<'_, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Adds replicas of an instance.
    pub fn add_replica_to_instance(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        location: ClusterReplicaLocation,
        config: ComputeReplicaConfig,
    ) -> Result<(), ReplicaCreationError> {
        let (enable_logging, interval) = match config.logging.interval {
            Some(interval) => (true, interval),
            None => (false, Duration::from_secs(1)),
        };

        let idle_arrangement_merge_effort = config
            .idle_arrangement_merge_effort
            .unwrap_or(DEFAULT_IDLE_ARRANGEMENT_MERGE_EFFORT);

        let replica_config = ReplicaConfig {
            location,
            logging: LoggingConfig {
                interval,
                enable_logging,
                log_logging: config.logging.log_logging,
                index_logs: Default::default(),
            },
            idle_arrangement_merge_effort,
            grpc_client: self.compute.config.grpc_client.clone(),
        };

        self.instance(instance_id)?
            .add_replica(replica_id, replica_config)?;
        Ok(())
    }

    /// Removes a replica from an instance, including its service in the orchestrator.
    pub fn drop_replica(
        &mut self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
    ) -> Result<(), ReplicaDropError> {
        self.instance(instance_id)?.remove_replica(replica_id)?;
        Ok(())
    }

    /// Create and maintain the described dataflows, and initialize state for their output.
    ///
    /// This method creates dataflows whose inputs are still readable at the dataflow `as_of`
    /// frontier, and initializes the outputs as readable from that frontier onward.
    /// It installs read dependencies from the outputs to the inputs, so that the input read
    /// capabilities will be held back to the output read capabilities, ensuring that we are
    /// always able to return to a state that can serve the output read capabilities.
    pub fn create_dataflow(
        &mut self,
        instance_id: ComputeInstanceId,
        dataflow: DataflowDescription<crate::plan::Plan<T>, (), T>,
    ) -> Result<(), DataflowCreationError> {
        self.instance(instance_id)?.create_dataflow(dataflow)?;
        Ok(())
    }

    /// Drop the read capability for the given collections and allow their resources to be
    /// reclaimed.
    pub fn drop_collections(
        &mut self,
        instance_id: ComputeInstanceId,
        collection_ids: Vec<GlobalId>,
    ) -> Result<(), CollectionUpdateError> {
        self.instance(instance_id)?
            .drop_collections(collection_ids)?;
        Ok(())
    }

    /// Initiate a peek request for the contents of the given collection at `timestamp`.
    pub fn peek(
        &mut self,
        instance_id: ComputeInstanceId,
        collection_id: GlobalId,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<ReplicaId>,
    ) -> Result<(), PeekError> {
        self.instance(instance_id)?.peek(
            collection_id,
            literal_constraints,
            uuid,
            timestamp,
            finishing,
            map_filter_project,
            target_replica,
        )?;
        Ok(())
    }

    /// Cancel an existing peek request.
    ///
    /// Canceling a peek is best effort. The caller may see any of the following
    /// after canceling a peek request:
    ///
    ///   * A `PeekResponse::Rows` indicating that the cancellation request did
    ///     not take effect in time and the query succeeded.
    ///   * A `PeekResponse::Canceled` affirming that the peek was canceled.
    ///   * No `PeekResponse` at all.
    pub fn cancel_peek(
        &mut self,
        instance_id: ComputeInstanceId,
        uuid: Uuid,
    ) -> Result<(), InstanceMissing> {
        self.instance(instance_id)?.cancel_peek(uuid);
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
    pub fn set_read_policy(
        &mut self,
        instance_id: ComputeInstanceId,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), CollectionUpdateError> {
        self.instance(instance_id)?.set_read_policy(policies)?;
        Ok(())
    }

    /// Processes the work queued by [`ComputeController::ready`].
    pub fn process(&mut self) -> Option<ComputeControllerResponse<T>> {
        // Update controller state metrics.
        for instance in self.compute.instances.values_mut() {
            instance.refresh_state_metrics();
        }

        // Rehydrate any failed replicas.
        for instance in self.compute.instances.values_mut() {
            instance.activate(self.storage).rehydrate_failed_replicas();
        }

        // Process pending ready responses.
        for instance in self.compute.instances.values_mut() {
            if let Some(response) = instance.ready_responses.pop_front() {
                return Some(response);
            }
        }

        // Process pending replica heartbeats.
        if let Some((replica_id, when)) = self.compute.replica_heartbeats.pop_first() {
            return Some(ComputeControllerResponse::ReplicaHeartbeat(
                replica_id, when,
            ));
        }

        // Process pending responses from replicas.
        if let Some((instance_id, replica_id, response)) = self.compute.stashed_response.take() {
            if let Ok(mut instance) = self.instance(instance_id) {
                return instance.handle_response(response, replica_id);
            } else {
                warn!(
                    ?instance_id,
                    ?response,
                    "processed response from unknown instance"
                );
            };
        }

        None
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
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, CollectionMissing> {
        self.instance.collection(id)
    }

    /// Return an iterator over the installed collections.
    pub fn collections(&self) -> impl Iterator<Item = (&GlobalId, &CollectionState<T>)> {
        self.instance.collections_iter()
    }
}

/// State maintained about individual compute collections.
///
/// A compute collection is either an index, or a storage sink, or a subscribe, exported by a
/// compute dataflow.
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
