// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller for a compute instance.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::num::NonZeroI64;
use std::time::Instant;

use differential_dataflow::lattice::Lattice;
use futures::stream::FuturesUnordered;
use futures::{future, StreamExt};
use mz_build_info::BuildInfo;
use mz_cluster_client::client::ClusterStartupEpoch;
use mz_expr::RowSetFinishing;
use mz_ore::cast::CastFrom;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Row};
use mz_storage_client::controller::{ReadPolicy, StorageController};
use thiserror::Error;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use timely::PartialOrder;
use uuid::Uuid;

use crate::controller::error::CollectionMissing;
use crate::controller::replica::{Replica, ReplicaConfig};
use crate::controller::{CollectionState, ComputeControllerResponse, ReplicaId};
use crate::logging::LogVariant;
use crate::metrics::InstanceMetrics;
use crate::metrics::UIntGauge;
use crate::protocol::command::{ComputeCommand, ComputeParameters, Peek};
use crate::protocol::history::ComputeCommandHistory;
use crate::protocol::response::{ComputeResponse, PeekResponse, SubscribeBatch, SubscribeResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};
use crate::types::dataflows::DataflowDescription;
use crate::types::sinks::{ComputeSinkConnection, ComputeSinkDesc, PersistSinkConnection};
use crate::types::sources::SourceInstanceDesc;

#[derive(Error, Debug)]
#[error("replica exists already: {0}")]
pub(super) struct ReplicaExists(pub ReplicaId);

#[derive(Error, Debug)]
#[error("replica does not exist: {0}")]
pub(super) struct ReplicaMissing(pub ReplicaId);

#[derive(Error, Debug)]
pub(super) enum DataflowCreationError {
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    #[error("dataflow definition lacks an as_of value")]
    MissingAsOf,
    #[error("dataflow has an as_of not beyond the since of collection: {0}")]
    SinceViolation(GlobalId),
}

impl From<CollectionMissing> for DataflowCreationError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

#[derive(Error, Debug)]
pub(super) enum PeekError {
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    #[error("peek timestamp is not beyond the since of collection: {0}")]
    SinceViolation(GlobalId),
}

impl From<CollectionMissing> for PeekError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

#[derive(Error, Debug)]
pub(super) enum SubscribeTargetError {
    #[error("subscribe does not exist: {0}")]
    SubscribeMissing(GlobalId),
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    #[error("subscribe has already produced output")]
    SubscribeAlreadyStarted,
}

/// The state we keep for a compute instance.
#[derive(Debug)]
pub(super) struct Instance<T> {
    /// Build info for spawning replicas
    build_info: &'static BuildInfo,
    /// Whether instance initialization has been completed.
    initialized: bool,
    /// The replicas of this compute instance.
    replicas: BTreeMap<ReplicaId, Replica<T>>,
    /// Currently installed compute collections.
    ///
    /// New entries are added for all collections exported from dataflows created through
    /// [`ActiveInstance::create_dataflows`].
    ///
    /// Entries are removed when two conditions are fulfilled:
    ///
    ///  * The collection's read frontier has advanced to the empty frontier, implying that
    ///    [`ActiveInstance::drop_collections`] was called.
    ///  * All replicas have reported the empty frontier for the collection, implying that they
    ///    have stopped reading from the collection's inputs.
    ///
    /// Only if both these conditions hold is dropping a collection's state, and the associated
    /// read holds on its inputs, sound.
    collections: BTreeMap<GlobalId, CollectionState<T>>,
    /// IDs of log sources maintained by this compute instance.
    log_sources: BTreeMap<LogVariant, GlobalId>,
    /// Currently outstanding peeks.
    ///
    /// New entries are added for all peeks initiated through [`ActiveInstance::peek`].
    ///
    /// The entry for a peek is only removed once all replicas have responded to the peek. This is
    /// currently required to ensure all replicas have stopped reading from the peeked collection's
    /// inputs before we allow them to compact. #16641 tracks changing this so we only have to wait
    /// for the first peek response.
    peeks: BTreeMap<Uuid, PendingPeek<T>>,
    /// Currently in-progress subscribes.
    ///
    /// New entries are added for all subscribes exported from dataflows created through
    /// [`ActiveInstance::create_dataflows`].
    ///
    /// The entry for a subscribe is removed once at least one replica has reported the subscribe
    /// to have advanced to the empty frontier or to have been dropped, implying that no further
    /// updates will be emitted for this subscribe.
    ///
    /// Note that subscribes are tracked both in `collections` and `subscribes`. `collections`
    /// keeps track of the subscribe's upper and since frontiers and ensures appropriate read holds
    /// on the subscribe's input. `subscribes` is only used to track which updates have been
    /// emitted, to decide if new ones should be emitted or suppressed.
    subscribes: BTreeMap<GlobalId, ActiveSubscribe<T>>,
    /// The command history, used when introducing new replicas or restarting existing replicas.
    history: ComputeCommandHistory<UIntGauge, T>,
    /// IDs of replicas that have failed and require rehydration.
    failed_replicas: BTreeSet<ReplicaId>,
    /// Ready compute controller responses to be delivered.
    pub ready_responses: VecDeque<ComputeControllerResponse<T>>,
    /// A number that increases with each restart of `environmentd`.
    envd_epoch: NonZeroI64,
    /// Numbers that increase with each restart of a replica.
    replica_epochs: BTreeMap<ReplicaId, u64>,
    /// The registry the controller uses to report metrics.
    metrics: InstanceMetrics,
}

impl<T> Instance<T> {
    /// Acquire a handle to the collection state associated with `id`.
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, CollectionMissing> {
        self.collections.get(&id).ok_or(CollectionMissing(id))
    }

    /// Acquire a mutable handle to the collection state associated with `id`.
    fn collection_mut(
        &mut self,
        id: GlobalId,
    ) -> Result<&mut CollectionState<T>, CollectionMissing> {
        self.collections.get_mut(&id).ok_or(CollectionMissing(id))
    }

    pub fn collections_iter(&self) -> impl Iterator<Item = (&GlobalId, &CollectionState<T>)> {
        self.collections.iter()
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

    /// Return whether this instance has any processing work scheduled.
    pub fn wants_processing(&self) -> bool {
        // Do we need to rehydrate failed replicas?
        !self.failed_replicas.is_empty()
        // Do we have responses ready to deliver?
        || !self.ready_responses.is_empty()
    }

    /// Returns whether the identified replica exists.
    pub fn replica_exists(&self, id: ReplicaId) -> bool {
        self.replicas.contains_key(&id)
    }

    /// Returns the ids of all replicas of this instance.
    pub fn replica_ids(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.replicas.keys().copied()
    }

    /// Return the IDs of in-progress subscribes targeting the specified replica.
    fn subscribes_targeting(&self, replica_id: ReplicaId) -> impl Iterator<Item = GlobalId> + '_ {
        self.subscribes.iter().filter_map(move |(id, subscribe)| {
            let targeting = subscribe.target_replica == Some(replica_id);
            targeting.then_some(*id)
        })
    }

    /// Refresh the controller state metrics for this instance.
    ///
    /// We could also do state metric updates directly in response to state changes, but that would
    /// mean littering the code with metric update calls. Encapsulating state metric maintenance in
    /// a single method is less noisy.
    ///
    /// This method is invoked by `ActiveComputeController::process`, which we expect to
    /// be periodically called during normal operation.
    pub(super) fn refresh_state_metrics(&self) {
        self.metrics
            .replica_count
            .set(u64::cast_from(self.replicas.len()));
        self.metrics
            .collection_count
            .set(u64::cast_from(self.collections.len()));
        self.metrics
            .peek_count
            .set(u64::cast_from(self.peeks.len()));
        self.metrics
            .subscribe_count
            .set(u64::cast_from(self.subscribes.len()));
    }
}

impl<T> Instance<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub fn new(
        build_info: &'static BuildInfo,
        arranged_logs: BTreeMap<LogVariant, GlobalId>,
        envd_epoch: NonZeroI64,
        metrics: InstanceMetrics,
    ) -> Self {
        let collections = arranged_logs
            .iter()
            .map(|(_, id)| {
                let state = CollectionState::new_log_collection();
                (*id, state)
            })
            .collect();
        let history = ComputeCommandHistory::new(metrics.for_history());

        let mut instance = Self {
            build_info,
            initialized: false,
            replicas: Default::default(),
            collections,
            log_sources: arranged_logs,
            peeks: Default::default(),
            subscribes: Default::default(),
            history,
            failed_replicas: Default::default(),
            ready_responses: Default::default(),
            envd_epoch,
            replica_epochs: Default::default(),
            metrics,
        };

        instance.send(ComputeCommand::CreateTimely {
            config: Default::default(),
            epoch: ClusterStartupEpoch::new(envd_epoch, 0),
        });

        let dummy_logging_config = Default::default();
        instance.send(ComputeCommand::CreateInstance(dummy_logging_config));

        instance
    }

    /// Update instance configuration.
    pub fn update_configuration(&mut self, config_params: ComputeParameters) {
        self.send(ComputeCommand::UpdateConfiguration(config_params));
    }

    /// Marks the end of any initialization commands.
    ///
    /// Intended to be called by `Controller`, rather than by other code.
    /// Calling this method repeatedly has no effect.
    pub fn initialization_complete(&mut self) {
        // The compute protocol requires that `InitializationComplete` is sent only once.
        if !self.initialized {
            self.send(ComputeCommand::InitializationComplete);
            self.initialized = true;
        }
    }

    /// Drop this compute instance.
    ///
    /// # Panics
    ///
    /// Panics if the compute instance still has active replicas.
    pub fn drop(self) {
        assert!(
            self.replicas.is_empty(),
            "cannot drop instances with provisioned replicas"
        );
    }

    /// Sends a command to all replicas of this instance.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn send(&mut self, cmd: ComputeCommand<T>) {
        // Record the command so that new replicas can be brought up to speed.
        self.history.push(cmd.clone(), &self.peeks);

        // Clone the command for each active replica.
        for (id, replica) in self.replicas.iter_mut() {
            // If sending the command fails, the replica requires rehydration.
            if replica.send(cmd.clone()).is_err() {
                self.failed_replicas.insert(*id);
            }
        }
    }

    /// Receives the next response from any replica of this instance.
    ///
    /// Returns `Err` if receiving from a replica has failed, to signal that it is in need of
    /// rehydration.
    ///
    /// This method is cancellation safe.
    pub async fn recv(&mut self) -> Result<(ReplicaId, ComputeResponse<T>), ReplicaId> {
        // Receive responses from any of the replicas, and take appropriate
        // action.
        let response = self
            .replicas
            .iter_mut()
            .map(|(id, replica)| async { (*id, replica.recv().await) })
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
                self.failed_replicas.insert(replica_id);
                Err(replica_id)
            }
            Some((replica_id, Some(response))) => {
                // A replica has produced a response. Return it.
                Ok((replica_id, response))
            }
        }
    }

    /// Assign a target replica to the identified subscribe.
    ///
    /// If a subscribe has a target replica assigned, only subscribe responses
    /// sent by that replica are considered.
    pub fn set_subscribe_target_replica(
        &mut self,
        id: GlobalId,
        target_replica: ReplicaId,
    ) -> Result<(), SubscribeTargetError> {
        if !self.replica_exists(target_replica) {
            return Err(SubscribeTargetError::ReplicaMissing(target_replica));
        }

        let Some(subscribe) = self.subscribes.get_mut(&id) else {
            return Err(SubscribeTargetError::SubscribeMissing(id));
        };

        // For sanity reasons, we don't allow re-targeting a subscribe for which we have already
        // produced output.
        if !subscribe.frontier.less_equal(&T::minimum()) {
            return Err(SubscribeTargetError::SubscribeAlreadyStarted);
        }

        subscribe.target_replica = Some(target_replica);
        Ok(())
    }
}

/// A wrapper around [`Instance`] with a live storage controller.
#[derive(Debug)]
pub(super) struct ActiveInstance<'a, T> {
    compute: &'a mut Instance<T>,
    storage_controller: &'a mut dyn StorageController<Timestamp = T>,
}

impl<'a, T> ActiveInstance<'a, T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Add a new instance replica, by ID.
    pub fn add_replica(
        &mut self,
        id: ReplicaId,
        mut config: ReplicaConfig,
    ) -> Result<(), ReplicaExists> {
        if self.compute.replica_exists(id) {
            return Err(ReplicaExists(id));
        }

        config.logging.index_logs = self.compute.log_sources.clone();
        let log_ids: BTreeSet<_> = config.logging.index_logs.values().collect();

        // Initialize frontier tracking for the new replica
        // and clean up any dropped collections that we can
        let mut updates = Vec::new();
        for (compute_id, collection) in &mut self.compute.collections {
            // Skip log collections not maintained by this replica.
            if collection.log_collection && !log_ids.contains(compute_id) {
                continue;
            }

            let read_frontier = collection.read_frontier();
            updates.push((*compute_id, read_frontier.to_owned()));
        }
        self.update_write_frontiers(id, &updates);

        let replica_epoch = self.compute.replica_epochs.entry(id).or_default();
        *replica_epoch += 1;
        let replica = Replica::spawn(
            id,
            self.compute.build_info,
            config,
            ClusterStartupEpoch::new(self.compute.envd_epoch, *replica_epoch),
            self.compute.metrics.for_replica(id),
        );

        // Take this opportunity to clean up the history we should present.
        self.compute.history.retain_peeks(&self.compute.peeks);
        self.compute.history.reduce();

        // Replay the commands at the client, creating new dataflow identifiers.
        for command in self.compute.history.iter() {
            if replica.send(command.clone()).is_err() {
                // We swallow the error here. On the next send, we will fail again, and
                // restart the connection as well as this rehydration.
                tracing::warn!("Replica {:?} connection terminated during hydration", id);
                break;
            }
        }

        // Add replica to tracked state.
        self.compute.replicas.insert(id, replica);
        for peek in self.compute.peeks.values_mut() {
            peek.unfinished.insert(id);
        }
        Ok(())
    }

    /// Remove an existing instance replica, by ID.
    pub fn remove_replica(&mut self, id: ReplicaId) -> Result<(), ReplicaMissing> {
        self.compute
            .replicas
            .remove(&id)
            .ok_or(ReplicaMissing(id))?;

        self.compute.failed_replicas.remove(&id);

        // Remove frontier tracking for this replica.
        self.remove_write_frontiers(id);

        // Removing a replica might implicitly finish peeks.
        let mut peeks_to_remove = BTreeSet::new();
        for (uuid, peek) in &mut self.compute.peeks {
            peek.unfinished.remove(&id);
            if peek.is_finished() {
                peeks_to_remove.insert(*uuid);
            }
        }
        self.remove_peeks(&peeks_to_remove);

        // Subscribes targeting this replica either won't be served anymore (if the replica is
        // dropped) or might produce inconsistent output (if the target collection is an
        // introspection index). We produce an error to inform upstream.
        let to_drop: Vec<_> = self.compute.subscribes_targeting(id).collect();
        for subscribe_id in to_drop {
            let subscribe = self.compute.subscribes.remove(&subscribe_id).unwrap();
            let response = ComputeControllerResponse::SubscribeResponse(
                subscribe_id,
                SubscribeResponse::Batch(SubscribeBatch {
                    lower: subscribe.frontier.clone(),
                    upper: subscribe.frontier,
                    updates: Err("target replica failed or was dropped".into()),
                }),
            );
            self.compute.ready_responses.push_back(response);
        }

        Ok(())
    }

    /// Rehydrate the given instance replica.
    ///
    /// # Panics
    ///
    /// Panics if the specified replica does not exist.
    fn rehydrate_replica(&mut self, id: ReplicaId) {
        let config = self.compute.replicas[&id].config.clone();
        self.remove_replica(id).expect("replica must exist");
        let result = self.add_replica(id, config);

        match result {
            Ok(()) => (),
            Err(ReplicaExists(_)) => unreachable!("replica was removed"),
        }
    }

    /// Rehydrate any failed replicas of this instance.
    pub fn rehydrate_failed_replicas(&mut self) {
        let failed_replicas = self.compute.failed_replicas.clone();
        for replica_id in failed_replicas {
            self.rehydrate_replica(replica_id);
            self.compute.failed_replicas.remove(&replica_id);
        }
    }

    /// Create the described dataflows and initializes state for their output.
    pub fn create_dataflows(
        &mut self,
        dataflows: Vec<DataflowDescription<crate::plan::Plan<T>, (), T>>,
    ) -> Result<(), DataflowCreationError> {
        // Validate dataflows as having inputs whose `since` is less or equal to the dataflow's `as_of`.
        // Start tracking frontiers for each dataflow, using its `as_of` for each index and sink.
        for dataflow in dataflows.iter() {
            let as_of = dataflow
                .as_of
                .as_ref()
                .ok_or(DataflowCreationError::MissingAsOf)?;

            // Record all transitive dependencies of the outputs.
            let mut storage_dependencies = Vec::new();
            let mut compute_dependencies = Vec::new();

            // Validate sources have `since.less_equal(as_of)`.
            for source_id in dataflow.source_imports.keys() {
                let since = &self
                    .storage_controller
                    .collection(*source_id)
                    .map_err(|_| DataflowCreationError::CollectionMissing(*source_id))?
                    .read_capabilities
                    .frontier();
                if !(timely::order::PartialOrder::less_equal(since, &as_of.borrow())) {
                    Err(DataflowCreationError::SinceViolation(*source_id))?;
                }

                storage_dependencies.push(*source_id);
            }

            // Validate indexes have `since.less_equal(as_of)`.
            // TODO(mcsherry): Instead, return an error from the constructing method.
            for index_id in dataflow.index_imports.keys() {
                let collection = self.compute.collection(*index_id)?;
                let since = collection.read_capabilities.frontier();
                if !(timely::order::PartialOrder::less_equal(&since, &as_of.borrow())) {
                    Err(DataflowCreationError::SinceViolation(*index_id))?;
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
                // TODO(benesch): fix this dangerous use of `as`.
                #[allow(clippy::as_conversions)]
                changes.update(time.clone(), outputs as i64);
            }
            // Update storage read capabilities for inputs.
            let mut storage_read_updates = storage_dependencies
                .iter()
                .map(|id| (*id, changes.clone()))
                .collect();
            self.storage_controller
                .update_read_capabilities(&mut storage_read_updates);
            // Update compute read capabilities for inputs.
            let mut compute_read_updates = compute_dependencies
                .iter()
                .map(|id| (*id, changes.clone()))
                .collect();
            self.update_read_capabilities(&mut compute_read_updates);

            // Install collection state for each of the exports.
            let mut updates = Vec::new();
            for export_id in dataflow.export_ids() {
                self.compute.collections.insert(
                    export_id,
                    CollectionState::new(
                        as_of.clone(),
                        storage_dependencies.clone(),
                        compute_dependencies.clone(),
                    ),
                );
                updates.push((export_id, as_of.clone()));
            }
            // Initialize tracking of replica frontiers.
            let replica_ids: Vec<_> = self.compute.replica_ids().collect();
            for replica_id in replica_ids {
                self.update_write_frontiers(replica_id, &updates);
            }

            // Initialize tracking of subscribes.
            for subscribe_id in dataflow.subscribe_ids() {
                self.compute
                    .subscribes
                    .insert(subscribe_id, ActiveSubscribe::new());
            }
        }

        // Here we augment all imported sources and all exported sinks with with the appropriate
        // storage metadata needed by the compute instance.
        let mut augmented_dataflows = Vec::with_capacity(dataflows.len());
        for d in dataflows {
            let mut source_imports = BTreeMap::new();
            for (id, (si, monotonic)) in d.source_imports {
                let collection = self
                    .storage_controller
                    .collection(id)
                    .map_err(|_| DataflowCreationError::CollectionMissing(id))?;
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
                            .collection(id)
                            .map_err(|_| DataflowCreationError::CollectionMissing(id))?
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
                    with_snapshot: se.with_snapshot,
                    up_to: se.up_to,
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
    pub fn drop_collections(&mut self, ids: Vec<GlobalId>) -> Result<(), CollectionMissing> {
        // Validate that the ids exist.
        self.validate_ids(ids.iter().cloned())?;

        let policies = ids
            .into_iter()
            .map(|id| (id, ReadPolicy::ValidFrom(Antichain::new())));
        self.set_read_policy(policies.collect())
    }

    /// Initiate a peek request for the contents of `id` at `timestamp`.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn peek(
        &mut self,
        id: GlobalId,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<ReplicaId>,
    ) -> Result<(), PeekError> {
        let since = self.compute.collection(id)?.read_capabilities.frontier();
        if !since.less_equal(&timestamp) {
            Err(PeekError::SinceViolation(id))?;
        }

        if let Some(target) = target_replica {
            if !self.compute.replica_exists(target) {
                return Err(PeekError::ReplicaMissing(target));
            }
        }

        // Install a compaction hold on `id` at `timestamp`.
        let mut updates = BTreeMap::new();
        updates.insert(id, ChangeBatch::new_from(timestamp.clone(), 1));
        self.update_read_capabilities(&mut updates);

        let unfinished = self.compute.replica_ids().collect();
        let otel_ctx = OpenTelemetryContext::obtain();
        self.compute.peeks.insert(
            uuid,
            PendingPeek {
                target: id,
                time: timestamp.clone(),
                unfinished,
                target_replica,
                // TODO(guswynn): can we just hold the `tracing::Span` here instead?
                otel_ctx: Some(otel_ctx.clone()),
                requested_at: Instant::now(),
            },
        );

        self.compute.send(ComputeCommand::Peek(Peek {
            id,
            literal_constraints,
            uuid,
            timestamp,
            finishing,
            map_filter_project,
            // Obtain an `OpenTelemetryContext` from the thread-local tracing
            // tree to forward it on to the compute worker.
            otel_ctx,
        }));

        Ok(())
    }

    /// Cancels existing peek requests.
    pub fn cancel_peeks(&mut self, uuids: BTreeSet<Uuid>) {
        for uuid in &uuids {
            let Some(peek) = self.compute.peeks.get_mut(uuid) else {
                tracing::warn!("did not find pending peek for {uuid}");
                continue;
            };

            // Canceled peeks should not be further responded to.
            let Some(otel_ctx) = peek.otel_ctx.take() else {
                tracing::warn!("peek {uuid} has already been served");
                continue;
            };

            let response = PeekResponse::Canceled;
            let duration = peek.requested_at.elapsed();
            self.compute
                .metrics
                .observe_peek_response(&response, duration);

            // Enqueue the response to the cancellation.
            self.compute
                .ready_responses
                .push_back(ComputeControllerResponse::PeekResponse(
                    *uuid, response, otel_ctx,
                ));
        }

        self.compute.send(ComputeCommand::CancelPeeks { uuids });
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
    pub fn set_read_policy(
        &mut self,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), CollectionMissing> {
        let mut read_capability_changes = BTreeMap::default();
        for (id, policy) in policies.into_iter() {
            let collection = self.compute.collection_mut(id)?;
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
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut read_capability_changes);
        }
        Ok(())
    }

    /// Validate that a collection exists for all identifiers, and error if any do not.
    fn validate_ids(&self, ids: impl Iterator<Item = GlobalId>) -> Result<(), CollectionMissing> {
        for id in ids {
            self.compute.collection(id)?;
        }
        Ok(())
    }

    /// Accept write frontier updates from the compute layer.
    ///
    /// # Panics
    ///
    /// Panics if any of the `updates` references an absent collection.
    /// Panics if any of the `updates` regresses an existing write frontier.
    #[tracing::instrument(level = "debug", skip(self))]
    fn update_write_frontiers(
        &mut self,
        replica_id: ReplicaId,
        updates: &[(GlobalId, Antichain<T>)],
    ) {
        let mut advanced_collections = Vec::new();
        let mut compute_read_capability_changes = BTreeMap::default();
        let mut storage_read_capability_changes = BTreeMap::default();
        let mut dropped_collection_ids = Vec::new();
        for (id, new_upper) in updates.iter() {
            let collection = self
                .compute
                .collection_mut(*id)
                .expect("reference to absent collection");

            if PartialOrder::less_than(&collection.write_frontier, new_upper) {
                advanced_collections.push(*id);
                collection.write_frontier = new_upper.clone();
            }

            let old_upper = collection
                .replica_write_frontiers
                .insert(replica_id, new_upper.clone());

            // Safety check against frontier regressions.
            if let Some(old) = &old_upper {
                assert!(
                    PartialOrder::less_equal(old, new_upper),
                    "Frontier regression: {old:?} -> {new_upper:?}, \
                     collection={id}, replica={replica_id}",
                );
            }

            if new_upper.is_empty() {
                dropped_collection_ids.push(*id);
            }

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
                    compute_read_capability_changes.insert(*id, update);
                }
            }

            // Update read holds on storage dependencies.
            for storage_id in &collection.storage_dependencies {
                let update = storage_read_capability_changes
                    .entry(*storage_id)
                    .or_insert_with(|| ChangeBatch::new());
                if let Some(old) = &old_upper {
                    update.extend(old.iter().map(|time| (time.clone(), -1)));
                }
                update.extend(new_upper.iter().map(|time| (time.clone(), 1)));
            }
        }
        if !compute_read_capability_changes.is_empty() {
            self.update_read_capabilities(&mut compute_read_capability_changes);
        }
        if !storage_read_capability_changes.is_empty() {
            self.storage_controller
                .update_read_capabilities(&mut storage_read_capability_changes);
        }

        // Tell the storage controller about new write frontiers for storage
        // collections that are advanced by compute sinks.
        // TODO(teskje): The storage controller should have a task to directly
        // keep track of the frontiers of storage collections, instead of
        // relying on others for that information.
        let storage_updates: Vec<_> = advanced_collections
            .into_iter()
            .filter(|id| self.storage_controller.collection(*id).is_ok())
            .map(|id| {
                let collection = self.compute.collection(id).unwrap();
                (id, collection.write_frontier.clone())
            })
            .collect();
        self.storage_controller
            .update_write_frontiers(&storage_updates);

        if !dropped_collection_ids.is_empty() {
            self.update_dropped_collections(dropped_collection_ids);
        }
    }

    /// Remove frontier tracking state for the given replica.
    #[tracing::instrument(level = "debug", skip(self))]
    fn remove_write_frontiers(&mut self, replica_id: ReplicaId) {
        let mut storage_read_capability_changes = BTreeMap::default();
        let mut dropped_collection_ids = Vec::new();
        for (id, collection) in self.compute.collections.iter_mut() {
            let last_upper = collection.replica_write_frontiers.remove(&replica_id);

            if let Some(frontier) = last_upper {
                dropped_collection_ids.push(*id);

                // Update read holds on storage dependencies.
                for storage_id in &collection.storage_dependencies {
                    let update = storage_read_capability_changes
                        .entry(*storage_id)
                        .or_insert_with(|| ChangeBatch::new());
                    update.extend(frontier.iter().map(|time| (time.clone(), -1)));
                }
            }
        }
        if !storage_read_capability_changes.is_empty() {
            self.storage_controller
                .update_read_capabilities(&mut storage_read_capability_changes);
        }
        if !dropped_collection_ids.is_empty() {
            self.update_dropped_collections(dropped_collection_ids);
        }
    }

    /// Applies `updates`, propagates consequences through other read capabilities, and sends an appropriate compaction command.
    #[tracing::instrument(level = "debug", skip(self))]
    fn update_read_capabilities(&mut self, updates: &mut BTreeMap<GlobalId, ChangeBatch<T>>) {
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
                if self.storage_controller.collection(key).is_ok() {
                    storage_todo
                        .entry(key)
                        .or_insert_with(ChangeBatch::new)
                        .extend(update.drain())
                } else {
                    tracing::error!(
                        "found neither compute nor storage collection with id {}",
                        key
                    );
                }
            }
        }

        // Translate our net compute actions into `AllowCompaction` commands
        // and a list of collections that are potentially ready to be dropped
        let mut compaction_commands = Vec::new();
        let mut dropped_collection_ids = Vec::new();
        for (id, change) in compute_net.iter_mut() {
            let frontier = self
                .compute
                .collection(*id)
                .expect("existence checked above")
                .read_frontier();
            if frontier.is_empty() {
                dropped_collection_ids.push(*id);
            }
            if !change.is_empty() {
                let frontier = frontier.to_owned();
                compaction_commands.push((*id, frontier));
            }
        }
        if !compaction_commands.is_empty() {
            self.compute
                .send(ComputeCommand::AllowCompaction(compaction_commands));
        }
        if !dropped_collection_ids.is_empty() {
            self.update_dropped_collections(dropped_collection_ids);
        }

        // We may have storage consequences to process.
        if !storage_todo.is_empty() {
            self.storage_controller
                .update_read_capabilities(&mut storage_todo);
        }
    }

    /// Removes a registered peek, unblocking compaction that might have waited on it.
    fn remove_peeks(&mut self, peek_ids: &BTreeSet<Uuid>) {
        let mut updates = peek_ids
            .into_iter()
            .flat_map(|uuid| {
                self.compute
                    .peeks
                    .remove(uuid)
                    .map(|peek| (peek.target, ChangeBatch::new_from(peek.time, -1)))
            })
            .collect();
        self.update_read_capabilities(&mut updates);
    }

    pub fn handle_response(
        &mut self,
        response: ComputeResponse<T>,
        replica_id: ReplicaId,
    ) -> Option<ComputeControllerResponse<T>> {
        match response {
            ComputeResponse::FrontierUppers(list) => {
                self.handle_frontier_uppers(list, replica_id);
                None
            }
            ComputeResponse::PeekResponse(uuid, peek_response, otel_ctx) => {
                self.handle_peek_response(uuid, peek_response, otel_ctx, replica_id)
            }
            ComputeResponse::SubscribeResponse(id, response) => {
                self.handle_subscribe_response(id, response, replica_id)
            }
        }
    }

    /// Cleans up collection state, if necessary, in response to drop operations targeted
    /// at a replica and given collections (via reporting of an empty frontier).
    fn update_dropped_collections(&mut self, dropped_collection_ids: Vec<GlobalId>) {
        for id in dropped_collection_ids {
            // clean up the given collection if read frontier is empty
            // and all replica frontiers are empty
            if let Ok(collection) = self.compute.collection(id) {
                if collection.read_frontier().is_empty()
                    && collection
                        .replica_write_frontiers
                        .values()
                        .all(|frontier| frontier.is_empty())
                {
                    self.compute.collections.remove(&id);
                }
            }
        }
    }

    fn handle_frontier_uppers(
        &mut self,
        list: Vec<(GlobalId, Antichain<T>)>,
        replica_id: ReplicaId,
    ) {
        // According to the compute protocol, replicas are not allowed to send `FrontierUppers`
        // that regress frontiers they have reported previously. We still perform a check here,
        // rather than risking the controller becoming confused trying to handle regressions.
        let mut updates = Vec::with_capacity(list.len());
        for (id, new_frontier) in list {
            let Ok(coll) = self.compute.collection(id) else {
                tracing::warn!(
                    ?replica_id,
                    "Frontier update for unknown collection {id}: {:?}",
                    new_frontier.elements(),
                );
                tracing::error!("Replica reported an untracked collection frontier");
                continue;
            };

            if let Some(old_frontier) = coll.replica_write_frontiers.get(&replica_id) {
                if !PartialOrder::less_equal(old_frontier, &new_frontier) {
                    tracing::warn!(
                        ?replica_id,
                        "Frontier of collection {id} regressed: {:?} -> {:?}",
                        old_frontier.elements(),
                        new_frontier.elements(),
                    );
                    tracing::error!("Replica reported a regressed collection frontier");
                    continue;
                }
            }

            updates.push((id, new_frontier));
        }

        self.update_write_frontiers(replica_id, &updates);
    }

    fn handle_peek_response(
        &mut self,
        uuid: Uuid,
        response: PeekResponse,
        otel_ctx: OpenTelemetryContext,
        replica_id: ReplicaId,
    ) -> Option<ComputeControllerResponse<T>> {
        let peek = match self.compute.peeks.get_mut(&uuid) {
            Some(peek) => peek,
            None => {
                tracing::warn!("did not find pending peek for {}", uuid);
                return None;
            }
        };

        // Forward the peek response, if we didn't already forward a response
        // to this peek previously. If the peek is targeting a replica, only
        // forward the response from that replica.
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
        let replica_targeted = peek.target_replica.unwrap_or(replica_id) == replica_id;
        let controller_response = if replica_targeted && peek.otel_ctx.take().is_some() {
            let duration = peek.requested_at.elapsed();
            self.compute
                .metrics
                .observe_peek_response(&response, duration);

            Some(ComputeControllerResponse::PeekResponse(
                uuid, response, otel_ctx,
            ))
        } else {
            None
        };

        // Update the per-replica tracking and draw appropriate consequences.
        peek.unfinished.remove(&replica_id);
        if peek.is_finished() {
            self.remove_peeks(&[uuid].into());
        }

        controller_response
    }

    fn handle_subscribe_response(
        &mut self,
        subscribe_id: GlobalId,
        response: SubscribeResponse<T>,
        replica_id: ReplicaId,
    ) -> Option<ComputeControllerResponse<T>> {
        if !self.compute.collections.contains_key(&subscribe_id) {
            tracing::warn!(?replica_id, "Response for unknown subscribe {subscribe_id}",);
            tracing::error!("Replica sent a response for an unknown subscibe");
            return None;
        }

        // Always apply write frontier updates. Even if the subscribe is not tracked anymore, there
        // might still be replicas reading from its inputs, so we need to track the frontiers until
        // all replicas have advanced to the empty one.
        let write_frontier = match &response {
            SubscribeResponse::Batch(batch) => batch.upper.clone(),
            SubscribeResponse::DroppedAt(_) => Antichain::new(),
        };
        self.update_write_frontiers(replica_id, &[(subscribe_id, write_frontier)]);

        // If the subscribe is not tracked, or targets a different replica, there is nothing to do.
        let mut subscribe = self.compute.subscribes.get(&subscribe_id)?.clone();
        let replica_targeted = subscribe.target_replica.unwrap_or(replica_id) == replica_id;
        if !replica_targeted {
            return None;
        }

        match response {
            SubscribeResponse::Batch(batch) => {
                let upper = batch.upper;
                let mut updates = batch.updates;

                // If this batch advances the subscribe's frontier, we emit all updates at times
                // greater or equal to the last frontier (to avoid emitting duplicate updates).
                if PartialOrder::less_than(&subscribe.frontier, &upper) {
                    let lower = std::mem::replace(&mut subscribe.frontier, upper.clone());

                    if upper.is_empty() {
                        // This subscribe cannot produce more data. Stop tracking it.
                        self.compute.subscribes.remove(&subscribe_id);
                    } else {
                        // This subscribe can produce more data. Update our tracking of it.
                        self.compute.subscribes.insert(subscribe_id, subscribe);
                    }

                    if let Ok(updates) = updates.as_mut() {
                        updates.retain(|(time, _data, _diff)| lower.less_equal(time));
                    }
                    Some(ComputeControllerResponse::SubscribeResponse(
                        subscribe_id,
                        SubscribeResponse::Batch(SubscribeBatch {
                            lower,
                            upper,
                            updates,
                        }),
                    ))
                } else {
                    None
                }
            }
            SubscribeResponse::DroppedAt(_) => {
                // This subscribe cannot produce more data. Stop tracking it.
                self.compute.subscribes.remove(&subscribe_id);

                Some(ComputeControllerResponse::SubscribeResponse(
                    subscribe_id,
                    SubscribeResponse::DroppedAt(subscribe.frontier),
                ))
            }
        }
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
    /// For replica-targeted peeks, this specifies the replica whose response we should pass on.
    ///
    /// If this value is `None`, we pass on the first response.
    target_replica: Option<ReplicaId>,
    /// The OpenTelemetry context for this peek.
    ///
    /// This value is `Some` as long as we have not yet passed a response up the chain, and `None`
    /// afterwards.
    otel_ctx: Option<OpenTelemetryContext>,
    /// The time at which the peek was requested.
    ///
    /// Used to track peek durations.
    requested_at: Instant,
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

#[derive(Debug, Clone)]
struct ActiveSubscribe<T> {
    /// Current upper frontier of this subscribe.
    frontier: Antichain<T>,
    /// For replica-targeted subscribes, this specifies the replica whose responses we should pass on.
    ///
    /// If this value is `None`, we pass on the first response for each time slice.
    target_replica: Option<ReplicaId>,
}

impl<T: Timestamp> ActiveSubscribe<T> {
    fn new() -> Self {
        Self {
            frontier: Antichain::from_elem(Timestamp::minimum()),
            target_replica: None,
        }
    }
}
