// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller for a compute instance.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use differential_dataflow::lattice::Lattice;
use futures::stream::FuturesUnordered;
use futures::{future, StreamExt};
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use timely::PartialOrder;
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_expr::RowSetFinishing;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Row};
use mz_storage::controller::{ReadPolicy, StorageController};

use crate::command::{
    ComputeCommand, ComputeCommandHistory, DataflowDescription, InstanceConfig, Peek, ReplicaId,
    SourceInstanceDesc,
};
use crate::logging::{LogVariant, LoggingConfig};
use crate::response::{ComputeResponse, PeekResponse, SubscribeBatch, SubscribeResponse};
use crate::service::{ComputeClient, ComputeGrpcClient};
use crate::sinks::{ComputeSinkConnection, ComputeSinkDesc, PersistSinkConnection};

use super::orchestrator::ComputeOrchestrator;
use super::replica::Replica;
use super::{
    CollectionState, ComputeControllerResponse, ComputeError, ComputeInstanceId,
    ComputeReplicaConfig, ComputeReplicaLocation,
};

/// The state we keep for a compute instance.
#[derive(Debug)]
pub(super) struct Instance<T> {
    /// ID of this instance
    instance_id: ComputeInstanceId,
    /// Build info for spawning replicas
    build_info: &'static BuildInfo,
    /// The replicas of this compute instance.
    replicas: HashMap<ReplicaId, Replica<T>>,
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
    /// IDs of replicas that have failed and require rehydration.
    failed_replicas: BTreeSet<ReplicaId>,
    /// Ready compute controller responses to be delivered.
    pub ready_responses: VecDeque<ComputeControllerResponse<T>>,
    /// Orchestrator for managing replicas
    orchestrator: ComputeOrchestrator,
}

impl<T> Instance<T> {
    /// Acquire a handle to the collection state associated with `id`.
    pub fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, ComputeError> {
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
}

impl<T> Instance<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub fn new(
        instance_id: ComputeInstanceId,
        build_info: &'static BuildInfo,
        arranged_logs: BTreeMap<LogVariant, GlobalId>,
        max_result_size: u32,
        orchestrator: ComputeOrchestrator,
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
            instance_id,
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
            failed_replicas: Default::default(),
            ready_responses: Default::default(),
            orchestrator,
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
    pub fn initialization_complete(&mut self) {
        self.send(ComputeCommand::InitializationComplete);
    }

    /// Drop this compute instance.
    ///
    /// # Panics
    /// - If the compute instance still has active replicas.
    pub fn drop(self) {
        assert!(
            self.replicas.is_empty(),
            "cannot drop instances with provisioned replicas"
        );
    }

    /// Introduce a new replica, and catch it up to the commands of other replicas.
    ///
    /// It is not yet clear under which circumstances a replica can be removed.
    fn add_replica(
        &mut self,
        id: ReplicaId,
        location: ComputeReplicaLocation,
        logging_config: Option<LoggingConfig>,
    ) {
        let replica = Replica::spawn(
            id,
            self.instance_id,
            self.build_info,
            location,
            logging_config,
            self.orchestrator.clone(),
        );

        // Take this opportunity to clean up the history we should present.
        self.history.retain_peeks(&self.peeks);
        self.history.reduce();

        // Replay the commands at the client, creating new dataflow identifiers.
        for command in self.history.iter() {
            if replica.send(command.clone()).is_err() {
                // We swallow the error here. On the next send, we will fail again, and
                // restart the connection as well as this rehydration.
                tracing::warn!("Replica {:?} connection terminated during rehydration", id);
                break;
            }
        }

        if let Some(logging) = &replica.logging_config {
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
        self.replicas.insert(id, replica);
        for uppers in self.uppers.values_mut() {
            uppers.add_replica(id);
        }
        for peek in self.peeks.values_mut() {
            peek.unfinished.insert(id);
        }
    }

    /// Sends a command to all replicas of this instance.
    #[tracing::instrument(level = "debug", skip(self))]
    pub fn send(&mut self, cmd: ComputeCommand<T>) {
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
    /// This method is cancellation safe.
    pub async fn recv(&mut self) -> (ReplicaId, ComputeResponse<T>) {
        // Receive responses from any of the replicas, and take appropriate
        // action.
        loop {
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
                }
                Some((replica_id, Some(response))) => {
                    // A replica has produced a response. Return it.
                    return (replica_id, response);
                }
            }
        }
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
            self.ready_responses
                .push_back(ComputeControllerResponse::SubscribeResponse(
                    id,
                    SubscribeResponse::DroppedAt(previous.bounds.upper),
                ));
        }
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
        config: ComputeReplicaConfig,
    ) -> Result<(), ComputeError> {
        let logging_config = if let Some(interval) = config.logging.interval {
            // Initialize state for per-replica log sources.
            let mut sink_logs = BTreeMap::new();
            for (variant, id) in config.logging.sources {
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
                log_logging: config.logging.log_logging,
                sink_logs,
            })
        } else {
            None
        };

        // Add the replica
        self.compute
            .add_replica(id, config.location, logging_config);
        Ok(())
    }

    /// Remove an existing instance replica, by ID. Will remove the replica from the
    /// orchestrator.
    pub async fn remove_replica(&mut self, id: ReplicaId) -> Result<(), ComputeError> {
        if let Err(e) = self.compute.replicas[&id].send(ComputeCommand::DropInstance) {
            tracing::warn!("Could not send DropInstance to replica {:?}: {}", &id, &e)
        }

        self.remove_replica_state(id).await
    }

    /// Remove all state related to a replica. This will not cause an orchestrator removal
    /// of the replica.
    async fn remove_replica_state(&mut self, id: ReplicaId) -> Result<(), ComputeError> {
        // Removing a replica might elicit changes to collection frontiers.
        let mut new_uppers = Vec::new();
        for (collection_id, uppers) in self.compute.uppers.iter_mut() {
            if uppers.remove_replica(id) {
                new_uppers.push((*collection_id, uppers.bounds.clone()));
            }
        }
        for (collection_id, uppers) in self.compute.index_log_uppers.iter_mut() {
            if uppers.tracks_replica(id) && uppers.remove_replica(id) {
                new_uppers.push((*collection_id, uppers.bounds.clone()));
            }
        }
        if !new_uppers.is_empty() {
            self.update_write_frontiers(&new_uppers).await?;
        }

        // Removing a replica might implicitly finish a peeks.
        let mut peeks_to_remove = BTreeSet::new();
        for (uuid, peek) in &mut self.compute.peeks {
            peek.unfinished.remove(&id);
            if peek.is_finished() {
                peeks_to_remove.insert(*uuid);
            }
        }
        self.remove_peeks(&peeks_to_remove).await?;

        let replica = self
            .compute
            .replicas
            .remove(&id)
            .expect("replica not found");

        // Cease tracking frontiers of persisted log collections.
        if let Some(logging) = replica.logging_config {
            for (collection_id, _) in logging.sink_logs.values() {
                let previous = self.compute.sink_log_uppers.remove(collection_id);
                assert!(previous.is_some());
            }
        }

        Ok(())
    }

    async fn rehydrate_replica(&mut self, id: ReplicaId) -> Result<(), ComputeError> {
        let location = self.compute.replicas[&id].location.clone();
        let logging_config = self.compute.replicas[&id].logging_config.clone();
        self.remove_replica_state(id).await?;
        self.compute.add_replica(id, location, logging_config);
        Ok(())
    }

    /// Rehydrate any failed replicas of this instance.
    pub async fn rehydrate_failed_replicas(&mut self) -> Result<(), ComputeError> {
        let failed_replicas = self.compute.failed_replicas.clone();
        for replica_id in failed_replicas {
            self.rehydrate_replica(replica_id).await?;
            self.compute.failed_replicas.remove(&replica_id);
        }
        Ok(())
    }

    /// Create the described dataflows and initializes state for their output.
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

            // Initialize tracking of subscribes.
            self.compute.subscribes.extend(dataflow.subscribe_ids());
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
            otel_ctx,
        }));

        Ok(())
    }

    /// Cancels existing peek requests.
    pub async fn cancel_peeks(&mut self, uuids: BTreeSet<Uuid>) -> Result<(), ComputeError> {
        self.remove_peeks(&uuids).await?;

        // Enqueue the response to the cancelation.
        for uuid in &uuids {
            let otel_ctx = self
                .compute
                .peeks
                .get_mut(uuid)
                // Canceled peeks should not be further responded to.
                .map(|pending| pending.otel_ctx.take())
                .unwrap_or_else(|| {
                    tracing::warn!("did not find pending peek for {}", uuid);
                    None
                });
            if let Some(ctx) = otel_ctx {
                self.compute
                    .ready_responses
                    .push_back(ComputeControllerResponse::PeekResponse(
                        *uuid,
                        PeekResponse::Canceled,
                        ctx,
                    ));
            }
        }

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
    pub async fn set_read_policy(
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
    pub fn update_max_result_size(&mut self, max_result_size: u32) {
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

    pub async fn handle_response(
        &mut self,
        response: ComputeResponse<T>,
        replica_id: ReplicaId,
    ) -> Result<Option<ComputeControllerResponse<T>>, ComputeError> {
        match response {
            ComputeResponse::FrontierUppers(list) => {
                self.handle_frontier_uppers(list, replica_id).await?;
                Ok(None)
            }
            ComputeResponse::PeekResponse(uuid, peek_response, otel_ctx) => {
                self.handle_peek_response(uuid, peek_response, otel_ctx, replica_id)
                    .await
            }
            ComputeResponse::SubscribeResponse(id, response) => {
                self.handle_subscribe_response(id, response, replica_id)
                    .await
            }
        }
    }

    async fn handle_frontier_uppers(
        &mut self,
        list: Vec<(GlobalId, Antichain<T>)>,
        replica_id: ReplicaId,
    ) -> Result<(), ComputeError> {
        let mut new_uppers = Vec::new();

        for (id, new_upper) in list {
            if let Some(reported) = self.compute.uppers.get_mut(&id) {
                if reported.update(replica_id, new_upper) {
                    new_uppers.push((id, reported.bounds.clone()));
                }
            } else if let Some(reported) = self.compute.index_log_uppers.get_mut(&id) {
                if reported.update(replica_id, new_upper) {
                    new_uppers.push((id, reported.bounds.clone()));
                }
            } else if let Some(reported) = self.compute.sink_log_uppers.get_mut(&id) {
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

        self.update_write_frontiers(&new_uppers).await
    }

    async fn handle_peek_response(
        &mut self,
        uuid: Uuid,
        response: PeekResponse,
        otel_ctx: OpenTelemetryContext,
        replica_id: ReplicaId,
    ) -> Result<Option<ComputeControllerResponse<T>>, ComputeError> {
        let peek = match self.compute.peeks.get_mut(&uuid) {
            Some(peek) => peek,
            None => {
                tracing::warn!("did not find pending peek for {}", uuid);
                return Ok(None);
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
        let controller_response = peek
            .otel_ctx
            .take()
            .map(|_| ComputeControllerResponse::PeekResponse(uuid, response, otel_ctx));

        // Update the per-replica tracking and draw appropriate consequences.
        peek.unfinished.remove(&replica_id);
        if peek.is_finished() {
            self.remove_peeks(&[uuid].into()).await?;
        }

        Ok(controller_response)
    }

    async fn handle_subscribe_response(
        &mut self,
        subscribe_id: GlobalId,
        response: SubscribeResponse<T>,
        replica_id: ReplicaId,
    ) -> Result<Option<ComputeControllerResponse<T>>, ComputeError> {
        let entry = match self.compute.uppers.get_mut(&subscribe_id) {
            Some(uppers) => uppers,
            None => return Ok(None),
        };

        let mut frontier_updates = Vec::new();
        let controller_response = match response {
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
                    return Ok(None);
                }

                frontier_updates.push((subscribe_id, entry.bounds.clone()));

                if PartialOrder::less_than(&old_upper_bound, &entry.bounds.upper) {
                    // When we get here, the subscribe must still be in progress.
                    assert!(self.compute.subscribes.get(&subscribe_id).is_some());

                    if upper.is_empty() {
                        // This subscribe has finished producing all its data. Remove it from the
                        // in-progress subscribes, so we don't emit a `DroppedAt` for it.
                        self.compute.subscribes.remove(&subscribe_id);
                    }

                    let new_lower = old_upper_bound;
                    updates.retain(|(time, _data, _diff)| new_lower.less_equal(time));
                    Some(ComputeControllerResponse::SubscribeResponse(
                        subscribe_id,
                        SubscribeResponse::Batch(SubscribeBatch {
                            lower: new_lower,
                            upper: entry.bounds.upper.clone(),
                            updates,
                        }),
                    ))
                } else {
                    None
                }
            }
            SubscribeResponse::DroppedAt(_) => {
                // We should never get here. A replica emits `DroppedAt` only in response to a
                // subscribe being dropped by its client (via `AllowCompaction`). When we handle
                // the `AllowCompaction` command, we cease tracking the subscribe's frontier. And
                // without a tracked frontier, we return immediately at the beginning of this
                // method.
                tracing::error!("unexpected `DroppedAt` received for subscribe {subscribe_id}");
                None
            }
        };

        self.update_write_frontiers(&frontier_updates).await?;
        Ok(controller_response)
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
    pub bounds: FrontierBounds<T>,
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
