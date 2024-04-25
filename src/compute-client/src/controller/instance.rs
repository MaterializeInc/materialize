// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A controller for a compute instance.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::num::NonZeroI64;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::lattice::Lattice;
use futures::stream::FuturesUnordered;
use futures::{future, StreamExt};
use mz_build_info::BuildInfo;
use mz_cluster_client::client::{ClusterStartupEpoch, TimelyConfig};
use mz_compute_types::dataflows::{BuildDesc, DataflowDescription};
use mz_compute_types::plan::flat_plan::FlatPlan;
use mz_compute_types::plan::LirId;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, PersistSinkConnection};
use mz_compute_types::sources::SourceInstanceDesc;
use mz_dyncfg::ConfigSet;
use mz_expr::RowSetFinishing;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::global_id::TransientIdGen;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_client::controller::IntrospectionType;
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::read_holds::{ReadHold, ReadHoldError};
use mz_storage_types::read_policy::ReadPolicy;
use serde::Serialize;
use thiserror::Error;
use timely::progress::{Antichain, ChangeBatch};
use timely::{Container, PartialOrder};
use uuid::Uuid;

use crate::controller::error::CollectionMissing;
use crate::controller::replica::{ReplicaClient, ReplicaConfig};
use crate::controller::{
    CollectionState, ComputeControllerResponse, ComputeControllerTimestamp, IntrospectionUpdates,
    ReplicaId,
};
use crate::logging::LogVariant;
use crate::metrics::{InstanceMetrics, ReplicaMetrics};
use crate::metrics::{ReplicaCollectionMetrics, UIntGauge};
use crate::protocol::command::{
    ComputeCommand, ComputeParameters, InstanceConfig, Peek, PeekTarget,
};
use crate::protocol::history::ComputeCommandHistory;
use crate::protocol::response::{
    ComputeResponse, CopyToResponse, FrontiersResponse, OperatorHydrationStatus, PeekResponse,
    StatusResponse, SubscribeBatch, SubscribeResponse,
};
use crate::service::{ComputeClient, ComputeGrpcClient};

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
    #[error("subscribe dataflow has an empty as_of")]
    EmptyAsOfForSubscribe,
    #[error("copy to dataflow has an empty as_of")]
    EmptyAsOfForCopyTo,
}

impl From<CollectionMissing> for DataflowCreationError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

impl From<ReadHoldError> for DataflowCreationError {
    fn from(error: ReadHoldError) -> Self {
        match error {
            ReadHoldError::CollectionMissing(id) => DataflowCreationError::CollectionMissing(id),
            ReadHoldError::SinceViolation(id) => DataflowCreationError::SinceViolation(id),
        }
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

impl From<ReadHoldError> for PeekError {
    fn from(error: ReadHoldError) -> Self {
        match error {
            ReadHoldError::CollectionMissing(id) => PeekError::CollectionMissing(id),
            ReadHoldError::SinceViolation(id) => PeekError::SinceViolation(id),
        }
    }
}

#[derive(Error, Debug)]
pub(super) enum ReadPolicyError {
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    #[error("collection is write-only: {0}")]
    WriteOnlyCollection(GlobalId),
}

impl From<CollectionMissing> for ReadPolicyError {
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
    /// A handle providing access to storage collections.
    storage_collections: Arc<dyn StorageCollections<Timestamp = T>>,
    /// Whether instance initialization has been completed.
    initialized: bool,
    /// Whether or not this instance is in read-only mode.
    ///
    /// When in read-only mode, neither the controller nor the instances
    /// controlled by it are allowed to affect changes to external systems
    /// (largely persist).
    read_only: bool,
    /// The replicas of this compute instance.
    replicas: BTreeMap<ReplicaId, ReplicaState<T>>,
    /// Currently installed compute collections.
    ///
    /// New entries are added for all collections exported from dataflows created through
    /// [`Instance::create_dataflow`].
    ///
    /// Entries are removed by [`Instance::cleanup_collections`]. See that method's documentation
    /// about the conditions for removing collection state.
    collections: BTreeMap<GlobalId, CollectionState<T>>,
    /// IDs of log sources maintained by this compute instance.
    log_sources: BTreeMap<LogVariant, GlobalId>,
    /// Currently outstanding peeks.
    ///
    /// New entries are added for all peeks initiated through [`Instance::peek`].
    ///
    /// The entry for a peek is only removed once all replicas have responded to the peek. This is
    /// currently required to ensure all replicas have stopped reading from the peeked collection's
    /// inputs before we allow them to compact. #16641 tracks changing this so we only have to wait
    /// for the first peek response.
    peeks: BTreeMap<Uuid, PendingPeek<T>>,
    /// Currently in-progress subscribes.
    ///
    /// New entries are added for all subscribes exported from dataflows created through
    /// [`Instance::create_dataflow`].
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
    /// Tracks all in-progress COPY TOs.
    ///
    /// New entries are added for all s3 oneshot sinks (corresponding to a COPY TO) exported from
    /// dataflows created through [`Instance::create_dataflow`].
    ///
    /// The entry for a copy to is removed once at least one replica has finished
    /// or the exporting collection is dropped.
    copy_tos: BTreeSet<GlobalId>,
    /// The command history, used when introducing new replicas or restarting existing replicas.
    history: ComputeCommandHistory<UIntGauge, T>,
    /// Sender for responses to be delivered.
    response_tx: crossbeam_channel::Sender<ComputeControllerResponse<T>>,
    /// Sender for introspection updates to be recorded.
    introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    /// A number that increases with each restart of `environmentd`.
    envd_epoch: NonZeroI64,
    /// Numbers that increase with each restart of a replica.
    replica_epochs: BTreeMap<ReplicaId, u64>,
    /// A generator for transient [`GlobalId`]s.
    // TODO(#26730): use this to generate IDs for introspection subscribes
    #[allow(dead_code)]
    transient_id_gen: Arc<TransientIdGen>,
    /// The registry the controller uses to report metrics.
    metrics: InstanceMetrics,
    /// Dynamic system configuration.
    dyncfg: Arc<ConfigSet>,
}

impl<T: ComputeControllerTimestamp> Instance<T> {
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

    /// Acquire a handle to the collection state associated with `id`.
    ///
    /// # Panics
    ///
    /// Panics if the identified collection does not exist.
    pub fn expect_collection(&self, id: GlobalId) -> &CollectionState<T> {
        self.collections.get(&id).expect("collection must exist")
    }

    /// Acquire a mutable handle to the collection state associated with `id`.
    ///
    /// # Panics
    ///
    /// Panics if the identified collection does not exist.
    fn expect_collection_mut(&mut self, id: GlobalId) -> &mut CollectionState<T> {
        self.collections
            .get_mut(&id)
            .expect("collection must exist")
    }

    pub fn collections_iter(&self) -> impl Iterator<Item = (&GlobalId, &CollectionState<T>)> {
        self.collections.iter()
    }

    /// Add a collection to the instance state.
    ///
    /// # Panics
    ///
    /// Panics if a collection with the same ID exists already.
    fn add_collection(
        &mut self,
        id: GlobalId,
        as_of: Antichain<T>,
        storage_dependencies: Vec<GlobalId>,
        compute_dependencies: Vec<GlobalId>,
        write_only: bool,
        initial_as_of: Option<Antichain<T>>,
        refresh_schedule: Option<RefreshSchedule>,
    ) {
        // Add global collection state.
        let mut state = CollectionState::new(
            id,
            as_of.clone(),
            storage_dependencies,
            compute_dependencies,
            self.introspection_tx.clone(),
            initial_as_of,
            refresh_schedule,
        );
        // If the collection is write-only, clear its read policy to reflect that.
        if write_only {
            state.read_policy = None;
        }

        if let Some(previous) = self.collections.insert(id, state) {
            panic!("attempt to add a collection with existing ID {id} (previous={previous:?}");
        }

        // Add per-replica collection state.
        for replica in self.replicas.values_mut() {
            replica.add_collection(id, as_of.clone());
        }

        // Update introspection.
        self.report_dependency_updates(id, 1);
    }

    fn remove_collection(&mut self, id: GlobalId) {
        // Update introspection.
        self.report_dependency_updates(id, -1);

        // Remove per-replica collection state.
        for replica in self.replicas.values_mut() {
            replica.remove_collection(id);
        }

        // Remove global collection state.
        self.collections.remove(&id);
    }

    fn add_replica_state(
        &mut self,
        id: ReplicaId,
        client: ReplicaClient<T>,
        config: ReplicaConfig,
    ) {
        let metrics = self.metrics.for_replica(id);
        let mut replica =
            ReplicaState::new(id, client, config, metrics, self.introspection_tx.clone());

        // Add per-replica collection state.
        for (collection_id, collection) in &self.collections {
            let as_of = collection.read_frontier().to_owned();
            replica.add_collection(*collection_id, as_of);
        }

        self.replicas.insert(id, replica);
    }

    fn acquire_storage_read_hold_at(
        &self,
        id: GlobalId,
        frontier: Antichain<T>,
    ) -> Result<ReadHold<T>, ReadHoldError> {
        let mut hold = self
            .storage_collections
            .acquire_read_holds(vec![id])?
            .into_element();
        hold.try_downgrade(frontier)
            .map_err(|_| ReadHoldError::SinceViolation(id))?;
        Ok(hold)
    }

    /// Enqueue the given response for delivery to the controller clients.
    fn deliver_response(&mut self, response: ComputeControllerResponse<T>) {
        self.response_tx
            .send(response)
            .expect("global controller never drops");
    }

    /// Enqueue the given introspection updates for recording.
    fn deliver_introspection_updates(
        &mut self,
        type_: IntrospectionType,
        updates: Vec<(Row, Diff)>,
    ) {
        self.introspection_tx
            .send((type_, updates))
            .expect("global controller never drops");
    }

    /// Returns whether the identified replica exists.
    pub fn replica_exists(&self, id: ReplicaId) -> bool {
        self.replicas.contains_key(&id)
    }

    /// Returns the ids of all replicas of this instance.
    pub fn replica_ids(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.replicas.keys().copied()
    }

    /// Return the IDs of pending peeks targeting the specified replica.
    fn peeks_targeting(
        &self,
        replica_id: ReplicaId,
    ) -> impl Iterator<Item = (Uuid, &PendingPeek<T>)> {
        self.peeks.iter().filter_map(move |(uuid, peek)| {
            if peek.target_replica == Some(replica_id) {
                Some((*uuid, peek))
            } else {
                None
            }
        })
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
    fn refresh_state_metrics(&self) {
        let unscheduled_collections_count =
            self.collections.values().filter(|c| !c.scheduled).count();

        self.metrics
            .replica_count
            .set(u64::cast_from(self.replicas.len()));
        self.metrics
            .collection_count
            .set(u64::cast_from(self.collections.len()));
        self.metrics
            .collection_unscheduled_count
            .set(u64::cast_from(unscheduled_collections_count));
        self.metrics
            .peek_count
            .set(u64::cast_from(self.peeks.len()));
        self.metrics
            .subscribe_count
            .set(u64::cast_from(self.subscribes.len()));
        self.metrics
            .copy_to_count
            .set(u64::cast_from(self.copy_tos.len()));
    }

    /// Report updates (inserts or retractions) to the identified collection's dependencies.
    ///
    /// # Panics
    ///
    /// Panics if the identified collection does not exist.
    fn report_dependency_updates(&mut self, id: GlobalId, diff: i64) {
        let collection = self.expect_collection(id);
        let dependencies = collection.dependency_ids();

        let updates = dependencies
            .map(|dependency_id| {
                let row = Row::pack_slice(&[
                    Datum::String(&id.to_string()),
                    Datum::String(&dependency_id.to_string()),
                ]);
                (row, diff)
            })
            .collect();

        self.deliver_introspection_updates(IntrospectionType::ComputeDependencies, updates);
    }

    /// Update the tracked hydration status for the given collection and replica according to an
    /// observed frontier update.
    fn update_hydration_status(
        &mut self,
        id: GlobalId,
        replica_id: ReplicaId,
        frontier: &Antichain<T>,
    ) {
        let Some(replica) = self.replicas.get_mut(&replica_id) else {
            tracing::error!(
                %id, %replica_id, frontier = ?frontier.elements(),
                "frontier update for an unknown replica"
            );
            return;
        };
        let Some(collection) = replica.collections.get_mut(&id) else {
            tracing::error!(
                %id, %replica_id, frontier = ?frontier.elements(),
                "frontier update for an unknown collection"
            );
            return;
        };

        // We may have already reported successful hydration before, in which case we have nothing
        // left to do.
        if collection.hydrated() {
            return;
        }

        // If the observed frontier is greater than the collection's as-of, the collection has
        // produced some output and is therefore hydrated now.
        if PartialOrder::less_than(&collection.as_of, frontier) {
            collection.set_hydrated();
        }
    }

    /// Update the tracked hydration status for an operator according to a received status update.
    fn update_operator_hydration_status(
        &mut self,
        replica_id: ReplicaId,
        status: OperatorHydrationStatus,
    ) {
        let Some(replica) = self.replicas.get_mut(&replica_id) else {
            tracing::error!(
                %replica_id, ?status,
                "status update for an unknown replica"
            );
            return;
        };
        let Some(collection) = replica.collections.get_mut(&status.collection_id) else {
            tracing::error!(
                %replica_id, ?status,
                "status update for an unknown collection"
            );
            return;
        };

        collection.hydration_state.operator_hydrated(
            status.lir_id,
            status.worker_id,
            status.hydrated,
        );
    }

    /// Clean up collection state that is not needed anymore.
    ///
    /// Three conditions need to be true before we can remove state for a collection:
    ///
    ///  1. A client must have explicitly dropped the collection. If that is not the case, clients
    ///     can still reasonably assume that the controller knows about the collection and can
    ///     answer queries about it.
    ///  2. There must be no outstanding read capabilities on the collection. As long as someone
    ///     still holds read capabilities on a collection, we need to keep it around to be able
    ///     to properly handle downgrading of said capabilities.
    ///  3. All replica frontiers for the collection must have advanced to the empty frontier.
    ///     Advancement to the empty frontiers signals that replicas are done computing the
    ///     collection and that they won't send more `ComputeResponse`s for it. As long as we might
    ///     receive responses for a collection we want to keep it around to be able to validate and
    ///     handle these responses.
    fn cleanup_collections(&mut self) {
        let to_remove: Vec<_> = self
            .collections_iter()
            .filter(|(_id, collection)| {
                collection.dropped
                    && collection.read_frontier().is_empty()
                    && collection
                        .replica_write_frontiers
                        .values()
                        .all(|frontier| frontier.is_empty())
                    && collection
                        .replica_input_frontiers
                        .values()
                        .all(|frontier| frontier.is_empty())
            })
            .map(|(id, _collection)| *id)
            .collect();

        for id in to_remove {
            self.remove_collection(id);
        }
    }

    /// List compute collections that depend on the given collection.
    pub fn collection_reverse_dependencies(&self, id: GlobalId) -> impl Iterator<Item = &GlobalId> {
        self.collections_iter().filter_map(move |(id2, state)| {
            if state.compute_dependencies.contains(&id) {
                Some(id2)
            } else {
                None
            }
        })
    }

    /// Returns the state of the [`Instance`] formatted as JSON.
    ///
    /// The returned value is not guaranteed to be stable and may change at any point in time.
    pub(crate) fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Note: We purposefully use the `Debug` formatting for the value of all fields in the
        // returned object as a tradeoff between usability and stability. `serde_json` will fail
        // to serialize an object if the keys aren't strings, so `Debug` formatting the values
        // prevents a future unrelated change from silently breaking this method.

        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            build_info: _,
            storage_collections: _,
            initialized,
            read_only,
            replicas,
            collections,
            log_sources: _,
            peeks,
            subscribes,
            copy_tos,
            history: _,
            response_tx: _,
            introspection_tx: _,
            envd_epoch,
            replica_epochs,
            transient_id_gen: _,
            metrics: _,
            dyncfg: _,
        } = self;

        fn field(
            key: &str,
            value: impl Serialize,
        ) -> Result<(String, serde_json::Value), anyhow::Error> {
            let value = serde_json::to_value(value)?;
            Ok((key.to_string(), value))
        }

        let replicas: BTreeMap<_, _> = replicas
            .iter()
            .map(|(id, replica)| Ok((id.to_string(), replica.dump()?)))
            .collect::<Result<_, anyhow::Error>>()?;
        let collections: BTreeMap<_, _> = collections
            .iter()
            .map(|(id, collection)| (id.to_string(), format!("{collection:?}")))
            .collect();
        let peeks: BTreeMap<_, _> = peeks
            .iter()
            .map(|(uuid, peek)| (uuid.to_string(), format!("{peek:?}")))
            .collect();
        let subscribes: BTreeMap<_, _> = subscribes
            .iter()
            .map(|(id, subscribe)| (id.to_string(), format!("{subscribe:?}")))
            .collect();
        let copy_tos: Vec<_> = copy_tos.iter().map(|id| id.to_string()).collect();
        let replica_epochs: BTreeMap<_, _> = replica_epochs
            .iter()
            .map(|(id, epoch)| (id.to_string(), epoch))
            .collect();

        let map = serde_json::Map::from_iter([
            field("initialized", initialized)?,
            field("read_only", read_only)?,
            field("replicas", replicas)?,
            field("collections", collections)?,
            field("peeks", peeks)?,
            field("subscribes", subscribes)?,
            field("copy_tos", copy_tos)?,
            field("envd_epoch", envd_epoch)?,
            field("replica_epochs", replica_epochs)?,
        ]);
        Ok(serde_json::Value::Object(map))
    }
}

impl<T> Instance<T>
where
    T: ComputeControllerTimestamp,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub fn new(
        build_info: &'static BuildInfo,
        storage: Arc<dyn StorageCollections<Timestamp = T>>,
        arranged_logs: BTreeMap<LogVariant, GlobalId>,
        envd_epoch: NonZeroI64,
        transient_id_gen: Arc<TransientIdGen>,
        metrics: InstanceMetrics,
        dyncfg: Arc<ConfigSet>,
        response_tx: crossbeam_channel::Sender<ComputeControllerResponse<T>>,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    ) -> Self {
        let collections = arranged_logs
            .iter()
            .map(|(_, id)| {
                let state =
                    CollectionState::new_log_collection(id.clone(), introspection_tx.clone());
                (*id, state)
            })
            .collect();
        let history = ComputeCommandHistory::new(metrics.for_history());

        let mut instance = Self {
            build_info,
            storage_collections: storage,
            initialized: false,
            read_only: true,
            replicas: Default::default(),
            collections,
            log_sources: arranged_logs,
            peeks: Default::default(),
            subscribes: Default::default(),
            copy_tos: Default::default(),
            history,
            response_tx,
            introspection_tx,
            envd_epoch,
            replica_epochs: Default::default(),
            transient_id_gen,
            metrics,
            dyncfg,
        };

        instance.send(ComputeCommand::CreateTimely {
            config: TimelyConfig::default(),
            epoch: ClusterStartupEpoch::new(envd_epoch, 0),
        });

        let dummy_logging_config = Default::default();
        instance.send(ComputeCommand::CreateInstance(InstanceConfig {
            logging: dummy_logging_config,
        }));

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

    /// Allows this instance to affect writes to external systems (persist).
    ///
    /// Calling this method repeatedly has no effect.
    pub fn allow_writes(&mut self) {
        if self.read_only {
            self.read_only = false;
            self.send(ComputeCommand::AllowWrites);
        }
    }

    /// Drop this compute instance.
    ///
    /// # Panics
    ///
    /// Panics if the compute instance still has active replicas.
    /// Panics if the compute instance still has collections installed.
    pub fn drop(mut self) {
        // Collections might have been dropped but not cleaned up yet.
        self.cleanup_collections();

        assert!(
            self.replicas.is_empty(),
            "cannot drop instances with provisioned replicas"
        );
        assert!(
            self.collections.values().all(|c| c.log_collection),
            "cannot drop instances with installed collections"
        );
    }

    /// Sends a command to all replicas of this instance.
    #[mz_ore::instrument(level = "debug")]
    pub fn send(&mut self, cmd: ComputeCommand<T>) {
        // Record the command so that new replicas can be brought up to speed.
        self.history.push(cmd.clone());

        // Clone the command for each active replica.
        for replica in self.replicas.values_mut() {
            // If sending the command fails, the replica requires rehydration.
            if replica.client.send(cmd.clone()).is_err() {
                replica.failed = true;
            }
        }
    }

    /// Receives the next response from any replica of this instance.
    ///
    /// This method is cancellation safe.
    pub async fn recv(&mut self) -> (ReplicaId, ComputeResponse<T>) {
        loop {
            let live_replicas = self.replicas.iter_mut().filter(|(_, r)| !r.failed);
            let response = live_replicas
                .map(|(id, replica)| async { (*id, replica.client.recv().await) })
                .collect::<FuturesUnordered<_>>()
                .next()
                .await;

            match response {
                None => {
                    // There are no live replicas left.
                    // Block forever to communicate that no response is ready.
                    future::pending().await
                }
                Some((replica_id, None)) => {
                    // A replica has failed and requires rehydration.
                    let replica = self.replicas.get_mut(&replica_id).unwrap();
                    replica.failed = true;
                }
                Some((replica_id, Some(response))) => {
                    // A replica has produced a response. Return it.
                    return (replica_id, response);
                }
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

    /// Add a new instance replica, by ID.
    pub fn add_replica(
        &mut self,
        id: ReplicaId,
        mut config: ReplicaConfig,
    ) -> Result<(), ReplicaExists> {
        if self.replica_exists(id) {
            return Err(ReplicaExists(id));
        }

        config.logging.index_logs = self.log_sources.clone();
        let log_ids: BTreeSet<_> = config.logging.index_logs.values().collect();

        // Initialize frontier tracking for the new replica.
        let mut updates = BTreeMap::new();
        for (compute_id, collection) in &mut self.collections {
            // Skip log collections not maintained by this replica.
            if collection.log_collection && !log_ids.contains(compute_id) {
                continue;
            }

            let read_frontier = collection.read_frontier();
            updates.insert(*compute_id, read_frontier.to_owned());
        }
        self.update_replica_write_frontiers(id, &updates);
        self.update_replica_input_frontiers(id, &updates);

        let replica_epoch = self.replica_epochs.entry(id).or_default();
        *replica_epoch += 1;
        let metrics = self.metrics.for_replica(id);
        let client = ReplicaClient::spawn(
            id,
            self.build_info,
            config.clone(),
            ClusterStartupEpoch::new(self.envd_epoch, *replica_epoch),
            metrics.clone(),
            Arc::clone(&self.dyncfg),
        );

        // Take this opportunity to clean up the history we should present.
        self.history.reduce();

        // Replay the commands at the client, creating new dataflow identifiers.
        for command in self.history.iter() {
            if client.send(command.clone()).is_err() {
                // We swallow the error here. On the next send, we will fail again, and
                // restart the connection as well as this rehydration.
                tracing::warn!("Replica {:?} connection terminated during hydration", id);
                break;
            }
        }

        // Add replica to tracked state.
        self.add_replica_state(id, client, config);

        Ok(())
    }

    /// Remove an existing instance replica, by ID.
    pub fn remove_replica(&mut self, id: ReplicaId) -> Result<(), ReplicaMissing> {
        self.replicas.remove(&id).ok_or(ReplicaMissing(id))?;

        // Remove frontier tracking for this replica.
        self.remove_replica_frontiers(id);

        // Subscribes targeting this replica either won't be served anymore (if the replica is
        // dropped) or might produce inconsistent output (if the target collection is an
        // introspection index). We produce an error to inform upstream.
        let to_drop: Vec<_> = self.subscribes_targeting(id).collect();
        for subscribe_id in to_drop {
            let subscribe = self.subscribes.remove(&subscribe_id).unwrap();
            let response = ComputeControllerResponse::SubscribeResponse(
                subscribe_id,
                SubscribeBatch {
                    lower: subscribe.frontier.clone(),
                    upper: subscribe.frontier,
                    updates: Err("target replica failed or was dropped".into()),
                },
            );
            self.deliver_response(response);
        }

        // Peeks targeting this replica might not be served anymore (if the replica is dropped).
        // If the replica has failed it might come back and respond to the peek later, but it still
        // seems like a good idea to cancel the peek to inform the caller about the failure. This
        // is consistent with how we handle targeted subscribes above.
        let mut peek_responses = Vec::new();
        let mut to_drop = Vec::new();
        for (uuid, peek) in self.peeks_targeting(id) {
            peek_responses.push(ComputeControllerResponse::PeekResponse(
                uuid,
                PeekResponse::Error("target replica failed or was dropped".into()),
                peek.otel_ctx.clone(),
            ));
            to_drop.push(uuid);
        }
        for response in peek_responses {
            self.deliver_response(response);
        }
        to_drop.into_iter().for_each(|uuid| self.remove_peek(uuid));

        Ok(())
    }

    /// Rehydrate the given instance replica.
    ///
    /// # Panics
    ///
    /// Panics if the specified replica does not exist.
    fn rehydrate_replica(&mut self, id: ReplicaId) {
        let config = self.replicas[&id].config.clone();
        self.remove_replica(id).expect("replica must exist");
        let result = self.add_replica(id, config);

        match result {
            Ok(()) => (),
            Err(ReplicaExists(_)) => unreachable!("replica was removed"),
        }
    }

    /// Rehydrate any failed replicas of this instance.
    fn rehydrate_failed_replicas(&mut self) {
        let replicas = self.replicas.iter();
        let failed_replicas: Vec<_> = replicas
            .filter_map(|(id, replica)| replica.failed.then_some(*id))
            .collect();

        for replica_id in failed_replicas {
            self.rehydrate_replica(replica_id);
        }
    }

    /// Create the described dataflows and initializes state for their output.
    pub fn create_dataflow(
        &mut self,
        dataflow: DataflowDescription<mz_compute_types::plan::Plan<T>, (), T>,
    ) -> Result<(), DataflowCreationError> {
        // Simple sanity checks around `as_of`
        let as_of = dataflow
            .as_of
            .as_ref()
            .ok_or(DataflowCreationError::MissingAsOf)?;
        if as_of.is_empty() && dataflow.subscribe_ids().next().is_some() {
            return Err(DataflowCreationError::EmptyAsOfForSubscribe);
        }
        if as_of.is_empty() && dataflow.copy_to_ids().next().is_some() {
            return Err(DataflowCreationError::EmptyAsOfForCopyTo);
        }

        // Validate the dataflow as having inputs whose `since` is less or equal to the dataflow's `as_of`.
        // Start tracking frontiers for each dataflow, using its `as_of` for each index and sink.

        // When we initialize per-replica input frontiers (and thereby the per-replica read
        // capabilities), we cannot use the `as_of` because of reconciliation: Existing
        // slow replicas might be reading from the inputs at times before the `as_of` and we
        // would rather not crash them by allowing their inputs to compact too far. So instead
        // we initialize the per-replica write frontiers with the smallest possible value that
        // is a valid read capability for all inputs, which is the join of all input `since`s.
        let mut replica_input_frontier = Antichain::from_elem(T::minimum());

        // Record all transitive dependencies of the outputs.
        let mut storage_dependencies = Vec::new();
        let mut compute_dependencies = Vec::new();

        // Any potentially acquired STORAGE read holds. We acquire them and
        // check whether our as_of is valid. They are dropped once we installed
        // read capabilities manually.
        //
        // TODO: Instead of acquiring these and then dropping later, we should
        // instead store them and don't "manually" acquire read holds using
        // `update_read_capabilities`.
        let mut storage_read_holds = Vec::new();

        // Validate sources have `since.less_equal(as_of)`.
        for source_id in dataflow.source_imports.keys() {
            let storage_read_hold = self.acquire_storage_read_hold_at(*source_id, as_of.clone())?;

            storage_dependencies.push(*source_id);
            replica_input_frontier.join_assign(storage_read_hold.since());

            storage_read_holds.push(storage_read_hold);
        }

        // Validate indexes have `since.less_equal(as_of)`.
        // TODO(mcsherry): Instead, return an error from the constructing method.
        for index_id in dataflow.index_imports.keys() {
            let collection = self.collection(*index_id)?;
            let since = collection.read_capabilities.frontier();
            if !(timely::order::PartialOrder::less_equal(&since, &as_of.borrow())) {
                Err(DataflowCreationError::SinceViolation(*index_id))?;
            }

            compute_dependencies.push(*index_id);
            replica_input_frontier.join_assign(&since.to_owned());
        }

        // If the `as_of` is empty, we are not going to create a dataflow, so replicas won't read
        // from the inputs.
        if as_of.is_empty() {
            replica_input_frontier = Antichain::new();
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
        self.storage_collections
            .update_read_capabilities(&mut storage_read_updates);
        // Drop the acquired read holds after we installed our old-style, manual
        // read capabilities.
        drop(storage_read_holds);

        // Update compute read capabilities for inputs.
        let compute_read_updates = compute_dependencies
            .iter()
            .map(|id| (*id, changes.clone()))
            .collect();
        self.update_read_capabilities(compute_read_updates);

        // Install collection state for each of the exports.
        for export_id in dataflow.export_ids() {
            let write_only = dataflow.sink_exports.contains_key(&export_id);
            self.add_collection(
                export_id,
                as_of.clone(),
                storage_dependencies.clone(),
                compute_dependencies.clone(),
                write_only,
                dataflow.initial_storage_as_of.clone(),
                dataflow.refresh_schedule.clone(),
            );
        }

        // Initialize tracking of replica frontiers.
        let replica_write_frontier_updates = dataflow
            .export_ids()
            .map(|id| (id, as_of.clone()))
            .collect();
        let replica_input_frontier_updates = dataflow
            .export_ids()
            .map(|id| (id, replica_input_frontier.clone()))
            .collect();
        let replica_ids: Vec<_> = self.replica_ids().collect();
        for replica_id in replica_ids {
            self.update_replica_write_frontiers(replica_id, &replica_write_frontier_updates);
            self.update_replica_input_frontiers(replica_id, &replica_input_frontier_updates);
        }

        // Initialize tracking of subscribes.
        for subscribe_id in dataflow.subscribe_ids() {
            self.subscribes.insert(subscribe_id, ActiveSubscribe::new());
        }

        // Initialize tracking of copy tos.
        for copy_to_id in dataflow.copy_to_ids() {
            self.copy_tos.insert(copy_to_id);
        }

        // Here we augment all imported sources and all exported sinks with the appropriate
        // storage metadata needed by the compute instance.
        let mut source_imports = BTreeMap::new();
        for (id, (si, monotonic)) in dataflow.source_imports {
            let collection_metadata = self
                .storage_collections
                .collection_metadata(id)
                .map_err(|_| DataflowCreationError::CollectionMissing(id))?;

            let desc = SourceInstanceDesc {
                storage_metadata: collection_metadata.clone(),
                arguments: si.arguments,
                typ: si.typ.clone(),
            };
            source_imports.insert(id, (desc, monotonic));
        }

        let mut sink_exports = BTreeMap::new();
        for (id, se) in dataflow.sink_exports {
            let connection = match se.connection {
                ComputeSinkConnection::Persist(conn) => {
                    let metadata = self
                        .storage_collections
                        .collection_metadata(id)
                        .map_err(|_| DataflowCreationError::CollectionMissing(id))?
                        .clone();
                    let conn = PersistSinkConnection {
                        value_desc: conn.value_desc,
                        storage_metadata: metadata,
                    };
                    ComputeSinkConnection::Persist(conn)
                }
                ComputeSinkConnection::Subscribe(conn) => ComputeSinkConnection::Subscribe(conn),
                ComputeSinkConnection::CopyToS3Oneshot(conn) => {
                    ComputeSinkConnection::CopyToS3Oneshot(conn)
                }
            };
            let desc = ComputeSinkDesc {
                from: se.from,
                from_desc: se.from_desc,
                connection,
                with_snapshot: se.with_snapshot,
                up_to: se.up_to,
                non_null_assertions: se.non_null_assertions,
                refresh_schedule: se.refresh_schedule,
            };
            sink_exports.insert(id, desc);
        }

        // Flatten the dataflow plans into the representation expected by replicas.
        let objects_to_build = dataflow
            .objects_to_build
            .into_iter()
            .map(|object| BuildDesc {
                id: object.id,
                plan: FlatPlan::from(object.plan),
            })
            .collect();

        let augmented_dataflow = DataflowDescription {
            source_imports,
            sink_exports,
            objects_to_build,
            // The rest of the fields are identical
            index_imports: dataflow.index_imports,
            index_exports: dataflow.index_exports,
            as_of: dataflow.as_of.clone(),
            until: dataflow.until,
            initial_storage_as_of: dataflow.initial_storage_as_of,
            refresh_schedule: dataflow.refresh_schedule,
            debug_name: dataflow.debug_name,
        };

        if augmented_dataflow.is_transient() {
            tracing::debug!(
                name = %augmented_dataflow.debug_name,
                import_ids = %augmented_dataflow.display_import_ids(),
                export_ids = %augmented_dataflow.display_export_ids(),
                as_of = ?augmented_dataflow.as_of.as_ref().unwrap().elements(),
                until = ?augmented_dataflow.until.elements(),
                "creating dataflow",
            );
        } else {
            tracing::info!(
                name = %augmented_dataflow.debug_name,
                import_ids = %augmented_dataflow.display_import_ids(),
                export_ids = %augmented_dataflow.display_export_ids(),
                as_of = ?augmented_dataflow.as_of.as_ref().unwrap().elements(),
                until = ?augmented_dataflow.until.elements(),
                "creating dataflow",
            );
        }

        // Skip the actual dataflow creation for an empty `as_of`. (Happens e.g. for the
        // bootstrapping of a REFRESH AT mat view that is past its last refresh.)
        if as_of.is_empty() {
            tracing::info!(
                name = %augmented_dataflow.debug_name,
                "not sending `CreateDataflow`, because of empty `as_of`",
            );
        } else {
            let collections: Vec<_> = augmented_dataflow.export_ids().collect();
            self.send(ComputeCommand::CreateDataflow(augmented_dataflow));

            for id in collections {
                self.maybe_schedule_collection(id);
            }
        }

        Ok(())
    }

    /// Schedule the identified collection if all its inputs are available.
    ///
    /// # Panics
    ///
    /// Panics if the identified collection does not exist.
    fn maybe_schedule_collection(&mut self, id: GlobalId) {
        let collection = self.expect_collection(id);

        // Don't schedule collections twice.
        if collection.scheduled {
            return;
        }

        let as_of = collection.read_frontier();

        // If the collection has an empty `as_of`, it was either never installed on the replica or
        // has since been dropped. In either case the replica does not expect any commands for it.
        if as_of.is_empty() {
            return;
        }

        let ready = if id.is_transient() {
            // Always schedule transient collections immediately. The assumption is that those are
            // created by interactive user commands and we want to schedule them as quickly as
            // possible. Inputs might not yet be available, but when they become available, we
            // don't need to wait for the controller to become aware and for the scheduling check
            // to run again.
            true
        } else {
            // Check dependency frontiers to determine if all inputs are
            // available. An input is available when its frontier is greater
            // than the `as_of`, i.e., all input data up to and including the
            // `as_of` has been sealed.
            let compute_frontiers = collection.compute_dependencies.iter().map(|id| {
                let dep = &self.expect_collection(*id);
                &dep.write_frontier
            });

            let storage_frontiers = self
                .storage_collections
                .collections_frontiers(collection.storage_dependencies.clone())
                .expect("must exist");
            let storage_frontiers = storage_frontiers.iter().map(|f| &f.write_frontier);

            let ready = compute_frontiers
                .chain(storage_frontiers)
                .all(|frontier| PartialOrder::less_than(&as_of, &frontier.borrow()));

            ready
        };

        if ready {
            self.send(ComputeCommand::Schedule(id));
            let collection = self.expect_collection_mut(id);
            collection.scheduled = true;
        }
    }

    /// Schedule any unscheduled collections that are ready.
    fn schedule_collections(&mut self) {
        let ids: Vec<_> = self.collections.keys().copied().collect();
        for id in ids {
            self.maybe_schedule_collection(id);
        }
    }

    /// Drops the read capability for the given collections and allows their resources to be
    /// reclaimed.
    pub fn drop_collections(&mut self, ids: Vec<GlobalId>) -> Result<(), CollectionMissing> {
        let mut read_capability_updates = BTreeMap::new();

        for id in &ids {
            let collection = self.collection_mut(*id)?;

            // Mark the collection as dropped to allow it to be removed from the controller state.
            collection.dropped = true;

            // Drop the implied and warmup read capabilities to announce that clients are not
            // interested in the collection anymore.
            let implied_capability = std::mem::take(&mut collection.implied_capability);
            let warmup_capability = std::mem::take(&mut collection.warmup_capability);
            let mut update = ChangeBatch::new();
            update.extend(implied_capability.iter().map(|t| (t.clone(), -1)));
            update.extend(warmup_capability.iter().map(|t| (t.clone(), -1)));
            read_capability_updates.insert(*id, update);

            // If the collection is a subscribe, stop tracking it. This ensures that the controller
            // ceases to produce `SubscribeResponse`s for this subscribe.
            self.subscribes.remove(id);
            // If the collection is a copy to, stop tracking it. This ensures that the controller
            // ceases to produce `CopyToResponse`s` for this copy to.
            self.copy_tos.remove(id);
        }

        if !read_capability_updates.is_empty() {
            self.update_read_capabilities(read_capability_updates);
        }

        Ok(())
    }

    /// Initiate a peek request for the contents of `id` at `timestamp`.
    #[mz_ore::instrument(level = "debug")]
    pub fn peek(
        &mut self,
        id: GlobalId,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_replica: Option<ReplicaId>,
        peek_target: PeekTarget,
    ) -> Result<(), PeekError> {
        // When querying persist directly, we acquire read holds and verify that
        // we can actually acquire them at the right time.
        let mut maybe_storage_read_hold = None;
        match &peek_target {
            PeekTarget::Index { .. } => {
                let since = self.collection(id)?.read_capabilities.frontier();
                if !since.less_equal(&timestamp) {
                    return Err(PeekError::SinceViolation(id));
                }
            }

            PeekTarget::Persist { .. } => {
                let storage_read_hold =
                    self.acquire_storage_read_hold_at(id, Antichain::from_elem(timestamp.clone()))?;

                maybe_storage_read_hold = Some(storage_read_hold);
            }
        }

        if let Some(target) = target_replica {
            if !self.replica_exists(target) {
                return Err(PeekError::ReplicaMissing(target));
            }
        }

        // Install a compaction hold on `id` at `timestamp`.
        let mut updates = BTreeMap::new();
        updates.insert(id, ChangeBatch::new_from(timestamp.clone(), 1));
        match &peek_target {
            PeekTarget::Index { .. } => self.update_read_capabilities(updates),
            PeekTarget::Persist { .. } => self
                .storage_collections
                .update_read_capabilities(&mut updates),
        };

        // Drop the acquired read hold after we installed our old-style, manual
        // read capabilities.
        drop(maybe_storage_read_hold);

        let otel_ctx = OpenTelemetryContext::obtain();
        self.peeks.insert(
            uuid,
            PendingPeek {
                target: peek_target.clone(),
                time: timestamp.clone(),
                target_replica,
                // TODO(guswynn): can we just hold the `tracing::Span` here instead?
                otel_ctx: otel_ctx.clone(),
                requested_at: Instant::now(),
            },
        );

        self.send(ComputeCommand::Peek(Peek {
            literal_constraints,
            uuid,
            timestamp,
            finishing,
            map_filter_project,
            // Obtain an `OpenTelemetryContext` from the thread-local tracing
            // tree to forward it on to the compute worker.
            otel_ctx,
            target: peek_target,
        }));

        Ok(())
    }

    /// Cancels an existing peek request.
    pub fn cancel_peek(&mut self, uuid: Uuid) {
        let Some(peek) = self.peeks.get_mut(&uuid) else {
            tracing::warn!("did not find pending peek for {uuid}");
            return;
        };

        let response = PeekResponse::Canceled;
        let duration = peek.requested_at.elapsed();
        self.metrics.observe_peek_response(&response, duration);

        // Enqueue the response to the cancellation.
        let otel_ctx = peek.otel_ctx.clone();
        self.deliver_response(ComputeControllerResponse::PeekResponse(
            uuid, response, otel_ctx,
        ));

        // Remove the peek.
        // This will also propagate the cancellation to the replicas.
        self.remove_peek(uuid);
    }

    /// Assigns a read policy to specific identifiers.
    ///
    /// The policies are assigned in the order presented, and repeated identifiers should
    /// conclude with the last policy. Changing a policy will immediately downgrade the read
    /// capability if appropriate, but it will not "recover" the read capability if the prior
    /// capability is already ahead of it.
    ///
    /// Identifiers not present in `policies` retain their existing read policies.
    ///
    /// It is an error to attempt to set a read policy for a collection that is not readable in the
    /// context of compute. At this time, only indexes are readable compute collections.
    #[mz_ore::instrument(level = "debug")]
    pub fn set_read_policy(
        &mut self,
        policies: Vec<(GlobalId, ReadPolicy<T>)>,
    ) -> Result<(), ReadPolicyError> {
        // Do error checking upfront, to avoid introducing inconsistencies between a collection's
        // `implied_capability` and `read_capabilities`.
        for (id, _policy) in &policies {
            let collection = self.collection(*id)?;
            if collection.read_policy.is_none() {
                return Err(ReadPolicyError::WriteOnlyCollection(*id));
            }
        }

        let mut read_capability_changes = BTreeMap::default();
        for (id, new_policy) in policies {
            let collection = self.expect_collection_mut(id);

            let old_capability = &collection.implied_capability;
            let new_capability = new_policy.frontier(collection.write_frontier.borrow());
            if PartialOrder::less_than(old_capability, &new_capability) {
                let entry = read_capability_changes
                    .entry(id)
                    .or_insert_with(ChangeBatch::new);
                entry.extend(old_capability.iter().map(|t| (t.clone(), -1)));
                entry.extend(new_capability.iter().map(|t| (t.clone(), 1)));
                collection.implied_capability = new_capability;
            }

            collection.read_policy = Some(new_policy);
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(read_capability_changes);
        }
        Ok(())
    }

    /// Accept write frontier updates from the compute layer.
    ///
    /// # Panics
    ///
    /// Panics if any of the `updates` references an absent collection.
    /// Panics if any of the `updates` regresses an existing write frontier.
    #[mz_ore::instrument(level = "debug")]
    fn update_write_frontiers(
        &mut self,
        replica_id: ReplicaId,
        updates: &BTreeMap<GlobalId, Antichain<T>>,
    ) {
        // Apply advancements of replica frontiers.
        self.update_replica_write_frontiers(replica_id, updates);

        // Apply advancements of global collection frontiers.
        self.maybe_update_global_write_frontiers(updates);
    }

    /// Apply replica write frontier updates.
    ///
    /// # Panics
    ///
    /// Panics if any of the `updates` references an absent collection.
    /// Panics if any of the `updates` regresses an existing replica write frontier.
    #[mz_ore::instrument(level = "debug")]
    fn update_replica_write_frontiers(
        &mut self,
        replica_id: ReplicaId,
        updates: &BTreeMap<GlobalId, Antichain<T>>,
    ) {
        for (id, new_upper) in updates {
            let collection = self.expect_collection_mut(*id);

            let old_upper = collection
                .replica_write_frontiers
                .insert(replica_id, new_upper.clone());

            // Safety check against frontier regressions.
            if let Some(old) = &old_upper {
                assert!(
                    PartialOrder::less_equal(old, new_upper),
                    "replica frontier regression: {old:?} -> {new_upper:?}, \
                     collection={id}, replica={replica_id}",
                );
            }
        }
    }

    /// Apply replica input frontier updates.
    ///
    /// # Panics
    ///
    /// Panics if any of the `updates` references an absent collection.
    /// Panics if any of the `updates` regresses an existing replica input frontier.
    #[mz_ore::instrument(level = "debug")]
    fn update_replica_input_frontiers(
        &mut self,
        replica_id: ReplicaId,
        updates: &BTreeMap<GlobalId, Antichain<T>>,
    ) {
        // Compute and apply read hold downgrades on storage dependencies that result from
        // input frontier advancements.
        let mut storage_read_capability_changes = BTreeMap::default();
        for (id, new_cap) in updates {
            let collection = self.expect_collection_mut(*id);

            let old_cap = collection
                .replica_input_frontiers
                .insert(replica_id, new_cap.clone());

            // Safety check against frontier regressions.
            if let Some(old) = &old_cap {
                assert!(
                    PartialOrder::less_equal(old, new_cap),
                    "replica input frontier regression: {old:?} -> {new_cap:?}, \
                     collection={id}, replica={replica_id}",
                );
            }

            // Update per-replica read holds on storage dependencies.
            for storage_id in &collection.storage_dependencies {
                let update = storage_read_capability_changes
                    .entry(*storage_id)
                    .or_insert_with(|| ChangeBatch::new());
                if let Some(old) = &old_cap {
                    update.extend(old.iter().map(|time| (time.clone(), -1)));
                }
                update.extend(new_cap.iter().map(|time| (time.clone(), 1)));
            }
        }

        // Prune empty changes. We might end up with empty changes for dependencies that have been
        // dropped already, which is fine but might be confusing if we reported them.
        storage_read_capability_changes.retain(|_key, update| !update.is_empty());

        if !storage_read_capability_changes.is_empty() {
            self.storage_collections
                .update_read_capabilities(&mut storage_read_capability_changes);
        }
    }

    /// Remove frontier tracking state for the given replica.
    #[mz_ore::instrument(level = "debug")]
    fn remove_replica_frontiers(&mut self, replica_id: ReplicaId) {
        let mut storage_read_capability_changes = BTreeMap::default();
        for collection in self.collections.values_mut() {
            // Remove the tracked write frontier.
            collection.replica_write_frontiers.remove(&replica_id);

            // Remove the tracked input frontier and release any corresponding read holds on
            // storage dependencies.
            let last_cap = collection.replica_input_frontiers.remove(&replica_id);
            if let Some(frontier) = last_cap {
                if !frontier.is_empty() {
                    for storage_id in &collection.storage_dependencies {
                        let update = storage_read_capability_changes
                            .entry(*storage_id)
                            .or_insert_with(ChangeBatch::new);
                        update.extend(frontier.iter().map(|time| (time.clone(), -1)));
                    }
                }
            }
        }

        if !storage_read_capability_changes.is_empty() {
            self.storage_collections
                .update_read_capabilities(&mut storage_read_capability_changes);
        }
    }

    /// Apply global write frontier updates.
    ///
    /// Frontier regressions are gracefully ignored.
    ///
    /// # Panics
    ///
    /// Panics if any of the `updates` references an absent collection.
    #[mz_ore::instrument(level = "debug")]
    fn maybe_update_global_write_frontiers(&mut self, updates: &BTreeMap<GlobalId, Antichain<T>>) {
        // Compute and apply read capability downgrades that result from collection frontier
        // advancements.
        let mut read_capability_changes = BTreeMap::new();
        for (id, new_upper) in updates {
            let collection = self.expect_collection_mut(*id);

            if !PartialOrder::less_than(&collection.write_frontier, new_upper) {
                continue; // frontier has not advanced
            }

            collection.write_frontier.clone_from(new_upper);

            let old_since = &collection.implied_capability;
            let new_since = match &collection.read_policy {
                Some(read_policy) => {
                    // For readable collections the read frontier is determined by applying the
                    // client-provided read policy to the write frontier.
                    read_policy.frontier(new_upper.borrow())
                }
                None => {
                    // Write-only collections cannot be read within the context of the compute
                    // controller, so we can immediately advance their read frontier to the new write
                    // frontier.
                    new_upper.clone()
                }
            };

            if PartialOrder::less_than(old_since, &new_since) {
                let mut update = ChangeBatch::new();
                update.extend(old_since.iter().map(|t| (t.clone(), -1)));
                update.extend(new_since.iter().map(|t| (t.clone(), 1)));
                read_capability_changes.insert(*id, update);
                collection.implied_capability = new_since;
            }
        }
        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(read_capability_changes);
        }
    }

    /// Applies `updates`, propagates consequences through other read capabilities, and sends
    /// appropriate compaction commands.
    #[mz_ore::instrument(level = "debug")]
    fn update_read_capabilities(&mut self, mut updates: BTreeMap<GlobalId, ChangeBatch<T>>) {
        // Records storage read capability updates.
        let mut storage_updates = BTreeMap::default();
        // Records compute collections with downgraded read frontiers.
        let mut compute_downgraded = BTreeSet::default();

        // We must not rely on any specific relative ordering of `GlobalId`s.
        // That said, it is reasonable to assume that collections generally have greater IDs than
        // their dependencies, so starting with the largest is a useful optimization.
        while let Some((id, mut update)) = updates.pop_last() {
            let Some(collection) = self.collections.get_mut(&id) else {
                tracing::error!(
                    %id, ?update,
                    "received read capability update for an unknown collection",
                );
                continue;
            };

            // Sanity check to prevent corrupted `read_capabilities`, which can cause hard-to-debug
            // issues (usually stuck read frontiers).
            let read_frontier = collection.read_capabilities.frontier();
            for (time, diff) in update.iter() {
                let count = collection.read_capabilities.count_for(time) + diff;
                if count < 0 {
                    panic!(
                        "invalid read capabilities update for collection {id}: negative capability \
                         (read_capabilities={:?}, update={update:?}",
                        collection.read_capabilities
                    );
                } else if count > 0 && !read_frontier.less_equal(time) {
                    panic!(
                        "invalid read capabilities update for collection {id}: frontier regression \
                         (read_capabilities={:?}, update={update:?}",
                        collection.read_capabilities
                    );
                }
            }

            // Apply read capability updates and learn about resulting changes to the read
            // frontier.
            let changes = collection.read_capabilities.update_iter(update.drain());
            update.extend(changes);

            if update.is_empty() {
                continue; // read frontier did not change
            }

            compute_downgraded.insert(id);

            // Propagate read frontier updates to dependencies.
            for dep_id in &collection.compute_dependencies {
                updates
                    .entry(*dep_id)
                    .or_insert_with(ChangeBatch::new)
                    .extend(update.iter().cloned());
            }
            for dep_id in &collection.storage_dependencies {
                storage_updates
                    .entry(*dep_id)
                    .or_insert_with(ChangeBatch::new)
                    .extend(update.iter().cloned());
            }
        }

        // Produce `AllowCompaction` commands for collections that had read frontier downgrades.
        for id in compute_downgraded {
            let collection = self.expect_collection(id);
            let frontier = collection.read_frontier().to_owned();
            self.send(ComputeCommand::AllowCompaction { id, frontier });
        }

        // Report storage read capability updates.
        if !storage_updates.is_empty() {
            self.storage_collections
                .update_read_capabilities(&mut storage_updates);
        }
    }

    /// Removes a registered peek and clean up associated state.
    ///
    /// As part of this we:
    ///  * Emit a `CancelPeek` command to instruct replicas to stop spending resources on this
    ///    peek, and to allow the `ComputeCommandHistory` to reduce away the corresponding `Peek`
    ///    command.
    ///  * Remove the read hold for this peek, unblocking compaction that might have waited on it.
    fn remove_peek(&mut self, uuid: Uuid) {
        let Some(peek) = self.peeks.remove(&uuid) else {
            return;
        };

        // NOTE: We need to send the `CancelPeek` command _before_ we release the peek's read hold,
        // to avoid the edge case that caused #16615.
        self.send(ComputeCommand::CancelPeek { uuid });

        let change = ChangeBatch::new_from(peek.time, -1);
        match &peek.target {
            PeekTarget::Index { id } => self.update_read_capabilities([(*id, change)].into()),
            PeekTarget::Persist { id, .. } => {
                let mut updates = [(*id, change)].into();
                self.storage_collections
                    .update_read_capabilities(&mut updates);
            }
        }
    }

    pub fn handle_response(&mut self, response: ComputeResponse<T>, replica_id: ReplicaId) {
        match response {
            ComputeResponse::Frontiers(id, frontiers) => {
                self.handle_frontiers_response(id, frontiers, replica_id);
            }
            ComputeResponse::PeekResponse(uuid, peek_response, otel_ctx) => {
                self.handle_peek_response(uuid, peek_response, otel_ctx, replica_id);
            }
            ComputeResponse::CopyToResponse(id, response) => {
                self.handle_copy_to_response(id, response, replica_id);
            }
            ComputeResponse::SubscribeResponse(id, response) => {
                self.handle_subscribe_response(id, response, replica_id);
            }
            ComputeResponse::Status(response) => {
                self.handle_status_response(response, replica_id);
            }
        }
    }

    /// Handle new frontiers, returning any compute response that needs to
    /// be sent to the client.
    fn handle_frontiers_response(
        &mut self,
        id: GlobalId,
        frontiers: FrontiersResponse<T>,
        replica_id: ReplicaId,
    ) {
        // According to the compute protocol, replicas are not allowed to send `Frontiers`
        // responses that regress frontiers they have reported previously. We still perform a check
        // here, rather than risking the controller becoming confused trying to handle regressions.
        let Ok(coll) = self.collection(id) else {
            tracing::error!(
               %id, %replica_id, ?frontiers,
               "frontiers update for unknown collection",
            );
            return;
        };

        // Apply a write frontier advancement.
        if let Some(new_frontier) = frontiers.write_frontier {
            if let Some(old_frontier) = coll.replica_write_frontiers.get(&replica_id) {
                if !PartialOrder::less_equal(old_frontier, &new_frontier) {
                    tracing::error!(
                       %id, %replica_id, ?old_frontier, ?new_frontier,
                       "collection write frontier regression",
                    );
                    return;
                }
            }

            let old_global_frontier = coll.write_frontier.clone();

            self.collection_mut(id)
                .expect("we know about the collection")
                .collection_introspection
                .frontier_update(&new_frontier);
            self.update_write_frontiers(replica_id, &[(id, new_frontier)].into());

            if let Ok(coll) = self.collection(id) {
                if coll.write_frontier != old_global_frontier {
                    self.deliver_response(ComputeControllerResponse::FrontierUpper {
                        id,
                        upper: coll.write_frontier.clone(),
                    });
                }
            }
        }

        // Apply an input frontier advancement.
        if let Some(new_frontier) = frontiers.input_frontier {
            self.update_replica_input_frontiers(replica_id, &[(id, new_frontier)].into());
        }

        // Apply an output frontier advancement.
        if let Some(new_frontier) = frontiers.output_frontier {
            self.update_hydration_status(id, replica_id, &new_frontier);
        }
    }

    fn handle_peek_response(
        &mut self,
        uuid: Uuid,
        response: PeekResponse,
        otel_ctx: OpenTelemetryContext,
        replica_id: ReplicaId,
    ) {
        // We might not be tracking this peek anymore, because we have served a response already or
        // because it was canceled. If this is the case, we ignore the response.
        let Some(peek) = self.peeks.get(&uuid) else {
            return;
        };

        // If the peek is targeting a replica, ignore responses from other replicas.
        let target_replica = peek.target_replica.unwrap_or(replica_id);
        if target_replica != replica_id {
            return;
        }

        let duration = peek.requested_at.elapsed();
        self.metrics.observe_peek_response(&response, duration);

        self.remove_peek(uuid);

        // NOTE: We use the `otel_ctx` from the response, not the pending peek, because we
        // currently want the parent to be whatever the compute worker did with this peek.
        self.deliver_response(ComputeControllerResponse::PeekResponse(
            uuid, response, otel_ctx,
        ))
    }

    fn handle_copy_to_response(
        &mut self,
        sink_id: GlobalId,
        response: CopyToResponse,
        replica_id: ReplicaId,
    ) {
        // We might not be tracking this COPY TO because we have already returned a response
        // from one of the replicas. In that case, we ignore the response.
        if !self.copy_tos.remove(&sink_id) {
            return;
        }

        let result = match response {
            CopyToResponse::RowCount(count) => Ok(count),
            CopyToResponse::Error(error) => Err(anyhow::anyhow!(error)),
            // We should never get here: Replicas only drop copy to collections in response
            // to the controller allowing them to do so, and when the controller drops a
            // copy to it also removes it from the list of tracked copy_tos (see
            // [`Instance::drop_collections`]).
            CopyToResponse::Dropped => {
                tracing::error!(
                    %sink_id, %replica_id,
                    "received `Dropped` response for a tracked copy to",
                );
                return;
            }
        };

        self.deliver_response(ComputeControllerResponse::CopyToResponse(sink_id, result));
    }

    fn handle_subscribe_response(
        &mut self,
        subscribe_id: GlobalId,
        response: SubscribeResponse<T>,
        replica_id: ReplicaId,
    ) {
        if !self.collections.contains_key(&subscribe_id) {
            tracing::error!(
                %subscribe_id, %replica_id,
                "received response for an unknown subscribe",
            );
            return;
        }

        // Always apply replica write frontier updates. Even if the subscribe is not tracked
        // anymore, there might still be replicas reading from its inputs, so we need to track the
        // frontiers until all replicas have advanced to the empty one.
        let write_frontier = match &response {
            SubscribeResponse::Batch(batch) => batch.upper.clone(),
            SubscribeResponse::DroppedAt(_) => Antichain::new(),
        };

        self.update_hydration_status(subscribe_id, replica_id, &write_frontier);

        let write_frontier_updates = [(subscribe_id, write_frontier)].into();
        self.update_replica_write_frontiers(replica_id, &write_frontier_updates);

        // For subscribes we downgrade replica input frontiers based on write frontiers. This
        // should be fine because subscribes can't jump their write frontiers ahead of the times
        // they have read from their inputs currently.
        // TODO(#16274): report subscribe input frontiers through `Frontiers` responses
        self.update_replica_input_frontiers(replica_id, &write_frontier_updates);

        // If the subscribe is not tracked, or targets a different replica, there is nothing to do.
        let Some(mut subscribe) = self.subscribes.get(&subscribe_id).cloned() else {
            return;
        };
        let replica_targeted = subscribe.target_replica.unwrap_or(replica_id) == replica_id;
        if !replica_targeted {
            return;
        }

        // Apply a global frontier update.
        // If this is a replica-targeted subscribe, it is important that we advance the global
        // frontier only based on responses from the targeted replica. Otherwise, another replica
        // could advance to the empty frontier, making us drop the subscribe on the targeted
        // replica prematurely.
        self.maybe_update_global_write_frontiers(&write_frontier_updates);

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
                        self.subscribes.remove(&subscribe_id);
                    } else {
                        // This subscribe can produce more data. Update our tracking of it.
                        self.subscribes.insert(subscribe_id, subscribe);
                    }

                    if let Ok(updates) = updates.as_mut() {
                        updates.retain(|(time, _data, _diff)| lower.less_equal(time));
                    }
                    self.deliver_response(ComputeControllerResponse::SubscribeResponse(
                        subscribe_id,
                        SubscribeBatch {
                            lower,
                            upper,
                            updates,
                        },
                    ));
                }
            }
            SubscribeResponse::DroppedAt(frontier) => {
                // We should never get here: Replicas only drop subscribe collections in response
                // to the controller allowing them to do so, and when the controller drops a
                // subscribe it also removes it from the list of tracked subscribes (see
                // [`Instance::drop_collections`]).
                tracing::error!(
                    %subscribe_id,
                    %replica_id,
                    frontier = ?frontier.elements(),
                    "received `DroppedAt` response for a tracked subscribe",
                );
                self.subscribes.remove(&subscribe_id);
            }
        }
    }

    fn handle_status_response(&mut self, response: StatusResponse, replica_id: ReplicaId) {
        match response {
            StatusResponse::OperatorHydration(status) => {
                self.update_operator_hydration_status(replica_id, status)
            }
        }
    }

    /// Downgrade the warmup capabilities of collections as much as possible.
    ///
    /// The only requirement we have for a collection's warmup capability is that it is for a time
    /// that is available in all of the collection's inputs. For each input the latest time that is
    /// the case for is `write_frontier - 1`. So the farthest we can downgrade a collection's
    /// warmup capability is the minimum of `write_frontier - 1` of all its inputs.
    ///
    /// This method expects to be periodically called as part of instance maintenance work.
    /// We would like to instead update the warmup capabilities synchronously in response to
    /// frontier updates of dependency collections, but that is not generally possible because we
    /// don't learn about frontier updates of storage collections synchronously. We could do
    /// synchronous updates for compute dependencies, but we refrain from doing for simplicity.
    fn downgrade_warmup_capabilities(&mut self) {
        let mut new_capabilities = BTreeMap::new();
        for (id, collection) in &self.collections {
            // For write-only collections that have advanced to the empty frontier, we can drop the
            // warmup capability entirely. There is no reason why we would need to hydrate those
            // collections again, so being able to warm them up is not useful.
            if collection.read_policy.is_none()
                && collection.write_frontier.is_empty()
                && !collection.warmup_capability.is_empty()
            {
                new_capabilities.insert(*id, Antichain::new());
                continue;
            }

            let compute_frontiers = collection.compute_dependencies.iter().flat_map(|dep_id| {
                let collection = self.collections.get(dep_id);
                collection.map(|c| c.write_frontier.clone())
            });

            let existing_storage_dependencies = collection
                .storage_dependencies
                .iter()
                .filter(|id| self.storage_collections.check_exists(**id).is_ok())
                .copied()
                .collect::<Vec<_>>();
            let storage_frontiers = self
                .storage_collections
                .collections_frontiers(existing_storage_dependencies)
                .expect("missing storage collections")
                .into_iter()
                .map(|f| f.write_frontier);

            let mut new_capability = Antichain::new();
            for frontier in compute_frontiers.chain(storage_frontiers) {
                for time in frontier.iter() {
                    new_capability.insert(time.step_back().unwrap_or(time.clone()));
                }
            }

            if PartialOrder::less_than(&collection.warmup_capability, &new_capability) {
                new_capabilities.insert(*id, new_capability);
            }
        }

        let mut read_capability_changes = BTreeMap::new();
        for (id, new_capability) in new_capabilities {
            let collection = self.expect_collection_mut(id);
            let old_capability = &collection.warmup_capability;

            let mut update = ChangeBatch::new();
            update.extend(old_capability.iter().map(|t| (t.clone(), -1)));
            update.extend(new_capability.iter().map(|t| (t.clone(), 1)));
            read_capability_changes.insert(id, update);
            collection.warmup_capability = new_capability;
        }

        if !read_capability_changes.is_empty() {
            self.update_read_capabilities(read_capability_changes);
        }
    }

    /// Process pending maintenance work.
    ///
    /// This method is invoked periodically by the global controller.
    /// It is a good place to perform maintenance work that arises from various controller state
    /// changes and that cannot conveniently be handled synchronously with those state changes.
    pub fn maintain(&mut self) {
        self.rehydrate_failed_replicas();
        self.downgrade_warmup_capabilities();
        self.schedule_collections();
        self.cleanup_collections();
        self.refresh_state_metrics();
    }
}

#[derive(Debug)]
struct PendingPeek<T> {
    /// Information about the collection targeted by the peek.
    target: PeekTarget,
    /// The peek time.
    time: T,
    /// For replica-targeted peeks, this specifies the replica whose response we should pass on.
    ///
    /// If this value is `None`, we pass on the first response.
    target_replica: Option<ReplicaId>,
    /// The OpenTelemetry context for this peek.
    otel_ctx: OpenTelemetryContext,
    /// The time at which the peek was requested.
    ///
    /// Used to track peek durations.
    requested_at: Instant,
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

impl<T: ComputeControllerTimestamp> ActiveSubscribe<T> {
    fn new() -> Self {
        Self {
            frontier: Antichain::from_elem(timely::progress::Timestamp::minimum()),
            target_replica: None,
        }
    }
}

/// State maintained about individual replicas.
#[derive(Debug)]
pub struct ReplicaState<T> {
    /// The ID of the replica.
    id: ReplicaId,
    /// Client for the running replica task.
    client: ReplicaClient<T>,
    /// The replica configuration.
    config: ReplicaConfig,
    /// Replica metrics.
    metrics: ReplicaMetrics,
    /// A channel through which introspection updates are delivered.
    introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    /// Per-replica collection state.
    collections: BTreeMap<GlobalId, ReplicaCollectionState<T>>,
    /// Whether the replica has failed and requires rehydration.
    failed: bool,
}

impl<T: Debug> ReplicaState<T> {
    fn new(
        id: ReplicaId,
        client: ReplicaClient<T>,
        config: ReplicaConfig,
        metrics: ReplicaMetrics,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    ) -> Self {
        Self {
            id,
            client,
            config,
            metrics,
            introspection_tx,
            collections: Default::default(),
            failed: false,
        }
    }

    /// Add a collection to the replica state.
    ///
    /// # Panics
    ///
    /// Panics if a collection with the same ID exists already.
    fn add_collection(&mut self, id: GlobalId, as_of: Antichain<T>) {
        let metrics = self.metrics.for_collection(id);
        let hydration_state = HydrationState::new(self.id, id, self.introspection_tx.clone());
        let mut state = ReplicaCollectionState {
            metrics,
            created_at: Instant::now(),
            as_of,
            hydration_state,
        };

        // We need to consider the edge case where the as-of is the empty frontier. Such an as-of
        // is not useful for indexes, because they wouldn't be readable. For write-only
        // collections, an empty as-of means that the collection has been fully written and no new
        // dataflow needs to be created for it. Consequently, no hydration will happen either.
        //
        // Based on this, we could set the hydration flag in two ways:
        //  * `false`, as in "the dataflow was never created"
        //  * `true`, as in "the dataflow completed immediately"
        //
        // Since hydration is often used as a measure of dataflow progress and we don't want to
        // give the impression that certain dataflows are somehow stuck when they are not, we go
        // go with the second interpretation here.
        if state.as_of.is_empty() {
            state.set_hydrated();
        }

        if let Some(previous) = self.collections.insert(id, state) {
            panic!("attempt to add a collection with existing ID {id} (previous={previous:?}");
        }
    }

    /// Remove state for a collection.
    fn remove_collection(&mut self, id: GlobalId) -> Option<ReplicaCollectionState<T>> {
        self.collections.remove(&id)
    }

    /// Returns the state of the [`ReplicaState`] formatted as JSON.
    ///
    /// The returned value is not guaranteed to be stable and may change at any point in time.
    pub fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
        // Note: We purposefully use the `Debug` formatting for the value of all fields in the
        // returned object as a tradeoff between usability and stability. `serde_json` will fail
        // to serialize an object if the keys aren't strings, so `Debug` formatting the values
        // prevents a future unrelated change from silently breaking this method.

        // Destructure `self` here so we don't forget to consider dumping newly added fields.
        let Self {
            id,
            client: _,
            config: _,
            metrics: _,
            introspection_tx: _,
            collections,
            failed,
        } = self;

        fn field(
            key: &str,
            value: impl Serialize,
        ) -> Result<(String, serde_json::Value), anyhow::Error> {
            let value = serde_json::to_value(value)?;
            Ok((key.to_string(), value))
        }

        let collections: BTreeMap<_, _> = collections
            .iter()
            .map(|(id, collection)| (id.to_string(), format!("{collection:?}")))
            .collect();

        let map = serde_json::Map::from_iter([
            field("id", id.to_string())?,
            field("collections", collections)?,
            field("failed", failed)?,
        ]);
        Ok(serde_json::Value::Object(map))
    }
}

#[derive(Debug)]
struct ReplicaCollectionState<T> {
    /// Metrics tracked for this collection.
    ///
    /// If this is `None`, no metrics are collected.
    metrics: Option<ReplicaCollectionMetrics>,
    /// Time at which this collection was installed.
    created_at: Instant,
    /// As-of frontier with which this collection was installed on the replica.
    as_of: Antichain<T>,
    /// Tracks hydration state for this collection.
    hydration_state: HydrationState,
}

impl<T> ReplicaCollectionState<T> {
    /// Returns whether this collection is hydrated.
    fn hydrated(&self) -> bool {
        self.hydration_state.hydrated
    }

    /// Marks the collection as hydrated and updates metrics and introspection accordingly.
    fn set_hydrated(&mut self) {
        if let Some(metrics) = &self.metrics {
            let duration = self.created_at.elapsed().as_secs_f64();
            metrics.initial_output_duration_seconds.set(duration);
        }

        self.hydration_state.collection_hydrated();
    }
}

/// Maintains both global and operator-level hydration introspection for a given replica and
/// collection, and ensures that reported introspection data is retracted when the flag is dropped.
#[derive(Debug)]
struct HydrationState {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// The ID of the compute collection.
    collection_id: GlobalId,
    /// Whether the collection is hydrated.
    hydrated: bool,
    /// Operator-level hydration state.
    /// (lir_id, worker_id) -> hydrated
    operators: BTreeMap<(LirId, usize), bool>,
    /// A channel through which introspection updates are delivered.
    introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
}

impl HydrationState {
    /// Create a new `HydrationState` and initialize introspection.
    fn new(
        replica_id: ReplicaId,
        collection_id: GlobalId,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    ) -> Self {
        let self_ = Self {
            replica_id,
            collection_id,
            hydrated: false,
            operators: Default::default(),
            introspection_tx,
        };

        let insertion = self_.row_for_collection();
        self_.send(
            IntrospectionType::ComputeHydrationStatus,
            vec![(insertion, 1)],
        );

        self_
    }

    /// Update the collection as hydrated.
    fn collection_hydrated(&mut self) {
        if self.hydrated {
            return; // nothing to do
        }

        let retraction = self.row_for_collection();
        self.hydrated = true;
        let insertion = self.row_for_collection();

        self.send(
            IntrospectionType::ComputeHydrationStatus,
            vec![(retraction, -1), (insertion, 1)],
        );
    }

    /// Update the given (lir_id, worker_id) pair as hydrated.
    fn operator_hydrated(&mut self, lir_id: LirId, worker_id: usize, hydrated: bool) {
        let retraction = self.row_for_operator(lir_id, worker_id);
        self.operators.insert((lir_id, worker_id), hydrated);
        let insertion = self.row_for_operator(lir_id, worker_id);

        if retraction == insertion {
            return; // no change
        }

        let updates = retraction
            .map(|r| (r, -1))
            .into_iter()
            .chain(insertion.map(|r| (r, 1)))
            .collect();
        self.send(IntrospectionType::ComputeOperatorHydrationStatus, updates);
    }

    /// Return a `Row` reflecting the current collection hydration status.
    fn row_for_collection(&self) -> Row {
        Row::pack_slice(&[
            Datum::String(&self.collection_id.to_string()),
            Datum::String(&self.replica_id.to_string()),
            Datum::from(self.hydrated),
        ])
    }

    /// Return a `Row` reflecting the current hydration status of the identified operator.
    ///
    /// Returns `None` if the identified operator is not tracked.
    fn row_for_operator(&self, lir_id: LirId, worker_id: usize) -> Option<Row> {
        self.operators.get(&(lir_id, worker_id)).map(|hydrated| {
            Row::pack_slice(&[
                Datum::String(&self.collection_id.to_string()),
                Datum::UInt64(lir_id),
                Datum::String(&self.replica_id.to_string()),
                Datum::UInt64(u64::cast_from(worker_id)),
                Datum::from(*hydrated),
            ])
        })
    }

    fn send(&self, introspection_type: IntrospectionType, updates: Vec<(Row, Diff)>) {
        let result = self.introspection_tx.send((introspection_type, updates));

        if result.is_err() {
            // The global controller holds on to the `introspection_rx`. So when we get here that
            // probably means that the controller was dropped and the process is shutting down, in
            // which case we don't care about introspection updates anymore.
            tracing::info!(
                ?introspection_type,
                "discarding introspection update because the receiver disconnected"
            );
        }
    }
}

impl Drop for HydrationState {
    fn drop(&mut self) {
        // Retract collection hydration status.
        let retraction = self.row_for_collection();
        self.send(
            IntrospectionType::ComputeHydrationStatus,
            vec![(retraction, -1)],
        );

        // Retract operator-level hydration status.
        let operators: Vec<_> = self.operators.keys().collect();
        let updates: Vec<_> = operators
            .into_iter()
            .flat_map(|(lir_id, worker_id)| self.row_for_operator(*lir_id, *worker_id))
            .map(|r| (r, -1))
            .collect();
        if !updates.is_empty() {
            self.send(IntrospectionType::ComputeOperatorHydrationStatus, updates)
        }
    }
}
