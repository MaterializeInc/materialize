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
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mz_build_info::BuildInfo;
use mz_cluster_client::client::{ClusterStartupEpoch, TimelyConfig};
use mz_cluster_client::WallclockLagFn;
use mz_compute_types::dataflows::{BuildDesc, DataflowDescription};
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_compute_types::plan::LirId;
use mz_compute_types::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, ContinualTaskConnection, MaterializedViewSinkConnection,
};
use mz_compute_types::sources::SourceInstanceDesc;
use mz_compute_types::ComputeInstanceId;
use mz_controller_types::dyncfgs::WALLCLOCK_LAG_REFRESH_INTERVAL;
use mz_dyncfg::ConfigSet;
use mz_expr::RowSetFinishing;
use mz_ore::cast::CastFrom;
use mz_ore::channel::instrumented_unbounded_channel;
use mz_ore::now::NowFn;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{soft_assert_or_log, soft_panic_or_log};
use mz_repr::adt::interval::Interval;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_client::controller::IntrospectionType;
use mz_storage_types::read_holds::{self, ReadHold};
use mz_storage_types::read_policy::ReadPolicy;
use serde::Serialize;
use thiserror::Error;
use timely::progress::frontier::MutableAntichain;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use timely::PartialOrder;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::controller::error::{
    CollectionLookupError, CollectionMissing, HydrationCheckBadTarget, ERROR_TARGET_REPLICA_FAILED,
};
use crate::controller::replica::{ReplicaClient, ReplicaConfig};
use crate::controller::{
    ComputeControllerResponse, ComputeControllerTimestamp, IntrospectionUpdates, PeekNotification,
    ReplicaId, StorageCollections,
};
use crate::logging::LogVariant;
use crate::metrics::IntCounter;
use crate::metrics::{InstanceMetrics, ReplicaCollectionMetrics, ReplicaMetrics, UIntGauge};
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
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    #[error("dataflow definition lacks an as_of value")]
    MissingAsOf,
    #[error("subscribe dataflow has an empty as_of")]
    EmptyAsOfForSubscribe,
    #[error("copy to dataflow has an empty as_of")]
    EmptyAsOfForCopyTo,
    #[error("no read hold provided for dataflow import: {0}")]
    ReadHoldMissing(GlobalId),
    #[error("insufficient read hold provided for dataflow import: {0}")]
    ReadHoldInsufficient(GlobalId),
}

impl From<CollectionMissing> for DataflowCreationError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

#[derive(Error, Debug)]
pub(super) enum PeekError {
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    #[error("read hold ID does not match peeked collection: {0}")]
    ReadHoldIdMismatch(GlobalId),
    #[error("insufficient read hold provided: {0}")]
    ReadHoldInsufficient(GlobalId),
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

/// A command sent to an [`Instance`] task.
pub type Command<T> = Box<dyn FnOnce(&mut Instance<T>) + Send>;

/// A client for an [`Instance`] task.
#[derive(Clone, derivative::Derivative)]
#[derivative(Debug)]
pub(super) struct Client<T: ComputeControllerTimestamp> {
    /// A sender for commands for the instance.
    command_tx: mpsc::UnboundedSender<Command<T>>,
    /// A sender for read hold changes for collections installed on the instance.
    #[derivative(Debug = "ignore")]
    read_hold_tx: read_holds::ChangeTx<T>,
}

impl<T: ComputeControllerTimestamp> Client<T> {
    pub fn send(&self, command: Command<T>) -> Result<(), SendError<Command<T>>> {
        self.command_tx.send(command)
    }

    pub fn read_hold_tx(&self) -> read_holds::ChangeTx<T> {
        Arc::clone(&self.read_hold_tx)
    }
}

impl<T> Client<T>
where
    T: ComputeControllerTimestamp,
    ComputeGrpcClient: ComputeClient<T>,
{
    pub fn spawn(
        id: ComputeInstanceId,
        build_info: &'static BuildInfo,
        storage: StorageCollections<T>,
        arranged_logs: Vec<(LogVariant, GlobalId, SharedCollectionState<T>)>,
        envd_epoch: NonZeroI64,
        metrics: InstanceMetrics,
        now: NowFn,
        wallclock_lag: WallclockLagFn<T>,
        dyncfg: Arc<ConfigSet>,
        response_tx: mpsc::UnboundedSender<ComputeControllerResponse<T>>,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let read_hold_tx: read_holds::ChangeTx<_> = {
            let command_tx = command_tx.clone();
            Arc::new(move |id, change: ChangeBatch<_>| {
                let cmd: Command<_> = {
                    let change = change.clone();
                    Box::new(move |i| i.apply_read_hold_change(id, change.clone()))
                };
                command_tx.send(cmd).map_err(|_| SendError((id, change)))
            })
        };

        mz_ore::task::spawn(
            || format!("compute-instance-{id}"),
            Instance::new(
                build_info,
                storage,
                arranged_logs,
                envd_epoch,
                metrics,
                now,
                wallclock_lag,
                dyncfg,
                command_rx,
                response_tx,
                Arc::clone(&read_hold_tx),
                introspection_tx,
            )
            .run(),
        );

        Self {
            command_tx,
            read_hold_tx,
        }
    }
}

/// A response from a replica, composed of a replica ID, the replica's current epoch, and the
/// compute response itself.
pub(super) type ReplicaResponse<T> = (ReplicaId, u64, ComputeResponse<T>);

/// The state we keep for a compute instance.
pub(super) struct Instance<T: ComputeControllerTimestamp> {
    /// Build info for spawning replicas
    build_info: &'static BuildInfo,
    /// A handle providing access to storage collections.
    storage_collections: StorageCollections<T>,
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
    /// inputs before we allow them to compact. database-issues#4822 tracks changing this so we only have to wait
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
    /// Receiver for commands to be executed.
    command_rx: mpsc::UnboundedReceiver<Command<T>>,
    /// Sender for responses to be delivered.
    response_tx: mpsc::UnboundedSender<ComputeControllerResponse<T>>,
    /// Sender for introspection updates to be recorded.
    introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    /// A number that increases with each restart of `environmentd`.
    envd_epoch: NonZeroI64,
    /// Numbers that increase with each restart of a replica.
    replica_epochs: BTreeMap<ReplicaId, u64>,
    /// The registry the controller uses to report metrics.
    metrics: InstanceMetrics,
    /// Dynamic system configuration.
    dyncfg: Arc<ConfigSet>,

    /// A function that produces the current wallclock time.
    now: NowFn,
    /// A function that computes the lag between the given time and wallclock time.
    wallclock_lag: WallclockLagFn<T>,
    /// The last time wallclock lag introspection was refreshed.
    wallclock_lag_last_refresh: Instant,

    /// Sender for updates to collection read holds.
    ///
    /// Copies of this sender are given to [`ReadHold`]s that are created in
    /// [`CollectionState::new`].
    read_hold_tx: read_holds::ChangeTx<T>,
    /// A sender for responses from replicas.
    replica_tx: mz_ore::channel::InstrumentedUnboundedSender<ReplicaResponse<T>, IntCounter>,
    /// A receiver for responses from replicas.
    replica_rx: mz_ore::channel::InstrumentedUnboundedReceiver<ReplicaResponse<T>, IntCounter>,
}

impl<T: ComputeControllerTimestamp> Instance<T> {
    /// Acquire a handle to the collection state associated with `id`.
    fn collection(&self, id: GlobalId) -> Result<&CollectionState<T>, CollectionMissing> {
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
    fn expect_collection(&self, id: GlobalId) -> &CollectionState<T> {
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

    fn collections_iter(&self) -> impl Iterator<Item = (GlobalId, &CollectionState<T>)> {
        self.collections.iter().map(|(id, coll)| (*id, coll))
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
        shared: SharedCollectionState<T>,
        storage_dependencies: BTreeMap<GlobalId, ReadHold<T>>,
        compute_dependencies: BTreeMap<GlobalId, ReadHold<T>>,
        replica_input_read_holds: Vec<ReadHold<T>>,
        write_only: bool,
        storage_sink: bool,
        initial_as_of: Option<Antichain<T>>,
        refresh_schedule: Option<RefreshSchedule>,
    ) {
        // Add global collection state.
        let introspection = CollectionIntrospection::new(
            id,
            self.introspection_tx.clone(),
            as_of.clone(),
            storage_sink,
            initial_as_of,
            refresh_schedule,
        );
        let mut state = CollectionState::new(
            id,
            as_of.clone(),
            shared,
            storage_dependencies,
            compute_dependencies,
            Arc::clone(&self.read_hold_tx),
            introspection,
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
            replica.add_collection(id, as_of.clone(), replica_input_read_holds.clone());
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
        epoch: ClusterStartupEpoch,
    ) {
        let log_ids: BTreeSet<_> = config.logging.index_logs.values().copied().collect();

        let metrics = self.metrics.for_replica(id);
        let mut replica = ReplicaState::new(
            id,
            client,
            config,
            metrics,
            self.introspection_tx.clone(),
            epoch,
        );

        // Add per-replica collection state.
        for (collection_id, collection) in &self.collections {
            // Skip log collections not maintained by this replica.
            if collection.log_collection && !log_ids.contains(collection_id) {
                continue;
            }

            let as_of = collection.read_frontier().to_owned();
            let input_read_holds = collection.storage_dependencies.values().cloned().collect();
            replica.add_collection(*collection_id, as_of, input_read_holds);
        }

        self.replicas.insert(id, replica);
    }

    /// Enqueue the given response for delivery to the controller clients.
    fn deliver_response(&self, response: ComputeControllerResponse<T>) {
        // Failure to send means the `ComputeController` has been dropped and doesn't care about
        // responses anymore.
        let _ = self.response_tx.send(response);
    }

    /// Enqueue the given introspection updates for recording.
    fn deliver_introspection_updates(&self, type_: IntrospectionType, updates: Vec<(Row, Diff)>) {
        // Failure to send means the `ComputeController` has been dropped and doesn't care about
        // introspection updates anymore.
        let _ = self.introspection_tx.send((type_, updates));
    }

    /// Returns whether the identified replica exists.
    fn replica_exists(&self, id: ReplicaId) -> bool {
        self.replicas.contains_key(&id)
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

    /// Update introspection with the current collection frontiers.
    ///
    /// We could also do this directly in response to frontier changes, but doing it periodically
    /// lets us avoid emitting some introspection updates that can be consolidated (e.g. a write
    /// frontier updated immediately followed by a read frontier update).
    ///
    /// This method is invoked by `ComputeController::maintain`, which we expect to be called once
    /// per second during normal operation.
    fn update_frontier_introspection(&mut self) {
        for collection in self.collections.values_mut() {
            collection
                .introspection
                .observe_frontiers(&collection.read_frontier(), &collection.write_frontier());
        }

        for replica in self.replicas.values_mut() {
            for collection in replica.collections.values_mut() {
                collection
                    .introspection
                    .observe_frontier(&collection.write_frontier);
            }
        }
    }

    /// Refresh the controller state metrics for this instance.
    ///
    /// We could also do state metric updates directly in response to state changes, but that would
    /// mean littering the code with metric update calls. Encapsulating state metric maintenance in
    /// a single method is less noisy.
    ///
    /// This method is invoked by `ComputeController::maintain`, which we expect to be called once
    /// per second during normal operation.
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

    /// Refresh the `WallclockLagHistory` introspection and the `wallclock_lag_*_seconds` metrics
    /// with the current lag values.
    ///
    /// This method is invoked by `ComputeController::maintain`, which we expect to be called once
    /// per second during normal operation.
    fn refresh_wallclock_lag(&mut self) {
        let refresh_introspection = !self.read_only
            && self.wallclock_lag_last_refresh.elapsed()
                >= WALLCLOCK_LAG_REFRESH_INTERVAL.get(&self.dyncfg);
        let mut introspection_updates = refresh_introspection.then(Vec::new);

        let now = mz_ore::now::to_datetime((self.now)());
        let now_tz = now.try_into().expect("must fit");

        for (replica_id, replica) in &mut self.replicas {
            for (collection_id, collection) in &mut replica.collections {
                let lag = match collection.write_frontier.as_option() {
                    Some(ts) => (self.wallclock_lag)(ts),
                    None => Duration::ZERO,
                };

                if let Some(wallclock_lag_max) = &mut collection.wallclock_lag_max {
                    *wallclock_lag_max = std::cmp::max(*wallclock_lag_max, lag);

                    if let Some(updates) = &mut introspection_updates {
                        let max_lag = std::mem::take(wallclock_lag_max);
                        let max_lag_us = i64::try_from(max_lag.as_micros()).expect("must fit");
                        let row = Row::pack_slice(&[
                            Datum::String(&collection_id.to_string()),
                            Datum::String(&replica_id.to_string()),
                            Datum::Interval(Interval::new(0, 0, max_lag_us)),
                            Datum::TimestampTz(now_tz),
                        ]);
                        updates.push((row, 1));
                    }
                }

                if let Some(metrics) = &mut collection.metrics {
                    metrics.wallclock_lag.observe(lag);
                };
            }
        }

        if let Some(updates) = introspection_updates {
            self.deliver_introspection_updates(IntrospectionType::WallclockLagHistory, updates);
            self.wallclock_lag_last_refresh = Instant::now();
        }
    }

    /// Report updates (inserts or retractions) to the identified collection's dependencies.
    ///
    /// # Panics
    ///
    /// Panics if the identified collection does not exist.
    fn report_dependency_updates(&self, id: GlobalId, diff: i64) {
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

        collection.introspection.operator_hydrated(
            status.lir_id,
            status.worker_id,
            status.hydrated,
        );
    }

    /// Returns `true` if the given collection is hydrated on at least one
    /// replica.
    ///
    /// This also returns `true` in case this cluster does not have any
    /// replicas.
    #[mz_ore::instrument(level = "debug")]
    pub fn collection_hydrated(
        &self,
        collection_id: GlobalId,
    ) -> Result<bool, CollectionLookupError> {
        if self.replicas.is_empty() {
            return Ok(true);
        }

        for replica_state in self.replicas.values() {
            let collection_state = replica_state
                .collections
                .get(&collection_id)
                .ok_or(CollectionLookupError::CollectionMissing(collection_id))?;

            if collection_state.hydrated() {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Returns `true` if each non-transient, non-excluded collection is hydrated on at
    /// least one replica.
    ///
    /// This also returns `true` in case this cluster does not have any
    /// replicas.
    #[mz_ore::instrument(level = "debug")]
    pub fn collections_hydrated_on_replicas(
        &self,
        target_replica_ids: Option<Vec<ReplicaId>>,
        exclude_collections: &BTreeSet<GlobalId>,
    ) -> Result<bool, HydrationCheckBadTarget> {
        if self.replicas.is_empty() {
            return Ok(true);
        }
        let mut all_hydrated = true;
        let target_replicas: BTreeSet<ReplicaId> = self
            .replicas
            .keys()
            .filter_map(|id| match target_replica_ids {
                None => Some(id.clone()),
                Some(ref ids) if ids.contains(id) => Some(id.clone()),
                Some(_) => None,
            })
            .collect();
        if let Some(targets) = target_replica_ids {
            if target_replicas.is_empty() {
                return Err(HydrationCheckBadTarget(targets));
            }
        }

        for (id, _collection) in self.collections_iter() {
            if id.is_transient() || exclude_collections.contains(&id) {
                continue;
            }

            let mut collection_hydrated = false;
            for replica_state in self.replicas.values() {
                if !target_replicas.contains(&replica_state.id) {
                    continue;
                }
                let collection_state = replica_state
                    .collections
                    .get(&id)
                    .expect("missing collection state");

                if collection_state.hydrated() {
                    collection_hydrated = true;
                    break;
                }
            }

            if !collection_hydrated {
                tracing::info!("collection {id} is not hydrated on any replica");
                all_hydrated = false;
                // We continue with our loop instead of breaking out early, so
                // that we log all non-hydrated replicas.
            }
        }

        Ok(all_hydrated)
    }

    /// Returns `true` if all non-transient, non-excluded collections are hydrated on at least one
    /// replica.
    ///
    /// This also returns `true` in case this cluster does not have any
    /// replicas.
    #[mz_ore::instrument(level = "debug")]
    pub fn collections_hydrated(&self, exclude_collections: &BTreeSet<GlobalId>) -> bool {
        self.collections_hydrated_on_replicas(None, exclude_collections)
            .expect("Cannot error if target_replica_ids is None")
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
            .filter(|(id, collection)| {
                collection.dropped
                    && collection.shared.lock_read_capabilities(|c| c.is_empty())
                    && self
                        .replicas
                        .values()
                        .all(|r| r.collection_frontiers_empty(*id))
            })
            .map(|(id, _collection)| id)
            .collect();

        for id in to_remove {
            self.remove_collection(id);
        }
    }

    /// Returns the state of the [`Instance`] formatted as JSON.
    ///
    /// The returned value is not guaranteed to be stable and may change at any point in time.
    #[mz_ore::instrument(level = "debug")]
    pub fn dump(&self) -> Result<serde_json::Value, anyhow::Error> {
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
            command_rx: _,
            response_tx: _,
            introspection_tx: _,
            envd_epoch,
            replica_epochs,
            metrics: _,
            dyncfg: _,
            now: _,
            wallclock_lag: _,
            wallclock_lag_last_refresh,
            read_hold_tx: _,
            replica_tx: _,
            replica_rx: _,
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
        let wallclock_lag_last_refresh = format!("{wallclock_lag_last_refresh:?}");

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
            field("wallclock_lag_last_refresh", wallclock_lag_last_refresh)?,
        ]);
        Ok(serde_json::Value::Object(map))
    }
}

impl<T> Instance<T>
where
    T: ComputeControllerTimestamp,
    ComputeGrpcClient: ComputeClient<T>,
{
    fn new(
        build_info: &'static BuildInfo,
        storage: StorageCollections<T>,
        arranged_logs: Vec<(LogVariant, GlobalId, SharedCollectionState<T>)>,
        envd_epoch: NonZeroI64,
        metrics: InstanceMetrics,
        now: NowFn,
        wallclock_lag: WallclockLagFn<T>,
        dyncfg: Arc<ConfigSet>,
        command_rx: mpsc::UnboundedReceiver<Command<T>>,
        response_tx: mpsc::UnboundedSender<ComputeControllerResponse<T>>,
        read_hold_tx: read_holds::ChangeTx<T>,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    ) -> Self {
        let mut collections = BTreeMap::new();
        let mut log_sources = BTreeMap::new();
        for (log, id, shared) in arranged_logs {
            let collection = CollectionState::new_log_collection(
                id,
                shared,
                Arc::clone(&read_hold_tx),
                introspection_tx.clone(),
            );
            collections.insert(id, collection);
            log_sources.insert(log, id);
        }

        let history = ComputeCommandHistory::new(metrics.for_history());

        let send_count = metrics.response_send_count.clone();
        let recv_count = metrics.response_recv_count.clone();
        let (replica_tx, replica_rx) = instrumented_unbounded_channel(send_count, recv_count);

        Self {
            build_info,
            storage_collections: storage,
            initialized: false,
            read_only: true,
            replicas: Default::default(),
            collections,
            log_sources,
            peeks: Default::default(),
            subscribes: Default::default(),
            copy_tos: Default::default(),
            history,
            command_rx,
            response_tx,
            introspection_tx,
            envd_epoch,
            replica_epochs: Default::default(),
            metrics,
            dyncfg,
            now,
            wallclock_lag,
            wallclock_lag_last_refresh: Instant::now(),
            read_hold_tx,
            replica_tx,
            replica_rx,
        }
    }

    async fn run(mut self) {
        self.send(ComputeCommand::CreateTimely {
            config: TimelyConfig::default(),
            epoch: ClusterStartupEpoch::new(self.envd_epoch, 0),
        });

        // Send a placeholder instance configuration for the replica task to fill in.
        let dummy_logging_config = Default::default();
        self.send(ComputeCommand::CreateInstance(InstanceConfig {
            logging: dummy_logging_config,
            expiration_offset: None,
        }));

        loop {
            tokio::select! {
                command = self.command_rx.recv() => match command {
                    Some(cmd) => cmd(&mut self),
                    None => break,
                },
                response = self.replica_rx.recv() => match response {
                    Some(response) => self.handle_response(response),
                    None => unreachable!("self owns a sender side of the channel"),
                }
            }
        }
    }

    /// Update instance configuration.
    #[mz_ore::instrument(level = "debug")]
    pub fn update_configuration(&mut self, config_params: ComputeParameters) {
        self.send(ComputeCommand::UpdateConfiguration(config_params));
    }

    /// Marks the end of any initialization commands.
    ///
    /// Intended to be called by `Controller`, rather than by other code.
    /// Calling this method repeatedly has no effect.
    #[mz_ore::instrument(level = "debug")]
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
    #[mz_ore::instrument(level = "debug")]
    pub fn allow_writes(&mut self) {
        if self.read_only {
            self.read_only = false;
            self.send(ComputeCommand::AllowWrites);
        }
    }

    /// Shut down this instance.
    ///
    /// This method runs various assertions ensuring the instance state is empty. It exists to help
    /// us find bugs where the client drops a compute instance that still has replicas or
    /// collections installed, and later assumes that said replicas/collections still exists.
    ///
    /// # Panics
    ///
    /// Panics if the compute instance still has active replicas.
    /// Panics if the compute instance still has collections installed.
    #[mz_ore::instrument(level = "debug")]
    pub fn shutdown(&mut self) {
        // Taking the `command_rx` ensures that the [`Instance::run`] loop terminates.
        let (_tx, rx) = mpsc::unbounded_channel();
        let mut command_rx = std::mem::replace(&mut self.command_rx, rx);

        // Apply all outstanding read hold changes. This might cause read hold downgrades to be
        // added to `command_tx`, so we need to apply those in a loop.
        //
        // TODO(teskje): Make `Command` an enum and assert that all received commands are read
        // hold downgrades.
        while let Ok(cmd) = command_rx.try_recv() {
            cmd(self);
        }

        // Collections might have been dropped but not cleaned up yet.
        self.cleanup_collections();

        let stray_replicas: Vec<_> = self.replicas.keys().collect();
        soft_assert_or_log!(
            stray_replicas.is_empty(),
            "dropped instance still has provisioned replicas: {stray_replicas:?}",
        );

        let collections = self.collections.iter();
        let stray_collections: Vec<_> = collections
            .filter(|(_, c)| !c.log_collection)
            .map(|(id, _)| id)
            .collect();
        soft_assert_or_log!(
            stray_collections.is_empty(),
            "dropped instance still has installed collections: {stray_collections:?}",
        );
    }

    /// Sends a command to all replicas of this instance.
    #[mz_ore::instrument(level = "debug")]
    fn send(&mut self, cmd: ComputeCommand<T>) {
        // Record the command so that new replicas can be brought up to speed.
        self.history.push(cmd.clone());

        // Clone the command for each active replica.
        for replica in self.replicas.values_mut() {
            // Swallow error, we'll notice because the replica task has stopped.
            let _ = replica.client.send(cmd.clone());
        }
    }

    /// Add a new instance replica, by ID.
    #[mz_ore::instrument(level = "debug")]
    pub fn add_replica(
        &mut self,
        id: ReplicaId,
        mut config: ReplicaConfig,
    ) -> Result<(), ReplicaExists> {
        if self.replica_exists(id) {
            return Err(ReplicaExists(id));
        }

        config.logging.index_logs = self.log_sources.clone();

        let replica_epoch = self.replica_epochs.entry(id).or_default();
        *replica_epoch += 1;
        let metrics = self.metrics.for_replica(id);
        let epoch = ClusterStartupEpoch::new(self.envd_epoch, *replica_epoch);
        let client = ReplicaClient::spawn(
            id,
            self.build_info,
            config.clone(),
            epoch,
            metrics.clone(),
            Arc::clone(&self.dyncfg),
            self.replica_tx.clone(),
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
        self.add_replica_state(id, client, config, epoch);

        Ok(())
    }

    /// Remove an existing instance replica, by ID.
    #[mz_ore::instrument(level = "debug")]
    pub fn remove_replica(&mut self, id: ReplicaId) -> Result<(), ReplicaMissing> {
        self.replicas.remove(&id).ok_or(ReplicaMissing(id))?;

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
                    updates: Err(ERROR_TARGET_REPLICA_FAILED.into()),
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
            peek_responses.push(ComputeControllerResponse::PeekNotification(
                uuid,
                PeekNotification::Error(ERROR_TARGET_REPLICA_FAILED.into()),
                peek.otel_ctx.clone(),
            ));
            to_drop.push(uuid);
        }
        for response in peek_responses {
            self.deliver_response(response);
        }
        for uuid in to_drop {
            let response = PeekResponse::Error(ERROR_TARGET_REPLICA_FAILED.into());
            self.finish_peek(uuid, response);
        }

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
            .filter_map(|(id, replica)| replica.client.is_failed().then_some(*id))
            .collect();

        for replica_id in failed_replicas {
            self.rehydrate_replica(replica_id);
        }
    }

    /// Creates the described dataflow and initializes state for its output.
    ///
    /// This method expects a `DataflowDescription` with an `as_of` frontier specified, as well as
    /// for each imported collection a read hold in `import_read_holds` at at least the `as_of`.
    ///
    /// If a `subscribe_target_replica` is given, any subscribes exported by the dataflow are
    /// configured to target that replica, i.e., only subscribe responses sent by that replica are
    /// considered.
    #[mz_ore::instrument(level = "debug")]
    pub fn create_dataflow(
        &mut self,
        dataflow: DataflowDescription<mz_compute_types::plan::Plan<T>, (), T>,
        import_read_holds: Vec<ReadHold<T>>,
        subscribe_target_replica: Option<ReplicaId>,
        mut shared_collection_state: BTreeMap<GlobalId, SharedCollectionState<T>>,
    ) -> Result<(), DataflowCreationError> {
        use DataflowCreationError::*;

        if let Some(replica_id) = subscribe_target_replica {
            if !self.replica_exists(replica_id) {
                return Err(ReplicaMissing(replica_id));
            }
        }

        // Simple sanity checks around `as_of`
        let as_of = dataflow.as_of.as_ref().ok_or(MissingAsOf)?;
        if as_of.is_empty() && dataflow.subscribe_ids().next().is_some() {
            return Err(EmptyAsOfForSubscribe);
        }
        if as_of.is_empty() && dataflow.copy_to_ids().next().is_some() {
            return Err(EmptyAsOfForCopyTo);
        }

        // Collect all dependencies of the dataflow, and read holds on them at the `as_of`.
        let mut storage_dependencies = BTreeMap::new();
        let mut compute_dependencies = BTreeMap::new();

        // When we install per-replica input read holds, we cannot use the `as_of` because of
        // reconciliation: Existing slow replicas might be reading from the inputs at times before
        // the `as_of` and we would rather not crash them by allowing their inputs to compact too
        // far. So instead we take read holds at the least time available.
        let mut replica_input_read_holds = Vec::new();

        let mut import_read_holds: BTreeMap<_, _> =
            import_read_holds.into_iter().map(|r| (r.id(), r)).collect();

        for &id in dataflow.source_imports.keys() {
            let mut read_hold = import_read_holds.remove(&id).ok_or(ReadHoldMissing(id))?;
            replica_input_read_holds.push(read_hold.clone());

            read_hold
                .try_downgrade(as_of.clone())
                .map_err(|_| ReadHoldInsufficient(id))?;
            storage_dependencies.insert(id, read_hold);
        }

        for &id in dataflow.index_imports.keys() {
            let mut read_hold = import_read_holds.remove(&id).ok_or(ReadHoldMissing(id))?;
            read_hold
                .try_downgrade(as_of.clone())
                .map_err(|_| ReadHoldInsufficient(id))?;
            compute_dependencies.insert(id, read_hold);
        }

        // If the `as_of` is empty, we are not going to create a dataflow, so replicas won't read
        // from the inputs.
        if as_of.is_empty() {
            replica_input_read_holds = Default::default();
        }

        // Install collection state for each of the exports.
        for export_id in dataflow.export_ids() {
            let shared = shared_collection_state
                .remove(&export_id)
                .unwrap_or_else(|| SharedCollectionState::new(as_of.clone()));
            let write_only = dataflow.sink_exports.contains_key(&export_id);
            let storage_sink = dataflow.persist_sink_ids().any(|id| id == export_id);
            self.add_collection(
                export_id,
                as_of.clone(),
                shared,
                storage_dependencies.clone(),
                compute_dependencies.clone(),
                replica_input_read_holds.clone(),
                write_only,
                storage_sink,
                dataflow.initial_storage_as_of.clone(),
                dataflow.refresh_schedule.clone(),
            );

            // If the export is a storage sink, we can advance its write frontier to the write
            // frontier of the target storage collection.
            if let Ok(frontiers) = self.storage_collections.collection_frontiers(export_id) {
                self.maybe_update_global_write_frontier(export_id, frontiers.write_frontier);
            }
        }

        // Initialize tracking of subscribes.
        for subscribe_id in dataflow.subscribe_ids() {
            self.subscribes
                .insert(subscribe_id, ActiveSubscribe::new(subscribe_target_replica));
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
                .expect("we have a read hold on this collection");

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
                ComputeSinkConnection::MaterializedView(conn) => {
                    let metadata = self
                        .storage_collections
                        .collection_metadata(id)
                        .map_err(|_| CollectionMissing(id))?
                        .clone();
                    let conn = MaterializedViewSinkConnection {
                        value_desc: conn.value_desc,
                        storage_metadata: metadata,
                    };
                    ComputeSinkConnection::MaterializedView(conn)
                }
                ComputeSinkConnection::ContinualTask(conn) => {
                    let metadata = self
                        .storage_collections
                        .collection_metadata(id)
                        .map_err(|_| DataflowCreationError::CollectionMissing(id))?
                        .clone();
                    let conn = ContinualTaskConnection {
                        input_id: conn.input_id,
                        storage_metadata: metadata,
                    };
                    ComputeSinkConnection::ContinualTask(conn)
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
                plan: RenderPlan::try_from(object.plan).expect("valid plan"),
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
            time_dependence: dataflow.time_dependence,
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
            // Ignore self-dependencies. Any self-dependencies do not need to be
            // available at the as_of for the dataflow to make progress, so we
            // can ignore them here. At the moment, only continual tasks have
            // self-dependencies, but this logic is correct for any dataflow, so
            // we don't special case it to CTs.
            let not_self_dep = |x: &GlobalId| *x != id;

            // Check dependency frontiers to determine if all inputs are
            // available. An input is available when its frontier is greater
            // than the `as_of`, i.e., all input data up to and including the
            // `as_of` has been sealed.
            let compute_deps = collection.compute_dependency_ids().filter(not_self_dep);
            let compute_frontiers = compute_deps.map(|id| {
                let dep = &self.expect_collection(id);
                dep.write_frontier()
            });

            let storage_deps = collection.storage_dependency_ids().filter(not_self_dep);
            let storage_frontiers = self
                .storage_collections
                .collections_frontiers(storage_deps.collect())
                .expect("must exist");
            let storage_frontiers = storage_frontiers.into_iter().map(|f| f.write_frontier);

            let ready = compute_frontiers
                .chain(storage_frontiers)
                .all(|frontier| PartialOrder::less_than(&as_of, &frontier));

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
    #[mz_ore::instrument(level = "debug")]
    pub fn drop_collections(&mut self, ids: Vec<GlobalId>) -> Result<(), CollectionMissing> {
        for id in &ids {
            let collection = self.collection_mut(*id)?;

            // Mark the collection as dropped to allow it to be removed from the controller state.
            collection.dropped = true;

            // Drop the implied and warmup read holds to announce that clients are not
            // interested in the collection anymore.
            collection.implied_read_hold.release();
            collection.warmup_read_hold.release();

            // If the collection is a subscribe, stop tracking it. This ensures that the controller
            // ceases to produce `SubscribeResponse`s for this subscribe.
            self.subscribes.remove(id);
            // If the collection is a copy to, stop tracking it. This ensures that the controller
            // ceases to produce `CopyToResponse`s` for this copy to.
            self.copy_tos.remove(id);
        }

        Ok(())
    }

    /// Initiate a peek request for the contents of `id` at `timestamp`.
    #[mz_ore::instrument(level = "debug")]
    pub fn peek(
        &mut self,
        peek_target: PeekTarget,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        mut read_hold: ReadHold<T>,
        target_replica: Option<ReplicaId>,
        peek_response_tx: oneshot::Sender<PeekResponse>,
    ) -> Result<(), PeekError> {
        use PeekError::*;

        // Downgrade the provided read hold to the peek time.
        let target_id = peek_target.id();
        if read_hold.id() != target_id {
            return Err(ReadHoldIdMismatch(read_hold.id()));
        }
        read_hold
            .try_downgrade(Antichain::from_elem(timestamp.clone()))
            .map_err(|_| ReadHoldInsufficient(target_id))?;

        if let Some(target) = target_replica {
            if !self.replica_exists(target) {
                return Err(ReplicaMissing(target));
            }
        }

        let otel_ctx = OpenTelemetryContext::obtain();
        self.peeks.insert(
            uuid,
            PendingPeek {
                target_replica,
                // TODO(guswynn): can we just hold the `tracing::Span` here instead?
                otel_ctx: otel_ctx.clone(),
                requested_at: Instant::now(),
                read_hold,
                peek_response_tx,
                limit: finishing.limit.map(usize::cast_from),
                offset: finishing.offset,
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
    #[mz_ore::instrument(level = "debug")]
    pub fn cancel_peek(&mut self, uuid: Uuid, reason: PeekResponse) {
        let Some(peek) = self.peeks.get_mut(&uuid) else {
            tracing::warn!("did not find pending peek for {uuid}");
            return;
        };

        let duration = peek.requested_at.elapsed();
        self.metrics
            .observe_peek_response(&PeekResponse::Canceled, duration);

        // Enqueue a notification for the cancellation.
        let otel_ctx = peek.otel_ctx.clone();
        otel_ctx.attach_as_parent();

        self.deliver_response(ComputeControllerResponse::PeekNotification(
            uuid,
            PeekNotification::Canceled,
            otel_ctx,
        ));

        // Finish the peek.
        // This will also propagate the cancellation to the replicas.
        self.finish_peek(uuid, reason);
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

        for (id, new_policy) in policies {
            let collection = self.expect_collection_mut(id);
            let new_since = new_policy.frontier(collection.write_frontier().borrow());
            let _ = collection.implied_read_hold.try_downgrade(new_since);
            collection.read_policy = Some(new_policy);
        }

        Ok(())
    }

    /// Advance the global write frontier of the given collection.
    ///
    /// Frontier regressions are gracefully ignored.
    ///
    /// # Panics
    ///
    /// Panics if the identified collection does not exist.
    #[mz_ore::instrument(level = "debug")]
    fn maybe_update_global_write_frontier(&mut self, id: GlobalId, new_frontier: Antichain<T>) {
        let collection = self.expect_collection_mut(id);

        let advanced = collection.shared.lock_write_frontier(|f| {
            let advanced = PartialOrder::less_than(f, &new_frontier);
            if advanced {
                f.clone_from(&new_frontier);
            }
            advanced
        });

        if !advanced {
            return;
        }

        // Relax the implied read hold according to the read policy.
        let new_since = match &collection.read_policy {
            Some(read_policy) => {
                // For readable collections the read frontier is determined by applying the
                // client-provided read policy to the write frontier.
                read_policy.frontier(new_frontier.borrow())
            }
            None => {
                // Write-only collections cannot be read within the context of the compute
                // controller, so their read frontier only controls the read holds taken on their
                // inputs. We can safely downgrade the input read holds to any time less than the
                // write frontier.
                //
                // Note that some write-only collections (continual tasks) need to observe changes
                // at their current write frontier during hydration. Thus, we cannot downgrade the
                // read frontier to the write frontier and instead step it back by one.
                Antichain::from_iter(
                    new_frontier
                        .iter()
                        .map(|t| t.step_back().unwrap_or(T::minimum())),
                )
            }
        };
        let _ = collection.implied_read_hold.try_downgrade(new_since);

        // Report the frontier advancement.
        self.deliver_response(ComputeControllerResponse::FrontierUpper {
            id,
            upper: new_frontier,
        });
    }

    /// Apply a collection read hold change.
    fn apply_read_hold_change(&mut self, id: GlobalId, mut update: ChangeBatch<T>) {
        let Some(collection) = self.collections.get_mut(&id) else {
            soft_panic_or_log!(
                "read hold change for absent collection (id={id}, changes={update:?})"
            );
            return;
        };

        let new_since = collection.shared.lock_read_capabilities(|caps| {
            // Sanity check to prevent corrupted `read_capabilities`, which can cause hard-to-debug
            // issues (usually stuck read frontiers).
            let read_frontier = caps.frontier();
            for (time, diff) in update.iter() {
                let count = caps.count_for(time) + diff;
                assert!(
                    count >= 0,
                    "invalid read capabilities update: negative capability \
             (id={id:?}, read_capabilities={caps:?}, update={update:?})",
                );
                assert!(
                    count == 0 || read_frontier.less_equal(time),
                    "invalid read capabilities update: frontier regression \
             (id={id:?}, read_capabilities={caps:?}, update={update:?})",
                );
            }

            // Apply read capability updates and learn about resulting changes to the read
            // frontier.
            let changes = caps.update_iter(update.drain());

            let changed = changes.count() > 0;
            changed.then(|| caps.frontier().to_owned())
        });

        let Some(new_since) = new_since else {
            return; // read frontier did not change
        };

        // Propagate read frontier update to dependencies.
        for read_hold in collection.compute_dependencies.values_mut() {
            read_hold
                .try_downgrade(new_since.clone())
                .expect("frontiers don't regress");
        }
        for read_hold in collection.storage_dependencies.values_mut() {
            read_hold
                .try_downgrade(new_since.clone())
                .expect("frontiers don't regress");
        }

        // Produce `AllowCompaction` command.
        self.send(ComputeCommand::AllowCompaction {
            id,
            frontier: new_since,
        });
    }

    /// Fulfills a registered peek and cleans up associated state.
    ///
    /// As part of this we:
    ///  * Send a `PeekResponse` through the peek's response channel.
    ///  * Emit a `CancelPeek` command to instruct replicas to stop spending resources on this
    ///    peek, and to allow the `ComputeCommandHistory` to reduce away the corresponding `Peek`
    ///    command.
    ///  * Remove the read hold for this peek, unblocking compaction that might have waited on it.
    fn finish_peek(&mut self, uuid: Uuid, response: PeekResponse) {
        let Some(peek) = self.peeks.remove(&uuid) else {
            return;
        };

        // The recipient might not be interested in the peek response anymore, which is fine.
        let _ = peek.peek_response_tx.send(response);

        // NOTE: We need to send the `CancelPeek` command _before_ we release the peek's read hold
        // (by dropping it), to avoid the edge case that caused database-issues#4812.
        self.send(ComputeCommand::CancelPeek { uuid });

        drop(peek.read_hold);
    }

    /// Handles a response from a replica. Replica IDs are re-used across replica restarts, so we
    /// use the replica incarnation to drop stale responses.
    fn handle_response(&mut self, (replica_id, incarnation, response): ReplicaResponse<T>) {
        // Filter responses from non-existing or stale replicas.
        if self
            .replicas
            .get(&replica_id)
            .filter(|replica| replica.epoch.replica() == incarnation)
            .is_none()
        {
            return;
        }

        // Invariant: the replica exists and has the expected incarnation.

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
        if !self.collections.contains_key(&id) {
            soft_panic_or_log!(
                "frontiers update for an unknown collection \
                 (id={id}, replica_id={replica_id}, frontiers={frontiers:?})"
            );
            return;
        }
        let Some(replica) = self.replicas.get_mut(&replica_id) else {
            soft_panic_or_log!(
                "frontiers update for an unknown replica \
                 (replica_id={replica_id}, frontiers={frontiers:?})"
            );
            return;
        };
        let Some(replica_collection) = replica.collections.get_mut(&id) else {
            soft_panic_or_log!(
                "frontiers update for an unknown replica collection \
                 (id={id}, replica_id={replica_id}, frontiers={frontiers:?})"
            );
            return;
        };

        if let Some(new_frontier) = frontiers.input_frontier {
            replica_collection.update_input_frontier(new_frontier.clone());
        }
        if let Some(new_frontier) = frontiers.output_frontier {
            replica_collection.update_output_frontier(new_frontier.clone());
        }
        if let Some(new_frontier) = frontiers.write_frontier {
            replica_collection.update_write_frontier(new_frontier.clone());
            self.maybe_update_global_write_frontier(id, new_frontier);
        }
    }

    #[mz_ore::instrument(level = "debug")]
    fn handle_peek_response(
        &mut self,
        uuid: Uuid,
        response: PeekResponse,
        otel_ctx: OpenTelemetryContext,
        replica_id: ReplicaId,
    ) {
        otel_ctx.attach_as_parent();

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

        let notification = PeekNotification::new(&response, peek.offset, peek.limit);
        // NOTE: We use the `otel_ctx` from the response, not the pending peek, because we
        // currently want the parent to be whatever the compute worker did with this peek.
        self.deliver_response(ComputeControllerResponse::PeekNotification(
            uuid,
            notification,
            otel_ctx,
        ));

        self.finish_peek(uuid, response)
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
            soft_panic_or_log!(
                "received response for an unknown subscribe \
                 (subscribe_id={subscribe_id}, replica_id={replica_id})",
            );
            return;
        }
        let Some(replica) = self.replicas.get_mut(&replica_id) else {
            soft_panic_or_log!(
                "subscribe response for an unknown replica (replica_id={replica_id})"
            );
            return;
        };
        let Some(replica_collection) = replica.collections.get_mut(&subscribe_id) else {
            soft_panic_or_log!(
                "subscribe response for an unknown replica collection \
                 (subscribe_id={subscribe_id}, replica_id={replica_id})"
            );
            return;
        };

        // Always apply replica write frontier updates. Even if the subscribe is not tracked
        // anymore, there might still be replicas reading from its inputs, so we need to track the
        // frontiers until all replicas have advanced to the empty one.
        let write_frontier = match &response {
            SubscribeResponse::Batch(batch) => batch.upper.clone(),
            SubscribeResponse::DroppedAt(_) => Antichain::new(),
        };

        // For subscribes we downgrade all replica frontiers based on write frontiers. This should
        // be fine because the input and output frontier of a subscribe track its write frontier.
        // TODO(database-issues#4701): report subscribe frontiers through `Frontiers` responses
        replica_collection.update_write_frontier(write_frontier.clone());
        replica_collection.update_input_frontier(write_frontier.clone());
        replica_collection.update_output_frontier(write_frontier.clone());

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
        self.maybe_update_global_write_frontier(subscribe_id, write_frontier);

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
                && collection.shared.lock_write_frontier(|f| f.is_empty())
            {
                new_capabilities.insert(*id, Antichain::new());
                continue;
            }

            let compute_frontiers = collection.compute_dependency_ids().filter_map(|dep_id| {
                let collection = self.collections.get(&dep_id);
                collection.map(|c| c.write_frontier())
            });
            let storage_frontiers = collection.storage_dependency_ids().filter_map(|dep_id| {
                let frontiers = self.storage_collections.collection_frontiers(dep_id).ok();
                frontiers.map(|f| f.write_frontier)
            });

            let mut new_capability = Antichain::new();
            for frontier in compute_frontiers.chain(storage_frontiers) {
                for time in frontier.iter() {
                    new_capability.insert(time.step_back().unwrap_or(time.clone()));
                }
            }

            new_capabilities.insert(*id, new_capability);
        }

        for (id, new_capability) in new_capabilities {
            let collection = self.expect_collection_mut(id);
            let _ = collection.warmup_read_hold.try_downgrade(new_capability);
        }
    }

    /// Process pending maintenance work.
    ///
    /// This method is invoked periodically by the global controller.
    /// It is a good place to perform maintenance work that arises from various controller state
    /// changes and that cannot conveniently be handled synchronously with those state changes.
    #[mz_ore::instrument(level = "debug")]
    pub fn maintain(&mut self) {
        self.rehydrate_failed_replicas();
        self.downgrade_warmup_capabilities();
        self.schedule_collections();
        self.cleanup_collections();
        self.update_frontier_introspection();
        self.refresh_state_metrics();
        self.refresh_wallclock_lag();
    }
}

/// State maintained about individual compute collections.
///
/// A compute collection is either an index, or a storage sink, or a subscribe, exported by a
/// compute dataflow.
#[derive(Debug)]
struct CollectionState<T: ComputeControllerTimestamp> {
    /// Whether this collection is a log collection.
    ///
    /// Log collections are special in that they are only maintained by a subset of all replicas.
    log_collection: bool,
    /// Whether this collection has been dropped by a controller client.
    ///
    /// The controller is allowed to remove the `CollectionState` for a collection only when
    /// `dropped == true`. Otherwise, clients might still expect to be able to query information
    /// about this collection.
    dropped: bool,
    /// Whether this collection has been scheduled, i.e., the controller has sent a `Schedule`
    /// command for it.
    scheduled: bool,

    /// State shared with the `ComputeController`.
    shared: SharedCollectionState<T>,

    /// A read hold maintaining the implicit capability of the collection.
    ///
    /// This capability is kept to ensure that the collection remains readable according to its
    /// `read_policy`. It also ensures that read holds on the collection's dependencies are kept at
    /// some time not greater than the collection's `write_frontier`, guaranteeing that the
    /// collection's next outputs can always be computed without skipping times.
    implied_read_hold: ReadHold<T>,
    /// A read hold held to enable dataflow warmup.
    ///
    /// Dataflow warmup is an optimization that allows dataflows to immediately start hydrating
    /// even when their next output time (as implied by the `write_frontier`) is in the future.
    /// By installing a read capability derived from the write frontiers of the collection's
    /// inputs, we ensure that the as-of of new dataflows installed for the collection is at a time
    /// that is immediately available, so hydration can begin immediately too.
    warmup_read_hold: ReadHold<T>,
    /// The policy to use to downgrade `self.implied_read_hold`.
    ///
    /// If `None`, the collection is a write-only collection (i.e. a sink). For write-only
    /// collections, the `implied_read_hold` is only required for maintaining read holds on the
    /// inputs, so we can immediately downgrade it to the `write_frontier`.
    read_policy: Option<ReadPolicy<T>>,

    /// Storage identifiers on which this collection depends, and read holds this collection
    /// requires on them.
    storage_dependencies: BTreeMap<GlobalId, ReadHold<T>>,
    /// Compute identifiers on which this collection depends, and read holds this collection
    /// requires on them.
    compute_dependencies: BTreeMap<GlobalId, ReadHold<T>>,

    /// Introspection state associated with this collection.
    introspection: CollectionIntrospection<T>,
}

impl<T: ComputeControllerTimestamp> CollectionState<T> {
    /// Creates a new collection state, with an initial read policy valid from `since`.
    fn new(
        collection_id: GlobalId,
        as_of: Antichain<T>,
        shared: SharedCollectionState<T>,
        storage_dependencies: BTreeMap<GlobalId, ReadHold<T>>,
        compute_dependencies: BTreeMap<GlobalId, ReadHold<T>>,
        read_hold_tx: read_holds::ChangeTx<T>,
        introspection: CollectionIntrospection<T>,
    ) -> Self {
        // A collection is not readable before the `as_of`.
        let since = as_of.clone();
        // A collection won't produce updates for times before the `as_of`.
        let upper = as_of;

        // Ensure that the provided `shared` is valid for the given `as_of`.
        assert!(shared.lock_read_capabilities(|c| c.frontier() == since.borrow()));
        assert!(shared.lock_write_frontier(|f| f == &upper));

        // Initialize collection read holds.
        // Note that the implied read hold was already added to the `read_capabilities` when
        // `shared` was created, so we only need to add the warmup read hold here.
        let implied_read_hold =
            ReadHold::new(collection_id, since.clone(), Arc::clone(&read_hold_tx));
        let warmup_read_hold = ReadHold::new(collection_id, since.clone(), read_hold_tx);

        let updates = warmup_read_hold.since().iter().map(|t| (t.clone(), 1));
        shared.lock_read_capabilities(|c| {
            c.update_iter(updates);
        });

        Self {
            log_collection: false,
            dropped: false,
            scheduled: false,
            shared,
            implied_read_hold,
            warmup_read_hold,
            read_policy: Some(ReadPolicy::ValidFrom(since)),
            storage_dependencies,
            compute_dependencies,
            introspection,
        }
    }

    /// Creates a new collection state for a log collection.
    fn new_log_collection(
        id: GlobalId,
        shared: SharedCollectionState<T>,
        read_hold_tx: read_holds::ChangeTx<T>,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    ) -> Self {
        let since = Antichain::from_elem(T::minimum());
        let introspection =
            CollectionIntrospection::new(id, introspection_tx, since.clone(), false, None, None);
        let mut state = Self::new(
            id,
            since,
            shared,
            Default::default(),
            Default::default(),
            read_hold_tx,
            introspection,
        );
        state.log_collection = true;
        // Log collections are created and scheduled implicitly as part of replica initialization.
        state.scheduled = true;
        state
    }

    /// Reports the current read frontier.
    fn read_frontier(&self) -> Antichain<T> {
        self.shared
            .lock_read_capabilities(|c| c.frontier().to_owned())
    }

    /// Reports the current write frontier.
    fn write_frontier(&self) -> Antichain<T> {
        self.shared.lock_write_frontier(|f| f.clone())
    }

    fn storage_dependency_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.storage_dependencies.keys().copied()
    }

    fn compute_dependency_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.compute_dependencies.keys().copied()
    }

    /// Reports the IDs of the dependencies of this collection.
    fn dependency_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.compute_dependency_ids()
            .chain(self.storage_dependency_ids())
    }
}

/// Collection state shared with the `ComputeController`.
///
/// Having this allows certain controller APIs, such as `ComputeController::collection_frontiers`
/// and `ComputeController::acquire_read_hold` to be non-`async`. This comes at the cost of
/// complexity (by introducing shared mutable state) and performance (by introducing locking). We
/// should aim to reduce the amount of shared state over time, rather than expand it.
///
/// Note that [`SharedCollectionState`]s are initialized by the `ComputeController` prior to the
/// collection's creation in the [`Instance`]. This is to allow compute clients to query frontiers
/// and take new read holds immediately, without having to wait for the [`Instance`] to update.
#[derive(Clone, Debug)]
pub(super) struct SharedCollectionState<T> {
    /// Accumulation of read capabilities for the collection.
    ///
    /// This accumulation contains the capabilities held by all [`ReadHold`]s given out for the
    /// collection, including `implied_read_hold` and `warmup_read_hold`.
    ///
    /// NOTE: This field may only be modified by [`Instance::apply_read_hold_change`] and
    /// `ComputeController::acquire_read_hold`. Nobody else should modify read capabilities
    /// directly. Instead, collection users should manage read holds through [`ReadHold`] objects
    /// acquired through `ComputeController::acquire_read_hold`.
    ///
    /// TODO(teskje): Restructure the code to enforce the above in the type system.
    read_capabilities: Arc<Mutex<MutableAntichain<T>>>,
    /// The write frontier of this collection.
    write_frontier: Arc<Mutex<Antichain<T>>>,
}

impl<T: Timestamp> SharedCollectionState<T> {
    pub fn new(as_of: Antichain<T>) -> Self {
        // A collection is not readable before the `as_of`.
        let since = as_of.clone();
        // A collection won't produce updates for times before the `as_of`.
        let upper = as_of;

        // Initialize read capabilities to the `since`.
        // The is the implied read capability. The corresponding [`ReadHold`] is created in
        // [`CollectionState::new`].
        let mut read_capabilities = MutableAntichain::new();
        read_capabilities.update_iter(since.iter().map(|time| (time.clone(), 1)));

        Self {
            read_capabilities: Arc::new(Mutex::new(read_capabilities)),
            write_frontier: Arc::new(Mutex::new(upper)),
        }
    }

    pub fn lock_read_capabilities<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut MutableAntichain<T>) -> R,
    {
        let mut caps = self.read_capabilities.lock().expect("poisoned");
        f(&mut *caps)
    }

    pub fn lock_write_frontier<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut Antichain<T>) -> R,
    {
        let mut frontier = self.write_frontier.lock().expect("poisoned");
        f(&mut *frontier)
    }
}

/// Manages certain introspection relations associated with a collection. Upon creation, it adds
/// rows to introspection relations. When dropped, it retracts its managed rows.
///
/// TODO: `ComputeDependencies` could be moved under this.
#[derive(Debug)]
struct CollectionIntrospection<T: ComputeControllerTimestamp> {
    /// The ID of the compute collection.
    collection_id: GlobalId,
    /// A channel through which introspection updates are delivered.
    introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
    /// Introspection state for `IntrospectionType::Frontiers`.
    ///
    /// `Some` if the collection does _not_ sink into a storage collection (i.e. is not an MV). If
    /// the collection sinks into storage, the storage controller reports its frontiers instead.
    frontiers: Option<FrontiersIntrospectionState<T>>,
    /// Introspection state for `IntrospectionType::ComputeMaterializedViewRefreshes`.
    ///
    /// `Some` if the collection is a REFRESH MV.
    refresh: Option<RefreshIntrospectionState<T>>,
}

impl<T: ComputeControllerTimestamp> CollectionIntrospection<T> {
    fn new(
        collection_id: GlobalId,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
        as_of: Antichain<T>,
        storage_sink: bool,
        initial_as_of: Option<Antichain<T>>,
        refresh_schedule: Option<RefreshSchedule>,
    ) -> Self {
        let refresh =
            match (refresh_schedule, initial_as_of) {
                (Some(refresh_schedule), Some(initial_as_of)) => Some(
                    RefreshIntrospectionState::new(refresh_schedule, initial_as_of, &as_of),
                ),
                (refresh_schedule, _) => {
                    // If we have a `refresh_schedule`, then the collection is a MV, so we should also have
                    // an `initial_as_of`.
                    soft_assert_or_log!(
                        refresh_schedule.is_none(),
                        "`refresh_schedule` without an `initial_as_of`: {collection_id}"
                    );
                    None
                }
            };
        let frontiers = (!storage_sink).then(|| FrontiersIntrospectionState::new(as_of));

        let self_ = Self {
            collection_id,
            introspection_tx,
            frontiers,
            refresh,
        };

        self_.report_initial_state();
        self_
    }

    /// Reports the initial introspection state.
    fn report_initial_state(&self) {
        if let Some(frontiers) = &self.frontiers {
            let row = frontiers.row_for_collection(self.collection_id);
            let updates = vec![(row, 1)];
            self.send(IntrospectionType::Frontiers, updates);
        }

        if let Some(refresh) = &self.refresh {
            let row = refresh.row_for_collection(self.collection_id);
            let updates = vec![(row, 1)];
            self.send(IntrospectionType::ComputeMaterializedViewRefreshes, updates);
        }
    }

    /// Observe the given current collection frontiers and update the introspection state as
    /// necessary.
    fn observe_frontiers(&mut self, read_frontier: &Antichain<T>, write_frontier: &Antichain<T>) {
        self.update_frontier_introspection(read_frontier, write_frontier);
        self.update_refresh_introspection(write_frontier);
    }

    fn update_frontier_introspection(
        &mut self,
        read_frontier: &Antichain<T>,
        write_frontier: &Antichain<T>,
    ) {
        let Some(frontiers) = &mut self.frontiers else {
            return;
        };

        if &frontiers.read_frontier == read_frontier && &frontiers.write_frontier == write_frontier
        {
            return; // no change
        };

        let retraction = frontiers.row_for_collection(self.collection_id);
        frontiers.update(read_frontier, write_frontier);
        let insertion = frontiers.row_for_collection(self.collection_id);
        let updates = vec![(retraction, -1), (insertion, 1)];
        self.send(IntrospectionType::Frontiers, updates);
    }

    fn update_refresh_introspection(&mut self, write_frontier: &Antichain<T>) {
        let Some(refresh) = &mut self.refresh else {
            return;
        };

        let retraction = refresh.row_for_collection(self.collection_id);
        refresh.frontier_update(write_frontier);
        let insertion = refresh.row_for_collection(self.collection_id);

        if retraction == insertion {
            return; // no change
        }

        let updates = vec![(retraction, -1), (insertion, 1)];
        self.send(IntrospectionType::ComputeMaterializedViewRefreshes, updates);
    }

    fn send(&self, introspection_type: IntrospectionType, updates: Vec<(Row, Diff)>) {
        // Failure to send means the `ComputeController` has been dropped and doesn't care about
        // introspection updates anymore.
        let _ = self.introspection_tx.send((introspection_type, updates));
    }
}

impl<T: ComputeControllerTimestamp> Drop for CollectionIntrospection<T> {
    fn drop(&mut self) {
        // Retract collection frontiers.
        if let Some(frontiers) = &self.frontiers {
            let row = frontiers.row_for_collection(self.collection_id);
            let updates = vec![(row, -1)];
            self.send(IntrospectionType::Frontiers, updates);
        }

        // Retract MV refresh state.
        if let Some(refresh) = &self.refresh {
            let retraction = refresh.row_for_collection(self.collection_id);
            let updates = vec![(retraction, -1)];
            self.send(IntrospectionType::ComputeMaterializedViewRefreshes, updates);
        }
    }
}

#[derive(Debug)]
struct FrontiersIntrospectionState<T> {
    read_frontier: Antichain<T>,
    write_frontier: Antichain<T>,
}

impl<T: ComputeControllerTimestamp> FrontiersIntrospectionState<T> {
    fn new(as_of: Antichain<T>) -> Self {
        Self {
            read_frontier: as_of.clone(),
            write_frontier: as_of,
        }
    }

    /// Return a `Row` reflecting the current collection frontiers.
    fn row_for_collection(&self, collection_id: GlobalId) -> Row {
        let read_frontier = self
            .read_frontier
            .as_option()
            .map_or(Datum::Null, |ts| ts.clone().into());
        let write_frontier = self
            .write_frontier
            .as_option()
            .map_or(Datum::Null, |ts| ts.clone().into());
        Row::pack_slice(&[
            Datum::String(&collection_id.to_string()),
            read_frontier,
            write_frontier,
        ])
    }

    /// Update the introspection state with the given new frontiers.
    fn update(&mut self, read_frontier: &Antichain<T>, write_frontier: &Antichain<T>) {
        if read_frontier != &self.read_frontier {
            self.read_frontier.clone_from(read_frontier);
        }
        if write_frontier != &self.write_frontier {
            self.write_frontier.clone_from(write_frontier);
        }
    }
}

/// Information needed to compute introspection updates for a REFRESH materialized view when the
/// write frontier advances.
#[derive(Debug)]
struct RefreshIntrospectionState<T> {
    // Immutable properties of the MV
    refresh_schedule: RefreshSchedule,
    initial_as_of: Antichain<T>,
    // Refresh state
    next_refresh: Datum<'static>,           // Null or an MzTimestamp
    last_completed_refresh: Datum<'static>, // Null or an MzTimestamp
}

impl<T> RefreshIntrospectionState<T> {
    /// Return a `Row` reflecting the current refresh introspection state.
    fn row_for_collection(&self, collection_id: GlobalId) -> Row {
        Row::pack_slice(&[
            Datum::String(&collection_id.to_string()),
            self.last_completed_refresh,
            self.next_refresh,
        ])
    }
}

impl<T: ComputeControllerTimestamp> RefreshIntrospectionState<T> {
    /// Construct a new [`RefreshIntrospectionState`], and apply an initial `frontier_update()` at
    /// the `upper`.
    fn new(
        refresh_schedule: RefreshSchedule,
        initial_as_of: Antichain<T>,
        upper: &Antichain<T>,
    ) -> Self {
        let mut self_ = Self {
            refresh_schedule: refresh_schedule.clone(),
            initial_as_of: initial_as_of.clone(),
            next_refresh: Datum::Null,
            last_completed_refresh: Datum::Null,
        };
        self_.frontier_update(upper);
        self_
    }

    /// Should be called whenever the write frontier of the collection advances. It updates the
    /// state that should be recorded in introspection relations, but doesn't send the updates yet.
    fn frontier_update(&mut self, write_frontier: &Antichain<T>) {
        if write_frontier.is_empty() {
            self.last_completed_refresh =
                if let Some(last_refresh) = self.refresh_schedule.last_refresh() {
                    last_refresh.into()
                } else {
                    // If there is no last refresh, then we have a `REFRESH EVERY`, in which case
                    // the saturating roundup puts a refresh at the maximum possible timestamp.
                    T::maximum().into()
                };
            self.next_refresh = Datum::Null;
        } else {
            if PartialOrder::less_equal(write_frontier, &self.initial_as_of) {
                // We are before the first refresh.
                self.last_completed_refresh = Datum::Null;
                let initial_as_of = self.initial_as_of.as_option().expect(
                    "initial_as_of can't be [], because then there would be no refreshes at all",
                );
                let first_refresh = initial_as_of
                    .round_up(&self.refresh_schedule)
                    .expect("sequencing makes sure that REFRESH MVs always have a first refresh");
                soft_assert_or_log!(
                    first_refresh == *initial_as_of,
                    "initial_as_of should be set to the first refresh"
                );
                self.next_refresh = first_refresh.into();
            } else {
                // The first refresh has already happened.
                let write_frontier = write_frontier.as_option().expect("checked above");
                self.last_completed_refresh = write_frontier
                    .round_down_minus_1(&self.refresh_schedule)
                    .map_or_else(
                        || {
                            soft_panic_or_log!(
                                "rounding down should have returned the first refresh or later"
                            );
                            Datum::Null
                        },
                        |last_completed_refresh| last_completed_refresh.into(),
                    );
                self.next_refresh = write_frontier.clone().into();
            }
        }
    }
}

/// A note of an outstanding peek response.
#[derive(Debug)]
struct PendingPeek<T: Timestamp> {
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
    /// The read hold installed to serve this peek.
    read_hold: ReadHold<T>,
    /// The channel to send peek results.
    peek_response_tx: oneshot::Sender<PeekResponse>,
    /// An optional limit of the peek's result size.
    limit: Option<usize>,
    /// The offset into the peek's result.
    offset: usize,
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
    fn new(target_replica: Option<ReplicaId>) -> Self {
        Self {
            frontier: Antichain::from_elem(T::minimum()),
            target_replica,
        }
    }
}

/// State maintained about individual replicas.
#[derive(Debug)]
struct ReplicaState<T: ComputeControllerTimestamp> {
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
    /// The epoch of the replica.
    epoch: ClusterStartupEpoch,
}

impl<T: ComputeControllerTimestamp> ReplicaState<T> {
    fn new(
        id: ReplicaId,
        client: ReplicaClient<T>,
        config: ReplicaConfig,
        metrics: ReplicaMetrics,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
        epoch: ClusterStartupEpoch,
    ) -> Self {
        Self {
            id,
            client,
            config,
            metrics,
            introspection_tx,
            epoch,
            collections: Default::default(),
        }
    }

    /// Add a collection to the replica state.
    ///
    /// # Panics
    ///
    /// Panics if a collection with the same ID exists already.
    fn add_collection(
        &mut self,
        id: GlobalId,
        as_of: Antichain<T>,
        input_read_holds: Vec<ReadHold<T>>,
    ) {
        let metrics = self.metrics.for_collection(id);
        let introspection = ReplicaCollectionIntrospection::new(
            self.id,
            id,
            self.introspection_tx.clone(),
            as_of.clone(),
        );
        let mut state =
            ReplicaCollectionState::new(metrics, as_of, introspection, input_read_holds);

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

        // In an effort to keep the produced wallclock lag introspection data small and
        // predictable, we disable wallclock lag tracking for transient collections, i.e. slow-path
        // select indexes and subscribes.
        if id.is_transient() {
            state.wallclock_lag_max = None;
        }

        if let Some(previous) = self.collections.insert(id, state) {
            panic!("attempt to add a collection with existing ID {id} (previous={previous:?}");
        }
    }

    /// Remove state for a collection.
    fn remove_collection(&mut self, id: GlobalId) -> Option<ReplicaCollectionState<T>> {
        self.collections.remove(&id)
    }

    /// Returns whether all replica frontiers of the given collection are empty.
    fn collection_frontiers_empty(&self, id: GlobalId) -> bool {
        self.collections.get(&id).map_or(true, |c| {
            c.write_frontier.is_empty()
                && c.input_frontier.is_empty()
                && c.output_frontier.is_empty()
        })
    }

    /// Returns the state of the [`ReplicaState`] formatted as JSON.
    ///
    /// The returned value is not guaranteed to be stable and may change at any point in time.
    #[mz_ore::instrument(level = "debug")]
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
            epoch,
            collections,
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
            field("epoch", epoch)?,
        ]);
        Ok(serde_json::Value::Object(map))
    }
}

#[derive(Debug)]
struct ReplicaCollectionState<T: ComputeControllerTimestamp> {
    /// The replica write frontier of this collection.
    ///
    /// See [`FrontiersResponse::write_frontier`].
    write_frontier: Antichain<T>,
    /// The replica input frontier of this collection.
    ///
    /// See [`FrontiersResponse::input_frontier`].
    input_frontier: Antichain<T>,
    /// The replica output frontier of this collection.
    ///
    /// See [`FrontiersResponse::output_frontier`].
    output_frontier: Antichain<T>,

    /// Metrics tracked for this collection.
    ///
    /// If this is `None`, no metrics are collected.
    metrics: Option<ReplicaCollectionMetrics>,
    /// Time at which this collection was installed.
    created_at: Instant,
    /// As-of frontier with which this collection was installed on the replica.
    as_of: Antichain<T>,
    /// Whether the collection is hydrated.
    hydrated: bool,
    /// Tracks introspection state for this collection.
    introspection: ReplicaCollectionIntrospection<T>,
    /// Read holds on storage inputs to this collection.
    ///
    /// These read holds are kept to ensure that the replica is able to read from storage inputs at
    /// all times it hasn't read yet. We only need to install read holds for storage inputs since
    /// compaction of compute inputs is implicitly held back by Timely/DD.
    input_read_holds: Vec<ReadHold<T>>,

    /// Maximum frontier wallclock lag since the last introspection update.
    ///
    /// If this is `None`, wallclock lag is not tracked for this collection.
    wallclock_lag_max: Option<Duration>,
}

impl<T: ComputeControllerTimestamp> ReplicaCollectionState<T> {
    fn new(
        metrics: Option<ReplicaCollectionMetrics>,
        as_of: Antichain<T>,
        introspection: ReplicaCollectionIntrospection<T>,
        input_read_holds: Vec<ReadHold<T>>,
    ) -> Self {
        Self {
            write_frontier: as_of.clone(),
            input_frontier: as_of.clone(),
            output_frontier: as_of.clone(),
            metrics,
            created_at: Instant::now(),
            as_of,
            hydrated: false,
            introspection,
            input_read_holds,
            wallclock_lag_max: Some(Duration::ZERO),
        }
    }

    /// Returns whether this collection is hydrated.
    fn hydrated(&self) -> bool {
        self.hydrated
    }

    /// Marks the collection as hydrated and updates metrics and introspection accordingly.
    fn set_hydrated(&mut self) {
        if let Some(metrics) = &self.metrics {
            let duration = self.created_at.elapsed().as_secs_f64();
            metrics.initial_output_duration_seconds.set(duration);
        }

        self.hydrated = true;
    }

    /// Updates the replica write frontier of this collection.
    fn update_write_frontier(&mut self, new_frontier: Antichain<T>) {
        if PartialOrder::less_than(&new_frontier, &self.write_frontier) {
            soft_panic_or_log!(
                "replica collection write frontier regression (old={:?}, new={new_frontier:?})",
                self.write_frontier,
            );
            return;
        } else if new_frontier == self.write_frontier {
            return;
        }

        self.write_frontier = new_frontier;
    }

    /// Updates the replica input frontier of this collection.
    fn update_input_frontier(&mut self, new_frontier: Antichain<T>) {
        if PartialOrder::less_than(&new_frontier, &self.input_frontier) {
            soft_panic_or_log!(
                "replica collection input frontier regression (old={:?}, new={new_frontier:?})",
                self.input_frontier,
            );
            return;
        } else if new_frontier == self.input_frontier {
            return;
        }

        self.input_frontier = new_frontier;

        // Relax our read holds on collection inputs.
        for read_hold in &mut self.input_read_holds {
            let result = read_hold.try_downgrade(self.input_frontier.clone());
            soft_assert_or_log!(
                result.is_ok(),
                "read hold downgrade failed (read_hold={read_hold:?}, new_since={:?})",
                self.input_frontier,
            );
        }
    }

    /// Updates the replica output frontier of this collection.
    fn update_output_frontier(&mut self, new_frontier: Antichain<T>) {
        if PartialOrder::less_than(&new_frontier, &self.output_frontier) {
            soft_panic_or_log!(
                "replica collection output frontier regression (old={:?}, new={new_frontier:?})",
                self.output_frontier,
            );
            return;
        } else if new_frontier == self.output_frontier {
            return;
        }

        self.output_frontier = new_frontier;

        // If the observed frontier is greater than the collection's as-of, the collection has
        // produced some output and is therefore hydrated now.
        if !self.hydrated() && PartialOrder::less_than(&self.as_of, &self.output_frontier) {
            self.set_hydrated();
        }
    }
}

/// Maintains the introspection state for a given replica and collection, and ensures that reported
/// introspection data is retracted when the collection is dropped.
#[derive(Debug)]
struct ReplicaCollectionIntrospection<T: ComputeControllerTimestamp> {
    /// The ID of the replica.
    replica_id: ReplicaId,
    /// The ID of the compute collection.
    collection_id: GlobalId,
    /// Operator-level hydration state.
    /// (lir_id, worker_id) -> hydrated
    operators: BTreeMap<(LirId, usize), bool>,
    /// The collection's reported replica write frontier.
    write_frontier: Antichain<T>,
    /// A channel through which introspection updates are delivered.
    introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
}

impl<T: ComputeControllerTimestamp> ReplicaCollectionIntrospection<T> {
    /// Create a new `HydrationState` and initialize introspection.
    fn new(
        replica_id: ReplicaId,
        collection_id: GlobalId,
        introspection_tx: crossbeam_channel::Sender<IntrospectionUpdates>,
        as_of: Antichain<T>,
    ) -> Self {
        let self_ = Self {
            replica_id,
            collection_id,
            operators: Default::default(),
            write_frontier: as_of,
            introspection_tx,
        };

        self_.report_initial_state();
        self_
    }

    /// Reports the initial introspection state.
    fn report_initial_state(&self) {
        let row = self.write_frontier_row();
        let updates = vec![(row, 1)];
        self.send(IntrospectionType::ReplicaFrontiers, updates);
    }

    /// Update the given (lir_id, worker_id) pair as hydrated.
    fn operator_hydrated(&mut self, lir_id: LirId, worker_id: usize, hydrated: bool) {
        let retraction = self.operator_hydration_row(lir_id, worker_id);
        self.operators.insert((lir_id, worker_id), hydrated);
        let insertion = self.operator_hydration_row(lir_id, worker_id);

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

    /// Return a `Row` reflecting the current hydration status of the identified operator.
    ///
    /// Returns `None` if the identified operator is not tracked.
    fn operator_hydration_row(&self, lir_id: LirId, worker_id: usize) -> Option<Row> {
        self.operators.get(&(lir_id, worker_id)).map(|hydrated| {
            Row::pack_slice(&[
                Datum::String(&self.collection_id.to_string()),
                Datum::UInt64(lir_id.into()),
                Datum::String(&self.replica_id.to_string()),
                Datum::UInt64(u64::cast_from(worker_id)),
                Datum::from(*hydrated),
            ])
        })
    }

    /// Observe the given current write frontier and update the introspection state as necessary.
    fn observe_frontier(&mut self, write_frontier: &Antichain<T>) {
        if self.write_frontier == *write_frontier {
            return; // no change
        }

        let retraction = self.write_frontier_row();
        self.write_frontier.clone_from(write_frontier);
        let insertion = self.write_frontier_row();

        let updates = vec![(retraction, -1), (insertion, 1)];
        self.send(IntrospectionType::ReplicaFrontiers, updates);
    }

    /// Return a `Row` reflecting the current replica write frontier.
    fn write_frontier_row(&self) -> Row {
        let write_frontier = self
            .write_frontier
            .as_option()
            .map_or(Datum::Null, |ts| ts.clone().into());
        Row::pack_slice(&[
            Datum::String(&self.collection_id.to_string()),
            Datum::String(&self.replica_id.to_string()),
            write_frontier,
        ])
    }

    fn send(&self, introspection_type: IntrospectionType, updates: Vec<(Row, Diff)>) {
        // Failure to send means the `ComputeController` has been dropped and doesn't care about
        // introspection updates anymore.
        let _ = self.introspection_tx.send((introspection_type, updates));
    }
}

impl<T: ComputeControllerTimestamp> Drop for ReplicaCollectionIntrospection<T> {
    fn drop(&mut self) {
        // Retract operator hydration status.
        let operators: Vec<_> = self.operators.keys().collect();
        let updates: Vec<_> = operators
            .into_iter()
            .flat_map(|(lir_id, worker_id)| self.operator_hydration_row(*lir_id, *worker_id))
            .map(|r| (r, -1))
            .collect();
        if !updates.is_empty() {
            self.send(IntrospectionType::ComputeOperatorHydrationStatus, updates);
        }

        // Retract the write frontier.
        let row = self.write_frontier_row();
        let updates = vec![(row, -1)];
        self.send(IntrospectionType::ReplicaFrontiers, updates);
    }
}
