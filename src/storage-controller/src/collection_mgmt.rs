// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tokio tasks (and support machinery) for maintaining storage-managed
//! collections.
//!
//! We differentiate between append-only collections and differential
//! collections. The intent is that knowing the type allows being more
//! intentional about what state we keep in memory and how we work when in
//! read-only mode / during zero-downtime upgrades.
//!
//! ## Append-only collections
//!
//! Writers only append blind writes. Those writes never fail. It does not
//! matter at what timestamp they happen (to a degree, but ...).
//!
//! While in read-only mode, the append-only write task can immediately write
//! updates out as batches, but only append them when going out of read-only
//! mode.
//!
//! ## Differential collections
//!
//! These are very similar to the self-correcting persist_sink. We have an
//! in-memory desired state and continually make it so that persist matches
//! desired. As described below (in the task implementation), we could do this
//! in a memory efficient way by keeping open a persist read handle and
//! continually updating/consolidating our desired collection. This way, we
//! would be memory-efficient even in read-only mode.
//!
//! This is an evolution of the current design where, on startup, we bring the
//! persist collection into a known state (mostly by retracting everything) and
//! then assume that this `envd` is the only writer. We panic when that is ever
//! not the case, which we notice when the upper of a collection changes
//! unexpectedly. With this new design we can instead continually update our
//! view of the persist shard and emit updates when needed, when desired
//! changed.
//!
//! NOTE: As it is, we always keep all of desired in memory. Only when told to
//! go out of read-only mode would we start attempting to write.
//!
//! ## Read-only mode
//!
//! When [`CollectionManager`] is in read-only mode it cannot write out to
//! persist. It will, however, maintain the `desired` state of differential
//! collections so that we can immediately start writing out updates when going
//! out of read-only mode.
//!
//! For append-only collections we either panic, in the case of
//! [`CollectionManager::blind_write`], or report back a
//! [`StorageError::ReadOnly`] when trying to append through a
//! [`MonotonicAppender`] returned from
//! [`CollectionManager::monotonic_appender`].

use std::any::Any;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::fmt::Debug;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail};
use chrono::{DateTime, Utc};
use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use futures::{Future, FutureExt};
use mz_cluster_client::ReplicaId;
use mz_dyncfg::ConfigSet;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::retry::Retry;
use mz_ore::soft_panic_or_log;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::Codec64;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{ColumnName, Diff, GlobalId, Row, TimestampManipulation};
use mz_storage_client::client::{AppendOnlyUpdate, Status, TimestamplessUpdate};
use mz_storage_client::controller::{IntrospectionType, MonotonicAppender, StorageWriteOp};
use mz_storage_client::healthcheck::{
    MZ_SINK_STATUS_HISTORY_DESC, MZ_SOURCE_STATUS_HISTORY_DESC, REPLICA_METRICS_HISTORY_DESC,
    WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW_DESC, WALLCLOCK_LAG_HISTORY_DESC,
};
use mz_storage_client::metrics::StorageControllerMetrics;
use mz_storage_client::statistics::ControllerSinkStatistics;
use mz_storage_client::storage_collections::StorageCollections;
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::InvalidUpper;
use mz_storage_types::dyncfgs::{
    REPLICA_METRICS_HISTORY_RETENTION_INTERVAL, WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RETENTION_INTERVAL,
    WALLCLOCK_LAG_HISTORY_RETENTION_INTERVAL,
};
use mz_storage_types::parameters::{
    STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT, StorageParameters,
};
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info};

use crate::{
    StatusHistoryDesc, StatusHistoryRetentionPolicy, StorageError, collection_mgmt,
    privatelink_status_history_desc, replica_status_history_desc, sink_status_history_desc,
    snapshot_statistics, source_status_history_desc, statistics,
};

// Default rate at which we advance the uppers of managed collections.
const DEFAULT_TICK_MS: u64 = 1_000;

/// A channel for sending writes to a differential collection.
type DifferentialWriteChannel<T> =
    mpsc::UnboundedSender<(StorageWriteOp, oneshot::Sender<Result<(), StorageError<T>>>)>;

/// A channel for sending writes to an append-only collection.
type AppendOnlyWriteChannel<T> = mpsc::UnboundedSender<(
    Vec<AppendOnlyUpdate>,
    oneshot::Sender<Result<(), StorageError<T>>>,
)>;

type WriteTask = AbortOnDropHandle<()>;
type ShutdownSender = oneshot::Sender<()>;

/// Types of storage-managed/introspection collections:
///
/// Append-only: Only accepts blind writes, writes that can be applied at any
/// timestamp and don’t depend on current collection contents.
///
/// Pseudo append-only: We treat them largely as append-only collections but
/// periodically (currently on bootstrap) retract old updates from them.
///
/// Differential: at any given time `t` , collection contents mirrors some
/// (small cardinality) state. The cardinality of the collection stays constant
/// if the thing that is mirrored doesn’t change in cardinality. At steady
/// state, updates always come in pairs of retractions/additions.
pub enum CollectionManagerKind {
    AppendOnly,
    Differential,
}

#[derive(Debug, Clone)]
pub struct CollectionManager<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    /// When a [`CollectionManager`] is in read-only mode it must not affect any
    /// changes to external state.
    read_only: bool,

    // WIP: Name TBD! I thought about `managed_collections`, `ivm_collections`,
    // `self_correcting_collections`.
    /// These are collections that we write to by adding/removing updates to an
    /// internal _desired_ collection. The `CollectionManager` continually makes
    /// sure that collection contents (in persist) match the desired state.
    differential_collections:
        Arc<Mutex<BTreeMap<GlobalId, (DifferentialWriteChannel<T>, WriteTask, ShutdownSender)>>>,

    /// Collections that we only append to using blind-writes.
    ///
    /// Every write succeeds at _some_ timestamp, and we never check what the
    /// actual contents of the collection (in persist) are.
    append_only_collections:
        Arc<Mutex<BTreeMap<GlobalId, (AppendOnlyWriteChannel<T>, WriteTask, ShutdownSender)>>>,

    /// Amount of time we'll wait before sending a batch of inserts to Persist, for user
    /// collections.
    user_batch_duration_ms: Arc<AtomicU64>,
    now: NowFn,
}

/// The `CollectionManager` provides two complementary functions:
/// - Providing an API to append values to a registered set of collections.
///   For this usecase:
///     - The `CollectionManager` expects to be the only writer.
///     - Appending to a closed collection panics
/// - Automatically advancing the timestamp of managed collections every
///   second. For this usecase:
///     - The `CollectionManager` handles contention by permitting and ignoring errors.
///     - Closed collections will not panic if they continue receiving these requests.
impl<T> CollectionManager<T>
where
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    pub(super) fn new(read_only: bool, now: NowFn) -> CollectionManager<T> {
        let batch_duration_ms: u64 = STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT
            .as_millis()
            .try_into()
            .expect("known to fit");

        CollectionManager {
            read_only,
            differential_collections: Arc::new(Mutex::new(BTreeMap::new())),
            append_only_collections: Arc::new(Mutex::new(BTreeMap::new())),
            user_batch_duration_ms: Arc::new(AtomicU64::new(batch_duration_ms)),
            now,
        }
    }

    /// Updates the duration we'll wait to batch events for user owned collections.
    pub fn update_user_batch_duration(&self, duration: Duration) {
        tracing::info!(?duration, "updating user batch duration");
        let millis: u64 = duration.as_millis().try_into().unwrap_or(u64::MAX);
        self.user_batch_duration_ms.store(millis, Ordering::Relaxed);
    }

    /// Registers a new _differential collection_.
    ///
    /// The [CollectionManager] will automatically advance the upper of every
    /// registered collection every second.
    ///
    /// Update the `desired` state of a differential collection using
    /// [Self::differential_write].
    pub(super) fn register_differential_collection<R>(
        &self,
        id: GlobalId,
        write_handle: WriteHandle<SourceData, (), T, StorageDiff>,
        read_handle_fn: R,
        force_writable: bool,
        introspection_config: DifferentialIntrospectionConfig<T>,
    ) where
        R: FnMut() -> Pin<
                Box<dyn Future<Output = ReadHandle<SourceData, (), T, StorageDiff>> + Send>,
            > + Send
            + Sync
            + 'static,
    {
        let mut guard = self
            .differential_collections
            .lock()
            .expect("collection_mgmt panicked");

        // Check if this collection is already registered.
        if let Some((_writer, task, _shutdown_tx)) = guard.get(&id) {
            // The collection is already registered and the task is still running so nothing to do.
            if !task.is_finished() {
                // TODO(parkmycar): Panic here if we never see this error in production.
                tracing::error!("Registered a collection twice! {id:?}");
                return;
            }
        }

        let read_only = self.get_read_only(id, force_writable);

        // Spawns a new task so we can write to this collection.
        let writer_and_handle = DifferentialWriteTask::spawn(
            id,
            write_handle,
            read_handle_fn,
            read_only,
            self.now.clone(),
            introspection_config,
        );
        let prev = guard.insert(id, writer_and_handle);

        // Double check the previous task was actually finished.
        if let Some((_, prev_task, _)) = prev {
            assert!(
                prev_task.is_finished(),
                "should only spawn a new task if the previous is finished"
            );
        }
    }

    /// Registers a new _append-only collection_.
    ///
    /// The [CollectionManager] will automatically advance the upper of every
    /// registered collection every second.
    pub(super) fn register_append_only_collection(
        &self,
        id: GlobalId,
        write_handle: WriteHandle<SourceData, (), T, StorageDiff>,
        force_writable: bool,
        introspection_config: Option<AppendOnlyIntrospectionConfig<T>>,
    ) {
        let mut guard = self
            .append_only_collections
            .lock()
            .expect("collection_mgmt panicked");

        // Check if this collection is already registered.
        if let Some((_writer, task, _shutdown_tx)) = guard.get(&id) {
            // The collection is already registered and the task is still running so nothing to do.
            if !task.is_finished() {
                // TODO(parkmycar): Panic here if we never see this error in production.
                tracing::error!("Registered a collection twice! {id:?}");
                return;
            }
        }

        let read_only = self.get_read_only(id, force_writable);

        // Spawns a new task so we can write to this collection.
        let writer_and_handle = AppendOnlyWriteTask::spawn(
            id,
            write_handle,
            read_only,
            self.now.clone(),
            Arc::clone(&self.user_batch_duration_ms),
            introspection_config,
        );
        let prev = guard.insert(id, writer_and_handle);

        // Double check the previous task was actually finished.
        if let Some((_, prev_task, _)) = prev {
            assert!(
                prev_task.is_finished(),
                "should only spawn a new task if the previous is finished"
            );
        }
    }

    /// Unregisters the given collection.
    ///
    /// Also waits until the `CollectionManager` has completed all outstanding work to ensure that
    /// it has stopped referencing the provided `id`.
    #[mz_ore::instrument(level = "debug")]
    pub(super) fn unregister_collection(&self, id: GlobalId) -> BoxFuture<'static, ()> {
        let prev = self
            .differential_collections
            .lock()
            .expect("CollectionManager panicked")
            .remove(&id);

        // Wait for the task to complete before reporting as unregisted.
        if let Some((_prev_writer, prev_task, shutdown_tx)) = prev {
            // Notify the task it needs to shutdown.
            //
            // We can ignore errors here because they indicate the task is already done.
            let _ = shutdown_tx.send(());
            return Box::pin(prev_task.map(|_| ()));
        }

        let prev = self
            .append_only_collections
            .lock()
            .expect("CollectionManager panicked")
            .remove(&id);

        // Wait for the task to complete before reporting as unregisted.
        if let Some((_prev_writer, prev_task, shutdown_tx)) = prev {
            // Notify the task it needs to shutdown.
            //
            // We can ignore errors here because they indicate the task is already done.
            let _ = shutdown_tx.send(());
            return Box::pin(prev_task.map(|_| ()));
        }

        Box::pin(futures::future::ready(()))
    }

    /// Returns a sender for writes to the given append-only collection.
    ///
    /// # Panics
    /// - If `id` does not belong to an append-only collections.
    pub(super) fn append_only_write_sender(&self, id: GlobalId) -> AppendOnlyWriteChannel<T> {
        let collections = self.append_only_collections.lock().expect("poisoned");
        match collections.get(&id) {
            Some((tx, _, _)) => tx.clone(),
            None => panic!("missing append-only collection: {id}"),
        }
    }

    /// Returns a sender for writes to the given differential collection.
    ///
    /// # Panics
    /// - If `id` does not belong to a differential collections.
    pub(super) fn differential_write_sender(&self, id: GlobalId) -> DifferentialWriteChannel<T> {
        let collections = self.differential_collections.lock().expect("poisoned");
        match collections.get(&id) {
            Some((tx, _, _)) => tx.clone(),
            None => panic!("missing differential collection: {id}"),
        }
    }

    /// Appends `updates` to the append-only collection identified by `id`, at
    /// _some_ timestamp. Does not wait for the append to complete.
    ///
    /// # Panics
    /// - If `id` does not belong to an append-only collections.
    /// - If this [`CollectionManager`] is in read-only mode.
    /// - If the collection closed.
    pub(super) fn blind_write(&self, id: GlobalId, updates: Vec<AppendOnlyUpdate>) {
        if self.read_only {
            panic!("attempting blind write to {} while in read-only mode", id);
        }

        if !updates.is_empty() {
            let update_tx = self.append_only_write_sender(id);
            let (tx, _rx) = oneshot::channel();
            update_tx.send((updates, tx)).expect("rx hung up");
        }
    }

    /// Updates the desired collection state of the differential collection identified by
    /// `id`. The underlying persist shard will reflect this change at
    /// _some_point. Does not wait for the change to complete.
    ///
    /// # Panics
    /// - If `id` does not belong to a differential collection.
    /// - If the collection closed.
    pub(super) fn differential_write(&self, id: GlobalId, op: StorageWriteOp) {
        if !op.is_empty_append() {
            let update_tx = self.differential_write_sender(id);
            let (tx, _rx) = oneshot::channel();
            update_tx.send((op, tx)).expect("rx hung up");
        }
    }

    /// Appends the given `updates` to the differential collection identified by `id`.
    ///
    /// # Panics
    /// - If `id` does not belong to a differential collection.
    /// - If the collection closed.
    pub(super) fn differential_append(&self, id: GlobalId, updates: Vec<(Row, Diff)>) {
        self.differential_write(id, StorageWriteOp::Append { updates })
    }

    /// Returns a [`MonotonicAppender`] that can be used to monotonically append updates to the
    /// collection correlated with `id`.
    pub(super) fn monotonic_appender(
        &self,
        id: GlobalId,
    ) -> Result<MonotonicAppender<T>, StorageError<T>> {
        let guard = self
            .append_only_collections
            .lock()
            .expect("CollectionManager panicked");
        let tx = guard
            .get(&id)
            .map(|(tx, _, _)| tx.clone())
            .ok_or(StorageError::IdentifierMissing(id))?;

        Ok(MonotonicAppender::new(tx))
    }

    fn get_read_only(&self, id: GlobalId, force_writable: bool) -> bool {
        if force_writable {
            assert!(id.is_system(), "unexpected non-system global id: {id:?}");
            false
        } else {
            self.read_only
        }
    }
}

pub(crate) struct DifferentialIntrospectionConfig<T>
where
    T: Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    pub(crate) recent_upper: Antichain<T>,
    pub(crate) introspection_type: IntrospectionType,
    pub(crate) storage_collections: Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
    pub(crate) collection_manager: collection_mgmt::CollectionManager<T>,
    pub(crate) source_statistics: Arc<Mutex<statistics::SourceStatistics>>,
    pub(crate) sink_statistics:
        Arc<Mutex<BTreeMap<(GlobalId, Option<ReplicaId>), ControllerSinkStatistics>>>,
    pub(crate) statistics_interval: Duration,
    pub(crate) statistics_interval_receiver: watch::Receiver<Duration>,
    pub(crate) statistics_retention_duration: Duration,
    pub(crate) metrics: StorageControllerMetrics,
    pub(crate) introspection_tokens: Arc<Mutex<BTreeMap<GlobalId, Box<dyn Any + Send + Sync>>>>,
}

/// A task that will make it so that the state in persist matches the desired
/// state and continuously bump the upper for the specified collection.
///
/// NOTE: This implementation is a bit clunky, and could be optimized by not keeping
/// all of desired in memory (see commend below). It is meant to showcase the
/// general approach.
struct DifferentialWriteTask<T, R>
where
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
    R: FnMut() -> Pin<Box<dyn Future<Output = ReadHandle<SourceData, (), T, StorageDiff>> + Send>>
        + Send
        + 'static,
{
    /// The collection that we are writing to.
    id: GlobalId,

    write_handle: WriteHandle<SourceData, (), T, StorageDiff>,

    /// For getting a [`ReadHandle`] to sync our state to persist contents.
    read_handle_fn: R,

    read_only: bool,

    now: NowFn,

    /// In the absence of updates, we regularly bump the upper to "now", on this
    /// interval. This makes it so the collection remains readable at recent
    /// timestamps.
    upper_tick_interval: tokio::time::Interval,

    /// Receiver for write commands. These change our desired state.
    cmd_rx: mpsc::UnboundedReceiver<(StorageWriteOp, oneshot::Sender<Result<(), StorageError<T>>>)>,

    /// We have to shut down when receiving from this.
    shutdown_rx: oneshot::Receiver<()>,

    /// The contents of the collection as it should be according to whoever is
    /// driving us around.
    // This is memory inefficient: we always keep a full copy of
    // desired, so that we can re-derive a to_write if/when someone else
    // writes to persist and we notice because of an upper conflict.
    // This is optimized for the case where we rarely have more than one
    // writer.
    //
    // We can optimize for a multi-writer case by keeping an open
    // ReadHandle and continually reading updates from persist, updating
    // a desired in place. Similar to the self-correcting persist_sink.
    desired: Vec<(Row, Diff)>,

    /// Updates that we have to write when next writing to persist. This is
    /// determined by looking at what is desired and what is in persist.
    to_write: Vec<(Row, Diff)>,

    /// Current upper of the persist shard. We keep track of this so that we
    /// realize when someone else writes to the shard, in which case we have to
    /// update our state of the world, that is update our `to_write` based on
    /// `desired` and the contents of the persist shard.
    current_upper: T,
}

impl<T, R> DifferentialWriteTask<T, R>
where
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
    R: FnMut() -> Pin<Box<dyn Future<Output = ReadHandle<SourceData, (), T, StorageDiff>> + Send>>
        + Send
        + Sync
        + 'static,
{
    /// Spawns a [`DifferentialWriteTask`] in an [`mz_ore::task`] and returns
    /// handles for interacting with it.
    fn spawn(
        id: GlobalId,
        write_handle: WriteHandle<SourceData, (), T, StorageDiff>,
        read_handle_fn: R,
        read_only: bool,
        now: NowFn,
        introspection_config: DifferentialIntrospectionConfig<T>,
    ) -> (DifferentialWriteChannel<T>, WriteTask, ShutdownSender) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let upper_tick_interval = tokio::time::interval(Duration::from_millis(DEFAULT_TICK_MS));

        let current_upper = T::minimum();

        let task = Self {
            id,
            write_handle,
            read_handle_fn,
            read_only,
            now,
            upper_tick_interval,
            cmd_rx: rx,
            shutdown_rx,
            desired: Vec::new(),
            to_write: Vec::new(),
            current_upper,
        };

        let handle = mz_ore::task::spawn(
            || format!("CollectionManager-differential_write_task-{id}"),
            async move {
                if !task.read_only {
                    task.prepare(introspection_config).await;
                }
                let res = task.run().await;

                match res {
                    ControlFlow::Break(reason) => {
                        info!("write_task-{} ending: {}", id, reason);
                    }
                    c => {
                        unreachable!(
                            "cannot break out of the loop with a Continue, but got: {:?}",
                            c
                        );
                    }
                }
            },
        );

        (tx, handle.abort_on_drop(), shutdown_tx)
    }

    /// Does any work that is required before this background task starts
    /// writing to the given introspection collection.
    ///
    /// This might include consolidation, deleting older entries or seeding
    /// in-memory state of, say, scrapers, with current collection contents.
    async fn prepare(&self, introspection_config: DifferentialIntrospectionConfig<T>) {
        tracing::info!(%self.id, ?introspection_config.introspection_type, "preparing differential introspection collection for writes");

        match introspection_config.introspection_type {
            IntrospectionType::ShardMapping => {
                // Done by the `append_shard_mappings` call.
            }
            IntrospectionType::Frontiers | IntrospectionType::ReplicaFrontiers => {
                // Differential collections start with an empty
                // desired state. No need to manually reset.
            }
            IntrospectionType::StorageSourceStatistics => {
                let prev = snapshot_statistics(
                    self.id,
                    introspection_config.recent_upper,
                    &introspection_config.storage_collections,
                )
                .await;

                let scraper_token = statistics::spawn_statistics_scraper(
                    self.id.clone(),
                    // These do a shallow copy.
                    introspection_config.collection_manager,
                    Arc::clone(&introspection_config.source_statistics),
                    prev,
                    introspection_config.statistics_interval.clone(),
                    introspection_config.statistics_interval_receiver.clone(),
                    introspection_config.statistics_retention_duration,
                    introspection_config.metrics,
                );
                let web_token = statistics::spawn_webhook_statistics_scraper(
                    introspection_config.source_statistics,
                    introspection_config.statistics_interval,
                    introspection_config.statistics_interval_receiver,
                );

                // Make sure these are dropped when the controller is
                // dropped, so that the internal task will stop.
                introspection_config
                    .introspection_tokens
                    .lock()
                    .expect("poisoned")
                    .insert(self.id, Box::new((scraper_token, web_token)));
            }
            IntrospectionType::StorageSinkStatistics => {
                let prev = snapshot_statistics(
                    self.id,
                    introspection_config.recent_upper,
                    &introspection_config.storage_collections,
                )
                .await;

                let scraper_token = statistics::spawn_statistics_scraper(
                    self.id.clone(),
                    introspection_config.collection_manager,
                    Arc::clone(&introspection_config.sink_statistics),
                    prev,
                    introspection_config.statistics_interval,
                    introspection_config.statistics_interval_receiver,
                    introspection_config.statistics_retention_duration,
                    introspection_config.metrics,
                );

                // Make sure this is dropped when the controller is
                // dropped, so that the internal task will stop.
                introspection_config
                    .introspection_tokens
                    .lock()
                    .expect("poisoned")
                    .insert(self.id, scraper_token);
            }

            IntrospectionType::ComputeDependencies
            | IntrospectionType::ComputeOperatorHydrationStatus
            | IntrospectionType::ComputeMaterializedViewRefreshes
            | IntrospectionType::ComputeErrorCounts
            | IntrospectionType::ComputeHydrationTimes => {
                // Differential collections start with an empty
                // desired state. No need to manually reset.
            }

            introspection_type @ IntrospectionType::ReplicaMetricsHistory
            | introspection_type @ IntrospectionType::WallclockLagHistory
            | introspection_type @ IntrospectionType::WallclockLagHistogram
            | introspection_type @ IntrospectionType::PreparedStatementHistory
            | introspection_type @ IntrospectionType::StatementExecutionHistory
            | introspection_type @ IntrospectionType::SessionHistory
            | introspection_type @ IntrospectionType::StatementLifecycleHistory
            | introspection_type @ IntrospectionType::SqlText
            | introspection_type @ IntrospectionType::SourceStatusHistory
            | introspection_type @ IntrospectionType::SinkStatusHistory
            | introspection_type @ IntrospectionType::PrivatelinkConnectionStatusHistory
            | introspection_type @ IntrospectionType::ReplicaStatusHistory => {
                unreachable!("not differential collection: {introspection_type:?}")
            }
        }
    }

    async fn run(mut self) -> ControlFlow<String> {
        const BATCH_SIZE: usize = 4096;
        let mut updates = Vec::with_capacity(BATCH_SIZE);
        loop {
            tokio::select! {
                // Prefer sending actual updates over just bumping the upper,
                // because sending updates also bump the upper.
                biased;

                // Listen for a shutdown signal so we can gracefully cleanup.
                _ = &mut self.shutdown_rx => {
                    self.handle_shutdown();

                    return ControlFlow::Break("graceful shutdown".to_string());
                }

                // Pull a chunk of queued updates off the channel.
                count = self.cmd_rx.recv_many(&mut updates, BATCH_SIZE) => {
                    if count > 0 {
                        let _ = self.handle_updates(&mut updates).await?;
                    } else {
                        // Sender has been dropped, which means the collection
                        // should have been unregistered, break out of the run
                        // loop if we weren't already aborted.
                        return ControlFlow::Break("sender has been dropped".to_string());
                    }
                }

                // If we haven't received any updates, then we'll move the upper forward.
                _ = self.upper_tick_interval.tick() => {
                    if self.read_only {
                        // Not bumping uppers while in read-only mode.
                        continue;
                    }
                    let _ = self.tick_upper().await?;
                },
            }
        }
    }

    async fn tick_upper(&mut self) -> ControlFlow<String> {
        let now = T::from((self.now)());

        if now <= self.current_upper {
            // Upper is already further along than current wall-clock time, no
            // need to bump it.
            return ControlFlow::Continue(());
        }

        assert!(!self.read_only);
        let res = self
            .write_handle
            .compare_and_append_batch(
                &mut [],
                Antichain::from_elem(self.current_upper.clone()),
                Antichain::from_elem(now.clone()),
                true,
            )
            .await
            .expect("valid usage");
        match res {
            // All good!
            Ok(()) => {
                tracing::debug!(%self.id, "bumped upper of differential collection");
                self.current_upper = now;
            }
            Err(err) => {
                // Someone else wrote to the collection or bumped the upper. We
                // need to sync to latest persist state and potentially patch up
                // our `to_write`, based on what we learn and `desired`.

                let actual_upper = if let Some(ts) = err.current.as_option() {
                    ts.clone()
                } else {
                    return ControlFlow::Break("upper is the empty antichain".to_string());
                };

                tracing::info!(%self.id, ?actual_upper, expected_upper = ?self.current_upper, "upper mismatch while bumping upper, syncing to persist state");

                self.current_upper = actual_upper;

                self.sync_to_persist().await;
            }
        }

        ControlFlow::Continue(())
    }

    fn handle_shutdown(&mut self) {
        let mut senders = Vec::new();

        // Prevent new messages from being sent.
        self.cmd_rx.close();

        // Get as many waiting senders as possible.
        while let Ok((_batch, sender)) = self.cmd_rx.try_recv() {
            senders.push(sender);
        }

        // Notify them that this collection is closed.
        //
        // Note: if a task is shutting down, that indicates the source has been
        // dropped, at which point the identifier is invalid. Returning this
        // error provides a better user experience.
        notify_listeners(senders, || Err(StorageError::IdentifierInvalid(self.id)));
    }

    async fn handle_updates(
        &mut self,
        batch: &mut Vec<(StorageWriteOp, oneshot::Sender<Result<(), StorageError<T>>>)>,
    ) -> ControlFlow<String> {
        // Put in place _some_ rate limiting.
        let batch_duration_ms = STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT;

        let use_batch_now = Instant::now();
        let min_time_to_complete = use_batch_now + batch_duration_ms;

        tracing::debug!(
            ?use_batch_now,
            ?batch_duration_ms,
            ?min_time_to_complete,
            "batch duration",
        );

        let mut responders = Vec::with_capacity(batch.len());
        for (op, tx) in batch.drain(..) {
            self.apply_write_op(op);
            responders.push(tx);
        }

        // TODO: Maybe don't do it every time?
        consolidation::consolidate(&mut self.desired);
        consolidation::consolidate(&mut self.to_write);

        // Reset the interval which is used to periodically bump the uppers
        // because the uppers will get bumped with the following update.
        // This makes it such that we will write at most once every
        // `interval`.
        //
        // For example, let's say our `DEFAULT_TICK` interval is 10, so at
        // `t + 10`, `t + 20`, ... we'll bump the uppers. If we receive an
        // update at `t + 3` we want to shift this window so we bump the
        // uppers at `t + 13`, `t + 23`, ... which resetting the interval
        // accomplishes.
        self.upper_tick_interval.reset();

        self.write_to_persist(responders).await?;

        // Wait until our artificial latency has completed.
        //
        // Note: if writing to persist took longer than `DEFAULT_TICK` this
        // await will resolve immediately.
        tokio::time::sleep_until(min_time_to_complete).await;

        ControlFlow::Continue(())
    }

    /// Apply the given write operation to the `desired`/`to_write` state.
    fn apply_write_op(&mut self, op: StorageWriteOp) {
        match op {
            StorageWriteOp::Append { updates } => {
                self.desired.extend_from_slice(&updates);
                self.to_write.extend(updates);
            }
            StorageWriteOp::Delete { filter } => {
                let to_delete = self.desired.extract_if(.., |(row, _)| filter(row));
                let retractions = to_delete.map(|(row, diff)| (row, -diff));
                self.to_write.extend(retractions);
            }
        }
    }

    /// Attempt to write what is currently in [Self::to_write] to persist,
    /// retrying and re-syncing to persist when necessary, that is when the
    /// upper was not what we expected.
    async fn write_to_persist(
        &mut self,
        responders: Vec<oneshot::Sender<Result<(), StorageError<T>>>>,
    ) -> ControlFlow<String> {
        if self.read_only {
            tracing::debug!(%self.id, "not writing to differential collection: read-only");
            // Not attempting to write while in read-only mode.
            return ControlFlow::Continue(());
        }

        // We'll try really hard to succeed, but eventually stop.
        //
        // Note: it's very rare we should ever need to retry, and if we need to
        // retry it should only take 1 or 2 attempts. We set `max_tries` to be
        // high though because if we hit some edge case we want to try hard to
        // commit the data.
        let retries = Retry::default()
            .initial_backoff(Duration::from_secs(1))
            .clamp_backoff(Duration::from_secs(3))
            .factor(1.25)
            .max_tries(20)
            .into_retry_stream();
        let mut retries = Box::pin(retries);

        loop {
            // Append updates to persist!
            let now = T::from((self.now)());
            let new_upper = std::cmp::max(
                now,
                TimestampManipulation::step_forward(&self.current_upper),
            );

            let updates_to_write = self
                .to_write
                .iter()
                .map(|(row, diff)| {
                    (
                        (SourceData(Ok(row.clone())), ()),
                        self.current_upper.clone(),
                        diff.into_inner(),
                    )
                })
                .collect::<Vec<_>>();

            assert!(!self.read_only);
            let res = self
                .write_handle
                .compare_and_append(
                    updates_to_write,
                    Antichain::from_elem(self.current_upper.clone()),
                    Antichain::from_elem(new_upper.clone()),
                )
                .await
                .expect("valid usage");
            match res {
                // Everything was successful!
                Ok(()) => {
                    // Notify all of our listeners.
                    notify_listeners(responders, || Ok(()));

                    self.current_upper = new_upper;

                    // Very important! This is empty at steady state, while
                    // desired keeps an in-memory copy of desired state.
                    self.to_write.clear();

                    tracing::debug!(%self.id, "appended to differential collection");

                    // Break out of the retry loop so we can wait for more data.
                    break;
                }
                // Failed to write to some collections,
                Err(err) => {
                    // Someone else wrote to the collection. We need to read
                    // from persist and update to_write based on that and the
                    // desired state.
                    let actual_upper = if let Some(ts) = err.current.as_option() {
                        ts.clone()
                    } else {
                        return ControlFlow::Break("upper is the empty antichain".to_string());
                    };

                    tracing::info!(%self.id, ?actual_upper, expected_upper = ?self.current_upper, "retrying append for differential collection");

                    // We've exhausted all of our retries, notify listeners and
                    // break out of the retry loop so we can wait for more data.
                    if retries.next().await.is_none() {
                        let invalid_upper = InvalidUpper {
                            id: self.id,
                            current_upper: err.current,
                        };
                        notify_listeners(responders, || {
                            Err(StorageError::InvalidUppers(vec![invalid_upper.clone()]))
                        });
                        error!(
                            "exhausted retries when appending to managed collection {}",
                            self.id
                        );
                        break;
                    }

                    self.current_upper = actual_upper;

                    self.sync_to_persist().await;

                    debug!(
                        "Retrying invalid-uppers error while appending to differential collection {}",
                        self.id
                    );
                }
            }
        }

        ControlFlow::Continue(())
    }

    /// Re-derives [Self::to_write] by looking at [Self::desired] and the
    /// current state in persist. We want to insert everything in desired and
    /// retract everything in persist. But ideally most of that cancels out in
    /// consolidation.
    ///
    /// To be called when a `compare_and_append` failed because the upper didn't
    /// match what we expected.
    async fn sync_to_persist(&mut self) {
        let mut read_handle = (self.read_handle_fn)().await;
        let as_of = self
            .current_upper
            .step_back()
            .unwrap_or_else(|| T::minimum());
        let as_of = Antichain::from_elem(as_of);
        let snapshot = read_handle.snapshot_and_fetch(as_of).await;

        let mut negated_oks = match snapshot {
            Ok(contents) => {
                let mut snapshot = Vec::with_capacity(contents.len());
                for ((data, _), _, diff) in contents {
                    let row = data.expect("invalid protobuf data").0.unwrap();
                    snapshot.push((row, -Diff::from(diff)));
                }
                snapshot
            }
            Err(_) => panic!("read before since"),
        };

        self.to_write.clear();
        self.to_write.extend(self.desired.iter().cloned());
        self.to_write.append(&mut negated_oks);
        consolidation::consolidate(&mut self.to_write);
    }
}

pub(crate) struct AppendOnlyIntrospectionConfig<T>
where
    T: Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    pub(crate) introspection_type: IntrospectionType,
    pub(crate) config_set: Arc<ConfigSet>,
    pub(crate) parameters: StorageParameters,
    pub(crate) storage_collections: Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
}

/// A task that writes to an append only collection and continuously bumps the upper for the specified
/// collection.
///
/// For status history collections, this task can deduplicate redundant [`Statuses`](Status).
struct AppendOnlyWriteTask<T>
where
    T: Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    /// The collection that we are writing to.
    id: GlobalId,
    write_handle: WriteHandle<SourceData, (), T, StorageDiff>,
    read_only: bool,
    now: NowFn,
    user_batch_duration_ms: Arc<AtomicU64>,
    /// Receiver for write commands.
    rx: mpsc::UnboundedReceiver<(
        Vec<AppendOnlyUpdate>,
        oneshot::Sender<Result<(), StorageError<T>>>,
    )>,

    /// We have to shut down when receiving from this.
    shutdown_rx: oneshot::Receiver<()>,
    /// If this collection deduplicates statuses, this map is used to track the previous status.
    previous_statuses: Option<BTreeMap<(GlobalId, Option<ReplicaId>), Status>>,
}

impl<T> AppendOnlyWriteTask<T>
where
    T: Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    /// Spawns an [`AppendOnlyWriteTask`] in an [`mz_ore::task`] that will continuously bump the
    /// upper for the specified collection,
    /// and append data that is sent via the provided [`mpsc::UnboundedSender`].
    ///
    /// TODO(parkmycar): One day if we want to customize the tick interval for each collection, that
    /// should be done here.
    /// TODO(parkmycar): Maybe add prometheus metrics for each collection?
    fn spawn(
        id: GlobalId,
        write_handle: WriteHandle<SourceData, (), T, StorageDiff>,
        read_only: bool,
        now: NowFn,
        user_batch_duration_ms: Arc<AtomicU64>,
        introspection_config: Option<AppendOnlyIntrospectionConfig<T>>,
    ) -> (AppendOnlyWriteChannel<T>, WriteTask, ShutdownSender) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let previous_statuses: Option<BTreeMap<(GlobalId, Option<ReplicaId>), Status>> =
            match introspection_config
                .as_ref()
                .map(|config| config.introspection_type)
            {
                Some(IntrospectionType::SourceStatusHistory)
                | Some(IntrospectionType::SinkStatusHistory) => Some(BTreeMap::new()),

                Some(IntrospectionType::ReplicaMetricsHistory)
                | Some(IntrospectionType::WallclockLagHistory)
                | Some(IntrospectionType::WallclockLagHistogram)
                | Some(IntrospectionType::PrivatelinkConnectionStatusHistory)
                | Some(IntrospectionType::ReplicaStatusHistory)
                | Some(IntrospectionType::PreparedStatementHistory)
                | Some(IntrospectionType::StatementExecutionHistory)
                | Some(IntrospectionType::SessionHistory)
                | Some(IntrospectionType::StatementLifecycleHistory)
                | Some(IntrospectionType::SqlText)
                | None => None,

                Some(introspection_type @ IntrospectionType::ShardMapping)
                | Some(introspection_type @ IntrospectionType::Frontiers)
                | Some(introspection_type @ IntrospectionType::ReplicaFrontiers)
                | Some(introspection_type @ IntrospectionType::StorageSourceStatistics)
                | Some(introspection_type @ IntrospectionType::StorageSinkStatistics)
                | Some(introspection_type @ IntrospectionType::ComputeDependencies)
                | Some(introspection_type @ IntrospectionType::ComputeOperatorHydrationStatus)
                | Some(introspection_type @ IntrospectionType::ComputeMaterializedViewRefreshes)
                | Some(introspection_type @ IntrospectionType::ComputeErrorCounts)
                | Some(introspection_type @ IntrospectionType::ComputeHydrationTimes) => {
                    unreachable!("not append-only collection: {introspection_type:?}")
                }
            };

        let mut task = Self {
            id,
            write_handle,
            rx,
            shutdown_rx,
            read_only,
            now,
            user_batch_duration_ms,
            previous_statuses,
        };

        let handle = mz_ore::task::spawn(
            || format!("CollectionManager-append_only_write_task-{id}"),
            async move {
                if !task.read_only {
                    task.prepare(introspection_config).await;
                }
                task.run().await;
            },
        );

        (tx, handle.abort_on_drop(), shutdown_tx)
    }

    /// Does any work that is required before the background task starts
    /// writing to the given append only introspection collection.
    ///
    /// This might include consolidation or deleting older entries.
    async fn prepare(&mut self, introspection_config: Option<AppendOnlyIntrospectionConfig<T>>) {
        let Some(AppendOnlyIntrospectionConfig {
            introspection_type,
            config_set,
            parameters,
            storage_collections,
        }) = introspection_config
        else {
            return;
        };
        let initial_statuses = match introspection_type {
            IntrospectionType::ReplicaMetricsHistory
            | IntrospectionType::WallclockLagHistory
            | IntrospectionType::WallclockLagHistogram => {
                let result = partially_truncate_metrics_history(
                    self.id,
                    introspection_type,
                    &mut self.write_handle,
                    config_set,
                    self.now.clone(),
                    storage_collections,
                )
                .await;
                if let Err(error) = result {
                    soft_panic_or_log!(
                        "error truncating metrics history: {error} (type={introspection_type:?})"
                    );
                }
                Vec::new()
            }

            IntrospectionType::PrivatelinkConnectionStatusHistory => {
                partially_truncate_status_history(
                    self.id,
                    IntrospectionType::PrivatelinkConnectionStatusHistory,
                    &mut self.write_handle,
                    privatelink_status_history_desc(&parameters),
                    self.now.clone(),
                    &storage_collections,
                )
                .await;
                Vec::new()
            }
            IntrospectionType::ReplicaStatusHistory => {
                partially_truncate_status_history(
                    self.id,
                    IntrospectionType::ReplicaStatusHistory,
                    &mut self.write_handle,
                    replica_status_history_desc(&parameters),
                    self.now.clone(),
                    &storage_collections,
                )
                .await;
                Vec::new()
            }

            // Note [btv] - we don't truncate these, because that uses
            // a huge amount of memory on environmentd startup.
            IntrospectionType::PreparedStatementHistory
            | IntrospectionType::StatementExecutionHistory
            | IntrospectionType::SessionHistory
            | IntrospectionType::StatementLifecycleHistory
            | IntrospectionType::SqlText => {
                // NOTE(aljoscha): We never remove from these
                // collections. Someone, at some point needs to
                // think about that! Issue:
                // https://github.com/MaterializeInc/database-issues/issues/7666
                Vec::new()
            }

            IntrospectionType::SourceStatusHistory => {
                let last_status_per_id = partially_truncate_status_history(
                    self.id,
                    IntrospectionType::SourceStatusHistory,
                    &mut self.write_handle,
                    source_status_history_desc(&parameters),
                    self.now.clone(),
                    &storage_collections,
                )
                .await;

                let status_col = MZ_SOURCE_STATUS_HISTORY_DESC
                    .get_by_name(&ColumnName::from("status"))
                    .expect("schema has not changed")
                    .0;

                last_status_per_id
                    .into_iter()
                    .map(|(id, row)| {
                        (
                            id,
                            Status::from_str(
                                row.iter()
                                    .nth(status_col)
                                    .expect("schema has not changed")
                                    .unwrap_str(),
                            )
                            .expect("statuses must be uncorrupted"),
                        )
                    })
                    .collect()
            }
            IntrospectionType::SinkStatusHistory => {
                let last_status_per_id = partially_truncate_status_history(
                    self.id,
                    IntrospectionType::SinkStatusHistory,
                    &mut self.write_handle,
                    sink_status_history_desc(&parameters),
                    self.now.clone(),
                    &storage_collections,
                )
                .await;

                let status_col = MZ_SINK_STATUS_HISTORY_DESC
                    .get_by_name(&ColumnName::from("status"))
                    .expect("schema has not changed")
                    .0;

                last_status_per_id
                    .into_iter()
                    .map(|(id, row)| {
                        (
                            id,
                            Status::from_str(
                                row.iter()
                                    .nth(status_col)
                                    .expect("schema has not changed")
                                    .unwrap_str(),
                            )
                            .expect("statuses must be uncorrupted"),
                        )
                    })
                    .collect()
            }

            introspection_type @ IntrospectionType::ShardMapping
            | introspection_type @ IntrospectionType::Frontiers
            | introspection_type @ IntrospectionType::ReplicaFrontiers
            | introspection_type @ IntrospectionType::StorageSourceStatistics
            | introspection_type @ IntrospectionType::StorageSinkStatistics
            | introspection_type @ IntrospectionType::ComputeDependencies
            | introspection_type @ IntrospectionType::ComputeOperatorHydrationStatus
            | introspection_type @ IntrospectionType::ComputeMaterializedViewRefreshes
            | introspection_type @ IntrospectionType::ComputeErrorCounts
            | introspection_type @ IntrospectionType::ComputeHydrationTimes => {
                unreachable!("not append-only collection: {introspection_type:?}")
            }
        };
        if let Some(previous_statuses) = &mut self.previous_statuses {
            previous_statuses.extend(initial_statuses);
        }
    }

    async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_TICK_MS));

        const BATCH_SIZE: usize = 4096;
        let mut batch: Vec<(Vec<_>, _)> = Vec::with_capacity(BATCH_SIZE);

        'run: loop {
            tokio::select! {
                // Prefer sending actual updates over just bumping the upper, because sending
                // updates also bump the upper.
                biased;

                // Listen for a shutdown signal so we can gracefully cleanup.
                _ = &mut self.shutdown_rx => {
                    let mut senders = Vec::new();

                    // Prevent new messages from being sent.
                    self.rx.close();

                    // Get as many waiting senders as possible.
                    while let Ok((_batch, sender)) = self.rx.try_recv() {
                        senders.push(sender);
                    }

                    // Notify them that this collection is closed.
                    //
                    // Note: if a task is shutting down, that indicates the source has been
                    // dropped, at which point the identifier is invalid. Returning this
                    // error provides a better user experience.
                    notify_listeners(senders, || Err(StorageError::IdentifierInvalid(self.id)));

                    break 'run;
                }

                // Pull a chunk of queued updates off the channel.
                count = self.rx.recv_many(&mut batch, BATCH_SIZE) => {
                    if count > 0 {
                        // To rate limit appends to persist we add artificial latency, and will
                        // finish no sooner than this instant.
                        let batch_duration_ms = match self.id {
                            GlobalId::User(_) => Duration::from_millis(self.user_batch_duration_ms.load(Ordering::Relaxed)),
                            // For non-user collections, always just use the default.
                            _ => STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT,
                        };
                        let use_batch_now = Instant::now();
                        let min_time_to_complete = use_batch_now + batch_duration_ms;

                        tracing::debug!(
                            ?use_batch_now,
                            ?batch_duration_ms,
                            ?min_time_to_complete,
                            "batch duration",
                        );

                        // Reset the interval which is used to periodically bump the uppers
                        // because the uppers will get bumped with the following update. This
                        // makes it such that we will write at most once every `interval`.
                        //
                        // For example, let's say our `DEFAULT_TICK` interval is 10, so at
                        // `t + 10`, `t + 20`, ... we'll bump the uppers. If we receive an
                        // update at `t + 3` we want to shift this window so we bump the uppers
                        // at `t + 13`, `t + 23`, ... which resetting the interval accomplishes.
                        interval.reset();


                        let mut all_rows = Vec::with_capacity(batch.iter().map(|(rows, _)| rows.len()).sum());
                        let mut responders = Vec::with_capacity(batch.len());

                        for (updates, responder) in batch.drain(..) {
                            let rows = self.process_updates(updates);

                            all_rows.extend(rows.map(|(row, diff)| TimestamplessUpdate { row, diff}));
                            responders.push(responder);
                        }

                        if self.read_only {
                            tracing::warn!(%self.id, ?all_rows, "append while in read-only mode");
                            notify_listeners(responders, || Err(StorageError::ReadOnly));
                            continue;
                        }

                        // Append updates to persist!
                        let at_least = T::from((self.now)());

                        if !all_rows.is_empty() {
                            monotonic_append(&mut self.write_handle, all_rows, at_least).await;
                        }
                        // Notify all of our listeners.
                        notify_listeners(responders, || Ok(()));

                        // Wait until our artificial latency has completed.
                        //
                        // Note: if writing to persist took longer than `DEFAULT_TICK` this
                        // await will resolve immediately.
                        tokio::time::sleep_until(min_time_to_complete).await;
                    } else {
                        // Sender has been dropped, which means the collection should have been
                        // unregistered, break out of the run loop if we weren't already
                        // aborted.
                        break 'run;
                    }
                }

                // If we haven't received any updates, then we'll move the upper forward.
                _ = interval.tick() => {
                    if self.read_only {
                        // Not bumping uppers while in read-only mode.
                        continue;
                    }

                    // Update our collection.
                    let now = T::from((self.now)());
                    let updates = vec![];
                    let at_least = now.clone();

                    // Failures don't matter when advancing collections' uppers. This might
                    // fail when a clusterd happens to be writing to this concurrently.
                    // Advancing uppers here is best-effort and only needs to succeed if no
                    // one else is advancing it; contention proves otherwise.
                    monotonic_append(&mut self.write_handle, updates, at_least).await;
                },
            }
        }

        info!("write_task-{} ending", self.id);
    }

    /// Deduplicate any [`mz_storage_client::client::StatusUpdate`] within `updates` and converts
    /// `updates` to rows and diffs.
    fn process_updates(
        &mut self,
        updates: Vec<AppendOnlyUpdate>,
    ) -> impl Iterator<Item = (Row, Diff)> {
        let updates = if let Some(previous_statuses) = &mut self.previous_statuses {
            let new: Vec<_> = updates
                .into_iter()
                .filter(|r| match r {
                    AppendOnlyUpdate::Row(_) => true,
                    AppendOnlyUpdate::Status(update) => {
                        match (
                            previous_statuses
                                .get(&(update.id, update.replica_id))
                                .as_deref(),
                            &update.status,
                        ) {
                            (None, _) => true,
                            (Some(old), new) => old.superseded_by(*new),
                        }
                    }
                })
                .collect();
            previous_statuses.extend(new.iter().filter_map(|update| match update {
                AppendOnlyUpdate::Row(_) => None,
                AppendOnlyUpdate::Status(update) => {
                    Some(((update.id, update.replica_id), update.status))
                }
            }));
            new
        } else {
            updates
        };

        updates.into_iter().map(AppendOnlyUpdate::into_row)
    }
}

/// Truncates the given metrics history by removing all entries older than that history's
/// configured retention interval.
///
/// # Panics
///
/// Panics if `collection` is not a metrics history.
async fn partially_truncate_metrics_history<T>(
    id: GlobalId,
    introspection_type: IntrospectionType,
    write_handle: &mut WriteHandle<SourceData, (), T, StorageDiff>,
    config_set: Arc<ConfigSet>,
    now: NowFn,
    storage_collections: Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
) -> Result<(), anyhow::Error>
where
    T: Codec64 + From<EpochMillis> + TimestampManipulation,
{
    let (keep_duration, occurred_at_col) = match introspection_type {
        IntrospectionType::ReplicaMetricsHistory => (
            REPLICA_METRICS_HISTORY_RETENTION_INTERVAL.get(&config_set),
            REPLICA_METRICS_HISTORY_DESC
                .get_by_name(&ColumnName::from("occurred_at"))
                .expect("schema has not changed")
                .0,
        ),
        IntrospectionType::WallclockLagHistory => (
            WALLCLOCK_LAG_HISTORY_RETENTION_INTERVAL.get(&config_set),
            WALLCLOCK_LAG_HISTORY_DESC
                .get_by_name(&ColumnName::from("occurred_at"))
                .expect("schema has not changed")
                .0,
        ),
        IntrospectionType::WallclockLagHistogram => (
            WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RETENTION_INTERVAL.get(&config_set),
            WALLCLOCK_GLOBAL_LAG_HISTOGRAM_RAW_DESC
                .get_by_name(&ColumnName::from("period_start"))
                .expect("schema has not changed")
                .0,
        ),
        _ => panic!("not a metrics history: {introspection_type:?}"),
    };

    let upper = write_handle.fetch_recent_upper().await;
    let Some(upper_ts) = upper.as_option() else {
        bail!("collection is sealed");
    };
    let Some(as_of_ts) = upper_ts.step_back() else {
        return Ok(()); // nothing to truncate
    };

    let mut rows = storage_collections
        .snapshot(id, as_of_ts)
        .await
        .map_err(|e| anyhow!("reading snapshot: {e:?}"))?;

    let now = mz_ore::now::to_datetime(now());
    let keep_since = now - keep_duration;

    // Produce retractions by inverting diffs of rows we want to delete and setting the diffs
    // of all other rows to 0.
    for (row, diff) in &mut rows {
        let datums = row.unpack();
        let occurred_at = datums[occurred_at_col].unwrap_timestamptz();
        *diff = if *occurred_at < keep_since { -*diff } else { 0 };
    }

    // Consolidate to avoid superfluous writes.
    consolidation::consolidate(&mut rows);

    if rows.is_empty() {
        return Ok(());
    }

    // It is very important that we append our retractions at the timestamp
    // right after the timestamp at which we got our snapshot. Otherwise,
    // it's possible for someone else to sneak in retractions or other
    // unexpected changes.
    let old_upper_ts = upper_ts.clone();
    let write_ts = old_upper_ts.clone();
    let new_upper_ts = TimestampManipulation::step_forward(&old_upper_ts);

    let updates = rows
        .into_iter()
        .map(|(row, diff)| ((SourceData(Ok(row)), ()), write_ts.clone(), diff));

    write_handle
        .compare_and_append(
            updates,
            Antichain::from_elem(old_upper_ts),
            Antichain::from_elem(new_upper_ts),
        )
        .await
        .expect("valid usage")
        .map_err(|e| anyhow!("appending retractions: {e:?}"))
}

/// Effectively truncates the status history shard based on its retention policy.
///
/// NOTE: The history collections are really append-only collections, but
/// every-now-and-then we want to retract old updates so that the collection
/// does not grow unboundedly. Crucially, these are _not_ incremental
/// collections, they are not derived from a state at some time `t` and we
/// cannot maintain a desired state for them.
///
/// Returns a map with latest unpacked row per key.
pub(crate) async fn partially_truncate_status_history<T, K>(
    id: GlobalId,
    introspection_type: IntrospectionType,
    write_handle: &mut WriteHandle<SourceData, (), T, StorageDiff>,
    status_history_desc: StatusHistoryDesc<K>,
    now: NowFn,
    storage_collections: &Arc<dyn StorageCollections<Timestamp = T> + Send + Sync>,
) -> BTreeMap<K, Row>
where
    T: Codec64 + From<EpochMillis> + TimestampManipulation,
    K: Clone + Debug + Ord + Send + Sync,
{
    let upper = write_handle.fetch_recent_upper().await.clone();

    let mut rows = match upper.as_option() {
        Some(f) if f > &T::minimum() => {
            let as_of = f.step_back().unwrap();

            storage_collections
                .snapshot(id, as_of)
                .await
                .expect("snapshot succeeds")
        }
        // If collection is closed or the frontier is the minimum, we cannot
        // or don't need to truncate (respectively).
        _ => return BTreeMap::new(),
    };

    // BTreeMap to keep track of the row with the latest timestamp for each key.
    let mut latest_row_per_key: BTreeMap<K, (CheckedTimestamp<DateTime<Utc>>, Row)> =
        BTreeMap::new();

    // Consolidate the snapshot, so we can process it correctly below.
    differential_dataflow::consolidation::consolidate(&mut rows);

    let mut deletions = vec![];

    let mut handle_row = {
        let latest_row_per_key = &mut latest_row_per_key;
        move |row: &Row, diff| {
            let datums = row.unpack();
            let key = (status_history_desc.extract_key)(&datums);
            let timestamp = (status_history_desc.extract_time)(&datums);

            assert!(
                diff > 0,
                "only know how to operate over consolidated data with diffs > 0, \
                    found diff {diff} for object {key:?} in {introspection_type:?}",
            );

            // Keep track of the timestamp of the latest row per key.
            match latest_row_per_key.get(&key) {
                Some(existing) if &existing.0 > &timestamp => {}
                _ => {
                    latest_row_per_key.insert(key.clone(), (timestamp, row.clone()));
                }
            };
            (key, timestamp)
        }
    };

    match status_history_desc.retention_policy {
        StatusHistoryRetentionPolicy::LastN(n) => {
            // BTreeMap to track the earliest events for each key.
            let mut last_n_entries_per_key: BTreeMap<
                K,
                BinaryHeap<Reverse<(CheckedTimestamp<DateTime<Utc>>, Row)>>,
            > = BTreeMap::new();

            for (row, diff) in rows {
                let (key, timestamp) = handle_row(&row, diff);

                // Duplicate rows ARE possible if many status changes happen in VERY quick succession,
                // so we handle duplicated rows separately.
                let entries = last_n_entries_per_key.entry(key).or_default();
                for _ in 0..diff {
                    // We CAN have multiple statuses (most likely Starting and Running) at the exact same
                    // millisecond, depending on how the `health_operator` is scheduled.
                    //
                    // Note that these will be arbitrarily ordered, so a Starting event might
                    // survive and a Running one won't. The next restart will remove the other,
                    // so we don't bother being careful about it.
                    //
                    // TODO(guswynn): unpack these into health-status objects and use
                    // their `Ord` impl.
                    entries.push(Reverse((timestamp, row.clone())));

                    // Retain some number of entries, using pop to mark the oldest entries for
                    // deletion.
                    while entries.len() > n {
                        if let Some(Reverse((_, r))) = entries.pop() {
                            deletions.push(r);
                        }
                    }
                }
            }
        }
        StatusHistoryRetentionPolicy::TimeWindow(time_window) => {
            // Get the lower bound of our retention window
            let now = mz_ore::now::to_datetime(now());
            let keep_since = now - time_window;

            // Mark any row outside the retention window for deletion
            for (row, diff) in rows {
                let (_, timestamp) = handle_row(&row, diff);

                if *timestamp < keep_since {
                    deletions.push(row);
                }
            }
        }
    }

    // It is very important that we append our retractions at the timestamp
    // right after the timestamp at which we got our snapshot. Otherwise,
    // it's possible for someone else to sneak in retractions or other
    // unexpected changes.
    let expected_upper = upper.into_option().expect("checked above");
    let new_upper = TimestampManipulation::step_forward(&expected_upper);

    // Updates are only deletes because everything else is already in the shard.
    let updates = deletions
        .into_iter()
        .map(|row| ((SourceData(Ok(row)), ()), expected_upper.clone(), -1))
        .collect::<Vec<_>>();

    let res = write_handle
        .compare_and_append(
            updates,
            Antichain::from_elem(expected_upper.clone()),
            Antichain::from_elem(new_upper),
        )
        .await
        .expect("usage was valid");

    match res {
        Ok(_) => {
            // All good, yay!
        }
        Err(err) => {
            // This is fine, it just means the upper moved because
            // of continual upper advancement or because someone
            // already appended some more retractions/updates.
            //
            // NOTE: We might want to attempt these partial
            // retractions on an interval, instead of only when
            // starting up!
            info!(
                %id, ?expected_upper, current_upper = ?err.current,
                "failed to append partial truncation",
            );
        }
    }

    latest_row_per_key
        .into_iter()
        .map(|(key, (_, row))| (key, row))
        .collect()
}

async fn monotonic_append<T: Timestamp + Lattice + Codec64 + TimestampManipulation>(
    write_handle: &mut WriteHandle<SourceData, (), T, StorageDiff>,
    updates: Vec<TimestamplessUpdate>,
    at_least: T,
) {
    let mut expected_upper = write_handle.shared_upper();
    loop {
        if updates.is_empty() && expected_upper.is_empty() {
            // Ignore timestamp advancement for
            // closed collections. TODO? Make this a
            // correctable error
            return;
        }

        let upper = expected_upper
            .into_option()
            .expect("cannot append data to closed collection");

        let lower = if upper.less_than(&at_least) {
            at_least.clone()
        } else {
            upper.clone()
        };

        let new_upper = TimestampManipulation::step_forward(&lower);
        let updates = updates
            .iter()
            .map(|TimestamplessUpdate { row, diff }| {
                (
                    (SourceData(Ok(row.clone())), ()),
                    lower.clone(),
                    diff.into_inner(),
                )
            })
            .collect::<Vec<_>>();
        let res = write_handle
            .compare_and_append(
                updates,
                Antichain::from_elem(upper),
                Antichain::from_elem(new_upper),
            )
            .await
            .expect("valid usage");
        match res {
            Ok(()) => return,
            Err(err) => {
                expected_upper = err.current;
                continue;
            }
        }
    }
}

// Helper method for notifying listeners.
fn notify_listeners<T>(
    responders: impl IntoIterator<Item = oneshot::Sender<T>>,
    result: impl Fn() -> T,
) {
    for r in responders {
        // We don't care if the listener disappeared.
        let _ = r.send(result());
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;
    use mz_repr::{Datum, Row};
    use mz_storage_client::client::StatusUpdate;
    use mz_storage_client::healthcheck::{
        MZ_SINK_STATUS_HISTORY_DESC, MZ_SOURCE_STATUS_HISTORY_DESC,
    };

    #[mz_ore::test]
    fn test_row() {
        let error_message = "error message";
        let hint = "hint message";
        let id = GlobalId::User(1);
        let status = Status::Dropped;
        let row = Row::from(StatusUpdate {
            id,
            timestamp: chrono::offset::Utc::now(),
            status,
            error: Some(error_message.to_string()),
            hints: BTreeSet::from([hint.to_string()]),
            namespaced_errors: Default::default(),
            replica_id: None,
        });

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status.to_str()));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::String(error_message));

        let details = row
            .iter()
            .nth(4)
            .unwrap()
            .unwrap_map()
            .iter()
            .collect::<Vec<_>>();

        assert_eq!(details.len(), 1);
        let hint_datum = &details[0];

        assert_eq!(hint_datum.0, "hints");
        assert_eq!(
            hint_datum.1.unwrap_list().iter().next().unwrap(),
            Datum::String(hint)
        );
    }

    #[mz_ore::test]
    fn test_row_without_hint() {
        let error_message = "error message";
        let id = GlobalId::User(1);
        let status = Status::Dropped;
        let row = Row::from(StatusUpdate {
            id,
            timestamp: chrono::offset::Utc::now(),
            status,
            error: Some(error_message.to_string()),
            hints: Default::default(),
            namespaced_errors: Default::default(),
            replica_id: None,
        });

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status.to_str()));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::String(error_message));
        assert_eq!(row.iter().nth(4).unwrap(), Datum::Null);
    }

    #[mz_ore::test]
    fn test_row_without_error() {
        let id = GlobalId::User(1);
        let status = Status::Dropped;
        let hint = "hint message";
        let row = Row::from(StatusUpdate {
            id,
            timestamp: chrono::offset::Utc::now(),
            status,
            error: None,
            hints: BTreeSet::from([hint.to_string()]),
            namespaced_errors: Default::default(),
            replica_id: None,
        });

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status.to_str()));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::Null);

        let details = row
            .iter()
            .nth(4)
            .unwrap()
            .unwrap_map()
            .iter()
            .collect::<Vec<_>>();

        assert_eq!(details.len(), 1);
        let hint_datum = &details[0];

        assert_eq!(hint_datum.0, "hints");
        assert_eq!(
            hint_datum.1.unwrap_list().iter().next().unwrap(),
            Datum::String(hint)
        );
    }

    #[mz_ore::test]
    fn test_row_with_namespaced() {
        let error_message = "error message";
        let id = GlobalId::User(1);
        let status = Status::Dropped;
        let row = Row::from(StatusUpdate {
            id,
            timestamp: chrono::offset::Utc::now(),
            status,
            error: Some(error_message.to_string()),
            hints: Default::default(),
            namespaced_errors: BTreeMap::from([("thing".to_string(), "error".to_string())]),
            replica_id: None,
        });

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status.to_str()));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::String(error_message));

        let details = row
            .iter()
            .nth(4)
            .unwrap()
            .unwrap_map()
            .iter()
            .collect::<Vec<_>>();

        assert_eq!(details.len(), 1);
        let ns_datum = &details[0];

        assert_eq!(ns_datum.0, "namespaced");
        assert_eq!(
            ns_datum.1.unwrap_map().iter().next().unwrap(),
            ("thing", Datum::String("error"))
        );
    }

    #[mz_ore::test]
    fn test_row_with_everything() {
        let error_message = "error message";
        let hint = "hint message";
        let id = GlobalId::User(1);
        let status = Status::Dropped;
        let row = Row::from(StatusUpdate {
            id,
            timestamp: chrono::offset::Utc::now(),
            status,
            error: Some(error_message.to_string()),
            hints: BTreeSet::from([hint.to_string()]),
            namespaced_errors: BTreeMap::from([("thing".to_string(), "error".to_string())]),
            replica_id: None,
        });

        for (datum, column_type) in row.iter().zip(MZ_SINK_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        for (datum, column_type) in row.iter().zip(MZ_SOURCE_STATUS_HISTORY_DESC.iter_types()) {
            assert!(datum.is_instance_of(column_type));
        }

        assert_eq!(row.iter().nth(1).unwrap(), Datum::String(&id.to_string()));
        assert_eq!(row.iter().nth(2).unwrap(), Datum::String(status.to_str()));
        assert_eq!(row.iter().nth(3).unwrap(), Datum::String(error_message));

        let details = row
            .iter()
            .nth(4)
            .unwrap()
            .unwrap_map()
            .iter()
            .collect::<Vec<_>>();

        assert_eq!(details.len(), 2);
        // These are always sorted
        let hint_datum = &details[0];
        let ns_datum = &details[1];

        assert_eq!(hint_datum.0, "hints");
        assert_eq!(
            hint_datum.1.unwrap_list().iter().next().unwrap(),
            Datum::String(hint)
        );

        assert_eq!(ns_datum.0, "namespaced");
        assert_eq!(
            ns_datum.1.unwrap_map().iter().next().unwrap(),
            ("thing", Datum::String("error"))
        );
    }
}
