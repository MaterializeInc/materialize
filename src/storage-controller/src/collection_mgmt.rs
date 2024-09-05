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

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use futures::{Future, FutureExt};
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::retry::Retry;
use mz_ore::task::AbortOnDropHandle;
use mz_ore::vec::VecExt;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, Row, TimestampManipulation};
use mz_storage_client::client::TimestamplessUpdate;
use mz_storage_client::controller::{MonotonicAppender, StorageWriteOp};
use mz_storage_types::controller::InvalidUpper;
use mz_storage_types::parameters::STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT;
use mz_storage_types::sources::SourceData;
use timely::progress::{Antichain, Timestamp};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info};

use crate::StorageError;

// Default rate at which we advance the uppers of managed collections.
const DEFAULT_TICK_MS: u64 = 1_000;

/// A channel for sending writes to a differential collection.
type DifferentialWriteChannel<T> =
    mpsc::UnboundedSender<(StorageWriteOp, oneshot::Sender<Result<(), StorageError<T>>>)>;

/// A channel for sending writes to an append-only collection.
type AppendOnlyWriteChannel<T> = mpsc::UnboundedSender<(
    Vec<(Row, Diff)>,
    oneshot::Sender<Result<(), StorageError<T>>>,
)>;

type WriteTask = AbortOnDropHandle<()>;
type ShutdownSender = oneshot::Sender<()>;

#[derive(Debug, Clone)]
pub struct CollectionManager<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    /// When a [`CollectionManager`] is in read-only mode it must not affect any
    /// changes to external state.
    ///
    /// This is a watch to making sharing this bit with write tasks easier and
    /// to allow write tasks to await changes.
    pub read_only_rx: watch::Receiver<bool>,

    /// Send-side for read-only bit.
    pub read_only_tx: Arc<watch::Sender<bool>>,

    /// A watch that is always false. This is a hacky workaround for migrating
    /// builtin sources in 0dt.
    /// TODO(jkosh44) Remove me when persist schema migrations are in.
    hacky_always_false_watch: (watch::Sender<bool>, watch::Receiver<bool>),

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

        let (read_only_tx, read_only_rx) = watch::channel(read_only);
        let (always_false_tx, always_false_rx) = watch::channel(false);

        CollectionManager {
            read_only_tx: Arc::new(read_only_tx),
            read_only_rx,
            hacky_always_false_watch: (always_false_tx, always_false_rx),
            differential_collections: Arc::new(Mutex::new(BTreeMap::new())),
            append_only_collections: Arc::new(Mutex::new(BTreeMap::new())),
            user_batch_duration_ms: Arc::new(AtomicU64::new(batch_duration_ms)),
            now,
        }
    }

    /// Allow this [`CollectionManager`] to write to external systems, from now
    /// on. That is allow it to actually write to collections from now on.
    pub fn allow_writes(&mut self) {
        self.read_only_tx
            .send(false)
            .expect("we are holding on to at least one receiver");
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
        write_handle: WriteHandle<SourceData, (), T, Diff>,
        read_handle_fn: R,
        force_writable: bool,
    ) where
        R: FnMut() -> Pin<Box<dyn Future<Output = ReadHandle<SourceData, (), T, Diff>> + Send>>
            + Send
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

        let read_only_rx = self.get_read_only_rx(id, force_writable);

        // Spawns a new task so we can write to this collection.
        let writer_and_handle = DifferentialWriteTask::spawn(
            id,
            write_handle,
            read_handle_fn,
            read_only_rx,
            self.now.clone(),
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
        write_handle: WriteHandle<SourceData, (), T, Diff>,
        force_writable: bool,
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

        let read_only_rx = self.get_read_only_rx(id, force_writable);

        // Spawns a new task so we can write to this collection.
        let writer_and_handle = append_only_write_task(
            id,
            write_handle,
            Arc::clone(&self.user_batch_duration_ms),
            self.now.clone(),
            read_only_rx,
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

    /// Appends `updates` to the append-only collection identified by `id`, at
    /// _some_ timestamp. Does not wait for the append to complete.
    ///
    /// # Panics
    /// - If `id` does not belong to an append-only collections.
    /// - If this [`CollectionManager`] is in read-only mode.
    /// - If the collection closed.
    pub(super) fn blind_write(&self, id: GlobalId, updates: Vec<(Row, Diff)>) {
        if *self.read_only_rx.borrow() {
            panic!("attempting blind write to {} while in read-only mode", id);
        }

        if !updates.is_empty() {
            // Get the update channel in a block to make sure the Mutex lock is scoped.
            let update_tx = {
                let guard = self
                    .append_only_collections
                    .lock()
                    .expect("CollectionManager panicked");
                let (update_tx, _, _) = guard.get(&id).expect("missing append-only collection");
                update_tx.clone()
            };

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
            // Get the update channel in a block to make sure the Mutex lock is scoped.
            let update_tx = {
                let guard = self
                    .differential_collections
                    .lock()
                    .expect("CollectionManager panicked");
                let (update_tx, _, _) = guard.get(&id).expect("missing differential collection");
                update_tx.clone()
            };

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

    fn get_read_only_rx(&self, id: GlobalId, force_writable: bool) -> watch::Receiver<bool> {
        if force_writable {
            assert!(id.is_system(), "unexpected non-system global id: {id:?}");
            self.hacky_always_false_watch.1.clone()
        } else {
            self.read_only_rx.clone()
        }
    }
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
    R: FnMut() -> Pin<Box<dyn Future<Output = ReadHandle<SourceData, (), T, Diff>> + Send>>
        + Send
        + 'static,
{
    /// The collection that we are writing to.
    id: GlobalId,

    write_handle: WriteHandle<SourceData, (), T, Diff>,

    /// For getting a [`ReadHandle`] to sync our state to persist contents.
    read_handle_fn: R,

    read_only_watch: watch::Receiver<bool>,

    // Keep track of the read-only bit in our own state. Also so that we can
    // assert that we don't flip back from read-write to read-only.
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
    desired: Vec<(Row, i64)>,

    /// Updates that we have to write when next writing to persist. This is
    /// determined by looking at what is desired and what is in persist.
    to_write: Vec<(Row, i64)>,

    /// Current upper of the persist shard. We keep track of this so that we
    /// realize when someone else writes to the shard, in which case we have to
    /// update our state of the world, that is update our `to_write` based on
    /// `desired` and the contents of the persist shard.
    current_upper: T,
}

impl<T, R> DifferentialWriteTask<T, R>
where
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
    R: FnMut() -> Pin<Box<dyn Future<Output = ReadHandle<SourceData, (), T, Diff>> + Send>>
        + Send
        + 'static,
{
    /// Spawns a [`DifferentialWriteTask`] in an [`mz_ore::task`] and returns
    /// handles for interacting with it.
    fn spawn(
        id: GlobalId,
        write_handle: WriteHandle<SourceData, (), T, Diff>,
        read_handle_fn: R,
        read_only_watch: watch::Receiver<bool>,
        now: NowFn,
    ) -> (DifferentialWriteChannel<T>, WriteTask, ShutdownSender) {
        let (tx, rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let upper_tick_interval = tokio::time::interval(Duration::from_millis(DEFAULT_TICK_MS));

        let current_upper = T::minimum();

        let read_only = *read_only_watch.borrow();
        let task = Self {
            id,
            write_handle,
            read_handle_fn,
            read_only_watch,
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

                _it_changed = self.read_only_watch.changed() => {
                    assert!(!*self.read_only_watch.borrow(), "can only switch from read-only to read-write");
                    self.read_only = false;

                    // We can now write, attempt to do that right away, even if
                    // our `to_write` is empty. This way, we might learn that
                    // there is something in persist that we have to retract.
                    let  _ = self.write_to_persist(vec![]).await?;
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
                let to_delete = self.desired.drain_filter_swapping(|(row, _)| filter(row));
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
                        diff.clone(),
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

                    debug!("Retrying invalid-uppers error while appending to differential collection {}", self.id);
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
                    snapshot.push((row, -diff));
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

/// Spawns an [`mz_ore::task`] that will continuously bump the upper for the specified collection,
/// and append data that is sent via the provided [`mpsc::UnboundedSender`].
///
/// TODO(parkmycar): One day if we want to customize the tick interval for each collection, that
/// should be done here.
/// TODO(parkmycar): Maybe add prometheus metrics for each collection?
fn append_only_write_task<T>(
    id: GlobalId,
    mut write_handle: WriteHandle<SourceData, (), T, Diff>,
    user_batch_duration_ms: Arc<AtomicU64>,
    now: NowFn,
    read_only: watch::Receiver<bool>,
) -> (AppendOnlyWriteChannel<T>, WriteTask, ShutdownSender)
where
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    let handle = mz_ore::task::spawn(
        || format!("CollectionManager-append_only_write_task-{id}"),
        async move {
            let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_TICK_MS));

            const BATCH_SIZE: usize = 4096;
            let mut batch: Vec<(Vec<_>, _)> = Vec::with_capacity(BATCH_SIZE);

            'run: loop {
                tokio::select! {
                    // Prefer sending actual updates over just bumping the upper, because sending
                    // updates also bump the upper.
                    biased;

                    // Listen for a shutdown signal so we can gracefully cleanup.
                    _ = &mut shutdown_rx => {
                        let mut senders = Vec::new();

                        // Prevent new messages from being sent.
                        rx.close();

                        // Get as many waiting senders as possible.
                        while let Ok((_batch, sender)) = rx.try_recv() {
                            senders.push(sender);
                        }

                        // Notify them that this collection is closed.
                        //
                        // Note: if a task is shutting down, that indicates the source has been
                        // dropped, at which point the identifier is invalid. Returning this
                        // error provides a better user experience.
                        notify_listeners(senders, || Err(StorageError::IdentifierInvalid(id)));

                        break 'run;
                    }

                    // Pull a chunk of queued updates off the channel.
                    count = rx.recv_many(&mut batch, BATCH_SIZE) => {
                        if count > 0 {
                            // To rate limit appends to persist we add artificial latency, and will
                            // finish no sooner than this instant.
                            let batch_duration_ms = match id {
                                GlobalId::User(_) => Duration::from_millis(user_batch_duration_ms.load(Ordering::Relaxed)),
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
                            // at `t + 13`, `t + 23`, ... which reseting the interval accomplishes.
                            interval.reset();

                            let mut all_rows = Vec::with_capacity(batch.iter().map(|(rows, _)| rows.len()).sum());
                            let mut responders = Vec::with_capacity(batch.len());

                            for (rows, reponders) in batch.drain(..) {
                                all_rows.extend(rows.into_iter().map(|(row, diff)| TimestamplessUpdate { row, diff}));
                                responders.push(reponders);
                            }

                            if *read_only.borrow() {
                                tracing::warn!(%id, ?all_rows, "append while in read-only mode");
                                notify_listeners(responders, || Err(StorageError::ReadOnly));
                                continue;
                            }

                            // Append updates to persist!
                            let at_least = T::from(now());

                            monotonic_append(&mut write_handle, all_rows, at_least).await;
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
                        if *read_only.borrow() {
                            // Not bumping uppers while in read-only mode.
                            continue;
                        }

                        // Update our collection.
                        let now = T::from(now());
                        let updates = vec![];
                        let at_least = now.clone();

                        // Failures don't matter when advancing collections' uppers. This might
                        // fail when a clusterd happens to be writing to this concurrently.
                        // Advancing uppers here is best-effort and only needs to succeed if no
                        // one else is advancing it; contention proves otherwise.
                        monotonic_append(&mut write_handle, updates, at_least).await;
                    },
                }
            }

            info!("write_task-{id} ending");
        },
    );

    (tx, handle.abort_on_drop(), shutdown_tx)
}

async fn monotonic_append<T: Timestamp + Lattice + Codec64 + TimestampManipulation>(
    write_handle: &mut WriteHandle<SourceData, (), T, Diff>,
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
                ((SourceData(Ok(row.clone())), ()), lower.clone(), diff)
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
