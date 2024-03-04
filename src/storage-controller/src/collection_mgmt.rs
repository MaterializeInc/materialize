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

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use differential_dataflow::lattice::Lattice;
use futures::future::BoxFuture;
use futures::stream::StreamExt;
use futures::FutureExt;
use mz_ore::channel::ReceiverExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::retry::Retry;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, Row, TimestampManipulation};
use mz_storage_client::client::TimestamplessUpdate;
use mz_storage_client::controller::MonotonicAppender;
use mz_storage_types::parameters::STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT;
use timely::progress::Timestamp;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info};

use crate::{persist_handles, StorageError};

// Note(parkmycar): The capacity here was chosen arbitrarily.
const CHANNEL_CAPACITY: usize = 4096;
// Default rate at which we advance the uppers of managed collections.
const DEFAULT_TICK_MS: u64 = 1_000;

type WriteChannel = mpsc::Sender<(Vec<(Row, Diff)>, oneshot::Sender<Result<(), StorageError>>)>;
type WriteTask = AbortOnDropHandle<()>;
type ShutdownSender = oneshot::Sender<()>;

#[derive(Debug, Clone)]
pub struct CollectionManager<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    collections: Arc<Mutex<BTreeMap<GlobalId, (WriteChannel, WriteTask, ShutdownSender)>>>,
    write_handle: persist_handles::PersistMonotonicWriteWorker<T>,
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
    pub(super) fn new(
        write_handle: persist_handles::PersistMonotonicWriteWorker<T>,
        now: NowFn,
    ) -> CollectionManager<T> {
        let batch_duration_ms: u64 = STORAGE_MANAGED_COLLECTIONS_BATCH_DURATION_DEFAULT
            .as_millis()
            .try_into()
            .expect("known to fit");
        CollectionManager {
            collections: Arc::new(Mutex::new(BTreeMap::new())),
            write_handle,
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

    /// Registers the collection as one that `CollectionManager` will:
    /// - Automatically advance the upper of every second
    /// - Accept appends for. However, note that when appending, the
    ///   `CollectionManager` expects to be the only writer.
    pub(super) fn register_collection(&self, id: GlobalId) {
        let mut guard = self.collections.lock().expect("collection_mgmt panicked");

        // Check if this collection is already registered.
        if let Some((_writer, task, _shutdown_tx)) = guard.get(&id) {
            // The collection is already registered and the task is still running so nothing to do.
            if !task.is_finished() {
                // TODO(parkmycar): Panic here if we never see this error in production.
                tracing::error!("Registered a collection twice! {id:?}");
                return;
            }
        }

        // Spawns a new task so we can write to this collection.
        let writer_and_handle = write_task(
            id,
            self.write_handle.clone(),
            Arc::clone(&self.user_batch_duration_ms),
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

    /// Unregisters the collection as one that `CollectionManager` will maintain.
    ///
    /// Also waits until the `CollectionManager` has completed all outstanding work to ensure that
    /// it has stopped referencing the provided `id`.
    #[mz_ore::instrument(level = "debug")]
    pub(super) fn unregister_collection(&self, id: GlobalId) -> BoxFuture<'static, ()> {
        let prev = self
            .collections
            .lock()
            .expect("CollectionManager panicked")
            .remove(&id);

        // Wait for the task to complete before reporting as unregisted.
        if let Some((_prev_writer, prev_task, shutdown_tx)) = prev {
            // Notify the task it needs to shutdown.
            //
            // We can ignore errors here because they indicate the task is already done.
            let _ = shutdown_tx.send(());
            Box::pin(prev_task.map(|_| ()))
        } else {
            Box::pin(futures::future::ready(()))
        }
    }

    /// Appends `updates` to the collection correlated with `id`, does not wait for the append to
    /// complete.
    ///
    /// # Panics
    /// - If `id` does not belong to managed collections.
    /// - If there is contention to write to the collection identified by `id`.
    /// - If the collection closed.
    pub(super) async fn append_to_collection(&self, id: GlobalId, updates: Vec<(Row, Diff)>) {
        if !updates.is_empty() {
            // Get the update channel in a block to make sure the Mutex lock is scoped.
            let update_tx = {
                let guard = self.collections.lock().expect("CollectionManager panicked");
                let (update_tx, _, _) = guard.get(&id).expect("id to exist");
                update_tx.clone()
            };

            // Specifically _do not_ wait for the append to complete, just for it to be sent.
            let (tx, _rx) = oneshot::channel();
            update_tx.send((updates, tx)).await.expect("rx hung up");
        }
    }

    /// Returns a [`MonotonicAppender`] that can be used to monotonically append updates to the
    /// collection correlated with `id`.
    pub(super) fn monotonic_appender(
        &self,
        id: GlobalId,
    ) -> Result<MonotonicAppender, StorageError> {
        let guard = self.collections.lock().expect("CollectionManager panicked");
        let tx = guard
            .get(&id)
            .map(|(tx, _, _)| tx.clone())
            .ok_or(StorageError::IdentifierMissing(id))?;

        Ok(MonotonicAppender::new(tx))
    }
}

/// Spawns an [`mz_ore::task`] that will continuously bump the upper for the specified collection,
/// and append data that is sent via the provided [`mpsc::Sender`].
///
/// TODO(parkmycar): One day if we want to customize the tick interval for each collection, that
/// should be done here.
/// TODO(parkmycar): Maybe add prometheus metrics for each collection?
fn write_task<T>(
    id: GlobalId,
    write_handle: persist_handles::PersistMonotonicWriteWorker<T>,
    user_batch_duration_ms: Arc<AtomicU64>,
    now: NowFn,
) -> (WriteChannel, WriteTask, ShutdownSender)
where
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    let handle = mz_ore::task::spawn(
        || format!("CollectionManager-write_task-{id}"),
        async move {
            let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_TICK_MS));

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
                        'collect: while let Ok((_batch, sender)) = rx.try_recv() {
                            senders.push(sender);

                            // Note: because we're shutting down the sending side of `rx` is no
                            // longer accessible, and thus we should no longer receive new
                            // requests. We add this check just as an extra guard.
                            if senders.len() > CHANNEL_CAPACITY {
                                // There's not a correctness issue if we receive new requests, just
                                // unexpected behavior.
                                tracing::error!("Write task channel should not be receiving new requests");
                                break 'collect;
                            }
                        }

                        // Notify them that this collection is closed.
                        //
                        // Note: if a task is shutting down, that indicates the source has been
                        // dropped, at which point the identifier is invalid. Returning this
                        // error provides a better user experience.
                        notify_listeners(senders, || Err(StorageError::IdentifierInvalid(id)));

                        break 'run;
                    }

                    // Pull as many queued updates off the channel as possible.
                    cmd = rx.recv_many(CHANNEL_CAPACITY) => {
                        if let Some(batch) = cmd {
                            // To rate limit appends to persist we add artifical latency, and will
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

                            let (rows, responders): (Vec<_>, Vec<_>) = batch
                                .into_iter()
                                .unzip();

                            // Append updates to persist!
                            let rows = rows
                                .into_iter()
                                .flatten()
                                .map(|(row, diff)| TimestamplessUpdate { row, diff })
                            .collect();
                            let request = vec![(id, rows, T::from(now()))];

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

                            'append_retry: loop {
                                let append_result = match write_handle.monotonic_append(request.clone()).await {
                                    // We got a response!
                                    Ok(append_result) => append_result,
                                    // Failed to receive which means the worker shutdown.
                                    Err(_recv_error) => {
                                        // Sender hung up, this seems fine and can happen when shutting down.
                                        notify_listeners(responders, || Err(StorageError::ShuttingDown("PersistMonotonicWriteWorker")));

                                        // End the task since we can no longer send writes to persist.
                                        break 'run;
                                    }
                                };

                                match append_result {
                                    // Everything was successful!
                                    Ok(()) => {
                                        // Notify all of our listeners.
                                        notify_listeners(responders, || Ok(()));
                                        // Break out of the retry loop so we can wait for more data.
                                        break 'append_retry;
                                    },
                                    // Failed to write to some collections,
                                    Err(StorageError::InvalidUppers(failed_ids)) => {
                                        // It's fine to retry invalid-uppers errors here, since
                                        // monotonic appends do not specify a particular upper or
                                        // timestamp.

                                        assert_eq!(failed_ids.len(), 1, "received errors for more than one collection");
                                        assert_eq!(failed_ids[0], id, "received errors for a different collection");

                                        // We've exhausted all of our retries, notify listeners
                                        // and break out of the retry loop so we can wait for more
                                        // data.
                                        if retries.next().await.is_none() {
                                            notify_listeners(responders, || Err(StorageError::InvalidUppers(vec![id])));
                                            error!("exhausted retries when appending to managed collection {failed_ids:?}");
                                            break 'append_retry;
                                        }

                                        debug!("Retrying invalid-uppers error while appending to managed collection {failed_ids:?}");
                                    }
                                    // Uh-oh, something else went wrong!
                                    Err(other) => {
                                        panic!("Unhandled error while appending to managed collection {id:?}: {other:?}")
                                    }
                                }
                            }

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
                        // Update our collection.
                        let now = T::from(now());
                        let updates = vec![(id, vec![], now.clone())];

                        // Failures don't matter when advancing collections' uppers. This might
                        // fail when a clusterd happens to be writing to this concurrently.
                        // Advancing uppers here is best-effort and only needs to succeed if no
                        // one else is advancing it; contention proves otherwise.
                        match write_handle.monotonic_append(updates).await {
                            // All good!
                            Ok(_append_result) => (),
                            // Sender hung up, this seems fine and can happen when shutting down.
                            Err(_recv_error) => {
                                // Exit the run loop because there is no other work we can do.
                                break 'run;
                            }
                        }
                    },
                }
            }

            info!("write_task-{id} ending");
        },
    );

    (tx, handle.abort_on_drop(), shutdown_tx)
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
