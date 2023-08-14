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
use std::sync::{Arc, Mutex};

use differential_dataflow::lattice::Lattice;
use futures::stream::StreamExt;
use mz_ore::channel::ReceiverExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_ore::retry::Retry;
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, Row, TimestampManipulation};
use timely::progress::Timestamp;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant};
use tracing::{debug, error, info};

use crate::client::TimestamplessUpdate;
use crate::controller::{persist_handles, StorageError};

// Note(parkmycar): The capacity here was chosen arbitrarily.
const CHANNEL_CAPACITY: usize = 128;
// Default rate at which we append data and advance the uppers of managed collections.
const DEFAULT_TICK: Duration = Duration::from_secs(1);

type WriteChannel = mpsc::Sender<(Vec<(Row, Diff)>, oneshot::Sender<Result<(), StorageError>>)>;
type WriteTask = tokio::task::JoinHandle<()>;

#[derive(Debug, Clone)]
pub struct CollectionManager<T>
where
    T: Timestamp + Lattice + Codec64 + TimestampManipulation,
{
    collections: Arc<Mutex<BTreeMap<GlobalId, (WriteChannel, WriteTask)>>>,
    write_handle: persist_handles::PersistWriteWorker<T>,
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
        write_handle: persist_handles::PersistWriteWorker<T>,
        now: NowFn,
    ) -> CollectionManager<T> {
        CollectionManager {
            collections: Arc::new(Mutex::new(BTreeMap::new())),
            write_handle,
            now,
        }
    }

    /// Registers the collection as one that `CollectionManager` will:
    /// - Automatically advance the upper of every second
    /// - Accept appends for. However, note that when appending, the
    ///   `CollectionManager` expects to be the only writer.
    pub(super) fn register_collection(&self, id: GlobalId) {
        let mut guard = self.collections.lock().expect("collection_mgmt panicked");

        // Check if this collection is already registered.
        if let Some((_writer, task)) = guard.get(&id) {
            // The collection is already registered and the task is still running so nothing to do.
            if !task.is_finished() {
                // TODO(parkmycar): Panic here if we never see this error in production.
                tracing::error!("Registered a collection twice! {id:?}");
                return;
            }
        }

        // Spawns a new task so we can write to this collection.
        let writer_and_handle = write_task(id, self.write_handle.clone(), self.now.clone());
        let prev = guard.insert(id, writer_and_handle);

        // Double check the previous task was actually finished.
        if let Some((_, prev_task)) = prev {
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
    pub(super) async fn unregsiter_collection(&self, id: GlobalId) -> bool {
        let prev = self
            .collections
            .lock()
            .expect("CollectionManager panicked")
            .remove(&id);
        let existed = prev.is_some();

        // Wait for the task to complete before reporting as unregisted.
        if let Some((_prev_writer, prev_task)) = prev {
            prev_task.abort();
            let _ = prev_task.await;
        }

        existed
    }

    /// Appends `updates` to the collection correlated with `id`.
    ///
    /// # Panics
    /// - If `id` does not belong to managed collections.
    /// - If there is contention to write to the collection identified by
    ///   `id`.
    /// - If the collection closed.
    pub(super) async fn append_to_collection(&self, id: GlobalId, updates: Vec<(Row, Diff)>) {
        if !updates.is_empty() {
            let appender = self.monotonic_appender(id).expect("id to exist");
            match appender.append(updates).await {
                Ok(()) => (),
                // There's nothing we can do if we're shutting down.
                Err(StorageError::ShuttingDown(_)) => (),
                Err(e) => panic!("unexpected error when appending to collection {id}, err: {e:?}"),
            }
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
            .map(|(tx, _)| tx.clone())
            .ok_or(StorageError::IdentifierMissing(id))?;

        Ok(MonotonicAppender { tx })
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
    write_handle: persist_handles::PersistWriteWorker<T>,
    now: NowFn,
) -> (WriteChannel, WriteTask)
where
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    let (tx, mut rx) = mpsc::channel(CHANNEL_CAPACITY);

    let handle = mz_ore::task::spawn(
        || format!("CollectionManager-write_task-{id}"),
        async move {
            let mut interval = tokio::time::interval(DEFAULT_TICK);

            'run: loop {
                tokio::select! {
                    // Prefer sending actual updates over just bumping the upper, because sending
                    // updates also bump the upper.
                    biased;

                    // Pull as many queued updates off the channel as possible.
                    cmd = rx.recv_many(CHANNEL_CAPACITY) => {
                        if let Some(batch) = cmd {
                            // To rate limit appends to persist we add artifical latency, and will
                            // finish no sooner than this instant.
                            let min_time_to_complete = Instant::now() + DEFAULT_TICK;

                            // Reset the interval which is used to periodically bump the uppers
                            // because the uppers will get bumped with the following update.
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
                                        notify_listeners(responders, || Err(StorageError::ShuttingDown("PersistWriteWorker")));

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

    (tx, handle)
}

/// A "oneshot"-like channel that allows you to append a set of updates to a pre-defined [`GlobalId`].
///
/// See `CollectionManager::monotonic_appender` to acquire a [`MonotonicAppender`].
#[derive(Debug)]
pub struct MonotonicAppender {
    tx: WriteChannel,
}

impl MonotonicAppender {
    pub async fn append(self, updates: Vec<(Row, Diff)>) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();

        // Make sure there is space available on the channel.
        let permit = self.tx.try_reserve().map_err(|e| {
            let msg = "collection manager";
            match e {
                TrySendError::Full(_) => StorageError::ResourceExhausted(msg),
                TrySendError::Closed(_) => StorageError::ShuttingDown(msg),
            }
        })?;

        // Send our update to the CollectionManager.
        permit.send((updates, tx));

        // Wait for a response, if we fail to receive then the CollectionManager has gone away.
        let result = rx
            .await
            .map_err(|_| StorageError::ShuttingDown("collection manager"))?;

        result
    }
}

// Note(parkmycar): While it technically could be `Clone` we want `MonotonicAppender` to have the
// same semantics as a oneshot channel, so we specifically don't make it `Clone`.
static_assertions::assert_not_impl_any!(MonotonicAppender: Clone);

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
