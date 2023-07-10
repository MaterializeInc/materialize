// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A tokio task (and support machinery) for maintaining storage-managed
//! collections.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use mz_ore::channel::ReceiverExt;
use mz_ore::now::{EpochMillis, NowFn};
use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, Row, TimestampManipulation};
use timely::progress::Timestamp;
use tokio::sync::{mpsc, oneshot, Mutex};
use tracing::debug;

use crate::client::TimestamplessUpdate;
use crate::controller::{persist_handles, StorageError};

#[derive(Debug, Clone)]
pub struct CollectionManager {
    // TODO(guswynn): this should be a sync mutex, as it protects
    // normal data.
    collections: Arc<Mutex<BTreeSet<GlobalId>>>,
    tx: mpsc::Sender<(
        GlobalId,
        Vec<(Row, Diff)>,
        oneshot::Sender<Result<(), StorageError>>,
    )>,
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
impl CollectionManager {
    pub(super) fn new<
        T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
    >(
        write_handle: persist_handles::PersistWriteWorker<T>,
        now: NowFn,
    ) -> CollectionManager {
        let collections = Arc::new(Mutex::new(BTreeSet::new()));
        let collections_outer = Arc::clone(&collections);
        // Note(parkmycar): The capacity here was chosen randomly.
        let (tx, mut rx) = mpsc::channel::<(
            GlobalId,
            Vec<(Row, Diff)>,
            oneshot::Sender<Result<(), StorageError>>,
        )>(256);

        mz_ore::task::spawn(|| "ControllerManagedCollectionWriter", async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(1_000));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let collections = &mut *collections.lock().await;

                        let now = T::from(now());
                        let updates = collections.iter().map(|id| {
                            (*id, vec![], now.clone())
                        }).collect::<Vec<_>>();

                        // Failures don't matter when advancing collections'
                        // uppers. This might fail when a clusterd happens
                        // to be writing to this concurrently. Advancing
                        // uppers here is best-effort and only needs to
                        // succeed if no one else is advancing it;
                        // contention proves otherwise.
                        match write_handle.monotonic_append(updates).await {
                            Ok(_append_result) => (), // All good!
                            Err(_recv_error) => {
                                // Sender hung up, this seems fine and can
                                // happen when shutting down.
                            }
                        }
                    },
                    cmd = rx.recv_many(64) => {
                        if let Some(batch) = cmd {
                            #[derive(Default)]
                            struct UpdateRequest {
                                rows: Vec<(Row, Diff)>,
                                notifs: Vec<oneshot::Sender<Result<(), StorageError>>>,
                            }

                            // Group all of our updates based on ID.
                            let mut updates: BTreeMap<GlobalId, UpdateRequest> = BTreeMap::new();
                            for (id, rows, notif) in batch {
                                let request = updates.entry(id).or_default();
                                request.rows.extend(rows);
                                request.notifs.push(notif);
                            }

                            // Make sure all of the collections exist.
                            let collections = collections.lock().await;

                            let (mut updates, non_existent): (BTreeMap<_, _>, BTreeMap<_, _>) = updates
                                .into_iter()
                                .partition(|(key, _val)| collections.contains(key));
                            drop(collections);

                            // Return errors for requests whose collection does not exist.
                            for (id, UpdateRequest { notifs, .. }) in non_existent {
                                for notif in notifs {
                                    // Note: Here we use IdentifierInvalid as oppossed to
                                    // IdentifierMissing because the ID might exist but wasn't
                                    // registered as a managed collection, which is different than
                                    // the ID missing entirely.
                                    //
                                    // We don't care if the listener disappeared.
                                    let _ = notif.send(Err(StorageError::IdentifierInvalid(id)));
                                }
                            }

                            // As updates succeed we'll remove them from the set.
                            while !updates.is_empty() {
                                // Gather all of the updates into a request for persist.
                                let request = updates
                                    .iter()
                                    .map(|(id, req)| {
                                        let rows = req
                                            .rows
                                            .clone()
                                            .into_iter()
                                            .map(|(row, diff)| TimestamplessUpdate { row, diff })
                                            .collect::<Vec<_>>();
                                        (*id, rows, T::from(now()))
                                    })
                                    .collect();

                                // Append updates to persist!
                                let append_result = write_handle.monotonic_append(request).await.expect("sender hung up");

                                match append_result {
                                    // Everything was successful!
                                    Ok(()) => {
                                        // Notify all of our listeners.
                                        for (_id, UpdateRequest { notifs, .. }) in updates {
                                            for notif in notifs {
                                                // We don't care if the listener went away.
                                                let _ = notif.send(Ok(()));
                                            }
                                        }
                                        // Break because there are no more updates to send.
                                        break
                                    },
                                    // Failed to write to some collections.
                                    Err(StorageError::InvalidUppers(failed_ids)) => {
                                        // It's fine to retry invalid-uppers errors here, since monotonic appends
                                        // do not specify a particular upper or timestamp.

                                        assert!(
                                            failed_ids.iter().all(|id| updates.contains_key(id)),
                                            "expect to receive errors only for collections we tried to update"
                                        );

                                        let (failed, success): (BTreeMap<_, _>, BTreeMap<_, _>) = updates
                                            .into_iter()
                                            .partition(|(id, _val)| failed_ids.contains(id));

                                        // Notify listeners of success.
                                        for (_id, UpdateRequest { notifs, .. }) in success {
                                            for notif in notifs {
                                                // We don't care if the listener went away.
                                                let _ = notif.send(Ok(()));
                                            }
                                        }

                                        // Retain and retry the updates that failed.
                                        updates = failed;

                                        debug!("Retrying invalid-uppers error while appending to managed collection {failed_ids:?}");
                                    }
                                    // Uh-oh, something else went wrong!
                                    Err(other) => {
                                        let failed_ids = updates.into_keys().collect::<Vec<_>>();
                                        panic!("Unhandled error while appending to managed collection {failed_ids:?}: {other:?}")
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        CollectionManager {
            tx,
            collections: collections_outer,
        }
    }

    /// Registers the collection as one that `CollectionManager` will:
    /// - Automatically advance the upper of every second
    /// - Accept appends for. However, note that when appending, the
    ///   `CollectionManager` expects to be the only writer.
    pub(super) async fn register_collection(&self, id: GlobalId) {
        self.collections.lock().await.insert(id);
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
            let (tx, _rx) = oneshot::channel();
            self.tx.send((id, updates, tx)).await.expect("rx hung up");
        }
    }

    /// Returns a [`MonotonicAppender`] that can be used to monotonically append updates to the
    /// collection correlated with `id`.
    pub(super) fn monotonic_appender(&self, id: GlobalId) -> MonotonicAppender {
        MonotonicAppender {
            id,
            tx: self.tx.clone(),
        }
    }
}

/// A "oneshot"-like channel that allows you to append a set of updates to a pre-defined [`GlobalId`].
///
/// See `CollectionManager::monotonic_appender` to acquire a [`MonotonicAppender`].
#[derive(Debug)]
pub struct MonotonicAppender {
    id: GlobalId,
    tx: mpsc::Sender<(
        GlobalId,
        Vec<(Row, Diff)>,
        oneshot::Sender<Result<(), StorageError>>,
    )>,
}

impl MonotonicAppender {
    pub async fn append(self, updates: Vec<(Row, Diff)>) -> Result<(), StorageError> {
        let (tx, rx) = oneshot::channel();

        // Make sure there is space available on the channel.
        let permit = self
            .tx
            .try_reserve()
            .map_err(|_| StorageError::ResourceExhausted("collection manager"))?;

        // Send our update to the CollectionManager.
        permit.send((self.id, updates, tx));

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
