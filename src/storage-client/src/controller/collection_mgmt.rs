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

use std::collections::HashSet;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use mz_ore::now::{EpochMillis, NowFn};
use timely::progress::Timestamp;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use mz_persist_types::Codec64;
use mz_repr::{Diff, GlobalId, Row, TimestampManipulation};

use crate::client::TimestamplessUpdate;

use super::persist_handles;

#[derive(Debug, Clone)]
pub struct CollectionManager {
    collections: Arc<Mutex<HashSet<GlobalId>>>,
    tx: mpsc::Sender<(GlobalId, Vec<(Row, Diff)>)>,
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
        let collections = Arc::new(Mutex::new(HashSet::new()));
        let collections_outer = Arc::clone(&collections);
        let (tx, mut rx) = mpsc::channel::<(GlobalId, Vec<(Row, Diff)>)>(1);

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
                    cmd = rx.recv() => {
                        if let Some((id, updates)) = cmd {
                            assert!(collections.lock().await.contains(&id));

                            let updates = vec![(id, updates.into_iter().map(|(row, diff)| TimestamplessUpdate {
                                row,
                                diff,
                            }).collect::<Vec<_>>(), T::from(now()))];

                            // TODO? Handle contention among multiple writers
                            write_handle.monotonic_append(updates)
                                .await
                                .expect("sender hung up")
                                .expect("no write contention on collections");
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
        self.tx.send((id, updates)).await.expect("rx hung up");
    }
}
