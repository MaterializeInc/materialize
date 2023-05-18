// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A tokio task (and support machinery) for producing storage statistics.

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use itertools::Itertools;
use mz_repr::{GlobalId, Row};
use timely::progress::ChangeBatch;
use tokio::sync::oneshot;

use crate::client::PackableStats;
use crate::controller::collection_mgmt::CollectionManager;

/// Spawns a task that continually (at an interval) writes statistics from storaged's
/// that are consolidated in shared memory in the controller.
pub(super) fn spawn_statistics_scraper<Stats: PackableStats + Send + 'static>(
    statistics_collection_id: GlobalId,
    collection_mgmt: CollectionManager,
    shared_stats: Arc<Mutex<BTreeMap<GlobalId, BTreeMap<usize, Stats>>>>,
) -> Box<dyn Any + Send + Sync> {
    // TODO(guswynn): Should this be configurable? Maybe via LaunchDarkly?
    const STATISTICS_INTERVAL: Duration = Duration::from_secs(30);

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    mz_ore::task::spawn(|| "statistics_scraper", async move {
        // Keep track of what we think is the contents of the output
        // collection, so that we can emit the required retractions/updates
        // when we learn about new metrics.
        //
        // We assume that `shared_stats` is kept up-to-date by the controller.
        let mut current_metrics = ChangeBatch::new();

        let mut interval = tokio::time::interval(STATISTICS_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _msg = &mut shutdown_rx => {
                    break;
                }

                _ = interval.tick() => {
                    let mut row_buf = Row::default();
                    let mut correction = current_metrics
                        .iter()
                        .cloned()
                        .map(|(row, diff)| (row, -diff))
                        .collect_vec();

                    // Ideally we move quickly when holding the lock here, as it can hold
                    // up the coordinator. Because we are just moving some data around, we should
                    // be fine!
                    //
                    // TODO: consider using a RwLock instead of a mutex.
                    {
                        let shared_stats = shared_stats.lock().expect("poisoned");

                        for (_, items) in shared_stats.iter() {
                            for (_, stats) in items.iter() {
                                stats.pack(row_buf.packer());
                                correction.push((row_buf.clone(), 1));
                            }
                        }
                    }

                    // Update our view of the output collection and write updates
                    // out to the collection.
                    if !correction.is_empty() {
                        current_metrics.extend(correction.iter().cloned());
                        collection_mgmt
                            .append_to_collection(statistics_collection_id, correction)
                            .await;
                    }
                }
            }
        }

        tracing::info!("shutting down statistics sender task");
    });

    Box::new(shutdown_tx)
}
