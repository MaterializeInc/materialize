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
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_ore::now::EpochMillis;
use mz_persist_types::Codec64;
use mz_repr::TimestampManipulation;
use mz_repr::{GlobalId, Row};
use mz_storage_client::statistics::PackableStats;
use timely::progress::ChangeBatch;
use timely::progress::Timestamp;
use tokio::sync::oneshot;
use tokio::sync::watch::Receiver;

use crate::collection_mgmt::CollectionManager;

/// Spawns a task that continually (at an interval) writes statistics from storaged's
/// that are consolidated in shared memory in the controller.
pub(super) fn spawn_statistics_scraper<Stats, T>(
    statistics_collection_id: GlobalId,
    collection_mgmt: CollectionManager<T>,
    shared_stats: Arc<Mutex<BTreeMap<GlobalId, Option<Stats>>>>,
    previous_values: Vec<Row>,
    initial_interval: Duration,
    mut interval_updated: Receiver<Duration>,
) -> Box<dyn Any + Send + Sync>
where
    Stats: PackableStats + Debug + Send + 'static,
    T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
{
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    mz_ore::task::spawn(|| "statistics_scraper", async move {
        // Keep track of what we think is the contents of the output
        // collection, so that we can emit the required retractions/updates
        // when we learn about new metrics.
        //
        // We assume that `shared_stats` is kept up-to-date (and initialized)
        // by the controller.
        let mut current_metrics = ChangeBatch::new();

        {
            let mut shared_stats = shared_stats.lock().expect("poisoned");
            for row in previous_values {
                current_metrics.update(row.clone(), 1);
                let current = Stats::unpack(row);
                shared_stats.insert(current.0, Some(current.1));
            }
        }

        let mut interval = tokio::time::interval(initial_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _msg = &mut shutdown_rx => {
                    break;
                }

               _ = interval_updated.changed() => {
                    let new_interval = *interval_updated.borrow_and_update();
                    if new_interval != interval.period() {
                        interval = tokio::time::interval(new_interval);
                        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                        // Note that the next interval will tick immediately. This is fine.
                    }
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
                    {
                        let shared_stats = shared_stats.lock().expect("poisoned");
                        for (_, stats) in shared_stats.iter() {
                            if let Some(stats) = stats {
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
