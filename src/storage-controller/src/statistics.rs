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
use std::collections::{BTreeMap, btree_map};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_cluster_client::ReplicaId;
use mz_ore::now::EpochMillis;
use mz_persist_types::Codec64;
use mz_repr::{Diff, TimestampManipulation};
use mz_repr::{GlobalId, Row};
use mz_storage_client::statistics::{
    ControllerSourceStatistics, ExpirableStats, ZeroInitializedStats,
};
use mz_storage_client::statistics::{PackableStats, WebhookStatistics};
use timely::progress::ChangeBatch;
use timely::progress::Timestamp;
use tokio::sync::oneshot;
use tokio::sync::watch::Receiver;

use crate::collection_mgmt::CollectionManager;

/// Conversion trait to allow multiple shapes of data in [`spawn_statistics_scraper`].
pub(super) trait AsStats<Stats> {
    fn as_stats(&self) -> &BTreeMap<(GlobalId, Option<ReplicaId>), Stats>;
    fn as_mut_stats(&mut self) -> &mut BTreeMap<(GlobalId, Option<ReplicaId>), Stats>;
}

impl<Stats> AsStats<Stats> for BTreeMap<(GlobalId, Option<ReplicaId>), Stats> {
    fn as_stats(&self) -> &BTreeMap<(GlobalId, Option<ReplicaId>), Stats> {
        self
    }

    fn as_mut_stats(&mut self) -> &mut BTreeMap<(GlobalId, Option<ReplicaId>), Stats> {
        self
    }
}

/// Spawns a task that continually (at an interval) writes statistics from storaged's
/// that are consolidated in shared memory in the controller.
pub(super) fn spawn_statistics_scraper<StatsWrapper, Stats, T>(
    statistics_collection_id: GlobalId,
    collection_mgmt: CollectionManager<T>,
    shared_stats: Arc<Mutex<StatsWrapper>>,
    previous_values: Vec<Row>,
    initial_interval: Duration,
    mut interval_updated: Receiver<Duration>,
    statistics_retention_duration: Duration,
    metrics: mz_storage_client::metrics::StorageControllerMetrics,
) -> Box<dyn Any + Send + Sync>
where
    StatsWrapper: AsStats<Stats> + Debug + Send + 'static,
    Stats: PackableStats + ExpirableStats + ZeroInitializedStats + Clone + Debug + Send + 'static,
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
        let mut current_metrics = <ChangeBatch<_>>::new();

        let mut correction = Vec::new();
        {
            let mut shared_stats = shared_stats.lock().expect("poisoned");
            for row in previous_values {
                let (collection_id, replica_id, current_stats) = Stats::unpack(row, &metrics);

                shared_stats
                    .as_mut_stats()
                    .insert((collection_id, replica_id), current_stats);
            }

            let mut row_buf = Row::default();
            for (_, stats) in shared_stats.as_stats().iter() {
                stats.pack(row_buf.packer());
                correction.push((row_buf.clone(), Diff::ONE));
            }
        }

        tracing::debug!(%statistics_collection_id, ?correction, "seeding stats collection");
        // Make sure that the desired state matches what is already there, when
        // we start up!
        if !correction.is_empty() {
            current_metrics.extend(correction.iter().map(|(r, d)| (r.clone(), d.into_inner())));

            collection_mgmt.differential_append(statistics_collection_id, correction);
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
                        let mut shared_stats = shared_stats.lock().expect("poisoned");

                        let now = Instant::now();
                        shared_stats.as_mut_stats().retain(|_key, stat| {
                            let inactive_time = now - stat.last_updated();
                            inactive_time < statistics_retention_duration
                        });

                        for (_, stats) in shared_stats.as_mut_stats().iter_mut() {
                            if stats.needs_zero_initialization() {
                                stats.zero_stat().pack(row_buf.packer());
                                stats.mark_zero_initialized();
                            } else {
                                stats.pack(row_buf.packer());
                            }
                            correction.push((row_buf.clone(), 1));
                        }
                    }

                    consolidation::consolidate(&mut correction);

                    tracing::trace!(%statistics_collection_id, ?correction, "updating stats collection");

                    // Update our view of the output collection and write updates
                    // out to the collection.
                    if !correction.is_empty() {
                        current_metrics.extend(correction.iter().cloned());
                        let updates = correction
                            .into_iter()
                            .map(|(r, d)| (r, d.into()))
                            .collect();
                        collection_mgmt.differential_append(
                            statistics_collection_id,
                            updates,
                        );
                    }
                }
            }
        }

        tracing::info!("shutting down statistics sender task");
    });

    Box::new(shutdown_tx)
}

/// A wrapper around source and webhook statistics maps so we can hold them within a single lock.
#[derive(Debug)]
pub(super) struct SourceStatistics {
    /// Statistics-per-source that will be emitted to the source statistics table with
    /// the [`spawn_statistics_scraper`] above.
    pub source_statistics: BTreeMap<(GlobalId, Option<ReplicaId>), ControllerSourceStatistics>,
    /// A shared map with atomics for webhook appenders to update the (currently 4)
    /// statistics that can meaningfully produce. These are periodically
    /// copied into `source_statistics` [`spawn_webhook_statistics_scraper`] to avoid
    /// contention.
    pub webhook_statistics: BTreeMap<GlobalId, Arc<WebhookStatistics>>,
}

impl AsStats<ControllerSourceStatistics> for SourceStatistics {
    fn as_stats(&self) -> &BTreeMap<(GlobalId, Option<ReplicaId>), ControllerSourceStatistics> {
        &self.source_statistics
    }

    fn as_mut_stats(
        &mut self,
    ) -> &mut BTreeMap<(GlobalId, Option<ReplicaId>), ControllerSourceStatistics> {
        &mut self.source_statistics
    }
}

/// Spawns a task that continually drains webhook statistics into `shared_stats.
pub(super) fn spawn_webhook_statistics_scraper(
    shared_stats: Arc<Mutex<SourceStatistics>>,
    initial_interval: Duration,
    mut interval_updated: Receiver<Duration>,
) -> Box<dyn Any + Send + Sync> {
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    mz_ore::task::spawn(|| "webhook_statistics_scraper", async move {
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
                },

                _ = interval.tick() => {
                    let mut shared_stats = shared_stats.lock().expect("poisoned");
                    let shared_stats = &mut *shared_stats;

                    for (id, ws) in shared_stats.webhook_statistics.iter() {
                        let entry = shared_stats
                            .source_statistics
                            .entry((*id, None));

                        let update = ws.drain_into_update(*id);

                        match entry {
                            btree_map::Entry::Vacant(vacant_entry) => {
                                let mut stats = ControllerSourceStatistics::new(*id, None);
                                stats.incorporate(update);
                                vacant_entry.insert(stats);
                            }
                            btree_map::Entry::Occupied(mut occupied_entry) => {
                                occupied_entry.get_mut().incorporate(update);
                            }
                        }
                    }
                }
            }
        }

        tracing::info!("shutting down statistics sender task");
    });

    Box::new(shutdown_tx)
}
