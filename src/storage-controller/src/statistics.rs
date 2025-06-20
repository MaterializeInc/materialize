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

use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice;
use itertools::Itertools;
use mz_cluster_client::ReplicaId;
use mz_ore::now::EpochMillis;
use mz_persist_types::Codec64;
use mz_repr::{Diff, TimestampManipulation};
use mz_repr::{GlobalId, Row};
use mz_storage_client::statistics::ControllerSourceStatistics;
use mz_storage_client::statistics::{PackableStats, WebhookStatistics};
use timely::progress::ChangeBatch;
use timely::progress::Timestamp;
use tokio::sync::oneshot;
use tokio::sync::watch::Receiver;

use crate::collection_mgmt::CollectionManager;
use StatsState::*;

#[derive(Debug, Clone)]
pub(super) enum StatsState<Stat> {
    /// This stat is fully initialized.
    Initialized(Stat),
    /// This stat has not been initialized. We must write a `zero_value`
    /// in one interval, before writing the first real update.
    NeedsInit {
        zero_value: Stat,
        future_update: Stat,
    },
}

impl<Stat: Clone> StatsState<Stat> {
    pub(super) fn new(zero_value: Stat) -> Self {
        let future_update = zero_value.clone();
        StatsState::NeedsInit {
            zero_value,
            future_update,
        }
    }

    /// Get a copy of the stat value that should be emitted to the statistics table.
    /// After calling this, you must call `mark_initialized`. (These are separate
    /// to avoid some clones).
    fn stat_to_emit(&mut self) -> &mut Stat {
        match self {
            Initialized(stat) => stat,
            NeedsInit { zero_value, .. } => zero_value,
        }
    }

    /// Make this stat as initialized.
    fn mark_initialized(&mut self) {
        match self {
            Initialized(_) => {}
            NeedsInit { future_update, .. } => *self = Initialized(future_update.clone()),
        }
    }

    /// Get a reference to a stat for additional incorporation.
    pub(super) fn stat(&mut self) -> &mut Stat {
        match self {
            Initialized(stat) => stat,
            NeedsInit { future_update, .. } => future_update,
        }
    }
}

/// Conversion trait to allow multiple shapes of data in [`spawn_statistics_scraper`].
pub(super) trait AsStats<Stats> {
    fn as_stats(&self) -> &BTreeMap<(GlobalId, Option<ReplicaId>), StatsState<Stats>>;
    fn as_mut_stats(&mut self) -> &mut BTreeMap<(GlobalId, Option<ReplicaId>), StatsState<Stats>>;
}

impl<Stats> AsStats<Stats> for BTreeMap<(GlobalId, Option<ReplicaId>), StatsState<Stats>> {
    fn as_stats(&self) -> &BTreeMap<(GlobalId, Option<ReplicaId>), StatsState<Stats>> {
        self
    }

    fn as_mut_stats(&mut self) -> &mut BTreeMap<(GlobalId, Option<ReplicaId>), StatsState<Stats>> {
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
    metrics: mz_storage_client::metrics::StorageControllerMetrics,
) -> Box<dyn Any + Send + Sync>
where
    StatsWrapper: AsStats<Stats> + Debug + Send + 'static,
    Stats: PackableStats + Clone + Debug + Send + 'static,
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

                shared_stats.as_mut_stats().insert(
                    (collection_id, replica_id),
                    StatsState::Initialized(current_stats),
                );
            }

            let mut row_buf = Row::default();
            for (_, stats) in shared_stats.as_stats().iter() {
                if let StatsState::Initialized(stats) = stats {
                    stats.pack(row_buf.packer());
                    correction.push((row_buf.clone(), Diff::ONE));
                }
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
                        for (_, stats) in shared_stats.as_mut_stats().iter_mut() {
                            let stat = stats.stat_to_emit();
                            stat.pack(row_buf.packer());
                            correction.push((row_buf.clone(), 1));
                            stats.mark_initialized();
                        }
                    }

                    consolidation::consolidate(&mut correction);

                    tracing::trace!(%statistics_collection_id, ?correction, "updating stats collection");

                    // Update our view of the output collection and write updates
                    // out to the collection.
                    if !correction.is_empty() {
                        current_metrics.extend(correction.iter().cloned());
                        collection_mgmt
                            .differential_append(statistics_collection_id, correction.into_iter().map(|(r, d)| (r, d.into())).collect());
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
    /// the [`spawn_statistics_scraper`] above. Values are optional as cluster-hosted
    /// sources must initialize the first values, but we need `None` to mark a source
    /// having been created vs dropped (particularly when a cluster can race and report
    /// statistics for a dropped source, which we must ignore).
    pub source_statistics:
        BTreeMap<(GlobalId, Option<ReplicaId>), StatsState<ControllerSourceStatistics>>,
    /// A shared map with atomics for webhook appenders to update the (currently 4)
    /// statistics that can meaningfully produce. These are periodically
    /// copied into `source_statistics` [`spawn_webhook_statistics_scraper`] to avoid
    /// contention.
    pub webhook_statistics: BTreeMap<GlobalId, Arc<WebhookStatistics>>,
}

impl AsStats<ControllerSourceStatistics> for SourceStatistics {
    fn as_stats(
        &self,
    ) -> &BTreeMap<(GlobalId, Option<ReplicaId>), StatsState<ControllerSourceStatistics>> {
        &self.source_statistics
    }

    fn as_mut_stats(
        &mut self,
    ) -> &mut BTreeMap<(GlobalId, Option<ReplicaId>), StatsState<ControllerSourceStatistics>> {
        &mut self.source_statistics
    }
}

// /// Spawns a task that continually writes per-replica source statistics to the raw statistics table.
// pub(super) fn spawn_per_replica_statistics_scraper<T>(
//     statistics_collection_id: GlobalId,
//     collection_mgmt: CollectionManager<T>,
//     shared_stats: Arc<Mutex<SourceStatistics>>,
//     previous_values: Vec<Row>,
//     initial_interval: Duration,
//     mut interval_updated: Receiver<Duration>,
//     metrics: mz_storage_client::metrics::StorageControllerMetrics,
// ) -> Box<dyn Any + Send + Sync>
// where
//     T: Timestamp + Lattice + Codec64 + From<EpochMillis> + TimestampManipulation,
// {
//     let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
//
//     mz_ore::task::spawn(|| "per_replica_statistics_scraper", async move {
//         // Keep track of what we think is the contents of the output
//         // collection, so that we can emit the required retractions/updates
//         // when we learn about new metrics.
//         //
//         // We assume that `shared_stats` is kept up-to-date (and initialized)
//         // by the controller.
//         let mut current_metrics = <ChangeBatch<_>>::new();
//
//         let mut correction = Vec::new();
//         {
//             let mut shared_stats = shared_stats.lock().expect("poisoned");
//             for row in previous_values {
//                 let (id, replica_id, stats) =
//                     ControllerSourceStatistics::unpack(row.clone(), &metrics);
//                 let replica_id = replica_id.expect("per-replica replicas have a replica_id");
//                 shared_stats
//                     .source_statistics
//                     .entry((id, Some(replica_id)))
//                     .or_insert_with(|| StatsState::Initialized(stats));
//             }
//
//             let mut row_buf = Row::default();
//             for (_, stats) in shared_stats.as_stats().iter() {
//                 if let StatsState::Initialized(stats) = stats {
//                     stats.pack(row_buf.packer());
//                     correction.push((row_buf.clone(), Diff::ONE));
//                 }
//             }
//         }
//
//         tracing::error!(%statistics_collection_id, ?correction, "seeding per-replica stats collection");
//
//         // Make sure that the desired state matches what is already there, when
//         // we start up!
//         if !correction.is_empty() {
//             current_metrics.extend(correction.iter().map(|(r, d)| (r.clone(), d.into_inner())));
//
//             collection_mgmt.differential_append(statistics_collection_id, correction);
//         }
//
//         let mut interval = tokio::time::interval(initial_interval);
//         interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
//
//         loop {
//             tokio::select! {
//                 _msg = &mut shutdown_rx => {
//                     break;
//                 }
//
//                 _ = interval_updated.changed() => {
//                     let new_interval = *interval_updated.borrow_and_update();
//                     if new_interval != interval.period() {
//                         interval = tokio::time::interval(new_interval);
//                         interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
//                     }
//                 },
//
//                 _ = interval.tick() => {
//                     let mut row_buf = Row::default();
//                     let mut correction = current_metrics
//                         .iter()
//                         .cloned()
//                         .map(|(row, diff)| (row, -diff))
//                         .collect_vec();
//
//                     // Ideally we move quickly when holding the lock here, as it can hold
//                     // up the coordinator. Because we are just moving some data around, we should
//                     // be fine!
//                     {
//                         let mut shared_stats = shared_stats.lock().expect("poisoned");
//                         tracing::error!(%statistics_collection_id, ?shared_stats, "shared stats");
//                         for (_, stats) in shared_stats.source_statistics.iter_mut() {
//                             let stat = stats.stat_to_emit();
//                             stat.pack(row_buf.packer());
//                             correction.push((row_buf.clone(), 1));
//                             stats.mark_initialized();
//                         }
//                     }
//
//                     consolidation::consolidate(&mut correction);
//
//                     tracing::error!(%statistics_collection_id, ?current_metrics, ?correction, "updating stats collection");
//
//                     // Update our view of the output collection and write updates
//                     // out to the collection.
//                     if !correction.is_empty() {
//                         current_metrics.extend(correction.iter().cloned());
//                         collection_mgmt
//                             .differential_append(statistics_collection_id, correction.into_iter().map(|(r, d)| (r, d.into())).collect());
//                     }
//                 }
//             }
//         }
//     });
//
//     Box::new(shutdown_tx)
// }

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
                        // Don't override it if its been removed.
                        shared_stats
                            .source_statistics
                            .entry((*id, None))
                            .and_modify(|current| {
                                let update = ws.drain_into_update(*id);
                                current.stat().incorporate(update);
                            });
                    }
                }
            }
        }

        tracing::info!("shutting down statistics sender task");
    });

    Box::new(shutdown_tx)
}
