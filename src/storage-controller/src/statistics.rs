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
use mz_ore::now::EpochMillis;
use mz_persist_types::Codec64;
use mz_repr::TimestampManipulation;
use mz_repr::{GlobalId, Row};
use mz_storage_client::statistics::{PackableStats, SourceStatisticsUpdate, WebhookStatistics};
use timely::progress::ChangeBatch;
use timely::progress::Timestamp;
use tokio::sync::oneshot;
use tokio::sync::watch::Receiver;

use crate::collection_mgmt::CollectionManager;

/// Conversion trait to allow multiple shapes of data in [`spawn_statistics_scraper`].
pub(super) trait AsStats<Stats> {
    fn as_stats(&self) -> &BTreeMap<GlobalId, Option<Stats>>;
    fn as_mut_stats(&mut self) -> &mut BTreeMap<GlobalId, Option<Stats>>;
}

impl<Stats> AsStats<Stats> for BTreeMap<GlobalId, Option<Stats>> {
    fn as_stats(&self) -> &BTreeMap<GlobalId, Option<Stats>> {
        self
    }
    fn as_mut_stats(&mut self) -> &mut BTreeMap<GlobalId, Option<Stats>> {
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

        let mut correction = Vec::new();
        {
            let mut shared_stats = shared_stats.lock().expect("poisoned");
            for row in previous_values {
                let current = Stats::unpack(row, &metrics);

                shared_stats
                    .as_mut_stats()
                    .insert(current.0, Some(current.1));
            }

            let mut row_buf = Row::default();
            for (_, stats) in shared_stats.as_stats().iter() {
                if let Some(stats) = stats {
                    stats.pack(row_buf.packer());
                    correction.push((row_buf.clone(), 1));
                }
            }
        }

        tracing::debug!(%statistics_collection_id, ?correction, "seeding stats collection");

        // Make sure that the desired state matches what is already there, when
        // we start up!
        if !correction.is_empty() {
            current_metrics.extend(correction.iter().cloned());

            collection_mgmt
                .differential_append(statistics_collection_id, correction)
                .await;
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
                        for (_, stats) in shared_stats.as_stats().iter() {
                            if let Some(stats) = stats {
                                stats.pack(row_buf.packer());
                                correction.push((row_buf.clone(), 1));
                            }
                        }
                    }

                    consolidation::consolidate(&mut correction);

                    tracing::trace!(%statistics_collection_id, ?correction, "updating stats collection");

                    // Update our view of the output collection and write updates
                    // out to the collection.
                    if !correction.is_empty() {
                        current_metrics.extend(correction.iter().cloned());
                        collection_mgmt
                            .differential_append(statistics_collection_id, correction)
                            .await;
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
    pub source_statistics: BTreeMap<GlobalId, Option<SourceStatisticsUpdate>>,
    /// A shared map with atomics for webhook appenders to update the (currently 4)
    /// statistics that can meaningfully produce. These are periodically
    /// copied into `source_statistics` [`spawn_webhook_statistics_scraper`] to avoid
    /// contention.
    pub webhook_statistics: BTreeMap<GlobalId, Arc<WebhookStatistics>>,
}

impl AsStats<SourceStatisticsUpdate> for SourceStatistics {
    fn as_stats(&self) -> &BTreeMap<GlobalId, Option<SourceStatisticsUpdate>> {
        &self.source_statistics
    }
    fn as_mut_stats(&mut self) -> &mut BTreeMap<GlobalId, Option<SourceStatisticsUpdate>> {
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
                        // Don't override it if its been removed.
                        shared_stats
                            .source_statistics
                            .entry(*id)
                            .and_modify(|current| match current {
                                Some(ref mut current) => current.incorporate(ws.drain_into_update(*id)),
                                None => *current = Some(ws.drain_into_update(*id)),
                            });
                    }
                }
            }
        }

        tracing::info!("shutting down statistics sender task");
    });

    Box::new(shutdown_tx)
}
