// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A tokio task (and support machinery) for scraping and writing down
//! metrics.

use std::any::Any;
use std::time::Duration;

use itertools::Itertools;
use timely::progress::ChangeBatch;
use tokio::sync::oneshot;

use mz_ore::cast::CastFrom;
use mz_repr::{Datum, GlobalId, Row};

use crate::controller::collection_mgmt::CollectionManager;
use crate::controller::hosts::MetricsFetcher;

/// Spawns a task that continually (at an interval) fetches metrics from the
/// given [`MetricsFetcher`] and writes it out to the collection identified by
/// `metrics_collection_id`. The task will be stopped when the returned token is
/// dropped.
///
/// This assumes that the output collection is empty (or at least that updates
/// sum up to a zero diff) and that we are the sole writer. Once we need support
/// for concurrency we need to either teach [`CollectionManager`] to deal with
/// that or we need to learn to reconciliate our knowledge about the contents of
/// the output shard with other processes, by reading back in the current
/// contents of the output shard.
pub(super) fn spawn_metrics_scraper(
    metrics_collection_id: GlobalId,
    mut metrics_fetcher: MetricsFetcher,
    collection_mgmt: CollectionManager,
) -> Box<dyn Any + Send + Sync> {
    // TODO(aljoscha): Should this be configurable? Maybe via LaunchDarkly?
    const METRICS_INTERVAL: Duration = Duration::from_secs(10);

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    mz_ore::task::spawn(|| "metrics_scraper", async move {
        // Keep track of what we think is the contents of the output
        // collection, so that we can emit the required retractions/updates
        // when we learn about new metrics.
        //
        // We assume that one update from the MetricsFetcher contains entries
        // for all known provisioned instances and if instances are not in an
        // update their updates should be removed from the output collection.
        let mut current_metrics = ChangeBatch::new();

        let mut interval = tokio::time::interval(METRICS_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            // TODO(guswynn): reduce the amount of un-formattable code here.
            tokio::select! {
                _msg = &mut shutdown_rx => {
                    break;
                }

                _ = interval.tick() => {
                    let metrics = metrics_fetcher.fetch_metrics().await;
                    tracing::trace!("metrics: {:?}", metrics);

                    let mut row_buf = Row::default();
                    let mut correction = current_metrics
                        .iter()
                        .cloned()
                        .map(|(row, diff)| (row, -diff))
                        .collect_vec();

                    // NOTE: host_addr is currently unused, it will be
                    // logged at trace level, which might be useful when
                    // debugging.
                    for (service_id, _host_addr, metrics) in metrics {
                        match metrics {
                            Ok(process_metrics) => {
                                for (process_id, metrics) in process_metrics.into_iter().enumerate()
                                {
                                    let mut packer = row_buf.packer();

                                    packer.push(Datum::from(service_id.to_string().as_str()));
                                    packer.push(Datum::from(u64::cast_from(process_id)));
                                    packer.push(Datum::from(metrics.cpu_nano_cores));
                                    packer.push(Datum::from(metrics.memory_bytes));

                                    correction.push((row_buf.clone(), 1));
                                }
                            }
                            Err(err) => {
                                tracing::warn!("error fetching metrics for {service_id}: {err}");
                            }
                        }
                    }

                    // Update our view of the output collection and write updates
                    // out to the collection.
                    if !correction.is_empty() {
                        current_metrics.extend(correction.iter().cloned());
                        collection_mgmt
                            .append_to_collection(metrics_collection_id, correction)
                            .await;
                    }
                }
            }
        }

        tracing::info!("shutting down metrics scraper task");
    });

    Box::new(shutdown_tx)
}
