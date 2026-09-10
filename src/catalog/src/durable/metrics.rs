// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus monitoring metrics.

use mz_ore::metric;
use mz_ore::metrics::{IntCounter, MetricsRegistry};
use mz_ore::stats::histogram_seconds_buckets;
use prometheus::{Histogram, IntCounterVec, IntGauge, IntGaugeVec};

#[derive(Debug, Clone)]
pub struct Metrics {
    pub transactions_started: IntCounter,
    pub transaction_commits: IntCounter,
    pub transaction_commit_latency_seconds: Histogram,
    pub snapshots_taken: IntCounter,
    pub snapshot_latency_seconds: Histogram,
    pub syncs: IntCounter,
    pub sync_latency_seconds: Histogram,
    pub collection_entries: IntGaugeVec,
    pub allocate_id_seconds: Histogram,
    pub snapshot_consolidations: IntCounter,
    pub snapshot_max_entries: IntGauge,
    /// Acknowledged row traffic through the catalog compare-and-append path, including bootstrap.
    /// Each pair counts updates and packed row bytes, ordered by compaction bound, maintained
    /// read requirement, and other kind. Each submitted row counts once regardless of its diff.
    pub(crate) committed_row_traffic: [(IntCounter, IntCounter); 3],
}

impl Metrics {
    /// Returns a new [Metrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        let updates: IntCounterVec = registry.register(metric!(
            name: "mz_catalog_committed_updates",
            help: "Number of catalog row updates in acknowledged compare-and-appends, including retractions and bootstrap writes.",
            var_labels: ["kind"],
        ));
        let bytes: IntCounterVec = registry.register(metric!(
            name: "mz_catalog_committed_update_bytes",
            help: "Packed SourceData row bytes in acknowledged catalog compare-and-appends, including retractions and bootstrap writes. Excludes timestamps, diffs, Persist encoding, compression, and network framing. Not blob or network bytes.",
            var_labels: ["kind"],
        ));
        let committed_row_traffic = ["compaction_bound", "maintained_read_requirement", "other"]
            .map(|kind| {
                (
                    updates.with_label_values(&[kind]),
                    bytes.with_label_values(&[kind]),
                )
            });
        Self {
            committed_row_traffic,
            transactions_started: registry.register(metric!(
                name: "mz_catalog_transactions_started",
                help: "Total number of started transactions.",
            )),
            transaction_commits: registry.register(metric!(
                name: "mz_catalog_transaction_commits",
                help: "Count of transaction commits.",
            )),
            transaction_commit_latency_seconds: registry.register(metric!(
                name: "mz_catalog_transaction_commit_latency_seconds",
                help: "Latency for committing a durable catalog transaction.",
                buckets: histogram_seconds_buckets(0.000_128, 32.0),
            )),
            snapshots_taken: registry.register(metric!(
                name: "mz_catalog_snapshots_taken",
                help: "Count of snapshots taken.",
            )),
            snapshot_latency_seconds: registry.register(metric!(
                name: "mz_catalog_snapshot_latency_seconds",
                help: "Latency for fetching a snapshot of the durable catalog.",
                buckets: histogram_seconds_buckets(0.000_128, 32.0),
            )),
            syncs: registry.register(metric!(
                name: "mz_catalog_syncs",
                help: "Count of catalog syncs.",
            )),
            sync_latency_seconds: registry.register(metric!(
                name: "mz_catalog_sync_latency_seconds",
                help: "Latency for syncing the in-memory state of the durable catalog with the persisted contents.",
                buckets: histogram_seconds_buckets(0.000_128, 32.0),
            )),
            collection_entries: registry.register(metric!(
                name: "mz_catalog_collection_entries",
                help: "Total number of entries, after consolidation, per catalog collection.",
                var_labels: ["collection"],
            )),
            allocate_id_seconds: registry.register(metric!(
                name: "mz_catalog_allocate_id_seconds",
                help: "The time it takes to allocate IDs in the durable catalog.",
                buckets: histogram_seconds_buckets(0.001, 32.0),
            )),
            snapshot_consolidations: registry.register(metric!(
                name: "mz_catalog_snapshot_consolidations",
                help: "Count of snapshot consolidation passes.",
            )),
            snapshot_max_entries: registry.register(metric!(
                name: "mz_catalog_snapshot_max_entries",
                help: "High-water mark of entries in the unconsolidated in-memory \
                       snapshot since process start.",
            )),
        }
    }
}
