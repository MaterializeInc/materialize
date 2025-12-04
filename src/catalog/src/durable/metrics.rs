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
use prometheus::{Counter, Histogram, IntGaugeVec};

#[derive(Debug, Clone)]
pub struct Metrics {
    pub transactions_started: IntCounter,
    pub transaction_commits: IntCounter,
    pub transaction_commit_latency_seconds: Counter,
    pub snapshots_taken: IntCounter,
    pub snapshot_latency_seconds: Counter,
    pub syncs: IntCounter,
    pub sync_latency_seconds: Counter,
    pub collection_entries: IntGaugeVec,
    pub allocate_id_seconds: Histogram,
}

impl Metrics {
    /// Returns a new [Metrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        Self {
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
                help: "Total latency for committing a durable catalog transactions.",
            )),
            snapshots_taken: registry.register(metric!(
                name: "mz_catalog_snapshots_taken",
                help: "Count of snapshots taken.",
            )),
            snapshot_latency_seconds: registry.register(metric!(
                name: "mz_catalog_snapshot_latency_seconds",
                help: "Total latency for fetching a snapshot of the durable catalog.",
            )),
            syncs: registry.register(metric!(
                name: "mz_catalog_syncs",
                help: "Count of catalog syncs.",
            )),
            sync_latency_seconds: registry.register(metric!(
                name: "mz_catalog_sync_latency_seconds",
                help: "Total latency for syncing the in-memory state of the durable catalog with the persisted contents.",
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
        }
    }
}
