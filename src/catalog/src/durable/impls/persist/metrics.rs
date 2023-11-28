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
use prometheus::Counter;

#[derive(Debug, Clone)]
pub struct Metrics {
    pub transactions: IntCounter,
    pub transaction_commit_errors: IntCounter,
    pub transaction_commit_latency_duration_seconds: Counter,
    pub transaction_commits_initiated: IntCounter,
    pub snapshot_latency_duration_seconds: Counter,
    pub snapshots_taken: IntCounter,
    pub sync_latency_duration_seconds: Counter,
    pub syncs: IntCounter,
}

impl Metrics {
    /// Returns a new [Metrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        Self {
            transactions: registry.register(metric!(
                name: "mz_catalog_transactions",
                help: "Total number of started transactions.",
            )),
            transaction_commit_errors: registry.register(metric!(
                name: "mz_catalog_transaction_errors",
                help: "Total number of transaction errors.",
                var_labels: ["cause"],
            )),
            transaction_commit_latency_duration_seconds: registry.register(metric!(
                name: "mz_catalog_transaction_latency_seconds",
                help: "Total latency for durable catalog transactions.",
            )),
            transaction_commits_initiated: registry.register(metric!(
                name: "mz_catalog_transaction_commits_initiated",
                help: "Count of transaction commits that were initiated, this includes ones that ended in successes or failures.",
            )),
            snapshot_latency_duration_seconds: registry.register(metric!(
                name: "mz_catalog_snapshot_latency_seconds",
                help: "Total latency for fetching a snapshot of the durable catalog.",
            )),
            snapshots_taken: registry.register(metric!(
                name: "mz_catalog_snapshots_taken",
                help: "Count of snapshots taken.",
            )),
            sync_latency_duration_seconds: registry.register(metric!(
                name: "mz_catalog_sync_latency_seconds",
                help: "Total latency for syncing the in-memory state of the durable catalog with the persisted contents.",
            )),
            syncs: registry.register(metric!(
                name: "mz_catalog_syncs",
                help: "Count of catalog syncs.",
            )),
        }
    }
}
