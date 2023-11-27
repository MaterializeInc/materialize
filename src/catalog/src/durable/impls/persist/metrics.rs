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
use prometheus::{Counter, IntCounterVec};

#[derive(Debug, Clone)]
pub struct Metrics {
    pub transactions: IntCounter,
    pub transaction_commit_errors: IntCounterVec,
    pub transaction_commit_latency_duration_seconds: Counter,
    pub transactions_committed: IntCounter,
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
                name: "catalog_transactions",
                help: "Total number of started transactions.",
            )),
            transaction_commit_errors: registry.register(metric!(
                name: "catalog_transaction_errors",
                help: "Total number of transaction errors.",
                var_labels: ["cause"],
            )),
            transaction_commit_latency_duration_seconds: registry.register(metric!(
                name: "catalog_transaction_latency_seconds",
                help: "Latency for durable catalog transactions.",
            )),
            transactions_committed: registry.register(metric!(
                name: "catalog_transactions_committed",
                help: "Count of transactions committed.",
            )),
            snapshot_latency_duration_seconds: registry.register(metric!(
                name: "catalog_snapshot_latency",
                help: "Latency for fetching a snapshot of the durable catalog.",
            )),
            snapshots_taken: registry.register(metric!(
                name: "catalog_snapshots_taken",
                help: "Count of snapshots taken.",
            )),
            sync_latency_duration_seconds: registry.register(metric!(
                name: "catalog_sync_latency",
                help: "Latency for syncing the in-memory state of the durable catalog with the persisted contents.",
            )),
            syncs: registry.register(metric!(
                name: "catalog_syncs",
                help: "Count of catalog syncs.",
            )),
        }
    }
}
