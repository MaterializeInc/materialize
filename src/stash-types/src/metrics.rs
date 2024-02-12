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
use mz_ore::metrics::MetricsRegistry;
use mz_ore::stats::histogram_seconds_buckets;
use prometheus::{Counter, HistogramVec, IntCounter, IntCounterVec};

#[derive(Debug, Clone)]
pub struct Metrics {
    pub transactions: IntCounter,
    pub catalog_transaction_commits: IntCounter,
    pub catalog_transaction_commit_latency_seconds: Counter,
    pub transaction_errors: IntCounterVec,
    pub query_latency_duration_seconds: HistogramVec,
    pub connection_attempts: IntCounterVec,
    pub connection_errors: IntCounter,
}

impl Metrics {
    pub fn register_into(registry: &MetricsRegistry) -> Metrics {
        let metrics = Metrics {
            transactions: registry.register(metric!(
                name: "mz_stash_transactions",
                help: "Total number of started transactions.",
            )),
            catalog_transaction_commits: registry.register(metric!(
                name: "mz_stash_catalog_transaction_commits",
                help: "Total number of catalog transaction commits.",
            )),
            catalog_transaction_commit_latency_seconds: registry.register(metric!(
                name: "mz_stash_transaction_latency_seconds",
                help: "Total latency for catalog transaction commits.",
            )),
            transaction_errors: registry.register(metric!(
                name: "mz_stash_transaction_errors",
                help: "Total number of transaction errors.",
                var_labels: ["cause"],
            )),
            query_latency_duration_seconds: registry.register(metric!(
                name: "mz_query_latency",
                help: "Latency for queries to CockroachDB",
                var_labels: ["query_kind"],
                buckets: histogram_seconds_buckets(0.000_128, 32.0),
            )),
            connection_attempts: registry.register(metric!(
                name: "mz_stash_connection_attempts",
                help: "Total number of connection attempts.",
                var_labels: ["status"],
            )),
            connection_errors: registry.register(metric!(
                name: "mz_stash_connection_errors",
                help: "Total number of connection errors, after the connection has been established.",
            )),
        };

        // Initialize error codes to 0 so we can observe their increase.
        metrics
            .transaction_errors
            .with_label_values(&["closed"])
            .inc_by(0);
        metrics
            .transaction_errors
            .with_label_values(&["retry"])
            .inc_by(0);
        metrics
            .transaction_errors
            .with_label_values(&["other"])
            .inc_by(0);

        metrics
            .connection_attempts
            .with_label_values(&["success"])
            .inc_by(0);
        metrics
            .connection_attempts
            .with_label_values(&["failure"])
            .inc_by(0);

        metrics
    }
}
