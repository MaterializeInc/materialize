// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for our Frontegg Authentication client.

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::stats::histogram_seconds_buckets;
use prometheus::{HistogramVec, IntCounterVec, IntGaugeVec};

#[derive(Debug, Clone)]
pub struct Metrics {
    /// Total number of requests since process start.
    pub http_request_count: IntCounterVec,
    /// How long it takes for a request to Frontegg to complete.
    pub request_duration_seconds: HistogramVec,
    /// The number of active refresh tasks we have running.
    pub refresh_tasks_active: IntGaugeVec,
    /// Number of sessions that have requested to start.
    pub session_request_count: IntCounterVec,
    /// Number of sessions that get refreshed.
    pub session_refresh_count: IntCounterVec,
}

impl Metrics {
    pub(crate) fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            http_request_count: registry.register(metric!(
                name: "mz_auth_request_count",
                help: "Total number of HTTP requests made to Frontegg for authentication",
                var_labels: ["path", "status"],
            )),
            request_duration_seconds: registry.register(metric!(
                name: "mz_auth_request_duration_seconds",
                help: "How long it takes for a request to Frontegg to complete in seconds.",
                var_labels: ["path"],
                buckets: histogram_seconds_buckets(0.000_128, 8.0),
            )),
            refresh_tasks_active: registry.register(metric!(
                name: "mz_auth_refresh_tasks_active",
                help: "The number of active refresh tasks we have running.",
            )),
            session_request_count: registry.register(metric!(
                name: "mz_auth_session_request_count",
                help: "Total number of session start requests the Authenticator has received.",
                var_labels: ["existing_session"],
            )),
            session_refresh_count: registry.register(metric!(
                name: "mz_auth_session_refresh_count",
                help: "Total number of authentication sessions that get refreshed.",
                var_labels: ["outstanding_receivers", "recent_drop"],
            )),
        }
    }
}
