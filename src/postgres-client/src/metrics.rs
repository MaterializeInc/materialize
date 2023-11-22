// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation-specific metrics for the internal, pooled Postgres client.

use mz_ore::metric;
use mz_ore::metrics::{Counter, IntCounter, MetricsRegistry, UIntGauge};

/// Metrics specific to [PostgresClient](crate::PostgresClient)'s internal
/// workings.
#[derive(Debug, Clone)]
pub struct PostgresClientMetrics {
    pub(crate) connpool_size: UIntGauge,
    pub(crate) connpool_acquires: IntCounter,
    pub(crate) connpool_acquire_seconds: Counter,
    pub(crate) connpool_available: prometheus::Gauge,
    pub(crate) connpool_connections_created: Counter,
    pub(crate) connpool_connection_errors: Counter,
    pub(crate) connpool_ttl_reconnections: Counter,
}

impl PostgresClientMetrics {
    /// Returns a new [PostgresClientMetrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry, prefix: &str) -> Self {
        Self {
            connpool_size: registry.register(metric!(
                name: format!("{}_postgres_connpool_size", prefix),
                help: "number of connections currently in pool",
            )),
            connpool_acquires: registry.register(metric!(
                name: format!("{}_postgres_connpool_acquires", prefix),
                help: "times a connection has been acquired from pool",
            )),
            connpool_acquire_seconds: registry.register(metric!(
                name: format!("{}_postgres_connpool_acquire_seconds", prefix),
                help: "time spent acquiring connections from pool",
            )),
            connpool_available: registry.register(metric!(
                name: format!("{}_postgres_connpool_available", prefix),
                help: "available connections in the pool",
            )),
            connpool_connections_created: registry.register(metric!(
                name: format!("{}_postgres_connpool_connections_created", prefix),
                help: "times a connection was created",
            )),
            connpool_connection_errors: registry.register(metric!(
                name: format!("{}_postgres_connpool_connection_errors", prefix),
                help: "number of errors when establishing a new connection",
            )),
            connpool_ttl_reconnections: registry.register(metric!(
                name: format!("{}_postgres_connpool_ttl_reconnections", prefix),
                help: "times a connection was recycled due to ttl",
            )),
        }
    }
}
