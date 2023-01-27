// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation-specific metrics for persist blobs and consensus

use mz_ore::metric;
use mz_ore::metrics::{Counter, IntCounter, MetricsRegistry, UIntGauge};

/// Metrics specific to S3Blob's internal workings.
#[derive(Debug, Clone)]
pub struct S3BlobMetrics {
    pub(crate) operation_timeouts: IntCounter,
    pub(crate) operation_attempt_timeouts: IntCounter,
    pub(crate) connect_timeouts: IntCounter,
    pub(crate) read_timeouts: IntCounter,
}

impl S3BlobMetrics {
    /// Returns a new [S3BlobMetrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        Self {
            operation_timeouts: registry.register(metric!(
                name: "mz_persist_s3_operation_timeouts",
                help: "number of operation timeouts (including retries)",
            )),
            operation_attempt_timeouts: registry.register(metric!(
                name: "mz_persist_s3_operation_attempt_timeouts",
                help: "number of operation attempt timeouts (within a single retry)",
            )),
            connect_timeouts: registry.register(metric!(
                name: "mz_persist_s3_connect_timeouts",
                help: "number of timeouts establishing a connection to S3",
            )),
            read_timeouts: registry.register(metric!(
                name: "mz_persist_s3_read_timeouts",
                help: "number of timeouts waiting on first response byte from S3",
            )),
        }
    }
}

/// Metrics specific to PostgresConsensus's internal workings.
#[derive(Debug, Clone)]
pub struct PostgresConsensusMetrics {
    pub(crate) connpool_size: UIntGauge,
    pub(crate) connpool_acquires: IntCounter,
    pub(crate) connpool_acquire_seconds: Counter,
    pub(crate) connpool_connections_created: Counter,
    pub(crate) connpool_ttl_reconnections: Counter,
}

impl PostgresConsensusMetrics {
    /// Returns a new [PostgresConsensusMetrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        Self {
            connpool_size: registry.register(metric!(
                name: "mz_persist_postgres_connpool_size",
                help: "number of connections currently in pool",
            )),
            connpool_acquires: registry.register(metric!(
                name: "mz_persist_postgres_connpool_acquires",
                help: "times a connection has been acquired from pool",
            )),
            connpool_acquire_seconds: registry.register(metric!(
                name: "mz_persist_postgres_connpool_acquire_seconds",
                help: "time spent acquiring connections from pool",
            )),
            connpool_connections_created: registry.register(metric!(
                name: "mz_persist_postgres_connpool_connections_created",
                help: "times a connection was created",
            )),
            connpool_ttl_reconnections: registry.register(metric!(
                name: "mz_persist_postgres_connpool_ttl_reconnections",
                help: "times a connection was recycled due to ttl",
            )),
        }
    }
}
