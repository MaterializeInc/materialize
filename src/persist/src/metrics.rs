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

/// Metrics specific to PostgresConsensus's internal workings.
#[derive(Debug, Clone)]
pub struct PostgresConsensusMetrics {
    pub(crate) connpool_size: UIntGauge,
    pub(crate) connpool_acquires: IntCounter,
    pub(crate) connpool_acquire_seconds: Counter,
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
        }
    }
}
