// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unified configuration for timestamp oracles.
//!
//! This module provides a [`TimestampOracleConfig`] enum that can hold
//! configuration for either a Postgres-backed or FoundationDB-backed
//! timestamp oracle, allowing the choice of backend to be made at startup time.

use std::sync::Arc;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::url::SensitiveUrl;
use mz_repr::Timestamp;

use crate::TimestampOracle;
#[cfg(any(target_os = "linux", feature = "fdb"))]
use crate::foundationdb_oracle::{FdbTimestampOracle, FdbTimestampOracleConfig};
use crate::metrics::Metrics;
use crate::postgres_oracle::{
    PostgresTimestampOracle, PostgresTimestampOracleConfig, TimestampOracleParameters,
};

/// Unified configuration for timestamp oracles.
///
/// This enum allows selecting between different timestamp oracle backends
/// at startup time.
#[derive(Clone, Debug)]
pub enum TimestampOracleConfig {
    /// Use a Postgres/CockroachDB-backed timestamp oracle.
    Postgres(PostgresTimestampOracleConfig),
    /// Use a FoundationDB-backed timestamp oracle.
    #[cfg(any(target_os = "linux", feature = "fdb"))]
    Fdb(FdbTimestampOracleConfig),
}

impl TimestampOracleConfig {
    /// Create a timestamp oracle configuration from a URL.
    ///
    /// The backend is determined by the URL scheme:
    /// - `postgres://` or `postgresql://` -> Postgres-backed oracle
    /// - `foundationdb://` -> FoundationDB-backed oracle
    ///
    /// Returns an error if the URL scheme is not recognized.
    pub fn from_url(
        url: &SensitiveUrl,
        metrics_registry: &MetricsRegistry,
    ) -> Result<Self, anyhow::Error> {
        let scheme = url.scheme();
        match scheme {
            "postgres" | "postgresql" => Ok(Self::new_postgres(url, metrics_registry)),
            #[cfg(any(target_os = "linux", feature = "fdb"))]
            "foundationdb" => Ok(Self::new_fdb(url.clone(), metrics_registry)),
            #[cfg(not(any(target_os = "linux", feature = "fdb")))]
            "foundationdb" => {
                anyhow::bail!("FoundationDB timestamp oracle is not supported on this platform")
            }
            _ => {
                anyhow::bail!(
                    "unsupported timestamp oracle URL scheme: '{}'. \
                     Supported schemes: postgres, postgresql, foundationdb",
                    scheme
                )
            }
        }
    }

    /// Create a new Postgres-backed timestamp oracle configuration.
    pub fn new_postgres(url: &SensitiveUrl, metrics_registry: &MetricsRegistry) -> Self {
        TimestampOracleConfig::Postgres(PostgresTimestampOracleConfig::new(url, metrics_registry))
    }

    /// Create a new FoundationDB-backed timestamp oracle configuration.
    #[cfg(any(target_os = "linux", feature = "fdb"))]
    pub fn new_fdb(url: SensitiveUrl, metrics_registry: &MetricsRegistry) -> Self {
        TimestampOracleConfig::Fdb(FdbTimestampOracleConfig::new(url, metrics_registry))
    }

    /// Returns the metrics for this configuration.
    pub fn metrics(&self) -> Arc<Metrics> {
        match self {
            TimestampOracleConfig::Postgres(config) => Arc::clone(config.metrics()),
            #[cfg(any(target_os = "linux", feature = "fdb"))]
            TimestampOracleConfig::Fdb(config) => Arc::clone(config.metrics()),
        }
    }

    /// Opens a timestamp oracle for the given timeline.
    pub async fn open(
        &self,
        timeline: String,
        initially: Timestamp,
        now_fn: NowFn,
        read_only: bool,
    ) -> Arc<dyn TimestampOracle<Timestamp> + Send + Sync> {
        match self {
            TimestampOracleConfig::Postgres(config) => Arc::new(
                PostgresTimestampOracle::open(
                    config.clone(),
                    timeline,
                    initially,
                    now_fn.clone(),
                    read_only,
                )
                .await,
            ),
            #[cfg(any(target_os = "linux", feature = "fdb"))]
            TimestampOracleConfig::Fdb(config) => {
                let fdb_oracle = FdbTimestampOracle::open(
                    config.clone(),
                    timeline,
                    initially,
                    now_fn,
                    read_only,
                )
                .await
                .expect("failed to open FdbTimestampOracle");
                Arc::new(fdb_oracle)
            }
        }
    }

    /// Returns all known timelines and their current timestamps.
    ///
    /// This is used during initialization to restore timestamp state from the backend.
    pub async fn get_all_timelines(&self) -> Result<Vec<(String, Timestamp)>, anyhow::Error> {
        match self {
            TimestampOracleConfig::Postgres(config) => {
                PostgresTimestampOracle::<NowFn>::get_all_timelines(config.clone()).await
            }
            #[cfg(any(target_os = "linux", feature = "fdb"))]
            TimestampOracleConfig::Fdb(config) => {
                FdbTimestampOracle::<NowFn>::get_all_timelines(config.clone()).await
            }
        }
    }

    /// Applies configuration parameters.
    ///
    /// This is a no-op for non-Postgres backends.
    pub fn apply_parameters(&self, params: TimestampOracleParameters) {
        // Only the Postgres oracle supports parameters for now.
        #[allow(irrefutable_let_patterns)]
        if let TimestampOracleConfig::Postgres(pg_config) = self {
            params.apply(pg_config)
        }
    }
}
