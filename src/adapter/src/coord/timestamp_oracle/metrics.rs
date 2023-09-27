// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus monitoring metrics.

use std::time::Instant;

use mz_ore::metric;
use mz_ore::metrics::{Counter, IntCounter, MetricsRegistry};
use mz_postgres_client::metrics::PostgresClientMetrics;
use prometheus::{CounterVec, IntCounterVec};

/// Prometheus monitoring metrics for timestamp oracles.
///
/// Intentionally not Clone because we expect this to be passed around in an
/// Arc.
pub struct Metrics {
    _vecs: MetricsVecs,

    /// Metrics for
    /// [`TimestampOracle`](crate::coord::timestamp_oracle::TimestampOracle).
    pub oracle: OracleMetrics,

    /// Metrics for [`PostgresClient`](mz_postgres_client::PostgresClient).
    pub postgres_client: PostgresClientMetrics,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Metrics {
    /// Returns a new [Metrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        let vecs = MetricsVecs::new(registry);

        Metrics {
            oracle: vecs.oracle_metrics(),
            postgres_client: PostgresClientMetrics::new(registry, "mz_ts_oracle"),
            _vecs: vecs,
        }
    }
}

#[derive(Debug)]
struct MetricsVecs {
    external_op_started: IntCounterVec,
    external_op_succeeded: IntCounterVec,
    _external_op_failed: IntCounterVec,
    external_op_seconds: CounterVec,
}

impl MetricsVecs {
    fn new(registry: &MetricsRegistry) -> Self {
        MetricsVecs {
            external_op_started: registry.register(metric!(
                name: "mz_ts_oracle_started_count",
                help: "count of oracle operations started",
                var_labels: ["op"],
            )),
            external_op_succeeded: registry.register(metric!(
                name: "mz_ts_oracle_succeeded_count",
                help: "count of oracle operations succeeded",
                var_labels: ["op"],
            )),
            _external_op_failed: registry.register(metric!(
                name: "mz_ts_oracle_failed_count",
                help: "count of oracle operations failed",
                var_labels: ["op"],
            )),
            external_op_seconds: registry.register(metric!(
                name: "mz_ts_oracle_seconds",
                help: "time spent in oracle operations",
                var_labels: ["op"],
            )),
        }
    }

    fn oracle_metrics(&self) -> OracleMetrics {
        OracleMetrics {
            write_ts: self.external_op_metrics("write_ts"),
            peek_write_ts: self.external_op_metrics("peek_write_ts"),
            read_ts: self.external_op_metrics("read_ts"),
            apply_write: self.external_op_metrics("apply_write"),
        }
    }

    fn external_op_metrics(&self, op: &str) -> ExternalOpMetrics {
        ExternalOpMetrics {
            started: self.external_op_started.with_label_values(&[op]),
            succeeded: self.external_op_succeeded.with_label_values(&[op]),
            seconds: self.external_op_seconds.with_label_values(&[op]),
        }
    }
}

#[derive(Debug)]
pub struct ExternalOpMetrics {
    started: IntCounter,
    succeeded: IntCounter,
    seconds: Counter,
}

impl ExternalOpMetrics {
    pub(crate) async fn run_op<R, F, OpFn>(&self, op_fn: OpFn) -> Result<R, anyhow::Error>
    where
        F: std::future::Future<Output = Result<R, anyhow::Error>>,
        OpFn: FnOnce() -> F,
    {
        self.started.inc();
        let start = Instant::now();
        let res = op_fn().await;
        let elapsed_seconds = start.elapsed().as_secs_f64();
        self.seconds.inc_by(elapsed_seconds);
        match res.as_ref() {
            Ok(_) => self.succeeded.inc(),
            Err(_err) => {
                // Not recording failures for now because the timestamp oracle
                // interface is infallible, so it wouldn't work with the
                // approach where we wrap it in a MetricsTimestampOracle.
            }
        };
        res
    }
}

#[derive(Debug)]
pub struct OracleMetrics {
    pub write_ts: ExternalOpMetrics,
    pub peek_write_ts: ExternalOpMetrics,
    pub read_ts: ExternalOpMetrics,
    pub apply_write: ExternalOpMetrics,
}
