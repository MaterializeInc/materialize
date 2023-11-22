// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus monitoring metrics.

use std::time::{Duration, Instant};

use mz_ore::metric;
use mz_ore::metrics::{Counter, IntCounter, MetricsRegistry};
use mz_postgres_client::metrics::PostgresClientMetrics;
use prometheus::{CounterVec, IntCounterVec};

use crate::coord::timestamp_oracle::retry::RetryStream;

/// Prometheus monitoring metrics for timestamp oracles.
///
/// Intentionally not Clone because we expect this to be passed around in an
/// Arc.
pub struct Metrics {
    _vecs: MetricsVecs,

    /// Metrics for
    /// [`TimestampOracle`](crate::coord::timestamp_oracle::TimestampOracle).
    pub oracle: OracleMetrics,

    /// Metrics for each retry loop.
    pub retries: RetriesMetrics,

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
            retries: vecs.retries_metrics(),
            postgres_client: PostgresClientMetrics::new(registry, "mz_ts_oracle"),
            _vecs: vecs,
        }
    }
}

#[derive(Debug)]
struct MetricsVecs {
    external_op_started: IntCounterVec,
    external_op_succeeded: IntCounterVec,
    external_op_failed: IntCounterVec,
    external_op_seconds: CounterVec,

    retry_started: IntCounterVec,
    retry_finished: IntCounterVec,
    retry_retries: IntCounterVec,
    retry_sleep_seconds: CounterVec,
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
            external_op_failed: registry.register(metric!(
                name: "mz_ts_oracle_failed_count",
                help: "count of oracle operations failed",
                var_labels: ["op"],
            )),
            external_op_seconds: registry.register(metric!(
                name: "mz_ts_oracle_seconds",
                help: "time spent in oracle operations",
                var_labels: ["op"],
            )),

            retry_started: registry.register(metric!(
                name: "mz_ts_oracle_retry_started_count",
                help: "count of retry loops started",
                var_labels: ["op"],
            )),
            retry_finished: registry.register(metric!(
                name: "mz_ts_oracle_retry_finished_count",
                help: "count of retry loops finished",
                var_labels: ["op"],
            )),
            retry_retries: registry.register(metric!(
                name: "mz_ts_oracle_retry_retries_count",
                help: "count of total attempts by retry loops",
                var_labels: ["op"],
            )),
            retry_sleep_seconds: registry.register(metric!(
                name: "mz_ts_oracle_retry_sleep_seconds",
                help: "time spent in retry loop backoff",
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
            failed: self.external_op_failed.with_label_values(&[op]),
            seconds: self.external_op_seconds.with_label_values(&[op]),
        }
    }

    fn retries_metrics(&self) -> RetriesMetrics {
        RetriesMetrics {
            open: self.retry_metrics("open"),
            get_all_timelines: self.retry_metrics("get_all_timelines"),
            write_ts: self.retry_metrics("write_ts"),
            peek_write_ts: self.retry_metrics("peek_write_ts"),
            read_ts: self.retry_metrics("read_ts"),
            apply_write: self.retry_metrics("apply_write"),
        }
    }

    fn retry_metrics(&self, name: &str) -> RetryMetrics {
        RetryMetrics {
            name: name.to_owned(),
            started: self.retry_started.with_label_values(&[name]),
            finished: self.retry_finished.with_label_values(&[name]),
            retries: self.retry_retries.with_label_values(&[name]),
            sleep_seconds: self.retry_sleep_seconds.with_label_values(&[name]),
        }
    }
}

#[derive(Debug)]
pub struct ExternalOpMetrics {
    started: IntCounter,
    succeeded: IntCounter,
    failed: IntCounter,
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
                self.failed.inc();
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

#[derive(Debug)]
pub struct RetryMetrics {
    pub(crate) name: String,
    pub(crate) started: IntCounter,
    pub(crate) finished: IntCounter,
    pub(crate) retries: IntCounter,
    pub(crate) sleep_seconds: Counter,
}

impl RetryMetrics {
    pub(crate) fn stream(&self, retry: RetryStream) -> MetricsRetryStream {
        MetricsRetryStream::new(retry, self)
    }
}

#[derive(Debug)]
pub struct RetriesMetrics {
    pub(crate) open: RetryMetrics,
    pub(crate) get_all_timelines: RetryMetrics,
    pub(crate) write_ts: RetryMetrics,
    pub(crate) peek_write_ts: RetryMetrics,
    pub(crate) read_ts: RetryMetrics,
    pub(crate) apply_write: RetryMetrics,
}

struct IncOnDrop(IntCounter);

impl Drop for IncOnDrop {
    fn drop(&mut self) {
        self.0.inc()
    }
}

pub struct MetricsRetryStream {
    retry: RetryStream,
    pub(crate) retries: IntCounter,
    sleep_seconds: Counter,
    _finished: IncOnDrop,
}

impl MetricsRetryStream {
    pub fn new(retry: RetryStream, metrics: &RetryMetrics) -> Self {
        metrics.started.inc();
        MetricsRetryStream {
            retry,
            retries: metrics.retries.clone(),
            sleep_seconds: metrics.sleep_seconds.clone(),
            _finished: IncOnDrop(metrics.finished.clone()),
        }
    }

    /// How many times [Self::sleep] has been called.
    pub fn attempt(&self) -> usize {
        self.retry.attempt()
    }

    /// The next sleep (without jitter for easy printing in logs).
    pub fn next_sleep(&self) -> Duration {
        self.retry.next_sleep()
    }

    /// Executes the next sleep in the series.
    ///
    /// This isn't cancel-safe, so it consumes and returns self, to prevent
    /// accidental mis-use.
    pub async fn sleep(self) -> Self {
        self.retries.inc();
        self.sleep_seconds
            .inc_by(self.retry.next_sleep().as_secs_f64());
        let retry = self.retry.sleep().await;
        MetricsRetryStream {
            retry,
            retries: self.retries,
            sleep_seconds: self.sleep_seconds,
            _finished: self._finished,
        }
    }
}
