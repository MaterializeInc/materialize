// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus monitoring metrics.

use std::future::Future;
use std::time::Instant;

use mz_ore::metric;
use mz_ore::metrics::{Counter, IntCounter, MetricsRegistry, UIntGauge};
use prometheus::{CounterVec, IntCounterVec, IntGauge};

/// Prometheus monitoring metrics.
pub struct Metrics {
    pub(crate) data_shard_count: UIntGauge,

    pub(crate) register: FallibleOpMetrics,
    pub(crate) commit: FallibleOpMetrics,
    pub(crate) forget: FallibleOpMetrics,
    pub(crate) forget_all: FallibleOpMetrics,
    pub(crate) apply_le: InfallibleOpMetrics,
    pub(crate) compact_to: InfallibleOpMetrics,

    pub(crate) batches: BatchMetrics,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics").finish_non_exhaustive()
    }
}

impl Metrics {
    /// Returns a new [Metrics] instance connected to the given registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        let ops = OpsMetrics::new(registry);
        Metrics {
            data_shard_count: registry.register(metric!(
                name: "mz_txn_data_shard_count",
                help: "count of data shards registered to the txn set",
            )),
            register: ops.fallible("register"),
            commit: ops.fallible("commit"),
            forget: ops.fallible("forget"),
            forget_all: ops.fallible("forget_all"),
            apply_le: ops.infallible("apply_le"),
            compact_to: ops.infallible("compact_to"),
            batches: BatchMetrics::new(registry),
        }
    }
}

struct OpsMetrics {
    started_count: IntCounterVec,
    retry_count: IntCounterVec,
    succeeded_count: IntCounterVec,
    errored_count: IntCounterVec,
    duration_seconds: CounterVec,
}

impl OpsMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        OpsMetrics {
            started_count: registry.register(metric!(
                name: "mz_txn_op_started_count",
                help: "count of times a txn operation started",
                var_labels: ["op"],
            )),
            retry_count: registry.register(metric!(
                name: "mz_txn_op_retry_count",
                help: "count of times a txn operation retried",
                var_labels: ["op"],
            )),
            succeeded_count: registry.register(metric!(
                name: "mz_txn_op_succeeded_count",
                help: "count of times a txn operation succeeded",
                var_labels: ["op"],
            )),
            errored_count: registry.register(metric!(
                name: "mz_txn_op_errored_count",
                help: "count of times a txn operation errored",
                var_labels: ["op"],
            )),
            duration_seconds: registry.register(metric!(
                name: "mz_txn_op_duration_seconds",
                help: "time spent running a txn operation",
                var_labels: ["op"],
            )),
        }
    }

    fn fallible(&self, name: &str) -> FallibleOpMetrics {
        FallibleOpMetrics {
            started_count: self.started_count.with_label_values(&[name]),
            retry_count: self.retry_count.with_label_values(&[name]),
            succeeded_count: self.succeeded_count.with_label_values(&[name]),
            errored_count: self.errored_count.with_label_values(&[name]),
            duration_seconds: self.duration_seconds.with_label_values(&[name]),
        }
    }

    fn infallible(&self, name: &str) -> InfallibleOpMetrics {
        InfallibleOpMetrics {
            started_count: self.started_count.with_label_values(&[name]),
            succeeded_count: self.succeeded_count.with_label_values(&[name]),
            duration_seconds: self.duration_seconds.with_label_values(&[name]),
        }
    }
}

pub(crate) struct FallibleOpMetrics {
    started_count: IntCounter,
    pub(crate) retry_count: IntCounter,
    succeeded_count: IntCounter,
    errored_count: IntCounter,
    duration_seconds: Counter,
}

impl FallibleOpMetrics {
    pub(crate) async fn run<R, E>(&self, op: impl Future<Output = Result<R, E>>) -> Result<R, E> {
        let start = Instant::now();
        self.started_count.inc();
        let res = op.await;
        match res {
            Ok(_) => self.succeeded_count.inc(),
            Err(_) => self.errored_count.inc(),
        }
        self.duration_seconds.inc_by(start.elapsed().as_secs_f64());
        res
    }
}

pub(crate) struct InfallibleOpMetrics {
    started_count: IntCounter,
    succeeded_count: IntCounter,
    duration_seconds: Counter,
}

impl InfallibleOpMetrics {
    pub(crate) async fn run<R>(&self, op: impl Future<Output = R>) -> R {
        let start = Instant::now();
        self.started_count.inc();
        let res = op.await;
        self.succeeded_count.inc();
        self.duration_seconds.inc_by(start.elapsed().as_secs_f64());
        res
    }
}

pub(crate) struct BatchMetrics {
    pub(crate) commit_count: IntCounter,
    pub(crate) commit_bytes: IntCounter,
    pub(crate) unapplied_count: UIntGauge,
    pub(crate) unapplied_bytes: UIntGauge,
    pub(crate) unapplied_min_ts: IntGauge,
}

impl BatchMetrics {
    fn new(registry: &MetricsRegistry) -> Self {
        BatchMetrics {
            commit_count: registry.register(metric!(
                name: "mz_txn_batch_commit_count",
                help: "count of batches committed via txn",
            )),
            commit_bytes: registry.register(metric!(
                name: "mz_txn_batch_commit_bytes",
                help: "total bytes committed via txn",
            )),
            unapplied_count: registry.register(metric!(
                name: "mz_txn_batch_unapplied_count",
                help: "count of batches committed via txn but not yet applied",
            )),
            unapplied_bytes: registry.register(metric!(
                name: "mz_txn_batch_unapplied_bytes",
                help: "total bytes committed via txn but not yet applied",
            )),
            unapplied_min_ts: registry.register(metric!(
                name: "mz_txn_batch_unapplied_min_ts",
                help: "minimum ts of txn committed via txn but not yet applied",
            )),
        }
    }
}
