// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for all things storage.
//!
//! The structure of this module is designed to make adding new metrics as easy as possible. The
//! structure and naming conventions are as follows:
//!
//! Metrics for X end up in the `x.rs` submodule, unless X fits into one of the existing
//! submodules. The struct `XMetricsDefs` defines the `CounterVec/GaugeVec/etc`'s that must be
//! registered with the `MetricsRegistry` to create new metrics. `XMetricsDefs` should be a
//! sub-field of `StorageMetrics` (or recursively a sub-field). `XMetricsDefs` has a
//! `register_with` function to create it using a `MetricsRegistry`.
//!
//! `XMetrics` contains the actual gauges/counters/etc that are created using `XMetricsDefs`.
//! Typically these are created with `new` functions that takes a `&XMetricsDefs`, a `GlobalId`,
//! and a worker id, but sometimes more complex schemes are used, for metrics that are globally
//! shared, or have some other shape to their labels.
//!
//! `StorageMetrics` is the main entry-point to this module, and for convenience, typically
//! provides a `get_x_metrics` to obtain an `XMetrics` struct. This is to prevent users from
//! needing to interact with metrics _definitions_ into the code that actually bumps those
//! metrics.

use mz_ore::metric;
use mz_ore::metrics::{DeleteOnDropCounter, IntCounterVec, MetricsRegistry};
use mz_persist_client::ShardId;
use mz_repr::GlobalId;
use prometheus::core::AtomicU64;

use crate::statistics::{SinkStatisticsMetricDefs, SourceStatisticsMetricDefs};

pub mod decode;
pub mod sink;
pub mod source;
pub mod upsert;

/// Metrics for the cross-worker statistics merge that worker 0 performs in
/// `AggregatedStatistics`.
#[derive(Clone, Debug)]
pub(crate) struct StatisticsMergeMetricDefs {
    regressed_offset_known: IntCounterVec,
}

impl StatisticsMergeMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            regressed_offset_known: registry.register(metric!(
                name: "mz_source_regressed_offset_known",
                help: "The number of times offset_known reported by a worker went backwards, which an upstream restore or failover can cause.",
                var_labels: ["source_id", "worker_id", "shard_id"],
            )),
        }
    }
}

/// Per-source instance of [`StatisticsMergeMetricDefs`], with one series per worker.
#[derive(Debug)]
pub(crate) struct StatisticsMergeMetrics {
    regressed_offset_known: Vec<DeleteOnDropCounter<AtomicU64, Vec<String>>>,
}

impl StatisticsMergeMetrics {
    pub(crate) fn new(
        defs: &StatisticsMergeMetricDefs,
        id: GlobalId,
        shard_id: &ShardId,
        worker_count: usize,
    ) -> Self {
        let regressed_offset_known = (0..worker_count)
            .map(|worker_id| {
                defs.regressed_offset_known.get_delete_on_drop_metric(vec![
                    id.to_string(),
                    worker_id.to_string(),
                    shard_id.to_string(),
                ])
            })
            .collect();
        Self {
            regressed_offset_known,
        }
    }

    pub(crate) fn inc_regressed_offset_known(&self, worker_id: usize) {
        self.regressed_offset_known[worker_id].inc();
    }
}

/// A top-level struct holding all various _definitions_ of all metrics
/// use by the `mz-storage` crate.
///
/// Created by registering it with a `MetricsRegistry`, it also provides helpers
/// to obtain various _instantiated_ time-series, either per-worker, shared globally,
/// or some more specific labeling scheme.
///
/// This struct can be cloned, and the various definitions are shared.
#[derive(Clone, Debug)]
pub struct StorageMetrics {
    pub(crate) source_defs: source::SourceMetricDefs,
    pub(crate) decode_defs: decode::DecodeMetricDefs,
    pub(crate) upsert_defs: upsert::UpsertMetricDefs,
    pub(crate) upsert_backpressure_defs: upsert::UpsertBackpressureMetricDefs,
    pub(crate) sink_defs: sink::SinkMetricDefs,

    // Defined in the `statistics` module, as they are kept in sync with
    // user-facing data.
    pub(crate) source_statistics: SourceStatisticsMetricDefs,
    pub(crate) sink_statistics: SinkStatisticsMetricDefs,

    pub(crate) statistics_merge: StatisticsMergeMetricDefs,
}

impl StorageMetrics {
    /// Register all metrics with the `MetricsRegistry`.
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            source_defs: source::SourceMetricDefs::register_with(registry),
            decode_defs: decode::DecodeMetricDefs::register_with(registry),
            upsert_defs: upsert::UpsertMetricDefs::register_with(registry),
            upsert_backpressure_defs: upsert::UpsertBackpressureMetricDefs::register_with(registry),
            sink_defs: sink::SinkMetricDefs::register_with(registry),
            source_statistics: SourceStatisticsMetricDefs::register_with(registry),
            sink_statistics: SinkStatisticsMetricDefs::register_with(registry),
            statistics_merge: StatisticsMergeMetricDefs::register_with(registry),
        }
    }

    /// Get the backpressure series for the given id and worker id.
    pub(crate) fn get_backpressure_metrics(
        &self,
        id: GlobalId,
        index: usize,
    ) -> upsert::UpsertBackpressureMetrics {
        upsert::UpsertBackpressureMetrics::new(&self.upsert_backpressure_defs, id, index)
    }

    /// Get an `UpsertMetrics` for the given id and worker id (and optional
    /// `UpsertBackpressureMetrics`).
    pub(crate) fn get_upsert_metrics(
        &self,
        id: GlobalId,
        worker_id: usize,
        backpressure_metrics: Option<upsert::UpsertBackpressureMetrics>,
    ) -> upsert::UpsertMetrics {
        upsert::UpsertMetrics::new(&self.upsert_defs, id, worker_id, backpressure_metrics)
    }

    /// Get a `SourcePersistSinkMetrics` for the given configuration.
    pub(crate) fn get_source_persist_sink_metrics(
        &self,
        export_id: GlobalId,
        primary_source_id: GlobalId,
        worker_id: usize,
        data_shard: &mz_persist_client::ShardId,
    ) -> source::SourcePersistSinkMetrics {
        source::SourcePersistSinkMetrics::new(
            &self.source_defs.source_defs,
            export_id,
            primary_source_id,
            worker_id,
            data_shard,
        )
    }

    /// Get a `SourceMetrics` for the given id and worker id.
    pub(crate) fn get_source_metrics(
        &self,
        id: GlobalId,
        worker_id: usize,
    ) -> source::SourceMetrics {
        source::SourceMetrics::new(&self.source_defs.source_defs, id, worker_id)
    }

    /// Get a `PgSourceMetrics` for the given id.
    pub(crate) fn get_postgres_source_metrics(
        &self,
        id: GlobalId,
    ) -> source::postgres::PgSourceMetrics {
        source::postgres::PgSourceMetrics::new(&self.source_defs.postgres_defs, id)
    }

    /// Get a `MySqlSourceMetrics` for the given id.
    pub(crate) fn get_mysql_source_metrics(
        &self,
        id: GlobalId,
    ) -> source::mysql::MySqlSourceMetrics {
        source::mysql::MySqlSourceMetrics::new(&self.source_defs.mysql_defs, id)
    }

    /// Get a `SqlServerSourceMetrics` for the given id.
    pub(crate) fn get_sql_server_source_metrics(
        &self,
        source_id: GlobalId,
        worker_id: usize,
    ) -> source::sql_server::SqlServerSourceMetrics {
        source::sql_server::SqlServerSourceMetrics::new(
            &self.source_defs.sql_server_defs,
            source_id,
            worker_id,
        )
    }

    /// Get an `OffsetCommitMetrics` for the given id.
    pub(crate) fn get_offset_commit_metrics(&self, id: GlobalId) -> source::OffsetCommitMetrics {
        source::OffsetCommitMetrics::new(&self.source_defs.source_defs, id)
    }

    /// Get an `KafkaSourceMetrics` for the given configuration.
    pub(crate) fn get_kafka_source_metrics(
        &self,
        ids: Vec<i32>,
        topic: String,
        source_id: GlobalId,
    ) -> source::kafka::KafkaSourceMetrics {
        source::kafka::KafkaSourceMetrics::new(
            &self.source_defs.kafka_source_defs,
            ids,
            topic,
            source_id,
        )
    }

    /// Get an `KafkaSinkMetrics` for the given configuration.
    pub(crate) fn get_kafka_sink_metrics(
        &self,
        sink_id: GlobalId,
    ) -> sink::kafka::KafkaSinkMetrics {
        sink::kafka::KafkaSinkMetrics::new(&self.sink_defs.kafka_defs, sink_id)
    }

    /// Get an `IcebergSinkMetrics` for the given configuration.
    pub(crate) fn get_iceberg_sink_metrics(
        &self,
        sink_id: GlobalId,
        worker_id: usize,
    ) -> sink::iceberg::IcebergSinkMetrics {
        sink::iceberg::IcebergSinkMetrics::new(&self.sink_defs.iceberg_defs, sink_id, worker_id)
    }
}
