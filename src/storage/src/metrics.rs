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

use std::sync::Arc;

use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::GlobalId;
use prometheus::{Histogram, HistogramVec};

use crate::statistics::{SinkStatisticsMetricDefs, SourceStatisticsMetricDefs};
use mz_storage_operators::metrics::BackpressureMetrics;

pub mod decode;
pub mod sink;
pub mod source;
pub mod upsert;

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

    // Timings.
    //
    // Note that this particular metric unfortunately takes some care to
    // interpret. It measures the duration of step_or_park calls, which
    // undesirably includes the parking. This is probably fine because we
    // regularly send progress information through persist sources, which likely
    // means the parking is capped at a second or two in practice. It also
    // doesn't do anything to let you pinpoint _which_ operator or worker isn't
    // yielding, but it should hopefully alert us when there is something to
    // look at.
    //
    // This mirrors an equivalent metric in the compute server
    // (`mz_compute::metrics::ComputeMetrics`).
    timely_step_duration_seconds: HistogramVec,
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
            // NB: This metric must have the same name, help text, and label
            // names as the equivalent metric in `mz_compute::metrics`, as
            // Prometheus requires identical metadata for metrics with the same
            // name.
            timely_step_duration_seconds: registry.register(metric!(
                name: "mz_timely_step_duration_seconds",
                help: "The time spent in each step_or_park call",
                const_labels: {"cluster" => "storage"},
                var_labels: ["worker_id"],
                buckets: mz_ore::stats::histogram_seconds_buckets(0.000_128, 32.0),
            )),
        }
    }

    /// Get per-worker metrics for the given worker.
    pub(crate) fn for_worker(&self, worker_id: usize) -> StorageWorkerMetrics {
        let worker = worker_id.to_string();
        StorageWorkerMetrics {
            timely_step_duration_seconds: self
                .timely_step_duration_seconds
                .with_label_values(&[&worker]),
        }
    }

    /// Get a `BackpressureMetrics` for the given id and worker id.
    pub(crate) fn get_backpressure_metrics(
        &self,
        id: GlobalId,
        index: usize,
    ) -> BackpressureMetrics {
        BackpressureMetrics {
            emitted_bytes: Arc::new(
                self.upsert_backpressure_defs
                    .emitted_bytes
                    .get_delete_on_drop_metric(vec![id.to_string(), index.to_string()]),
            ),
            last_backpressured_bytes: Arc::new(
                self.upsert_backpressure_defs
                    .last_backpressured_bytes
                    .get_delete_on_drop_metric(vec![id.to_string(), index.to_string()]),
            ),
            retired_bytes: Arc::new(
                self.upsert_backpressure_defs
                    .retired_bytes
                    .get_delete_on_drop_metric(vec![id.to_string(), index.to_string()]),
            ),
        }
    }

    /// Get an `UpsertMetrics` for the given id and worker id (and optional `BackpressureMetrics`).
    pub(crate) fn get_upsert_metrics(
        &self,
        id: GlobalId,
        worker_id: usize,
        backpressure_metrics: Option<BackpressureMetrics>,
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

/// Per-worker metrics for storage.
#[derive(Clone, Debug)]
pub struct StorageWorkerMetrics {
    /// Histogram of Timely step timings.
    pub(crate) timely_step_duration_seconds: Histogram,
}
