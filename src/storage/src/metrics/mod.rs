// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for all things storage.

use std::sync::Arc;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::metrics::{CounterVecExt, GaugeVecExt};
use mz_repr::GlobalId;

use crate::statistics::{SinkStatisticsMetricDefs, SourceStatisticsMetricDefs};
use mz_storage_operators::metrics::BackpressureMetrics;

pub mod decode;
pub mod kafka;
pub mod postgres;
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
    pub(crate) decode_defs: decode::DecodeMetricDefs,
    pub(crate) source_defs: source::SourceMetricDefs,
    pub(crate) sink_defs: sink::SinkMetricDefs,

    // Defined in the `statistics` module, as they are kept in sync with
    // user-facing data.
    pub(crate) source_statistics: SourceStatisticsMetricDefs,
    pub(crate) sink_statistics: SinkStatisticsMetricDefs,
}

impl StorageMetrics {
    /// Register all metrics with the `MetricsRegistry`.
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            decode_defs: decode::DecodeMetricDefs::register_with(registry),
            source_defs: source::SourceMetricDefs::register_with(registry),
            sink_defs: sink::SinkMetricDefs::register_with(registry),
            source_statistics: SourceStatisticsMetricDefs::register_with(registry),
            sink_statistics: SinkStatisticsMetricDefs::register_with(registry),
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
                self.source_defs
                    .upsert_backpressure_defs
                    .emitted_bytes
                    .get_delete_on_drop_counter(vec![id.to_string(), index.to_string()]),
            ),
            last_backpressured_bytes: Arc::new(
                self.source_defs
                    .upsert_backpressure_defs
                    .last_backpressured_bytes
                    .get_delete_on_drop_gauge(vec![id.to_string(), index.to_string()]),
            ),
            retired_bytes: Arc::new(
                self.source_defs
                    .upsert_backpressure_defs
                    .retired_bytes
                    .get_delete_on_drop_counter(vec![id.to_string(), index.to_string()]),
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
        upsert::UpsertMetrics::new(
            &self.source_defs.upsert_defs,
            id,
            worker_id,
            backpressure_metrics,
        )
    }

    /// Get a `SourcePersistSinkMetrics` for the given configuration.
    pub(crate) fn get_source_persist_sink_metrics(
        &self,
        export_id: GlobalId,
        primary_source_id: GlobalId,
        worker_id: usize,
        data_shard: &mz_persist_client::ShardId,
        output_index: usize,
    ) -> source::SourcePersistSinkMetrics {
        source::SourcePersistSinkMetrics::new(
            &self.source_defs.source_defs,
            export_id,
            primary_source_id,
            worker_id,
            data_shard,
            output_index,
        )
    }

    /// Get a `SinkMetrics` for the given connection, id, and worker id.
    pub(crate) fn get_sink_metrics(
        &self,
        topic_name: &str,
        id: GlobalId,
        worker_id: usize,
    ) -> sink::SinkMetrics {
        sink::SinkMetrics::new(&self.sink_defs, topic_name, id, worker_id)
    }

    /// Get a `SourceMetrics` for the given id and worker id.
    pub(crate) fn get_source_metrics(
        &self,
        name: &str,
        id: GlobalId,
        worker_id: usize,
    ) -> source::SourceMetrics {
        source::SourceMetrics::new(&self.source_defs.source_defs, name, id, worker_id)
    }

    /// Get a `PgMetrics` for the given id.
    pub(crate) fn get_postgres_metrics(&self, id: GlobalId) -> postgres::PgSourceMetrics {
        postgres::PgSourceMetrics::new(&self.source_defs.postgres_defs, id)
    }

    /// Get an `OffsetCommitMetrics` for the given id.
    pub(crate) fn get_offset_commit_metrics(&self, id: GlobalId) -> source::OffsetCommitMetrics {
        source::OffsetCommitMetrics::new(&self.source_defs.source_defs, id)
    }

    /// Get an `KafkaPartitionMetrics` for the given configuration.
    pub(crate) fn get_kafka_partition_metrics(
        &self,
        ids: Vec<i32>,
        topic: String,
        source_id: GlobalId,
    ) -> kafka::KafkaPartitionMetrics {
        kafka::KafkaPartitionMetrics::new(
            &self.source_defs.kafka_partition_defs,
            ids,
            topic,
            source_id,
        )
    }
}
