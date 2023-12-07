// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics that sinks report.

use mz_ore::metric;
use mz_ore::metrics::{
    CounterVecExt, DeleteOnDropCounter, DeleteOnDropGauge, GaugeVecExt, IntCounterVec,
    MetricsRegistry, UIntGaugeVec,
};
use mz_repr::GlobalId;
use prometheus::core::AtomicU64;

/// Definitions for metrics reported by each kafka sink.
#[derive(Clone, Debug)]
pub struct SinkMetricDefs {
    pub(crate) messages_sent_counter: IntCounterVec,
    pub(crate) message_send_errors_counter: IntCounterVec,
    pub(crate) message_delivery_errors_counter: IntCounterVec,
    pub(crate) rows_queued: UIntGaugeVec,
}

impl SinkMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            messages_sent_counter: registry.register(metric!(
                name: "mz_kafka_messages_sent_total",
                help: "The number of messages the Kafka producer successfully sent for this sink",
                var_labels: ["topic", "sink_id", "worker_id"],
            )),
            message_send_errors_counter: registry.register(metric!(
                name: "mz_kafka_message_send_errors_total",
                help: "The number of times the Kafka producer encountered an error on send",
                var_labels: ["topic", "sink_id", "worker_id"],
            )),
            message_delivery_errors_counter: registry.register(metric!(
                name: "mz_kafka_message_delivery_errors_total",
                help: "The number of messages that the Kafka producer could not deliver to the topic",
                var_labels: ["topic", "sink_id", "worker_id"],
            )),
            rows_queued: registry.register(metric!(
                name: "mz_kafka_sink_rows_queued",
                help: "The current number of rows queued by the Kafka sink operator (note that one row can generate multiple Kafka messages)",
                var_labels: ["topic", "sink_id", "worker_id"],
            )),
        }
    }
}

/// Per-Kafka sink metrics.
pub(crate) struct SinkMetrics {
    pub(crate) messages_sent_counter: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) message_send_errors_counter: DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) message_delivery_errors_counter:
        DeleteOnDropCounter<'static, AtomicU64, Vec<String>>,
    pub(crate) rows_queued: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl SinkMetrics {
    /// Create a `SinkMetrics` from the `SinkMetricDefs`.
    pub(crate) fn new(
        base: &SinkMetricDefs,
        topic_name: &str,
        sink_id: GlobalId,
        worker_id: usize,
    ) -> SinkMetrics {
        let labels = vec![
            topic_name.to_string(),
            sink_id.to_string(),
            worker_id.to_string(),
        ];
        SinkMetrics {
            messages_sent_counter: base
                .messages_sent_counter
                .get_delete_on_drop_counter(labels.clone()),
            message_send_errors_counter: base
                .message_send_errors_counter
                .get_delete_on_drop_counter(labels.clone()),
            message_delivery_errors_counter: base
                .message_delivery_errors_counter
                .get_delete_on_drop_counter(labels.clone()),
            rows_queued: base.rows_queued.get_delete_on_drop_gauge(labels),
        }
    }
}
