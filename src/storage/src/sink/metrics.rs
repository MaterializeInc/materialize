// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics that sinks report.

use mz_ore::{
    metric,
    metrics::{IntCounterVec, MetricsRegistry, UIntGaugeVec},
};

/// Metrics reported by each kafka sink.
#[derive(Clone)]
pub struct KafkaBaseMetrics {
    pub(crate) messages_sent_counter: IntCounterVec,
    pub(crate) message_send_errors_counter: IntCounterVec,
    pub(crate) message_delivery_errors_counter: IntCounterVec,
    pub(crate) rows_queued: UIntGaugeVec,
}

impl KafkaBaseMetrics {
    pub fn register_with(registry: &MetricsRegistry) -> Self {
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

/// TODO(undocumented)
#[derive(Clone)]
pub struct SinkBaseMetrics {
    pub(crate) kafka: KafkaBaseMetrics,
}

impl SinkBaseMetrics {
    /// TODO(undocumented)
    pub fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            kafka: KafkaBaseMetrics::register_with(registry),
        }
    }
}
