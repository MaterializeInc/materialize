// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for kafka sinks.

use mz_ore::metric;
use mz_ore::metrics::{DeleteOnDropGauge, IntGaugeVec, MetricsRegistry, UIntGaugeVec};
use mz_repr::GlobalId;
use prometheus::core::{AtomicI64, AtomicU64};

/// Definitions for librdkafka produced metrics used in sinks.
#[derive(Clone, Debug)]
pub(crate) struct KafkaSinkMetricDefs {
    /// The current number of messages in producer queues.
    pub rdkafka_msg_cnt: UIntGaugeVec,
    /// The current total size of messages in producer queues.
    pub rdkafka_msg_size: UIntGaugeVec,
    /// The total number of messages transmitted (produced) to brokers.
    pub rdkafka_txmsgs: IntGaugeVec,
    /// The total number of bytes transmitted (produced) to brokers.
    pub rdkafka_txmsg_bytes: IntGaugeVec,
    /// The total number of requests sent to brokers.
    pub rdkafka_tx: IntGaugeVec,
    /// The total number of bytes transmitted to brokers.
    pub rdkafka_tx_bytes: IntGaugeVec,
    /// The number of requests awaiting transmission across all brokers.
    pub rdkafka_outbuf_cnt: IntGaugeVec,
    /// The number of messages awaiting transmission across all brokers.
    pub rdkafka_outbuf_msg_cnt: IntGaugeVec,
    /// The number of requests in-flight across all brokers that are awaiting a response.
    pub rdkafka_waitresp_cnt: IntGaugeVec,
    /// The number of messages in-flight across all brokers that are awaiting a response.
    pub rdkafka_waitresp_msg_cnt: IntGaugeVec,
    /// The total number of transmission errors across all brokers.
    pub rdkafka_txerrs: UIntGaugeVec,
    /// The total number of request retries across all brokers.
    pub rdkafka_txretries: UIntGaugeVec,
    /// The total number of requests that timed out across all brokers.
    pub rdkafka_req_timeouts: UIntGaugeVec,
    /// The number of connection attempts, including successful and failed attempts, and name
    /// resolution failures across all brokers.
    pub rdkafka_connects: IntGaugeVec,
    /// The number of disconnections, whether triggered by the broker, the network, the load
    /// balancer, or something else across all brokers.
    pub rdkafka_disconnects: IntGaugeVec,
    /// The number of outstanding progress records that need to be read before the sink can resume.
    pub outstanding_progress_records: UIntGaugeVec,
    /// The number of progress records consumed while resuming the sink.
    pub consumed_progress_records: UIntGaugeVec,
    /// The number of partitions this sink is publishing to.
    pub partition_count: UIntGaugeVec,
}

impl KafkaSinkMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            rdkafka_msg_cnt: registry.register(metric!(
                name: "mz_sink_rdkafka_msg_cnt",
                help: "The current number of messages in producer queues.",
                var_labels: ["sink_id"],
            )),
            rdkafka_msg_size: registry.register(metric!(
                name: "mz_sink_rdkafka_msg_size",
                help: "The current total size of messages in producer queues.",
                var_labels: ["sink_id"],
            )),
            rdkafka_txmsgs: registry.register(metric!(
                name: "mz_sink_rdkafka_txmsgs",
                help: "The total number of messages transmitted (produced) to brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_txmsg_bytes: registry.register(metric!(
                name: "mz_sink_rdkafka_txmsg_bytes",
                help: "The total number of bytes transmitted (produced) to brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_tx: registry.register(metric!(
                name: "mz_sink_rdkafka_tx",
                help: "The total number of requests sent to brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_tx_bytes: registry.register(metric!(
                name: "mz_sink_rdkafka_tx_bytes",
                help: "The total number of bytes transmitted to brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_outbuf_cnt: registry.register(metric!(
                name: "mz_sink_rdkafka_outbuf_cnt",
                help: "The number of requests awaiting transmission across all brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_outbuf_msg_cnt: registry.register(metric!(
                name: "mz_sink_rdkafka_outbuf_msg_cnt",
                help: "The number of messages awaiting transmission across all brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_waitresp_cnt: registry.register(metric!(
                name: "mz_sink_rdkafka_waitresp_cnt",
                help: "The number of requests in-flight across all brokers that are awaiting a \
                       response.",
                var_labels: ["sink_id"],
            )),
            rdkafka_waitresp_msg_cnt: registry.register(metric!(
                name: "mz_sink_rdkafka_waitresp_msg_cnt",
                help: "The number of messages in-flight across all brokers that are awaiting a \
                       response.",
                var_labels: ["sink_id"],
            )),
            rdkafka_txerrs: registry.register(metric!(
                name: "mz_sink_rdkafka_txerrs",
                help: "The total number of transmission errors across all brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_txretries: registry.register(metric!(
                name: "mz_sink_rdkafka_txretries",
                help: "The total number of request retries across all brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_req_timeouts: registry.register(metric!(
                name: "mz_sink_rdkafka_req_timeouts",
                help: "The total number of requests that timed out across all brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_connects: registry.register(metric!(
                name: "mz_sink_rdkafka_connects",
                help: "The number of connection attempts, including successful and failed \
                       attempts, and name resolution failures across all brokers.",
                var_labels: ["sink_id"],
            )),
            rdkafka_disconnects: registry.register(metric!(
                name: "mz_sink_rdkafka_disconnects",
                help: "The number of disconnections, whether triggered by the broker, the \
                      network, the load balancer, or something else across all brokers.",
                var_labels: ["sink_id"],
            )),
            outstanding_progress_records: registry.register(metric!(
                name: "mz_sink_oustanding_progress_records",
                help: "The number of outstanding progress records that need to be read before the sink can resume.",
                var_labels: ["sink_id"],
            )),
            consumed_progress_records: registry.register(metric!(
                name: "mz_sink_consumed_progress_records",
                help: "The number of progress records consumed by the sink.",
                var_labels: ["sink_id"],
            )),
            partition_count: registry.register(metric!(
                name: "mz_sink_partition_count",
                help: "The number of partitions this sink is publishing to.",
                var_labels: ["sink_id"],
            )),
        }
    }
}

/// Metrics reported by librdkafka
pub(crate) struct KafkaSinkMetrics {
    /// The current number of messages in producer queues.
    pub rdkafka_msg_cnt: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The current total size of messages in producer queues.
    pub rdkafka_msg_size: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The total number of messages transmitted (produced) to brokers.
    pub rdkafka_txmsgs: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The total number of bytes transmitted (produced) to brokers.
    pub rdkafka_txmsg_bytes: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The total number of requests sent to brokers.
    pub rdkafka_tx: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The total number of bytes transmitted to brokers.
    pub rdkafka_tx_bytes: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The number of requests awaiting transmission across all brokers.
    pub rdkafka_outbuf_cnt: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The number of messages awaiting transmission across all brokers.
    pub rdkafka_outbuf_msg_cnt: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The number of requests in-flight across all brokers that are awaiting a response.
    pub rdkafka_waitresp_cnt: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The number of messages in-flight across all brokers that are awaiting a response.
    pub rdkafka_waitresp_msg_cnt: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The total number of transmission errors across all brokers.
    pub rdkafka_txerrs: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The total number of request retries across all brokers.
    pub rdkafka_txretries: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The total number of requests that timed out across all brokers.
    pub rdkafka_req_timeouts: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The number of connection attempts, including successful and failed attempts, and name
    /// resolution failures across all brokers.
    pub rdkafka_connects: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The number of disconnections, whether triggered by the broker, the network, the load
    /// balancer, or something else across all brokers.
    pub rdkafka_disconnects: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
    /// The number of outstanding progress records that need to be read before the sink can resume.
    pub outstanding_progress_records: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The number of progress records consumed while resuming the sink.
    pub consumed_progress_records: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
    /// The number of partitions this sink is publishing to.
    pub partition_count: DeleteOnDropGauge<'static, AtomicU64, Vec<String>>,
}

impl KafkaSinkMetrics {
    /// Initializes source metrics for a given (source_id, worker_id)
    pub fn new(defs: &KafkaSinkMetricDefs, sink_id: GlobalId) -> Self {
        let labels = &[sink_id.to_string()];
        Self {
            rdkafka_msg_cnt: defs
                .rdkafka_msg_cnt
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_msg_size: defs
                .rdkafka_msg_size
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_txmsgs: defs
                .rdkafka_txmsgs
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_txmsg_bytes: defs
                .rdkafka_txmsg_bytes
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_tx: defs.rdkafka_tx.get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_tx_bytes: defs
                .rdkafka_tx_bytes
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_outbuf_cnt: defs
                .rdkafka_outbuf_cnt
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_outbuf_msg_cnt: defs
                .rdkafka_outbuf_msg_cnt
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_waitresp_cnt: defs
                .rdkafka_waitresp_cnt
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_waitresp_msg_cnt: defs
                .rdkafka_waitresp_msg_cnt
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_txerrs: defs
                .rdkafka_txerrs
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_txretries: defs
                .rdkafka_txretries
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_req_timeouts: defs
                .rdkafka_req_timeouts
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_connects: defs
                .rdkafka_connects
                .get_delete_on_drop_metric(labels.to_vec()),
            rdkafka_disconnects: defs
                .rdkafka_disconnects
                .get_delete_on_drop_metric(labels.to_vec()),
            outstanding_progress_records: defs
                .outstanding_progress_records
                .get_delete_on_drop_metric(labels.to_vec()),
            consumed_progress_records: defs
                .consumed_progress_records
                .get_delete_on_drop_metric(labels.to_vec()),
            partition_count: defs
                .partition_count
                .get_delete_on_drop_metric(labels.to_vec()),
        }
    }
}
