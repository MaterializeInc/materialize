// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for Kafka consumption.

use std::collections::BTreeMap;

use mz_ore::cast::CastLossy;
use mz_ore::iter::IteratorExt;
use mz_ore::metric;
use mz_ore::metrics::{
    DeregisterOnDropCounter, DeregisterOnDropGauge, DeregisterOnDropVec, MetricsRegistry, raw,
};
use mz_repr::GlobalId;
use prometheus::core::{AtomicF64, AtomicI64, AtomicU64};
use rdkafka::statistics::Statistics;
use tracing::debug;

/// Definitions for the metrics derived from the librdkafka statistics of Kafka sources.
#[derive(Clone, Debug)]
pub(crate) struct KafkaSourceMetricDefs {
    pub(crate) partition_offset_max: DeregisterOnDropVec<raw::IntGaugeVec>,
    rdkafka_tx: DeregisterOnDropVec<raw::IntGaugeVec>,
    rdkafka_rx: DeregisterOnDropVec<raw::IntGaugeVec>,
    rdkafka_rx_bytes: DeregisterOnDropVec<raw::IntGaugeVec>,
    rdkafka_outbuf_cnt: DeregisterOnDropVec<raw::IntGaugeVec>,
    rdkafka_waitresp_cnt: DeregisterOnDropVec<raw::IntGaugeVec>,
    rdkafka_txretries: DeregisterOnDropVec<raw::UIntGaugeVec>,
    rdkafka_req_timeouts: DeregisterOnDropVec<raw::UIntGaugeVec>,
    rdkafka_connects: DeregisterOnDropVec<raw::IntGaugeVec>,
    rdkafka_disconnects: DeregisterOnDropVec<raw::IntGaugeVec>,
    rdkafka_broker_rtt_seconds_total: DeregisterOnDropVec<raw::CounterVec>,
    rdkafka_broker_rtt_requests_total: DeregisterOnDropVec<raw::IntCounterVec>,
}

impl KafkaSourceMetricDefs {
    pub(crate) fn register_with(registry: &MetricsRegistry) -> Self {
        Self {
            partition_offset_max: registry.register(metric!(
                name: "mz_kafka_partition_offset_max",
                help: "High watermark offset on broker for partition",
                var_labels: ["topic", "source_id", "partition_id"],
            )),
            rdkafka_tx: registry.register(metric!(
                name: "mz_source_rdkafka_tx",
                help: "The total number of requests sent to brokers.",
                var_labels: ["source_id", "worker_id", "client"],
            )),
            rdkafka_rx: registry.register(metric!(
                name: "mz_source_rdkafka_rx",
                help: "The total number of responses received from brokers.",
                var_labels: ["source_id", "worker_id", "client"],
            )),
            rdkafka_rx_bytes: registry.register(metric!(
                name: "mz_source_rdkafka_rx_bytes",
                help: "The total number of bytes received from brokers.",
                var_labels: ["source_id", "worker_id", "client"],
            )),
            rdkafka_outbuf_cnt: registry.register(metric!(
                name: "mz_source_rdkafka_outbuf_cnt",
                help: "The number of requests awaiting transmission across all brokers.",
                var_labels: ["source_id", "worker_id", "client"],
            )),
            rdkafka_waitresp_cnt: registry.register(metric!(
                name: "mz_source_rdkafka_waitresp_cnt",
                help: "The number of requests in-flight across all brokers that are awaiting a \
                       response.",
                var_labels: ["source_id", "worker_id", "client"],
            )),
            rdkafka_txretries: registry.register(metric!(
                name: "mz_source_rdkafka_txretries",
                help: "The total number of request retries across all brokers.",
                var_labels: ["source_id", "worker_id", "client"],
            )),
            rdkafka_req_timeouts: registry.register(metric!(
                name: "mz_source_rdkafka_req_timeouts",
                help: "The total number of requests that timed out across all brokers.",
                var_labels: ["source_id", "worker_id", "client"],
            )),
            rdkafka_connects: registry.register(metric!(
                name: "mz_source_rdkafka_connects",
                help: "The number of connection attempts, including successful and failed \
                       attempts, and name resolution failures across all brokers.",
                var_labels: ["source_id", "worker_id", "client"],
            )),
            rdkafka_disconnects: registry.register(metric!(
                name: "mz_source_rdkafka_disconnects",
                help: "The number of disconnections, whether triggered by the broker, the \
                       network, the load balancer, or something else across all brokers.",
                var_labels: ["source_id", "worker_id", "client"],
            )),
            rdkafka_broker_rtt_seconds_total: registry.register(metric!(
                name: "mz_source_rdkafka_broker_rtt_seconds_total",
                help: "The total round-trip time of the requests a broker has responded to, in \
                       seconds.",
                var_labels: ["source_id", "worker_id", "client", "broker"],
            )),
            rdkafka_broker_rtt_requests_total: registry.register(metric!(
                name: "mz_source_rdkafka_broker_rtt_requests_total",
                help: "The total number of requests a broker has responded to.",
                var_labels: ["source_id", "worker_id", "client", "broker"],
            )),
        }
    }
}

/// Which of a Kafka source's clients a [`KafkaSourceMetrics`] reports on.
#[derive(Clone, Copy, Debug)]
pub(crate) enum KafkaSourceClient {
    /// The client that fetches messages.
    Reader,
    /// The client that probes the topic's partitions and offsets.
    Metadata,
}

impl KafkaSourceClient {
    fn label(self) -> &'static str {
        match self {
            Self::Reader => "reader",
            Self::Metadata => "metadata",
        }
    }
}

/// The cumulative round-trip time of the requests one broker has responded to.
struct BrokerRttMetrics {
    seconds: DeregisterOnDropCounter<AtomicF64>,
    requests: DeregisterOnDropCounter<AtomicU64>,
}

/// Metrics derived from the librdkafka statistics of one client of a Kafka source.
pub(crate) struct KafkaSourceMetrics {
    defs: KafkaSourceMetricDefs,
    topic: String,
    source_id: GlobalId,
    /// The values of the `source_id`, `worker_id` and `client` labels.
    client_labels: Vec<String>,
    partition_offset_map: BTreeMap<i32, DeregisterOnDropGauge<AtomicI64>>,
    tx: DeregisterOnDropGauge<AtomicI64>,
    rx: DeregisterOnDropGauge<AtomicI64>,
    rx_bytes: DeregisterOnDropGauge<AtomicI64>,
    outbuf_cnt: DeregisterOnDropGauge<AtomicI64>,
    waitresp_cnt: DeregisterOnDropGauge<AtomicI64>,
    txretries: DeregisterOnDropGauge<AtomicU64>,
    req_timeouts: DeregisterOnDropGauge<AtomicU64>,
    connects: DeregisterOnDropGauge<AtomicI64>,
    disconnects: DeregisterOnDropGauge<AtomicI64>,
    /// Keyed by the broker name librdkafka reports, the value of the `broker` label.
    broker_rtt: BTreeMap<String, BrokerRttMetrics>,
}

impl KafkaSourceMetrics {
    /// Create a `KafkaSourceMetrics` from the `KafkaSourceMetricDefs`.
    pub(crate) fn new(
        defs: &KafkaSourceMetricDefs,
        topic: String,
        source_id: GlobalId,
        worker_id: usize,
        client: KafkaSourceClient,
    ) -> Self {
        let labels = vec![
            source_id.to_string(),
            worker_id.to_string(),
            client.label().to_string(),
        ];
        Self {
            defs: defs.clone(),
            topic,
            source_id,
            partition_offset_map: BTreeMap::new(),
            tx: defs
                .rdkafka_tx
                .get_deregister_on_drop_metric(labels.clone()),
            rx: defs
                .rdkafka_rx
                .get_deregister_on_drop_metric(labels.clone()),
            rx_bytes: defs
                .rdkafka_rx_bytes
                .get_deregister_on_drop_metric(labels.clone()),
            outbuf_cnt: defs
                .rdkafka_outbuf_cnt
                .get_deregister_on_drop_metric(labels.clone()),
            waitresp_cnt: defs
                .rdkafka_waitresp_cnt
                .get_deregister_on_drop_metric(labels.clone()),
            txretries: defs
                .rdkafka_txretries
                .get_deregister_on_drop_metric(labels.clone()),
            req_timeouts: defs
                .rdkafka_req_timeouts
                .get_deregister_on_drop_metric(labels.clone()),
            connects: defs
                .rdkafka_connects
                .get_deregister_on_drop_metric(labels.clone()),
            disconnects: defs
                .rdkafka_disconnects
                .get_deregister_on_drop_metric(labels.clone()),
            broker_rtt: BTreeMap::new(),
            client_labels: labels,
        }
    }

    /// Records one statistics emission of the client.
    ///
    /// Round-trip times accumulate, so every emission must be recorded exactly once.
    pub(crate) fn update(&mut self, stats: &Statistics) {
        if let Some(topic) = stats.topics.get(&self.topic) {
            for (id, partition) in &topic.partitions {
                self.set_offset_max(*id, partition.hi_offset);
            }
        }

        let mut outbuf_cnt: i64 = 0;
        let mut waitresp_cnt: i64 = 0;
        let mut txretries: u64 = 0;
        let mut req_timeouts: u64 = 0;
        let mut connects: i64 = 0;
        let mut disconnects: i64 = 0;
        for broker in stats.brokers.values() {
            outbuf_cnt += broker.outbuf_cnt;
            waitresp_cnt += broker.waitresp_cnt;
            txretries += broker.txretries;
            req_timeouts += broker.req_timeouts;
            connects += broker.connects.unwrap_or(0);
            disconnects += broker.disconnects.unwrap_or(0);
        }
        self.tx.set(stats.tx);
        self.rx.set(stats.rx);
        self.rx_bytes.set(stats.rx_bytes);
        self.outbuf_cnt.set(outbuf_cnt);
        self.waitresp_cnt.set(waitresp_cnt);
        self.txretries.set(txretries);
        self.req_timeouts.set(req_timeouts);
        self.connects.set(connects);
        self.disconnects.set(disconnects);

        // librdkafka renames a bootstrap broker once it learns its node id, and removes brokers
        // that left the cluster.
        self.broker_rtt
            .retain(|name, _| stats.brokers.contains_key(name));
        for (name, broker) in &stats.brokers {
            // The internal broker handles no requests.
            let Some(rtt) = broker.rtt.as_ref().filter(|_| broker.source != "internal") else {
                continue;
            };
            let metrics = self.broker_rtt.entry(name.clone()).or_insert_with(|| {
                let labels: Vec<_> = self
                    .client_labels
                    .iter()
                    .cloned()
                    .chain_one(name.clone())
                    .collect();
                BrokerRttMetrics {
                    seconds: self
                        .defs
                        .rdkafka_broker_rtt_seconds_total
                        .get_deregister_on_drop_metric(labels.clone()),
                    requests: self
                        .defs
                        .rdkafka_broker_rtt_requests_total
                        .get_deregister_on_drop_metric(labels),
                }
            });
            // NOTE: librdkafka resets the window on every emission, so `sum` and `cnt` cover
            // only the requests answered since the previous one. `sum` is in microseconds.
            metrics
                .seconds
                .inc_by(f64::cast_lossy(rtt.sum.max(0)) / 1e6);
            metrics.requests.inc_by(u64::try_from(rtt.cnt).unwrap_or(0));
        }
    }

    fn set_offset_max(&mut self, id: i32, offset: i64) {
        // Valid partition ids start at 0, librdkafka uses -1 as a sentinel for unassigned partitions
        if id < 0 {
            return;
        }
        // This offset value is another librdkafka sentinel indicating it got an invalid high watermark from the broker
        if offset == -1001 {
            // TODO(nharring-adjacent): This is potentially spammy so its at debug but it would be better as info with sampling
            debug!("Got invalid high watermark for partition {}", id);
            return;
        }
        self.partition_offset_map
            .entry(id)
            .or_insert_with_key(|id| {
                self.defs
                    .partition_offset_max
                    .get_deregister_on_drop_metric(vec![
                        self.topic.clone(),
                        self.source_id.to_string(),
                        id.to_string(),
                    ])
            })
            .set(offset);
    }
}
