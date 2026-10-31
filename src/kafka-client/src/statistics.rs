// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Client and broker statistics.
//!
//! Emitted to [`ClientContext::stats`] every `statistics.interval.ms` when that
//! is non-zero. The struct tree and its serde shape are rust-rdkafka's, which
//! follow librdkafka's `STATISTICS.md`: every non-`Option` field is required
//! when deserializing. Maps are `BTreeMap`s instead of `HashMap`s. The client
//! fills the fields Materialize reads; the others keep their defaults.
//!
//! [`ClientContext::stats`]: crate::ClientContext::stats

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Overall statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Statistics {
    /// The name of the librdkafka handle.
    pub name: String,
    /// The configured `client.id`.
    pub client_id: String,
    /// The instance type (producer or consumer).
    #[serde(rename = "type")]
    pub client_type: String,
    /// The current value of the client's internal monotonic clock, in
    /// microseconds since start.
    pub ts: i64,
    /// Wall clock time, in seconds since the Unix epoch.
    pub time: i64,
    /// Time since this client instance was created, in microseconds.
    pub age: i64,
    /// The number of operations (callbacks, events, etc.) waiting in queue.
    pub replyq: i64,
    /// The current number of messages in producer queues.
    pub msg_cnt: u64,
    /// The current total size of messages in producer queues.
    pub msg_size: u64,
    /// The maximum number of messages allowed in the producer queues.
    pub msg_max: u64,
    /// The maximum total size of messages allowed in the producer queues.
    pub msg_size_max: u64,
    /// The total number of requests sent to brokers.
    pub tx: i64,
    /// The total number of bytes transmitted to brokers.
    pub tx_bytes: i64,
    /// The total number of responses received from brokers.
    pub rx: i64,
    /// The total number of bytes received from brokers.
    pub rx_bytes: i64,
    /// The total number of messages transmitted (produced) to brokers.
    pub txmsgs: i64,
    /// The total number of bytes transmitted (produced) to brokers.
    pub txmsg_bytes: i64,
    /// The total number of messages consumed from brokers, not including
    /// ignored messages.
    pub rxmsgs: i64,
    /// The total number of bytes (including framing) consumed from brokers.
    pub rxmsg_bytes: i64,
    /// Internal tracking of legacy vs. new consumer API state.
    pub simple_cnt: i64,
    /// Number of topics in the metadata cache.
    pub metadata_cache_cnt: i64,
    /// Per-broker statistics.
    pub brokers: BTreeMap<String, Broker>,
    /// Per-topic statistics.
    pub topics: BTreeMap<String, Topic>,
    /// Consumer group statistics.
    pub cgrp: Option<ConsumerGroup>,
    /// Exactly-once semantics and idempotent producer statistics.
    pub eos: Option<ExactlyOnceSemantics>,
}

/// Per-broker statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Broker {
    /// The broker hostname, port, and ID, in the form `HOSTNAME:PORT/ID`.
    pub name: String,
    /// The broker ID (-1 for bootstraps).
    pub nodeid: i32,
    /// The broker hostname and port.
    pub nodename: String,
    /// The broker source (learned, configured, internal, or logical).
    pub source: String,
    /// The broker state (INIT, DOWN, CONNECT, AUTH, APIVERSION_QUERY,
    /// AUTH_HANDSHAKE, UP, UPDATE).
    pub state: String,
    /// The time since the last broker state change, in microseconds.
    pub stateage: i64,
    /// The number of requests awaiting transmission to the broker.
    pub outbuf_cnt: i64,
    /// The number of messages awaiting transmission to the broker.
    pub outbuf_msg_cnt: i64,
    /// The number of requests in-flight to the broker that are awaiting a
    /// response.
    pub waitresp_cnt: i64,
    /// The number of messages in-flight to the broker that are awaiting a
    /// response.
    pub waitresp_msg_cnt: i64,
    /// The total number of requests sent to the broker.
    pub tx: u64,
    /// The total number of bytes sent to the broker.
    pub txbytes: u64,
    /// The total number of transmission errors.
    pub txerrs: u64,
    /// The total number of request retries.
    pub txretries: u64,
    /// Microseconds since last socket send, or -1 if no sends yet for the
    /// current connection.
    pub txidle: i64,
    /// The total number of requests that timed out.
    pub req_timeouts: u64,
    /// The total number of responses received from the broker.
    pub rx: u64,
    /// The total number of bytes received from the broker.
    pub rxbytes: u64,
    /// The total number of receive errors.
    pub rxerrs: u64,
    /// The number of unmatched correlation IDs in response, typically for
    /// timed out requests.
    pub rxcorriderrs: u64,
    /// The total number of partial message sets received. The broker may return
    /// partial responses if the full message set could not fit in the remaining
    /// fetch response size.
    pub rxpartial: u64,
    /// Microseconds since last socket receive, or -1 if no receives yet for the
    /// current connection.
    pub rxidle: i64,
    /// Request type counters. The object key is the name of the request type
    /// and the value is the number of requests of that type that have been
    /// sent.
    pub req: BTreeMap<String, i64>,
    /// The total number of decompression buffer size increases.
    pub zbuf_grow: u64,
    /// The total number of buffer size increases (deprecated and unused).
    pub buf_grow: u64,
    /// The number of broker thread poll wakeups.
    pub wakeups: Option<u64>,
    /// The number of connection attempts, including successful and failed
    /// attempts, and name resolution failures.
    pub connects: Option<i64>,
    /// The number of disconnections, whether triggered by the broker, the
    /// network, the load balancer, or something else.
    pub disconnects: Option<i64>,
    /// Rolling window statistics for the internal producer queue latency, in
    /// microseconds.
    pub int_latency: Option<Window>,
    /// Rolling window statistics for the internal request queue latency, in
    /// microseconds.
    ///
    /// This is the time between when a request is enqueued on the transmit
    /// (outbuf) queue and the time the request is written to the TCP socket.
    /// Additional buffering and latency may be incurred by the TCP stack and
    /// network.
    pub outbuf_latency: Option<Window>,
    /// Rolling window statistics for the broker latency/round-trip time,
    /// in microseconds.
    pub rtt: Option<Window>,
    /// Rolling window statistics for the broker throttling time, in
    /// milliseconds.
    pub throttle: Option<Window>,
    /// The partitions that are handled by this broker handle.
    pub toppars: BTreeMap<String, TopicPartition>,
}

/// Rolling window statistics.
///
/// These values are not exact; they are sampled estimates maintained by an
/// HDR histogram in librdkafka.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Window {
    /// The smallest value.
    pub min: i64,
    /// The largest value.
    pub max: i64,
    /// The mean value.
    pub avg: i64,
    /// The sum of all values.
    pub sum: i64,
    /// The total number of values.
    pub cnt: i64,
    /// The standard deviation.
    pub stddev: i64,
    /// The memory size of the underlying HDR histogram.
    pub hdrsize: i64,
    /// The 50th percentile.
    pub p50: i64,
    /// The 75th percentile.
    pub p75: i64,
    /// The 90th percentile.
    pub p90: i64,
    /// The 95th percentile.
    pub p95: i64,
    /// The 99th percentile.
    pub p99: i64,
    /// The 99.99th percentile.
    pub p99_99: i64,
    /// The number of values not included in the underlying histogram because
    /// they were out of range.
    pub outofrange: i64,
}

/// A topic and partition specifier.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct TopicPartition {
    /// The name of the topic.
    pub topic: String,
    /// The ID of the partition.
    pub partition: i32,
}

/// Per-topic statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Topic {
    /// The name of the topic.
    pub topic: String,
    /// The age of the client's metadata for this topic, in milliseconds.
    pub metadata_age: i64,
    /// Rolling window statistics for batch sizes, in bytes.
    pub batchsize: Window,
    /// Rolling window statistics for batch message counts.
    pub batchcnt: Window,
    /// Per-partition statistics.
    pub partitions: BTreeMap<i32, Partition>,
}

/// Per-partition statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Partition {
    /// The partition ID.
    pub partition: i32,
    /// The ID of the broker from which messages are currently being fetched.
    pub broker: i32,
    /// The broker ID of the leader.
    pub leader: i32,
    /// Whether the partition is explicitly desired by the application.
    pub desired: bool,
    /// Whether the partition is not seen in the topic metadata from the broker.
    pub unknown: bool,
    /// The number of messages waiting to be produced in the first-level queue.
    pub msgq_cnt: i64,
    /// The number of bytes waiting to be produced in the first-level queue.
    pub msgq_bytes: u64,
    /// The number of messages ready to be produced in the transmit queue.
    pub xmit_msgq_cnt: i64,
    /// The number of bytes ready to be produced in the transmit queue.
    pub xmit_msgq_bytes: u64,
    /// The number of prefetched messages in the fetch queue.
    pub fetchq_cnt: i64,
    /// The number of bytes in the fetch queue.
    pub fetchq_size: u64,
    /// The consumer fetch state for this partition (none, stopping, stopped,
    /// offset-query, offset-wait, active).
    pub fetch_state: String,
    /// The current/last logical offset query.
    pub query_offset: i64,
    /// The next offset to fetch.
    pub next_offset: i64,
    /// The offset of the last message passed to the application, plus one.
    pub app_offset: i64,
    /// The offset to be committed.
    pub stored_offset: i64,
    /// The last committed offset.
    pub committed_offset: i64,
    /// The last offset for which partition EOF was signaled.
    pub eof_offset: i64,
    /// The low watermark offset on the broker.
    pub lo_offset: i64,
    /// The high watermark offset on the broker.
    pub hi_offset: i64,
    /// The last stable offset on the broker.
    pub ls_offset: i64,
    /// The difference between `hi_offset` and `committed_offset`.
    pub consumer_lag: i64,
    /// The difference between `hi_offset` and `stored_offset`.
    pub consumer_lag_stored: i64,
    /// The total number of messages transmitted (produced).
    pub txmsgs: u64,
    /// The total number of bytes transmitted (produced).
    pub txbytes: u64,
    /// The total number of messages consumed, not included ignored messages.
    pub rxmsgs: u64,
    /// The total bytes consumed.
    pub rxbytes: u64,
    /// The total number of messages received, for consumers, or the total
    /// number of messages produced, for producers.
    pub msgs: u64,
    /// The number of dropped outdated messages.
    pub rx_ver_drops: u64,
    /// The current number of messages in flight to or from the broker.
    pub msgs_inflight: i64,
    /// The next expected acked sequence number, for idempotent producers.
    pub next_ack_seq: i64,
    /// The next expected errored sequence number, for idempotent producers.
    pub next_err_seq: i64,
    /// The last acked internal message ID, for idempotent producers.
    pub acked_msgid: u64,
}

/// Consumer group manager statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ConsumerGroup {
    /// The local consumer group handler's state.
    pub state: String,
    /// The time elapsed since the last state change, in milliseconds.
    pub stateage: i64,
    /// The local consumer group hander's join state.
    pub join_state: String,
    /// The time elapsed since the last rebalance (assign or revoke), in
    /// milliseconds.
    pub rebalance_age: i64,
    /// The total number of rebalances (assign or revoke).
    pub rebalance_cnt: i64,
    /// The reason for the last rebalance.
    ///
    /// This string will be empty if no rebalances have occurred.
    pub rebalance_reason: String,
    /// The partition count for the current assignment.
    pub assignment_size: i32,
}

/// Exactly-once semantics statistics.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ExactlyOnceSemantics {
    /// The current idempotent producer state.
    pub idemp_state: String,
    /// THe time elapsed since the last idempotent producer state change, in
    /// milliseconds.
    pub idemp_stateage: i64,
    /// The current transactional producer state.
    pub txn_state: String,
    /// The time elapsed since the last transactional producer state change, in
    /// milliseconds.
    pub txn_stateage: i64,
    /// Whether the transactional state allows enqueing (producing) new
    /// messages.
    pub txn_may_enq: bool,
    /// The currently assigned producer ID, or -1.
    pub producer_id: i64,
    /// The current epoch, or -1.
    pub producer_epoch: i64,
    /// The number of producer ID assignments.
    pub epoch_cnt: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn round_trip() {
        let mut stats = Statistics {
            name: "rdkafka#producer-1".into(),
            client_type: "producer".into(),
            msg_cnt: 320,
            txmsgs: 7,
            ..Default::default()
        };
        stats.brokers.insert(
            "localhost:9092/0".into(),
            Broker {
                outbuf_msg_cnt: 3,
                connects: Some(1),
                ..Default::default()
            },
        );
        let mut topic = Topic::default();
        topic.partitions.insert(
            -1,
            Partition {
                partition: -1,
                hi_offset: -1001,
                ..Default::default()
            },
        );
        topic.partitions.insert(
            0,
            Partition {
                hi_offset: 42,
                ..Default::default()
            },
        );
        stats.topics.insert("t".into(), topic);

        let json = serde_json::to_string(&stats).unwrap();
        assert!(json.contains("\"type\":\"producer\""));
        let parsed: Statistics = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "rdkafka#producer-1");
        assert_eq!(parsed.msg_cnt, 320);
        assert_eq!(parsed.txmsgs, 7);
        assert_eq!(parsed.brokers["localhost:9092/0"].outbuf_msg_cnt, 3);
        assert_eq!(parsed.brokers["localhost:9092/0"].connects, Some(1));
        assert_eq!(parsed.topics["t"].partitions[&0].hi_offset, 42);
        assert_eq!(parsed.topics["t"].partitions[&-1].hi_offset, -1001);
        assert!(parsed.cgrp.is_none());
        assert!(parsed.eos.is_none());
    }

    #[mz_ore::test]
    fn required_fields() {
        // librdkafka emits every non-`Option` field; a blob missing one is
        // rejected, as with rust-rdkafka.
        assert!(serde_json::from_str::<Statistics>("{\"name\": \"x\"}").is_err());
    }
}
