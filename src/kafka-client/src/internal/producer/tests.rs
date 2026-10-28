// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Producer tests.
//!
//! Tests that need a broker run only when `MZ_KAFKA_CLIENT_TEST_BROKERS` is
//! set (see `internal::cluster::tests`); they run against every listed
//! cluster. Records are read back with raw Fetch requests, filtering aborted
//! transactions like a `read_committed` consumer.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
use kafka_protocol::messages::{BrokerId, FetchRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::client::ClientContext;
use crate::config::settings::{ClientKind, ClientSettings};
use crate::config::{ClientConfig, FromClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use crate::error::KafkaError;
use crate::internal::cluster::{Cluster, PartitionMetadata, TopicMetadata};
use crate::internal::net::RequestOptions;
use crate::internal::records::{ControlRecordKind, parse_batches};
use crate::internal::runtime;
use crate::message::{Header, Message, OwnedHeaders};
use crate::producer::{
    BaseRecord, DeliveryResult, FutureProducer, FutureRecord, Producer, ProducerContext,
    ThreadedProducer,
};
use crate::types::RDKafkaErrorCode;
use crate::util::{Deadline, Timeout};

use super::Config;
use super::state::{Effects, NewRecord, PartKey, State, TopicStatus, UaMessage, Undelivered};
use super::txn::{Eos, TxnState};
use super::wire_size;

/// A delivery report, as recorded by [`Recorder`].
#[derive(Clone, Debug, PartialEq)]
struct Report {
    opaque: usize,
    partition: i32,
    offset: i64,
    error: Option<RDKafkaErrorCode>,
    key: Option<Vec<u8>>,
    payload: Option<Vec<u8>>,
}

/// Records delivery reports, logs and error callbacks.
#[derive(Default)]
struct Recorder {
    reports: Mutex<Vec<Report>>,
    logs: Mutex<Vec<(RDKafkaLogLevel, String, String)>>,
    errors: Mutex<Vec<(KafkaError, String)>>,
    stats: Mutex<Vec<crate::statistics::Statistics>>,
}

#[derive(Clone, Default)]
struct Ctx(Arc<Recorder>);

impl ClientContext for Ctx {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.0
            .logs
            .lock()
            .unwrap()
            .push((level, fac.to_string(), log_message.to_string()));
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.0
            .errors
            .lock()
            .unwrap()
            .push((error, reason.to_string()));
    }

    fn stats(&self, statistics: crate::statistics::Statistics) {
        self.0.stats.lock().unwrap().push(statistics);
    }
}

impl ProducerContext for Ctx {
    type DeliveryOpaque = usize;

    fn delivery(&self, result: &DeliveryResult<'_>, opaque: usize) {
        let (message, error) = match result {
            Ok(m) => (m, None),
            Err((KafkaError::MessageProduction(code), m)) => (m, Some(*code)),
            Err((e, _)) => panic!("unexpected delivery error {e:?}"),
        };
        self.0.reports.lock().unwrap().push(Report {
            opaque,
            partition: message.partition(),
            offset: message.offset(),
            error,
            key: message.key().map(<[u8]>::to_vec),
            payload: message.payload().map(<[u8]>::to_vec),
        });
    }
}

/// Like [`Ctx`], with message contents in successful delivery reports.
#[derive(Clone, Default)]
struct FullCtx(Ctx);

impl ClientContext for FullCtx {}

impl ProducerContext for FullCtx {
    type DeliveryOpaque = usize;
    const DELIVERY_REPORT_CONTENTS: bool = true;

    fn delivery(&self, result: &DeliveryResult<'_>, opaque: usize) {
        if let Ok(m) = result {
            // Contents carry the create time.
            assert!(matches!(m.timestamp(), crate::message::Timestamp::CreateTime(t) if t > 0));
        }
        self.0.delivery(result, opaque)
    }
}

impl Recorder {
    fn reports(&self) -> Vec<Report> {
        self.reports.lock().unwrap().clone()
    }
}

/// The bootstrap server lists to test against, if configured.
fn test_brokers() -> Vec<String> {
    std::env::var("MZ_KAFKA_CLIENT_TEST_BROKERS")
        .ok()
        .map(|v| {
            v.split(';')
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Creates a topic through the controller and waits until its partitions
/// have leaders.
fn create_topic(cluster: &Arc<Cluster>, name: &str, partitions: i32) {
    use kafka_protocol::messages::CreateTopicsRequest;
    use kafka_protocol::messages::create_topics_request::CreatableTopic;
    let cluster = Arc::clone(cluster);
    let name = name.to_string();
    runtime::block_on(async move {
        let deadline = Deadline::new(Duration::from_secs(30));
        let controller = cluster.controller(deadline).await.unwrap();
        let response = controller
            .request::<CreateTopicsRequest, _>(
                |_| {
                    CreateTopicsRequest::default()
                        .with_timeout_ms(10_000)
                        .with_topics(vec![
                            CreatableTopic::default()
                                .with_name(TopicName(StrBytes::from_string(name.clone())))
                                .with_num_partitions(partitions)
                                .with_replication_factor(1),
                        ])
                },
                RequestOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(response.topics[0].error_code, 0, "{response:?}");
        // Brokers learn about new topics asynchronously.
        for p in 0..partitions {
            loop {
                match cluster.leader(&name, p, deadline).await {
                    Ok(_) => break,
                    Err(e) if !deadline.is_elapsed() => {
                        assert_eq!(e.code(), RDKafkaErrorCode::UnknownTopicOrPartition, "{e}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    Err(e) => panic!("topic {name} has no leader: {e}"),
                }
            }
        }
    })
}

/// Deletes topics when dropped, so that tests do not exhaust the brokers'
/// partition limits.
struct DropTopics(Arc<Cluster>, Vec<String>);

impl DropTopics {
    fn new(cluster: &Arc<Cluster>, topics: &[&str]) -> DropTopics {
        DropTopics(
            Arc::clone(cluster),
            topics.iter().map(|t| t.to_string()).collect(),
        )
    }
}

impl Drop for DropTopics {
    fn drop(&mut self) {
        use kafka_protocol::messages::DeleteTopicsRequest;
        let cluster = Arc::clone(&self.0);
        let names = std::mem::take(&mut self.1);
        runtime::block_on(async move {
            let deadline = Deadline::new(Duration::from_secs(10));
            let Ok(controller) = cluster.controller(deadline).await else {
                return;
            };
            let _ = controller
                .request::<DeleteTopicsRequest, _>(
                    |_| {
                        DeleteTopicsRequest::default()
                            .with_timeout_ms(5_000)
                            .with_topic_names(
                                names
                                    .iter()
                                    .map(|n| TopicName(StrBytes::from_string(n.clone())))
                                    .collect(),
                            )
                    },
                    RequestOptions::default(),
                )
                .await;
        });
    }
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[allow(clippy::disallowed_methods)]
fn config(brokers: &str, pairs: &[(&str, &str)]) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    config.set_log_level(RDKafkaLogLevel::Warning);
    for (k, v) in pairs {
        config.set(*k, *v);
    }
    config
}

fn producer(brokers: &str, pairs: &[(&str, &str)]) -> (ThreadedProducer<Ctx>, Arc<Recorder>) {
    let ctx = Ctx::default();
    let recorder = Arc::clone(&ctx.0);
    let p = ThreadedProducer::from_config_and_context(&config(brokers, pairs), ctx).unwrap();
    (p, recorder)
}

/// A record read back from a topic.
#[derive(Clone, Debug, PartialEq)]
struct Fetched {
    offset: i64,
    key: Option<Bytes>,
    value: Option<Bytes>,
    headers: Vec<(String, Option<Vec<u8>>)>,
    timestamp: i64,
}

/// Reads a partition from offset 0 to its end with raw Fetch requests. With
/// `read_committed`, reads up to the last stable offset and drops records of
/// aborted transactions, like a `read_committed` consumer.
fn fetch_all(
    cluster: &Arc<Cluster>,
    topic: &str,
    partition: i32,
    read_committed: bool,
) -> Vec<Fetched> {
    let cluster = Arc::clone(cluster);
    let topic = topic.to_string();
    runtime::block_on(async move {
        let deadline = Deadline::new(Duration::from_secs(30));
        let mut out = Vec::new();
        let mut offset = 0;
        // Aborted transactions by producer id: their first offsets.
        let mut aborted: BTreeMap<i64, Vec<i64>> = BTreeMap::new();
        let mut active_aborted: std::collections::BTreeSet<i64> = Default::default();
        let mut seen_aborted: std::collections::BTreeSet<(i64, i64)> = Default::default();
        loop {
            let leader = cluster.leader(&topic, partition, deadline).await.unwrap();
            let response = leader
                .request::<FetchRequest, _>(
                    |_| {
                        FetchRequest::default()
                            .with_replica_id(BrokerId(-1))
                            .with_max_wait_ms(100)
                            .with_min_bytes(1)
                            .with_max_bytes(64 << 20)
                            .with_isolation_level(i8::from(read_committed))
                            .with_topics(vec![
                                FetchTopic::default()
                                    .with_topic(TopicName(StrBytes::from_string(topic.clone())))
                                    .with_partitions(vec![
                                        FetchPartition::default()
                                            .with_partition(partition)
                                            .with_fetch_offset(offset)
                                            .with_partition_max_bytes(64 << 20),
                                    ]),
                            ])
                    },
                    RequestOptions::default(),
                )
                .await
                .unwrap();
            let p = &response.responses[0].partitions[0];
            assert_eq!(p.error_code, 0, "fetch error");
            // Each response lists the aborted transactions overlapping it, so
            // later responses repeat ones already handled.
            for a in p.aborted_transactions.iter().flatten() {
                if seen_aborted.insert((a.producer_id.0, a.first_offset)) {
                    aborted
                        .entry(a.producer_id.0)
                        .or_default()
                        .push(a.first_offset);
                }
            }
            if read_committed && p.last_stable_offset < p.high_watermark && !deadline.is_elapsed() {
                // Brokers write transaction markers after answering EndTxn;
                // the tests have no transaction open when they read.
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
            let end = if read_committed {
                p.last_stable_offset
            } else {
                p.high_watermark
            };
            let records = p.records.clone().unwrap_or_default();
            for batch in parse_batches(records) {
                let batch = batch.unwrap();
                let h = batch.header().clone();
                if h.last_offset < offset {
                    continue;
                }
                if read_committed && h.is_transactional {
                    if let Some(firsts) = aborted.get_mut(&h.producer_id) {
                        if firsts.iter().any(|f| *f <= h.base_offset) {
                            active_aborted.insert(h.producer_id);
                            firsts.retain(|f| *f > h.last_offset);
                        }
                    }
                }
                if h.is_control {
                    let control = batch.control_record().unwrap();
                    if control.is_some_and(|c| c.kind == ControlRecordKind::Abort) {
                        active_aborted.remove(&h.producer_id);
                    }
                    offset = h.last_offset + 1;
                    continue;
                }
                let skip =
                    read_committed && h.is_transactional && active_aborted.contains(&h.producer_id);
                for record in batch.records(usize::MAX).unwrap() {
                    let record = record.unwrap();
                    if record.offset < offset {
                        continue;
                    }
                    if !skip {
                        out.push(Fetched {
                            offset: record.offset,
                            key: record.key.clone(),
                            value: record.value.clone(),
                            headers: record
                                .headers
                                .iter()
                                .map(|h| {
                                    let (k, v) = h.unwrap();
                                    (
                                        String::from_utf8(k.to_vec()).unwrap(),
                                        v.map(<[u8]>::to_vec),
                                    )
                                })
                                .collect(),
                            timestamp: record.timestamp,
                        });
                    }
                }
                offset = h.last_offset + 1;
            }
            if offset >= end {
                return out;
            }
        }
    })
}

/// The cluster of a producer, for raw requests.
fn cluster_of<C: ProducerContext>(p: &ThreadedProducer<C>) -> Arc<Cluster> {
    Arc::clone(p.client().cluster())
}

fn wait_for(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let start = Instant::now();
    while !f() {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

const CODECS: [&str; 5] = ["none", "gzip", "snappy", "lz4", "zstd"];

// Tests without a broker.

#[mz_ore::test]
fn wire_size_matches_librdkafka() {
    // RD_KAFKAP_MESSAGE_V2_MAX_OVERHEAD is 36.
    assert_eq!(wire_size(None, None, None), 36);
    assert_eq!(wire_size(Some(b"k"), Some(b"vv"), None), 39);
    let headers = OwnedHeaders::new()
        .insert(Header {
            key: "a",
            value: Some(b"xyz".as_slice()),
        })
        .insert(Header::<&[u8]> {
            key: "bb",
            value: None,
        });
    // "a": 1 + 3 + varint(1) + varint(3); "bb": 2 + 0 + varint(2) + varint(0).
    assert_eq!(wire_size(None, None, Some(&headers)), 36 + 6 + 4);
    let long = vec![0; 64];
    let headers = OwnedHeaders::new().insert(Header {
        key: "k",
        value: Some(long.as_slice()),
    });
    // A length of 64 takes two zigzag varint bytes.
    assert_eq!(wire_size(None, None, Some(&headers)), 36 + 1 + 64 + 1 + 2);
}

#[mz_ore::test]
fn txn_state_transitions() {
    use TxnState::*;
    assert_eq!(Init.transition(WaitPid), Some(true));
    assert_eq!(Ready.transition(BeginCommit), None);
    assert_eq!(InTransaction.transition(AbortableError), Some(true));
    assert_eq!(BeginAbort.transition(AbortableError), Some(false));
    assert_eq!(Ready.transition(AbortableError), None);
    assert_eq!(Ready.transition(FatalError), Some(true));
    assert_eq!(AbortNotAcked.name(), "AbortedNotAcked");
}

/// A transactional producer's state and config with a 1s
/// `topic.metadata.propagation.max.ms`.
fn txn_state(now: Instant) -> (State<usize>, Config) {
    let settings = ClientSettings::from_config(
        &config(
            "127.0.0.1:1",
            &[
                ("transactional.id", "t"),
                ("topic.metadata.propagation.max.ms", "1000"),
            ],
        ),
        ClientKind::Producer,
    )
    .unwrap();
    (
        State::new(Eos::new(true, true, now)),
        Config::from_settings(&settings),
    )
}

/// Metadata for a topic whose partitions are led by broker 1.
fn topic_md(error: Option<RDKafkaErrorCode>, partitions: i32) -> TopicMetadata {
    TopicMetadata {
        name: "t".into(),
        error,
        partitions: (0..partitions)
            .map(|id| PartitionMetadata {
                id,
                leader: 1,
                leader_epoch: -1,
                replicas: vec![1],
                isr: vec![1],
                error: None,
            })
            .collect(),
        is_internal: false,
        updated_at: Instant::now(),
    }
}

fn queue_record(st: &mut State<usize>, cfg: &Config, key: PartKey, opaque: usize, now: Instant) {
    let record = NewRecord {
        timestamp: 0,
        key: None,
        value: Some(b"x"),
        headers: &[],
        wire_size: wire_size(None, Some(b"x"), None),
        payload_len: 1,
        kv_len: 1,
    };
    let state = &mut *st;
    state.topics[key.topic]
        .partition_mut(key.partition)
        .unwrap()
        .append(&record, opaque, cfg, &mut state.buffers, now);
    st.added_to_partition(key, true, &mut Effects::default());
}

/// The opaques and errors of failed deliveries, sorted.
fn failed(fx: &Effects<usize>) -> Vec<(i32, usize, RDKafkaErrorCode)> {
    let mut failed = Vec::new();
    for f in &fx.failures {
        match &f.records {
            Undelivered::Batch(b) => {
                failed.extend(b.opaques.iter().map(|o| (f.partition, *o, f.error)))
            }
            Undelivered::Ua(m) => failed.push((f.partition, m.opaque, f.error)),
        }
    }
    failed.sort_by_key(|(partition, opaque, _)| (*partition, *opaque));
    failed
}

/// Like librdkafka's `rd_kafka_topic_set_notexists`, metadata saying that a
/// known topic is gone is believed once `topic.metadata.propagation.max.ms`
/// passed since the topic's status last changed. Its partitions then go away
/// with their queued records and transaction registrations, and metadata
/// saying it exists again is held back by the same window.
#[mz_ore::test]
fn deleted_topic_partitions_go_away() {
    let t0 = Instant::now();
    let at = |ms| t0 + Duration::from_millis(ms);
    let (mut st, cfg) = txn_state(t0);
    let (id, _) = st.topic_id("t", t0);
    let mut fx = Effects::default();
    st.apply_topic_metadata(id, Some(&topic_md(None, 2)), Some(1), &cfg, t0, &mut fx);
    assert_eq!(st.topics[id].status, TopicStatus::Exists);
    let keys = [0, 1].map(|partition| PartKey {
        topic: id,
        partition,
    });
    for (opaque, key) in keys.iter().enumerate() {
        queue_record(&mut st, &cfg, *key, opaque, t0);
    }
    st.topics[id].partitions[0].seal();
    // Partition 1's AddPartitionsToTxn is outstanding.
    assert!(st.eos.pending.remove(&keys[1]));
    st.eos.registering.push(keys[1]);
    let old_incarnation = st.topics[id].partitions[0].incarnation;

    let gone = topic_md(Some(RDKafkaErrorCode::UnknownTopicOrPartition), 0);
    let mut fx = Effects::default();
    st.apply_topic_metadata(id, Some(&gone), Some(2), &cfg, at(500), &mut fx);
    assert_eq!(st.topics[id].status, TopicStatus::Exists);
    assert_eq!(st.topics[id].partitions.len(), 2);
    assert!(fx.failures.is_empty());

    // The maintenance tick re-applies the metadata once the window passed.
    let mut fx = Effects::default();
    st.apply_topic_metadata(id, Some(&gone), None, &cfg, at(1000), &mut fx);
    assert_eq!(st.topics[id].status, TopicStatus::NotExists);
    assert!(st.topics[id].partitions.is_empty());
    assert_eq!(
        failed(&fx),
        vec![
            (0, 0, RDKafkaErrorCode::UnknownPartition),
            (1, 1, RDKafkaErrorCode::UnknownPartition),
        ]
    );
    assert!(st.eos.pending.is_empty());
    assert!(st.eos.registering.is_empty());
    assert!(st.senders[&1].parts.is_empty());

    // A broker that has not learned about the deletion yet.
    let mut fx = Effects::default();
    st.apply_topic_metadata(
        id,
        Some(&topic_md(None, 2)),
        Some(3),
        &cfg,
        at(1500),
        &mut fx,
    );
    assert_eq!(st.topics[id].status, TopicStatus::NotExists);
    assert!(st.topics[id].partitions.is_empty());

    // Recreated: new partitions, which responses for the old ones do not
    // match.
    st.apply_topic_metadata(id, Some(&topic_md(None, 2)), None, &cfg, at(2000), &mut fx);
    assert_eq!(st.topics[id].status, TopicStatus::Exists);
    assert_eq!(st.topics[id].partitions.len(), 2);
    assert!(st.topics[id].partition_since(0, old_incarnation).is_none());
    assert_eq!(st.senders[&1].parts.len(), 2);
}

/// A topic missing from an all-topics metadata response, as after the
/// application's `fetch_metadata(None)` following a deletion, becomes unknown
/// on the next maintenance tick, which restarts the propagation window. New
/// records then wait in the UA queue and fail with the metadata's error once
/// the topic is known to be gone.
#[mz_ore::test]
fn topic_missing_from_metadata_becomes_unknown() {
    let t0 = Instant::now();
    let at = |ms| t0 + Duration::from_millis(ms);
    let (mut st, cfg) = txn_state(t0);
    let (id, _) = st.topic_id("t", t0);
    let mut fx = Effects::default();
    st.apply_topic_metadata(id, Some(&topic_md(None, 1)), Some(1), &cfg, t0, &mut fx);
    let key = PartKey {
        topic: id,
        partition: 0,
    };
    queue_record(&mut st, &cfg, key, 0, t0);

    let mut fx = Effects::default();
    st.apply_topic_metadata(id, None, Some(2), &cfg, at(4500), &mut fx);
    assert_eq!(st.topics[id].status, TopicStatus::Exists);
    st.apply_topic_metadata(id, None, None, &cfg, at(5000), &mut fx);
    assert_eq!(st.topics[id].status, TopicStatus::Unknown);
    assert!(fx.refresh_metadata);
    st.topics[id].ua.push_back(UaMessage {
        key: None,
        payload: Some(b"x".to_vec()),
        headers: None,
        timestamp: 0,
        partition: Some(0),
        opaque: 1,
        enqueued: at(5000),
    });

    // Within the window restarted by the change to unknown.
    let gone = topic_md(Some(RDKafkaErrorCode::UnknownTopicOrPartition), 0);
    let mut fx = Effects::default();
    st.apply_topic_metadata(id, Some(&gone), Some(3), &cfg, at(5500), &mut fx);
    assert_eq!(st.topics[id].status, TopicStatus::Unknown);
    assert!(fx.refresh_metadata);
    assert!(fx.failures.is_empty());

    let mut fx = Effects::default();
    st.apply_topic_metadata(id, Some(&gone), None, &cfg, at(6000), &mut fx);
    assert_eq!(st.topics[id].status, TopicStatus::NotExists);
    assert_eq!(
        failed(&fx),
        vec![
            (0, 0, RDKafkaErrorCode::UnknownPartition),
            (0, 1, RDKafkaErrorCode::UnknownTopicOrPartition),
        ]
    );
}

#[mz_ore::test]
fn send_validation_without_broker() {
    // No broker is reachable; validation is local.
    let (p, _) = producer(
        "127.0.0.1:1",
        &[
            ("message.max.bytes", "1000"),
            ("queue.buffering.max.messages", "2"),
        ],
    );
    let big = vec![0u8; 1000];
    let err = p
        .send(BaseRecord::<(), [u8], usize>::with_opaque_to("t", 0).payload(&big))
        .unwrap_err()
        .0;
    assert_eq!(
        err,
        KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge)
    );
    // 36 bytes of overhead + 964 bytes fits exactly.
    let ok = vec![0u8; 964];
    p.send(BaseRecord::<(), [u8], usize>::with_opaque_to("t", 1).payload(&ok))
        .unwrap();
    p.send(BaseRecord::<(), [u8], usize>::with_opaque_to("t", 2).payload(&ok))
        .unwrap();
    // The third message exceeds queue.buffering.max.messages.
    let err = p
        .send(BaseRecord::<(), [u8], usize>::with_opaque_to("t", 3).payload(&ok))
        .unwrap_err()
        .0;
    assert_eq!(
        err,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    );
    assert_eq!(p.in_flight_count(), 2);
    let err = p.flush(Duration::from_millis(50)).unwrap_err();
    assert_eq!(err, KafkaError::Flush(RDKafkaErrorCode::OperationTimedOut));
}

#[mz_ore::test]
fn queue_full_counts_payload_bytes() {
    let (p, _) = producer("127.0.0.1:1", &[("queue.buffering.max.kbytes", "1")]);
    let payload = vec![0u8; 1000];
    let key = vec![0u8; 5000];
    // Keys do not count.
    p.send(
        BaseRecord::<[u8], [u8], usize>::with_opaque_to("t", 0)
            .payload(&payload)
            .key(&key),
    )
    .unwrap();
    let err = p
        .send(BaseRecord::<[u8], [u8], usize>::with_opaque_to("t", 1).payload(&payload))
        .unwrap_err()
        .0;
    assert_eq!(
        err,
        KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull)
    );
}

#[mz_ore::test]
fn transactional_api_states_without_broker() {
    let (p, _) = producer("127.0.0.1:1", &[]);
    let err = p.begin_transaction().unwrap_err();
    let KafkaError::Transaction(e) = err else {
        panic!()
    };
    assert_eq!(e.code(), RDKafkaErrorCode::NotConfigured);
    assert_eq!(
        e.string(),
        "The Transactional API requires transactional.id to be configured"
    );

    let (p, _) = producer("127.0.0.1:1", &[("transactional.id", "t")]);
    // Producing outside a transaction.
    let err = p
        .send(BaseRecord::<(), [u8], usize>::with_opaque_to("t", 0).payload(b"x".as_slice()))
        .unwrap_err()
        .0;
    assert_eq!(err, KafkaError::MessageProduction(RDKafkaErrorCode::State));
    let KafkaError::Transaction(e) = p.begin_transaction().unwrap_err() else {
        panic!()
    };
    assert_eq!(e.code(), RDKafkaErrorCode::State);
    assert_eq!(e.string(), "Operation not valid in state Init");
    assert!(!e.is_fatal() && !e.is_retriable() && !e.txn_requires_abort());
    let KafkaError::Transaction(e) = p.commit_transaction(Duration::from_secs(1)).unwrap_err()
    else {
        panic!()
    };
    assert_eq!(e.code(), RDKafkaErrorCode::State);
    // No broker: init times out and can be resumed.
    let start = Instant::now();
    let KafkaError::Transaction(e) = p.init_transactions(Duration::from_millis(300)).unwrap_err()
    else {
        panic!()
    };
    assert!(start.elapsed() < Duration::from_secs(5));
    assert_eq!(e.code(), RDKafkaErrorCode::OperationTimedOut, "{e}");
    assert!(e.is_retriable());
    // Another API while init_transactions is resumable conflicts.
    let KafkaError::Transaction(e) = p.begin_transaction().unwrap_err() else {
        panic!()
    };
    assert_eq!(e.code(), RDKafkaErrorCode::Conflict);
    assert_eq!(
        e.string(),
        "Conflicting init_transactions API call is already in progress"
    );
    assert!(e.is_retriable());
}

// Tests against brokers.

#[mz_ore::test]
fn produce_plain_all_codecs() {
    for brokers in test_brokers() {
        for codec in CODECS {
            let (p, rec) = producer(&brokers, &[("compression.type", codec), ("linger.ms", "5")]);
            let topic = unique(&format!("kc-prod-plain-{codec}"));
            create_topic(&cluster_of(&p), &topic, 3);
            let _drop = DropTopics::new(&cluster_of(&p), &[&topic]);
            let n = 3000;
            for i in 0..n {
                let key = format!("key-{i}");
                let value = format!("value-{i}-{}", "x".repeat(i % 50));
                let headers = OwnedHeaders::new().insert(Header {
                    key: "h",
                    value: Some(format!("{i}").as_bytes()),
                });
                p.send(
                    BaseRecord::with_opaque_to(&topic, i)
                        .key(key.as_bytes())
                        .payload(value.as_bytes())
                        .partition(i32::try_from(i % 3).unwrap())
                        .headers(headers),
                )
                .unwrap();
            }
            p.flush(Duration::from_secs(30)).unwrap();
            assert_eq!(p.in_flight_count(), 0);
            let reports = rec.reports();
            assert_eq!(reports.len(), n);
            assert!(reports.iter().all(|r| r.error.is_none()), "{codec}");
            // Per partition, offsets follow send order.
            for partition in 0..3 {
                let offsets: Vec<i64> = reports
                    .iter()
                    .filter(|r| r.partition == partition)
                    .map(|r| r.offset)
                    .collect();
                let expected: Vec<i64> = (0..i64::try_from(offsets.len()).unwrap()).collect();
                assert_eq!(offsets, expected, "{codec} partition {partition}");
                let fetched = fetch_all(&cluster_of(&p), &topic, partition, false);
                assert_eq!(fetched.len(), offsets.len());
                for (j, f) in fetched.iter().enumerate() {
                    let i = j * 3 + usize::try_from(partition).unwrap();
                    assert_eq!(f.key.as_deref(), Some(format!("key-{i}").as_bytes()));
                    assert_eq!(
                        f.value.as_deref(),
                        Some(format!("value-{i}-{}", "x".repeat(i % 50)).as_bytes())
                    );
                    assert_eq!(
                        f.headers,
                        vec![("h".to_string(), Some(format!("{i}").into_bytes()))]
                    );
                }
            }
        }
    }
}

#[mz_ore::test]
fn produce_null_and_empty_and_timestamps() {
    for brokers in test_brokers() {
        let (p, rec) = producer(&brokers, &[]);
        let topic = unique("kc-prod-null");
        create_topic(&cluster_of(&p), &topic, 1);
        let _drop = DropTopics::new(&cluster_of(&p), &[&topic]);
        let headers = OwnedHeaders::new()
            .insert(Header::<&[u8]> {
                key: "null",
                value: None,
            })
            .insert(Header {
                key: "empty",
                value: Some(b"".as_slice()),
            });
        p.send(BaseRecord::<[u8], [u8], usize>::with_opaque_to(&topic, 0).partition(0))
            .unwrap();
        p.send(
            BaseRecord::<[u8], [u8], usize>::with_opaque_to(&topic, 1)
                .partition(0)
                .key(b"")
                .payload(b"")
                .headers(headers)
                .timestamp(1234),
        )
        .unwrap();
        let before = crate::util::current_time_millis();
        // A timestamp of 0 means now, like librdkafka.
        p.send(
            BaseRecord::<[u8], [u8], usize>::with_opaque_to(&topic, 2)
                .partition(0)
                .payload(b"x")
                .timestamp(0),
        )
        .unwrap();
        p.flush(Duration::from_secs(30)).unwrap();
        assert_eq!(rec.reports().len(), 3);
        let fetched = fetch_all(&cluster_of(&p), &topic, 0, false);
        assert_eq!(fetched[0].key, None);
        assert_eq!(fetched[0].value, None);
        assert!(fetched[0].headers.is_empty());
        assert_eq!(fetched[1].key.as_deref(), Some(b"".as_slice()));
        assert_eq!(fetched[1].value.as_deref(), Some(b"".as_slice()));
        assert_eq!(fetched[1].timestamp, 1234);
        assert_eq!(
            fetched[1].headers,
            vec![
                ("null".to_string(), None),
                ("empty".to_string(), Some(vec![]))
            ]
        );
        assert!(fetched[2].timestamp >= before);
    }
}

#[mz_ore::test]
fn produce_idempotent_all_codecs() {
    for brokers in test_brokers() {
        for codec in CODECS {
            let (p, rec) = producer(
                &brokers,
                &[
                    ("compression.type", codec),
                    ("enable.idempotence", "true"),
                    ("batch.num.messages", "100"),
                ],
            );
            let topic = unique(&format!("kc-prod-idemp-{codec}"));
            create_topic(&cluster_of(&p), &topic, 2);
            let _drop = DropTopics::new(&cluster_of(&p), &[&topic]);
            let n = 2000;
            let payload = vec![b'p'; 100];
            for i in 0..n {
                p.send(
                    BaseRecord::<[u8], [u8], usize>::with_opaque_to(&topic, i)
                        .partition(i32::try_from(i % 2).unwrap())
                        .payload(&payload),
                )
                .unwrap();
            }
            p.flush(Duration::from_secs(30)).unwrap();
            let reports = rec.reports();
            assert_eq!(reports.len(), n);
            assert!(
                reports.iter().all(|r| r.error.is_none()),
                "{codec}: {:?} {:?}",
                rec.errors.lock().unwrap(),
                rec.logs.lock().unwrap()
            );
            for partition in 0..2 {
                let fetched = fetch_all(&cluster_of(&p), &topic, partition, false);
                assert_eq!(fetched.len(), n / 2, "{codec}");
            }
            // The producer id is assigned and visible in statistics.
            let mut stats = crate::statistics::Statistics::default();
            super::StatsSource::fill(&**p.inner(), &mut stats);
            let eos = stats.eos.unwrap();
            assert_eq!(eos.idemp_state, "Assigned");
            assert!(eos.producer_id >= 0);
            assert_eq!(stats.txmsgs, i64::try_from(n).unwrap());
            assert_eq!(stats.msg_cnt, 0);
        }
    }
}

/// Runs one transaction like Materialize's Kafka sink: explicit partitions
/// and headers for the data, a keyed progress record without partition.
fn sink_transaction(
    p: &ThreadedProducer<Ctx>,
    topic: &str,
    progress_topic: &str,
    first: usize,
    n: usize,
    partitions: i32,
) {
    p.begin_transaction().unwrap();
    for i in first..first + n {
        let key = format!("k{i}");
        let value = format!("v{i}");
        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "materialize-timestamp",
                value: Some(b"1700000000000".as_slice()),
            })
            .insert(Header {
                key: "materialize-sink-id",
                value: Some(b"u1".as_slice()),
            });
        p.send(BaseRecord {
            topic,
            partition: Some(i32::try_from(i).unwrap() % partitions),
            payload: Some(value.as_bytes()),
            key: Some(key.as_bytes()),
            timestamp: None,
            headers: Some(headers),
            delivery_opaque: i,
        })
        .unwrap();
    }
    let progress = format!("{{\"frontier\":[{}],\"version\":1}}", first + n);
    p.send(
        BaseRecord::with_opaque_to(progress_topic, usize::MAX)
            .payload(progress.as_bytes())
            .key(b"mz-sink-u1".as_slice()),
    )
    .unwrap();
}

#[mz_ore::test]
fn produce_transactional_commit_and_abort() {
    for brokers in test_brokers() {
        for codec in CODECS {
            let txn_id = unique("kc-txn");
            let (p, rec) = producer(
                &brokers,
                &[
                    ("compression.type", codec),
                    ("transactional.id", &txn_id),
                    ("transaction.timeout.ms", "60000"),
                    ("queue.buffering.max.ms", "10"),
                ],
            );
            let topic = unique(&format!("kc-txn-data-{codec}"));
            let progress = format!("{topic}-progress");
            let cluster = cluster_of(&p);
            create_topic(&cluster, &topic, 3);
            create_topic(&cluster, &progress, 1);
            let _drop = DropTopics::new(&cluster, &[&topic, &progress]);
            p.init_transactions(Duration::from_secs(30)).unwrap();
            // An empty transaction commits without EndTxn.
            p.begin_transaction().unwrap();
            p.commit_transaction(Duration::from_secs(30)).unwrap();
            // Committed.
            sink_transaction(&p, &topic, &progress, 0, 300, 3);
            p.commit_transaction(Duration::from_secs(30)).unwrap();
            // Aborted.
            sink_transaction(&p, &topic, &progress, 300, 300, 3);
            p.abort_transaction(Duration::from_secs(30)).unwrap();
            // Committed again.
            sink_transaction(&p, &topic, &progress, 600, 300, 3);
            p.commit_transaction(Duration::from_secs(30)).unwrap();

            let reports = rec.reports();
            // Aborted messages were purged or delivered (if sent before the
            // abort); either way every message got a report.
            assert_eq!(reports.len(), 3 * 301, "{codec}");
            let mut committed: Vec<usize> = Vec::new();
            for partition in 0..3 {
                let fetched = fetch_all(&cluster, &topic, partition, true);
                for f in fetched {
                    let i: usize = std::str::from_utf8(&f.value.unwrap()[1..])
                        .unwrap()
                        .parse()
                        .unwrap();
                    assert_eq!(
                        f.headers,
                        vec![
                            (
                                "materialize-timestamp".into(),
                                Some(b"1700000000000".to_vec())
                            ),
                            ("materialize-sink-id".into(), Some(b"u1".to_vec()))
                        ]
                    );
                    committed.push(i);
                }
            }
            committed.sort();
            let expected: Vec<usize> = (0..300).chain(600..900).collect();
            assert_eq!(committed, expected, "{codec}");
            let progress_records = fetch_all(&cluster, &progress, 0, true);
            let frontiers: Vec<_> = progress_records
                .iter()
                .map(|f| String::from_utf8(f.value.clone().unwrap().to_vec()).unwrap())
                .collect();
            assert_eq!(
                frontiers,
                vec![
                    "{\"frontier\":[300],\"version\":1}",
                    "{\"frontier\":[900],\"version\":1}"
                ]
            );
            // A read_uncommitted reader sees the aborted data too.
            let all: usize = (0..3)
                .map(|pt| fetch_all(&cluster, &topic, pt, false).len())
                .sum();
            assert!(all >= 600);
        }
    }
}

#[mz_ore::test]
fn transactional_fencing() {
    for brokers in test_brokers() {
        let txn_id = unique("kc-fence");
        let topic = unique("kc-fence-data");
        let pairs = [("transactional.id", txn_id.as_str())];
        let (old, _) = producer(&brokers, &pairs);
        create_topic(&cluster_of(&old), &topic, 1);
        let _drop = DropTopics::new(&cluster_of(&old), &[&topic]);
        old.init_transactions(Duration::from_secs(30)).unwrap();
        old.begin_transaction().unwrap();
        old.send(
            BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, 0)
                .partition(0)
                .payload(b"old".as_slice()),
        )
        .unwrap();
        old.flush(Duration::from_secs(30)).unwrap();

        let (new, _) = producer(&brokers, &pairs);
        new.init_transactions(Duration::from_secs(60)).unwrap();

        // The old producer's next transactional operation fails fatally.
        let err = old.commit_transaction(Duration::from_secs(30)).unwrap_err();
        let KafkaError::Transaction(e) = err else {
            panic!("{err:?}")
        };
        assert!(e.is_fatal(), "{e:?}");
        assert!(!e.txn_requires_abort());
        assert_eq!(e.code(), RDKafkaErrorCode::Fenced, "{e}");
        // And so does everything after.
        let KafkaError::Transaction(e2) = old.begin_transaction().unwrap_err() else {
            panic!()
        };
        assert!(e2.is_fatal());
        assert_eq!(e2.code(), e.code());
        let err = old
            .send(
                BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, 1)
                    .partition(0)
                    .payload(b"x".as_slice()),
            )
            .unwrap_err()
            .0;
        assert_eq!(err, KafkaError::MessageProduction(RDKafkaErrorCode::Fatal));

        // The new producer works, and the old transaction is aborted.
        new.begin_transaction().unwrap();
        new.send(
            BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, 2)
                .partition(0)
                .payload(b"new".as_slice()),
        )
        .unwrap();
        new.commit_transaction(Duration::from_secs(30)).unwrap();
        let values: Vec<_> = fetch_all(&cluster_of(&new), &topic, 0, true)
            .into_iter()
            .map(|f| f.value.unwrap())
            .collect();
        assert_eq!(values, vec![Bytes::from_static(b"new")]);
    }
}

#[mz_ore::test]
fn transactional_fencing_while_producing() {
    for brokers in test_brokers() {
        let txn_id = unique("kc-fence2");
        let topic = unique("kc-fence2-data");
        let pairs = [("transactional.id", txn_id.as_str())];
        let (old, rec) = producer(&brokers, &pairs);
        create_topic(&cluster_of(&old), &topic, 1);
        let _drop = DropTopics::new(&cluster_of(&old), &[&topic]);
        old.init_transactions(Duration::from_secs(30)).unwrap();
        old.begin_transaction().unwrap();
        old.send(
            BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, 0)
                .partition(0)
                .payload(b"a".as_slice()),
        )
        .unwrap();
        old.flush(Duration::from_secs(30)).unwrap();
        let (new, _) = producer(&brokers, &pairs);
        new.init_transactions(Duration::from_secs(60)).unwrap();
        // The old producer still thinks it is in a transaction; its next
        // produce request fails with a fenced epoch.
        old.send(
            BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, 1)
                .partition(0)
                .payload(b"b".as_slice()),
        )
        .unwrap();
        let err = old.commit_transaction(Duration::from_secs(30)).unwrap_err();
        let KafkaError::Transaction(e) = err else {
            panic!("{err:?}")
        };
        assert!(e.is_fatal(), "{e:?}");
        let reports = rec.reports();
        assert_eq!(reports.len(), 2);
        assert!(reports[1].error.is_some());
    }
}

#[mz_ore::test]
fn transactional_abortable_error() {
    for brokers in test_brokers() {
        let txn_id = unique("kc-abortable");
        let (p, rec) = producer(
            &brokers,
            &[
                ("transactional.id", &txn_id),
                ("message.max.bytes", "2000000"),
            ],
        );
        let topic = unique("kc-abortable-data");
        let cluster = cluster_of(&p);
        create_topic(&cluster, &topic, 1);
        let _drop = DropTopics::new(&cluster, &[&topic]);
        p.init_transactions(Duration::from_secs(30)).unwrap();
        p.begin_transaction().unwrap();
        // Larger than the broker's limit (1 MiB by default), within ours.
        let big = vec![b'x'; 1_500_000];
        p.send(
            BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, 0)
                .partition(0)
                .payload(&big),
        )
        .unwrap();
        let err = p.commit_transaction(Duration::from_secs(30)).unwrap_err();
        let KafkaError::Transaction(e) = err else {
            panic!("{err:?}")
        };
        assert!(e.txn_requires_abort(), "{e:?}");
        assert!(!e.is_fatal() && !e.is_retriable());
        // The report carries the message.
        let reports = rec.reports();
        assert_eq!(reports.len(), 1);
        assert!(reports[0].error.is_some());
        assert_eq!(reports[0].payload.as_deref(), Some(big.as_slice()));
        // Abort, then the producer is usable again.
        p.abort_transaction(Duration::from_secs(30)).unwrap();
        p.begin_transaction().unwrap();
        p.send(
            BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, 1)
                .partition(0)
                .payload(b"ok".as_slice()),
        )
        .unwrap();
        p.commit_transaction(Duration::from_secs(30)).unwrap();
        let values: Vec<_> = fetch_all(&cluster, &topic, 0, true)
            .into_iter()
            .map(|f| f.value.unwrap())
            .collect();
        assert_eq!(values, vec![Bytes::from_static(b"ok")]);
    }
}

#[mz_ore::test]
fn unknown_partition_and_topic() {
    for brokers in test_brokers() {
        let (p, rec) = producer(
            &brokers,
            &[
                ("allow.auto.create.topics", "false"),
                ("topic.metadata.propagation.max.ms", "1000"),
            ],
        );
        let topic = unique("kc-unknown-part");
        create_topic(&cluster_of(&p), &topic, 2);
        let _drop = DropTopics::new(&cluster_of(&p), &[&topic]);
        // Learn the metadata the way the sink does.
        p.client()
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        let err = p
            .send(
                BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, 0)
                    .partition(2)
                    .payload(b"x".as_slice()),
            )
            .unwrap_err()
            .0;
        assert_eq!(
            err,
            KafkaError::MessageProduction(RDKafkaErrorCode::UnknownPartition)
        );
        p.send(
            BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, 1)
                .partition(1)
                .payload(b"x".as_slice()),
        )
        .unwrap();
        // A topic that does not exist: queued, then failed once metadata
        // says so and the propagation window passed.
        let missing = unique("kc-missing");
        p.send(BaseRecord::<(), [u8], usize>::with_opaque_to(&missing, 2).payload(b"x".as_slice()))
            .unwrap();
        p.flush(Duration::from_secs(30)).unwrap();
        let reports = rec.reports();
        let missing_report = reports.iter().find(|r| r.opaque == 2).unwrap();
        assert_eq!(
            missing_report.error,
            Some(RDKafkaErrorCode::UnknownTopicOrPartition)
        );
        // Now known not to exist: rejected synchronously.
        let err = p
            .send(
                BaseRecord::<(), [u8], usize>::with_opaque_to(&missing, 3).payload(b"x".as_slice()),
            )
            .unwrap_err()
            .0;
        assert_eq!(
            err,
            KafkaError::MessageProduction(RDKafkaErrorCode::UnknownTopic)
        );
    }
}

#[mz_ore::test]
fn keyed_partitioning_matches_librdkafka() {
    for brokers in test_brokers() {
        let (p, rec) = producer(&brokers, &[]);
        let topic = unique("kc-keyed");
        create_topic(&cluster_of(&p), &topic, 7);
        let _drop = DropTopics::new(&cluster_of(&p), &[&topic]);
        for i in 0..50 {
            let key = format!("key-{i}");
            p.send(
                BaseRecord::<[u8], [u8], usize>::with_opaque_to(&topic, i)
                    .key(key.as_bytes())
                    .payload(b"v"),
            )
            .unwrap();
        }
        // Null keys stick to one partition for a while.
        for i in 50..60 {
            p.send(BaseRecord::<[u8], [u8], usize>::with_opaque_to(&topic, i).payload(b"v"))
                .unwrap();
        }
        p.flush(Duration::from_secs(30)).unwrap();
        let reports = rec.reports();
        for r in &reports {
            if r.opaque < 50 {
                let key = format!("key-{}", r.opaque);
                let expected = i32::try_from(crc32fast::hash(key.as_bytes()) % 7).unwrap();
                assert_eq!(r.partition, expected);
            }
        }
        let sticky: std::collections::BTreeSet<i32> = reports
            .iter()
            .filter(|r| r.opaque >= 50)
            .map(|r| r.partition)
            .collect();
        assert_eq!(sticky.len(), 1, "{reports:?}");
    }
}

#[mz_ore::test]
fn future_producer() {
    for brokers in test_brokers() {
        let producer: FutureProducer = FutureProducer::from_config(&config(&brokers, &[])).unwrap();
        let topic = unique("kc-future");
        create_topic(producer.client().cluster(), &topic, 1);
        let _drop = DropTopics::new(producer.client().cluster(), &[&topic]);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            for i in 0..10i64 {
                let value = format!("v{i}");
                let (partition, offset) = producer
                    .send(
                        FutureRecord::<(), _>::to(&topic).payload(value.as_bytes()),
                        Duration::from_secs(1),
                    )
                    .await
                    .unwrap();
                assert_eq!((partition, offset), (0, i));
            }
            // A failed delivery returns the message.
            let missing_partition = producer
                .send(
                    FutureRecord::<(), _>::to(&topic)
                        .payload(b"x".as_slice())
                        .partition(5),
                    Duration::from_secs(1),
                )
                .await
                .unwrap_err();
            assert_eq!(
                missing_partition.0,
                KafkaError::MessageProduction(RDKafkaErrorCode::UnknownPartition)
            );
            assert_eq!(missing_partition.1.payload(), Some(b"x".as_slice()));
            let fut = producer
                .send_result(FutureRecord::<(), _>::to(&topic).payload(b"y".as_slice()))
                .unwrap();
            let delivered = fut.await.unwrap().unwrap();
            assert_eq!(delivered, (0, 10));
        });
        // Flushing a FutureProducer with nothing outstanding succeeds.
        producer
            .flush(Timeout::After(Duration::from_secs(1)))
            .unwrap();
    }
}

#[mz_ore::test]
fn statistics_report_producer_fields() {
    for brokers in test_brokers() {
        let txn_id = unique("kc-stats");
        let (p, rec) = producer(
            &brokers,
            &[
                ("transactional.id", &txn_id),
                ("statistics.interval.ms", "100"),
            ],
        );
        let topic = unique("kc-stats-data");
        create_topic(&cluster_of(&p), &topic, 1);
        let _drop = DropTopics::new(&cluster_of(&p), &[&topic]);
        p.init_transactions(Duration::from_secs(30)).unwrap();
        p.begin_transaction().unwrap();
        for i in 0..100 {
            p.send(
                BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, i)
                    .partition(0)
                    .payload(b"abc".as_slice()),
            )
            .unwrap();
        }
        p.commit_transaction(Duration::from_secs(30)).unwrap();
        wait_for("statistics", Duration::from_secs(10), || {
            rec.stats.lock().unwrap().iter().any(|s| s.txmsgs == 100)
        });
        let stats = rec.stats.lock().unwrap().last().unwrap().clone();
        assert_eq!(stats.msg_cnt, 0);
        assert_eq!(stats.txmsg_bytes, 300);
        let eos = stats.eos.unwrap();
        assert_eq!(eos.txn_state, "Ready");
        assert!(stats.brokers.values().any(|b| b.tx > 0));
    }
}

#[mz_ore::test]
fn delivery_reports_with_contents_and_acks_zero() {
    for brokers in test_brokers() {
        let ctx = FullCtx::default();
        let rec = Arc::clone(&ctx.0.0);
        let p: ThreadedProducer<FullCtx> = ThreadedProducer::from_config_and_context(
            &config(&brokers, &[("compression.type", "lz4")]),
            ctx,
        )
        .unwrap();
        let topic = unique("kc-contents");
        create_topic(&cluster_of(&p), &topic, 1);
        let _drop = DropTopics::new(&cluster_of(&p), &[&topic]);
        for i in 0..100 {
            let key = format!("k{i}");
            let value = "v".repeat(i);
            p.send(
                BaseRecord::<[u8], [u8], usize>::with_opaque_to(&topic, i)
                    .partition(0)
                    .key(key.as_bytes())
                    .payload(value.as_bytes()),
            )
            .unwrap();
        }
        p.flush(Duration::from_secs(30)).unwrap();
        let reports = rec.reports();
        assert_eq!(reports.len(), 100);
        for (i, r) in reports.iter().enumerate() {
            assert_eq!(r.opaque, i);
            assert_eq!(r.offset, i64::try_from(i).unwrap());
            assert_eq!(r.key.as_deref(), Some(format!("k{i}").as_bytes()));
            assert_eq!(r.payload.as_deref(), Some("v".repeat(i).as_bytes()));
        }

        // acks=0: delivered once written, without offsets.
        let (p0, rec0) = producer(&brokers, &[("acks", "0")]);
        for i in 0..10 {
            p0.send(
                BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, i)
                    .partition(0)
                    .payload(b"zero".as_slice()),
            )
            .unwrap();
        }
        p0.flush(Duration::from_secs(30)).unwrap();
        let reports = rec0.reports();
        assert_eq!(reports.len(), 10);
        assert!(
            reports
                .iter()
                .all(|r| r.error.is_none() && r.offset == -1001)
        );
        wait_for("acks=0 records", Duration::from_secs(10), || {
            fetch_all(&cluster_of(&p), &topic, 0, false).len() == 110
        });
    }
}

/// Restarts a broker container while an idempotent producer and a
/// transactional producer stream records. Ignored: it restarts shared
/// containers. Run with `MZ_KAFKA_CLIENT_TEST_RESTART='<brokers>=<container>'`.
#[mz_ore::test]
#[ignore]
fn broker_restart_mid_produce() {
    let Ok(spec) = std::env::var("MZ_KAFKA_CLIENT_TEST_RESTART") else {
        return;
    };
    let (brokers, container) = spec.split_once('=').expect("<brokers>=<container>");
    let (p, rec) = producer(
        brokers,
        &[("enable.idempotence", "true"), ("compression.type", "lz4")],
    );
    let txn_id = unique("kc-restart-txn");
    let (t, _) = producer(
        brokers,
        &[
            ("transactional.id", &txn_id),
            ("transaction.timeout.ms", "60000"),
        ],
    );
    let topic = unique("kc-restart");
    let txn_topic = unique("kc-restart-txn-data");
    let cluster = cluster_of(&p);
    create_topic(&cluster, &topic, 2);
    create_topic(&cluster, &txn_topic, 2);
    let _drop = DropTopics::new(&cluster, &[&topic, &txn_topic]);
    t.init_transactions(Duration::from_secs(30)).unwrap();

    let container = container.to_string();
    let restarter = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(2));
        let status = std::process::Command::new("docker")
            .args(["restart", "-t", "1", &container])
            .status()
            .unwrap();
        assert!(status.success());
    });
    let n = 40_000;
    let mut committed = Vec::new();
    let mut txn_errors = Vec::new();
    let start = Instant::now();
    for i in 0..n {
        let v = format!("{i}");
        p.send(
            BaseRecord::<(), [u8], usize>::with_opaque_to(&topic, i)
                .partition(i32::try_from(i % 2).unwrap())
                .payload(v.as_bytes()),
        )
        .unwrap();
        if i % 1000 == 999 {
            // A transaction of 100 records, aborted on an abortable error
            // like the sink does.
            let first = i - 99;
            let r = t.begin_transaction().and_then(|()| {
                for j in first..=i {
                    let v = format!("{j}");
                    t.send(
                        BaseRecord::<(), [u8], usize>::with_opaque_to(&txn_topic, j)
                            .partition(i32::try_from(j % 2).unwrap())
                            .payload(v.as_bytes()),
                    )
                    .map_err(|(e, _)| e)?;
                }
                t.commit_transaction(Duration::from_secs(60))
            });
            match r {
                Ok(()) => committed.extend(first..=i),
                Err(KafkaError::Transaction(e)) if e.txn_requires_abort() => {
                    txn_errors.push(e.to_string());
                    t.abort_transaction(Duration::from_secs(60)).unwrap();
                }
                Err(e) => panic!("unexpected transaction error {e:?}"),
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    if let Err(e) = p.flush(Duration::from_secs(120)) {
        let mut stats = crate::statistics::Statistics::default();
        super::StatsSource::fill(&**p.inner(), &mut stats);
        panic!(
            "flush failed: {e}; topics {:?}; eos {:?}; errors {:?}",
            stats.topics,
            stats.eos,
            rec.errors.lock().unwrap()
        );
    }
    restarter.join().unwrap();
    eprintln!(
        "produced {n} in {:?}, {} transactions committed, aborted: {txn_errors:?}",
        start.elapsed(),
        committed.len() / 100
    );
    // Every record was delivered exactly once, in order.
    let reports = rec.reports();
    assert_eq!(reports.len(), n);
    assert!(reports.iter().all(|r| r.error.is_none()), "{reports:?}");
    for partition in 0..2 {
        let values: Vec<usize> = fetch_all(&cluster, &topic, partition, false)
            .into_iter()
            .map(|f| {
                std::str::from_utf8(&f.value.unwrap())
                    .unwrap()
                    .parse()
                    .unwrap()
            })
            .collect();
        let expected: Vec<usize> = (0..n)
            .filter(|i| i % 2 == usize::try_from(partition).unwrap())
            .collect();
        assert_eq!(values, expected);
    }
    // Committed transactions are visible exactly once, aborted ones not.
    let mut visible: Vec<usize> = (0..2)
        .flat_map(|partition| fetch_all(&cluster, &txn_topic, partition, true))
        .map(|f| {
            std::str::from_utf8(&f.value.unwrap())
                .unwrap()
                .parse()
                .unwrap()
        })
        .collect();
    visible.sort();
    assert_eq!(visible, committed);
}

/// A transaction producing to a topic deleted after an earlier transaction
/// fails with an abortable error within `topic.metadata.propagation.max.ms`
/// of the deletion becoming visible, instead of retrying until
/// `message.timeout.ms`. Mirrors the Kafka sink: no topic auto-creation,
/// explicit partitions, metadata learned and refreshed with
/// `fetch_metadata(None)`, a long transaction timeout.
#[mz_ore::test]
fn deleted_topic_fails_transaction() {
    for brokers in test_brokers() {
        let txn_id = unique("kc-deleted");
        let (p, rec) = producer(
            &brokers,
            &[
                ("allow.auto.create.topics", "false"),
                ("transactional.id", &txn_id),
                ("transaction.timeout.ms", "600000"),
                ("topic.metadata.propagation.max.ms", "2000"),
            ],
        );
        // Named like the sink's topics: the progress topic sorts first.
        let progress = unique("kc-deleted-a-progress");
        let topic = unique("kc-deleted-b-data");
        let cluster = cluster_of(&p);
        create_topic(&cluster, &progress, 1);
        create_topic(&cluster, &topic, 1);
        let _drop = DropTopics::new(&cluster, &[&progress]);
        p.client()
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        p.init_transactions(Duration::from_secs(30)).unwrap();
        sink_transaction(&p, &topic, &progress, 0, 1, 1);
        p.commit_transaction(Duration::from_secs(30)).unwrap();

        drop(DropTopics::new(&cluster, &[&topic]));
        let start = Instant::now();
        sink_transaction(&p, &topic, &progress, 1, 1, 1);
        // Like the sink's partition count task.
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let refresher = {
            let p = p.clone();
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                    let _ = p.client().fetch_metadata(None, Duration::from_secs(10));
                    std::thread::sleep(Duration::from_secs(1));
                }
            })
        };
        let res = p.commit_transaction(Duration::from_secs(60));
        stop.store(true, std::sync::atomic::Ordering::SeqCst);
        refresher.join().unwrap();
        let err = res.unwrap_err();
        let KafkaError::Transaction(e) = &err else {
            panic!("{err:?}")
        };
        assert!(e.txn_requires_abort(), "{e:?}");
        let elapsed = start.elapsed();
        assert!(elapsed < Duration::from_secs(40), "{elapsed:?}");
        p.abort_transaction(Duration::from_secs(30)).unwrap();
        let report = rec.reports().into_iter().find(|r| r.opaque == 1).unwrap();
        assert!(report.error.is_some(), "{report:?}");

        // The producer stays usable for the remaining topics.
        p.begin_transaction().unwrap();
        p.send(
            BaseRecord::with_opaque_to(&progress, 2)
                .payload(b"after".as_slice())
                .key(b"mz-sink-u1".as_slice()),
        )
        .unwrap();
        p.commit_transaction(Duration::from_secs(30)).unwrap();
        let values: Vec<_> = fetch_all(&cluster, &progress, 0, true)
            .into_iter()
            .map(|f| f.value.unwrap())
            .collect();
        assert_eq!(values.last(), Some(&Bytes::from_static(b"after")));
    }
}
