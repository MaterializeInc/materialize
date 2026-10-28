// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the consumer against brokers.
//!
//! They run only when `MZ_KAFKA_CLIENT_TEST_BROKERS` is set to one or more
//! `;`-separated bootstrap server lists, e.g.
//! `127.0.0.1:19092;127.0.0.1:29092`. Test data is produced with raw
//! requests, including transactions, so that these tests do not depend on
//! the producer engine.

#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::StreamExt;
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::create_partitions_request::CreatePartitionsTopic;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, CreatePartitionsRequest, CreateTopicsRequest, DeleteTopicsRequest,
    EndTxnRequest, InitProducerIdRequest, ProduceRequest, ProducerId, TopicName, TransactionalId,
};
use kafka_protocol::protocol::StrBytes;

use crate::client::{Client, ClientContext, DefaultClientContext};
use crate::config::settings::{ClientKind, ClientSettings};
use crate::config::{ClientConfig, FromClientConfigAndContext, RDKafkaLogLevel};
use crate::consumer::base_consumer::PartitionQueue;
use crate::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer,
};
use crate::error::{KafkaError, KafkaResult};
use crate::internal::cluster::{Cluster, CoordinatorKind};
use crate::internal::compression::Compression;
use crate::internal::net::RequestOptions;
use crate::internal::records::{BatchBuilder, ProducerIdentity, RecordHeader};
use crate::internal::runtime;
use crate::message::{Headers, Message, Timestamp};
use crate::topic_partition_list::{Offset, TopicPartitionList};
use crate::types::RDKafkaErrorCode;
use crate::util::Deadline;

use super::fetcher;

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

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// Records callbacks of a consumer.
#[derive(Default)]
struct Recorder {
    logs: Mutex<Vec<(RDKafkaLogLevel, String, String)>>,
    errors: Mutex<Vec<(KafkaError, String)>>,
    rebalances: Mutex<Vec<String>>,
    commits: Mutex<Vec<KafkaResult<()>>>,
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

    /// The auth test broker advertises its listeners as `kafka:<port>`.
    fn resolve_broker_addr(
        &self,
        host: &str,
        port: u16,
    ) -> Result<Vec<std::net::SocketAddr>, std::io::Error> {
        use std::net::ToSocketAddrs;
        let host = if host == "kafka" { "127.0.0.1" } else { host };
        (host, port).to_socket_addrs().map(|a| a.collect())
    }
}

impl ConsumerContext for Ctx {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
        let text = match rebalance {
            Rebalance::Assign(tpl) => format!("pre-assign {}", tpl.count()),
            Rebalance::Revoke(tpl) => format!("pre-revoke {}", tpl.count()),
            Rebalance::Error(e) => format!("error {e}"),
        };
        self.0.rebalances.lock().unwrap().push(text);
    }

    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        let text = match rebalance {
            Rebalance::Assign(tpl) => format!("post-assign {}", tpl.count()),
            Rebalance::Revoke(tpl) => format!("post-revoke {}", tpl.count()),
            Rebalance::Error(e) => format!("error {e}"),
        };
        self.0.rebalances.lock().unwrap().push(text);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        self.0.commits.lock().unwrap().push(result);
    }
}

impl Recorder {
    fn logs_with(&self, fac: &str) -> Vec<String> {
        self.logs
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, f, _)| f == fac)
            .map(|(_, _, m)| m.clone())
            .collect()
    }
}

fn config(brokers: &str, pairs: &[(&str, &str)]) -> ClientConfig {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers);
    config.set_log_level(RDKafkaLogLevel::Info);
    for (k, v) in pairs {
        config.set(*k, *v);
    }
    config
}

fn consumer(brokers: &str, pairs: &[(&str, &str)]) -> (Arc<BaseConsumer<Ctx>>, Arc<Recorder>) {
    let ctx = Ctx::default();
    let recorder = Arc::clone(&ctx.0);
    let consumer = BaseConsumer::from_config_and_context(&config(brokers, pairs), ctx).unwrap();
    (Arc::new(consumer), recorder)
}

/// A client for raw admin and produce requests.
struct Admin {
    client: Client<DefaultClientContext>,
}

impl Admin {
    fn new(brokers: &str) -> Admin {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", brokers);
        let settings = ClientSettings::from_config(&config, ClientKind::Producer).unwrap();
        Admin {
            client: Client::new(settings, DefaultClientContext).unwrap(),
        }
    }

    fn cluster(&self) -> Arc<Cluster> {
        Arc::clone(self.client.cluster())
    }

    /// Creates a topic with a unique name that is deleted when the returned
    /// guard drops.
    fn new_topic(&self, prefix: &str, partitions: i32) -> TopicGuard {
        let name = unique(prefix);
        self.create_topic(&name, partitions);
        TopicGuard {
            cluster: self.cluster(),
            name,
        }
    }

    fn create_topic(&self, name: &str, partitions: i32) {
        let cluster = self.cluster();
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
            for p in 0..partitions {
                wait_for_leader(&cluster, &name, p, deadline).await;
            }
        })
    }

    fn create_partitions(&self, name: &str, total: i32) {
        let cluster = self.cluster();
        let name = name.to_string();
        runtime::block_on(async move {
            let deadline = Deadline::new(Duration::from_secs(30));
            let controller = cluster.controller(deadline).await.unwrap();
            let response = controller
                .request::<CreatePartitionsRequest, _>(
                    |_| {
                        CreatePartitionsRequest::default()
                            .with_timeout_ms(10_000)
                            .with_topics(vec![
                                CreatePartitionsTopic::default()
                                    .with_name(TopicName(StrBytes::from_string(name.clone())))
                                    .with_count(total)
                                    .with_assignments(None),
                            ])
                    },
                    RequestOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(response.results[0].error_code, 0, "{response:?}");
            let topics = [name.clone()];
            loop {
                let md = cluster.refresh_metadata(&topics, deadline).await.unwrap();
                if md.partition_count(&name) == Some(usize::try_from(total).unwrap()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            for p in 0..total {
                wait_for_leader(&cluster, &name, p, deadline).await;
            }
        })
    }

    fn delete_topic(&self, name: &str) {
        let cluster = self.cluster();
        let name = name.to_string();
        runtime::block_on(async move {
            let deadline = Deadline::new(Duration::from_secs(30));
            let controller = cluster.controller(deadline).await.unwrap();
            let response = controller
                .request::<DeleteTopicsRequest, _>(
                    |_| {
                        DeleteTopicsRequest::default()
                            .with_timeout_ms(10_000)
                            .with_topic_names(vec![TopicName(StrBytes::from_string(name.clone()))])
                    },
                    RequestOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(response.responses[0].error_code, 0, "{response:?}");
        })
    }

    /// Produces records to a partition with acks=all, returning the base
    /// offset.
    fn produce(&self, topic: &str, partition: i32, records: &[Rec<'_>]) -> i64 {
        self.produce_with(topic, partition, records, Compression::None, None)
    }

    fn produce_with(
        &self,
        topic: &str,
        partition: i32,
        records: &[Rec<'_>],
        compression: Compression,
        txn: Option<(&str, ProducerIdentity, i32)>,
    ) -> i64 {
        let mut batch = BatchBuilder::new(compression, None, txn.is_some());
        for r in records {
            let headers: Vec<RecordHeader<'_>> = r
                .headers
                .iter()
                .map(|(key, value)| RecordHeader { key, value: *value })
                .collect();
            batch.append(r.timestamp.unwrap_or_else(now_ms), r.key, r.value, &headers);
        }
        let (ident, seq) = match txn {
            Some((_, ident, seq)) => (ident, seq),
            None => (ProducerIdentity::NONE, -1),
        };
        let records = batch.build(ident, seq).unwrap().bytes();
        let cluster = self.cluster();
        let topic = topic.to_string();
        let txn_id = txn.map(|(id, _, _)| id.to_string());
        runtime::block_on(async move {
            let deadline = Deadline::new(Duration::from_secs(30));
            loop {
                let leader = cluster.leader(&topic, partition, deadline).await.unwrap();
                let response = leader
                    .request::<ProduceRequest, _>(
                        |_| {
                            ProduceRequest::default()
                                .with_transactional_id(
                                    txn_id
                                        .clone()
                                        .map(|t| TransactionalId(StrBytes::from_string(t))),
                                )
                                .with_acks(-1)
                                .with_timeout_ms(10_000)
                                .with_topic_data(vec![
                                    TopicProduceData::default()
                                        .with_name(TopicName(StrBytes::from_string(topic.clone())))
                                        .with_partition_data(vec![
                                            PartitionProduceData::default()
                                                .with_index(partition)
                                                .with_records(Some(records.clone())),
                                        ]),
                                ])
                        },
                        RequestOptions::default(),
                    )
                    .await
                    .unwrap();
                let p = &response.responses[0].partition_responses[0];
                // A new topic's leader may not be ready yet.
                if matches!(
                    RDKafkaErrorCode::from(p.error_code),
                    RDKafkaErrorCode::NotLeaderForPartition
                        | RDKafkaErrorCode::LeaderNotAvailable
                        | RDKafkaErrorCode::UnknownTopicOrPartition
                        | RDKafkaErrorCode::NotEnoughReplicas
                ) && !deadline.is_elapsed()
                {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let _ = cluster
                        .refresh_metadata(std::slice::from_ref(&topic), deadline)
                        .await;
                    continue;
                }
                assert_eq!(p.error_code, 0, "{response:?}");
                return p.base_offset;
            }
        })
    }

    fn values(&self, topic: &str, partition: i32, values: &[&str]) -> i64 {
        let records: Vec<Rec<'_>> = values.iter().map(|v| Rec::value(v.as_bytes())).collect();
        self.produce(topic, partition, &records)
    }
}

/// A test topic, deleted on drop.
struct TopicGuard {
    cluster: Arc<Cluster>,
    name: String,
}

impl Drop for TopicGuard {
    fn drop(&mut self) {
        let cluster = Arc::clone(&self.cluster);
        let name = self.name.clone();
        let _ = runtime::block_on(async move {
            let deadline = Deadline::new(Duration::from_secs(10));
            let controller = cluster.controller(deadline).await?;
            controller
                .request::<DeleteTopicsRequest, _>(
                    |_| {
                        DeleteTopicsRequest::default()
                            .with_timeout_ms(10_000)
                            .with_topic_names(vec![TopicName(StrBytes::from_string(name))])
                    },
                    RequestOptions::default(),
                )
                .await
        });
    }
}

/// A record to produce.
#[derive(Clone, Default)]
struct Rec<'a> {
    key: Option<&'a [u8]>,
    value: Option<&'a [u8]>,
    headers: Vec<(&'a str, Option<&'a [u8]>)>,
    timestamp: Option<i64>,
}

impl<'a> Rec<'a> {
    fn value(v: &'a [u8]) -> Rec<'a> {
        Rec {
            value: Some(v),
            ..Rec::default()
        }
    }
}

/// A transactional producer built from raw requests.
struct Txn<'a> {
    admin: &'a Admin,
    id: String,
    ident: ProducerIdentity,
    sequences: BTreeMap<(String, i32), i32>,
    added: Vec<(String, i32)>,
}

/// Retries a transactional request while the coordinator is busy.
fn txn_retriable(code: i16) -> bool {
    matches!(
        RDKafkaErrorCode::from(code),
        RDKafkaErrorCode::ConcurrentTransactions
            | RDKafkaErrorCode::CoordinatorLoadInProgress
            | RDKafkaErrorCode::CoordinatorNotAvailable
            | RDKafkaErrorCode::NotCoordinator
    )
}

impl<'a> Txn<'a> {
    fn new(admin: &'a Admin) -> Txn<'a> {
        let id = unique("mzkc-txn");
        let cluster = admin.cluster();
        let txn_id = id.clone();
        let ident = runtime::block_on(async move {
            let deadline = Deadline::new(Duration::from_secs(60));
            loop {
                let coordinator = cluster
                    .coordinator(CoordinatorKind::Transaction, &txn_id, deadline)
                    .await
                    .unwrap();
                let response = coordinator
                    .request::<InitProducerIdRequest, _>(
                        |_| {
                            InitProducerIdRequest::default()
                                .with_transactional_id(Some(TransactionalId(
                                    StrBytes::from_string(txn_id.clone()),
                                )))
                                .with_transaction_timeout_ms(60_000)
                        },
                        RequestOptions::default(),
                    )
                    .await
                    .unwrap();
                if txn_retriable(response.error_code) {
                    cluster.invalidate_coordinator(CoordinatorKind::Transaction, &txn_id);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
                assert_eq!(response.error_code, 0, "{response:?}");
                return ProducerIdentity {
                    producer_id: response.producer_id.0,
                    producer_epoch: response.producer_epoch,
                };
            }
        });
        Txn {
            admin,
            id,
            ident,
            sequences: BTreeMap::new(),
            added: Vec::new(),
        }
    }

    fn add_partition(&mut self, topic: &str, partition: i32) {
        if self
            .added
            .iter()
            .any(|(t, p)| t == topic && *p == partition)
        {
            return;
        }
        self.added.push((topic.to_string(), partition));
        let cluster = self.admin.cluster();
        let (id, ident, topic) = (self.id.clone(), self.ident, topic.to_string());
        runtime::block_on(async move {
            let deadline = Deadline::new(Duration::from_secs(60));
            loop {
                let coordinator = cluster
                    .coordinator(CoordinatorKind::Transaction, &id, deadline)
                    .await
                    .unwrap();
                let response = coordinator
                    .request::<AddPartitionsToTxnRequest, _>(
                        |_| {
                            AddPartitionsToTxnRequest::default()
                                .with_v3_and_below_transactional_id(TransactionalId(
                                    StrBytes::from_string(id.clone()),
                                ))
                                .with_v3_and_below_producer_id(ProducerId(ident.producer_id))
                                .with_v3_and_below_producer_epoch(ident.producer_epoch)
                                .with_v3_and_below_topics(vec![
                                    AddPartitionsToTxnTopic::default()
                                        .with_name(TopicName(StrBytes::from_string(topic.clone())))
                                        .with_partitions(vec![partition]),
                                ])
                        },
                        RequestOptions::default(),
                    )
                    .await
                    .unwrap();
                let code = response.results_by_topic_v3_and_below[0].results_by_partition[0]
                    .partition_error_code;
                if txn_retriable(code) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                assert_eq!(code, 0, "{response:?}");
                return;
            }
        })
    }

    /// Produces values in the transaction, returning the base offset.
    fn produce(&mut self, topic: &str, partition: i32, values: &[&str]) -> i64 {
        self.add_partition(topic, partition);
        let key = (topic.to_string(), partition);
        let seq = *self.sequences.get(&key).unwrap_or(&0);
        let records: Vec<Rec<'_>> = values.iter().map(|v| Rec::value(v.as_bytes())).collect();
        let offset = self.admin.produce_with(
            topic,
            partition,
            &records,
            Compression::None,
            Some((&self.id, self.ident, seq)),
        );
        self.sequences
            .insert(key, seq + i32::try_from(values.len()).unwrap());
        offset
    }

    fn end(self, commit: bool) {
        let cluster = self.admin.cluster();
        let (id, ident) = (self.id.clone(), self.ident);
        runtime::block_on(async move {
            let deadline = Deadline::new(Duration::from_secs(60));
            loop {
                let coordinator = cluster
                    .coordinator(CoordinatorKind::Transaction, &id, deadline)
                    .await
                    .unwrap();
                let response = coordinator
                    .request::<EndTxnRequest, _>(
                        |_| {
                            EndTxnRequest::default()
                                .with_transactional_id(TransactionalId(StrBytes::from_string(
                                    id.clone(),
                                )))
                                .with_producer_id(ProducerId(ident.producer_id))
                                .with_producer_epoch(ident.producer_epoch)
                                .with_committed(commit)
                        },
                        RequestOptions::default(),
                    )
                    .await
                    .unwrap();
                if txn_retriable(response.error_code) {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
                assert_eq!(response.error_code, 0, "{response:?}");
                return;
            }
        })
    }
}

/// What a poll returned, for comparisons.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Polled {
    Msg(i32, i64, Option<String>),
    Eof(i32),
    Err(String),
}

fn describe(result: KafkaResult<crate::message::BorrowedMessage<'_>>) -> Polled {
    match result {
        Ok(m) => Polled::Msg(
            m.partition(),
            m.offset(),
            m.payload().map(|p| String::from_utf8_lossy(p).into_owned()),
        ),
        Err(KafkaError::PartitionEOF(p)) => Polled::Eof(p),
        Err(e) => Polled::Err(e.to_string()),
    }
}

/// Polls the main queue until `pred` holds for the collected items or the
/// timeout passes.
fn poll_until(
    consumer: &BaseConsumer<Ctx>,
    timeout: Duration,
    mut pred: impl FnMut(&[Polled]) -> bool,
) -> Vec<Polled> {
    let deadline = Instant::now() + timeout;
    let mut out = Vec::new();
    while !pred(&out) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("timed out; got {out:?}");
        }
        if let Some(r) = consumer.poll(remaining.min(Duration::from_millis(200))) {
            out.push(describe(r));
        }
    }
    out
}

fn queue_until(
    queue: &PartitionQueue<Ctx>,
    timeout: Duration,
    mut pred: impl FnMut(&[Polled]) -> bool,
) -> Vec<Polled> {
    let deadline = Instant::now() + timeout;
    let mut out = Vec::new();
    while !pred(&out) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            panic!("timed out; got {out:?}");
        }
        if let Some(r) = queue.poll(remaining.min(Duration::from_millis(200))) {
            out.push(describe(r));
        }
    }
    out
}

fn position(consumer: &BaseConsumer<Ctx>, topic: &str, partition: i32) -> Offset {
    consumer
        .position()
        .unwrap()
        .find_partition(topic, partition)
        .unwrap()
        .offset()
}

fn tpl(entries: &[(&str, i32, Offset)]) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();
    for (t, p, o) in entries {
        tpl.add_partition_offset(t, *p, *o).unwrap();
    }
    tpl
}

fn msgs(items: &[Polled]) -> Vec<i64> {
    items
        .iter()
        .filter_map(|p| match p {
            Polled::Msg(_, o, _) => Some(*o),
            _ => None,
        })
        .collect()
}

/// A consumer whose principal may not use its group learns so from the
/// coordinator lookup that librdkafka starts at creation for every consumer
/// with a `group.id`: polling returns `GroupAuthorizationFailed` once, also
/// for a consumer that only assigns partitions (Materialize's source and
/// sink progress consumers).
///
/// Runs when `MZ_KAFKA_CLIENT_TEST_AUTH_SECRETS` is set, against the broker
/// of `test/kafka-auth` (see `internal/cluster/auth_tests.rs`), whose
/// `materialize_lockdown` user may not use groups outside the prefix `lockdown`.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn group_authorization_failed() {
    if std::env::var("MZ_KAFKA_CLIENT_TEST_AUTH_SECRETS").is_err() {
        return;
    }
    let sasl = |user: &str| {
        vec![
            ("security.protocol", "SASL_PLAINTEXT".to_string()),
            ("sasl.mechanisms", "PLAIN".to_string()),
            ("sasl.username", user.to_string()),
            ("sasl.password", "sekurity".to_string()),
            ("group.id", unique("mzkc-auth-group")),
        ]
    };
    let poll_for = |consumer: &BaseConsumer<Ctx>, d: Duration| {
        let deadline = Instant::now() + d;
        let mut out = Vec::new();
        while Instant::now() < deadline {
            match consumer.poll(Duration::from_millis(100)) {
                Some(Ok(m)) => out.push(format!("message {}", m.offset())),
                Some(Err(e)) => out.push(e.to_string()),
                None => {}
            }
        }
        out
    };
    for assign in [false, true] {
        let pairs = sasl("materialize_lockdown");
        let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
        let (consumer, _) = consumer("kafka:39095", &pairs);
        if assign {
            consumer
                .assign(&tpl(&[("mzkc-auth-topic", 0, Offset::Offset(0))]))
                .unwrap();
        }
        let got = poll_for(&consumer, Duration::from_secs(5));
        assert_eq!(
            got.iter()
                .filter(|e| e.contains("GroupAuthorizationFailed"))
                .count(),
            1,
            "assign={assign}: {got:?}"
        );
        assert_eq!(
            got[0],
            "Message consumption error: GroupAuthorizationFailed (Broker: Group authorization \
             failed)"
        );
    }
    // An authorized principal sees no error.
    let pairs = sasl("materialize");
    let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
    let (consumer, _) = consumer("kafka:39095", &pairs);
    assert_eq!(
        poll_for(&consumer, Duration::from_secs(2)),
        Vec::<String>::new()
    );
}

/// A failed committed offset lookup that retrying is not expected to fix is
/// reported as a consumer error, once, while the lookup is retried. Here the
/// principal may use its group but not the topic.
///
/// Runs when `MZ_KAFKA_CLIENT_TEST_AUTH_SECRETS` is set, like
/// [`group_authorization_failed`]. The broker of `test/kafka-auth` lets
/// `materialize_lockdown` use groups prefixed `lockdown`, but not the topic.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn offset_fetch_authorization_failed() {
    if std::env::var("MZ_KAFKA_CLIENT_TEST_AUTH_SECRETS").is_err() {
        return;
    }
    let group = unique("lockdown-mzkc");
    let (consumer, _) = consumer(
        "kafka:39095",
        &[
            ("security.protocol", "SASL_PLAINTEXT"),
            ("sasl.mechanisms", "PLAIN"),
            ("sasl.username", "materialize_lockdown"),
            ("sasl.password", "sekurity"),
            ("group.id", &group),
        ],
    );
    consumer
        .assign(&tpl(&[("mzkc-auth-topic", 0, Offset::Stored)]))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got = Vec::new();
    while Instant::now() < deadline {
        if let Some(r) = consumer.poll(Duration::from_millis(100)) {
            got.push(describe(r));
        }
    }
    assert_eq!(
        got,
        vec![Polled::Err(
            "Message consumption error: TopicAuthorizationFailed (Broker: Topic authorization \
             failed)"
                .into()
        )]
    );
}

#[mz_ore::test]
fn no_group_id() {
    let (consumer, _) = consumer("127.0.0.1:1", &[]);
    assert!(matches!(
        consumer.poll(Duration::ZERO),
        Some(Err(KafkaError::MessageConsumption(
            RDKafkaErrorCode::UnknownGroup
        )))
    ));
    assert_eq!(
        consumer.assignment().unwrap_err(),
        KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownGroup)
    );
    assert_eq!(
        consumer.position().unwrap_err(),
        KafkaError::MetadataFetch(RDKafkaErrorCode::UnknownGroup)
    );
    assert_eq!(
        consumer
            .assign(&tpl(&[("t", 0, Offset::Beginning)]))
            .unwrap_err()
            .to_string(),
        "Subscription error: Local: Unknown group"
    );
    assert_eq!(
        consumer
            .commit(&tpl(&[("t", 0, Offset::Offset(1))]), CommitMode::Sync)
            .unwrap_err(),
        KafkaError::ConsumerCommit(RDKafkaErrorCode::UnknownGroup)
    );
    assert!(consumer.split_partition_queue("t", 0).is_some());
    let err = StreamConsumer::from_config_and_context(
        &config("127.0.0.1:1", &[]),
        crate::consumer::DefaultConsumerContext,
    )
    .err()
    .unwrap();
    assert_eq!(
        err.to_string(),
        "Client creation error: librdkafka failed to create consumer queue"
    );
}

#[mz_ore::test]
fn assign_validation() {
    let (consumer, _) = consumer("127.0.0.1:1", &[("group.id", "g")]);
    let dup = tpl(&[("t", 0, Offset::Offset(1)), ("t", 0, Offset::Offset(2))]);
    assert_eq!(
        consumer.assign(&dup).unwrap_err().to_string(),
        "Subscription error: Local: Invalid argument or configuration"
    );
    consumer
        .assign(&tpl(&[
            ("b", 1, Offset::Offset(5)),
            ("a", 0, Offset::Invalid),
            ("a", 1, Offset::OffsetTail(3)),
        ]))
        .unwrap();
    let assignment = consumer.assignment().unwrap();
    let got: Vec<_> = assignment
        .elements()
        .iter()
        .map(|e| (e.topic().to_string(), e.partition(), e.offset()))
        .collect();
    assert_eq!(
        got,
        vec![
            ("a".into(), 0, Offset::Stored),
            ("a".into(), 1, Offset::OffsetTail(3)),
            ("b".into(), 1, Offset::Offset(5)),
        ]
    );
    let positions = consumer.position().unwrap();
    assert!(
        positions
            .elements()
            .iter()
            .all(|e| e.offset() == Offset::Invalid)
    );
    consumer.assign(&TopicPartitionList::new()).unwrap();
    assert_eq!(consumer.assignment().unwrap().count(), 0);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn assign_poll_position_eof() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-basic", 2);
        let topic = topic_guard.name.clone();
        let ts = 1_700_000_000_000;
        admin.produce(
            &topic,
            0,
            &[
                Rec {
                    key: Some(b"k0"),
                    value: Some(b"v0"),
                    headers: vec![("h1", Some(b"a")), ("h2", None), ("h1", Some(b""))],
                    timestamp: Some(ts),
                },
                Rec {
                    key: None,
                    value: None,
                    headers: vec![],
                    timestamp: Some(ts + 1),
                },
                Rec {
                    key: Some(b""),
                    value: Some(b""),
                    headers: vec![],
                    timestamp: Some(ts + 2),
                },
            ],
        );
        let (consumer, _) = consumer(
            &brokers,
            &[("group.id", "mzkc-test"), ("enable.partition.eof", "true")],
        );
        consumer
            .assign(&tpl(&[(&topic, 0, Offset::Offset(0))]))
            .unwrap();
        assert_eq!(position(&consumer, &topic, 0), Offset::Invalid);

        let m = consumer.poll(Duration::from_secs(10)).unwrap().unwrap();
        assert_eq!(m.topic(), topic);
        assert_eq!((m.partition(), m.offset()), (0, 0));
        assert_eq!(m.timestamp(), Timestamp::CreateTime(ts));
        assert_eq!(m.key(), Some(&b"k0"[..]));
        assert_eq!(m.payload(), Some(&b"v0"[..]));
        let headers: Vec<_> = m
            .headers()
            .unwrap()
            .iter()
            .map(|h| (h.key.to_string(), h.value.map(|v| v.to_vec())))
            .collect();
        assert_eq!(
            headers,
            vec![
                ("h1".to_string(), Some(b"a".to_vec())),
                ("h2".to_string(), None),
                ("h1".to_string(), Some(Vec::new())),
            ]
        );
        drop(m);
        assert_eq!(position(&consumer, &topic, 0), Offset::Offset(1));

        let m = consumer.poll(Duration::from_secs(10)).unwrap().unwrap();
        assert_eq!(m.offset(), 1);
        assert_eq!((m.key(), m.payload()), (None, None));
        assert!(m.headers().is_none());
        drop(m);
        let m = consumer.poll(Duration::from_secs(10)).unwrap().unwrap();
        assert_eq!(m.offset(), 2);
        assert_eq!((m.key(), m.payload()), (Some(&b""[..]), Some(&b""[..])));
        assert_eq!(m.timestamp().to_millis(), Some(ts + 2));
        drop(m);
        assert_eq!(position(&consumer, &topic, 0), Offset::Offset(3));
        assert!(matches!(
            consumer.poll(Duration::from_secs(10)),
            Some(Err(KafkaError::PartitionEOF(0)))
        ));
        // No second EOF without progress.
        assert!(consumer.poll(Duration::from_millis(1500)).is_none());

        admin.values(&topic, 0, &["d", "e"]);
        let items = poll_until(&consumer, Duration::from_secs(10), |items| {
            items.contains(&Polled::Eof(0))
        });
        assert_eq!(msgs(&items), vec![3, 4]);
        assert_eq!(items.last(), Some(&Polled::Eof(0)));
        assert_eq!(position(&consumer, &topic, 0), Offset::Offset(5));
        assert!(consumer.poll(Duration::ZERO).is_none());
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn compression_codecs() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-codec", 1);
        let topic = topic_guard.name.clone();
        let codecs = [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ];
        let mut expected = Vec::new();
        for codec in codecs {
            let values: Vec<String> = (0..50).map(|i| format!("{codec:?}-{i}")).collect();
            let records: Vec<Rec<'_>> = values.iter().map(|v| Rec::value(v.as_bytes())).collect();
            admin.produce_with(&topic, 0, &records, codec, None);
            expected.extend(values);
        }
        let (consumer, _) = consumer(&brokers, &[("group.id", "mzkc-test")]);
        // Start in the middle of the gzip batch: earlier records are skipped.
        consumer
            .assign(&tpl(&[(&topic, 0, Offset::Offset(60))]))
            .unwrap();
        let items = poll_until(&consumer, Duration::from_secs(20), |items| {
            items.len() == expected.len() - 60
        });
        let values: Vec<String> = items
            .iter()
            .map(|p| match p {
                Polled::Msg(_, _, Some(v)) => v.clone(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(values, expected[60..]);
        assert_eq!(msgs(&items), (60..250).collect::<Vec<i64>>());
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn split_queues_and_callbacks() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-split", 3);
        let topic = topic_guard.name.clone();
        admin.values(&topic, 0, &["a0", "a1"]);
        admin.values(&topic, 1, &["b0"]);
        admin.values(&topic, 2, &["c0"]);
        let (consumer, _) = consumer(&brokers, &[("group.id", "mzkc-test")]);
        let mut q0 = consumer.split_partition_queue(&topic, 0).unwrap();
        let mut q1 = consumer.split_partition_queue(&topic, 1).unwrap();
        let fired0 = Arc::new(AtomicUsize::new(0));
        let fired1 = Arc::new(AtomicUsize::new(0));
        let f0 = Arc::clone(&fired0);
        q0.set_nonempty_callback(move || {
            f0.fetch_add(1, Ordering::SeqCst);
        });
        let f1 = Arc::clone(&fired1);
        q1.set_nonempty_callback(move || {
            f1.fetch_add(1, Ordering::SeqCst);
        });
        consumer
            .assign(&tpl(&[
                (&topic, 0, Offset::Beginning),
                (&topic, 1, Offset::Beginning),
                (&topic, 2, Offset::Beginning),
            ]))
            .unwrap();
        // Partition 2 is not split: it comes out of the main queue.
        let main = poll_until(&consumer, Duration::from_secs(10), |i| i.len() == 1);
        assert_eq!(main, vec![Polled::Msg(2, 0, Some("c0".into()))]);
        let deadline = Instant::now() + Duration::from_secs(10);
        while fired0.load(Ordering::SeqCst) == 0 || fired1.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "callbacks did not fire");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            queue_until(&q0, Duration::from_secs(5), |i| i.len() == 2),
            vec![
                Polled::Msg(0, 0, Some("a0".into())),
                Polled::Msg(0, 1, Some("a1".into()))
            ]
        );
        assert!(q0.poll(Duration::ZERO).is_none());
        assert_eq!(
            queue_until(&q1, Duration::from_secs(5), |i| i.len() == 1),
            vec![Polled::Msg(1, 0, Some("b0".into()))]
        );
        assert!(q1.poll(Duration::ZERO).is_none());
        assert!(consumer.poll(Duration::from_millis(500)).is_none());
        // Edge-triggered: one more firing for new data after draining.
        let before = fired0.load(Ordering::SeqCst);
        admin.values(&topic, 0, &["a2"]);
        let deadline = Instant::now() + Duration::from_secs(10);
        while fired0.load(Ordering::SeqCst) == before {
            assert!(Instant::now() < deadline, "callback did not fire again");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            queue_until(&q0, Duration::from_secs(5), |i| i.len() == 1),
            vec![Polled::Msg(0, 2, Some("a2".into()))]
        );
        // Re-splitting after a re-assign returns the same queue; partitions
        // that stay assigned keep their position.
        consumer
            .assign(&tpl(&[
                (&topic, 0, Offset::Beginning),
                (&topic, 1, Offset::Beginning),
            ]))
            .unwrap();
        let q0b = consumer.split_partition_queue(&topic, 0).unwrap();
        drop(q0);
        admin.values(&topic, 0, &["a3"]);
        assert_eq!(
            queue_until(&q0b, Duration::from_secs(10), |i| i.len() == 1),
            vec![Polled::Msg(0, 3, Some("a3".into()))]
        );
        assert_eq!(position(&consumer, &topic, 0), Offset::Offset(4));
        assert_eq!(position(&consumer, &topic, 1), Offset::Offset(1));
        assert_eq!(consumer.assignment().unwrap().count(), 2);
    }
}

/// Materialize's source after every `assign`: it splits a new queue for
/// every partition, sets the nonempty callback on the new handle, and only
/// then drops the old handle of the same partition. Dropping the old handle
/// must neither undo the split nor remove the new handle's callback.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn new_queue_handle_survives_dropping_the_old_one() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-requeue", 2);
        let topic = topic_guard.name.clone();
        let (consumer, _) = consumer(&brokers, &[("group.id", "mzkc-test")]);
        let counter = |c: &Arc<AtomicUsize>| {
            let c = Arc::clone(c);
            move || {
                c.fetch_add(1, Ordering::SeqCst);
            }
        };
        let old_fired = Arc::new(AtomicUsize::new(0));
        let mut old_q0 = consumer.split_partition_queue(&topic, 0).unwrap();
        old_q0.set_nonempty_callback(counter(&old_fired));
        consumer
            .assign(&tpl(&[(&topic, 0, Offset::Offset(0))]))
            .unwrap();

        // Add partition 1 with a full re-assign, then re-split as the source
        // does: new handles first, old handles dropped afterwards.
        consumer
            .assign(&tpl(&[
                (&topic, 0, Offset::Offset(0)),
                (&topic, 1, Offset::Offset(0)),
            ]))
            .unwrap();
        let new_fired = Arc::new(AtomicUsize::new(0));
        let mut new_q0 = consumer.split_partition_queue(&topic, 0).unwrap();
        new_q0.set_nonempty_callback(counter(&new_fired));
        let mut q1 = consumer.split_partition_queue(&topic, 1).unwrap();
        q1.set_nonempty_callback(counter(&new_fired));
        drop(old_q0);

        admin.values(&topic, 0, &["a"]);
        admin.values(&topic, 1, &["b"]);
        let deadline = Instant::now() + Duration::from_secs(10);
        while new_fired.load(Ordering::SeqCst) < 2 {
            assert!(
                Instant::now() < deadline,
                "callbacks of the new handles did not fire"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(old_fired.load(Ordering::SeqCst), 0);
        assert_eq!(
            queue_until(&new_q0, Duration::from_secs(5), |i| i.len() == 1),
            vec![Polled::Msg(0, 0, Some("a".into()))]
        );
        assert_eq!(
            queue_until(&q1, Duration::from_secs(5), |i| i.len() == 1),
            vec![Polled::Msg(1, 0, Some("b".into()))]
        );
        // Still split: nothing reaches the main queue.
        assert!(consumer.poll(Duration::from_millis(500)).is_none());

        // The other order also keeps the newer callback: replacing the
        // callback of a live handle, then dropping the replaced handle.
        let newest_fired = Arc::new(AtomicUsize::new(0));
        let mut newest_q0 = consumer.split_partition_queue(&topic, 0).unwrap();
        newest_q0.set_nonempty_callback(counter(&newest_fired));
        drop(new_q0);
        admin.values(&topic, 0, &["c"]);
        let deadline = Instant::now() + Duration::from_secs(10);
        while newest_fired.load(Ordering::SeqCst) == 0 {
            assert!(
                Instant::now() < deadline,
                "callback of the newest handle did not fire"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            queue_until(&newest_q0, Duration::from_secs(5), |i| i.len() == 1),
            vec![Polled::Msg(0, 1, Some("c".into()))]
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn reassign_restarts_changed_offsets_and_adds_partitions() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-reassign", 1);
        let topic = topic_guard.name.clone();
        admin.values(&topic, 0, &["a", "b", "c"]);
        let (consumer, _) = consumer(&brokers, &[("group.id", "mzkc-test")]);
        let q0 = consumer.split_partition_queue(&topic, 0).unwrap();
        consumer
            .assign(&tpl(&[(&topic, 0, Offset::Offset(0))]))
            .unwrap();
        let got = queue_until(&q0, Duration::from_secs(10), |i| i.len() == 3);
        assert_eq!(msgs(&got), vec![0, 1, 2]);
        // A new partition appears and is added, like the Kafka source does.
        admin.create_partitions(&topic, 2);
        admin.values(&topic, 1, &["x", "y"]);
        admin.values(&topic, 0, &["d"]);
        consumer
            .assign(&tpl(&[
                (&topic, 0, Offset::Offset(0)),
                (&topic, 1, Offset::Offset(0)),
            ]))
            .unwrap();
        let q0 = consumer.split_partition_queue(&topic, 0).unwrap();
        let q1 = consumer.split_partition_queue(&topic, 1).unwrap();
        let got0 = queue_until(&q0, Duration::from_secs(10), |i| i.len() == 1);
        assert_eq!(msgs(&got0), vec![3], "no duplicates after re-assign");
        let got1 = queue_until(&q1, Duration::from_secs(10), |i| i.len() == 2);
        assert_eq!(msgs(&got1), vec![0, 1]);
        // A changed start offset restarts the partition.
        consumer
            .assign(&tpl(&[
                (&topic, 0, Offset::Offset(1)),
                (&topic, 1, Offset::Offset(0)),
            ]))
            .unwrap();
        assert_eq!(position(&consumer, &topic, 0), Offset::Invalid);
        assert_eq!(position(&consumer, &topic, 1), Offset::Offset(2));
        let got0 = queue_until(&q0, Duration::from_secs(10), |i| i.len() == 3);
        assert_eq!(msgs(&got0), vec![1, 2, 3]);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn offset_reset() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-reset", 1);
        let topic = topic_guard.name.clone();
        admin.values(&topic, 0, &["a", "b", "c"]);

        let (earliest, recorder) = consumer(
            &brokers,
            &[("group.id", "mzkc-test"), ("auto.offset.reset", "earliest")],
        );
        earliest
            .assign(&tpl(&[(&topic, 0, Offset::Offset(1000))]))
            .unwrap();
        let got = poll_until(&earliest, Duration::from_secs(10), |i| i.len() == 3);
        assert_eq!(msgs(&got), vec![0, 1, 2]);
        let warnings = recorder.logs_with("OFFSET");
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains(&format!(
                "{topic} [0]: offset reset (at offset 1000 (leader epoch -1), broker "
            )),
            "{warnings:?}"
        );
        assert!(
            warnings[0].ends_with(
                ": fetch failed due to requested offset not available on the broker: \
                 Broker: Offset out of range"
            ),
            "{warnings:?}"
        );

        let (latest, _) = consumer(
            &brokers,
            &[("group.id", "mzkc-test"), ("auto.offset.reset", "latest")],
        );
        latest
            .assign(&tpl(&[(&topic, 0, Offset::Offset(1000))]))
            .unwrap();
        assert!(latest.poll(Duration::from_secs(2)).is_none());
        admin.values(&topic, 0, &["d"]);
        let got = poll_until(&latest, Duration::from_secs(10), |i| i.len() == 1);
        assert_eq!(msgs(&got), vec![3]);

        let (error, _) = consumer(
            &brokers,
            &[("group.id", "mzkc-test"), ("auto.offset.reset", "error")],
        );
        error
            .assign(&tpl(&[(&topic, 0, Offset::Offset(1000))]))
            .unwrap();
        let got = poll_until(&error, Duration::from_secs(10), |i| i.len() == 1);
        assert_eq!(
            got,
            vec![Polled::Err(
                "Message consumption error: AutoOffsetReset (Local: No offset to automatically \
                 reset to)"
                    .into()
            )]
        );
        // Logical start offsets.
        let (c, _) = consumer(&brokers, &[("group.id", unique("mzkc-g").as_str())]);
        c.assign(&tpl(&[(&topic, 0, Offset::OffsetTail(2))]))
            .unwrap();
        let got = poll_until(&c, Duration::from_secs(10), |i| i.len() == 2);
        assert_eq!(msgs(&got), vec![2, 3]);
        c.assign(&tpl(&[(&topic, 0, Offset::End)])).unwrap();
        assert!(c.poll(Duration::from_secs(1)).is_none());
        // Stored without a committed offset falls back to
        // auto.offset.reset (default latest).
        c.assign(&tpl(&[(&topic, 0, Offset::Stored)])).unwrap();
        assert!(c.poll(Duration::from_secs(1)).is_none());
        admin.values(&topic, 0, &["e"]);
        let got = poll_until(&c, Duration::from_secs(10), |i| i.len() == 1);
        assert_eq!(msgs(&got), vec![4]);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn read_committed_transactions() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-txn", 1);
        let topic = topic_guard.name.clone();
        let p0 = admin.values(&topic, 0, &["p0"]);
        // Committed, then a commit marker.
        let mut t1 = Txn::new(&admin);
        let c1 = t1.produce(&topic, 0, &["c1", "c2"]);
        t1.end(true);
        // Aborted, then an abort marker.
        let mut t2 = Txn::new(&admin);
        let x1 = t2.produce(&topic, 0, &["x1", "x2"]);
        t2.end(false);
        // Interleaved aborted and committed transactions.
        let mut a = Txn::new(&admin);
        let mut c = Txn::new(&admin);
        let x3 = a.produce(&topic, 0, &["x3"]);
        let c3 = c.produce(&topic, 0, &["c3"]);
        let x4 = a.produce(&topic, 0, &["x4"]);
        a.end(false);
        c.end(true);
        // A trailing plain record after the markers.
        let p1 = admin.values(&topic, 0, &["p1"]);
        // Coordinators write transaction markers after EndTxn returns, so a
        // marker may follow p1: the end is where the LSO catches up with the
        // high watermark.
        let end = wait_for_stable_end(&brokers, &topic);

        let (rc, _) = consumer(
            &brokers,
            &[("group.id", "mzkc-test"), ("enable.partition.eof", "true")],
        );
        assert_eq!(
            rc.fetch_watermarks(&topic, 0, Duration::from_secs(10))
                .unwrap(),
            (0, end)
        );
        rc.assign(&tpl(&[(&topic, 0, Offset::Beginning)])).unwrap();
        let got = poll_until(&rc, Duration::from_secs(10), |i| {
            i.contains(&Polled::Eof(0))
        });
        let values: Vec<_> = got
            .iter()
            .filter_map(|p| match p {
                Polled::Msg(_, o, Some(v)) => Some((*o, v.clone())),
                _ => None,
            })
            .collect();
        assert_eq!(
            values,
            vec![
                (p0, "p0".to_string()),
                (c1, "c1".into()),
                (c1 + 1, "c2".into()),
                (c3, "c3".into()),
                (p1, "p1".into())
            ]
        );
        assert_eq!(position(&rc, &topic, 0), Offset::Offset(end));

        let (ru, _) = consumer(
            &brokers,
            &[
                ("group.id", "mzkc-test"),
                ("enable.partition.eof", "true"),
                ("isolation.level", "read_uncommitted"),
            ],
        );
        ru.assign(&tpl(&[(&topic, 0, Offset::Beginning)])).unwrap();
        let got = poll_until(&ru, Duration::from_secs(10), |i| {
            i.contains(&Polled::Eof(0))
        });
        assert_eq!(msgs(&got), vec![p0, c1, c1 + 1, x1, x1 + 1, x3, c3, x4, p1]);
        assert_eq!(position(&ru, &topic, 0), Offset::Offset(end));

        // A topic that ends with a commit marker: the position moves past it
        // without a message, which is what lets sink progress searches and
        // source frontiers reach the end.
        let topic2_guard = admin.new_topic("mzkc-c-txn-end", 1);
        let topic2 = topic2_guard.name.clone();
        let mut t = Txn::new(&admin);
        let only = t.produce(&topic2, 0, &["only"]);
        t.end(true);
        let (_, end2) = ru
            .fetch_watermarks(&topic2, 0, Duration::from_secs(10))
            .unwrap();
        let (rc, _) = consumer(
            &brokers,
            &[("group.id", "mzkc-test"), ("enable.partition.eof", "true")],
        );
        rc.assign(&tpl(&[(&topic2, 0, Offset::Offset(0))])).unwrap();
        let got = poll_until(&rc, Duration::from_secs(10), |i| {
            i.contains(&Polled::Eof(0))
        });
        assert_eq!(msgs(&got), vec![only]);
        assert_eq!(position(&rc, &topic2, 0), Offset::Offset(end2));

        // An open transaction holds back read_committed consumers at the
        // LSO; aborting it moves the position past the abort marker.
        let mut open = Txn::new(&admin);
        let pending = open.produce(&topic2, 0, &["pending"]);
        // The LSO is the first offset of the open transaction, which is
        // its data on Kafka and a preceding fence batch on Redpanda.
        let (lo, lso) = rc
            .fetch_watermarks(&topic2, 0, Duration::from_secs(10))
            .unwrap();
        assert_eq!(lo, 0);
        assert!(end2 <= lso && lso <= pending, "{end2} {lso} {pending}");
        let (_, hwm) = ru
            .fetch_watermarks(&topic2, 0, Duration::from_secs(10))
            .unwrap();
        assert_eq!(hwm, pending + 1);
        assert!(rc.poll(Duration::from_secs(1)).is_none());
        assert_eq!(position(&rc, &topic2, 0), Offset::Offset(end2));
        open.end(false);
        let (_, end3) = ru
            .fetch_watermarks(&topic2, 0, Duration::from_secs(10))
            .unwrap();
        assert!(end3 > pending + 1);
        let got = poll_until(&rc, Duration::from_secs(10), |i| {
            i.contains(&Polled::Eof(0))
        });
        assert_eq!(msgs(&got), Vec::<i64>::new());
        assert_eq!(position(&rc, &topic2, 0), Offset::Offset(end3));
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn commit_and_committed() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-commit", 2);
        let topic = topic_guard.name.clone();
        admin.values(&topic, 0, &["a", "b", "c"]);
        let group = unique("mzkc-group");
        let (consumer, recorder) = consumer(
            &brokers,
            &[
                ("group.id", group.as_str()),
                ("enable.auto.commit", "false"),
                ("enable.auto.offset.store", "false"),
            ],
        );
        let mut query = TopicPartitionList::new();
        query.add_partition(&topic, 0);
        query.add_partition(&topic, 1);
        let committed = consumer
            .committed_offsets(query.clone(), Duration::from_secs(10))
            .unwrap();
        assert!(
            committed
                .elements()
                .iter()
                .all(|e| e.offset() == Offset::Invalid)
        );
        consumer
            .commit(&tpl(&[(&topic, 0, Offset::Offset(2))]), CommitMode::Sync)
            .unwrap();
        let committed = consumer
            .committed_offsets(query.clone(), Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            committed.find_partition(&topic, 0).unwrap().offset(),
            Offset::Offset(2)
        );
        assert_eq!(
            committed.find_partition(&topic, 1).unwrap().offset(),
            Offset::Invalid
        );
        assert_eq!(recorder.commits.lock().unwrap().as_slice(), &[Ok(())]);

        // Assigning at the stored offset starts at the committed offset.
        consumer
            .assign(&tpl(&[(&topic, 0, Offset::Stored)]))
            .unwrap();
        let m = consumer.poll(Duration::from_secs(10)).unwrap().unwrap();
        assert_eq!(m.offset(), 2);
        assert_eq!(
            consumer.commit_consumer_state(CommitMode::Sync),
            Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset))
        );
        consumer.store_offset_from_message(&m).unwrap();
        drop(m);
        consumer.commit_consumer_state(CommitMode::Sync).unwrap();
        let committed = consumer
            .committed_offsets(query, Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            committed.find_partition(&topic, 0).unwrap().offset(),
            Offset::Offset(3)
        );
        // Nothing new to commit.
        assert_eq!(
            consumer.commit_consumer_state(CommitMode::Sync),
            Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::NoOffset))
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn offsets_for_times() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-times", 2);
        let topic = topic_guard.name.clone();
        let base = 1_600_000_000_000;
        let records: Vec<Rec<'_>> = (0..5)
            .map(|i| Rec {
                value: Some(b"v"),
                timestamp: Some(base + i * 1000),
                ..Rec::default()
            })
            .collect();
        admin.produce(&topic, 0, &records);
        // No group needed.
        let (consumer, _) = consumer(&brokers, &[]);
        let query = tpl(&[
            (&topic, 0, Offset::Offset(base + 2500)),
            (&topic, 1, Offset::Offset(base)),
        ]);
        let result = consumer
            .offsets_for_times(query, Duration::from_secs(10))
            .unwrap();
        let got: Vec<_> = result.elements().iter().map(|e| e.offset()).collect();
        assert_eq!(got, vec![Offset::Offset(3), Offset::End]);
        let query = tpl(&[(&topic, 0, Offset::Beginning), (&topic, 1, Offset::End)]);
        let result = consumer
            .offsets_for_times(query, Duration::from_secs(10))
            .unwrap();
        let got: Vec<_> = result.elements().iter().map(|e| e.offset()).collect();
        assert_eq!(got, vec![Offset::Offset(0), Offset::Offset(0)]);
        let query = tpl(&[(&topic, 0, Offset::End)]);
        let result = consumer
            .offsets_for_times(query, Duration::from_secs(10))
            .unwrap();
        assert_eq!(result.elements()[0].offset(), Offset::Offset(5));
        let missing = tpl(&[(&unique("mzkc-missing"), 0, Offset::End)]);
        assert_eq!(
            consumer.offsets_for_times(missing, Duration::from_secs(5)),
            Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::UnknownPartition
            ))
        );
        assert_eq!(
            consumer.offsets_for_times(TopicPartitionList::new(), Duration::from_secs(5)),
            Err(KafkaError::MetadataFetch(RDKafkaErrorCode::InvalidArgument))
        );
    }
}

/// Runs `f` on a fresh current-thread tokio runtime.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn stream_consumer_group() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-group", 4);
        let topic = topic_guard.name.clone();
        for p in 0..4 {
            admin.values(&topic, p, &["a", "b"]);
        }
        let group = unique("mzkc-group");
        let cfg = config(
            &brokers,
            &[
                ("group.id", group.as_str()),
                ("enable.auto.commit", "false"),
                ("enable.auto.offset.store", "false"),
                ("auto.offset.reset", "earliest"),
                ("session.timeout.ms", "10000"),
                ("heartbeat.interval.ms", "500"),
            ],
        );
        let ctx1 = Ctx::default();
        let rec1 = Arc::clone(&ctx1.0);
        let c1: StreamConsumer<Ctx> = StreamConsumer::from_config_and_context(&cfg, ctx1).unwrap();
        c1.subscribe(&[&topic]).unwrap();
        let mut seen = Vec::new();
        block_on(async {
            let mut stream = c1.stream();
            while seen.len() < 8 {
                let m = tokio::time::timeout(Duration::from_secs(30), stream.next())
                    .await
                    .expect("timed out")
                    .unwrap()
                    .unwrap();
                seen.push((m.partition(), m.offset()));
                c1.store_offset_from_message(&m).unwrap();
            }
        });
        seen.sort();
        assert_eq!(
            seen,
            (0..4).flat_map(|p| [(p, 0), (p, 1)]).collect::<Vec<_>>()
        );
        assert_eq!(c1.assignment().unwrap().count(), 4);
        c1.commit_consumer_state(CommitMode::Sync).unwrap();
        assert_eq!(
            rec1.rebalances.lock().unwrap().as_slice(),
            &["pre-assign 4".to_string(), "post-assign 4".to_string()]
        );

        // A second member joins: the partitions are split between them.
        let ctx2 = Ctx::default();
        let c2: StreamConsumer<Ctx> = StreamConsumer::from_config_and_context(&cfg, ctx2).unwrap();
        c2.subscribe(&[&topic]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            // Keep polling so that both members take part in the rebalance.
            block_on(async {
                let _ = tokio::time::timeout(Duration::from_millis(100), c1.recv()).await;
                let _ = tokio::time::timeout(Duration::from_millis(100), c2.recv()).await;
            });
            let (n1, n2) = (
                c1.assignment().unwrap().count(),
                c2.assignment().unwrap().count(),
            );
            if n1 == 2 && n2 == 2 {
                break;
            }
            assert!(Instant::now() < deadline, "no rebalance: {n1} {n2}");
        }
        // Each member resumes its partitions at the committed offsets.
        for p in 0..4 {
            admin.values(&topic, p, &["c"]);
        }
        let mut seen = Vec::new();
        block_on(async {
            let deadline = Instant::now() + Duration::from_secs(30);
            while seen.len() < 4 {
                assert!(Instant::now() < deadline, "got {seen:?}");
                for c in [&c1, &c2] {
                    if let Ok(m) = tokio::time::timeout(Duration::from_millis(100), c.recv()).await
                    {
                        let m = m.unwrap();
                        seen.push((m.partition(), m.offset()));
                    }
                }
            }
        });
        seen.sort();
        assert_eq!(seen, (0..4).map(|p| (p, 2)).collect::<Vec<_>>());
        // When a member leaves, the other takes over all partitions.
        drop(c2);
        let deadline = Instant::now() + Duration::from_secs(60);
        while c1.assignment().unwrap().count() != 4 {
            block_on(async {
                let _ = tokio::time::timeout(Duration::from_millis(100), c1.recv()).await;
            });
            assert!(Instant::now() < deadline, "no rebalance after leave");
        }
        let rebalances = rec1.rebalances.lock().unwrap().clone();
        assert!(
            rebalances.contains(&"pre-revoke 4".to_string()),
            "{rebalances:?}"
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn subscribe_missing_topic() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic = unique("mzkc-c-later");
        let group = unique("mzkc-group");
        let cfg = config(
            &brokers,
            &[
                ("group.id", group.as_str()),
                ("auto.offset.reset", "earliest"),
            ],
        );
        let c: StreamConsumer<Ctx> =
            StreamConsumer::from_config_and_context(&cfg, Ctx::default()).unwrap();
        c.subscribe(&[&topic]).unwrap();
        block_on(async {
            let first = tokio::time::timeout(Duration::from_secs(30), c.recv())
                .await
                .expect("timed out");
            assert_eq!(
                first.err(),
                Some(KafkaError::MessageConsumption(
                    RDKafkaErrorCode::UnknownTopicOrPartition
                ))
            );
        });
        admin.create_topic(&topic, 1);
        let _guard = TopicGuard {
            cluster: admin.cluster(),
            name: topic.clone(),
        };
        admin.values(&topic, 0, &["hello"]);
        block_on(async {
            let m = tokio::time::timeout(Duration::from_secs(30), c.recv())
                .await
                .expect("timed out")
                .unwrap();
            assert_eq!(m.payload(), Some(&b"hello"[..]));
        });
    }
}

/// Waits until partition 0's last stable offset equals its high watermark,
/// i.e. all transaction markers are written, and returns it.
fn wait_for_stable_end(brokers: &str, topic: &str) -> i64 {
    let (rc, _) = consumer(brokers, &[]);
    let (ru, _) = consumer(brokers, &[("isolation.level", "read_uncommitted")]);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (_, lso) = rc
            .fetch_watermarks(topic, 0, Duration::from_secs(10))
            .unwrap();
        let (_, hwm) = ru
            .fetch_watermarks(topic, 0, Duration::from_secs(10))
            .unwrap();
        if lso == hwm {
            return hwm;
        }
        assert!(
            Instant::now() < deadline,
            "LSO {lso} never reached HWM {hwm}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Waits until a new partition has a leader that the metadata reports.
async fn wait_for_leader(cluster: &Cluster, topic: &str, partition: i32, deadline: Deadline) {
    loop {
        match cluster.leader(topic, partition, deadline).await {
            Ok(_) => return,
            Err(_) if !deadline.is_elapsed() => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("no leader for {topic} [{partition}]: {e}"),
        }
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn deleted_topic() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-deleted", 1);
        let topic = topic_guard.name.clone();
        admin.values(&topic, 0, &["a"]);
        let (consumer, recorder) = consumer(
            &brokers,
            &[
                ("group.id", "mzkc-test"),
                ("topic.metadata.propagation.max.ms", "1000"),
            ],
        );
        let q = consumer.split_partition_queue(&topic, 0).unwrap();
        consumer
            .assign(&tpl(&[(&topic, 0, Offset::Beginning)]))
            .unwrap();
        assert_eq!(
            msgs(&queue_until(&q, Duration::from_secs(10), |i| i.len() == 1)),
            vec![0]
        );
        admin.delete_topic(&topic);
        // Like librdkafka, the deletion is reported through the error
        // callback when the queue is served, not as a consumer error.
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(q.poll(Duration::from_millis(100)).is_none());
            let errors = recorder.errors.lock().unwrap().clone();
            if errors.len() >= 2 {
                assert_eq!(
                    errors[0],
                    (
                        KafkaError::Global(RDKafkaErrorCode::UnknownPartition),
                        format!(
                            "{topic} [0]: desired partition is no longer available \
                             (Local: Unknown partition)"
                        )
                    )
                );
                assert_eq!(
                    errors[1],
                    (
                        KafkaError::Global(RDKafkaErrorCode::UnknownTopicOrPartition),
                        format!(
                            "{topic} [0]: topic does not exist (Broker: Unknown topic or partition)"
                        )
                    )
                );
                break;
            }
            assert!(Instant::now() < deadline, "no error callback");
        }
    }
}

/// Restarts the broker container named by `MZ_KAFKA_CLIENT_TEST_RESTART`
/// (`<container>=<bootstrap servers>`) while consuming, and checks that
/// consumption resumes without loss or duplicates.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn broker_restart() {
    let Ok(spec) = std::env::var("MZ_KAFKA_CLIENT_TEST_RESTART") else {
        return;
    };
    let (container, brokers) = spec.split_once('=').expect("<container>=<brokers>");
    let admin = Admin::new(brokers);
    let topic_guard = admin.new_topic("mzkc-c-restart", 1);
    let topic = topic_guard.name.clone();
    let first: Vec<String> = (0..100).map(|i| format!("v{i}")).collect();
    admin.values(
        &topic,
        0,
        &first.iter().map(String::as_str).collect::<Vec<_>>(),
    );
    let (consumer, _) = consumer(brokers, &[("group.id", "mzkc-test")]);
    let q = consumer.split_partition_queue(&topic, 0).unwrap();
    consumer
        .assign(&tpl(&[(&topic, 0, Offset::Offset(0))]))
        .unwrap();
    let got = queue_until(&q, Duration::from_secs(10), |i| i.len() == 100);
    assert_eq!(msgs(&got), (0..100).collect::<Vec<_>>());
    let status = std::process::Command::new("docker")
        .args(["restart", "-t", "2", container])
        .status()
        .unwrap();
    assert!(status.success());
    // Producing retries until the broker is back.
    let admin = Admin::new(brokers);
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let cluster = admin.cluster();
        let t = topic.clone();
        let ok = runtime::block_on(async move {
            cluster
                .leader(&t, 0, Deadline::new(Duration::from_secs(5)))
                .await
                .is_ok()
        });
        if ok {
            break;
        }
        assert!(Instant::now() < deadline, "broker did not come back");
    }
    admin.values(&topic, 0, &["after"]);
    let got = queue_until(&q, Duration::from_secs(120), |i| !i.is_empty());
    assert_eq!(got, vec![Polled::Msg(0, 100, Some("after".into()))]);
    assert_eq!(position(&consumer, &topic, 0), Offset::Offset(101));
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn backpressure() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-bp", 1);
        let topic = topic_guard.name.clone();
        let value = vec![b'x'; 10_000];
        for _ in 0..20 {
            let records: Vec<Rec<'_>> = (0..10).map(|_| Rec::value(&value)).collect();
            admin.produce(&topic, 0, &records);
        }
        // 1 KB (1000 bytes) queue limit, 1 KiB partition fetches: the queue
        // never holds much more than one fetch.
        let (consumer, _) = consumer(
            &brokers,
            &[
                ("group.id", "mzkc-test"),
                ("queued.max.messages.kbytes", "1"),
                ("fetch.message.max.bytes", "1024"),
            ],
        );
        let q = consumer.split_partition_queue(&topic, 0).unwrap();
        consumer
            .assign(&tpl(&[(&topic, 0, Offset::Beginning)]))
            .unwrap();
        std::thread::sleep(Duration::from_secs(2));
        let partition = consumer
            .inner()
            .lock_state()
            .partitions
            .get(&topic)
            .unwrap()
            .get(&0)
            .cloned()
            .unwrap();
        let (bytes, _) = partition.queued();
        assert!(bytes > 0 && bytes < 250_000, "queued {bytes}");
        let got = queue_until(&q, Duration::from_secs(30), |i| i.len() == 200);
        assert_eq!(msgs(&got), (0..200).collect::<Vec<_>>());
        assert!(q.poll(Duration::ZERO).is_none());
        assert_eq!(partition.queued(), (0, 0));
    }
}

/// A partition with a large backlog does not starve the other partitions led
/// by the same broker: brokers fill a Fetch response in request order, so
/// requests list the partitions that got data last.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn fetch_rotation() {
    for brokers in test_brokers() {
        let admin = Admin::new(&brokers);
        let topic_guard = admin.new_topic("mzkc-c-rotation", 2);
        let topic = topic_guard.name.clone();
        // A backlog of 100 batches of about 1 KB in partition 0, then one
        // record in partition 1 that does not fit in what a response that is
        // full of partition 0 leaves.
        let value = "x".repeat(1000);
        for _ in 0..100 {
            admin.values(&topic, 0, &[&value]);
        }
        admin.values(&topic, 1, &[&"y".repeat(5000)]);
        let (consumer, _) = consumer(
            &brokers,
            &[
                ("group.id", "mzkc-test"),
                ("message.max.bytes", "1000"),
                ("fetch.max.bytes", "10000"),
                ("fetch.message.max.bytes", "10000"),
            ],
        );
        // Partition 0 is split but never polled: how much of it is queued
        // when partition 1's record arrives tells how many responses that
        // took.
        let _q0 = consumer.split_partition_queue(&topic, 0).unwrap();
        let mut q1 = consumer.split_partition_queue(&topic, 1).unwrap();
        let p0 = Arc::clone(&consumer.inner().lock_state().partitions[&topic][&0]);
        let p0_queued = Arc::new(AtomicU64::new(u64::MAX));
        let seen = Arc::clone(&p0_queued);
        q1.set_nonempty_callback(move || {
            let _ =
                seen.compare_exchange(u64::MAX, p0.queued().1, Ordering::SeqCst, Ordering::SeqCst);
        });
        consumer
            .assign(&tpl(&[
                (&topic, 0, Offset::Beginning),
                (&topic, 1, Offset::Beginning),
            ]))
            .unwrap();
        assert_eq!(
            msgs(&queue_until(&q1, Duration::from_secs(10), |i| i.len() == 1)),
            vec![0]
        );
        // The callback runs after waking the poller.
        let deadline = Instant::now() + Duration::from_secs(5);
        while p0_queued.load(Ordering::SeqCst) == u64::MAX {
            assert!(Instant::now() < deadline, "callback did not fire");
            std::thread::sleep(Duration::from_millis(10));
        }
        let queued = p0_queued.load(Ordering::SeqCst);
        assert!(
            queued < 50,
            "partition 0 had {queued} of 100 messages queued"
        );
    }
}

/// A record set that cannot be decoded at all is reported as a consumer
/// error, and the partition backs off instead of refetching it at once. An
/// entry ending at the largest offset is malformed: it neither wraps the
/// fetch offset nor the position.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn undecodable_record_sets() {
    let topic = "mzkc-c-undecodable";
    let (consumer, _) = consumer(
        "127.0.0.1:1",
        &[("group.id", "mzkc-test"), ("fetch.error.backoff.ms", "300")],
    );
    consumer
        .assign(&tpl(&[(topic, 0, Offset::Offset(5))]))
        .unwrap();
    let inner = consumer.inner();
    let partition = Arc::clone(&inner.lock_state().partitions[topic][&0]);
    // Hands `records` to the fetcher as if fetched at the fetch offset, and
    // returns the next fetch offset and the backoff.
    let fetched = |records: Vec<u8>| {
        let mut wakes = Vec::new();
        let mut state = partition.lock_state();
        state.fetch.backoff_until = None;
        fetcher::handle_records(
            inner,
            &partition,
            &mut state,
            Bytes::from(records),
            Vec::new(),
            false,
            &mut wakes,
        );
        let result = (state.fetch.offset, state.fetch.backoff_until);
        drop(state);
        for wake in wakes {
            partition.notify(inner, wake);
        }
        result
    };
    let batch = |base_offset: i64, values: &[&str]| {
        let mut builder = BatchBuilder::new(Compression::None, None, false);
        for v in values {
            builder.append(now_ms(), None, Some(v.as_bytes()), &[]);
        }
        let mut bytes = builder
            .build(ProducerIdentity::NONE, -1)
            .unwrap()
            .bytes()
            .to_vec();
        // The CRC does not cover the base offset.
        bytes[..8].copy_from_slice(&base_offset.to_be_bytes());
        bytes
    };
    let bad_message =
        Polled::Err("Message consumption error: BadMessage (Local: Bad message format)".into());

    // Base offset 5, length 5, leader epoch 0, magic 3.
    let mut unknown_magic = Vec::new();
    unknown_magic.extend_from_slice(&5i64.to_be_bytes());
    unknown_magic.extend_from_slice(&5i32.to_be_bytes());
    unknown_magic.extend_from_slice(&0i32.to_be_bytes());
    unknown_magic.push(3);
    let before = Instant::now();
    let (offset, backoff) = fetched(unknown_magic);
    assert_eq!(offset, 5);
    assert!(backoff.is_some_and(|at| at >= before + Duration::from_millis(300)));
    let got = poll_until(&consumer, Duration::from_secs(5), |i| !i.is_empty());
    assert_eq!(got, vec![bad_message.clone()]);

    let mut records = batch(5, &["a", "b"]);
    records.extend(batch(i64::MAX, &["c"]));
    let (offset, backoff) = fetched(records);
    assert_eq!((offset, backoff), (7, None));
    let got = poll_until(&consumer, Duration::from_secs(5), |i| i.len() == 3);
    assert_eq!(
        got,
        vec![
            Polled::Msg(0, 5, Some("a".into())),
            Polled::Msg(0, 6, Some("b".into())),
            bad_message.clone(),
        ]
    );
    assert_eq!(position(&consumer, topic, 0), Offset::Offset(7));

    let (offset, backoff) = fetched(batch(i64::MAX, &["c"]));
    assert_eq!(offset, 7);
    assert!(backoff.is_some());
    let got = poll_until(&consumer, Duration::from_secs(5), |i| !i.is_empty());
    assert_eq!(got, vec![bad_message]);
    assert_eq!(position(&consumer, topic, 0), Offset::Offset(7));
}

/// A commit that finds no coordinator fails after `session.timeout.ms`, plus
/// `socket.timeout.ms` for the request, like librdkafka's, instead of
/// waiting for the cluster indefinitely.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn commit_without_coordinator_times_out() {
    let (consumer, recorder) = consumer(
        "127.0.0.1:1",
        &[
            ("group.id", "mzkc-test"),
            ("session.timeout.ms", "1000"),
            ("socket.timeout.ms", "1000"),
        ],
    );
    let offsets = tpl(&[("mzkc-c-commit", 0, Offset::Offset(1))]);
    let start = Instant::now();
    let result = consumer.commit(&offsets, CommitMode::Sync);
    let elapsed = start.elapsed();
    let want = Err(KafkaError::ConsumerCommit(
        RDKafkaErrorCode::WaitingForCoordinator,
    ));
    assert_eq!(result, want);
    assert!(
        elapsed >= Duration::from_millis(1500) && elapsed < Duration::from_secs(10),
        "{elapsed:?}"
    );
    assert_eq!(*recorder.commits.lock().unwrap(), vec![want]);
}

/// A poll that may block counts as polling while it waits, like librdkafka's,
/// so that it does not exceed `max.poll.interval.ms`. That includes polls of
/// a partition's queue.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn blocking_poll_counts_as_polling() {
    let (consumer, _) = consumer("127.0.0.1:1", &[("group.id", "mzkc-test")]);
    let inner = Arc::clone(consumer.inner());
    std::thread::sleep(Duration::from_millis(200));
    assert!(inner.since_poll() >= Duration::from_millis(200));
    let c = Arc::clone(&consumer);
    let poller = std::thread::spawn(move || {
        let _ = c.poll(Duration::from_millis(1000));
    });
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(inner.since_poll(), Duration::ZERO);
    poller.join().unwrap();
    assert!(inner.since_poll() < Duration::from_millis(500));

    consumer
        .assign(&tpl(&[("mzkc-c-maxpoll", 0, Offset::Offset(0))]))
        .unwrap();
    let q = consumer.split_partition_queue("mzkc-c-maxpoll", 0).unwrap();
    std::thread::sleep(Duration::from_millis(200));
    assert!(inner.since_poll() >= Duration::from_millis(200));
    let poller = std::thread::spawn(move || {
        let _ = q.poll(Duration::from_millis(1000));
    });
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(inner.since_poll(), Duration::ZERO);
    poller.join().unwrap();
    assert!(inner.since_poll() < Duration::from_millis(500));
}

/// A context whose error callback uses the consumer, as applications may.
#[derive(Default)]
struct Reentrant {
    consumer: OnceLock<Weak<BaseConsumer<Arc<Reentrant>>>>,
    queue: OnceLock<Weak<PartitionQueue<Arc<Reentrant>>>>,
    reasons: Mutex<Vec<String>>,
}

impl ClientContext for Arc<Reentrant> {
    fn error(&self, _error: KafkaError, reason: &str) {
        self.reasons.lock().unwrap().push(reason.to_string());
        if let Some(consumer) = self.consumer.get().and_then(Weak::upgrade) {
            let _ = consumer.poll(Duration::ZERO);
            consumer.assignment().unwrap();
        }
        if let Some(queue) = self.queue.get().and_then(Weak::upgrade) {
            let _ = queue.poll(Duration::ZERO);
        }
    }
}

impl ConsumerContext for Arc<Reentrant> {}

/// The error callback for a notice in a partition's queue runs with no lock
/// of the consumer held, so it may poll and inspect the consumer.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn error_callback_may_use_the_consumer() {
    let topic = "mzkc-c-reentrant";
    let ctx = Arc::new(Reentrant::default());
    let consumer = Arc::new(
        BaseConsumer::from_config_and_context(
            &config("127.0.0.1:1", &[("group.id", "mzkc-test")]),
            Arc::clone(&ctx),
        )
        .unwrap(),
    );
    ctx.consumer.set(Arc::downgrade(&consumer)).unwrap();
    consumer
        .assign(&tpl(&[
            (topic, 0, Offset::Offset(0)),
            (topic, 1, Offset::Offset(0)),
        ]))
        .unwrap();
    let q1 = Arc::new(consumer.split_partition_queue(topic, 1).unwrap());
    ctx.queue.set(Arc::downgrade(&q1)).unwrap();
    let notice = |id: i32, reason: &str| {
        let inner = consumer.inner();
        let partition = Arc::clone(&inner.lock_state().partitions[topic][&id]);
        let mut state = partition.lock_state();
        let wake = state
            .push_notice(
                RDKafkaErrorCode::UnknownPartition,
                reason.to_string(),
                partition.is_split(),
            )
            .unwrap();
        drop(state);
        partition.notify(inner, wake);
    };
    let delivered = |reason: &str| ctx.reasons.lock().unwrap().iter().any(|r| r == reason);
    // Runs `f` on another thread, failing if it does not return in time.
    let within = |f: Box<dyn FnOnce() + Send>| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            f();
            let _ = tx.send(());
        });
        rx.recv_timeout(Duration::from_secs(10))
            .expect("poll deadlocked");
    };

    notice(0, "main");
    let c = Arc::clone(&consumer);
    within(Box::new(move || {
        let _ = c.poll(Duration::from_millis(500));
    }));
    assert!(delivered("main"));

    notice(1, "split");
    let q = Arc::clone(&q1);
    within(Box::new(move || {
        let _ = q.poll(Duration::from_millis(500));
    }));
    assert!(delivered("split"));
}
