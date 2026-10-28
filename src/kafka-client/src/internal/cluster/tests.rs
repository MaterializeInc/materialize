// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the cluster and the client facade against brokers.
//!
//! Tests that need a broker run only when `MZ_KAFKA_CLIENT_TEST_BROKERS` is
//! set to one or more `;`-separated bootstrap server lists, e.g.
//! `127.0.0.1:19092;127.0.0.1:29092`, and are skipped otherwise. The
//! others use closed ports and unresolvable names.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
use kafka_protocol::messages::{CreateTopicsRequest, MetadataRequest, ProduceRequest, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::client::{Client, ClientContext};
use crate::config::settings::{ClientKind, ClientSettings};
use crate::config::{ClientConfig, RDKafkaLogLevel};
use crate::error::KafkaError;
use crate::internal::compression::Compression;
use crate::internal::net::RequestOptions;
use crate::internal::records::{BatchBuilder, ProducerIdentity};
use crate::internal::runtime;
use crate::types::RDKafkaErrorCode;
use crate::util::Deadline;

use super::{Cluster, MetadataTopics};

/// Records log lines and error callbacks.
#[derive(Default)]
pub(crate) struct Recorder {
    pub logs: Mutex<Vec<(RDKafkaLogLevel, String, String)>>,
    pub errors: Mutex<Vec<(KafkaError, String)>>,
}

impl ClientContext for Recorder {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        self.logs
            .lock()
            .unwrap()
            .push((level, fac.to_string(), log_message.to_string()));
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.errors
            .lock()
            .unwrap()
            .push((error, reason.to_string()));
    }
}

impl Recorder {
    fn fail_lines(&self) -> Vec<String> {
        self.logs
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, fac, _)| fac == "FAIL")
            .map(|(_, _, msg)| msg.clone())
            .collect()
    }

    fn error_reasons(&self) -> Vec<String> {
        self.errors
            .lock()
            .unwrap()
            .iter()
            .map(|(e, reason)| format!("{e}: {reason}"))
            .collect()
    }
}

/// The bootstrap server lists to test against, if configured.
pub(crate) fn test_brokers() -> Vec<String> {
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

pub(crate) fn client(
    kind: ClientKind,
    pairs: &[(&str, &str)],
) -> (Client<Arc<Recorder>>, Arc<Recorder>) {
    let mut config = ClientConfig::new();
    config.set_log_level(RDKafkaLogLevel::Debug);
    for (k, v) in pairs {
        config.set(*k, *v);
    }
    let settings = ClientSettings::from_config(&config, kind).unwrap();
    let recorder = Arc::new(Recorder::default());
    let client = Client::new(settings, Arc::clone(&recorder)).unwrap();
    (client, recorder)
}

impl ClientContext for Arc<Recorder> {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        Recorder::log(self, level, fac, log_message)
    }

    fn error(&self, error: KafkaError, reason: &str) {
        Recorder::error(self, error, reason)
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

/// Creates a topic through the controller and waits until its partitions
/// have leaders.
pub(crate) fn create_topic(cluster: &Arc<Cluster>, name: &str, partitions: i32) {
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
        for p in 0..partitions {
            cluster.leader(&name, p, deadline).await.unwrap();
        }
    })
}

/// Produces `values` to a partition with acks=1, returning the base offset.
pub(crate) fn produce(
    cluster: &Arc<Cluster>,
    topic: &str,
    partition: i32,
    values: &[&[u8]],
) -> i64 {
    let cluster = Arc::clone(cluster);
    let topic = topic.to_string();
    let mut batch = BatchBuilder::new(Compression::None, None, false);
    for v in values {
        batch.append(1, None, Some(v), &[]);
    }
    let records = batch.build(ProducerIdentity::NONE, -1).unwrap().bytes();
    runtime::block_on(async move {
        let deadline = Deadline::new(Duration::from_secs(30));
        let leader = cluster.leader(&topic, partition, deadline).await.unwrap();
        let response = leader
            .request::<ProduceRequest, _>(
                |_| {
                    ProduceRequest::default()
                        .with_acks(1)
                        .with_timeout_ms(10_000)
                        .with_topic_data(vec![
                            TopicProduceData::default()
                                .with_name(TopicName(StrBytes::from_string(topic.clone())))
                                .with_partition_data(vec![
                                    PartitionProduceData::default()
                                        .with_index(partition)
                                        .with_records(Some(records)),
                                ]),
                        ])
                },
                RequestOptions::default(),
            )
            .await
            .unwrap();
        let p = &response.responses[0].partition_responses[0];
        assert_eq!(p.error_code, 0, "{response:?}");
        p.base_offset
    })
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn metadata_and_watermarks() {
    for brokers in test_brokers() {
        let (client, recorder) = client(
            ClientKind::Consumer,
            &[("bootstrap.servers", brokers.as_str())],
        );
        let md = client
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
        assert!(!md.brokers().is_empty(), "{md:?}");
        assert!(md.orig_broker_name().ends_with("/bootstrap"), "{md:?}");

        let missing = unique("mzkc-missing");
        let md = client
            .fetch_metadata(Some(&missing), Duration::from_secs(10))
            .unwrap();
        assert_eq!(md.topics().len(), 1);
        assert_eq!(md.topics()[0].name(), missing);
        assert_eq!(
            md.topics()[0].error(),
            Some(RDKafkaErrorCode::UnknownTopicOrPartition)
        );
        // Not auto-created by a consumer's metadata request.
        let md = client
            .fetch_metadata(Some(&missing), Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            md.topics()[0].error(),
            Some(RDKafkaErrorCode::UnknownTopicOrPartition)
        );
        assert_eq!(
            client.fetch_watermarks(&missing, 0, Duration::from_secs(5)),
            Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::UnknownTopicOrPartition
            ))
        );

        let topic = unique("mzkc-wm");
        create_topic(client.cluster(), &topic, 2);
        assert_eq!(
            client.fetch_watermarks(&topic, 0, Duration::from_secs(10)),
            Ok((0, 0))
        );
        assert_eq!(produce(client.cluster(), &topic, 1, &[b"a", b"b", b"c"]), 0);
        assert_eq!(
            client.fetch_watermarks(&topic, 1, Duration::from_secs(10)),
            Ok((0, 3))
        );
        assert_eq!(
            client.fetch_watermarks(&topic, 7, Duration::from_secs(5)),
            Err(KafkaError::MetadataFetch(
                RDKafkaErrorCode::UnknownPartition
            ))
        );
        let md = client
            .fetch_metadata(Some(&topic), Duration::from_secs(10))
            .unwrap();
        let t = &md.topics()[0];
        assert_eq!(t.error(), None);
        assert_eq!(t.partitions().len(), 2);
        assert!(
            t.partitions()
                .iter()
                .all(|p| p.leader() >= 0 && !p.replicas().is_empty() && !p.isr().is_empty())
        );

        // The cache has the topic, and brokers were learned.
        let snapshot = client.cluster().metadata();
        assert_eq!(snapshot.partition_count(&topic), Some(2));
        assert!(client.cluster().brokers().iter().all(|b| b.id() >= 0));
        assert!(
            recorder.error_reasons().is_empty(),
            "{:?}",
            recorder.error_reasons()
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn pipelined_requests_and_metadata_watch() {
    for brokers in test_brokers() {
        let (client, _recorder) = client(
            ClientKind::Producer,
            &[
                ("bootstrap.servers", brokers.as_str()),
                ("max.in.flight", "7"),
            ],
        );
        let cluster = Arc::clone(client.cluster());
        let mut watch = cluster.subscribe_metadata();
        let n = runtime::block_on(async move {
            let deadline = Deadline::new(Duration::from_secs(10));
            let broker = cluster.any_broker(deadline).await.unwrap();
            let start = Instant::now();
            let requests = (0..200).map(|_| {
                broker.request::<MetadataRequest, _>(
                    |_| MetadataRequest::default().with_topics(Some(vec![])),
                    RequestOptions::default(),
                )
            });
            let responses = futures::future::join_all(requests).await;
            assert!(responses.iter().all(|r| r.is_ok()));
            assert!(start.elapsed() < Duration::from_secs(10));
            assert!(watch.has_changed().unwrap());
            let version = watch.borrow_and_update().version;
            assert!(version >= 200, "{version}");
            responses.len()
        });
        assert_eq!(n, 200);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn drop_shuts_down() {
    for brokers in test_brokers() {
        let (client, _recorder) = client(
            ClientKind::Consumer,
            &[("bootstrap.servers", brokers.as_str())],
        );
        // The first request goes to the bootstrap broker, which is retired
        // once brokers are learned; the second connects a learned broker.
        for _ in 0..2 {
            client
                .fetch_metadata(None, Duration::from_secs(10))
                .unwrap();
        }
        let cluster = Arc::downgrade(client.cluster());
        let token = client.cluster().shutdown_token();
        let brokers = client.cluster().brokers();
        assert!(brokers.iter().any(|b| b.is_up()));
        drop(client);
        assert!(token.is_cancelled());
        // Tasks observe the cancellation and release the cluster; connections
        // close and brokers report down soon after.
        let start = Instant::now();
        while cluster.upgrade().is_some() || brokers.iter().any(|b| b.is_up()) {
            assert!(start.elapsed() < Duration::from_secs(5));
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn connection_refused() {
    // Find a closed port.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let bootstrap = format!("127.0.0.1:{port}");
    let (client, recorder) = client(
        ClientKind::Consumer,
        &[
            ("bootstrap.servers", bootstrap.as_str()),
            ("reconnect.backoff.ms", "10"),
            ("reconnect.backoff.max.ms", "50"),
        ],
    );
    let start = Instant::now();
    let err = client
        .fetch_metadata(None, Duration::from_millis(500))
        .unwrap_err();
    // Waits the full timeout for a broker, like rd_kafka_metadata.
    assert!(start.elapsed() >= Duration::from_millis(500));
    assert_eq!(
        err,
        KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure)
    );
    assert_eq!(
        err.to_string(),
        "Meta data fetch error: BrokerTransportFailure (Local: Broker transport failure)"
    );
    let lines = recorder.fail_lines();
    let expected = format!(
        "[thrd:127.0.0.1:{port}/bootstrap]: 127.0.0.1:{port}/bootstrap: Connect to \
         ipv4#127.0.0.1:{port} failed: Connection refused (after "
    );
    assert!(
        lines
            .first()
            .is_some_and(|l| l.starts_with(&expected) && l.ends_with("ms in state CONNECT)")),
        "{lines:#?}"
    );
    let errors = recorder.error_reasons();
    assert!(
        errors.iter().any(|e| e
            == "Global error: AllBrokersDown (Local: All broker connections are down): 1/1 brokers are down"),
        "{errors:#?}"
    );
    assert!(
        errors.iter().any(|e| e.starts_with(&format!(
            "Global error: BrokerTransportFailure (Local: Broker transport failure): \
             127.0.0.1:{port}/bootstrap: Connect to ipv4#127.0.0.1:{port} failed: Connection refused"
        ))),
        "{errors:#?}"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn resolve_failure() {
    let (client, recorder) = client(
        ClientKind::Consumer,
        &[("bootstrap.servers", "mzkc-does-not-exist.invalid:9092")],
    );
    let err = client
        .fetch_metadata(None, Duration::from_millis(300))
        .unwrap_err();
    assert_eq!(
        err,
        KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure)
    );
    let lines = recorder.fail_lines();
    assert!(
        lines.first().is_some_and(|l| l.starts_with(
            "[thrd:mzkc-does-not-exist.invalid:9092/bootstrap]: \
             mzkc-does-not-exist.invalid:9092/bootstrap: Failed to resolve \
             'mzkc-does-not-exist.invalid:9092': Name or service not known (after "
        )),
        "{lines:#?}"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn no_bootstrap_servers() {
    let (client, recorder) = client(ClientKind::Consumer, &[]);
    let err = client
        .fetch_metadata(None, Duration::from_millis(100))
        .unwrap_err();
    assert_eq!(
        err,
        KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure)
    );
    let logs = recorder.logs.lock().unwrap();
    assert!(logs.iter().any(
        |(_, fac, msg)| fac == "CONFWARN" && msg.contains("No `bootstrap.servers` configured")
    ));
    drop(logs);
    let cluster = Arc::clone(client.cluster());
    let err = runtime::block_on(async move {
        cluster
            .fetch_metadata(MetadataTopics::All, Deadline::new(Duration::ZERO))
            .await
            .unwrap_err()
    });
    assert_eq!(err.code(), RDKafkaErrorCode::BrokerTransportFailure);
}
