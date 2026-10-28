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
use crate::statistics::Statistics;
use crate::types::RDKafkaErrorCode;
use crate::util::Deadline;

use super::{Cluster, MetadataTopics, StatsSource};

/// Records log lines and error callbacks.
#[derive(Default)]
pub(crate) struct Recorder {
    pub logs: Mutex<Vec<(RDKafkaLogLevel, String, String)>>,
    pub errors: Mutex<Vec<(KafkaError, String)>>,
    pub stats: Mutex<Vec<Statistics>>,
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

    fn stats(&self, statistics: Statistics) {
        self.stats.lock().unwrap().push(statistics);
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

struct FixedStats;

impl StatsSource for FixedStats {
    fn fill(&self, stats: &mut Statistics) {
        stats.msg_cnt = 7;
        stats.txmsgs = 3;
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn statistics() {
    for brokers in test_brokers() {
        let (client, recorder) = client(
            ClientKind::Producer,
            &[
                ("bootstrap.servers", brokers.as_str()),
                ("statistics.interval.ms", "100"),
                ("client.id", "stats-test"),
            ],
        );
        let source: Arc<dyn StatsSource> = Arc::new(FixedStats);
        client
            .cluster()
            .register_stats_source(Arc::downgrade(&source));
        // The second request goes to a learned broker; the bootstrap broker
        // (and its counters) is retired, as in librdkafka.
        for _ in 0..2 {
            client
                .fetch_metadata(None, Duration::from_secs(10))
                .unwrap();
        }
        recorder.stats.lock().unwrap().clear();
        let start = Instant::now();
        while recorder.stats.lock().unwrap().len() < 2 {
            assert!(start.elapsed() < Duration::from_secs(5));
            std::thread::sleep(Duration::from_millis(20));
        }
        let stats = recorder.stats.lock().unwrap().last().cloned().unwrap();
        assert!(
            stats.name.starts_with("stats-test#producer-"),
            "{}",
            stats.name
        );
        assert_eq!(stats.client_id, "stats-test");
        assert_eq!(stats.client_type, "producer");
        assert_eq!(stats.msg_cnt, 7);
        assert_eq!(stats.txmsgs, 3);
        assert!(
            stats.tx >= 1 && stats.rx >= 1 && stats.tx_bytes > 0,
            "{stats:?}"
        );
        assert!(!stats.brokers.is_empty());
        let total_connects: i64 = stats.brokers.values().filter_map(|b| b.connects).sum();
        assert!(total_connects >= 1);
        // Statistics round-trip through JSON with every required field.
        let json = serde_json::to_string(&stats).unwrap();
        let _: Statistics = serde_json::from_str(&json).unwrap();
        drop(source);
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn idle_connections_close() {
    for brokers in test_brokers() {
        let (client, recorder) = client(
            ClientKind::Consumer,
            &[
                ("bootstrap.servers", brokers.as_str()),
                ("connections.max.idle.ms", "1000"),
                ("debug", "broker"),
            ],
        );
        for _ in 0..2 {
            client
                .fetch_metadata(None, Duration::from_secs(10))
                .unwrap();
        }
        let start = Instant::now();
        while client.cluster().brokers().iter().any(|b| b.is_up()) {
            assert!(start.elapsed() < Duration::from_secs(5));
            std::thread::sleep(Duration::from_millis(50));
        }
        let logs = recorder.logs.lock().unwrap().clone();
        assert!(
            logs.iter()
                .any(|(level, fac, msg)| *level == RDKafkaLogLevel::Debug
                    && fac == "FAIL"
                    && msg.contains("Connection max idle time exceeded (")),
            "{logs:#?}"
        );
        // Idle closes are planned: no error callbacks.
        assert!(recorder.error_reasons().is_empty());
        // And the client reconnects on demand.
        client
            .fetch_metadata(None, Duration::from_secs(10))
            .unwrap();
    }
}

/// A fake broker that answers ApiVersions only with version 0 and handles
/// other requests with `respond(api_key, correlation_id)`, which returns the
/// raw bytes to write (length prefix included), or `None` for no response.
fn fake_broker(
    respond: impl Fn(i16, [u8; 4]) -> Option<Vec<u8>> + Send + Sync + 'static,
) -> std::net::SocketAddr {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let respond = Arc::new(respond);
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { return };
            let respond = Arc::clone(&respond);
            std::thread::spawn(move || {
                loop {
                    let mut len = [0u8; 4];
                    if conn.read_exact(&mut len).is_err() {
                        return;
                    }
                    let mut frame = vec![0u8; usize::try_from(i32::from_be_bytes(len)).unwrap()];
                    if conn.read_exact(&mut frame).is_err() {
                        return;
                    }
                    let key = i16::from_be_bytes([frame[0], frame[1]]);
                    let version = i16::from_be_bytes([frame[2], frame[3]]);
                    let corr: [u8; 4] = frame[4..8].try_into().unwrap();
                    let out = if key == 18 {
                        let body = if version > 0 {
                            // UNSUPPORTED_VERSION, no API list.
                            vec![0, 35, 0, 0, 0, 0]
                        } else {
                            let mut b = vec![0, 0, 0, 0, 0, 2];
                            // Metadata 0..12, ApiVersions 0..3.
                            b.extend_from_slice(&[0, 3, 0, 0, 0, 12, 0, 18, 0, 0, 0, 3]);
                            b
                        };
                        Some(raw_frame(corr, &body))
                    } else {
                        respond(key, corr)
                    };
                    if let Some(out) = out {
                        if conn.write_all(&out).is_err() {
                            return;
                        }
                    }
                }
            });
        }
    });
    addr
}

/// A response frame: length, correlation id, body.
fn raw_frame(corr: [u8; 4], body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let len = i32::try_from(body.len() + 4).unwrap();
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&corr);
    out.extend_from_slice(body);
    out
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn request_timeouts_disconnect() {
    // Answers ApiVersions (after falling back to v0), never Metadata.
    let addr = fake_broker(|_, _| None);
    let bootstrap = addr.to_string();
    let (client, recorder) = client(
        ClientKind::Consumer,
        &[
            ("bootstrap.servers", bootstrap.as_str()),
            ("socket.timeout.ms", "300"),
            ("debug", "broker"),
        ],
    );
    let start = Instant::now();
    let err = client
        .fetch_metadata(None, Duration::from_secs(5))
        .unwrap_err();
    // Three attempts of 300 ms each, as librdkafka's two retries.
    assert_eq!(
        err,
        KafkaError::MetadataFetch(RDKafkaErrorCode::OperationTimedOut)
    );
    assert!(
        start.elapsed() < Duration::from_secs(3),
        "{:?}",
        start.elapsed()
    );
    let logs = recorder.logs.lock().unwrap().clone();
    let name = format!("{addr}/bootstrap");
    let has = |level: RDKafkaLogLevel, fac: &str, text: &str| {
        logs.iter()
            .any(|(l, f, m)| *l == level && f == fac && m.contains(text))
    };
    assert!(has(
        RDKafkaLogLevel::Notice,
        "REQTMOUT",
        &format!("[thrd:{name}]: {name}: Timed out MetadataRequest in flight (after ")
    ));
    assert!(has(
        RDKafkaLogLevel::Warning,
        "REQTMOUT",
        "Timed out 1 in-flight, 0 retry-queued, 0 out-queue, 0 partially-sent requests"
    ));
    assert!(
        has(
            RDKafkaLogLevel::Debug,
            "FAIL",
            "1 request(s) timed out: disconnect (after "
        ),
        "{logs:#?}"
    );
    let counters = client.cluster().brokers()[0]
        .counters()
        .req_timeouts
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(counters, 3);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn bad_responses() {
    // A Metadata response declaring 2 MB, above receive.message.max.bytes.
    let addr = fake_broker(|_, corr| {
        let mut out = 2_000_000i32.to_be_bytes().to_vec();
        out.extend_from_slice(&corr);
        Some(out)
    });
    let bootstrap = addr.to_string();
    // A producer: consumers require more than `fetch.max.bytes`.
    let (client, recorder) = client(
        ClientKind::Producer,
        &[
            ("bootstrap.servers", bootstrap.as_str()),
            ("receive.message.max.bytes", "1000"),
        ],
    );
    let err = client
        .fetch_metadata(None, Duration::from_secs(2))
        .unwrap_err();
    assert_eq!(err, KafkaError::MetadataFetch(RDKafkaErrorCode::BadMessage));
    let lines = recorder.fail_lines();
    assert!(
        lines[0].contains(
            "/bootstrap: Receive failed: Invalid response size 2000000 (0..1000): increase \
             receive.message.max.bytes (after "
        ),
        "{lines:#?}"
    );

    // An undecodable Metadata response.
    let addr = fake_broker(|_, corr| Some(raw_frame(corr, &[0, 0])));
    let bootstrap = addr.to_string();
    let (client2, _recorder) = self::client(
        ClientKind::Consumer,
        &[("bootstrap.servers", bootstrap.as_str())],
    );
    let err = client2
        .fetch_metadata(None, Duration::from_secs(2))
        .unwrap_err();
    assert_eq!(err, KafkaError::MetadataFetch(RDKafkaErrorCode::BadMessage));
}

/// Prints log lines and error callbacks like the librdkafka differential
/// harness does.
struct PrintCtx {
    start: Instant,
    alias: Option<String>,
}

impl ClientContext for PrintCtx {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, msg: &str) {
        println!(
            "{:>6}ms LOG {:?} {} {}",
            self.start.elapsed().as_millis(),
            level,
            fac,
            msg
        );
    }

    fn error(&self, error: KafkaError, reason: &str) {
        println!(
            "{:>6}ms ERR {} | {}",
            self.start.elapsed().as_millis(),
            error,
            reason
        );
    }

    fn resolve_broker_addr(
        &self,
        host: &str,
        port: u16,
    ) -> std::io::Result<Vec<std::net::SocketAddr>> {
        use std::net::ToSocketAddrs;
        let host = if self.alias.as_deref() == Some(host) {
            "127.0.0.1"
        } else {
            host
        };
        (host, port).to_socket_addrs().map(|a| a.collect())
    }
}

/// Counterpart of the librdkafka differential harness: runs the scenario in
/// `MZKC_DIFF` (`<timeout_ms> key=value ...`, `@S@` replaced by
/// `MZKC_SECRETS`), with `MZKC_ALIAS`, `MZKC_TOPIC`, `MZKC_KIND` and
/// `MZKC_LINGER_MS` as in the harness.
#[mz_ore::test]
#[ignore]
fn diff_scenario() {
    let spec = std::env::var("MZKC_DIFF").expect("MZKC_DIFF");
    let secrets = std::env::var("MZKC_SECRETS").unwrap_or_default();
    let mut parts = spec.split_whitespace();
    let timeout = Duration::from_millis(parts.next().unwrap().parse().unwrap());
    let mut config = ClientConfig::new();
    config.set_log_level(RDKafkaLogLevel::Debug);
    for kv in parts {
        let (k, v) = kv.split_once('=').unwrap();
        config.set(k, v.replace("@S@", &secrets));
    }
    let kind = match std::env::var("MZKC_KIND").as_deref() {
        Ok("producer") => ClientKind::Producer,
        _ => ClientKind::Consumer,
    };
    let start = Instant::now();
    let ctx = PrintCtx {
        start,
        alias: std::env::var("MZKC_ALIAS").ok().filter(|a| !a.is_empty()),
    };
    let client = match ClientSettings::from_config(&config, kind)
        .and_then(|settings| Client::new(settings, ctx))
    {
        Ok(c) => c,
        Err(e) => {
            println!("CREATE_ERR {e}");
            return;
        }
    };
    let topic = std::env::var("MZKC_TOPIC").ok();
    match client.fetch_metadata(topic.as_deref(), timeout) {
        Ok(md) => println!(
            "{:>6}ms RESULT Ok orig={} brokers={:?} topics={:?}",
            start.elapsed().as_millis(),
            md.orig_broker_name(),
            md.brokers()
                .iter()
                .map(|b| format!("{}:{}:{}", b.id(), b.host(), b.port()))
                .collect::<Vec<_>>(),
            md.topics()
                .iter()
                .map(|t| format!("{}:{:?}:{}", t.name(), t.error(), t.partitions().len()))
                .collect::<Vec<_>>()
        ),
        Err(e) => println!("{:>6}ms RESULT Err {e}", start.elapsed().as_millis()),
    }
    let linger = std::env::var("MZKC_LINGER_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(500);
    std::thread::sleep(Duration::from_millis(linger));
}
