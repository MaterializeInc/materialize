// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests that drive the server with librdkafka producers.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::future::BoxFuture;
use mz_kafka_server::{
    Admission, CompactedRecord, KafkaServer, KafkaServerConfig, ProducedRecord, ProducerStates,
    RecordValidator, Rejection, ResponseError, SaslPlainCredentials, TopicOptions,
    TopicRegistration, TopicSchemas,
};
use rdkafka::Message;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use tokio::net::TcpListener;

async fn start_server() -> (KafkaServer, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = KafkaServer::new(KafkaServerConfig {
        advertised_host: addr.ip().to_string(),
        advertised_port: addr.port(),
        cluster_id: "test".into(),
    });
    let accept_server = server.clone();
    mz_ore::task::spawn(|| "accept", async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let server = accept_server.clone();
            mz_ore::task::spawn(
                || "conn",
                async move { server.serve_connection(stream).await },
            );
        }
    });
    (server, addr)
}

fn producer(addr: SocketAddr, extra: &[(&str, &str)]) -> FutureProducer {
    let mut config = mz_kafka_util::client::create_new_client_config_simple();
    config
        .set("bootstrap.servers", addr.to_string())
        .set("message.timeout.ms", "10000")
        .set("linger.ms", "0");
    for (k, v) in extra {
        config.set(*k, *v);
    }
    mz_kafka_util::client::create(&config).unwrap()
}

/// Acknowledges every batch received on `registration`, assigning offsets sequentially, and
/// returns the received records over a channel.
fn ack_all(
    mut registration: TopicRegistration,
) -> tokio::sync::mpsc::UnboundedReceiver<(i64, ProducedRecord)> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    mz_ore::task::spawn(|| "ack_all", async move {
        let mut next_offset = 0;
        loop {
            let batch = registration.recv().await;
            let base_offset = next_offset;
            for record in batch.records {
                let _ = tx.send((next_offset, record));
                next_offset += 1;
            }
            batch.ack.commit(base_offset);
        }
    });
    rx
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn test_produce() {
    let (server, addr) = start_server().await;
    let registration = server
        .register_topic(
            "events".into(),
            TopicOptions {
                credentials: None,
                validator: None,
                schemas: TopicSchemas::default(),
                queue_bytes: 1024 * 1024,
                compacted: Vec::new(),
            },
        )
        .unwrap();
    let mut received = ack_all(registration);

    for compression in ["none", "gzip", "snappy", "lz4"] {
        for idempotence in ["false", "true"] {
            let producer = producer(
                addr,
                &[
                    ("compression.type", compression),
                    ("enable.idempotence", idempotence),
                ],
            );
            let headers = OwnedHeaders::new().insert(Header {
                key: "h",
                value: Some("v"),
            });
            let record = FutureRecord::to("events")
                .key("k")
                .payload("hello")
                .headers(headers)
                .timestamp(1234);
            let (partition, offset) = producer
                .send(record, Duration::from_secs(10))
                .await
                .map_err(|(e, _)| e)
                .unwrap();
            assert_eq!(partition, 0);

            let (received_offset, record) = received.recv().await.unwrap();
            assert_eq!(offset, received_offset, "compression={compression}");
            assert_eq!(
                record,
                ProducedRecord {
                    key: Some(Bytes::from("k")),
                    value: Some(Bytes::from("hello")),
                    headers: vec![("h".into(), Some(Bytes::from("v")))],
                    timestamp: 1234,
                }
            );
        }
    }

    // A tombstone.
    let producer = producer(addr, &[]);
    producer
        .send(
            FutureRecord::<str, str>::to("events").key("k"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap();
    let (_, record) = received.recv().await.unwrap();
    assert_eq!(record.value, None);
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn test_unknown_topic() {
    let (_server, addr) = start_server().await;
    let producer = producer(addr, &[("message.timeout.ms", "2000")]);
    let err = producer
        .send(
            FutureRecord::<str, str>::to("missing").payload("x"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap_err();
    assert!(
        matches!(
            err,
            KafkaError::MessageProduction(
                RDKafkaErrorCode::UnknownTopicOrPartition
                    | RDKafkaErrorCode::UnknownTopic
                    // librdkafka retries unknown topics until the message times out.
                    | RDKafkaErrorCode::MessageTimedOut
            )
        ),
        "{err:?}"
    );
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn test_failed_ack() {
    let (server, addr) = start_server().await;
    let mut registration = server
        .register_topic(
            "events".into(),
            TopicOptions {
                credentials: None,
                validator: None,
                schemas: TopicSchemas::default(),
                queue_bytes: 1024 * 1024,
                compacted: Vec::new(),
            },
        )
        .unwrap();
    mz_ore::task::spawn(|| "fail_all", async move {
        loop {
            let batch = registration.recv().await;
            batch.ack.fail(ResponseError::InvalidRecord);
        }
    });
    let producer = producer(addr, &[]);
    let err = producer
        .send(
            FutureRecord::<str, str>::to("events").payload("x"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap_err();
    assert!(
        matches!(
            err,
            KafkaError::MessageProduction(RDKafkaErrorCode::InvalidRecord)
        ),
        "{err:?}"
    );
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn test_sasl_plain() {
    let (server, addr) = start_server().await;
    let credentials = SaslPlainCredentials {
        username: "alice".into(),
        password: "secret".into(),
    };
    let registration = server
        .register_topic(
            "private".into(),
            TopicOptions {
                credentials: Some(credentials),
                validator: None,
                schemas: TopicSchemas::default(),
                queue_bytes: 1024 * 1024,
                compacted: Vec::new(),
            },
        )
        .unwrap();
    let mut received = ack_all(registration);

    let sasl = |password| {
        producer(
            addr,
            &[
                ("security.protocol", "SASL_PLAINTEXT"),
                ("sasl.mechanisms", "PLAIN"),
                ("sasl.username", "alice"),
                ("sasl.password", password),
                ("message.timeout.ms", "3000"),
            ],
        )
    };

    sasl("secret")
        .send(
            FutureRecord::<str, str>::to("private").payload("x"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap();
    assert_eq!(
        received.recv().await.unwrap().1.value,
        Some(Bytes::from("x"))
    );

    // Wrong password.
    sasl("wrong")
        .send(
            FutureRecord::<str, str>::to("private").payload("x"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap_err();

    // No authentication.
    let err = producer(addr, &[("message.timeout.ms", "3000")])
        .send(
            FutureRecord::<str, str>::to("private").payload("x"),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(e, _)| e)
        .unwrap_err();
    assert!(
        matches!(
            err,
            KafkaError::MessageProduction(RDKafkaErrorCode::TopicAuthorizationFailed)
        ),
        "{err:?}"
    );
}

/// Rejects every record whose value starts with "bad".
#[derive(Debug)]
struct RejectBad;

impl RecordValidator for RejectBad {
    fn validate<'a>(
        &'a self,
        records: &'a [ProducedRecord],
    ) -> BoxFuture<'a, Result<(), Rejection>> {
        let record_errors: Vec<_> = records
            .iter()
            .enumerate()
            .filter(|(_, r)| r.value.as_ref().is_some_and(|v| v.starts_with(b"bad")))
            .map(|(i, _)| (i, "bad record".to_string()))
            .collect();
        let result = if record_errors.is_empty() {
            Ok(())
        } else {
            Err(Rejection {
                error: ResponseError::InvalidRecord,
                message: Some("invalid batch".into()),
                record_errors,
            })
        };
        Box::pin(futures::future::ready(result))
    }
}

/// An idempotent producer whose batch is rejected while later batches are in flight must
/// deliver every other record exactly once. The later batches fail with
/// `OUT_OF_ORDER_SEQUENCE_NUMBER`, because the rejected batch's sequence numbers were never
/// appended, and the producer recovers by bumping its epoch.
#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn test_validation_rejects_batch_mid_sequence() {
    let (server, addr) = start_server().await;
    let mut registration = server
        .register_topic(
            "events".into(),
            TopicOptions {
                credentials: None,
                validator: Some(Arc::new(RejectBad)),
                schemas: TopicSchemas::default(),
                queue_bytes: 1024 * 1024,
                compacted: Vec::new(),
            },
        )
        .unwrap();
    let (tx, mut received) = tokio::sync::mpsc::unbounded_channel();
    let out_of_order = Arc::new(AtomicUsize::new(0));
    let out_of_order_count = Arc::clone(&out_of_order);
    mz_ore::task::spawn(|| "ack_idempotent", async move {
        let mut producers = ProducerStates::default();
        let mut next_offset = 0;
        loop {
            let batch = registration.recv().await;
            let id = batch.producer.expect("idempotent producer");
            match producers.admit(&id, batch.records.len()) {
                Admission::Append => {}
                Admission::Duplicate { base_offset } => {
                    batch.ack.commit(base_offset);
                    continue;
                }
                Admission::Reject(e) => {
                    if e == ResponseError::OutOfOrderSequenceNumber {
                        out_of_order_count.fetch_add(1, Ordering::Relaxed);
                    }
                    batch.ack.fail(e);
                    continue;
                }
            }
            producers.record(&id, batch.records.len(), next_offset, 0);
            let base_offset = next_offset;
            // Holding the acknowledgement of the batches just before a bad record makes the
            // producer send the bad batch and the batches after it before it learns that the bad
            // one was rejected, because responses are returned in request order.
            let precedes_bad = batch.records.iter().any(|r| {
                let value: i64 = std::str::from_utf8(r.value.as_ref().unwrap())
                    .unwrap()
                    .parse()
                    .unwrap();
                (40..50).contains(&(value % 100))
            });
            for record in batch.records {
                let _ = tx.send((next_offset, record));
                next_offset += 1;
            }
            mz_ore::task::spawn(|| "ack", async move {
                if precedes_bad {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                batch.ack.commit(base_offset);
            });
        }
    });

    let producer = producer(
        addr,
        &[
            ("enable.idempotence", "true"),
            ("max.in.flight.requests.per.connection", "5"),
            // librdkafka keeps at most 5 messages of an idempotent partition in flight
            // (`RD_KAFKA_IDEMP_MAX_INFLIGHT`), so only batches of a single record are pipelined.
            ("batch.num.messages", "1"),
        ],
    );
    let values: Vec<String> = (0..500)
        .map(|i| {
            if i % 100 == 50 {
                format!("bad-{i}")
            } else {
                i.to_string()
            }
        })
        .collect();
    let sends = values.iter().map(|value| {
        producer.send(
            FutureRecord::<str, str>::to("events").payload(value),
            Duration::from_secs(10),
        )
    });
    let results = futures::future::join_all(sends).await;

    let mut acked = BTreeMap::new();
    for (i, result) in results.into_iter().enumerate() {
        let value = &values[i];
        match result {
            Ok((_, offset)) => {
                assert!(!value.starts_with("bad"), "{value} was acknowledged");
                acked.insert(offset, value.clone());
            }
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::InvalidRecord), _)) => {}
            Err((e, _)) => panic!("{value} failed with {e:?}"),
        }
    }
    assert_eq!(acked.len(), 495, "only the bad records fail");
    assert!(
        out_of_order.load(Ordering::Relaxed) > 0,
        "no batch was in flight behind a rejected one"
    );

    let mut delivered = BTreeMap::new();
    while delivered.len() < acked.len() {
        let (offset, record) = received.recv().await.unwrap();
        let value = String::from_utf8(record.value.unwrap().to_vec()).unwrap();
        delivered.insert(offset, value);
    }
    assert!(
        received.try_recv().is_err(),
        "unacknowledged records delivered"
    );
    assert_eq!(acked, delivered);
}

/// Never finishes validating a batch whose first value is "hang".
#[derive(Debug)]
struct HangOnMarked;

impl RecordValidator for HangOnMarked {
    fn validate<'a>(
        &'a self,
        records: &'a [ProducedRecord],
    ) -> BoxFuture<'a, Result<(), Rejection>> {
        if records[0].value.as_deref() == Some(b"hang".as_slice()) {
            Box::pin(futures::future::pending())
        } else {
            Box::pin(futures::future::ready(Ok(())))
        }
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn test_slow_validation_does_not_hold_back_earlier_responses() {
    let (server, addr) = start_server().await;
    let mut registration = server
        .register_topic(
            "events".into(),
            TopicOptions {
                credentials: None,
                validator: Some(Arc::new(HangOnMarked)),
                schemas: TopicSchemas::default(),
                queue_bytes: 1024 * 1024,
                compacted: Vec::new(),
            },
        )
        .unwrap();
    // The acknowledgement comes after the next request is read, whose validation never ends.
    mz_ore::task::spawn(|| "ack_late", async move {
        loop {
            let batch = registration.recv().await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            batch.ack.commit(0);
        }
    });
    let producer = producer(addr, &[("batch.num.messages", "1")]);
    let first = producer
        .send_result(FutureRecord::<str, str>::to("events").payload("first"))
        .unwrap();
    let _hang = producer
        .send_result(FutureRecord::<str, str>::to("events").payload("hang"))
        .unwrap();
    let delivered = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .expect("the first record's response waits for the second's validation");
    delivered.unwrap().map_err(|(e, _)| e).unwrap();
}

/// Acknowledges every batch received on `registration` and returns each batch's records and
/// compacted records over a channel.
fn ack_batches(
    mut registration: TopicRegistration,
) -> tokio::sync::mpsc::UnboundedReceiver<(Vec<ProducedRecord>, Vec<CompactedRecord>)> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    mz_ore::task::spawn(|| "ack_batches", async move {
        loop {
            let batch = registration.recv().await;
            let _ = tx.send((batch.records, batch.compacted));
            batch.ack.commit(0);
        }
    });
    rx
}

/// Reads compacted topic `topic` up to its high watermark and returns the values by key.
fn read_compacted(addr: SocketAddr, topic: &str) -> Vec<(String, Option<String>)> {
    let mut config = mz_kafka_util::client::create_new_client_config_simple();
    config
        .set("bootstrap.servers", addr.to_string())
        .set("group.id", "reader")
        .set("enable.auto.commit", "false")
        .set("isolation.level", "read_committed")
        .set("enable.partition.eof", "true");
    let consumer: BaseConsumer = mz_kafka_util::client::create(&config).unwrap();
    let (lo, hi) = consumer
        .fetch_watermarks(topic, 0, Duration::from_secs(10))
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic, 0, Offset::Offset(lo))
        .unwrap();
    consumer.assign(&tpl).unwrap();
    let mut records = Vec::new();
    loop {
        let position = match consumer
            .position()
            .unwrap()
            .find_partition(topic, 0)
            .unwrap()
            .offset()
        {
            Offset::Offset(o) => o,
            _ => lo,
        };
        if position >= hi {
            return records;
        }
        match consumer.poll(Duration::from_secs(10)) {
            Some(Ok(message)) => records.push((
                String::from_utf8(message.key().unwrap().to_vec()).unwrap(),
                message
                    .payload()
                    .map(|v| String::from_utf8(v.to_vec()).unwrap()),
            )),
            Some(Err(KafkaError::PartitionEOF(_))) => {}
            other => panic!("unexpected poll result: {other:?}"),
        }
    }
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn test_transactions() {
    let (server, addr) = start_server().await;
    let registration = server
        .register_topic(
            "events".into(),
            TopicOptions {
                credentials: None,
                validator: None,
                schemas: TopicSchemas::default(),
                queue_bytes: 1024 * 1024,
                compacted: Vec::new(),
            },
        )
        .unwrap();
    let mut received = ack_batches(registration);

    let mut admin_config = mz_kafka_util::client::create_new_client_config_simple();
    admin_config.set("bootstrap.servers", addr.to_string());
    let admin: AdminClient<DefaultClientContext> =
        mz_kafka_util::client::create(&admin_config).unwrap();
    let new_topic = NewTopic::new("progress", 1, TopicReplication::Fixed(1));
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .unwrap();
    assert!(results[0].is_ok(), "{results:?}");
    let results = admin
        .create_topics([&new_topic], &AdminOptions::new())
        .await
        .unwrap();
    assert!(
        matches!(results[0], Err((_, RDKafkaErrorCode::TopicAlreadyExists))),
        "{results:?}"
    );

    let txn_producer = || {
        let producer = producer(addr, &[("transactional.id", "sink")]);
        producer.init_transactions(Duration::from_secs(10)).unwrap();
        producer
    };
    let send = |producer: &FutureProducer, topic: &'static str, key: &'static str, value| {
        producer
            .send_result(FutureRecord::to(topic).key(key).payload(value))
            .unwrap();
    };
    let producer = txn_producer();

    // A committed transaction reaches the topic as one batch, with its compacted records.
    producer.begin_transaction().unwrap();
    send(&producer, "events", "a", "1");
    send(&producer, "events", "b", "2");
    send(&producer, "progress", "p", "old");
    send(&producer, "progress", "p", "t1");
    producer
        .commit_transaction(Duration::from_secs(10))
        .unwrap();
    let (records, compacted) = received.recv().await.unwrap();
    let values: Vec<_> = records.iter().map(|r| r.value.clone().unwrap()).collect();
    assert_eq!(values, vec![Bytes::from("1"), Bytes::from("2")]);
    assert_eq!(
        compacted,
        vec![CompactedRecord {
            topic: "progress".into(),
            key: Bytes::from("p"),
            value: Some(Bytes::from("t1")),
        }]
    );

    // An aborted transaction reaches nothing.
    producer.begin_transaction().unwrap();
    send(&producer, "events", "c", "3");
    send(&producer, "progress", "p", "aborted");
    producer.abort_transaction(Duration::from_secs(10)).unwrap();

    // A transaction that writes only compacted records goes to the topic that holds their keys.
    producer.begin_transaction().unwrap();
    send(&producer, "progress", "p", "t2");
    producer
        .commit_transaction(Duration::from_secs(10))
        .unwrap();
    let (records, compacted) = received.recv().await.unwrap();
    assert!(records.is_empty());
    assert_eq!(compacted[0].value, Some(Bytes::from("t2")));
    assert!(
        received.try_recv().is_err(),
        "the aborted transaction arrived"
    );

    let read =
        mz_ore::task::spawn_blocking(|| "read", move || read_compacted(addr, "progress")).await;
    assert_eq!(read, vec![("p".into(), Some("t2".into()))]);

    // A new producer with the same transactional ID fences the old one.
    let new_producer = txn_producer();
    producer.begin_transaction().unwrap();
    send(&producer, "progress", "p", "zombie");
    assert!(
        producer
            .commit_transaction(Duration::from_secs(10))
            .is_err()
    );
    new_producer.begin_transaction().unwrap();
    send(&new_producer, "progress", "p", "t3");
    new_producer
        .commit_transaction(Duration::from_secs(10))
        .unwrap();
    let (_, compacted) = received.recv().await.unwrap();
    assert_eq!(compacted[0].value, Some(Bytes::from("t3")));
}

#[mz_ore::test(tokio::test(flavor = "multi_thread"))]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn test_compacted_records_survive_reregistration() {
    let (server, addr) = start_server().await;
    let restored = vec![CompactedRecord {
        topic: "progress".into(),
        key: Bytes::from("p"),
        value: Some(Bytes::from("restored")),
    }];
    let registration = server
        .register_topic(
            "events".into(),
            TopicOptions {
                credentials: None,
                validator: None,
                schemas: TopicSchemas::default(),
                queue_bytes: 1024 * 1024,
                compacted: restored.clone(),
            },
        )
        .unwrap();
    let read =
        mz_ore::task::spawn_blocking(|| "read", move || read_compacted(addr, "progress")).await;
    assert_eq!(read, vec![("p".into(), Some("restored".into()))]);

    // While the topic that holds a record is released, the record is withheld, since the
    // topic's consumer may have stored a newer one.
    drop(registration);
    let reader = mz_ore::task::spawn_blocking(|| "read", move || read_compacted(addr, "progress"));
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!reader.is_finished(), "read a withheld record");
    let _registration = server
        .register_topic(
            "events".into(),
            TopicOptions {
                credentials: None,
                validator: None,
                schemas: TopicSchemas::default(),
                queue_bytes: 1024 * 1024,
                compacted: restored,
            },
        )
        .unwrap();
    assert_eq!(reader.await, vec![("p".into(), Some("restored".into()))]);
}
