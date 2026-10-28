// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Delivery of `ClientContext::error` like librdkafka's main queue: consumers
//! deliver errors only when polled, producers right away, admin clients
//! never. Uses a closed local port; needs no broker.

// The client crate's own tests use its constructors directly, not
// Materialize's wrappers.
#![allow(clippy::disallowed_methods)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use mz_kafka_client::admin::AdminClient;
use mz_kafka_client::client::ClientContext;
use mz_kafka_client::config::{ClientConfig, RDKafkaLogLevel};
use mz_kafka_client::consumer::{BaseConsumer, Consumer, ConsumerContext};
use mz_kafka_client::error::KafkaError;
use mz_kafka_client::producer::{Producer, ProducerContext, ThreadedProducer};
use mz_kafka_client::types::RDKafkaErrorCode;

#[derive(Clone, Default)]
struct Ctx {
    /// Error reasons with the time they were delivered.
    errors: Arc<Mutex<Vec<(Instant, String)>>>,
    fail_logs: Arc<Mutex<Vec<String>>>,
}

impl ClientContext for Ctx {
    fn log(&self, _level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        if fac == "FAIL" {
            self.fail_logs.lock().unwrap().push(log_message.to_string());
        }
    }

    fn error(&self, error: KafkaError, reason: &str) {
        self.errors
            .lock()
            .unwrap()
            .push((Instant::now(), format!("{error}: {reason}")));
    }
}

impl ConsumerContext for Ctx {}

impl ProducerContext for Ctx {
    type DeliveryOpaque = ();
    fn delivery(&self, _: &mz_kafka_client::producer::DeliveryResult<'_>, _: ()) {}
}

impl Ctx {
    fn errors(&self) -> Vec<String> {
        self.errors
            .lock()
            .unwrap()
            .iter()
            .map(|(_, e)| e.clone())
            .collect()
    }
}

fn closed_port_config() -> ClientConfig {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", format!("127.0.0.1:{port}"))
        .set_log_level(RDKafkaLogLevel::Info);
    config
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn unpolled_consumer_receives_no_errors() {
    // Like Materialize's connection validation: create a consumer, fetch
    // metadata, never poll.
    let ctx = Ctx::default();
    let consumer: BaseConsumer<Ctx> = closed_port_config()
        .create_with_context(ctx.clone())
        .unwrap();
    let err = consumer
        .fetch_metadata(None, Duration::from_millis(300))
        .unwrap_err();
    assert_eq!(
        err,
        KafkaError::MetadataFetch(RDKafkaErrorCode::BrokerTransportFailure)
    );
    assert!(!ctx.fail_logs.lock().unwrap().is_empty());
    assert!(ctx.errors().is_empty(), "{:?}", ctx.errors());

    // A poll delivers them, on the polling thread, before returning.
    assert_eq!(
        consumer.poll(Duration::ZERO).map(|r| r.map(|_| ())),
        Some(Err(KafkaError::MessageConsumption(
            RDKafkaErrorCode::UnknownGroup
        )))
    );
    let errors = ctx.errors();
    assert!(
        errors[0].starts_with(
            "Global error: BrokerTransportFailure (Local: Broker transport failure): 127.0.0.1:"
        ),
        "{errors:#?}"
    );
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn blocked_poll_delivers_errors() {
    let ctx = Ctx::default();
    let mut config = closed_port_config();
    config.set("group.id", "g");
    let consumer: Arc<BaseConsumer<Ctx>> =
        Arc::new(config.create_with_context(ctx.clone()).unwrap());
    let c = Arc::clone(&consumer);
    let fetch = std::thread::spawn(move || c.fetch_metadata(None, Duration::from_millis(300)));
    let start = Instant::now();
    // The poll waits for messages; errors are delivered while it waits.
    assert!(consumer.poll(Duration::from_secs(2)).is_none());
    let first = ctx.errors.lock().unwrap().first().map(|(at, _)| *at);
    assert!(
        first.is_some_and(|at| at.duration_since(start) < Duration::from_secs(1)),
        "{:?}",
        ctx.errors()
    );
    assert!(fetch.join().unwrap().is_err());
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn producer_delivers_errors_without_polling() {
    let ctx = Ctx::default();
    let producer: ThreadedProducer<Ctx> = closed_port_config()
        .create_with_context(ctx.clone())
        .unwrap();
    assert!(
        producer
            .client()
            .fetch_metadata(None, Duration::from_millis(300))
            .is_err()
    );
    assert!(!ctx.errors().is_empty());
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // network
fn admin_client_never_delivers_errors() {
    let ctx = Ctx::default();
    let admin: AdminClient<Ctx> = closed_port_config()
        .create_with_context(ctx.clone())
        .unwrap();
    assert!(
        admin
            .inner()
            .fetch_metadata(None, Duration::from_millis(300))
            .is_err()
    );
    std::thread::sleep(Duration::from_millis(100));
    assert!(!ctx.fail_logs.lock().unwrap().is_empty());
    assert!(ctx.errors().is_empty(), "{:?}", ctx.errors());
}
