// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use log::error;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::{ClientConfig, ClientContext};
use rdkafka::{Message, Timestamp as KafkaTimestamp};
use std::sync::Mutex;
use std::time::Duration;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::activate::SyncActivator;

use super::util::source;
use super::SharedCapability;
use dataflow_types::{Diff, KafkaSourceConnector, Timestamp};
use interchange::avro;
use repr::Row;

use lazy_static::lazy_static;
use prometheus::{IntCounter, IntCounterVec};

use prometheus_static_metric::make_static_metric;

make_static_metric! {
    struct EventsRead: IntCounter {
        "status" => { success, error }
    }
}

lazy_static! {
    static ref BYTES_READ_COUNTER: IntCounter = register_int_counter!(
        "mz_kafka_bytes_read_total",
        "Count of kafka bytes we have read from the wire"
    )
    .unwrap();
    static ref EVENTS_COUNTER_INTERNAL: IntCounterVec = register_int_counter_vec!(
        "mz_kafka_events_read_total",
        "Count of kafka events we have read from the wire",
        &["status"]
    )
    .unwrap();
    static ref EVENTS_COUNTER: EventsRead = EventsRead::from(&EVENTS_COUNTER_INTERNAL);
}

pub fn kafka<G>(
    scope: &G,
    name: &str,
    connector: KafkaSourceConnector,
    read_kafka: bool,
) -> (Stream<G, (Row, Timestamp, Diff)>, Option<SharedCapability>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let KafkaSourceConnector {
        addr,
        topic,
        raw_schema,
        schema_registry_url,
    } = connector;
    let (stream, capability) = source(scope, name, move |info| {
        let name = name.to_owned();
        let activator = scope.activator_for(&info.address[..]);

        let mut config = ClientConfig::new();
        config
            .set("auto.offset.reset", "smallest")
            .set("group.id", &format!("materialize-{}", name))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &addr.to_string());

        let mut consumer: Option<BaseConsumer<GlueConsumerContext>> = if read_kafka {
            let cx = GlueConsumerContext(Mutex::new(scope.sync_activator_for(&info.address[..])));
            Some(
                config
                    .create_with_context(cx)
                    .expect("Failed to create Kafka Consumer"),
            )
        } else {
            None
        };

        if let Some(consumer) = consumer.as_mut() {
            consumer.subscribe(&[&topic]).unwrap();
        }

        move |cap, output| {
            if let Some(consumer) = consumer.as_mut() {
                // Repeatedly interrogate Kafka for messages. Cease when
                // Kafka stops returning new data, or after 10 milliseconds.
                let timer = std::time::Instant::now();

                while let Some(result) = consumer.poll(Duration::from_millis(0)) {
                    match result {
                        Ok(message) => {
                            let payload = match message.payload() {
                                Some(p) => p,
                                // Null payloads are expected from Debezium.
                                // See https://github.com/MaterializeInc/materialize/issues/439#issuecomment-534236276
                                None => continue,
                            };

                            let ms = match message.timestamp() {
                                KafkaTimestamp::NotAvailable => {
                                    // TODO(benesch): do we need to do something
                                    // else?
                                    error!("dropped kafka message with no timestamp");
                                    continue;
                                }
                                KafkaTimestamp::CreateTime(ms)
                                | KafkaTimestamp::LogAppendTime(ms) => ms as u64,
                            };
                            if ms >= *cap.time() {
                                cap.downgrade(&ms)
                            } else {
                                let cur = *cap.time();
                                error!(
                                    "{}: fast-forwarding out-of-order Kafka timestamp {}ms ({} -> {})",
                                    name,
                                    cur - ms,
                                    ms,
                                    cur,
                                );
                            };

                            let out = payload.to_vec();
                            BYTES_READ_COUNTER.inc_by(out.len() as i64);
                            output.session(&cap).give(out);
                        }
                        Err(err) => error!("kafka error: {}: {}", name, err),
                    }

                    if timer.elapsed().as_millis() > 10 {
                        // We didn't drain the entire queue, so indicate that we
                        // should run again. We suppress the activation when the
                        // queue is drained, as in that case librdkafka is
                        // configured to unpark our thread when a new message
                        // arrives.
                        activator.activate();
                        return;
                    }
                }
            }
            // Ensure that we poll kafka more often than the eviction timeout
            activator.activate_after(Duration::from_secs(60));
        }
    });

    use differential_dataflow::hashable::Hashable;
    use timely::dataflow::channels::pact::Exchange;
    use timely::dataflow::operators::generic::operator::Operator;

    let stream = stream.unary(
        Exchange::new(|x: &Vec<u8>| x.hashed()),
        "AvroDecode",
        move |_, _| {
            let mut decoder = avro::Decoder::new(&raw_schema, schema_registry_url);
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for payload in data.iter() {
                        match decoder.decode(payload) {
                            Ok(diff_pair) => {
                                EVENTS_COUNTER.success.inc();
                                if let Some(before) = diff_pair.before {
                                    session.give((before, *cap.time(), -1));
                                }
                                if let Some(after) = diff_pair.after {
                                    session.give((after, *cap.time(), 1));
                                }
                            }
                            Err(err) => {
                                EVENTS_COUNTER.error.inc();
                                error!("avro deserialization error: {}", err)
                            }
                        }
                    }
                });
            }
        },
    );

    if read_kafka {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}

/// An implementation of [`ConsumerContext`] that unparks the wrapped thread
/// when the message queue switches from nonempty to empty.
struct GlueConsumerContext(Mutex<SyncActivator>);

impl ClientContext for GlueConsumerContext {}

impl ConsumerContext for GlueConsumerContext {
    fn message_queue_nonempty_callback(&self) {
        let activator = self.0.lock().unwrap();
        activator
            .activate()
            .expect("timely operator hung up while Kafka source active");
    }
}
