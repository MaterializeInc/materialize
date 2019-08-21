// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use log::error;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::{Message, Timestamp as KafkaTimestamp};
use std::time::Duration;
use timely::dataflow::{Scope, Stream};

use super::util::source;
use super::SharedCapability;
use crate::types::{Diff, KafkaSourceConnector, Timestamp};
use interchange::avro;
use repr::Datum;

pub fn kafka<G>(
    scope: &G,
    name: &str,
    connector: KafkaSourceConnector,
    read_kafka: bool,
) -> (
    Stream<G, (Vec<Datum>, Timestamp, Diff)>,
    Option<SharedCapability>,
)
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
            .set("produce.offset.report", "true")
            .set("auto.offset.reset", "smallest")
            .set("group.id", &format!("materialize-{}", name))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("fetch.message.max.bytes", "134217728")
            .set("enable.sparse.connections", "true")
            .set("bootstrap.servers", &addr.to_string());

        let mut consumer: Option<BaseConsumer<DefaultConsumerContext>> = if read_kafka {
            Some(config.create().expect("Failed to create Kafka Consumer"))
        } else {
            None
        };

        if let Some(consumer) = consumer.as_mut() {
            consumer.subscribe(&[&topic]).unwrap();
        }

        move |cap, output| {
            if let Some(consumer) = consumer.as_mut() {
                // Indicate that we should run again.
                activator.activate();

                // Repeatedly interrogate Kafka for messages. Cease only when
                // Kafka stops returning new data. We could cease earlier, if we
                // had a better policy.
                let timer = std::time::Instant::now();

                while let Some(result) = consumer.poll(Duration::from_millis(0)) {
                    match result {
                        Ok(message) => {
                            let payload = match message.payload() {
                                Some(p) => p,
                                None => {
                                    // TODO(benesch): what does it mean to have a
                                    // Kafka message with no payload?
                                    error!("dropped kafka message with no payload");
                                    continue;
                                }
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
                                error!(
                                    "{}: fast-forwarding out-of-order Kafka timestamp from {} to {}",
                                    name,
                                    ms,
                                    *cap.time()
                                );
                            };

                            output.session(&cap).give(payload.to_vec());
                        }
                        Err(err) => error!("kafka error: {}: {}", name, err),
                    }

                    if timer.elapsed().as_millis() > 10 {
                        return;
                    }
                }
            }
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
                                if let Some(before) = diff_pair.before {
                                    session.give((before, *cap.time(), -1));
                                }
                                if let Some(after) = diff_pair.after {
                                    session.give((after, *cap.time(), 1));
                                }
                            }
                            Err(err) => error!("avro deserialization error: {}", err),
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
