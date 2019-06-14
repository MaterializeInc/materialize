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
use crate::dataflow::types::{Diff, KafkaSourceConnector, Timestamp};
use crate::interchange::avro;
use crate::repr::Datum;

pub fn kafka<G>(
    scope: &G,
    name: &str,
    connector: &KafkaSourceConnector,
) -> (Stream<G, (Vec<Datum>, Timestamp, Diff)>, SharedCapability)
where
    G: Scope<Timestamp = Timestamp>,
{
    source(scope, name, move |info| {
        let name = name.to_owned();
        let activator = scope.activator_for(&info.address[..]);

        let mut decoder =
            avro::Decoder::new(&connector.raw_schema, connector.schema_registry_url.clone());

        let mut config = ClientConfig::new();
        config
            .set("produce.offset.report", "true")
            .set("auto.offset.reset", "smallest")
            .set("group.id", &format!("materialize-{}", name))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("bootstrap.servers", &connector.addr.to_string());

        let consumer: BaseConsumer<DefaultConsumerContext> = config.create().unwrap();
        consumer.subscribe(&[&connector.topic]).unwrap();

        move |cap, output| {
            // Indicate that we should run again.
            activator.activate();

            // Repeatedly interrogate Kafka for messages. Cease only when
            // Kafka stops returning new data. We could cease earlier, if we
            // had a better policy.
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
                            KafkaTimestamp::CreateTime(ms) | KafkaTimestamp::LogAppendTime(ms) => {
                                ms as u64
                            }
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

                        match decoder.decode(payload) {
                            Ok(diff_pair) => {
                                if let Some(before) = diff_pair.before {
                                    output.session(&cap).give((before, *cap.time(), -1));
                                }
                                if let Some(after) = diff_pair.after {
                                    output.session(&cap).give((after, *cap.time(), 1));
                                }
                            }
                            Err(err) => error!("avro deserialization error: {}", err),
                        }
                    }
                    Err(err) => error!("kafka error: {}: {}", name, err),
                }
            }
        }
    })
}
