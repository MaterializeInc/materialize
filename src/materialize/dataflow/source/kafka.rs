// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use log::error;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::Message;
use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;
use timely::dataflow::operators::generic::source;
use timely::dataflow::{Scope, Stream};

use crate::dataflow::types::{Diff, KafkaSourceConnector, Timestamp};
use crate::interchange::avro;
use crate::repr::Datum;

pub fn kafka<G>(
    scope: &G,
    name: &str,
    connector: &KafkaSourceConnector,
    done: Rc<Cell<bool>>,
    timer: std::time::Instant,
) -> Stream<G, (Vec<Datum>, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    if scope.index() != 0 {
        // Only the first worker reads from Kafka, to ensure it has a complete
        // view of the topic. The other workers get dummy sources that never
        // produce any data.
        return source(scope, name, move |_cap, _info| move |_output| ());
    }

    source(scope, name, move |cap, info| {
        let name = name.to_owned();
        let activator = scope.activator_for(&info.address[..]);
        let mut maybe_cap = Some(cap);
        // let clock = clock.clone();

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

        move |output| {
            if done.get() {
                maybe_cap = None;
                return;
            }
            let cap = maybe_cap.as_mut().unwrap();

            // Indicate that we should run again.
            activator.activate();

            let ts = timer.elapsed().as_millis() as u64;

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
                        let cap = cap.delayed(&ts);
                        match decoder.decode(payload) {
                            Ok(data) => output.session(&cap).give((data, *cap.time(), 1)),
                            Err(err) => error!("avro deserialization error: {}", err),
                        }
                    }
                    Err(err) => error!("kafka error: {}: {}", name, err),
                }
            }

            cap.downgrade(&ts);
        }
    })
}
