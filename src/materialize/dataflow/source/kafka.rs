// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::Data;
use log::error;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, DefaultConsumerContext};
use rdkafka::Message;
use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};

pub fn kafka<G, D, L>(
    scope: &G,
    name: &str,
    topic: &str,
    addr: &str,
    done: Rc<Cell<bool>>,
    logic: L,
) -> Stream<G, D>
where
    G: Scope<Timestamp = u64>,
    D: Data,
    L: Fn(
            &[u8],
            &Capability<G::Timestamp>,
            &mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>,
        ) + 'static,
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
        let clock = Instant::now();
        let mut maybe_cap = Some(cap);

        let mut config = ClientConfig::new();
        config
            .set("produce.offset.report", "true")
            .set("auto.offset.reset", "smallest")
            .set("group.id", &format!("materialize-{}", name))
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("bootstrap.servers", &addr.to_string());

        let consumer: BaseConsumer<DefaultConsumerContext> = config.create().unwrap();
        consumer.subscribe(&[&topic]).unwrap();

        move |output| {
            if done.get() {
                maybe_cap = None;
                return;
            }
            let cap = maybe_cap.as_mut().unwrap();

            // Indicate that we should run again.
            activator.activate();

            let ts = clock.elapsed().as_millis() as u64;

            // Repeatedly interrogate Kafka for messages. Cease only when
            // Kafka stops returning new data. We could cease earlier, if we
            // had a better policy.
            while let Some(result) = consumer.poll(Duration::from_millis(0)) {
                match result {
                    Ok(message) => {
                        // TODO(benesch): this will explode if the timestamps
                        // are sufficiently out of order.
                        let msg_cap = cap.delayed(&ts);
                        if let Some(payload) = message.payload() {
                            logic(payload, &msg_cap, output);
                        }
                        // TODO(benesch): what does it mean to have a Kafka
                        // message with no payload?
                    }
                    Err(err) => error!("kafka error: {}: {}", name, err),
                }
            }

            cap.downgrade(&ts);
        }
    })
}
