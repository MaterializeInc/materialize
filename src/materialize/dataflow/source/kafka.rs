// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::Data;
use rdkafka::consumer::{BaseConsumer, ConsumerContext};
use rdkafka::Message;
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::generic::OutputHandle;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};

pub fn kafka<C, G, D, L>(scope: &G, name: &str, consumer: BaseConsumer<C>, logic: L) -> Stream<G, D>
where
    C: ConsumerContext + 'static,
    G: Scope<Timestamp = std::time::Duration>,
    D: Data,
    L: Fn(
            &[u8],
            &mut Capability<G::Timestamp>,
            &mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>,
        ) -> bool
        + 'static,
{
    source(scope, name, move |capability, info| {
        let activator = scope.activator_for(&info.address[..]);
        let mut cap = Some(capability);

        let then = std::time::Instant::now();

        // define a closure to call repeatedly.
        move |output| {
            // Act only if we retain the capability to send data.
            let mut complete = false;
            if let Some(capability) = cap.as_mut() {
                // Indicate that we should run again.
                activator.activate();

                // Repeatedly interrogate Kafka for [u8] messages.
                // Cease only when Kafka stops returning new data.
                // Could cease earlier, if we had a better policy.
                while let Some(result) = consumer.poll(std::time::Duration::from_millis(0)) {
                    // If valid data back from Kafka
                    match result {
                        Ok(message) => {
                            // Attempt to interpret bytes as utf8  ...
                            if let Some(payload) = message.payload() {
                                complete = logic(payload, capability, output) || complete;
                            }
                        }
                        Err(err) => println!("Kafka error: {}", err),
                    }
                }
                // We need some rule to advance timestamps ...
                capability.downgrade(&then.elapsed());
            }

            if complete {
                cap = None;
            }
        }
    })
}
