// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::Hashable;
use lazy_static::lazy_static;
use log::error;
use prometheus::IntCounterVec;
use prometheus_static_metric::make_static_metric;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use url::Url;

use dataflow_types::{Diff, Timestamp};
use repr::Row;

make_static_metric! {
    struct EventsRead: IntCounter {
        "status" => { success, error }
    }
}

lazy_static! {
    static ref EVENTS_COUNTER_INTERNAL: IntCounterVec = prometheus::register_int_counter_vec!(
        "mz_kafka_events_read_total",
        "Count of kafka events we have read from the wire",
        &["status"]
    )
    .unwrap();
    static ref EVENTS_COUNTER: EventsRead = EventsRead::from(&EVENTS_COUNTER_INTERNAL);
}

pub fn avro<G>(
    stream: &Stream<G, Vec<u8>>,
    raw_schema: &str,
    schema_registry: Option<Url>,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary(
        Exchange::new(|x: &Vec<u8>| x.hashed()),
        "AvroDecode",
        move |_, _| {
            let mut decoder = interchange::avro::Decoder::new(raw_schema, schema_registry);
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
    )
}
