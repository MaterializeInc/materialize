// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use dataflow_types::{Diff, Timestamp};
use differential_dataflow::Hashable;
use log::error;
use repr::Row;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use lazy_static::lazy_static;
use prometheus::IntCounterVec;

use prometheus_static_metric::make_static_metric;
use timely::dataflow::operators::Operator;
use url::Url;

make_static_metric! {
    struct EventsRead: IntCounter {
        "status" => { success, error }
    }
}

lazy_static! {
    static ref EVENTS_COUNTER_INTERNAL: IntCounterVec = register_int_counter_vec!(
        "mz_protobuf_events_read_total",
        "Count of protobuf events we have read from the wire",
        &["status"]
    )
    .unwrap();
    static ref EVENTS_COUNTER: EventsRead = EventsRead::from(&EVENTS_COUNTER_INTERNAL);
}

pub fn protobuf<G>(
    stream: &Stream<G, Vec<u8>>,
    descriptor_file: &str,
    message_name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary(
        Exchange::new(|x: &Vec<u8>| x.hashed()),
        "ProtobufDecode",
        move |_, _| {
            let mut decoder = interchange::protobuf::Decoder::from_descriptor_file(descriptor_file, message_name);
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for payload in data.iter() {
                        match decoder.decode(payload) {
                            Ok(row) => {
                                EVENTS_COUNTER.success.inc();
                                if let Some(row) = row {
                                    session.give((row, *cap.time(), 1));
                                }
                            }
                            Err(err) => {
                                EVENTS_COUNTER.error.inc();
                                error!("protobuf deserialization error: {}", err)
                            }
                        }
                    }
                });
            }
        },
    )
}
