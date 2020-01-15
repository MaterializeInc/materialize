// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use differential_dataflow::Hashable;
use log::error;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, Timestamp};
use interchange::protobuf::Decoder;
use repr::Row;

use super::EVENTS_COUNTER;

pub fn protobuf<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    descriptor_file: &str,
    message_name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let descriptor_file = descriptor_file.to_owned();
    let message_name = message_name.to_owned();

    stream.unary(
        Exchange::new(|x: &(Vec<u8>, _)| x.0.hashed()),
        "ProtobufDecode",
        move |_, _| {
            move |input, output| {
                let mut decoder =
                    match Decoder::from_descriptor_file(&descriptor_file, &message_name) {
                        Ok(decoder) => decoder,
                        Err(e) => {
                            error!(
                                "Unable to construct protobuf decoder \
                                 descriptor_file={} message_name={} error: {}",
                                descriptor_file, message_name, e
                            );
                            return;
                        }
                    };
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for (payload, _) in data.iter() {
                        match decoder.decode(payload) {
                            Ok(row) => {
                                EVENTS_COUNTER.protobuf.success.inc();
                                if let Some(row) = row {
                                    session.give((row, *cap.time(), 1));
                                } else {
                                    EVENTS_COUNTER.protobuf.error.inc();
                                    error!("protobuf deserialization returned None");
                                }
                            }
                            Err(err) => {
                                EVENTS_COUNTER.protobuf.error.inc();
                                error!("protobuf deserialization error: {}", err)
                            }
                        }
                    }
                })
            }
        },
    )
}
