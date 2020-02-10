// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::Hashable;
use log::error;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{Diff, Timestamp};
use interchange::protobuf::{self, Decoder};
use repr::Row;

use super::EVENTS_COUNTER;

pub fn protobuf<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    descriptors: &[u8],
    message_name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    let message_name = message_name.to_owned();
    let descriptors = protobuf::decode_descriptors(descriptors)
        .expect("descriptors provided to protobuf source are pre-validated");
    let mut decoder = Decoder::new(descriptors, &message_name);

    stream.unary(
        Exchange::new(|x: &(Vec<u8>, _)| x.0.hashed()),
        "ProtobufDecode",
        move |_, _| {
            move |input, output| {
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
