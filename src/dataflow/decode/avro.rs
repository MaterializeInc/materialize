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
use url::Url;

use super::EVENTS_COUNTER;
use dataflow_types::{Diff, Timestamp};
use repr::Row;

pub fn avro<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    raw_schema: &str,
    schema_registry: Option<Url>,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary(
        Exchange::new(|x: &(Vec<u8>, _)| x.0.hashed()),
        "AvroDecode",
        move |_, _| {
            let mut decoder = interchange::avro::Decoder::new(raw_schema, schema_registry);
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for (payload, _) in data.iter() {
                        match decoder.decode(payload) {
                            Ok(diff_pair) => {
                                EVENTS_COUNTER.avro.success.inc();
                                if let Some(before) = diff_pair.before {
                                    session.give((before, *cap.time(), 1));
                                }
                                if let Some(after) = diff_pair.after {
                                    session.give((after, *cap.time(), 1));
                                }
                            }
                            Err(err) => {
                                EVENTS_COUNTER.avro.error.inc();
                                error!("avro deserialization error: {}", err)
                            }
                        }
                    }
                });
            }
        },
    )
}
