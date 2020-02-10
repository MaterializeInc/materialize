// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use lazy_static::lazy_static;

use differential_dataflow::hashable::Hashable;
use prometheus::{register_int_counter_vec, IntCounterVec};
use prometheus_static_metric::make_static_metric;
use timely::dataflow::{
    channels::pact::Exchange,
    operators::{map::Map, Operator},
    Scope, Stream,
};

use dataflow_types::{DataEncoding, Diff, Timestamp};
use repr::Datum;
use repr::Row;

mod avro;
mod csv;
mod protobuf;
mod regex;

use self::csv::csv;
use self::regex::regex as regex_fn;
use avro::avro;
use protobuf::protobuf;
use std::iter;

make_static_metric! {
    struct EventsRead: IntCounter {
        "format" => { avro, csv, protobuf },
        "status" => { success, error }
    }
}

lazy_static! {
    static ref EVENTS_COUNTER_INTERNAL: IntCounterVec = register_int_counter_vec!(
        "mz_dataflow_events_read_total",
        "Count of events we have read from the wire",
        &["format", "status"]
    )
    .unwrap();
    static ref EVENTS_COUNTER: EventsRead = EventsRead::from(&EVENTS_COUNTER_INTERNAL);
}

fn raw<G>(stream: &Stream<G, (Vec<u8>, Option<i64>)>) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary(
        Exchange::new(|x: &(Vec<u8>, _)| x.0.hashed()),
        "RawBytes",
        move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for (payload, line_no) in data.iter() {
                        session.give((
                            Row::pack(
                                iter::once(Datum::from(payload.as_slice()))
                                    .chain(line_no.map(Datum::from)),
                            ),
                            *cap.time(),
                            1,
                        ));
                    }
                });
            }
        },
    )
}

pub fn decode<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    encoding: DataEncoding,
    name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    match encoding {
        DataEncoding::Csv(enc) => csv(stream, enc.n_cols, enc.delimiter),
        DataEncoding::Avro(enc) => avro(stream, &enc.raw_schema, enc.schema_registry_url),
        DataEncoding::Regex { regex } => regex_fn(stream, regex, name),
        DataEncoding::Protobuf(enc) => protobuf(stream, &enc.descriptors, &enc.message_name),
        DataEncoding::Bytes => raw(stream),
        DataEncoding::Text => raw(stream).map(|(row, r, d)| {
            let datums = row.unpack();
            (
                Row::pack(
                    iter::once(Datum::from(
                        std::str::from_utf8(datums[0].unwrap_bytes()).ok(),
                    ))
                    .chain(iter::once(datums[1])),
                ),
                r,
                d,
            )
        }),
    }
}
