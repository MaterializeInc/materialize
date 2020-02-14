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

use dataflow_types::{DataEncoding, Diff, Envelope, Timestamp};
use repr::Datum;
use repr::Row;

mod avro;
mod csv;
mod protobuf;
mod regex;

use self::csv::csv;
use self::regex::regex as regex_fn;
use avro::avro;
use avro_rs::types::Value;
use interchange::avro::{extract_debezium_slow, extract_row, DiffPair};

use log::error;
use protobuf::protobuf;
use std::iter;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};

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

fn pass_through<G, Data, P>(
    stream: &Stream<G, Data>,
    name: &str,
    pact: P,
) -> Stream<G, (Data, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
    Data: timely::Data,
    P: ParallelizationContract<Timestamp, Data>,
{
    stream.unary(
        pact,
        //Exchange::new(|x: &Data| x.hashed()),
        name,
        move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut v = Vec::new();
                    data.swap(&mut v);
                    let mut session = output.session(&cap);
                    session.give_iterator(v.into_iter().map(|payload| (payload, *cap.time(), 1)));
                });
            }
        },
    )
}

pub fn decode_avro_values<G>(
    stream: &Stream<G, (Value, Option<i64>)>,
    envelope: Envelope,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    // TODO(brennan) -- If this ends up being a bottleneck,
    // refactor the avro `Reader` to separate reading from decoding,
    // so that we can spread the decoding among all the workers.
    pass_through(stream, "AvroValues", Pipeline).flat_map(move |((value, index), r, d)| {
        let diffs = match envelope {
            Envelope::None => extract_row(value, false, index.map(Datum::from)).map(|r| DiffPair {
                before: None,
                after: r,
            }),
            Envelope::Debezium => extract_debezium_slow(value),
        }
        .unwrap_or_else(|e| {
            error!("Failed to extract avro row: {}", e);
            DiffPair {
                before: None,
                after: None,
            }
        });

        diffs
            .before
            .into_iter()
            .chain(diffs.after.into_iter())
            .map(move |row| (row, r, d))
    })
}

pub fn decode<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    encoding: DataEncoding,
    name: &str,
    envelope: Envelope,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    match (encoding, envelope) {
        (DataEncoding::Csv(enc), Envelope::None) => csv(stream, enc.n_cols, enc.delimiter),
        (DataEncoding::Avro(enc), envelope) => avro(
            stream,
            &enc.value_schema,
            enc.schema_registry_url,
            envelope == Envelope::Debezium,
        ),
        (DataEncoding::AvroOcf { .. }, _) => {
            unreachable!("Internal error: Cannot decode Avro OCF separately from reading")
        }
        (_, Envelope::Debezium) => unreachable!(
            "Internal error: A non-Avro Debezium-envelope source should not have been created."
        ),
        (DataEncoding::Regex { regex }, Envelope::None) => regex_fn(stream, regex, name),
        (DataEncoding::Protobuf(enc), Envelope::None) => {
            protobuf(stream, &enc.descriptors, &enc.message_name)
        }
        (DataEncoding::Bytes, Envelope::None) => pass_through(
            stream,
            "RawBytes",
            Exchange::new(|payload: &(Vec<u8>, Option<i64>)| payload.0.hashed()),
        )
        .map(|((payload, line_no), r, d)| {
            (
                Row::pack(
                    iter::once(Datum::from(payload.as_slice())).chain(line_no.map(Datum::from)),
                ),
                r,
                d,
            )
        }),
        (DataEncoding::Text, Envelope::None) => pass_through(
            stream,
            "Text",
            Exchange::new(|payload: &(Vec<u8>, Option<i64>)| payload.0.hashed()),
        )
        .map(|((payload, line_no), r, d)| {
            (
                Row::pack(
                    iter::once(Datum::from(std::str::from_utf8(&payload).ok()))
                        .chain(line_no.map(Datum::from)),
                ),
                r,
                d,
            )
        }),
    }
}
