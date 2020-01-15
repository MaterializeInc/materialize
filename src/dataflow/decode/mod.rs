// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use lazy_static::lazy_static;

use prometheus::{register_int_counter_vec, IntCounterVec};
use prometheus_static_metric::make_static_metric;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{DataEncoding, Diff, Timestamp};
use repr::Row;

mod avro;
mod csv;
mod protobuf;
mod regex;

use self::csv::csv;
use self::regex::regex as regex_fn;
use avro::avro;
use protobuf::protobuf;

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

pub fn decode<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    encoding: DataEncoding,
    name: &str,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    match encoding {
        DataEncoding::Csv(enc) => csv(stream, enc.n_cols),
        DataEncoding::Avro(enc) => avro(stream, &enc.raw_schema, enc.schema_registry_url),
        DataEncoding::Regex { regex } => regex_fn(stream, regex, name),
        DataEncoding::Protobuf(enc) => protobuf(stream, &enc.descriptor_file, &enc.message_name),
    }
}
