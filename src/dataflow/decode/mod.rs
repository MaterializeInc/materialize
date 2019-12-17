// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use timely::dataflow::{Scope, Stream};

use dataflow_types::{DataEncoding, Diff, Timestamp};
use repr::Row;

mod avro;
mod csv;
mod regex;
use self::csv::csv;
use self::regex::regex as regex_fn;
use avro::avro;

pub fn decode<G>(
    stream: &Stream<G, Vec<u8>>,
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
    }
}
