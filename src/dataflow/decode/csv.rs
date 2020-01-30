// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::iter;

use differential_dataflow::Hashable;
use log::error;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use super::EVENTS_COUNTER;
use dataflow_types::{Diff, Timestamp};
use repr::{Datum, Row};

pub fn csv<G>(
    stream: &Stream<G, (Vec<u8>, Option<i64>)>,
    n_cols: usize,
) -> Stream<G, (Row, Timestamp, Diff)>
where
    G: Scope<Timestamp = Timestamp>,
{
    stream.unary(
        Exchange::new(|x: &(Vec<u8>, _)| x.0.hashed()),
        "CsvDecode",
        |_, _| {
            move |input, output| {
                input.for_each(|cap, lines| {
                    let mut session = output.session(&cap);
                    // TODO: There is extra work going on here:
                    // LinesCodec is already splitting our input into lines,
                    // but the CsvReader *itself* searches for line breaks.
                    // This is mainly an aesthetic/performance-golfing
                    // issue as I doubt it will ever be a bottleneck.
                    for (line, line_no) in &*lines {
                        let mut csv_reader = csv::ReaderBuilder::new()
                            .has_headers(false)
                            .from_reader(line.as_slice());
                        for result in csv_reader.records() {
                            let record = result.unwrap();
                            if record.len() != n_cols {
                                EVENTS_COUNTER.csv.error.inc();
                                error!(
                                    "CSV error: expected {} columns, got {}. Ignoring row.",
                                    n_cols,
                                    record.len()
                                );
                                continue;
                            }
                            EVENTS_COUNTER.csv.success.inc();
                            session.give((
                                Row::pack(
                                    record
                                        .iter()
                                        .map(|s| Datum::String(s))
                                        .chain(iter::once(line_no.map(Datum::Int64).into())),
                                ),
                                *cap.time(),
                                1,
                            ));
                        }
                    }
                });
            }
        },
    )
}
