// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
    delimiter: u8,
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
                            .delimiter(delimiter)
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
