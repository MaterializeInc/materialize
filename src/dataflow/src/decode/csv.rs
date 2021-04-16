// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;

use dataflow_types::LinearOperator;

use differential_dataflow::{AsCollection, Collection};
use timely::dataflow::operators::{OkErr, Operator};
use timely::dataflow::{Scope, Stream};

use dataflow_types::{DataflowError, DecodeError};
use repr::{Datum, Diff, Row, Timestamp};

use crate::{metrics::EVENTS_COUNTER, source::SourceOutput};

pub fn csv<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    header_row: bool,
    n_cols: usize,
    delimiter: u8,
    operators: &mut Option<LinearOperator>,
) -> (
    Collection<G, Row, Diff>,
    Option<Collection<G, dataflow_types::DataflowError, Diff>>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    // Delimiters must be single-byte utf8 to safely treat all matched fields as valid utf8.
    assert!(delimiter.is_ascii());

    let operators = operators.take();
    let demanded = (0..n_cols)
        .map(move |c| {
            operators
                .as_ref()
                .map(|o| o.projection.contains(&c))
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();

    let stream = stream.unary(
        SourceOutput::<Vec<u8>, Vec<u8>>::position_value_contract(),
        "CsvDecode",
        |_, _| {
            // Temporary storage, and a re-useable CSV reader.
            let mut buffer = vec![0u8];
            let mut bounds = vec![0usize];
            let mut csv_reader = csv_core::ReaderBuilder::new().delimiter(delimiter).build();
            let mut row_packer = Row::default();
            move |input, output| {
                let mut events_success = 0;
                let mut events_error = 0;
                input.for_each(|cap, lines| {
                    let mut session = output.session(&cap);
                    // TODO: There is extra work going on here:
                    // LinesCodec is already splitting our input into lines,
                    // but the CsvReader *itself* searches for line breaks.
                    // This is mainly an aesthetic/performance-golfing
                    // issue as I doubt it will ever be a bottleneck.
                    for SourceOutput {
                        key: _,
                        value: line,
                        position: line_no,
                        upstream_time_millis: _,
                    } in &*lines
                    {
                        // We only want to process utf8 strings, as this ensures that all fields
                        // will be utf8 as well, allowing some unsafe shenanigans.
                        if std::str::from_utf8(line.as_slice()).is_err() {
                            // Generate an error object for the error stream
                            events_error += 1;
                            session.give((
                                Err(DataflowError::DecodeError(DecodeError::Text(
                                    match line_no {
                                        Some(line_no) => format!("CSV error at lineno {}: invalid UTF-8", line_no),
                                        None => format!("CSV error at lineno 'unknown': invalid UTF-8"),
                                    }
                                ))),
                                *cap.time(),
                                1,
                            ));
                        } else {
                            // Reset the reader to read a new series of records.
                            csv_reader.reset();
                            if let Some(line_no) = line_no {
                                csv_reader.set_line(*line_no as u64);
                                if header_row && *line_no == 1 {
                                    continue;
                                }
                            }

                            let mut input = line.as_slice();
                            let mut buffer_valid = 0;
                            let mut bounds_valid = 0;
                            let mut done = false;

                            while !done {
                                // Note that we protect the first element of `bounds`, a zero, so that ranges are easier to extract below.
                                let (result, in_read, out_wrote, ends_wrote) = csv_reader
                                    .read_record(
                                        input,
                                        &mut buffer[buffer_valid..],
                                        &mut bounds[1 + bounds_valid..],
                                    );

                                // Advance buffers, as requested by return values.
                                input = &input[in_read..];
                                buffer_valid += out_wrote;
                                bounds_valid += ends_wrote;

                                match result {
                                    csv_core::ReadRecordResult::InputEmpty => {
                                        // We will go around the loop again with an empty input buffer and flush any final record.
                                    }
                                    csv_core::ReadRecordResult::OutputFull => {
                                        let length = buffer.len();
                                        buffer.extend(std::iter::repeat(0).take(length));
                                    }
                                    csv_core::ReadRecordResult::OutputEndsFull => {
                                        let length = bounds.len();
                                        bounds.extend(std::iter::repeat(0).take(length));
                                    }
                                    csv_core::ReadRecordResult::Record => {
                                        if bounds_valid != n_cols {
                                            events_error += 1;
                                            session.give((
                                                Err(DataflowError::DecodeError(DecodeError::Text(
                                                    match line_no {
                                                        Some(line_no) => format!( "CSV error at lineno {}: expected {} columns, got {}.", line_no, n_cols, bounds_valid),
                                                        None => format!("CSV error at lineno 'unknown': expected {} columns, got {}.", n_cols, bounds_valid),
                                                    }
                                                ))),
                                                *cap.time(),
                                                1,
                                            ));
                                        } else {
                                            events_success += 1;
                                            row_packer.extend(
                                                (0..n_cols)
                                                    .map(|i| {
                                                        // Unsafety rationalized as 1. the input text is determined to be
                                                        // valid utf8, and 2. the delimiter is ascii, which should make each
                                                        // delimited region also utf8.
                                                        Datum::String(unsafe {
                                                            if demanded[i] {
                                                                std::str::from_utf8_unchecked(
                                                                    &buffer[bounds[i]
                                                                        ..bounds[i + 1]],
                                                                )
                                                            } else {
                                                                ""
                                                            }
                                                        })
                                                    })
                                                    .chain(iter::once(
                                                        line_no.map(Datum::Int64).into(),
                                                    )),
                                            );
                                            session.give((
                                                Ok(row_packer.finish_and_reuse()),
                                                *cap.time(),
                                                1,
                                            ));
                                            // Reset valid data to extract the next record, should one exist.
                                            buffer_valid = 0;
                                            bounds_valid = 0;
                                        }
                                    }
                                    csv_core::ReadRecordResult::End => {
                                        done = true;
                                    }
                                }
                            }
                        }
                    }
                });
                if events_success > 0 {
                    EVENTS_COUNTER.csv.success.inc_by(events_success);
                }
                if events_error > 0 {
                    EVENTS_COUNTER.csv.error.inc_by(events_error);
                }
            }
        },
    );

    let (oks, errs) = stream.ok_err(|(data, time, diff)| match data {
        Ok(data) => Ok((data, time, diff)),
        Err(err) => Err((err, time, diff)),
    });

    (oks.as_collection(), Some(errs.as_collection()))
}
