// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;

use expr::MapFilterProject;

use log::error;

use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::DataflowError;
use repr::{Datum, Diff, Row, RowArena, Timestamp};

use crate::{metrics::EVENTS_COUNTER, source::SourceOutput};

pub fn csv<G>(
    stream: &Stream<G, SourceOutput<Vec<u8>, Vec<u8>>>,
    header_row: bool,
    n_cols: usize,
    delimiter: u8,
    map_filter_project: &mut MapFilterProject,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
)
where
    G: Scope<Timestamp = Timestamp>,
{
    // Delimiters must be single-byte utf8 to safely treat all matched fields as valid utf8.
    assert!(delimiter.is_ascii());
    // Take ownership of MFP and leave behind the identity operators.
    let map_filter_project = std::mem::replace(
        map_filter_project,
        MapFilterProject::new(map_filter_project.input_arity),
    );

    // TODO: Directly write operator to produce two output streams rather than `ok_err` to simplify dataflow.
    use timely::dataflow::operators::OkErr;

    stream.unary(
        SourceOutput::<Vec<u8>, Vec<u8>>::position_value_contract(),
        "CsvDecode",
        |_, _| {
            // Temporary storage, and a re-useable CSV reader.
            let mut buffer = vec![0u8];
            let mut bounds = vec![0usize];
            let mut csv_reader = csv_core::ReaderBuilder::new().delimiter(delimiter).build();
            let mut row_packer = repr::RowPacker::new();
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
                    for SourceOutput { key: _, value: line, position: line_no , upstream_time_millis: _ } in &*lines {
                        // We only want to process utf8 strings, as this ensures that all fields
                        // will be utf8 as well, allowing some unsafe shenanigans.
                        if std::str::from_utf8(line.as_slice()).is_err() {
                            events_error += 1;
                            // TODO(mcsherry): Produce dataflow error rather than console error.
                            error!("CSV error: input text is not utf8");
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
                                    .read_record(input, &mut buffer[buffer_valid..], &mut bounds[1+bounds_valid..]);

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
                                            error!(
                                                "CSV error: expected {} columns, got {}. Ignoring row.",
                                                n_cols, bounds_valid,
                                            );
                                        } else {
                                            events_success += 1;
                                            // Push strings into a vector for MFP evaluation.
                                            // TODO(mcsherry): re-use allocations for datums.
                                            let mut datums = (0 .. n_cols)
                                                .map(|i| unsafe {
                                                    // NOTE(mcsherry): unsafety justified because the source string is utf8 and the
                                                    // delimiter is a single-byte utf8, which we concluded ensured that all fields
                                                    // would also be valid utf8, through discussion and head scratching.
                                                    Datum::String(std::str::from_utf8_unchecked(&buffer[bounds[i]..bounds[i + 1]]))
                                                })
                                                .chain(iter::once(line_no.map(Datum::Int64).into()))
                                                .collect::<Vec<_>>();
                                            // Evaluation may error, in which case we produce the error rather than a partial row.
                                            // We flag this as a "success" as the failure happens not in the source, but in subsequent logic.
                                            let temp_storage = RowArena::new();

                                            if let Some(result) = map_filter_project.evaluate_iter(&mut datums, &temp_storage).map_err(DataflowError::from).transpose() {
                                                session.give((
                                                    // Produce either the resulting row, or any evaluation error.
                                                    result.map(|iter| {
                                                        row_packer.clear();
                                                        row_packer.extend(iter);
                                                        row_packer.finish_and_reuse()
                                                    }),
                                                    *cap.time(),
                                                    1,
                                                ));
                                            }

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
    ).ok_err(|(d,t,r)| match d {
        Ok(d) => Ok((d,t,r)),
        Err(e) => Err((e,t,r)),
    })
}
