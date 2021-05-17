// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;

use dataflow_types::CsvEncoding;
use dataflow_types::LinearOperator;

use dataflow_types::{DataflowError, DecodeError};
use repr::{Datum, Row};

#[derive(Debug)]
pub struct CsvDecoderState {
    header_row: bool,
    n_cols: usize,
    output: Vec<u8>,
    output_cursor: usize,
    ends: Vec<usize>,
    ends_cursor: usize,
    csv_reader: csv_core::Reader,
    demanded: Vec<bool>,
    row_packer: Row,
    events_error: usize,
    events_success: usize,
}

impl CsvDecoderState {
    fn total_events(&self) -> usize {
        self.events_error + self.events_success
    }

    pub fn new(format: CsvEncoding, operators: &mut Option<LinearOperator>) -> Self {
        let CsvEncoding {
            header_row,
            n_cols,
            delimiter,
        } = format;

        let operators = operators.take();
        let demanded = (0..n_cols)
            .map(move |c| {
                operators
                    .as_ref()
                    .map(|o| o.projection.contains(&c))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        Self {
            header_row,
            n_cols,
            output: vec![0],
            output_cursor: 0,
            ends: vec![0],
            ends_cursor: 1,
            csv_reader: csv_core::ReaderBuilder::new().delimiter(delimiter).build(),
            demanded,
            row_packer: Default::default(),
            events_error: 0,
            events_success: 0,
        }
    }

    pub fn next(
        &mut self,
        chunk: &mut &[u8],
        coord: Option<i64>,
        push_metadata: bool,
    ) -> Result<Option<Row>, DataflowError> {
        loop {
            let (result, n_input, n_output, n_ends) = self.csv_reader.read_record(
                *chunk,
                &mut self.output[self.output_cursor..],
                &mut self.ends[self.ends_cursor..],
            );
            self.output_cursor += n_output;
            *chunk = &(*chunk)[n_input..];
            self.ends_cursor += n_ends;
            match result {
                csv_core::ReadRecordResult::InputEmpty => break Ok(None),
                csv_core::ReadRecordResult::OutputFull => {
                    let length = self.output.len();
                    self.output.extend(std::iter::repeat(0).take(length));
                }
                csv_core::ReadRecordResult::OutputEndsFull => {
                    let length = self.ends.len();
                    self.ends.extend(std::iter::repeat(0).take(length));
                }
                csv_core::ReadRecordResult::Record | csv_core::ReadRecordResult::End => {
                    let result = {
                        let ends_valid = self.ends_cursor - 1;
                        if ends_valid == 0 {
                            break Ok(None);
                        }
                        if ends_valid != self.n_cols {
                            self.events_error += 1;
                            Err(DataflowError::DecodeError(DecodeError::Text(format!(
                                "CSV error at record number {}: expected {} columns, got {}.",
                                self.total_events(),
                                self.n_cols,
                                ends_valid
                            ))))
                        } else {
                            match std::str::from_utf8(&self.output[0..self.output_cursor]) {
                                Ok(output) => {
                                    self.events_success += 1;
                                    let mut row_packer = std::mem::take(&mut self.row_packer);
                                    if push_metadata {
                                        row_packer.extend(
                                            (0..self.n_cols)
                                                .map(|i| {
                                                    Datum::String(if self.demanded[i] {
                                                        &output[self.ends[i]..self.ends[i + 1]]
                                                    } else {
                                                        ""
                                                    })
                                                })
                                                .chain(iter::once(Datum::from(coord))),
                                        );
                                    } else {
                                        row_packer.extend((0..self.n_cols).map(|i| {
                                            Datum::String(if self.demanded[i] {
                                                &output[self.ends[i]..self.ends[i + 1]]
                                            } else {
                                                ""
                                            })
                                        }));
                                    }
                                    self.row_packer = row_packer;
                                    self.output_cursor = 0;
                                    self.ends_cursor = 1;
                                    Ok(Some(self.row_packer.finish_and_reuse()))
                                }
                                Err(e) => {
                                    self.events_error += 1;
                                    Err(DataflowError::DecodeError(DecodeError::Text(format!(
                                        "CSV error at record number {}: invalid UTF-8 ({})",
                                        self.total_events(),
                                        e
                                    ))))
                                }
                            }
                        }
                    };
                    if self.header_row {
                        self.header_row = false;
                        if chunk.is_empty() {
                            break Ok(None);
                        }
                    } else {
                        break result;
                    }
                }
            }
        }
    }
}
