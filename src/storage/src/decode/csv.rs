// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{Datum, Row};

use crate::types::errors::DecodeErrorKind;
use crate::types::sources::encoding::CsvEncoding;
use crate::types::transforms::LinearOperator;

#[derive(Debug)]
pub struct CsvDecoderState {
    next_row_is_header: bool,
    header_names: Option<Vec<String>>,
    n_cols: usize,
    output: Vec<u8>,
    output_cursor: usize,
    ends: Vec<usize>,
    ends_cursor: usize,
    csv_reader: csv_core::Reader,
    demanded: Vec<bool>,
    row_buf: Row,
    events_error: usize,
    events_success: usize,
}

impl CsvDecoderState {
    fn total_events(&self) -> usize {
        self.events_error + self.events_success
    }

    pub fn new(format: CsvEncoding, operators: &mut Option<LinearOperator>) -> Self {
        let CsvEncoding { columns, delimiter } = format;
        let n_cols = columns.arity();

        let operators = operators.take();
        let demanded = (0..n_cols)
            .map(move |c| {
                operators
                    .as_ref()
                    .map(|o| o.projection.contains(&c))
                    .unwrap_or(true)
            })
            .collect::<Vec<_>>();

        let header_names = columns.into_header_names();
        Self {
            next_row_is_header: header_names.is_some(),
            header_names,
            n_cols,
            output: vec![0],
            output_cursor: 0,
            ends: vec![0],
            ends_cursor: 1,
            csv_reader: csv_core::ReaderBuilder::new().delimiter(delimiter).build(),
            demanded,
            row_buf: Row::default(),
            events_error: 0,
            events_success: 0,
        }
    }

    pub fn reset_for_new_object(&mut self) {
        if self.header_names.is_some() {
            self.next_row_is_header = true;
        }
    }

    pub fn decode(&mut self, chunk: &mut &[u8]) -> Result<Option<Row>, DecodeErrorKind> {
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
                // Error cases
                csv_core::ReadRecordResult::InputEmpty => break Ok(None),
                csv_core::ReadRecordResult::OutputFull => {
                    let length = self.output.len();
                    self.output.extend(std::iter::repeat(0).take(length));
                }
                csv_core::ReadRecordResult::OutputEndsFull => {
                    let length = self.ends.len();
                    self.ends.extend(std::iter::repeat(0).take(length));
                }
                // Success cases
                csv_core::ReadRecordResult::Record | csv_core::ReadRecordResult::End => {
                    let result = {
                        let ends_valid = self.ends_cursor - 1;
                        if ends_valid == 0 {
                            break Ok(None);
                        }
                        if ends_valid != self.n_cols {
                            self.events_error += 1;
                            Err(DecodeErrorKind::Text(format!(
                                "CSV error at record number {}: expected {} columns, got {}.",
                                self.total_events(),
                                self.n_cols,
                                ends_valid
                            )))
                        } else {
                            match std::str::from_utf8(&self.output[0..self.output_cursor]) {
                                Ok(output) => {
                                    self.events_success += 1;
                                    let mut row_packer = self.row_buf.packer();
                                    row_packer.extend((0..self.n_cols).map(|i| {
                                        Datum::String(
                                            if self.next_row_is_header || self.demanded[i] {
                                                &output[self.ends[i]..self.ends[i + 1]]
                                            } else {
                                                ""
                                            },
                                        )
                                    }));
                                    self.output_cursor = 0;
                                    self.ends_cursor = 1;
                                    Ok(Some(self.row_buf.clone()))
                                }
                                Err(e) => {
                                    self.events_error += 1;
                                    Err(DecodeErrorKind::Text(format!(
                                        "CSV error at record number {}: invalid UTF-8 ({})",
                                        self.total_events(),
                                        e
                                    )))
                                }
                            }
                        }
                    };

                    // skip header rows, do not send them into dataflow
                    if self.next_row_is_header {
                        self.next_row_is_header = false;

                        if let Ok(Some(row)) = &result {
                            let mismatched = row
                                .iter()
                                .zip(self.header_names.iter().flatten())
                                .enumerate()
                                .find(|(_, (actual, expected))| actual.unwrap_str() != &**expected);
                            if let Some((i, (actual, expected))) = mismatched {
                                break Err(DecodeErrorKind::Text(format!(
                                    "source file contains incorrect columns '{:?}', \
                                     first mismatched column at index {} expected={} actual={}",
                                    row,
                                    i + 1,
                                    expected,
                                    actual
                                )));
                            }
                        }
                        if chunk.is_empty() {
                            break Ok(None);
                        } else if result.is_err() {
                            break result;
                        }
                    } else {
                        break result;
                    }
                }
            }
        }
    }
}
