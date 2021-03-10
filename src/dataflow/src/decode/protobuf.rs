// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dataflow_types::{DataflowError, DecodeError};
use interchange::protobuf::{self, Decoder};
use repr::{Diff, Row, Timestamp};

use super::{DecoderState, PushSession};
use crate::metrics::EVENTS_COUNTER;

pub struct ProtobufDecoderState {
    decoder: Decoder,
    events_success: i64,
    events_error: i64,
}

impl ProtobufDecoderState {
    pub fn new(descriptors: &[u8], message_name: &str) -> Self {
        let descriptors = protobuf::decode_descriptors(descriptors)
            .expect("descriptors provided to protobuf source are pre-validated");
        ProtobufDecoderState {
            decoder: Decoder::new(descriptors, message_name),
            events_success: 0,
            events_error: 0,
        }
    }
}

impl DecoderState for ProtobufDecoderState {
    fn decode_key(&mut self, bytes: &[u8]) -> Result<Option<Row>, String> {
        // Note that we're passing `None` as the offset for the key -
        // since the key must remain stable (and we don't have an
        // offset available here anyway), we instruct the decoder to
        // not add any of that data.
        match self.decoder.decode(bytes, None) {
            Ok(row) => {
                if let Some(row) = row {
                    self.events_success += 1;
                    Ok(Some(row))
                } else {
                    self.events_error += 1;
                    Err("protobuf deserialization returned None".to_string())
                }
            }
            Err(err) => {
                self.events_error += 1;
                Err(format!("protobuf deserialization error: {:#}", err))
            }
        }
    }

    /// give a session a key-value pair
    fn decode_upsert_value<'a>(
        &mut self,
        bytes: &[u8],
        position: Option<i64>,
        _upstream_time_millis: Option<i64>,
    ) -> Result<Option<Row>, String> {
        match self.decoder.decode(bytes, position) {
            Ok(row) => {
                self.events_success += 1;
                Ok(row)
            }
            Err(err) => {
                self.events_error += 1;
                Err(format!("protobuf deserialization error: {:#}", err))
            }
        }
    }

    /// give a session a plain value
    fn give_value<'a>(
        &mut self,
        bytes: &[u8],
        position: Option<i64>,
        _: Option<i64>,
        session: &mut PushSession<'a, (Result<Row, DataflowError>, Timestamp, Diff)>,
        time: Timestamp,
    ) {
        match self.decoder.decode(bytes, position) {
            Ok(row) => {
                if let Some(row) = row {
                    self.events_success += 1;
                    session.give((Ok(row), time, 1));
                } else {
                    self.events_error += 1;
                    session.give((
                        Err(DataflowError::DecodeError(DecodeError::Text(format!(
                            "protobuf deserialization returned None"
                        )))),
                        time,
                        1,
                    ));
                }
            }
            Err(err) => {
                self.events_error += 1;
                session.give((
                    Err(DataflowError::DecodeError(DecodeError::Text(format!(
                        "protobuf deserialization error: {:#}",
                        err
                    )))),
                    time,
                    1,
                ));
            }
        }
    }

    /// Register number of success and failures with decoding,
    /// and reset count of pending events
    fn log_error_count(&mut self) {
        if self.events_success > 0 {
            EVENTS_COUNTER.protobuf.success.inc_by(self.events_success);
            self.events_success = 0;
        }
        if self.events_error > 0 {
            EVENTS_COUNTER.protobuf.error.inc_by(self.events_error);
            self.events_error = 0;
        }
    }
}
