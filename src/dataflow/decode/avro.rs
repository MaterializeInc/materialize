// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use log::error;

use super::{DecoderState, PushSession, EVENTS_COUNTER};
use dataflow_types::{Diff, Timestamp};
use interchange::avro::{Decoder, EnvelopeType};
use repr::Row;

pub struct AvroDecoderState {
    decoder: Decoder,
    events_success: i64,
    events_error: i64,
}

impl AvroDecoderState {
    pub fn new(
        reader_schema: &str,
        schema_registry_url: Option<url::Url>,
        envelope: EnvelopeType,
    ) -> Self {
        AvroDecoderState {
            decoder: Decoder::new(reader_schema, schema_registry_url, envelope),
            events_success: 0,
            events_error: 0,
        }
    }
}

impl DecoderState for AvroDecoderState {
    /// Reset number of success and failures with decoding
    fn reset_event_count(&mut self) {
        self.events_success = 0;
        self.events_error = 0;
    }

    fn decode_key(&mut self, bytes: &[u8]) -> Result<Row, String> {
        match self.decoder.decode(bytes) {
            Ok(diff_pair) => {
                if let Some(after) = diff_pair.after {
                    self.events_success += 1;
                    Ok(after)
                } else {
                    self.events_error += 1;
                    Err("no avro key found for record".to_string())
                }
            }
            Err(err) => {
                self.events_error += 1;
                Err(format!("avro deserialization error: {}", err))
            }
        }
    }

    /// give a session a key-value pair
    fn give_key_value<'a>(
        &mut self,
        key: Row,
        bytes: &[u8],
        _: Option<i64>,
        session: &mut PushSession<'a, (Row, Option<Row>, Timestamp)>,
        time: Timestamp,
    ) {
        let result = self.decoder.decode(bytes);
        match result {
            Ok(diff_pair) => {
                self.events_success += 1;
                session.give((key, diff_pair.after, time));
            }
            Err(err) => {
                self.events_error += 1;
                error!("avro deserialization error: {}", err)
            }
        }
    }

    /// give a session a plain value
    fn give_value<'a>(
        &mut self,
        bytes: &[u8],
        _: Option<i64>,
        session: &mut PushSession<'a, (Row, Timestamp, Diff)>,
        time: Timestamp,
    ) {
        match self.decoder.decode(bytes) {
            Ok(diff_pair) => {
                self.events_success += 1;
                if let Some(before) = diff_pair.before {
                    // Note - this is indeed supposed to be an insert,
                    // not a retraction! `before` already contains a `-1` value as the last
                    // element of the data, which will cause it to turn into a retraction
                    // in a future call to `explode`
                    // (currently in dataflow/render/mod.rs:299)
                    session.give((before, time, 1));
                }
                if let Some(after) = diff_pair.after {
                    session.give((after, time, 1));
                }
            }
            Err(err) => {
                self.events_error += 1;
                error!("avro deserialization error: {}", err)
            }
        }
    }

    /// Register number of success and failures with decoding
    fn log_error_count(&self) {
        if self.events_success > 0 {
            EVENTS_COUNTER.avro.success.inc_by(self.events_success);
        }
        if self.events_error > 0 {
            EVENTS_COUNTER.avro.error.inc_by(self.events_error);
        }
    }
}
