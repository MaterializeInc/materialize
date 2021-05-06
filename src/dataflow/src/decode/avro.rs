// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::executor::block_on;

use dataflow_types::{DataflowError, DecodeError};
use interchange::avro::{Decoder, EnvelopeType};
use repr::Row;

use super::DecoderState;
use crate::metrics::EVENTS_COUNTER;

pub struct AvroDecoderState {
    decoder: Decoder,
    events_success: i64,
    events_error: i64,
}

impl AvroDecoderState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        value_schema: &str,
        schema_registry_config: Option<ccsr::ClientConfig>,
        envelope: EnvelopeType,
        reject_non_inserts: bool,
        debug_name: String,
        confluent_wire_format: bool,
    ) -> Result<Self, anyhow::Error> {
        Ok(AvroDecoderState {
            decoder: Decoder::new(
                value_schema,
                schema_registry_config,
                envelope,
                debug_name,
                confluent_wire_format,
                reject_non_inserts,
            )?,
            events_success: 0,
            events_error: 0,
        })
    }
}

impl DecoderState for AvroDecoderState {
    fn decode_key(&mut self, _: &[u8]) -> Result<Option<Row>, String> {
        Err(format!("Plain avro decoder has no key decoder"))
    }

    /// give a session a key-value pair
    fn decode_upsert_value<'a>(
        &mut self,
        bytes: &[u8],
        coord: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> Result<Option<Row>, String> {
        match block_on(self.decoder.decode(bytes, coord, upstream_time_millis)) {
            Ok(row) => {
                self.events_success += 1;
                Ok(Some(row))
            }
            Err(err) => {
                self.events_error += 1;
                Err(format!("avro deserialization error: {}", err))
            }
        }
    }

    /// give a session a plain value
    fn get_value(
        &mut self,
        bytes: &[u8],
        coord: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> Option<Result<Row, DataflowError>> {
        match block_on(self.decoder.decode(bytes, coord, upstream_time_millis)) {
            Ok(row) => {
                self.events_success += 1;
                Some(Ok(row))
            }
            Err(err) => {
                self.events_error += 1;
                Some(Err(DataflowError::DecodeError(DecodeError::Text(format!(
                    "avro deserialization error: {}",
                    err
                )))))
            }
        }
    }

    /// Register number of success and failures with decoding,
    /// and reset count of pending events
    fn log_error_count(&mut self) {
        if self.events_success > 0 {
            EVENTS_COUNTER.avro.success.inc_by(self.events_success);
            self.events_success = 0;
        }
        if self.events_error > 0 {
            EVENTS_COUNTER.avro.error.inc_by(self.events_error);
            self.events_error = 0;
        }
    }
}
