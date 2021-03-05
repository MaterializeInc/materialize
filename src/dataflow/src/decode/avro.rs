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
use interchange::avro::{DebeziumDeduplicationStrategy, Decoder, EnvelopeType};
use repr::{Diff, Row, Timestamp};

use super::{DecoderState, PushSession};
use crate::metrics::EVENTS_COUNTER;

pub struct AvroDecoderState {
    decoder: Decoder,
    events_success: i64,
    events_error: i64,
}

impl AvroDecoderState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        reader_schema: &str,
        schema_registry_config: Option<ccsr::ClientConfig>,
        envelope: EnvelopeType,
        reject_non_inserts: bool,
        debug_name: String,
        worker_index: usize,
        dedup_strat: Option<DebeziumDeduplicationStrategy>,
        dbz_key_indices: Option<Vec<usize>>,
        confluent_wire_format: bool,
    ) -> Result<Self, anyhow::Error> {
        Ok(AvroDecoderState {
            decoder: Decoder::new(
                reader_schema,
                schema_registry_config,
                envelope,
                debug_name,
                worker_index,
                dedup_strat,
                dbz_key_indices,
                confluent_wire_format,
                reject_non_inserts,
            )?,
            events_success: 0,
            events_error: 0,
        })
    }
}

impl DecoderState for AvroDecoderState {
    fn decode_key(&mut self, bytes: &[u8]) -> Result<Option<Row>, String> {
        match block_on(self.decoder.decode(bytes, None, None)) {
            Ok(Some(row)) => {
                self.events_success += 1;
                Ok(Some(row))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                self.events_error += 1;
                Err(format!("avro deserialization error: {}", err))
            }
        }
    }

    /// give a session a key-value pair
    fn decode_upsert_value<'a>(
        &mut self,
        bytes: &[u8],
        coord: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> Result<Option<Row>, String> {
        match block_on(self.decoder.decode(bytes, coord, upstream_time_millis)) {
            Ok(Some(row)) => {
                self.events_success += 1;
                Ok(Some(row))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                self.events_error += 1;
                Err(format!("avro deserialization error: {}", err))
            }
        }
    }

    /// give a session a plain value
    fn give_value<'a>(
        &mut self,
        bytes: &[u8],
        coord: Option<i64>,
        upstream_time_millis: Option<i64>,
        session: &mut PushSession<'a, (Result<Row, DataflowError>, Timestamp, Diff)>,
        time: Timestamp,
    ) {
        match block_on(self.decoder.decode(bytes, coord, upstream_time_millis)) {
            Ok(Some(row)) => {
                self.events_success += 1;
                session.give((Ok(row), time, 1));
            }
            Ok(None) => {}
            Err(err) => {
                self.events_error += 1;
                session.give((
                    Err(DataflowError::DecodeError(DecodeError::Text(format!(
                        "avro deserialization error: {}",
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
            EVENTS_COUNTER.avro.success.inc_by(self.events_success);
            self.events_success = 0;
        }
        if self.events_error > 0 {
            EVENTS_COUNTER.avro.error.inc_by(self.events_error);
            self.events_error = 0;
        }
    }
}
