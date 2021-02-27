// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::executor::block_on;
use log::error;

use interchange::avro::{DebeziumDeduplicationStrategy, Decoder, EnvelopeType};
use repr::{Diff, Row, Timestamp};

use super::{DecoderState, PushSession};
use crate::metrics::EVENTS_COUNTER;

pub struct AvroDecoderState {
    decoder: Decoder,
    events_success: i64,
    events_error: i64,
    reject_non_inserts: bool,
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
            )?,
            events_success: 0,
            events_error: 0,
            reject_non_inserts,
        })
    }
}

impl DecoderState for AvroDecoderState {
    fn decode_key(&mut self, bytes: &[u8]) -> Result<Row, String> {
        match block_on(self.decoder.decode(bytes, None, None)) {
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
    fn decode_upsert_value<'a>(
        &mut self,
        bytes: &[u8],
        coord: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> Result<Option<Row>, String> {
        match block_on(self.decoder.decode(bytes, coord, upstream_time_millis)) {
            Ok(diff_pair) => {
                self.events_success += 1;
                Ok(diff_pair.after)
            }
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
        session: &mut PushSession<'a, (Row, Timestamp, Diff)>,
        time: Timestamp,
    ) {
        match block_on(self.decoder.decode(bytes, coord, upstream_time_millis)) {
            Ok(diff_pair) => {
                self.events_success += 1;
                if diff_pair.before.is_some() {
                    if self.reject_non_inserts {
                        panic!("Updates and deletes are not allowed for this source! This probably means it was started with `start_offset`. Got diff pair: {:#?}", diff_pair)
                    }
                    // Note - this is indeed supposed to be an insert,
                    // not a retraction! `before` already contains a `-1` value as the last
                    // element of the data, which will cause it to turn into a retraction
                    // in a future call to `explode`
                    // (currently in dataflow/render/mod.rs:299)
                    session.give((diff_pair.before.unwrap(), time, 1));
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
