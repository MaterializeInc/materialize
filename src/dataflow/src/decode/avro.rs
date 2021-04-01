// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use futures::executor::block_on;

use dataflow_types::{DataflowError, DecodeError};
use interchange::avro::{DebeziumDeduplicationStrategy, Decoder, EnvelopeType};
use repr::Row;

use super::DecoderState;
use crate::metrics::EVENTS_COUNTER;

pub struct AvroDecoderState {
    decoder: Decoder,
    key_decoder: Option<Decoder>,
    events_success: i64,
    events_error: i64,
}

impl AvroDecoderState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        key_schema: Option<&str>,
        value_schema: &str,
        schema_registry_config: Option<ccsr::ClientConfig>,
        envelope: EnvelopeType,
        reject_non_inserts: bool,
        debug_name: String,
        worker_index: usize,
        dedup_strat: Option<DebeziumDeduplicationStrategy>,
        dbz_key_indices: Option<Vec<usize>>,
        confluent_wire_format: bool,
    ) -> Result<Self, anyhow::Error> {
        let key_decoder = key_schema
            .map(|key_schema| {
                Decoder::new(
                    key_schema,
                    schema_registry_config.clone(),
                    EnvelopeType::None,
                    debug_name.clone(),
                    worker_index,
                    None,
                    dbz_key_indices.clone(),
                    confluent_wire_format,
                    reject_non_inserts,
                )
            })
            .transpose()
            .context("Creating Kafka Avro Key decoder")?;
        Ok(AvroDecoderState {
            decoder: Decoder::new(
                value_schema,
                schema_registry_config,
                envelope,
                debug_name,
                worker_index,
                dedup_strat,
                dbz_key_indices,
                confluent_wire_format,
                reject_non_inserts,
            )?,
            key_decoder,
            events_success: 0,
            events_error: 0,
        })
    }
}

impl DecoderState for AvroDecoderState {
    fn decode_key(&mut self, bytes: &[u8]) -> Result<Option<Row>, String> {
        match self.key_decoder.as_mut() {
            Some(key_decoder) => match block_on(key_decoder.decode(bytes, None, None)) {
                Ok(Some(row)) => {
                    self.events_success += 1;
                    Ok(Some(row))
                }
                Ok(None) => Ok(None),
                Err(err) => {
                    self.events_error += 1;
                    Err(format!("avro deserialization error: {}", err))
                }
            },
            None => Err(format!("No key decoder configured")),
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
    fn get_value(
        &mut self,
        bytes: &[u8],
        coord: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> Option<Result<Row, DataflowError>> {
        match block_on(self.decoder.decode(bytes, coord, upstream_time_millis)) {
            Ok(Some(row)) => {
                self.events_success += 1;
                Some(Ok(row))
            }
            Ok(None) => None,
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
