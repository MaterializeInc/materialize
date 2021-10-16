// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dataflow_types::{DataflowError, DecodeError};
use interchange::avro::{Decoder, EnvelopeType};
use repr::Datum;
use repr::Row;

#[derive(Debug)]
pub struct AvroDecoderState {
    decoder: Decoder,
    events_success: i64,
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
        })
    }

    pub async fn decode(
        &mut self,
        bytes: &mut &[u8],
        coord: Option<i64>,
        upstream_time_millis: Option<i64>,
        push_metadata: bool,
    ) -> Result<Option<Row>, DataflowError> {
        match self
            .decoder
            .decode(bytes, coord, upstream_time_millis)
            .await
        {
            Ok(mut row) => {
                self.events_success += 1;
                if push_metadata {
                    row.push(Datum::from(coord))
                }
                Ok(Some(row))
            }
            Err(err) => Err(DataflowError::DecodeError(DecodeError::Text(format!(
                "avro deserialization error: {:#}",
                err
            )))),
        }
    }
}
