// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::executor::block_on;

use dataflow_types::DecodeError;
use interchange::avro::Decoder;
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
        debug_name: String,
        confluent_wire_format: bool,
    ) -> Result<Self, anyhow::Error> {
        Ok(AvroDecoderState {
            decoder: Decoder::new(
                value_schema,
                schema_registry_config,
                debug_name,
                confluent_wire_format,
            )?,
            events_success: 0,
        })
    }

    pub fn decode(&mut self, bytes: &mut &[u8]) -> Result<Option<Row>, DecodeError> {
        match block_on(self.decoder.decode(bytes)) {
            Ok(row) => {
                self.events_success += 1;
                Ok(Some(row))
            }
            Err(err) => Err(DecodeError::Text(format!(
                "avro deserialization error: {:#}",
                err
            ))),
        }
    }
}
