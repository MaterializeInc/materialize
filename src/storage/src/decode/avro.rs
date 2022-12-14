// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use tokio::runtime::Handle as TokioHandle;

use mz_interchange::avro::Decoder;
use mz_repr::Row;
use mz_storage_client::types::connections::CsrClient;
use mz_storage_client::types::errors::DecodeErrorKind;

#[derive(Debug)]
pub struct AvroDecoderState {
    tokio_handle: TokioHandle,
    decoder: Decoder<CsrClient>,
    events_success: i64,
}

impl AvroDecoderState {
    pub fn new(
        value_schema: &str,
        ccsr_client: Option<CsrClient>,
        debug_name: String,
        confluent_wire_format: bool,
    ) -> Result<Self, anyhow::Error> {
        Ok(AvroDecoderState {
            tokio_handle: TokioHandle::current(),
            decoder: Decoder::new(value_schema, ccsr_client, debug_name, confluent_wire_format)?,
            events_success: 0,
        })
    }

    pub fn decode(&mut self, bytes: &mut &[u8]) -> Result<Option<Row>, DecodeErrorKind> {
        match self.tokio_handle.block_on(self.decoder.decode(bytes)) {
            Ok(row) => {
                self.events_success += 1;
                Ok(Some(row))
            }
            Err(err) => Err(DecodeErrorKind::Text(format!(
                "avro deserialization error: {:#}",
                err
            ))),
        }
    }
}
