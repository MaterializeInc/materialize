// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dataflow_types::{DecodeError, ProtobufEncoding};
use interchange::protobuf::decode::{DecodedDescriptors, Decoder};
use repr::Row;

#[derive(Debug)]
pub struct ProtobufDecoderState {
    decoder: Decoder,
    events_success: i64,
    events_error: i64,
}

impl ProtobufDecoderState {
    pub fn new(
        ProtobufEncoding {
            descriptors,
            message_name,
        }: ProtobufEncoding,
    ) -> Self {
        let descriptors = DecodedDescriptors::from_bytes(&descriptors, message_name)
            .expect("descriptors provided to protobuf source are pre-validated");
        ProtobufDecoderState {
            decoder: Decoder::new(descriptors),
            events_success: 0,
            events_error: 0,
        }
    }
    pub fn get_value(&mut self, bytes: &[u8]) -> Option<Result<Row, DecodeError>> {
        match self.decoder.decode(bytes) {
            Ok(row) => {
                if let Some(row) = row {
                    self.events_success += 1;
                    Some(Ok(row))
                } else {
                    self.events_error += 1;
                    Some(Err(DecodeError::Text(format!(
                        "protobuf deserialization returned None"
                    ))))
                }
            }
            Err(err) => {
                self.events_error += 1;
                Some(Err(DecodeError::Text(format!(
                    "protobuf deserialization error: {:#}",
                    err
                ))))
            }
        }
    }
}
