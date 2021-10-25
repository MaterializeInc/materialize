// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dataflow_types::{DataflowError, DecodeError};
use interchange::protobuf;
use interchange::protobuf::decode::{DecodedDescriptors, Decoder};
use repr::Row;

#[derive(Debug)]
pub struct ProtobufDecoderState {
    decoder: Decoder,
    events_success: i64,
    events_error: i64,
}

impl ProtobufDecoderState {
    pub fn new(descriptors: &[u8], message_name: Option<String>) -> Self {
        let DecodedDescriptors {
            descriptors,
            first_message_name,
        } = protobuf::decode_descriptors(descriptors)
            .expect("descriptors provided to protobuf source are pre-validated");
        let message_name = message_name.as_ref().unwrap_or_else(|| &first_message_name);
        ProtobufDecoderState {
            decoder: Decoder::new(descriptors, &message_name),
            events_success: 0,
            events_error: 0,
        }
    }
    pub fn get_value(&mut self, bytes: &[u8]) -> Option<Result<Row, DataflowError>> {
        match self.decoder.decode(bytes) {
            Ok(row) => {
                if let Some(row) = row {
                    self.events_success += 1;
                    Some(Ok(row))
                } else {
                    self.events_error += 1;
                    Some(Err(DataflowError::DecodeError(DecodeError::Text(format!(
                        "protobuf deserialization returned None"
                    )))))
                }
            }
            Err(err) => {
                self.events_error += 1;
                Some(Err(DataflowError::DecodeError(DecodeError::Text(format!(
                    "protobuf deserialization error: {:#}",
                    err
                )))))
            }
        }
    }
}
