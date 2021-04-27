// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::DecoderState;

pub(crate) struct KeyValueDecoder {
    key_decoder: Box<dyn DecoderState>,
    value_decoder: Box<dyn DecoderState>,
}

impl KeyValueDecoder {
    pub fn new(
        key_decoder: Box<dyn DecoderState>,
        value_decoder: Box<dyn DecoderState>,
    ) -> KeyValueDecoder {
        KeyValueDecoder {
            key_decoder,
            value_decoder,
        }
    }
}

impl DecoderState for KeyValueDecoder {
    fn decode_key(&mut self, bytes: &[u8]) -> Result<Option<repr::Row>, String> {
        self.key_decoder.decode_upsert_value(bytes, None, None)
    }

    fn decode_upsert_value(
        &mut self,
        bytes: &[u8],
        aux_num: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> Result<Option<repr::Row>, String> {
        self.value_decoder
            .decode_upsert_value(bytes, aux_num, upstream_time_millis)
    }

    fn get_value(
        &mut self,
        bytes: &[u8],
        aux_num: Option<i64>,
        upstream_time_millis: Option<i64>,
    ) -> Option<Result<repr::Row, dataflow_types::DataflowError>> {
        self.value_decoder
            .get_value(bytes, aux_num, upstream_time_millis)
    }

    fn log_error_count(&mut self) {
        self.key_decoder.log_error_count();
        self.value_decoder.log_error_count()
    }
}
