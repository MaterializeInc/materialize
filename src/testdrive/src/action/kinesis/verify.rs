// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str;

use rusoto_kinesis::{GetRecordsInput, GetShardIteratorInput, Kinesis, ListShardsInput};

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct VerifyAction {
    stream_prefix: String,
    expected_messages: Vec<String>,
}

pub fn build_verify(mut cmd: BuiltinCommand) -> Result<VerifyAction, String> {
    let stream_prefix = cmd.args.string("stream")?;
    let expected_messages = cmd.input;

    cmd.args.done()?;

    Ok(VerifyAction {
        stream_prefix,
        expected_messages,
    })
}

impl Action for VerifyAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    // Consume messages from the stream, assert they match the expected.
    fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("testdrive-{}-{}", self.stream_prefix, state.seed);

        let mut records = Vec::new();
        while records.is_empty() {
            let list_shards_input = ListShardsInput {
                exclusive_start_shard_id: None,
                max_results: None,
                next_token: None,
                stream_creation_timestamp: None,
                stream_name: Some(stream_name.clone()),
            };

            // Right now, we only support reading from a single
            // Kinesis stream&shard.
            // todo@jldlaughin: Update this when we support multiple shards.
            let shard = match state
                .tokio_runtime
                .block_on(state.kinesis_client.list_shards(list_shards_input))
                .map_err(|e| format!("listing Kinesis shards: {}", e))?
                .shards
                .as_deref()
            {
                Some([shard]) => shard.clone(),
                None | Some(_) => {
                    return Err(String::from("Kinesis stream must have exactly one shard"))
                }
            };

            let shard_iterator_input = GetShardIteratorInput {
                shard_id: shard.shard_id.clone(),
                shard_iterator_type: String::from("TRIM_HORIZON"),
                starting_sequence_number: None,
                stream_name: stream_name.clone(),
                timestamp: None,
            };
            let shard_iterator = match state
                .tokio_runtime
                .block_on(
                    state
                        .kinesis_client
                        .get_shard_iterator(shard_iterator_input),
                )
                .map_err(|e| format!("getting Kinesis shard iterator: {}", e))?
                .shard_iterator
            {
                Some(iterator) => iterator,
                None => return Err(String::from("No shard iterator")),
            };

            let get_records_input = GetRecordsInput {
                limit: None,
                shard_iterator,
            };
            records = state
                .tokio_runtime
                .block_on(state.kinesis_client.get_records(get_records_input))
                .map_err(|e| format!("getting Kinesis records: {}", e))?
                .records;
        }

        for (expected, actual) in self.expected_messages.iter().zip(records.iter_mut()) {
            let record_string = str::from_utf8(actual.data.as_ref())
                .map_err(|e| format!("converting Kinesis record bytes to utf8: {}", e))?;
            assert_eq!(expected, record_string);
        }

        Ok(())
    }
}
