// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// The undo should do nothing.
/// The redo should check expected output.
use futures::executor::block_on;
use rusoto_kinesis::{GetRecordsInput, GetShardIteratorInput, Kinesis, ListShardsInput};

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;
use failure::_core::str::from_utf8;

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
    // Do nothing! :tada:
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    // Consume messages from the stream, assert they match the expected.
    fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("testdrive-{}-{}", self.stream_prefix, state.seed);
        println!("Reading from Kinesis stream {}", &stream_name);

        // Right now, we only support reading from a single
        // Kinesis stream&shard.
        // todo@jldlaughin: Update this when we support multiple shards.

        let mut records = Vec::new();
        while records.is_empty() {
            let list_shards_input = ListShardsInput {
                exclusive_start_shard_id: None,
                max_results: None,
                next_token: None,
                stream_creation_timestamp: None,
                stream_name: Some(stream_name.clone()),
            };
            let shard_iterator = match state
                .tokio_runtime
                .block_on(state.kinesis_client.list_shards(list_shards_input))
            {
                Ok(output) => match output.shards.as_deref() {
                    Some([shard]) => {
                        let shard_iterator_input = GetShardIteratorInput {
                            shard_id: shard.shard_id.clone(),
                            shard_iterator_type: String::from("TRIM_HORIZON"),
                            starting_sequence_number: None,
                            stream_name: stream_name.clone(),
                            timestamp: None,
                        };
                        match state.tokio_runtime.block_on(
                            state
                                .kinesis_client
                                .get_shard_iterator(shard_iterator_input),
                        ) {
                            Ok(output) => match output.shard_iterator {
                                Some(iterator) => iterator,
                                None => {
                                    println!(
                                        "unable to find a shard iterator for Kinesis stream {}",
                                        &stream_name
                                    );
                                    continue;
                                    //                                        return Err(format!(
                                    //                                            "unable to find a shard iterator for Kinesis stream {}",
                                    //                                            &stream_name
                                    //                                        ))
                                }
                            },
                            Err(e) => {
                                println!(
                                    "hit error trying to get Kinesis shard iterator: {}",
                                    e.to_string()
                                );
                                continue;
                                //                                    return Err(format!(
                                //                                        "hit error trying to get Kinesis shard iterator: {}",
                                //                                        e.to_string()
                                //                                    ))
                            }
                        }
                    }
                    None | Some(_) => {
                        println!("Kinesis stream must have exactly one shard.");
                        continue;
                        //                            return Err(format!("Kinesis stream must have exactly one shard."))
                    }
                },
                Err(e) => {
                    println!("hit error trying to get Kinesis shards: {}", e.to_string());
                    continue;
                    //                        return Err(format!(
                    //                            "hit error trying to get Kinesis shards: {}",
                    //                            e.to_string()
                    //                        ))
                }
            };

            let get_records_input = GetRecordsInput {
                limit: None,
                shard_iterator,
            };
            records = match state
                .tokio_runtime
                .block_on(state.kinesis_client.get_records(get_records_input))
            {
                Ok(output) => output.records,
                Err(e) => {
                    return Err(format!(
                        "hit error getting Kinesis records: {}",
                        e.to_string()
                    ))
                }
            };
        }

        for (expected, actual) in self.expected_messages.iter().zip(records.iter_mut()) {
            let record_string = match from_utf8(actual.data.as_ref()) {
                Ok(str) => str,
                Err(e) => {
                    return Err(format!(
                        "hit error converting record bytes to utf8: {}",
                        e.to_string()
                    ))
                }
            };
            assert_eq!(expected, record_string);
        }
        println!("all equal!");

        Ok(())
    }
}
