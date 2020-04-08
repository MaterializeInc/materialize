// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet, VecDeque};
use std::iter::FromIterator;
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

        // Get all of the stream's shards.
        let (shard_queue, shard_to_iterator) = &mut get_shard_information(&stream_name, state)?;

        // todo: should put some type of timeout here
        let mut records = Vec::new();
        while let Some(shard) = shard_queue.pop_front() {
            println!("on shard: {}", shard);
            if let Some(shard_iterator) = shard_to_iterator.get(&shard) {
                let mut shard_iterator = shard_iterator.clone();
                while let Some(iterator) = &shard_iterator {
                    let get_records_input = GetRecordsInput {
                        limit: None,
                        shard_iterator: iterator.clone(),
                    };
                    let output = state
                        .tokio_runtime
                        .block_on(state.kinesis_client.get_records(get_records_input))
                        .map_err(|e| format!("getting Kinesis records: {}", e))?;
                    for record in output.records {
                        dbg!(&record);
                        records.push(record);
                    }

                    match output.millis_behind_latest {
                        Some(0) => shard_iterator = None,
                        _ => shard_iterator = output.next_shard_iterator,
                    }
                }
                println!("done with shard: {}", shard);
            }
        }

        // We don't guarantee order!
        let mut expected_set: HashSet<String> =
            HashSet::from_iter(self.expected_messages.iter().cloned());
        for record in records {
            let record_string = str::from_utf8(record.data.as_ref())
                .map_err(|e| format!("converting Kinesis record bytes to utf8: {}", e))?;
            if let false = expected_set.remove(record_string) {
                return Err(format!("Found extra Kinesis record: {}", record_string));
            }
        }
        if !expected_set.is_empty() {
            println!("Did not find expected Kinesis records:");
            for expected in expected_set.iter() {
                println!("{}", expected);
            }
            return Err(String::from("Missing expected Kinesis records"));
        }

        Ok(())
    }
}

fn get_shard_information(
    stream_name: &str,
    state: &mut State,
) -> Result<(VecDeque<String>, HashMap<String, Option<String>>), String> {
    let list_shards_input = ListShardsInput {
        exclusive_start_shard_id: None,
        max_results: None,
        next_token: None,
        stream_creation_timestamp: None,
        stream_name: Some(stream_name.to_string()),
    };
    let mut shard_ids = VecDeque::new();
    let mut shard_to_iterator = HashMap::new();
    match state
        .tokio_runtime
        .block_on(state.kinesis_client.list_shards(list_shards_input))
        .map_err(|e| format!("listing Kinesis shards: {}", e))?
        .shards
        .as_deref()
    {
        Some(shards) => {
            for shard in shards {
                shard_ids.push_back(shard.shard_id.clone());

                let shard_iterator_input = GetShardIteratorInput {
                    shard_id: shard.shard_id.clone(),
                    shard_iterator_type: String::from("TRIM_HORIZON"),
                    starting_sequence_number: None,
                    stream_name: stream_name.to_string(),
                    timestamp: None,
                };
                shard_to_iterator.insert(
                    shard.shard_id.clone(),
                    state
                        .tokio_runtime
                        .block_on(
                            state
                                .kinesis_client
                                .get_shard_iterator(shard_iterator_input),
                        )
                        .map_err(|e| format!("getting Kinesis shard iterator: {}", e))?
                        .shard_iterator,
                );
            }
        }
        None => return Err(String::from("Kinesis stream does not have any shards.")),
    }

    Ok((shard_ids, shard_to_iterator))
}
