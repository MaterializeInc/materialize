// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashSet, VecDeque};
use std::str;
use std::time::Instant;

use async_trait::async_trait;
use itertools::Itertools;
use rusoto_kinesis::{GetRecordsInput, GetShardIteratorInput, Kinesis, KinesisClient};

use ore::kinesis::get_shard_ids;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;
use crate::util::kinesis::DEFAULT_KINESIS_TIMEOUT;

pub struct VerifyAction {
    stream_prefix: String,
    expected_records: HashSet<String>,
}

pub fn build_verify(mut cmd: BuiltinCommand) -> Result<VerifyAction, String> {
    let stream_prefix = cmd.args.string("stream")?;
    let expected_records: HashSet<String> = cmd.input.into_iter().collect();

    cmd.args.done()?;

    Ok(VerifyAction {
        stream_prefix,
        expected_records,
    })
}

#[async_trait]
impl Action for VerifyAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("testdrive-{}-{}", self.stream_prefix, state.seed);

        let mut shard_iterators = get_shard_iterators(&state.kinesis_client, &stream_name).await?;
        let timer = Instant::now();
        let mut records: HashSet<String> = HashSet::new();
        while let Some(iterator) = shard_iterators.pop_front() {
            if let Some(iterator) = &iterator {
                let output = state
                    .kinesis_client
                    .get_records(GetRecordsInput {
                        limit: None,
                        shard_iterator: iterator.clone(),
                    })
                    .await
                    .map_err(|e| format!("getting Kinesis records: {}", e))?;
                for record in output.records {
                    records.insert(
                        String::from_utf8(record.data.to_vec()).map_err(|e| {
                            format!("converting Kinesis record bytes to utf8: {}", e)
                        })?,
                    );
                }
                match output.millis_behind_latest {
                    // Test hack!
                    // Assume all records have already been written to the stream. Once you've
                    // caught up, you're done with that shard.
                    // NOTE: this is not true for real Kinesis streams as data could still be
                    // arriving.
                    Some(0) => (),
                    _ => shard_iterators.push_back(output.next_shard_iterator),
                };
                if timer.elapsed() > DEFAULT_KINESIS_TIMEOUT {
                    // Unable to read all Kinesis records in the default
                    // time allotted -- fail.
                    return Err(format!(
                        "timeout reading from Kinesis stream: {}",
                        stream_name
                    ));
                }
            }
        }

        // For now, we don't guarantee any type of ordering!
        if records != self.expected_records {
            let missing_records = &self.expected_records - &records;
            let extra_records = &records - &self.expected_records;
            return Err(format!(
                "kinesis records did not match:\nmissing:\n{}\nextra:\n{}",
                missing_records.iter().join("\n"),
                extra_records.iter().join("\n")
            ));
        }

        Ok(())
    }
}

async fn get_shard_iterators(
    kinesis_client: &KinesisClient,
    stream_name: &str,
) -> Result<VecDeque<Option<String>>, String> {
    let mut iterators: VecDeque<Option<String>> = VecDeque::new();
    for shard_id in get_shard_ids(kinesis_client, stream_name)
        .await
        .map_err(|e| format!("listing Kinesis shards: {}", e))?
    {
        iterators.push_back(
            kinesis_client
                .get_shard_iterator(GetShardIteratorInput {
                    shard_id: shard_id.clone(),
                    shard_iterator_type: String::from("TRIM_HORIZON"),
                    starting_sequence_number: None,
                    stream_name: stream_name.to_string(),
                    timestamp: None,
                })
                .await
                .map_err(|e| format!("getting Kinesis shard iterator: {}", e))?
                .shard_iterator,
        );
    }

    Ok(iterators)
}
