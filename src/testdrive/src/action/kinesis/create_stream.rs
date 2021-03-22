// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::convert::TryFrom;
use std::time::Duration;

use async_trait::async_trait;
use rusoto_kinesis::{CreateStreamInput, DescribeStreamInput, Kinesis};

use ore::retry::Retry;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct CreateStreamAction {
    stream_name: String,
    shard_count: i64,
}

pub fn build_create_stream(mut cmd: BuiltinCommand) -> Result<CreateStreamAction, String> {
    let stream_name = format!("testdrive-{}", cmd.args.string("stream")?);
    let shard_count = cmd.args.parse("shards")?;
    cmd.args.done()?;

    Ok(CreateStreamAction {
        stream_name,
        shard_count,
    })
}

#[async_trait]
impl Action for CreateStreamAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("{}-{}", self.stream_name, state.seed);
        println!("Creating Kinesis stream {}", stream_name);

        state
            .kinesis_client
            .create_stream(CreateStreamInput {
                shard_count: self.shard_count,
                stream_name: stream_name.clone(),
            })
            .await
            .map_err(|e| format!("creating stream: {}", e))?;
        state.kinesis_stream_names.push(stream_name.clone());

        Retry::default()
            .max_duration(cmp::max(state.default_timeout, Duration::from_secs(60)))
            .retry(|_| async {
                let description = state
                    .kinesis_client
                    .describe_stream(DescribeStreamInput {
                        exclusive_start_shard_id: None,
                        limit: None,
                        stream_name: stream_name.clone(),
                    })
                    .await
                    .map_err(|e| format!("getting current shard count: {}", e))?
                    .stream_description;
                if description.stream_status != "ACTIVE" {
                    return Err(format!(
                        "stream {} is not active, is {}",
                        stream_name, description.stream_status
                    ));
                }

                let active_shards_len = i64::try_from(
                    description
                        .shards
                        .iter()
                        .filter(|shard| {
                            shard.sequence_number_range.ending_sequence_number.is_none()
                        })
                        .count(),
                )
                .map_err(|e| format!("converting shard length to i64: {}", e))?;
                if active_shards_len != self.shard_count {
                    return Err(format!(
                        "expected {} shards, found {}",
                        self.shard_count, active_shards_len
                    ));
                }

                Ok(())
            })
            .await
    }
}
