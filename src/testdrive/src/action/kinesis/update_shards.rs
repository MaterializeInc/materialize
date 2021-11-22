// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::time::Duration;

use async_trait::async_trait;
use aws_sdk_kinesis::model::{ScalingType, StreamStatus};

use ore::retry::Retry;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct UpdateShardCountAction {
    stream_name: String,
    target_shard_count: i32,
}

pub fn build_update_shards(mut cmd: BuiltinCommand) -> Result<UpdateShardCountAction, String> {
    let stream_name = format!("testdrive-{}", cmd.args.string("stream")?);
    let target_shard_count = cmd.args.parse("shards")?;
    cmd.args.done()?;

    Ok(UpdateShardCountAction {
        stream_name,
        target_shard_count,
    })
}

#[async_trait]
impl Action for UpdateShardCountAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("{}-{}", self.stream_name, state.seed);
        println!(
            "Updating Kinesis stream {} to have {} shards",
            stream_name, self.target_shard_count
        );

        state
            .kinesis_client
            .update_shard_count()
            .scaling_type(ScalingType::UniformScaling)
            .stream_name(&stream_name)
            .target_shard_count(self.target_shard_count)
            .send()
            .await
            .map_err(|e| format!("adding shards to stream {}: {}", &stream_name, e))?;

        // Verify the current shard count.
        Retry::default()
            .max_duration(cmp::max(state.default_timeout, Duration::from_secs(60)))
            .retry(|_| async {
                // Wait for shards to stop updating.
                let description = state
                    .kinesis_client
                    .describe_stream()
                    .stream_name(&stream_name)
                    .send()
                    .await
                    .map_err(|e| format!("getting current shard count: {}", e))?
                    .stream_description
                    .unwrap();
                if description.stream_status != Some(StreamStatus::Active) {
                    return Err(format!(
                        "stream {} is not active, is {:?}",
                        stream_name, description.stream_status
                    ));
                }

                let active_shards_len = i32::try_from(
                    description
                        .shards
                        .unwrap()
                        .iter()
                        .filter(|shard| {
                            shard
                                .sequence_number_range
                                .as_ref()
                                .unwrap()
                                .ending_sequence_number
                                .is_none()
                        })
                        .count(),
                )
                .map_err(|e| format!("converting shard length to i32: {}", e))?;
                if active_shards_len != self.target_shard_count {
                    return Err(format!(
                        "Expected {} shards, found {}",
                        self.target_shard_count, active_shards_len
                    ));
                }
                Ok(())
            })
            .await
    }
}
