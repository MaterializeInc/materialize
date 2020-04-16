// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use rusoto_kinesis::{DescribeStreamInput, Kinesis, UpdateShardCountInput};

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;
use crate::util::retry;

pub struct UpdateShardCountAction {
    stream_name: String,
    target_shard_count: i64,
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
            "updating Kinesis stream {} to have {} shards",
            stream_name, self.target_shard_count
        );

        let out = state
            .kinesis_client
            .update_shard_count(UpdateShardCountInput {
                scaling_type: "UNIFORM_SCALING".to_owned(),
                stream_name: stream_name.clone(),
                target_shard_count: self.target_shard_count,
            })
            .await
            .map_err(|e| format!("adding shards to stream {}: {}", &stream_name, e))?;
        dbg!(&out);

        // Verify the current shard count.
        retry::retry(|| async {
            let current_shard_count = state
                .kinesis_client
                .describe_stream(DescribeStreamInput {
                    exclusive_start_shard_id: None,
                    limit: None,
                    stream_name: stream_name.clone(),
                })
                .await
                .map_err(|e| format!("getting current shard count: {}", e))?
                .stream_description
                .shards
                .len();
            //https://stackoverflow.com/questions/28273169/how-do-i-convert-between-numeric-types-safely-and-idiomatically
            if current_shard_count as i64 != self.target_shard_count {
                return Err(format!(
                    "Expected {} shards, found {}",
                    self.target_shard_count, current_shard_count
                ));
            }
            Ok(())
        })
        .await
    }
}
