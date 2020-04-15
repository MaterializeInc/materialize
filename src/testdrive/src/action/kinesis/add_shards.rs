// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use futures::executor::block_on;
use rusoto_kinesis::{Kinesis, UpdateShardCountInput};

use crate::action::kinesis::{get_current_shard_count, verify_shard_count};
use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct AddShardsAction {
    stream_name: String,
    target_shard_count: i64,
}

pub fn build_add_shards(mut cmd: BuiltinCommand) -> Result<AddShardsAction, String> {
    let stream_name = format!("testdrive-{}", cmd.args.string("stream")?);
    let target_shard_count = cmd.args.parse("shards")?;
    cmd.args.done()?;

    Ok(AddShardsAction {
        stream_name,
        target_shard_count,
    })
}

#[async_trait]
impl Action for AddShardsAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("{}-{}", self.stream_name, state.seed);
        println!(
            "adding {} shards to Kinesis stream {}",
            self.target_shard_count, stream_name
        );

        let current_shard_count = get_current_shard_count(&state.kinesis_client, &stream_name)?;
        state
            .kinesis_client
            .update_shard_count(UpdateShardCountInput {
                scaling_type: "UNIFORM_SCALING".to_owned(),
                stream_name: stream_name.clone(),
                target_shard_count: current_shard_count + self.target_shard_count,
            })
            .await
            .map_err(|e| format!("adding shards to stream {}: {}", &stream_name, e))?;

        block_on(verify_shard_count(
            &state.kinesis_client,
            &stream_name,
            self.target_shard_count,
        ))?;

        Ok(())
    }
}
