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

pub struct RemoveShardsAction {
    stream_name: String,
    shards_to_remove: i64,
}

pub fn build_remove_shards(mut cmd: BuiltinCommand) -> Result<RemoveShardsAction, String> {
    let stream_name = format!("testdrive-{}", cmd.args.string("stream")?);
    let shards_to_remove = cmd.args.parse("shards")?;
    cmd.args.done()?;

    Ok(RemoveShardsAction {
        stream_name,
        shards_to_remove,
    })
}

#[async_trait]
impl Action for RemoveShardsAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("{}-{}", self.stream_name, state.seed);
        println!(
            "removing {} shards from Kinesis stream {}",
            self.shards_to_remove, stream_name
        );

        let current_shard_count = get_current_shard_count(&state.kinesis_client, &stream_name)?;
        if current_shard_count - self.shards_to_remove <= 0 {
            return Err(format!(
                "Kinesis streams must have at least one shard. Tried to remove {} shards, have {}",
                self.shards_to_remove, current_shard_count
            ));
        }
        state
            .kinesis_client
            .update_shard_count(UpdateShardCountInput {
                scaling_type: "UNIFORM_SCALING".to_owned(),
                stream_name: stream_name.clone(),
                target_shard_count: current_shard_count - self.shards_to_remove,
            })
            .await
            .map_err(|e| format!("removing shards from stream {}: {}", &stream_name, e))?;

        block_on(verify_shard_count(
            &state.kinesis_client,
            &stream_name,
            self.shards_to_remove,
        ))?;

        Ok(())
    }
}
