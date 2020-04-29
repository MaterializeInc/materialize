// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use rusoto_kinesis::{CreateStreamInput, Kinesis};

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;
use crate::util;

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

        let create_stream_input = CreateStreamInput {
            shard_count: self.shard_count,
            stream_name: stream_name.clone(),
        };
        state
            .kinesis_client
            .create_stream(create_stream_input)
            .await
            .map_err(|e| format!("creating stream: {}", e))?;

        util::kinesis::wait_for_stream_shards(&state.kinesis_client, stream_name, self.shard_count)
            .await
    }
}
