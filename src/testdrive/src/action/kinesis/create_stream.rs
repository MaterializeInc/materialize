// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use rusoto_kinesis::{CreateStreamInput, DeleteStreamInput, Kinesis, ListStreamsInput};

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
    async fn undo(&self, state: &mut State) -> Result<(), String> {
        let list_streams_input = ListStreamsInput {
            exclusive_start_stream_name: None,
            limit: None,
        };
        let stream_names = state
            .kinesis_client
            .list_streams(list_streams_input)
            .await
            .map_err(|e| format!("listing Kinesis streams: {}", e))?
            .stream_names;

        for stream_name in stream_names {
            let delete_stream_input = DeleteStreamInput {
                enforce_consumer_deletion: Some(true),
                stream_name: stream_name.clone(),
            };
            state
                .kinesis_client
                .delete_stream(delete_stream_input)
                .await
                .map_err(|e| format!("deleting Kinesis stream: {}", e))?;
            println!("deleted stale Kinesis stream: {}", &stream_name);
        }
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("{}-{}", self.stream_name, state.seed);
        println!("creating Kinesis stream {}", stream_name);

        let create_stream_input = CreateStreamInput {
            shard_count: self.shard_count,
            stream_name,
        };
        state
            .kinesis_client
            .create_stream(create_stream_input)
            .await
            .map_err(|e| format!("creating stream: {}", e))?;

        Ok(())
    }
}
