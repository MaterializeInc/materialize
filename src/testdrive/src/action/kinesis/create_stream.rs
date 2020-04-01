// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rusoto_kinesis::{CreateStreamInput, DeleteStreamInput, Kinesis, ListStreamsInput};

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct CreateStreamAction {
    stream_name: String,
}

pub fn build_create_stream(mut cmd: BuiltinCommand) -> Result<CreateStreamAction, String> {
    let stream_name = format!("testdrive-{}", cmd.args.string("stream")?);
    cmd.args.done()?;

    Ok(CreateStreamAction { stream_name })
}

impl Action for CreateStreamAction {
    fn undo(&self, state: &mut State) -> Result<(), String> {
        let list_streams_input = ListStreamsInput {
            exclusive_start_stream_name: None,
            limit: None,
        };
        let stream_names = match state
            .tokio_runtime
            .block_on(state.kinesis_client.list_streams(list_streams_input))
        {
            Ok(output) => output.stream_names,
            Err(e) => {
                return Err(format!(
                    "hit error listing Kinesis streams: {}",
                    e.to_string()
                ))
            }
        };

        for stream_name in stream_names {
            let delete_stream_input = DeleteStreamInput {
                enforce_consumer_deletion: Some(true),
                stream_name: stream_name.clone(),
            };
            match state
                .tokio_runtime
                .block_on(state.kinesis_client.delete_stream(delete_stream_input))
            {
                Ok(_output) => {
                    println!("deleted stale Kinesis stream: {}", &stream_name);
                }
                Err(e) => {
                    return Err(format!(
                        "hit error deleting stale Kinesis stream: {}",
                        e.to_string()
                    ))
                }
            }
        }
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("{}-{}", self.stream_name, state.seed);
        println!("creating Kinesis stream {}", stream_name);

        let create_stream_input = CreateStreamInput {
            shard_count: 1,
            stream_name,
        };
        if let Err(e) = state
            .tokio_runtime
            .block_on(state.kinesis_client.create_stream(create_stream_input))
        {
            return Err(format!("hit error creating stream: {}", e.to_string()));
        }

        Ok(())
    }
}
