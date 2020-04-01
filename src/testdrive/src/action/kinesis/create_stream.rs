// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::Duration;

use futures::executor::block_on;
use rusoto_kinesis::{
    CreateStreamInput, DeleteStreamInput, DescribeStreamInput, Kinesis, KinesisClient,
    ListStreamsInput, PutRecordError, PutRecordInput,
};

use ore::collections::CollectionExt;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct CreateStreamAction {
    stream_name: String,
}

pub fn build_create_stream(mut cmd: BuiltinCommand) -> Result<CreateStreamAction, String> {
    println!("in build create stream");
    let stream_name = format!("testdrive-{}", cmd.args.string("stream")?);
    cmd.args.done()?;

    Ok(CreateStreamAction { stream_name })
}

impl Action for CreateStreamAction {
    // delete the stream, if it exists
    fn undo(&self, state: &mut State) -> Result<(), String> {
        println!(
            "Deleting stale Kinesis streams with prefix {}",
            self.stream_name
        );

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
                Ok(output) => {
                    println!("Deleted stale Kinesis stream: {}", &stream_name);
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
        println!("Creating Kinesis stream {}", stream_name);

        let create_stream_input = CreateStreamInput {
            shard_count: 1,
            stream_name: stream_name.to_string(),
        };
        if let Err(e) = state
            .tokio_runtime
            .block_on(state.kinesis_client.create_stream(create_stream_input))
        {
            return Err(format!("hit error creating stream: {}", e.to_string()));
        }
        println!("created stream! fuck yeah!");

        // We need the ARN of the created stream to create a
        // Kinesis source in Materialize.
        let describe_input = DescribeStreamInput {
            exclusive_start_shard_id: None,
            limit: None,
            stream_name: stream_name.to_string(),
        };
        let arn = match state
            .tokio_runtime
            .block_on(state.kinesis_client.describe_stream(describe_input))
        {
            Ok(output) => {
                dbg!(&output);
                output.stream_description.stream_arn
            }
            Err(err) => {
                return Err(format!(
                    "hit error trying to describe Kinesis stream: {}",
                    err.to_string()
                ))
            }
        };

        Ok(())
    }
}
