// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// The undo should delete stale records from a stream? -- don't think this is possible.
/// The redo should push records to a stream?
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use futures::executor::block_on;
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_kinesis::{
    CreateStreamInput, DeleteStreamInput, DescribeStreamInput, Kinesis, KinesisClient,
    ListStreamsInput, PutRecordError, PutRecordInput,
};

use ore::collections::CollectionExt;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct IngestAction {
    stream_prefix: String,
    format: Format,
    rows: Vec<String>,
}

enum Format {
    Bytes,
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let stream_prefix = format!("testdrive-{}", cmd.args.string("stream")?);
    let format = match cmd.args.string("format")?.as_str() {
        "bytes" => Format::Bytes,
        f => return Err(format!("unsupported message format for Kinesis: {}", f)),
    };
    cmd.args.done()?;

    Ok(IngestAction {
        stream_prefix,
        format,
        rows: cmd.input,
    })
}

impl Action for IngestAction {
    // Delete any stale stuff??
    // Won't be a thing....?
    fn undo(&self, state: &mut State) -> Result<(), String> {
        Ok(())
    }

    // Push data into Kinesis stream
    fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("{}-{}", self.stream_prefix, state.seed);
        println!("Creating Kinesis stream {}", &stream_name);

        // match the format
        // for each row, push to localstack.
        for row in &self.rows {
            let put_input = PutRecordInput {
                data: Bytes::from(row.clone()),
                explicit_hash_key: None,
                partition_key: String::from("testdrive"), // doesn't matter for now, only one shard
                sequence_number_for_ordering: None,
                stream_name: stream_name.clone(),
            };

            // The stream might not be immediately available to put records
            // into. Try a few times.
            // todo: have some sort of upper limit for failure here.
            let mut put_record = false;
            while !put_record {
                match state
                    .tokio_runtime
                    .block_on(state.kinesis_client.put_record(put_input.clone()))
                {
                    Ok(output) => {
                        put_record = true;
                        println!("put a record!!");
                        dbg!(&output);
                    }
                    Err(RusotoError::Service(PutRecordError::ResourceNotFound(err_string))) => {
                        println!("{} trying again in 1 second...", err_string);
                        thread::sleep(Duration::from_secs(1));
                    }
                    Err(err) => {
                        return Err(format!("unable to put Kinesis record: {}", err.to_string()))
                    }
                }
            }
        }

        Ok(())
    }
}
