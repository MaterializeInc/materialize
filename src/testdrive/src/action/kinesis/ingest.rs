// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::{self, Write};

use async_trait::async_trait;
use bytes::Bytes;
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use rusoto_core::RusotoError;
use rusoto_kinesis::{Kinesis, PutRecordError, PutRecordInput};
use tokio::time::{self, Duration};

use crate::action::kinesis::DEFAULT_KINESIS_TIMEOUT;
use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct IngestAction {
    stream_prefix: String,
    rows: Vec<String>,
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let stream_prefix = format!("testdrive-{}", cmd.args.string("stream")?);
    match cmd.args.string("format")?.as_str() {
        "bytes" => (),
        f => return Err(format!("unsupported message format for Kinesis: {}", f)),
    }
    cmd.args.done()?;

    Ok(IngestAction {
        stream_prefix,
        rows: cmd.input,
    })
}

#[async_trait]
impl Action for IngestAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let stream_name = format!("{}-{}", self.stream_prefix, state.seed);

        for row in &self.rows {
            // Generating and using random partition keys allows us to test
            // reading Kinesis records from a variable number of shards that
            // are distributed differently on every run.
            let random_partition_key: String =
                thread_rng().sample_iter(&Alphanumeric).take(30).collect();
            let put_input = PutRecordInput {
                data: Bytes::from(row.clone()),
                explicit_hash_key: None,
                partition_key: random_partition_key,
                sequence_number_for_ordering: None,
                stream_name: stream_name.clone(),
            };

            // The Kinesis stream might not be immediately available,
            // be prepared to back off.
            let mut total_backoff = Duration::from_millis(0);
            let mut backoff = Duration::from_millis(100);
            loop {
                match state.kinesis_client.put_record(put_input.clone()).await {
                    Ok(_output) => {
                        println!();
                        break;
                    }
                    Err(RusotoError::Service(PutRecordError::ResourceNotFound(err))) => {
                        if total_backoff == Duration::from_millis(0) {
                            print!("unable to write to kinesis stream; retrying {:?}", backoff);
                        } else if total_backoff < DEFAULT_KINESIS_TIMEOUT {
                            backoff *= 2;
                            print!(" {:?}", backoff);
                            io::stdout().flush().unwrap();
                        } else {
                            println!();
                            return Err(err);
                        }
                    }
                    Err(err) => {
                        return Err(format!("unable to put Kinesis record: {}", err.to_string()))
                    }
                }
                time::delay_for(backoff).await;
                total_backoff += backoff;
            }
        }
        Ok(())
    }
}
