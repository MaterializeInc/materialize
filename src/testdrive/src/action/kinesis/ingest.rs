// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Context};
use async_trait::async_trait;
use aws_sdk_kinesis::types::{Blob, SdkError};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use mz_ore::retry::Retry;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct IngestAction {
    stream_prefix: String,
    rows: Vec<String>,
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, anyhow::Error> {
    let stream_prefix = format!("testdrive-{}", cmd.args.string("stream")?);
    match cmd.args.string("format")?.as_str() {
        "bytes" => (),
        f => bail!("unsupported message format for Kinesis: {}", f),
    }
    cmd.args.done()?;

    Ok(IngestAction {
        stream_prefix,
        rows: cmd.input,
    })
}

#[async_trait]
impl Action for IngestAction {
    async fn run(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let stream_name = format!("{}-{}", self.stream_prefix, state.seed);

        for row in &self.rows {
            // Generating and using random partition keys allows us to test
            // reading Kinesis records from a variable number of shards that
            // are distributed differently on every run.
            let random_partition_key: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect();

            // The Kinesis stream might not be immediately available,
            // be prepared to back off.
            Retry::default()
                .max_duration(state.default_timeout)
                .retry_async_canceling(|_| async {
                    match state
                        .kinesis_client
                        .put_record()
                        .data(Blob::new(row.as_bytes()))
                        .partition_key(&random_partition_key)
                        .stream_name(&stream_name)
                        .send()
                        .await
                    {
                        Ok(_output) => Ok(()),
                        Err(SdkError::ServiceError { err, .. })
                            if err.is_resource_not_found_exception() =>
                        {
                            bail!("resource not found: {}", err)
                        }
                        Err(err) => Err(err).context("putting Kinesis record"),
                    }
                })
                .await
                .context("putting Kinesis record")?;
        }
        Ok(ControlFlow::Continue)
    }
}
