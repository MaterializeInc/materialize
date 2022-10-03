// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Context};
use aws_sdk_kinesis::types::{Blob, SdkError};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

use mz_ore::retry::Retry;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_ingest(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let stream_prefix = format!("testdrive-{}", cmd.args.string("stream")?);
    match cmd.args.string("format")?.as_str() {
        "bytes" => (),
        f => bail!("unsupported message format for Kinesis: {}", f),
    }
    cmd.args.done()?;

    let stream_name = format!("{}-{}", stream_prefix, state.seed);

    for row in cmd.input {
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
