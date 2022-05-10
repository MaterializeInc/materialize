// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashSet, VecDeque};
use std::str;
use std::time::Instant;

use anyhow::{bail, Context};
use async_trait::async_trait;
use aws_sdk_kinesis::Client as KinesisClient;
use itertools::Itertools;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct VerifyAction {
    stream_prefix: String,
    expected_records: HashSet<String>,
}

pub fn build_verify(mut cmd: BuiltinCommand) -> Result<VerifyAction, anyhow::Error> {
    let stream_prefix = cmd.args.string("stream")?;
    cmd.args.done()?;
    Ok(VerifyAction {
        stream_prefix,
        expected_records: cmd.input.into_iter().collect(),
    })
}

#[async_trait]
impl Action for VerifyAction {
    async fn undo(&self, _state: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let stream_name = format!("testdrive-{}-{}", self.stream_prefix, state.seed);

        let mut shard_iterators = get_shard_iterators(&state.kinesis_client, &stream_name).await?;
        let timer = Instant::now();
        let mut records: HashSet<String> = HashSet::new();
        while let Some(iterator) = shard_iterators.pop_front() {
            if let Some(iterator) = &iterator {
                let output = state
                    .kinesis_client
                    .get_records()
                    .shard_iterator(iterator)
                    .send()
                    .await
                    .context("getting Kinesis records")?;
                for record in output.records.unwrap() {
                    records.insert(
                        String::from_utf8(record.data.unwrap().into_inner())
                            .context("converting Kinesis record bytes to string")?,
                    );
                }
                match output.millis_behind_latest {
                    // Test hack!
                    // Assume all records have already been written to the stream. Once you've
                    // caught up, you're done with that shard.
                    // NOTE: this is not true for real Kinesis streams as data could still be
                    // arriving.
                    Some(0) => (),
                    _ => shard_iterators.push_back(output.next_shard_iterator),
                };
                if timer.elapsed() > state.default_timeout {
                    // Unable to read all Kinesis records in the default
                    // time allotted -- fail.
                    bail!("timeout reading from Kinesis stream: {}", stream_name);
                }
            }
        }

        // For now, we don't guarantee any type of ordering!
        if records != self.expected_records {
            let missing_records = &self.expected_records - &records;
            let extra_records = &records - &self.expected_records;
            bail!(
                "kinesis records did not match:\nmissing:\n{}\nextra:\n{}",
                missing_records.iter().join("\n"),
                extra_records.iter().join("\n")
            );
        }

        Ok(ControlFlow::Continue)
    }
}

async fn get_shard_iterators(
    kinesis_client: &KinesisClient,
    stream_name: &str,
) -> Result<VecDeque<Option<String>>, anyhow::Error> {
    let mut iterators: VecDeque<Option<String>> = VecDeque::new();
    for shard_id in mz_kinesis_util::get_shard_ids(kinesis_client, stream_name)
        .await
        .context("listing Kinesis shards")?
    {
        iterators.push_back(
            mz_kinesis_util::get_shard_iterator(kinesis_client, stream_name, &shard_id)
                .await
                .context("getting Kinesis shard iterator")?,
        );
    }

    Ok(iterators)
}
