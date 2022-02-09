// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::time::Duration;

use anyhow::{bail, Context};
use async_trait::async_trait;
use rdkafka::admin::NewPartitions;
use rdkafka::producer::Producer;

use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct AddPartitionsAction {
    topic_prefix: String,
    partitions: usize,
}

pub fn build_add_partitions(mut cmd: BuiltinCommand) -> Result<AddPartitionsAction, anyhow::Error> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partitions = cmd.args.opt_parse("total-partitions")?.unwrap_or(1);
    cmd.args.done()?;

    Ok(AddPartitionsAction {
        topic_prefix,
        partitions,
    })
}

#[async_trait]
impl Action for AddPartitionsAction {
    async fn undo(&self, _: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), anyhow::Error> {
        let topic_name = format!("{}-{}", self.topic_prefix, state.seed);
        println!(
            "Raising partition count of Kafka topic {} to {}",
            topic_name, self.partitions
        );

        match state.kafka_topics.get(&topic_name) {
            Some(partitions) => {
                if self.partitions <= *partitions {
                    bail!(
                        "new partition count {} is not greater than current partition count {}",
                        self.partitions,
                        partitions
                    );
                }
            }
            None => {
                bail!(
                    "topic {} not created by kafka-create-topic",
                    topic_name.quoted(),
                )
            }
        }

        let partitions = NewPartitions::new(&topic_name, self.partitions);
        let res = state
            .kafka_admin
            .create_partitions(&[partitions], &state.kafka_admin_opts)
            .await
            .context("creating partitions")?;
        if res.len() != 1 {
            bail!(
                "kafka partition addition returned {} results, but exactly one result was expected",
                res.len()
            );
        }
        if let Err((_topic_name, e)) = res.into_element() {
            return Err(e.into());
        }

        Retry::default()
            .max_duration(state.default_timeout)
            .retry_async(|_| async {
                let metadata = state.kafka_producer.client().fetch_metadata(
                    Some(&topic_name),
                    Some(cmp::max(state.default_timeout, Duration::from_secs(1))),
                )?;
                if metadata.topics().len() != 1 {
                    bail!("metadata fetch returned no topics");
                }
                let topic = metadata.topics().into_element();
                if topic.partitions().len() != self.partitions {
                    bail!(
                        "topic {} has {} partitions when exactly {} was expected",
                        topic_name,
                        topic.partitions().len(),
                        self.partitions,
                    );
                }
                Ok(())
            })
            .await?;

        state.kafka_topics.insert(topic_name, self.partitions);
        Ok(())
    }
}
