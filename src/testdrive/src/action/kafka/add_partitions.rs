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
use rdkafka::admin::NewPartitions;
use rdkafka::producer::Producer;

use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_add_partitions(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let total_partitions = cmd.args.opt_parse("total-partitions")?.unwrap_or(1);
    cmd.args.done()?;

    let topic_name = format!("{}-{}", topic_prefix, state.seed);
    println!(
        "Raising partition count of Kafka topic {} to {}",
        topic_name, total_partitions
    );

    match state.kafka_topics.get(&topic_name) {
        Some(current_partitions) => {
            if total_partitions <= *current_partitions {
                bail!(
                    "new partition count {} is not greater than current partition count {}",
                    total_partitions,
                    total_partitions
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

    let partitions = NewPartitions::new(&topic_name, total_partitions);
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
        .retry_async_canceling(|_| async {
            let metadata = state.kafka_producer.client().fetch_metadata(
                Some(&topic_name),
                Some(cmp::max(state.default_timeout, Duration::from_secs(1))),
            )?;
            if metadata.topics().len() != 1 {
                bail!("metadata fetch returned no topics");
            }
            let topic = metadata.topics().into_element();
            if topic.partitions().len() != total_partitions {
                bail!(
                    "topic {} has {} partitions when exactly {} was expected",
                    topic_name,
                    topic.partitions().len(),
                    total_partitions,
                );
            }
            Ok(())
        })
        .await?;

    state.kafka_topics.insert(topic_name, total_partitions);
    Ok(ControlFlow::Continue)
}
