// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::{bail, Context};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use mz_ore::retry::Retry;
use mz_ore::str::StrExt;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_verify_commit(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let consumer_group_id = cmd.args.string("consumer-group-id")?;
    let topic = cmd.args.string("topic")?;
    let partition = cmd.args.parse("partition")?;
    cmd.args.done()?;

    let topic = format!("testdrive-{}-{}", topic, state.seed);
    let expected_offset = match &cmd.input[..] {
        [line] => Offset::Offset(line.parse().context("parsing expected offset")?),
        _ => bail!("kafka-verify-commit requires a single expected offset as input"),
    };

    println!(
        "Verifying committed Kafka offset for topic {} and consumer group {}...",
        topic.quoted(),
        consumer_group_id.quoted(),
    );

    let mut config = state.kafka_config.clone();
    config.set("group.id", &consumer_group_id);
    Retry::default()
        .max_duration(state.default_timeout)
        .retry_async_canceling(|_| async {
            let config = config.clone();
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(&topic, partition);
            let committed_tpl = mz_ore::task::spawn_blocking(
                || "kakfa_committed_offsets".to_string(),
                move || {
                    let consumer: StreamConsumer =
                        config.create().context("creating kafka consumer")?;

                    Ok::<_, anyhow::Error>(
                        consumer.committed_offsets(tpl, Duration::from_secs(10))?,
                    )
                },
            )
            .await
            .unwrap()?;

            let found_offset = committed_tpl.elements_for_topic(&topic)[0].offset();
            if found_offset != expected_offset {
                bail!(
                    "found committed offset `{:?}` does not match expected offset `{:?}`",
                    found_offset,
                    expected_offset
                );
            }
            Ok(())
        })
        .await?;
    Ok(ControlFlow::Continue)
}
