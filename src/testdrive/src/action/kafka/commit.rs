// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::topic_partition_list::Offset;

use mz_ore::retry::Retry;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct VerifyCommitAction {
    source: String,
    // TODO(guswynn): can we get the topic from
    // a system table?
    topic: String,
    partition: i32,
    expected_offset: Offset,
}

pub fn build_verify_commit(mut cmd: BuiltinCommand) -> Result<VerifyCommitAction, anyhow::Error> {
    Ok(VerifyCommitAction {
        source: cmd.args.string("source")?,
        topic: cmd.args.string("topic")?,
        partition: cmd.args.parse("partition")?,
        expected_offset: Offset::Offset(cmd.input[0].parse()?),
    })
}

async fn get_env_id(state: &mut State) -> Result<String, anyhow::Error> {
    let query = "select mz_environment_id();".to_string();
    let result = state
        .pgclient
        .query_one(query.as_str(), &[])
        .await
        .context("retrieving env id")?
        .get(0);
    Ok(result)
}

async fn get_src_id(source: &str, state: &mut State) -> Result<String, anyhow::Error> {
    let query = "select id from mz_sources where name = $1".to_string();
    let result = state
        .pgclient
        .query_one(query.as_str(), &[&source])
        .await
        .context("retrieving source id")?
        .get(0);
    Ok(result)
}

#[async_trait]
impl Action for VerifyCommitAction {
    async fn undo(&self, _state: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let env_id = get_env_id(state).await?;
        let source_id = get_src_id(&self.source, state).await?;
        let topic = format!("testdrive-{}-{}", self.topic, state.seed);

        let mut config = state.kafka_config.clone();
        config.set(
            "group.id",
            format!("materialize-{}-kafka-{}", env_id, source_id),
        );
        Retry::default()
            .max_duration(state.default_timeout)
            .retry_async_canceling(|_| async {
                let config = config.clone();
                let mut tpl = rdkafka::topic_partition_list::TopicPartitionList::new();
                tpl.add_partition(&topic, self.partition);
                let committed_tpl = mz_ore::task::spawn_blocking(
                    || "kakfa_committed_offsets".to_string(),
                    move || {
                        let consumer: StreamConsumer =
                            config.create().context("creating kafka consumer")?;

                        Ok::<_, anyhow::Error>(
                            consumer.committed_offsets(tpl, std::time::Duration::from_secs(10))?,
                        )
                    },
                )
                .await
                .unwrap()?;

                let found_offset = committed_tpl.elements_for_topic(&topic)[0].offset();
                if found_offset != self.expected_offset {
                    Err(anyhow!(
                        "Found committed offset `{:?}` does not match expected offset `{:?}`",
                        found_offset,
                        self.expected_offset
                    ))
                } else {
                    Ok(())
                }
            })
            .await?;
        Ok(ControlFlow::Continue)
    }
}
