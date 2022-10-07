// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{anyhow, Context};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::topic_partition_list::Offset;

use mz_ore::retry::Retry;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

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

async fn get_id(table: &str, name: &str, state: &mut State) -> Result<String, anyhow::Error> {
    let query = format!("select id from {table} where name = $1");
    let result = state
        .pgclient
        .query_one(query.as_str(), &[&name])
        .await
        .context("retrieving source id")?
        .get(0);
    Ok(result)
}

pub async fn run_verify_commit(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let source = cmd.args.string("source")?;
    let conn = cmd.args.string("conn")?;
    let topic = cmd.args.string("topic")?;
    let partition = cmd.args.parse("partition")?;
    let expected_offset = Offset::Offset(cmd.input[0].parse()?);

    let env_id = get_env_id(state).await?;
    let source_id = get_id("mz_sources", &source, state).await?;
    let conn_id = get_id("mz_connections", &conn, state).await?;
    let topic = format!("testdrive-{}-{}", topic, state.seed);

    let mut config = state.kafka_config.clone();
    config.set(
        "group.id",
        format!("materialize-{}-{}-{}", env_id, conn_id, source_id),
    );
    println!(
        "Verifying committed kafka offset for topic ({}) and consumer group ({})",
        topic,
        config.get("group.id").unwrap()
    );
    Retry::default()
        .max_duration(state.default_timeout)
        .retry_async_canceling(|_| async {
            let config = config.clone();
            let mut tpl = rdkafka::topic_partition_list::TopicPartitionList::new();
            tpl.add_partition(&topic, partition);
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
            if found_offset != expected_offset {
                Err(anyhow!(
                    "Found committed offset `{:?}` does not match expected offset `{:?}`",
                    found_offset,
                    expected_offset
                ))
            } else {
                Ok(())
            }
        })
        .await?;
    Ok(ControlFlow::Continue)
}
