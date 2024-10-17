// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_ore::retry::Retry;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_wait_topic(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let topic = cmd.args.string("topic")?;

    println!("Waiting for Kafka topic {} to exist", topic);
    Retry::default()
        .initial_backoff(Duration::from_millis(50))
        .factor(1.5)
        .max_duration(state.timeout)
        .retry_async_canceling(|_| async { check_topic_exists(&topic, &*state).await })
        .await?;

    Ok(ControlFlow::Continue)
}

pub(crate) async fn check_topic_exists(topic: &str, state: &State) -> Result<(), anyhow::Error> {
    let metadata = state
        .kafka_admin
        .inner()
        // N.B. It is extremely important not to ask specifically
        // about the topic here, even though the API supports it!
        // Asking about the topic will create it automatically...
        // with the wrong number of partitions. Yes, this is
        // unbelievably horrible.
        .fetch_metadata(None, Some(Duration::from_secs(10)))?;

    let topic_exists = metadata.topics().iter().any(|t| t.name() == topic);
    if !topic_exists {
        Err(anyhow::anyhow!("topic {} doesn't exist", topic))
    } else {
        Ok(())
    }
}
