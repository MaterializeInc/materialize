// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_delete_topic(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    cmd.args.done()?;

    let topic_name = format!("{}-{}", topic_prefix, state.seed);

    println!("Deleting Kafka topic {topic_name}");

    mz_kafka_util::admin::delete_topic(&state.kafka_admin, &state.kafka_admin_opts, &topic_name)
        .await?;
    state.kafka_topics.remove(&topic_name);
    Ok(ControlFlow::Continue)
}
