// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str;
use std::time::Duration;

use anyhow::{bail, Context};
use rdkafka::admin::AdminClient;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

enum Topic {
    FromSink(String),
    Named(String),
}

async fn get_topic(
    sink: &str,
    topic_field: &str,
    state: &mut State,
) -> Result<String, anyhow::Error> {
    let query = format!(
        "SELECT {} FROM mz_sinks JOIN mz_kafka_sinks \
        ON mz_sinks.id = mz_kafka_sinks.id \
        JOIN mz_schemas s ON s.id = mz_sinks.schema_id \
        LEFT JOIN mz_databases d ON d.id = s.database_id \
        WHERE d.name = $1 \
        AND s.name = $2 \
        AND mz_sinks.name = $3",
        topic_field
    );
    let sink_fields: Vec<&str> = sink.split('.').collect();
    let result = state
        .pgclient
        .query_one(
            query.as_str(),
            &[&sink_fields[0], &sink_fields[1], &sink_fields[2]],
        )
        .await
        .context("retrieving topic name")?
        .get(topic_field);
    Ok(result)
}

pub async fn run_verify_topic(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let source = match (cmd.args.opt_string("sink"), cmd.args.opt_string("topic")) {
        (Some(sink), None) => Topic::FromSink(sink),
        (None, Some(topic)) => Topic::Named(topic),
        (Some(_), Some(_)) => {
            bail!("Can't provide both `source` and `topic` to kafka-verify-topic")
        }
        (None, None) => bail!("kafka-verify-topic expects either `source` or `topic`"),
    };

    cmd.args.done()?;

    let topic: String = match &source {
        Topic::FromSink(sink) => get_topic(sink, "topic", state).await?,
        Topic::Named(name) => name.clone(),
    };

    println!("Verifying Kafka topic {}", topic);

    let mut config = state.kafka_config.clone();
    config.set("enable.auto.offset.store", "false");

    let client: AdminClient<_> = config.create().context("creating kafka consumer")?;

    let start = std::time::Instant::now();
    let mut topic_created = false;

    println!("waiting for Kafka topic creation...");

    while !topic_created && start.elapsed() < state.default_timeout {
        let meta = client
            .inner()
            .fetch_metadata(None, Duration::from_secs(1))?;

        topic_created = meta.topics().iter().find(|t| t.name() == topic).is_some()
    }

    if topic_created {
        Ok(ControlFlow::Continue)
    } else {
        bail!("deadline elapsed")
    }
}
