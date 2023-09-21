// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str;

use anyhow::{bail, Context};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use tokio::pin;
use tokio_stream::StreamExt;

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

    let consumer: StreamConsumer = config.create().context("creating kafka consumer")?;
    consumer
        .subscribe(&[&topic])
        .context("subscribing to kafka topic")?;

    let message_stream = consumer.stream().timeout(state.default_timeout);
    pin!(message_stream);

    let start = std::time::Instant::now();
    let mut topic_created = false;

    while !topic_created {
        match message_stream.next().await {
            Some(Ok(message)) => {
                match message {
                    // We create topics after creating sinks, so we permit
                    // retries here while waiting for the topic to get created.
                    Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::UnknownTopicOrPartition,
                    )) if start.elapsed() < state.default_timeout && !topic_created => {
                        println!("waiting for Kafka topic creation...");
                        continue;
                    }
                    Ok(_) => topic_created = true,
                    Err(e) => Err(e)?,
                };
            }
            Some(Err(e)) => {
                println!("Received error from Kafka stream consumer: {}", e);
                break;
            }
            None => {
                break;
            }
        }
    }

    Ok(ControlFlow::Continue)
}
