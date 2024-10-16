// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::str;
use std::time::Duration;

use anyhow::{bail, Context};
use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

enum Topic {
    FromSink(String),
    Named(String),
}

async fn get_topic(sink: &str, topic_field: &str, state: &State) -> Result<String, anyhow::Error> {
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
        .materialize
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
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let source = match (cmd.args.opt_string("sink"), cmd.args.opt_string("topic")) {
        (Some(sink), None) => Topic::FromSink(sink),
        (None, Some(topic)) => Topic::Named(topic),
        (Some(_), Some(_)) => {
            bail!("Can't provide both `source` and `topic` to kafka-verify-topic")
        }
        (None, None) => bail!("kafka-verify-topic expects either `source` or `topic`"),
    };

    let topic: String = match &source {
        Topic::FromSink(sink) => get_topic(sink, "topic", state).await?,
        Topic::Named(name) => name.clone(),
    };

    let await_value_schema = cmd.args.opt_bool("await-value-schema")?.unwrap_or(false);
    let await_key_schema = cmd.args.opt_bool("await-key-schema")?.unwrap_or(false);

    let topic_config: Option<serde_json::Value> = cmd.args.opt_parse("topic-config")?;
    let partition_count: Option<usize> = cmd.args.opt_parse("partition-count")?;
    let replication_factor: Option<usize> = cmd.args.opt_parse("replication-factor")?;

    cmd.args.done()?;

    println!("Verifying Kafka topic {}", topic);

    let mut config = state.kafka_config.clone();
    config.set("enable.auto.offset.store", "false");

    let client: AdminClient<_> = config.create().context("creating kafka consumer")?;

    println!("waiting to create Kafka topic...");

    Retry::default()
        .max_duration(state.default_timeout)
        .retry_async(|_state| async {
            let meta = client
                .inner()
                .fetch_metadata(None, Duration::from_secs(1))?;

            let topic = meta
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .ok_or(anyhow::anyhow!("topic not found"))?;

            if let Some(partitions) = partition_count {
                if topic.partitions().len() != partitions {
                    bail!(
                        "expected {} partitions but found {}",
                        partitions,
                        topic.partitions().len()
                    );
                }
            }

            if let Some(replication_factor) = replication_factor {
                for partition in topic.partitions() {
                    if partition.replicas().len() != replication_factor {
                        bail!(
                            "expected replication factor {} but found {}",
                            replication_factor,
                            partition.replicas().len()
                        );
                    }
                }
            }
            Ok(())
        })
        .await?;

    if let Some(topic_config) = topic_config {
        println!("verifying topic configuration...");
        Retry::default()
            .max_duration(state.default_timeout)
            .retry_async(|_state| async {
                let config = client
                    .describe_configs(
                        &[ResourceSpecifier::Topic(&topic)],
                        &AdminOptions::new().request_timeout(Some(Duration::from_secs(2))),
                    )
                    .await?
                    .into_element()?;
                let configs = config
                    .entries
                    .into_iter()
                    .map(|e| (e.name, e.value))
                    .collect::<BTreeMap<_, _>>();

                for (key, value) in topic_config.as_object().expect("json object") {
                    let value = match value {
                        serde_json::Value::String(s) => s.as_str(),
                        _ => bail!("expected string value for key {}", key),
                    };
                    if let Some(Some(actual)) = configs.get(key) {
                        if actual != value {
                            bail!("expected {}={} but found {}", key, value, actual);
                        }
                    } else {
                        bail!("expected {}={} but not found", key, value);
                    }
                }

                Ok(())
            })
            .await?;
    }

    let mut await_schemas = vec![];
    if await_value_schema {
        await_schemas.push(format!("{topic}-value"));
    }
    if await_key_schema {
        await_schemas.push(format!("{topic}-key"));
    }

    for schema_subject in await_schemas {
        println!("waiting for schema subject {}...", schema_subject);
        Retry::default()
            .max_duration(state.default_timeout)
            .retry_async(|_state| async {
                state
                    .ccsr_client
                    .list_subjects()
                    .await?
                    .iter()
                    .find(|subject| subject == &&schema_subject)
                    .ok_or(anyhow::anyhow!("schema not found"))
                    .map(|_| ())
            })
            .await?;
    }

    Ok(ControlFlow::Continue)
}
