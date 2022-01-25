// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::str;
use std::time::Duration;

use anyhow::{bail, Context as _};
use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::pin;
use tokio_stream::StreamExt;

use crate::action::{Action, Context, State};
use crate::format::{avro, json};
use crate::parser::BuiltinCommand;

pub enum SinkFormat {
    Avro,
    Json { key: bool },
}

pub enum SinkConsistencyFormat {
    Debezium,
}

pub struct VerifyAction {
    sink: String,
    format: SinkFormat,
    consistency: Option<SinkConsistencyFormat>,
    sort_messages: bool,
    expected_messages: Vec<String>,
    context: Context,
    partial_search: Option<usize>,
}

pub fn build_verify(
    mut cmd: BuiltinCommand,
    context: Context,
) -> Result<VerifyAction, anyhow::Error> {
    let format = match cmd.args.string("format")?.as_str() {
        "avro" => SinkFormat::Avro,
        "json" => SinkFormat::Json {
            key: cmd.args.parse("key")?,
        },
        f => bail!("unknown format: {}", f),
    };
    let sink = cmd.args.string("sink")?;
    let consistency = match cmd.args.opt_string("consistency").as_deref() {
        Some("debezium") => Some(SinkConsistencyFormat::Debezium),
        Some(s) => bail!("unknown sink consistency format {}", s),
        None => None,
    };

    let sort_messages = cmd.args.opt_bool("sort-messages")?.unwrap_or(false);
    let expected_messages = cmd.input;
    if expected_messages.len() == 0 {
        // verify with 0 messages doesn't check that no messages have been written -
        // it 'verifies' 0 messages and trivially returns true
        bail!("kafka-verify requires a non-empty list of expected messages");
    }
    let partial_search = cmd.args.opt_parse("partial-search")?;
    cmd.args.done()?;
    Ok(VerifyAction {
        sink,
        format,
        consistency,
        sort_messages,
        expected_messages,
        context,
        partial_search,
    })
}

fn avro_from_bytes(
    schema: &mz_avro::Schema,
    mut bytes: &[u8],
) -> Result<mz_avro::types::Value, anyhow::Error> {
    if bytes.len() < 5 {
        bail!(
            "avro datum is too few bytes: expected at least 5 bytes, got {}",
            bytes.len()
        );
    }
    let magic = bytes[0];
    let _schema_id = BigEndian::read_i32(&bytes[1..5]);
    bytes = &bytes[5..];

    if magic != 0 {
        bail!(
            "wrong avro serialization magic: expected 0, got {}",
            bytes[0]
        );
    }

    let datum = avro::from_avro_datum(schema, &mut bytes).context("decoding avro datum")?;
    Ok(datum)
}

async fn get_topic(
    sink: &str,
    topic_field: &str,
    state: &mut State,
) -> Result<String, anyhow::Error> {
    let query = format!("SELECT {} FROM mz_catalog_names JOIN mz_kafka_sinks ON global_id = sink_id WHERE name = $1", topic_field);
    let result = state
        .pgclient
        .query_one(query.as_str(), &[&sink])
        .await
        .context("retrieving topic name")?
        .get(topic_field);
    Ok(result)
}

#[async_trait]
impl Action for VerifyAction {
    async fn undo(&self, _state: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), anyhow::Error> {
        let topic: String = match self.consistency {
            None => get_topic(&self.sink, "topic", state).await?,
            Some(SinkConsistencyFormat::Debezium) => {
                get_topic(&self.sink, "consistency_topic", state).await?
            }
        };

        println!("Verifying results in Kafka topic {}", topic);

        let mut config = state.kafka_config.clone();
        config.set("enable.auto.offset.store", "false");

        let consumer: StreamConsumer = config.create().context("creating kafka consumer")?;
        consumer
            .subscribe(&[&topic])
            .context("subscribing to kafka topic")?;

        let (stream_size, stream_timeout) = match self.partial_search {
            Some(size) => (size, state.default_timeout),
            None => (self.expected_messages.len(), Duration::from_secs(15)),
        };

        let message_stream = consumer
            .stream()
            .take(stream_size)
            .timeout(cmp::max(state.default_timeout, stream_timeout));
        pin!(message_stream);

        // Collect all messages that arrive without timing out. If we trip
        // the timeout, suppress the error and return what we have. This
        // is nicer than returning "timeout expired", as the user will
        // instead get an error message about the expected messages that
        // were missing.
        let mut actual_bytes = vec![];
        loop {
            match message_stream.next().await {
                Some(Ok(message)) => {
                    let message = message?;
                    consumer
                        .store_offset_from_message(&message)
                        .context("storing message offset")?;
                    actual_bytes.push((
                        message.key().and_then(|bytes| Some(bytes.to_owned())),
                        message.payload().and_then(|bytes| Some(bytes.to_owned())),
                    ));
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

        match &self.format {
            SinkFormat::Avro => {
                let value_schema = state
                    .ccsr_client
                    .get_schema_by_subject(&format!("{}-value", topic))
                    .await
                    .context("fetching schema")?
                    .raw;

                let key_schema = state
                    .ccsr_client
                    .get_schema_by_subject(&format!("{}-key", topic))
                    .await
                    .ok()
                    .map(|key_schema| {
                        avro::parse_schema(&key_schema.raw).context("parsing avro schema")
                    })
                    .transpose()?;

                let value_schema =
                    avro::parse_schema(&value_schema).context("parsing avro schema")?;
                let value_schema = &value_schema;

                let mut actual_messages = vec![];
                for (key, value) in actual_bytes {
                    let key_datum = key_schema
                        .as_ref()
                        .map(|key_schema| {
                            let bytes = match key {
                                Some(key) => key,
                                None => bail!("empty message key"),
                            };
                            avro_from_bytes(key_schema, &bytes)
                        })
                        .transpose()?;
                    let value_datum = match value {
                        None => None,
                        Some(bytes) => Some(avro_from_bytes(value_schema, &bytes)?),
                    };
                    actual_messages.push((key_datum, value_datum));
                }

                if self.sort_messages {
                    actual_messages.sort_by_key(|k| format!("{:?}", k.1));
                }

                avro::validate_sink_with_partial_search(
                    key_schema.as_ref(),
                    value_schema,
                    &self.expected_messages,
                    &actual_messages,
                    &self.context.regex,
                    &self.context.regex_replacement,
                    self.partial_search.is_some(),
                )
            }
            SinkFormat::Json { key } => {
                assert!(
                    self.partial_search.is_none(),
                    "partial search not yet implemented for json formatted sinks"
                );
                let mut actual_messages = vec![];
                for (key, value) in actual_bytes {
                    let key_datum = match key {
                        None => None,
                        Some(bytes) => {
                            Some(serde_json::from_slice(&bytes).context("decoding json")?)
                        }
                    };
                    let value_datum = match value {
                        None => None,
                        Some(bytes) => {
                            Some(serde_json::from_slice(&bytes).context("decoding json")?)
                        }
                    };

                    actual_messages.push((key_datum, value_datum));
                }

                if self.sort_messages {
                    actual_messages.sort_by_key(|k| format!("{:?}", k.1));
                }

                json::validate_sink(
                    *key,
                    &self.expected_messages,
                    &actual_messages,
                    &self.context.regex,
                    &self.context.regex_replacement,
                )
            }
        }
    }
}

pub struct VerifySchemaAction {
    sink: String,
    format: SinkFormat,
    expected_key_schema: Option<String>,
    expected_value_schema: String,
}

pub fn build_verify_schema(mut cmd: BuiltinCommand) -> Result<VerifySchemaAction, anyhow::Error> {
    let format = match cmd.args.string("format")?.as_str() {
        "avro" => SinkFormat::Avro,
        "json" => SinkFormat::Json {
            key: cmd.args.parse("key")?,
        },
        f => bail!("unknown format: {}", f),
    };
    let sink = cmd.args.string("sink")?;

    let (key, value) = match &cmd.input[..] {
        [value] => (None, value.clone()),
        [key, value] => (Some(key.clone()), value.clone()),
        _ => bail!("unable to read key/value schema inputs"),
    };

    cmd.args.done()?;
    Ok(VerifySchemaAction {
        sink,
        format,
        expected_key_schema: key,
        expected_value_schema: value,
    })
}

#[async_trait]
impl Action for VerifySchemaAction {
    async fn undo(&self, _state: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), anyhow::Error> {
        let topic = get_topic(&self.sink, "topic", state).await?;

        match &self.format {
            SinkFormat::Avro => {
                let generated_value_schema = state
                    .ccsr_client
                    .get_schema_by_subject(&format!("{}-value", topic))
                    .await
                    .context("fetching schema")?
                    .raw;

                let generated_key_schema = state
                    .ccsr_client
                    .get_schema_by_subject(&format!("{}-key", topic))
                    .await
                    .ok()
                    .map(|key_schema| {
                        avro::parse_schema(&key_schema.raw).context("parsing avro schema")
                    })
                    .transpose()?;

                let generated_value_schema = avro::parse_schema(&generated_value_schema)
                    .context("parsing generated avro schema")?;
                let expected_value_schema = avro::parse_schema(&self.expected_value_schema)
                    .context("parsing expected avro schema")?;

                if expected_value_schema.ne(&generated_value_schema) {
                    bail!(
                        "value schema did not match\nexpected:\n{:?}\n\nactual:\n{:?}",
                        expected_value_schema,
                        generated_value_schema
                    );
                }

                if let Some(expected_key_schema) = &self.expected_key_schema {
                    let expected_key_schema = avro::parse_schema(expected_key_schema)
                        .context("parsing expected avro schema")?;

                    if generated_key_schema.is_none() {
                        bail!("empty generated key schema");
                    }

                    let generated_key_schema = generated_key_schema.unwrap();
                    if expected_key_schema.ne(&generated_key_schema) {
                        bail!(
                            "key schema did not match\nexpected:\n{:?}\n\nactual:\n{:?}",
                            expected_key_schema,
                            generated_key_schema
                        );
                    }
                }
            }
            _ => {
                bail!("kafka-verify-schema is only supported for Avro sinks")
            }
        }

        Ok(())
    }
}
