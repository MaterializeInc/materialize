// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::time::Duration;
use std::{cmp, str};

use anyhow::{bail, ensure, Context};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{Headers, Message};
use rdkafka::types::RDKafkaErrorCode;
use regex::Regex;
use tokio::pin;
use tokio_stream::StreamExt;

use crate::action::{ControlFlow, State};
use crate::format::avro::{self, DebugValue};
use crate::parser::BuiltinCommand;

#[derive(Debug, Clone, Copy)]
enum Format {
    Avro,
    Json,
    Bytes,
    Text,
}

impl TryFrom<&str> for Format {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "avro" => Ok(Format::Avro),
            "json" => Ok(Format::Json),
            "bytes" => Ok(Format::Bytes),
            "text" => Ok(Format::Text),
            f => bail!("unknown format: {}", f),
        }
    }
}

#[derive(Debug)]
struct RecordFormat {
    key: Format,
    value: Format,
    requires_key: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
enum DecodedValue {
    Avro(DebugValue),
    Json(serde_json::Value),
    Bytes(Vec<u8>),
    Text(String),
}

enum Topic {
    FromSink(String),
    Named(String),
}

#[derive(Debug, Clone)]
struct Record<A> {
    headers: Vec<String>,
    key: Option<A>,
    value: Option<A>,
    partition: Option<i32>,
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

pub async fn run_verify_data(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let mut format = if let Some(format_str) = cmd.args.opt_string("format") {
        // If just a single format is provided, the user should specify `key=true` if they expect a
        // in each message.  However for format=avro, we will conveniently set this based
        // on the presence of the key schema in the registry, so this argument is not required.
        let requires_key: bool = cmd.args.opt_bool("key")?.unwrap_or(false);
        let format_type = format_str.as_str().try_into()?;
        RecordFormat {
            key: format_type,
            value: format_type,
            requires_key,
        }
    } else {
        let key_format = cmd.args.string("key-format")?.as_str().try_into()?;
        let value_format = cmd.args.string("value-format")?.as_str().try_into()?;
        RecordFormat {
            key: key_format,
            value: value_format,
            requires_key: true,
        }
    };

    let source = match (cmd.args.opt_string("sink"), cmd.args.opt_string("topic")) {
        (Some(sink), None) => Topic::FromSink(sink),
        (None, Some(topic)) => Topic::Named(topic),
        (Some(_), Some(_)) => bail!("Can't provide both `source` and `topic` to kafka-verify-data"),
        (None, None) => bail!("kafka-verify-data expects either `source` or `topic`"),
    };

    let sort_messages = cmd.args.opt_bool("sort-messages")?.unwrap_or(false);

    let header_keys: Vec<_> = cmd
        .args
        .opt_string("headers")
        .map(|s| s.split(',').map(str::to_owned).collect())
        .unwrap_or_default();

    let expected_messages = cmd.input;
    if expected_messages.len() == 0 {
        // verify with 0 messages doesn't check that no messages have been written -
        // it 'verifies' 0 messages and trivially returns true
        bail!("kafka-verify-data requires a non-empty list of expected messages");
    }
    let partial_search = cmd.args.opt_parse("partial-search")?;
    let debug_print_only = cmd.args.opt_bool("debug-print-only")?.unwrap_or(false);
    cmd.args.done()?;

    let topic: String = match &source {
        Topic::FromSink(sink) => get_topic(sink, "topic", state).await?,
        Topic::Named(name) => name.clone(),
    };

    println!("Verifying results in Kafka topic {}", topic);

    let mut config = state.kafka_config.clone();
    config.set("enable.auto.offset.store", "false");

    let consumer: StreamConsumer = config.create().context("creating kafka consumer")?;
    consumer
        .subscribe(&[&topic])
        .context("subscribing to kafka topic")?;

    let (mut stream_messages_remaining, stream_timeout) = match partial_search {
        Some(size) => (size, state.default_timeout),
        None => (expected_messages.len(), Duration::from_secs(15)),
    };

    let timeout = cmp::max(state.default_timeout, stream_timeout);

    let message_stream = consumer.stream().timeout(timeout);
    pin!(message_stream);

    // Collect all messages that arrive without timing out. If we trip
    // the timeout, suppress the error and return what we have. This
    // is nicer than returning "timeout expired", as the user will
    // instead get an error message about the expected messages that
    // were missing.
    let mut actual_bytes = vec![];

    let start = std::time::Instant::now();
    let mut topic_created = false;

    while stream_messages_remaining > 0 {
        match message_stream.next().await {
            Some(Ok(message)) => {
                let message = match message {
                    // We create topics after creating sinks, so we permit
                    // retries here while waiting for the topic to get created.
                    Err(KafkaError::MessageConsumption(
                        RDKafkaErrorCode::UnknownTopicOrPartition,
                    )) if start.elapsed() < timeout && !topic_created => {
                        println!("waiting for Kafka topic creation...");
                        continue;
                    }
                    e => e?,
                };

                stream_messages_remaining -= 1;
                topic_created = true;

                consumer
                    .store_offset_from_message(&message)
                    .context("storing message offset")?;

                let mut headers = vec![];
                for header_key in &header_keys {
                    // Expect a unique header with the given key and a UTF8-formatted body.
                    let hs = message.headers().context("expected headers for message")?;
                    let mut hs = hs.iter().filter(|i| i.key == header_key);
                    let h = hs.next();
                    if hs.next().is_some() {
                        bail!("expected at most one header with key {header_key}");
                    }
                    match h {
                        None => headers.push("<missing>".into()),
                        Some(h) => {
                            let value = str::from_utf8(h.value.unwrap_or(b"<null>"))?;
                            headers.push(value.into());
                        }
                    }
                }

                actual_bytes.push(Record {
                    headers,
                    key: message.key().map(|b| b.to_owned()),
                    value: message.payload().map(|b| b.to_owned()),
                    partition: Some(message.partition()),
                });
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

    let key_schema = if let Format::Avro = format.key {
        let schema = state
            .ccsr_client
            .get_schema_by_subject(&format!("{}-key", topic))
            .await
            .ok()
            .map(|key_schema| avro::parse_schema(&key_schema.raw).context("parsing avro schema"))
            .transpose()?;
        // for avro, we can determine if a key is required based on the presence of the key schema
        // rather than requiring the user to specify the key=true flag
        if schema.is_some() {
            format.requires_key = true;
        }
        schema
    } else {
        None
    };
    let value_schema = if let Format::Avro = format.value {
        let val_schema = state
            .ccsr_client
            .get_schema_by_subject(&format!("{}-value", topic))
            .await
            .context("fetching schema")?
            .raw;
        Some(avro::parse_schema(&val_schema).context("parsing avro schema")?)
    } else {
        None
    };

    let mut actual_messages = decode_messages(actual_bytes, &key_schema, &value_schema, &format)?;

    if sort_messages {
        actual_messages.sort_by_key(|r| format!("{:?}", r));
    }

    if debug_print_only {
        bail!(
            "records in sink:\n{}",
            actual_messages
                .into_iter()
                .map(|a| format!("{:#?}", a))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }

    let expected = parse_expected_messages(
        expected_messages,
        key_schema,
        value_schema,
        &format,
        &header_keys,
    )?;

    verify_with_partial_search(
        &expected,
        &actual_messages,
        &state.regex,
        &state.regex_replacement,
        partial_search.is_some(),
    )?;

    Ok(ControlFlow::Continue)
}

/// Expect and split out `n` whitespace-delimited headers before the main contents of the 'expect' row.
fn split_headers(input: &str, n_headers: usize) -> anyhow::Result<(Vec<String>, &str)> {
    let whitespace = Regex::new("\\s+").expect("building known-valid regex");
    let mut parts = whitespace.splitn(input, n_headers + 1);
    let mut headers = Vec::with_capacity(n_headers);
    for _ in 0..n_headers {
        headers.push(
            parts
                .next()
                .context("expected another header in the input")?
                .to_string(),
        )
    }
    let rest = parts
        .next()
        .context("expected some contents after any message headers")?;

    ensure!(
        parts.next().is_none(),
        "more than n+1 elements from a call to splitn(_, n+1)"
    );

    Ok((headers, rest))
}

fn decode_messages(
    actual_bytes: Vec<Record<Vec<u8>>>,
    key_schema: &Option<mz_avro::Schema>,
    value_schema: &Option<mz_avro::Schema>,
    format: &RecordFormat,
) -> Result<Vec<Record<DecodedValue>>, anyhow::Error> {
    let mut actual_messages = vec![];

    for record in actual_bytes {
        let Record { key, value, .. } = record;
        let key = if format.requires_key {
            match (key, format.key) {
                (Some(bytes), Format::Avro) => Some(DecodedValue::Avro(DebugValue(
                    avro::from_confluent_bytes(key_schema.as_ref().unwrap(), &bytes)?,
                ))),
                (Some(bytes), Format::Json) => Some(DecodedValue::Json(
                    serde_json::from_slice(&bytes).context("decoding json")?,
                )),
                (Some(bytes), Format::Bytes) => Some(DecodedValue::Bytes(bytes)),
                (Some(bytes), Format::Text) => Some(DecodedValue::Text(String::from_utf8(bytes)?)),
                (None, _) if format.requires_key => bail!("empty message key"),
                (None, _) => None,
            }
        } else {
            None
        };

        let value = match (value, format.value) {
            (Some(bytes), Format::Avro) => Some(DecodedValue::Avro(DebugValue(
                avro::from_confluent_bytes(value_schema.as_ref().unwrap(), &bytes)?,
            ))),
            (Some(bytes), Format::Json) => Some(DecodedValue::Json(
                serde_json::from_slice(&bytes).context("decoding json")?,
            )),
            (Some(bytes), Format::Bytes) => Some(DecodedValue::Bytes(bytes)),
            (Some(bytes), Format::Text) => Some(DecodedValue::Text(String::from_utf8(bytes)?)),
            (None, _) => None,
        };

        actual_messages.push(Record {
            headers: record.headers.clone(),
            key,
            value,
            partition: record.partition,
        });
    }

    Ok(actual_messages)
}

fn parse_expected_messages(
    expected_messages: Vec<String>,
    key_schema: Option<mz_avro::Schema>,
    value_schema: Option<mz_avro::Schema>,
    format: &RecordFormat,
    header_keys: &[String],
) -> Result<Vec<Record<DecodedValue>>, anyhow::Error> {
    let mut expected = vec![];

    for msg in expected_messages {
        let (headers, content) = split_headers(&msg, header_keys.len())?;
        let mut content = content.as_bytes();
        let mut deserializer = serde_json::Deserializer::from_reader(&mut content).into_iter();

        let key = if format.requires_key {
            let key: serde_json::Value = deserializer
                .next()
                .context("key missing in input line")?
                .context("parsing json")?;

            Some(match format.key {
                Format::Avro => DecodedValue::Avro(DebugValue(avro::from_json(
                    &key,
                    key_schema.as_ref().unwrap().top_node(),
                )?)),
                Format::Json => DecodedValue::Json(key),
                Format::Bytes => {
                    unimplemented!("bytes format not yet supported in tests")
                }
                Format::Text => DecodedValue::Text(
                    key.as_str()
                        .map(|s| s.to_string())
                        .unwrap_or_else(|| key.to_string()),
                ),
            })
        } else {
            None
        };

        let value = match deserializer.next().transpose().context("parsing json")? {
            None => None,
            Some(value) if value.as_str() == Some("<null>") => None,
            Some(value) => match format.value {
                Format::Avro => Some(DecodedValue::Avro(DebugValue(avro::from_json(
                    &value,
                    value_schema.as_ref().unwrap().top_node(),
                )?))),
                Format::Json => Some(DecodedValue::Json(value)),
                Format::Bytes => {
                    unimplemented!("bytes format not yet supported in tests")
                }
                Format::Text => Some(DecodedValue::Text(value.to_string())),
            },
        };

        let content =
            str::from_utf8(content).context("internal error: contents were previously a string")?;
        let partition = match content.trim().split_once("=") {
            None if content.trim() != "" => bail!("unexpected cruft at end of line: {content}"),
            None => None,
            Some((label, partition)) => {
                if label != "partition" {
                    bail!("partition expectation has unexpected label: {label}")
                }
                Some(partition.parse().context("parsing expected partition")?)
            }
        };

        expected.push(Record {
            headers,
            key,
            value,
            partition,
        });
    }

    Ok(expected)
}

fn verify_with_partial_search<A>(
    expected: &[Record<A>],
    actual: &[Record<A>],
    regex: &Option<Regex>,
    regex_replacement: &String,
    partial_search: bool,
) -> Result<(), anyhow::Error>
where
    A: Debug + Clone,
{
    let mut expected = expected.iter();
    let mut actual = actual.iter();
    let mut index = 0..;

    let mut found_beginning = !partial_search;
    let mut expected_item = expected.next();
    let mut actual_item = actual.next();
    loop {
        let i = index.next().expect("known to exist");
        match (expected_item, actual_item) {
            (Some(e), Some(a)) => {
                let mut a = a.clone();
                if e.partition.is_none() {
                    a.partition = None;
                }
                let e_str = format!("{:#?}", e);
                let a_str = match &regex {
                    Some(regex) => regex
                        .replace_all(&format!("{:#?}", a).to_string(), regex_replacement.as_str())
                        .to_string(),
                    _ => format!("{:#?}", a),
                };

                if e_str != a_str {
                    if found_beginning {
                        bail!(
                            "record {} did not match\nexpected:\n{}\n\nactual:\n{}",
                            i,
                            e_str,
                            a_str,
                        );
                    }
                    actual_item = actual.next();
                } else {
                    found_beginning = true;
                    expected_item = expected.next();
                    actual_item = actual.next();
                }
            }
            (Some(e), None) => bail!("missing record {}: {:#?}", i, e),
            (None, Some(a)) => {
                if !partial_search {
                    bail!("extra record {}: {:#?}", i, a);
                }
                break;
            }
            (None, None) => break,
        }
    }
    let expected: Vec<_> = expected.map(|e| format!("{:#?}", e)).collect();
    let actual: Vec<_> = actual.map(|a| format!("{:#?}", a)).collect();

    if !expected.is_empty() {
        bail!("missing records:\n{}", expected.join("\n"))
    } else if !actual.is_empty() && !partial_search {
        bail!("extra records:\n{}", actual.join("\n"))
    } else {
        Ok(())
    }
}
