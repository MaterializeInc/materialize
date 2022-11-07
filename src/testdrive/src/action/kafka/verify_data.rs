// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::fmt::Debug;
use std::str;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context};
use itertools::Itertools;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use regex::Regex;
use tokio::pin;
use tokio_stream::StreamExt;

use crate::action::{ControlFlow, State};
use crate::format::avro::{self, DebugValue};
use crate::format::json;
use crate::parser::BuiltinCommand;

enum Format {
    Avro,
    Json { key: bool },
}

enum Topic {
    FromSink(String),
    Named(String),
}

#[derive(Debug)]
struct Record<A> {
    headers: Vec<String>,
    key: Option<A>,
    value: Option<A>,
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

pub async fn run_verify_data(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let format = match cmd.args.string("format")?.as_str() {
        "avro" => Format::Avro,
        "json" => Format::Json {
            key: cmd.args.parse("key")?,
        },
        f => bail!("unknown format: {}", f),
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

    let (stream_size, stream_timeout) = match partial_search {
        Some(size) => (size, state.default_timeout),
        None => (expected_messages.len(), Duration::from_secs(15)),
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
                let headers = header_keys
                    .iter()
                    .map(|k| {
                        // Expect a unique header with the given key and a UTF8-formatted body.
                        let headers = message.headers().context("expected headers for message")?;
                        let header = headers
                            .iter()
                            .filter(|i| i.key == k)
                            .exactly_one()
                            .map_err(|_| {
                                anyhow!("expected exactly one header with the given key")
                            })?;
                        let value =
                            str::from_utf8(header.value.context("expected value for header")?)?;
                        Ok(value.to_owned())
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                actual_bytes.push(Record {
                    headers,
                    key: message.key().map(|b| b.to_owned()),
                    value: message.payload().map(|b| b.to_owned()),
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

    match &format {
        Format::Avro => {
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

            let value_schema = &avro::parse_schema(&value_schema).context("parsing avro schema")?;

            let mut actual_messages = vec![];
            for record in actual_bytes {
                let key = key_schema
                    .as_ref()
                    .map(|key_schema| {
                        let bytes = match record.key {
                            Some(key) => key,
                            None => bail!("empty message key"),
                        };
                        avro::from_confluent_bytes(key_schema, &bytes)
                    })
                    .transpose()?
                    .map(DebugValue);
                let value = match record.value {
                    None => None,
                    Some(bytes) => Some(avro::from_confluent_bytes(value_schema, &bytes)?),
                }
                .map(DebugValue);
                actual_messages.push(Record {
                    headers: record.headers,
                    key,
                    value,
                });
            }

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

            let expected = expected_messages
                .iter()
                .map(|v| {
                    let (headers, v) = split_headers(v, header_keys.len())?;
                    let mut deserializer = serde_json::Deserializer::from_str(v).into_iter();
                    let key = if let Some(key_schema) = &key_schema {
                        let key: serde_json::Value = match deserializer.next() {
                            None => bail!("key missing in input line"),
                            Some(r) => r?,
                        };
                        Some(avro::from_json(&key, key_schema.top_node())?)
                    } else {
                        None
                    }
                    .map(DebugValue);
                    let value = match deserializer.next() {
                        None => None,
                        Some(r) => {
                            let value = r.context("parsing json")?;
                            Some(avro::from_json(&value, value_schema.top_node())?)
                        }
                    }
                    .map(DebugValue);
                    ensure!(
                        deserializer.next().is_none(),
                        "at most two avro records per expect line"
                    );
                    Ok(Record {
                        headers,
                        key,
                        value,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            verify_with_partial_search(
                &expected,
                &actual_messages,
                &state.regex,
                &state.regex_replacement,
                partial_search.is_some(),
            )?
        }
        Format::Json { key: has_key } => {
            let mut actual_messages = vec![];
            for record in actual_bytes {
                let key = match record.key {
                    Some(bytes) => {
                        if *has_key {
                            Some(serde_json::from_slice(&bytes).context("decoding json")?)
                        } else {
                            None
                        }
                    }
                    None => None,
                };
                let value = match record.value {
                    None => None,
                    Some(bytes) => Some(serde_json::from_slice(&bytes).context("decoding json")?),
                };

                actual_messages.push(Record {
                    headers: record.headers,
                    key,
                    value,
                });
            }

            if sort_messages {
                actual_messages.sort_by_key(|r| format!("{:?}", r.value));
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

            let expected = expected_messages
                .iter()
                .map(|v| {
                    let (headers, v) = split_headers(v, header_keys.len())?;
                    let mut deserializer = json::parse_many(v)?.into_iter();
                    let key = if *has_key { deserializer.next() } else { None };
                    let value = deserializer.next();
                    ensure!(
                        deserializer.next().is_none(),
                        "at most two avro records per expect line"
                    );
                    Ok(Record {
                        headers,
                        key,
                        value,
                    })
                })
                .collect::<Result<Vec<_>, anyhow::Error>>()?;

            verify_with_partial_search(
                &expected,
                &actual_messages,
                &state.regex,
                &state.regex_replacement,
                partial_search.is_some(),
            )?;
        }
    }
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

fn verify_with_partial_search<A>(
    expected: &[Record<A>],
    actual: &[Record<A>],
    regex: &Option<Regex>,
    regex_replacement: &String,
    partial_search: bool,
) -> Result<(), anyhow::Error>
where
    A: Debug,
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
