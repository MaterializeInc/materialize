// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::time::Duration;

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::pin;
use tokio_stream::StreamExt;

use crate::action::{Action, State};
use crate::format::avro;
use crate::parser::BuiltinCommand;

pub enum SinkConsistencyFormat {
    Debezium,
}

pub struct VerifyAction {
    sink: String,
    consistency: Option<SinkConsistencyFormat>,
    sort_messages: bool,
    expected_messages: Vec<String>,
}

pub fn build_verify(mut cmd: BuiltinCommand) -> Result<VerifyAction, String> {
    let _format = cmd.args.string("format")?;
    let sink = cmd.args.string("sink")?;
    let consistency = match cmd.args.opt_string("consistency").as_deref() {
        Some("debezium") => Some(SinkConsistencyFormat::Debezium),
        Some(s) => return Err(format!("unknown sink consistency format {}", s)),
        None => None,
    };

    let sort_messages = cmd.args.opt_bool("sort-messages")?.unwrap_or(false);
    let expected_messages = cmd.input;
    cmd.args.done()?;
    Ok(VerifyAction {
        sink,
        consistency,
        sort_messages,
        expected_messages,
    })
}

fn avro_from_bytes(
    schema: &mz_avro::Schema,
    mut bytes: &[u8],
) -> Result<mz_avro::types::Value, String> {
    if bytes.len() < 5 {
        return Err(format!(
            "avro datum is too few bytes: expected at least 5 bytes, got {}",
            bytes.len()
        ));
    }
    let magic = bytes[0];
    let _schema_id = BigEndian::read_i32(&bytes[1..5]);
    bytes = &bytes[5..];

    if magic != 0 {
        return Err(format!(
            "wrong avro serialization magic: expected 0, got {}",
            bytes[0]
        ));
    }

    let datum = avro::from_avro_datum(schema, &mut bytes)
        .map_err(|e| format!("from_avro_datum: {}", e.to_string()))?;
    Ok(datum)
}

#[async_trait]
impl Action for VerifyAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let topic_prefix: String = state
            .pgclient
            .query_one(
                "SELECT topic FROM mz_catalog_names JOIN mz_kafka_sinks ON global_id = sink_id WHERE name = $1",
                &[&self.sink],
            )
            .await
            .map_err(|e| format!("retrieving topic name: {}", e))?
            .get("topic");

        let topic = match self.consistency {
            Some(SinkConsistencyFormat::Debezium) => format!("{}-consistency", topic_prefix),
            None => topic_prefix,
        };

        println!("Verifying results in Kafka topic {}", topic);

        let value_schema = state
            .ccsr_client
            .get_schema_by_subject(&format!("{}-value", topic))
            .await
            .map_err(|e| format!("fetching schema: {}", e))?
            .raw;

        let key_schema = state
            .ccsr_client
            .get_schema_by_subject(&format!("{}-key", topic))
            .await
            .ok()
            .map(|key_schema| {
                avro::parse_schema(&key_schema.raw)
                    .map_err(|e| format!("parsing avro schema: {}", e))
            })
            .transpose()?;

        let config = state.kafka_config.clone();

        let value_schema =
            avro::parse_schema(&value_schema).map_err(|e| format!("parsing avro schema: {}", e))?;
        let value_schema = &value_schema;

        let consumer: StreamConsumer = config
            .create()
            .map_err(|e| format!("creating kafka consumer: {}", e))?;
        consumer
            .subscribe(&[&topic])
            .map_err(|e| format!("subscribing: {}", e.to_string()))?;

        // Wait up to 15 seconds for each message.
        let message_stream = consumer
            .stream()
            .take(self.expected_messages.len())
            .timeout(cmp::max(state.default_timeout, Duration::from_secs(15)));
        pin!(message_stream);

        let mut actual_messages = vec![];

        // Collect all messages that arrive without timing out. If we trip
        // the timeout, suppress the error and return what we have. This
        // is nicer than returning "timeout expired", as the user will
        // instead get an error message about the expected messages that
        // were missing.
        while let Some(Ok(message)) = message_stream.next().await {
            let message = message.map_err(|e| e.to_string())?;

            let bytes = match message.payload() {
                None => return Err("empty message payload".into()),
                Some(bytes) => bytes,
            };
            let value_datum = avro_from_bytes(value_schema, bytes)?;
            let key_datum = key_schema
                .as_ref()
                .map(|key_schema| {
                    let bytes = match message.key() {
                        Some(key) => key,
                        None => return Err("empty message key".into()),
                    };
                    avro_from_bytes(key_schema, bytes)
                })
                .transpose()?;
            actual_messages.push((key_datum, value_datum));
        }

        if self.sort_messages {
            actual_messages.sort_by_key(|k| format!("{:?}", k.1));
        }

        avro::validate_sink(
            key_schema.as_ref(),
            value_schema,
            &self.expected_messages,
            &actual_messages,
        )
    }
}
