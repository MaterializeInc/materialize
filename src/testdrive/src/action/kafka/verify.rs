// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use async_trait::async_trait;
use byteorder::{BigEndian, ByteOrder};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::stream::StreamExt;

use ore::retry;

use crate::action::{Action, State};
use crate::format::avro;
use crate::parser::BuiltinCommand;

pub struct VerifyAction {
    sink: String,
    expected_messages: Vec<String>,
}

pub fn build_verify(mut cmd: BuiltinCommand) -> Result<VerifyAction, String> {
    let _format = cmd.args.string("format")?;
    let sink = cmd.args.string("sink")?;
    let expected_messages = cmd.input;
    cmd.args.done()?;
    Ok(VerifyAction {
        sink,
        expected_messages,
    })
}

#[async_trait]
impl Action for VerifyAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let topic: String = retry::retry_for(Duration::from_secs(8), |_| async {
            let row = state
                .pgclient
                .query_one(
                    "SELECT topic FROM mz_catalog_names NATURAL JOIN mz_kafka_sinks \
                     WHERE name = $1",
                    &[&self.sink],
                )
                .await
                .map_err(|e| format!("retrieving topic name: {}", e))?;
            Ok::<_, String>(row.get("topic"))
        })
        .await?;

        println!("Verifying results in Kafka topic {}", topic);

        let schema = state
            .ccsr_client
            .get_schema_by_subject(&format!("{}-value", topic))
            .await
            .map_err(|e| format!("fetching schema: {}", e))?
            .raw;

        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &state.kafka_url);
        config.set("auto.offset.reset", "earliest");
        config.set("group.id", "materialize-testdrive");

        let schema =
            avro::parse_schema(&schema).map_err(|e| format!("parsing avro schema: {}", e))?;
        let schema = &schema;

        let consumer: StreamConsumer = config
            .create()
            .map_err(|e| format!("creating kafka consumer: {}", e))?;
        consumer.subscribe(&[&topic]).map_err(|e| e.to_string())?;

        // Wait up to 10 seconds for each message.
        let mut message_stream = consumer
            .start()
            .take(self.expected_messages.len())
            .timeout(Duration::from_secs(15));

        let mut actual_messages = vec![];

        // Collect all messages that arrive without timing out. If we trip
        // the timeout, suppress the error and return what we have. This
        // is nicer than returning "timeout expired", as the user will
        // instead get an error message about the expected messages that
        // were missing.
        while let Some(Ok(message)) = message_stream.next().await {
            let message = message.map_err(|e| e.to_string())?;

            let mut bytes = match message.payload() {
                None => return Err("empty message payload".into()),
                Some(bytes) => bytes,
            };

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
            actual_messages.push(datum);
        }

        avro::validate_sink(schema, &self.expected_messages, &actual_messages)
    }
}
