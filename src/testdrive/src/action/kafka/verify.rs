// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use avro::types::Value as AvroValue;
use byteorder::{BigEndian, ByteOrder};
use futures::executor::block_on;
use futures::stream::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct VerifyAction {
    topic_prefix: String,
    schema: String,
    expected_messages: Vec<String>,
}

pub fn build_verify(mut cmd: BuiltinCommand) -> Result<VerifyAction, String> {
    let _format = cmd.args.string("format")?;
    let topic_prefix = cmd.args.string("topic")?;
    let schema = cmd.args.string("schema")?;
    let expected_messages = cmd.input;

    cmd.args.done()?;

    Ok(VerifyAction {
        topic_prefix,
        schema,
        expected_messages,
    })
}

impl Action for VerifyAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &state.kafka_addr);
        config.set("auto.offset.reset", "earliest");
        config.set("group.id", "materialize-testdrive");

        let schema = interchange::avro::parse_schema(&self.schema)
            .map_err(|e| format!("parsing avro schema: {}", e))?;
        let mut converted_expected_messages = Vec::new();
        for expected in &self.expected_messages {
            converted_expected_messages.push(
                crate::format::avro::json_to_avro(
                    &serde_json::from_str(expected)
                        .map_err(|e| format!("parsing avro datum: {}", e.to_string()))?,
                    &schema,
                )
                .unwrap(),
            );
        }
        let consumer: StreamConsumer = config
            .create()
            .map_err(|e| format!("creating kafka consumer: {}", e))?;
        consumer
            .subscribe(&[&self.topic_prefix])
            .map_err(|e| e.to_string())?;
        let mut message_stream = consumer.start();
        let mut actual_messages = Vec::new();
        for _i in 0..converted_expected_messages.len() {
            let output = block_on(message_stream.next());
            match output {
                Some(result) => match result {
                    Ok(m) => match m.payload() {
                        Some(mut bytes) => {
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
                            actual_messages.push(
                                block_on(avro::from_avro_datum(&schema, &mut bytes, None))
                                    .map_err(|e| format!("from_avro_datum: {}", e.to_string()))?,
                            );
                        }
                        None => {
                            return Err(String::from("No bytes found in Kafka message payload."))
                        }
                    },
                    Err(e) => return Err(e.to_string()),
                },
                None => {
                    return Err(format!(
                        "No Kafka messages found for topic {}",
                        &self.topic_prefix
                    ))
                }
            }
        }
        // NB: We can't compare messages as they come in because
        // Kafka sinks do not currently support ordering.
        // Additionally, we do this bummer of a comparison because
        // avro::types::Value does not implement Eq or Ord.
        // TODO@jldlaughlin: update this once we have Kafka ordering guarantees
        let missing_values =
            get_values_in_first_list_not_in_second(&converted_expected_messages, &actual_messages);
        let additional_values =
            get_values_in_first_list_not_in_second(&actual_messages, &converted_expected_messages);

        if !missing_values.is_empty() || !additional_values.is_empty() {
            return Err(format!(
                "Mismatched Kafka sink rows. Missing: {:#?}, Unexpected: {:#?}",
                missing_values, additional_values
            ));
        }

        Ok(())
    }
}

fn get_values_in_first_list_not_in_second(
    first_list: &[AvroValue],
    second_list: &[AvroValue],
) -> Vec<AvroValue> {
    let mut first_list_clone: Vec<AvroValue> = first_list.to_vec();
    let mut missing_values = Vec::new();
    for s in second_list {
        let pos = first_list_clone.iter().position(|x| *x == *s);
        match pos {
            Some(index) => {
                first_list_clone.remove(index);
                continue;
            }
            None => missing_values.push(s.clone()),
        }
    }
    missing_values
}
