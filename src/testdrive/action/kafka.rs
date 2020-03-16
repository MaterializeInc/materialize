// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::Duration;

use avro::types::Value as AvroValue;
use avro::Schema;
use byteorder::{BigEndian, ByteOrder, NetworkEndian, WriteBytesExt};
use futures::future::{self, TryFutureExt};
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use rdkafka::admin::NewPartitions;
use rdkafka::admin::{NewTopic, TopicReplication};
use rdkafka::consumer::Consumer;
use rdkafka::error::RDKafkaError;
use rdkafka::message::Message;
use rdkafka::producer::FutureRecord;

use ore::cast::CastFrom;
use ore::collections::CollectionExt;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;
use crate::protobuf::native::{Batch, Struct};
use crate::protobuf::{decode, json_to_protobuf};

pub struct CreateTopicAction {
    topic_prefix: String,
    partitions: i32,
}

pub fn build_create_topic(mut cmd: BuiltinCommand) -> Result<CreateTopicAction, String> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partitions = cmd.args.opt_parse("partitions")?.unwrap_or(1);
    cmd.args.done()?;

    Ok(CreateTopicAction {
        topic_prefix,
        partitions,
    })
}

impl Action for CreateTopicAction {
    fn undo(&self, state: &mut State) -> Result<(), String> {
        let metadata = state
            .kafka_consumer
            .fetch_metadata(None, Some(Duration::from_secs(1)))
            .map_err(|e| e.to_string())?;

        let stale_kafka_topics: Vec<_> = metadata
            .topics()
            .iter()
            .filter_map(|t| {
                if t.name().starts_with(&self.topic_prefix) {
                    Some(t.name())
                } else {
                    None
                }
            })
            .collect();

        if !stale_kafka_topics.is_empty() {
            println!(
                "Deleting stale Kafka topics {}",
                stale_kafka_topics.join(", ")
            );
            let res = state.tokio_runtime.block_on(
                state
                    .kafka_admin
                    .delete_topics(&stale_kafka_topics, &state.kafka_admin_opts),
            );
            let res = match res {
                Err(err) => return Err(err.to_string()),
                Ok(res) => res,
            };
            if res.len() != stale_kafka_topics.len() {
                return Err(format!(
                    "kafka topic deletion returned {} results, but exactly {} expected",
                    res.len(),
                    stale_kafka_topics.len()
                ));
            }
            for (res, topic) in res.iter().zip(stale_kafka_topics.iter()) {
                match res {
                    Ok(_) | Err((_, RDKafkaError::UnknownTopicOrPartition)) => (),
                    Err((_, err)) => {
                        eprintln!("warning: unable to delete {}: {}", topic, err.to_string())
                    }
                }
            }
        }
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let topic_name = format!("{}-{}", self.topic_prefix, state.seed);
        println!(
            "Creating Kafka topic {} with partition count of {}",
            topic_name, self.partitions
        );
        // NOTE(benesch): it is critical that we invent a new topic name on
        // every testdrive run. We previously tried to delete and recreate the
        // topic with a fixed name, but ran into serious race conditions in
        // Kafka that would regularly cause CI to hang. Details follow.
        //
        // Kafka topic creation and deletion is documented to be asynchronous.
        // That seems fine at first, as the Kafka admin API exposes an
        // `operation_timeout` option that would appear to allow you to opt into
        // a synchronous request by setting a massive timeout. As it turns out,
        // this parameter doesn't actually do anything [0].
        //
        // So, fine, we can implement our own polling for topic creation and
        // deletion, since the Kafka API exposes the list of topics currently
        // known to Kafka. This polling works well enough for topic creation.
        // After issuing a CreateTopics request, we poll the metadata list until
        // the topic appears with the requested number of partitions. (Yes,
        // sometimes the topic will appear with the wrong number of partitions
        // at first, and later sort itself out.)
        //
        // For deletion, though, there's another problem. Not only is deletion
        // of the topic metadata asynchronous, but deletion of the
        // topic data is *also* asynchronous, and independently so. As best as
        // I can tell, the following sequence of events is not only plausible,
        // but likely:
        //
        //     1. Client issues DeleteTopics(FOO).
        //     2. Kafka launches garbage collection of topic FOO.
        //     3. Kafka deletes metadata for topic FOO.
        //     4. Client polls and discovers topic FOO's metadata is gone.
        //     5. Client issues CreateTopics(FOO).
        //     6. Client writes some data to topic FOO.
        //     7. Kafka deletes data for topic FOO, including the data that was
        //        written to the second incarnation of topic FOO.
        //     8. Client attempts to read data written to topic FOO and waits
        //        forever, since there is no longer any data in the topic.
        //        Client becomes very confused and sad.
        //
        // There doesn't appear to be any sane way to poll to determine whether
        // the data has been deleted, since Kafka doesn't expose how many
        // messages are in a topic, and it's therefore impossible to distinguish
        // an empty topic from a deleted topic. And that's not even accounting
        // for the behavior when auto.create.topics.enable is true, which it
        // is by default, where asking about a topic that doesn't exist will
        // automatically create it.
        //
        // All this to say: please think twice before changing the topic naming
        // strategy.
        //
        // [0]: https://github.com/confluentinc/confluent-kafka-python/issues/524#issuecomment-456783176
        let new_topic = NewTopic::new(&topic_name, self.partitions, TopicReplication::Fixed(1))
            // Disabling retention is very important! Our testdrive tests
            // use hardcoded timestamps that are immediately eligible for
            // deletion by Kafka's garbage collector. E.g., the timestamp
            // "1" is interpreted as January 1, 1970 00:00:01, which is
            // breaches the default 7-day retention policy.
            .set("retention.ms", "-1");
        let res = state.tokio_runtime.block_on(
            state
                .kafka_admin
                .create_topics(&[new_topic], &state.kafka_admin_opts),
        );
        let res = match res {
            Err(err) => return Err(err.to_string()),
            Ok(res) => res,
        };
        if res.len() != 1 {
            return Err(format!(
                "kafka topic creation returned {} results, but exactly one result was expected",
                res.len()
            ));
        }
        match res.into_element() {
            Ok(_) | Err((_, RDKafkaError::TopicAlreadyExists)) => Ok(()),
            Err((_, err)) => Err(err.to_string()),
        }?;

        // Topic creation is asynchronous, and if we don't wait for it to
        // complete, we might produce a message (below) that causes it to
        // get automatically created with multiple partitions. (Since
        // multiple partitions have no ordering guarantees, this violates
        // many assumptions that our tests make.)
        let mut i = 0;
        loop {
            let res = (|| {
                let metadata = state
                    .kafka_consumer
                    // N.B. It is extremely important not to ask specifically
                    // about the topic here, even though the API supports it!
                    // Asking about the topic will create it automatically...
                    // with the wrong number of partitions. Yes, this is
                    // unbelievably horrible.
                    .fetch_metadata(None, Some(Duration::from_secs(1)))
                    .map_err(|e| e.to_string())?;
                if metadata.topics().is_empty() {
                    return Err("metadata fetch returned no topics".to_string());
                }
                let topic = match metadata.topics().iter().find(|t| t.name() == topic_name) {
                    Some(topic) => topic,
                    None => {
                        return Err(format!(
                            "metadata fetch did not return topic {}",
                            topic_name,
                        ))
                    }
                };
                if topic.partitions().is_empty() {
                    return Err("metadata fetch returned a topic with no partitions".to_string());
                } else if topic.partitions().len() as i32 != self.partitions {
                    return Err(format!(
                        "topic {} was created with {} partitions when exactly {} was expected",
                        topic_name,
                        topic.partitions().len(),
                        self.partitions
                    ));
                }
                Ok(())
            })();
            match res {
                Ok(()) => break,
                Err(e) if i == 6 => return Err(e),
                _ => {
                    thread::sleep(Duration::from_millis(100 * 2_u64.pow(i)));
                    i += 1;
                }
            }
        }
        state.kafka_topics.insert(topic_name, self.partitions);
        Ok(())
    }
}

pub struct AddPartitionsAction {
    topic_prefix: String,
    partitions: i32,
}

pub fn build_add_partitions(mut cmd: BuiltinCommand) -> Result<AddPartitionsAction, String> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partitions = cmd.args.opt_parse("total-partitions")?.unwrap_or(1);
    cmd.args.done()?;

    Ok(AddPartitionsAction {
        topic_prefix,
        partitions,
    })
}

impl Action for AddPartitionsAction {
    fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let topic_name = format!("{}-{}", self.topic_prefix, state.seed);
        println!(
            "Raising partition count of Kafka topic {} to {}",
            topic_name, self.partitions
        );

        match state.kafka_topics.get(&topic_name) {
            Some(partitions) => {
                if self.partitions <= *partitions {
                    return Err(format!(
                        "new partition count {} is not greater than current partition count {}",
                        self.partitions, partitions
                    ));
                }
            }
            None => {
                return Err(format!(
                    "topic {} not created by kafka-create-topic",
                    topic_name
                ))
            }
        }

        let partitions = NewPartitions::new(&topic_name, usize::cast_from(self.partitions));
        let res = state.tokio_runtime.block_on(
            state
                .kafka_admin
                .create_partitions(&[partitions], &state.kafka_admin_opts)
                .map_err(|e| e.to_string()),
        )?;
        if res.len() != 1 {
            return Err(format!(
                "kafka partition addition returned {} results, but exactly one result was expected",
                res.len()
            ));
        }
        if let Err((_topic_name, e)) = res.into_element() {
            return Err(e.to_string());
        }

        let mut i = 0;
        loop {
            let res = (|| {
                let metadata = state
                    .kafka_consumer
                    .fetch_metadata(Some(&topic_name), Some(Duration::from_secs(1)))
                    .map_err(|e| e.to_string())?;
                if metadata.topics().len() != 1 {
                    return Err("metadata fetch returned no topics".to_string());
                }
                let topic = metadata.topics().into_element();
                if topic.partitions().len() as i32 != self.partitions {
                    return Err(format!(
                        "topic {} has {} partitions when exactly {} was expected",
                        topic_name,
                        topic.partitions().len(),
                        self.partitions,
                    ));
                }
                Ok(())
            })();
            match res {
                Ok(()) => break,
                Err(e) if i == 6 => return Err(e),
                _ => {
                    thread::sleep(Duration::from_millis(100 * 2_u64.pow(i)));
                    i += 1;
                }
            }
        }

        state.kafka_topics.insert(topic_name, self.partitions);
        Ok(())
    }
}

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
        state
            .kafka_consumer
            .subscribe(&[&self.topic_prefix])
            .map_err(|e| e.to_string())?;
        let schema = interchange::avro::parse_schema(&self.schema)
            .map_err(|e| format!("parsing avro schema: {}", e))?;
        let mut converted_expected_messages = Vec::new();
        for expected in &self.expected_messages {
            converted_expected_messages.push(
                crate::avro::json_to_avro(
                    &serde_json::from_str(expected)
                        .map_err(|e| format!("parsing avro datum: {}", e.to_string()))?,
                    &schema,
                )
                .unwrap(),
            );
        }
        let mut message_stream = state.kafka_consumer.start();
        let mut actual_messages = Vec::new();
        for _i in 0..converted_expected_messages.len() {
            let output = state.tokio_runtime.block_on(message_stream.next());
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
                                state
                                    .tokio_runtime
                                    .block_on(avro::from_avro_datum(&schema, &mut bytes, None))
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

pub struct IngestAction {
    topic_prefix: String,
    partition: i32,
    message_format: RawSchema,
    timestamp: Option<i64>,
    publish: bool,
    rows: Vec<String>,
}

/// The raw definition of a collection of messages
///
/// We use this to publish metadata
enum RawSchema {
    /// An avro schema
    Avro {
        key_schema: Option<String>,
        schema: String,
    },
    Proto {
        /// The name of the message to be sent
        message: String,
    },
    Bytes,
}

/// The parsed format
///
/// This includes information required for us to parse individual messages, and what we
/// need to send to kafka along with each message in order for materialize to handle messages
enum ParsedSchema {
    Avro {
        schema: Schema,
        schema_id: i32,
    },
    Proto {
        parser: &'static dyn Fn(&str) -> Result<crate::protobuf::DynMessage, failure::Error>,
        validator: &'static dyn Fn(&[u8]) -> Result<Box<dyn std::fmt::Debug>, failure::Error>,
    },
    Bytes,
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let format = cmd.args.string("format")?;
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partition = cmd.args.opt_parse::<i32>("partition")?.unwrap_or(0);
    let message_format = match format.as_ref() {
        "avro" => {
            let schema = cmd.args.string("schema")?;
            let key_schema = cmd.args.opt_string("key_schema");
            RawSchema::Avro { key_schema, schema }
        }
        "protobuf" => {
            let message = cmd.args.string("message")?;
            RawSchema::Proto { message }
        }
        "raw" => RawSchema::Bytes {},
        _ => return Err(format!("Unknown message format: {}", format)),
    };

    let timestamp = cmd.args.opt_parse("timestamp")?;
    let publish = cmd.args.opt_bool("publish")?;
    cmd.args.done()?;
    if !["protobuf", "avro", "raw"].contains(&&*format) {
        return Err("formats besides avro, protobuf or raw are not supported".into());
    }
    Ok(IngestAction {
        topic_prefix,
        partition,
        message_format,
        timestamp,
        publish,
        rows: cmd.input,
    })
}

impl Action for IngestAction {
    fn undo(&self, state: &mut State) -> Result<(), String> {
        if self.publish {
            let subjects = state.tokio_runtime.block_on(
                state
                    .ccsr_client
                    .list_subjects()
                    .map_err(|e| format!("unable to list subjects in schema registry: {}", e)),
            )?;

            let stale_subjects: Vec<_> = subjects
                .iter()
                .filter(|s| s.starts_with(&self.topic_prefix))
                .collect();

            for subject in stale_subjects {
                println!("Deleting stale schema registry subject {}", subject);
                match state
                    .tokio_runtime
                    .block_on(state.ccsr_client.delete_subject(&subject))
                {
                    Ok(()) | Err(ccsr::DeleteError::SubjectNotFound) => (),
                    Err(e) => return Err(e.to_string()),
                }
            }
        }

        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let topic_name = format!("{}-{}", self.topic_prefix, state.seed);
        if !state.kafka_topics.contains_key(&topic_name) {
            return Err(format!(
                "topic {} not created by kafka-create-topic",
                topic_name
            ));
        }
        println!("Ingesting data into Kafka topic {:?}", topic_name);
        let format = match &self.message_format {
            RawSchema::Avro { key_schema, schema } => {
                let schema_id = if self.publish {
                    let ccsr_subject = format!("{}-value", topic_name);
                    let schema_id = state.tokio_runtime.block_on(
                        state
                            .ccsr_client
                            .publish_schema(&ccsr_subject, &schema)
                            .map_err(|e| format!("schema registry error: {}", e)),
                    )?;
                    if let Some(key_schema) = key_schema {
                        let key_subject = format!("{}-key", topic_name);
                        state.tokio_runtime.block_on(
                            state
                                .ccsr_client
                                .publish_schema(&key_subject, &key_schema)
                                .map_err(|e| format!("schema registry error: {}", e)),
                        )?;
                    }
                    schema_id
                } else {
                    1
                };
                let schema = interchange::avro::parse_schema(&schema)
                    .map_err(|e| format!("parsing avro schema: {}", e))?;
                ParsedSchema::Avro { schema, schema_id }
            }
            RawSchema::Proto { message } => match message.as_ref() {
                ".Struct" => ParsedSchema::Proto {
                    parser: &json_to_protobuf::<Struct>,
                    validator: &decode::<Struct>,
                },
                ".Batch" => ParsedSchema::Proto {
                    parser: &json_to_protobuf::<Batch>,
                    validator: &decode::<Batch>,
                },
                _ => return Err(format!("unknown testdrive protobuf message: {}", message)),
            },
            RawSchema::Bytes {} => ParsedSchema::Bytes {},
        };

        let futs = FuturesUnordered::new();
        for row in &self.rows {
            let mut buf = Vec::new();
            match &format {
                ParsedSchema::Avro { schema, schema_id } => {
                    let val = crate::avro::json_to_avro(
                        &serde_json::from_str(row)
                            .map_err(|e| format!("parsing avro datum: {}", e.to_string()))?,
                        &schema,
                    )?
                    .resolve(&schema)
                    .map_err(|e| format!("resolving avro schema: {}", e))?;
                    // The first byte is a magic byte (0) that indicates the Confluent
                    // serialization format version, and the next four bytes are a
                    // 32-bit schema ID.
                    //
                    // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
                    buf.write_u8(0).unwrap();
                    buf.write_i32::<NetworkEndian>(*schema_id).unwrap();
                    buf.extend(avro::to_avro_datum(&schema, val).map_err(|e| e.to_string())?);
                }
                ParsedSchema::Proto { parser, validator } => {
                    let msg = parser(row)
                        .map_err(|e| format!("converting row to type {} -> {}", row, e))?;
                    buf = msg
                        .write_to_bytes()
                        .map_err(|e| format!("writing protobuf message for {}: {}", row, e))?;
                    // There are a variety of `write_*` methods on `Message` that don't
                    // seem to automatically do the right thing. This should always
                    // succeed, otherwise there is no chance for the server.
                    let _parsed = validator(&buf)
                        .map_err(|e| format!("error validating proto row={}\nerror={}", row, e))?;
                }
                ParsedSchema::Bytes {} => {
                    buf = row.as_bytes().to_vec();
                }
            }

            let mut record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&topic_name)
                .payload(&buf)
                .partition(self.partition);
            if let Some(timestamp) = self.timestamp {
                record = record.timestamp(timestamp);
            }
            futs.push(state.kafka_producer.send(record, 1000 /* block_ms */));
        }
        state
            .tokio_runtime
            .block_on(futs.try_for_each(|_| future::ok(())))
            .map_err(|e| e.to_string())
    }
}
