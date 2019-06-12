// Copyright 2018 Flavien Raynaud
// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
//
// Portions of this file are derived from the ToAvro implementation for
// serde_json::Value that is shipped with the avro_rs project. The original
// source code was retrieved on April 25, 2019 from:
//
//     https://github.com/flavray/avro-rs/blob/c4971ac08f52750db6bc95559c2b5faa6c0c9a06/src/types.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

use avro_rs::types::Value as AvroValue;
use avro_rs::Schema;
use backoff::{ExponentialBackoff, Operation};
use byteorder::{NetworkEndian, WriteBytesExt};
use futures::stream::FuturesUnordered;
use futures::Future;
use rdkafka::admin::{NewTopic, TopicReplication};
use rdkafka::consumer::Consumer;
use rdkafka::error::RDKafkaError;
use rdkafka::producer::FutureRecord;
use serde_json::Value as JsonValue;
use std::thread;
use std::time::Duration;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;
use ore::collections::CollectionExt;
use ore::future::StreamExt;

pub struct IngestAction {
    topic: String,
    schema: String,
    timestamp: Option<i64>,
    ccsr_subject: Option<String>,
    rows: Vec<String>,
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let format = cmd.args.string("format")?;
    let topic = cmd.args.string("topic")?;
    let schema = cmd.args.string("schema")?;
    let timestamp = cmd.args.opt_parse("timestamp")?;
    let publish = cmd.args.opt_bool("publish")?;
    cmd.args.done()?;
    if format != "avro" {
        return Err("formats besides avro are not supported".into());
    }
    let ccsr_subject = if publish {
        Some(format!("{}-value", topic))
    } else {
        None
    };
    Ok(IngestAction {
        topic,
        schema,
        timestamp,
        ccsr_subject,
        rows: cmd.input,
    })
}

impl Action for IngestAction {
    fn undo(&self, state: &mut State) -> Result<(), String> {
        if let Some(subject) = &self.ccsr_subject {
            println!("Deleting schema for {:?}", self.topic);
            match state.ccsr_client.delete_subject(subject) {
                Ok(()) | Err(ccsr::DeleteError::SubjectNotFound) => (),
                Err(e) => return Err(e.to_string()),
            }
        }

        let metadata = state
            .kafka_consumer
            .fetch_metadata(None, Some(Duration::from_secs(1)))
            .map_err(|e| e.to_string())?;
        if !metadata.topics().iter().any(|t| t.name() == self.topic) {
            // The topic is already deleted. Deleting it again will fire off an
            // asynchronous deletion request that may interfere with our attempt
            // to create the topic below.
            return Ok(());
        }

        println!("Deleting Kafka topic {:?}", self.topic);
        let res = state
            .kafka_admin
            .delete_topics(&[&self.topic], &state.kafka_admin_opts)
            .wait();
        let res = match res {
            Err(err) => return Err(err.to_string()),
            Ok(res) => res,
        };
        if res.len() != 1 {
            return Err(format!(
                "kafka topic deletion returned {} results, but exactly one result was expected",
                res.len()
            ));
        }
        match res.into_element() {
            Ok(_) | Err((_, RDKafkaError::UnknownTopicOrPartition)) => (),
            Err((_, err)) => Err(err.to_string())?,
        }

        let mut backoff = ExponentialBackoff::default();
        backoff.max_elapsed_time = Some(Duration::from_secs(5));
        (|| {
            let metadata = state
                .kafka_consumer
                // N.B. It is extremely important not to ask specifically
                // about the topic here, even though the API supports it!
                // Asking about the topic will create it automatically...
                // with the wrong number of partitions. Yes, this is
                // unbelievably horrible.
                .fetch_metadata(None, Some(Duration::from_secs(1)))
                .map_err(|e| e.to_string())?;
            if metadata.topics().iter().any(|t| t.name() == self.topic) {
                Err(format!("topic {} not deleted yet", self.topic))?
            }
            Ok(())
        })
        .retry(&mut backoff)
        .map_err(|e| e.to_string())?;

        // Apparently Kafka lies about when the deletion is processed. Wait a
        // bit so the deletion request doesn't nuke us when we create the topic
        // later.
        thread::sleep(Duration::from_millis(100));

        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        println!("Ingesting data into Kafka topic {:?}", self.topic);
        {
            let num_partitions = 1;
            let new_topic = NewTopic::new(&self.topic, num_partitions, TopicReplication::Fixed(1));
            let res = state
                .kafka_admin
                .create_topics(&[new_topic], &state.kafka_admin_opts)
                .wait();
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
            let mut backoff = ExponentialBackoff::default();
            backoff.max_elapsed_time = Some(Duration::from_secs(5));
            (|| {
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
                    Err("metadata fetch returned no topics".to_string())?
                }
                let topic = match metadata.topics().iter().find(|t| t.name() == self.topic) {
                    Some(topic) => topic,
                    None => Err(format!(
                        "metadata fetch did not return topic {}",
                        self.topic
                    ))?,
                };
                if topic.partitions().is_empty() {
                    Err("metadata fetch returned a topic with no partitions".to_string())?
                } else if topic.partitions().len() != 1 {
                    Err(format!(
                        "topic {} was created with {} partitions when exactly one was expected",
                        self.topic,
                        topic.partitions().len()
                    ))?
                }
                Ok(())
            })
            .retry(&mut backoff)
            .map_err(|e| e.to_string())?
        }
        let schema_id = if let Some(subject) = &self.ccsr_subject {
            state
                .ccsr_client
                .publish_schema(subject, &self.schema)
                .map_err(|e| format!("schema registry error: {}", e))?
        } else {
            1
        };
        let schema =
            Schema::parse_str(&self.schema).map_err(|e| format!("parsing avro schema: {}", e))?;
        let mut futs = FuturesUnordered::new();
        for row in &self.rows {
            let val = json_to_avro(
                serde_json::from_str(row)
                    .map_err(|e| format!("parsing avro datum: {}", e.to_string()))?,
            )
            .resolve(&schema)
            .map_err(|e| format!("resolving avro schema: {}", e))?;

            // The first byte is a magic byte (0) that indicates the Confluent
            // serialization format version, and the next four bytes are a
            // 32-bit schema ID.
            //
            // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
            let mut buf = Vec::new();
            buf.write_u8(0).unwrap();
            buf.write_i32::<NetworkEndian>(schema_id).unwrap();
            buf.extend(avro_rs::to_avro_datum(&schema, val).map_err(|e| e.to_string())?);

            let mut record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&self.topic).payload(&buf);
            if let Some(timestamp) = self.timestamp {
                record = record.timestamp(timestamp);
            }
            futs.push(state.kafka_producer.send(record, 1000 /* block_ms */));
        }
        futs.drain().wait().map_err(|e| e.to_string())
    }
}

// This function is derived from code in the avro_rs project. Update the license
// header on this file accordingly if you move it to a new home.
fn json_to_avro(json: JsonValue) -> AvroValue {
    match json {
        JsonValue::Null => AvroValue::Null,
        JsonValue::Bool(b) => AvroValue::Boolean(b),
        JsonValue::Number(ref n) if n.is_i64() => AvroValue::Long(n.as_i64().unwrap()),
        JsonValue::Number(ref n) if n.is_f64() => AvroValue::Double(n.as_f64().unwrap()),
        // TODO(benesch): this is silently wrong for large numbers
        JsonValue::Number(n) => AvroValue::Long(n.as_u64().unwrap() as i64),
        JsonValue::String(s) => AvroValue::String(s),
        JsonValue::Array(items) => AvroValue::Array(items.into_iter().map(json_to_avro).collect()),
        JsonValue::Object(items) => AvroValue::Record(
            items
                .into_iter()
                .map(|(key, value)| (key, json_to_avro(value)))
                .collect(),
        ),
    }
}
