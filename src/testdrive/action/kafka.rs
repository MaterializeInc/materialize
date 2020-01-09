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

use std::convert::{TryFrom, TryInto};
use std::num::TryFromIntError;
use std::time::Duration;

use avro_rs::types::Value as AvroValue;
use avro_rs::Schema;
use backoff::{ExponentialBackoff, Operation};
use byteorder::{NetworkEndian, WriteBytesExt};
use futures::executor::block_on;
use futures::future;
use futures::stream::{FuturesUnordered, TryStreamExt};
use rdkafka::admin::{NewTopic, TopicReplication};
use rdkafka::consumer::Consumer;
use rdkafka::error::RDKafkaError;
use rdkafka::producer::FutureRecord;
use serde_json::Value as JsonValue;

use ore::collections::CollectionExt;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct IngestAction {
    topic_prefix: String,
    schema: String,
    key_schema: Option<String>,
    timestamp: Option<i64>,
    publish: bool,
    rows: Vec<String>,
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let format = cmd.args.string("format")?;
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let schema = cmd.args.string("schema")?;
    let key_schema = cmd.args.opt_string("key_schema");
    let timestamp = cmd.args.opt_parse("timestamp")?;
    let publish = cmd.args.opt_bool("publish")?;
    cmd.args.done()?;
    if format != "avro" {
        return Err("formats besides avro are not supported".into());
    }
    Ok(IngestAction {
        topic_prefix,
        schema,
        key_schema,
        timestamp,
        publish,
        rows: cmd.input,
    })
}

impl IngestAction {
    fn do_undo(&self, state: &mut State) -> Result<(), String> {
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
            let res = block_on(
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

        if self.publish {
            let subjects = state
                .ccsr_client
                .list_subjects()
                .map_err(|e| format!("unable to list subjects in schema registry: {}", e))?;

            let stale_subjects: Vec<_> = subjects
                .iter()
                .filter(|s| s.starts_with(&self.topic_prefix))
                .collect();

            for subject in stale_subjects {
                println!("Deleting stale schema registry subject {}", subject);
                match state.ccsr_client.delete_subject(&subject) {
                    Ok(()) | Err(ccsr::DeleteError::SubjectNotFound) => (),
                    Err(e) => return Err(e.to_string()),
                }
            }
        }

        Ok(())
    }

    fn do_redo(&self, state: &mut State) -> Result<(), String> {
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
        let topic_name = format!("{}-{}", self.topic_prefix, state.seed);
        println!("Ingesting data into Kafka topic {:?}", topic_name);
        {
            let num_partitions = 1;
            let new_topic = NewTopic::new(&topic_name, num_partitions, TopicReplication::Fixed(1))
                // Disabling retention is very important! Our testdrive tests
                // use hardcoded timestamps that are immediately eligible for
                // deletion by Kafka's garbage collector. E.g., the timestamp
                // "1" is interpreted as January 1, 1970 00:00:01, which is
                // breaches the default 7-day retention policy.
                .set("retention.ms", "-1");
            let res = block_on(
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
            let mut backoff = ExponentialBackoff::default();
            backoff.max_elapsed_time = Some(Duration::from_secs(5));
            #[allow(clippy::try_err)]
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
                let topic = match metadata.topics().iter().find(|t| t.name() == topic_name) {
                    Some(topic) => topic,
                    None => Err(format!(
                        "metadata fetch did not return topic {}",
                        topic_name,
                    ))?,
                };
                if topic.partitions().is_empty() {
                    Err("metadata fetch returned a topic with no partitions".to_string())?
                } else if topic.partitions().len() != 1 {
                    Err(format!(
                        "topic {} was created with {} partitions when exactly one was expected",
                        topic_name,
                        topic.partitions().len()
                    ))?
                }
                Ok(())
            })
            .retry(&mut backoff)
            .map_err(|e| e.to_string())?
        }

        let ccsr_subject = if self.publish {
            Some(format!("{}-value", topic_name))
        } else {
            None
        };

        let schema_id = if let Some(subject) = ccsr_subject {
            state
                .ccsr_client
                .publish_schema(&subject, &self.schema)
                .map_err(|e| format!("schema registry error: {}", e))?
        } else {
            1
        };

        if let Some(key_schema) = &self.key_schema {
            let key_subject = format!("{}-key", topic_name);
            state
                .ccsr_client
                .publish_schema(&key_subject, &key_schema)
                .map_err(|e| format!("schema registry error: {}", e))?;
        }

        let schema = interchange::avro::parse_schema(&self.schema)
            .map_err(|e| format!("parsing avro schema: {}", e))?;
        let futs = FuturesUnordered::new();
        for row in &self.rows {
            let val = json_to_avro(
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
            let mut buf = Vec::new();
            buf.write_u8(0).unwrap();
            buf.write_i32::<NetworkEndian>(schema_id).unwrap();
            buf.extend(avro_rs::to_avro_datum(&schema, val).map_err(|e| e.to_string())?);

            let mut record: FutureRecord<&Vec<u8>, _> = FutureRecord::to(&topic_name).payload(&buf);
            if let Some(timestamp) = self.timestamp {
                record = record.timestamp(timestamp);
            }
            futs.push(state.kafka_producer.send(record, 1000 /* block_ms */));
        }
        block_on(futs.try_for_each(|_| future::ok(()))).map_err(|e| e.to_string())
    }
}

impl Action for IngestAction {
    fn undo(&self, state: &mut State) -> Result<(), String> {
        tokio::runtime::Runtime::new()
            .unwrap()
            .enter(|| self.do_undo(state))
    }
    fn redo(&self, state: &mut State) -> Result<(), String> {
        tokio::runtime::Runtime::new()
            .unwrap()
            .enter(|| self.do_redo(state))
    }
}

// This function is derived from code in the avro_rs project. Update the license
// header on this file accordingly if you move it to a new home.
fn json_to_avro(json: &JsonValue, schema: &Schema) -> Result<AvroValue, String> {
    match (json, schema) {
        (JsonValue::Null, Schema::Null) => Ok(AvroValue::Null),
        (JsonValue::Bool(b), Schema::Boolean) => Ok(AvroValue::Boolean(*b)),
        (JsonValue::Number(ref n), Schema::Int) => Ok(AvroValue::Int(
            n.as_i64()
                .unwrap()
                .try_into()
                .map_err(|e: TryFromIntError| e.to_string())?,
        )),
        (JsonValue::Number(ref n), Schema::Long) => Ok(AvroValue::Long(n.as_i64().unwrap())),
        (JsonValue::Number(ref n), Schema::Float) => {
            Ok(AvroValue::Float(n.as_f64().unwrap() as f32))
        }
        (JsonValue::Number(ref n), Schema::Double) => Ok(AvroValue::Double(n.as_f64().unwrap())),
        (JsonValue::Number(ref n), Schema::Date) => Ok(AvroValue::Date(
            chrono::NaiveDate::from_ymd(1970, 1, 1) + chrono::Duration::days(n.as_i64().unwrap()),
        )),
        (JsonValue::Number(ref n), Schema::TimestampMilli) => {
            let ts = n.as_i64().unwrap();
            Ok(AvroValue::Timestamp(chrono::NaiveDateTime::from_timestamp(
                ts / 1_000,
                ts as u32 % 1_000,
            )))
        }
        (JsonValue::Number(ref n), Schema::TimestampMicro) => {
            let ts = n.as_i64().unwrap();
            Ok(AvroValue::Timestamp(chrono::NaiveDateTime::from_timestamp(
                ts / 1_000_000,
                ts as u32 % 1_000_000,
            )))
        }
        (JsonValue::Array(items), Schema::Array(inner)) => Ok(AvroValue::Array(
            items
                .iter()
                .map(|x| json_to_avro(x, inner))
                .collect::<Result<_, _>>()?,
        )),
        (JsonValue::String(s), Schema::String) => Ok(AvroValue::String(s.clone())),
        (
            JsonValue::Array(items),
            Schema::Decimal {
                precision, scale, ..
            },
        ) => {
            let bytes = match items
                .iter()
                .map(|x| x.as_i64().and_then(|x| u8::try_from(x).ok()))
                .collect::<Option<Vec<u8>>>()
            {
                Some(bytes) => bytes,
                None => return Err("decimal was not represented by byte array".into()),
            };
            Ok(AvroValue::Decimal {
                unscaled: bytes,
                precision: *precision,
                scale: *scale,
            })
        }
        (JsonValue::Object(items), Schema::Record { fields, .. }) => Ok(AvroValue::Record(
            items
                .iter()
                .zip(fields)
                .map(|((key, value), field)| Ok((key.clone(), json_to_avro(value, &field.schema)?)))
                .collect::<Result<_, String>>()?,
        )),
        (val, Schema::Union(us)) => {
            let variants = us.variants();
            let mut last_err = format!("Union schema {:?} did not match {:?}", variants, val);
            for variant in variants {
                match json_to_avro(val, variant) {
                    Ok(avro) => return Ok(avro),
                    Err(msg) => last_err = msg,
                }
            }
            Err(last_err)
        }
        _ => Err(format!(
            "unable to match JSON value to schema: {:?} vs {:?}",
            json, schema
        )),
    }
}
