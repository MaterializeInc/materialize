// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use avro::Schema;
use byteorder::{NetworkEndian, WriteBytesExt};
use futures::executor::block_on;
use futures::future::{self, TryFutureExt};
use futures::stream::{FuturesUnordered, TryStreamExt};
use rdkafka::producer::FutureRecord;
use serde_json::{Deserializer, Value};

use crate::action::{Action, State};
use crate::format::protobuf;
use crate::format::protobuf::native::{Batch, Struct};
use crate::parser::BuiltinCommand;

pub struct IngestAction {
    topic_prefix: String,
    partition: i32,
    format: Format,
    timestamp: Option<i64>,
    publish: bool,
    rows: Vec<String>,
}

enum Format {
    Avro {
        key_schema: Option<String>,
        value_schema: String,
    },
    Proto {
        message: String,
    },
    Bytes,
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partition = cmd.args.opt_parse::<i32>("partition")?.unwrap_or(0);
    let format = match cmd.args.string("format")?.as_str() {
        "avro" => {
            let key_schema = cmd.args.opt_string("key-schema");
            let value_schema = cmd.args.string("schema")?;
            Format::Avro {
                key_schema,
                value_schema,
            }
        }
        "protobuf" => {
            let message = cmd.args.string("message")?;
            Format::Proto { message }
        }
        "bytes" => Format::Bytes,
        f => return Err(format!("unknown message format: {}", f)),
    };
    let timestamp = cmd.args.opt_parse("timestamp")?;
    let publish = cmd.args.opt_bool("publish")?;
    cmd.args.done()?;

    Ok(IngestAction {
        topic_prefix,
        partition,
        format,
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
        enum Encoder {
            Avro {
                key_encode: Option<(Schema, i32)>,
                value_schema: Schema,
                value_schema_id: i32,
            },
            Proto {
                parser: &'static dyn Fn(&str) -> Result<protobuf::DynMessage, failure::Error>,
                validator: &'static dyn Fn(&[u8]) -> Result<Box<dyn fmt::Debug>, failure::Error>,
            },
            Bytes,
        }

        let topic_name = format!("{}-{}", self.topic_prefix, state.seed);
        if !state.kafka_topics.contains_key(&topic_name) {
            return Err(format!(
                "topic {} not created by kafka-create-topic",
                topic_name
            ));
        }
        println!(
            "Ingesting data into partition {} of Kafka topic {}",
            self.partition, topic_name
        );
        let encoder = match &self.format {
            Format::Avro {
                key_schema,
                value_schema,
            } => {
                let value_schema_id = if self.publish {
                    let ccsr_subject = format!("{}-value", topic_name);
                    let schema_id = state.tokio_runtime.block_on(
                        state
                            .ccsr_client
                            .publish_schema(&ccsr_subject, &value_schema)
                            .map_err(|e| format!("schema registry error: {}", e)),
                    )?;
                    schema_id
                } else {
                    1
                };
                let key_encode = if let Some(key_schema) = key_schema {
                    let schema = interchange::avro::parse_schema(&key_schema)
                        .map_err(|e| format!("parsing avro schema: {}", e))?;
                    if self.publish {
                        let key_subject = format!("{}-key", topic_name);
                        let schema_id = state.tokio_runtime.block_on(
                            state
                                .ccsr_client
                                .publish_schema(&key_subject, &key_schema)
                                .map_err(|e| format!("schema registry error: {}", e)),
                        )?;
                        Some((schema, schema_id))
                    } else {
                        Some((schema, 2))
                    }
                } else {
                    None
                };
                let value_schema = interchange::avro::parse_schema(&value_schema)
                    .map_err(|e| format!("parsing avro schema: {}", e))?;
                Encoder::Avro {
                    key_encode,
                    value_schema,
                    value_schema_id,
                }
            }
            Format::Proto { message } => match message.as_ref() {
                ".Struct" => Encoder::Proto {
                    parser: &protobuf::json_to_protobuf::<Struct>,
                    validator: &protobuf::decode::<Struct>,
                },
                ".Batch" => Encoder::Proto {
                    parser: &protobuf::json_to_protobuf::<Batch>,
                    validator: &protobuf::decode::<Batch>,
                },
                _ => return Err(format!("unknown testdrive protobuf message: {}", message)),
            },
            Format::Bytes => Encoder::Bytes,
        };

        let futs = FuturesUnordered::new();
        for row in &self.rows {
            let mut val_buf = Vec::new();
            let mut key_buf = Vec::new();
            match &encoder {
                Encoder::Avro {
                    key_encode,
                    value_schema,
                    value_schema_id,
                } => {
                    let (key_row, val_row) = if key_encode.is_some() {
                        let mut tokens = Deserializer::from_str(row).into_iter::<Value>();
                        let key_row = tokens.next();
                        let val_row = tokens.next();

                        if tokens.next().is_some() || key_row.is_none() || val_row.is_none() {
                            return Err(format!(
                                "invalid row: {}; testdrive expects two json objects",
                                row
                            ));
                        }

                        (Some(key_row.unwrap()), val_row.unwrap())
                    } else {
                        (None, serde_json::from_str(row))
                    };
                    let val = crate::format::avro::json_to_avro(
                        &val_row.map_err(|e| format!("parsing avro datum: {}", e.to_string()))?,
                        value_schema.top_node(),
                    )?;
                    // The first byte is a magic byte (0) that indicates the Confluent
                    // serialization format version, and the next four bytes are a
                    // 32-bit schema ID.
                    //
                    // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
                    val_buf.write_u8(0).unwrap();
                    val_buf
                        .write_i32::<NetworkEndian>(*value_schema_id)
                        .unwrap();
                    val_buf.extend(
                        avro::to_avro_datum(&value_schema, val).map_err(|e| e.to_string())?,
                    );
                    if let Some((key_schema, key_schema_id)) = key_encode {
                        let key = crate::format::avro::json_to_avro(
                            &key_row
                                .unwrap()
                                .map_err(|e| format!("parsing avro datum: {}", e.to_string()))?,
                            key_schema.top_node(),
                        )?;
                        key_buf.write_u8(0).unwrap();
                        key_buf.write_i32::<NetworkEndian>(*key_schema_id).unwrap();
                        key_buf.extend(
                            avro::to_avro_datum(&key_schema, key).map_err(|e| e.to_string())?,
                        );
                    }
                }
                Encoder::Proto { parser, validator } => {
                    let msg = parser(row)
                        .map_err(|e| format!("converting row to type {} -> {}", row, e))?;
                    val_buf = msg
                        .write_to_bytes()
                        .map_err(|e| format!("writing protobuf message for {}: {}", row, e))?;
                    // There are a variety of `write_*` methods on `Message` that don't
                    // seem to automatically do the right thing. This should always
                    // succeed, otherwise there is no chance for the server.
                    let _parsed = validator(&val_buf)
                        .map_err(|e| format!("error validating proto row={}\nerror={}", row, e))?;
                }
                Encoder::Bytes => {
                    val_buf = row.as_bytes().to_vec();
                }
            }

            let mut record: FutureRecord<_, _> = FutureRecord::to(&topic_name)
                .payload(&val_buf)
                .partition(self.partition);

            if let Format::Avro {
                key_schema: Some(_key_schema),
                ..
            } = &self.format
            {
                record = record.key(&key_buf);
            }
            if let Some(timestamp) = self.timestamp {
                record = record.timestamp(timestamp);
            }
            futs.push(state.kafka_producer.send(record, 1000 /* block_ms */));
        }
        block_on(futs.map_err(|e| e.to_string()).try_for_each(|r| match r {
            Ok(_) => future::ok(()),
            Err((e, _)) => future::err(e.to_string()),
        }))
    }
}
