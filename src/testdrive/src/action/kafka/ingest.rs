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
    key_format: Option<Format>,
    timestamp: Option<i64>,
    publish: bool,
    rows: Vec<String>,
}

enum Format {
    Avro { schema: String },
    Proto { message: String },
    Bytes,
}

enum Encoder {
    Avro {
        schema: Schema,
        schema_id: i32,
    },
    Proto {
        parser: &'static dyn Fn(&str) -> Result<protobuf::DynMessage, failure::Error>,
        validator: &'static dyn Fn(&[u8]) -> Result<Box<dyn fmt::Debug>, failure::Error>,
    },
    Bytes,
}

impl Encoder {
    fn encode_to_bytes(&self, row: &str, buf: &mut Vec<u8>) -> Result<(), String> {
        match self {
            Encoder::Avro { schema, schema_id } => {
                let val = crate::format::avro::json_to_avro(
                    &serde_json::from_str(row)
                        .map_err(|e| format!("parsing avro datum: {}", e.to_string()))?,
                    schema.top_node(),
                )?;
                // The first byte is a magic byte (0) that indicates the Confluent
                // serialization format version, and the next four bytes are a
                // 32-bit schema ID.
                //
                // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
                buf.write_u8(0).unwrap();
                buf.write_i32::<NetworkEndian>(*schema_id).unwrap();
                buf.extend(avro::to_avro_datum(&schema, val).map_err(|e| e.to_string())?);
            }
            Encoder::Proto { parser, validator } => {
                let msg =
                    parser(row).map_err(|e| format!("converting row to type {} -> {}", row, e))?;
                *buf = msg
                    .write_to_bytes()
                    .map_err(|e| format!("writing protobuf message for {}: {}", row, e))?;
                // There are a variety of `write_*` methods on `Message` that don't
                // seem to automatically do the right thing. This should always
                // succeed, otherwise there is no chance for the server.
                let _parsed = validator(&buf)
                    .map_err(|e| format!("error validating proto row={}\nerror={}", row, e))?;
            }
            Encoder::Bytes => {
                let json_string: serde_json::Value = serde_json::from_str(row)
                    .map_err(|e| format!("parsing json string datum: {}", e.to_string()))?;
                *buf = json_string.as_str().unwrap().as_bytes().to_vec();
            }
        }
        Ok(())
    }
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partition = cmd.args.opt_parse::<i32>("partition")?.unwrap_or(0);
    let format = match cmd.args.string("format")?.as_str() {
        "avro" => {
            let schema = cmd.args.string("schema")?;
            Format::Avro { schema }
        }
        "protobuf" => {
            let message = cmd.args.string("message")?;
            Format::Proto { message }
        }
        "bytes" => Format::Bytes,
        f => return Err(format!("unknown message format: {}", f)),
    };
    let key_format = cmd.args.opt_string("key-format");
    let key_schema = cmd.args.opt_string("key-schema");
    let key_format = match (key_format.map(|s| s.to_lowercase()).as_deref(), key_schema) {
        (Some("avro"), Some(key_schema)) => Some(Format::Avro { schema: key_schema }),
        (None, Some(key_schema)) => Some(Format::Avro { schema: key_schema }),
        (Some("bytes"), None) => Some(Format::Bytes),
        (None, None) => None,
        (Some(format_name), Some(_)) => {
            return Err(format!(
                "Cannot specify key-schema for key-format {}",
                format_name
            ));
        }
        (Some(format_name), None) => {
            return Err(format!("unknown key-format {}", format_name));
        }
    };
    let timestamp = cmd.args.opt_parse("timestamp")?;
    let publish = cmd.args.opt_bool("publish")?;
    cmd.args.done()?;

    Ok(IngestAction {
        topic_prefix,
        partition,
        format,
        key_format,
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
        let mut make_encoder = |format: &Format, typ| match *format {
            Format::Avro { ref schema } => {
                let schema_id = if self.publish {
                    let ccsr_subject = format!("{}-{}", topic_name, typ);
                    let schema_id = state.tokio_runtime.block_on(
                        state
                            .ccsr_client
                            .publish_schema(&ccsr_subject, &schema)
                            .map_err(|e| format!("schema registry error: {}", e)),
                    )?;
                    schema_id
                } else {
                    1
                };
                let schema = interchange::avro::parse_schema(&schema)
                    .map_err(|e| format!("parsing avro schema: {}", e))?;
                Ok(Encoder::Avro { schema, schema_id })
            }
            Format::Proto { ref message } => match message.as_str() {
                ".Struct" => Ok(Encoder::Proto {
                    parser: &protobuf::json_to_protobuf::<Struct>,
                    validator: &protobuf::decode::<Struct>,
                }),
                ".Batch" => Ok(Encoder::Proto {
                    parser: &protobuf::json_to_protobuf::<Batch>,
                    validator: &protobuf::decode::<Batch>,
                }),
                _ => Err(format!("unknown testdrive protobuf message: {}", message)),
            },
            Format::Bytes => Ok(Encoder::Bytes),
        };
        let value_encoder = make_encoder(&self.format, "value")?;
        let key_encoder = match &self.key_format {
            None => None,
            Some(f) => Some(make_encoder(f, "key")?),
        };

        let futs = FuturesUnordered::new();
        for row in &self.rows {
            let mut val_buf = Vec::new();
            let mut key_buf = Vec::new();
            let (key_row, val_row) = if key_encoder.is_some() {
                let mut tokens = Deserializer::from_str(&row).into_iter::<Value>();
                let key_row = tokens.next();
                let val_row = tokens.next();

                if tokens.next().is_some() || key_row.is_none() || val_row.is_none() {
                    return Err(format!(
                        "invalid row: {}; testdrive expects two json objects",
                        row
                    ));
                }

                (
                    Some(
                        key_row
                            .unwrap()
                            .map_err(|e| format!("parsing avro datum: {}", e.to_string()))?
                            .to_string(),
                    ),
                    val_row
                        .unwrap()
                        .map_err(|e| format!("parsing avro datum: {}", e.to_string()))?
                        .to_string(),
                )
            } else {
                (None, row.clone())
            };

            value_encoder.encode_to_bytes(&val_row, &mut val_buf)?;
            if let Some(key_encoder) = &key_encoder {
                key_encoder.encode_to_bytes(&key_row.unwrap(), &mut key_buf)?;
            }

            let mut record: FutureRecord<_, _> = FutureRecord::to(&topic_name)
                .payload(&val_buf)
                .partition(self.partition);

            if self.key_format.is_some() {
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
