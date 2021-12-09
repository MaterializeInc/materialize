// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::io::{BufRead, Read};
use std::time::Duration;

use async_trait::async_trait;
use byteorder::{NetworkEndian, WriteBytesExt};
use futures::stream::{FuturesUnordered, StreamExt};
use maplit::hashmap;
use protobuf::descriptor::FileDescriptorSet;
use protobuf::reflect::FileDescriptor;
use protobuf::reflect::MessageDescriptor;
use protobuf::Message;
use rdkafka::producer::FutureRecord;
use serde::de::DeserializeOwned;
use tokio::fs;

use ore::display::DisplayExt;
use ore::result::ResultExt;

use crate::action::{substitute_vars, Action, State};
use crate::format::avro::{self, Schema};
use crate::format::bytes;
use crate::parser::BuiltinCommand;

const INGEST_BATCH_SIZE: isize = 10000;

pub struct IngestAction {
    topic_prefix: String,
    partition: Option<i32>,
    format: Format,
    key_format: Option<Format>,
    timestamp: Option<i64>,
    publish: bool,
    rows: Vec<String>,
    repeat: isize,
}

#[derive(Clone)]
enum Format {
    Avro {
        schema: String,
        confluent_wire_format: bool,
    },
    Protobuf {
        descriptor_file: String,
        message: String,
        confluent_wire_format: bool,
        schema_id: i32,
        schema_message_id: u8,
    },
    Bytes {
        terminator: Option<u8>,
    },
}

enum Transcoder {
    Avro {
        schema: Schema,
        schema_id: i32,
        confluent_wire_format: bool,
    },
    Protobuf {
        message: MessageDescriptor,
        confluent_wire_format: bool,
        schema_id: i32,
        schema_message_id: u8,
    },
    Bytes {
        terminator: Option<u8>,
    },
}

impl Transcoder {
    fn decode_json<R, T>(row: R) -> Result<Option<T>, String>
    where
        R: Read,
        T: DeserializeOwned,
    {
        let deserializer = serde_json::Deserializer::from_reader(row);
        match deserializer.into_iter().next() {
            None => Ok(None),
            Some(r) => r.map(Some).map_err(|e| format!("parsing json: {:#}", e)),
        }
    }

    fn transcode<R>(&self, mut row: R) -> Result<Option<Vec<u8>>, String>
    where
        R: BufRead,
    {
        match self {
            Transcoder::Avro {
                schema,
                schema_id,
                confluent_wire_format,
            } => {
                if let Some(val) = Self::decode_json(row)? {
                    let val = avro::from_json(&val, schema.top_node())?;
                    let mut out = vec![];
                    if *confluent_wire_format {
                        // The first byte is a magic byte (0) that indicates the Confluent
                        // serialization format version, and the next four bytes are a
                        // 32-bit schema ID.
                        //
                        // https://docs.confluent.io/3.3.0/schema-registry/docs/serializer-formatter.html#wire-format
                        out.write_u8(0).unwrap();
                        out.write_i32::<NetworkEndian>(*schema_id).unwrap();
                    }
                    out.extend(avro::to_avro_datum(&schema, val).map_err_to_string()?);
                    Ok(Some(out))
                } else {
                    Ok(None)
                }
            }
            Transcoder::Protobuf {
                message,
                confluent_wire_format,
                schema_id,
                schema_message_id,
            } => {
                if let Some(val) = Self::decode_json::<_, Box<serde_json::value::RawValue>>(row)? {
                    let message = protobuf::json::parse_dynamic_from_str(message, val.get())
                        .map_err(|e| format!("parsing protobuf JSON: {}", e))?;
                    let mut out = vec![];
                    if *confluent_wire_format {
                        // See: https://github.com/MaterializeInc/materialize/issues/9250
                        // The first byte is a magic byte (0) that indicates the Confluent
                        // serialization format version, and the next four bytes are a
                        // 32-bit schema ID, which we default to something fun.
                        // And, as we only support single-message proto files for now,
                        // we also set the following message id to 0.
                        out.write_u8(0).unwrap();
                        out.write_i32::<NetworkEndian>(*schema_id).unwrap();
                        out.write_u8(*schema_message_id).unwrap();
                    }
                    message
                        .write_to_vec_dyn(&mut out)
                        .map_err(|e| e.to_string())?;
                    Ok(Some(out))
                } else {
                    Ok(None)
                }
            }
            Transcoder::Bytes { terminator } => {
                let mut out = vec![];
                match terminator {
                    Some(t) => {
                        row.read_until(*t, &mut out).map_err_to_string()?;
                        out.pop();
                    }
                    None => {
                        row.read_to_end(&mut out).map_err_to_string()?;
                    }
                }
                if out.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(bytes::unescape(&out)?))
                }
            }
        }
    }
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partition = cmd.args.opt_parse::<i32>("partition")?;
    let repeat = cmd.args.opt_parse::<isize>("repeat")?.unwrap_or(1);
    let publish = cmd.args.opt_bool("publish")?.unwrap_or(false);
    let format = match cmd.args.string("format")?.as_str() {
        "avro" => Format::Avro {
            schema: cmd.args.string("schema")?,
            confluent_wire_format: cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(true),
        },
        "protobuf" => {
            let descriptor_file = cmd.args.string("descriptor-file")?;
            // This was introduced after the avro format's confluent-wire-format, so it defaults to
            // false
            let message = cmd.args.string("message")?;
            validate_protobuf_message_name(&message)?;
            Format::Protobuf {
                descriptor_file,
                message,
                confluent_wire_format: cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(false),
                schema_id: cmd.args.opt_parse::<i32>("schema-id")?.unwrap_or(885),
                schema_message_id: cmd.args.opt_parse::<u8>("schema-message-id")?.unwrap_or(0),
            }
        }
        "bytes" => Format::Bytes { terminator: None },
        f => return Err(format!("unknown format: {}", f)),
    };
    let key_format = match cmd.args.opt_string("key-format").as_deref() {
        Some("avro") => Some(Format::Avro {
            schema: cmd.args.string("key-schema")?,
            confluent_wire_format: cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(true),
        }),
        Some("protobuf") => {
            let descriptor_file = cmd.args.string("key-descriptor-file")?;
            let message = cmd.args.string("key-message")?;
            validate_protobuf_message_name(&message)?;
            Some(Format::Protobuf {
                descriptor_file,
                message,
                confluent_wire_format: cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(false),
                schema_id: cmd.args.opt_parse::<i32>("schema-id")?.unwrap_or(885),
                schema_message_id: cmd.args.opt_parse::<u8>("schema-message-id")?.unwrap_or(0),
            })
        }
        Some("bytes") => Some(Format::Bytes {
            terminator: match cmd.args.opt_parse::<char>("key-terminator")? {
                Some(c) if c.is_ascii() => Some(c as u8),
                Some(_) => return Err("key terminator must be single ASCII character".into()),
                None => Some(b':'),
            },
        }),
        Some(f) => return Err(format!("unknown key format: {}", f)),
        None => None,
    };
    let timestamp = cmd.args.opt_parse("timestamp")?;
    cmd.args.done()?;

    if publish
        && !matches!(format, Format::Avro { .. })
        && !matches!(key_format, Some(Format::Avro { .. }))
    {
        return Err("publish=true is invalid unless format=avro or key-format=avro".into());
    }

    Ok(IngestAction {
        topic_prefix,
        partition,
        format,
        key_format,
        timestamp,
        publish,
        rows: cmd.input,
        repeat,
    })
}

fn validate_protobuf_message_name(message: &str) -> Result<(), String> {
    if !message.starts_with('.') {
        return Err(format!(
            "protobuf message name must start with a dot: {}",
            message
        ));
    }
    Ok(())
}

#[async_trait]
impl Action for IngestAction {
    async fn undo(&self, state: &mut State) -> Result<(), String> {
        if self.publish {
            let subjects = state
                .ccsr_client
                .list_subjects()
                .await
                .map_err(|e| format!("unable to list subjects in schema registry: {}", e))?;

            let stale_subjects: Vec<_> = subjects
                .iter()
                .filter(|s| s.starts_with(&self.topic_prefix))
                .collect();

            for subject in stale_subjects {
                println!("Deleting stale schema registry subject {}", subject);
                match state.ccsr_client.delete_subject(&subject).await {
                    Ok(()) | Err(ccsr::DeleteError::SubjectNotFound) => (),
                    Err(e) => return Err(e.to_string()),
                }
            }
        }

        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let topic_name = &format!("{}-{}", self.topic_prefix, state.seed);
        println!("Ingesting data into Kafka topic {}", topic_name);

        let ccsr_client = &state.ccsr_client;
        let temp_path = &state.temp_path;
        let make_transcoder = |format, typ| async move {
            match format {
                Format::Avro {
                    schema,
                    confluent_wire_format,
                } => {
                    let schema_id = if self.publish {
                        let ccsr_subject = format!("{}-{}", topic_name, typ);
                        let schema_id = ccsr_client
                            .publish_schema(&ccsr_subject, &schema, ccsr::SchemaType::Avro, &[])
                            .await
                            .map_err(|e| format!("schema registry error: {}", e))?;
                        schema_id
                    } else {
                        1
                    };
                    let schema = avro::parse_schema(&schema)
                        .map_err(|e| format!("parsing avro schema: {}\nschema={}", e, schema))?;
                    Ok::<_, String>(Transcoder::Avro {
                        schema,
                        schema_id,
                        confluent_wire_format,
                    })
                }
                Format::Protobuf {
                    descriptor_file,
                    message,
                    confluent_wire_format,
                    schema_id,
                    schema_message_id,
                } => {
                    let bytes = fs::read(temp_path.join(descriptor_file))
                        .await
                        .map_err(|e| format!("reading protobuf descriptor file: {}", e))?;
                    let fds = FileDescriptorSet::parse_from_bytes(&bytes)
                        .map_err(|e| format!("parsing protobuf descriptor file: {}", e))?;
                    let fds = FileDescriptor::new_dynamic_fds(fds.file);
                    let message = fds
                        .iter()
                        .find_map(|fd| fd.message_by_full_name(&message))
                        .ok_or_else(|| format!("unknown message name {}", message))?;
                    Ok(Transcoder::Protobuf {
                        message,
                        confluent_wire_format,
                        schema_id,
                        schema_message_id,
                    })
                }
                Format::Bytes { terminator } => Ok(Transcoder::Bytes { terminator }),
            }
        };

        let value_transcoder = make_transcoder(self.format.clone(), "value").await?;
        let key_transcoder = match self.key_format.clone() {
            None => None,
            Some(f) => Some(make_transcoder(f, "key").await?),
        };

        let mut futs = FuturesUnordered::new();

        for iteration in 0..self.repeat {
            for row in &self.rows {
                let row = substitute_vars(
                    row,
                    &hashmap! { "kafka-ingest.iteration".into() => iteration.to_string() },
                    &None,
                )?;
                let mut row = row.as_bytes();
                let key = match &key_transcoder {
                    None => None,
                    Some(kt) => kt.transcode(&mut row)?,
                };
                let value = value_transcoder
                    .transcode(&mut row)
                    .map_err(|e| format!("parsing row: {} {}", String::from_utf8_lossy(row), e))?;
                let producer = &state.kafka_producer;
                let timeout = cmp::max(state.default_timeout, Duration::from_secs(1));
                futs.push(async move {
                    let mut record: FutureRecord<_, _> = FutureRecord::to(topic_name);

                    if let Some(partition) = self.partition {
                        record = record.partition(partition);
                    }
                    if let Some(key) = &key {
                        record = record.key(key);
                    }
                    if let Some(value) = &value {
                        record = record.payload(value);
                    }
                    if let Some(timestamp) = self.timestamp {
                        record = record.timestamp(timestamp);
                    }
                    producer.send(record, timeout).await
                });
            }

            if iteration % INGEST_BATCH_SIZE == 0 || iteration == (self.repeat - 1) {
                while let Some(res) = futs.next().await {
                    res.map_err(|(e, _message)| e.to_string_alt())?;
                }
            }
        }
        Ok(())
    }
}
