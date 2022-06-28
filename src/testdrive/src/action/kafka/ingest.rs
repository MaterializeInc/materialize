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

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use byteorder::{NetworkEndian, WriteBytesExt};
use futures::stream::{FuturesUnordered, StreamExt};
use maplit::hashmap;
use prost::Message;
use prost_reflect::{DynamicMessage, FileDescriptor, MessageDescriptor};
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::FutureRecord;
use serde::de::DeserializeOwned;
use tokio::fs;

use crate::action::{self, Action, ControlFlow, State};
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
    start_iteration: isize,
    repeat: isize,
    headers: Option<Vec<(String, Option<String>)>>,
    omit_key: bool,
    omit_value: bool,
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
        schema_id_subject: Option<String>,
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
    fn decode_json<R, T>(row: R) -> Result<Option<T>, anyhow::Error>
    where
        R: Read,
        T: DeserializeOwned,
    {
        let deserializer = serde_json::Deserializer::from_reader(row);
        deserializer
            .into_iter()
            .next()
            .transpose()
            .context("parsing json")
    }

    fn transcode<R>(&self, mut row: R) -> Result<Option<Vec<u8>>, anyhow::Error>
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
                    out.extend(avro::to_avro_datum(&schema, val)?);
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
                if let Some(val) = Self::decode_json::<_, serde_json::Value>(row)? {
                    let message = DynamicMessage::deserialize(message.clone(), val)
                        .context("parsing protobuf JSON")?;
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
                    message.encode(&mut out)?;
                    Ok(Some(out))
                } else {
                    Ok(None)
                }
            }
            Transcoder::Bytes { terminator } => {
                let mut out = vec![];
                match terminator {
                    Some(t) => {
                        row.read_until(*t, &mut out)?;
                        out.pop();
                    }
                    None => {
                        row.read_to_end(&mut out)?;
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

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, anyhow::Error> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partition = cmd.args.opt_parse::<i32>("partition")?;
    let start_iteration = cmd.args.opt_parse::<isize>("start-iteration")?.unwrap_or(0);
    let repeat = cmd.args.opt_parse::<isize>("repeat")?.unwrap_or(1);
    let publish = cmd.args.opt_bool("publish")?.unwrap_or(false);
    let omit_key = cmd.args.opt_bool("omit-key")?.unwrap_or(false);
    let omit_value = cmd.args.opt_bool("omit-value")?.unwrap_or(false);
    let format = match cmd.args.string("format")?.as_str() {
        "avro" => Format::Avro {
            schema: cmd.args.string("schema")?,
            confluent_wire_format: cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(true),
        },
        "protobuf" => {
            let descriptor_file = cmd.args.string("descriptor-file")?;
            let message = cmd.args.string("message")?;
            Format::Protobuf {
                descriptor_file,
                message,
                // This was introduced after the avro format's confluent-wire-format, so it defaults to
                // false
                confluent_wire_format: cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(false),
                schema_id_subject: cmd.args.opt_string("schema-id-subject"),
                schema_message_id: cmd.args.opt_parse::<u8>("schema-message-id")?.unwrap_or(0),
            }
        }
        "bytes" => Format::Bytes { terminator: None },
        f => bail!("unknown format: {}", f),
    };
    let key_format = match cmd.args.opt_string("key-format").as_deref() {
        Some("avro") => Some(Format::Avro {
            schema: cmd.args.string("key-schema")?,
            confluent_wire_format: cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(true),
        }),
        Some("protobuf") => {
            let descriptor_file = cmd.args.string("key-descriptor-file")?;
            let message = cmd.args.string("key-message")?;
            Some(Format::Protobuf {
                descriptor_file,
                message,
                confluent_wire_format: cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(false),
                schema_id_subject: cmd.args.opt_string("key-schema-id-subject"),
                schema_message_id: cmd
                    .args
                    .opt_parse::<u8>("key-schema-message-id")?
                    .unwrap_or(0),
            })
        }
        Some("bytes") => Some(Format::Bytes {
            terminator: match cmd.args.opt_parse::<char>("key-terminator")? {
                Some(c) if c.is_ascii() => Some(c as u8),
                Some(_) => bail!("key terminator must be single ASCII character"),
                None => Some(b':'),
            },
        }),
        Some(f) => bail!("unknown key format: {}", f),
        None => None,
    };

    let timestamp = cmd.args.opt_parse("timestamp")?;

    use serde_json::Value;
    let headers = if let Some(headers_val) = cmd.args.opt_parse::<serde_json::Value>("headers")? {
        let mut headers = Vec::new();
        let headers_maps = match headers_val {
            Value::Array(values) => {
                let mut headers_map = Vec::new();
                for value in values {
                    if let Value::Object(m) = value {
                        headers_map.push(m)
                    } else {
                        bail!("`headers` array values must be maps")
                    }
                }
                headers_map
            }
            Value::Object(v) => vec![v],
            _ => bail!("`headers` must be a map or an array"),
        };

        for headers_map in headers_maps {
            for (k, v) in headers_map.iter() {
                if let Value::String(val) = v {
                    headers.push((k.clone(), Some(val.clone())));
                } else if let Value::Null = v {
                    headers.push((k.clone(), None));
                } else {
                    bail!("`headers` must have string or null values")
                }
            }
        }
        Some(headers)
    } else {
        None
    };

    cmd.args.done()?;

    if publish
        && !matches!(format, Format::Avro { .. })
        && !matches!(key_format, Some(Format::Avro { .. }))
    {
        bail!("publish=true is invalid unless format=avro or key-format=avro");
    }

    Ok(IngestAction {
        topic_prefix,
        partition,
        format,
        key_format,
        timestamp,
        publish,
        rows: cmd.input,
        start_iteration,
        repeat,
        headers,
        omit_key,
        omit_value,
    })
}

#[async_trait]
impl Action for IngestAction {
    async fn undo(&self, state: &mut State) -> Result<(), anyhow::Error> {
        if self.publish {
            let subjects = state
                .ccsr_client
                .list_subjects()
                .await
                .context("listing schema registry subjects")?;

            let stale_subjects: Vec<_> = subjects
                .iter()
                .filter(|s| s.starts_with(&self.topic_prefix))
                .collect();

            for subject in stale_subjects {
                println!("Deleting stale schema registry subject {}", subject);
                match state.ccsr_client.delete_subject(&subject).await {
                    Ok(()) | Err(mz_ccsr::DeleteError::SubjectNotFound) => (),
                    Err(e) => return Err(e.into()),
                }
            }
        }

        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let topic_name = &format!("{}-{}", self.topic_prefix, state.seed);
        println!(
            "Ingesting data into Kafka topic {} with repeat {}",
            topic_name, self.repeat
        );

        let ccsr_client = &state.ccsr_client;
        let temp_path = &state.temp_path;
        let make_transcoder = |format, typ| async move {
            let ccsr_subject = format!("{}-{}", topic_name, typ);
            match format {
                Format::Avro {
                    schema,
                    confluent_wire_format,
                } => {
                    let schema_id = if self.publish {
                        let schema_id = ccsr_client
                            .publish_schema(&ccsr_subject, &schema, mz_ccsr::SchemaType::Avro, &[])
                            .await
                            .context("publishing to schema registry")?;
                        schema_id
                    } else {
                        1
                    };
                    let schema = avro::parse_schema(&schema)
                        .with_context(|| format!("parsing avro schema: {}", schema))?;
                    Ok::<_, anyhow::Error>(Transcoder::Avro {
                        schema,
                        schema_id,
                        confluent_wire_format,
                    })
                }
                Format::Protobuf {
                    descriptor_file,
                    message,
                    confluent_wire_format,
                    schema_id_subject,
                    schema_message_id,
                } => {
                    let schema_id = if confluent_wire_format {
                        ccsr_client
                            .get_schema_by_subject(
                                schema_id_subject.as_deref().unwrap_or(&ccsr_subject),
                            )
                            .await
                            .context("fetching schema from registry")?
                            .id
                    } else {
                        0
                    };

                    let bytes = fs::read(temp_path.join(descriptor_file))
                        .await
                        .context("reading protobuf descriptor file")?;
                    let fd = FileDescriptor::decode(&*bytes)
                        .context("parsing protobuf descriptor file")?;
                    let message = fd
                        .get_message_by_name(&message)
                        .ok_or_else(|| anyhow!("unknown message name {}", message))?;
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

        for iteration in self.start_iteration..(self.start_iteration + self.repeat) {
            let iter = &mut self.rows.iter().peekable();

            for row in iter {
                let row = action::substitute_vars(
                    row,
                    &hashmap! { "kafka-ingest.iteration".into() => iteration.to_string() },
                    &None,
                    false,
                )?;
                let mut row = row.as_bytes();
                let key = match (self.omit_key, &key_transcoder) {
                    (true, _) => None,
                    (false, None) => None,
                    (false, Some(kt)) => kt.transcode(&mut row)?,
                };
                let value = if self.omit_value {
                    None
                } else {
                    value_transcoder
                        .transcode(&mut row)
                        .with_context(|| format!("parsing row: {}", String::from_utf8_lossy(row)))?
                };
                let producer = &state.kafka_producer;
                let timeout = cmp::max(state.default_timeout, Duration::from_secs(1));
                let headers = self.headers.clone();
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
                    if let Some(headers) = headers {
                        let mut rd_meta = OwnedHeaders::new();
                        for (k, v) in &headers {
                            rd_meta = rd_meta.insert(Header {
                                key: k,
                                value: v.as_deref(),
                            });
                        }
                        record = record.headers(rd_meta);
                    }
                    producer.send(record, timeout).await
                });
            }

            // Reap the futures thus produced periodically or after the last iteration
            if iteration % INGEST_BATCH_SIZE == 0
                || iteration == (self.start_iteration + self.repeat - 1)
            {
                while let Some(res) = futs.next().await {
                    res.map_err(|(e, _message)| e)?;
                }
            }
        }
        Ok(ControlFlow::Continue)
    }
}
