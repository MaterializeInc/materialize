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
use lazy_static::lazy_static;
use maplit::hashmap;
use ore::display::DisplayExt;
use ore::result::ResultExt;
use rdkafka::producer::FutureRecord;
use regex::Regex;
use serde::de::DeserializeOwned;

use crate::action::{substitute_vars, Action, State};
use crate::format::avro::{self, Schema};
use crate::format::bytes;
use crate::format::protobuf;
use crate::parser::BuiltinCommand;

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
        message: protobuf::MessageType,
        schema: Option<String>,
        confluent_wire_format: bool,
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
        message: protobuf::MessageType,
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
            Transcoder::Protobuf { message } => {
                fn convert<T: protobuf::Message>(decoded: T) -> Box<dyn protobuf::Message> {
                    let d: Box<dyn protobuf::Message> = Box::new(decoded);
                    d
                }
                let val = match message {
                    protobuf::MessageType::Batch => {
                        Self::decode_json::<_, protobuf::gen::billing::Batch>(row)?.map(convert)
                    }
                    protobuf::MessageType::Measurement => {
                        Self::decode_json::<_, protobuf::gen::billing::Measurement>(row)?
                            .map(convert)
                    }
                    protobuf::MessageType::Struct => {
                        Self::decode_json::<_, protobuf::gen::simple::Struct>(row)?.map(convert)
                    }
                    protobuf::MessageType::SimpleId => {
                        Self::decode_json::<_, protobuf::gen::simple::SimpleId>(row)?.map(convert)
                    }
                    protobuf::MessageType::NestedOuter => {
                        Self::decode_json::<_, protobuf::gen::nested::NestedOuter>(row)?
                            .map(convert)
                    }
                    protobuf::MessageType::SimpleNestedOuter => {
                        Self::decode_json::<_, protobuf::gen::nested::SimpleNestedOuter>(row)?
                            .map(convert)
                    }
                    protobuf::MessageType::Imported => {
                        Self::decode_json::<_, protobuf::gen::imported::Imported>(row)?.map(convert)
                    }
                    protobuf::MessageType::TimestampId => {
                        Self::decode_json::<_, protobuf::gen::well_known_imports::TimestampId>(row)?
                            .map(convert)
                    }
                };
                let val = if let Some(decoded) = val {
                    decoded
                } else {
                    return Ok(None);
                };
                Ok(Some(val.write_to_bytes().map_err_to_string()?))
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
                Ok(Some(bytes::unescape(&out)?))
            }
        }
    }
}

pub fn build_ingest(mut cmd: BuiltinCommand) -> Result<IngestAction, String> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partition = cmd.args.opt_parse::<i32>("partition")?;
    let repeat = cmd.args.opt_parse::<isize>("repeat")?.unwrap_or(1);
    let format = match cmd.args.string("format")?.as_str() {
        "avro" => Format::Avro {
            schema: cmd.args.string("schema")?,
            confluent_wire_format: cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(true),
        },
        "protobuf" => {
            let message = cmd.args.parse("message")?;
            let schema = cmd.args.opt_parse::<String>("schema")?;
            let confluent_wire_format = cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(true);
            Format::Protobuf {
                message,
                schema,
                confluent_wire_format,
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
            let message = cmd.args.parse("key-message")?;
            let schema = cmd.args.opt_parse::<String>("key-schema")?;
            let confluent_wire_format = cmd.args.opt_bool("confluent-wire-format")?.unwrap_or(true);
            Some(Format::Protobuf {
                message,
                schema,
                confluent_wire_format,
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
    let publish = cmd.args.opt_bool("publish")?.unwrap_or(false);
    cmd.args.done()?;

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
        let ccsr_client = &state.ccsr_client;
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
                    message, schema, ..
                } => {
                    // TODO(chae): once this is fixed https://github.com/stepancheg/rust-protobuf/issues/576,
                    // use protobuf-codegen-pure, an actual parser, rather than a regex.  For now, this is
                    // just used in testdrive so it's not worth hand-rolling something for now.
                    lazy_static! {
                        static ref PROTO_IMPORT_REGEX: Regex =
                            Regex::new(r#"import\s+["'](.*)["']"#).unwrap();
                    }
                    if self.publish {
                        let schema = schema.expect("schema");
                        let ccsr_subject = format!("{}-{}", topic_name, typ);
                        let schema_refs: Vec<_> = futures::future::join_all(
                            PROTO_IMPORT_REGEX.captures_iter(&schema).map(|capture| {
                                let imp = capture.get(1).expect("REGEX HAS ONE GROUP").as_str();
                                ccsr_client.get_subject(imp)
                            }),
                        )
                        .await
                        .into_iter()
                        .filter_map(|r| {
                            r.ok().map(|subject| ccsr::SchemaReference {
                                name: subject.name.clone(),
                                subject: subject.name,
                                version: subject.version,
                            })
                        })
                        .collect();

                        ccsr_client
                            .publish_schema(
                                &ccsr_subject,
                                &schema,
                                ccsr::SchemaType::Protobuf,
                                &schema_refs,
                            )
                            .await
                            .map_err(|e| format!("schema registry error: {}", e))?;
                    }
                    Ok(Transcoder::Protobuf { message })
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
        }
        while let Some(res) = futs.next().await {
            res.map_err(|(e, _message)| e.to_string_alt())?;
        }
        Ok(())
    }
}
