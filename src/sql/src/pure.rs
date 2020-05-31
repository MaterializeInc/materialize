// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Statement purification.

use std::collections::HashMap;

use failure::{bail, ResultExt};
use tokio::io::AsyncBufReadExt;

use repr::strconv;
use sql_parser::ast::{AvroSchema, Connector, CsrSeed, Format, Ident, Statement};

use crate::kafka_util;
use crate::normalize;

/// Removes dependencies on external state from `stmt`: inlining schemas in
/// files, fetching schemas from registries, and so on. The [`Statement`]
/// returned from this function will be valid to pass to `Plan`.
///
/// Note that purification is asynchronous, and may take an unboundedly long
/// time to complete.

pub async fn purify_statement(mut stmt: Statement) -> Result<Statement, failure::Error> {
    if let Statement::CreateSource {
        col_names,
        connector,
        format,
        with_options,
        envelope,
        ..
    } = &mut stmt
    {
        let with_options_map = normalize::with_options(with_options);
        let mut config_options = HashMap::new();

        let mut file = None;
        match connector {
            Connector::Kafka { broker, .. } => {
                if !broker.contains(':') {
                    *broker += ":9092";
                }

                // Verify that the provided security options are valid and then test them.
                config_options = kafka_util::extract_config(&mut with_options_map.clone())?;
                kafka_util::test_config(&config_options)?;
            }
            Connector::AvroOcf { path, .. } => {
                let path = path.clone();
                let f = std::fs::File::open(path)?;
                let r = avro::Reader::new(f)?;
                if !with_options_map.contains_key("reader_schema") {
                    let schema = serde_json::to_string(r.writer_schema()).unwrap();
                    with_options.push(sql_parser::ast::SqlOption {
                        name: sql_parser::ast::Ident::new("reader_schema"),
                        value: sql_parser::ast::Value::SingleQuotedString(schema),
                    });
                }
            }
            // Report an error if a file cannot be opened.
            Connector::File { path, .. } => {
                let path = path.clone();
                file = Some(tokio::fs::File::open(path).await?);
            }
            _ => (),
        }

        purify_format(format, connector, col_names, file, &config_options).await?;
        if let sql_parser::ast::Envelope::Upsert(format) = envelope {
            purify_format(format, connector, col_names, None, &config_options).await?;
        }
    }
    Ok(stmt)
}

async fn purify_format(
    format: &mut Option<Format>,
    connector: &mut Connector,
    col_names: &mut Vec<Ident>,
    file: Option<tokio::fs::File>,
    specified_options: &HashMap<String, String>,
) -> Result<(), failure::Error> {
    match format {
        Some(Format::Avro(schema)) => match schema {
            AvroSchema::CsrUrl { url, seed } => {
                let topic = if let Connector::Kafka { topic, .. } = connector {
                    topic
                } else {
                    bail!("Confluent Schema Registry is only supported with Kafka sources")
                };
                if seed.is_none() {
                    let url = url.parse()?;

                    let ccsr_config =
                        kafka_util::generate_ccsr_client_config(url, &specified_options)?;

                    let Schema {
                        key_schema,
                        value_schema,
                        ..
                    } = get_remote_avro_schema(ccsr_config, topic.clone()).await?;
                    *seed = Some(CsrSeed {
                        key_schema,
                        value_schema,
                    });
                }
            }
            AvroSchema::Schema(sql_parser::ast::Schema::File(path)) => {
                let value_schema = tokio::fs::read_to_string(path).await?;
                *schema = AvroSchema::Schema(sql_parser::ast::Schema::Inline(value_schema));
            }
            _ => {}
        },
        Some(Format::Protobuf { schema, .. }) => {
            if let sql_parser::ast::Schema::File(path) = schema {
                let descriptors = tokio::fs::read(path).await?;
                let mut buf = String::new();
                strconv::format_bytes(&mut buf, &descriptors);
                *schema = sql_parser::ast::Schema::Inline(buf);
            }
        }
        Some(Format::Csv {
            header_row,
            delimiter,
            ..
        }) => {
            if *header_row && col_names.is_empty() {
                if let Some(file) = file {
                    let file = tokio::io::BufReader::new(file);
                    let csv_header = file.lines().next_line().await?;
                    match csv_header {
                        Some(csv_header) => {
                            csv_header
                                .split(*delimiter as char)
                                .for_each(|v| col_names.push(Ident::from(v)));
                        }
                        None => bail!("CSV file expected header line, but is empty"),
                    }
                } else {
                    bail!("CSV format with headers only works with file connectors")
                }
            }
        }
        _ => (),
    }
    Ok(())
}

#[derive(Debug)]
pub struct Schema {
    pub key_schema: Option<String>,
    pub value_schema: String,
    pub schema_registry_config: Option<ccsr::ClientConfig>,
}

async fn get_remote_avro_schema(
    schema_registry_config: ccsr::ClientConfig,
    topic: String,
) -> Result<Schema, failure::Error> {
    let ccsr_client = schema_registry_config.clone().build();

    let value_schema_name = format!("{}-value", topic);
    let value_schema = ccsr_client
        .get_schema_by_subject(&value_schema_name)
        .await
        .with_context(|err| {
            format!(
                "fetching latest schema for subject '{}' from registry: {}",
                value_schema_name, err
            )
        })?;
    let subject = format!("{}-key", topic);
    let key_schema = ccsr_client.get_schema_by_subject(&subject).await.ok();
    Ok(Schema {
        key_schema: key_schema.map(|s| s.raw),
        value_schema: value_schema.raw,
        schema_registry_config: Some(schema_registry_config),
    })
}
