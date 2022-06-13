// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL purification.
//!
//! See the [crate-level documentation](crate) for details.
use std::iter;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context};
use aws_arn::ARN;
use mz_sql_parser::ast::{CsrConnection, KafkaConnection, KafkaSourceConnection};
use prost::Message;
use protobuf_native::compiler::{SourceTreeDescriptorDatabase, VirtualSourceTree};
use protobuf_native::MessageLite;
use tracing::info;
use uuid::Uuid;

use mz_ccsr::{Client, GetBySubjectError};
use mz_dataflow_types::connections::aws::{AwsConfig, AwsExternalIdPrefix};
use mz_dataflow_types::connections::{Connection, ConnectionContext};
use mz_dataflow_types::sources::PostgresSourceDetails;
use mz_repr::proto::RustType;
use mz_repr::strconv;

use crate::ast::{
    AvroSchema, CreateSourceConnection, CreateSourceFormat, CreateSourceStatement,
    CsrConnectionAvro, CsrConnectionProto, CsrSeed, CsrSeedCompiled, CsrSeedCompiledEncoding,
    CsrSeedCompiledOrLegacy, CsvColumns, DbzMode, Envelope, Format, Ident, ProtobufSchema, Value,
    WithOption, WithOptionValue,
};
use crate::catalog::SessionCatalog;
use crate::kafka_util;
use crate::names::Aug;
use crate::normalize;
use crate::plan::StatementContext;

/// Purifies a statement, removing any dependencies on external state.
///
/// See the section on [purification](crate#purification) in the crate
/// documentation for details.
///
/// Note that purification is asynchronous, and may take an unboundedly long
/// time to complete. As a result purification does *not* have access to a
/// [`SessionCatalog`](crate::catalog::SessionCatalog), as that would require
/// locking access to the catalog for an unbounded amount of time.
pub async fn purify_create_source(
    catalog: Box<dyn SessionCatalog>,
    now: u64,
    mut stmt: CreateSourceStatement<Aug>,
    connection_context: ConnectionContext,
) -> Result<CreateSourceStatement<Aug>, anyhow::Error> {
    let CreateSourceStatement {
        connection,
        format,
        envelope,
        with_options,
        include_metadata: _,
        ..
    } = &mut stmt;

    let _ = catalog;

    let mut with_options_map = normalize::options(with_options)?;

    match connection {
        CreateSourceConnection::Kafka(KafkaSourceConnection {
            connection, topic, ..
        }) => {
            let (broker, connection_options) = match connection {
                KafkaConnection::Reference { connection } => {
                    let scx = StatementContext::new(None, &*catalog);
                    let item = scx.get_item_by_resolved_name(&connection)?;
                    match item.connection()? {
                        Connection::Kafka(connection) => {
                            (connection.broker.to_string(), connection.options.clone())
                        }
                        _ => bail!("{} is not a kafka connection", item.name()),
                    }
                }
                KafkaConnection::Inline { broker } => (
                    broker.to_string(),
                    kafka_util::extract_config(&*catalog, &mut with_options_map)?,
                ),
            };
            let consumer = kafka_util::create_consumer(
                &broker,
                &topic,
                &connection_options,
                connection_context.librdkafka_log_level,
                catalog.secrets_reader(),
            )
            .await
            .map_err(|e| anyhow!("Failed to create and connect Kafka consumer: {}", e))?;

            // Translate `kafka_time_offset` to `start_offset`.
            match kafka_util::lookup_start_offsets(
                Arc::clone(&consumer),
                &topic,
                &with_options_map,
                now,
            )
            .await?
            {
                Some(start_offsets) => {
                    // Drop `kafka_time_offset`
                    with_options.retain(|val| match val {
                        WithOption { key, .. } => key.as_str() != "kafka_time_offset",
                    });
                    info!("add start_offset {:?}", start_offsets);
                    // Add `start_offset`
                    with_options.push(WithOption {
                        key: Ident::new("start_offset"),
                        value: Some(WithOptionValue::Value(Value::Array(
                            start_offsets
                                .iter()
                                .map(|offset| Value::Number(offset.to_string()))
                                .collect(),
                        ))),
                    });
                }
                None => {}
            }
        }
        CreateSourceConnection::S3 { .. } => {
            let aws_config = normalize::aws_config(&mut with_options_map, None)?;
            validate_aws_credentials(
                &aws_config,
                connection_context.aws_external_id_prefix.as_ref(),
            )
            .await?;
        }
        CreateSourceConnection::Kinesis { arn } => {
            let region = arn
                .parse::<ARN>()
                .context("Unable to parse provided ARN")?
                .region
                .ok_or_else(|| anyhow!("Provided ARN does not include an AWS region"))?;

            let aws_config = normalize::aws_config(&mut with_options_map, Some(region.into()))?;
            validate_aws_credentials(
                &aws_config,
                connection_context.aws_external_id_prefix.as_ref(),
            )
            .await?;
        }
        CreateSourceConnection::Postgres {
            conn,
            publication,
            details: details_ast,
        } => {
            // verify that we can connect upstream and snapshot publication metadata
            let tables = mz_postgres_util::publication_info(&conn, &publication).await?;

            let details = PostgresSourceDetails {
                tables,
                slot: format!(
                    "materialize_{}",
                    Uuid::new_v4().to_string().replace('-', "")
                ),
            };
            *details_ast = Some(hex::encode(details.into_proto().encode_to_vec()));
        }
        CreateSourceConnection::PubNub { .. } => (),
    }

    purify_source_format(&*catalog, format, connection, &envelope).await?;

    Ok(stmt)
}

async fn purify_source_format(
    catalog: &dyn SessionCatalog,
    format: &mut CreateSourceFormat<Aug>,
    connection: &mut CreateSourceConnection<Aug>,
    envelope: &Option<Envelope<Aug>>,
) -> Result<(), anyhow::Error> {
    if matches!(format, CreateSourceFormat::KeyValue { .. })
        && !matches!(connection, CreateSourceConnection::Kafka { .. })
    {
        bail!("Kafka sources are the only source type that can provide KEY/VALUE formats")
    }

    // For backwards compatibility, using ENVELOPE UPSERT with a bare FORMAT
    // BYTES or FORMAT TEXT uses the specified format for both the key and the
    // value.
    //
    // TODO(bwm): We should either make these semantics apply everywhere, or
    // deprecate this.
    match (&connection, &envelope, &*format) {
        (
            CreateSourceConnection::Kafka { .. },
            Some(Envelope::Upsert),
            CreateSourceFormat::Bare(f @ Format::Bytes | f @ Format::Text),
        ) => {
            *format = CreateSourceFormat::KeyValue {
                key: f.clone(),
                value: f.clone(),
            };
        }
        _ => (),
    }

    match format {
        CreateSourceFormat::None => {}
        CreateSourceFormat::Bare(format) => {
            purify_source_format_single(catalog, format, connection, envelope).await?;
        }

        CreateSourceFormat::KeyValue { key, value: val } => {
            purify_source_format_single(catalog, key, connection, envelope).await?;
            purify_source_format_single(catalog, val, connection, envelope).await?;
        }
    }
    Ok(())
}

async fn purify_source_format_single(
    catalog: &dyn SessionCatalog,
    format: &mut Format<Aug>,
    connection: &mut CreateSourceConnection<Aug>,
    envelope: &Option<Envelope<Aug>>,
) -> Result<(), anyhow::Error> {
    match format {
        Format::Avro(schema) => match schema {
            AvroSchema::Csr { csr_connection } => {
                purify_csr_connection_avro(catalog, connection, csr_connection, envelope).await?
            }
            AvroSchema::InlineSchema { schema, .. } => {
                if let mz_sql_parser::ast::Schema::File(path) = schema {
                    let file_schema = tokio::fs::read_to_string(path).await?;
                    *schema = mz_sql_parser::ast::Schema::Inline(file_schema);
                }
            }
        },
        Format::Protobuf(schema) => match schema {
            ProtobufSchema::Csr { csr_connection } => {
                purify_csr_connection_proto(catalog, connection, csr_connection, envelope).await?;
            }
            ProtobufSchema::InlineSchema {
                message_name: _,
                schema,
            } => {
                if let mz_sql_parser::ast::Schema::File(path) = schema {
                    let descriptors = tokio::fs::read(path).await?;
                    let mut buf = String::new();
                    strconv::format_bytes(&mut buf, &descriptors);
                    *schema = mz_sql_parser::ast::Schema::Inline(buf);
                }
            }
        },
        Format::Csv {
            delimiter: _,
            ref mut columns,
        } => {
            if let CsvColumns::Header { names } = columns {
                match connection {
                    CreateSourceConnection::S3 { .. } => {
                        if names.is_empty() {
                            bail!("CSV WITH HEADER for S3 sources requires specifying the header columns");
                        }
                    }
                    _ => bail!("CSV WITH HEADER is only supported for S3 sources"),
                }
            }
        }
        Format::Bytes | Format::Regex(_) | Format::Json | Format::Text => (),
    }
    Ok(())
}

async fn purify_csr_connection_proto(
    catalog: &dyn SessionCatalog,
    connection: &mut CreateSourceConnection<Aug>,
    csr_connection: &mut CsrConnectionProto<Aug>,
    envelope: &Option<Envelope<Aug>>,
) -> Result<(), anyhow::Error> {
    let topic =
        if let CreateSourceConnection::Kafka(KafkaSourceConnection { topic, .. }) = connection {
            topic
        } else {
            bail!("Confluent Schema Registry is only supported with Kafka sources")
        };

    let CsrConnectionProto {
        connection,
        seed,
        with_options: ccsr_options,
    } = csr_connection;
    match seed {
        None => {
            let ccsr_config = match connection {
                CsrConnection::Inline { url } => kafka_util::generate_ccsr_client_config(
                    url.parse()?,
                    &mut kafka_util::extract_config_ccsr(
                        &*catalog,
                        &mut normalize::options(&ccsr_options)?,
                    )?,
                    catalog.secrets_reader(),
                )?,
                CsrConnection::Reference { connection } => {
                    let scx = StatementContext::new(None, &*catalog);
                    let item = scx.get_item_by_resolved_name(&connection)?;
                    match item.connection()? {
                        Connection::Csr(config) => config.clone(),
                        _ => bail!("{} is not a schema registry connection", item.name()),
                    }
                }
            };

            let value =
                compile_proto(&format!("{}-value", topic), ccsr_config.clone().build()?).await?;
            let key = compile_proto(&format!("{}-key", topic), ccsr_config.build()?)
                .await
                .ok();

            if matches!(envelope, Some(Envelope::Debezium(DbzMode::Upsert))) && key.is_none() {
                bail!("Key schema is required for ENVELOPE DEBEZIUM UPSERT");
            }

            *seed = Some(CsrSeedCompiledOrLegacy::Compiled(CsrSeedCompiled {
                value,
                key,
            }));
        }
        Some(CsrSeedCompiledOrLegacy::Compiled(..)) => (),
        Some(CsrSeedCompiledOrLegacy::Legacy(..)) => {
            unreachable!("Should not be able to purify CsrSeedCompiledOrLegacy::Legacy")
        }
    }

    Ok(())
}

async fn purify_csr_connection_avro(
    catalog: &dyn SessionCatalog,
    connection: &mut CreateSourceConnection<Aug>,
    csr_connection: &mut CsrConnectionAvro<Aug>,
    envelope: &Option<Envelope<Aug>>,
) -> Result<(), anyhow::Error> {
    let topic =
        if let CreateSourceConnection::Kafka(KafkaSourceConnection { topic, .. }) = connection {
            topic
        } else {
            bail!("Confluent Schema Registry is only supported with Kafka sources")
        };

    let CsrConnectionAvro {
        connection,
        seed,
        with_options: ccsr_options,
    } = csr_connection;
    if seed.is_none() {
        let ccsr_config = match connection {
            CsrConnection::Inline { url } => kafka_util::generate_ccsr_client_config(
                url.parse()?,
                &mut kafka_util::extract_config_ccsr(
                    &*catalog,
                    &mut normalize::options(&ccsr_options)?,
                )?,
                catalog.secrets_reader(),
            )?,
            CsrConnection::Reference { connection } => {
                let scx = StatementContext::new(None, &*catalog);
                let item = scx.get_item_by_resolved_name(&connection)?;
                match item.connection()? {
                    Connection::Csr(config) => config.clone(),
                    _ => bail!("{} is not a schema registry connection", item.name()),
                }
            }
        };

        let Schema {
            key_schema,
            value_schema,
            ..
        } = get_remote_csr_schema(ccsr_config, topic.clone()).await?;
        if matches!(envelope, Some(Envelope::Debezium(DbzMode::Upsert))) && key_schema.is_none() {
            bail!("Key schema is required for ENVELOPE DEBEZIUM UPSERT");
        }

        *seed = Some(CsrSeed {
            key_schema,
            value_schema,
        })
    }

    Ok(())
}

#[derive(Debug)]
pub struct Schema {
    pub key_schema: Option<String>,
    pub value_schema: String,
    pub schema_registry_config: Option<mz_ccsr::ClientConfig>,
    pub confluent_wire_format: bool,
}

async fn get_remote_csr_schema(
    schema_registry_config: mz_ccsr::ClientConfig,
    topic: String,
) -> Result<Schema, anyhow::Error> {
    let ccsr_client = schema_registry_config.clone().build()?;

    let value_schema_name = format!("{}-value", topic);
    let value_schema = ccsr_client
        .get_schema_by_subject(&value_schema_name)
        .await
        .with_context(|| {
            format!(
                "fetching latest schema for subject '{}' from registry",
                value_schema_name
            )
        })?;
    let subject = format!("{}-key", topic);
    let key_schema = match ccsr_client.get_schema_by_subject(&subject).await {
        Ok(ks) => Some(ks),
        Err(GetBySubjectError::SubjectNotFound) => None,
        Err(e) => bail!(e),
    };
    Ok(Schema {
        key_schema: key_schema.map(|s| s.raw),
        value_schema: value_schema.raw,
        schema_registry_config: Some(schema_registry_config),
        confluent_wire_format: true,
    })
}

/// Collect protobuf message descriptor from CSR and compile the descriptor.
async fn compile_proto(
    subject_name: &String,
    ccsr_client: Client,
) -> Result<CsrSeedCompiledEncoding, anyhow::Error> {
    let (primary_subject, dependency_subjects) =
        ccsr_client.get_subject_and_references(subject_name).await?;

    // Compile .proto files into a file descriptor set.
    let mut source_tree = VirtualSourceTree::new();
    for subject in iter::once(&primary_subject).chain(dependency_subjects.iter()) {
        source_tree.as_mut().add_file(
            Path::new(&subject.name),
            subject.schema.raw.as_bytes().to_vec(),
        );
    }
    let mut db = SourceTreeDescriptorDatabase::new(source_tree.as_mut());
    let fds = db
        .as_mut()
        .build_file_descriptor_set(&[Path::new(&primary_subject.name)])?;

    // Ensure there is exactly one message in the file.
    let primary_fd = fds.file(0);
    let message_name = match primary_fd.message_type_size() {
        1 => String::from_utf8_lossy(primary_fd.message_type(0).name()).into_owned(),
        0 => bail_unsupported!(9598, "Protobuf schemas with no messages"),
        _ => bail_unsupported!(9598, "Protobuf schemas with multiple messages"),
    };

    // Encode the file descriptor set into a SQL byte string.
    let mut schema = String::new();
    strconv::format_bytes(&mut schema, &fds.serialize()?);

    Ok(CsrSeedCompiledEncoding {
        schema,
        message_name,
    })
}

/// Makes an always-valid AWS API call to perform a basic sanity check of
/// whether the specified AWS configuration is valid.
async fn validate_aws_credentials(
    config: &AwsConfig,
    external_id_prefix: Option<&AwsExternalIdPrefix>,
) -> Result<(), anyhow::Error> {
    let config = config.load(external_id_prefix, None).await;
    let sts_client = aws_sdk_sts::Client::new(&config);
    let _ = sts_client
        .get_caller_identity()
        .send()
        .await
        .context("Unable to validate AWS credentials")?;
    Ok(())
}
