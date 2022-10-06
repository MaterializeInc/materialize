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

use std::collections::HashMap;
use std::error::Error as StdError;
use std::iter;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context};
use itertools::Itertools;
use prost::Message;
use protobuf_native::compiler::{SourceTreeDescriptorDatabase, VirtualSourceTree};
use protobuf_native::MessageLite;
use tracing::info;
use uuid::Uuid;

use mz_ccsr::Schema as CcsrSchema;
use mz_ccsr::{Client, GetByIdError, GetBySubjectError};
use mz_ore::cast::CastFrom;
use mz_proto::RustType;
use mz_repr::{strconv, GlobalId};
use mz_secrets::SecretsReader;
use mz_sql_parser::ast::{
    ColumnDef, CsrConnection, CsrSeedAvro, CsrSeedProtobuf, CsrSeedProtobufSchema, DbzMode,
    Envelope, Ident, KafkaConfigOption, KafkaConfigOptionName, KafkaConnection,
    KafkaSourceConnection, PgConfigOption, PgConfigOptionName, ReaderSchemaSelectionStrategy,
    UnresolvedObjectName,
};
use mz_storage::types::connections::aws::{AwsConfig, AwsExternalIdPrefix};
use mz_storage::types::connections::{Connection, ConnectionContext};
use mz_storage::types::sources::PostgresSourceDetails;

use crate::ast::{
    AvroSchema, CreateSourceConnection, CreateSourceFormat, CreateSourceStatement,
    CreateSourceSubsource, CreateSourceSubsources, CreateSubsourceStatement, CsrConnectionAvro,
    CsrConnectionProtobuf, CsvColumns, Format, ProtobufSchema, Value, WithOptionValue,
};
use crate::catalog::SessionCatalog;
use crate::kafka_util;
use crate::kafka_util::KafkaConfigOptionExtracted;
use crate::names::{Aug, ResolvedObjectName};
use crate::normalize;
use crate::plan::StatementContext;

/// Purifies a statement, removing any dependencies on external state.
///
/// See the section on [purification](crate#purification) in the crate
/// documentation for details.
pub async fn purify_create_source(
    catalog: Box<dyn SessionCatalog>,
    now: u64,
    mut stmt: CreateSourceStatement<Aug>,
    connection_context: ConnectionContext,
) -> Result<
    (
        Vec<(GlobalId, CreateSubsourceStatement<Aug>)>,
        CreateSourceStatement<Aug>,
    ),
    anyhow::Error,
> {
    let CreateSourceStatement {
        connection,
        format,
        envelope,
        include_metadata: _,
        subsources: requested_subsources,
        ..
    } = &mut stmt;

    // Disallow manually targetting subsources, this syntax is reserved for purification only
    if let Some(CreateSourceSubsources::Subset(subsources)) = requested_subsources {
        for subsource in subsources {
            if matches!(subsource, CreateSourceSubsource::Resolved(_, _)) {
                bail!("Cannot alias subsource using `INTO`, use `AS` instead");
            }
        }
    }

    let mut subsources = vec![];

    match connection {
        CreateSourceConnection::Kafka(KafkaSourceConnection {
            connection:
                KafkaConnection {
                    connection,
                    options: base_with_options,
                },
            ..
        }) => {
            let scx = StatementContext::new(None, &*catalog);
            let connection = {
                let item = scx.get_item_by_resolved_name(connection)?;
                // Get Kafka connection
                match item.connection()? {
                    Connection::Kafka(connection) => connection.clone(),
                    _ => bail!("{} is not a kafka connection", item.name()),
                }
            };

            let extracted_options: KafkaConfigOptionExtracted =
                base_with_options.clone().try_into()?;

            let offset_type =
                Option::<kafka_util::KafkaStartOffsetType>::try_from(&extracted_options)?;
            let config_options = kafka_util::LibRdKafkaConfig::try_from(&extracted_options)?.0;

            let topic = extracted_options
                .topic
                .ok_or_else(|| sql_err!("KAFKA CONNECTION without TOPIC"))?;

            let consumer = kafka_util::create_consumer(
                &topic,
                &connection,
                &config_options,
                connection_context.librdkafka_log_level,
                &*connection_context.secrets_reader,
            )
            .await
            .map_err(|e| anyhow!("Failed to create and connect Kafka consumer: {}", e))?;

            if let Some(offset_type) = offset_type {
                // Translate `START TIMESTAMP` to a start offset
                match kafka_util::lookup_start_offsets(
                    Arc::clone(&consumer),
                    &topic,
                    offset_type,
                    now,
                )
                .await?
                {
                    Some(start_offsets) => {
                        // Drop the value we are purifying
                        base_with_options.retain(|val| match val {
                            KafkaConfigOption {
                                name: KafkaConfigOptionName::StartTimestamp,
                                ..
                            } => false,
                            _ => true,
                        });
                        info!("add start_offset {:?}", start_offsets);
                        base_with_options.push(KafkaConfigOption {
                            name: KafkaConfigOptionName::StartOffset,
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
        }
        CreateSourceConnection::S3 { connection, .. } => {
            let scx = StatementContext::new(None, &*catalog);
            let aws = {
                let item = scx.get_item_by_resolved_name(connection)?;
                match item.connection()? {
                    Connection::Aws(aws) => aws.clone(),
                    _ => bail!("{} is not an AWS connection", item.name()),
                }
            };
            validate_aws_credentials(
                &aws,
                connection_context.aws_external_id_prefix.as_ref(),
                &*connection_context.secrets_reader,
            )
            .await?;
        }
        CreateSourceConnection::Kinesis { connection, .. } => {
            let scx = StatementContext::new(None, &*catalog);
            let aws = {
                let item = scx.get_item_by_resolved_name(connection)?;
                match item.connection()? {
                    Connection::Aws(aws) => aws.clone(),
                    _ => bail!("{} is not an AWS connection", item.name()),
                }
            };
            validate_aws_credentials(
                &aws,
                connection_context.aws_external_id_prefix.as_ref(),
                &*connection_context.secrets_reader,
            )
            .await?;
        }
        CreateSourceConnection::Postgres {
            connection,
            options,
        } => {
            let scx = StatementContext::new(None, &*catalog);
            let connection = {
                let item = scx.get_item_by_resolved_name(connection)?;
                match item.connection()? {
                    Connection::Postgres(connection) => connection.clone(),
                    _ => bail!("{} is not a postgres connection", item.name()),
                }
            };
            let crate::plan::statement::PgConfigOptionExtracted { publication, .. } =
                options.clone().try_into()?;
            let publication = publication
                .ok_or_else(|| sql_err!("POSTGRES CONNECTION must specify PUBLICATION"))?;

            // verify that we can connect upstream and snapshot publication metadata
            let config = connection
                .config(&*connection_context.secrets_reader)
                .await?;
            let tables = mz_postgres_util::publication_info(&config, &publication).await?;

            let mut targeted_subsources = vec![];

            let mut validated_requested_subsources = vec![];
            match requested_subsources {
                Some(CreateSourceSubsources::All) => {
                    for table in &tables {
                        let name =
                            UnresolvedObjectName::qualified(&[&table.namespace, &table.name]);
                        validated_requested_subsources.push((name.clone(), name, table));
                    }
                }
                Some(CreateSourceSubsources::Subset(subsources)) => {
                    // The user manually selected a subset of upstream tables so we need to
                    // validate that the names actually exist and are not ambiguous

                    // An index from table_name -> schema_name -> PostgresTableDesc
                    let mut tables_by_name = HashMap::new();
                    for table in &tables {
                        tables_by_name
                            .entry(table.name.clone())
                            .or_insert_with(HashMap::new)
                            .entry(table.namespace.clone())
                            .or_insert(table);
                    }

                    for subsource in subsources {
                        let (upstream_name, subsource_name) = match subsource.clone() {
                            CreateSourceSubsource::Bare(name) => (name.clone(), name),
                            CreateSourceSubsource::Aliased(name, alias) => (name, alias),
                            CreateSourceSubsource::Resolved(_, _) => {
                                bail!("Cannot alias subsource using `INTO`, use `AS` instead")
                            }
                        };

                        let partial_upstream_name =
                            normalize::unresolved_object_name(upstream_name.clone())?;
                        let schemas =
                            tables_by_name
                                .get(&partial_upstream_name.item)
                                .ok_or_else(|| {
                                    sql_err!(
                                        "table {} not found in upstream database",
                                        partial_upstream_name
                                    )
                                })?;

                        let (schema, table) = match &partial_upstream_name.schema {
                            Some(schema) => match schemas.get(schema) {
                                Some(table) => (schema, table),
                                None => {
                                    return Err(sql_err!(
                                        "schema {} does not exist in upstream database",
                                        schema
                                    )
                                    .into())
                                }
                            },
                            None => match schemas.iter().exactly_one() {
                                Ok((schema, table)) => (schema, table),
                                Err(_) => {
                                    return Err(sql_err!(
                                        "table {} is ambiguous, consider specifying the schema",
                                        upstream_name
                                    )
                                    .into())
                                }
                            },
                        };
                        let qualified_upstream_name =
                            UnresolvedObjectName::qualified(&[schema, &partial_upstream_name.item]);
                        validated_requested_subsources.push((
                            qualified_upstream_name,
                            subsource_name,
                            table,
                        ));
                    }
                }
                None => {}
            };

            // Now that we have an explicit list of validated requested subsources we can create them
            for (i, (upstream_name, subsource_name, table)) in
                validated_requested_subsources.into_iter().enumerate()
            {
                // Figure out the schema of the subsource
                let mut columns = vec![];
                for c in table.columns.iter() {
                    let name = Ident::new(c.name.clone());

                    let ty = mz_pgrepr::Type::from_oid_and_typmod(c.type_oid, c.type_mod)
                        .map_err(|e| sql_err!("{}", e))?;
                    let data_type = scx.resolve_type(ty)?;

                    columns.push(ColumnDef {
                        name,
                        data_type,
                        collation: None,
                        options: vec![],
                    });
                }

                // Create the targeted AST node for the original CREATE SOURCE statement
                let transient_id = GlobalId::Transient(u64::cast_from(i));
                let partial_subsource_name =
                    normalize::unresolved_object_name(subsource_name.clone())?;
                let qualified_name = scx.allocate_qualified_name(partial_subsource_name.clone())?;
                let full_name = scx.allocate_full_name(partial_subsource_name)?;
                targeted_subsources.push(CreateSourceSubsource::Resolved(
                    upstream_name,
                    ResolvedObjectName::Object {
                        id: transient_id,
                        qualifiers: qualified_name.qualifiers,
                        full_name,
                        print_id: false,
                    },
                ));

                // Create the subsource statement
                let subsource = CreateSubsourceStatement {
                    name: subsource_name,
                    columns,
                    constraints: vec![],
                    if_not_exists: false,
                };
                subsources.push((transient_id, subsource));
            }
            *requested_subsources = Some(CreateSourceSubsources::Subset(targeted_subsources));

            // Remove any old detail references
            options.retain(|PgConfigOption { name, .. }| name != &PgConfigOptionName::Details);
            let details = PostgresSourceDetails {
                tables,
                slot: format!(
                    "materialize_{}",
                    Uuid::new_v4().to_string().replace('-', "")
                ),
            };
            options.push(PgConfigOption {
                name: PgConfigOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            })
        }
        CreateSourceConnection::LoadGenerator { .. } => (),
    }

    purify_source_format(&*catalog, format, connection, envelope, &connection_context).await?;

    Ok((subsources, stmt))
}

async fn purify_source_format(
    catalog: &dyn SessionCatalog,
    format: &mut CreateSourceFormat<Aug>,
    connection: &mut CreateSourceConnection<Aug>,
    envelope: &Option<Envelope>,
    connection_context: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    if matches!(format, CreateSourceFormat::KeyValue { .. })
        && !matches!(connection, CreateSourceConnection::Kafka { .. })
    {
        bail!("Kafka sources are the only source type that can provide KEY/VALUE formats")
    }

    match format {
        CreateSourceFormat::None => {}
        CreateSourceFormat::Bare(format) => {
            purify_source_format_single(catalog, format, connection, envelope, connection_context)
                .await?;
        }

        CreateSourceFormat::KeyValue { key, value: val } => {
            purify_source_format_single(catalog, key, connection, envelope, connection_context)
                .await?;
            purify_source_format_single(catalog, val, connection, envelope, connection_context)
                .await?;
        }
    }
    Ok(())
}

async fn purify_source_format_single(
    catalog: &dyn SessionCatalog,
    format: &mut Format<Aug>,
    connection: &mut CreateSourceConnection<Aug>,
    envelope: &Option<Envelope>,
    connection_context: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    match format {
        Format::Avro(schema) => match schema {
            AvroSchema::Csr { csr_connection } => {
                purify_csr_connection_avro(
                    catalog,
                    connection,
                    csr_connection,
                    envelope,
                    connection_context,
                )
                .await?
            }
            AvroSchema::InlineSchema { .. } => {}
        },
        Format::Protobuf(schema) => match schema {
            ProtobufSchema::Csr { csr_connection } => {
                purify_csr_connection_proto(
                    catalog,
                    connection,
                    csr_connection,
                    envelope,
                    connection_context,
                )
                .await?;
            }
            ProtobufSchema::InlineSchema { .. } => {}
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
    csr_connection: &mut CsrConnectionProtobuf<Aug>,
    envelope: &Option<Envelope>,
    connection_context: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    let topic = if let CreateSourceConnection::Kafka(KafkaSourceConnection {
        connection: KafkaConnection { options, .. },
        ..
    }) = connection
    {
        let KafkaConfigOptionExtracted { topic, .. } = options
            .clone()
            .try_into()
            .expect("already verified options valid provided");
        topic.expect("already validated topic provided")
    } else {
        bail!("Confluent Schema Registry is only supported with Kafka sources")
    };

    let CsrConnectionProtobuf {
        seed,
        connection: CsrConnection {
            connection,
            options: _,
        },
    } = csr_connection;
    match seed {
        None => {
            let scx = StatementContext::new(None, &*catalog);

            let ccsr_connection = match scx.get_item_by_resolved_name(connection)?.connection()? {
                Connection::Csr(connection) => connection.clone(),
                _ => bail!("{} is not a schema registry connection", connection),
            };

            let ccsr_client = ccsr_connection
                .connect(&*connection_context.secrets_reader)
                .await?;

            let value = compile_proto(&format!("{}-value", topic), &ccsr_client).await?;
            let key = compile_proto(&format!("{}-key", topic), &ccsr_client)
                .await
                .ok();

            if matches!(envelope, Some(Envelope::Debezium(DbzMode::Plain))) && key.is_none() {
                bail!("Key schema is required for ENVELOPE DEBEZIUM");
            }

            *seed = Some(CsrSeedProtobuf { value, key });
        }
        Some(_) => (),
    }

    Ok(())
}

async fn purify_csr_connection_avro(
    catalog: &dyn SessionCatalog,
    connection: &mut CreateSourceConnection<Aug>,
    csr_connection: &mut CsrConnectionAvro<Aug>,
    envelope: &Option<Envelope>,
    connection_context: &ConnectionContext,
) -> Result<(), anyhow::Error> {
    let topic = if let CreateSourceConnection::Kafka(KafkaSourceConnection {
        connection: KafkaConnection { options, .. },
        ..
    }) = connection
    {
        let KafkaConfigOptionExtracted { topic, .. } = options
            .clone()
            .try_into()
            .expect("already verified options valid provided");
        topic.expect("already validated topic provided")
    } else {
        bail!("Confluent Schema Registry is only supported with Kafka sources")
    };

    let CsrConnectionAvro {
        connection: CsrConnection { connection, .. },
        seed,
        key_strategy,
        value_strategy,
    } = csr_connection;
    if seed.is_none() {
        let scx = StatementContext::new(None, &*catalog);
        let csr_connection = match scx.get_item_by_resolved_name(connection)?.connection()? {
            Connection::Csr(connection) => connection.clone(),
            _ => bail!("{} is not a schema registry connection", connection),
        };
        let ccsr_client = csr_connection
            .connect(&*connection_context.secrets_reader)
            .await?;

        let Schema {
            key_schema,
            value_schema,
        } = get_remote_csr_schema(
            &ccsr_client,
            key_strategy.clone().unwrap_or_default(),
            value_strategy.clone().unwrap_or_default(),
            topic,
        )
        .await?;
        if matches!(envelope, Some(Envelope::Debezium(DbzMode::Plain))) && key_schema.is_none() {
            bail!("Key schema is required for ENVELOPE DEBEZIUM");
        }

        *seed = Some(CsrSeedAvro {
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
}

#[derive(Debug)]
pub enum GetSchemaError {
    Subject(GetBySubjectError),
    Id(GetByIdError),
}

impl From<GetBySubjectError> for GetSchemaError {
    fn from(inner: GetBySubjectError) -> Self {
        Self::Subject(inner)
    }
}

impl From<GetByIdError> for GetSchemaError {
    fn from(inner: GetByIdError) -> Self {
        Self::Id(inner)
    }
}

impl std::fmt::Display for GetSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GetSchemaError::Subject(e) => write!(f, "failed to look up schema by subject: {e}"),
            GetSchemaError::Id(e) => write!(f, "failed to look up schema by id: {e}"),
        }
    }
}

impl StdError for GetSchemaError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            GetSchemaError::Subject(e) => Some(e),
            GetSchemaError::Id(e) => Some(e),
        }
    }
}

async fn get_schema_with_strategy(
    client: &Client,
    strategy: ReaderSchemaSelectionStrategy,
    subject: &str,
) -> Result<Option<String>, GetSchemaError> {
    match strategy {
        ReaderSchemaSelectionStrategy::Latest => {
            match client.get_schema_by_subject(subject).await {
                Ok(CcsrSchema { raw, .. }) => Ok(Some(raw)),
                Err(GetBySubjectError::SubjectNotFound) => Ok(None),
                Err(e) => Err(e.into()),
            }
        }
        ReaderSchemaSelectionStrategy::Inline(raw) => Ok(Some(raw)),
        ReaderSchemaSelectionStrategy::ById(id) => match client.get_schema_by_id(id).await {
            Ok(CcsrSchema { raw, .. }) => Ok(Some(raw)),
            Err(GetByIdError::SchemaNotFound) => Ok(None),
            Err(e) => Err(e.into()),
        },
    }
}

async fn get_remote_csr_schema(
    ccsr_client: &mz_ccsr::Client,
    key_strategy: ReaderSchemaSelectionStrategy,
    value_strategy: ReaderSchemaSelectionStrategy,
    topic: String,
) -> Result<Schema, anyhow::Error> {
    let value_schema_name = format!("{}-value", topic);
    let value_schema = get_schema_with_strategy(ccsr_client, value_strategy, &value_schema_name)
        .await
        .with_context(|| {
            format!(
                "fetching latest schema for subject '{}' from registry",
                value_schema_name
            )
        })?;
    let value_schema = value_schema.ok_or_else(|| anyhow!("No value schema found"))?;
    let subject = format!("{}-key", topic);
    let key_schema = get_schema_with_strategy(ccsr_client, key_strategy, &subject).await?;
    Ok(Schema {
        key_schema,
        value_schema,
    })
}

/// Collect protobuf message descriptor from CSR and compile the descriptor.
async fn compile_proto(
    subject_name: &String,
    ccsr_client: &Client,
) -> Result<CsrSeedProtobufSchema, anyhow::Error> {
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

    Ok(CsrSeedProtobufSchema {
        schema,
        message_name,
    })
}

/// Makes an always-valid AWS API call to perform a basic sanity check of
/// whether the specified AWS configuration is valid.
async fn validate_aws_credentials(
    config: &AwsConfig,
    external_id_prefix: Option<&AwsExternalIdPrefix>,
    secrets_reader: &dyn SecretsReader,
) -> Result<(), anyhow::Error> {
    let config = config.load(external_id_prefix, None, secrets_reader).await;
    let sts_client = aws_sdk_sts::Client::new(&config);
    let _ = sts_client
        .get_caller_identity()
        .send()
        .await
        .context("Unable to validate AWS credentials")?;
    Ok(())
}
