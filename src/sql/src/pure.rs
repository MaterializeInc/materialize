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

use std::collections::{BTreeMap, BTreeSet};
use std::iter;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use mz_repr::adt::system::Oid;
use prost::Message;
use protobuf_native::compiler::{SourceTreeDescriptorDatabase, VirtualSourceTree};
use protobuf_native::MessageLite;
use tracing::info;
use uuid::Uuid;

use mz_ccsr::Schema as CcsrSchema;
use mz_ccsr::{Client, GetByIdError, GetBySubjectError};
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_ore::cast::CastFrom;
use mz_ore::str::StrExt;
use mz_proto::RustType;
use mz_repr::{strconv, GlobalId};
use mz_secrets::SecretsReader;
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{
    ColumnDef, CreateSubsourceOption, CreateSubsourceOptionName, CsrConnection, CsrSeedAvro,
    CsrSeedProtobuf, CsrSeedProtobufSchema, DbzMode, DeferredObjectName, Envelope, Ident,
    KafkaConfigOption, KafkaConfigOptionName, KafkaConnection, KafkaSourceConnection,
    PgConfigOption, PgConfigOptionName, ReaderSchemaSelectionStrategy, UnresolvedObjectName,
};
use mz_storage_client::types::connections::aws::AwsConfig;
use mz_storage_client::types::connections::{Connection, ConnectionContext};
use mz_storage_client::types::sources::PostgresSourcePublicationDetails;

use crate::ast::{
    AvroSchema, CreateSourceConnection, CreateSourceFormat, CreateSourceStatement,
    CreateSourceSubsource, CreateSubsourceStatement, CsrConnectionAvro, CsrConnectionProtobuf,
    CsvColumns, Format, ProtobufSchema, ReferencedSubsources, Value, WithOptionValue,
};
use crate::catalog::{ErsatzCatalog, SessionCatalog};
use crate::kafka_util;
use crate::kafka_util::KafkaConfigOptionExtracted;
use crate::names::{Aug, RawDatabaseSpecifier};
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::statement::ddl::load_generator_ast_to_generator;
use crate::plan::StatementContext;

fn subsource_gen<'a, T>(
    selected_subsources: &mut Vec<CreateSourceSubsource<Aug>>,
    catalog: &ErsatzCatalog<'a, T>,
) -> Result<Vec<(UnresolvedObjectName, UnresolvedObjectName, &'a T)>, PlanError> {
    let mut validated_requested_subsources = vec![];

    for subsource in selected_subsources {
        let subsource_name = match &subsource.subsource {
            Some(name) => match name {
                DeferredObjectName::Deferred(name) => name.clone(),
                DeferredObjectName::Named(..) => {
                    sql_bail!("Cannot manually ID qualify subsources")
                }
            },
            None => {
                // Use the entered name as the upstream reference, and then use
                // the item as the subsource name to ensure it's created in the
                // current schema, not mirroring the schema of the reference.
                UnresolvedObjectName::unqualified(
                    &normalize::unresolved_object_name(subsource.reference.clone())?.item,
                )
            }
        };

        let (qualified_upstream_name, desc) = catalog.resolve(subsource.reference.clone())?;

        validated_requested_subsources.push((qualified_upstream_name, subsource_name, desc));
    }

    Ok(validated_requested_subsources)
}

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
    PlanError,
> {
    let CreateSourceStatement {
        connection,
        format,
        envelope,
        include_metadata: _,
        referenced_subsources: requested_subsources,
        progress_subsource,
        ..
    } = &mut stmt;

    if progress_subsource.is_some() {
        bail_unsupported!("PROGRESS subsources not yet supported");
    }

    // Disallow manually targetting subsources, this syntax is reserved for purification only
    if let Some(ReferencedSubsources::Subset(subsources)) = requested_subsources {
        for CreateSourceSubsource {
            subsource,
            reference: _,
        } in subsources
        {
            if let Some(DeferredObjectName::Named(_)) = subsource {
                sql_bail!("Cannot manually ID qualify subsources");
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
            let mut connection = {
                let item = scx.get_item_by_resolved_name(connection)?;
                // Get Kafka connection
                match item.connection()? {
                    Connection::Kafka(connection) => connection.clone(),
                    _ => sql_bail!("{} is not a kafka connection", item.name()),
                }
            };

            let extracted_options: KafkaConfigOptionExtracted =
                base_with_options.clone().try_into()?;

            let offset_type =
                Option::<kafka_util::KafkaStartOffsetType>::try_from(&extracted_options)?;

            for (k, v) in kafka_util::LibRdKafkaConfig::try_from(&extracted_options)?.0 {
                connection.options.insert(k, v);
            }

            let topic = extracted_options
                .topic
                .ok_or_else(|| sql_err!("KAFKA CONNECTION without TOPIC"))?;

            let consumer = kafka_util::create_consumer(&connection_context, &connection, &topic)
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
                            value: Some(WithOptionValue::Sequence(
                                start_offsets
                                    .iter()
                                    .map(|offset| {
                                        WithOptionValue::Value(Value::Number(offset.to_string()))
                                    })
                                    .collect(),
                            )),
                        });
                    }
                    None => {}
                }
            }
        }
        CreateSourceConnection::TestScript { desc_json: _ } => {
            // TODO: verify valid json and valid schema
        }
        CreateSourceConnection::S3 { connection, .. } => {
            let scx = StatementContext::new(None, &*catalog);
            let aws = {
                let item = scx.get_item_by_resolved_name(connection)?;
                match item.connection()? {
                    Connection::Aws(aws) => aws.clone(),
                    _ => sql_bail!("{} is not an AWS connection", item.name()),
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
                    _ => sql_bail!("{} is not an AWS connection", item.name()),
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
                    _ => sql_bail!("{} is not a postgres connection", item.name()),
                }
            };
            let crate::plan::statement::PgConfigOptionExtracted {
                publication,
                mut text_columns,
                ..
            } = options.clone().try_into()?;
            let publication = publication
                .ok_or_else(|| sql_err!("POSTGRES CONNECTION must specify PUBLICATION"))?;

            // verify that we can connect upstream and snapshot publication metadata
            let config = connection
                .config(&*connection_context.secrets_reader)
                .await?;
            let publication_tables = mz_postgres_util::publication_info(&config, &publication)
                .await
                .map_err(|cause| PlanError::FetchingPostgresPublicationInfoFailed {
                    cause: Arc::new(cause),
                })?;

            // An index from table name -> schema name -> database name -> PostgresTableDesc
            let mut tables_by_name = BTreeMap::new();
            for table in &publication_tables {
                tables_by_name
                    .entry(table.name.clone())
                    .or_insert_with(BTreeMap::new)
                    .entry(table.namespace.clone())
                    .or_insert_with(BTreeMap::new)
                    .entry(connection.database.clone())
                    .or_insert(table);
            }

            let publication_catalog = ErsatzCatalog(tables_by_name);

            let mut targeted_subsources = vec![];

            let mut validated_requested_subsources = vec![];
            match requested_subsources {
                Some(ReferencedSubsources::All) => {
                    for table in &publication_tables {
                        let upstream_name = UnresolvedObjectName::qualified(&[
                            &connection.database,
                            &table.namespace,
                            &table.name,
                        ]);
                        let subsource_name = UnresolvedObjectName::unqualified(&table.name);
                        validated_requested_subsources.push((upstream_name, subsource_name, table));
                    }
                }
                Some(ReferencedSubsources::Subset(subsources)) => {
                    // The user manually selected a subset of upstream tables so we need to
                    // validate that the names actually exist and are not ambiguous

                    // An index from table name -> schema name -> database name -> PostgresTableDesc
                    let mut tables_by_name = BTreeMap::new();
                    for table in &publication_tables {
                        tables_by_name
                            .entry(table.name.clone())
                            .or_insert_with(BTreeMap::new)
                            .entry(table.namespace.clone())
                            .or_insert_with(BTreeMap::new)
                            .entry(connection.database.clone())
                            .or_insert(table);
                    }

                    validated_requested_subsources
                        .extend(subsource_gen(subsources, &publication_catalog)?);
                }
                None => {}
            };

            let mut text_cols_dict: BTreeMap<u32, BTreeSet<String>> = BTreeMap::new();

            for name in text_columns.iter_mut() {
                let (qual, col) = match name.0.split_last().expect("must have at least one element")
                {
                    (col, qual) if qual.is_empty() => {
                        return Err(PlanError::InvalidOptionValue {
                            option_name: PgConfigOptionName::TextColumns.to_ast_string(),
                            err: Box::new(PlanError::UnderqualifiedColumnName(
                                col.as_str().to_string(),
                            )),
                        });
                    }
                    (col, qual) => (qual.to_vec(), col.as_str().to_string()),
                };

                let qual_name = UnresolvedObjectName(qual);

                let (mut fully_qualified_name, desc) = publication_catalog
                    .resolve(qual_name)
                    .map_err(|e| PlanError::InvalidOptionValue {
                        option_name: PgConfigOptionName::TextColumns.to_ast_string(),
                        err: Box::new(e),
                    })?;

                if !desc.columns.iter().any(|column| column.name == col) {
                    return Err(PlanError::InvalidOptionValue {
                        option_name: PgConfigOptionName::TextColumns.to_ast_string(),
                        err: Box::new(PlanError::UnknownColumn {
                            table: Some(
                                normalize::unresolved_object_name(fully_qualified_name)
                                    .expect("known to be of valid len"),
                            ),
                            column: mz_repr::ColumnName::from(col),
                        }),
                    });
                }

                // Rewrite fully qualified name.
                fully_qualified_name.0.push(col.as_str().to_string().into());
                *name = fully_qualified_name;

                let new = text_cols_dict
                    .entry(desc.oid)
                    .or_default()
                    .insert(col.as_str().to_string());

                if !new {
                    return Err(PlanError::InvalidOptionValue {
                        option_name: PgConfigOptionName::TextColumns.to_ast_string(),
                        err: Box::new(PlanError::UnexpectedDuplicateReference {
                            name: name.clone(),
                        }),
                    });
                }
            }

            // Normalize options to contain full qualified values.
            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == PgConfigOptionName::TextColumns)
            {
                let seq = text_columns
                    .into_iter()
                    .map(WithOptionValue::UnresolvedObjectName)
                    .collect();
                text_cols_option.value = Some(WithOptionValue::Sequence(seq));
            }

            // Aggregate all unrecognized types.
            let mut unsupported_cols = vec![];

            // Now that we have an explicit list of validated requested subsources we can create them
            for (i, (upstream_name, subsource_name, table)) in
                validated_requested_subsources.into_iter().enumerate()
            {
                // Figure out the schema of the subsource
                let mut columns = vec![];
                for c in table.columns.iter() {
                    let name = Ident::new(c.name.clone());
                    let ty = match text_cols_dict.get(&table.oid) {
                        Some(names) if names.contains(&c.name) => mz_pgrepr::Type::Text,
                        _ => match mz_pgrepr::Type::from_oid_and_typmod(c.type_oid, c.type_mod) {
                            Ok(t) => t,
                            Err(_) => {
                                let mut full_name = upstream_name.0.clone();
                                full_name.push(name);
                                unsupported_cols.push((
                                    UnresolvedObjectName(full_name).to_ast_string(),
                                    Oid(c.type_oid),
                                ));
                                continue;
                            }
                        },
                    };

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
                let subsource =
                    scx.allocate_resolved_object_name(transient_id, subsource_name.clone())?;

                targeted_subsources.push(CreateSourceSubsource {
                    reference: upstream_name,
                    subsource: Some(DeferredObjectName::Named(subsource)),
                });

                // Create the subsource statement
                let subsource = CreateSubsourceStatement {
                    name: subsource_name,
                    columns,
                    // TODO(petrosagg): nothing stops us from getting the constraints of the
                    // upstream tables and mirroring them here which will lead to more optimization
                    // opportunities if for example there is a primary key or an index.
                    //
                    // If we ever do that we must triple check that we will get notified *in the
                    // replication stream*, if our assumptions change. Failure to do that could
                    // mean that an upstream table that started with an index was then altered to
                    // one without and now we're producing garbage data.
                    constraints: vec![],
                    if_not_exists: false,
                    with_options: vec![CreateSubsourceOption {
                        name: CreateSubsourceOptionName::References,
                        value: Some(WithOptionValue::Value(Value::Boolean(true))),
                    }],
                };
                subsources.push((transient_id, subsource));
            }

            if !unsupported_cols.is_empty() {
                return Err(PlanError::UnrecognizedTypeInPostgresSource {
                    cols: unsupported_cols,
                });
            }

            *requested_subsources = Some(ReferencedSubsources::Subset(targeted_subsources));

            // Remove any old detail references
            options.retain(|PgConfigOption { name, .. }| name != &PgConfigOptionName::Details);
            let details = PostgresSourcePublicationDetails {
                tables: publication_tables,
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
        CreateSourceConnection::LoadGenerator { generator, options } => {
            let scx = StatementContext::new(None, &*catalog);

            let (_load_generator, available_subsources) =
                load_generator_ast_to_generator(generator, options)?;

            let mut targeted_subsources = vec![];

            let mut validated_requested_subsources = vec![];
            match requested_subsources {
                Some(ReferencedSubsources::All) => {
                    let available_subsources = match &available_subsources {
                        Some(available_subsources) => available_subsources,
                        None => {
                            sql_bail!("FOR ALL TABLES is only valid for multi-output sources")
                        }
                    };
                    for (name, (_, desc)) in available_subsources {
                        let upstream_name = UnresolvedObjectName::from(name.clone());
                        let subsource_name = UnresolvedObjectName::unqualified(&name.item);
                        validated_requested_subsources.push((upstream_name, subsource_name, desc));
                    }
                }
                Some(ReferencedSubsources::Subset(selected_subsources)) => {
                    let available_subsources = match &available_subsources {
                        Some(available_subsources) => available_subsources,
                        None => {
                            sql_bail!("FOR TABLES (..) is only valid for multi-output sources")
                        }
                    };
                    // The user manually selected a subset of upstream tables so we need to
                    // validate that the names actually exist and are not ambiguous

                    // An index from table name -> schema name -> database name -> PostgresTableDesc
                    let mut tables_by_name = BTreeMap::new();
                    for (subsource_name, (_, desc)) in available_subsources {
                        let database = match &subsource_name.database {
                            RawDatabaseSpecifier::Name(database) => database.clone(),
                            RawDatabaseSpecifier::Ambient => unreachable!(),
                        };
                        tables_by_name
                            .entry(subsource_name.item.clone())
                            .or_insert_with(BTreeMap::new)
                            .entry(subsource_name.schema.clone())
                            .or_insert_with(BTreeMap::new)
                            .entry(database)
                            .or_insert(desc);
                    }

                    validated_requested_subsources.extend(subsource_gen(
                        selected_subsources,
                        &ErsatzCatalog(tables_by_name),
                    )?);
                }
                None => {
                    if available_subsources.is_some() {
                        sql_bail!("multi-output sources require a FOR TABLES (..) or FOR ALL TABLES statement");
                    }
                }
            };

            // Now that we have an explicit list of validated requested subsources we can create them
            for (i, (upstream_name, subsource_name, desc)) in
                validated_requested_subsources.into_iter().enumerate()
            {
                let (columns, table_constraints) = scx.relation_desc_into_table_defs(desc)?;

                // Create the targeted AST node for the original CREATE SOURCE statement
                let transient_id = GlobalId::Transient(u64::cast_from(i));
                let subsource =
                    scx.allocate_resolved_object_name(transient_id, subsource_name.clone())?;
                targeted_subsources.push(CreateSourceSubsource {
                    reference: upstream_name,
                    subsource: Some(DeferredObjectName::Named(subsource)),
                });

                // Create the subsource statement
                let subsource = CreateSubsourceStatement {
                    name: subsource_name,
                    columns,
                    // unlike sources that come from an external upstream, we
                    // have more leniency to introduce different constraints
                    // every time the load generator is run; i.e. we are not as
                    // worried about introducing junk data.
                    constraints: table_constraints,
                    if_not_exists: false,
                    with_options: vec![CreateSubsourceOption {
                        name: CreateSubsourceOptionName::References,
                        value: Some(WithOptionValue::Value(Value::Boolean(true))),
                    }],
                };
                subsources.push((transient_id, subsource));
            }
            if available_subsources.is_some() {
                *requested_subsources = Some(ReferencedSubsources::Subset(targeted_subsources));
            }
        }
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
) -> Result<(), PlanError> {
    if matches!(format, CreateSourceFormat::KeyValue { .. })
        && !matches!(
            connection,
            CreateSourceConnection::Kafka { .. } | CreateSourceConnection::TestScript { .. }
        )
    {
        // We don't mention `TestScript` to users here
        sql_bail!("Kafka sources are the only source type that can provide KEY/VALUE formats")
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
) -> Result<(), PlanError> {
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
                            sql_bail!("CSV WITH HEADER for S3 sources requires specifying the header columns");
                        }
                    }
                    _ => sql_bail!("CSV WITH HEADER is only supported for S3 sources"),
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
) -> Result<(), PlanError> {
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
        sql_bail!("Confluent Schema Registry is only supported with Kafka sources")
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
                _ => sql_bail!("{} is not a schema registry connection", connection),
            };

            let ccsr_client = ccsr_connection
                .connect(&*connection_context.secrets_reader)
                .await?;

            let value = compile_proto(&format!("{}-value", topic), &ccsr_client).await?;
            let key = compile_proto(&format!("{}-key", topic), &ccsr_client)
                .await
                .ok();

            if matches!(envelope, Some(Envelope::Debezium(DbzMode::Plain))) && key.is_none() {
                sql_bail!("Key schema is required for ENVELOPE DEBEZIUM");
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
) -> Result<(), PlanError> {
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
        sql_bail!("Confluent Schema Registry is only supported with Kafka sources")
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
            _ => sql_bail!("{} is not a schema registry connection", connection),
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
            sql_bail!("Key schema is required for ENVELOPE DEBEZIUM");
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

async fn get_schema_with_strategy(
    client: &Client,
    strategy: ReaderSchemaSelectionStrategy,
    subject: &str,
) -> Result<Option<String>, PlanError> {
    match strategy {
        ReaderSchemaSelectionStrategy::Latest => {
            match client.get_schema_by_subject(subject).await {
                Ok(CcsrSchema { raw, .. }) => Ok(Some(raw)),
                Err(GetBySubjectError::SubjectNotFound) => Ok(None),
                Err(e) => Err(PlanError::FetchingCsrSchemaFailed {
                    schema_lookup: format!("subject {}", subject.quoted()),
                    cause: Arc::new(e),
                }),
            }
        }
        ReaderSchemaSelectionStrategy::Inline(raw) => Ok(Some(raw)),
        ReaderSchemaSelectionStrategy::ById(id) => match client.get_schema_by_id(id).await {
            Ok(CcsrSchema { raw, .. }) => Ok(Some(raw)),
            Err(GetByIdError::SchemaNotFound) => Ok(None),
            Err(e) => Err(PlanError::FetchingCsrSchemaFailed {
                schema_lookup: format!("ID {}", id),
                cause: Arc::new(e),
            }),
        },
    }
}

async fn get_remote_csr_schema(
    ccsr_client: &mz_ccsr::Client,
    key_strategy: ReaderSchemaSelectionStrategy,
    value_strategy: ReaderSchemaSelectionStrategy,
    topic: String,
) -> Result<Schema, PlanError> {
    let value_schema_name = format!("{}-value", topic);
    let value_schema =
        get_schema_with_strategy(ccsr_client, value_strategy, &value_schema_name).await?;
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
) -> Result<CsrSeedProtobufSchema, PlanError> {
    let (primary_subject, dependency_subjects) = ccsr_client
        .get_subject_and_references(subject_name)
        .await
        .map_err(|e| PlanError::FetchingCsrSchemaFailed {
            schema_lookup: format!("subject {}", subject_name.quoted()),
            cause: Arc::new(e),
        })?;

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
        .build_file_descriptor_set(&[Path::new(&primary_subject.name)])
        .map_err(|cause| PlanError::InvalidProtobufSchema { cause })?;

    // Ensure there is exactly one message in the file.
    let primary_fd = fds.file(0);
    let message_name = match primary_fd.message_type_size() {
        1 => String::from_utf8_lossy(primary_fd.message_type(0).name()).into_owned(),
        0 => bail_unsupported!(9598, "Protobuf schemas with no messages"),
        _ => bail_unsupported!(9598, "Protobuf schemas with multiple messages"),
    };

    // Encode the file descriptor set into a SQL byte string.
    let bytes = &fds
        .serialize()
        .map_err(|cause| PlanError::InvalidProtobufSchema { cause })?;
    let mut schema = String::new();
    strconv::format_bytes(&mut schema, bytes);

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
) -> Result<(), PlanError> {
    let config = config.load(external_id_prefix, None, secrets_reader).await;
    let sts_client = aws_sdk_sts::Client::new(&config);
    let _ = sts_client
        .get_caller_identity()
        .send()
        .await
        .context("Unable to validate AWS credentials")?;
    Ok(())
}
