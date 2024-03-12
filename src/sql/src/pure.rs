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

use anyhow::anyhow;
use itertools::Itertools;
use mz_ccsr::{Client, GetByIdError, GetBySubjectError, Schema as CcsrSchema};
use mz_kafka_util::client::MzClientContext;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::iter::IteratorExt;
use mz_ore::str::StrExt;
use mz_postgres_util::replication::WalLevel;
use mz_proto::RustType;
use mz_repr::{strconv, Timestamp};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit::{visit_function, Visit};
use mz_sql_parser::ast::visit_mut::{visit_expr_mut, VisitMut};
use mz_sql_parser::ast::{
    AlterSourceAction, AlterSourceAddSubsourceOptionName, AlterSourceStatement, AvroDocOn,
    CreateMaterializedViewStatement, CreateSinkConnection, CreateSinkStatement,
    CreateSubsourceOption, CreateSubsourceOptionName, CsrConfigOption, CsrConfigOptionName,
    CsrConnection, CsrSeedAvro, CsrSeedProtobuf, CsrSeedProtobufSchema, DeferredItemName,
    DocOnIdentifier, DocOnSchema, Expr, Function, FunctionArgs, Ident, KafkaSourceConfigOption,
    KafkaSourceConfigOptionName, MaterializedViewOption, MaterializedViewOptionName,
    MySqlConfigOption, MySqlConfigOptionName, PgConfigOption, PgConfigOptionName, RawItemName,
    ReaderSchemaSelectionStrategy, RefreshAtOptionValue, RefreshEveryOptionValue,
    RefreshOptionValue, SourceEnvelope, Statement, UnresolvedItemName,
};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::connections::Connection;
use mz_storage_types::errors::ContextCreationError;
use mz_storage_types::sources::mysql::MySqlSourceDetails;
use mz_storage_types::sources::postgres::PostgresSourcePublicationDetails;
use mz_storage_types::sources::{GenericSourceConnection, SourceConnection};
use prost::Message;
use protobuf_native::compiler::{SourceTreeDescriptorDatabase, VirtualSourceTree};
use protobuf_native::MessageLite;
use rdkafka::admin::AdminClient;
use uuid::Uuid;

use crate::ast::{
    AvroSchema, CreateSourceConnection, CreateSourceFormat, CreateSourceStatement,
    CreateSourceSubsource, CreateSubsourceStatement, CsrConnectionAvro, CsrConnectionProtobuf,
    Format, ProtobufSchema, ReferencedSubsources, Value, WithOptionValue,
};
use crate::catalog::{CatalogItemType, SessionCatalog, SubsourceCatalog};
use crate::kafka_util::{KafkaSinkConfigOptionExtracted, KafkaSourceConfigOptionExtracted};
use crate::names::{
    Aug, FullItemName, PartialItemName, ResolvedColumnName, ResolvedDataType, ResolvedIds,
    ResolvedItemName,
};
use crate::plan::error::PlanError;
use crate::plan::statement::ddl::load_generator_ast_to_generator;
use crate::plan::StatementContext;
use crate::{kafka_util, normalize};

use self::error::{
    CsrPurificationError, KafkaSinkPurificationError, KafkaSourcePurificationError,
    LoadGeneratorSourcePurificationError, MySqlSourcePurificationError, PgSourcePurificationError,
};

pub(crate) mod error;
mod mysql;
mod postgres;

pub(crate) struct RequestedSubsource<'a, T> {
    upstream_name: UnresolvedItemName,
    subsource_name: UnresolvedItemName,
    table: &'a T,
}

fn subsource_gen<'a, T>(
    selected_subsources: &mut Vec<CreateSourceSubsource<Aug>>,
    catalog: &SubsourceCatalog<&'a T>,
    source_name: &UnresolvedItemName,
) -> Result<Vec<RequestedSubsource<'a, T>>, PlanError> {
    let mut validated_requested_subsources = vec![];

    for subsource in selected_subsources {
        let subsource_name = match &subsource.subsource {
            Some(name) => match name {
                DeferredItemName::Deferred(name) => {
                    let partial = normalize::unresolved_item_name(name.clone())?;
                    match partial.schema {
                        Some(_) => name.clone(),
                        // In cases when a prefix is not provided for the deferred name
                        // fallback to using the schema of the source with the given name
                        None => subsource_name_gen(source_name, &partial.item)?,
                    }
                }
                DeferredItemName::Named(..) => {
                    unreachable!("already errored on this condition")
                }
            },
            None => {
                // Use the entered name as the upstream reference, and then use
                // the item as the subsource name to ensure it's created in the
                // current schema or the source's schema if provided, not mirroring
                // the schema of the reference.
                subsource_name_gen(
                    source_name,
                    &normalize::unresolved_item_name(subsource.reference.clone())?.item,
                )?
            }
        };

        let (qualified_upstream_name, desc) = catalog.resolve(subsource.reference.clone())?;

        validated_requested_subsources.push(RequestedSubsource {
            upstream_name: qualified_upstream_name,
            subsource_name,
            table: *desc,
        });
    }

    Ok(validated_requested_subsources)
}

// Convenience function to ensure subsources are not named.
fn named_subsource_err(name: &Option<DeferredItemName<Aug>>) -> Result<(), PlanError> {
    match name {
        Some(DeferredItemName::Named(_)) => {
            sql_bail!("Cannot manually ID qualify subsources")
        }
        _ => Ok(()),
    }
}

/// Generates a subsource name by prepending source schema name if present
///
/// For eg. if source is `a.b`, then `a` will be prepended to the subsource name
/// so that it's generated in the same schema as source
fn subsource_name_gen(
    source_name: &UnresolvedItemName,
    subsource_name: &String,
) -> Result<UnresolvedItemName, PlanError> {
    let mut partial = normalize::unresolved_item_name(source_name.clone())?;
    partial.item = subsource_name.to_string();
    Ok(UnresolvedItemName::from(partial))
}

/// Validates the requested subsources do not have name conflicts with each other
/// and that the same upstream table is not referenced multiple times.
fn validate_subsource_names<T>(
    requested_subsources: &[RequestedSubsource<T>],
) -> Result<(), PlanError> {
    // This condition would get caught during the catalog transaction, but produces a
    // vague, non-contextual error. Instead, error here so we can suggest to the user
    // how to fix the problem.
    if let Some(name) = requested_subsources
        .iter()
        .map(|subsource| &subsource.subsource_name)
        .duplicates()
        .next()
        .cloned()
    {
        let mut upstream_references: Vec<_> = requested_subsources
            .into_iter()
            .filter_map(|subsource| {
                if &subsource.subsource_name == &name {
                    Some(subsource.upstream_name.clone())
                } else {
                    None
                }
            })
            .collect();

        upstream_references.sort();

        Err(PlanError::SubsourceNameConflict {
            name,
            upstream_references,
        })?;
    }

    // We technically could allow multiple subsources to ingest the same upstream table, but
    // it is almost certainly an error on the user's end.
    if let Some(name) = requested_subsources
        .iter()
        .map(|subsource| &subsource.upstream_name)
        .duplicates()
        .next()
        .cloned()
    {
        let mut target_names: Vec<_> = requested_subsources
            .into_iter()
            .filter_map(|subsource| {
                if &subsource.upstream_name == &name {
                    Some(subsource.subsource_name.clone())
                } else {
                    None
                }
            })
            .collect();

        target_names.sort();

        Err(PlanError::SubsourceDuplicateReference { name, target_names })?;
    }

    Ok(())
}

pub fn add_output_index_to_subsources<F>(
    subsources: &mut Vec<CreateSubsourceStatement<Aug>>,
    output_idx_for_name: F,
) where
    F: Fn(&UnresolvedItemName) -> Option<usize>,
{
    for subsource in subsources {
        let option = subsource
            .with_options
            .iter()
            .find(|o| o.name == CreateSubsourceOptionName::ExternalReference)
            .expect("all can only add external reference subsoutrces");
        let name = match &option.value {
            Some(WithOptionValue::UnresolvedItemName(name)) => name,
            _ => unreachable!(),
        };

        let output_idx = output_idx_for_name(name)
            .expect("only determine output index for tables you've validated exist");
        subsource.with_options.push(CreateSubsourceOption {
            name: CreateSubsourceOptionName::InitOutputIndex,
            value: Some(WithOptionValue::Value(Value::Number(
                output_idx.to_string(),
            ))),
        })
    }
}

/// Purifies a statement, removing any dependencies on external state.
///
/// See the section on [purification](crate#purification) in the crate
/// documentation for details.
///
/// Note that this doesn't handle CREATE MATERIALIZED VIEW, which is
/// handled by [purify_create_materialized_view_options] instead.
/// This could be made more consistent by a refactoring discussed here:
/// <https://github.com/MaterializeInc/materialize/pull/23870#discussion_r1435922709>
pub async fn purify_statement(
    catalog: impl SessionCatalog,
    now: u64,
    stmt: Statement<Aug>,
    storage_configuration: &StorageConfiguration,
) -> Result<(Vec<CreateSubsourceStatement<Aug>>, Statement<Aug>), PlanError> {
    match stmt {
        Statement::CreateSource(stmt) => {
            purify_create_source(catalog, now, stmt, storage_configuration).await
        }
        Statement::AlterSource(stmt) => {
            purify_alter_source(catalog, stmt, storage_configuration).await
        }
        Statement::CreateSink(stmt) => {
            let r = purify_create_sink(catalog, stmt, storage_configuration).await?;
            Ok((vec![], r))
        }
        o => unreachable!("{:?} does not need to be purified", o),
    }
}

/// Updates the CREATE SINK statement with materialize comments
/// if `enable_sink_doc_on_option` feature flag is enabled
pub(crate) fn add_materialize_comments(
    catalog: &dyn SessionCatalog,
    stmt: &mut CreateSinkStatement<Aug>,
) -> Result<(), PlanError> {
    // updating avro format with comments so that they are frozen in the `create_sql`
    // only if the feature is enabled
    if catalog.system_vars().enable_sink_doc_on_option() {
        let from_id = stmt.from.item_id();
        let from = catalog.get_item(from_id);
        let object_ids = from.references().0.clone().into_iter().chain_one(from.id());

        // add comments to the avro doc comments
        if let Some(Format::Avro(AvroSchema::Csr {
            csr_connection:
                CsrConnectionAvro {
                    connection:
                        CsrConnection {
                            connection: _,
                            options,
                        },
                    ..
                },
        })) = &mut stmt.format
        {
            let user_provided_comments = &options
                .iter()
                .filter_map(|CsrConfigOption { name, .. }| match name {
                    CsrConfigOptionName::AvroDocOn(doc_on) => Some(doc_on.clone()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();

            // Adding existing comments if not already provided by user
            for object_id in object_ids {
                let item = catalog.get_item(&object_id);
                let full_name = catalog.resolve_full_name(item.name());
                let full_resolved_name = ResolvedItemName::Item {
                    id: object_id,
                    qualifiers: item.name().qualifiers.clone(),
                    full_name: full_name.clone(),
                    print_id: !matches!(
                        item.item_type(),
                        CatalogItemType::Func | CatalogItemType::Type
                    ),
                };

                if let Some(comments_map) = catalog.get_item_comments(&object_id) {
                    // Getting comment on the item
                    let doc_on_item_key = AvroDocOn {
                        identifier: DocOnIdentifier::Type(full_resolved_name.clone()),
                        for_schema: DocOnSchema::All,
                    };
                    if !user_provided_comments.contains(&doc_on_item_key) {
                        if let Some(root_comment) = comments_map.get(&None) {
                            options.push(CsrConfigOption {
                                name: CsrConfigOptionName::AvroDocOn(doc_on_item_key),
                                value: Some(mz_sql_parser::ast::WithOptionValue::Value(
                                    Value::String(root_comment.clone()),
                                )),
                            });
                        }
                    }

                    // Getting comments on columns in the item
                    if let Ok(desc) = item.desc(&full_name) {
                        for (pos, column_name) in desc.iter_names().enumerate() {
                            let comment = comments_map.get(&Some(pos + 1));
                            if let Some(comment_str) = comment {
                                let doc_on_column_key = AvroDocOn {
                                    identifier: DocOnIdentifier::Column(
                                        ResolvedColumnName::Column {
                                            relation: full_resolved_name.clone(),
                                            name: column_name.to_owned(),
                                            index: pos,
                                        },
                                    ),
                                    for_schema: DocOnSchema::All,
                                };
                                if !user_provided_comments.contains(&doc_on_column_key) {
                                    options.push(CsrConfigOption {
                                        name: CsrConfigOptionName::AvroDocOn(doc_on_column_key),
                                        value: Some(mz_sql_parser::ast::WithOptionValue::Value(
                                            Value::String(comment_str.clone()),
                                        )),
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

/// Checks that the sink described in the statement can connect to its external
/// resources.
///
/// We must not leave any state behind in the Kafka broker, so just ensure that
/// we can connect. This means we don't ensure that we can create the topic and
/// introduces TOCTOU errors, but creating an inoperable sink is infinitely
/// preferable to leaking state in users' environments.
async fn purify_create_sink(
    catalog: impl SessionCatalog,
    mut stmt: CreateSinkStatement<Aug>,
    storage_configuration: &StorageConfiguration,
) -> Result<Statement<Aug>, PlanError> {
    add_materialize_comments(&catalog, &mut stmt)?;
    // General purification
    let CreateSinkStatement {
        connection, format, ..
    } = &stmt;

    match &connection {
        CreateSinkConnection::Kafka {
            connection,
            options,
            key: _,
        } => {
            let scx = StatementContext::new(None, &catalog);
            let connection = {
                let item = scx.get_item_by_resolved_name(connection)?;
                // Get Kafka connection
                match item.connection()? {
                    Connection::Kafka(connection) => {
                        connection.clone().into_inline_connection(scx.catalog)
                    }
                    _ => sql_bail!(
                        "{} is not a kafka connection",
                        scx.catalog.resolve_full_name(item.name())
                    ),
                }
            };

            let extracted_options: KafkaSinkConfigOptionExtracted = options.clone().try_into()?;

            if extracted_options.legacy_ids == Some(true) {
                sql_bail!("LEGACY IDs option is not supported");
            }

            let client: AdminClient<_> = connection
                .create_with_context(
                    storage_configuration,
                    MzClientContext::default(),
                    &BTreeMap::new(),
                    InTask::No,
                )
                .await
                .map_err(|e| {
                    // anyhow doesn't support Clone, so not trivial to move into PlanError
                    KafkaSinkPurificationError::AdminClientError(Arc::new(e))
                })?;

            let metadata = client
                .inner()
                .fetch_metadata(
                    None,
                    storage_configuration
                        .parameters
                        .kafka_timeout_config
                        .fetch_metadata_timeout,
                )
                .map_err(|e| {
                    KafkaSinkPurificationError::AdminClientError(Arc::new(
                        ContextCreationError::KafkaError(e),
                    ))
                })?;

            if metadata.brokers().len() == 0 {
                Err(KafkaSinkPurificationError::ZeroBrokers)?;
            }
        }
    }

    if let Some(format) = format {
        match format {
            Format::Avro(AvroSchema::Csr {
                csr_connection: CsrConnectionAvro { connection, .. },
            })
            | Format::Protobuf(ProtobufSchema::Csr {
                csr_connection: CsrConnectionProtobuf { connection, .. },
            }) => {
                let connection = {
                    let scx = StatementContext::new(None, &catalog);
                    let item = scx.get_item_by_resolved_name(&connection.connection)?;
                    // Get Kafka connection
                    match item.connection()? {
                        Connection::Csr(connection) => {
                            connection.clone().into_inline_connection(&catalog)
                        }
                        _ => Err(CsrPurificationError::NotCsrConnection(
                            scx.catalog.resolve_full_name(item.name()),
                        ))?,
                    }
                };

                let client = connection
                    .connect(storage_configuration, InTask::No)
                    .await
                    .map_err(|e| CsrPurificationError::ClientError(Arc::new(e)))?;

                client
                    .list_subjects()
                    .await
                    .map_err(|e| CsrPurificationError::ListSubjectsError(Arc::new(e)))?;
            }
            Format::Avro(AvroSchema::InlineSchema { .. })
            | Format::Bytes
            | Format::Csv { .. }
            | Format::Json { .. }
            | Format::Protobuf(ProtobufSchema::InlineSchema { .. })
            | Format::Regex(..)
            | Format::Text => {}
        }
    }

    Ok(Statement::CreateSink(stmt))
}

async fn purify_create_source(
    catalog: impl SessionCatalog,
    now: u64,
    mut stmt: CreateSourceStatement<Aug>,
    storage_configuration: &StorageConfiguration,
) -> Result<(Vec<CreateSubsourceStatement<Aug>>, Statement<Aug>), PlanError> {
    let CreateSourceStatement {
        name: source_name,
        connection,
        format,
        envelope,
        include_metadata,
        referenced_subsources,
        progress_subsource,
        ..
    } = &mut stmt;

    // Disallow manually targetting subsources, this syntax is reserved for purification only
    named_subsource_err(progress_subsource)?;

    if let Some(ReferencedSubsources::SubsetTables(subsources)) = referenced_subsources {
        for CreateSourceSubsource {
            subsource,
            reference: _,
        } in subsources
        {
            named_subsource_err(subsource)?;
        }
    }

    let mut subsources = vec![];

    let progress_desc = match &connection {
        CreateSourceConnection::Kafka { .. } => {
            &mz_storage_types::sources::kafka::KAFKA_PROGRESS_DESC
        }
        CreateSourceConnection::Postgres { .. } => {
            &mz_storage_types::sources::postgres::PG_PROGRESS_DESC
        }
        CreateSourceConnection::MySql { .. } => {
            &mz_storage_types::sources::mysql::MYSQL_PROGRESS_DESC
        }
        CreateSourceConnection::LoadGenerator { .. } => {
            &mz_storage_types::sources::load_generator::LOAD_GEN_PROGRESS_DESC
        }
    };

    match connection {
        CreateSourceConnection::Kafka {
            connection,
            options: base_with_options,
            ..
        } => {
            if let Some(referenced_subsources) = referenced_subsources {
                Err(KafkaSourcePurificationError::ReferencedSubsources(
                    referenced_subsources.clone(),
                ))?;
            }

            let scx = StatementContext::new(None, &catalog);
            let connection = {
                let item = scx.get_item_by_resolved_name(connection)?;
                // Get Kafka connection
                match item.connection()? {
                    Connection::Kafka(connection) => {
                        connection.clone().into_inline_connection(&catalog)
                    }
                    _ => Err(KafkaSourcePurificationError::NotKafkaConnection(
                        scx.catalog.resolve_full_name(item.name()),
                    ))?,
                }
            };

            let extracted_options: KafkaSourceConfigOptionExtracted =
                base_with_options.clone().try_into()?;

            let topic = extracted_options
                .topic
                .ok_or(KafkaSourcePurificationError::ConnectionMissingTopic)?;

            let consumer = connection
                .create_with_context(
                    storage_configuration,
                    MzClientContext::default(),
                    &BTreeMap::new(),
                    InTask::No,
                )
                .await
                .map_err(|e| {
                    // anyhow doesn't support Clone, so not trivial to move into PlanError
                    KafkaSourcePurificationError::KafkaConsumerError(
                        e.display_with_causes().to_string(),
                    )
                })?;
            let consumer = Arc::new(consumer);

            match (
                extracted_options.start_offset,
                extracted_options.start_timestamp,
            ) {
                (None, None) => {
                    // Validate that the topic at least exists.
                    kafka_util::ensure_topic_exists(
                        Arc::clone(&consumer),
                        &topic,
                        storage_configuration
                            .parameters
                            .kafka_timeout_config
                            .fetch_metadata_timeout,
                    )
                    .await?;
                }
                (Some(_), Some(_)) => {
                    sql_bail!("cannot specify START TIMESTAMP and START OFFSET at same time")
                }
                (Some(start_offsets), None) => {
                    // Validate the start offsets.
                    kafka_util::validate_start_offsets(
                        Arc::clone(&consumer),
                        &topic,
                        start_offsets,
                        storage_configuration
                            .parameters
                            .kafka_timeout_config
                            .fetch_metadata_timeout,
                    )
                    .await?;
                }
                (None, Some(time_offset)) => {
                    // Translate `START TIMESTAMP` to a start offset.
                    let start_offsets = kafka_util::lookup_start_offsets(
                        Arc::clone(&consumer),
                        &topic,
                        time_offset,
                        now,
                        storage_configuration
                            .parameters
                            .kafka_timeout_config
                            .fetch_metadata_timeout,
                    )
                    .await?;

                    base_with_options.retain(|val| {
                        !matches!(val.name, KafkaSourceConfigOptionName::StartTimestamp)
                    });
                    base_with_options.push(KafkaSourceConfigOption {
                        name: KafkaSourceConfigOptionName::StartOffset,
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
            }
        }
        CreateSourceConnection::Postgres {
            connection,
            options,
        } => {
            let scx = StatementContext::new(None, &catalog);
            let connection = {
                let item = scx.get_item_by_resolved_name(connection)?;
                match item.connection().map_err(PlanError::from)? {
                    Connection::Postgres(connection) => {
                        connection.clone().into_inline_connection(&catalog)
                    }
                    _ => Err(PgSourcePurificationError::NotPgConnection(
                        scx.catalog.resolve_full_name(item.name()),
                    ))?,
                }
            };
            let crate::plan::statement::PgConfigOptionExtracted {
                publication,
                mut text_columns,
                details,
                ..
            } = options.clone().try_into()?;
            let publication =
                publication.ok_or(PgSourcePurificationError::ConnectionMissingPublication)?;

            if details.is_some() {
                Err(PgSourcePurificationError::UserSpecifiedDetails)?;
            }

            // verify that we can connect upstream and snapshot publication metadata
            let config = connection
                .config(
                    &storage_configuration.connection_context.secrets_reader,
                    storage_configuration,
                    InTask::No,
                )
                .await?;

            let wal_level = mz_postgres_util::get_wal_level(
                &storage_configuration.connection_context.ssh_tunnel_manager,
                &config,
            )
            .await?;

            if wal_level < WalLevel::Logical {
                Err(PgSourcePurificationError::InsufficientWalLevel { wal_level })?;
            }

            let max_wal_senders = mz_postgres_util::get_max_wal_senders(
                &storage_configuration.connection_context.ssh_tunnel_manager,
                &config,
            )
            .await?;

            if max_wal_senders < 1 {
                Err(PgSourcePurificationError::ReplicationDisabled)?;
            }

            let available_replication_slots = mz_postgres_util::available_replication_slots(
                &storage_configuration.connection_context.ssh_tunnel_manager,
                &config,
            )
            .await?;

            // We need 1 replication slot for the snapshots and 1 for the continuing replication
            if available_replication_slots < 2 {
                Err(PgSourcePurificationError::InsufficientReplicationSlotsAvailable { count: 2 })?;
            }

            let publication_tables = mz_postgres_util::publication_info(
                &storage_configuration.connection_context.ssh_tunnel_manager,
                &config,
                &publication,
            )
            .await?;

            if publication_tables.is_empty() {
                Err(PgSourcePurificationError::EmptyPublication(
                    publication.to_string(),
                ))?;
            }

            let publication_catalog = postgres::derive_catalog_from_publication_tables(
                &connection.database,
                &publication_tables,
            )?;

            let mut validated_requested_subsources = vec![];
            match referenced_subsources
                .as_mut()
                .ok_or(PgSourcePurificationError::RequiresReferencedSubsources)?
            {
                ReferencedSubsources::All => {
                    for table in &publication_tables {
                        let upstream_name = UnresolvedItemName::qualified(&[
                            Ident::new(&connection.database)?,
                            Ident::new(&table.namespace)?,
                            Ident::new(&table.name)?,
                        ]);
                        let subsource_name = subsource_name_gen(source_name, &table.name)?;
                        validated_requested_subsources.push(RequestedSubsource {
                            upstream_name,
                            subsource_name,
                            table,
                        });
                    }
                }
                ReferencedSubsources::SubsetSchemas(schemas) => {
                    let available_schemas: BTreeSet<_> = mz_postgres_util::get_schemas(
                        &storage_configuration.connection_context.ssh_tunnel_manager,
                        &config,
                    )
                    .await?
                    .into_iter()
                    .map(|s| s.name)
                    .collect();

                    let requested_schemas: BTreeSet<_> =
                        schemas.iter().map(|s| s.as_str().to_string()).collect();

                    let missing_schemas: Vec<_> = requested_schemas
                        .difference(&available_schemas)
                        .map(|s| s.to_string())
                        .collect();

                    if !missing_schemas.is_empty() {
                        Err(PgSourcePurificationError::DatabaseMissingFilteredSchemas {
                            database: connection.database.clone(),
                            schemas: missing_schemas,
                        })?;
                    }

                    for table in &publication_tables {
                        if !requested_schemas.contains(table.namespace.as_str()) {
                            continue;
                        }

                        let upstream_name = UnresolvedItemName::qualified(&[
                            Ident::new(&connection.database)?,
                            Ident::new(&table.namespace)?,
                            Ident::new(&table.name)?,
                        ]);
                        let subsource_name = subsource_name_gen(source_name, &table.name)?;
                        validated_requested_subsources.push(RequestedSubsource {
                            upstream_name,
                            subsource_name,
                            table,
                        });
                    }
                }
                ReferencedSubsources::SubsetTables(subsources) => {
                    // The user manually selected a subset of upstream tables so we need to
                    // validate that the names actually exist and are not ambiguous
                    validated_requested_subsources.extend(subsource_gen(
                        subsources,
                        &publication_catalog,
                        source_name,
                    )?);
                }
            };

            if validated_requested_subsources.is_empty() {
                sql_bail!(
                    "[internal error]: Postgres source must ingest at least one table, but {} matched none",
                    referenced_subsources.as_ref().unwrap().to_ast_string()
                );
            }

            validate_subsource_names(&validated_requested_subsources)?;

            postgres::validate_requested_subsources_privileges(
                &config,
                &validated_requested_subsources,
                &storage_configuration.connection_context.ssh_tunnel_manager,
            )
            .await?;

            let text_cols_dict = postgres::generate_text_columns(
                &publication_catalog,
                &mut text_columns,
                &PgConfigOptionName::TextColumns.to_ast_string(),
            )?;

            // Normalize options to contain full qualified values.
            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == PgConfigOptionName::TextColumns)
            {
                let mut seq: Vec<_> = text_columns
                    .into_iter()
                    .map(WithOptionValue::UnresolvedItemName)
                    .collect();

                seq.sort();
                seq.dedup();

                text_cols_option.value = Some(WithOptionValue::Sequence(seq));
            }

            let mut new_subsources = postgres::generate_targeted_subsources(
                &scx,
                None,
                validated_requested_subsources,
                text_cols_dict,
                &publication_tables,
            )?;

            // Now that we know which subsources to create alongside this
            // statement, remove the references so it is not canonicalized as
            // part of the `CREATE SOURCE` statement in the catalog.
            *referenced_subsources = None;

            // Record the active replication timeline_id to allow detection of a future upstream
            // point-in-time-recovery that will put the source into an error state.
            let replication_client = config
                .connect_replication(&storage_configuration.connection_context.ssh_tunnel_manager)
                .await?;
            let timeline_id = mz_postgres_util::get_timeline_id(&replication_client).await?;

            // Remove any old detail references
            options.retain(|PgConfigOption { name, .. }| name != &PgConfigOptionName::Details);
            let details = PostgresSourcePublicationDetails {
                tables: publication_tables,
                slot: format!(
                    "materialize_{}",
                    Uuid::new_v4().to_string().replace('-', "")
                ),
                timeline_id: Some(timeline_id),
            };

            add_output_index_to_subsources(&mut new_subsources, |name| {
                details.output_idx_for_name(name)
            });

            subsources.extend(new_subsources);

            options.push(PgConfigOption {
                name: PgConfigOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            })
        }
        CreateSourceConnection::MySql {
            connection,
            options,
        } => {
            let scx = StatementContext::new(None, &catalog);
            let connection_item = scx.get_item_by_resolved_name(connection)?;
            let connection = match connection_item.connection()? {
                Connection::MySql(connection) => {
                    connection.clone().into_inline_connection(&catalog)
                }
                _ => Err(MySqlSourcePurificationError::NotMySqlConnection(
                    scx.catalog.resolve_full_name(connection_item.name()),
                ))?,
            };
            let crate::plan::statement::ddl::MySqlConfigOptionExtracted {
                details,
                text_columns,
                ignore_columns,
                seen: _,
            } = options.clone().try_into()?;

            if details.is_some() {
                Err(MySqlSourcePurificationError::UserSpecifiedDetails)?;
            }

            let config = connection
                .config(
                    &storage_configuration.connection_context.secrets_reader,
                    storage_configuration,
                    InTask::No,
                )
                .await?;

            let mut conn = config
                .connect(
                    "mysql purification",
                    &storage_configuration.connection_context.ssh_tunnel_manager,
                )
                .await?;

            // Check if the MySQL database is configured to allow row-based consistent GTID replication
            let mut replication_errors = vec![];
            for error in [
                mz_mysql_util::ensure_gtid_consistency(&mut conn)
                    .await
                    .err(),
                mz_mysql_util::ensure_full_row_binlog_format(&mut conn)
                    .await
                    .err(),
                mz_mysql_util::ensure_replication_commit_order(&mut conn)
                    .await
                    .err(),
            ] {
                match error {
                    Some(mz_mysql_util::MySqlError::InvalidSystemSetting {
                        setting,
                        expected,
                        actual,
                    }) => {
                        replication_errors.push((setting, expected, actual));
                    }
                    Some(err) => Err(err)?,
                    None => (),
                }
            }
            if !replication_errors.is_empty() {
                Err(MySqlSourcePurificationError::ReplicationSettingsError(
                    replication_errors,
                ))?;
            }

            // Determine which table schemas to request from mysql. Note that in mysql
            // a 'schema' is the same as a 'database', and a fully qualified table
            // name is 'schema_name.table_name' (there is no db_name)
            let table_schema_request = match referenced_subsources
                .as_mut()
                .ok_or(MySqlSourcePurificationError::RequiresReferencedSubsources)?
            {
                ReferencedSubsources::All => mz_mysql_util::SchemaRequest::All,
                ReferencedSubsources::SubsetSchemas(schemas) => {
                    mz_mysql_util::SchemaRequest::Schemas(
                        schemas.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                    )
                }
                ReferencedSubsources::SubsetTables(tables) => mz_mysql_util::SchemaRequest::Tables(
                    tables
                        .iter()
                        .map(|t| {
                            let idents = &t.reference.0;
                            // We only support fully qualified table names for now
                            if idents.len() != 2 {
                                Err(MySqlSourcePurificationError::InvalidTableReference(
                                    t.reference.to_ast_string(),
                                ))?;
                            }
                            Ok((idents[0].as_str(), idents[1].as_str()))
                        })
                        .collect::<Result<Vec<_>, MySqlSourcePurificationError>>()?,
                ),
            };

            let text_cols_map =
                mysql::map_column_refs(&text_columns, MySqlConfigOptionName::TextColumns)?;
            let ignore_cols_map =
                mysql::map_column_refs(&ignore_columns, MySqlConfigOptionName::IgnoreColumns)?;

            // Retrieve schemas for all requested tables
            // NOTE: mysql will only expose the schemas of tables we have at least one privilege on
            // and we can't tell if a table exists without a privilege, so in some cases we may
            // return an EmptyDatabase error in the case of privilege issues.
            let tables = mz_mysql_util::schema_info(
                &mut *conn,
                &table_schema_request,
                &text_cols_map,
                &ignore_cols_map,
            )
            .await
            .map_err(|err| match err {
                mz_mysql_util::MySqlError::UnsupportedDataTypes { columns } => {
                    PlanError::from(MySqlSourcePurificationError::UnrecognizedTypes {
                        cols: columns
                            .into_iter()
                            .map(|c| (c.qualified_table_name, c.column_name, c.column_type))
                            .collect(),
                    })
                }
                mz_mysql_util::MySqlError::DuplicatedColumnNames {
                    qualified_table_name,
                    columns,
                } => PlanError::from(MySqlSourcePurificationError::DuplicatedColumnNames(
                    qualified_table_name,
                    columns,
                )),
                _ => err.into(),
            })?;

            if tables.is_empty() {
                Err(MySqlSourcePurificationError::EmptyDatabase)?;
            }

            let mysql_catalog = mysql::derive_catalog_from_tables(&tables)?;

            // Normalize column options and remove unused column references.
            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == MySqlConfigOptionName::TextColumns)
            {
                text_cols_option.value = Some(WithOptionValue::Sequence(
                    mysql::normalize_column_refs(text_columns, &mysql_catalog)?,
                ));
            }
            if let Some(ignore_cols_option) = options
                .iter_mut()
                .find(|option| option.name == MySqlConfigOptionName::IgnoreColumns)
            {
                ignore_cols_option.value = Some(WithOptionValue::Sequence(
                    mysql::normalize_column_refs(ignore_columns, &mysql_catalog)?,
                ));
            }

            let mut validated_requested_subsources = vec![];
            match referenced_subsources
                .as_mut()
                .ok_or(MySqlSourcePurificationError::RequiresReferencedSubsources)?
            {
                ReferencedSubsources::All => {
                    for table in &tables {
                        let upstream_name = mysql::mysql_upstream_name(table)?;
                        let subsource_name = subsource_name_gen(source_name, &table.name)?;
                        validated_requested_subsources.push(RequestedSubsource {
                            upstream_name,
                            subsource_name,
                            table,
                        });
                    }
                }
                ReferencedSubsources::SubsetSchemas(schemas) => {
                    let available_schemas: BTreeSet<_> =
                        tables.iter().map(|t| t.schema_name.as_str()).collect();
                    let requested_schemas: BTreeSet<_> =
                        schemas.iter().map(|s| s.as_str()).collect();
                    let missing_schemas: Vec<_> = requested_schemas
                        .difference(&available_schemas)
                        .map(|s| s.to_string())
                        .collect();
                    if !missing_schemas.is_empty() {
                        Err(MySqlSourcePurificationError::NoTablesFoundForSchemas(
                            missing_schemas,
                        ))?;
                    }

                    for table in &tables {
                        if !requested_schemas.contains(table.schema_name.as_str()) {
                            continue;
                        }

                        let upstream_name = mysql::mysql_upstream_name(table)?;
                        let subsource_name = subsource_name_gen(source_name, &table.name)?;
                        validated_requested_subsources.push(RequestedSubsource {
                            upstream_name,
                            subsource_name,
                            table,
                        });
                    }
                }
                ReferencedSubsources::SubsetTables(subsources) => {
                    // The user manually selected a subset of upstream tables so we need to
                    // validate that the names actually exist and are not ambiguous
                    validated_requested_subsources.extend(subsource_gen(
                        subsources,
                        &mysql_catalog,
                        source_name,
                    )?);
                }
            }

            if validated_requested_subsources.is_empty() {
                sql_bail!(
                    "[internal error]: MySQL source must ingest at least one table, but {} matched none",
                    referenced_subsources.as_ref().unwrap().to_ast_string()
                );
            }

            validate_subsource_names(&validated_requested_subsources)?;

            mysql::validate_requested_subsources_privileges(
                &validated_requested_subsources,
                &mut conn,
            )
            .await?;

            let new_subsources =
                mysql::generate_targeted_subsources(&scx, validated_requested_subsources)?;

            // Now that we know which subsources to create alongside this
            // statement, remove the references so it is not canonicalized as
            // part of the `CREATE SOURCE` statement in the catalog.
            *referenced_subsources = None;
            subsources.extend(new_subsources);

            // Retrieve the current @gtid_executed value of the server to mark as the effective
            // initial snapshot point such that we can ensure consistency if the initial source
            // snapshot is broken up over multiple points in time.
            let initial_gtid_set =
                mz_mysql_util::query_sys_var(&mut conn, "global.gtid_executed").await?;

            // Remove any old detail references
            options
                .retain(|MySqlConfigOption { name, .. }| name != &MySqlConfigOptionName::Details);
            let details = MySqlSourceDetails {
                tables,
                initial_gtid_set,
            };
            options.push(MySqlConfigOption {
                name: MySqlConfigOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            })
        }
        CreateSourceConnection::LoadGenerator { generator, options } => {
            let scx = StatementContext::new(None, &catalog);

            let (_load_generator, available_subsources) =
                load_generator_ast_to_generator(&scx, generator, options, include_metadata)?;

            let mut validated_requested_subsources = vec![];
            match referenced_subsources {
                Some(ReferencedSubsources::All) => {
                    let available_subsources = match &available_subsources {
                        Some(available_subsources) => available_subsources,
                        None => Err(LoadGeneratorSourcePurificationError::ForAllTables)?,
                    };
                    for (name, (_, desc)) in available_subsources {
                        let upstream_name = UnresolvedItemName::from(name.clone());
                        let subsource_name = subsource_name_gen(source_name, &name.item)?;
                        validated_requested_subsources.push((upstream_name, subsource_name, desc));
                    }
                }
                Some(ReferencedSubsources::SubsetSchemas(..)) => {
                    Err(LoadGeneratorSourcePurificationError::ForSchemas)?
                }
                Some(ReferencedSubsources::SubsetTables(_)) => {
                    Err(LoadGeneratorSourcePurificationError::ForTables)?
                }
                None => {
                    if available_subsources.is_some() {
                        Err(LoadGeneratorSourcePurificationError::MultiOutputRequiresForAllTables)?
                    }
                }
            };

            // Now that we have an explicit list of validated requested subsources we can create them
            for (upstream_name, subsource_name, desc) in validated_requested_subsources.into_iter()
            {
                let (columns, table_constraints) = scx.relation_desc_into_table_defs(desc)?;

                // Create the subsource statement
                let subsource = CreateSubsourceStatement {
                    name: subsource_name,
                    columns,
                    of_source: None,
                    // unlike sources that come from an external upstream, we
                    // have more leniency to introduce different constraints
                    // every time the load generator is run; i.e. we are not as
                    // worried about introducing junk data.
                    constraints: table_constraints,
                    if_not_exists: false,
                    with_options: vec![CreateSubsourceOption {
                        name: CreateSubsourceOptionName::ExternalReference,
                        value: Some(WithOptionValue::UnresolvedItemName(upstream_name)),
                    }],
                };
                subsources.push(subsource);
            }

            // Now that we know which subsources to create alongside this
            // statement, remove the references so it is not canonicalized as
            // part of the `CREATE SOURCE` statement in the catalog.
            *referenced_subsources = None;
        }
    }

    // Generate progress subsource

    // Create the targeted AST node for the original CREATE SOURCE statement
    let scx = StatementContext::new(None, &catalog);

    // Take name from input or generate name
    let name = match progress_subsource {
        Some(name) => match name {
            DeferredItemName::Deferred(name) => name.clone(),
            DeferredItemName::Named(_) => unreachable!("already checked for this value"),
        },
        None => {
            let (item, prefix) = source_name.0.split_last().unwrap();
            let item_name = Ident::try_generate_name(item.to_string(), "_progress", |candidate| {
                let mut suggested_name = prefix.to_vec();
                suggested_name.push(candidate.clone());

                let partial = normalize::unresolved_item_name(UnresolvedItemName(suggested_name))?;
                let qualified = scx.allocate_qualified_name(partial)?;
                let item_exists = scx.catalog.get_item_by_name(&qualified).is_some();
                let type_exists = scx.catalog.get_type_by_name(&qualified).is_some();
                Ok::<_, PlanError>(!item_exists && !type_exists)
            })?;

            let mut full_name = prefix.to_vec();
            full_name.push(item_name);
            let full_name = normalize::unresolved_item_name(UnresolvedItemName(full_name))?;
            let qualified_name = scx.allocate_qualified_name(full_name)?;
            let full_name = scx.catalog.resolve_full_name(&qualified_name);

            UnresolvedItemName::from(full_name.clone())
        }
    };

    let (columns, constraints) = scx.relation_desc_into_table_defs(progress_desc)?;

    // Create the subsource statement
    let subsource = CreateSubsourceStatement {
        name,
        columns,
        of_source: None,
        constraints,
        if_not_exists: false,
        with_options: vec![CreateSubsourceOption {
            name: CreateSubsourceOptionName::Progress,
            value: Some(WithOptionValue::Value(Value::Boolean(true))),
        }],
    };
    subsources.push(subsource);

    purify_source_format(
        &catalog,
        format,
        connection,
        envelope,
        storage_configuration,
    )
    .await?;

    Ok((subsources, Statement::CreateSource(stmt)))
}

/// Equivalent to `purify_create_source` but for `AlterSourceStatement`.
///
/// On success, returns the `GlobalId` and `CreateSubsourceStatement`s for any
/// subsources created by this statement, in addition to the
/// `AlterSourceStatement` with any modifications that are only accessible while
/// we are permitted to use async code.
async fn purify_alter_source(
    catalog: impl SessionCatalog,
    mut stmt: AlterSourceStatement<Aug>,
    storage_configuration: &StorageConfiguration,
) -> Result<(Vec<CreateSubsourceStatement<Aug>>, Statement<Aug>), PlanError> {
    let scx = StatementContext::new(None, &catalog);
    let AlterSourceStatement {
        source_name,
        action,
        if_exists,
    } = &mut stmt;

    // Get connection
    let (source_name, mut pg_source_connection, subsource_native_idxs) = {
        // Get name.
        let item = match scx.resolve_item(RawItemName::Name(source_name.clone())) {
            Ok(item) => item,
            Err(_) if *if_exists => {
                return Ok((vec![], Statement::AlterSource(stmt)));
            }
            Err(e) => return Err(e),
        };

        // Ensure it's an ingestion-based and alterable source.
        let desc = match item.source_desc()? {
            Some(desc) => desc.clone().into_inline_connection(scx.catalog),
            None => {
                sql_bail!("cannot ALTER this type of source")
            }
        };

        let name = item.name();
        let full_name = scx.catalog.resolve_full_name(name);
        let source_name = ResolvedItemName::Item {
            id: item.id(),
            qualifiers: item.name().qualifiers.clone(),
            full_name,
            print_id: true,
        };

        // Get all of the output indexes being used.
        let subsource_native_idxs: BTreeSet<_> = item
            .used_by()
            .iter()
            .filter_map(|id| {
                scx.get_item(id)
                    .subsource_details()
                    .map(|(_id, output_idx)| output_idx - 1)
            })
            .collect();

        match desc.connection {
            GenericSourceConnection::Postgres(pg_connection) => {
                (source_name, pg_connection, subsource_native_idxs)
            }
            _ => sql_bail!(
                "{} is a {} source, which does not support ALTER SOURCE.",
                scx.catalog.minimal_qualification(item.name()),
                desc.connection.name()
            ),
        }
    };

    // If we don't need to handle added subsources, early return.
    let (targeted_subsources, details, options) = match action {
        AlterSourceAction::AddSubsources {
            subsources,
            details,
            options,
        } => (subsources, details, options),
        _ => return Ok((vec![], Statement::AlterSource(stmt))),
    };

    assert!(
        details.is_none(),
        "details cannot be set before purification"
    );

    let crate::plan::statement::ddl::AlterSourceAddSubsourceOptionExtracted {
        mut text_columns,
        ..
    } = options.clone().try_into()?;

    for CreateSourceSubsource {
        subsource,
        reference: _,
    } in targeted_subsources.iter()
    {
        named_subsource_err(subsource)?;
    }

    // Get PostgresConnection for generating subsources.
    let pg_connection = &pg_source_connection.connection;

    let config = pg_connection
        .config(
            &storage_configuration.connection_context.secrets_reader,
            storage_configuration,
            InTask::No,
        )
        .await?;

    let available_replication_slots = mz_postgres_util::available_replication_slots(
        &storage_configuration.connection_context.ssh_tunnel_manager,
        &config,
    )
    .await?;

    // We need 1 additional replication slot for the snapshots
    if available_replication_slots < 1 {
        Err(PgSourcePurificationError::InsufficientReplicationSlotsAvailable { count: 1 })?;
    }

    let new_publication_tables = mz_postgres_util::publication_info(
        &storage_configuration.connection_context.ssh_tunnel_manager,
        &config,
        &pg_source_connection.publication,
    )
    .await?;

    if new_publication_tables.is_empty() {
        Err(PgSourcePurificationError::EmptyPublication(
            pg_source_connection.publication.to_string(),
        ))?;
    }

    let mut current_publication_details_name_by_idx = BTreeMap::new();
    for (native_idx, table_desc) in pg_source_connection
        .publication_details
        .tables
        .iter()
        .enumerate()
    {
        current_publication_details_name_by_idx.insert(
            UnresolvedItemName(vec![
                Ident::new(pg_connection.database.clone())?,
                Ident::new(table_desc.namespace.clone())?,
                Ident::new(table_desc.name.clone())?,
            ]),
            native_idx,
        );
    }

    // Fixup the publication info
    for table in new_publication_tables.into_iter() {
        let name = UnresolvedItemName(vec![
            Ident::new(pg_connection.database.clone())?,
            Ident::new(table.namespace.clone())?,
            Ident::new(table.name.clone())?,
        ]);

        // Update the existing publication tables to contain updated or new
        // tables. We need to do this in a way that maintains the current output
        // indices because it is easier to do this than update all of the
        // existing output indexes transactionally.
        match current_publication_details_name_by_idx.get(&name) {
            // Do not change the definition of existing subsources.
            Some(idx) if subsource_native_idxs.contains(idx) => {}
            // All unused subsources, update their definition.
            Some(idx) => {
                pg_source_connection.publication_details.tables[*idx] = table.clone();
            }
            // If this table did not yet exist, move it into the publication.
            None => pg_source_connection
                .publication_details
                .tables
                .push(table.clone()),
        }
    }

    let publication_tables = pg_source_connection.publication_details.tables;

    let publication_catalog = postgres::derive_catalog_from_publication_tables(
        &pg_connection.database,
        &publication_tables,
    )?;

    let unresolved_source_name = UnresolvedItemName::from(source_name.full_item_name().clone());

    let validated_requested_subsources = subsource_gen(
        targeted_subsources,
        &publication_catalog,
        &unresolved_source_name,
    )?;

    for RequestedSubsource { upstream_name, .. } in validated_requested_subsources.iter() {
        if let Some(native_idx) = current_publication_details_name_by_idx.get(upstream_name) {
            if subsource_native_idxs.contains(native_idx) {
                Err(PlanError::SubsourceAlreadyReferredTo {
                    name: upstream_name.clone(),
                })?;
            }
        }
    }

    validate_subsource_names(&validated_requested_subsources)?;

    postgres::validate_requested_subsources_privileges(
        &config,
        &validated_requested_subsources,
        &storage_configuration.connection_context.ssh_tunnel_manager,
    )
    .await?;

    let text_cols_dict = postgres::generate_text_columns(
        &publication_catalog,
        &mut text_columns,
        &AlterSourceAddSubsourceOptionName::TextColumns.to_ast_string(),
    )?;

    // Normalize options to contain full qualified values.
    if let Some(text_cols_option) = options
        .iter_mut()
        .find(|option| option.name == AlterSourceAddSubsourceOptionName::TextColumns)
    {
        let mut seq: Vec<_> = text_columns
            .into_iter()
            .map(WithOptionValue::UnresolvedItemName)
            .collect();

        seq.sort();
        seq.dedup();

        text_cols_option.value = Some(WithOptionValue::Sequence(seq));
    }

    let mut new_subsources = postgres::generate_targeted_subsources(
        &scx,
        Some(source_name),
        validated_requested_subsources,
        text_cols_dict,
        &publication_tables,
    )?;

    let timeline_id = match pg_source_connection.publication_details.timeline_id {
        None => {
            // If we had not yet been able to fill in the source's timeline ID, fill it in now.
            let replication_client = config
                .connect_replication(&storage_configuration.connection_context.ssh_tunnel_manager)
                .await?;
            let timeline_id = mz_postgres_util::get_timeline_id(&replication_client).await?;
            Some(timeline_id)
        }
        timeline_id => timeline_id,
    };

    let new_details = PostgresSourcePublicationDetails {
        tables: publication_tables,
        slot: pg_source_connection.publication_details.slot.clone(),
        timeline_id,
    };

    add_output_index_to_subsources(&mut new_subsources, |name| {
        new_details.output_idx_for_name(name)
    });

    *details = Some(WithOptionValue::Value(Value::String(hex::encode(
        new_details.into_proto().encode_to_vec(),
    ))));

    Ok((new_subsources, Statement::AlterSource(stmt)))
}

async fn purify_source_format(
    catalog: &dyn SessionCatalog,
    format: &mut Option<CreateSourceFormat<Aug>>,
    connection: &mut CreateSourceConnection<Aug>,
    envelope: &Option<SourceEnvelope>,
    storage_configuration: &StorageConfiguration,
) -> Result<(), PlanError> {
    if matches!(format, Some(CreateSourceFormat::KeyValue { .. }))
        && !matches!(connection, CreateSourceConnection::Kafka { .. })
    {
        sql_bail!("Kafka sources are the only source type that can provide KEY/VALUE formats")
    }

    match format.as_mut() {
        None => {}
        Some(CreateSourceFormat::Bare(format)) => {
            purify_source_format_single(
                catalog,
                format,
                connection,
                envelope,
                storage_configuration,
            )
            .await?;
        }

        Some(CreateSourceFormat::KeyValue { key, value: val }) => {
            purify_source_format_single(catalog, key, connection, envelope, storage_configuration)
                .await?;
            purify_source_format_single(catalog, val, connection, envelope, storage_configuration)
                .await?;
        }
    }
    Ok(())
}

async fn purify_source_format_single(
    catalog: &dyn SessionCatalog,
    format: &mut Format<Aug>,
    connection: &mut CreateSourceConnection<Aug>,
    envelope: &Option<SourceEnvelope>,
    storage_configuration: &StorageConfiguration,
) -> Result<(), PlanError> {
    match format {
        Format::Avro(schema) => match schema {
            AvroSchema::Csr { csr_connection } => {
                purify_csr_connection_avro(
                    catalog,
                    connection,
                    csr_connection,
                    envelope,
                    storage_configuration,
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
                    storage_configuration,
                )
                .await?;
            }
            ProtobufSchema::InlineSchema { .. } => {}
        },
        Format::Bytes
        | Format::Regex(_)
        | Format::Json { .. }
        | Format::Text
        | Format::Csv { .. } => (),
    }
    Ok(())
}

async fn purify_csr_connection_proto(
    catalog: &dyn SessionCatalog,
    connection: &mut CreateSourceConnection<Aug>,
    csr_connection: &mut CsrConnectionProtobuf<Aug>,
    envelope: &Option<SourceEnvelope>,
    storage_configuration: &StorageConfiguration,
) -> Result<(), PlanError> {
    let topic = if let CreateSourceConnection::Kafka { options, .. } = connection {
        let KafkaSourceConfigOptionExtracted { topic, .. } = options
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
                Connection::Csr(connection) => connection.clone().into_inline_connection(catalog),
                _ => sql_bail!("{} is not a schema registry connection", connection),
            };

            let ccsr_client = ccsr_connection
                .connect(storage_configuration, InTask::No)
                .await
                .map_err(|e| CsrPurificationError::ClientError(Arc::new(e)))?;

            let value = compile_proto(&format!("{}-value", topic), &ccsr_client).await?;
            let key = compile_proto(&format!("{}-key", topic), &ccsr_client)
                .await
                .ok();

            if matches!(envelope, Some(SourceEnvelope::Debezium)) && key.is_none() {
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
    envelope: &Option<SourceEnvelope>,
    storage_configuration: &StorageConfiguration,
) -> Result<(), PlanError> {
    let topic = if let CreateSourceConnection::Kafka { options, .. } = connection {
        let KafkaSourceConfigOptionExtracted { topic, .. } = options
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
            Connection::Csr(connection) => connection.clone().into_inline_connection(catalog),
            _ => sql_bail!("{} is not a schema registry connection", connection),
        };
        let ccsr_client = csr_connection
            .connect(storage_configuration, InTask::No)
            .await
            .map_err(|e| CsrPurificationError::ClientError(Arc::new(e)))?;

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
        if matches!(envelope, Some(SourceEnvelope::Debezium)) && key_schema.is_none() {
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
                Err(GetBySubjectError::SubjectNotFound)
                | Err(GetBySubjectError::VersionNotFound(_)) => Ok(None),
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

const MZ_NOW_NAME: &str = "mz_now";
const MZ_NOW_SCHEMA: &str = "mz_catalog";

/// Purifies a CREATE MATERIALIZED VIEW statement. Additionally, it adjusts `resolved_ids` if
/// references to ids appear or disappear during the purification.
///
/// Note that in contrast with [`purify_statement`], this doesn't need to be async, because
/// this function is not making any network calls.
pub fn purify_create_materialized_view_options(
    catalog: impl SessionCatalog,
    mz_now: Option<Timestamp>,
    cmvs: &mut CreateMaterializedViewStatement<Aug>,
    resolved_ids: &mut ResolvedIds,
) {
    // 0. Preparations:
    // Prepare an expression that calls `mz_now()`, which we can insert in various later steps.
    let (mz_now_id, mz_now_expr) = {
        let item = catalog
            .resolve_function(&PartialItemName {
                database: None,
                schema: Some(MZ_NOW_SCHEMA.to_string()),
                item: MZ_NOW_NAME.to_string(),
            })
            .expect("we should be able to resolve mz_now");
        (
            item.id(),
            Expr::Function(Function {
                name: ResolvedItemName::Item {
                    id: item.id(),
                    qualifiers: item.name().qualifiers.clone(),
                    full_name: catalog.resolve_full_name(item.name()),
                    print_id: false,
                },
                args: FunctionArgs::Args {
                    args: Vec::new(),
                    order_by: Vec::new(),
                },
                filter: None,
                over: None,
                distinct: false,
            }),
        )
    };
    // Prepare the `mz_timestamp` type.
    let (mz_timestamp_id, mz_timestamp_type) = {
        let item = catalog.get_system_type("mz_timestamp");
        let full_name = catalog.resolve_full_name(item.name());
        (
            item.id(),
            ResolvedDataType::Named {
                id: item.id(),
                qualifiers: item.name().qualifiers.clone(),
                full_name,
                modifiers: vec![],
                print_id: true,
            },
        )
    };

    let mut introduced_mz_timestamp = false;

    for option in cmvs.with_options.iter_mut() {
        // 1. Purify `REFRESH AT CREATION` to `REFRESH AT mz_now()`.
        if matches!(
            option.value,
            Some(WithOptionValue::Refresh(RefreshOptionValue::AtCreation))
        ) {
            option.value = Some(WithOptionValue::Refresh(RefreshOptionValue::At(
                RefreshAtOptionValue {
                    time: mz_now_expr.clone(),
                },
            )));
        }

        // 2. If `REFRESH EVERY` doesn't have an `ALIGNED TO`, then add `ALIGNED TO mz_now()`.
        if let Some(WithOptionValue::Refresh(RefreshOptionValue::Every(
            RefreshEveryOptionValue { aligned_to, .. },
        ))) = &mut option.value
        {
            if aligned_to.is_none() {
                *aligned_to = Some(mz_now_expr.clone());
            }
        }

        // 3. Substitute `mz_now()` with the timestamp chosen for the CREATE MATERIALIZED VIEW
        // statement. (This has to happen after the above steps, which might introduce `mz_now()`.)
        match &mut option.value {
            Some(WithOptionValue::Refresh(RefreshOptionValue::At(RefreshAtOptionValue {
                time,
            }))) => {
                let mut visitor = MzNowPurifierVisitor::new(mz_now, mz_timestamp_type.clone());
                visitor.visit_expr_mut(time);
                introduced_mz_timestamp |= visitor.introduced_mz_timestamp;
            }
            Some(WithOptionValue::Refresh(RefreshOptionValue::Every(
                RefreshEveryOptionValue {
                    interval: _,
                    aligned_to: Some(aligned_to),
                },
            ))) => {
                let mut visitor = MzNowPurifierVisitor::new(mz_now, mz_timestamp_type.clone());
                visitor.visit_expr_mut(aligned_to);
                introduced_mz_timestamp |= visitor.introduced_mz_timestamp;
            }
            _ => {}
        }
    }

    // 4. If the user didn't give any REFRESH option, then default to ON COMMIT.
    if !cmvs.with_options.iter().any(|o| {
        matches!(
            o,
            MaterializedViewOption {
                value: Some(WithOptionValue::Refresh(..)),
                ..
            }
        )
    }) {
        cmvs.with_options.push(MaterializedViewOption {
            name: MaterializedViewOptionName::Refresh,
            value: Some(WithOptionValue::Refresh(RefreshOptionValue::OnCommit)),
        })
    }

    // 5. Attend to `resolved_ids`: The purification might have
    // - added references to `mz_timestamp`;
    // - removed references to `mz_now`.
    if introduced_mz_timestamp {
        resolved_ids.0.insert(mz_timestamp_id);
    }
    // Even though we always remove `mz_now()` from the `with_options`, there might be `mz_now()`
    // remaining in the main query expression of the MV, so let's visit the entire statement to look
    // for `mz_now()` everywhere.
    let mut visitor = ExprContainsTemporalVisitor::new();
    visitor.visit_create_materialized_view_statement(cmvs);
    if !visitor.contains_temporal {
        resolved_ids.0.remove(&mz_now_id);
    }
}

/// Returns true if the [MaterializedViewOption] either already involves `mz_now()` or will involve
/// after purification.
pub fn materialized_view_option_contains_temporal(mvo: &MaterializedViewOption<Aug>) -> bool {
    match &mvo.value {
        Some(WithOptionValue::Refresh(RefreshOptionValue::At(RefreshAtOptionValue { time }))) => {
            let mut visitor = ExprContainsTemporalVisitor::new();
            visitor.visit_expr(time);
            visitor.contains_temporal
        }
        Some(WithOptionValue::Refresh(RefreshOptionValue::Every(RefreshEveryOptionValue {
            interval: _,
            aligned_to: Some(aligned_to),
        }))) => {
            let mut visitor = ExprContainsTemporalVisitor::new();
            visitor.visit_expr(aligned_to);
            visitor.contains_temporal
        }
        Some(WithOptionValue::Refresh(RefreshOptionValue::Every(RefreshEveryOptionValue {
            interval: _,
            aligned_to: None,
        }))) => {
            // For a `REFRESH EVERY` without an `ALIGNED TO`, purification will default the
            // `ALIGNED TO` to `mz_now()`.
            true
        }
        Some(WithOptionValue::Refresh(RefreshOptionValue::AtCreation)) => {
            // `REFRESH AT CREATION` will be purified to `REFRESH AT mz_now()`.
            true
        }
        _ => false,
    }
}

/// Determines whether the AST involves `mz_now()`.
struct ExprContainsTemporalVisitor {
    pub contains_temporal: bool,
}

impl ExprContainsTemporalVisitor {
    pub fn new() -> ExprContainsTemporalVisitor {
        ExprContainsTemporalVisitor {
            contains_temporal: false,
        }
    }
}

impl Visit<'_, Aug> for ExprContainsTemporalVisitor {
    fn visit_function(&mut self, func: &Function<Aug>) {
        self.contains_temporal |= func.name.full_item_name().item == MZ_NOW_NAME;
        visit_function(self, func);
    }
}

struct MzNowPurifierVisitor {
    pub mz_now: Option<Timestamp>,
    pub mz_timestamp_type: ResolvedDataType,
    pub introduced_mz_timestamp: bool,
}

impl MzNowPurifierVisitor {
    pub fn new(
        mz_now: Option<Timestamp>,
        mz_timestamp_type: ResolvedDataType,
    ) -> MzNowPurifierVisitor {
        MzNowPurifierVisitor {
            mz_now,
            mz_timestamp_type,
            introduced_mz_timestamp: false,
        }
    }
}

impl VisitMut<'_, Aug> for MzNowPurifierVisitor {
    fn visit_expr_mut(&mut self, expr: &'_ mut Expr<Aug>) {
        match expr {
            Expr::Function(Function {
                name:
                    ResolvedItemName::Item {
                        full_name: FullItemName { item, .. },
                        ..
                    },
                ..
            }) if item == &MZ_NOW_NAME.to_string() => {
                let mz_now = self.mz_now.expect(
                    "we should have chosen a timestamp if the expression contains mz_now()",
                );
                // We substitute `mz_now()` with number + a cast to `mz_timestamp`. The cast is to
                // not alter the type of the expression.
                *expr = Expr::Cast {
                    expr: Box::new(Expr::Value(Value::Number(mz_now.to_string()))),
                    data_type: self.mz_timestamp_type.clone(),
                };
                self.introduced_mz_timestamp = true;
            }
            _ => visit_expr_mut(self, expr),
        }
    }
}
