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
use std::fmt;
use std::iter;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use itertools::Itertools;
use mz_ccsr::{Client, GetByIdError, GetBySubjectError, Schema as CcsrSchema};
use mz_controller_types::ClusterId;
use mz_kafka_util::client::MzClientContext;
use mz_mysql_util::MySqlTableDesc;
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::iter::IteratorExt;
use mz_ore::str::StrExt;
use mz_ore::{assert_none, soft_panic_or_log};
use mz_postgres_util::desc::PostgresTableDesc;
use mz_proto::RustType;
use mz_repr::{CatalogItemId, RelationDesc, RelationVersionSelector, Timestamp, strconv};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit::{Visit, visit_function};
use mz_sql_parser::ast::visit_mut::{VisitMut, visit_expr_mut};
use mz_sql_parser::ast::{
    AlterSourceAction, AlterSourceAddSubsourceOptionName, AlterSourceStatement, AvroDocOn,
    ColumnName, CreateMaterializedViewStatement, CreateSinkConnection, CreateSinkOptionName,
    CreateSinkStatement, CreateSourceOptionName, CreateSubsourceOption, CreateSubsourceOptionName,
    CreateTableFromSourceStatement, CsrConfigOption, CsrConfigOptionName, CsrConnection,
    CsrSeedAvro, CsrSeedProtobuf, CsrSeedProtobufSchema, DeferredItemName, DocOnIdentifier,
    DocOnSchema, Expr, Function, FunctionArgs, Ident, KafkaSourceConfigOption,
    KafkaSourceConfigOptionName, LoadGenerator, LoadGeneratorOption, LoadGeneratorOptionName,
    MaterializedViewOption, MaterializedViewOptionName, MySqlConfigOption, MySqlConfigOptionName,
    PgConfigOption, PgConfigOptionName, RawItemName, ReaderSchemaSelectionStrategy,
    RefreshAtOptionValue, RefreshEveryOptionValue, RefreshOptionValue, SourceEnvelope,
    SqlServerConfigOption, SqlServerConfigOptionName, Statement, TableFromSourceColumns,
    TableFromSourceOption, TableFromSourceOptionName, UnresolvedItemName,
};
use mz_sql_server_util::desc::SqlServerTableDesc;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::Connection;
use mz_storage_types::connections::inline::IntoInlineConnection;
use mz_storage_types::errors::ContextCreationError;
use mz_storage_types::sources::load_generator::LoadGeneratorOutput;
use mz_storage_types::sources::mysql::MySqlSourceDetails;
use mz_storage_types::sources::postgres::PostgresSourcePublicationDetails;
use mz_storage_types::sources::{
    GenericSourceConnection, SourceDesc, SourceExportStatementDetails, SqlServerSourceExtras,
};
use prost::Message;
use protobuf_native::MessageLite;
use protobuf_native::compiler::{SourceTreeDescriptorDatabase, VirtualSourceTree};
use rdkafka::admin::AdminClient;
use references::{RetrievedSourceReferences, SourceReferenceClient};
use uuid::Uuid;

use crate::ast::{
    AlterSourceAddSubsourceOption, AvroSchema, CreateSourceConnection, CreateSourceStatement,
    CreateSubsourceStatement, CsrConnectionAvro, CsrConnectionProtobuf, ExternalReferenceExport,
    ExternalReferences, Format, FormatSpecifier, ProtobufSchema, Value, WithOptionValue,
};
use crate::catalog::{CatalogItemType, SessionCatalog};
use crate::kafka_util::{KafkaSinkConfigOptionExtracted, KafkaSourceConfigOptionExtracted};
use crate::names::{
    Aug, FullItemName, PartialItemName, ResolvedColumnReference, ResolvedDataType, ResolvedIds,
    ResolvedItemName,
};
use crate::plan::error::PlanError;
use crate::plan::statement::ddl::load_generator_ast_to_generator;
use crate::plan::{SourceReferences, StatementContext};
use crate::pure::error::{IcebergSinkPurificationError, SqlServerSourcePurificationError};
use crate::session::vars::ENABLE_SQL_SERVER_SOURCE;
use crate::{kafka_util, normalize};

use self::error::{
    CsrPurificationError, KafkaSinkPurificationError, KafkaSourcePurificationError,
    LoadGeneratorSourcePurificationError, MySqlSourcePurificationError, PgSourcePurificationError,
};

pub(crate) mod error;
mod references;

pub mod mysql;
pub mod postgres;
pub mod sql_server;

pub(crate) struct RequestedSourceExport<T> {
    external_reference: UnresolvedItemName,
    name: UnresolvedItemName,
    meta: T,
}

impl<T: fmt::Debug> fmt::Debug for RequestedSourceExport<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequestedSourceExport")
            .field("external_reference", &self.external_reference)
            .field("name", &self.name)
            .field("meta", &self.meta)
            .finish()
    }
}

impl<T> RequestedSourceExport<T> {
    fn change_meta<F>(self, new_meta: F) -> RequestedSourceExport<F> {
        RequestedSourceExport {
            external_reference: self.external_reference,
            name: self.name,
            meta: new_meta,
        }
    }
}

/// Generates a subsource name by prepending source schema name if present
///
/// For eg. if source is `a.b`, then `a` will be prepended to the subsource name
/// so that it's generated in the same schema as source
fn source_export_name_gen(
    source_name: &UnresolvedItemName,
    subsource_name: &str,
) -> Result<UnresolvedItemName, PlanError> {
    let mut partial = normalize::unresolved_item_name(source_name.clone())?;
    partial.item = subsource_name.to_string();
    Ok(UnresolvedItemName::from(partial))
}

// TODO(database-issues#8620): Remove once subsources are removed
/// Validates the requested subsources do not have name conflicts with each other
/// and that the same upstream table is not referenced multiple times.
fn validate_source_export_names<T>(
    requested_source_exports: &[RequestedSourceExport<T>],
) -> Result<(), PlanError> {
    // This condition would get caught during the catalog transaction, but produces a
    // vague, non-contextual error. Instead, error here so we can suggest to the user
    // how to fix the problem.
    if let Some(name) = requested_source_exports
        .iter()
        .map(|subsource| &subsource.name)
        .duplicates()
        .next()
        .cloned()
    {
        let mut upstream_references: Vec<_> = requested_source_exports
            .into_iter()
            .filter_map(|subsource| {
                if &subsource.name == &name {
                    Some(subsource.external_reference.clone())
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

    // We disallow subsource statements from referencing the same upstream table, but we allow
    // `CREATE TABLE .. FROM SOURCE` statements to do so. Since `CREATE TABLE .. FROM SOURCE`
    // purification will only provide 1 `requested_source_export`, we can leave this here without
    // needing to differentiate between the two types of statements.
    // TODO(roshan): Remove this when auto-generated subsources are deprecated.
    if let Some(name) = requested_source_exports
        .iter()
        .map(|export| &export.external_reference)
        .duplicates()
        .next()
        .cloned()
    {
        let mut target_names: Vec<_> = requested_source_exports
            .into_iter()
            .filter_map(|export| {
                if &export.external_reference == &name {
                    Some(export.name.clone())
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PurifiedStatement {
    PurifiedCreateSource {
        // The progress subsource, if we are offloading progress info to a separate relation
        create_progress_subsource_stmt: Option<CreateSubsourceStatement<Aug>>,
        create_source_stmt: CreateSourceStatement<Aug>,
        // Map of subsource names to external details
        subsources: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
        /// All the available upstream references that can be added as tables
        /// to this primary source.
        available_source_references: SourceReferences,
    },
    PurifiedAlterSource {
        alter_source_stmt: AlterSourceStatement<Aug>,
    },
    PurifiedAlterSourceAddSubsources {
        // This just saves us an annoying catalog lookup
        source_name: ResolvedItemName,
        /// Options that we will need the values of to update the source's
        /// definition.
        options: Vec<AlterSourceAddSubsourceOption<Aug>>,
        // Map of subsource names to external details
        subsources: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
    },
    PurifiedAlterSourceRefreshReferences {
        source_name: ResolvedItemName,
        /// The updated available upstream references for the primary source.
        available_source_references: SourceReferences,
    },
    PurifiedCreateSink(CreateSinkStatement<Aug>),
    PurifiedCreateTableFromSource {
        stmt: CreateTableFromSourceStatement<Aug>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurifiedSourceExport {
    pub external_reference: UnresolvedItemName,
    pub details: PurifiedExportDetails,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PurifiedExportDetails {
    MySql {
        table: MySqlTableDesc,
        text_columns: Option<Vec<Ident>>,
        exclude_columns: Option<Vec<Ident>>,
        initial_gtid_set: String,
    },
    Postgres {
        table: PostgresTableDesc,
        text_columns: Option<Vec<Ident>>,
        exclude_columns: Option<Vec<Ident>>,
    },
    SqlServer {
        table: SqlServerTableDesc,
        text_columns: Option<Vec<Ident>>,
        excl_columns: Option<Vec<Ident>>,
        capture_instance: Arc<str>,
        initial_lsn: mz_sql_server_util::cdc::Lsn,
    },
    Kafka {},
    LoadGenerator {
        table: Option<RelationDesc>,
        output: LoadGeneratorOutput,
    },
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
) -> (Result<PurifiedStatement, PlanError>, Option<ClusterId>) {
    match stmt {
        Statement::CreateSource(stmt) => {
            let cluster_id = stmt.in_cluster.as_ref().map(|cluster| cluster.id.clone());
            (
                purify_create_source(catalog, now, stmt, storage_configuration).await,
                cluster_id,
            )
        }
        Statement::AlterSource(stmt) => (
            purify_alter_source(catalog, stmt, storage_configuration).await,
            None,
        ),
        Statement::CreateSink(stmt) => {
            let cluster_id = stmt.in_cluster.as_ref().map(|cluster| cluster.id.clone());
            (
                purify_create_sink(catalog, stmt, storage_configuration).await,
                cluster_id,
            )
        }
        Statement::CreateTableFromSource(stmt) => (
            purify_create_table_from_source(catalog, stmt, storage_configuration).await,
            None,
        ),
        o => unreachable!("{:?} does not need to be purified", o),
    }
}

/// Injects `DOC ON` comments into all Avro formats that are using a schema
/// registry by finding all SQL `COMMENT`s that are attached to the sink's
/// underlying materialized view (as well as any types referenced by that
/// underlying materialized view).
pub(crate) fn purify_create_sink_avro_doc_on_options(
    catalog: &dyn SessionCatalog,
    from_id: CatalogItemId,
    format: &mut Option<FormatSpecifier<Aug>>,
) -> Result<(), PlanError> {
    // Collect all objects referenced by the sink.
    let from = catalog.get_item(&from_id);
    let object_ids = from
        .references()
        .items()
        .copied()
        .chain_one(from.id())
        .collect::<Vec<_>>();

    // Collect all Avro formats that use a schema registry, as well as a set of
    // all identifiers named in user-provided `DOC ON` options.
    let mut avro_format_options = vec![];
    for_each_format(format, |doc_on_schema, fmt| match fmt {
        Format::Avro(AvroSchema::InlineSchema { .. })
        | Format::Bytes
        | Format::Csv { .. }
        | Format::Json { .. }
        | Format::Protobuf(..)
        | Format::Regex(..)
        | Format::Text => (),
        Format::Avro(AvroSchema::Csr {
            csr_connection: CsrConnectionAvro { connection, .. },
        }) => {
            avro_format_options.push((doc_on_schema, &mut connection.options));
        }
    });

    // For each Avro format in the sink, inject the appropriate `DOC ON` options
    // for each item referenced by the sink.
    for (for_schema, options) in avro_format_options {
        let user_provided_comments = options
            .iter()
            .filter_map(|CsrConfigOption { name, .. }| match name {
                CsrConfigOptionName::AvroDocOn(doc_on) => Some(doc_on.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();

        // Adding existing comments if not already provided by user
        for object_id in &object_ids {
            // Always add comments to the latest version of the item.
            let item = catalog
                .get_item(object_id)
                .at_version(RelationVersionSelector::Latest);
            let full_resolved_name = ResolvedItemName::Item {
                id: *object_id,
                qualifiers: item.name().qualifiers.clone(),
                full_name: catalog.resolve_full_name(item.name()),
                print_id: !matches!(
                    item.item_type(),
                    CatalogItemType::Func | CatalogItemType::Type
                ),
                version: RelationVersionSelector::Latest,
            };

            if let Some(comments_map) = catalog.get_item_comments(object_id) {
                // Attach comment for the item itself, if the user has not
                // already provided an overriding `DOC ON` option for the item.
                let doc_on_item_key = AvroDocOn {
                    identifier: DocOnIdentifier::Type(full_resolved_name.clone()),
                    for_schema,
                };
                if !user_provided_comments.contains(&doc_on_item_key) {
                    if let Some(root_comment) = comments_map.get(&None) {
                        options.push(CsrConfigOption {
                            name: CsrConfigOptionName::AvroDocOn(doc_on_item_key),
                            value: Some(mz_sql_parser::ast::WithOptionValue::Value(Value::String(
                                root_comment.clone(),
                            ))),
                        });
                    }
                }

                // Attach comment for each column in the item, if the user has
                // not already provided an overriding `DOC ON` option for the
                // column.
                let column_descs = match item.type_details() {
                    Some(details) => details.typ.desc(catalog).unwrap_or_default(),
                    None => item.relation_desc().map(|d| d.into_owned()),
                };

                if let Some(desc) = column_descs {
                    for (pos, column_name) in desc.iter_names().enumerate() {
                        if let Some(comment_str) = comments_map.get(&Some(pos + 1)) {
                            let doc_on_column_key = AvroDocOn {
                                identifier: DocOnIdentifier::Column(ColumnName {
                                    relation: full_resolved_name.clone(),
                                    column: ResolvedColumnReference::Column {
                                        name: column_name.to_owned(),
                                        index: pos,
                                    },
                                }),
                                for_schema,
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

    Ok(())
}

/// Checks that the sink described in the statement can connect to its external
/// resources.
async fn purify_create_sink(
    catalog: impl SessionCatalog,
    mut create_sink_stmt: CreateSinkStatement<Aug>,
    storage_configuration: &StorageConfiguration,
) -> Result<PurifiedStatement, PlanError> {
    // General purification
    let CreateSinkStatement {
        connection,
        format,
        with_options,
        name: _,
        in_cluster: _,
        if_not_exists: _,
        from,
        envelope: _,
    } = &mut create_sink_stmt;

    // The list of options that the user is allowed to specify.
    const USER_ALLOWED_WITH_OPTIONS: &[CreateSinkOptionName] = &[
        CreateSinkOptionName::Snapshot,
        CreateSinkOptionName::CommitInterval,
    ];

    if let Some(op) = with_options
        .iter()
        .find(|op| !USER_ALLOWED_WITH_OPTIONS.contains(&op.name))
    {
        sql_bail!(
            "CREATE SINK...WITH ({}..) is not allowed",
            op.name.to_ast_string_simple(),
        )
    }

    match &connection {
        CreateSinkConnection::Kafka {
            connection,
            options,
            key: _,
            headers: _,
        } => {
            // We must not leave any state behind in the Kafka broker, so just ensure that
            // we can connect. This means we don't ensure that we can create the topic and
            // introduces TOCTOU errors, but creating an inoperable sink is infinitely
            // preferable to leaking state in users' environments.
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
        CreateSinkConnection::Iceberg {
            connection,
            aws_connection,
            ..
        } => {
            let scx = StatementContext::new(None, &catalog);
            let connection = {
                let item = scx.get_item_by_resolved_name(connection)?;
                // Get Iceberg connection
                match item.connection()? {
                    Connection::IcebergCatalog(connection) => {
                        connection.clone().into_inline_connection(scx.catalog)
                    }
                    _ => sql_bail!(
                        "{} is not an iceberg connection",
                        scx.catalog.resolve_full_name(item.name())
                    ),
                }
            };

            let aws_conn_id = aws_connection.item_id();

            let aws_connection = {
                let item = scx.get_item_by_resolved_name(aws_connection)?;
                // Get AWS connection
                match item.connection()? {
                    Connection::Aws(aws_connection) => aws_connection.clone(),
                    _ => sql_bail!(
                        "{} is not an aws connection",
                        scx.catalog.resolve_full_name(item.name())
                    ),
                }
            };

            let _catalog = connection
                .connect(storage_configuration, InTask::No)
                .await
                .map_err(|e| IcebergSinkPurificationError::CatalogError(Arc::new(e)))?;

            let sdk_config = aws_connection
                .load_sdk_config(
                    &storage_configuration.connection_context,
                    aws_conn_id.clone(),
                    InTask::No,
                )
                .await
                .map_err(|e| IcebergSinkPurificationError::AwsSdkContextError(Arc::new(e)))?;

            let sts_client = aws_sdk_sts::Client::new(&sdk_config);
            let _ = sts_client.get_caller_identity().send().await.map_err(|e| {
                IcebergSinkPurificationError::StsIdentityError(Arc::new(e.into_service_error()))
            })?;
        }
    }

    let mut csr_connection_ids = BTreeSet::new();
    for_each_format(format, |_, fmt| match fmt {
        Format::Avro(AvroSchema::InlineSchema { .. })
        | Format::Bytes
        | Format::Csv { .. }
        | Format::Json { .. }
        | Format::Protobuf(ProtobufSchema::InlineSchema { .. })
        | Format::Regex(..)
        | Format::Text => (),
        Format::Avro(AvroSchema::Csr {
            csr_connection: CsrConnectionAvro { connection, .. },
        })
        | Format::Protobuf(ProtobufSchema::Csr {
            csr_connection: CsrConnectionProtobuf { connection, .. },
        }) => {
            csr_connection_ids.insert(*connection.connection.item_id());
        }
    });

    let scx = StatementContext::new(None, &catalog);
    for csr_connection_id in csr_connection_ids {
        let connection = {
            let item = scx.get_item(&csr_connection_id);
            // Get Kafka connection
            match item.connection()? {
                Connection::Csr(connection) => connection.clone().into_inline_connection(&catalog),
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

    purify_create_sink_avro_doc_on_options(&catalog, *from.item_id(), format)?;

    Ok(PurifiedStatement::PurifiedCreateSink(create_sink_stmt))
}

/// Runs a function on each format within the format specifier.
///
/// The function is provided with a `DocOnSchema` that indicates whether the
/// format is for the key, value, or both.
///
// TODO(benesch): rename `DocOnSchema` to the more general `FormatRestriction`.
fn for_each_format<'a, F>(format: &'a mut Option<FormatSpecifier<Aug>>, mut f: F)
where
    F: FnMut(DocOnSchema, &'a mut Format<Aug>),
{
    match format {
        None => (),
        Some(FormatSpecifier::Bare(fmt)) => f(DocOnSchema::All, fmt),
        Some(FormatSpecifier::KeyValue { key, value }) => {
            f(DocOnSchema::KeyOnly, key);
            f(DocOnSchema::ValueOnly, value);
        }
    }
}

/// Defines whether purification should enforce that at least one valid source
/// reference is provided on the provided statement.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum SourceReferencePolicy {
    /// Don't allow source references to be provided. This is used for
    /// enforcing that `CREATE SOURCE` statements don't create subsources.
    NotAllowed,
    /// Allow empty references, such as when creating a source that
    /// will have tables added afterwards.
    Optional,
    /// Require that at least one reference is resolved to an upstream
    /// object.
    Required,
}

async fn purify_create_source(
    catalog: impl SessionCatalog,
    now: u64,
    mut create_source_stmt: CreateSourceStatement<Aug>,
    storage_configuration: &StorageConfiguration,
) -> Result<PurifiedStatement, PlanError> {
    let CreateSourceStatement {
        name: source_name,
        col_names,
        key_constraint,
        connection: source_connection,
        format,
        envelope,
        include_metadata,
        external_references,
        progress_subsource,
        with_options,
        ..
    } = &mut create_source_stmt;

    let uses_old_syntax = !col_names.is_empty()
        || key_constraint.is_some()
        || format.is_some()
        || envelope.is_some()
        || !include_metadata.is_empty()
        || external_references.is_some()
        || progress_subsource.is_some();

    if let Some(DeferredItemName::Named(_)) = progress_subsource {
        sql_bail!("Cannot manually ID qualify progress subsource")
    }

    let mut requested_subsource_map = BTreeMap::new();

    let progress_desc = match &source_connection {
        CreateSourceConnection::Kafka { .. } => {
            &mz_storage_types::sources::kafka::KAFKA_PROGRESS_DESC
        }
        CreateSourceConnection::Postgres { .. } => {
            &mz_storage_types::sources::postgres::PG_PROGRESS_DESC
        }
        CreateSourceConnection::SqlServer { .. } => {
            &mz_storage_types::sources::sql_server::SQL_SERVER_PROGRESS_DESC
        }
        CreateSourceConnection::MySql { .. } => {
            &mz_storage_types::sources::mysql::MYSQL_PROGRESS_DESC
        }
        CreateSourceConnection::LoadGenerator { .. } => {
            &mz_storage_types::sources::load_generator::LOAD_GEN_PROGRESS_DESC
        }
    };
    let scx = StatementContext::new(None, &catalog);

    // Depending on if the user must or can use the `CREATE TABLE .. FROM SOURCE` statement
    // to add tables after this source is created we might need to enforce that
    // auto-generated subsources are created or not created by this source statement.
    let reference_policy = if scx.catalog.system_vars().enable_create_table_from_source()
        && scx.catalog.system_vars().force_source_table_syntax()
    {
        SourceReferencePolicy::NotAllowed
    } else if scx.catalog.system_vars().enable_create_table_from_source() {
        SourceReferencePolicy::Optional
    } else {
        SourceReferencePolicy::Required
    };

    let mut format_options = SourceFormatOptions::Default;

    let retrieved_source_references: RetrievedSourceReferences;

    match source_connection {
        CreateSourceConnection::Kafka {
            connection,
            options: base_with_options,
            ..
        } => {
            if let Some(external_references) = external_references {
                Err(KafkaSourcePurificationError::ReferencedSubsources(
                    external_references.clone(),
                ))?;
            }

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

            let reference_client = SourceReferenceClient::Kafka { topic: &topic };
            retrieved_source_references = reference_client.get_source_references().await?;

            format_options = SourceFormatOptions::Kafka { topic };
        }
        CreateSourceConnection::Postgres {
            connection,
            options,
        } => {
            let connection_item = scx.get_item_by_resolved_name(connection)?;
            let connection = match connection_item.connection().map_err(PlanError::from)? {
                Connection::Postgres(connection) => {
                    connection.clone().into_inline_connection(&catalog)
                }
                _ => Err(PgSourcePurificationError::NotPgConnection(
                    scx.catalog.resolve_full_name(connection_item.name()),
                ))?,
            };
            let crate::plan::statement::PgConfigOptionExtracted {
                publication,
                text_columns,
                exclude_columns,
                details,
                ..
            } = options.clone().try_into()?;
            let publication =
                publication.ok_or(PgSourcePurificationError::ConnectionMissingPublication)?;

            if details.is_some() {
                Err(PgSourcePurificationError::UserSpecifiedDetails)?;
            }

            let client = connection
                .validate(connection_item.id(), storage_configuration)
                .await?;

            let reference_client = SourceReferenceClient::Postgres {
                client: &client,
                publication: &publication,
                database: &connection.database,
            };
            retrieved_source_references = reference_client.get_source_references().await?;

            let postgres::PurifiedSourceExports {
                source_exports: subsources,
                normalized_text_columns,
            } = postgres::purify_source_exports(
                &client,
                &retrieved_source_references,
                external_references,
                text_columns,
                exclude_columns,
                source_name,
                &reference_policy,
            )
            .await?;

            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == PgConfigOptionName::TextColumns)
            {
                text_cols_option.value = Some(WithOptionValue::Sequence(normalized_text_columns));
            }

            requested_subsource_map.extend(subsources);

            // Record the active replication timeline_id to allow detection of a future upstream
            // point-in-time-recovery that will put the source into an error state.
            let timeline_id = mz_postgres_util::get_timeline_id(&client).await?;

            // Remove any old detail references
            options.retain(|PgConfigOption { name, .. }| name != &PgConfigOptionName::Details);
            let details = PostgresSourcePublicationDetails {
                slot: format!(
                    "materialize_{}",
                    Uuid::new_v4().to_string().replace('-', "")
                ),
                timeline_id: Some(timeline_id),
                database: connection.database,
            };
            options.push(PgConfigOption {
                name: PgConfigOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            })
        }
        CreateSourceConnection::SqlServer {
            connection,
            options,
        } => {
            scx.require_feature_flag(&ENABLE_SQL_SERVER_SOURCE)?;

            // Connect to the upstream SQL Server instance so we can validate
            // we're compatible with CDC.
            let connection_item = scx.get_item_by_resolved_name(connection)?;
            let connection = match connection_item.connection()? {
                Connection::SqlServer(connection) => {
                    connection.clone().into_inline_connection(&catalog)
                }
                _ => Err(SqlServerSourcePurificationError::NotSqlServerConnection(
                    scx.catalog.resolve_full_name(connection_item.name()),
                ))?,
            };
            let crate::plan::statement::ddl::SqlServerConfigOptionExtracted {
                details,
                text_columns,
                exclude_columns,
                seen: _,
            } = options.clone().try_into()?;

            if details.is_some() {
                Err(SqlServerSourcePurificationError::UserSpecifiedDetails)?;
            }

            let mut client = connection
                .validate(connection_item.id(), storage_configuration)
                .await?;

            let database: Arc<str> = connection.database.into();
            let reference_client = SourceReferenceClient::SqlServer {
                client: &mut client,
                database: Arc::clone(&database),
            };
            retrieved_source_references = reference_client.get_source_references().await?;
            tracing::debug!(?retrieved_source_references, "got source references");

            let timeout = mz_storage_types::sources::sql_server::MAX_LSN_WAIT
                .get(storage_configuration.config_set());

            let purified_source_exports = sql_server::purify_source_exports(
                &*database,
                &mut client,
                &retrieved_source_references,
                external_references,
                &text_columns,
                &exclude_columns,
                source_name,
                timeout,
                &reference_policy,
            )
            .await?;

            let sql_server::PurifiedSourceExports {
                source_exports,
                normalized_text_columns,
                normalized_excl_columns,
            } = purified_source_exports;

            // Update our set of requested source exports.
            requested_subsource_map.extend(source_exports);

            // Record the most recent restore_history_id, or none if the system has never been
            // restored.

            let restore_history_id =
                mz_sql_server_util::inspect::get_latest_restore_history_id(&mut client).await?;
            let details = SqlServerSourceExtras { restore_history_id };

            options.retain(|SqlServerConfigOption { name, .. }| {
                name != &SqlServerConfigOptionName::Details
            });
            options.push(SqlServerConfigOption {
                name: SqlServerConfigOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            });

            // Update our 'TEXT' and 'EXCLUDE' column options with the purified and normalized set.
            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == SqlServerConfigOptionName::TextColumns)
            {
                text_cols_option.value = Some(WithOptionValue::Sequence(normalized_text_columns));
            }
            if let Some(excl_cols_option) = options
                .iter_mut()
                .find(|option| option.name == SqlServerConfigOptionName::ExcludeColumns)
            {
                excl_cols_option.value = Some(WithOptionValue::Sequence(normalized_excl_columns));
            }
        }
        CreateSourceConnection::MySql {
            connection,
            options,
        } => {
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
                exclude_columns,
                seen: _,
            } = options.clone().try_into()?;

            if details.is_some() {
                Err(MySqlSourcePurificationError::UserSpecifiedDetails)?;
            }

            let mut conn = connection
                .validate(connection_item.id(), storage_configuration)
                .await
                .map_err(MySqlSourcePurificationError::InvalidConnection)?;

            // Retrieve the current @gtid_executed value of the server to mark as the effective
            // initial snapshot point such that we can ensure consistency if the initial source
            // snapshot is broken up over multiple points in time.
            let initial_gtid_set =
                mz_mysql_util::query_sys_var(&mut conn, "global.gtid_executed").await?;

            let reference_client = SourceReferenceClient::MySql {
                conn: &mut conn,
                include_system_schemas: mysql::references_system_schemas(external_references),
            };
            retrieved_source_references = reference_client.get_source_references().await?;

            let mysql::PurifiedSourceExports {
                source_exports: subsources,
                normalized_text_columns,
                normalized_exclude_columns,
            } = mysql::purify_source_exports(
                &mut conn,
                &retrieved_source_references,
                external_references,
                text_columns,
                exclude_columns,
                source_name,
                initial_gtid_set.clone(),
                &reference_policy,
            )
            .await?;
            requested_subsource_map.extend(subsources);

            // We don't have any fields in this details struct but keep this around for
            // conformity with postgres and in-case we end up needing it again in the future.
            let details = MySqlSourceDetails {};
            // Update options with the purified details
            options
                .retain(|MySqlConfigOption { name, .. }| name != &MySqlConfigOptionName::Details);
            options.push(MySqlConfigOption {
                name: MySqlConfigOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            });

            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == MySqlConfigOptionName::TextColumns)
            {
                text_cols_option.value = Some(WithOptionValue::Sequence(normalized_text_columns));
            }
            if let Some(exclude_cols_option) = options
                .iter_mut()
                .find(|option| option.name == MySqlConfigOptionName::ExcludeColumns)
            {
                exclude_cols_option.value =
                    Some(WithOptionValue::Sequence(normalized_exclude_columns));
            }
        }
        CreateSourceConnection::LoadGenerator { generator, options } => {
            let load_generator =
                load_generator_ast_to_generator(&scx, generator, options, include_metadata)?;

            let reference_client = SourceReferenceClient::LoadGenerator {
                generator: &load_generator,
            };
            retrieved_source_references = reference_client.get_source_references().await?;
            // Filter to the references that need to be created as 'subsources', which
            // doesn't include the default output for single-output sources.
            // TODO(database-issues#8620): Remove once subsources are removed
            let multi_output_sources =
                retrieved_source_references
                    .all_references()
                    .iter()
                    .any(|r| {
                        r.load_generator_output().expect("is loadgen")
                            != &LoadGeneratorOutput::Default
                    });

            match external_references {
                Some(requested)
                    if matches!(reference_policy, SourceReferencePolicy::NotAllowed) =>
                {
                    Err(PlanError::UseTablesForSources(requested.to_string()))?
                }
                Some(requested) if !multi_output_sources => match requested {
                    ExternalReferences::SubsetTables(_) => {
                        Err(LoadGeneratorSourcePurificationError::ForTables)?
                    }
                    ExternalReferences::SubsetSchemas(_) => {
                        Err(LoadGeneratorSourcePurificationError::ForSchemas)?
                    }
                    ExternalReferences::All => {
                        Err(LoadGeneratorSourcePurificationError::ForAllTables)?
                    }
                },
                Some(requested) => {
                    let requested_exports = retrieved_source_references
                        .requested_source_exports(Some(requested), source_name)?;
                    for export in requested_exports {
                        requested_subsource_map.insert(
                            export.name,
                            PurifiedSourceExport {
                                external_reference: export.external_reference,
                                details: PurifiedExportDetails::LoadGenerator {
                                    table: export
                                        .meta
                                        .load_generator_desc()
                                        .expect("is loadgen")
                                        .clone(),
                                    output: export
                                        .meta
                                        .load_generator_output()
                                        .expect("is loadgen")
                                        .clone(),
                                },
                            },
                        );
                    }
                }
                None => {
                    if multi_output_sources
                        && matches!(reference_policy, SourceReferencePolicy::Required)
                    {
                        Err(LoadGeneratorSourcePurificationError::MultiOutputRequiresForAllTables)?
                    }
                }
            }

            if let LoadGenerator::Clock = generator {
                if !options
                    .iter()
                    .any(|p| p.name == LoadGeneratorOptionName::AsOf)
                {
                    let now = catalog.now();
                    options.push(LoadGeneratorOption {
                        name: LoadGeneratorOptionName::AsOf,
                        value: Some(WithOptionValue::Value(Value::Number(now.to_string()))),
                    });
                }
            }
        }
    }

    // Now that we know which subsources to create alongside this
    // statement, remove the references so it is not canonicalized as
    // part of the `CREATE SOURCE` statement in the catalog.
    *external_references = None;

    // Generate progress subsource for old syntax
    let create_progress_subsource_stmt = if uses_old_syntax {
        // Take name from input or generate name
        let name = match progress_subsource {
            Some(name) => match name {
                DeferredItemName::Deferred(name) => name.clone(),
                DeferredItemName::Named(_) => unreachable!("already checked for this value"),
            },
            None => {
                let (item, prefix) = source_name.0.split_last().unwrap();
                let item_name =
                    Ident::try_generate_name(item.to_string(), "_progress", |candidate| {
                        let mut suggested_name = prefix.to_vec();
                        suggested_name.push(candidate.clone());

                        let partial =
                            normalize::unresolved_item_name(UnresolvedItemName(suggested_name))?;
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
        let mut progress_with_options: Vec<_> = with_options
            .iter()
            .filter_map(|opt| match opt.name {
                CreateSourceOptionName::TimestampInterval => None,
                CreateSourceOptionName::RetainHistory => Some(CreateSubsourceOption {
                    name: CreateSubsourceOptionName::RetainHistory,
                    value: opt.value.clone(),
                }),
            })
            .collect();
        progress_with_options.push(CreateSubsourceOption {
            name: CreateSubsourceOptionName::Progress,
            value: Some(WithOptionValue::Value(Value::Boolean(true))),
        });

        Some(CreateSubsourceStatement {
            name,
            columns,
            // Progress subsources do not refer to the source to which they belong.
            // Instead the primary source depends on it (the opposite is true of
            // ingestion exports, which depend on the primary source).
            of_source: None,
            constraints,
            if_not_exists: false,
            with_options: progress_with_options,
        })
    } else {
        None
    };

    purify_source_format(
        &catalog,
        format,
        &format_options,
        envelope,
        storage_configuration,
    )
    .await?;

    Ok(PurifiedStatement::PurifiedCreateSource {
        create_progress_subsource_stmt,
        create_source_stmt,
        subsources: requested_subsource_map,
        available_source_references: retrieved_source_references.available_source_references(),
    })
}

/// On success, returns the details on new subsources and updated
/// 'options' that sequencing expects for handling `ALTER SOURCE` statements.
async fn purify_alter_source(
    catalog: impl SessionCatalog,
    stmt: AlterSourceStatement<Aug>,
    storage_configuration: &StorageConfiguration,
) -> Result<PurifiedStatement, PlanError> {
    let scx = StatementContext::new(None, &catalog);
    let AlterSourceStatement {
        source_name: unresolved_source_name,
        action,
        if_exists,
    } = stmt;

    // Get name.
    let item = match scx.resolve_item(RawItemName::Name(unresolved_source_name.clone())) {
        Ok(item) => item,
        Err(_) if if_exists => {
            return Ok(PurifiedStatement::PurifiedAlterSource {
                alter_source_stmt: AlterSourceStatement {
                    source_name: unresolved_source_name,
                    action,
                    if_exists,
                },
            });
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

    let source_name = item.name();

    let resolved_source_name = ResolvedItemName::Item {
        id: item.id(),
        qualifiers: item.name().qualifiers.clone(),
        full_name: scx.catalog.resolve_full_name(source_name),
        print_id: true,
        version: RelationVersionSelector::Latest,
    };

    let partial_name = scx.catalog.minimal_qualification(source_name);

    match action {
        AlterSourceAction::AddSubsources {
            external_references,
            options,
        } => {
            if scx.catalog.system_vars().enable_create_table_from_source()
                && scx.catalog.system_vars().force_source_table_syntax()
            {
                Err(PlanError::UseTablesForSources(
                    "ALTER SOURCE .. ADD SUBSOURCES ..".to_string(),
                ))?;
            }

            purify_alter_source_add_subsources(
                external_references,
                options,
                desc,
                partial_name,
                unresolved_source_name,
                resolved_source_name,
                storage_configuration,
            )
            .await
        }
        AlterSourceAction::RefreshReferences => {
            purify_alter_source_refresh_references(
                desc,
                resolved_source_name,
                storage_configuration,
            )
            .await
        }
        _ => Ok(PurifiedStatement::PurifiedAlterSource {
            alter_source_stmt: AlterSourceStatement {
                source_name: unresolved_source_name,
                action,
                if_exists,
            },
        }),
    }
}

// TODO(database-issues#8620): Remove once subsources are removed
/// Equivalent to `purify_create_source` but for `AlterSourceStatement`.
async fn purify_alter_source_add_subsources(
    external_references: Vec<ExternalReferenceExport>,
    mut options: Vec<AlterSourceAddSubsourceOption<Aug>>,
    desc: SourceDesc,
    partial_source_name: PartialItemName,
    unresolved_source_name: UnresolvedItemName,
    resolved_source_name: ResolvedItemName,
    storage_configuration: &StorageConfiguration,
) -> Result<PurifiedStatement, PlanError> {
    // Validate this is a source that can have subsources added.
    let connection_id = match &desc.connection {
        GenericSourceConnection::Postgres(c) => c.connection_id,
        GenericSourceConnection::MySql(c) => c.connection_id,
        GenericSourceConnection::SqlServer(c) => c.connection_id,
        _ => sql_bail!(
            "source {} does not support ALTER SOURCE.",
            partial_source_name
        ),
    };

    let crate::plan::statement::ddl::AlterSourceAddSubsourceOptionExtracted {
        text_columns,
        exclude_columns,
        details,
        seen: _,
    } = options.clone().try_into()?;
    assert_none!(details, "details cannot be explicitly set");

    let mut requested_subsource_map = BTreeMap::new();

    match desc.connection {
        GenericSourceConnection::Postgres(pg_source_connection) => {
            // Get PostgresConnection for generating subsources.
            let pg_connection = &pg_source_connection.connection;

            let client = pg_connection
                .validate(connection_id, storage_configuration)
                .await?;

            let reference_client = SourceReferenceClient::Postgres {
                client: &client,
                publication: &pg_source_connection.publication,
                database: &pg_connection.database,
            };
            let retrieved_source_references = reference_client.get_source_references().await?;

            let postgres::PurifiedSourceExports {
                source_exports: subsources,
                normalized_text_columns,
            } = postgres::purify_source_exports(
                &client,
                &retrieved_source_references,
                &Some(ExternalReferences::SubsetTables(external_references)),
                text_columns,
                exclude_columns,
                &unresolved_source_name,
                &SourceReferencePolicy::Required,
            )
            .await?;

            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == AlterSourceAddSubsourceOptionName::TextColumns)
            {
                text_cols_option.value = Some(WithOptionValue::Sequence(normalized_text_columns));
            }

            requested_subsource_map.extend(subsources);
        }
        GenericSourceConnection::MySql(mysql_source_connection) => {
            let mysql_connection = &mysql_source_connection.connection;
            let config = mysql_connection
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

            // Retrieve the current @gtid_executed value of the server to mark as the effective
            // initial snapshot point for these subsources.
            let initial_gtid_set =
                mz_mysql_util::query_sys_var(&mut conn, "global.gtid_executed").await?;

            let requested_references = Some(ExternalReferences::SubsetTables(external_references));

            let reference_client = SourceReferenceClient::MySql {
                conn: &mut conn,
                include_system_schemas: mysql::references_system_schemas(&requested_references),
            };
            let retrieved_source_references = reference_client.get_source_references().await?;

            let mysql::PurifiedSourceExports {
                source_exports: subsources,
                normalized_text_columns,
                normalized_exclude_columns,
            } = mysql::purify_source_exports(
                &mut conn,
                &retrieved_source_references,
                &requested_references,
                text_columns,
                exclude_columns,
                &unresolved_source_name,
                initial_gtid_set,
                &SourceReferencePolicy::Required,
            )
            .await?;
            requested_subsource_map.extend(subsources);

            // Update options with the purified details
            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == AlterSourceAddSubsourceOptionName::TextColumns)
            {
                text_cols_option.value = Some(WithOptionValue::Sequence(normalized_text_columns));
            }
            if let Some(exclude_cols_option) = options
                .iter_mut()
                .find(|option| option.name == AlterSourceAddSubsourceOptionName::ExcludeColumns)
            {
                exclude_cols_option.value =
                    Some(WithOptionValue::Sequence(normalized_exclude_columns));
            }
        }
        GenericSourceConnection::SqlServer(sql_server_source) => {
            // Open a connection to the upstream SQL Server instance.
            let sql_server_connection = &sql_server_source.connection;
            let config = sql_server_connection
                .resolve_config(
                    &storage_configuration.connection_context.secrets_reader,
                    storage_configuration,
                    InTask::No,
                )
                .await?;
            let mut client = mz_sql_server_util::Client::connect(config).await?;

            // Query the upstream SQL Server instance for available tables to replicate.
            let database = sql_server_connection.database.clone().into();
            let source_references = SourceReferenceClient::SqlServer {
                client: &mut client,
                database: Arc::clone(&database),
            }
            .get_source_references()
            .await?;
            let requested_references = Some(ExternalReferences::SubsetTables(external_references));

            let timeout = mz_storage_types::sources::sql_server::MAX_LSN_WAIT
                .get(storage_configuration.config_set());

            let result = sql_server::purify_source_exports(
                &*database,
                &mut client,
                &source_references,
                &requested_references,
                &text_columns,
                &exclude_columns,
                &unresolved_source_name,
                timeout,
                &SourceReferencePolicy::Required,
            )
            .await;
            let sql_server::PurifiedSourceExports {
                source_exports,
                normalized_text_columns,
                normalized_excl_columns,
            } = result?;

            // Add the new exports to our subsource map.
            requested_subsource_map.extend(source_exports);

            // Update options on the CREATE SOURCE statement with the purified details.
            if let Some(text_cols_option) = options
                .iter_mut()
                .find(|option| option.name == AlterSourceAddSubsourceOptionName::TextColumns)
            {
                text_cols_option.value = Some(WithOptionValue::Sequence(normalized_text_columns));
            }
            if let Some(exclude_cols_option) = options
                .iter_mut()
                .find(|option| option.name == AlterSourceAddSubsourceOptionName::ExcludeColumns)
            {
                exclude_cols_option.value =
                    Some(WithOptionValue::Sequence(normalized_excl_columns));
            }
        }
        _ => unreachable!(),
    };

    Ok(PurifiedStatement::PurifiedAlterSourceAddSubsources {
        source_name: resolved_source_name,
        options,
        subsources: requested_subsource_map,
    })
}

async fn purify_alter_source_refresh_references(
    desc: SourceDesc,
    resolved_source_name: ResolvedItemName,
    storage_configuration: &StorageConfiguration,
) -> Result<PurifiedStatement, PlanError> {
    let retrieved_source_references = match desc.connection {
        GenericSourceConnection::Postgres(pg_source_connection) => {
            // Get PostgresConnection for generating subsources.
            let pg_connection = &pg_source_connection.connection;

            let config = pg_connection
                .config(
                    &storage_configuration.connection_context.secrets_reader,
                    storage_configuration,
                    InTask::No,
                )
                .await?;

            let client = config
                .connect(
                    "postgres_purification",
                    &storage_configuration.connection_context.ssh_tunnel_manager,
                )
                .await?;
            let reference_client = SourceReferenceClient::Postgres {
                client: &client,
                publication: &pg_source_connection.publication,
                database: &pg_connection.database,
            };
            reference_client.get_source_references().await?
        }
        GenericSourceConnection::MySql(mysql_source_connection) => {
            let mysql_connection = &mysql_source_connection.connection;
            let config = mysql_connection
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

            let reference_client = SourceReferenceClient::MySql {
                conn: &mut conn,
                include_system_schemas: false,
            };
            reference_client.get_source_references().await?
        }
        GenericSourceConnection::SqlServer(sql_server_source) => {
            // Open a connection to the upstream SQL Server instance.
            let sql_server_connection = &sql_server_source.connection;
            let config = sql_server_connection
                .resolve_config(
                    &storage_configuration.connection_context.secrets_reader,
                    storage_configuration,
                    InTask::No,
                )
                .await?;
            let mut client = mz_sql_server_util::Client::connect(config).await?;

            // Query the upstream SQL Server instance for available tables to replicate.
            let source_references = SourceReferenceClient::SqlServer {
                client: &mut client,
                database: sql_server_connection.database.clone().into(),
            }
            .get_source_references()
            .await?;
            source_references
        }
        GenericSourceConnection::LoadGenerator(load_gen_connection) => {
            let reference_client = SourceReferenceClient::LoadGenerator {
                generator: &load_gen_connection.load_generator,
            };
            reference_client.get_source_references().await?
        }
        GenericSourceConnection::Kafka(kafka_conn) => {
            let reference_client = SourceReferenceClient::Kafka {
                topic: &kafka_conn.topic,
            };
            reference_client.get_source_references().await?
        }
    };
    Ok(PurifiedStatement::PurifiedAlterSourceRefreshReferences {
        source_name: resolved_source_name,
        available_source_references: retrieved_source_references.available_source_references(),
    })
}

async fn purify_create_table_from_source(
    catalog: impl SessionCatalog,
    mut stmt: CreateTableFromSourceStatement<Aug>,
    storage_configuration: &StorageConfiguration,
) -> Result<PurifiedStatement, PlanError> {
    let scx = StatementContext::new(None, &catalog);
    let CreateTableFromSourceStatement {
        name: _,
        columns,
        constraints,
        source: source_name,
        if_not_exists: _,
        external_reference,
        format,
        envelope,
        include_metadata: _,
        with_options,
    } = &mut stmt;

    // Columns and constraints cannot be specified by the user but will be populated below.
    if matches!(columns, TableFromSourceColumns::Defined(_)) {
        sql_bail!("CREATE TABLE .. FROM SOURCE column definitions cannot be specified directly");
    }
    if !constraints.is_empty() {
        sql_bail!(
            "CREATE TABLE .. FROM SOURCE constraint definitions cannot be specified directly"
        );
    }

    // Get the source item
    let item = match scx.get_item_by_resolved_name(source_name) {
        Ok(item) => item,
        Err(e) => return Err(e),
    };

    // Ensure it's an ingestion-based and alterable source.
    let desc = match item.source_desc()? {
        Some(desc) => desc.clone().into_inline_connection(scx.catalog),
        None => {
            sql_bail!("cannot ALTER this type of source")
        }
    };
    let unresolved_source_name: UnresolvedItemName = source_name.full_item_name().clone().into();

    let crate::plan::statement::ddl::TableFromSourceOptionExtracted {
        text_columns,
        exclude_columns,
        retain_history: _,
        details,
        partition_by: _,
        seen: _,
    } = with_options.clone().try_into()?;
    assert_none!(details, "details cannot be explicitly set");

    // Our text column values are unqualified (just column names), but the purification methods below
    // expect to match the fully-qualified names against the full set of tables in upstream, so we
    // need to qualify them using the external reference first.
    let qualified_text_columns = text_columns
        .iter()
        .map(|col| {
            UnresolvedItemName(
                external_reference
                    .as_ref()
                    .map(|er| er.0.iter().chain_one(col).map(|i| i.clone()).collect())
                    .unwrap_or_else(|| vec![col.clone()]),
            )
        })
        .collect_vec();
    let qualified_exclude_columns = exclude_columns
        .iter()
        .map(|col| {
            UnresolvedItemName(
                external_reference
                    .as_ref()
                    .map(|er| er.0.iter().chain_one(col).map(|i| i.clone()).collect())
                    .unwrap_or_else(|| vec![col.clone()]),
            )
        })
        .collect_vec();

    // Should be overriden below if a source-specific format is required.
    let mut format_options = SourceFormatOptions::Default;

    let retrieved_source_references: RetrievedSourceReferences;

    let requested_references = external_reference.as_ref().map(|ref_name| {
        ExternalReferences::SubsetTables(vec![ExternalReferenceExport {
            reference: ref_name.clone(),
            alias: None,
        }])
    });

    // Run purification work specific to each source type: resolve the external reference to
    // a fully qualified name and obtain the appropriate details for the source-export statement
    let purified_export = match desc.connection {
        GenericSourceConnection::Postgres(pg_source_connection) => {
            // Get PostgresConnection for generating subsources.
            let pg_connection = &pg_source_connection.connection;

            let client = pg_connection
                .validate(pg_source_connection.connection_id, storage_configuration)
                .await?;

            let reference_client = SourceReferenceClient::Postgres {
                client: &client,
                publication: &pg_source_connection.publication,
                database: &pg_connection.database,
            };
            retrieved_source_references = reference_client.get_source_references().await?;

            let postgres::PurifiedSourceExports {
                source_exports,
                // TODO(database-issues#8620): Remove once subsources are removed
                // This `normalized_text_columns` is not relevant for us and is only returned for
                // `CREATE SOURCE` statements that automatically generate subsources
                normalized_text_columns: _,
            } = postgres::purify_source_exports(
                &client,
                &retrieved_source_references,
                &requested_references,
                qualified_text_columns,
                qualified_exclude_columns,
                &unresolved_source_name,
                &SourceReferencePolicy::Required,
            )
            .await?;
            // There should be exactly one source_export returned for this statement
            let (_, purified_export) = source_exports.into_element();
            purified_export
        }
        GenericSourceConnection::MySql(mysql_source_connection) => {
            let mysql_connection = &mysql_source_connection.connection;
            let config = mysql_connection
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

            // Retrieve the current @gtid_executed value of the server to mark as the effective
            // initial snapshot point for this table.
            let initial_gtid_set =
                mz_mysql_util::query_sys_var(&mut conn, "global.gtid_executed").await?;

            let reference_client = SourceReferenceClient::MySql {
                conn: &mut conn,
                include_system_schemas: mysql::references_system_schemas(&requested_references),
            };
            retrieved_source_references = reference_client.get_source_references().await?;

            let mysql::PurifiedSourceExports {
                source_exports,
                // TODO(database-issues#8620): Remove once subsources are removed
                // `normalized_text/exclude_columns` is not relevant for us and is only returned for
                // `CREATE SOURCE` statements that automatically generate subsources
                normalized_text_columns: _,
                normalized_exclude_columns: _,
            } = mysql::purify_source_exports(
                &mut conn,
                &retrieved_source_references,
                &requested_references,
                qualified_text_columns,
                qualified_exclude_columns,
                &unresolved_source_name,
                initial_gtid_set,
                &SourceReferencePolicy::Required,
            )
            .await?;
            // There should be exactly one source_export returned for this statement
            let (_, purified_export) = source_exports.into_element();
            purified_export
        }
        GenericSourceConnection::SqlServer(sql_server_source) => {
            let connection = sql_server_source.connection;
            let config = connection
                .resolve_config(
                    &storage_configuration.connection_context.secrets_reader,
                    storage_configuration,
                    InTask::No,
                )
                .await?;
            let mut client = mz_sql_server_util::Client::connect(config).await?;

            let database: Arc<str> = connection.database.into();
            let reference_client = SourceReferenceClient::SqlServer {
                client: &mut client,
                database: Arc::clone(&database),
            };
            retrieved_source_references = reference_client.get_source_references().await?;
            tracing::debug!(?retrieved_source_references, "got source references");

            let timeout = mz_storage_types::sources::sql_server::MAX_LSN_WAIT
                .get(storage_configuration.config_set());

            let purified_source_exports = sql_server::purify_source_exports(
                &*database,
                &mut client,
                &retrieved_source_references,
                &requested_references,
                &qualified_text_columns,
                &qualified_exclude_columns,
                &unresolved_source_name,
                timeout,
                &SourceReferencePolicy::Required,
            )
            .await?;

            // There should be exactly one source_export returned for this statement
            let (_, purified_export) = purified_source_exports.source_exports.into_element();
            purified_export
        }
        GenericSourceConnection::LoadGenerator(load_gen_connection) => {
            let reference_client = SourceReferenceClient::LoadGenerator {
                generator: &load_gen_connection.load_generator,
            };
            retrieved_source_references = reference_client.get_source_references().await?;

            let requested_exports = retrieved_source_references
                .requested_source_exports(requested_references.as_ref(), &unresolved_source_name)?;
            // There should be exactly one source_export returned
            let export = requested_exports.into_element();
            PurifiedSourceExport {
                external_reference: export.external_reference,
                details: PurifiedExportDetails::LoadGenerator {
                    table: export
                        .meta
                        .load_generator_desc()
                        .expect("is loadgen")
                        .clone(),
                    output: export
                        .meta
                        .load_generator_output()
                        .expect("is loadgen")
                        .clone(),
                },
            }
        }
        GenericSourceConnection::Kafka(kafka_conn) => {
            let reference_client = SourceReferenceClient::Kafka {
                topic: &kafka_conn.topic,
            };
            retrieved_source_references = reference_client.get_source_references().await?;
            let requested_exports = retrieved_source_references
                .requested_source_exports(requested_references.as_ref(), &unresolved_source_name)?;
            // There should be exactly one source_export returned
            let export = requested_exports.into_element();

            format_options = SourceFormatOptions::Kafka {
                topic: kafka_conn.topic.clone(),
            };
            PurifiedSourceExport {
                external_reference: export.external_reference,
                details: PurifiedExportDetails::Kafka {},
            }
        }
    };

    purify_source_format(
        &catalog,
        format,
        &format_options,
        envelope,
        storage_configuration,
    )
    .await?;

    // Update the external reference in the statement to the resolved fully-qualified
    // external reference
    *external_reference = Some(purified_export.external_reference.clone());

    // Update options in the statement using the purified export details
    match &purified_export.details {
        PurifiedExportDetails::Postgres { .. } => {
            let mut unsupported_cols = vec![];
            let postgres::PostgresExportStatementValues {
                columns: gen_columns,
                constraints: gen_constraints,
                text_columns: gen_text_columns,
                exclude_columns: gen_exclude_columns,
                details: gen_details,
                external_reference: _,
            } = postgres::generate_source_export_statement_values(
                &scx,
                purified_export,
                &mut unsupported_cols,
            )?;
            if !unsupported_cols.is_empty() {
                unsupported_cols.sort();
                Err(PgSourcePurificationError::UnrecognizedTypes {
                    cols: unsupported_cols,
                })?;
            }

            if let Some(text_cols_option) = with_options
                .iter_mut()
                .find(|option| option.name == TableFromSourceOptionName::TextColumns)
            {
                match gen_text_columns {
                    Some(gen_text_columns) => {
                        text_cols_option.value = Some(WithOptionValue::Sequence(gen_text_columns))
                    }
                    None => soft_panic_or_log!(
                        "text_columns should be Some if text_cols_option is present"
                    ),
                }
            }
            if let Some(exclude_cols_option) = with_options
                .iter_mut()
                .find(|option| option.name == TableFromSourceOptionName::ExcludeColumns)
            {
                match gen_exclude_columns {
                    Some(gen_exclude_columns) => {
                        exclude_cols_option.value =
                            Some(WithOptionValue::Sequence(gen_exclude_columns))
                    }
                    None => soft_panic_or_log!(
                        "exclude_columns should be Some if exclude_cols_option is present"
                    ),
                }
            }
            match columns {
                TableFromSourceColumns::Defined(_) => unreachable!(),
                TableFromSourceColumns::NotSpecified => {
                    *columns = TableFromSourceColumns::Defined(gen_columns);
                    *constraints = gen_constraints;
                }
                TableFromSourceColumns::Named(_) => {
                    sql_bail!("columns cannot be named for Postgres sources")
                }
            }
            with_options.push(TableFromSourceOption {
                name: TableFromSourceOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    gen_details.into_proto().encode_to_vec(),
                )))),
            })
        }
        PurifiedExportDetails::MySql { .. } => {
            let mysql::MySqlExportStatementValues {
                columns: gen_columns,
                constraints: gen_constraints,
                text_columns: gen_text_columns,
                exclude_columns: gen_exclude_columns,
                details: gen_details,
                external_reference: _,
            } = mysql::generate_source_export_statement_values(&scx, purified_export)?;

            if let Some(text_cols_option) = with_options
                .iter_mut()
                .find(|option| option.name == TableFromSourceOptionName::TextColumns)
            {
                match gen_text_columns {
                    Some(gen_text_columns) => {
                        text_cols_option.value = Some(WithOptionValue::Sequence(gen_text_columns))
                    }
                    None => soft_panic_or_log!(
                        "text_columns should be Some if text_cols_option is present"
                    ),
                }
            }
            if let Some(exclude_cols_option) = with_options
                .iter_mut()
                .find(|option| option.name == TableFromSourceOptionName::ExcludeColumns)
            {
                match gen_exclude_columns {
                    Some(gen_exclude_columns) => {
                        exclude_cols_option.value =
                            Some(WithOptionValue::Sequence(gen_exclude_columns))
                    }
                    None => soft_panic_or_log!(
                        "exclude_columns should be Some if exclude_cols_option is present"
                    ),
                }
            }
            match columns {
                TableFromSourceColumns::Defined(_) => unreachable!(),
                TableFromSourceColumns::NotSpecified => {
                    *columns = TableFromSourceColumns::Defined(gen_columns);
                    *constraints = gen_constraints;
                }
                TableFromSourceColumns::Named(_) => {
                    sql_bail!("columns cannot be named for MySQL sources")
                }
            }
            with_options.push(TableFromSourceOption {
                name: TableFromSourceOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    gen_details.into_proto().encode_to_vec(),
                )))),
            })
        }
        PurifiedExportDetails::SqlServer { .. } => {
            let sql_server::SqlServerExportStatementValues {
                columns: gen_columns,
                constraints: gen_constraints,
                text_columns: gen_text_columns,
                excl_columns: gen_excl_columns,
                details: gen_details,
                external_reference: _,
            } = sql_server::generate_source_export_statement_values(&scx, purified_export)?;

            if let Some(text_cols_option) = with_options
                .iter_mut()
                .find(|opt| opt.name == TableFromSourceOptionName::TextColumns)
            {
                match gen_text_columns {
                    Some(gen_text_columns) => {
                        text_cols_option.value = Some(WithOptionValue::Sequence(gen_text_columns))
                    }
                    None => soft_panic_or_log!(
                        "text_columns should be Some if text_cols_option is present"
                    ),
                }
            }
            if let Some(exclude_cols_option) = with_options
                .iter_mut()
                .find(|opt| opt.name == TableFromSourceOptionName::ExcludeColumns)
            {
                match gen_excl_columns {
                    Some(gen_excl_columns) => {
                        exclude_cols_option.value =
                            Some(WithOptionValue::Sequence(gen_excl_columns))
                    }
                    None => soft_panic_or_log!(
                        "excl_columns should be Some if excl_cols_option is present"
                    ),
                }
            }

            match columns {
                TableFromSourceColumns::NotSpecified => {
                    *columns = TableFromSourceColumns::Defined(gen_columns);
                    *constraints = gen_constraints;
                }
                TableFromSourceColumns::Named(_) => {
                    sql_bail!("columns cannot be named for SQL Server sources")
                }
                TableFromSourceColumns::Defined(_) => unreachable!(),
            }

            with_options.push(TableFromSourceOption {
                name: TableFromSourceOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    gen_details.into_proto().encode_to_vec(),
                )))),
            })
        }
        PurifiedExportDetails::LoadGenerator { .. } => {
            let (desc, output) = match purified_export.details {
                PurifiedExportDetails::LoadGenerator { table, output } => (table, output),
                _ => unreachable!("purified export details must be load generator"),
            };
            // We only determine the table description for multi-output load generator sources here,
            // whereas single-output load generators will have their relation description
            // determined during statment planning as envelope and format options may affect their
            // schema.
            if let Some(desc) = desc {
                let (gen_columns, gen_constraints) = scx.relation_desc_into_table_defs(&desc)?;
                match columns {
                    TableFromSourceColumns::Defined(_) => unreachable!(),
                    TableFromSourceColumns::NotSpecified => {
                        *columns = TableFromSourceColumns::Defined(gen_columns);
                        *constraints = gen_constraints;
                    }
                    TableFromSourceColumns::Named(_) => {
                        sql_bail!("columns cannot be named for multi-output load generator sources")
                    }
                }
            }
            let details = SourceExportStatementDetails::LoadGenerator { output };
            with_options.push(TableFromSourceOption {
                name: TableFromSourceOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            })
        }
        PurifiedExportDetails::Kafka {} => {
            // NOTE: Kafka tables have their 'schemas' purified into the statement inside the
            // format field, so we don't specify any columns or constraints to be stored
            // on the statement here. The RelationDesc will be determined during planning.
            let details = SourceExportStatementDetails::Kafka {};
            with_options.push(TableFromSourceOption {
                name: TableFromSourceOptionName::Details,
                value: Some(WithOptionValue::Value(Value::String(hex::encode(
                    details.into_proto().encode_to_vec(),
                )))),
            })
        }
    };

    // TODO: We might as well use the retrieved available references to update the source
    // available references table in the catalog, so plumb this through.
    // available_source_references: retrieved_source_references.available_source_references(),
    Ok(PurifiedStatement::PurifiedCreateTableFromSource { stmt })
}

enum SourceFormatOptions {
    Default,
    Kafka { topic: String },
}

async fn purify_source_format(
    catalog: &dyn SessionCatalog,
    format: &mut Option<FormatSpecifier<Aug>>,
    options: &SourceFormatOptions,
    envelope: &Option<SourceEnvelope>,
    storage_configuration: &StorageConfiguration,
) -> Result<(), PlanError> {
    if matches!(format, Some(FormatSpecifier::KeyValue { .. }))
        && !matches!(options, SourceFormatOptions::Kafka { .. })
    {
        sql_bail!("Kafka sources are the only source type that can provide KEY/VALUE formats")
    }

    match format.as_mut() {
        None => {}
        Some(FormatSpecifier::Bare(format)) => {
            purify_source_format_single(catalog, format, options, envelope, storage_configuration)
                .await?;
        }

        Some(FormatSpecifier::KeyValue { key, value: val }) => {
            purify_source_format_single(catalog, key, options, envelope, storage_configuration)
                .await?;
            purify_source_format_single(catalog, val, options, envelope, storage_configuration)
                .await?;
        }
    }
    Ok(())
}

async fn purify_source_format_single(
    catalog: &dyn SessionCatalog,
    format: &mut Format<Aug>,
    options: &SourceFormatOptions,
    envelope: &Option<SourceEnvelope>,
    storage_configuration: &StorageConfiguration,
) -> Result<(), PlanError> {
    match format {
        Format::Avro(schema) => match schema {
            AvroSchema::Csr { csr_connection } => {
                purify_csr_connection_avro(
                    catalog,
                    options,
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
                    options,
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

pub fn generate_subsource_statements(
    scx: &StatementContext,
    source_name: ResolvedItemName,
    subsources: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
) -> Result<Vec<CreateSubsourceStatement<Aug>>, PlanError> {
    // get the first subsource to determine the connection type
    if subsources.is_empty() {
        return Ok(vec![]);
    }
    let (_, purified_export) = subsources.iter().next().unwrap();

    let statements = match &purified_export.details {
        PurifiedExportDetails::Postgres { .. } => {
            crate::pure::postgres::generate_create_subsource_statements(
                scx,
                source_name,
                subsources,
            )?
        }
        PurifiedExportDetails::MySql { .. } => {
            crate::pure::mysql::generate_create_subsource_statements(scx, source_name, subsources)?
        }
        PurifiedExportDetails::SqlServer { .. } => {
            crate::pure::sql_server::generate_create_subsource_statements(
                scx,
                source_name,
                subsources,
            )?
        }
        PurifiedExportDetails::LoadGenerator { .. } => {
            let mut subsource_stmts = Vec::with_capacity(subsources.len());
            for (subsource_name, purified_export) in subsources {
                let (desc, output) = match purified_export.details {
                    PurifiedExportDetails::LoadGenerator { table, output } => (table, output),
                    _ => unreachable!("purified export details must be load generator"),
                };
                let desc =
                    desc.expect("subsources cannot be generated for single-output load generators");

                let (columns, table_constraints) = scx.relation_desc_into_table_defs(&desc)?;
                let details = SourceExportStatementDetails::LoadGenerator { output };
                // Create the subsource statement
                let subsource = CreateSubsourceStatement {
                    name: subsource_name,
                    columns,
                    of_source: Some(source_name.clone()),
                    // unlike sources that come from an external upstream, we
                    // have more leniency to introduce different constraints
                    // every time the load generator is run; i.e. we are not as
                    // worried about introducing junk data.
                    constraints: table_constraints,
                    if_not_exists: false,
                    with_options: vec![
                        CreateSubsourceOption {
                            name: CreateSubsourceOptionName::ExternalReference,
                            value: Some(WithOptionValue::UnresolvedItemName(
                                purified_export.external_reference,
                            )),
                        },
                        CreateSubsourceOption {
                            name: CreateSubsourceOptionName::Details,
                            value: Some(WithOptionValue::Value(Value::String(hex::encode(
                                details.into_proto().encode_to_vec(),
                            )))),
                        },
                    ],
                };
                subsource_stmts.push(subsource);
            }

            subsource_stmts
        }
        PurifiedExportDetails::Kafka { .. } => {
            // TODO: as part of database-issues#8322, Kafka sources will begin
            // producing data––we'll need to understand the schema
            // of the output here.
            assert!(
                subsources.is_empty(),
                "Kafka sources do not produce data-bearing subsources"
            );
            vec![]
        }
    };
    Ok(statements)
}

async fn purify_csr_connection_proto(
    catalog: &dyn SessionCatalog,
    options: &SourceFormatOptions,
    csr_connection: &mut CsrConnectionProtobuf<Aug>,
    envelope: &Option<SourceEnvelope>,
    storage_configuration: &StorageConfiguration,
) -> Result<(), PlanError> {
    let SourceFormatOptions::Kafka { topic } = options else {
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
    options: &SourceFormatOptions,
    csr_connection: &mut CsrConnectionAvro<Aug>,
    envelope: &Option<SourceEnvelope>,
    storage_configuration: &StorageConfiguration,
) -> Result<(), PlanError> {
    let SourceFormatOptions::Kafka { topic } = options else {
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
    topic: &str,
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
        0 => bail_unsupported!(29603, "Protobuf schemas with no messages"),
        _ => bail_unsupported!(29603, "Protobuf schemas with multiple messages"),
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
                    version: RelationVersionSelector::Latest,
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
        resolved_ids.add_item(mz_timestamp_id);
    }
    // Even though we always remove `mz_now()` from the `with_options`, there might be `mz_now()`
    // remaining in the main query expression of the MV, so let's visit the entire statement to look
    // for `mz_now()` everywhere.
    let mut visitor = ExprContainsTemporalVisitor::new();
    visitor.visit_create_materialized_view_statement(cmvs);
    if !visitor.contains_temporal {
        resolved_ids.remove_item(&mz_now_id);
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
