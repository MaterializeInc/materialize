// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data definition language (DDL).
//!
//! This module houses the handlers for statements that modify the catalog, like
//! `ALTER`, `CREATE`, and `DROP`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::iter;

use itertools::Itertools;
use mz_controller_types::{ClusterId, ReplicaId, DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS};
use mz_expr::CollectionPlan;
use mz_interchange::avro::{AvroSchemaGenerator, AvroSchemaOptions, DocTarget};
use mz_ore::cast::{self, CastFrom, TryCastFrom};
use mz_ore::collections::HashSet;
use mz_ore::str::StrExt;
use mz_proto::RustType;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::adt::system::Oid;
use mz_repr::role_id::RoleId;
use mz_repr::{strconv, ColumnName, ColumnType, GlobalId, RelationDesc, RelationType, ScalarType};
use mz_sql_parser::ast::display::comma_separated;
use mz_sql_parser::ast::{
    AlterClusterAction, AlterClusterStatement, AlterRoleOption, AlterRoleStatement,
    AlterSetClusterStatement, AlterSinkAction, AlterSinkStatement, AlterSourceAction,
    AlterSourceAddSubsourceOption, AlterSourceAddSubsourceOptionName, AlterSourceStatement,
    AlterSystemResetAllStatement, AlterSystemResetStatement, AlterSystemSetStatement,
    CommentObjectType, CommentStatement, CreateConnectionOption, CreateConnectionOptionName,
    CreateTypeListOption, CreateTypeListOptionName, CreateTypeMapOption, CreateTypeMapOptionName,
    DeferredItemName, DocOnIdentifier, DocOnSchema, DropOwnedStatement, SetRoleVar,
    SshConnectionOption, UnresolvedItemName, UnresolvedObjectName, UnresolvedSchemaName, Value,
};
use mz_storage_types::connections::aws::{AwsAssumeRole, AwsConfig, AwsCredentials};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::{
    AwsPrivatelink, AwsPrivatelinkConnection, Connection, CsrConnectionHttpAuth, KafkaConnection,
    KafkaSecurity, KafkaTlsConfig, SaslConfig, SshTunnel, StringOrSecret, TlsIdentity, Tunnel,
};
use mz_storage_types::sinks::{
    KafkaConsistencyConfig, KafkaSinkAvroFormatState, KafkaSinkConnection,
    KafkaSinkConnectionRetention, KafkaSinkFormat, SinkEnvelope, StorageSinkConnection,
};
use mz_storage_types::sources::encoding::{
    included_column_desc, AvroEncoding, ColumnSpec, CsvEncoding, DataEncoding, DataEncodingInner,
    ProtobufEncoding, RegexEncoding, SourceDataEncoding, SourceDataEncodingInner,
};
use mz_storage_types::sources::{
    GenericSourceConnection, KafkaMetadataKind, KafkaSourceConnection, KeyEnvelope, LoadGenerator,
    LoadGeneratorSourceConnection, PostgresSourceConnection, PostgresSourcePublicationDetails,
    ProtoPostgresSourcePublicationDetails, SourceConnection, SourceDesc, SourceEnvelope,
    TestScriptSourceConnection, Timeline, UnplannedSourceEnvelope, UpsertStyle,
};
use prost::Message;

use crate::ast::display::AstDisplay;
use crate::ast::{
    AlterConnectionStatement, AlterIndexAction, AlterIndexStatement, AlterObjectRenameStatement,
    AlterObjectSwapStatement, AlterSecretStatement, AvroSchema, AvroSchemaOption,
    AvroSchemaOptionName, AwsConnectionOption, AwsConnectionOptionName,
    AwsPrivatelinkConnectionOption, AwsPrivatelinkConnectionOptionName, ClusterOption,
    ClusterOptionName, ColumnOption, CreateClusterReplicaStatement, CreateClusterStatement,
    CreateConnection, CreateConnectionStatement, CreateDatabaseStatement, CreateIndexStatement,
    CreateMaterializedViewStatement, CreateRoleStatement, CreateSchemaStatement,
    CreateSecretStatement, CreateSinkConnection, CreateSinkOption, CreateSinkOptionName,
    CreateSinkStatement, CreateSourceConnection, CreateSourceFormat, CreateSourceOption,
    CreateSourceOptionName, CreateSourceStatement, CreateSubsourceOption,
    CreateSubsourceOptionName, CreateSubsourceStatement, CreateTableStatement, CreateTypeAs,
    CreateTypeStatement, CreateViewStatement, CreateWebhookSourceStatement, CsrConfigOption,
    CsrConfigOptionName, CsrConnection, CsrConnectionAvro, CsrConnectionOption,
    CsrConnectionOptionName, CsrConnectionProtobuf, CsrSeedProtobuf, CsvColumns, DbzMode,
    DropObjectsStatement, Envelope, Expr, Format, Ident, IfExistsBehavior, IndexOption,
    IndexOptionName, KafkaBroker, KafkaBrokerAwsPrivatelinkOption,
    KafkaBrokerAwsPrivatelinkOptionName, KafkaBrokerTunnel, KafkaConfigOptionName,
    KafkaConnectionOption, KafkaConnectionOptionName, KeyConstraint, LoadGeneratorOption,
    LoadGeneratorOptionName, PgConfigOption, PgConfigOptionName, PostgresConnectionOption,
    PostgresConnectionOptionName, ProtobufSchema, QualifiedReplica, ReferencedSubsources,
    ReplicaDefinition, ReplicaOption, ReplicaOptionName, RoleAttribute, SourceIncludeMetadata,
    SourceIncludeMetadataType, SshConnectionOptionName, Statement, TableConstraint,
    UnresolvedDatabaseName, ViewDefinition,
};
use crate::catalog::{
    CatalogCluster, CatalogDatabase, CatalogError, CatalogItem, CatalogItemType,
    CatalogRecordField, CatalogType, CatalogTypeDetails, ObjectType, SystemObjectType,
};
use crate::kafka_util::{self, KafkaConfigOptionExtracted, KafkaStartOffsetType};
use crate::names::{
    Aug, CommentObjectId, DatabaseId, ObjectId, PartialItemName, QualifiedItemName,
    RawDatabaseSpecifier, ResolvedClusterName, ResolvedColumnName, ResolvedDataType,
    ResolvedDatabaseSpecifier, ResolvedItemName, SchemaSpecifier, SystemObjectId,
};
use crate::normalize::{self, ident};
use crate::plan::error::PlanError;
use crate::plan::expr::ColumnRef;
use crate::plan::query::{scalar_type_from_catalog, ExprContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::{scl, StatementContext, StatementDesc};
use crate::plan::typeconv::{plan_cast, CastContext};
use crate::plan::with_options::{self, OptionalInterval, TryFromValue};
use crate::plan::{
    plan_utils, query, transform_ast, AlterClusterPlan, AlterClusterRenamePlan,
    AlterClusterReplicaRenamePlan, AlterClusterSwapPlan, AlterIndexResetOptionsPlan,
    AlterIndexSetOptionsPlan, AlterItemRenamePlan, AlterNoopPlan, AlterOptionParameter,
    AlterRolePlan, AlterSchemaRenamePlan, AlterSchemaSwapPlan, AlterSecretPlan,
    AlterSetClusterPlan, AlterSinkPlan, AlterSourcePlan, AlterSystemResetAllPlan,
    AlterSystemResetPlan, AlterSystemSetPlan, CommentPlan, ComputeReplicaConfig,
    ComputeReplicaIntrospectionConfig, CreateClusterManagedPlan, CreateClusterPlan,
    CreateClusterReplicaPlan, CreateClusterUnmanagedPlan, CreateClusterVariant,
    CreateConnectionPlan, CreateDatabasePlan, CreateIndexPlan, CreateMaterializedViewPlan,
    CreateRolePlan, CreateSchemaPlan, CreateSecretPlan, CreateSinkPlan, CreateSourcePlan,
    CreateTablePlan, CreateTypePlan, CreateViewPlan, DataSourceDesc, DropObjectsPlan,
    DropOwnedPlan, FullItemName, HirScalarExpr, Index, Ingestion, MaterializedView, Params, Plan,
    PlanClusterOption, PlanNotice, QueryContext, ReplicaConfig, RotateKeysPlan, Secret, Sink,
    Source, SourceSinkClusterConfig, Table, Type, VariableValue, View, WebhookHeaderFilters,
    WebhookHeaders, WebhookValidation,
};
use crate::session::vars;

// TODO: Figure out what the maximum number of columns we can actually support is, and set that.
//
// The real max is probably higher than this, but it's easier to relax a constraint than make it
// more strict.
const MAX_NUM_COLUMNS: usize = 256;

const MANAGED_REPLICA_PATTERN: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| regex::Regex::new(r"^r(\d)+$").unwrap());

pub fn describe_create_database(
    _: &StatementContext,
    _: CreateDatabaseStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_database(
    _: &StatementContext,
    CreateDatabaseStatement {
        name,
        if_not_exists,
    }: CreateDatabaseStatement,
) -> Result<Plan, PlanError> {
    Ok(Plan::CreateDatabase(CreateDatabasePlan {
        name: normalize::ident(name.0),
        if_not_exists,
    }))
}

pub fn describe_create_schema(
    _: &StatementContext,
    _: CreateSchemaStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_schema(
    scx: &StatementContext,
    CreateSchemaStatement {
        mut name,
        if_not_exists,
    }: CreateSchemaStatement,
) -> Result<Plan, PlanError> {
    if name.0.len() > 2 {
        sql_bail!("schema name {} has more than two components", name);
    }
    let schema_name = normalize::ident(
        name.0
            .pop()
            .expect("names always have at least one component"),
    );
    let database_spec = match name.0.pop() {
        None => match scx.catalog.active_database() {
            Some(id) => ResolvedDatabaseSpecifier::Id(id.clone()),
            None => sql_bail!("no database specified and no active database"),
        },
        Some(n) => match scx.resolve_database(&UnresolvedDatabaseName(n.clone())) {
            Ok(database) => ResolvedDatabaseSpecifier::Id(database.id()),
            Err(_) => sql_bail!("invalid database {}", n.as_str()),
        },
    };
    Ok(Plan::CreateSchema(CreateSchemaPlan {
        database_spec,
        schema_name,
        if_not_exists,
    }))
}

pub fn describe_create_table(
    _: &StatementContext,
    _: CreateTableStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_table(
    scx: &StatementContext,
    stmt: CreateTableStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateTableStatement {
        name,
        columns,
        constraints,
        if_not_exists,
        temporary,
    } = &stmt;

    let names: Vec<_> = columns
        .iter()
        .map(|c| normalize::column_name(c.name.clone()))
        .collect();

    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    // Build initial relation type that handles declared data types
    // and NOT NULL constraints.
    let mut column_types = Vec::with_capacity(columns.len());
    let mut defaults = Vec::with_capacity(columns.len());
    let mut keys = Vec::new();

    for (i, c) in columns.into_iter().enumerate() {
        let aug_data_type = &c.data_type;
        let ty = query::scalar_type_from_sql(scx, aug_data_type)?;
        let mut nullable = true;
        let mut default = Expr::null();
        for option in &c.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(expr) => {
                    // Ensure expression can be planned and yields the correct
                    // type.
                    let mut expr = expr.clone();
                    transform_ast::transform(scx, &mut expr)?;
                    let _ = query::plan_default_expr(scx, &expr, &ty)?;
                    default = expr.clone();
                }
                ColumnOption::Unique { is_primary } => {
                    keys.push(vec![i]);
                    if *is_primary {
                        nullable = false;
                    }
                }
                other => {
                    bail_unsupported!(format!("CREATE TABLE with column constraint: {}", other))
                }
            }
        }
        column_types.push(ty.nullable(nullable));
        defaults.push(default);
    }

    let mut seen_primary = false;
    'c: for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary,
                nulls_not_distinct,
            } => {
                if seen_primary && *is_primary {
                    sql_bail!(
                        "multiple primary keys for table {} are not allowed",
                        name.to_ast_string_stable()
                    );
                }
                seen_primary = *is_primary || seen_primary;

                let mut key = vec![];
                for column in columns {
                    let column = normalize::column_name(column.clone());
                    match names.iter().position(|name| *name == column) {
                        None => sql_bail!("unknown column in constraint: {}", column),
                        Some(i) => {
                            let nullable = &mut column_types[i].nullable;
                            if *is_primary {
                                if *nulls_not_distinct {
                                    sql_bail!(
                                        "[internal error] PRIMARY KEY does not support NULLS NOT DISTINCT"
                                    );
                                }

                                *nullable = false;
                            } else if !(*nulls_not_distinct || !*nullable) {
                                // Non-primary key unique constraints are only keys if all of their
                                // columns are `NOT NULL` or the constraint is `NULLS NOT DISTINCT`.
                                break 'c;
                            }

                            key.push(i);
                        }
                    }
                }

                if *is_primary {
                    keys.insert(0, key);
                } else {
                    keys.push(key);
                }
            }
            TableConstraint::ForeignKey { .. } => {
                // Foreign key constraints are not presently enforced. We allow
                // them with feature flags for sqllogictest's sake.
                scx.require_feature_flag(&vars::ENABLE_TABLE_FOREIGN_KEY)?
            }
            TableConstraint::Check { .. } => {
                // Check constraints are not presently enforced. We allow them
                // with feature flags for sqllogictest's sake.
                scx.require_feature_flag(&vars::ENABLE_TABLE_CHECK_CONSTRAINT)?
            }
        }
    }

    if !keys.is_empty() {
        // Unique constraints are not presently enforced. We allow them with feature flags for
        // sqllogictest's sake.
        scx.require_feature_flag(&vars::ENABLE_TABLE_KEYS)?
    }

    let typ = RelationType::new(column_types).with_keys(keys);

    let temporary = *temporary;
    let name = if temporary {
        scx.allocate_temporary_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    } else {
        scx.allocate_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    };

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    let desc = RelationDesc::new(typ, names);

    let create_sql = normalize::create_statement(scx, Statement::CreateTable(stmt.clone()))?;
    let table = Table {
        create_sql,
        desc,
        defaults,
        temporary,
    };
    Ok(Plan::CreateTable(CreateTablePlan {
        name,
        table,
        if_not_exists: *if_not_exists,
    }))
}

pub fn describe_create_webhook_source(
    _: &StatementContext,
    _: CreateWebhookSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn describe_create_source(
    _: &StatementContext,
    _: CreateSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn describe_create_subsource(
    _: &StatementContext,
    _: CreateSubsourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(
    CreateSourceOption,
    (IgnoreKeys, bool),
    (Size, String),
    (Timeline, String),
    (TimestampInterval, Interval)
);

generate_extracted_config!(
    PgConfigOption,
    (Details, String),
    (Publication, String),
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

pub fn plan_create_webhook_source(
    scx: &StatementContext,
    stmt: CreateWebhookSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let create_sql =
        normalize::create_statement(scx, Statement::CreateWebhookSource(stmt.clone()))?;

    let CreateWebhookSourceStatement {
        name,
        if_not_exists,
        body_format,
        include_headers,
        validate_using,
        in_cluster,
    } = stmt;

    let validate_using = validate_using
        .map(|stmt| query::plan_webhook_validate_using(scx, stmt))
        .transpose()?;
    if let Some(WebhookValidation { expression, .. }) = &validate_using {
        // If the validation expression doesn't reference any part of the request, then we should
        // return an error because it's almost definitely wrong.
        if !expression.contains_column() {
            return Err(PlanError::WebhookValidationDoesNotUseColumns);
        }
        // Validation expressions cannot contain unmaterializable functions, e.g. now().
        if expression.contains_unmaterializable() {
            return Err(PlanError::WebhookValidationNonDeterministic);
        }
    }

    let body_scalar_type = match body_format {
        Format::Bytes => ScalarType::Bytes,
        Format::Json => ScalarType::Jsonb,
        Format::Text => ScalarType::String,
        // TODO(parkmycar): Make an issue to support more types, or change this to NeverSupported.
        ty => {
            return Err(PlanError::Unsupported {
                feature: format!("{ty} is not a valid BODY FORMAT for a WEBHOOK source"),
                issue_no: None,
            })
        }
    };

    let mut column_ty = vec![
        // Always include the body of the request as the first column.
        ColumnType {
            scalar_type: body_scalar_type,
            nullable: false,
        },
    ];
    let mut column_names = vec!["body".to_string()];

    let mut headers = WebhookHeaders::default();

    // Include a `headers` column, possibly filtered.
    if let Some(filters) = include_headers.column {
        column_ty.push(ColumnType {
            scalar_type: ScalarType::Map {
                value_type: Box::new(ScalarType::String),
                custom_id: None,
            },
            nullable: false,
        });
        column_names.push("headers".to_string());

        let (allow, block): (BTreeSet<_>, BTreeSet<_>) =
            filters.into_iter().partition_map(|filter| {
                if filter.block {
                    itertools::Either::Right(filter.header_name)
                } else {
                    itertools::Either::Left(filter.header_name)
                }
            });
        headers.header_column = Some(WebhookHeaderFilters { allow, block });
    }

    // Map headers to specific columns.
    for header in include_headers.mappings {
        let scalar_type = header
            .use_bytes
            .then_some(ScalarType::Bytes)
            .unwrap_or(ScalarType::String);
        column_ty.push(ColumnType {
            scalar_type,
            nullable: true,
        });
        column_names.push(header.column_name.into_string());

        let column_idx = column_ty.len() - 1;
        // Double check we're consistent with column names.
        assert_eq!(
            column_idx,
            column_names.len() - 1,
            "header column names and types don't match"
        );
        headers
            .mapped_headers
            .insert(column_idx, (header.header_name, header.use_bytes));
    }

    // Validate our columns.
    let mut unique_check = HashSet::with_capacity(column_names.len());
    for name in &column_names {
        if !unique_check.insert(name) {
            return Err(PlanError::AmbiguousColumn(name.clone().into()));
        }
    }
    if column_names.len() > MAX_NUM_COLUMNS {
        return Err(PlanError::TooManyColumns {
            max_num_columns: MAX_NUM_COLUMNS,
            req_num_columns: column_names.len(),
        });
    }

    let typ = RelationType::new(column_ty);
    let desc = RelationDesc::new(typ, column_names);

    let cluster_config = source_sink_cluster_config(scx, "source", Some(&in_cluster), None)?;

    // Check for an object in the catalog with this same name
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    // Note(parkmycar): We don't currently support specifying a timeline for Webhook sources. As
    // such, we always use a default of EpochMilliseconds.
    let timeline = Timeline::EpochMilliseconds;

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source: Source {
            create_sql,
            data_source: DataSourceDesc::Webhook {
                validate_using,
                headers,
            },
            desc,
        },
        if_not_exists,
        timeline,
        cluster_config,
    }))
}

pub fn plan_create_source(
    scx: &StatementContext,
    stmt: CreateSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSourceStatement {
        name,
        in_cluster,
        col_names,
        connection,
        envelope,
        if_not_exists,
        format,
        key_constraint,
        include_metadata,
        with_options,
        referenced_subsources,
        progress_subsource,
    } = &stmt;

    let envelope = envelope.clone().unwrap_or(Envelope::None);

    let allowed_with_options = vec![
        CreateSourceOptionName::Size,
        CreateSourceOptionName::TimestampInterval,
    ];
    if let Some(op) = with_options
        .iter()
        .find(|op| !allowed_with_options.contains(&op.name))
    {
        scx.require_feature_flag_w_dynamic_desc(
            &vars::ENABLE_CREATE_SOURCE_DENYLIST_WITH_OPTIONS,
            format!("CREATE SOURCE...WITH ({}..)", op.name.to_ast_string()),
            format!(
                "permitted options are {}",
                comma_separated(&allowed_with_options)
            ),
        )?;
    }

    if !matches!(connection, CreateSourceConnection::Kafka { .. })
        && include_metadata
            .iter()
            .any(|sic| sic.ty == SourceIncludeMetadataType::Headers)
    {
        // TODO(guswynn): should this be `bail_unsupported!`?
        sql_bail!("INCLUDE HEADERS with non-Kafka sources not supported");
    }
    if !matches!(connection, CreateSourceConnection::Kafka { .. }) && !include_metadata.is_empty() {
        bail_unsupported!("INCLUDE metadata with non-Kafka sources");
    }

    let (mut external_connection, encoding, available_subsources) = match connection {
        CreateSourceConnection::Kafka(mz_sql_parser::ast::KafkaSourceConnection {
            connection:
                mz_sql_parser::ast::KafkaConnection {
                    connection: connection_name,
                    options,
                },
            key: _,
        }) => {
            let connection_item = scx.get_item_by_resolved_name(connection_name)?;
            let mut kafka_connection = match connection_item.connection()? {
                Connection::Kafka(connection) => connection.clone(),
                _ => sql_bail!(
                    "{} is not a kafka connection",
                    scx.catalog.resolve_full_name(connection_item.name())
                ),
            };

            // Starting offsets are allowed out with feature flags mode, as they are a simple,
            // useful way to specify where to start reading a topic.
            const ALLOWED_OPTIONS: &[KafkaConfigOptionName] = &[
                KafkaConfigOptionName::StartOffset,
                KafkaConfigOptionName::StartTimestamp,
                KafkaConfigOptionName::Topic,
            ];

            if let Some(op) = options
                .iter()
                .find(|op| !ALLOWED_OPTIONS.contains(&op.name))
            {
                scx.require_feature_flag_w_dynamic_desc(
                    &vars::ENABLE_KAFKA_CONFIG_DENYLIST_OPTIONS,
                    format!("FROM KAFKA CONNECTION ({}...)", op.name.to_ast_string()),
                    format!("permitted options are {}", comma_separated(ALLOWED_OPTIONS)),
                )?;
            }

            kafka_util::validate_options_for_context(
                options,
                kafka_util::KafkaOptionCheckContext::Source,
            )?;

            let extracted_options: KafkaConfigOptionExtracted = options.clone().try_into()?;

            let optional_start_offset =
                Option::<kafka_util::KafkaStartOffsetType>::try_from(&extracted_options)?;

            for (k, v) in kafka_util::LibRdKafkaConfig::try_from(&extracted_options)?.0 {
                kafka_connection.options.insert(k, v);
            }

            let topic = extracted_options
                .topic
                .expect("validated exists during purification");
            let group_id_prefix = extracted_options.group_id_prefix;

            let mut start_offsets = BTreeMap::new();
            match optional_start_offset {
                None => (),
                Some(KafkaStartOffsetType::StartOffset(offsets)) => {
                    for (part, offset) in offsets.iter().enumerate() {
                        if *offset < 0 {
                            sql_bail!("START OFFSET must be a nonnegative integer");
                        }
                        start_offsets.insert(i32::try_from(part)?, *offset);
                    }
                }
                Some(KafkaStartOffsetType::StartTimestamp(_)) => {
                    unreachable!("time offsets should be converted in purification")
                }
            }

            if !start_offsets.is_empty() && envelope.requires_all_input() {
                sql_bail!("START OFFSET is not supported with ENVELOPE {}", envelope)
            }

            let encoding = get_encoding(scx, format, &envelope, Some(connection))?;

            if !include_metadata.is_empty()
                && !matches!(
                    envelope,
                    Envelope::Upsert | Envelope::None | Envelope::Debezium(DbzMode::Plain)
                )
            {
                // TODO(guswynn): should this be `bail_unsupported!`?
                sql_bail!("INCLUDE <metadata> requires ENVELOPE (NONE|UPSERT|DEBEZIUM)");
            }

            let metadata_columns = include_metadata
                .into_iter()
                .flat_map(|item| match item.ty {
                    SourceIncludeMetadataType::Timestamp => {
                        let name = match item.alias.as_ref() {
                            Some(name) => name.to_string(),
                            None => "timestamp".to_owned(),
                        };
                        Some((name, KafkaMetadataKind::Timestamp))
                    }
                    SourceIncludeMetadataType::Partition => {
                        let name = match item.alias.as_ref() {
                            Some(name) => name.to_string(),
                            None => "partition".to_owned(),
                        };
                        Some((name, KafkaMetadataKind::Partition))
                    }
                    SourceIncludeMetadataType::Offset => {
                        let name = match item.alias.as_ref() {
                            Some(name) => name.to_string(),
                            None => "offset".to_owned(),
                        };
                        Some((name, KafkaMetadataKind::Offset))
                    }
                    SourceIncludeMetadataType::Headers => {
                        let name = match item.alias.as_ref() {
                            Some(name) => name.to_string(),
                            None => "headers".to_owned(),
                        };
                        Some((name, KafkaMetadataKind::Headers))
                    }
                    SourceIncludeMetadataType::Key => {
                        // handled below
                        None
                    }
                })
                .collect();

            let connection = KafkaSourceConnection::<ReferencedConnection> {
                connection: connection_item.id(),
                connection_id: connection_item.id(),
                topic,
                start_offsets,
                group_id_prefix,
                environment_id: scx.catalog.config().environment_id.to_string(),
                metadata_columns,
            };

            let connection = GenericSourceConnection::Kafka(connection);

            (connection, encoding, None)
        }
        CreateSourceConnection::Postgres {
            connection,
            options,
        } => {
            let connection_item = scx.get_item_by_resolved_name(connection)?;
            let connection = match connection_item.connection()? {
                Connection::Postgres(connection) => connection.clone(),
                _ => sql_bail!(
                    "{} is not a postgres connection",
                    scx.catalog.resolve_full_name(connection_item.name())
                ),
            };
            let PgConfigOptionExtracted {
                details,
                publication,
                text_columns,
                seen: _,
            } = options.clone().try_into()?;

            let details = details
                .as_ref()
                .ok_or_else(|| sql_err!("internal error: Postgres source missing details"))?;
            let details = hex::decode(details).map_err(|e| sql_err!("{}", e))?;
            let details = ProtoPostgresSourcePublicationDetails::decode(&*details)
                .map_err(|e| sql_err!("{}", e))?;

            // Create a "catalog" of the tables in the PG details.
            let mut tables_by_name = BTreeMap::new();
            for table in details.tables.iter() {
                tables_by_name
                    .entry(table.name.clone())
                    .or_insert_with(BTreeMap::new)
                    .entry(table.namespace.clone())
                    .or_insert_with(BTreeMap::new)
                    .entry(connection.database.clone())
                    .or_insert(table);
            }

            let publication_catalog = crate::catalog::ErsatzCatalog(tables_by_name);

            let mut text_cols: BTreeMap<Oid, BTreeSet<String>> = BTreeMap::new();

            // Look up the referenced text_columns in the publication_catalog.
            for name in text_columns {
                let (qual, col) = match name.0.split_last().expect("must have at least one element")
                {
                    (col, qual) => (UnresolvedItemName(qual.to_vec()), col.as_str().to_string()),
                };

                let (_name, table_desc) = publication_catalog
                    .resolve(qual)
                    .expect("known to exist from purification");

                assert!(
                    table_desc
                        .columns
                        .iter()
                        .find(|column| column.name == col)
                        .is_some(),
                    "validated column exists in table during purification"
                );

                text_cols
                    .entry(Oid(table_desc.oid))
                    .or_default()
                    .insert(col);
            }

            // Register the available subsources
            let mut available_subsources = BTreeMap::new();

            // Here we will generate the cast expressions required to convert the text encoded
            // columns into the appropriate target types, creating a Vec<MirScalarExpr> per table.
            // The postgres source reader will then eval each of those on the incoming rows based
            // on the target table
            let mut table_casts = BTreeMap::new();

            for (i, table) in details.tables.iter().enumerate() {
                // First, construct an expression context where the expression is evaluated on an
                // imaginary row which has the same number of columns as the upstream table but all
                // of the types are text
                let mut cast_scx = scx.clone();
                cast_scx.param_types = Default::default();
                let cast_qcx = QueryContext::root(&cast_scx, QueryLifetime::Source);
                let mut column_types = vec![];
                for column in table.columns.iter() {
                    column_types.push(ColumnType {
                        nullable: column.nullable,
                        scalar_type: ScalarType::String,
                    });
                }

                let cast_ecx = ExprContext {
                    qcx: &cast_qcx,
                    name: "plan_postgres_source_cast",
                    scope: &Scope::empty(),
                    relation_type: &RelationType {
                        column_types,
                        keys: vec![],
                    },
                    allow_aggregates: false,
                    allow_subqueries: false,
                    allow_parameters: false,
                    allow_windows: false,
                };

                // Then, for each column we will generate a MirRelationExpr that extracts the nth
                // column and casts it to the appropriate target type
                let mut column_casts = vec![];
                for (i, column) in table.columns.iter().enumerate() {
                    let ty = match text_cols.get(&Oid(table.oid)) {
                        // Treat the column as text if it was referenced in
                        // `TEXT COLUMNS`. This is the only place we need to
                        // perform this logic; even if the type is unsupported,
                        // we'll be able to ingest its values as text in
                        // storage.
                        Some(names) if names.contains(&column.name) => mz_pgrepr::Type::Text,
                        _ => {
                            match mz_pgrepr::Type::from_oid_and_typmod(
                                column.type_oid,
                                column.type_mod,
                            ) {
                                Ok(t) => t,
                                // If this reference survived purification, we
                                // do not expect it to be from a table that the
                                // user will consume., i.e. expect this table to
                                // be filtered out of table casts.
                                Err(_) => {
                                    column_casts.push(
                                        HirScalarExpr::CallVariadic {
                                            func: mz_expr::VariadicFunc::ErrorIfNull,
                                            exprs: vec![
                                                HirScalarExpr::literal_null(ScalarType::String),
                                                HirScalarExpr::literal(
                                                    mz_repr::Datum::from(
                                                        format!(
                                                            "Unsupported type with OID {}",
                                                            column.type_oid
                                                        )
                                                        .as_str(),
                                                    ),
                                                    ScalarType::String,
                                                ),
                                            ],
                                        }
                                        .lower_uncorrelated()
                                        .expect("no correlation"),
                                    );
                                    continue;
                                }
                            }
                        }
                    };

                    let data_type = scx.resolve_type(ty)?;
                    let scalar_type = query::scalar_type_from_sql(scx, &data_type)?;

                    let col_expr = HirScalarExpr::Column(ColumnRef {
                        level: 0,
                        column: i,
                    });

                    let cast_expr =
                        plan_cast(&cast_ecx, CastContext::Explicit, col_expr, &scalar_type)?;

                    let cast = if column.nullable {
                        cast_expr
                    } else {
                        // We must enforce nullability constraint on cast
                        // because PG replication stream does not propagate
                        // constraint changes and we want to error subsource if
                        // e.g. the constraint is dropped and we don't notice
                        // it.
                        HirScalarExpr::CallVariadic {
                            func: mz_expr::VariadicFunc::ErrorIfNull,
                            exprs: vec![
                                cast_expr,
                                HirScalarExpr::literal(
                                    mz_repr::Datum::from(
                                        format!(
                                            "PG column {}.{}.{} contained NULL data, despite having NOT NULL constraint",
                                            table.namespace.clone(),
                                            table.name.clone(),
                                            column.name.clone())
                                            .as_str(),
                                    ),
                                    ScalarType::String,
                                ),
                            ],
                        }
                    };

                    let mir_cast = cast.lower_uncorrelated().expect(
                        "lower_uncorrelated should not fail given that there is no correlation \
                            in the input col_expr",
                    );

                    column_casts.push(mir_cast);
                }
                let r = table_casts.insert(i + 1, column_casts);
                assert!(r.is_none(), "cannot have table defined multiple times");

                let name = FullItemName {
                    database: RawDatabaseSpecifier::Name(connection.database.clone()),
                    schema: table.namespace.clone(),
                    item: table.name.clone(),
                };

                // The zero-th output is the main output
                // TODO(petrosagg): these plus ones are an accident waiting to happen. Find a way
                // to handle the main source and the subsources uniformly
                available_subsources.insert(name, i + 1);
            }

            let publication_details = PostgresSourcePublicationDetails::from_proto(details)
                .map_err(|e| sql_err!("{}", e))?;

            let connection =
                GenericSourceConnection::<ReferencedConnection>::from(PostgresSourceConnection {
                    connection: connection_item.id(),
                    connection_id: connection_item.id(),
                    table_casts,
                    publication: publication.expect("validated exists during purification"),
                    publication_details,
                });
            // The postgres source only outputs data to its subsources. The catalog object
            // representing the source itself is just an empty relation with no columns
            let encoding = SourceDataEncoding::Single(DataEncoding::new(
                DataEncodingInner::RowCodec(RelationDesc::empty()),
            ));
            (connection, encoding, Some(available_subsources))
        }
        CreateSourceConnection::LoadGenerator { generator, options } => {
            let (load_generator, available_subsources) =
                load_generator_ast_to_generator(generator, options)?;
            let available_subsources = available_subsources
                .map(|a| BTreeMap::from_iter(a.into_iter().map(|(k, v)| (k, v.0))));

            let LoadGeneratorOptionExtracted { tick_interval, .. } = options.clone().try_into()?;
            let tick_micros = match tick_interval {
                Some(interval) => {
                    let micros: u64 = interval.as_microseconds().try_into()?;
                    Some(micros)
                }
                None => None,
            };

            let encoding = load_generator.data_encoding();

            let connection = GenericSourceConnection::from(LoadGeneratorSourceConnection {
                load_generator,
                tick_micros,
            });

            (connection, encoding, available_subsources)
        }
        CreateSourceConnection::TestScript { desc_json } => {
            scx.require_feature_flag(&vars::ENABLE_CREATE_SOURCE_FROM_TESTSCRIPT)?;
            let connection = GenericSourceConnection::from(TestScriptSourceConnection {
                desc_json: desc_json.clone(),
            });
            // we just use the encoding from the format and envelope
            let encoding = get_encoding(scx, format, &envelope, None)?;
            (connection, encoding, None)
        }
    };

    let (available_subsources, requested_subsources) = match (
        available_subsources,
        referenced_subsources,
    ) {
        (Some(available_subsources), Some(ReferencedSubsources::SubsetTables(subsources))) => {
            let mut requested_subsources = vec![];
            for subsource in subsources {
                let name = subsource.reference.clone();

                let target = match &subsource.subsource {
                    Some(DeferredItemName::Named(target)) => target.clone(),
                    _ => {
                        sql_bail!("[internal error] subsources must be named during purification")
                    }
                };

                requested_subsources.push((name, target));
            }
            (available_subsources, requested_subsources)
        }
        (Some(_), None) => {
            // Multi-output sources must have a table selection clause
            sql_bail!("This is a multi-output source. Use `FOR TABLE (..)` or `FOR ALL TABLES` to select which ones to ingest");
        }
        (None, Some(_))
        | (Some(_), Some(ReferencedSubsources::All | ReferencedSubsources::SubsetSchemas(_))) => {
            sql_bail!("[internal error] subsources should be resolved during purification")
        }
        (None, None) => (BTreeMap::new(), vec![]),
    };

    let mut subsource_exports = BTreeMap::new();
    for (name, target) in requested_subsources {
        let name = normalize::full_name(name)?;
        let idx = match available_subsources.get(&name) {
            Some(idx) => idx,
            None => sql_bail!("Requested non-existent subtable: {name}"),
        };

        let target_id = match target {
            ResolvedItemName::Item { id, .. } => id,
            ResolvedItemName::Cte { .. } | ResolvedItemName::Error => {
                sql_bail!("[internal error] invalid target id")
            }
        };

        // TODO(petrosagg): This is the point where we would normally look into the catalog for the
        // item with ID `target` and verify that its RelationDesc is identical to the type of the
        // dataflow output. We can't do that here however because the subsources are created in the
        // same transaction as this source and they are not yet in the catalog. In the future, when
        // provisional catalogs are made available to the planner we could do the check. For now
        // we don't allow users to manually target subsources and rely on purification generating
        // correct definitions.
        subsource_exports.insert(target_id, *idx);
    }

    if let GenericSourceConnection::Postgres(conn) = &mut external_connection {
        // Now that we know which subsources sources we want, we can remove all
        // unused table casts from this connection; this represents the
        // authoritative statement about which publication tables should be
        // used within storage.

        // we want to temporarily test if any users are referring to the same table in their PG
        // sources.
        let mut used_pos: Vec<_> = subsource_exports.values().collect();
        used_pos.sort();

        if let Some(_) = used_pos.iter().duplicates().next() {
            tracing::warn!("multiple references to same upstream table in PG source");
        }

        let used_pos: BTreeSet<_> = used_pos.into_iter().collect();
        conn.table_casts.retain(|pos, _| used_pos.contains(pos));
    }

    let CreateSourceOptionExtracted {
        size,
        timeline,
        timestamp_interval,
        ignore_keys,
        seen: _,
    } = CreateSourceOptionExtracted::try_from(with_options.clone())?;

    let (key_desc, value_desc) = encoding.desc()?;

    let mut key_envelope = get_key_envelope(include_metadata, &envelope, &encoding)?;

    // Not all source envelopes are compatible with all source connections.
    // Whoever constructs the source ingestion pipeline is responsible for
    // choosing compatible envelopes and connections.
    //
    // TODO(guswynn): ambiguously assert which connections and envelopes are
    // compatible in typechecking
    //
    // TODO: remove bails as more support for upsert is added.
    let envelope = match &envelope {
        // TODO: fixup key envelope
        mz_sql_parser::ast::Envelope::None => UnplannedSourceEnvelope::None(key_envelope),
        mz_sql_parser::ast::Envelope::Debezium(mode) => {
            //TODO check that key envelope is not set
            let (_before_idx, after_idx) = typecheck_debezium(&value_desc)?;

            match mode {
                DbzMode::Plain => UnplannedSourceEnvelope::Upsert {
                    style: UpsertStyle::Debezium { after_idx },
                },
            }
        }
        mz_sql_parser::ast::Envelope::Upsert => {
            let key_encoding = match encoding.key_ref() {
                None => {
                    bail_unsupported!(format!("upsert requires a key/value format: {:?}", format))
                }
                Some(key_encoding) => key_encoding,
            };
            // `ENVELOPE UPSERT` implies `INCLUDE KEY`, if it is not explicitly
            // specified.
            if key_envelope == KeyEnvelope::None {
                key_envelope = get_unnamed_key_envelope(key_encoding)?;
            }
            UnplannedSourceEnvelope::Upsert {
                style: UpsertStyle::Default(key_envelope),
            }
        }
        mz_sql_parser::ast::Envelope::CdcV2 => {
            scx.require_feature_flag(&vars::ENABLE_ENVELOPE_MATERIALIZE)?;
            //TODO check that key envelope is not set
            match format {
                CreateSourceFormat::Bare(Format::Avro(_)) => {}
                _ => bail_unsupported!("non-Avro-encoded ENVELOPE MATERIALIZE"),
            }
            UnplannedSourceEnvelope::CdcV2
        }
    };

    let metadata_columns = external_connection.metadata_columns();
    let metadata_desc = included_column_desc(metadata_columns.clone());
    let (envelope, mut desc) = envelope.desc(key_desc, value_desc, metadata_desc)?;

    if ignore_keys.unwrap_or(false) {
        desc = desc.without_keys();
    }

    plan_utils::maybe_rename_columns(format!("source {}", name), &mut desc, col_names)?;

    let names: Vec<_> = desc.iter_names().cloned().collect();
    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    // Apply user-specified key constraint
    if let Some(KeyConstraint::PrimaryKeyNotEnforced { columns }) = key_constraint.clone() {
        // Don't remove this without addressing
        // https://github.com/MaterializeInc/materialize/issues/15272.
        scx.require_feature_flag(&vars::ENABLE_PRIMARY_KEY_NOT_ENFORCED)?;

        let key_columns = columns
            .into_iter()
            .map(normalize::column_name)
            .collect::<Vec<_>>();

        let mut uniq = BTreeSet::new();
        for col in key_columns.iter() {
            if !uniq.insert(col) {
                sql_bail!("Repeated column name in source key constraint: {}", col);
            }
        }

        let key_indices = key_columns
            .iter()
            .map(|col| {
                let name_idx = desc
                    .get_by_name(col)
                    .map(|(idx, _type)| idx)
                    .ok_or_else(|| sql_err!("No such column in source key constraint: {}", col))?;
                if desc.get_unambiguous_name(name_idx).is_none() {
                    sql_bail!("Ambiguous column in source key constraint: {}", col);
                }
                Ok(name_idx)
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !desc.typ().keys.is_empty() {
            return Err(key_constraint_err(&desc, &key_columns));
        } else {
            desc = desc.with_key(key_indices);
        }
    }

    let cluster_config = source_sink_cluster_config(scx, "source", in_cluster.as_ref(), size)?;

    let timestamp_interval = match timestamp_interval {
        Some(timestamp_interval) => {
            let duration = timestamp_interval.duration()?;
            let min = scx.catalog.system_vars().min_timestamp_interval();
            let max = scx.catalog.system_vars().max_timestamp_interval();
            if duration < min || duration > max {
                return Err(PlanError::InvalidTimestampInterval {
                    min,
                    max,
                    requested: duration,
                });
            }
            duration
        }
        None => scx.catalog.config().timestamp_interval,
    };

    let source_desc = SourceDesc::<ReferencedConnection> {
        connection: external_connection,
        encoding,
        envelope: envelope.clone(),
        timestamp_interval,
    };

    let progress_subsource = match progress_subsource {
        Some(name) => match name {
            DeferredItemName::Named(name) => match name {
                ResolvedItemName::Item { id, .. } => *id,
                ResolvedItemName::Cte { .. } | ResolvedItemName::Error => {
                    sql_bail!("[internal error] invalid target id")
                }
            },
            DeferredItemName::Deferred(_) => {
                sql_bail!("[internal error] progress subsource must be named during purification")
            }
        },
        _ => sql_bail!("[internal error] progress subsource must be named during purification"),
    };

    let if_not_exists = *if_not_exists;
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name.clone())?)?;

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    let create_sql = normalize::create_statement(scx, Statement::CreateSource(stmt))?;

    // Allow users to specify a timeline. If they do not, determine a default
    // timeline for the source.
    let timeline = match timeline {
        None => match envelope {
            SourceEnvelope::CdcV2 => {
                Timeline::External(scx.catalog.resolve_full_name(&name).to_string())
            }
            _ => Timeline::EpochMilliseconds,
        },
        // TODO(benesch): if we stabilize this, can we find a better name than
        // `mz_epoch_ms`? Maybe just `mz_system`?
        Some(timeline) if timeline == "mz_epoch_ms" => Timeline::EpochMilliseconds,
        Some(timeline) if timeline.starts_with("mz_") => {
            return Err(PlanError::UnacceptableTimelineName(timeline));
        }
        Some(timeline) => Timeline::User(timeline),
    };

    let source = Source {
        create_sql,
        data_source: DataSourceDesc::Ingestion(Ingestion {
            desc: source_desc,
            // Currently no source reads from another source
            source_imports: BTreeSet::new(),
            subsource_exports,
            progress_subsource,
        }),
        desc,
    };

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        timeline,
        cluster_config,
    }))
}

generate_extracted_config!(
    CreateSubsourceOption,
    (Progress, bool, Default(false)),
    (References, bool, Default(false))
);

pub fn plan_create_subsource(
    scx: &StatementContext,
    stmt: CreateSubsourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSubsourceStatement {
        name,
        columns,
        constraints,
        if_not_exists,
        with_options,
    } = &stmt;

    let CreateSubsourceOptionExtracted {
        progress,
        references,
        ..
    } = with_options.clone().try_into()?;

    // This invariant is enforced during purification; we are responsible for
    // creating the AST for subsources as a response to CREATE SOURCE
    // statements, so this would fire in integration testing if we failed to
    // uphold it.
    assert!(
        progress ^ references,
        "CREATE SUBSOURCE statement must specify either PROGRESS or REFERENCES option"
    );

    let names: Vec<_> = columns
        .iter()
        .map(|c| normalize::column_name(c.name.clone()))
        .collect();

    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    // Build initial relation type that handles declared data types
    // and NOT NULL constraints.
    let mut column_types = Vec::with_capacity(columns.len());
    let mut keys = Vec::new();

    for (i, c) in columns.into_iter().enumerate() {
        let aug_data_type = &c.data_type;
        let ty = query::scalar_type_from_sql(scx, aug_data_type)?;
        let mut nullable = true;
        for option in &c.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(_) => {
                    bail_unsupported!("Subsources cannot have default values")
                }
                ColumnOption::Unique { is_primary } => {
                    keys.push(vec![i]);
                    if *is_primary {
                        nullable = false;
                    }
                }
                other => {
                    bail_unsupported!(format!(
                        "CREATE SUBSOURCE with column constraint: {}",
                        other
                    ))
                }
            }
        }
        column_types.push(ty.nullable(nullable));
    }

    let mut seen_primary = false;
    'c: for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary,
                nulls_not_distinct,
            } => {
                if seen_primary && *is_primary {
                    sql_bail!(
                        "multiple primary keys for source {} are not allowed",
                        name.to_ast_string_stable()
                    );
                }
                seen_primary = *is_primary || seen_primary;

                let mut key = vec![];
                for column in columns {
                    let column = normalize::column_name(column.clone());
                    match names.iter().position(|name| *name == column) {
                        None => sql_bail!("unknown column in constraint: {}", column),
                        Some(i) => {
                            let nullable = &mut column_types[i].nullable;
                            if *is_primary {
                                if *nulls_not_distinct {
                                    sql_bail!(
                                        "[internal error] PRIMARY KEY does not support NULLS NOT DISTINCT"
                                    );
                                }
                                *nullable = false;
                            } else if !(*nulls_not_distinct || !*nullable) {
                                // Non-primary key unique constraints are only keys if all of their
                                // columns are `NOT NULL` or the constraint is `NULLS NOT DISTINCT`.
                                break 'c;
                            }

                            key.push(i);
                        }
                    }
                }

                if *is_primary {
                    keys.insert(0, key);
                } else {
                    keys.push(key);
                }
            }
            TableConstraint::ForeignKey { .. } => {
                bail_unsupported!("CREATE SUBSOURCE with a foreign key")
            }
            TableConstraint::Check { .. } => {
                bail_unsupported!("CREATE SUBSOURCE with a check constraint")
            }
        }
    }

    let if_not_exists = *if_not_exists;
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name.clone())?)?;
    let create_sql = normalize::create_statement(scx, Statement::CreateSubsource(stmt))?;

    let typ = RelationType::new(column_types).with_keys(keys);
    let desc = RelationDesc::new(typ, names);

    let source = Source {
        create_sql,
        data_source: if progress {
            DataSourceDesc::Progress
        } else if references {
            DataSourceDesc::Source
        } else {
            unreachable!("state prohibited above")
        },
        desc,
    };

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        timeline: Timeline::EpochMilliseconds,
        cluster_config: SourceSinkClusterConfig::Undefined,
    }))
}

generate_extracted_config!(
    LoadGeneratorOption,
    (TickInterval, Interval),
    (ScaleFactor, f64),
    (MaxCardinality, u64)
);

pub(crate) fn load_generator_ast_to_generator(
    loadgen: &mz_sql_parser::ast::LoadGenerator,
    options: &[LoadGeneratorOption<Aug>],
) -> Result<
    (
        LoadGenerator,
        Option<BTreeMap<FullItemName, (usize, RelationDesc)>>,
    ),
    PlanError,
> {
    let load_generator = match loadgen {
        mz_sql_parser::ast::LoadGenerator::Auction => LoadGenerator::Auction,
        mz_sql_parser::ast::LoadGenerator::Counter => {
            let LoadGeneratorOptionExtracted {
                max_cardinality, ..
            } = options.to_vec().try_into()?;
            LoadGenerator::Counter { max_cardinality }
        }
        mz_sql_parser::ast::LoadGenerator::Marketing => LoadGenerator::Marketing,
        mz_sql_parser::ast::LoadGenerator::Datums => LoadGenerator::Datums,
        mz_sql_parser::ast::LoadGenerator::Tpch => {
            let LoadGeneratorOptionExtracted { scale_factor, .. } = options.to_vec().try_into()?;

            // Default to 0.01 scale factor (=10MB).
            let sf: f64 = scale_factor.unwrap_or(0.01);
            if !sf.is_finite() || sf < 0.0 {
                sql_bail!("unsupported scale factor {sf}");
            }

            let f_to_i = |multiplier: f64| -> Result<i64, PlanError> {
                let total = (sf * multiplier).floor();
                let mut i = i64::try_cast_from(total)
                    .ok_or_else(|| sql_err!("unsupported scale factor {sf}"))?;
                if i < 1 {
                    i = 1;
                }
                Ok(i)
            };

            // The multiplications here are safely unchecked because they will
            // overflow to infinity, which will be caught by f64_to_i64.
            let count_supplier = f_to_i(10_000f64)?;
            let count_part = f_to_i(200_000f64)?;
            let count_customer = f_to_i(150_000f64)?;
            let count_orders = f_to_i(150_000f64 * 10f64)?;
            let count_clerk = f_to_i(1_000f64)?;

            LoadGenerator::Tpch {
                count_supplier,
                count_part,
                count_customer,
                count_orders,
                count_clerk,
            }
        }
    };

    let mut available_subsources = BTreeMap::new();
    for (i, (name, desc)) in load_generator.views().iter().enumerate() {
        let name = FullItemName {
            database: RawDatabaseSpecifier::Name("mz_load_generators".to_owned()),
            schema: match load_generator {
                LoadGenerator::Counter { .. } => "counter".into(),
                LoadGenerator::Marketing => "marketing".into(),
                LoadGenerator::Auction => "auction".into(),
                LoadGenerator::Datums => "datums".into(),
                LoadGenerator::Tpch { .. } => "tpch".into(),
                // Please use `snake_case` for any multi-word load generators
                // that you add.
            },
            item: name.to_string(),
        };
        // The zero-th output is the main output
        // TODO(petrosagg): these plus ones are an accident waiting to happen. Find a way
        // to handle the main source and the subsources uniformly
        available_subsources.insert(name, (i + 1, desc.clone()));
    }
    let available_subsources = if available_subsources.is_empty() {
        None
    } else {
        Some(available_subsources)
    };

    Ok((load_generator, available_subsources))
}

fn typecheck_debezium(value_desc: &RelationDesc) -> Result<(Option<usize>, usize), PlanError> {
    let before = value_desc.get_by_name(&"before".into());
    let (after_idx, after_ty) = value_desc
        .get_by_name(&"after".into())
        .ok_or_else(|| sql_err!("'after' column missing from debezium input"))?;
    let before_idx = if let Some((before_idx, before_ty)) = before {
        if !matches!(before_ty.scalar_type, ScalarType::Record { .. }) {
            sql_bail!("'before' column must be of type record");
        }
        if before_ty != after_ty {
            sql_bail!("'before' type differs from 'after' column");
        }
        Some(before_idx)
    } else {
        None
    };
    Ok((before_idx, after_idx))
}

fn get_encoding(
    scx: &StatementContext,
    format: &CreateSourceFormat<Aug>,
    envelope: &Envelope,
    connection: Option<&CreateSourceConnection<Aug>>,
) -> Result<SourceDataEncoding<ReferencedConnection>, PlanError> {
    let encoding = match format {
        CreateSourceFormat::None => sql_bail!("Source format must be specified"),
        CreateSourceFormat::Bare(format) => get_encoding_inner(scx, format)?,
        CreateSourceFormat::KeyValue { key, value } => {
            let key = match get_encoding_inner(scx, key)? {
                SourceDataEncodingInner::Single(key) => key,
                SourceDataEncodingInner::KeyValue { key, .. } => key,
            };
            let value = match get_encoding_inner(scx, value)? {
                SourceDataEncodingInner::Single(value) => value,
                SourceDataEncodingInner::KeyValue { value, .. } => value,
            };
            SourceDataEncodingInner::KeyValue { key, value }
        }
    };

    let force_nullable_keys = matches!(connection, Some(CreateSourceConnection::Kafka(_)))
        && matches!(envelope, Envelope::None);
    let encoding = encoding.into_source_data_encoding(force_nullable_keys);

    let requires_keyvalue = matches!(
        envelope,
        Envelope::Debezium(DbzMode::Plain) | Envelope::Upsert
    );
    let is_keyvalue = matches!(encoding, SourceDataEncoding::KeyValue { .. });
    if requires_keyvalue && !is_keyvalue {
        sql_bail!("ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified");
    };

    Ok(encoding)
}

fn source_sink_cluster_config(
    scx: &StatementContext,
    ty: &'static str,
    in_cluster: Option<&ResolvedClusterName>,
    size: Option<String>,
) -> Result<SourceSinkClusterConfig, PlanError> {
    match (in_cluster, size) {
        (None, None) => Ok(SourceSinkClusterConfig::Undefined),
        (Some(in_cluster), None) => {
            ensure_cluster_can_host_storage_item(scx, in_cluster.id, ty)?;

            // We also don't allow more objects to be added to a cluster that is already
            // linked to another object.
            ensure_cluster_is_not_linked(scx, in_cluster.id)?;

            Ok(SourceSinkClusterConfig::Existing { id: in_cluster.id })
        }
        (None, Some(size)) => Ok(SourceSinkClusterConfig::Linked { size }),
        _ => sql_bail!("only one of IN CLUSTER or SIZE can be set"),
    }
}

generate_extracted_config!(AvroSchemaOption, (ConfluentWireFormat, bool, Default(true)));

#[derive(Debug)]
pub struct Schema {
    pub key_schema: Option<String>,
    pub value_schema: String,
    pub csr_connection: Option<mz_storage_types::connections::CsrConnection<ReferencedConnection>>,
    pub confluent_wire_format: bool,
}

fn get_encoding_inner(
    scx: &StatementContext,
    format: &Format<Aug>,
) -> Result<SourceDataEncodingInner<ReferencedConnection>, PlanError> {
    // Avro/CSR can return a `SourceDataEncoding::KeyValue`
    Ok(SourceDataEncodingInner::Single(match format {
        Format::Bytes => DataEncodingInner::Bytes,
        Format::Avro(schema) => {
            let Schema {
                key_schema,
                value_schema,
                csr_connection,
                confluent_wire_format,
            } = match schema {
                // TODO(jldlaughlin): we need a way to pass in primary key information
                // when building a source from a string or file.
                AvroSchema::InlineSchema {
                    schema: mz_sql_parser::ast::Schema { schema },
                    with_options,
                } => {
                    let AvroSchemaOptionExtracted {
                        confluent_wire_format,
                        ..
                    } = with_options.clone().try_into()?;

                    Schema {
                        key_schema: None,
                        value_schema: schema.clone(),
                        csr_connection: None,
                        confluent_wire_format,
                    }
                }
                AvroSchema::Csr {
                    csr_connection:
                        CsrConnectionAvro {
                            connection,
                            seed,
                            key_strategy: _,
                            value_strategy: _,
                        },
                } => {
                    let item = scx.get_item_by_resolved_name(&connection.connection)?;
                    let csr_connection = match item.connection()? {
                        Connection::Csr(connection) => connection.clone(),
                        _ => {
                            sql_bail!(
                                "{} is not a schema registry connection",
                                scx.catalog
                                    .resolve_full_name(item.name())
                                    .to_string()
                                    .quoted()
                            )
                        }
                    };

                    if let Some(seed) = seed {
                        Schema {
                            key_schema: seed.key_schema.clone(),
                            value_schema: seed.value_schema.clone(),
                            csr_connection: Some(csr_connection),
                            confluent_wire_format: true,
                        }
                    } else {
                        unreachable!("CSR seed resolution should already have been called: Avro")
                    }
                }
            };

            if let Some(key_schema) = key_schema {
                return Ok(SourceDataEncodingInner::KeyValue {
                    key: DataEncodingInner::Avro(AvroEncoding {
                        schema: key_schema,
                        csr_connection: csr_connection.clone(),
                        confluent_wire_format,
                    }),
                    value: DataEncodingInner::Avro(AvroEncoding {
                        schema: value_schema,
                        csr_connection,
                        confluent_wire_format,
                    }),
                });
            } else {
                DataEncodingInner::Avro(AvroEncoding {
                    schema: value_schema,
                    csr_connection,
                    confluent_wire_format,
                })
            }
        }
        Format::Protobuf(schema) => match schema {
            ProtobufSchema::Csr {
                csr_connection:
                    CsrConnectionProtobuf {
                        connection:
                            CsrConnection {
                                connection,
                                options,
                            },
                        seed,
                    },
            } => {
                if let Some(CsrSeedProtobuf { key, value }) = seed {
                    let item = scx.get_item_by_resolved_name(connection)?;
                    let _ = match item.connection()? {
                        Connection::Csr(connection) => connection,
                        _ => {
                            sql_bail!(
                                "{} is not a schema registry connection",
                                scx.catalog
                                    .resolve_full_name(item.name())
                                    .to_string()
                                    .quoted()
                            )
                        }
                    };

                    if !options.is_empty() {
                        sql_bail!("Protobuf CSR connections do not support any options");
                    }

                    let value = DataEncodingInner::Protobuf(ProtobufEncoding {
                        descriptors: strconv::parse_bytes(&value.schema)?,
                        message_name: value.message_name.clone(),
                        confluent_wire_format: true,
                    });
                    if let Some(key) = key {
                        return Ok(SourceDataEncodingInner::KeyValue {
                            key: DataEncodingInner::Protobuf(ProtobufEncoding {
                                descriptors: strconv::parse_bytes(&key.schema)?,
                                message_name: key.message_name.clone(),
                                confluent_wire_format: true,
                            }),
                            value,
                        });
                    }
                    value
                } else {
                    unreachable!("CSR seed resolution should already have been called: Proto")
                }
            }
            ProtobufSchema::InlineSchema {
                message_name,
                schema: mz_sql_parser::ast::Schema { schema },
            } => {
                let descriptors = strconv::parse_bytes(schema)?;

                DataEncodingInner::Protobuf(ProtobufEncoding {
                    descriptors,
                    message_name: message_name.to_owned(),
                    confluent_wire_format: false,
                })
            }
        },
        Format::Regex(regex) => DataEncodingInner::Regex(RegexEncoding {
            regex: mz_repr::adt::regex::Regex::new(regex.clone(), false)
                .map_err(|e| sql_err!("parsing regex: {e}"))?,
        }),
        Format::Csv { columns, delimiter } => {
            let columns = match columns {
                CsvColumns::Header { names } => {
                    if names.is_empty() {
                        sql_bail!("[internal error] column spec should get names in purify")
                    }
                    ColumnSpec::Header {
                        names: names.iter().cloned().map(|n| n.into_string()).collect(),
                    }
                }
                CsvColumns::Count(n) => ColumnSpec::Count(usize::cast_from(*n)),
            };
            DataEncodingInner::Csv(CsvEncoding {
                columns,
                delimiter: u8::try_from(*delimiter)
                    .map_err(|_| sql_err!("CSV delimiter must be an ASCII character"))?,
            })
        }
        Format::Json => DataEncodingInner::Json,
        Format::Text => DataEncodingInner::Text,
    }))
}

/// Extract the key envelope, if it is requested
fn get_key_envelope(
    included_items: &[SourceIncludeMetadata],
    envelope: &Envelope,
    encoding: &SourceDataEncoding<ReferencedConnection>,
) -> Result<KeyEnvelope, PlanError> {
    let key_definition = included_items
        .iter()
        .find(|i| i.ty == SourceIncludeMetadataType::Key);
    if matches!(envelope, Envelope::Debezium { .. }) && key_definition.is_some() {
        sql_bail!(
            "Cannot use INCLUDE KEY with ENVELOPE DEBEZIUM: Debezium values include all keys."
        );
    }
    if let Some(kd) = key_definition {
        match (&kd.alias, encoding) {
            (Some(name), SourceDataEncoding::KeyValue { .. }) => {
                Ok(KeyEnvelope::Named(name.as_str().to_string()))
            }
            (None, SourceDataEncoding::KeyValue { key, .. }) => get_unnamed_key_envelope(key),
            (_, SourceDataEncoding::Single(_)) => {
                // `kd.alias` == `None` means `INCLUDE KEY`
                // `kd.alias` == `Some(_) means INCLUDE KEY AS ___`
                // These both make sense with the same error message
                sql_bail!(
                    "INCLUDE KEY requires specifying KEY FORMAT .. VALUE FORMAT, \
                        got bare FORMAT"
                );
            }
        }
    } else {
        Ok(KeyEnvelope::None)
    }
}

/// Gets the key envelope for a given key encoding when no name for the key has
/// been requested by the user.
fn get_unnamed_key_envelope(
    key: &DataEncoding<ReferencedConnection>,
) -> Result<KeyEnvelope, PlanError> {
    // If the key is requested but comes from an unnamed type then it gets the name "key"
    //
    // Otherwise it gets the names of the columns in the type
    let is_composite = match key.inner {
        DataEncodingInner::RowCodec(_) => {
            sql_bail!("{} sources cannot use INCLUDE KEY", key.op_name())
        }
        DataEncodingInner::Bytes | DataEncodingInner::Json | DataEncodingInner::Text => false,
        DataEncodingInner::Avro(_)
        | DataEncodingInner::Csv(_)
        | DataEncodingInner::Protobuf(_)
        | DataEncodingInner::Regex { .. } => true,
    };

    if is_composite {
        Ok(KeyEnvelope::Flattened)
    } else {
        Ok(KeyEnvelope::Named("key".to_string()))
    }
}

pub fn describe_create_view(
    _: &StatementContext,
    _: CreateViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_view(
    scx: &StatementContext,
    def: &mut ViewDefinition<Aug>,
    params: &Params,
    temporary: bool,
) -> Result<(QualifiedItemName, View), PlanError> {
    let create_sql = normalize::create_statement(
        scx,
        Statement::CreateView(CreateViewStatement {
            if_exists: IfExistsBehavior::Error,
            temporary,
            definition: def.clone(),
        }),
    )?;

    let ViewDefinition {
        name,
        columns,
        query,
    } = def;

    let query::PlannedQuery {
        mut expr,
        mut desc,
        finishing,
        scope: _,
    } = query::plan_root_query(scx, query.clone(), QueryLifetime::View)?;
    // We get back a trivial finishing, because `plan_root_query` applies the given finishing.
    // Note: Earlier, we were thinking to maybe persist the finishing information with the view
    // here to help with materialize#724. However, in the meantime, there might be a better
    // approach to solve materialize#724:
    // https://github.com/MaterializeInc/materialize/issues/724#issuecomment-1688293709
    assert!(finishing.is_trivial(expr.arity()));

    expr.bind_parameters(params)?;

    let name = if temporary {
        scx.allocate_temporary_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    } else {
        scx.allocate_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    };

    plan_utils::maybe_rename_columns(
        format!("view {}", scx.catalog.resolve_full_name(&name)),
        &mut desc,
        columns,
    )?;
    let names: Vec<ColumnName> = desc.iter_names().cloned().collect();

    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    let view = View {
        create_sql,
        expr,
        column_names: names,
        temporary,
    };

    Ok((name, view))
}

pub fn plan_create_view(
    scx: &StatementContext,
    mut stmt: CreateViewStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let CreateViewStatement {
        temporary,
        if_exists,
        definition,
    } = &mut stmt;
    let (name, view) = plan_view(scx, definition, params, *temporary)?;

    let replace = if *if_exists == IfExistsBehavior::Replace {
        let if_exists = true;
        let cascade = false;
        if let Some(id) = plan_drop_item(
            scx,
            ObjectType::View,
            if_exists,
            definition.name.clone(),
            cascade,
        )? {
            if view.expr.depends_on().contains(&id) {
                let item = scx.catalog.get_item(&id);
                sql_bail!(
                    "cannot replace view {0}: depended upon by new {0} definition",
                    scx.catalog.resolve_full_name(item.name())
                );
            }
            Some(id)
        } else {
            None
        }
    } else {
        None
    };
    let drop_ids = replace
        .map(|id| {
            scx.catalog
                .item_dependents(id)
                .into_iter()
                .map(|id| id.unwrap_item_id())
                .collect()
        })
        .unwrap_or_default();

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (IfExistsBehavior::Error, Ok(item)) =
        (*if_exists, scx.catalog.resolve_item(&partial_name))
    {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    Ok(Plan::CreateView(CreateViewPlan {
        name,
        view,
        replace,
        drop_ids,
        if_not_exists: *if_exists == IfExistsBehavior::Skip,
        ambiguous_columns: *scx.ambiguous_columns.borrow(),
    }))
}

pub fn describe_create_materialized_view(
    _: &StatementContext,
    _: CreateMaterializedViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_materialized_view(
    scx: &StatementContext,
    mut stmt: CreateMaterializedViewStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let cluster_id = match &stmt.in_cluster {
        None => scx.resolve_cluster(None)?.id(),
        Some(in_cluster) => in_cluster.id,
    };
    stmt.in_cluster = Some(ResolvedClusterName {
        id: cluster_id,
        print_name: None,
    });

    let create_sql =
        normalize::create_statement(scx, Statement::CreateMaterializedView(stmt.clone()))?;

    let partial_name = normalize::unresolved_item_name(stmt.name)?;
    let name = scx.allocate_qualified_name(partial_name.clone())?;

    if !scx
        .catalog
        .is_system_schema_specifier(&name.qualifiers.schema_spec)
    {
        ensure_cluster_is_not_linked(scx, cluster_id)?;
    }

    let query::PlannedQuery {
        mut expr,
        mut desc,
        finishing,
        scope: _,
    } = query::plan_root_query(scx, stmt.query, QueryLifetime::MaterializedView)?;
    // We get back a trivial finishing, see comment in `plan_view`.
    assert!(finishing.is_trivial(expr.arity()));

    expr.bind_parameters(params)?;

    plan_utils::maybe_rename_columns(
        format!("materialized view {}", scx.catalog.resolve_full_name(&name)),
        &mut desc,
        &stmt.columns,
    )?;
    let column_names: Vec<ColumnName> = desc.iter_names().cloned().collect();
    if !stmt.non_null_assertions.is_empty() {
        scx.require_feature_flag(&crate::session::vars::ENABLE_ASSERT_NOT_NULL)?;
    }
    let mut non_null_assertions = stmt
        .non_null_assertions
        .into_iter()
        .map(normalize::column_name)
        .map(|assertion_name| {
            column_names
                .iter()
                .position(|col| col == &assertion_name)
                .ok_or_else(|| {
                    sql_err!(
                        "column {} in ASSERT NOT NULL option not found",
                        assertion_name.as_str().quoted()
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    non_null_assertions.sort();
    if let Some(dup) = non_null_assertions.iter().duplicates().next() {
        let dup = &column_names[*dup];
        sql_bail!(
            "duplicate column {} in non-null assertions",
            dup.as_str().quoted()
        );
    }

    if let Some(dup) = column_names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    let mut replace = None;
    let mut if_not_exists = false;
    match stmt.if_exists {
        IfExistsBehavior::Replace => {
            let if_exists = true;
            let cascade = false;
            let replace_id = plan_drop_item(
                scx,
                ObjectType::MaterializedView,
                if_exists,
                partial_name.into(),
                cascade,
            )?;
            if let Some(id) = replace_id {
                if expr.depends_on().contains(&id) {
                    let item = scx.catalog.get_item(&id);
                    sql_bail!(
                        "cannot replace materialized view {0}: depended upon by new {0} definition",
                        scx.catalog.resolve_full_name(item.name())
                    );
                }
                replace = Some(id);
            }
        }
        IfExistsBehavior::Skip => if_not_exists = true,
        IfExistsBehavior::Error => (),
    }
    let drop_ids = replace
        .map(|id| {
            scx.catalog
                .item_dependents(id)
                .into_iter()
                .map(|id| id.unwrap_item_id())
                .collect()
        })
        .unwrap_or_default();

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (IfExistsBehavior::Error, Ok(item)) =
        (stmt.if_exists, scx.catalog.resolve_item(&partial_name))
    {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    Ok(Plan::CreateMaterializedView(CreateMaterializedViewPlan {
        name,
        materialized_view: MaterializedView {
            create_sql,
            expr,
            column_names,
            cluster_id,
            non_null_assertions,
        },
        replace,
        drop_ids,
        if_not_exists,
        ambiguous_columns: *scx.ambiguous_columns.borrow(),
    }))
}

pub fn describe_create_sink(
    _: &StatementContext,
    _: CreateSinkStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(CreateSinkOption, (Size, String), (Snapshot, bool));

pub fn plan_create_sink(
    scx: &StatementContext,
    stmt: CreateSinkStatement<Aug>,
) -> Result<Plan, PlanError> {
    let create_sql = normalize::create_statement(scx, Statement::CreateSink(stmt.clone()))?;

    let CreateSinkStatement {
        name,
        in_cluster,
        from,
        connection,
        format,
        envelope,
        if_not_exists,
        with_options,
    } = stmt;

    const ALLOWED_WITH_OPTIONS: &[CreateSinkOptionName] =
        &[CreateSinkOptionName::Size, CreateSinkOptionName::Snapshot];

    if let Some(op) = with_options
        .iter()
        .find(|op| !ALLOWED_WITH_OPTIONS.contains(&op.name))
    {
        scx.require_feature_flag_w_dynamic_desc(
            &vars::ENABLE_CREATE_SOURCE_DENYLIST_WITH_OPTIONS,
            format!("CREATE SINK...WITH ({}..)", op.name.to_ast_string()),
            format!(
                "permitted options are {}",
                comma_separated(ALLOWED_WITH_OPTIONS)
            ),
        )?;
    }

    let envelope = match envelope {
        None => sql_bail!("ENVELOPE clause is required"),
        Some(Envelope::Debezium(mz_sql_parser::ast::DbzMode::Plain)) => SinkEnvelope::Debezium,
        Some(Envelope::Upsert) => SinkEnvelope::Upsert,
        Some(Envelope::CdcV2) => bail_unsupported!("CDCv2 sinks"),
        Some(Envelope::None) => bail_unsupported!("\"ENVELOPE NONE\" sinks"),
    };
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }
    let from_name = &from;
    let from = scx.get_item_by_resolved_name(&from)?;
    let desc = from.desc(&scx.catalog.resolve_full_name(from.name()))?;
    let key_indices = match &connection {
        CreateSinkConnection::Kafka { key, .. } => {
            if let Some(key) = key.clone() {
                let key_columns = key
                    .key_columns
                    .into_iter()
                    .map(normalize::column_name)
                    .collect::<Vec<_>>();
                let mut uniq = BTreeSet::new();
                for col in key_columns.iter() {
                    if !uniq.insert(col) {
                        sql_bail!("Repeated column name in sink key: {}", col);
                    }
                }
                let indices = key_columns
                    .iter()
                    .map(|col| -> anyhow::Result<usize> {
                        let name_idx = desc
                            .get_by_name(col)
                            .map(|(idx, _type)| idx)
                            .ok_or_else(|| sql_err!("No such column: {}", col))?;
                        if desc.get_unambiguous_name(name_idx).is_none() {
                            sql_err!("Ambiguous column: {}", col);
                        }
                        Ok(name_idx)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let is_valid_key =
                    desc.typ().keys.iter().any(|key_columns| {
                        key_columns.iter().all(|column| indices.contains(column))
                    });

                if !is_valid_key && envelope == SinkEnvelope::Upsert {
                    if key.not_enforced {
                        scx.catalog
                            .add_notice(PlanNotice::UpsertSinkKeyNotEnforced {
                                key: key_columns.clone(),
                                name: name.item.clone(),
                            })
                    } else {
                        return Err(PlanError::UpsertSinkWithInvalidKey {
                            name: from_name.full_name_str(),
                            desired_key: key_columns.iter().map(|c| c.to_string()).collect(),
                            valid_keys: desc
                                .typ()
                                .keys
                                .iter()
                                .map(|key| {
                                    key.iter()
                                        .map(|col| desc.get_name(*col).as_str().into())
                                        .collect()
                                })
                                .collect(),
                        });
                    }
                }
                Some(indices)
            } else {
                None
            }
        }
    };

    // pick the first valid natural relation key, if any
    let relation_key_indices = desc.typ().keys.get(0).cloned();

    let key_desc_and_indices = key_indices.map(|key_indices| {
        let cols = desc
            .iter()
            .map(|(name, ty)| (name.clone(), ty.clone()))
            .collect::<Vec<_>>();
        let (names, types): (Vec<_>, Vec<_>) =
            key_indices.iter().map(|&idx| cols[idx].clone()).unzip();
        let typ = RelationType::new(types);
        (RelationDesc::new(typ, names), key_indices)
    });

    if key_desc_and_indices.is_none() && envelope == SinkEnvelope::Upsert {
        return Err(PlanError::UpsertSinkWithoutKey);
    }

    let connection_builder = match connection {
        CreateSinkConnection::Kafka { connection, .. } => kafka_sink_builder(
            scx,
            connection,
            format,
            relation_key_indices,
            key_desc_and_indices,
            desc.into_owned(),
            envelope,
            from.id(),
        )?,
    };

    let CreateSinkOptionExtracted {
        size,
        snapshot,
        seen: _,
    } = with_options.try_into()?;

    let cluster_config = source_sink_cluster_config(scx, "sink", in_cluster.as_ref(), size)?;

    // WITH SNAPSHOT defaults to true
    let with_snapshot = snapshot.unwrap_or(true);

    Ok(Plan::CreateSink(CreateSinkPlan {
        name,
        sink: Sink {
            create_sql,
            from: from.id(),
            connection: connection_builder,
            envelope,
        },
        with_snapshot,
        if_not_exists,
        cluster_config,
    }))
}

fn key_constraint_err(desc: &RelationDesc, user_keys: &[ColumnName]) -> PlanError {
    let user_keys = user_keys.iter().map(|column| column.as_str()).join(", ");

    let existing_keys = desc
        .typ()
        .keys
        .iter()
        .map(|key_columns| {
            key_columns
                .iter()
                .map(|col| desc.get_name(*col).as_str())
                .join(", ")
        })
        .join(", ");

    sql_err!(
        "Key constraint ({}) conflicts with existing key ({})",
        user_keys,
        existing_keys
    )
}

/// Creating this by hand instead of using generate_extracted_config! macro
/// because the macro doesn't support parameterized enums. See <https://github.com/MaterializeInc/materialize/issues/22213>
#[derive(Debug, Default, PartialEq, Clone)]
pub struct CsrConfigOptionExtracted {
    seen: ::std::collections::BTreeSet<CsrConfigOptionName<Aug>>,
    pub(crate) avro_key_fullname: Option<String>,
    pub(crate) avro_value_fullname: Option<String>,
    pub(crate) null_defaults: bool,
    pub(crate) value_doc_options: BTreeMap<DocTarget, String>,
    pub(crate) key_doc_options: BTreeMap<DocTarget, String>,
}

impl std::convert::TryFrom<Vec<CsrConfigOption<Aug>>> for CsrConfigOptionExtracted {
    type Error = crate::plan::PlanError;
    fn try_from(v: Vec<CsrConfigOption<Aug>>) -> Result<CsrConfigOptionExtracted, Self::Error> {
        let mut extracted = CsrConfigOptionExtracted::default();
        let mut common_doc_comments = BTreeMap::new();
        for option in v {
            if !extracted.seen.insert(option.name.clone()) {
                return Err(PlanError::Unstructured({
                    format!("{} specified more than once", option.name)
                }));
            }
            let option_name = option.name.clone();
            let option_name_str = option_name.to_ast_string();
            let better_error = |e: PlanError| {
                PlanError::Unstructured(format!("invalid {}: {}", option_name.to_ast_string(), e))
            };
            match option.name {
                CsrConfigOptionName::AvroKeyFullname => {
                    extracted.avro_key_fullname =
                        <Option<String>>::try_from_value(option.value).map_err(better_error)?;
                }
                CsrConfigOptionName::AvroValueFullname => {
                    extracted.avro_value_fullname =
                        <Option<String>>::try_from_value(option.value).map_err(better_error)?;
                }
                CsrConfigOptionName::NullDefaults => {
                    extracted.null_defaults =
                        <bool>::try_from_value(option.value).map_err(better_error)?;
                }
                CsrConfigOptionName::AvroDocOn(doc_on) => {
                    let value = String::try_from_value(option.value.ok_or_else(|| {
                        PlanError::InvalidOptionValue {
                            option_name: option_name_str,
                            err: Box::new(PlanError::Unstructured("cannot be empty".to_string())),
                        }
                    })?)
                    .map_err(better_error)?;
                    let key = match doc_on.identifier {
                        DocOnIdentifier::Column(ResolvedColumnName::Column {
                            relation: ResolvedItemName::Item { id, .. },
                            name,
                            index: _,
                        }) => DocTarget::Field {
                            object_id: id,
                            column_name: name,
                        },
                        DocOnIdentifier::Type(ResolvedItemName::Item { id, .. }) => {
                            DocTarget::Type(id)
                        }
                        _ => unreachable!(),
                    };

                    match doc_on.for_schema {
                        DocOnSchema::KeyOnly => {
                            extracted.key_doc_options.insert(key, value);
                        }
                        DocOnSchema::ValueOnly => {
                            extracted.value_doc_options.insert(key, value);
                        }
                        DocOnSchema::All => {
                            common_doc_comments.insert(key, value);
                        }
                    }
                }
            }
        }

        for (key, value) in common_doc_comments {
            if !extracted.key_doc_options.contains_key(&key) {
                extracted.key_doc_options.insert(key.clone(), value.clone());
            }
            if !extracted.value_doc_options.contains_key(&key) {
                extracted.value_doc_options.insert(key, value);
            }
        }
        Ok(extracted)
    }
}

fn kafka_sink_builder(
    scx: &StatementContext,
    mz_sql_parser::ast::KafkaConnection {
        connection,
        options,
    }: mz_sql_parser::ast::KafkaConnection<Aug>,
    format: Option<Format<Aug>>,
    relation_key_indices: Option<Vec<usize>>,
    key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    value_desc: RelationDesc,
    envelope: SinkEnvelope,
    sink_from: GlobalId,
) -> Result<StorageSinkConnection<ReferencedConnection>, PlanError> {
    let item = scx.get_item_by_resolved_name(&connection)?;
    // Get Kafka connection
    let mut connection = match item.connection()? {
        Connection::Kafka(connection) => connection.clone(),
        _ => sql_bail!(
            "{} is not a kafka connection",
            scx.catalog.resolve_full_name(item.name())
        ),
    };

    // Starting offsets are allowed with feature flags mode, as they are a simple,
    // useful way to specify where to start reading a topic.
    const ALLOWED_OPTIONS: &[KafkaConfigOptionName] = &[KafkaConfigOptionName::Topic];

    if let Some(op) = options
        .iter()
        .find(|op| !ALLOWED_OPTIONS.contains(&op.name))
    {
        scx.require_feature_flag_w_dynamic_desc(
            &vars::ENABLE_KAFKA_CONFIG_DENYLIST_OPTIONS,
            format!("FROM KAFKA CONNECTION ({}...)", op.name.to_ast_string()),
            format!("permitted options are {}", comma_separated(ALLOWED_OPTIONS)),
        )?;
    }

    kafka_util::validate_options_for_context(&options, kafka_util::KafkaOptionCheckContext::Sink)?;

    let extracted_options: KafkaConfigOptionExtracted = options.try_into()?;

    for (k, v) in kafka_util::LibRdKafkaConfig::try_from(&extracted_options)?.0 {
        connection.options.insert(k, v);
    }

    let connection_id = item.id();
    let KafkaConfigOptionExtracted {
        topic,
        partition_count,
        replication_factor,
        retention_ms,
        retention_bytes,
        ..
    } = extracted_options;

    let topic_name = topic.ok_or_else(|| sql_err!("KAFKA CONNECTION must specify TOPIC"))?;

    let format = match format {
        Some(Format::Avro(AvroSchema::Csr {
            csr_connection:
                CsrConnectionAvro {
                    connection:
                        CsrConnection {
                            connection,
                            options,
                        },
                    seed,
                    key_strategy,
                    value_strategy,
                },
        })) => {
            if seed.is_some() {
                sql_bail!("SEED option does not make sense with sinks");
            }
            if key_strategy.is_some() {
                sql_bail!("KEY STRATEGY option does not make sense with sinks");
            }
            if value_strategy.is_some() {
                sql_bail!("VALUE STRATEGY option does not make sense with sinks");
            }

            let item = scx.get_item_by_resolved_name(&connection)?;
            let csr_connection = match item.connection()? {
                Connection::Csr(connection) => connection.clone(),
                _ => {
                    sql_bail!(
                        "{} is not a schema registry connection",
                        scx.catalog
                            .resolve_full_name(item.name())
                            .to_string()
                            .quoted()
                    )
                }
            };
            let CsrConfigOptionExtracted {
                avro_key_fullname,
                avro_value_fullname,
                null_defaults,
                key_doc_options,
                value_doc_options,
                ..
            } = options.try_into()?;

            if key_desc_and_indices.is_none() && avro_key_fullname.is_some() {
                sql_bail!("Cannot specify AVRO KEY FULLNAME without a corresponding KEY field");
            }

            if key_desc_and_indices.is_some()
                && (avro_key_fullname.is_some() ^ avro_value_fullname.is_some())
            {
                sql_bail!("Must specify both AVRO KEY FULLNAME and AVRO VALUE FULLNAME when specifying generated schema names");
            }

            if !value_doc_options.is_empty() || !key_doc_options.is_empty() {
                scx.require_feature_flag(&vars::ENABLE_SINK_DOC_ON_OPTION)?;
            }

            let options = AvroSchemaOptions {
                avro_key_fullname,
                avro_value_fullname,
                set_null_defaults: null_defaults,
                is_debezium: matches!(envelope, SinkEnvelope::Debezium),
                sink_from: Some(sink_from),
                value_doc_options,
                key_doc_options,
            };

            let schema_generator = AvroSchemaGenerator::new(
                key_desc_and_indices
                    .as_ref()
                    .map(|(desc, _indices)| desc.clone()),
                value_desc.clone(),
                options,
            )?;
            let value_schema = schema_generator.value_writer_schema().to_string();
            let key_schema = schema_generator
                .key_writer_schema()
                .map(|key_schema| key_schema.to_string());

            KafkaSinkFormat::Avro(KafkaSinkAvroFormatState::UnpublishedMaybe {
                key_schema,
                value_schema,
                csr_connection,
            })
        }
        Some(Format::Json) => KafkaSinkFormat::Json,
        Some(format) => bail_unsupported!(format!("sink format {:?}", format)),
        None => bail_unsupported!("sink without format"),
    };

    let consistency_config = KafkaConsistencyConfig::Progress {
        topic: connection.progress_topic.clone().unwrap_or_else(|| {
            scx.catalog
                .config()
                .default_kafka_sink_progress_topic(connection_id)
        }),
    };

    if partition_count == 0 || partition_count < -1 {
        sql_bail!(
            "PARTION COUNT for sink topics must be a positive integer or -1 for broker default"
        );
    }

    if replication_factor == 0 || replication_factor < -1 {
        sql_bail!(
            "REPLICATION FACTOR for sink topics must be a positive integer or -1 for broker default"
        );
    }

    if retention_ms.unwrap_or(0) < -1 {
        sql_bail!("RETENTION MS for sink topics must be greater than or equal to -1");
    }

    if retention_bytes.unwrap_or(0) < -1 {
        sql_bail!("RETENTION BYTES for sink topics must be greater than or equal to -1");
    }

    let retention = KafkaSinkConnectionRetention {
        duration: retention_ms,
        bytes: retention_bytes,
    };

    Ok(StorageSinkConnection::Kafka(KafkaSinkConnection {
        connection_id,
        connection,
        format,
        topic: topic_name,
        consistency_config,
        partition_count,
        replication_factor,
        fuel: 10000,
        relation_key_indices,
        key_desc_and_indices,
        value_desc,
        retention,
    }))
}

pub fn describe_create_index(
    _: &StatementContext,
    _: CreateIndexStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_index(
    scx: &StatementContext,
    mut stmt: CreateIndexStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateIndexStatement {
        name,
        on_name,
        in_cluster,
        key_parts,
        with_options,
        if_not_exists,
    } = &mut stmt;
    let on = scx.get_item_by_resolved_name(on_name)?;

    if CatalogItemType::View != on.item_type()
        && CatalogItemType::MaterializedView != on.item_type()
        && CatalogItemType::Source != on.item_type()
        && CatalogItemType::Table != on.item_type()
    {
        sql_bail!(
            "index cannot be created on {} because it is a {}",
            on_name.full_name_str(),
            on.item_type()
        )
    }

    let on_desc = on.desc(&scx.catalog.resolve_full_name(on.name()))?;

    let filled_key_parts = match key_parts {
        Some(kp) => kp.to_vec(),
        None => {
            // `key_parts` is None if we're creating a "default" index.
            on.desc(&scx.catalog.resolve_full_name(on.name()))?
                .typ()
                .default_key()
                .iter()
                .map(|i| match on_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![Ident::new(n.to_string())]),
                    _ => Expr::Value(Value::Number((i + 1).to_string())),
                })
                .collect()
        }
    };
    let keys = query::plan_index_exprs(scx, &on_desc, filled_key_parts.clone())?;

    let index_name = if let Some(name) = name {
        QualifiedItemName {
            qualifiers: on.name().qualifiers.clone(),
            item: normalize::ident(name.clone()),
        }
    } else {
        let mut idx_name = QualifiedItemName {
            qualifiers: on.name().qualifiers.clone(),
            item: on.name().item.clone(),
        };
        if key_parts.is_none() {
            // We're trying to create the "default" index.
            idx_name.item += "_primary_idx";
        } else {
            // Use PG schema for automatically naming indexes:
            // `<table>_<_-separated indexed expressions>_idx`
            let index_name_col_suffix = keys
                .iter()
                .map(|k| match k {
                    mz_expr::MirScalarExpr::Column(i) => match on_desc.get_unambiguous_name(*i) {
                        Some(col_name) => col_name.to_string(),
                        None => format!("{}", i + 1),
                    },
                    _ => "expr".to_string(),
                })
                .join("_");
            write!(idx_name.item, "_{index_name_col_suffix}_idx")
                .expect("write on strings cannot fail");
            idx_name.item = normalize::ident(Ident::new(&idx_name.item))
        }

        if !*if_not_exists {
            scx.catalog.find_available_name(idx_name)
        } else {
            idx_name
        }
    };

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&index_name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (*if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    let options = plan_index_options(scx, with_options.clone())?;
    let cluster_id = match in_cluster {
        None => scx.resolve_cluster(None)?.id(),
        Some(in_cluster) => in_cluster.id,
    };
    if !scx
        .catalog
        .is_system_schema_specifier(&index_name.qualifiers.schema_spec)
    {
        ensure_cluster_is_not_linked(scx, cluster_id)?;
    }
    *in_cluster = Some(ResolvedClusterName {
        id: cluster_id,
        print_name: None,
    });

    // Normalize `stmt`.
    *name = Some(Ident::new(index_name.item.clone()));
    *key_parts = Some(filled_key_parts);
    let if_not_exists = *if_not_exists;
    if let ResolvedItemName::Item { print_id, .. } = &mut stmt.on_name {
        *print_id = false;
    }
    let create_sql = normalize::create_statement(scx, Statement::CreateIndex(stmt))?;

    Ok(Plan::CreateIndex(CreateIndexPlan {
        name: index_name,
        index: Index {
            create_sql,
            on: on.id(),
            keys,
            cluster_id,
        },
        options,
        if_not_exists,
    }))
}

pub fn describe_create_type(
    _: &StatementContext,
    _: CreateTypeStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_type(
    scx: &StatementContext,
    stmt: CreateTypeStatement<Aug>,
) -> Result<Plan, PlanError> {
    let create_sql = normalize::create_statement(scx, Statement::CreateType(stmt.clone()))?;
    let CreateTypeStatement { name, as_type, .. } = stmt;

    fn validate_data_type(
        scx: &StatementContext,
        data_type: ResolvedDataType,
        as_type: &str,
        key: &str,
    ) -> Result<(GlobalId, Vec<i64>), PlanError> {
        let (id, modifiers) = match data_type {
            ResolvedDataType::Named { id, modifiers, .. } => (id, modifiers),
            _ => sql_bail!(
                "CREATE TYPE ... AS {}option {} can only use named data types, but \
                        found unnamed data type {}. Use CREATE TYPE to create a named type first",
                as_type,
                key,
                data_type.human_readable_name(),
            ),
        };

        let item = scx.catalog.get_item(&id);
        match item.type_details() {
            None => sql_bail!(
                "{} must be of class type, but received {} which is of class {}",
                key,
                scx.catalog.resolve_full_name(item.name()),
                item.item_type()
            ),
            Some(CatalogTypeDetails {
                typ: CatalogType::Char,
                ..
            }) => {
                bail_unsupported!("embedding char type in a list or map")
            }
            _ => {
                // Validate that the modifiers are actually valid.
                scalar_type_from_catalog(scx.catalog, id, &modifiers)?;

                Ok((id, modifiers))
            }
        }
    }

    let inner = match as_type {
        CreateTypeAs::List { options } => {
            let CreateTypeListOptionExtracted {
                element_type,
                seen: _,
            } = CreateTypeListOptionExtracted::try_from(options)?;
            let element_type =
                element_type.ok_or_else(|| sql_err!("ELEMENT TYPE option is required"))?;
            let (id, modifiers) = validate_data_type(scx, element_type, "LIST ", "ELEMENT TYPE")?;
            CatalogType::List {
                element_reference: id,
                element_modifiers: modifiers,
            }
        }
        CreateTypeAs::Map { options } => {
            let CreateTypeMapOptionExtracted {
                key_type,
                value_type,
                seen: _,
            } = CreateTypeMapOptionExtracted::try_from(options)?;
            let key_type = key_type.ok_or_else(|| sql_err!("KEY TYPE option is required"))?;
            let value_type = value_type.ok_or_else(|| sql_err!("VALUE TYPE option is required"))?;
            let (key_id, key_modifiers) = validate_data_type(scx, key_type, "MAP ", "KEY TYPE")?;
            let (value_id, value_modifiers) =
                validate_data_type(scx, value_type, "MAP ", "VALUE TYPE")?;
            CatalogType::Map {
                key_reference: key_id,
                key_modifiers,
                value_reference: value_id,
                value_modifiers,
            }
        }
        CreateTypeAs::Record { column_defs } => {
            let mut fields = vec![];
            for column_def in column_defs {
                let data_type = column_def.data_type;
                let key = ident(column_def.name.clone());
                let (id, modifiers) = validate_data_type(scx, data_type, "", &key)?;
                fields.push(CatalogRecordField {
                    name: ColumnName::from(key.clone()),
                    type_reference: id,
                    type_modifiers: modifiers,
                });
            }
            CatalogType::Record { fields }
        }
    };

    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let Ok(item) = scx.catalog.resolve_item(&partial_name) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    Ok(Plan::CreateType(CreateTypePlan {
        name,
        typ: Type { create_sql, inner },
    }))
}

generate_extracted_config!(CreateTypeListOption, (ElementType, ResolvedDataType));

generate_extracted_config!(
    CreateTypeMapOption,
    (KeyType, ResolvedDataType),
    (ValueType, ResolvedDataType)
);

#[derive(Debug)]
pub enum PlannedAlterRoleOption {
    Attributes(PlannedRoleAttributes),
    Variable(PlannedRoleVariable),
}

#[derive(Debug)]
pub struct PlannedRoleAttributes {
    pub inherit: Option<bool>,
}

fn plan_role_attributes(options: Vec<RoleAttribute>) -> Result<PlannedRoleAttributes, PlanError> {
    let mut planned_attributes = PlannedRoleAttributes { inherit: None };

    for option in options {
        match option {
            RoleAttribute::Login | RoleAttribute::NoLogin => {
                bail_never_supported!("LOGIN attribute", "sql/create-role/#details");
            }
            RoleAttribute::SuperUser | RoleAttribute::NoSuperUser => {
                bail_never_supported!("SUPERUSER attribute", "sql/create-role/#details");
            }
            RoleAttribute::Inherit | RoleAttribute::NoInherit
                if planned_attributes.inherit.is_some() =>
            {
                sql_bail!("conflicting or redundant options");
            }
            RoleAttribute::CreateCluster | RoleAttribute::NoCreateCluster => {
                bail_never_supported!(
                    "CREATECLUSTER attribute",
                    "sql/create-role/#details",
                    "Use system privileges instead."
                );
            }
            RoleAttribute::CreateDB | RoleAttribute::NoCreateDB => {
                bail_never_supported!(
                    "CREATEDB attribute",
                    "sql/create-role/#details",
                    "Use system privileges instead."
                );
            }
            RoleAttribute::CreateRole | RoleAttribute::NoCreateRole => {
                bail_never_supported!(
                    "CREATEROLE attribute",
                    "sql/create-role/#details",
                    "Use system privileges instead."
                );
            }

            RoleAttribute::Inherit => planned_attributes.inherit = Some(true),
            RoleAttribute::NoInherit => planned_attributes.inherit = Some(false),
        }
    }
    if planned_attributes.inherit == Some(false) {
        bail_unsupported!("non inherit roles");
    }

    Ok(planned_attributes)
}

#[derive(Debug)]
pub enum PlannedRoleVariable {
    Set { name: String, value: VariableValue },
    Reset { name: String },
}

impl PlannedRoleVariable {
    pub fn name(&self) -> &str {
        match self {
            PlannedRoleVariable::Set { name, .. } => name,
            PlannedRoleVariable::Reset { name } => name,
        }
    }
}

fn plan_role_variable(variable: SetRoleVar) -> Result<PlannedRoleVariable, PlanError> {
    let plan = match variable {
        SetRoleVar::Set { name, value } => PlannedRoleVariable::Set {
            name: name.to_string(),
            value: scl::plan_set_variable_to(value)?,
        },
        SetRoleVar::Reset { name } => PlannedRoleVariable::Reset {
            name: name.to_string(),
        },
    };
    Ok(plan)
}

pub fn describe_create_role(
    _: &StatementContext,
    _: CreateRoleStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_role(
    _: &StatementContext,
    CreateRoleStatement { name, options }: CreateRoleStatement,
) -> Result<Plan, PlanError> {
    let attributes = plan_role_attributes(options)?;
    Ok(Plan::CreateRole(CreateRolePlan {
        name: normalize::ident(name),
        attributes: attributes.into(),
    }))
}

pub fn describe_create_cluster(
    _: &StatementContext,
    _: CreateClusterStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(
    ClusterOption,
    (AvailabilityZones, Vec<String>),
    (Disk, bool),
    (IdleArrangementMergeEffort, u32),
    (IntrospectionDebugging, bool),
    (IntrospectionInterval, OptionalInterval),
    (Managed, bool),
    (Replicas, Vec<ReplicaDefinition<Aug>>),
    (ReplicationFactor, u32),
    (Size, String)
);

pub fn plan_create_cluster(
    scx: &StatementContext,
    CreateClusterStatement { name, options }: CreateClusterStatement<Aug>,
) -> Result<Plan, PlanError> {
    let ClusterOptionExtracted {
        availability_zones,
        idle_arrangement_merge_effort,
        introspection_debugging,
        introspection_interval,
        managed,
        replicas,
        replication_factor,
        seen: _,
        size,
        disk,
    }: ClusterOptionExtracted = options.try_into()?;

    let managed = managed.unwrap_or_else(|| replicas.is_none());

    if managed {
        if replicas.is_some() {
            sql_bail!("REPLICAS not supported for managed clusters");
        }
        let Some(size) = size else {
            sql_bail!("SIZE must be specified for managed clusters");
        };

        let compute = plan_compute_replica_config(
            introspection_interval,
            introspection_debugging.unwrap_or(false),
            idle_arrangement_merge_effort,
        )?;

        let replication_factor = replication_factor.unwrap_or(1);
        let availability_zones = availability_zones.unwrap_or_default();

        if !availability_zones.is_empty() {
            scx.require_feature_flag(&vars::ENABLE_MANAGED_CLUSTER_AVAILABILITY_ZONES)?;
        }

        let disk_default = scx.catalog.system_vars().disk_cluster_replicas_default();
        let disk = disk.unwrap_or(disk_default);
        if disk {
            scx.require_feature_flag(&vars::ENABLE_DISK_CLUSTER_REPLICAS)?;
        }

        Ok(Plan::CreateCluster(CreateClusterPlan {
            name: normalize::ident(name),
            variant: CreateClusterVariant::Managed(CreateClusterManagedPlan {
                replication_factor,
                size,
                availability_zones,
                compute,
                disk,
            }),
        }))
    } else {
        let Some(replica_defs) = replicas else {
            sql_bail!("REPLICAS must be specified for unmanaged clusters");
        };
        if availability_zones.is_some() {
            sql_bail!("AVAILABILITY ZONES not supported for unmanaged clusters");
        }
        if replication_factor.is_some() {
            sql_bail!("REPLICATION FACTOR not supported for unmanaged clusters");
        }
        if idle_arrangement_merge_effort.is_some() {
            sql_bail!("IDLE ARRANGEMENT MERGE EFFORT not supported for unmanaged clusters");
        }
        if introspection_debugging.is_some() {
            sql_bail!("INTROSPECTION DEBUGGING not supported for unmanaged clusters");
        }
        if introspection_interval.is_some() {
            sql_bail!("INTROSPECTION INTERVAL not supported for unmanaged clusters");
        }
        if size.is_some() {
            sql_bail!("SIZE not supported for unmanaged clusters");
        }
        if disk.is_some() {
            sql_bail!("DISK not supported for unmanaged clusters");
        }
        let mut replicas = vec![];
        for ReplicaDefinition { name, options } in replica_defs {
            replicas.push((normalize::ident(name), plan_replica_config(scx, options)?));
        }

        Ok(Plan::CreateCluster(CreateClusterPlan {
            name: normalize::ident(name),
            variant: CreateClusterVariant::Unmanaged(CreateClusterUnmanagedPlan { replicas }),
        }))
    }
}

const DEFAULT_REPLICA_INTROSPECTION_INTERVAL: Interval = Interval {
    micros: cast::u32_to_i64(DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS),
    months: 0,
    days: 0,
};

generate_extracted_config!(
    ReplicaOption,
    (AvailabilityZone, String),
    (BilledAs, String),
    (ComputeAddresses, Vec<String>),
    (ComputectlAddresses, Vec<String>),
    (Disk, bool),
    (IdleArrangementMergeEffort, u32),
    (Internal, bool, Default(false)),
    (IntrospectionDebugging, bool, Default(false)),
    (IntrospectionInterval, OptionalInterval),
    (Size, String),
    (StorageAddresses, Vec<String>),
    (StoragectlAddresses, Vec<String>),
    (Workers, u16)
);

fn plan_replica_config(
    scx: &StatementContext,
    options: Vec<ReplicaOption<Aug>>,
) -> Result<ReplicaConfig, PlanError> {
    let ReplicaOptionExtracted {
        availability_zone,
        billed_as,
        compute_addresses,
        computectl_addresses,
        disk,
        idle_arrangement_merge_effort,
        internal,
        introspection_debugging,
        introspection_interval,
        size,
        storage_addresses,
        storagectl_addresses,
        workers,
        ..
    }: ReplicaOptionExtracted = options.try_into()?;

    let compute = plan_compute_replica_config(
        introspection_interval,
        introspection_debugging,
        idle_arrangement_merge_effort,
    )?;

    if disk.is_some() {
        scx.require_feature_flag(&vars::ENABLE_DISK_CLUSTER_REPLICAS)?;
    }

    match (
        size,
        availability_zone,
        billed_as,
        storagectl_addresses,
        storage_addresses,
        computectl_addresses,
        compute_addresses,
        workers,
    ) {
        // Common cases we expect end users to hit.
        (None, _, None, None, None, None, None, None) => {
            // We don't mention the unmanaged options in the error message
            // because they are only available in unsafe mode.
            sql_bail!("SIZE option must be specified");
        }
        (Some(size), availability_zone, billed_as, None, None, None, None, None) => {
            let disk_default = scx.catalog.system_vars().disk_cluster_replicas_default();
            let disk = disk.unwrap_or(disk_default);

            Ok(ReplicaConfig::Managed {
                size,
                availability_zone,
                compute,
                disk,
                billed_as,
                internal,
            })
        }

        (
            None,
            None,
            None,
            storagectl_addresses,
            storage_addresses,
            computectl_addresses,
            compute_addresses,
            workers,
        ) => {
            scx.require_feature_flag(&vars::ENABLE_UNMANAGED_CLUSTER_REPLICAS)?;

            // When manually testing Materialize in unsafe mode, it's easy to
            // accidentally omit one of these options, so we try to produce
            // helpful error messages.
            let Some(storagectl_addrs) = storagectl_addresses else {
                sql_bail!("missing STORAGECTL ADDRESSES option");
            };
            let Some(storage_addrs) = storage_addresses else {
                sql_bail!("missing STORAGE ADDRESSES option");
            };
            let Some(computectl_addrs) = computectl_addresses else {
                sql_bail!("missing COMPUTECTL ADDRESSES option");
            };
            let Some(compute_addrs) = compute_addresses else {
                sql_bail!("missing COMPUTE ADDRESSES option");
            };
            let workers = workers.unwrap_or(1);

            if computectl_addrs.len() != compute_addrs.len() {
                sql_bail!("COMPUTECTL ADDRESSES and COMPUTE ADDRESSES must have the same length");
            }
            if storagectl_addrs.len() != storage_addrs.len() {
                sql_bail!("STORAGECTL ADDRESSES and STORAGE ADDRESSES must have the same length");
            }
            if storagectl_addrs.len() != computectl_addrs.len() {
                sql_bail!(
                    "COMPUTECTL ADDRESSES and STORAGECTL ADDRESSES must have the same length"
                );
            }

            if workers == 0 {
                sql_bail!("WORKERS must be greater than 0");
            }

            if disk.is_some() {
                sql_bail!("DISK can't be specified for unmanaged clusters");
            }

            Ok(ReplicaConfig::Unmanaged {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers: workers.into(),
                compute,
            })
        }
        _ => {
            // We don't bother trying to produce a more helpful error message
            // here because no user is likely to hit this path.
            sql_bail!("invalid mixture of managed and unmanaged replica options");
        }
    }
}

fn plan_compute_replica_config(
    introspection_interval: Option<OptionalInterval>,
    introspection_debugging: bool,
    idle_arrangement_merge_effort: Option<u32>,
) -> Result<ComputeReplicaConfig, PlanError> {
    let introspection_interval = introspection_interval
        .map(|OptionalInterval(i)| i)
        .unwrap_or(Some(DEFAULT_REPLICA_INTROSPECTION_INTERVAL));
    let introspection = match introspection_interval {
        Some(interval) => Some(ComputeReplicaIntrospectionConfig {
            interval: interval.duration()?,
            debugging: introspection_debugging,
        }),
        None if introspection_debugging => {
            sql_bail!("INTROSPECTION DEBUGGING cannot be specified without INTROSPECTION INTERVAL")
        }
        None => None,
    };
    let compute = ComputeReplicaConfig {
        introspection,
        idle_arrangement_merge_effort,
    };
    Ok(compute)
}

pub fn describe_create_cluster_replica(
    _: &StatementContext,
    _: CreateClusterReplicaStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_cluster_replica(
    scx: &StatementContext,
    CreateClusterReplicaStatement {
        definition: ReplicaDefinition { name, options },
        of_cluster,
    }: CreateClusterReplicaStatement<Aug>,
) -> Result<Plan, PlanError> {
    let cluster = scx
        .catalog
        .resolve_cluster(Some(&normalize::ident(of_cluster)))?;
    if is_storage_cluster(scx, cluster) && cluster.replica_ids().len() > 0 {
        sql_bail!("cannot create more than one replica of a cluster containing sources or sinks");
    }
    ensure_cluster_is_not_linked(scx, cluster.id())?;

    let config = plan_replica_config(scx, options)?;

    if let ReplicaConfig::Managed { internal: true, .. } = &config {
        if MANAGED_REPLICA_PATTERN.is_match(name.as_str()) {
            return Err(PlanError::MangedReplicaName(name.into_string()));
        }
    } else {
        ensure_cluster_is_not_managed(scx, cluster.id())?;
    }

    Ok(Plan::CreateClusterReplica(CreateClusterReplicaPlan {
        name: normalize::ident(name),
        cluster_id: cluster.id(),
        config,
    }))
}

pub fn describe_create_secret(
    _: &StatementContext,
    _: CreateSecretStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_secret(
    scx: &StatementContext,
    stmt: CreateSecretStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSecretStatement {
        name,
        if_not_exists,
        value,
    } = &stmt;

    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?;
    let mut create_sql_statement = stmt.clone();
    create_sql_statement.value = Expr::Value(Value::String("********".to_string()));
    let create_sql =
        normalize::create_statement(scx, Statement::CreateSecret(create_sql_statement))?;
    let secret_as = query::plan_secret_as(scx, value.clone())?;

    let secret = Secret {
        create_sql,
        secret_as,
    };

    Ok(Plan::CreateSecret(CreateSecretPlan {
        name,
        secret,
        if_not_exists: *if_not_exists,
    }))
}

pub fn describe_create_connection(
    _: &StatementContext,
    _: CreateConnectionStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(
    KafkaConnectionOption,
    (Broker, Vec<KafkaBroker<Aug>>),
    (Brokers, Vec<KafkaBroker<Aug>>),
    (ProgressTopic, String),
    (SshTunnel, with_options::Object),
    (SslKey, with_options::Secret),
    (SslCertificate, StringOrSecret),
    (SslCertificateAuthority, StringOrSecret),
    (SaslMechanisms, String),
    (SaslUsername, StringOrSecret),
    (SaslPassword, with_options::Secret)
);

impl KafkaConnectionOptionExtracted {
    pub fn get_brokers(
        &self,
        scx: &StatementContext,
    ) -> Result<Vec<mz_storage_types::connections::KafkaBroker<ReferencedConnection>>, PlanError>
    {
        let mut brokers = match (&self.broker, &self.brokers) {
            (Some(_), Some(_)) => sql_bail!("invalid CONNECTION: cannot set BROKER and BROKERS"),
            (None, None) => sql_bail!("invalid CONNECTION: must set either BROKER or BROKERS"),
            (Some(v), None) => v.to_vec(),
            (None, Some(v)) => v.to_vec(),
        };

        // NOTE: we allow broker configurations to be mixed and matched. If/when we support
        // a top-level `SSH TUNNEL` configuration, we will need additional assertions.

        let mut out = vec![];
        for broker in &mut brokers {
            if broker.address.contains(',') {
                sql_bail!("invalid CONNECTION: cannot specify multiple Kafka broker addresses in one string.\n\n
Instead, specify BROKERS using multiple strings, e.g. BROKERS ('kafka:9092', 'kafka:9093')");
            }

            let tunnel = match &broker.tunnel {
                KafkaBrokerTunnel::Direct => Tunnel::Direct,
                KafkaBrokerTunnel::AwsPrivatelink(aws_privatelink) => {
                    let KafkaBrokerAwsPrivatelinkOptionExtracted {
                        availability_zone,
                        port,
                        seen: _,
                    } = KafkaBrokerAwsPrivatelinkOptionExtracted::try_from(
                        aws_privatelink.options.clone(),
                    )?;

                    let id = match &aws_privatelink.connection {
                        ResolvedItemName::Item { id, .. } => id,
                        _ => sql_bail!(
                            "internal error: Kafka PrivateLink connection was not resolved"
                        ),
                    };
                    let entry = scx.catalog.get_item(id);
                    match entry.connection()? {
                        Connection::AwsPrivatelink(connection) => {
                            if let Some(az) = &availability_zone {
                                if !connection.availability_zones.contains(az) {
                                    sql_bail!("AWS PrivateLink availability zone {} does not match any of the \
                                      availability zones on the AWS PrivateLink connection {}",
                                      az.quoted(),
                                        scx.catalog.resolve_full_name(entry.name()).to_string().quoted())
                                }
                            }
                            Tunnel::AwsPrivatelink(AwsPrivatelink {
                                connection_id: *id,
                                availability_zone,
                                port,
                            })
                        }
                        _ => {
                            sql_bail!("{} is not an AWS PRIVATELINK connection", entry.name().item)
                        }
                    }
                }
                KafkaBrokerTunnel::SshTunnel(ssh) => {
                    let id = match &ssh {
                        ResolvedItemName::Item { id, .. } => id,
                        _ => sql_bail!(
                            "internal error: Kafka SSH tunnel connection was not resolved"
                        ),
                    };
                    let ssh_tunnel = scx.catalog.get_item(id);
                    match ssh_tunnel.connection()? {
                        Connection::Ssh(_connection) => Tunnel::Ssh(SshTunnel {
                            connection_id: *id,
                            connection: *id,
                        }),
                        _ => {
                            sql_bail!("{} is not an SSH connection", ssh_tunnel.name().item)
                        }
                    }
                }
            };

            out.push(mz_storage_types::connections::KafkaBroker {
                address: broker.address.clone(),
                tunnel,
            });
        }

        Ok(out)
    }
    pub fn ssl_config(&self) -> BTreeSet<KafkaConnectionOptionName> {
        use KafkaConnectionOptionName::*;
        BTreeSet::from([SslKey, SslCertificate])
    }
    pub fn sasl_config(&self) -> BTreeSet<KafkaConnectionOptionName> {
        use KafkaConnectionOptionName::*;
        BTreeSet::from([SaslMechanisms, SaslUsername, SaslPassword])
    }
}

impl From<&KafkaConnectionOptionExtracted> for Option<KafkaTlsConfig> {
    fn from(k: &KafkaConnectionOptionExtracted) -> Self {
        if k.ssl_config().iter().all(|config| k.seen.contains(config)) {
            Some(KafkaTlsConfig {
                identity: Some(TlsIdentity {
                    key: k.ssl_key.unwrap().into(),
                    cert: k.ssl_certificate.clone().unwrap(),
                }),
                root_cert: k.ssl_certificate_authority.clone(),
            })
        } else {
            None
        }
    }
}

impl TryFrom<&KafkaConnectionOptionExtracted> for Option<SaslConfig> {
    type Error = PlanError;
    fn try_from(k: &KafkaConnectionOptionExtracted) -> Result<Self, Self::Error> {
        let res = if k.sasl_config().iter().all(|config| k.seen.contains(config)) {
            // librdkafka requires SASL mechanisms to be upper case (PLAIN,
            // SCRAM-SHA-256). For usability, we automatically uppercase the
            // mechanism that user provides. This avoids a frustrating
            // interaction with identifier case folding. Consider `SASL
            // MECHANISMS = PLAIN`. Identifier case folding results in a SASL
            // mechanism of `plain` (note the lowercase), which Materialize
            // previously rejected with an error of "SASL mechanism must be
            // uppercase." This was deeply frustarting for users who were not
            // familiar with identifier case folding rules. See #22205.
            let sasl_mechanism = k.sasl_mechanisms.clone().unwrap().to_uppercase();

            Some(SaslConfig {
                sasl_mechanism,
                username: k.sasl_username.clone().unwrap(),
                password: k.sasl_password.unwrap().into(),
                tls_root_cert: k.ssl_certificate_authority.clone(),
            })
        } else {
            None
        };
        Ok(res)
    }
}

impl TryFrom<&KafkaConnectionOptionExtracted> for Option<KafkaSecurity> {
    type Error = PlanError;
    fn try_from(value: &KafkaConnectionOptionExtracted) -> Result<Self, Self::Error> {
        let ssl_config = Option::<KafkaTlsConfig>::from(value).map(KafkaSecurity::from);
        let sasl_config = Option::<SaslConfig>::try_from(value)?.map(KafkaSecurity::from);

        let mut security_iter = vec![ssl_config, sasl_config].into_iter();
        let res = match security_iter.find(|v| v.is_some()) {
            Some(config) => {
                if security_iter.find(|v| v.is_some()).is_some() {
                    sql_bail!("invalid CONNECTION: cannot specify multiple security protocols");
                }
                config
            }
            None => None,
        };

        if res.is_none()
            && [value.sasl_config(), value.ssl_config()]
                .iter()
                .flatten()
                .any(|c| value.seen.contains(c))
        {
            sql_bail!("invalid CONNECTION: under-specified security configuration");
        }

        Ok(res)
    }
}

impl KafkaConnectionOptionExtracted {
    fn to_connection(
        self,
        scx: &StatementContext,
    ) -> Result<mz_storage_types::connections::KafkaConnection<ReferencedConnection>, PlanError>
    {
        if self.ssh_tunnel.is_some() {
            scx.require_feature_flag(&vars::ENABLE_DEFAULT_KAFKA_SSH_TUNNEL)?;
        }
        Ok(KafkaConnection {
            brokers: self.get_brokers(scx)?,
            default_tunnel: scx.build_tunnel_definition(self.ssh_tunnel, None)?,
            security: Option::<KafkaSecurity>::try_from(&self)?,
            progress_topic: self.progress_topic,
            options: BTreeMap::new(),
        })
    }
}

generate_extracted_config!(
    KafkaBrokerAwsPrivatelinkOption,
    (AvailabilityZone, String),
    (Port, u16)
);

generate_extracted_config!(
    CsrConnectionOption,
    (AwsPrivatelink, with_options::Object),
    (Port, u16),
    (Url, String),
    (SslKey, with_options::Secret),
    (SslCertificate, StringOrSecret),
    (SslCertificateAuthority, StringOrSecret),
    (Username, StringOrSecret),
    (Password, with_options::Secret),
    (SshTunnel, with_options::Object)
);

impl CsrConnectionOptionExtracted {
    fn to_connection(
        self,
        scx: &StatementContext,
    ) -> Result<mz_storage_types::connections::CsrConnection<ReferencedConnection>, PlanError> {
        let url: reqwest::Url = match self.url {
            Some(url) => url
                .parse()
                .map_err(|e| sql_err!("parsing schema registry url: {e}"))?,
            None => sql_bail!("invalid CONNECTION: must specify URL"),
        };
        let _ = url
            .host_str()
            .ok_or_else(|| sql_err!("invalid CONNECTION: URL must specify domain name"))?;
        if url.path() != "/" {
            sql_bail!("invalid CONNECTION: URL must have an empty path");
        }
        let cert = self.ssl_certificate;
        let key = self.ssl_key.map(|secret| secret.into());
        let tls_identity = match (cert, key) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some(TlsIdentity { cert, key }),
            _ => sql_bail!(
                "invalid CONNECTION: reading from SSL-auth Confluent Schema Registry requires both SSL KEY and SSL CERTIFICATE"
            ),
        };
        let http_auth = self.username.map(|username| CsrConnectionHttpAuth {
            username,
            password: self.password.map(|secret| secret.into()),
        });

        let tunnel = scx.build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?;

        Ok(mz_storage_types::connections::CsrConnection {
            url,
            tls_root_cert: self.ssl_certificate_authority,
            tls_identity,
            http_auth,
            tunnel,
        })
    }
}

generate_extracted_config!(
    PostgresConnectionOption,
    (AwsPrivatelink, with_options::Object),
    (Database, String),
    (Host, String),
    (Password, with_options::Secret),
    (Port, u16, Default(5432_u16)),
    (SshTunnel, with_options::Object),
    (SslCertificate, StringOrSecret),
    (SslCertificateAuthority, StringOrSecret),
    (SslKey, with_options::Secret),
    (SslMode, String),
    (User, StringOrSecret)
);

impl PostgresConnectionOptionExtracted {
    fn to_connection(
        self,
        scx: &StatementContext,
    ) -> Result<mz_storage_types::connections::PostgresConnection<ReferencedConnection>, PlanError>
    {
        let cert = self.ssl_certificate;
        let key = self.ssl_key.map(|secret| secret.into());
        let tls_identity = match (cert, key) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some(TlsIdentity { cert, key }),
            _ => sql_bail!("invalid CONNECTION: both SSL KEY and SSL CERTIFICATE are required"),
        };
        let tls_mode = match self.ssl_mode.as_ref().map(|m| m.as_str()) {
            None | Some("disable") => tokio_postgres::config::SslMode::Disable,
            // "prefer" intentionally omitted because it has dubious security
            // properties.
            Some("require") => tokio_postgres::config::SslMode::Require,
            Some("verify_ca") | Some("verify-ca") => tokio_postgres::config::SslMode::VerifyCa,
            Some("verify_full") | Some("verify-full") => {
                tokio_postgres::config::SslMode::VerifyFull
            }
            Some(m) => sql_bail!("invalid CONNECTION: unknown SSL MODE {}", m.quoted()),
        };

        let tunnel = scx.build_tunnel_definition(self.ssh_tunnel, self.aws_privatelink)?;

        Ok(mz_storage_types::connections::PostgresConnection {
            database: self
                .database
                .ok_or_else(|| sql_err!("DATABASE option is required"))?,
            password: self.password.map(|password| password.into()),
            host: self
                .host
                .ok_or_else(|| sql_err!("HOST option is required"))?,
            port: self.port,
            tunnel,
            tls_mode,
            tls_root_cert: self.ssl_certificate_authority,
            tls_identity,
            user: self
                .user
                .ok_or_else(|| sql_err!("USER option is required"))?,
        })
    }
}

generate_extracted_config!(
    SshConnectionOption,
    (Host, String),
    (Port, u16, Default(22_u16)),
    (User, String)
);

impl TryFrom<SshConnectionOptionExtracted> for mz_storage_types::connections::SshConnection {
    type Error = PlanError;

    fn try_from(options: SshConnectionOptionExtracted) -> Result<Self, Self::Error> {
        Ok(mz_storage_types::connections::SshConnection {
            host: options
                .host
                .ok_or_else(|| sql_err!("HOST option is required"))?,
            port: options.port,
            user: options
                .user
                .ok_or_else(|| sql_err!("USER option is required"))?,
            public_keys: None,
        })
    }
}

generate_extracted_config!(
    AwsConnectionOption,
    (AccessKeyId, StringOrSecret),
    (SecretAccessKey, with_options::Secret),
    (Token, StringOrSecret),
    (Endpoint, String),
    (Region, String),
    (RoleArn, String)
);

impl TryFrom<AwsConnectionOptionExtracted> for AwsConfig {
    type Error = PlanError;

    fn try_from(options: AwsConnectionOptionExtracted) -> Result<Self, Self::Error> {
        Ok(AwsConfig {
            credentials: AwsCredentials {
                access_key_id: options
                    .access_key_id
                    .ok_or_else(|| sql_err!("ACCESS KEY ID option is required"))?,
                secret_access_key: options
                    .secret_access_key
                    .ok_or_else(|| sql_err!("SECRET ACCESS KEY option is required"))?
                    .into(),
                session_token: options.token,
            },
            endpoint: match options.endpoint {
                // TODO(benesch): this should not treat an empty endpoint as equivalent to a `NULL`
                // endpoint, but making that change now would break testdrive. AWS connections are
                // all behind feature flags mode right now, so no particular urgency to correct
                // this.
                Some(endpoint) if !endpoint.is_empty() => Some(endpoint),
                _ => None,
            },
            region: options.region,
            role: options.role_arn.map(|arn| AwsAssumeRole { arn }),
        })
    }
}

generate_extracted_config!(
    AwsPrivatelinkConnectionOption,
    (ServiceName, String),
    (AvailabilityZones, Vec<String>)
);

impl TryFrom<AwsPrivatelinkConnectionOptionExtracted> for AwsPrivatelinkConnection {
    type Error = PlanError;

    fn try_from(options: AwsPrivatelinkConnectionOptionExtracted) -> Result<Self, Self::Error> {
        Ok(AwsPrivatelinkConnection {
            service_name: options
                .service_name
                .ok_or_else(|| sql_err!("SERVICE NAME option is required"))?,
            availability_zones: options
                .availability_zones
                .ok_or_else(|| sql_err!("AVAILABILITY ZONES option is required"))?,
        })
    }
}

generate_extracted_config!(CreateConnectionOption, (Validate, bool));

pub fn plan_create_connection(
    scx: &StatementContext,
    stmt: CreateConnectionStatement<Aug>,
) -> Result<Plan, PlanError> {
    let create_sql = normalize::create_statement(scx, Statement::CreateConnection(stmt.clone()))?;
    let CreateConnectionStatement {
        name,
        connection,
        if_not_exists,
        with_options,
    } = stmt;
    let connection = match connection {
        CreateConnection::Kafka { options } => {
            let c = KafkaConnectionOptionExtracted::try_from(options)?;
            Connection::Kafka(c.to_connection(scx)?)
        }
        CreateConnection::Csr { options } => {
            let c = CsrConnectionOptionExtracted::try_from(options)?;
            Connection::Csr(c.to_connection(scx)?)
        }
        CreateConnection::Postgres { options } => {
            let c = PostgresConnectionOptionExtracted::try_from(options)?;
            Connection::Postgres(c.to_connection(scx)?)
        }
        CreateConnection::Aws { options } => {
            let c = AwsConnectionOptionExtracted::try_from(options)?;
            let connection = AwsConfig::try_from(c)?;
            Connection::Aws(connection)
        }
        CreateConnection::AwsPrivatelink { options } => {
            let c = AwsPrivatelinkConnectionOptionExtracted::try_from(options)?;
            let connection = AwsPrivatelinkConnection::try_from(c)?;
            if let Some(supported_azs) = scx.catalog.aws_privatelink_availability_zones() {
                for connection_az in &connection.availability_zones {
                    if !supported_azs.contains(connection_az) {
                        return Err(PlanError::InvalidPrivatelinkAvailabilityZone {
                            name: connection_az.to_string(),
                            supported_azs,
                        });
                    }
                }
            }
            Connection::AwsPrivatelink(connection)
        }
        CreateConnection::Ssh { options } => {
            let c = SshConnectionOptionExtracted::try_from(options)?;
            let connection = mz_storage_types::connections::SshConnection::try_from(c)?;
            Connection::Ssh(connection)
        }
    };
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;

    let options = CreateConnectionOptionExtracted::try_from(with_options)?;
    if options.validate.is_some() {
        scx.require_feature_flag(&vars::ENABLE_CONNECTION_VALIDATION_SYNTAX)?;
    }
    let validate = match options.validate {
        Some(val) => val,
        None => {
            scx.catalog
                .system_vars()
                .enable_default_connection_validation()
                && connection.validate_by_default()
        }
    };

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    let plan = CreateConnectionPlan {
        name,
        if_not_exists,
        connection: crate::plan::Connection {
            create_sql,
            connection,
        },
        validate,
    };
    Ok(Plan::CreateConnection(plan))
}

fn plan_drop_database(
    scx: &StatementContext,
    if_exists: bool,
    name: &UnresolvedDatabaseName,
    cascade: bool,
) -> Result<Option<DatabaseId>, PlanError> {
    Ok(match resolve_database(scx, name, if_exists)? {
        Some(database) => {
            if !cascade && database.has_schemas() {
                sql_bail!(
                    "database '{}' cannot be dropped with RESTRICT while it contains schemas",
                    name,
                );
            }
            Some(database.id())
        }
        None => None,
    })
}

pub fn describe_drop_objects(
    _: &StatementContext,
    _: DropObjectsStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_objects(
    scx: &mut StatementContext,
    DropObjectsStatement {
        object_type,
        if_exists,
        names,
        cascade,
    }: DropObjectsStatement,
) -> Result<Plan, PlanError> {
    assert_ne!(
        object_type,
        mz_sql_parser::ast::ObjectType::Func,
        "rejected in parser"
    );
    let object_type = object_type.into();

    let mut referenced_ids = Vec::new();
    for name in names {
        let id = match &name {
            UnresolvedObjectName::Cluster(name) => {
                plan_drop_cluster(scx, if_exists, name, cascade)?.map(ObjectId::Cluster)
            }
            UnresolvedObjectName::ClusterReplica(name) => {
                plan_drop_cluster_replica(scx, if_exists, name)?.map(ObjectId::ClusterReplica)
            }
            UnresolvedObjectName::Database(name) => {
                plan_drop_database(scx, if_exists, name, cascade)?.map(ObjectId::Database)
            }
            UnresolvedObjectName::Schema(name) => {
                plan_drop_schema(scx, if_exists, name, cascade)?.map(ObjectId::Schema)
            }
            UnresolvedObjectName::Role(name) => {
                plan_drop_role(scx, if_exists, name)?.map(ObjectId::Role)
            }
            UnresolvedObjectName::Item(name) => {
                plan_drop_item(scx, object_type, if_exists, name.clone(), cascade)?
                    .map(ObjectId::Item)
            }
        };
        match id {
            Some(id) => referenced_ids.push(id),
            None => scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            }),
        }
    }
    let drop_ids = scx.catalog.object_dependents(&referenced_ids);

    Ok(Plan::DropObjects(DropObjectsPlan {
        referenced_ids,
        drop_ids,
        object_type,
    }))
}

fn plan_drop_schema(
    scx: &StatementContext,
    if_exists: bool,
    name: &UnresolvedSchemaName,
    cascade: bool,
) -> Result<Option<(ResolvedDatabaseSpecifier, SchemaSpecifier)>, PlanError> {
    Ok(match resolve_schema(scx, name.clone(), if_exists)? {
        Some((database_spec, schema_spec)) => {
            if let ResolvedDatabaseSpecifier::Ambient = database_spec {
                sql_bail!(
                    "cannot drop schema {name} because it is required by the database system",
                );
            }
            if let SchemaSpecifier::Temporary = schema_spec {
                sql_bail!("cannot drop schema {name} because it is a temporary schema",)
            }
            let schema = scx.get_schema(&database_spec, &schema_spec);
            if !cascade && schema.has_items() {
                let full_schema_name = scx.catalog.resolve_full_schema_name(schema.name());
                sql_bail!(
                    "schema '{}' cannot be dropped without CASCADE while it contains objects",
                    full_schema_name
                );
            }
            Some((database_spec, schema_spec))
        }
        None => None,
    })
}

fn plan_drop_role(
    scx: &StatementContext,
    if_exists: bool,
    name: &Ident,
) -> Result<Option<RoleId>, PlanError> {
    match scx.catalog.resolve_role(name.as_str()) {
        Ok(role) => {
            let id = role.id();
            if &id == scx.catalog.active_role_id() {
                sql_bail!("current role cannot be dropped");
            }
            for role in scx.catalog.get_roles() {
                for (member_id, grantor_id) in role.membership() {
                    if &id == grantor_id {
                        let member_role = scx.catalog.get_role(member_id);
                        sql_bail!(
                            "cannot drop role {}: still depended up by membership of role {} in role {}",
                            name.as_str(), role.name(), member_role.name()
                        );
                    }
                }
            }
            Ok(Some(role.id()))
        }
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn plan_drop_cluster(
    scx: &StatementContext,
    if_exists: bool,
    name: &Ident,
    cascade: bool,
) -> Result<Option<ClusterId>, PlanError> {
    Ok(match resolve_cluster(scx, name, if_exists)? {
        Some(cluster) => {
            if !cascade && !cluster.bound_objects().is_empty() {
                return Err(PlanError::DependentObjectsStillExist {
                    object_type: "cluster".to_string(),
                    object_name: cluster.name().to_string(),
                    dependents: Vec::new(),
                });
            }
            ensure_cluster_is_not_linked(scx, cluster.id())?;
            Some(cluster.id())
        }
        None => None,
    })
}

/// Returns `true` if the cluster has any storage object. Return `false` if the cluster has no
/// objects.
fn is_storage_cluster(scx: &StatementContext, cluster: &dyn CatalogCluster) -> bool {
    cluster.bound_objects().iter().any(|id| {
        matches!(
            scx.catalog.get_item(id).item_type(),
            CatalogItemType::Source | CatalogItemType::Sink
        )
    })
}

/// Check that the cluster can host storage items.
fn ensure_cluster_can_host_storage_item(
    scx: &StatementContext,
    cluster_id: ClusterId,
    ty: &'static str,
) -> Result<(), PlanError> {
    let cluster = scx.catalog.get_cluster(cluster_id);
    // At most 1 replica
    if cluster.replica_ids().len() > 1 {
        sql_bail!("cannot create {ty} in cluster with more than one replica")
    }
    let enable_unified_cluster = scx.catalog.system_vars().enable_unified_clusters();
    let only_storage_objects = cluster.bound_objects().iter().all(|id| {
        matches!(
            scx.catalog.get_item(id).item_type(),
            CatalogItemType::Source | CatalogItemType::Sink
        )
    });
    // unified clusters or only storage objects on cluster
    if !enable_unified_cluster && !only_storage_objects {
        sql_bail!("cannot create {ty} in cluster containing indexes or materialized views");
    }
    Ok(())
}

fn plan_drop_cluster_replica(
    scx: &StatementContext,
    if_exists: bool,
    name: &QualifiedReplica,
) -> Result<Option<(ClusterId, ReplicaId)>, PlanError> {
    let cluster = resolve_cluster_replica(scx, name, if_exists)?;
    if let Some((cluster, _)) = &cluster {
        ensure_cluster_is_not_linked(scx, cluster.id())?;
    }
    Ok(cluster.map(|(cluster, replica_id)| (cluster.id(), replica_id)))
}

fn plan_drop_item(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: UnresolvedItemName,
    cascade: bool,
) -> Result<Option<GlobalId>, PlanError> {
    plan_drop_item_inner(scx, object_type, if_exists, name, cascade, false)
}

/// Like [`plan_drop_item`] but specialized for the case of dropping subsources
/// in response to `ALTER SOURCE...DROP SUBSOURCE...`
fn plan_drop_subsource(
    scx: &StatementContext,
    if_exists: bool,
    name: UnresolvedItemName,
    cascade: bool,
) -> Result<Option<GlobalId>, PlanError> {
    plan_drop_item_inner(scx, ObjectType::Source, if_exists, name, cascade, true)
}

fn plan_drop_item_inner(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: UnresolvedItemName,
    cascade: bool,
    allow_dropping_subsources: bool,
) -> Result<Option<GlobalId>, PlanError> {
    Ok(match resolve_item(scx, name, if_exists)? {
        Some(catalog_item) => {
            if catalog_item.id().is_system() {
                sql_bail!(
                    "cannot drop {} {} because it is required by the database system",
                    catalog_item.item_type(),
                    scx.catalog.minimal_qualification(catalog_item.name()),
                );
            }
            let item_type = catalog_item.item_type();

            // Return a more helpful error on `DROP VIEW <materialized-view>`.
            if object_type == ObjectType::View && item_type == CatalogItemType::MaterializedView {
                let name = scx
                    .catalog
                    .resolve_full_name(catalog_item.name())
                    .to_string();
                return Err(PlanError::DropViewOnMaterializedView(name));
            } else if object_type != item_type {
                sql_bail!(
                    "\"{}\" is a {} not a {}",
                    scx.catalog.resolve_full_name(catalog_item.name()),
                    catalog_item.item_type(),
                    format!("{object_type}").to_lowercase(),
                );
            }

            // Check if object is subsource if drop command doesn't allow dropping them, e.g. ALTER
            // SOURCE can drop subsources, but DROP SOURCE cannot.
            let primary_source = match item_type {
                CatalogItemType::Source => catalog_item
                    .used_by()
                    .iter()
                    .find(|id| scx.catalog.get_item(id).item_type() == CatalogItemType::Source),
                _ => None,
            };

            if let Some(source_id) = primary_source {
                // Progress collections can never get dropped independently.
                if Some(catalog_item.id()) == scx.catalog.get_item(source_id).progress_id() {
                    return Err(PlanError::DropProgressCollection {
                        progress_collection: scx
                            .catalog
                            .minimal_qualification(catalog_item.name())
                            .to_string(),
                        source: scx
                            .catalog
                            .minimal_qualification(scx.catalog.get_item(source_id).name())
                            .to_string(),
                    });
                }
                if !allow_dropping_subsources {
                    return Err(PlanError::DropSubsource {
                        subsource: scx
                            .catalog
                            .minimal_qualification(catalog_item.name())
                            .to_string(),
                        source: scx
                            .catalog
                            .minimal_qualification(scx.catalog.get_item(source_id).name())
                            .to_string(),
                    });
                }
            }

            if !cascade {
                let entry_id = catalog_item.id();
                // When this item gets dropped it will also drop its subsources, so we need to check the
                // users of those
                let mut dropped_items = catalog_item
                    .subsources()
                    .iter()
                    .map(|id| scx.catalog.get_item(id))
                    .collect_vec();
                dropped_items.push(catalog_item);

                for entry in dropped_items {
                    for id in entry.used_by() {
                        // The catalog_entry we're trying to drop will appear in the used_by list of
                        // its subsources so we need to exclude it from cascade checking since it
                        // will be dropped. Similarly, if we're dropping a subsource, the primary
                        // source will show up in its dependents but should not prevent the drop.
                        if id == &entry_id || Some(id) == primary_source {
                            continue;
                        }

                        let dep = scx.catalog.get_item(id);
                        if dependency_prevents_drop(object_type, dep) {
                            return Err(PlanError::DependentObjectsStillExist {
                                object_type: catalog_item.item_type().to_string(),
                                object_name: scx
                                    .catalog
                                    .minimal_qualification(catalog_item.name())
                                    .to_string(),
                                dependents: vec![(
                                    dep.item_type().to_string(),
                                    scx.catalog.minimal_qualification(dep.name()).to_string(),
                                )],
                            });
                        }
                    }
                    // TODO(jkosh44) It would be nice to also check if any active subscribe or pending peek
                    //  relies on entry. Unfortunately, we don't have that information readily available.
                }
            }
            Some(catalog_item.id())
        }
        None => None,
    })
}

/// Does the dependency `dep` prevent a drop of a non-cascade query?
fn dependency_prevents_drop(object_type: ObjectType, dep: &dyn CatalogItem) -> bool {
    match object_type {
        ObjectType::Type => true,
        _ => match dep.item_type() {
            CatalogItemType::Func
            | CatalogItemType::Table
            | CatalogItemType::Source
            | CatalogItemType::View
            | CatalogItemType::MaterializedView
            | CatalogItemType::Sink
            | CatalogItemType::Type
            | CatalogItemType::Secret
            | CatalogItemType::Connection => true,
            CatalogItemType::Index => false,
        },
    }
}

pub fn describe_alter_index_options(
    _: &StatementContext,
    _: AlterIndexStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn describe_drop_owned(
    _: &StatementContext,
    _: DropOwnedStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_owned(
    scx: &StatementContext,
    DropOwnedStatement {
        role_names,
        cascade,
    }: DropOwnedStatement<Aug>,
) -> Result<Plan, PlanError> {
    let role_ids: BTreeSet<_> = role_names.into_iter().map(|role| role.id).collect();
    let mut drop_ids = Vec::new();
    let mut privilege_revokes = Vec::new();
    let mut default_privilege_revokes = Vec::new();

    fn update_privilege_revokes(
        object_id: SystemObjectId,
        privileges: &PrivilegeMap,
        role_ids: &BTreeSet<RoleId>,
        privilege_revokes: &mut Vec<(SystemObjectId, MzAclItem)>,
    ) {
        privilege_revokes.extend(iter::zip(
            iter::repeat(object_id),
            privileges
                .all_values()
                .filter(|privilege| role_ids.contains(&privilege.grantee))
                .cloned(),
        ));
    }

    // Replicas
    for replica in scx.catalog.get_cluster_replicas() {
        let cluster = scx.catalog.get_cluster(replica.cluster_id());
        // We skip over linked cluster replicas because they will be added later when collecting
        // the dependencies of the linked object.
        if cluster.linked_object_id().is_none() && role_ids.contains(&replica.owner_id()) {
            drop_ids.push((replica.cluster_id(), replica.replica_id()).into());
        }
    }

    // Clusters
    for cluster in scx.catalog.get_clusters() {
        // We skip over linked clusters because they will be added later when collecting
        // the dependencies of the linked object.
        if cluster.linked_object_id().is_none() && role_ids.contains(&cluster.owner_id()) {
            // Note: CASCADE is not required for replicas.
            if !cascade {
                let non_owned_bound_objects: Vec<_> = cluster
                    .bound_objects()
                    .into_iter()
                    .map(|global_id| scx.catalog.get_item(global_id))
                    .filter(|item| !role_ids.contains(&item.owner_id()))
                    .collect();
                if !non_owned_bound_objects.is_empty() {
                    let names: Vec<_> = non_owned_bound_objects
                        .into_iter()
                        .map(|item| {
                            (
                                item.item_type().to_string(),
                                scx.catalog.resolve_full_name(item.name()).to_string(),
                            )
                        })
                        .collect();
                    return Err(PlanError::DependentObjectsStillExist {
                        object_type: "cluster".to_string(),
                        object_name: cluster.name().to_string(),
                        dependents: names,
                    });
                }
            }
            drop_ids.push(cluster.id().into());
        }
        update_privilege_revokes(
            SystemObjectId::Object(cluster.id().into()),
            cluster.privileges(),
            &role_ids,
            &mut privilege_revokes,
        );
    }

    // Items
    for item in scx.catalog.get_items() {
        if role_ids.contains(&item.owner_id()) {
            if !cascade {
                // When this item gets dropped it will also drop its subsources, so we need to
                // check the users of those.
                for sub_item in item
                    .subsources()
                    .iter()
                    .map(|id| scx.catalog.get_item(id))
                    .chain(iter::once(item))
                {
                    let non_owned_dependencies: Vec<_> = sub_item
                        .used_by()
                        .into_iter()
                        .map(|global_id| scx.catalog.get_item(global_id))
                        .filter(|item| dependency_prevents_drop(item.item_type().into(), *item))
                        .filter(|item| !role_ids.contains(&item.owner_id()))
                        .collect();
                    if !non_owned_dependencies.is_empty() {
                        let names: Vec<_> = non_owned_dependencies
                            .into_iter()
                            .map(|item| {
                                (
                                    item.item_type().to_string(),
                                    scx.catalog.resolve_full_name(item.name()).to_string(),
                                )
                            })
                            .collect();
                        return Err(PlanError::DependentObjectsStillExist {
                            object_type: item.item_type().to_string(),
                            object_name: scx
                                .catalog
                                .resolve_full_name(item.name())
                                .to_string()
                                .to_string(),
                            dependents: names,
                        });
                    }
                }
            }
            drop_ids.push(item.id().into());
        }
        update_privilege_revokes(
            SystemObjectId::Object(item.id().into()),
            item.privileges(),
            &role_ids,
            &mut privilege_revokes,
        );
    }

    // Schemas
    for schema in scx.catalog.get_schemas() {
        if !schema.id().is_temporary() {
            if role_ids.contains(&schema.owner_id()) {
                if !cascade {
                    let non_owned_dependencies: Vec<_> = schema
                        .item_ids()
                        .values()
                        .map(|global_id| scx.catalog.get_item(global_id))
                        .filter(|item| dependency_prevents_drop(item.item_type().into(), *item))
                        .filter(|item| !role_ids.contains(&item.owner_id()))
                        .collect();
                    if !non_owned_dependencies.is_empty() {
                        let full_schema_name = scx.catalog.resolve_full_schema_name(schema.name());
                        sql_bail!(
                            "schema {} cannot be dropped without CASCADE while it contains non-owned objects",
                            full_schema_name.to_string().quoted()
                        );
                    }
                }
                drop_ids.push((*schema.database(), *schema.id()).into())
            }
            update_privilege_revokes(
                SystemObjectId::Object((*schema.database(), *schema.id()).into()),
                schema.privileges(),
                &role_ids,
                &mut privilege_revokes,
            );
        }
    }

    // Databases
    for database in scx.catalog.get_databases() {
        if role_ids.contains(&database.owner_id()) {
            if !cascade {
                let non_owned_schemas: Vec<_> = database
                    .schemas()
                    .into_iter()
                    .filter(|schema| !role_ids.contains(&schema.owner_id()))
                    .collect();
                if !non_owned_schemas.is_empty() {
                    sql_bail!(
                        "database {} cannot be dropped without CASCADE while it contains non-owned schemas",
                        database.name().quoted(),
                    );
                }
            }
            drop_ids.push(database.id().into());
        }
        update_privilege_revokes(
            SystemObjectId::Object(database.id().into()),
            database.privileges(),
            &role_ids,
            &mut privilege_revokes,
        );
    }

    // System
    update_privilege_revokes(
        SystemObjectId::System,
        scx.catalog.get_system_privileges(),
        &role_ids,
        &mut privilege_revokes,
    );

    for (default_privilege_object, default_privilege_acl_items) in
        scx.catalog.get_default_privileges()
    {
        for default_privilege_acl_item in default_privilege_acl_items {
            if role_ids.contains(&default_privilege_object.role_id)
                || role_ids.contains(&default_privilege_acl_item.grantee)
            {
                default_privilege_revokes.push((
                    default_privilege_object.clone(),
                    default_privilege_acl_item.clone(),
                ));
            }
        }
    }

    let drop_ids = scx.catalog.object_dependents(&drop_ids);

    let system_ids: Vec<_> = drop_ids.iter().filter(|id| id.is_system()).collect();
    if !system_ids.is_empty() {
        let mut owners = system_ids
            .into_iter()
            .filter_map(|object_id| scx.catalog.get_owner_id(object_id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|role_id| scx.catalog.get_role(&role_id).name().quoted());
        sql_bail!(
            "cannot drop objects owned by role {} because they are required by the database system",
            owners.join(", "),
        );
    }

    Ok(Plan::DropOwned(DropOwnedPlan {
        role_ids: role_ids.into_iter().collect(),
        drop_ids,
        privilege_revokes,
        default_privilege_revokes,
    }))
}

generate_extracted_config!(IndexOption, (LogicalCompactionWindow, OptionalInterval));

fn plan_index_options(
    scx: &StatementContext,
    with_opts: Vec<IndexOption<Aug>>,
) -> Result<Vec<crate::plan::IndexOption>, PlanError> {
    if !with_opts.is_empty() {
        // Index options are not durable.
        scx.require_feature_flag(&vars::ENABLE_INDEX_OPTIONS)?;
    }

    let IndexOptionExtracted {
        logical_compaction_window,
        ..
    }: IndexOptionExtracted = with_opts.try_into()?;

    let mut out = Vec::with_capacity(1);

    if let Some(OptionalInterval(lcw)) = logical_compaction_window {
        scx.require_feature_flag(&vars::ENABLE_LOGICAL_COMPACTION_WINDOW)?;
        out.push(crate::plan::IndexOption::LogicalCompactionWindow(
            lcw.map(|interval| interval.duration()).transpose()?,
        ))
    }

    Ok(out)
}

pub fn plan_alter_index_options(
    scx: &mut StatementContext,
    AlterIndexStatement {
        index_name,
        if_exists,
        action: actions,
    }: AlterIndexStatement<Aug>,
) -> Result<Plan, PlanError> {
    let index_name = normalize::unresolved_item_name(index_name)?;
    let entry = match scx.catalog.resolve_item(&index_name) {
        Ok(index) => index,
        Err(_) if if_exists => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: index_name.to_string(),
                object_type: ObjectType::Index,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Index,
            }));
        }
        Err(e) => return Err(e.into()),
    };
    if entry.item_type() != CatalogItemType::Index {
        sql_bail!(
            "\"{}\" is a {} not a index",
            scx.catalog.resolve_full_name(entry.name()),
            entry.item_type()
        )
    }
    let id = entry.id();

    match actions {
        AlterIndexAction::ResetOptions(options) => {
            Ok(Plan::AlterIndexResetOptions(AlterIndexResetOptionsPlan {
                id,
                options: options.into_iter().collect(),
            }))
        }
        AlterIndexAction::SetOptions(options) => {
            Ok(Plan::AlterIndexSetOptions(AlterIndexSetOptionsPlan {
                id,
                options: plan_index_options(scx, options)?,
            }))
        }
    }
}

pub fn describe_alter_cluster_set_options(
    _: &StatementContext,
    _: AlterClusterStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_cluster(
    scx: &mut StatementContext,
    AlterClusterStatement {
        name,
        action,
        if_exists,
    }: AlterClusterStatement<Aug>,
) -> Result<Plan, PlanError> {
    let cluster = match resolve_cluster(scx, &name, if_exists)? {
        Some(entry) => entry,
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type: ObjectType::Cluster,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Cluster,
            }));
        }
    };

    // Prevent changes to linked clusters.
    ensure_cluster_is_not_linked(scx, cluster.id())?;

    let mut options: PlanClusterOption = Default::default();

    match action {
        AlterClusterAction::SetOptions(set_options) => {
            let ClusterOptionExtracted {
                availability_zones,
                idle_arrangement_merge_effort,
                introspection_debugging,
                introspection_interval,
                managed,
                replicas: replica_defs,
                replication_factor,
                seen: _,
                size,
                disk,
            }: ClusterOptionExtracted = set_options.try_into()?;

            match managed.unwrap_or_else(|| cluster.is_managed()) {
                true => {
                    if replica_defs.is_some() {
                        sql_bail!("REPLICAS not supported for managed clusters");
                    }
                    if let Some(replication_factor) = replication_factor {
                        if is_storage_cluster(scx, cluster) && replication_factor > 1 {
                            sql_bail!("cannot create more than one replica of a cluster containing sources or sinks");
                        }
                    }
                }
                false => {
                    if availability_zones.is_some() {
                        sql_bail!("AVAILABILITY ZONES not supported for unmanaged clusters");
                    }
                    if replication_factor.is_some() {
                        sql_bail!("REPLICATION FACTOR not supported for unmanaged clusters");
                    }
                    if idle_arrangement_merge_effort.is_some() {
                        sql_bail!(
                            "IDLE ARRANGEMENT MERGE EFFORT not supported for unmanaged clusters"
                        );
                    }
                    if introspection_debugging.is_some() {
                        sql_bail!("INTROSPECTION DEBUGGING not supported for unmanaged clusters");
                    }
                    if introspection_interval.is_some() {
                        sql_bail!("INTROSPECTION INTERVAL not supported for unmanaged clusters");
                    }
                    if size.is_some() {
                        sql_bail!("SIZE not supported for unmanaged clusters");
                    }
                    if disk.is_some() {
                        sql_bail!("DISK not supported for unmanaged clusters");
                    }
                }
            }

            let mut replicas = vec![];
            for ReplicaDefinition { name, options } in
                replica_defs.into_iter().flat_map(Vec::into_iter)
            {
                replicas.push((normalize::ident(name), plan_replica_config(scx, options)?));
            }

            if let Some(managed) = managed {
                options.managed = AlterOptionParameter::Set(managed);
            }
            if let Some(replication_factor) = replication_factor {
                options.replication_factor = AlterOptionParameter::Set(replication_factor);
            }
            if let Some(size) = size {
                options.size = AlterOptionParameter::Set(size);
            }
            if let Some(availability_zones) = availability_zones {
                options.availability_zones = AlterOptionParameter::Set(availability_zones);
            }
            if let Some(idle_arrangement_merge_effort) = idle_arrangement_merge_effort {
                options.idle_arrangement_merge_effort =
                    AlterOptionParameter::Set(idle_arrangement_merge_effort);
            }
            if let Some(introspection_debugging) = introspection_debugging {
                options.introspection_debugging =
                    AlterOptionParameter::Set(introspection_debugging);
            }
            if let Some(introspection_interval) = introspection_interval {
                options.introspection_interval = AlterOptionParameter::Set(introspection_interval);
            }
            if let Some(disk) = disk {
                if disk {
                    scx.require_feature_flag(&vars::ENABLE_DISK_CLUSTER_REPLICAS)?;
                }
                options.disk = AlterOptionParameter::Set(disk);
            }
            if !replicas.is_empty() {
                options.replicas = AlterOptionParameter::Set(replicas);
            }
        }
        AlterClusterAction::ResetOptions(reset_options) => {
            use AlterOptionParameter::Reset;
            use ClusterOptionName::*;
            for option in reset_options {
                match option {
                    AvailabilityZones => options.availability_zones = Reset,
                    Disk => options.disk = Reset,
                    IntrospectionInterval => options.introspection_interval = Reset,
                    IntrospectionDebugging => options.introspection_debugging = Reset,
                    IdleArrangementMergeEffort => options.idle_arrangement_merge_effort = Reset,
                    Managed => options.managed = Reset,
                    Replicas => options.replicas = Reset,
                    ReplicationFactor => options.replication_factor = Reset,
                    Size => options.size = Reset,
                }
            }
        }
    }
    Ok(Plan::AlterCluster(AlterClusterPlan {
        id: cluster.id(),
        name: cluster.name().to_string(),
        options,
    }))
}

pub fn describe_alter_set_cluster(
    _: &StatementContext,
    _: AlterSetClusterStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_item_set_cluster(
    scx: &StatementContext,
    AlterSetClusterStatement {
        if_exists,
        set_cluster: in_cluster_name,
        name,
        object_type,
    }: AlterSetClusterStatement<Aug>,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_ALTER_SET_CLUSTER)?;

    let object_type = object_type.into();

    // Prevent access to `SET CLUSTER` for unsupported objects.
    match object_type {
        ObjectType::MaterializedView => {}
        ObjectType::Index | ObjectType::Sink | ObjectType::Source => {
            bail_unsupported!(20841, format!("ALTER {object_type} SET CLUSTER"))
        }
        _ => {
            bail_never_supported!(
                format!("ALTER {object_type} SET CLUSTER"),
                "sql/alter-set-cluster/",
                format!("{object_type} has no associated cluster")
            )
        }
    }

    let in_cluster = scx.catalog.get_cluster(in_cluster_name.id);

    match resolve_item(scx, name.clone(), if_exists)? {
        Some(entry) => {
            let catalog_object_type: ObjectType = entry.item_type().into();
            if catalog_object_type != object_type {
                sql_bail!("Cannot modify {} as {object_type}", entry.item_type());
            }
            let current_cluster = entry.cluster_id();
            let Some(current_cluster) = current_cluster else {
                sql_bail!("No cluster associated with {name}");
            };

            if !scx
                .catalog
                .is_system_schema_specifier(&entry.name().qualifiers.schema_spec)
            {
                ensure_cluster_is_not_linked(scx, in_cluster.id())?;
            }

            if current_cluster == in_cluster.id() {
                Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
            } else {
                Ok(Plan::AlterSetCluster(AlterSetClusterPlan {
                    id: entry.id(),
                    set_cluster: in_cluster.id(),
                }))
            }
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn describe_alter_object_rename(
    _: &StatementContext,
    _: AlterObjectRenameStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_object_rename(
    scx: &mut StatementContext,
    AlterObjectRenameStatement {
        name,
        object_type,
        to_item_name,
        if_exists,
    }: AlterObjectRenameStatement,
) -> Result<Plan, PlanError> {
    let object_type = object_type.into();
    match (object_type, name) {
        (
            ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Table
            | ObjectType::Source
            | ObjectType::Index
            | ObjectType::Sink
            | ObjectType::Secret
            | ObjectType::Connection,
            UnresolvedObjectName::Item(name),
        ) => plan_alter_item_rename(scx, object_type, name, to_item_name, if_exists),
        (ObjectType::Cluster, UnresolvedObjectName::Cluster(name)) => {
            plan_alter_cluster_rename(scx, object_type, name, to_item_name, if_exists)
        }
        (ObjectType::ClusterReplica, UnresolvedObjectName::ClusterReplica(name)) => {
            plan_alter_cluster_replica_rename(scx, object_type, name, to_item_name, if_exists)
        }
        (ObjectType::Schema, UnresolvedObjectName::Schema(name)) => {
            plan_alter_schema_rename(scx, name, to_item_name, if_exists)
        }
        (object_type, name) => {
            unreachable!("parser set the wrong object type '{object_type:?}' for name {name:?}")
        }
    }
}

pub fn plan_alter_schema_rename(
    scx: &mut StatementContext,
    name: UnresolvedSchemaName,
    to_schema_name: Ident,
    if_exists: bool,
) -> Result<Plan, PlanError> {
    let Some((db_spec, schema_spec)) = resolve_schema(scx, name.clone(), if_exists)? else {
        let object_type = ObjectType::Schema;
        scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
            name: name.to_ast_string(),
            object_type,
        });
        return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
    };

    // Make sure the name is unique.
    if scx
        .resolve_schema_in_database(&db_spec, &to_schema_name)
        .is_ok()
    {
        return Err(PlanError::Catalog(CatalogError::SchemaAlreadyExists(
            to_schema_name.clone().into_string(),
        )));
    }

    // Prevent users from renaming system related schemas.
    let schema = scx.catalog.get_schema(&db_spec, &schema_spec);
    if schema.id().is_system() {
        bail_never_supported!(format!("renaming the {} schema", schema.name().schema))
    }

    Ok(Plan::AlterSchemaRename(AlterSchemaRenamePlan {
        cur_schema_spec: (db_spec, schema_spec),
        new_schema_name: to_schema_name.into_string(),
    }))
}

pub fn plan_alter_schema_swap<F>(
    scx: &mut StatementContext,
    name_a: UnresolvedSchemaName,
    name_b: Ident,
    gen_temp_suffix: F,
) -> Result<Plan, PlanError>
where
    F: Fn(&dyn Fn(&str) -> bool) -> Result<String, PlanError>,
{
    let schema_a = scx.resolve_schema(name_a.clone())?;

    let db_spec = schema_a.database().clone();
    if matches!(db_spec, ResolvedDatabaseSpecifier::Ambient) {
        sql_bail!("cannot swap schemas that are in the ambient database");
    };
    let schema_b = scx.resolve_schema_in_database(&db_spec, &name_b)?;

    // We cannot swap system schemas.
    if schema_a.id().is_system() || schema_b.id().is_system() {
        bail_never_supported!("swapping a system schema".to_string())
    }

    // Generate a temporary name we can swap schema_a to.
    //
    // 'check' returns if the temp schema name would be valid.
    let check = |temp_suffix: &str| {
        let temp_name = Ident::new(format!("mz_schema_swap_{temp_suffix}"));
        scx.resolve_schema_in_database(&db_spec, &temp_name)
            .is_err()
    };
    let temp_suffix = gen_temp_suffix(&check)?;
    let name_temp = format!("mz_schema_swap_{temp_suffix}");

    Ok(Plan::AlterSchemaSwap(AlterSchemaSwapPlan {
        schema_a_spec: (*schema_a.database(), *schema_a.id()),
        schema_a_name: schema_a.name().schema.to_string(),
        schema_b_spec: (*schema_b.database(), *schema_b.id()),
        schema_b_name: schema_b.name().schema.to_string(),
        name_temp,
    }))
}

pub fn plan_alter_item_rename(
    scx: &mut StatementContext,
    object_type: ObjectType,
    name: UnresolvedItemName,
    to_item_name: Ident,
    if_exists: bool,
) -> Result<Plan, PlanError> {
    match resolve_item(scx, name.clone(), if_exists)? {
        Some(entry) => {
            let full_name = scx.catalog.resolve_full_name(entry.name());
            let item_type = entry.item_type();

            // Return a more helpful error on `ALTER VIEW <materialized-view>`.
            if object_type == ObjectType::View && item_type == CatalogItemType::MaterializedView {
                return Err(PlanError::AlterViewOnMaterializedView(
                    full_name.to_string(),
                ));
            } else if object_type != item_type {
                sql_bail!(
                    "\"{}\" is a {} not a {}",
                    full_name,
                    entry.item_type(),
                    format!("{object_type}").to_lowercase()
                )
            }
            let proposed_name = QualifiedItemName {
                qualifiers: entry.name().qualifiers.clone(),
                item: to_item_name.clone().into_string(),
            };
            if scx.item_exists(&proposed_name) {
                sql_bail!("catalog item '{}' already exists", to_item_name);
            }
            Ok(Plan::AlterItemRename(AlterItemRenamePlan {
                id: entry.id(),
                current_full_name: full_name,
                to_name: normalize::ident(to_item_name),
                object_type,
            }))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn plan_alter_cluster_rename(
    scx: &mut StatementContext,
    object_type: ObjectType,
    name: Ident,
    to_name: Ident,
    if_exists: bool,
) -> Result<Plan, PlanError> {
    match resolve_cluster(scx, &name, if_exists)? {
        Some(entry) => Ok(Plan::AlterClusterRename(AlterClusterRenamePlan {
            id: entry.id(),
            name: entry.name().to_string(),
            to_name: ident(to_name),
        })),
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn plan_alter_cluster_swap<F>(
    scx: &mut StatementContext,
    name_a: Ident,
    name_b: Ident,
    gen_temp_suffix: F,
) -> Result<Plan, PlanError>
where
    F: Fn(&dyn Fn(&str) -> bool) -> Result<String, PlanError>,
{
    let cluster_a = scx.resolve_cluster(Some(&name_a))?;
    let cluster_b = scx.resolve_cluster(Some(&name_b))?;

    let check = |temp_suffix: &str| {
        let name_temp = Ident::new(format!("mz_cluster_swap_{temp_suffix}"));
        match scx.catalog.resolve_cluster(Some(name_temp.as_str())) {
            // Temp name does not exist, so we can use it.
            Err(CatalogError::UnknownCluster(_)) => true,
            // Temp name already exists!
            Ok(_) | Err(_) => false,
        }
    };
    let temp_suffix = gen_temp_suffix(&check)?;
    let name_temp = format!("mz_cluster_swap_{temp_suffix}");

    Ok(Plan::AlterClusterSwap(AlterClusterSwapPlan {
        id_a: cluster_a.id(),
        id_b: cluster_b.id(),
        name_a: name_a.into_string(),
        name_b: name_b.into_string(),
        name_temp,
    }))
}

pub fn plan_alter_cluster_replica_rename(
    scx: &mut StatementContext,
    object_type: ObjectType,
    name: QualifiedReplica,
    to_item_name: Ident,
    if_exists: bool,
) -> Result<Plan, PlanError> {
    match resolve_cluster_replica(scx, &name, if_exists)? {
        Some((cluster, replica)) => {
            ensure_cluster_is_not_managed(scx, cluster.id())?;
            Ok(Plan::AlterClusterReplicaRename(
                AlterClusterReplicaRenamePlan {
                    cluster_id: cluster.id(),
                    replica_id: replica,
                    name: QualifiedReplica {
                        cluster: cluster.name().into(),
                        replica: name.replica,
                    },
                    to_name: normalize::ident(to_item_name),
                },
            ))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn describe_alter_object_swap(
    _: &StatementContext,
    _: AlterObjectSwapStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_object_swap(
    scx: &mut StatementContext,
    stmt: AlterObjectSwapStatement,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_ALTER_SWAP)?;

    let AlterObjectSwapStatement {
        object_type,
        name_a,
        name_b,
    } = stmt;
    let object_type = object_type.into();

    // We'll try 10 times to generate a temporary suffix.
    let gen_temp_suffix = |check_fn: &dyn Fn(&str) -> bool| {
        let mut attempts = 0;
        let name_temp = loop {
            attempts += 1;
            if attempts > 10 {
                tracing::warn!("Unable to generate temp id for swapping");
                sql_bail!("unable to swap!");
            }

            // To make these temporary names a bit more manageable, we make them short, by using
            // the last component of a UUID, which should be 12 characters long.
            //
            // Note: the reason we use the last 12 characters is because the bits 6, 7, and 12 - 15
            // are all hard coded <https://www.rfc-editor.org/rfc/rfc4122#section-4.4>.
            let temp_uuid = uuid::Uuid::new_v4().as_hyphenated().to_string();
            let short_id: String = temp_uuid.chars().rev().take_while(|c| *c != '-').collect();

            // Call the provided closure to make sure this name is unique!
            if check_fn(&short_id) {
                break short_id;
            }
        };

        Ok(name_temp)
    };

    match (object_type, name_a, name_b) {
        (ObjectType::Schema, UnresolvedObjectName::Schema(name_a), name_b) => {
            plan_alter_schema_swap(scx, name_a, name_b, gen_temp_suffix)
        }
        (ObjectType::Cluster, UnresolvedObjectName::Cluster(name_a), name_b) => {
            plan_alter_cluster_swap(scx, name_a, name_b, gen_temp_suffix)
        }
        (object_type, _, _) => Err(PlanError::Unsupported {
            feature: format!("ALTER {object_type} .. SWAP WITH ..."),
            issue_no: Some(12972),
        }),
    }
}

pub fn describe_alter_secret_options(
    _: &StatementContext,
    _: AlterSecretStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_secret(
    scx: &mut StatementContext,
    stmt: AlterSecretStatement<Aug>,
) -> Result<Plan, PlanError> {
    let AlterSecretStatement {
        name,
        if_exists,
        value,
    } = stmt;
    let name = normalize::unresolved_item_name(name)?;
    let entry = match scx.catalog.resolve_item(&name) {
        Ok(secret) => secret,
        Err(_) if if_exists => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_string(),
                object_type: ObjectType::Secret,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Secret,
            }));
        }
        Err(e) => return Err(e.into()),
    };
    if entry.item_type() != CatalogItemType::Secret {
        sql_bail!(
            "\"{}\" is a {} not a secret",
            scx.catalog.resolve_full_name(entry.name()),
            entry.item_type()
        )
    }
    let id = entry.id();
    let secret_as = query::plan_secret_as(scx, value)?;

    Ok(Plan::AlterSecret(AlterSecretPlan { id, secret_as }))
}

pub fn describe_alter_sink(
    _: &StatementContext,
    _: AlterSinkStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_sink(
    scx: &mut StatementContext,
    stmt: AlterSinkStatement<Aug>,
) -> Result<Plan, PlanError> {
    let AlterSinkStatement {
        sink_name,
        if_exists,
        action,
    } = stmt;

    let sink_name = normalize::unresolved_item_name(sink_name)?;
    let entry = match scx.catalog.resolve_item(&sink_name) {
        Ok(sink) => sink,
        Err(_) if if_exists => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: sink_name.to_string(),
                object_type: ObjectType::Sink,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Sink,
            }));
        }
        Err(e) => return Err(e.into()),
    };
    if entry.item_type() != CatalogItemType::Sink {
        sql_bail!(
            "\"{}\" is a {} not a sink",
            scx.catalog.resolve_full_name(entry.name()),
            entry.item_type()
        )
    }
    let id = entry.id();

    let mut size = AlterOptionParameter::Unchanged;
    match action {
        AlterSinkAction::SetOptions(options) => {
            let CreateSinkOptionExtracted {
                size: size_opt,
                snapshot,
                seen: _,
            } = options.try_into()?;

            if let Some(value) = size_opt {
                size = AlterOptionParameter::Set(value);
            }
            if let Some(_) = snapshot {
                sql_bail!("Cannot modify the SNAPSHOT of a SINK.");
            }
        }
        AlterSinkAction::ResetOptions(reset) => {
            for name in reset {
                match name {
                    CreateSinkOptionName::Size => {
                        size = AlterOptionParameter::Reset;
                    }
                    CreateSinkOptionName::Snapshot => {
                        sql_bail!("Cannot modify the SNAPSHOT of a SINK.");
                    }
                }
            }
        }
    };

    Ok(Plan::AlterSink(AlterSinkPlan { id, size }))
}

pub fn describe_alter_source(
    _: &StatementContext,
    _: AlterSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    // TODO: put the options here, right?
    Ok(StatementDesc::new(None))
}

pub fn describe_alter_connection(
    _: &StatementContext,
    _: AlterConnectionStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(
    AlterSourceAddSubsourceOption,
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

pub fn plan_alter_source(
    scx: &mut StatementContext,
    stmt: AlterSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let AlterSourceStatement {
        source_name,
        if_exists,
        action,
    } = stmt;
    let source_name = normalize::unresolved_item_name(source_name)?;
    let entry = match scx.catalog.resolve_item(&source_name) {
        Ok(source) => source,
        Err(_) if if_exists => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: source_name.to_string(),
                object_type: ObjectType::Source,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Source,
            }));
        }
        Err(e) => return Err(e.into()),
    };
    if entry.item_type() != CatalogItemType::Source {
        sql_bail!(
            "\"{}\" is a {} not a source",
            scx.catalog.resolve_full_name(entry.name()),
            entry.item_type()
        )
    }
    let id = entry.id();

    let mut size = AlterOptionParameter::Unchanged;
    let action = match action {
        AlterSourceAction::SetOptions(options) => {
            let CreateSourceOptionExtracted {
                seen,
                size: size_opt,
                ..
            } = CreateSourceOptionExtracted::try_from(options)?;

            if let Some(option) = seen
                .iter()
                .find(|o| !matches!(o, CreateSourceOptionName::Size))
            {
                sql_bail!("Cannot modify the {} of a SOURCE.", option.to_ast_string());
            }

            if let Some(value) = size_opt {
                size = AlterOptionParameter::Set(value);
            }

            crate::plan::AlterSourceAction::Resize(size)
        }
        AlterSourceAction::ResetOptions(reset) => {
            for name in reset {
                match name {
                    CreateSourceOptionName::Size => {
                        size = AlterOptionParameter::Reset;
                    }
                    o => {
                        sql_bail!("Cannot modify the {} of a SOURCE.", o.to_ast_string());
                    }
                }
            }

            crate::plan::AlterSourceAction::Resize(size)
        }
        AlterSourceAction::DropSubsources {
            if_exists,
            names,
            cascade,
        } => {
            let mut to_drop = BTreeSet::new();
            let subsources = entry.subsources();
            for name in names {
                match plan_drop_subsource(scx, if_exists, name.clone(), cascade)? {
                    Some(dropped_id) => {
                        if !subsources.contains(&dropped_id) {
                            let dropped_entry = scx.catalog.get_item(&dropped_id);
                            return Err(PlanError::DropNonSubsource {
                                non_subsource: scx
                                    .catalog
                                    .minimal_qualification(dropped_entry.name())
                                    .to_string(),
                                source: scx.catalog.minimal_qualification(entry.name()).to_string(),
                            });
                        }

                        to_drop.insert(dropped_id);
                    }
                    None => {
                        scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                            name: name.to_string(),
                            object_type: ObjectType::Source,
                        });
                    }
                }
            }

            // Cannot drop last non-progress subsource
            if subsources.len().saturating_sub(to_drop.len()) <= 1 {
                return Err(PlanError::DropLastSubsource {
                    source: scx.catalog.minimal_qualification(entry.name()).to_string(),
                });
            }

            if to_drop.is_empty() {
                return Ok(Plan::AlterNoop(AlterNoopPlan {
                    object_type: ObjectType::Source,
                }));
            } else {
                crate::plan::AlterSourceAction::DropSubsourceExports { to_drop }
            }
        }
        AlterSourceAction::AddSubsources {
            subsources,
            details,
            options,
        } => crate::plan::AlterSourceAction::AddSubsourceExports {
            subsources,
            details,
            options,
        },
    };

    Ok(Plan::AlterSource(AlterSourcePlan { id, action }))
}

pub fn describe_alter_system_set(
    _: &StatementContext,
    _: AlterSystemSetStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_system_set(
    _: &StatementContext,
    AlterSystemSetStatement { name, to }: AlterSystemSetStatement,
) -> Result<Plan, PlanError> {
    let name = name.to_string();
    Ok(Plan::AlterSystemSet(AlterSystemSetPlan {
        name,
        value: scl::plan_set_variable_to(to)?,
    }))
}

pub fn describe_alter_system_reset(
    _: &StatementContext,
    _: AlterSystemResetStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_system_reset(
    _: &StatementContext,
    AlterSystemResetStatement { name }: AlterSystemResetStatement,
) -> Result<Plan, PlanError> {
    let name = name.to_string();
    Ok(Plan::AlterSystemReset(AlterSystemResetPlan { name }))
}

pub fn describe_alter_system_reset_all(
    _: &StatementContext,
    _: AlterSystemResetAllStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_system_reset_all(
    _: &StatementContext,
    _: AlterSystemResetAllStatement,
) -> Result<Plan, PlanError> {
    Ok(Plan::AlterSystemResetAll(AlterSystemResetAllPlan {}))
}

pub fn plan_alter_connection(
    scx: &mut StatementContext,
    stmt: AlterConnectionStatement,
) -> Result<Plan, PlanError> {
    let AlterConnectionStatement { name, if_exists } = stmt;
    let name = normalize::unresolved_item_name(name)?;
    let entry = match scx.catalog.resolve_item(&name) {
        Ok(connection) => connection,
        Err(_) if if_exists => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_string(),
                object_type: ObjectType::Connection,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Connection,
            }));
        }
        Err(e) => return Err(e.into()),
    };
    if !matches!(entry.connection()?, Connection::Ssh(_)) {
        sql_bail!(
            "{} is not an SSH connection",
            scx.catalog.resolve_full_name(entry.name())
        )
    }

    let id = entry.id();
    Ok(Plan::RotateKeys(RotateKeysPlan { id }))
}

pub fn describe_alter_role(
    _: &StatementContext,
    _: AlterRoleStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_role(
    scx: &StatementContext,
    AlterRoleStatement { name, option }: AlterRoleStatement<Aug>,
) -> Result<Plan, PlanError> {
    let option = match option {
        AlterRoleOption::Attributes(attrs) => {
            let attrs = plan_role_attributes(attrs)?;
            PlannedAlterRoleOption::Attributes(attrs)
        }
        AlterRoleOption::Variable(variable) => {
            // Make sure the LaunchDarkly flag is enabled.
            scx.require_feature_flag(&vars::ENABLE_ROLE_VARS)?;

            let var = plan_role_variable(variable)?;
            PlannedAlterRoleOption::Variable(var)
        }
    };

    Ok(Plan::AlterRole(AlterRolePlan {
        id: name.id,
        name: name.name,
        option,
    }))
}

pub fn describe_comment(
    _: &StatementContext,
    _: CommentStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_comment(
    scx: &mut StatementContext,
    stmt: CommentStatement<Aug>,
) -> Result<Plan, PlanError> {
    const MAX_COMMENT_LENGTH: usize = 1024;

    scx.require_feature_flag(&vars::ENABLE_COMMENT)?;

    let CommentStatement { object, comment } = stmt;

    // TODO(parkmycar): Make max comment length configurable.
    if let Some(c) = &comment {
        if c.len() > 1024 {
            return Err(PlanError::CommentTooLong {
                length: c.len(),
                max_size: MAX_COMMENT_LENGTH,
            });
        }
    }

    let (object_id, column_pos) = match &object {
        com_ty @ CommentObjectType::Table { name }
        | com_ty @ CommentObjectType::View { name }
        | com_ty @ CommentObjectType::MaterializedView { name }
        | com_ty @ CommentObjectType::Index { name }
        | com_ty @ CommentObjectType::Func { name }
        | com_ty @ CommentObjectType::Connection { name }
        | com_ty @ CommentObjectType::Source { name }
        | com_ty @ CommentObjectType::Sink { name }
        | com_ty @ CommentObjectType::Type { name }
        | com_ty @ CommentObjectType::Secret { name } => {
            let item = scx.get_item_by_resolved_name(name)?;
            match (com_ty, item.item_type()) {
                (CommentObjectType::Table { .. }, CatalogItemType::Table) => {
                    (CommentObjectId::Table(item.id()), None)
                }
                (CommentObjectType::View { .. }, CatalogItemType::View) => {
                    (CommentObjectId::View(item.id()), None)
                }
                (CommentObjectType::MaterializedView { .. }, CatalogItemType::MaterializedView) => {
                    (CommentObjectId::MaterializedView(item.id()), None)
                }
                (CommentObjectType::Index { .. }, CatalogItemType::Index) => {
                    (CommentObjectId::Index(item.id()), None)
                }
                (CommentObjectType::Func { .. }, CatalogItemType::Func) => {
                    (CommentObjectId::Func(item.id()), None)
                }
                (CommentObjectType::Connection { .. }, CatalogItemType::Connection) => {
                    (CommentObjectId::Connection(item.id()), None)
                }
                (CommentObjectType::Source { .. }, CatalogItemType::Source) => {
                    (CommentObjectId::Source(item.id()), None)
                }
                (CommentObjectType::Sink { .. }, CatalogItemType::Sink) => {
                    (CommentObjectId::Sink(item.id()), None)
                }
                (CommentObjectType::Type { .. }, CatalogItemType::Type) => {
                    (CommentObjectId::Type(item.id()), None)
                }
                (CommentObjectType::Secret { .. }, CatalogItemType::Secret) => {
                    (CommentObjectId::Secret(item.id()), None)
                }
                (com_ty, cat_ty) => {
                    let expected_type = match com_ty {
                        CommentObjectType::Table { .. } => ObjectType::Table,
                        CommentObjectType::View { .. } => ObjectType::View,
                        CommentObjectType::MaterializedView { .. } => ObjectType::MaterializedView,
                        CommentObjectType::Index { .. } => ObjectType::Index,
                        CommentObjectType::Func { .. } => ObjectType::Func,
                        CommentObjectType::Connection { .. } => ObjectType::Connection,
                        CommentObjectType::Source { .. } => ObjectType::Source,
                        CommentObjectType::Sink { .. } => ObjectType::Sink,
                        CommentObjectType::Type { .. } => ObjectType::Type,
                        CommentObjectType::Secret { .. } => ObjectType::Secret,
                        _ => unreachable!("these are the only types we match on"),
                    };

                    return Err(PlanError::InvalidObjectType {
                        expected_type: SystemObjectType::Object(expected_type),
                        actual_type: SystemObjectType::Object(cat_ty.into()),
                        object_name: item.name().item.clone(),
                    });
                }
            }
        }
        CommentObjectType::Column { name } => {
            let (item, pos) = scx.get_column_by_resolved_name(name)?;
            match item.item_type() {
                CatalogItemType::Table => (CommentObjectId::Table(item.id()), Some(pos + 1)),
                CatalogItemType::Source => (CommentObjectId::Source(item.id()), Some(pos + 1)),
                CatalogItemType::View => (CommentObjectId::View(item.id()), Some(pos + 1)),
                CatalogItemType::MaterializedView => {
                    (CommentObjectId::MaterializedView(item.id()), Some(pos + 1))
                }
                CatalogItemType::Type => (CommentObjectId::Type(item.id()), Some(pos + 1)),
                r => {
                    return Err(PlanError::Unsupported {
                        feature: format!("Specifying comments on a column of {r}"),
                        issue_no: Some(21465),
                    });
                }
            }
        }
        CommentObjectType::Role { name } => (CommentObjectId::Role(name.id), None),
        CommentObjectType::Database { name } => {
            (CommentObjectId::Database(*name.database_id()), None)
        }
        CommentObjectType::Schema { name } => (
            CommentObjectId::Schema((*name.database_spec(), *name.schema_spec())),
            None,
        ),
        CommentObjectType::Cluster { name } => (CommentObjectId::Cluster(name.id), None),
        CommentObjectType::ClusterReplica { name } => {
            let replica = scx.catalog.resolve_cluster_replica(name)?;
            (
                CommentObjectId::ClusterReplica((replica.cluster_id(), replica.replica_id())),
                None,
            )
        }
    };

    // Note: the `mz_comments` table uses an `Int4` for the column position, but in the Stash we
    // store a `usize` which would be a `Uint8`. We guard against a safe conversion here because
    // it's the easiest place to raise an error.
    //
    // TODO(parkmycar): https://github.com/MaterializeInc/materialize/issues/22246.
    if let Some(p) = column_pos {
        i32::try_from(p).map_err(|_| PlanError::TooManyColumns {
            max_num_columns: MAX_NUM_COLUMNS,
            req_num_columns: p,
        })?;
    }

    Ok(Plan::Comment(CommentPlan {
        object_id,
        sub_component: column_pos,
        comment,
    }))
}

pub(crate) fn resolve_cluster<'a>(
    scx: &'a StatementContext,
    name: &'a Ident,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogCluster<'a>>, PlanError> {
    match scx.resolve_cluster(Some(name)) {
        Ok(cluster) => Ok(Some(cluster)),
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_cluster_replica<'a>(
    scx: &'a StatementContext,
    name: &QualifiedReplica,
    if_exists: bool,
) -> Result<Option<(&'a dyn CatalogCluster<'a>, ReplicaId)>, PlanError> {
    match scx.resolve_cluster(Some(&name.cluster)) {
        Ok(cluster) => match cluster.replica_ids().get(name.replica.as_str()) {
            Some(replica_id) => Ok(Some((cluster, *replica_id))),
            None if if_exists => Ok(None),
            None => Err(sql_err!(
                "CLUSTER {} has no CLUSTER REPLICA named {}",
                cluster.name(),
                name.replica.as_str().quoted(),
            )),
        },
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_database<'a>(
    scx: &'a StatementContext,
    name: &'a UnresolvedDatabaseName,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogDatabase>, PlanError> {
    match scx.resolve_database(name) {
        Ok(database) => Ok(Some(database)),
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_schema<'a>(
    scx: &'a StatementContext,
    name: UnresolvedSchemaName,
    if_exists: bool,
) -> Result<Option<(ResolvedDatabaseSpecifier, SchemaSpecifier)>, PlanError> {
    match scx.resolve_schema(name) {
        Ok(schema) => Ok(Some((schema.database().clone(), schema.id().clone()))),
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_item<'a>(
    scx: &'a StatementContext,
    name: UnresolvedItemName,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogItem>, PlanError> {
    let name = normalize::unresolved_item_name(name)?;
    match scx.catalog.resolve_item(&name) {
        Ok(item) => Ok(Some(item)),
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Returns an error if the given cluster is a linked cluster
pub(crate) fn ensure_cluster_is_not_linked(
    scx: &StatementContext,
    cluster_id: ClusterId,
) -> Result<(), PlanError> {
    let cluster = scx.catalog.get_cluster(cluster_id);
    if let Some(linked_id) = cluster.linked_object_id() {
        let cluster_name = scx.catalog.get_cluster(cluster_id).name().to_string();
        let linked_object_name = scx
            .catalog
            .resolve_full_name(scx.catalog.get_item(&linked_id).name())
            .to_string();
        Err(PlanError::ModifyLinkedCluster {
            cluster_name,
            linked_object_name,
        })
    } else {
        Ok(())
    }
}

/// Returns an error if the given cluster is a managed cluster
fn ensure_cluster_is_not_managed(
    scx: &StatementContext,
    cluster_id: ClusterId,
) -> Result<(), PlanError> {
    let cluster = scx.catalog.get_cluster(cluster_id);
    if cluster.is_managed() {
        Err(PlanError::ManagedCluster {
            cluster_name: cluster.name().to_string(),
        })
    } else {
        Ok(())
    }
}
