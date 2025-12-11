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
use std::num::NonZeroU32;
use std::time::Duration;

use itertools::{Either, Itertools};
use mz_adapter_types::compaction::{CompactionWindow, DEFAULT_LOGICAL_COMPACTION_WINDOW_DURATION};
use mz_adapter_types::dyncfgs::ENABLE_MULTI_REPLICA_SOURCES;
use mz_auth::password::Password;
use mz_controller_types::{ClusterId, DEFAULT_REPLICA_LOGGING_INTERVAL, ReplicaId};
use mz_expr::{CollectionPlan, UnmaterializableFunc};
use mz_interchange::avro::{AvroSchemaGenerator, DocTarget};
use mz_ore::cast::{CastFrom, TryCastFrom};
use mz_ore::collections::{CollectionExt, HashSet};
use mz_ore::num::NonNeg;
use mz_ore::soft_panic_or_log;
use mz_ore::str::StrExt;
use mz_proto::RustType;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::optimize::OptimizerFeatureOverrides;
use mz_repr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use mz_repr::role_id::RoleId;
use mz_repr::{
    CatalogItemId, ColumnName, RelationDesc, RelationVersion, RelationVersionSelector,
    SqlColumnType, SqlRelationType, SqlScalarType, Timestamp, VersionedRelationDesc,
    preserves_order, strconv,
};
use mz_sql_parser::ast::{
    self, AlterClusterAction, AlterClusterStatement, AlterConnectionAction, AlterConnectionOption,
    AlterConnectionOptionName, AlterConnectionStatement, AlterIndexAction, AlterIndexStatement,
    AlterMaterializedViewApplyReplacementStatement, AlterNetworkPolicyStatement,
    AlterObjectRenameStatement, AlterObjectSwapStatement, AlterRetainHistoryStatement,
    AlterRoleOption, AlterRoleStatement, AlterSecretStatement, AlterSetClusterStatement,
    AlterSinkAction, AlterSinkStatement, AlterSourceAction, AlterSourceAddSubsourceOption,
    AlterSourceAddSubsourceOptionName, AlterSourceStatement, AlterSystemResetAllStatement,
    AlterSystemResetStatement, AlterSystemSetStatement, AlterTableAddColumnStatement, AvroSchema,
    AvroSchemaOption, AvroSchemaOptionName, ClusterAlterOption, ClusterAlterOptionName,
    ClusterAlterOptionValue, ClusterAlterUntilReadyOption, ClusterAlterUntilReadyOptionName,
    ClusterFeature, ClusterFeatureName, ClusterOption, ClusterOptionName,
    ClusterScheduleOptionValue, ColumnDef, ColumnOption, CommentObjectType, CommentStatement,
    ConnectionOption, ConnectionOptionName, ContinualTaskOption, ContinualTaskOptionName,
    CreateClusterReplicaStatement, CreateClusterStatement, CreateConnectionOption,
    CreateConnectionOptionName, CreateConnectionStatement, CreateConnectionType,
    CreateContinualTaskStatement, CreateDatabaseStatement, CreateIndexStatement,
    CreateMaterializedViewStatement, CreateNetworkPolicyStatement, CreateRoleStatement,
    CreateSchemaStatement, CreateSecretStatement, CreateSinkConnection, CreateSinkOption,
    CreateSinkOptionName, CreateSinkStatement, CreateSourceConnection, CreateSourceOption,
    CreateSourceOptionName, CreateSourceStatement, CreateSubsourceOption,
    CreateSubsourceOptionName, CreateSubsourceStatement, CreateTableFromSourceStatement,
    CreateTableStatement, CreateTypeAs, CreateTypeListOption, CreateTypeListOptionName,
    CreateTypeMapOption, CreateTypeMapOptionName, CreateTypeStatement, CreateViewStatement,
    CreateWebhookSourceStatement, CsrConfigOption, CsrConfigOptionName, CsrConnection,
    CsrConnectionAvro, CsrConnectionProtobuf, CsrSeedProtobuf, CsvColumns, DeferredItemName,
    DocOnIdentifier, DocOnSchema, DropObjectsStatement, DropOwnedStatement, Expr, Format,
    FormatSpecifier, IcebergSinkConfigOption, Ident, IfExistsBehavior, IndexOption,
    IndexOptionName, KafkaSinkConfigOption, KeyConstraint, LoadGeneratorOption,
    LoadGeneratorOptionName, MaterializedViewOption, MaterializedViewOptionName, MySqlConfigOption,
    MySqlConfigOptionName, NetworkPolicyOption, NetworkPolicyOptionName,
    NetworkPolicyRuleDefinition, NetworkPolicyRuleOption, NetworkPolicyRuleOptionName,
    PgConfigOption, PgConfigOptionName, ProtobufSchema, QualifiedReplica, RefreshAtOptionValue,
    RefreshEveryOptionValue, RefreshOptionValue, ReplicaDefinition, ReplicaOption,
    ReplicaOptionName, RoleAttribute, SetRoleVar, SourceErrorPolicy, SourceIncludeMetadata,
    SqlServerConfigOption, SqlServerConfigOptionName, Statement, TableConstraint,
    TableFromSourceColumns, TableFromSourceOption, TableFromSourceOptionName, TableOption,
    TableOptionName, UnresolvedDatabaseName, UnresolvedItemName, UnresolvedObjectName,
    UnresolvedSchemaName, Value, ViewDefinition, WithOptionValue,
};
use mz_sql_parser::ident;
use mz_sql_parser::parser::StatementParseResult;
use mz_storage_types::connections::inline::{ConnectionAccess, ReferencedConnection};
use mz_storage_types::connections::{Connection, KafkaTopicOptions};
use mz_storage_types::sinks::{
    IcebergSinkConnection, KafkaIdStyle, KafkaSinkConnection, KafkaSinkFormat, KafkaSinkFormatType,
    SinkEnvelope, StorageSinkConnection,
};
use mz_storage_types::sources::encoding::{
    AvroEncoding, ColumnSpec, CsvEncoding, DataEncoding, ProtobufEncoding, RegexEncoding,
    SourceDataEncoding, included_column_desc,
};
use mz_storage_types::sources::envelope::{
    KeyEnvelope, NoneEnvelope, SourceEnvelope, UnplannedSourceEnvelope, UpsertStyle,
};
use mz_storage_types::sources::kafka::{
    KafkaMetadataKind, KafkaSourceConnection, KafkaSourceExportDetails, kafka_metadata_columns_desc,
};
use mz_storage_types::sources::load_generator::{
    KeyValueLoadGenerator, LOAD_GENERATOR_KEY_VALUE_OFFSET_DEFAULT, LoadGenerator,
    LoadGeneratorOutput, LoadGeneratorSourceConnection, LoadGeneratorSourceExportDetails,
};
use mz_storage_types::sources::mysql::{
    MySqlSourceConnection, MySqlSourceDetails, ProtoMySqlSourceDetails,
};
use mz_storage_types::sources::postgres::{
    PostgresSourceConnection, PostgresSourcePublicationDetails,
    ProtoPostgresSourcePublicationDetails,
};
use mz_storage_types::sources::sql_server::{
    ProtoSqlServerSourceExtras, SqlServerSourceExportDetails,
};
use mz_storage_types::sources::{
    GenericSourceConnection, MySqlSourceExportDetails, PostgresSourceExportDetails,
    ProtoSourceExportStatementDetails, SourceConnection, SourceDesc, SourceExportDataConfig,
    SourceExportDetails, SourceExportStatementDetails, SqlServerSourceConnection,
    SqlServerSourceExtras, Timeline,
};
use prost::Message;

use crate::ast::display::AstDisplay;
use crate::catalog::{
    CatalogCluster, CatalogDatabase, CatalogError, CatalogItem, CatalogItemType,
    CatalogRecordField, CatalogType, CatalogTypeDetails, ObjectType, SystemObjectType,
};
use crate::iceberg::IcebergSinkConfigOptionExtracted;
use crate::kafka_util::{KafkaSinkConfigOptionExtracted, KafkaSourceConfigOptionExtracted};
use crate::names::{
    Aug, CommentObjectId, DatabaseId, DependencyIds, ObjectId, PartialItemName, QualifiedItemName,
    ResolvedClusterName, ResolvedColumnReference, ResolvedDataType, ResolvedDatabaseSpecifier,
    ResolvedItemName, ResolvedNetworkPolicyName, SchemaSpecifier, SystemObjectId,
};
use crate::normalize::{self, ident};
use crate::plan::error::PlanError;
use crate::plan::query::{
    CteDesc, ExprContext, QueryLifetime, cast_relation, plan_expr, scalar_type_from_catalog,
    scalar_type_from_sql,
};
use crate::plan::scope::Scope;
use crate::plan::statement::ddl::connection::{INALTERABLE_OPTIONS, MUTUALLY_EXCLUSIVE_SETS};
use crate::plan::statement::{StatementContext, StatementDesc, scl};
use crate::plan::typeconv::CastContext;
use crate::plan::with_options::{OptionalDuration, OptionalString, TryFromValue};
use crate::plan::{
    AlterClusterPlan, AlterClusterPlanStrategy, AlterClusterRenamePlan,
    AlterClusterReplicaRenamePlan, AlterClusterSwapPlan, AlterConnectionPlan, AlterItemRenamePlan,
    AlterMaterializedViewApplyReplacementPlan, AlterNetworkPolicyPlan, AlterNoopPlan,
    AlterOptionParameter, AlterRetainHistoryPlan, AlterRolePlan, AlterSchemaRenamePlan,
    AlterSchemaSwapPlan, AlterSecretPlan, AlterSetClusterPlan, AlterSinkPlan,
    AlterSystemResetAllPlan, AlterSystemResetPlan, AlterSystemSetPlan, AlterTablePlan,
    ClusterSchedule, CommentPlan, ComputeReplicaConfig, ComputeReplicaIntrospectionConfig,
    ConnectionDetails, CreateClusterManagedPlan, CreateClusterPlan, CreateClusterReplicaPlan,
    CreateClusterUnmanagedPlan, CreateClusterVariant, CreateConnectionPlan,
    CreateContinualTaskPlan, CreateDatabasePlan, CreateIndexPlan, CreateMaterializedViewPlan,
    CreateNetworkPolicyPlan, CreateRolePlan, CreateSchemaPlan, CreateSecretPlan, CreateSinkPlan,
    CreateSourcePlan, CreateTablePlan, CreateTypePlan, CreateViewPlan, DataSourceDesc,
    DropObjectsPlan, DropOwnedPlan, HirRelationExpr, Index, MaterializedView, NetworkPolicyRule,
    NetworkPolicyRuleAction, NetworkPolicyRuleDirection, Plan, PlanClusterOption, PlanNotice,
    PolicyAddress, QueryContext, ReplicaConfig, Secret, Sink, Source, Table, TableDataSource, Type,
    VariableValue, View, WebhookBodyFormat, WebhookHeaderFilters, WebhookHeaders,
    WebhookValidation, literal, plan_utils, query, transform_ast,
};
use crate::session::vars::{
    self, ENABLE_CLUSTER_SCHEDULE_REFRESH, ENABLE_COLLECTION_PARTITION_BY,
    ENABLE_CREATE_TABLE_FROM_SOURCE, ENABLE_KAFKA_SINK_HEADERS, ENABLE_REFRESH_EVERY_MVS,
};
use crate::{names, parse};

mod connection;

// TODO: Figure out what the maximum number of columns we can actually support is, and set that.
//
// The real max is probably higher than this, but it's easier to relax a constraint than make it
// more strict.
const MAX_NUM_COLUMNS: usize = 256;

static MANAGED_REPLICA_PATTERN: std::sync::LazyLock<regex::Regex> =
    std::sync::LazyLock::new(|| regex::Regex::new(r"^r(\d)+$").unwrap());

/// Given a relation desc and a column list, checks that:
/// - the column list is a prefix of the desc;
/// - all the listed columns are types that have meaningful Persist-level ordering.
fn check_partition_by(desc: &RelationDesc, mut partition_by: Vec<Ident>) -> Result<(), PlanError> {
    if partition_by.len() > desc.len() {
        tracing::error!(
            "PARTITION BY contains more columns than the relation. (expected at most {}, got {})",
            desc.len(),
            partition_by.len()
        );
        partition_by.truncate(desc.len());
    }

    let desc_prefix = desc.iter().take(partition_by.len());
    for (idx, ((desc_name, desc_type), partition_name)) in
        desc_prefix.zip_eq(partition_by).enumerate()
    {
        let partition_name = normalize::column_name(partition_name);
        if *desc_name != partition_name {
            sql_bail!(
                "PARTITION BY columns should be a prefix of the relation's columns (expected {desc_name} at index {idx}, got {partition_name})"
            );
        }
        if !preserves_order(&desc_type.scalar_type) {
            sql_bail!("PARTITION BY column {partition_name} has unsupported type");
        }
    }
    Ok(())
}

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
        with_options,
    } = &stmt;

    let names: Vec<_> = columns
        .iter()
        .filter(|c| {
            // This set of `names` is used to create the initial RelationDesc.
            // Columns that have been added at later versions of the table will
            // get added further below.
            let is_versioned = c
                .options
                .iter()
                .any(|o| matches!(o.option, ColumnOption::Versioned { .. }));
            !is_versioned
        })
        .map(|c| normalize::column_name(c.name.clone()))
        .collect();

    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.quoted());
    }

    // Build initial relation type that handles declared data types
    // and NOT NULL constraints.
    let mut column_types = Vec::with_capacity(columns.len());
    let mut defaults = Vec::with_capacity(columns.len());
    let mut changes = BTreeMap::new();
    let mut keys = Vec::new();

    for (i, c) in columns.into_iter().enumerate() {
        let aug_data_type = &c.data_type;
        let ty = query::scalar_type_from_sql(scx, aug_data_type)?;
        let mut nullable = true;
        let mut default = Expr::null();
        let mut versioned = false;
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
                ColumnOption::Versioned { action, version } => {
                    let version = RelationVersion::from(*version);
                    versioned = true;

                    let name = normalize::column_name(c.name.clone());
                    let typ = ty.clone().nullable(nullable);

                    changes.insert(version, (action.clone(), name, typ));
                }
                other => {
                    bail_unsupported!(format!("CREATE TABLE with column constraint: {}", other))
                }
            }
        }
        // TODO(alter_table): This assumes all versioned columns are at the
        // end. This will no longer be true when we support dropping columns.
        if !versioned {
            column_types.push(ty.nullable(nullable));
        }
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
                scx.require_feature_flag(&vars::UNSAFE_ENABLE_TABLE_FOREIGN_KEY)?
            }
            TableConstraint::Check { .. } => {
                // Check constraints are not presently enforced. We allow them
                // with feature flags for sqllogictest's sake.
                scx.require_feature_flag(&vars::UNSAFE_ENABLE_TABLE_CHECK_CONSTRAINT)?
            }
        }
    }

    if !keys.is_empty() {
        // Unique constraints are not presently enforced. We allow them with feature flags for
        // sqllogictest's sake.
        scx.require_feature_flag(&vars::UNSAFE_ENABLE_TABLE_KEYS)?
    }

    let typ = SqlRelationType::new(column_types).with_keys(keys);

    let temporary = *temporary;
    let name = if temporary {
        scx.allocate_temporary_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    } else {
        scx.allocate_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    };

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    // For PostgreSQL compatibility, we need to prevent creating tables when
    // there is an existing object *or* type of the same name.
    if let (false, Ok(item)) = (
        if_not_exists,
        scx.catalog.resolve_item_or_type(&partial_name),
    ) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    let desc = RelationDesc::new(typ, names);
    let mut desc = VersionedRelationDesc::new(desc);
    for (version, (_action, name, typ)) in changes.into_iter() {
        let new_version = desc.add_column(name, typ);
        if version != new_version {
            return Err(PlanError::InvalidTable {
                name: full_name.item,
            });
        }
    }

    let create_sql = normalize::create_statement(scx, Statement::CreateTable(stmt.clone()))?;

    // Table options should only consider the original columns, since those
    // were the only ones in scope when the table was created.
    //
    // TODO(alter_table): Will need to reconsider this when we support ALTERing
    // the PARTITION BY columns.
    let original_desc = desc.at_version(RelationVersionSelector::Specific(RelationVersion::root()));
    let options = plan_table_options(scx, &original_desc, with_options.clone())?;

    let compaction_window = options.iter().find_map(|o| {
        #[allow(irrefutable_let_patterns)]
        if let crate::plan::TableOption::RetainHistory(lcw) = o {
            Some(lcw.clone())
        } else {
            None
        }
    });

    let table = Table {
        create_sql,
        desc,
        temporary,
        compaction_window,
        data_source: TableDataSource::TableWrites { defaults },
    };
    Ok(Plan::CreateTable(CreateTablePlan {
        name,
        table,
        if_not_exists: *if_not_exists,
    }))
}

pub fn describe_create_table_from_source(
    _: &StatementContext,
    _: CreateTableFromSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
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
    (TimestampInterval, Duration),
    (RetainHistory, OptionalDuration)
);

generate_extracted_config!(
    PgConfigOption,
    (Details, String),
    (Publication, String),
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![])),
    (ExcludeColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

generate_extracted_config!(
    MySqlConfigOption,
    (Details, String),
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![])),
    (ExcludeColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

generate_extracted_config!(
    SqlServerConfigOption,
    (Details, String),
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![])),
    (ExcludeColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

pub fn plan_create_webhook_source(
    scx: &StatementContext,
    mut stmt: CreateWebhookSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    if stmt.is_table {
        scx.require_feature_flag(&ENABLE_CREATE_TABLE_FROM_SOURCE)?;
    }

    // We will rewrite the cluster if one is not provided, so we must use the `in_cluster` value
    // we plan to normalize when we canonicalize the create statement.
    let in_cluster = source_sink_cluster_config(scx, &mut stmt.in_cluster)?;
    let enable_multi_replica_sources =
        ENABLE_MULTI_REPLICA_SOURCES.get(scx.catalog.system_vars().dyncfgs());
    if !enable_multi_replica_sources {
        if in_cluster.replica_ids().len() > 1 {
            sql_bail!("cannot create webhook source in cluster with more than one replica")
        }
    }
    let create_sql =
        normalize::create_statement(scx, Statement::CreateWebhookSource(stmt.clone()))?;

    let CreateWebhookSourceStatement {
        name,
        if_not_exists,
        body_format,
        include_headers,
        validate_using,
        is_table,
        // We resolved `in_cluster` above, so we want to ignore it here.
        in_cluster: _,
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
        // Validation expressions cannot contain unmaterializable functions, except `now()`. We
        // allow calls to `now()` because some webhook providers recommend rejecting requests that
        // are older than a certain threshold.
        if expression.contains_unmaterializable_except(&[UnmaterializableFunc::CurrentTimestamp]) {
            return Err(PlanError::WebhookValidationNonDeterministic);
        }
    }

    let body_format = match body_format {
        Format::Bytes => WebhookBodyFormat::Bytes,
        Format::Json { array } => WebhookBodyFormat::Json { array },
        Format::Text => WebhookBodyFormat::Text,
        // TODO(parkmycar): Make an issue to support more types, or change this to NeverSupported.
        ty => {
            return Err(PlanError::Unsupported {
                feature: format!("{ty} is not a valid BODY FORMAT for a WEBHOOK source"),
                discussion_no: None,
            });
        }
    };

    let mut column_ty = vec![
        // Always include the body of the request as the first column.
        SqlColumnType {
            scalar_type: SqlScalarType::from(body_format),
            nullable: false,
        },
    ];
    let mut column_names = vec!["body".to_string()];

    let mut headers = WebhookHeaders::default();

    // Include a `headers` column, possibly filtered.
    if let Some(filters) = include_headers.column {
        column_ty.push(SqlColumnType {
            scalar_type: SqlScalarType::Map {
                value_type: Box::new(SqlScalarType::String),
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
            .then_some(SqlScalarType::Bytes)
            .unwrap_or(SqlScalarType::String);
        column_ty.push(SqlColumnType {
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

    let typ = SqlRelationType::new(column_ty);
    let desc = RelationDesc::new(typ, column_names);

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

    let plan = if is_table {
        let data_source = DataSourceDesc::Webhook {
            validate_using,
            body_format,
            headers,
            cluster_id: Some(in_cluster.id()),
        };
        let data_source = TableDataSource::DataSource {
            desc: data_source,
            timeline,
        };
        Plan::CreateTable(CreateTablePlan {
            name,
            if_not_exists,
            table: Table {
                create_sql,
                desc: VersionedRelationDesc::new(desc),
                temporary: false,
                compaction_window: None,
                data_source,
            },
        })
    } else {
        let data_source = DataSourceDesc::Webhook {
            validate_using,
            body_format,
            headers,
            // Important: The cluster is set at the `Source` level.
            cluster_id: None,
        };
        Plan::CreateSource(CreateSourcePlan {
            name,
            source: Source {
                create_sql,
                data_source,
                desc,
                compaction_window: None,
            },
            if_not_exists,
            timeline,
            in_cluster: Some(in_cluster.id()),
        })
    };

    Ok(plan)
}

pub fn plan_create_source(
    scx: &StatementContext,
    mut stmt: CreateSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSourceStatement {
        name,
        in_cluster: _,
        col_names,
        connection: source_connection,
        envelope,
        if_not_exists,
        format,
        key_constraint,
        include_metadata,
        with_options,
        external_references: referenced_subsources,
        progress_subsource,
    } = &stmt;

    mz_ore::soft_assert_or_log!(
        referenced_subsources.is_none(),
        "referenced subsources must be cleared in purification"
    );

    let force_source_table_syntax = scx.catalog.system_vars().enable_create_table_from_source()
        && scx.catalog.system_vars().force_source_table_syntax();

    // If the new source table syntax is forced all the options related to the primary
    // source output should be un-set.
    if force_source_table_syntax {
        if envelope.is_some() || format.is_some() || !include_metadata.is_empty() {
            Err(PlanError::UseTablesForSources(
                "CREATE SOURCE (ENVELOPE|FORMAT|INCLUDE)".to_string(),
            ))?;
        }
    }

    let envelope = envelope.clone().unwrap_or(ast::SourceEnvelope::None);

    if !matches!(source_connection, CreateSourceConnection::Kafka { .. })
        && include_metadata
            .iter()
            .any(|sic| matches!(sic, SourceIncludeMetadata::Headers { .. }))
    {
        // TODO(guswynn): should this be `bail_unsupported!`?
        sql_bail!("INCLUDE HEADERS with non-Kafka sources not supported");
    }
    if !matches!(
        source_connection,
        CreateSourceConnection::Kafka { .. } | CreateSourceConnection::LoadGenerator { .. }
    ) && !include_metadata.is_empty()
    {
        bail_unsupported!("INCLUDE metadata with non-Kafka sources");
    }

    if !include_metadata.is_empty()
        && !matches!(
            envelope,
            ast::SourceEnvelope::Upsert { .. }
                | ast::SourceEnvelope::None
                | ast::SourceEnvelope::Debezium
        )
    {
        sql_bail!("INCLUDE <metadata> requires ENVELOPE (NONE|UPSERT|DEBEZIUM)");
    }

    let external_connection =
        plan_generic_source_connection(scx, source_connection, include_metadata)?;

    let CreateSourceOptionExtracted {
        timestamp_interval,
        retain_history,
        seen: _,
    } = CreateSourceOptionExtracted::try_from(with_options.clone())?;

    let metadata_columns_desc = match external_connection {
        GenericSourceConnection::Kafka(KafkaSourceConnection {
            ref metadata_columns,
            ..
        }) => kafka_metadata_columns_desc(metadata_columns),
        _ => vec![],
    };

    // Generate the relation description for the primary export of the source.
    let (mut desc, envelope, encoding) = apply_source_envelope_encoding(
        scx,
        &envelope,
        format,
        Some(external_connection.default_key_desc()),
        external_connection.default_value_desc(),
        include_metadata,
        metadata_columns_desc,
        &external_connection,
    )?;
    plan_utils::maybe_rename_columns(format!("source {}", name), &mut desc, col_names)?;

    let names: Vec<_> = desc.iter_names().cloned().collect();
    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.quoted());
    }

    // Apply user-specified key constraint
    if let Some(KeyConstraint::PrimaryKeyNotEnforced { columns }) = key_constraint.clone() {
        // Don't remove this without addressing
        // https://github.com/MaterializeInc/database-issues/issues/4371.
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

    let timestamp_interval = match timestamp_interval {
        Some(duration) => {
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

    let (desc, data_source) = match progress_subsource {
        Some(name) => {
            let DeferredItemName::Named(name) = name else {
                sql_bail!("[internal error] progress subsource must be named during purification");
            };
            let ResolvedItemName::Item { id, .. } = name else {
                sql_bail!("[internal error] invalid target id");
            };

            let details = match external_connection {
                GenericSourceConnection::Kafka(ref c) => {
                    SourceExportDetails::Kafka(KafkaSourceExportDetails {
                        metadata_columns: c.metadata_columns.clone(),
                    })
                }
                GenericSourceConnection::LoadGenerator(ref c) => match c.load_generator {
                    LoadGenerator::Auction
                    | LoadGenerator::Marketing
                    | LoadGenerator::Tpch { .. } => SourceExportDetails::None,
                    LoadGenerator::Counter { .. }
                    | LoadGenerator::Clock
                    | LoadGenerator::Datums
                    | LoadGenerator::KeyValue(_) => {
                        SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails {
                            output: LoadGeneratorOutput::Default,
                        })
                    }
                },
                GenericSourceConnection::Postgres(_)
                | GenericSourceConnection::MySql(_)
                | GenericSourceConnection::SqlServer(_) => SourceExportDetails::None,
            };

            let data_source = DataSourceDesc::OldSyntaxIngestion {
                desc: SourceDesc {
                    connection: external_connection,
                    timestamp_interval,
                },
                progress_subsource: *id,
                data_config: SourceExportDataConfig {
                    encoding,
                    envelope: envelope.clone(),
                },
                details,
            };
            (desc, data_source)
        }
        None => {
            let desc = external_connection.timestamp_desc();
            let data_source = DataSourceDesc::Ingestion(SourceDesc {
                connection: external_connection,
                timestamp_interval,
            });
            (desc, data_source)
        }
    };

    let if_not_exists = *if_not_exists;
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name.clone())?)?;

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    // For PostgreSQL compatibility, we need to prevent creating sources when
    // there is an existing object *or* type of the same name.
    if let (false, Ok(item)) = (
        if_not_exists,
        scx.catalog.resolve_item_or_type(&partial_name),
    ) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    // We will rewrite the cluster if one is not provided, so we must use the
    // `in_cluster` value we plan to normalize when we canonicalize the create
    // statement.
    let in_cluster = source_sink_cluster_config(scx, &mut stmt.in_cluster)?;

    let create_sql = normalize::create_statement(scx, Statement::CreateSource(stmt))?;

    // Determine a default timeline for the source.
    let timeline = match envelope {
        SourceEnvelope::CdcV2 => {
            Timeline::External(scx.catalog.resolve_full_name(&name).to_string())
        }
        _ => Timeline::EpochMilliseconds,
    };

    let compaction_window = plan_retain_history_option(scx, retain_history)?;

    let source = Source {
        create_sql,
        data_source,
        desc,
        compaction_window,
    };

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        timeline,
        in_cluster: Some(in_cluster.id()),
    }))
}

pub fn plan_generic_source_connection(
    scx: &StatementContext<'_>,
    source_connection: &CreateSourceConnection<Aug>,
    include_metadata: &Vec<SourceIncludeMetadata>,
) -> Result<GenericSourceConnection<ReferencedConnection>, PlanError> {
    Ok(match source_connection {
        CreateSourceConnection::Kafka {
            connection,
            options,
        } => GenericSourceConnection::Kafka(plan_kafka_source_connection(
            scx,
            connection,
            options,
            include_metadata,
        )?),
        CreateSourceConnection::Postgres {
            connection,
            options,
        } => GenericSourceConnection::Postgres(plan_postgres_source_connection(
            scx, connection, options,
        )?),
        CreateSourceConnection::SqlServer {
            connection,
            options,
        } => GenericSourceConnection::SqlServer(plan_sqlserver_source_connection(
            scx, connection, options,
        )?),
        CreateSourceConnection::MySql {
            connection,
            options,
        } => {
            GenericSourceConnection::MySql(plan_mysql_source_connection(scx, connection, options)?)
        }
        CreateSourceConnection::LoadGenerator { generator, options } => {
            GenericSourceConnection::LoadGenerator(plan_load_generator_source_connection(
                scx,
                generator,
                options,
                include_metadata,
            )?)
        }
    })
}

fn plan_load_generator_source_connection(
    scx: &StatementContext<'_>,
    generator: &ast::LoadGenerator,
    options: &Vec<LoadGeneratorOption<Aug>>,
    include_metadata: &Vec<SourceIncludeMetadata>,
) -> Result<LoadGeneratorSourceConnection, PlanError> {
    let load_generator =
        load_generator_ast_to_generator(scx, generator, options, include_metadata)?;
    let LoadGeneratorOptionExtracted {
        tick_interval,
        as_of,
        up_to,
        ..
    } = options.clone().try_into()?;
    let tick_micros = match tick_interval {
        Some(interval) => Some(interval.as_micros().try_into()?),
        None => None,
    };
    if up_to < as_of {
        sql_bail!("UP TO cannot be less than AS OF");
    }
    Ok(LoadGeneratorSourceConnection {
        load_generator,
        tick_micros,
        as_of,
        up_to,
    })
}

fn plan_mysql_source_connection(
    scx: &StatementContext<'_>,
    connection: &ResolvedItemName,
    options: &Vec<MySqlConfigOption<Aug>>,
) -> Result<MySqlSourceConnection<ReferencedConnection>, PlanError> {
    let connection_item = scx.get_item_by_resolved_name(connection)?;
    match connection_item.connection()? {
        Connection::MySql(connection) => connection,
        _ => sql_bail!(
            "{} is not a MySQL connection",
            scx.catalog.resolve_full_name(connection_item.name())
        ),
    };
    let MySqlConfigOptionExtracted {
        details,
        // text/exclude columns are already part of the source-exports and are only included
        // in these options for round-tripping of a `CREATE SOURCE` statement. This should
        // be removed once we drop support for implicitly created subsources.
        text_columns: _,
        exclude_columns: _,
        seen: _,
    } = options.clone().try_into()?;
    let details = details
        .as_ref()
        .ok_or_else(|| sql_err!("internal error: MySQL source missing details"))?;
    let details = hex::decode(details).map_err(|e| sql_err!("{}", e))?;
    let details = ProtoMySqlSourceDetails::decode(&*details).map_err(|e| sql_err!("{}", e))?;
    let details = MySqlSourceDetails::from_proto(details).map_err(|e| sql_err!("{}", e))?;
    Ok(MySqlSourceConnection {
        connection: connection_item.id(),
        connection_id: connection_item.id(),
        details,
    })
}

fn plan_sqlserver_source_connection(
    scx: &StatementContext<'_>,
    connection: &ResolvedItemName,
    options: &Vec<SqlServerConfigOption<Aug>>,
) -> Result<SqlServerSourceConnection<ReferencedConnection>, PlanError> {
    let connection_item = scx.get_item_by_resolved_name(connection)?;
    match connection_item.connection()? {
        Connection::SqlServer(connection) => connection,
        _ => sql_bail!(
            "{} is not a SQL Server connection",
            scx.catalog.resolve_full_name(connection_item.name())
        ),
    };
    let SqlServerConfigOptionExtracted { details, .. } = options.clone().try_into()?;
    let details = details
        .as_ref()
        .ok_or_else(|| sql_err!("internal error: SQL Server source missing details"))?;
    let extras = hex::decode(details)
        .map_err(|e| sql_err!("{e}"))
        .and_then(|raw| ProtoSqlServerSourceExtras::decode(&*raw).map_err(|e| sql_err!("{e}")))
        .and_then(|proto| SqlServerSourceExtras::from_proto(proto).map_err(|e| sql_err!("{e}")))?;
    Ok(SqlServerSourceConnection {
        connection_id: connection_item.id(),
        connection: connection_item.id(),
        extras,
    })
}

fn plan_postgres_source_connection(
    scx: &StatementContext<'_>,
    connection: &ResolvedItemName,
    options: &Vec<PgConfigOption<Aug>>,
) -> Result<PostgresSourceConnection<ReferencedConnection>, PlanError> {
    let connection_item = scx.get_item_by_resolved_name(connection)?;
    let PgConfigOptionExtracted {
        details,
        publication,
        // text columns are already part of the source-exports and are only included
        // in these options for round-tripping of a `CREATE SOURCE` statement. This should
        // be removed once we drop support for implicitly created subsources.
        text_columns: _,
        // exclude columns are already part of the source-exports and are only included
        // in these options for round-tripping of a `CREATE SOURCE` statement. This should
        // be removed once we drop support for implicitly created subsources.
        exclude_columns: _,
        seen: _,
    } = options.clone().try_into()?;
    let details = details
        .as_ref()
        .ok_or_else(|| sql_err!("internal error: Postgres source missing details"))?;
    let details = hex::decode(details).map_err(|e| sql_err!("{}", e))?;
    let details =
        ProtoPostgresSourcePublicationDetails::decode(&*details).map_err(|e| sql_err!("{}", e))?;
    let publication_details =
        PostgresSourcePublicationDetails::from_proto(details).map_err(|e| sql_err!("{}", e))?;
    Ok(PostgresSourceConnection {
        connection: connection_item.id(),
        connection_id: connection_item.id(),
        publication: publication.expect("validated exists during purification"),
        publication_details,
    })
}

fn plan_kafka_source_connection(
    scx: &StatementContext<'_>,
    connection_name: &ResolvedItemName,
    options: &Vec<ast::KafkaSourceConfigOption<Aug>>,
    include_metadata: &Vec<SourceIncludeMetadata>,
) -> Result<KafkaSourceConnection<ReferencedConnection>, PlanError> {
    let connection_item = scx.get_item_by_resolved_name(connection_name)?;
    if !matches!(connection_item.connection()?, Connection::Kafka(_)) {
        sql_bail!(
            "{} is not a kafka connection",
            scx.catalog.resolve_full_name(connection_item.name())
        )
    }
    let KafkaSourceConfigOptionExtracted {
        group_id_prefix,
        topic,
        topic_metadata_refresh_interval,
        start_timestamp: _, // purified into `start_offset`
        start_offset,
        seen: _,
    }: KafkaSourceConfigOptionExtracted = options.clone().try_into()?;
    let topic = topic.expect("validated exists during purification");
    let mut start_offsets = BTreeMap::new();
    if let Some(offsets) = start_offset {
        for (part, offset) in offsets.iter().enumerate() {
            if *offset < 0 {
                sql_bail!("START OFFSET must be a nonnegative integer");
            }
            start_offsets.insert(i32::try_from(part)?, *offset);
        }
    }
    if topic_metadata_refresh_interval > Duration::from_secs(60 * 60) {
        // This is a librdkafka-enforced restriction that, if violated,
        // would result in a runtime error for the source.
        sql_bail!("TOPIC METADATA REFRESH INTERVAL cannot be greater than 1 hour");
    }
    let metadata_columns = include_metadata
        .into_iter()
        .flat_map(|item| match item {
            SourceIncludeMetadata::Timestamp { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "timestamp".to_owned(),
                };
                Some((name, KafkaMetadataKind::Timestamp))
            }
            SourceIncludeMetadata::Partition { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "partition".to_owned(),
                };
                Some((name, KafkaMetadataKind::Partition))
            }
            SourceIncludeMetadata::Offset { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "offset".to_owned(),
                };
                Some((name, KafkaMetadataKind::Offset))
            }
            SourceIncludeMetadata::Headers { alias } => {
                let name = match alias {
                    Some(name) => name.to_string(),
                    None => "headers".to_owned(),
                };
                Some((name, KafkaMetadataKind::Headers))
            }
            SourceIncludeMetadata::Header {
                alias,
                key,
                use_bytes,
            } => Some((
                alias.to_string(),
                KafkaMetadataKind::Header {
                    key: key.clone(),
                    use_bytes: *use_bytes,
                },
            )),
            SourceIncludeMetadata::Key { .. } => {
                // handled below
                None
            }
        })
        .collect();
    Ok(KafkaSourceConnection {
        connection: connection_item.id(),
        connection_id: connection_item.id(),
        topic,
        start_offsets,
        group_id_prefix,
        topic_metadata_refresh_interval,
        metadata_columns,
    })
}

fn apply_source_envelope_encoding(
    scx: &StatementContext,
    envelope: &ast::SourceEnvelope,
    format: &Option<FormatSpecifier<Aug>>,
    key_desc: Option<RelationDesc>,
    value_desc: RelationDesc,
    include_metadata: &[SourceIncludeMetadata],
    metadata_columns_desc: Vec<(&str, SqlColumnType)>,
    source_connection: &GenericSourceConnection<ReferencedConnection>,
) -> Result<
    (
        RelationDesc,
        SourceEnvelope,
        Option<SourceDataEncoding<ReferencedConnection>>,
    ),
    PlanError,
> {
    let encoding = match format {
        Some(format) => Some(get_encoding(scx, format, envelope)?),
        None => None,
    };

    let (key_desc, value_desc) = match &encoding {
        Some(encoding) => {
            // If we are applying an encoding we need to ensure that the incoming value_desc is a
            // single column of type bytes.
            match value_desc.typ().columns() {
                [typ] => match typ.scalar_type {
                    SqlScalarType::Bytes => {}
                    _ => sql_bail!(
                        "The schema produced by the source is incompatible with format decoding"
                    ),
                },
                _ => sql_bail!(
                    "The schema produced by the source is incompatible with format decoding"
                ),
            }

            let (key_desc, value_desc) = encoding.desc()?;

            // TODO(petrosagg): This piece of code seems to be making a statement about the
            // nullability of the NONE envelope when the source is Kafka. As written, the code
            // misses opportunities to mark columns as not nullable and is over conservative. For
            // example in the case of `FORMAT BYTES ENVELOPE NONE` the output is indeed
            // non-nullable but we will mark it as nullable anyway. This kind of crude reasoning
            // should be replaced with precise type-level reasoning.
            let key_desc = key_desc.map(|desc| {
                let is_kafka = matches!(source_connection, GenericSourceConnection::Kafka(_));
                let is_envelope_none = matches!(envelope, ast::SourceEnvelope::None);
                if is_kafka && is_envelope_none {
                    RelationDesc::from_names_and_types(
                        desc.into_iter()
                            .map(|(name, typ)| (name, typ.nullable(true))),
                    )
                } else {
                    desc
                }
            });
            (key_desc, value_desc)
        }
        None => (key_desc, value_desc),
    };

    // KEY VALUE load generators are the only UPSERT source that
    // has no encoding but defaults to `INCLUDE KEY`.
    //
    // As discussed
    // <https://github.com/MaterializeInc/materialize/pull/26246#issuecomment-2023558097>,
    // removing this special case amounts to deciding how to handle null keys
    // from sources, in a holistic way. We aren't yet prepared to do this, so we leave
    // this special case in.
    //
    // Note that this is safe because this generator is
    // 1. The only source with no encoding that can have its key included.
    // 2. Never produces null keys (or values, for that matter).
    let key_envelope_no_encoding = matches!(
        source_connection,
        GenericSourceConnection::LoadGenerator(LoadGeneratorSourceConnection {
            load_generator: LoadGenerator::KeyValue(_),
            ..
        })
    );
    let mut key_envelope = get_key_envelope(
        include_metadata,
        encoding.as_ref(),
        key_envelope_no_encoding,
    )?;

    match (&envelope, &key_envelope) {
        (ast::SourceEnvelope::Debezium, KeyEnvelope::None) => {}
        (ast::SourceEnvelope::Debezium, _) => sql_bail!(
            "Cannot use INCLUDE KEY with ENVELOPE DEBEZIUM: Debezium values include all keys."
        ),
        _ => {}
    };

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
        ast::SourceEnvelope::None => UnplannedSourceEnvelope::None(key_envelope),
        ast::SourceEnvelope::Debezium => {
            //TODO check that key envelope is not set
            let after_idx = match typecheck_debezium(&value_desc) {
                Ok((_before_idx, after_idx)) => Ok(after_idx),
                Err(type_err) => match encoding.as_ref().map(|e| &e.value) {
                    Some(DataEncoding::Avro(_)) => Err(type_err),
                    _ => Err(sql_err!(
                        "ENVELOPE DEBEZIUM requires that VALUE FORMAT is set to AVRO"
                    )),
                },
            }?;

            UnplannedSourceEnvelope::Upsert {
                style: UpsertStyle::Debezium { after_idx },
            }
        }
        ast::SourceEnvelope::Upsert {
            value_decode_err_policy,
        } => {
            let key_encoding = match encoding.as_ref().and_then(|e| e.key.as_ref()) {
                None => {
                    if !key_envelope_no_encoding {
                        bail_unsupported!(format!(
                            "UPSERT requires a key/value format: {:?}",
                            format
                        ))
                    }
                    None
                }
                Some(key_encoding) => Some(key_encoding),
            };
            // `ENVELOPE UPSERT` implies `INCLUDE KEY`, if it is not explicitly
            // specified.
            if key_envelope == KeyEnvelope::None {
                key_envelope = get_unnamed_key_envelope(key_encoding)?;
            }
            // If the value decode error policy is not set we use the default upsert style.
            let style = match value_decode_err_policy.as_slice() {
                [] => UpsertStyle::Default(key_envelope),
                [SourceErrorPolicy::Inline { alias }] => {
                    scx.require_feature_flag(&vars::ENABLE_ENVELOPE_UPSERT_INLINE_ERRORS)?;
                    UpsertStyle::ValueErrInline {
                        key_envelope,
                        error_column: alias
                            .as_ref()
                            .map_or_else(|| "error".to_string(), |a| a.to_string()),
                    }
                }
                _ => {
                    bail_unsupported!("ENVELOPE UPSERT with unsupported value decode error policy")
                }
            };

            UnplannedSourceEnvelope::Upsert { style }
        }
        ast::SourceEnvelope::CdcV2 => {
            scx.require_feature_flag(&vars::ENABLE_ENVELOPE_MATERIALIZE)?;
            //TODO check that key envelope is not set
            match format {
                Some(FormatSpecifier::Bare(Format::Avro(_))) => {}
                _ => bail_unsupported!("non-Avro-encoded ENVELOPE MATERIALIZE"),
            }
            UnplannedSourceEnvelope::CdcV2
        }
    };

    let metadata_desc = included_column_desc(metadata_columns_desc);
    let (envelope, desc) = envelope.desc(key_desc, value_desc, metadata_desc)?;

    Ok((desc, envelope, encoding))
}

/// Plans the RelationDesc for a source export (subsource or table) that has a defined list
/// of columns and constraints.
fn plan_source_export_desc(
    scx: &StatementContext,
    name: &UnresolvedItemName,
    columns: &Vec<ColumnDef<Aug>>,
    constraints: &Vec<TableConstraint<Aug>>,
) -> Result<RelationDesc, PlanError> {
    let names: Vec<_> = columns
        .iter()
        .map(|c| normalize::column_name(c.name.clone()))
        .collect();

    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.quoted());
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
                    bail_unsupported!("Source export with default value")
                }
                ColumnOption::Unique { is_primary } => {
                    keys.push(vec![i]);
                    if *is_primary {
                        nullable = false;
                    }
                }
                other => {
                    bail_unsupported!(format!("Source export with column constraint: {}", other))
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
                        "multiple primary keys for source export {} are not allowed",
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
                bail_unsupported!("Source export with a foreign key")
            }
            TableConstraint::Check { .. } => {
                bail_unsupported!("Source export with a check constraint")
            }
        }
    }

    let typ = SqlRelationType::new(column_types).with_keys(keys);
    let desc = RelationDesc::new(typ, names);
    Ok(desc)
}

generate_extracted_config!(
    CreateSubsourceOption,
    (Progress, bool, Default(false)),
    (ExternalReference, UnresolvedItemName),
    (RetainHistory, OptionalDuration),
    (TextColumns, Vec::<Ident>, Default(vec![])),
    (ExcludeColumns, Vec::<Ident>, Default(vec![])),
    (Details, String)
);

pub fn plan_create_subsource(
    scx: &StatementContext,
    stmt: CreateSubsourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSubsourceStatement {
        name,
        columns,
        of_source,
        constraints,
        if_not_exists,
        with_options,
    } = &stmt;

    let CreateSubsourceOptionExtracted {
        progress,
        retain_history,
        external_reference,
        text_columns,
        exclude_columns,
        details,
        seen: _,
    } = with_options.clone().try_into()?;

    // This invariant is enforced during purification; we are responsible for
    // creating the AST for subsources as a response to CREATE SOURCE
    // statements, so this would fire in integration testing if we failed to
    // uphold it.
    assert!(
        progress ^ (external_reference.is_some() && of_source.is_some()),
        "CREATE SUBSOURCE statement must specify either PROGRESS or REFERENCES option"
    );

    let desc = plan_source_export_desc(scx, name, columns, constraints)?;

    let data_source = if let Some(source_reference) = of_source {
        // If the new source table syntax is forced we should not be creating any non-progress
        // subsources.
        if scx.catalog.system_vars().enable_create_table_from_source()
            && scx.catalog.system_vars().force_source_table_syntax()
        {
            Err(PlanError::UseTablesForSources(
                "CREATE SUBSOURCE".to_string(),
            ))?;
        }

        // This is a subsource with the "natural" dependency order, i.e. it is
        // not a legacy subsource with the inverted structure.
        let ingestion_id = *source_reference.item_id();
        let external_reference = external_reference.unwrap();

        // Decode the details option stored on the subsource statement, which contains information
        // created during the purification process.
        let details = details
            .as_ref()
            .ok_or_else(|| sql_err!("internal error: source-export subsource missing details"))?;
        let details = hex::decode(details).map_err(|e| sql_err!("{}", e))?;
        let details =
            ProtoSourceExportStatementDetails::decode(&*details).map_err(|e| sql_err!("{}", e))?;
        let details =
            SourceExportStatementDetails::from_proto(details).map_err(|e| sql_err!("{}", e))?;
        let details = match details {
            SourceExportStatementDetails::Postgres { table } => {
                SourceExportDetails::Postgres(PostgresSourceExportDetails {
                    column_casts: crate::pure::postgres::generate_column_casts(
                        scx,
                        &table,
                        &text_columns,
                    )?,
                    table,
                })
            }
            SourceExportStatementDetails::MySql {
                table,
                initial_gtid_set,
            } => SourceExportDetails::MySql(MySqlSourceExportDetails {
                table,
                initial_gtid_set,
                text_columns: text_columns.into_iter().map(|c| c.into_string()).collect(),
                exclude_columns: exclude_columns
                    .into_iter()
                    .map(|c| c.into_string())
                    .collect(),
            }),
            SourceExportStatementDetails::SqlServer {
                table,
                capture_instance,
                initial_lsn,
            } => SourceExportDetails::SqlServer(SqlServerSourceExportDetails {
                capture_instance,
                table,
                initial_lsn,
                text_columns: text_columns.into_iter().map(|c| c.into_string()).collect(),
                exclude_columns: exclude_columns
                    .into_iter()
                    .map(|c| c.into_string())
                    .collect(),
            }),
            SourceExportStatementDetails::LoadGenerator { output } => {
                SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails { output })
            }
            SourceExportStatementDetails::Kafka {} => {
                bail_unsupported!("subsources cannot reference Kafka sources")
            }
        };
        DataSourceDesc::IngestionExport {
            ingestion_id,
            external_reference,
            details,
            // Subsources don't currently support non-default envelopes / encoding
            data_config: SourceExportDataConfig {
                envelope: SourceEnvelope::None(NoneEnvelope {
                    key_envelope: KeyEnvelope::None,
                    key_arity: 0,
                }),
                encoding: None,
            },
        }
    } else if progress {
        DataSourceDesc::Progress
    } else {
        panic!("subsources must specify one of `external_reference`, `progress`, or `references`")
    };

    let if_not_exists = *if_not_exists;
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name.clone())?)?;

    let create_sql = normalize::create_statement(scx, Statement::CreateSubsource(stmt))?;

    let compaction_window = plan_retain_history_option(scx, retain_history)?;
    let source = Source {
        create_sql,
        data_source,
        desc,
        compaction_window,
    };

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        timeline: Timeline::EpochMilliseconds,
        in_cluster: None,
    }))
}

generate_extracted_config!(
    TableFromSourceOption,
    (TextColumns, Vec::<Ident>, Default(vec![])),
    (ExcludeColumns, Vec::<Ident>, Default(vec![])),
    (PartitionBy, Vec<Ident>),
    (RetainHistory, OptionalDuration),
    (Details, String)
);

pub fn plan_create_table_from_source(
    scx: &StatementContext,
    stmt: CreateTableFromSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    if !scx.catalog.system_vars().enable_create_table_from_source() {
        sql_bail!("CREATE TABLE ... FROM SOURCE is not supported");
    }

    let CreateTableFromSourceStatement {
        name,
        columns,
        constraints,
        if_not_exists,
        source,
        external_reference,
        envelope,
        format,
        include_metadata,
        with_options,
    } = &stmt;

    let envelope = envelope.clone().unwrap_or(ast::SourceEnvelope::None);

    let TableFromSourceOptionExtracted {
        text_columns,
        exclude_columns,
        retain_history,
        partition_by,
        details,
        seen: _,
    } = with_options.clone().try_into()?;

    let source_item = scx.get_item_by_resolved_name(source)?;
    let ingestion_id = source_item.id();

    // Decode the details option stored on the statement, which contains information
    // created during the purification process.
    let details = details
        .as_ref()
        .ok_or_else(|| sql_err!("internal error: source-export missing details"))?;
    let details = hex::decode(details).map_err(|e| sql_err!("{}", e))?;
    let details =
        ProtoSourceExportStatementDetails::decode(&*details).map_err(|e| sql_err!("{}", e))?;
    let details =
        SourceExportStatementDetails::from_proto(details).map_err(|e| sql_err!("{}", e))?;

    if !matches!(details, SourceExportStatementDetails::Kafka { .. })
        && include_metadata
            .iter()
            .any(|sic| matches!(sic, SourceIncludeMetadata::Headers { .. }))
    {
        // TODO(guswynn): should this be `bail_unsupported!`?
        sql_bail!("INCLUDE HEADERS with non-Kafka source table not supported");
    }
    if !matches!(
        details,
        SourceExportStatementDetails::Kafka { .. }
            | SourceExportStatementDetails::LoadGenerator { .. }
    ) && !include_metadata.is_empty()
    {
        bail_unsupported!("INCLUDE metadata with non-Kafka source table");
    }

    let details = match details {
        SourceExportStatementDetails::Postgres { table } => {
            SourceExportDetails::Postgres(PostgresSourceExportDetails {
                column_casts: crate::pure::postgres::generate_column_casts(
                    scx,
                    &table,
                    &text_columns,
                )?,
                table,
            })
        }
        SourceExportStatementDetails::MySql {
            table,
            initial_gtid_set,
        } => SourceExportDetails::MySql(MySqlSourceExportDetails {
            table,
            initial_gtid_set,
            text_columns: text_columns.into_iter().map(|c| c.into_string()).collect(),
            exclude_columns: exclude_columns
                .into_iter()
                .map(|c| c.into_string())
                .collect(),
        }),
        SourceExportStatementDetails::SqlServer {
            table,
            capture_instance,
            initial_lsn,
        } => SourceExportDetails::SqlServer(SqlServerSourceExportDetails {
            table,
            capture_instance,
            initial_lsn,
            text_columns: text_columns.into_iter().map(|c| c.into_string()).collect(),
            exclude_columns: exclude_columns
                .into_iter()
                .map(|c| c.into_string())
                .collect(),
        }),
        SourceExportStatementDetails::LoadGenerator { output } => {
            SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails { output })
        }
        SourceExportStatementDetails::Kafka {} => {
            if !include_metadata.is_empty()
                && !matches!(
                    envelope,
                    ast::SourceEnvelope::Upsert { .. }
                        | ast::SourceEnvelope::None
                        | ast::SourceEnvelope::Debezium
                )
            {
                // TODO(guswynn): should this be `bail_unsupported!`?
                sql_bail!("INCLUDE <metadata> requires ENVELOPE (NONE|UPSERT|DEBEZIUM)");
            }

            let metadata_columns = include_metadata
                .into_iter()
                .flat_map(|item| match item {
                    SourceIncludeMetadata::Timestamp { alias } => {
                        let name = match alias {
                            Some(name) => name.to_string(),
                            None => "timestamp".to_owned(),
                        };
                        Some((name, KafkaMetadataKind::Timestamp))
                    }
                    SourceIncludeMetadata::Partition { alias } => {
                        let name = match alias {
                            Some(name) => name.to_string(),
                            None => "partition".to_owned(),
                        };
                        Some((name, KafkaMetadataKind::Partition))
                    }
                    SourceIncludeMetadata::Offset { alias } => {
                        let name = match alias {
                            Some(name) => name.to_string(),
                            None => "offset".to_owned(),
                        };
                        Some((name, KafkaMetadataKind::Offset))
                    }
                    SourceIncludeMetadata::Headers { alias } => {
                        let name = match alias {
                            Some(name) => name.to_string(),
                            None => "headers".to_owned(),
                        };
                        Some((name, KafkaMetadataKind::Headers))
                    }
                    SourceIncludeMetadata::Header {
                        alias,
                        key,
                        use_bytes,
                    } => Some((
                        alias.to_string(),
                        KafkaMetadataKind::Header {
                            key: key.clone(),
                            use_bytes: *use_bytes,
                        },
                    )),
                    SourceIncludeMetadata::Key { .. } => {
                        // handled below
                        None
                    }
                })
                .collect();

            SourceExportDetails::Kafka(KafkaSourceExportDetails { metadata_columns })
        }
    };

    let source_connection = &source_item.source_desc()?.expect("is source").connection;

    // Some source-types (e.g. postgres, mysql, multi-output load-gen sources) define a value_schema
    // during purification and define the `columns` and `constraints` fields for the statement,
    // whereas other source-types (e.g. kafka, single-output load-gen sources) do not, so instead
    // we use the source connection's default schema.
    let (key_desc, value_desc) =
        if matches!(columns, TableFromSourceColumns::Defined(_)) || !constraints.is_empty() {
            let columns = match columns {
                TableFromSourceColumns::Defined(columns) => columns,
                _ => unreachable!(),
            };
            let desc = plan_source_export_desc(scx, name, columns, constraints)?;
            (None, desc)
        } else {
            let key_desc = source_connection.default_key_desc();
            let value_desc = source_connection.default_value_desc();
            (Some(key_desc), value_desc)
        };

    let metadata_columns_desc = match &details {
        SourceExportDetails::Kafka(KafkaSourceExportDetails {
            metadata_columns, ..
        }) => kafka_metadata_columns_desc(metadata_columns),
        _ => vec![],
    };

    let (mut desc, envelope, encoding) = apply_source_envelope_encoding(
        scx,
        &envelope,
        format,
        key_desc,
        value_desc,
        include_metadata,
        metadata_columns_desc,
        source_connection,
    )?;
    if let TableFromSourceColumns::Named(col_names) = columns {
        plan_utils::maybe_rename_columns(format!("source table {}", name), &mut desc, col_names)?;
    }

    let names: Vec<_> = desc.iter_names().cloned().collect();
    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.quoted());
    }

    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name.clone())?)?;

    // Allow users to specify a timeline. If they do not, determine a default
    // timeline for the source.
    let timeline = match envelope {
        SourceEnvelope::CdcV2 => {
            Timeline::External(scx.catalog.resolve_full_name(&name).to_string())
        }
        _ => Timeline::EpochMilliseconds,
    };

    if let Some(partition_by) = partition_by {
        scx.require_feature_flag(&ENABLE_COLLECTION_PARTITION_BY)?;
        check_partition_by(&desc, partition_by)?;
    }

    let data_source = DataSourceDesc::IngestionExport {
        ingestion_id,
        external_reference: external_reference
            .as_ref()
            .expect("populated in purification")
            .clone(),
        details,
        data_config: SourceExportDataConfig { envelope, encoding },
    };

    let if_not_exists = *if_not_exists;

    let create_sql = normalize::create_statement(scx, Statement::CreateTableFromSource(stmt))?;

    let compaction_window = plan_retain_history_option(scx, retain_history)?;
    let table = Table {
        create_sql,
        desc: VersionedRelationDesc::new(desc),
        temporary: false,
        compaction_window,
        data_source: TableDataSource::DataSource {
            desc: data_source,
            timeline,
        },
    };

    Ok(Plan::CreateTable(CreateTablePlan {
        name,
        table,
        if_not_exists,
    }))
}

generate_extracted_config!(
    LoadGeneratorOption,
    (TickInterval, Duration),
    (AsOf, u64, Default(0_u64)),
    (UpTo, u64, Default(u64::MAX)),
    (ScaleFactor, f64),
    (MaxCardinality, u64),
    (Keys, u64),
    (SnapshotRounds, u64),
    (TransactionalSnapshot, bool),
    (ValueSize, u64),
    (Seed, u64),
    (Partitions, u64),
    (BatchSize, u64)
);

impl LoadGeneratorOptionExtracted {
    pub(super) fn ensure_only_valid_options(
        &self,
        loadgen: &ast::LoadGenerator,
    ) -> Result<(), PlanError> {
        use mz_sql_parser::ast::LoadGeneratorOptionName::*;

        let mut options = self.seen.clone();

        let permitted_options: &[_] = match loadgen {
            ast::LoadGenerator::Auction => &[TickInterval, AsOf, UpTo],
            ast::LoadGenerator::Clock => &[TickInterval, AsOf, UpTo],
            ast::LoadGenerator::Counter => &[TickInterval, AsOf, UpTo, MaxCardinality],
            ast::LoadGenerator::Marketing => &[TickInterval, AsOf, UpTo],
            ast::LoadGenerator::Datums => &[TickInterval, AsOf, UpTo],
            ast::LoadGenerator::Tpch => &[TickInterval, AsOf, UpTo, ScaleFactor],
            ast::LoadGenerator::KeyValue => &[
                TickInterval,
                Keys,
                SnapshotRounds,
                TransactionalSnapshot,
                ValueSize,
                Seed,
                Partitions,
                BatchSize,
            ],
        };

        for o in permitted_options {
            options.remove(o);
        }

        if !options.is_empty() {
            sql_bail!(
                "{} load generators do not support {} values",
                loadgen,
                options.iter().join(", ")
            )
        }

        Ok(())
    }
}

pub(crate) fn load_generator_ast_to_generator(
    scx: &StatementContext,
    loadgen: &ast::LoadGenerator,
    options: &[LoadGeneratorOption<Aug>],
    include_metadata: &[SourceIncludeMetadata],
) -> Result<LoadGenerator, PlanError> {
    let extracted: LoadGeneratorOptionExtracted = options.to_vec().try_into()?;
    extracted.ensure_only_valid_options(loadgen)?;

    if loadgen != &ast::LoadGenerator::KeyValue && !include_metadata.is_empty() {
        sql_bail!("INCLUDE metadata only supported with `KEY VALUE` load generators");
    }

    let load_generator = match loadgen {
        ast::LoadGenerator::Auction => LoadGenerator::Auction,
        ast::LoadGenerator::Clock => {
            scx.require_feature_flag(&vars::ENABLE_LOAD_GENERATOR_CLOCK)?;
            LoadGenerator::Clock
        }
        ast::LoadGenerator::Counter => {
            scx.require_feature_flag(&vars::ENABLE_LOAD_GENERATOR_COUNTER)?;
            let LoadGeneratorOptionExtracted {
                max_cardinality, ..
            } = extracted;
            LoadGenerator::Counter { max_cardinality }
        }
        ast::LoadGenerator::Marketing => LoadGenerator::Marketing,
        ast::LoadGenerator::Datums => {
            scx.require_feature_flag(&vars::ENABLE_LOAD_GENERATOR_DATUMS)?;
            LoadGenerator::Datums
        }
        ast::LoadGenerator::Tpch => {
            let LoadGeneratorOptionExtracted { scale_factor, .. } = extracted;

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
        mz_sql_parser::ast::LoadGenerator::KeyValue => {
            scx.require_feature_flag(&vars::ENABLE_LOAD_GENERATOR_KEY_VALUE)?;
            let LoadGeneratorOptionExtracted {
                keys,
                snapshot_rounds,
                transactional_snapshot,
                value_size,
                tick_interval,
                seed,
                partitions,
                batch_size,
                ..
            } = extracted;

            let mut include_offset = None;
            for im in include_metadata {
                match im {
                    SourceIncludeMetadata::Offset { alias } => {
                        include_offset = match alias {
                            Some(alias) => Some(alias.to_string()),
                            None => Some(LOAD_GENERATOR_KEY_VALUE_OFFSET_DEFAULT.to_string()),
                        }
                    }
                    SourceIncludeMetadata::Key { .. } => continue,

                    _ => {
                        sql_bail!("only `INCLUDE OFFSET` and `INCLUDE KEY` is supported");
                    }
                };
            }

            let lgkv = KeyValueLoadGenerator {
                keys: keys.ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires KEYS"))?,
                snapshot_rounds: snapshot_rounds
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires SNAPSHOT ROUNDS"))?,
                // Defaults to true.
                transactional_snapshot: transactional_snapshot.unwrap_or(true),
                value_size: value_size
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires VALUE SIZE"))?,
                partitions: partitions
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires PARTITIONS"))?,
                tick_interval,
                batch_size: batch_size
                    .ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires BATCH SIZE"))?,
                seed: seed.ok_or_else(|| sql_err!("LOAD GENERATOR KEY VALUE requires SEED"))?,
                include_offset,
            };

            if lgkv.keys == 0
                || lgkv.partitions == 0
                || lgkv.value_size == 0
                || lgkv.batch_size == 0
            {
                sql_bail!("LOAD GENERATOR KEY VALUE options must be non-zero")
            }

            if lgkv.keys % lgkv.partitions != 0 {
                sql_bail!("KEYS must be a multiple of PARTITIONS")
            }

            if lgkv.batch_size > lgkv.keys {
                sql_bail!("KEYS must be larger than BATCH SIZE")
            }

            // This constraints simplifies the source implementation.
            // We can lift it later.
            if (lgkv.keys / lgkv.partitions) % lgkv.batch_size != 0 {
                sql_bail!("PARTITIONS * BATCH SIZE must be a divisor of KEYS")
            }

            if lgkv.snapshot_rounds == 0 {
                sql_bail!("SNAPSHOT ROUNDS must be larger than 0")
            }

            LoadGenerator::KeyValue(lgkv)
        }
    };

    Ok(load_generator)
}

fn typecheck_debezium(value_desc: &RelationDesc) -> Result<(Option<usize>, usize), PlanError> {
    let before = value_desc.get_by_name(&"before".into());
    let (after_idx, after_ty) = value_desc
        .get_by_name(&"after".into())
        .ok_or_else(|| sql_err!("'after' column missing from debezium input"))?;
    let before_idx = if let Some((before_idx, before_ty)) = before {
        if !matches!(before_ty.scalar_type, SqlScalarType::Record { .. }) {
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
    format: &FormatSpecifier<Aug>,
    envelope: &ast::SourceEnvelope,
) -> Result<SourceDataEncoding<ReferencedConnection>, PlanError> {
    let encoding = match format {
        FormatSpecifier::Bare(format) => get_encoding_inner(scx, format)?,
        FormatSpecifier::KeyValue { key, value } => {
            let key = {
                let encoding = get_encoding_inner(scx, key)?;
                Some(encoding.key.unwrap_or(encoding.value))
            };
            let value = get_encoding_inner(scx, value)?.value;
            SourceDataEncoding { key, value }
        }
    };

    let requires_keyvalue = matches!(
        envelope,
        ast::SourceEnvelope::Debezium | ast::SourceEnvelope::Upsert { .. }
    );
    let is_keyvalue = encoding.key.is_some();
    if requires_keyvalue && !is_keyvalue {
        sql_bail!("ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified");
    };

    Ok(encoding)
}

/// Determine the cluster ID to use for this item.
///
/// If `in_cluster` is `None` we will update it to refer to the default cluster.
/// Because of this, do not normalize/canonicalize the create SQL statement
/// until after calling this function.
fn source_sink_cluster_config<'a, 'ctx>(
    scx: &'a StatementContext<'ctx>,
    in_cluster: &mut Option<ResolvedClusterName>,
) -> Result<&'a dyn CatalogCluster<'ctx>, PlanError> {
    let cluster = match in_cluster {
        None => {
            let cluster = scx.catalog.resolve_cluster(None)?;
            *in_cluster = Some(ResolvedClusterName {
                id: cluster.id(),
                print_name: None,
            });
            cluster
        }
        Some(in_cluster) => scx.catalog.get_cluster(in_cluster.id),
    };

    Ok(cluster)
}

generate_extracted_config!(AvroSchemaOption, (ConfluentWireFormat, bool, Default(true)));

#[derive(Debug)]
pub struct Schema {
    pub key_schema: Option<String>,
    pub value_schema: String,
    pub csr_connection: Option<<ReferencedConnection as ConnectionAccess>::Csr>,
    pub confluent_wire_format: bool,
}

fn get_encoding_inner(
    scx: &StatementContext,
    format: &Format<Aug>,
) -> Result<SourceDataEncoding<ReferencedConnection>, PlanError> {
    let value = match format {
        Format::Bytes => DataEncoding::Bytes,
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
                    schema: ast::Schema { schema },
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
                        Connection::Csr(_) => item.id(),
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
                return Ok(SourceDataEncoding {
                    key: Some(DataEncoding::Avro(AvroEncoding {
                        schema: key_schema,
                        csr_connection: csr_connection.clone(),
                        confluent_wire_format,
                    })),
                    value: DataEncoding::Avro(AvroEncoding {
                        schema: value_schema,
                        csr_connection,
                        confluent_wire_format,
                    }),
                });
            } else {
                DataEncoding::Avro(AvroEncoding {
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

                    let value = DataEncoding::Protobuf(ProtobufEncoding {
                        descriptors: strconv::parse_bytes(&value.schema)?,
                        message_name: value.message_name.clone(),
                        confluent_wire_format: true,
                    });
                    if let Some(key) = key {
                        return Ok(SourceDataEncoding {
                            key: Some(DataEncoding::Protobuf(ProtobufEncoding {
                                descriptors: strconv::parse_bytes(&key.schema)?,
                                message_name: key.message_name.clone(),
                                confluent_wire_format: true,
                            })),
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
                schema: ast::Schema { schema },
            } => {
                let descriptors = strconv::parse_bytes(schema)?;

                DataEncoding::Protobuf(ProtobufEncoding {
                    descriptors,
                    message_name: message_name.to_owned(),
                    confluent_wire_format: false,
                })
            }
        },
        Format::Regex(regex) => DataEncoding::Regex(RegexEncoding {
            regex: mz_repr::adt::regex::Regex::new(regex, false)
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
            DataEncoding::Csv(CsvEncoding {
                columns,
                delimiter: u8::try_from(*delimiter)
                    .map_err(|_| sql_err!("CSV delimiter must be an ASCII character"))?,
            })
        }
        Format::Json { array: false } => DataEncoding::Json,
        Format::Json { array: true } => bail_unsupported!("JSON ARRAY format in sources"),
        Format::Text => DataEncoding::Text,
    };
    Ok(SourceDataEncoding { key: None, value })
}

/// Extract the key envelope, if it is requested
fn get_key_envelope(
    included_items: &[SourceIncludeMetadata],
    encoding: Option<&SourceDataEncoding<ReferencedConnection>>,
    key_envelope_no_encoding: bool,
) -> Result<KeyEnvelope, PlanError> {
    let key_definition = included_items
        .iter()
        .find(|i| matches!(i, SourceIncludeMetadata::Key { .. }));
    if let Some(SourceIncludeMetadata::Key { alias }) = key_definition {
        match (alias, encoding.and_then(|e| e.key.as_ref())) {
            (Some(name), Some(_)) => Ok(KeyEnvelope::Named(name.as_str().to_string())),
            (None, Some(key)) => get_unnamed_key_envelope(Some(key)),
            (Some(name), _) if key_envelope_no_encoding => {
                Ok(KeyEnvelope::Named(name.as_str().to_string()))
            }
            (None, _) if key_envelope_no_encoding => get_unnamed_key_envelope(None),
            (_, None) => {
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
    key: Option<&DataEncoding<ReferencedConnection>>,
) -> Result<KeyEnvelope, PlanError> {
    // If the key is requested but comes from an unnamed type then it gets the name "key"
    //
    // Otherwise it gets the names of the columns in the type
    let is_composite = match key {
        Some(DataEncoding::Bytes | DataEncoding::Json | DataEncoding::Text) => false,
        Some(
            DataEncoding::Avro(_)
            | DataEncoding::Csv(_)
            | DataEncoding::Protobuf(_)
            | DataEncoding::Regex { .. },
        ) => true,
        None => false,
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

    let query::PlannedRootQuery {
        expr,
        mut desc,
        finishing,
        scope: _,
    } = query::plan_root_query(scx, query.clone(), QueryLifetime::View)?;
    // We get back a trivial finishing, because `plan_root_query` applies the given finishing.
    // Note: Earlier, we were thinking to maybe persist the finishing information with the view
    // here to help with database-issues#236. However, in the meantime, there might be a better
    // approach to solve database-issues#236:
    // https://github.com/MaterializeInc/database-issues/issues/236#issuecomment-1688293709
    assert!(HirRelationExpr::is_trivial_row_set_finishing_hir(
        &finishing,
        expr.arity()
    ));
    if expr.contains_parameters()? {
        return Err(PlanError::ParameterNotAllowed("views".to_string()));
    }

    let dependencies = expr
        .depends_on()
        .into_iter()
        .map(|gid| scx.catalog.resolve_item_id(&gid))
        .collect();

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
        sql_bail!("column {} specified more than once", dup.quoted());
    }

    let view = View {
        create_sql,
        expr,
        dependencies,
        column_names: names,
        temporary,
    };

    Ok((name, view))
}

pub fn plan_create_view(
    scx: &StatementContext,
    mut stmt: CreateViewStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateViewStatement {
        temporary,
        if_exists,
        definition,
    } = &mut stmt;
    let (name, view) = plan_view(scx, definition, *temporary)?;

    // Override the statement-level IfExistsBehavior with Skip if this is
    // explicitly requested in the PlanContext (the default is `false`).
    let ignore_if_exists_errors = scx.pcx().map_or(false, |pcx| pcx.ignore_if_exists_errors);

    let replace = if *if_exists == IfExistsBehavior::Replace && !ignore_if_exists_errors {
        let if_exists = true;
        let cascade = false;
        let maybe_item_to_drop = plan_drop_item(
            scx,
            ObjectType::View,
            if_exists,
            definition.name.clone(),
            cascade,
        )?;

        // Check if the new View depends on the item that we would be replacing.
        if let Some(id) = maybe_item_to_drop {
            let dependencies = view.expr.depends_on();
            let invalid_drop = scx
                .get_item(&id)
                .global_ids()
                .any(|gid| dependencies.contains(&gid));
            if invalid_drop {
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
    // For PostgreSQL compatibility, we need to prevent creating views when
    // there is an existing object *or* type of the same name.
    if let (Ok(item), IfExistsBehavior::Error, false) = (
        scx.catalog.resolve_item_or_type(&partial_name),
        *if_exists,
        ignore_if_exists_errors,
    ) {
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

pub fn describe_create_continual_task(
    _: &StatementContext,
    _: CreateContinualTaskStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn describe_create_network_policy(
    _: &StatementContext,
    _: CreateNetworkPolicyStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn describe_alter_network_policy(
    _: &StatementContext,
    _: AlterNetworkPolicyStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_materialized_view(
    scx: &StatementContext,
    mut stmt: CreateMaterializedViewStatement<Aug>,
) -> Result<Plan, PlanError> {
    let cluster_id =
        crate::plan::statement::resolve_cluster_for_materialized_view(scx.catalog, &stmt)?;
    stmt.in_cluster = Some(ResolvedClusterName {
        id: cluster_id,
        print_name: None,
    });

    let create_sql =
        normalize::create_statement(scx, Statement::CreateMaterializedView(stmt.clone()))?;

    let partial_name = normalize::unresolved_item_name(stmt.name)?;
    let name = scx.allocate_qualified_name(partial_name.clone())?;

    // Validate the replacement target, if one is given.
    let replacement_target = if let Some(target_name) = &stmt.replacing {
        scx.require_feature_flag(&vars::ENABLE_REPLACEMENT_MATERIALIZED_VIEWS)?;

        let target = scx.get_item_by_resolved_name(target_name)?;
        if target.item_type() != CatalogItemType::MaterializedView {
            return Err(PlanError::InvalidReplacement {
                item_type: target.item_type(),
                item_name: scx.catalog.minimal_qualification(target.name()),
                replacement_type: CatalogItemType::MaterializedView,
                replacement_name: partial_name,
            });
        }
        if target.id().is_system() {
            sql_bail!(
                "cannot replace {} because it is required by the database system",
                scx.catalog.minimal_qualification(target.name()),
            );
        }
        Some(target.id())
    } else {
        None
    };

    let query::PlannedRootQuery {
        expr,
        mut desc,
        finishing,
        scope: _,
    } = query::plan_root_query(scx, stmt.query, QueryLifetime::MaterializedView)?;
    // We get back a trivial finishing, see comment in `plan_view`.
    assert!(HirRelationExpr::is_trivial_row_set_finishing_hir(
        &finishing,
        expr.arity()
    ));
    if expr.contains_parameters()? {
        return Err(PlanError::ParameterNotAllowed(
            "materialized views".to_string(),
        ));
    }

    plan_utils::maybe_rename_columns(
        format!("materialized view {}", scx.catalog.resolve_full_name(&name)),
        &mut desc,
        &stmt.columns,
    )?;
    let column_names: Vec<ColumnName> = desc.iter_names().cloned().collect();

    let MaterializedViewOptionExtracted {
        assert_not_null,
        partition_by,
        retain_history,
        refresh,
        seen: _,
    }: MaterializedViewOptionExtracted = stmt.with_options.try_into()?;

    if let Some(partition_by) = partition_by {
        scx.require_feature_flag(&ENABLE_COLLECTION_PARTITION_BY)?;
        check_partition_by(&desc, partition_by)?;
    }

    let refresh_schedule = {
        let mut refresh_schedule = RefreshSchedule::default();
        let mut on_commits_seen = 0;
        for refresh_option_value in refresh {
            if !matches!(refresh_option_value, RefreshOptionValue::OnCommit) {
                scx.require_feature_flag(&ENABLE_REFRESH_EVERY_MVS)?;
            }
            match refresh_option_value {
                RefreshOptionValue::OnCommit => {
                    on_commits_seen += 1;
                }
                RefreshOptionValue::AtCreation => {
                    soft_panic_or_log!("REFRESH AT CREATION should have been purified away");
                    sql_bail!("INTERNAL ERROR: REFRESH AT CREATION should have been purified away")
                }
                RefreshOptionValue::At(RefreshAtOptionValue { mut time }) => {
                    transform_ast::transform(scx, &mut time)?; // Desugar the expression
                    let ecx = &ExprContext {
                        qcx: &QueryContext::root(scx, QueryLifetime::OneShot),
                        name: "REFRESH AT",
                        scope: &Scope::empty(),
                        relation_type: &SqlRelationType::empty(),
                        allow_aggregates: false,
                        allow_subqueries: false,
                        allow_parameters: false,
                        allow_windows: false,
                    };
                    let hir = plan_expr(ecx, &time)?.cast_to(
                        ecx,
                        CastContext::Assignment,
                        &SqlScalarType::MzTimestamp,
                    )?;
                    // (mz_now was purified away to a literal earlier)
                    let timestamp = hir
                        .into_literal_mz_timestamp()
                        .ok_or_else(|| PlanError::InvalidRefreshAt)?;
                    refresh_schedule.ats.push(timestamp);
                }
                RefreshOptionValue::Every(RefreshEveryOptionValue {
                    interval,
                    aligned_to,
                }) => {
                    let interval = Interval::try_from_value(Value::Interval(interval))?;
                    if interval.as_microseconds() <= 0 {
                        sql_bail!("REFRESH interval must be positive; got: {}", interval);
                    }
                    if interval.months != 0 {
                        // This limitation is because we want Intervals to be cleanly convertable
                        // to a unix epoch timestamp difference. When the interval involves months, then
                        // this is not true anymore, because months have variable lengths.
                        // See `Timestamp::round_up`.
                        sql_bail!("REFRESH interval must not involve units larger than days");
                    }
                    let interval = interval.duration()?;
                    if u64::try_from(interval.as_millis()).is_err() {
                        sql_bail!("REFRESH interval too large");
                    }

                    let mut aligned_to = match aligned_to {
                        Some(aligned_to) => aligned_to,
                        None => {
                            soft_panic_or_log!(
                                "ALIGNED TO should have been filled in by purification"
                            );
                            sql_bail!(
                                "INTERNAL ERROR: ALIGNED TO should have been filled in by purification"
                            )
                        }
                    };

                    // Desugar the `aligned_to` expression
                    transform_ast::transform(scx, &mut aligned_to)?;

                    let ecx = &ExprContext {
                        qcx: &QueryContext::root(scx, QueryLifetime::OneShot),
                        name: "REFRESH EVERY ... ALIGNED TO",
                        scope: &Scope::empty(),
                        relation_type: &SqlRelationType::empty(),
                        allow_aggregates: false,
                        allow_subqueries: false,
                        allow_parameters: false,
                        allow_windows: false,
                    };
                    let aligned_to_hir = plan_expr(ecx, &aligned_to)?.cast_to(
                        ecx,
                        CastContext::Assignment,
                        &SqlScalarType::MzTimestamp,
                    )?;
                    // (mz_now was purified away to a literal earlier)
                    let aligned_to_const = aligned_to_hir
                        .into_literal_mz_timestamp()
                        .ok_or_else(|| PlanError::InvalidRefreshEveryAlignedTo)?;

                    refresh_schedule.everies.push(RefreshEvery {
                        interval,
                        aligned_to: aligned_to_const,
                    });
                }
            }
        }

        if on_commits_seen > 1 {
            sql_bail!("REFRESH ON COMMIT cannot be specified multiple times");
        }
        if on_commits_seen > 0 && refresh_schedule != RefreshSchedule::default() {
            sql_bail!("REFRESH ON COMMIT is not compatible with any of the other REFRESH options");
        }

        if refresh_schedule == RefreshSchedule::default() {
            None
        } else {
            Some(refresh_schedule)
        }
    };

    let as_of = stmt.as_of.map(Timestamp::from);
    let compaction_window = plan_retain_history_option(scx, retain_history)?;
    let mut non_null_assertions = assert_not_null
        .into_iter()
        .map(normalize::column_name)
        .map(|assertion_name| {
            column_names
                .iter()
                .position(|col| col == &assertion_name)
                .ok_or_else(|| {
                    sql_err!(
                        "column {} in ASSERT NOT NULL option not found",
                        assertion_name.quoted()
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    non_null_assertions.sort();
    if let Some(dup) = non_null_assertions.iter().duplicates().next() {
        let dup = &column_names[*dup];
        sql_bail!("duplicate column {} in non-null assertions", dup.quoted());
    }

    if let Some(dup) = column_names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.quoted());
    }

    // Override the statement-level IfExistsBehavior with Skip if this is
    // explicitly requested in the PlanContext (the default is `false`).
    let if_exists = match scx.pcx().map(|pcx| pcx.ignore_if_exists_errors) {
        Ok(true) => IfExistsBehavior::Skip,
        _ => stmt.if_exists,
    };

    let mut replace = None;
    let mut if_not_exists = false;
    match if_exists {
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

            // Check if the new Materialized View depends on the item that we would be replacing.
            if let Some(id) = replace_id {
                let dependencies = expr.depends_on();
                let invalid_drop = scx
                    .get_item(&id)
                    .global_ids()
                    .any(|gid| dependencies.contains(&gid));
                if invalid_drop {
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
    let mut dependencies: BTreeSet<_> = expr
        .depends_on()
        .into_iter()
        .map(|gid| scx.catalog.resolve_item_id(&gid))
        .collect();

    if let Some(id) = replacement_target {
        dependencies.insert(id);
    }

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    // For PostgreSQL compatibility, we need to prevent creating materialized
    // views when there is an existing object *or* type of the same name.
    if let (IfExistsBehavior::Error, Ok(item)) =
        (if_exists, scx.catalog.resolve_item_or_type(&partial_name))
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
            dependencies: DependencyIds(dependencies),
            column_names,
            replacement_target,
            cluster_id,
            non_null_assertions,
            compaction_window,
            refresh_schedule,
            as_of,
        },
        replace,
        drop_ids,
        if_not_exists,
        ambiguous_columns: *scx.ambiguous_columns.borrow(),
    }))
}

generate_extracted_config!(
    MaterializedViewOption,
    (AssertNotNull, Ident, AllowMultiple),
    (PartitionBy, Vec<Ident>),
    (RetainHistory, OptionalDuration),
    (Refresh, RefreshOptionValue<Aug>, AllowMultiple)
);

pub fn plan_create_continual_task(
    scx: &StatementContext,
    mut stmt: CreateContinualTaskStatement<Aug>,
) -> Result<Plan, PlanError> {
    match &stmt.sugar {
        None => scx.require_feature_flag(&vars::ENABLE_CONTINUAL_TASK_CREATE)?,
        Some(ast::CreateContinualTaskSugar::Transform { .. }) => {
            scx.require_feature_flag(&vars::ENABLE_CONTINUAL_TASK_TRANSFORM)?
        }
        Some(ast::CreateContinualTaskSugar::Retain { .. }) => {
            scx.require_feature_flag(&vars::ENABLE_CONTINUAL_TASK_RETAIN)?
        }
    };
    let cluster_id = match &stmt.in_cluster {
        None => scx.catalog.resolve_cluster(None)?.id(),
        Some(in_cluster) => in_cluster.id,
    };
    stmt.in_cluster = Some(ResolvedClusterName {
        id: cluster_id,
        print_name: None,
    });

    let create_sql =
        normalize::create_statement(scx, Statement::CreateContinualTask(stmt.clone()))?;

    let ContinualTaskOptionExtracted { snapshot, seen: _ } = stmt.with_options.try_into()?;

    // It seems desirable for a CT that e.g. simply filters the input to keep
    // the same nullability. So, start by assuming all columns are non-nullable,
    // and then make them nullable below if any of the exprs plan them as
    // nullable.
    let mut desc = match stmt.columns {
        None => None,
        Some(columns) => {
            let mut desc_columns = Vec::with_capacity(columns.capacity());
            for col in columns.iter() {
                desc_columns.push((
                    normalize::column_name(col.name.clone()),
                    SqlColumnType {
                        scalar_type: scalar_type_from_sql(scx, &col.data_type)?,
                        nullable: false,
                    },
                ));
            }
            Some(RelationDesc::from_names_and_types(desc_columns))
        }
    };
    let input = scx.get_item_by_resolved_name(&stmt.input)?;
    match input.item_type() {
        // Input must be a thing directly backed by a persist shard, so we can
        // use a persist listen to efficiently rehydrate.
        CatalogItemType::ContinualTask
        | CatalogItemType::Table
        | CatalogItemType::MaterializedView
        | CatalogItemType::Source => {}
        CatalogItemType::Sink
        | CatalogItemType::View
        | CatalogItemType::Index
        | CatalogItemType::Type
        | CatalogItemType::Func
        | CatalogItemType::Secret
        | CatalogItemType::Connection => {
            sql_bail!(
                "CONTINUAL TASK cannot use {} as an input",
                input.item_type()
            );
        }
    }

    let mut qcx = QueryContext::root(scx, QueryLifetime::MaterializedView);
    let ct_name = stmt.name;
    let placeholder_id = match &ct_name {
        ResolvedItemName::ContinualTask { id, name } => {
            let desc = match desc.as_ref().cloned() {
                Some(x) => x,
                None => {
                    // The user didn't specify the CT's columns. Take a wild
                    // guess that the CT has the same shape as the input. It's
                    // fine if this is wrong, we'll get an error below after
                    // planning the query.
                    let desc = input.relation_desc().expect("item type checked above");
                    desc.into_owned()
                }
            };
            qcx.ctes.insert(
                *id,
                CteDesc {
                    name: name.item.clone(),
                    desc,
                },
            );
            Some(*id)
        }
        _ => None,
    };

    let mut exprs = Vec::new();
    for (idx, stmt) in stmt.stmts.iter().enumerate() {
        let query = continual_task_query(&ct_name, stmt).ok_or_else(|| sql_err!("TODO(ct3)"))?;
        let query::PlannedRootQuery {
            expr,
            desc: desc_query,
            finishing,
            scope: _,
        } = query::plan_ct_query(&mut qcx, query)?;
        // We get back a trivial finishing because we plan with a "maintained"
        // QueryLifetime, see comment in `plan_view`.
        assert!(HirRelationExpr::is_trivial_row_set_finishing_hir(
            &finishing,
            expr.arity()
        ));
        if expr.contains_parameters()? {
            if expr.contains_parameters()? {
                return Err(PlanError::ParameterNotAllowed(
                    "continual tasks".to_string(),
                ));
            }
        }
        let expr = match desc.as_mut() {
            None => {
                desc = Some(desc_query);
                expr
            }
            Some(desc) => {
                // We specify the columns for DELETE, so if any columns types don't
                // match, it's because it's an INSERT.
                if desc_query.arity() > desc.arity() {
                    sql_bail!(
                        "statement {}: INSERT has more expressions than target columns",
                        idx
                    );
                }
                if desc_query.arity() < desc.arity() {
                    sql_bail!(
                        "statement {}: INSERT has more target columns than expressions",
                        idx
                    );
                }
                // Ensure the types of the source query match the types of the target table,
                // installing assignment casts where necessary and possible.
                let target_types = desc.iter_types().map(|x| &x.scalar_type);
                let expr = cast_relation(&qcx, CastContext::Assignment, expr, target_types);
                let expr = expr.map_err(|e| {
                    sql_err!(
                        "statement {}: column {} is of type {} but expression is of type {}",
                        idx,
                        desc.get_name(e.column).quoted(),
                        qcx.humanize_scalar_type(&e.target_type, false),
                        qcx.humanize_scalar_type(&e.source_type, false),
                    )
                })?;

                // Update ct nullability as necessary. The `ne` above verified that the
                // types are the same len.
                let zip_types = || desc.iter_types().zip_eq(desc_query.iter_types());
                let updated = zip_types().any(|(ct, q)| q.nullable && !ct.nullable);
                if updated {
                    let new_types = zip_types().map(|(ct, q)| {
                        let mut ct = ct.clone();
                        if q.nullable {
                            ct.nullable = true;
                        }
                        ct
                    });
                    *desc = RelationDesc::from_names_and_types(
                        desc.iter_names().cloned().zip_eq(new_types),
                    );
                }

                expr
            }
        };
        match stmt {
            ast::ContinualTaskStmt::Insert(_) => exprs.push(expr),
            ast::ContinualTaskStmt::Delete(_) => exprs.push(expr.negate()),
        }
    }
    // TODO(ct3): Collect things by output and assert that there is only one (or
    // support multiple outputs).
    let expr = exprs
        .into_iter()
        .reduce(|acc, expr| acc.union(expr))
        .ok_or_else(|| sql_err!("TODO(ct3)"))?;
    let dependencies = expr
        .depends_on()
        .into_iter()
        .map(|gid| scx.catalog.resolve_item_id(&gid))
        .collect();

    let desc = desc.ok_or_else(|| sql_err!("TODO(ct3)"))?;
    let column_names: Vec<ColumnName> = desc.iter_names().cloned().collect();
    if let Some(dup) = column_names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.quoted());
    }

    // Check for an object in the catalog with this same name
    let name = match &ct_name {
        ResolvedItemName::Item { id, .. } => scx.catalog.get_item(id).name().clone(),
        ResolvedItemName::ContinualTask { name, .. } => {
            let name = scx.allocate_qualified_name(name.clone())?;
            let full_name = scx.catalog.resolve_full_name(&name);
            let partial_name = PartialItemName::from(full_name.clone());
            // For PostgreSQL compatibility, we need to prevent creating this when there
            // is an existing object *or* type of the same name.
            if let Ok(item) = scx.catalog.resolve_item_or_type(&partial_name) {
                return Err(PlanError::ItemAlreadyExists {
                    name: full_name.to_string(),
                    item_type: item.item_type(),
                });
            }
            name
        }
        ResolvedItemName::Cte { .. } => unreachable!("name should not resolve to a CTE"),
        ResolvedItemName::Error => unreachable!("error should be returned in name resolution"),
    };

    let as_of = stmt.as_of.map(Timestamp::from);
    Ok(Plan::CreateContinualTask(CreateContinualTaskPlan {
        name,
        placeholder_id,
        desc,
        input_id: input.global_id(),
        with_snapshot: snapshot.unwrap_or(true),
        continual_task: MaterializedView {
            create_sql,
            expr,
            dependencies,
            column_names,
            replacement_target: None,
            cluster_id,
            non_null_assertions: Vec::new(),
            compaction_window: None,
            refresh_schedule: None,
            as_of,
        },
    }))
}

fn continual_task_query<'a>(
    ct_name: &ResolvedItemName,
    stmt: &'a ast::ContinualTaskStmt<Aug>,
) -> Option<ast::Query<Aug>> {
    match stmt {
        ast::ContinualTaskStmt::Insert(ast::InsertStatement {
            table_name: _,
            columns,
            source,
            returning,
        }) => {
            if !columns.is_empty() || !returning.is_empty() {
                return None;
            }
            match source {
                ast::InsertSource::Query(query) => Some(query.clone()),
                ast::InsertSource::DefaultValues => None,
            }
        }
        ast::ContinualTaskStmt::Delete(ast::DeleteStatement {
            table_name: _,
            alias,
            using,
            selection,
        }) => {
            if !using.is_empty() {
                return None;
            }
            // Construct a `SELECT *` with the `DELETE` selection as a `WHERE`.
            let from = ast::TableWithJoins {
                relation: ast::TableFactor::Table {
                    name: ct_name.clone(),
                    alias: alias.clone(),
                },
                joins: Vec::new(),
            };
            let select = ast::Select {
                from: vec![from],
                selection: selection.clone(),
                distinct: None,
                projection: vec![ast::SelectItem::Wildcard],
                group_by: Vec::new(),
                having: None,
                qualify: None,
                options: Vec::new(),
            };
            let query = ast::Query {
                ctes: ast::CteBlock::Simple(Vec::new()),
                body: ast::SetExpr::Select(Box::new(select)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            };
            // Then negate it to turn it into retractions (after planning it).
            Some(query)
        }
    }
}

generate_extracted_config!(ContinualTaskOption, (Snapshot, bool));

pub fn describe_create_sink(
    _: &StatementContext,
    _: CreateSinkStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(
    CreateSinkOption,
    (Snapshot, bool),
    (PartitionStrategy, String),
    (Version, u64),
    (CommitInterval, Duration)
);

pub fn plan_create_sink(
    scx: &StatementContext,
    stmt: CreateSinkStatement<Aug>,
) -> Result<Plan, PlanError> {
    // Check for an object in the catalog with this same name
    let Some(name) = stmt.name.clone() else {
        return Err(PlanError::MissingName(CatalogItemType::Sink));
    };
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (stmt.if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    plan_sink(scx, stmt)
}

/// This function will plan a sink as if it does not exist in the catalog. This is so the planning
/// logic is reused by both CREATE SINK and ALTER SINK planning. It is the responsibility of the
/// callers (plan_create_sink and plan_alter_sink) to check for name collisions if this is
/// important.
fn plan_sink(
    scx: &StatementContext,
    mut stmt: CreateSinkStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSinkStatement {
        name,
        in_cluster: _,
        from,
        connection,
        format,
        envelope,
        if_not_exists,
        with_options,
    } = stmt.clone();

    let Some(name) = name else {
        return Err(PlanError::MissingName(CatalogItemType::Sink));
    };
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;

    let envelope = match envelope {
        Some(ast::SinkEnvelope::Upsert) => SinkEnvelope::Upsert,
        Some(ast::SinkEnvelope::Debezium) => SinkEnvelope::Debezium,
        None => sql_bail!("ENVELOPE clause is required"),
    };

    let from_name = &from;
    let from = scx.get_item_by_resolved_name(&from)?;

    {
        use CatalogItemType::*;
        match from.item_type() {
            Table | Source | MaterializedView | ContinualTask => {}
            Sink | View | Index | Type | Func | Secret | Connection => {
                let name = scx.catalog.minimal_qualification(from.name());
                return Err(PlanError::InvalidSinkFrom {
                    name: name.to_string(),
                    item_type: from.item_type(),
                });
            }
        }
    }

    if from.id().is_system() {
        bail_unsupported!("creating a sink directly on a catalog object");
    }

    let desc = from.relation_desc().expect("item type checked above");
    let key_indices = match &connection {
        CreateSinkConnection::Kafka { key: Some(key), .. }
        | CreateSinkConnection::Iceberg { key: Some(key), .. } => {
            let key_columns = key
                .key_columns
                .clone()
                .into_iter()
                .map(normalize::column_name)
                .collect::<Vec<_>>();
            let mut uniq = BTreeSet::new();
            for col in key_columns.iter() {
                if !uniq.insert(col) {
                    sql_bail!("duplicate column referenced in KEY: {}", col);
                }
            }
            let indices = key_columns
                .iter()
                .map(|col| -> anyhow::Result<usize> {
                    let name_idx =
                        desc.get_by_name(col)
                            .map(|(idx, _type)| idx)
                            .ok_or_else(|| {
                                sql_err!("column referenced in KEY does not exist: {}", col)
                            })?;
                    if desc.get_unambiguous_name(name_idx).is_none() {
                        sql_err!("column referenced in KEY is ambiguous: {}", col);
                    }
                    Ok(name_idx)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let is_valid_key = desc
                .typ()
                .keys
                .iter()
                .any(|key_columns| key_columns.iter().all(|column| indices.contains(column)));

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
        }
        CreateSinkConnection::Kafka { key: None, .. }
        | CreateSinkConnection::Iceberg { key: None, .. } => None,
    };

    let headers_index = match &connection {
        CreateSinkConnection::Kafka {
            headers: Some(headers),
            ..
        } => {
            scx.require_feature_flag(&ENABLE_KAFKA_SINK_HEADERS)?;

            match envelope {
                SinkEnvelope::Upsert => (),
                SinkEnvelope::Debezium => {
                    sql_bail!("HEADERS option is not supported with ENVELOPE DEBEZIUM")
                }
            };

            let headers = normalize::column_name(headers.clone());
            let (idx, ty) = desc
                .get_by_name(&headers)
                .ok_or_else(|| sql_err!("HEADERS column ({}) is unknown", headers))?;

            if desc.get_unambiguous_name(idx).is_none() {
                sql_bail!("HEADERS column ({}) is ambiguous", headers);
            }

            match &ty.scalar_type {
                SqlScalarType::Map { value_type, .. }
                    if matches!(&**value_type, SqlScalarType::String | SqlScalarType::Bytes) => {}
                _ => sql_bail!(
                    "HEADERS column must have type map[text => text] or map[text => bytea]"
                ),
            }

            Some(idx)
        }
        _ => None,
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
        let typ = SqlRelationType::new(types);
        (RelationDesc::new(typ, names), key_indices)
    });

    if key_desc_and_indices.is_none() && envelope == SinkEnvelope::Upsert {
        return Err(PlanError::UpsertSinkWithoutKey);
    }

    let CreateSinkOptionExtracted {
        snapshot,
        version,
        partition_strategy: _,
        seen: _,
        commit_interval,
    } = with_options.try_into()?;

    let connection_builder = match connection {
        CreateSinkConnection::Kafka {
            connection,
            options,
            ..
        } => kafka_sink_builder(
            scx,
            connection,
            options,
            format,
            relation_key_indices,
            key_desc_and_indices,
            headers_index,
            desc.into_owned(),
            envelope,
            from.id(),
            commit_interval,
        )?,
        CreateSinkConnection::Iceberg {
            connection,
            aws_connection,
            options,
            ..
        } => iceberg_sink_builder(
            scx,
            connection,
            aws_connection,
            options,
            relation_key_indices,
            key_desc_and_indices,
            commit_interval,
        )?,
    };

    // WITH SNAPSHOT defaults to true
    let with_snapshot = snapshot.unwrap_or(true);
    // VERSION defaults to 0
    let version = version.unwrap_or(0);

    // We will rewrite the cluster if one is not provided, so we must use the
    // `in_cluster` value we plan to normalize when we canonicalize the create
    // statement.
    let in_cluster = source_sink_cluster_config(scx, &mut stmt.in_cluster)?;
    let create_sql = normalize::create_statement(scx, Statement::CreateSink(stmt))?;

    Ok(Plan::CreateSink(CreateSinkPlan {
        name,
        sink: Sink {
            create_sql,
            from: from.global_id(),
            connection: connection_builder,
            envelope,
            version,
            commit_interval,
        },
        with_snapshot,
        if_not_exists,
        in_cluster: in_cluster.id(),
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
/// because the macro doesn't support parameterized enums. See <https://github.com/MaterializeInc/database-issues/issues/6698>
#[derive(Debug, Default, PartialEq, Clone)]
pub struct CsrConfigOptionExtracted {
    seen: ::std::collections::BTreeSet<CsrConfigOptionName<Aug>>,
    pub(crate) avro_key_fullname: Option<String>,
    pub(crate) avro_value_fullname: Option<String>,
    pub(crate) null_defaults: bool,
    pub(crate) value_doc_options: BTreeMap<DocTarget, String>,
    pub(crate) key_doc_options: BTreeMap<DocTarget, String>,
    pub(crate) key_compatibility_level: Option<mz_ccsr::CompatibilityLevel>,
    pub(crate) value_compatibility_level: Option<mz_ccsr::CompatibilityLevel>,
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
            let option_name_str = option_name.to_ast_string_simple();
            let better_error = |e: PlanError| PlanError::InvalidOptionValue {
                option_name: option_name.to_ast_string_simple(),
                err: e.into(),
            };
            let to_compatibility_level = |val: Option<WithOptionValue<Aug>>| {
                val.map(|s| match s {
                    WithOptionValue::Value(Value::String(s)) => {
                        mz_ccsr::CompatibilityLevel::try_from(s.to_uppercase().as_str())
                    }
                    _ => Err("must be a string".to_string()),
                })
                .transpose()
                .map_err(PlanError::Unstructured)
                .map_err(better_error)
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
                        DocOnIdentifier::Column(ast::ColumnName {
                            relation: ResolvedItemName::Item { id, .. },
                            column: ResolvedColumnReference::Column { name, index: _ },
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
                CsrConfigOptionName::KeyCompatibilityLevel => {
                    extracted.key_compatibility_level = to_compatibility_level(option.value)?;
                }
                CsrConfigOptionName::ValueCompatibilityLevel => {
                    extracted.value_compatibility_level = to_compatibility_level(option.value)?;
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

fn iceberg_sink_builder(
    scx: &StatementContext,
    catalog_connection: ResolvedItemName,
    aws_connection: ResolvedItemName,
    options: Vec<IcebergSinkConfigOption<Aug>>,
    relation_key_indices: Option<Vec<usize>>,
    key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    commit_interval: Option<Duration>,
) -> Result<StorageSinkConnection<ReferencedConnection>, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_ICEBERG_SINK)?;
    let catalog_connection_item = scx.get_item_by_resolved_name(&catalog_connection)?;
    let catalog_connection_id = catalog_connection_item.id();
    let aws_connection_item = scx.get_item_by_resolved_name(&aws_connection)?;
    let aws_connection_id = aws_connection_item.id();
    if !matches!(
        catalog_connection_item.connection()?,
        Connection::IcebergCatalog(_)
    ) {
        sql_bail!(
            "{} is not an iceberg catalog connection",
            scx.catalog
                .resolve_full_name(catalog_connection_item.name())
                .to_string()
                .quoted()
        );
    };

    if !matches!(aws_connection_item.connection()?, Connection::Aws(_)) {
        sql_bail!(
            "{} is not an AWS connection",
            scx.catalog
                .resolve_full_name(aws_connection_item.name())
                .to_string()
                .quoted()
        );
    }

    let IcebergSinkConfigOptionExtracted {
        table,
        namespace,
        seen: _,
    }: IcebergSinkConfigOptionExtracted = options.try_into()?;

    let Some(table) = table else {
        sql_bail!("Iceberg sink must specify TABLE");
    };
    let Some(namespace) = namespace else {
        sql_bail!("Iceberg sink must specify NAMESPACE");
    };
    if commit_interval.is_none() {
        sql_bail!("Iceberg sink must specify COMMIT INTERVAL");
    }

    Ok(StorageSinkConnection::Iceberg(IcebergSinkConnection {
        catalog_connection_id,
        catalog_connection: catalog_connection_id,
        aws_connection_id,
        aws_connection: aws_connection_id,
        table,
        namespace,
        relation_key_indices,
        key_desc_and_indices,
    }))
}

fn kafka_sink_builder(
    scx: &StatementContext,
    connection: ResolvedItemName,
    options: Vec<KafkaSinkConfigOption<Aug>>,
    format: Option<FormatSpecifier<Aug>>,
    relation_key_indices: Option<Vec<usize>>,
    key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    headers_index: Option<usize>,
    value_desc: RelationDesc,
    envelope: SinkEnvelope,
    sink_from: CatalogItemId,
    commit_interval: Option<Duration>,
) -> Result<StorageSinkConnection<ReferencedConnection>, PlanError> {
    // Get Kafka connection.
    let connection_item = scx.get_item_by_resolved_name(&connection)?;
    let connection_id = connection_item.id();
    match connection_item.connection()? {
        Connection::Kafka(_) => (),
        _ => sql_bail!(
            "{} is not a kafka connection",
            scx.catalog.resolve_full_name(connection_item.name())
        ),
    };

    if commit_interval.is_some() {
        sql_bail!("COMMIT INTERVAL option is not supported with KAFKA sinks");
    }

    let KafkaSinkConfigOptionExtracted {
        topic,
        compression_type,
        partition_by,
        progress_group_id_prefix,
        transactional_id_prefix,
        legacy_ids,
        topic_config,
        topic_metadata_refresh_interval,
        topic_partition_count,
        topic_replication_factor,
        seen: _,
    }: KafkaSinkConfigOptionExtracted = options.try_into()?;

    let transactional_id = match (transactional_id_prefix, legacy_ids) {
        (Some(_), Some(true)) => {
            sql_bail!("LEGACY IDS cannot be used at the same time as TRANSACTIONAL ID PREFIX")
        }
        (None, Some(true)) => KafkaIdStyle::Legacy,
        (prefix, _) => KafkaIdStyle::Prefix(prefix),
    };

    let progress_group_id = match (progress_group_id_prefix, legacy_ids) {
        (Some(_), Some(true)) => {
            sql_bail!("LEGACY IDS cannot be used at the same time as PROGRESS GROUP ID PREFIX")
        }
        (None, Some(true)) => KafkaIdStyle::Legacy,
        (prefix, _) => KafkaIdStyle::Prefix(prefix),
    };

    let topic_name = topic.ok_or_else(|| sql_err!("KAFKA CONNECTION must specify TOPIC"))?;

    if topic_metadata_refresh_interval > Duration::from_secs(60 * 60) {
        // This is a librdkafka-enforced restriction that, if violated,
        // would result in a runtime error for the source.
        sql_bail!("TOPIC METADATA REFRESH INTERVAL cannot be greater than 1 hour");
    }

    let assert_positive = |val: Option<i32>, name: &str| {
        if let Some(val) = val {
            if val <= 0 {
                sql_bail!("{} must be a positive integer", name);
            }
        }
        val.map(NonNeg::try_from)
            .transpose()
            .map_err(|_| PlanError::Unstructured(format!("{} must be a positive integer", name)))
    };
    let topic_partition_count = assert_positive(topic_partition_count, "TOPIC PARTITION COUNT")?;
    let topic_replication_factor =
        assert_positive(topic_replication_factor, "TOPIC REPLICATION FACTOR")?;

    // Helper method to parse avro connection options for format specifiers that use avro
    // for either key or value encoding.
    let gen_avro_schema_options = |conn| {
        let CsrConnectionAvro {
            connection:
                CsrConnection {
                    connection,
                    options,
                },
            seed,
            key_strategy,
            value_strategy,
        } = conn;
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
            Connection::Csr(_) => item.id(),
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
        let extracted_options: CsrConfigOptionExtracted = options.try_into()?;

        if key_desc_and_indices.is_none() && extracted_options.avro_key_fullname.is_some() {
            sql_bail!("Cannot specify AVRO KEY FULLNAME without a corresponding KEY field");
        }

        if key_desc_and_indices.is_some()
            && (extracted_options.avro_key_fullname.is_some()
                ^ extracted_options.avro_value_fullname.is_some())
        {
            sql_bail!(
                "Must specify both AVRO KEY FULLNAME and AVRO VALUE FULLNAME when specifying generated schema names"
            );
        }

        Ok((csr_connection, extracted_options))
    };

    let map_format = |format: Format<Aug>, desc: &RelationDesc, is_key: bool| match format {
        Format::Json { array: false } => Ok::<_, PlanError>(KafkaSinkFormatType::Json),
        Format::Bytes if desc.arity() == 1 => {
            let col_type = &desc.typ().column_types[0].scalar_type;
            if !mz_pgrepr::Value::can_encode_binary(col_type) {
                bail_unsupported!(format!(
                    "BYTES format with non-encodable type: {:?}",
                    col_type
                ));
            }

            Ok(KafkaSinkFormatType::Bytes)
        }
        Format::Text if desc.arity() == 1 => Ok(KafkaSinkFormatType::Text),
        Format::Bytes | Format::Text => {
            bail_unsupported!("BYTES or TEXT format with multiple columns")
        }
        Format::Json { array: true } => bail_unsupported!("JSON ARRAY format in sinks"),
        Format::Avro(AvroSchema::Csr { csr_connection }) => {
            let (csr_connection, options) = gen_avro_schema_options(csr_connection)?;
            let schema = if is_key {
                AvroSchemaGenerator::new(
                    desc.clone(),
                    false,
                    options.key_doc_options,
                    options.avro_key_fullname.as_deref().unwrap_or("row"),
                    options.null_defaults,
                    Some(sink_from),
                    false,
                )?
                .schema()
                .to_string()
            } else {
                AvroSchemaGenerator::new(
                    desc.clone(),
                    matches!(envelope, SinkEnvelope::Debezium),
                    options.value_doc_options,
                    options.avro_value_fullname.as_deref().unwrap_or("envelope"),
                    options.null_defaults,
                    Some(sink_from),
                    true,
                )?
                .schema()
                .to_string()
            };
            Ok(KafkaSinkFormatType::Avro {
                schema,
                compatibility_level: if is_key {
                    options.key_compatibility_level
                } else {
                    options.value_compatibility_level
                },
                csr_connection,
            })
        }
        format => bail_unsupported!(format!("sink format {:?}", format)),
    };

    let partition_by = match &partition_by {
        Some(partition_by) => {
            let mut scope = Scope::from_source(None, value_desc.iter_names());

            match envelope {
                SinkEnvelope::Upsert => (),
                SinkEnvelope::Debezium => {
                    let key_indices: HashSet<_> = key_desc_and_indices
                        .as_ref()
                        .map(|(_desc, indices)| indices.as_slice())
                        .unwrap_or_default()
                        .into_iter()
                        .collect();
                    for (i, item) in scope.items.iter_mut().enumerate() {
                        if !key_indices.contains(&i) {
                            item.error_if_referenced = Some(|_table, column| {
                                PlanError::InvalidPartitionByEnvelopeDebezium {
                                    column_name: column.to_string(),
                                }
                            });
                        }
                    }
                }
            };

            let ecx = &ExprContext {
                qcx: &QueryContext::root(scx, QueryLifetime::OneShot),
                name: "PARTITION BY",
                scope: &scope,
                relation_type: value_desc.typ(),
                allow_aggregates: false,
                allow_subqueries: false,
                allow_parameters: false,
                allow_windows: false,
            };
            let expr = plan_expr(ecx, partition_by)?.cast_to(
                ecx,
                CastContext::Assignment,
                &SqlScalarType::UInt64,
            )?;
            let expr = expr.lower_uncorrelated()?;

            Some(expr)
        }
        _ => None,
    };

    // Map from the format specifier of the statement to the individual key/value formats for the sink.
    let format = match format {
        Some(FormatSpecifier::KeyValue { key, value }) => {
            let key_format = match key_desc_and_indices.as_ref() {
                Some((desc, _indices)) => Some(map_format(key, desc, true)?),
                None => None,
            };
            KafkaSinkFormat {
                value_format: map_format(value, &value_desc, false)?,
                key_format,
            }
        }
        Some(FormatSpecifier::Bare(format)) => {
            let key_format = match key_desc_and_indices.as_ref() {
                Some((desc, _indices)) => Some(map_format(format.clone(), desc, true)?),
                None => None,
            };
            KafkaSinkFormat {
                value_format: map_format(format, &value_desc, false)?,
                key_format,
            }
        }
        None => bail_unsupported!("sink without format"),
    };

    Ok(StorageSinkConnection::Kafka(KafkaSinkConnection {
        connection_id,
        connection: connection_id,
        format,
        topic: topic_name,
        relation_key_indices,
        key_desc_and_indices,
        headers_index,
        value_desc,
        partition_by,
        compression_type,
        progress_group_id,
        transactional_id,
        topic_options: KafkaTopicOptions {
            partition_count: topic_partition_count,
            replication_factor: topic_replication_factor,
            topic_config: topic_config.unwrap_or_default(),
        },
        topic_metadata_refresh_interval,
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
    let on_item_type = on.item_type();

    if !matches!(
        on_item_type,
        CatalogItemType::View
            | CatalogItemType::MaterializedView
            | CatalogItemType::Source
            | CatalogItemType::Table
    ) {
        sql_bail!(
            "index cannot be created on {} because it is a {}",
            on_name.full_name_str(),
            on.item_type()
        )
    }

    let on_desc = on.relation_desc().expect("item type checked above");

    let filled_key_parts = match key_parts {
        Some(kp) => kp.to_vec(),
        None => {
            // `key_parts` is None if we're creating a "default" index.
            let key = on_desc.typ().default_key();
            key.iter()
                .map(|i| match on_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![n.clone().into()]),
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
                    mz_expr::MirScalarExpr::Column(i, name) => {
                        match (on_desc.get_unambiguous_name(*i), &name.0) {
                            (Some(col_name), _) => col_name.to_string(),
                            (None, Some(name)) => name.to_string(),
                            (None, None) => format!("{}", i + 1),
                        }
                    }
                    _ => "expr".to_string(),
                })
                .join("_");
            write!(idx_name.item, "_{index_name_col_suffix}_idx")
                .expect("write on strings cannot fail");
            idx_name.item = normalize::ident(Ident::new(&idx_name.item)?)
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
    // For PostgreSQL compatibility, we need to prevent creating indexes when
    // there is an existing object *or* type of the same name.
    //
    // Technically, we only need to prevent coexistence of indexes and types
    // that have an associated relation (record types but not list/map types).
    // Enforcing that would be more complicated, though. It's backwards
    // compatible to weaken this restriction in the future.
    if let (Ok(item), false, false) = (
        scx.catalog.resolve_item_or_type(&partial_name),
        *if_not_exists,
        scx.pcx().map_or(false, |pcx| pcx.ignore_if_exists_errors),
    ) {
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

    *in_cluster = Some(ResolvedClusterName {
        id: cluster_id,
        print_name: None,
    });

    // Normalize `stmt`.
    *name = Some(Ident::new(index_name.item.clone())?);
    *key_parts = Some(filled_key_parts);
    let if_not_exists = *if_not_exists;

    let create_sql = normalize::create_statement(scx, Statement::CreateIndex(stmt))?;
    let compaction_window = options.iter().find_map(|o| {
        #[allow(irrefutable_let_patterns)]
        if let crate::plan::IndexOption::RetainHistory(lcw) = o {
            Some(lcw.clone())
        } else {
            None
        }
    });

    Ok(Plan::CreateIndex(CreateIndexPlan {
        name: index_name,
        index: Index {
            create_sql,
            on: on.global_id(),
            keys,
            cluster_id,
            compaction_window,
        },
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
    ) -> Result<(CatalogItemId, Vec<i64>), PlanError> {
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
    // For PostgreSQL compatibility, we need to prevent creating types when
    // there is an existing object *or* type of the same name.
    if let Ok(item) = scx.catalog.resolve_item_or_type(&partial_name) {
        if item.item_type().conflicts_with_type() {
            return Err(PlanError::ItemAlreadyExists {
                name: full_name.to_string(),
                item_type: item.item_type(),
            });
        }
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

#[derive(Debug, Clone)]
pub struct PlannedRoleAttributes {
    pub inherit: Option<bool>,
    pub password: Option<Password>,
    pub scram_iterations: Option<NonZeroU32>,
    /// `nopassword` is set to true if the password is from the parser is None.
    /// This is semantically different than not supplying a password at all,
    /// to allow for unsetting a password.
    pub nopassword: Option<bool>,
    pub superuser: Option<bool>,
    pub login: Option<bool>,
}

fn plan_role_attributes(
    options: Vec<RoleAttribute>,
    scx: &StatementContext,
) -> Result<PlannedRoleAttributes, PlanError> {
    let mut planned_attributes = PlannedRoleAttributes {
        inherit: None,
        password: None,
        scram_iterations: None,
        superuser: None,
        login: None,
        nopassword: None,
    };

    for option in options {
        match option {
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
            RoleAttribute::Password(_) if planned_attributes.password.is_some() => {
                sql_bail!("conflicting or redundant options");
            }

            RoleAttribute::Inherit => planned_attributes.inherit = Some(true),
            RoleAttribute::NoInherit => planned_attributes.inherit = Some(false),
            RoleAttribute::Password(password) => {
                if let Some(password) = password {
                    planned_attributes.password = Some(password.into());
                    planned_attributes.scram_iterations =
                        Some(scx.catalog.system_vars().scram_iterations())
                } else {
                    planned_attributes.nopassword = Some(true);
                }
            }
            RoleAttribute::SuperUser => {
                if planned_attributes.superuser == Some(false) {
                    sql_bail!("conflicting or redundant options");
                }
                planned_attributes.superuser = Some(true);
            }
            RoleAttribute::NoSuperUser => {
                if planned_attributes.superuser == Some(true) {
                    sql_bail!("conflicting or redundant options");
                }
                planned_attributes.superuser = Some(false);
            }
            RoleAttribute::Login => {
                if planned_attributes.login == Some(false) {
                    sql_bail!("conflicting or redundant options");
                }
                planned_attributes.login = Some(true);
            }
            RoleAttribute::NoLogin => {
                if planned_attributes.login == Some(true) {
                    sql_bail!("conflicting or redundant options");
                }
                planned_attributes.login = Some(false);
            }
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
    scx: &StatementContext,
    CreateRoleStatement { name, options }: CreateRoleStatement,
) -> Result<Plan, PlanError> {
    let attributes = plan_role_attributes(options, scx)?;
    Ok(Plan::CreateRole(CreateRolePlan {
        name: normalize::ident(name),
        attributes: attributes.into(),
    }))
}

pub fn plan_create_network_policy(
    ctx: &StatementContext,
    CreateNetworkPolicyStatement { name, options }: CreateNetworkPolicyStatement<Aug>,
) -> Result<Plan, PlanError> {
    ctx.require_feature_flag(&vars::ENABLE_NETWORK_POLICIES)?;
    let policy_options: NetworkPolicyOptionExtracted = options.try_into()?;

    let Some(rule_defs) = policy_options.rules else {
        sql_bail!("RULES must be specified when creating network policies.");
    };

    let mut rules = vec![];
    for NetworkPolicyRuleDefinition { name, options } in rule_defs {
        let NetworkPolicyRuleOptionExtracted {
            seen: _,
            direction,
            action,
            address,
        } = options.try_into()?;
        let (direction, action, address) = match (direction, action, address) {
            (Some(direction), Some(action), Some(address)) => (
                NetworkPolicyRuleDirection::try_from(direction.as_str())?,
                NetworkPolicyRuleAction::try_from(action.as_str())?,
                PolicyAddress::try_from(address.as_str())?,
            ),
            (_, _, _) => {
                sql_bail!("Direction, Address, and Action must specified when creating a rule")
            }
        };
        rules.push(NetworkPolicyRule {
            name: normalize::ident(name),
            direction,
            action,
            address,
        });
    }

    if rules.len()
        > ctx
            .catalog
            .system_vars()
            .max_rules_per_network_policy()
            .try_into()?
    {
        sql_bail!("RULES count exceeds max_rules_per_network_policy.")
    }

    Ok(Plan::CreateNetworkPolicy(CreateNetworkPolicyPlan {
        name: normalize::ident(name),
        rules,
    }))
}

pub fn plan_alter_network_policy(
    ctx: &StatementContext,
    AlterNetworkPolicyStatement { name, options }: AlterNetworkPolicyStatement<Aug>,
) -> Result<Plan, PlanError> {
    ctx.require_feature_flag(&vars::ENABLE_NETWORK_POLICIES)?;

    let policy_options: NetworkPolicyOptionExtracted = options.try_into()?;
    let policy = ctx.catalog.resolve_network_policy(&name.to_string())?;

    let Some(rule_defs) = policy_options.rules else {
        sql_bail!("RULES must be specified when creating network policies.");
    };

    let mut rules = vec![];
    for NetworkPolicyRuleDefinition { name, options } in rule_defs {
        let NetworkPolicyRuleOptionExtracted {
            seen: _,
            direction,
            action,
            address,
        } = options.try_into()?;

        let (direction, action, address) = match (direction, action, address) {
            (Some(direction), Some(action), Some(address)) => (
                NetworkPolicyRuleDirection::try_from(direction.as_str())?,
                NetworkPolicyRuleAction::try_from(action.as_str())?,
                PolicyAddress::try_from(address.as_str())?,
            ),
            (_, _, _) => {
                sql_bail!("Direction, Address, and Action must specified when creating a rule")
            }
        };
        rules.push(NetworkPolicyRule {
            name: normalize::ident(name),
            direction,
            action,
            address,
        });
    }
    if rules.len()
        > ctx
            .catalog
            .system_vars()
            .max_rules_per_network_policy()
            .try_into()?
    {
        sql_bail!("RULES count exceeds max_rules_per_network_policy.")
    }

    Ok(Plan::AlterNetworkPolicy(AlterNetworkPolicyPlan {
        id: policy.id(),
        name: normalize::ident(name),
        rules,
    }))
}

pub fn describe_create_cluster(
    _: &StatementContext,
    _: CreateClusterStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

// WARNING:
// DO NOT set any `Default` value here using the built-in mechanism of `generate_extracted_config`!
// These options are also used in ALTER CLUSTER, where not giving an option means that the value of
// that option stays the same. If you were to give a default value here, then not giving that option
// to ALTER CLUSTER would always reset the value of that option to the default.
generate_extracted_config!(
    ClusterOption,
    (AvailabilityZones, Vec<String>),
    (Disk, bool),
    (IntrospectionDebugging, bool),
    (IntrospectionInterval, OptionalDuration),
    (Managed, bool),
    (Replicas, Vec<ReplicaDefinition<Aug>>),
    (ReplicationFactor, u32),
    (Size, String),
    (Schedule, ClusterScheduleOptionValue),
    (WorkloadClass, OptionalString)
);

generate_extracted_config!(
    NetworkPolicyOption,
    (Rules, Vec<NetworkPolicyRuleDefinition<Aug>>)
);

generate_extracted_config!(
    NetworkPolicyRuleOption,
    (Direction, String),
    (Action, String),
    (Address, String)
);

generate_extracted_config!(ClusterAlterOption, (Wait, ClusterAlterOptionValue<Aug>));

generate_extracted_config!(
    ClusterAlterUntilReadyOption,
    (Timeout, Duration),
    (OnTimeout, String)
);

generate_extracted_config!(
    ClusterFeature,
    (ReoptimizeImportedViews, Option<bool>, Default(None)),
    (EnableEagerDeltaJoins, Option<bool>, Default(None)),
    (EnableNewOuterJoinLowering, Option<bool>, Default(None)),
    (EnableVariadicLeftJoinLowering, Option<bool>, Default(None)),
    (EnableLetrecFixpointAnalysis, Option<bool>, Default(None)),
    (EnableJoinPrioritizeArranged, Option<bool>, Default(None)),
    (
        EnableProjectionPushdownAfterRelationCse,
        Option<bool>,
        Default(None)
    )
);

/// Convert a [`CreateClusterStatement`] into a [`Plan`].
///
/// The reverse of [`unplan_create_cluster`].
pub fn plan_create_cluster(
    scx: &StatementContext,
    stmt: CreateClusterStatement<Aug>,
) -> Result<Plan, PlanError> {
    let plan = plan_create_cluster_inner(scx, stmt)?;

    // Roundtrip through unplan and make sure that we end up with the same plan.
    if let CreateClusterVariant::Managed(_) = &plan.variant {
        let stmt = unplan_create_cluster(scx, plan.clone())
            .map_err(|e| PlanError::Replan(e.to_string()))?;
        let create_sql = stmt.to_ast_string_stable();
        let stmt = parse::parse(&create_sql)
            .map_err(|e| PlanError::Replan(e.to_string()))?
            .into_element()
            .ast;
        let (stmt, _resolved_ids) =
            names::resolve(scx.catalog, stmt).map_err(|e| PlanError::Replan(e.to_string()))?;
        let stmt = match stmt {
            Statement::CreateCluster(stmt) => stmt,
            stmt => {
                return Err(PlanError::Replan(format!(
                    "replan does not match: plan={plan:?}, create_sql={create_sql:?}, stmt={stmt:?}"
                )));
            }
        };
        let replan =
            plan_create_cluster_inner(scx, stmt).map_err(|e| PlanError::Replan(e.to_string()))?;
        if plan != replan {
            return Err(PlanError::Replan(format!(
                "replan does not match: plan={plan:?}, replan={replan:?}"
            )));
        }
    }

    Ok(Plan::CreateCluster(plan))
}

pub fn plan_create_cluster_inner(
    scx: &StatementContext,
    CreateClusterStatement {
        name,
        options,
        features,
    }: CreateClusterStatement<Aug>,
) -> Result<CreateClusterPlan, PlanError> {
    let ClusterOptionExtracted {
        availability_zones,
        introspection_debugging,
        introspection_interval,
        managed,
        replicas,
        replication_factor,
        seen: _,
        size,
        disk,
        schedule,
        workload_class,
    }: ClusterOptionExtracted = options.try_into()?;

    let managed = managed.unwrap_or_else(|| replicas.is_none());

    if !scx.catalog.active_role_id().is_system() {
        if !features.is_empty() {
            sql_bail!("FEATURES not supported for non-system users");
        }
        if workload_class.is_some() {
            sql_bail!("WORKLOAD CLASS not supported for non-system users");
        }
    }

    let schedule = schedule.unwrap_or(ClusterScheduleOptionValue::Manual);
    let workload_class = workload_class.and_then(|v| v.0);

    if managed {
        if replicas.is_some() {
            sql_bail!("REPLICAS not supported for managed clusters");
        }
        let Some(size) = size else {
            sql_bail!("SIZE must be specified for managed clusters");
        };

        if disk.is_some() {
            // The `DISK` option is a no-op for legacy cluster sizes and was never allowed for
            // `cc` sizes. The long term plan is to phase out the legacy sizes, at which point
            // we'll be able to remove the `DISK` option entirely.
            if scx.catalog.is_cluster_size_cc(&size) {
                sql_bail!(
                    "DISK option not supported for modern cluster sizes because disk is always enabled"
                );
            }

            scx.catalog
                .add_notice(PlanNotice::ReplicaDiskOptionDeprecated);
        }

        let compute = plan_compute_replica_config(
            introspection_interval,
            introspection_debugging.unwrap_or(false),
        )?;

        let replication_factor = if matches!(schedule, ClusterScheduleOptionValue::Manual) {
            replication_factor.unwrap_or_else(|| {
                scx.catalog
                    .system_vars()
                    .default_cluster_replication_factor()
            })
        } else {
            scx.require_feature_flag(&ENABLE_CLUSTER_SCHEDULE_REFRESH)?;
            if replication_factor.is_some() {
                sql_bail!(
                    "REPLICATION FACTOR cannot be given together with any SCHEDULE other than MANUAL"
                );
            }
            // If we have a non-trivial schedule, then let's not have any replicas initially,
            // to avoid quickly going back and forth if the schedule doesn't want a replica
            // initially.
            0
        };
        let availability_zones = availability_zones.unwrap_or_default();

        if !availability_zones.is_empty() {
            scx.require_feature_flag(&vars::ENABLE_MANAGED_CLUSTER_AVAILABILITY_ZONES)?;
        }

        // Plan OptimizerFeatureOverrides.
        let ClusterFeatureExtracted {
            reoptimize_imported_views,
            enable_eager_delta_joins,
            enable_new_outer_join_lowering,
            enable_variadic_left_join_lowering,
            enable_letrec_fixpoint_analysis,
            enable_join_prioritize_arranged,
            enable_projection_pushdown_after_relation_cse,
            seen: _,
        } = ClusterFeatureExtracted::try_from(features)?;
        let optimizer_feature_overrides = OptimizerFeatureOverrides {
            reoptimize_imported_views,
            enable_eager_delta_joins,
            enable_new_outer_join_lowering,
            enable_variadic_left_join_lowering,
            enable_letrec_fixpoint_analysis,
            enable_join_prioritize_arranged,
            enable_projection_pushdown_after_relation_cse,
            ..Default::default()
        };

        let schedule = plan_cluster_schedule(schedule)?;

        Ok(CreateClusterPlan {
            name: normalize::ident(name),
            variant: CreateClusterVariant::Managed(CreateClusterManagedPlan {
                replication_factor,
                size,
                availability_zones,
                compute,
                optimizer_feature_overrides,
                schedule,
            }),
            workload_class,
        })
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
        if !features.is_empty() {
            sql_bail!("FEATURES not supported for unmanaged clusters");
        }
        if !matches!(schedule, ClusterScheduleOptionValue::Manual) {
            sql_bail!(
                "cluster schedules other than MANUAL are not supported for unmanaged clusters"
            );
        }

        let mut replicas = vec![];
        for ReplicaDefinition { name, options } in replica_defs {
            replicas.push((normalize::ident(name), plan_replica_config(scx, options)?));
        }

        Ok(CreateClusterPlan {
            name: normalize::ident(name),
            variant: CreateClusterVariant::Unmanaged(CreateClusterUnmanagedPlan { replicas }),
            workload_class,
        })
    }
}

/// Convert a [`CreateClusterPlan`] into a [`CreateClusterStatement`].
///
/// The reverse of [`plan_create_cluster`].
pub fn unplan_create_cluster(
    scx: &StatementContext,
    CreateClusterPlan {
        name,
        variant,
        workload_class,
    }: CreateClusterPlan,
) -> Result<CreateClusterStatement<Aug>, PlanError> {
    match variant {
        CreateClusterVariant::Managed(CreateClusterManagedPlan {
            replication_factor,
            size,
            availability_zones,
            compute,
            optimizer_feature_overrides,
            schedule,
        }) => {
            let schedule = unplan_cluster_schedule(schedule);
            let OptimizerFeatureOverrides {
                enable_guard_subquery_tablefunc: _,
                enable_consolidate_after_union_negate: _,
                enable_reduce_mfp_fusion: _,
                enable_cardinality_estimates: _,
                persist_fast_path_limit: _,
                reoptimize_imported_views,
                enable_eager_delta_joins,
                enable_new_outer_join_lowering,
                enable_variadic_left_join_lowering,
                enable_letrec_fixpoint_analysis,
                enable_reduce_reduction: _,
                enable_join_prioritize_arranged,
                enable_projection_pushdown_after_relation_cse,
                enable_less_reduce_in_eqprop: _,
                enable_dequadratic_eqprop_map: _,
                enable_eq_classes_withholding_errors: _,
                enable_fast_path_plan_insights: _,
            } = optimizer_feature_overrides;
            // The ones from above that don't occur below are not wired up to cluster features.
            let features_extracted = ClusterFeatureExtracted {
                // Seen is ignored when unplanning.
                seen: Default::default(),
                reoptimize_imported_views,
                enable_eager_delta_joins,
                enable_new_outer_join_lowering,
                enable_variadic_left_join_lowering,
                enable_letrec_fixpoint_analysis,
                enable_join_prioritize_arranged,
                enable_projection_pushdown_after_relation_cse,
            };
            let features = features_extracted.into_values(scx.catalog);
            let availability_zones = if availability_zones.is_empty() {
                None
            } else {
                Some(availability_zones)
            };
            let (introspection_interval, introspection_debugging) =
                unplan_compute_replica_config(compute);
            // Replication factor cannot be explicitly specified with a refresh schedule, it's
            // always 1 or less.
            let replication_factor = match &schedule {
                ClusterScheduleOptionValue::Manual => Some(replication_factor),
                ClusterScheduleOptionValue::Refresh { .. } => {
                    assert!(
                        replication_factor <= 1,
                        "replication factor, {replication_factor:?}, must be <= 1"
                    );
                    None
                }
            };
            let workload_class = workload_class.map(|s| OptionalString(Some(s)));
            let options_extracted = ClusterOptionExtracted {
                // Seen is ignored when unplanning.
                seen: Default::default(),
                availability_zones,
                disk: None,
                introspection_debugging: Some(introspection_debugging),
                introspection_interval,
                managed: Some(true),
                replicas: None,
                replication_factor,
                size: Some(size),
                schedule: Some(schedule),
                workload_class,
            };
            let options = options_extracted.into_values(scx.catalog);
            let name = Ident::new_unchecked(name);
            Ok(CreateClusterStatement {
                name,
                options,
                features,
            })
        }
        CreateClusterVariant::Unmanaged(_) => {
            bail_unsupported!("SHOW CREATE for unmanaged clusters")
        }
    }
}

generate_extracted_config!(
    ReplicaOption,
    (AvailabilityZone, String),
    (BilledAs, String),
    (ComputeAddresses, Vec<String>),
    (ComputectlAddresses, Vec<String>),
    (Disk, bool),
    (Internal, bool, Default(false)),
    (IntrospectionDebugging, bool, Default(false)),
    (IntrospectionInterval, OptionalDuration),
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
        computectl_addresses,
        disk,
        internal,
        introspection_debugging,
        introspection_interval,
        size,
        storagectl_addresses,
        ..
    }: ReplicaOptionExtracted = options.try_into()?;

    let compute = plan_compute_replica_config(introspection_interval, introspection_debugging)?;

    match (
        size,
        availability_zone,
        billed_as,
        storagectl_addresses,
        computectl_addresses,
    ) {
        // Common cases we expect end users to hit.
        (None, _, None, None, None) => {
            // We don't mention the unmanaged options in the error message
            // because they are only available in unsafe mode.
            sql_bail!("SIZE option must be specified");
        }
        (Some(size), availability_zone, billed_as, None, None) => {
            if disk.is_some() {
                // The `DISK` option is a no-op for legacy cluster sizes and was never allowed for
                // `cc` sizes. The long term plan is to phase out the legacy sizes, at which point
                // we'll be able to remove the `DISK` option entirely.
                if scx.catalog.is_cluster_size_cc(&size) {
                    sql_bail!(
                        "DISK option not supported for modern cluster sizes because disk is always enabled"
                    );
                }

                scx.catalog
                    .add_notice(PlanNotice::ReplicaDiskOptionDeprecated);
            }

            Ok(ReplicaConfig::Orchestrated {
                size,
                availability_zone,
                compute,
                billed_as,
                internal,
            })
        }

        (None, None, None, storagectl_addresses, computectl_addresses) => {
            scx.require_feature_flag(&vars::UNSAFE_ENABLE_UNORCHESTRATED_CLUSTER_REPLICAS)?;

            // When manually testing Materialize in unsafe mode, it's easy to
            // accidentally omit one of these options, so we try to produce
            // helpful error messages.
            let Some(storagectl_addrs) = storagectl_addresses else {
                sql_bail!("missing STORAGECTL ADDRESSES option");
            };
            let Some(computectl_addrs) = computectl_addresses else {
                sql_bail!("missing COMPUTECTL ADDRESSES option");
            };

            if storagectl_addrs.len() != computectl_addrs.len() {
                sql_bail!(
                    "COMPUTECTL ADDRESSES and STORAGECTL ADDRESSES must have the same length"
                );
            }

            if disk.is_some() {
                sql_bail!("DISK can't be specified for unorchestrated clusters");
            }

            Ok(ReplicaConfig::Unorchestrated {
                storagectl_addrs,
                computectl_addrs,
                compute,
            })
        }
        _ => {
            // We don't bother trying to produce a more helpful error message
            // here because no user is likely to hit this path.
            sql_bail!("invalid mixture of orchestrated and unorchestrated replica options");
        }
    }
}

/// Convert an [`Option<OptionalDuration>`] and [`bool`] into a [`ComputeReplicaConfig`].
///
/// The reverse of [`unplan_compute_replica_config`].
fn plan_compute_replica_config(
    introspection_interval: Option<OptionalDuration>,
    introspection_debugging: bool,
) -> Result<ComputeReplicaConfig, PlanError> {
    let introspection_interval = introspection_interval
        .map(|OptionalDuration(i)| i)
        .unwrap_or(Some(DEFAULT_REPLICA_LOGGING_INTERVAL));
    let introspection = match introspection_interval {
        Some(interval) => Some(ComputeReplicaIntrospectionConfig {
            interval,
            debugging: introspection_debugging,
        }),
        None if introspection_debugging => {
            sql_bail!("INTROSPECTION DEBUGGING cannot be specified without INTROSPECTION INTERVAL")
        }
        None => None,
    };
    let compute = ComputeReplicaConfig { introspection };
    Ok(compute)
}

/// Convert a [`ComputeReplicaConfig`] into an [`Option<OptionalDuration>`] and [`bool`].
///
/// The reverse of [`plan_compute_replica_config`].
fn unplan_compute_replica_config(
    compute_replica_config: ComputeReplicaConfig,
) -> (Option<OptionalDuration>, bool) {
    match compute_replica_config.introspection {
        Some(ComputeReplicaIntrospectionConfig {
            debugging,
            interval,
        }) => (Some(OptionalDuration(Some(interval))), debugging),
        None => (Some(OptionalDuration(None)), false),
    }
}

/// Convert a [`ClusterScheduleOptionValue`] into a [`ClusterSchedule`].
///
/// The reverse of [`unplan_cluster_schedule`].
fn plan_cluster_schedule(
    schedule: ClusterScheduleOptionValue,
) -> Result<ClusterSchedule, PlanError> {
    Ok(match schedule {
        ClusterScheduleOptionValue::Manual => ClusterSchedule::Manual,
        // If `HYDRATION TIME ESTIMATE` is not explicitly given, we default to 0.
        ClusterScheduleOptionValue::Refresh {
            hydration_time_estimate: None,
        } => ClusterSchedule::Refresh {
            hydration_time_estimate: Duration::from_millis(0),
        },
        // Otherwise we convert the `IntervalValue` to a `Duration`.
        ClusterScheduleOptionValue::Refresh {
            hydration_time_estimate: Some(interval_value),
        } => {
            let interval = Interval::try_from_value(Value::Interval(interval_value))?;
            if interval.as_microseconds() < 0 {
                sql_bail!(
                    "HYDRATION TIME ESTIMATE must be non-negative; got: {}",
                    interval
                );
            }
            if interval.months != 0 {
                // This limitation is because we want this interval to be cleanly convertable
                // to a unix epoch timestamp difference. When the interval involves months, then
                // this is not true anymore, because months have variable lengths.
                sql_bail!("HYDRATION TIME ESTIMATE must not involve units larger than days");
            }
            let duration = interval.duration()?;
            if u64::try_from(duration.as_millis()).is_err()
                || Interval::from_duration(&duration).is_err()
            {
                sql_bail!("HYDRATION TIME ESTIMATE too large");
            }
            ClusterSchedule::Refresh {
                hydration_time_estimate: duration,
            }
        }
    })
}

/// Convert a [`ClusterSchedule`] into a [`ClusterScheduleOptionValue`].
///
/// The reverse of [`plan_cluster_schedule`].
fn unplan_cluster_schedule(schedule: ClusterSchedule) -> ClusterScheduleOptionValue {
    match schedule {
        ClusterSchedule::Manual => ClusterScheduleOptionValue::Manual,
        ClusterSchedule::Refresh {
            hydration_time_estimate,
        } => {
            let interval = Interval::from_duration(&hydration_time_estimate)
                .expect("planning ensured that this is convertible back to Interval");
            let interval_value = literal::unplan_interval(&interval);
            ClusterScheduleOptionValue::Refresh {
                hydration_time_estimate: Some(interval_value),
            }
        }
    }
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
    let current_replica_count = cluster.replica_ids().iter().count();
    if contains_single_replica_objects(scx, cluster) && current_replica_count > 0 {
        let internal_replica_count = cluster.replicas().iter().filter(|r| r.internal()).count();
        return Err(PlanError::CreateReplicaFailStorageObjects {
            current_replica_count,
            internal_replica_count,
            hypothetical_replica_count: current_replica_count + 1,
        });
    }

    let config = plan_replica_config(scx, options)?;

    if let ReplicaConfig::Orchestrated { internal: true, .. } = &config {
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

generate_extracted_config!(CreateConnectionOption, (Validate, bool));

pub fn plan_create_connection(
    scx: &StatementContext,
    mut stmt: CreateConnectionStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateConnectionStatement {
        name,
        connection_type,
        values,
        if_not_exists,
        with_options,
    } = stmt.clone();
    let connection_options_extracted = connection::ConnectionOptionExtracted::try_from(values)?;
    let details = connection_options_extracted.try_into_connection_details(scx, connection_type)?;
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
                && details.to_connection().validate_by_default()
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

    // For SSH connections, overwrite the public key options based on the
    // connection details, in case we generated new keys during planning.
    if let ConnectionDetails::Ssh { key_1, key_2, .. } = &details {
        stmt.values.retain(|v| {
            v.name != ConnectionOptionName::PublicKey1 && v.name != ConnectionOptionName::PublicKey2
        });
        stmt.values.push(ConnectionOption {
            name: ConnectionOptionName::PublicKey1,
            value: Some(WithOptionValue::Value(Value::String(key_1.public_key()))),
        });
        stmt.values.push(ConnectionOption {
            name: ConnectionOptionName::PublicKey2,
            value: Some(WithOptionValue::Value(Value::String(key_2.public_key()))),
        });
    }
    let create_sql = normalize::create_statement(scx, Statement::CreateConnection(stmt))?;

    let plan = CreateConnectionPlan {
        name,
        if_not_exists,
        connection: crate::plan::Connection {
            create_sql,
            details,
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
            UnresolvedObjectName::NetworkPolicy(name) => {
                plan_drop_network_policy(scx, if_exists, name)?.map(ObjectId::NetworkPolicy)
            }
        };
        match id {
            Some(id) => referenced_ids.push(id),
            None => scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string_simple(),
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
                            name.as_str(),
                            role.name(),
                            member_role.name()
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
            Some(cluster.id())
        }
        None => None,
    })
}

fn plan_drop_network_policy(
    scx: &StatementContext,
    if_exists: bool,
    name: &Ident,
) -> Result<Option<NetworkPolicyId>, PlanError> {
    match scx.catalog.resolve_network_policy(name.as_str()) {
        Ok(policy) => {
            // TODO(network_policy): When we support role based network policies, check if any role
            // currently has the specified policy set.
            if scx.catalog.system_vars().default_network_policy_name() == policy.name() {
                Err(PlanError::NetworkPolicyInUse)
            } else {
                Ok(Some(policy.id()))
            }
        }
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Returns `true` if the cluster has any object that requires a single replica.
/// Returns `false` if the cluster has no objects.
fn contains_single_replica_objects(scx: &StatementContext, cluster: &dyn CatalogCluster) -> bool {
    // If this feature is enabled then all objects support multiple-replicas
    if ENABLE_MULTI_REPLICA_SOURCES.get(scx.catalog.system_vars().dyncfgs()) {
        false
    } else {
        // Othewise we check for the existence of sources or sinks
        cluster.bound_objects().iter().any(|id| {
            let item = scx.catalog.get_item(id);
            matches!(
                item.item_type(),
                CatalogItemType::Sink | CatalogItemType::Source
            )
        })
    }
}

fn plan_drop_cluster_replica(
    scx: &StatementContext,
    if_exists: bool,
    name: &QualifiedReplica,
) -> Result<Option<(ClusterId, ReplicaId)>, PlanError> {
    let cluster = resolve_cluster_replica(scx, name, if_exists)?;
    Ok(cluster.map(|(cluster, replica_id)| (cluster.id(), replica_id)))
}

/// Returns the [`CatalogItemId`] of the item we should drop, if it exists.
fn plan_drop_item(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: UnresolvedItemName,
    cascade: bool,
) -> Result<Option<CatalogItemId>, PlanError> {
    let resolved = match resolve_item_or_type(scx, object_type, name, if_exists) {
        Ok(r) => r,
        // Return a more helpful error on `DROP VIEW <materialized-view>`.
        Err(PlanError::MismatchedObjectType {
            name,
            is_type: ObjectType::MaterializedView,
            expected_type: ObjectType::View,
        }) => {
            return Err(PlanError::DropViewOnMaterializedView(name.to_string()));
        }
        e => e?,
    };

    Ok(match resolved {
        Some(catalog_item) => {
            if catalog_item.id().is_system() {
                sql_bail!(
                    "cannot drop {} {} because it is required by the database system",
                    catalog_item.item_type(),
                    scx.catalog.minimal_qualification(catalog_item.name()),
                );
            }

            if !cascade {
                for id in catalog_item.used_by() {
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
            | CatalogItemType::Connection
            | CatalogItemType::ContinualTask => true,
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
    drop: DropOwnedStatement<Aug>,
) -> Result<Plan, PlanError> {
    let cascade = drop.cascade();
    let role_ids: BTreeSet<_> = drop.role_names.into_iter().map(|role| role.id).collect();
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
        if role_ids.contains(&replica.owner_id()) {
            drop_ids.push((replica.cluster_id(), replica.replica_id()).into());
        }
    }

    // Clusters
    for cluster in scx.catalog.get_clusters() {
        if role_ids.contains(&cluster.owner_id()) {
            // Note: CASCADE is not required for replicas.
            if !cascade {
                let non_owned_bound_objects: Vec<_> = cluster
                    .bound_objects()
                    .into_iter()
                    .map(|item_id| scx.catalog.get_item(item_id))
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
                // Checks if any items still depend on this one, returning an error if so.
                let check_if_dependents_exist = |used_by: &[CatalogItemId]| {
                    let non_owned_dependencies: Vec<_> = used_by
                        .into_iter()
                        .map(|item_id| scx.catalog.get_item(item_id))
                        .filter(|item| dependency_prevents_drop(item.item_type().into(), *item))
                        .filter(|item| !role_ids.contains(&item.owner_id()))
                        .collect();
                    if !non_owned_dependencies.is_empty() {
                        let names: Vec<_> = non_owned_dependencies
                            .into_iter()
                            .map(|item| {
                                let item_typ = item.item_type().to_string();
                                let item_name =
                                    scx.catalog.resolve_full_name(item.name()).to_string();
                                (item_typ, item_name)
                            })
                            .collect();
                        Err(PlanError::DependentObjectsStillExist {
                            object_type: item.item_type().to_string(),
                            object_name: scx
                                .catalog
                                .resolve_full_name(item.name())
                                .to_string()
                                .to_string(),
                            dependents: names,
                        })
                    } else {
                        Ok(())
                    }
                };

                // When this item gets dropped it will also drop its progress source, so we need to
                // check the users of those.
                if let Some(id) = item.progress_id() {
                    let progress_item = scx.catalog.get_item(&id);
                    check_if_dependents_exist(progress_item.used_by())?;
                }
                check_if_dependents_exist(item.used_by())?;
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
                        .map(|item_id| scx.catalog.get_item(&item_id))
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

fn plan_retain_history_option(
    scx: &StatementContext,
    retain_history: Option<OptionalDuration>,
) -> Result<Option<CompactionWindow>, PlanError> {
    if let Some(OptionalDuration(lcw)) = retain_history {
        Ok(Some(plan_retain_history(scx, lcw)?))
    } else {
        Ok(None)
    }
}

// Convert a specified RETAIN HISTORY option into a compaction window. `None` corresponds to
// `DisableCompaction`. A zero duration will error. This is because the `OptionalDuration` type
// already converts the zero duration into `None`. This function must not be called in the `RESET
// (RETAIN HISTORY)` path, which should be handled by the outer `Option<OptionalDuration>` being
// `None`.
fn plan_retain_history(
    scx: &StatementContext,
    lcw: Option<Duration>,
) -> Result<CompactionWindow, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_LOGICAL_COMPACTION_WINDOW)?;
    match lcw {
        // A zero duration has already been converted to `None` by `OptionalDuration` (and means
        // disable compaction), and should never occur here. Furthermore, some things actually do
        // break when this is set to real zero:
        // https://github.com/MaterializeInc/database-issues/issues/3798.
        Some(Duration::ZERO) => Err(PlanError::InvalidOptionValue {
            option_name: "RETAIN HISTORY".to_string(),
            err: Box::new(PlanError::Unstructured(
                "internal error: unexpectedly zero".to_string(),
            )),
        }),
        Some(duration) => {
            // Error if the duration is low and enable_unlimited_retain_history is not set (which
            // should only be possible during testing).
            if duration < DEFAULT_LOGICAL_COMPACTION_WINDOW_DURATION
                && scx
                    .require_feature_flag(&vars::ENABLE_UNLIMITED_RETAIN_HISTORY)
                    .is_err()
            {
                return Err(PlanError::RetainHistoryLow {
                    limit: DEFAULT_LOGICAL_COMPACTION_WINDOW_DURATION,
                });
            }
            Ok(duration.try_into()?)
        }
        // In the past `RETAIN HISTORY FOR '0'` meant disable compaction. Disabling compaction seems
        // to be a bad choice, so prevent it.
        None => {
            if scx
                .require_feature_flag(&vars::ENABLE_UNLIMITED_RETAIN_HISTORY)
                .is_err()
            {
                Err(PlanError::RetainHistoryRequired)
            } else {
                Ok(CompactionWindow::DisableCompaction)
            }
        }
    }
}

generate_extracted_config!(IndexOption, (RetainHistory, OptionalDuration));

fn plan_index_options(
    scx: &StatementContext,
    with_opts: Vec<IndexOption<Aug>>,
) -> Result<Vec<crate::plan::IndexOption>, PlanError> {
    if !with_opts.is_empty() {
        // Index options are not durable.
        scx.require_feature_flag(&vars::ENABLE_INDEX_OPTIONS)?;
    }

    let IndexOptionExtracted { retain_history, .. }: IndexOptionExtracted = with_opts.try_into()?;

    let mut out = Vec::with_capacity(1);
    if let Some(cw) = plan_retain_history_option(scx, retain_history)? {
        out.push(crate::plan::IndexOption::RetainHistory(cw));
    }
    Ok(out)
}

generate_extracted_config!(
    TableOption,
    (PartitionBy, Vec<Ident>),
    (RetainHistory, OptionalDuration),
    (RedactedTest, String)
);

fn plan_table_options(
    scx: &StatementContext,
    desc: &RelationDesc,
    with_opts: Vec<TableOption<Aug>>,
) -> Result<Vec<crate::plan::TableOption>, PlanError> {
    let TableOptionExtracted {
        partition_by,
        retain_history,
        redacted_test,
        ..
    }: TableOptionExtracted = with_opts.try_into()?;

    if let Some(partition_by) = partition_by {
        scx.require_feature_flag(&ENABLE_COLLECTION_PARTITION_BY)?;
        check_partition_by(desc, partition_by)?;
    }

    if redacted_test.is_some() {
        scx.require_feature_flag(&vars::ENABLE_REDACTED_TEST_OPTION)?;
    }

    let mut out = Vec::with_capacity(1);
    if let Some(cw) = plan_retain_history_option(scx, retain_history)? {
        out.push(crate::plan::TableOption::RetainHistory(cw));
    }
    Ok(out)
}

pub fn plan_alter_index_options(
    scx: &mut StatementContext,
    AlterIndexStatement {
        index_name,
        if_exists,
        action,
    }: AlterIndexStatement<Aug>,
) -> Result<Plan, PlanError> {
    let object_type = ObjectType::Index;
    match action {
        AlterIndexAction::ResetOptions(options) => {
            let mut options = options.into_iter();
            if let Some(opt) = options.next() {
                match opt {
                    IndexOptionName::RetainHistory => {
                        if options.next().is_some() {
                            sql_bail!("RETAIN HISTORY must be only option");
                        }
                        return alter_retain_history(
                            scx,
                            object_type,
                            if_exists,
                            UnresolvedObjectName::Item(index_name),
                            None,
                        );
                    }
                }
            }
            sql_bail!("expected option");
        }
        AlterIndexAction::SetOptions(options) => {
            let mut options = options.into_iter();
            if let Some(opt) = options.next() {
                match opt.name {
                    IndexOptionName::RetainHistory => {
                        if options.next().is_some() {
                            sql_bail!("RETAIN HISTORY must be only option");
                        }
                        return alter_retain_history(
                            scx,
                            object_type,
                            if_exists,
                            UnresolvedObjectName::Item(index_name),
                            opt.value,
                        );
                    }
                }
            }
            sql_bail!("expected option");
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
                name: name.to_ast_string_simple(),
                object_type: ObjectType::Cluster,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Cluster,
            }));
        }
    };

    let mut options: PlanClusterOption = Default::default();
    let mut alter_strategy: AlterClusterPlanStrategy = AlterClusterPlanStrategy::None;

    match action {
        AlterClusterAction::SetOptions {
            options: set_options,
            with_options,
        } => {
            let ClusterOptionExtracted {
                availability_zones,
                introspection_debugging,
                introspection_interval,
                managed,
                replicas: replica_defs,
                replication_factor,
                seen: _,
                size,
                disk,
                schedule,
                workload_class,
            }: ClusterOptionExtracted = set_options.try_into()?;

            if !scx.catalog.active_role_id().is_system() {
                if workload_class.is_some() {
                    sql_bail!("WORKLOAD CLASS not supported for non-system users");
                }
            }

            match managed.unwrap_or_else(|| cluster.is_managed()) {
                true => {
                    let alter_strategy_extracted =
                        ClusterAlterOptionExtracted::try_from(with_options)?;
                    alter_strategy = AlterClusterPlanStrategy::try_from(alter_strategy_extracted)?;

                    match alter_strategy {
                        AlterClusterPlanStrategy::None => {}
                        _ => {
                            scx.require_feature_flag(
                                &crate::session::vars::ENABLE_ZERO_DOWNTIME_CLUSTER_RECONFIGURATION,
                            )?;
                        }
                    }

                    if replica_defs.is_some() {
                        sql_bail!("REPLICAS not supported for managed clusters");
                    }
                    if schedule.is_some()
                        && !matches!(schedule, Some(ClusterScheduleOptionValue::Manual))
                    {
                        scx.require_feature_flag(&ENABLE_CLUSTER_SCHEDULE_REFRESH)?;
                    }

                    if let Some(replication_factor) = replication_factor {
                        if schedule.is_some()
                            && !matches!(schedule, Some(ClusterScheduleOptionValue::Manual))
                        {
                            sql_bail!(
                                "REPLICATION FACTOR cannot be given together with any SCHEDULE other than MANUAL"
                            );
                        }
                        if let Some(current_schedule) = cluster.schedule() {
                            if !matches!(current_schedule, ClusterSchedule::Manual) {
                                sql_bail!(
                                    "REPLICATION FACTOR cannot be set if the cluster SCHEDULE is anything other than MANUAL"
                                );
                            }
                        }

                        let internal_replica_count =
                            cluster.replicas().iter().filter(|r| r.internal()).count();
                        let hypothetical_replica_count =
                            internal_replica_count + usize::cast_from(replication_factor);

                        // Total number of replicas running is internal replicas
                        // + replication factor.
                        if contains_single_replica_objects(scx, cluster)
                            && hypothetical_replica_count > 1
                        {
                            return Err(PlanError::CreateReplicaFailStorageObjects {
                                current_replica_count: cluster.replica_ids().iter().count(),
                                internal_replica_count,
                                hypothetical_replica_count,
                            });
                        }
                    } else if alter_strategy.is_some() {
                        // AlterClusterPlanStrategies that are not None will standup pending replicas of the new configuration
                        // and violate the single replica for sources constraint. If there are any storage objects (sources or sinks) we should
                        // just fail.
                        let internal_replica_count =
                            cluster.replicas().iter().filter(|r| r.internal()).count();
                        let hypothetical_replica_count = internal_replica_count * 2;
                        if contains_single_replica_objects(scx, cluster) {
                            return Err(PlanError::CreateReplicaFailStorageObjects {
                                current_replica_count: cluster.replica_ids().iter().count(),
                                internal_replica_count,
                                hypothetical_replica_count,
                            });
                        }
                    }
                }
                false => {
                    if !alter_strategy.is_none() {
                        sql_bail!("ALTER... WITH not supported for unmanaged clusters");
                    }
                    if availability_zones.is_some() {
                        sql_bail!("AVAILABILITY ZONES not supported for unmanaged clusters");
                    }
                    if replication_factor.is_some() {
                        sql_bail!("REPLICATION FACTOR not supported for unmanaged clusters");
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
                    if schedule.is_some()
                        && !matches!(schedule, Some(ClusterScheduleOptionValue::Manual))
                    {
                        sql_bail!(
                            "cluster schedules other than MANUAL are not supported for unmanaged clusters"
                        );
                    }
                    if let Some(current_schedule) = cluster.schedule() {
                        if !matches!(current_schedule, ClusterSchedule::Manual)
                            && schedule.is_none()
                        {
                            sql_bail!(
                                "when switching a cluster to unmanaged, if the managed \
                                cluster's SCHEDULE is anything other than MANUAL, you have to \
                                explicitly set the SCHEDULE to MANUAL"
                            );
                        }
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
            if let Some(size) = &size {
                options.size = AlterOptionParameter::Set(size.clone());
            }
            if let Some(availability_zones) = availability_zones {
                options.availability_zones = AlterOptionParameter::Set(availability_zones);
            }
            if let Some(introspection_debugging) = introspection_debugging {
                options.introspection_debugging =
                    AlterOptionParameter::Set(introspection_debugging);
            }
            if let Some(introspection_interval) = introspection_interval {
                options.introspection_interval = AlterOptionParameter::Set(introspection_interval);
            }
            if disk.is_some() {
                // The `DISK` option is a no-op for legacy cluster sizes and was never allowed for
                // `cc` sizes. The long term plan is to phase out the legacy sizes, at which point
                // we'll be able to remove the `DISK` option entirely.
                let size = size.as_deref().unwrap_or_else(|| {
                    cluster.managed_size().expect("cluster known to be managed")
                });
                if scx.catalog.is_cluster_size_cc(size) {
                    sql_bail!(
                        "DISK option not supported for modern cluster sizes because disk is always enabled"
                    );
                }

                scx.catalog
                    .add_notice(PlanNotice::ReplicaDiskOptionDeprecated);
            }
            if !replicas.is_empty() {
                options.replicas = AlterOptionParameter::Set(replicas);
            }
            if let Some(schedule) = schedule {
                options.schedule = AlterOptionParameter::Set(plan_cluster_schedule(schedule)?);
            }
            if let Some(workload_class) = workload_class {
                options.workload_class = AlterOptionParameter::Set(workload_class.0);
            }
        }
        AlterClusterAction::ResetOptions(reset_options) => {
            use AlterOptionParameter::Reset;
            use ClusterOptionName::*;

            if !scx.catalog.active_role_id().is_system() {
                if reset_options.contains(&WorkloadClass) {
                    sql_bail!("WORKLOAD CLASS not supported for non-system users");
                }
            }

            for option in reset_options {
                match option {
                    AvailabilityZones => options.availability_zones = Reset,
                    Disk => scx
                        .catalog
                        .add_notice(PlanNotice::ReplicaDiskOptionDeprecated),
                    IntrospectionInterval => options.introspection_interval = Reset,
                    IntrospectionDebugging => options.introspection_debugging = Reset,
                    Managed => options.managed = Reset,
                    Replicas => options.replicas = Reset,
                    ReplicationFactor => options.replication_factor = Reset,
                    Size => options.size = Reset,
                    Schedule => options.schedule = Reset,
                    WorkloadClass => options.workload_class = Reset,
                }
            }
        }
    }
    Ok(Plan::AlterCluster(AlterClusterPlan {
        id: cluster.id(),
        name: cluster.name().to_string(),
        options,
        strategy: alter_strategy,
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
            bail_unsupported!(29606, format!("ALTER {object_type} SET CLUSTER"))
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

    match resolve_item_or_type(scx, object_type, name.clone(), if_exists)? {
        Some(entry) => {
            let current_cluster = entry.cluster_id();
            let Some(current_cluster) = current_cluster else {
                sql_bail!("No cluster associated with {name}");
            };

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
                name: name.to_ast_string_simple(),
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
            name: name.to_ast_string_simple(),
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
        let mut temp_name = ident!("mz_schema_swap_");
        temp_name.append_lossy(temp_suffix);
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
    let resolved = match resolve_item_or_type(scx, object_type, name.clone(), if_exists) {
        Ok(r) => r,
        // Return a more helpful error on `DROP VIEW <materialized-view>`.
        Err(PlanError::MismatchedObjectType {
            name,
            is_type: ObjectType::MaterializedView,
            expected_type: ObjectType::View,
        }) => {
            return Err(PlanError::AlterViewOnMaterializedView(name.to_string()));
        }
        e => e?,
    };

    match resolved {
        Some(entry) => {
            let full_name = scx.catalog.resolve_full_name(entry.name());
            let item_type = entry.item_type();

            let proposed_name = QualifiedItemName {
                qualifiers: entry.name().qualifiers.clone(),
                item: to_item_name.clone().into_string(),
            };

            // For PostgreSQL compatibility, items and types cannot have
            // overlapping names in a variety of situations. See the comment on
            // `CatalogItemType::conflicts_with_type` for details.
            let conflicting_type_exists;
            let conflicting_item_exists;
            if item_type == CatalogItemType::Type {
                conflicting_type_exists = scx.catalog.get_type_by_name(&proposed_name).is_some();
                conflicting_item_exists = scx
                    .catalog
                    .get_item_by_name(&proposed_name)
                    .map(|item| item.item_type().conflicts_with_type())
                    .unwrap_or(false);
            } else {
                conflicting_type_exists = item_type.conflicts_with_type()
                    && scx.catalog.get_type_by_name(&proposed_name).is_some();
                conflicting_item_exists = scx.catalog.get_item_by_name(&proposed_name).is_some();
            };
            if conflicting_type_exists || conflicting_item_exists {
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
                name: name.to_ast_string_simple(),
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
                name: name.to_ast_string_simple(),
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
        let mut temp_name = ident!("mz_schema_swap_");
        temp_name.append_lossy(temp_suffix);
        match scx.catalog.resolve_cluster(Some(temp_name.as_str())) {
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
                        cluster: Ident::new(cluster.name())?,
                        replica: name.replica,
                    },
                    to_name: normalize::ident(to_item_name),
                },
            ))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string_simple(),
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

            // Call the provided closure to make sure this name is unique!
            let short_id = mz_ore::id_gen::temp_id();
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
            discussion_no: None,
        }),
    }
}

pub fn describe_alter_retain_history(
    _: &StatementContext,
    _: AlterRetainHistoryStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_retain_history(
    scx: &StatementContext,
    AlterRetainHistoryStatement {
        object_type,
        if_exists,
        name,
        history,
    }: AlterRetainHistoryStatement<Aug>,
) -> Result<Plan, PlanError> {
    alter_retain_history(scx, object_type.into(), if_exists, name, history)
}

fn alter_retain_history(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: UnresolvedObjectName,
    history: Option<WithOptionValue<Aug>>,
) -> Result<Plan, PlanError> {
    let name = match (object_type, name) {
        (
            // View gets a special error below.
            ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Table
            | ObjectType::Source
            | ObjectType::Index,
            UnresolvedObjectName::Item(name),
        ) => name,
        (object_type, _) => {
            sql_bail!("{object_type} does not support RETAIN HISTORY")
        }
    };
    match resolve_item_or_type(scx, object_type, name.clone(), if_exists)? {
        Some(entry) => {
            let full_name = scx.catalog.resolve_full_name(entry.name());
            let item_type = entry.item_type();

            // Return a more helpful error on `ALTER VIEW <materialized-view>`.
            if object_type == ObjectType::View && item_type == CatalogItemType::MaterializedView {
                return Err(PlanError::AlterViewOnMaterializedView(
                    full_name.to_string(),
                ));
            } else if object_type == ObjectType::View {
                sql_bail!("{object_type} does not support RETAIN HISTORY")
            } else if object_type != item_type {
                sql_bail!(
                    "\"{}\" is a {} not a {}",
                    full_name,
                    entry.item_type(),
                    format!("{object_type}").to_lowercase()
                )
            }

            // Save the original value so we can write it back down in the create_sql catalog item.
            let (value, lcw) = match &history {
                Some(WithOptionValue::RetainHistoryFor(value)) => {
                    let window = OptionalDuration::try_from_value(value.clone())?;
                    (Some(value.clone()), window.0)
                }
                // None is RESET, so use the default CW.
                None => (None, Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_DURATION)),
                _ => sql_bail!("unexpected value type for RETAIN HISTORY"),
            };
            let window = plan_retain_history(scx, lcw)?;

            Ok(Plan::AlterRetainHistory(AlterRetainHistoryPlan {
                id: entry.id(),
                value,
                window,
                object_type,
            }))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string_simple(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
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
    let object_type = ObjectType::Secret;
    let id = match resolve_item_or_type(scx, object_type, name.clone(), if_exists)? {
        Some(entry) => entry.id(),
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_string(),
                object_type,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
        }
    };

    let secret_as = query::plan_secret_as(scx, value)?;

    Ok(Plan::AlterSecret(AlterSecretPlan { id, secret_as }))
}

pub fn describe_alter_connection(
    _: &StatementContext,
    _: AlterConnectionStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(AlterConnectionOption, (Validate, bool));

pub fn plan_alter_connection(
    scx: &StatementContext,
    stmt: AlterConnectionStatement<Aug>,
) -> Result<Plan, PlanError> {
    let AlterConnectionStatement {
        name,
        if_exists,
        actions,
        with_options,
    } = stmt;
    let conn_name = normalize::unresolved_item_name(name)?;
    let entry = match scx.catalog.resolve_item(&conn_name) {
        Ok(entry) => entry,
        Err(_) if if_exists => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: conn_name.to_string(),
                object_type: ObjectType::Sink,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Connection,
            }));
        }
        Err(e) => return Err(e.into()),
    };

    let connection = entry.connection()?;

    if actions
        .iter()
        .any(|action| matches!(action, AlterConnectionAction::RotateKeys))
    {
        if actions.len() > 1 {
            sql_bail!("cannot specify any other actions alongside ALTER CONNECTION...ROTATE KEYS");
        }

        if !with_options.is_empty() {
            sql_bail!(
                "ALTER CONNECTION...ROTATE KEYS does not support WITH ({})",
                with_options
                    .iter()
                    .map(|o| o.to_ast_string_simple())
                    .join(", ")
            );
        }

        if !matches!(connection, Connection::Ssh(_)) {
            sql_bail!(
                "{} is not an SSH connection",
                scx.catalog.resolve_full_name(entry.name())
            )
        }

        return Ok(Plan::AlterConnection(AlterConnectionPlan {
            id: entry.id(),
            action: crate::plan::AlterConnectionAction::RotateKeys,
        }));
    }

    let options = AlterConnectionOptionExtracted::try_from(with_options)?;
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

    let connection_type = match connection {
        Connection::Aws(_) => CreateConnectionType::Aws,
        Connection::AwsPrivatelink(_) => CreateConnectionType::AwsPrivatelink,
        Connection::Kafka(_) => CreateConnectionType::Kafka,
        Connection::Csr(_) => CreateConnectionType::Csr,
        Connection::Postgres(_) => CreateConnectionType::Postgres,
        Connection::Ssh(_) => CreateConnectionType::Ssh,
        Connection::MySql(_) => CreateConnectionType::MySql,
        Connection::SqlServer(_) => CreateConnectionType::SqlServer,
        Connection::IcebergCatalog(_) => CreateConnectionType::IcebergCatalog,
    };

    // Collect all options irrespective of action taken on them.
    let specified_options: BTreeSet<_> = actions
        .iter()
        .map(|action: &AlterConnectionAction<Aug>| match action {
            AlterConnectionAction::SetOption(option) => option.name.clone(),
            AlterConnectionAction::DropOption(name) => name.clone(),
            AlterConnectionAction::RotateKeys => unreachable!(),
        })
        .collect();

    for invalid in INALTERABLE_OPTIONS {
        if specified_options.contains(invalid) {
            sql_bail!("cannot ALTER {} option {}", connection_type, invalid);
        }
    }

    connection::validate_options_per_connection_type(connection_type, specified_options)?;

    // Partition operations into set and drop
    let (set_options_vec, mut drop_options): (Vec<_>, BTreeSet<_>) =
        actions.into_iter().partition_map(|action| match action {
            AlterConnectionAction::SetOption(option) => Either::Left(option),
            AlterConnectionAction::DropOption(name) => Either::Right(name),
            AlterConnectionAction::RotateKeys => unreachable!(),
        });

    let set_options: BTreeMap<_, _> = set_options_vec
        .clone()
        .into_iter()
        .map(|option| (option.name, option.value))
        .collect();

    // Type check values + avoid duplicates; we don't want to e.g. let users
    // drop and set the same option in the same statement, so treating drops as
    // sets here is fine.
    let connection_options_extracted =
        connection::ConnectionOptionExtracted::try_from(set_options_vec)?;

    let duplicates: Vec<_> = connection_options_extracted
        .seen
        .intersection(&drop_options)
        .collect();

    if !duplicates.is_empty() {
        sql_bail!(
            "cannot both SET and DROP/RESET options {}",
            duplicates
                .iter()
                .map(|option| option.to_string())
                .join(", ")
        )
    }

    for mutually_exclusive_options in MUTUALLY_EXCLUSIVE_SETS {
        let set_options_count = mutually_exclusive_options
            .iter()
            .filter(|o| set_options.contains_key(o))
            .count();
        let drop_options_count = mutually_exclusive_options
            .iter()
            .filter(|o| drop_options.contains(o))
            .count();

        // Disallow setting _and_ resetting mutually exclusive options
        if set_options_count > 0 && drop_options_count > 0 {
            sql_bail!(
                "cannot both SET and DROP/RESET mutually exclusive {} options {}",
                connection_type,
                mutually_exclusive_options
                    .iter()
                    .map(|option| option.to_string())
                    .join(", ")
            )
        }

        // If any option is either set or dropped, ensure all mutually exclusive
        // options are dropped. We do this "behind the scenes", even though we
        // disallow users from performing the same action because this is the
        // mechanism by which we overwrite values elsewhere in the code.
        if set_options_count > 0 || drop_options_count > 0 {
            drop_options.extend(mutually_exclusive_options.iter().cloned());
        }

        // n.b. if mutually exclusive options are set, those will error when we
        // try to replan the connection.
    }

    Ok(Plan::AlterConnection(AlterConnectionPlan {
        id: entry.id(),
        action: crate::plan::AlterConnectionAction::AlterOptions {
            set_options,
            drop_options,
            validate,
        },
    }))
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

    let object_type = ObjectType::Sink;
    let item = resolve_item_or_type(scx, object_type, sink_name.clone(), if_exists)?;

    let Some(item) = item else {
        scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
            name: sink_name.to_string(),
            object_type,
        });

        return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
    };
    // Always ALTER objects from their latest version.
    let item = item.at_version(RelationVersionSelector::Latest);

    match action {
        AlterSinkAction::ChangeRelation(new_from) => {
            // First we reconstruct the original CREATE SINK statement
            let create_sql = item.create_sql();
            let stmts = mz_sql_parser::parser::parse_statements(create_sql)?;
            let [stmt]: [StatementParseResult; 1] = stmts
                .try_into()
                .expect("create sql of sink was not exactly one statement");
            let Statement::CreateSink(stmt) = stmt.ast else {
                unreachable!("invalid create SQL for sink item");
            };

            // Then resolve and swap the resolved from relation to the new one
            let (mut stmt, _) = crate::names::resolve(scx.catalog, stmt)?;
            stmt.from = new_from;

            // Finally re-plan the modified create sink statement to verify the new configuration is valid
            let Plan::CreateSink(mut plan) = plan_sink(scx, stmt)? else {
                unreachable!("invalid plan for CREATE SINK statement");
            };

            plan.sink.version += 1;

            Ok(Plan::AlterSink(AlterSinkPlan {
                item_id: item.id(),
                global_id: item.global_id(),
                sink: plan.sink,
                with_snapshot: plan.with_snapshot,
                in_cluster: plan.in_cluster,
            }))
        }
        AlterSinkAction::SetOptions(_) => bail_unsupported!("ALTER SINK SET options"),
        AlterSinkAction::ResetOptions(_) => bail_unsupported!("ALTER SINK RESET option"),
    }
}

pub fn describe_alter_source(
    _: &StatementContext,
    _: AlterSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    // TODO: put the options here, right?
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(
    AlterSourceAddSubsourceOption,
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![])),
    (ExcludeColumns, Vec::<UnresolvedItemName>, Default(vec![])),
    (Details, String)
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
    let object_type = ObjectType::Source;

    if resolve_item_or_type(scx, object_type, source_name.clone(), if_exists)?.is_none() {
        scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
            name: source_name.to_string(),
            object_type,
        });

        return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
    }

    match action {
        AlterSourceAction::SetOptions(options) => {
            let mut options = options.into_iter();
            let option = options.next().unwrap();
            if option.name == CreateSourceOptionName::RetainHistory {
                if options.next().is_some() {
                    sql_bail!("RETAIN HISTORY must be only option");
                }
                return alter_retain_history(
                    scx,
                    object_type,
                    if_exists,
                    UnresolvedObjectName::Item(source_name),
                    option.value,
                );
            }
            // n.b we use this statement in purification in a way that cannot be
            // planned directly.
            sql_bail!(
                "Cannot modify the {} of a SOURCE.",
                option.name.to_ast_string_simple()
            );
        }
        AlterSourceAction::ResetOptions(reset) => {
            let mut options = reset.into_iter();
            let option = options.next().unwrap();
            if option == CreateSourceOptionName::RetainHistory {
                if options.next().is_some() {
                    sql_bail!("RETAIN HISTORY must be only option");
                }
                return alter_retain_history(
                    scx,
                    object_type,
                    if_exists,
                    UnresolvedObjectName::Item(source_name),
                    None,
                );
            }
            sql_bail!(
                "Cannot modify the {} of a SOURCE.",
                option.to_ast_string_simple()
            );
        }
        AlterSourceAction::DropSubsources { .. } => {
            sql_bail!("ALTER SOURCE...DROP SUBSOURCE no longer supported; use DROP SOURCE")
        }
        AlterSourceAction::AddSubsources { .. } => {
            unreachable!("ALTER SOURCE...ADD SUBSOURCE must be purified")
        }
        AlterSourceAction::RefreshReferences => {
            unreachable!("ALTER SOURCE...REFRESH REFERENCES must be purified")
        }
    };
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
            let attrs = plan_role_attributes(attrs, scx)?;
            PlannedAlterRoleOption::Attributes(attrs)
        }
        AlterRoleOption::Variable(variable) => {
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

pub fn describe_alter_table_add_column(
    _: &StatementContext,
    _: AlterTableAddColumnStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_table_add_column(
    scx: &StatementContext,
    stmt: AlterTableAddColumnStatement<Aug>,
) -> Result<Plan, PlanError> {
    let AlterTableAddColumnStatement {
        if_exists,
        name,
        if_col_not_exist,
        column_name,
        data_type,
    } = stmt;
    let object_type = ObjectType::Table;

    scx.require_feature_flag(&vars::ENABLE_ALTER_TABLE_ADD_COLUMN)?;

    let (relation_id, item_name, desc) =
        match resolve_item_or_type(scx, object_type, name.clone(), if_exists)? {
            Some(item) => {
                // Always add columns to the latest version of the item.
                let item_name = scx.catalog.resolve_full_name(item.name());
                let item = item.at_version(RelationVersionSelector::Latest);
                let desc = item.relation_desc().expect("table has desc").into_owned();
                (item.id(), item_name, desc)
            }
            None => {
                scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                    name: name.to_ast_string_simple(),
                    object_type,
                });
                return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
            }
        };

    let column_name = ColumnName::from(column_name.as_str());
    if desc.get_by_name(&column_name).is_some() {
        if if_col_not_exist {
            scx.catalog.add_notice(PlanNotice::ColumnAlreadyExists {
                column_name: column_name.to_string(),
                object_name: item_name.item,
            });
            return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
        } else {
            return Err(PlanError::ColumnAlreadyExists {
                column_name,
                object_name: item_name.item,
            });
        }
    }

    let scalar_type = scalar_type_from_sql(scx, &data_type)?;
    // TODO(alter_table): Support non-nullable columns with default values.
    let column_type = scalar_type.nullable(true);
    // "unresolve" our data type so we can later update the persisted create_sql.
    let raw_sql_type = mz_sql_parser::parser::parse_data_type(&data_type.to_ast_string_stable())?;

    Ok(Plan::AlterTableAddColumn(AlterTablePlan {
        relation_id,
        column_name,
        column_type,
        raw_sql_type,
    }))
}

pub fn describe_alter_materialized_view_apply_replacement(
    _: &StatementContext,
    _: AlterMaterializedViewApplyReplacementStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_materialized_view_apply_replacement(
    scx: &StatementContext,
    stmt: AlterMaterializedViewApplyReplacementStatement,
) -> Result<Plan, PlanError> {
    let AlterMaterializedViewApplyReplacementStatement {
        if_exists,
        name,
        replacement_name,
    } = stmt;

    scx.require_feature_flag(&vars::ENABLE_REPLACEMENT_MATERIALIZED_VIEWS)?;

    let object_type = ObjectType::MaterializedView;
    let Some(mv) = resolve_item_or_type(scx, object_type, name.clone(), if_exists)? else {
        scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
            name: name.to_ast_string_simple(),
            object_type,
        });
        return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
    };

    let replacement = resolve_item_or_type(scx, object_type, replacement_name, false)?
        .expect("if_exists not set");

    if replacement.replacement_target() != Some(mv.id()) {
        return Err(PlanError::InvalidReplacement {
            item_type: mv.item_type(),
            item_name: scx.catalog.minimal_qualification(mv.name()),
            replacement_type: replacement.item_type(),
            replacement_name: scx.catalog.minimal_qualification(replacement.name()),
        });
    }

    Ok(Plan::AlterMaterializedViewApplyReplacement(
        AlterMaterializedViewApplyReplacementPlan {
            id: mv.id(),
            replacement_id: replacement.id(),
        },
    ))
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
        | com_ty @ CommentObjectType::Secret { name }
        | com_ty @ CommentObjectType::ContinualTask { name } => {
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
                (CommentObjectType::Secret { .. }, CatalogItemType::Secret) => {
                    (CommentObjectId::Secret(item.id()), None)
                }
                (CommentObjectType::ContinualTask { .. }, CatalogItemType::ContinualTask) => {
                    (CommentObjectId::ContinualTask(item.id()), None)
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
        CommentObjectType::Type { ty } => match ty {
            ResolvedDataType::AnonymousList(_) | ResolvedDataType::AnonymousMap { .. } => {
                sql_bail!("cannot comment on anonymous list or map type");
            }
            ResolvedDataType::Named { id, modifiers, .. } => {
                if !modifiers.is_empty() {
                    sql_bail!("cannot comment on type with modifiers");
                }
                (CommentObjectId::Type(*id), None)
            }
            ResolvedDataType::Error => unreachable!("should have been caught in name resolution"),
        },
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
                        discussion_no: None,
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
        CommentObjectType::NetworkPolicy { name } => {
            (CommentObjectId::NetworkPolicy(name.id), None)
        }
    };

    // Note: the `mz_comments` table uses an `Int4` for the column position, but in the catalog storage we
    // store a `usize` which would be a `Uint8`. We guard against a safe conversion here because
    // it's the easiest place to raise an error.
    //
    // TODO(parkmycar): https://github.com/MaterializeInc/database-issues/issues/6711.
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

pub(crate) fn resolve_network_policy<'a>(
    scx: &'a StatementContext,
    name: Ident,
    if_exists: bool,
) -> Result<Option<ResolvedNetworkPolicyName>, PlanError> {
    match scx.catalog.resolve_network_policy(&name.to_string()) {
        Ok(policy) => Ok(Some(ResolvedNetworkPolicyName {
            id: policy.id(),
            name: policy.name().to_string(),
        })),
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn resolve_item_or_type<'a>(
    scx: &'a StatementContext,
    object_type: ObjectType,
    name: UnresolvedItemName,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogItem>, PlanError> {
    let name = normalize::unresolved_item_name(name)?;
    let catalog_item = match object_type {
        ObjectType::Type => scx.catalog.resolve_type(&name),
        _ => scx.catalog.resolve_item(&name),
    };

    match catalog_item {
        Ok(item) => {
            let is_type = ObjectType::from(item.item_type());
            if object_type == is_type {
                Ok(Some(item))
            } else {
                Err(PlanError::MismatchedObjectType {
                    name: scx.catalog.minimal_qualification(item.name()),
                    is_type,
                    expected_type: object_type,
                })
            }
        }
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e.into()),
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
