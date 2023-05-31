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
use mz_controller::clusters::{ClusterId, ReplicaId, DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS};
use mz_expr::CollectionPlan;
use mz_interchange::avro::AvroSchemaGenerator;
use mz_ore::cast::{self, CastFrom, TryCastFrom};
use mz_ore::str::StrExt;
use mz_proto::RustType;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem, PrivilegeMap};
use mz_repr::adt::system::Oid;
use mz_repr::role_id::RoleId;
use mz_repr::{strconv, ColumnName, ColumnType, GlobalId, RelationDesc, RelationType, ScalarType};
use mz_sql_parser::ast::display::comma_separated;
use mz_sql_parser::ast::{
    AlterOwnerStatement, AlterRoleStatement, AlterSinkAction, AlterSinkStatement,
    AlterSourceAction, AlterSourceStatement, AlterSystemResetAllStatement,
    AlterSystemResetStatement, AlterSystemSetStatement, CreateTypeListOption,
    CreateTypeListOptionName, CreateTypeMapOption, CreateTypeMapOptionName, DeferredItemName,
    DropOwnedStatement, GrantPrivilegesStatement, GrantRoleStatement, Privilege,
    PrivilegeSpecification, ReassignOwnedStatement, RevokePrivilegesStatement, RevokeRoleStatement,
    SshConnectionOption, UnresolvedItemName, UnresolvedObjectName, UnresolvedSchemaName, Value,
};
use mz_storage_client::types::connections::aws::{AwsAssumeRole, AwsConfig, AwsCredentials};
use mz_storage_client::types::connections::{
    AwsPrivatelink, AwsPrivatelinkConnection, Connection, CsrConnectionHttpAuth, KafkaConnection,
    KafkaSecurity, KafkaTlsConfig, SaslConfig, SshTunnel, StringOrSecret, TlsIdentity, Tunnel,
};
use mz_storage_client::types::sinks::{
    KafkaConsistencyConfig, KafkaSinkConnectionBuilder, KafkaSinkConnectionRetention,
    KafkaSinkFormat, SinkEnvelope, StorageSinkConnectionBuilder,
};
use mz_storage_client::types::sources::encoding::{
    included_column_desc, AvroEncoding, ColumnSpec, CsvEncoding, DataEncoding, DataEncodingInner,
    ProtobufEncoding, RegexEncoding, SourceDataEncoding, SourceDataEncodingInner,
};
use mz_storage_client::types::sources::{
    GenericSourceConnection, IncludedColumnPos, KafkaSourceConnection, KeyEnvelope, LoadGenerator,
    LoadGeneratorSourceConnection, PostgresSourceConnection, PostgresSourcePublicationDetails,
    ProtoPostgresSourcePublicationDetails, SourceConnection, SourceDesc, SourceEnvelope,
    TestScriptSourceConnection, Timeline, UnplannedSourceEnvelope, UpsertStyle,
};
use prost::Message;
use regex::Regex;
use tracing::warn;

use crate::ast::display::AstDisplay;
use crate::ast::{
    AlterConnectionStatement, AlterIndexAction, AlterIndexStatement, AlterObjectRenameStatement,
    AlterSecretStatement, AvroSchema, AvroSchemaOption, AvroSchemaOptionName, AwsConnectionOption,
    AwsConnectionOptionName, AwsPrivatelinkConnectionOption, AwsPrivatelinkConnectionOptionName,
    ClusterOption, ClusterOptionName, ColumnOption, CreateClusterReplicaStatement,
    CreateClusterStatement, CreateConnection, CreateConnectionStatement, CreateDatabaseStatement,
    CreateIndexStatement, CreateMaterializedViewStatement, CreateRoleStatement,
    CreateSchemaStatement, CreateSecretStatement, CreateSinkConnection, CreateSinkOption,
    CreateSinkOptionName, CreateSinkStatement, CreateSourceConnection, CreateSourceFormat,
    CreateSourceOption, CreateSourceOptionName, CreateSourceStatement, CreateSubsourceOption,
    CreateSubsourceOptionName, CreateSubsourceStatement, CreateTableStatement, CreateTypeAs,
    CreateTypeStatement, CreateViewStatement, CsrConfigOption, CsrConfigOptionName, CsrConnection,
    CsrConnectionAvro, CsrConnectionOption, CsrConnectionOptionName, CsrConnectionProtobuf,
    CsrSeedProtobuf, CsvColumns, DbzMode, DropObjectsStatement, Envelope, Expr, Format, Ident,
    IfExistsBehavior, IndexOption, IndexOptionName, KafkaBroker, KafkaBrokerAwsPrivatelinkOption,
    KafkaBrokerAwsPrivatelinkOptionName, KafkaBrokerTunnel, KafkaConfigOptionName,
    KafkaConnectionOption, KafkaConnectionOptionName, KeyConstraint, LoadGeneratorOption,
    LoadGeneratorOptionName, ObjectType, PgConfigOption, PgConfigOptionName,
    PostgresConnectionOption, PostgresConnectionOptionName, ProtobufSchema, QualifiedReplica,
    ReferencedSubsources, ReplicaDefinition, ReplicaOption, ReplicaOptionName, RoleAttribute,
    SourceIncludeMetadata, SourceIncludeMetadataType, SshConnectionOptionName, Statement,
    TableConstraint, UnresolvedDatabaseName, ViewDefinition,
};
use crate::catalog::{
    CatalogCluster, CatalogDatabase, CatalogItem, CatalogItemType, CatalogType, CatalogTypeDetails,
};
use crate::kafka_util::{self, KafkaConfigOptionExtracted, KafkaStartOffsetType};
use crate::names::{
    Aug, DatabaseId, ObjectId, PartialItemName, QualifiedItemName, RawDatabaseSpecifier,
    ResolvedClusterName, ResolvedDataType, ResolvedDatabaseSpecifier, ResolvedItemName,
    ResolvedObjectName, ResolvedRoleName, SchemaSpecifier,
};
use crate::normalize::{self, ident};
use crate::plan::error::PlanError;
use crate::plan::expr::ColumnRef;
use crate::plan::query::{ExprContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::{scl, StatementContext, StatementDesc};
use crate::plan::typeconv::{plan_cast, CastContext};
use crate::plan::with_options::{self, OptionalInterval, TryFromValue};
use crate::plan::{
    plan_utils, query, transform_ast, AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan,
    AlterItemRenamePlan, AlterNoopPlan, AlterOptionParameter, AlterOwnerPlan, AlterRolePlan,
    AlterSecretPlan, AlterSinkPlan, AlterSourcePlan, AlterSystemResetAllPlan, AlterSystemResetPlan,
    AlterSystemSetPlan, ComputeReplicaConfig, ComputeReplicaIntrospectionConfig, CreateClusterPlan,
    CreateClusterReplicaPlan, CreateConnectionPlan, CreateDatabasePlan, CreateIndexPlan,
    CreateMaterializedViewPlan, CreateRolePlan, CreateSchemaPlan, CreateSecretPlan, CreateSinkPlan,
    CreateSourcePlan, CreateTablePlan, CreateTypePlan, CreateViewPlan, DataSourceDesc,
    DropObjectsPlan, DropOwnedPlan, FullItemName, GrantPrivilegesPlan, GrantRolePlan,
    HirScalarExpr, Index, Ingestion, MaterializedView, Params, Plan, QueryContext,
    ReassignOwnedPlan, ReplicaConfig, RevokePrivilegesPlan, RevokeRolePlan, RotateKeysPlan, Secret,
    Sink, Source, SourceSinkClusterConfig, Table, Type, UpdatePrivilege, View,
};
use crate::session::user::SYSTEM_USER;
use crate::session::vars;

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
    (TimestampInterval, Interval),
    (Disk, bool)
);

generate_extracted_config!(
    PgConfigOption,
    (Details, String),
    (Publication, String),
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

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

    let mut allowed_with_options = vec![CreateSourceOptionName::Size];

    if scx
        .require_feature_flag(&vars::ENABLE_UPSERT_SOURCE_DISK)
        .is_ok()
    {
        allowed_with_options.push(CreateSourceOptionName::Disk);
    }

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
                _ => sql_bail!("{} is not a kafka connection", connection_item.name()),
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

            let mut connection = KafkaSourceConnection {
                connection: kafka_connection,
                connection_id: connection_item.id(),
                topic,
                start_offsets,
                group_id_prefix,
                environment_id: scx.catalog.config().environment_id.to_string(),
                include_timestamp: None,
                include_partition: None,
                include_topic: None,
                include_offset: None,
                include_headers: None,
            };

            let unwrap_name = |alias: Option<Ident>, default, pos| {
                Some(IncludedColumnPos {
                    name: alias
                        .map(|a| a.to_string())
                        .unwrap_or_else(|| String::from(default)),
                    pos,
                })
            };

            if !matches!(envelope, Envelope::Upsert | Envelope::None)
                && include_metadata
                    .iter()
                    .any(|sic| sic.ty == SourceIncludeMetadataType::Headers)
            {
                // TODO(guswynn): should this be `bail_unsupported!`?
                sql_bail!("INCLUDE HEADERS requires ENVELOPE UPSERT or no ENVELOPE");
            }

            for (pos, item) in include_metadata.iter().cloned().enumerate() {
                match item.ty {
                    SourceIncludeMetadataType::Timestamp => {
                        connection.include_timestamp = unwrap_name(item.alias, "timestamp", pos);
                    }
                    SourceIncludeMetadataType::Partition => {
                        connection.include_partition = unwrap_name(item.alias, "partition", pos);
                    }
                    SourceIncludeMetadataType::Topic => {
                        // TODO(bwm): This requires deeper thought, the current structure of the
                        // code requires us to clone the topic name around all over the place
                        // whether or not anyone ever uses it. Considering we expect the
                        // overwhelming majority of people will *not* want topics in dataflows that
                        // is an unnacceptable cost.
                        bail_unsupported!("INCLUDE TOPIC");
                    }
                    SourceIncludeMetadataType::Offset => {
                        connection.include_offset = unwrap_name(item.alias, "offset", pos);
                    }
                    SourceIncludeMetadataType::Headers => {
                        connection.include_headers = unwrap_name(item.alias, "headers", pos);
                    }
                    SourceIncludeMetadataType::Key => {} // handled below
                }
            }

            let connection = GenericSourceConnection::from(connection);

            (connection, encoding, None)
        }
        CreateSourceConnection::Postgres {
            connection,
            options,
        } => {
            let connection_item = scx.get_item_by_resolved_name(connection)?;
            let connection = match connection_item.connection()? {
                Connection::Postgres(connection) => connection.clone(),
                _ => sql_bail!("{} is not a postgres connection", connection_item.name()),
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
                let cast_qcx = QueryContext::root(&cast_scx, QueryLifetime::Static);
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

            let connection = GenericSourceConnection::from(PostgresSourceConnection {
                connection,
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
        disk,
        seen: _,
    } = CreateSourceOptionExtracted::try_from(with_options.clone())?;

    let (key_desc, value_desc) = encoding.desc()?;

    let mut key_envelope = get_key_envelope(include_metadata, &envelope, &encoding)?;

    let disk_default = scx.catalog.system_vars().upsert_source_disk_default();

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
                    disk: disk.unwrap_or(disk_default),
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
                disk: disk.unwrap_or(disk_default),
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

    if disk.is_some() {
        match &envelope {
            UnplannedSourceEnvelope::Upsert { .. } => {
                scx.require_feature_flag(&vars::ENABLE_UPSERT_SOURCE_DISK)?
            }
            _ => {
                bail_unsupported!("ON DISK used with non-UPSERT/DEBEZIUM ENVELOPE");
            }
        }
    }

    let metadata_columns = external_connection.metadata_columns();
    let metadata_column_types = external_connection.metadata_column_types();
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
        Some(timestamp_interval) => timestamp_interval.duration()?,
        None => scx.catalog.config().timestamp_interval,
    };

    let source_desc = SourceDesc {
        connection: external_connection,
        encoding,
        envelope: envelope.clone(),
        metadata_columns: metadata_column_types,
        timestamp_interval,
    };

    // MIGRATION: v0.44 This can be converted to an unwrap in v0.46
    let progress_subsource = progress_subsource
        .as_ref()
        .map(|name| match name {
            DeferredItemName::Named(name) => match name {
                ResolvedItemName::Item { id, .. } => Ok(*id),
                ResolvedItemName::Cte { .. } | ResolvedItemName::Error => {
                    sql_bail!("[internal error] invalid target id")
                }
            },
            DeferredItemName::Deferred(_) => {
                sql_bail!("[internal error] progress subsource must be named during purification")
            }
        })
        .transpose()?;

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
            SourceEnvelope::CdcV2 => Timeline::External(name.to_string()),
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
    (MaxCardinality, i64)
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
            if let Some(max_cardinality) = max_cardinality {
                if max_cardinality < 0 {
                    sql_bail!("unsupported max cardinality {max_cardinality}");
                }
            }
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
) -> Result<SourceDataEncoding, PlanError> {
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
            let cluster = scx.catalog.get_cluster(in_cluster.id);
            if cluster.replica_ids().len() > 1 {
                sql_bail!("cannot create {ty} in cluster with more than one replica")
            }
            if !is_storage_cluster(scx, cluster) {
                sql_bail!("cannot create {ty} in cluster containing indexes or materialized views");
            }

            // We also don't allow more objects to be added to a cluster that is already
            // linked to another object.
            ensure_cluster_is_not_linked(scx, cluster.id())?;

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
    pub csr_connection: Option<mz_storage_client::types::connections::CsrConnection>,
    pub confluent_wire_format: bool,
}

fn get_encoding_inner(
    scx: &StatementContext,
    format: &Format<Aug>,
) -> Result<SourceDataEncodingInner, PlanError> {
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
        Format::Regex(regex) => {
            let regex = Regex::new(regex).map_err(|e| sql_err!("parsing regex: {e}"))?;
            DataEncodingInner::Regex(RegexEncoding {
                regex: mz_repr::adt::regex::Regex(regex),
            })
        }
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
        Format::Json => {
            scx.require_feature_flag(&crate::session::vars::ENABLE_FORMAT_JSON)?;
            DataEncodingInner::Json
        }
        Format::Text => DataEncodingInner::Text,
    }))
}

/// Extract the key envelope, if it is requested
fn get_key_envelope(
    included_items: &[SourceIncludeMetadata],
    envelope: &Envelope,
    encoding: &SourceDataEncoding,
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
fn get_unnamed_key_envelope(key: &DataEncoding) -> Result<KeyEnvelope, PlanError> {
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
    } = query::plan_root_query(scx, query.clone(), QueryLifetime::Static)?;

    expr.bind_parameters(params)?;
    //TODO: materialize#724 - persist finishing information with the view?
    expr.finish(finishing);
    let relation_expr = expr.optimize_and_lower(&scx.into())?;

    let name = if temporary {
        scx.allocate_temporary_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    } else {
        scx.allocate_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    };

    plan_utils::maybe_rename_columns(format!("view {}", name), &mut desc, columns)?;
    let names: Vec<ColumnName> = desc.iter_names().cloned().collect();

    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    let view = View {
        create_sql,
        expr: relation_expr,
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
    } = query::plan_root_query(scx, stmt.query, QueryLifetime::Static)?;

    expr.bind_parameters(params)?;
    expr.finish(finishing);
    let expr = expr.optimize_and_lower(&scx.into())?;

    plan_utils::maybe_rename_columns(
        format!("materialized view {}", name),
        &mut desc,
        &stmt.columns,
    )?;
    let column_names: Vec<ColumnName> = desc.iter_names().cloned().collect();

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
                if key.not_enforced && envelope == SinkEnvelope::Upsert {
                    // TODO: We should report a warning notice back to the user via the pgwire
                    // protocol. See https://github.com/MaterializeInc/materialize/issues/9333.
                    warn!(
                        "Verification of upsert key disabled for sink '{}' via 'NOT ENFORCED'. This is potentially dangerous and can lead to crashing materialize when the specified key is not in fact a unique key of the sinked view.",
                        name
                    );
                } else if !is_valid_key && envelope == SinkEnvelope::Upsert {
                    return Err(invalid_upsert_key_err(&desc, &key_columns));
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
            connection_builder,
            envelope,
        },
        with_snapshot,
        if_not_exists,
        cluster_config,
    }))
}

fn invalid_upsert_key_err(desc: &RelationDesc, requested_user_key: &[ColumnName]) -> PlanError {
    let requested_user_key = requested_user_key
        .iter()
        .map(|column| column.as_str())
        .join(", ");
    let requested_user_key = format!("({})", requested_user_key);
    let valid_keys = if desc.typ().keys.is_empty() {
        "there are no valid keys".to_owned()
    } else {
        let valid_keys = desc
            .typ()
            .keys
            .iter()
            .map(|key_columns| {
                let columns_string = key_columns
                    .iter()
                    .map(|col| desc.get_name(*col).as_str())
                    .join(", ");
                format!("({})", columns_string)
            })
            .join(", ");
        format!("valid keys are: {}", valid_keys)
    };
    sql_err!("Invalid upsert key: {}, {}", requested_user_key, valid_keys,)
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

generate_extracted_config!(
    CsrConfigOption,
    (AvroKeyFullname, String),
    (AvroValueFullname, String)
);

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
) -> Result<StorageSinkConnectionBuilder, PlanError> {
    let item = scx.get_item_by_resolved_name(&connection)?;
    // Get Kafka connection
    let mut connection = match item.connection()? {
        Connection::Kafka(connection) => connection.clone(),
        _ => sql_bail!("{} is not a kafka connection", item.name()),
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

            let schema_generator = AvroSchemaGenerator::new(
                avro_key_fullname.as_deref(),
                avro_value_fullname.as_deref(),
                key_desc_and_indices
                    .as_ref()
                    .map(|(desc, _indices)| desc.clone()),
                value_desc.clone(),
                matches!(envelope, SinkEnvelope::Debezium),
            )?;
            let value_schema = schema_generator.value_writer_schema().to_string();
            let key_schema = schema_generator
                .key_writer_schema()
                .map(|key_schema| key_schema.to_string());

            KafkaSinkFormat::Avro {
                key_schema,
                value_schema,
                csr_connection,
            }
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

    Ok(StorageSinkConnectionBuilder::Kafka(
        KafkaSinkConnectionBuilder {
            connection_id,
            connection,
            format,
            topic_name,
            consistency_config,
            partition_count,
            replication_factor,
            fuel: 10000,
            relation_key_indices,
            key_desc_and_indices,
            value_desc,
            retention,
        },
    ))
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
        data_type: &ResolvedDataType,
        as_type: &str,
        key: &str,
    ) -> Result<GlobalId, PlanError> {
        let item = match data_type {
            ResolvedDataType::Named {
                id,
                full_name,
                modifiers,
                ..
            } => {
                if !modifiers.is_empty() {
                    sql_bail!(
                        "CREATE TYPE ... AS {}option {} cannot accept type modifier on \
                                {}, you must use the default type",
                        as_type,
                        key,
                        full_name
                    );
                }
                scx.catalog.get_item(id)
            }
            _ => sql_bail!(
                "CREATE TYPE ... AS {}option {} can only use named data types, but \
                        found unnamed data type {}. Use CREATE TYPE to create a named type first",
                as_type,
                key,
                data_type.to_ast_string(),
            ),
        };

        match scx.catalog.get_item(&item.id()).type_details() {
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
            _ => Ok(item.id()),
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
            CatalogType::List {
                element_reference: validate_data_type(scx, &element_type, "LIST ", "ELEMENT TYPE")?,
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
            CatalogType::Map {
                key_reference: validate_data_type(scx, &key_type, "MAP ", "KEY TYPE")?,
                value_reference: validate_data_type(scx, &value_type, "MAP ", "VALUE TYPE")?,
            }
        }
        CreateTypeAs::Record { ref column_defs } => {
            let mut fields = vec![];
            for column_def in column_defs {
                let data_type = &column_def.data_type;
                let key = ident(column_def.name.clone());
                let id = validate_data_type(scx, data_type, "", &key)?;
                fields.push((ColumnName::from(key.clone()), id));
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
pub struct PlannedRoleAttributes {
    pub inherit: Option<bool>,
    pub create_role: Option<bool>,
    pub create_db: Option<bool>,
    pub create_cluster: Option<bool>,
}

fn plan_role_attributes(options: Vec<RoleAttribute>) -> Result<PlannedRoleAttributes, PlanError> {
    let mut planned_attributes = PlannedRoleAttributes {
        inherit: None,
        create_role: None,
        create_db: None,
        create_cluster: None,
    };

    for option in options {
        match option {
            RoleAttribute::Login | RoleAttribute::NoLogin => {
                bail_never_supported!("LOGIN attribute", "sql/create-role/#details")
            }
            RoleAttribute::SuperUser | RoleAttribute::NoSuperUser => {
                bail_never_supported!("SUPERUSER attribute", "sql/create-role/#details")
            }
            RoleAttribute::Inherit | RoleAttribute::NoInherit
                if planned_attributes.inherit.is_some() =>
            {
                sql_bail!("conflicting or redundant options");
            }
            RoleAttribute::CreateCluster | RoleAttribute::NoCreateCluster
                if planned_attributes.create_cluster.is_some() =>
            {
                sql_bail!("conflicting or redundant options");
            }
            RoleAttribute::CreateDB | RoleAttribute::NoCreateDB
                if planned_attributes.create_db.is_some() =>
            {
                sql_bail!("conflicting or redundant options");
            }
            RoleAttribute::CreateRole | RoleAttribute::NoCreateRole
                if planned_attributes.create_role.is_some() =>
            {
                sql_bail!("conflicting or redundant options");
            }

            RoleAttribute::Inherit => planned_attributes.inherit = Some(true),
            RoleAttribute::NoInherit => planned_attributes.inherit = Some(false),
            RoleAttribute::CreateCluster => planned_attributes.create_cluster = Some(true),
            RoleAttribute::NoCreateCluster => planned_attributes.create_cluster = Some(false),
            RoleAttribute::CreateDB => planned_attributes.create_db = Some(true),
            RoleAttribute::NoCreateDB => planned_attributes.create_db = Some(false),
            RoleAttribute::CreateRole => planned_attributes.create_role = Some(true),
            RoleAttribute::NoCreateRole => planned_attributes.create_role = Some(false),
        }
    }
    if planned_attributes.inherit == Some(false) {
        bail_unsupported!("non inherit roles");
    }

    Ok(planned_attributes)
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

generate_extracted_config!(ClusterOption, (Replicas, Vec<ReplicaDefinition<Aug>>));

pub fn plan_create_cluster(
    scx: &StatementContext,
    CreateClusterStatement { name, options }: CreateClusterStatement<Aug>,
) -> Result<Plan, PlanError> {
    let ClusterOptionExtracted { replicas, .. }: ClusterOptionExtracted = options.try_into()?;

    let replica_defs = match replicas {
        Some(replica_defs) => replica_defs,
        None => sql_bail!("REPLICAS option is required"),
    };

    let mut replicas = vec![];
    for ReplicaDefinition { name, options } in replica_defs {
        replicas.push((normalize::ident(name), plan_replica_config(scx, options)?));
    }

    Ok(Plan::CreateCluster(CreateClusterPlan {
        name: normalize::ident(name),
        replicas,
    }))
}

const DEFAULT_REPLICA_INTROSPECTION_INTERVAL: Interval = Interval {
    micros: cast::u32_to_i64(DEFAULT_REPLICA_LOGGING_INTERVAL_MICROS),
    months: 0,
    days: 0,
};

generate_extracted_config!(
    ReplicaOption,
    (Size, String),
    (AvailabilityZone, String),
    (StoragectlAddresses, Vec<String>),
    (StorageAddresses, Vec<String>),
    (ComputectlAddresses, Vec<String>),
    (ComputeAddresses, Vec<String>),
    (Workers, u16),
    (IntrospectionInterval, OptionalInterval),
    (IntrospectionDebugging, bool, Default(false)),
    (IdleArrangementMergeEffort, u32)
);

fn plan_replica_config(
    scx: &StatementContext,
    options: Vec<ReplicaOption<Aug>>,
) -> Result<ReplicaConfig, PlanError> {
    let ReplicaOptionExtracted {
        size,
        availability_zone,
        storagectl_addresses,
        storage_addresses,
        computectl_addresses,
        compute_addresses,
        workers,
        introspection_interval,
        introspection_debugging,
        idle_arrangement_merge_effort,
        ..
    }: ReplicaOptionExtracted = options.try_into()?;

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

    match (
        size,
        availability_zone,
        storagectl_addresses,
        storage_addresses,
        computectl_addresses,
        compute_addresses,
        workers,
    ) {
        // Common cases we expect end users to hit.
        (None, _, None, None, None, None, None) => {
            // We don't mention the unmanaged options in the error message
            // because they are only available in unsafe mode.
            sql_bail!("SIZE option must be specified");
        }
        (Some(size), availability_zone, None, None, None, None, None) => {
            Ok(ReplicaConfig::Managed {
                size,
                availability_zone,
                compute,
            })
        }

        (
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
    let cluster = scx.catalog.resolve_cluster(Some(&of_cluster.to_string()))?;
    if is_storage_cluster(scx, cluster)
        && cluster.bound_objects().len() > 0
        && cluster.replica_ids().len() > 0
    {
        sql_bail!("cannot create more than one replica of a cluster containing sources or sinks");
    }
    ensure_cluster_is_not_linked(scx, cluster.id())?;
    Ok(Plan::CreateClusterReplica(CreateClusterReplicaPlan {
        name: normalize::ident(name),
        cluster_id: cluster.id(),
        config: plan_replica_config(scx, options)?,
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
    ) -> Result<Vec<mz_storage_client::types::connections::KafkaBroker>, PlanError> {
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
                                      entry.name().to_string().quoted())
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
                        Connection::Ssh(connection) => Tunnel::Ssh(SshTunnel {
                            connection_id: *id,
                            connection: connection.clone(),
                        }),
                        _ => {
                            sql_bail!("{} is not an SSH connection", ssh_tunnel.name().item)
                        }
                    }
                }
            };

            out.push(mz_storage_client::types::connections::KafkaBroker {
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
            let sasl_mechanism = k.sasl_mechanisms.clone().unwrap();
            if sasl_mechanism
                .chars()
                .any(|c| c.is_ascii_alphabetic() && !c.is_uppercase())
            {
                sql_bail!(
                    "invalid SASL MECHANISM {}: must be uppercase",
                    sasl_mechanism.quoted()
                );
            }
            Some(SaslConfig {
                mechanisms: sasl_mechanism,
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
    ) -> Result<mz_storage_client::types::connections::KafkaConnection, PlanError> {
        Ok(KafkaConnection {
            brokers: self.get_brokers(scx)?,
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
    ) -> Result<mz_storage_client::types::connections::CsrConnection, PlanError> {
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

        Ok(mz_storage_client::types::connections::CsrConnection {
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
    ) -> Result<mz_storage_client::types::connections::PostgresConnection, PlanError> {
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

        Ok(mz_storage_client::types::connections::PostgresConnection {
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

impl TryFrom<SshConnectionOptionExtracted>
    for mz_storage_client::types::connections::SshConnection
{
    type Error = PlanError;

    fn try_from(options: SshConnectionOptionExtracted) -> Result<Self, Self::Error> {
        Ok(mz_storage_client::types::connections::SshConnection {
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

pub fn plan_create_connection(
    scx: &StatementContext,
    stmt: CreateConnectionStatement<Aug>,
) -> Result<Plan, PlanError> {
    let create_sql = normalize::create_statement(scx, Statement::CreateConnection(stmt.clone()))?;
    let CreateConnectionStatement {
        name,
        connection,
        if_not_exists,
    } = stmt;
    let connection = match connection {
        CreateConnection::Kafka { with_options } => {
            let c = KafkaConnectionOptionExtracted::try_from(with_options)?;
            Connection::Kafka(c.to_connection(scx)?)
        }
        CreateConnection::Csr { with_options } => {
            let c = CsrConnectionOptionExtracted::try_from(with_options)?;
            Connection::Csr(c.to_connection(scx)?)
        }
        CreateConnection::Postgres { with_options } => {
            let c = PostgresConnectionOptionExtracted::try_from(with_options)?;
            Connection::Postgres(c.to_connection(scx)?)
        }
        CreateConnection::Aws { with_options } => {
            let c = AwsConnectionOptionExtracted::try_from(with_options)?;
            let connection = AwsConfig::try_from(c)?;
            Connection::Aws(connection)
        }
        CreateConnection::AwsPrivatelink { with_options } => {
            let c = AwsPrivatelinkConnectionOptionExtracted::try_from(with_options)?;
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
        CreateConnection::Ssh { with_options } => {
            let c = SshConnectionOptionExtracted::try_from(with_options)?;
            let connection = mz_storage_client::types::connections::SshConnection::try_from(c)?;
            Connection::Ssh(connection)
        }
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

    let plan = CreateConnectionPlan {
        name,
        if_not_exists,
        connection: crate::plan::Connection {
            create_sql,
            connection,
        },
    };
    Ok(Plan::CreateConnection(plan))
}

fn plan_drop_database(
    scx: &StatementContext,
    if_exists: bool,
    name: UnresolvedDatabaseName,
    cascade: bool,
) -> Result<Option<DatabaseId>, PlanError> {
    Ok(match resolve_database(scx, &name, if_exists)? {
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
    scx: &StatementContext,
    DropObjectsStatement {
        object_type,
        if_exists,
        names,
        cascade,
    }: DropObjectsStatement,
) -> Result<Plan, PlanError> {
    assert_ne!(object_type, ObjectType::Func, "rejected in parser");

    let mut referenced_ids = Vec::new();
    for name in names {
        let id = match name {
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
                plan_drop_item(scx, object_type, if_exists, name, cascade)?.map(ObjectId::Item)
            }
        };
        if let Some(id) = id {
            referenced_ids.push(id);
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
    name: UnresolvedSchemaName,
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
    name: Ident,
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
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the
            // role does not exist.
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}

fn plan_drop_cluster(
    scx: &StatementContext,
    if_exists: bool,
    name: Ident,
    cascade: bool,
) -> Result<Option<ClusterId>, PlanError> {
    Ok(match resolve_cluster(scx, &name, if_exists)? {
        Some(cluster) => {
            if !cascade && !cluster.bound_objects().is_empty() {
                sql_bail!("cannot drop cluster with active objects");
            }
            ensure_cluster_is_not_linked(scx, cluster.id())?;
            Some(cluster.id())
        }
        None => None,
    })
}

fn is_storage_cluster(scx: &StatementContext, cluster: &dyn CatalogCluster) -> bool {
    cluster.bound_objects().iter().all(|id| {
        matches!(
            scx.catalog.get_item(id).item_type(),
            CatalogItemType::Source | CatalogItemType::Sink
        )
    })
}

fn plan_drop_cluster_replica(
    scx: &StatementContext,
    if_exists: bool,
    name: QualifiedReplica,
) -> Result<Option<(ClusterId, ReplicaId)>, PlanError> {
    let id = resolve_cluster_replica(scx, &name, if_exists)?;
    if let Some((cluster_id, _)) = &id {
        ensure_cluster_is_not_linked(scx, *cluster_id)?;
    }
    Ok(id)
}

fn plan_drop_item(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: UnresolvedItemName,
    cascade: bool,
) -> Result<Option<GlobalId>, PlanError> {
    Ok(match resolve_item(scx, name, if_exists)? {
        Some(catalog_item) => {
            if catalog_item.id().is_system() {
                sql_bail!(
                    "cannot drop item {} because it is required by the database system",
                    scx.catalog.resolve_full_name(catalog_item.name()),
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

            // We currently prohibit dropping subsources entirely and instead rely on
            // dropping their primary sources.
            if item_type == CatalogItemType::Source {
                if let Some(source_id) = catalog_item
                    .used_by()
                    .iter()
                    .find(|id| scx.catalog.get_item(id).item_type() == CatalogItemType::Source)
                {
                    return Err(PlanError::DropSubsource {
                        subsource: scx
                            .catalog
                            .resolve_full_name(catalog_item.name())
                            .to_string(),
                        source: scx
                            .catalog
                            .resolve_full_name(scx.catalog.get_item(source_id).name())
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
                        // The catalog_entry we're trying to drop will appear in the used_by list of its
                        // subsources so we need to exclude it from cascade checking since it will be
                        // dropped
                        if id == &entry_id {
                            continue;
                        }

                        let dep = scx.catalog.get_item(id);
                        if dependency_prevents_drop(object_type, dep) {
                            // TODO: Add a hint to add cascade.
                            sql_bail!(
                                "cannot drop {}: still depended upon by catalog item '{}'",
                                scx.catalog.resolve_full_name(catalog_item.name()),
                                scx.catalog.resolve_full_name(dep.name())
                            );
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
    let mut revokes = Vec::new();

    fn update_revokes(
        object_id: ObjectId,
        privileges: &PrivilegeMap,
        role_ids: &BTreeSet<RoleId>,
        revokes: &mut Vec<(ObjectId, MzAclItem)>,
    ) {
        revokes.extend(iter::zip(
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
            if !cascade && !cluster.bound_objects().is_empty() {
                sql_bail!(
                    "cannot drop cluster {} without CASCADE while it contains active objects",
                    cluster.name().quoted()
                );
            }
            drop_ids.push(cluster.id().into());
        }
        update_revokes(
            cluster.id().into(),
            cluster.privileges(),
            &role_ids,
            &mut revokes,
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
                            .map(|item| scx.catalog.resolve_full_name(item.name()))
                            .map(|name| name.to_string().quoted().to_string())
                            .collect();
                        sql_bail!(
                            "cannot drop {} without CASCADE: still depended upon by non-owned catalog items {}",
                            scx.catalog.resolve_full_name(item.name()).to_string().quoted(),
                            names.join(", ")
                        );
                    }
                }
            }
            drop_ids.push(item.id().into());
        }
        update_revokes(item.id().into(), item.privileges(), &role_ids, &mut revokes);
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
            update_revokes(
                (*schema.database(), *schema.id()).into(),
                schema.privileges(),
                &role_ids,
                &mut revokes,
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
        update_revokes(
            database.id().into(),
            database.privileges(),
            &role_ids,
            &mut revokes,
        );
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
        revokes,
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
    scx: &StatementContext,
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
            // TODO(benesch): generate a notice indicating this index does not
            // exist.
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

pub fn describe_alter_owner(
    _: &StatementContext,
    _: AlterOwnerStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_owner(
    scx: &StatementContext,
    AlterOwnerStatement {
        object_type,
        if_exists,
        name,
        new_owner,
    }: AlterOwnerStatement<Aug>,
) -> Result<Plan, PlanError> {
    match (object_type, name) {
        (ObjectType::Cluster, UnresolvedObjectName::Cluster(name)) => {
            plan_alter_cluster_owner(scx, if_exists, name, new_owner.id)
        }
        (ObjectType::ClusterReplica, UnresolvedObjectName::ClusterReplica(name)) => {
            plan_alter_cluster_replica_owner(scx, if_exists, name, new_owner.id)
        }
        (ObjectType::Database, UnresolvedObjectName::Database(name)) => {
            plan_alter_database_owner(scx, if_exists, name, new_owner.id)
        }
        (ObjectType::Schema, UnresolvedObjectName::Schema(name)) => {
            plan_alter_schema_owner(scx, if_exists, name, new_owner.id)
        }
        (ObjectType::Role, UnresolvedObjectName::Role(_)) => unreachable!("rejected by the parser"),
        (
            object_type @ ObjectType::Cluster
            | object_type @ ObjectType::ClusterReplica
            | object_type @ ObjectType::Database
            | object_type @ ObjectType::Schema
            | object_type @ ObjectType::Role,
            name,
        )
        | (
            object_type,
            name @ UnresolvedObjectName::Cluster(_)
            | name @ UnresolvedObjectName::ClusterReplica(_)
            | name @ UnresolvedObjectName::Database(_)
            | name @ UnresolvedObjectName::Schema(_)
            | name @ UnresolvedObjectName::Role(_),
        ) => {
            unreachable!("parser set the wrong object type '{object_type:?}' for name {name:?}")
        }
        (object_type, UnresolvedObjectName::Item(name)) => {
            plan_alter_item_owner(scx, object_type, if_exists, name, new_owner.id)
        }
    }
}

fn plan_alter_cluster_owner(
    scx: &StatementContext,
    if_exists: bool,
    name: Ident,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_cluster(scx, &name, if_exists)? {
        Some(cluster) => Ok(Plan::AlterOwner(AlterOwnerPlan {
            id: ObjectId::Cluster(cluster.id()),
            object_type: ObjectType::Cluster,
            new_owner,
        })),
        None => Ok(Plan::AlterNoop(AlterNoopPlan {
            object_type: ObjectType::Cluster,
        })),
    }
}

fn plan_alter_cluster_replica_owner(
    scx: &StatementContext,
    if_exists: bool,
    name: QualifiedReplica,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_cluster_replica(scx, &name, if_exists)? {
        Some((cluster_id, replica_id)) => Ok(Plan::AlterOwner(AlterOwnerPlan {
            id: ObjectId::ClusterReplica((cluster_id, replica_id)),
            object_type: ObjectType::ClusterReplica,
            new_owner,
        })),
        None => Ok(Plan::AlterNoop(AlterNoopPlan {
            object_type: ObjectType::ClusterReplica,
        })),
    }
}

fn plan_alter_database_owner(
    scx: &StatementContext,
    if_exists: bool,
    name: UnresolvedDatabaseName,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_database(scx, &name, if_exists)? {
        Some(database) => Ok(Plan::AlterOwner(AlterOwnerPlan {
            id: ObjectId::Database(database.id()),
            object_type: ObjectType::Database,
            new_owner,
        })),
        None => Ok(Plan::AlterNoop(AlterNoopPlan {
            object_type: ObjectType::Database,
        })),
    }
}

fn plan_alter_schema_owner(
    scx: &StatementContext,
    if_exists: bool,
    name: UnresolvedSchemaName,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_schema(scx, name.clone(), if_exists)? {
        Some((database_spec, schema_spec)) => {
            if let ResolvedDatabaseSpecifier::Ambient = database_spec {
                sql_bail!(
                    "cannot alter schema {name} because it is required by the database system",
                );
            }
            if let SchemaSpecifier::Temporary = schema_spec {
                sql_bail!("cannot alter schema {name} because it is a temporary schema",)
            }
            Ok(Plan::AlterOwner(AlterOwnerPlan {
                id: ObjectId::Schema((database_spec, schema_spec)),
                object_type: ObjectType::Schema,
                new_owner,
            }))
        }
        None => Ok(Plan::AlterNoop(AlterNoopPlan {
            object_type: ObjectType::Database,
        })),
    }
}

fn plan_alter_item_owner(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: UnresolvedItemName,
    new_owner: RoleId,
) -> Result<Plan, PlanError> {
    match resolve_item(scx, name, if_exists)? {
        Some(item) => {
            if item.id().is_system() {
                sql_bail!(
                    "cannot alter item {} because it is required by the database system",
                    scx.catalog.resolve_full_name(item.name()),
                );
            }
            let item_type = item.item_type();

            // Return a more helpful error on `ALTER VIEW <materialized-view>`.
            if object_type == ObjectType::View && item_type == CatalogItemType::MaterializedView {
                let name = scx.catalog.resolve_full_name(item.name()).to_string();
                return Err(PlanError::AlterViewOnMaterializedView(name));
            } else if object_type != item_type {
                sql_bail!(
                    "{} is a {} not a {}",
                    scx.catalog
                        .resolve_full_name(item.name())
                        .to_string()
                        .quoted(),
                    item.item_type(),
                    format!("{object_type}").to_lowercase(),
                );
            }
            Ok(Plan::AlterOwner(AlterOwnerPlan {
                id: ObjectId::Item(item.id()),
                object_type,
                new_owner,
            }))
        }
        None => Ok(Plan::AlterNoop(AlterNoopPlan { object_type })),
    }
}

pub fn describe_alter_object_rename(
    _: &StatementContext,
    _: AlterObjectRenameStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_object_rename(
    scx: &StatementContext,
    AlterObjectRenameStatement {
        name,
        object_type,
        to_item_name,
        if_exists,
    }: AlterObjectRenameStatement,
) -> Result<Plan, PlanError> {
    match resolve_item(scx, name, if_exists)? {
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
        None => Ok(Plan::AlterNoop(AlterNoopPlan { object_type })),
    }
}

pub fn describe_alter_secret_options(
    _: &StatementContext,
    _: AlterSecretStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_secret(
    scx: &StatementContext,
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
            // TODO(benesch): generate a notice indicating this secret does not
            // exist.
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
    scx: &StatementContext,
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

pub fn plan_alter_source(
    scx: &StatementContext,
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
    match action {
        AlterSourceAction::SetOptions(options) => {
            let CreateSourceOptionExtracted {
                seen: _,
                size: size_opt,
                timeline: timeline_opt,
                timestamp_interval: timestamp_interval_opt,
                ignore_keys: ignore_keys_opt,
                disk: disk_opt,
            } = CreateSourceOptionExtracted::try_from(options)?;

            if let Some(value) = size_opt {
                size = AlterOptionParameter::Set(value);
            }
            if let Some(_) = timeline_opt {
                sql_bail!("Cannot modify the TIMELINE of a SOURCE.");
            }
            if let Some(_) = timestamp_interval_opt {
                sql_bail!("Cannot modify the TIMESTAMP INTERVAL of a SOURCE.");
            }
            if let Some(_) = ignore_keys_opt {
                sql_bail!("Cannot modify the IGNORE KEYS property of a SOURCE.");
            }
            if let Some(_) = disk_opt {
                sql_bail!("Cannot modify the DISK property of a SOURCE.");
            }
        }
        AlterSourceAction::ResetOptions(reset) => {
            for name in reset {
                match name {
                    CreateSourceOptionName::Size => {
                        size = AlterOptionParameter::Reset;
                    }
                    CreateSourceOptionName::Timeline => {
                        sql_bail!("Cannot modify the TIMELINE of a SOURCE.");
                    }
                    CreateSourceOptionName::TimestampInterval => {
                        sql_bail!("Cannot modify the TIMESTAMP INTERVAL of a SOURCE.");
                    }
                    CreateSourceOptionName::IgnoreKeys => {
                        sql_bail!("Cannot modify the IGNORE KEYS property of a SOURCE.");
                    }
                    CreateSourceOptionName::Disk => {
                        sql_bail!("Cannot modify the DISK property of a SOURCE.");
                    }
                }
            }
        }
    };

    Ok(Plan::AlterSource(AlterSourcePlan { id, size }))
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
    scx: &StatementContext,
    stmt: AlterConnectionStatement,
) -> Result<Plan, PlanError> {
    let AlterConnectionStatement { name, if_exists } = stmt;
    let name = normalize::unresolved_item_name(name)?;
    let entry = match scx.catalog.resolve_item(&name) {
        Ok(connection) => connection,
        Err(_) if if_exists => {
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
    _: &StatementContext,
    AlterRoleStatement { name, options }: AlterRoleStatement<Aug>,
) -> Result<Plan, PlanError> {
    let attributes = plan_role_attributes(options)?;

    Ok(Plan::AlterRole(AlterRolePlan {
        id: name.id,
        name: name.name,
        attributes,
    }))
}

pub fn describe_grant_role(
    _: &StatementContext,
    _: GrantRoleStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_grant_role(
    scx: &StatementContext,
    GrantRoleStatement {
        role_name,
        member_names,
    }: GrantRoleStatement<Aug>,
) -> Result<Plan, PlanError> {
    // In PostgreSQL, the grantor must either be a role with ADMIN OPTION on the role being granted,
    // or the bootstrap superuser. We do not have ADMIN OPTION implemented and 'mz_system' is our
    // equivalent of the bootstrap superuser. Therefore the grantor is always 'mz_system'.
    // For more details see:
    // https://github.com/postgres/postgres/blob/064eb89e83ea0f59426c92906329f1e6c423dfa4/src/backend/commands/user.c#L2180-L2238
    let grantor_id = scx
        .catalog
        .resolve_role(&SYSTEM_USER.name)
        .expect("system user must exist")
        .id();
    Ok(Plan::GrantRole(GrantRolePlan {
        role_id: role_name.id,
        member_ids: member_names
            .into_iter()
            .map(|member_name| member_name.id)
            .collect(),
        grantor_id,
    }))
}

pub fn describe_revoke_role(
    _: &StatementContext,
    _: RevokeRoleStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_revoke_role(
    scx: &StatementContext,
    RevokeRoleStatement {
        role_name,
        member_names,
    }: RevokeRoleStatement<Aug>,
) -> Result<Plan, PlanError> {
    // In PostgreSQL, the same role membership can be granted multiple times by different grantors.
    // When revoking a role membership, only the membership granted by the specified grantor is
    // revoked. The grantor must either be a role with ADMIN OPTION on the role being granted,
    // or the bootstrap superuser. We do not have ADMIN OPTION implemented and 'mz_system' is our
    // equivalent of the bootstrap superuser. Therefore the grantor is always 'mz_system'.
    // For more details see:
    // https://github.com/postgres/postgres/blob/064eb89e83ea0f59426c92906329f1e6c423dfa4/src/backend/commands/user.c#L2180-L2238
    let grantor_id = scx
        .catalog
        .resolve_role(&SYSTEM_USER.name)
        .expect("system user must exist")
        .id();
    Ok(Plan::RevokeRole(RevokeRolePlan {
        role_id: role_name.id,
        member_ids: member_names
            .into_iter()
            .map(|member_name| member_name.id)
            .collect(),
        grantor_id,
    }))
}

pub fn describe_grant_privileges(
    _: &StatementContext,
    _: GrantPrivilegesStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_grant_privileges(
    scx: &StatementContext,
    GrantPrivilegesStatement {
        privileges,
        object_type,
        names,
        roles,
    }: GrantPrivilegesStatement<Aug>,
) -> Result<Plan, PlanError> {
    let plan = plan_update_privilege(scx, privileges, object_type, names, roles)?;
    Ok(Plan::GrantPrivileges(plan.into()))
}

pub fn describe_revoke_privileges(
    _: &StatementContext,
    _: RevokePrivilegesStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_revoke_privileges(
    scx: &StatementContext,
    RevokePrivilegesStatement {
        privileges,
        object_type,
        names,
        roles,
    }: RevokePrivilegesStatement<Aug>,
) -> Result<Plan, PlanError> {
    let plan = plan_update_privilege(scx, privileges, object_type, names, roles)?;
    Ok(Plan::RevokePrivileges(plan.into()))
}

struct UpdatePrivilegesPlan {
    update_privileges: Vec<UpdatePrivilege>,
    grantees: Vec<RoleId>,
}

impl From<UpdatePrivilegesPlan> for GrantPrivilegesPlan {
    fn from(
        UpdatePrivilegesPlan {
            update_privileges,
            grantees,
        }: UpdatePrivilegesPlan,
    ) -> GrantPrivilegesPlan {
        GrantPrivilegesPlan {
            update_privileges,
            grantees,
        }
    }
}

impl From<UpdatePrivilegesPlan> for RevokePrivilegesPlan {
    fn from(
        UpdatePrivilegesPlan {
            update_privileges,
            grantees,
        }: UpdatePrivilegesPlan,
    ) -> RevokePrivilegesPlan {
        RevokePrivilegesPlan {
            update_privileges,
            revokees: grantees,
        }
    }
}

fn plan_update_privilege(
    scx: &StatementContext,
    privileges: PrivilegeSpecification,
    object_type: ObjectType,
    names: Vec<ResolvedObjectName>,
    roles: Vec<ResolvedRoleName>,
) -> Result<UpdatePrivilegesPlan, PlanError> {
    let mut update_privileges = Vec::with_capacity(names.len());

    for name in names {
        let object_id = name
            .try_into()
            .expect("name resolution should handle invalid objects");
        let actual_object_type = scx.get_object_type(&object_id);
        let mut reference_object_type = actual_object_type.clone();

        let acl_mode = match &privileges {
            PrivilegeSpecification::All => scx.catalog.all_object_privileges(actual_object_type),
            PrivilegeSpecification::Privileges(privileges) => privileges
                .into_iter()
                .map(|privilege| privilege_to_acl_mode(privilege.clone()))
                // PostgreSQL doesn't care about duplicate privileges, so we don't either.
                .fold(AclMode::empty(), |accum, acl_mode| accum.union(acl_mode)),
        };

        if let ObjectId::Item(id) = &object_id {
            let item = scx.get_item(id);
            let item_type: ObjectType = item.item_type().into();
            if (item_type == ObjectType::View
                || item_type == ObjectType::MaterializedView
                || item_type == ObjectType::Source)
                && object_type == ObjectType::Table
            {
                // This is an expected mis-match to match PostgreSQL semantics.
                reference_object_type = ObjectType::Table;
            } else if item_type != object_type {
                let object_name = scx.catalog.resolve_full_name(item.name()).to_string();
                return Err(PlanError::InvalidObjectType {
                    expected_type: object_type,
                    actual_type: actual_object_type,
                    object_name,
                });
            }
        }

        let all_object_privileges = scx.catalog.all_object_privileges(reference_object_type);
        let invalid_privileges = acl_mode.difference(all_object_privileges);
        if !invalid_privileges.is_empty() {
            let object_name = scx.catalog.get_object_name(&object_id);
            return Err(PlanError::InvalidPrivilegeTypes {
                invalid_privileges,
                object_type: actual_object_type,
                object_name,
            });
        }

        // In PostgreSQL, the grantor must always be either the object owner or some role that has been
        // been explicitly granted grant options. In Materialize, we haven't implemented grant options
        // so the grantor is always the object owner.
        //
        // For more details see:
        // https://github.com/postgres/postgres/blob/78d5952dd0e66afc4447eec07f770991fa406cce/src/backend/utils/adt/acl.c#L5154-L5246
        let grantor = scx
            .catalog
            .get_owner_id(&object_id)
            .expect("cannot revoke privileges on objects without owners");

        update_privileges.push(UpdatePrivilege {
            acl_mode,
            object_id,
            grantor,
        });
    }

    let grantees = roles.into_iter().map(|role| role.id).collect();

    Ok(UpdatePrivilegesPlan {
        update_privileges,
        grantees,
    })
}

fn privilege_to_acl_mode(privilege: Privilege) -> AclMode {
    match privilege {
        Privilege::SELECT => AclMode::SELECT,
        Privilege::INSERT => AclMode::INSERT,
        Privilege::UPDATE => AclMode::UPDATE,
        Privilege::DELETE => AclMode::DELETE,
        Privilege::USAGE => AclMode::USAGE,
        Privilege::CREATE => AclMode::CREATE,
    }
}

pub fn describe_reassign_owned(
    _: &StatementContext,
    _: ReassignOwnedStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_reassign_owned(
    scx: &StatementContext,
    ReassignOwnedStatement {
        old_roles,
        new_role,
    }: ReassignOwnedStatement<Aug>,
) -> Result<Plan, PlanError> {
    let old_roles: BTreeSet<_> = old_roles.into_iter().map(|role| role.id).collect();
    let mut reassign_ids: Vec<ObjectId> = Vec::new();

    // Replicas
    for replica in scx.catalog.get_cluster_replicas() {
        if old_roles.contains(&replica.owner_id()) {
            reassign_ids.push((replica.cluster_id(), replica.replica_id()).into());
        }
    }
    // Clusters
    for cluster in scx.catalog.get_clusters() {
        if old_roles.contains(&cluster.owner_id()) {
            reassign_ids.push(cluster.id().into());
        }
    }
    // Items
    for item in scx.catalog.get_items() {
        if old_roles.contains(&item.owner_id()) {
            reassign_ids.push(item.id().into());
        }
    }
    // Schemas
    for schema in scx.catalog.get_schemas() {
        if !schema.id().is_temporary() {
            if old_roles.contains(&schema.owner_id()) {
                reassign_ids.push((*schema.database(), *schema.id()).into())
            }
        }
    }
    // Databases
    for database in scx.catalog.get_databases() {
        if old_roles.contains(&database.owner_id()) {
            reassign_ids.push(database.id().into());
        }
    }

    let system_ids: Vec<_> = reassign_ids.iter().filter(|id| id.is_system()).collect();
    if !system_ids.is_empty() {
        let mut owners = system_ids
            .into_iter()
            .filter_map(|object_id| scx.catalog.get_owner_id(object_id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|role_id| scx.catalog.get_role(&role_id).name().quoted());
        sql_bail!(
            "cannot reassign objects owned by role {} because they are required by the database system",
            owners.join(", "),
        );
    }

    Ok(Plan::ReassignOwned(ReassignOwnedPlan {
        old_roles: old_roles.into_iter().collect(),
        new_role: new_role.id,
        reassign_ids,
    }))
}

fn resolve_cluster<'a>(
    scx: &'a StatementContext,
    name: &'a Ident,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogCluster<'a>>, PlanError> {
    match scx.resolve_cluster(Some(name)) {
        Ok(cluster) => Ok(Some(cluster)),
        // TODO(benesch): generate a notice indicating that the
        // cluster does not exist.
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

fn resolve_cluster_replica(
    scx: &StatementContext,
    name: &QualifiedReplica,
    if_exists: bool,
) -> Result<Option<(ClusterId, ReplicaId)>, PlanError> {
    match scx.resolve_cluster(Some(&name.cluster)) {
        Ok(cluster) => match cluster.replica_ids().get(name.replica.as_str()) {
            Some(replica_id) => Ok(Some((cluster.id(), *replica_id))),
            // TODO(benesch): generate a notice indicating that the
            // replica does not exist.
            None if if_exists => Ok(None),
            None => Err(sql_err!(
                "CLUSTER {} has no CLUSTER REPLICA named {}",
                cluster.name(),
                name.replica.as_str().quoted(),
            )),
        },
        // TODO(benesch): generate a notice indicating that the
        // cluster does not exist.
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

fn resolve_database<'a>(
    scx: &'a StatementContext,
    name: &'a UnresolvedDatabaseName,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogDatabase>, PlanError> {
    match scx.resolve_database(name) {
        Ok(database) => Ok(Some(database)),
        // TODO(benesch): generate a notice indicating that the
        // cluster does not exist.
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

fn resolve_schema<'a>(
    scx: &'a StatementContext,
    name: UnresolvedSchemaName,
    if_exists: bool,
) -> Result<Option<(ResolvedDatabaseSpecifier, SchemaSpecifier)>, PlanError> {
    match scx.resolve_schema(name) {
        Ok(schema) => Ok(Some((schema.database().clone(), schema.id().clone()))),
        // TODO(benesch/jkosh44): generate a notice indicating that the
        // schema does not exist.
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

fn resolve_item<'a>(
    scx: &'a StatementContext,
    name: UnresolvedItemName,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogItem>, PlanError> {
    let name = normalize::unresolved_item_name(name)?;
    match scx.catalog.resolve_item(&name) {
        Ok(item) => Ok(Some(item)),
        // TODO(benesch/jkosh44): generate a notice indicating items do not exist.
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Returns an error if the given cluster is a linked cluster
fn ensure_cluster_is_not_linked(
    scx: &StatementContext,
    cluster_id: ClusterId,
) -> Result<(), PlanError> {
    let cluster = scx.catalog.get_cluster(cluster_id);
    if let Some(linked_id) = cluster.linked_object_id() {
        let cluster_name = scx.catalog.get_cluster(cluster_id).name().to_string();
        let linked_object_name = scx.catalog.get_item(&linked_id).name().to_string();
        Err(PlanError::ModifyLinkedCluster {
            cluster_name,
            linked_object_name,
        })
    } else {
        Ok(())
    }
}
