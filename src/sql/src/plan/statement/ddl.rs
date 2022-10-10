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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt::Write;
use std::num::NonZeroUsize;
use std::str::FromStr;

use aws_arn::ResourceName as AmazonResourceName;
use globset::GlobBuilder;
use itertools::Itertools;
use prost::Message;
use regex::Regex;
use tracing::warn;

use mz_expr::CollectionPlan;
use mz_interchange::avro::AvroSchemaGenerator;
use mz_kafka_util::KafkaAddrs;
use mz_ore::collections::CollectionExt;
use mz_ore::str::StrExt;
use mz_proto::RustType;
use mz_repr::adt::interval::Interval;
use mz_repr::strconv;
use mz_repr::{ColumnName, ColumnType, GlobalId, RelationDesc, RelationType, ScalarType};
use mz_sql_parser::ast::display::comma_separated;
use mz_sql_parser::ast::{
    AlterSourceAction, AlterSourceStatement, AlterSystemResetAllStatement,
    AlterSystemResetStatement, AlterSystemSetStatement, CreateTypeListOption,
    CreateTypeListOptionName, CreateTypeMapOption, CreateTypeMapOptionName, SetVariableValue,
    SshConnectionOption,
};
use mz_storage::source::generator::as_generator;
use mz_storage::types::connections::aws::{AwsAssumeRole, AwsConfig, AwsCredentials, SerdeUri};
use mz_storage::types::connections::{
    Connection, CsrConnectionHttpAuth, KafkaConnection, KafkaSecurity, KafkaTlsConfig, SaslConfig,
    StringOrSecret, TlsIdentity,
};
use mz_storage::types::sinks::{
    KafkaConsistencyConfig, KafkaSinkConnectionBuilder, KafkaSinkConnectionRetention,
    KafkaSinkFormat, SinkEnvelope, StorageSinkConnectionBuilder,
};
use mz_storage::types::sources::encoding::{
    included_column_desc, AvroEncoding, ColumnSpec, CsvEncoding, DataEncoding, DataEncodingInner,
    ProtobufEncoding, RegexEncoding, SourceDataEncoding, SourceDataEncodingInner,
};
use mz_storage::types::sources::{
    IncludedColumnPos, KafkaSourceConnection, KeyEnvelope, KinesisSourceConnection,
    LoadGeneratorSourceConnection, PostgresSourceConnection, PostgresSourceDetails,
    ProtoPostgresSourceDetails, S3SourceConnection, SourceConnection, SourceDesc, SourceEnvelope,
    Timeline, UnplannedSourceEnvelope, UpsertStyle,
};

use crate::ast::display::AstDisplay;
use crate::ast::{
    AlterConnectionStatement, AlterIndexAction, AlterIndexStatement, AlterObjectRenameStatement,
    AlterSecretStatement, AvroSchema, AvroSchemaOption, AvroSchemaOptionName, AwsConnectionOption,
    AwsConnectionOptionName, ClusterOption, ColumnOption, Compression,
    CreateClusterReplicaStatement, CreateClusterStatement, CreateConnection,
    CreateConnectionStatement, CreateDatabaseStatement, CreateIndexStatement,
    CreateMaterializedViewStatement, CreateRoleOption, CreateRoleStatement, CreateSchemaStatement,
    CreateSecretStatement, CreateSinkConnection, CreateSinkOption, CreateSinkOptionName,
    CreateSinkStatement, CreateSourceConnection, CreateSourceFormat, CreateSourceOption,
    CreateSourceOptionName, CreateSourceStatement, CreateSourceSubsource, CreateSourceSubsources,
    CreateSubsourceStatement, CreateTableStatement, CreateTypeAs, CreateTypeStatement,
    CreateViewStatement, CsrConfigOption, CsrConfigOptionName, CsrConnection, CsrConnectionAvro,
    CsrConnectionOption, CsrConnectionOptionName, CsrConnectionProtobuf, CsrSeedProtobuf,
    CsvColumns, DbzMode, DropClusterReplicasStatement, DropClustersStatement,
    DropDatabaseStatement, DropObjectsStatement, DropRolesStatement, DropSchemaStatement, Envelope,
    Expr, Format, Ident, IfExistsBehavior, IndexOption, IndexOptionName, KafkaConfigOptionName,
    KafkaConnectionOption, KafkaConnectionOptionName, KeyConstraint, LoadGeneratorOption,
    LoadGeneratorOptionName, ObjectType, PgConfigOption, PgConfigOptionName,
    PostgresConnectionOption, PostgresConnectionOptionName, ProtobufSchema, QualifiedReplica,
    ReplicaDefinition, ReplicaOption, ReplicaOptionName, SourceIncludeMetadata,
    SourceIncludeMetadataType, SshConnectionOptionName, Statement, TableConstraint,
    UnresolvedDatabaseName, Value, ViewDefinition,
};
use crate::catalog::{CatalogItem, CatalogItemType, CatalogType, CatalogTypeDetails};
use crate::kafka_util::{self, KafkaConfigOptionExtracted, KafkaStartOffsetType};
use crate::names::{
    Aug, FullSchemaName, QualifiedObjectName, RawDatabaseSpecifier, ResolvedClusterName,
    ResolvedDataType, ResolvedDatabaseSpecifier, ResolvedObjectName, SchemaSpecifier,
};
use crate::normalize::{self, ident};
use crate::plan::error::PlanError;
use crate::plan::expr::ColumnRef;
use crate::plan::query::{ExprContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::typeconv::{plan_cast, CastContext};
use crate::plan::with_options::{self, OptionalInterval, TryFromValue};
use crate::plan::{
    plan_utils, query, AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan, AlterItemRenamePlan,
    AlterNoopPlan, AlterSecretPlan, AlterSourceItem, AlterSourcePlan, AlterSystemResetAllPlan,
    AlterSystemResetPlan, AlterSystemSetPlan, ComputeReplicaConfig,
    ComputeReplicaIntrospectionConfig, CreateComputeInstancePlan, CreateComputeReplicaPlan,
    CreateConnectionPlan, CreateDatabasePlan, CreateIndexPlan, CreateMaterializedViewPlan,
    CreateRolePlan, CreateSchemaPlan, CreateSecretPlan, CreateSinkPlan, CreateSourcePlan,
    CreateTablePlan, CreateTypePlan, CreateViewPlan, DropComputeInstancesPlan,
    DropComputeReplicasPlan, DropDatabasePlan, DropItemsPlan, DropRolesPlan, DropSchemaPlan,
    FullObjectName, HirScalarExpr, Index, Ingestion, MaterializedView, Params, Plan, QueryContext,
    RotateKeysPlan, Secret, Sink, Source, StorageHostConfig, Table, Type, View,
};

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
                    let _ = query::plan_default_expr(scx, expr, &ty)?;
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

    for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary,
            } => {
                let mut key = vec![];
                for column in columns {
                    let column = normalize::column_name(column.clone());
                    match names.iter().position(|name| *name == column) {
                        None => sql_bail!("unknown column in constraint: {}", column),
                        Some(i) => {
                            key.push(i);
                            if *is_primary {
                                column_types[i].nullable = false;
                            }
                        }
                    }
                }
                keys.push(key);
            }
            TableConstraint::ForeignKey { .. } => {
                // Foreign key constraints are not presently enforced. We allow
                // them in unsafe mode for sqllogictest's sake.
                scx.require_unsafe_mode("CREATE TABLE with a foreign key")?
            }
            TableConstraint::Check { .. } => {
                // Check constraints are not presently enforced. We allow them
                // in unsafe mode for sqllogictest's sake.
                scx.require_unsafe_mode("CREATE TABLE with a check constraint")?
            }
        }
    }

    if !keys.is_empty() {
        // Unique constraints are not presently enforced. We allow them in
        // unsafe mode for sqllogictest's sake.
        scx.require_unsafe_mode("CREATE TABLE with a primary key or unique constraint")?;
    }

    let typ = RelationType::new(column_types).with_keys(keys);

    let temporary = *temporary;
    let name = if temporary {
        scx.allocate_temporary_qualified_name(normalize::unresolved_object_name(name.to_owned())?)?
    } else {
        scx.allocate_qualified_name(normalize::unresolved_object_name(name.to_owned())?)?
    };
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
    (Remote, String),
    (Size, String),
    (Timeline, String),
    (TimestampInterval, Interval)
);

generate_extracted_config!(PgConfigOption, (Details, String), (Publication, String));

pub fn plan_create_source(
    scx: &StatementContext,
    stmt: CreateSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSourceStatement {
        name,
        col_names,
        connection,
        envelope,
        if_not_exists,
        format,
        key_constraint,
        include_metadata,
        with_options,
        subsources,
    } = &stmt;

    let envelope = envelope.clone().unwrap_or(Envelope::None);

    const SAFE_WITH_OPTIONS: &[CreateSourceOptionName] = &[CreateSourceOptionName::Size];

    if with_options
        .iter()
        .any(|op| !SAFE_WITH_OPTIONS.contains(&op.name))
    {
        scx.require_unsafe_mode(&format!(
            "creating sources with WITH options other than {}",
            comma_separated(SAFE_WITH_OPTIONS)
        ))?;
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

    let (external_connection, encoding, available_subsources) = match connection {
        CreateSourceConnection::Kafka(mz_sql_parser::ast::KafkaSourceConnection {
            connection:
                mz_sql_parser::ast::KafkaConnection {
                    connection: connection_name,
                    options,
                },
            key: _,
        }) => {
            let connection_item = scx.get_item_by_resolved_name(connection_name)?;
            let kafka_connection = match connection_item.connection()? {
                Connection::Kafka(connection) => connection.clone(),
                _ => sql_bail!("{} is not a kafka connection", connection_item.name()),
            };

            // Starting offsets are allowed out unsafe mode, as they are a simple,
            // useful way to specify where to start reading a topic.
            if let Some(opt) = options.iter().find(|opt| {
                opt.name != KafkaConfigOptionName::StartOffset
                    && opt.name != KafkaConfigOptionName::StartTimestamp
                    && opt.name != KafkaConfigOptionName::Topic
            }) {
                scx.require_unsafe_mode(&format!("KAFKA CONNECTION option {}", opt.name))?;
            }

            kafka_util::validate_options_for_context(
                options,
                kafka_util::KafkaOptionCheckContext::Source,
            )?;

            let extracted_options: KafkaConfigOptionExtracted = options.clone().try_into()?;

            let optional_start_offset =
                Option::<kafka_util::KafkaStartOffsetType>::try_from(&extracted_options)?;
            let options = kafka_util::LibRdKafkaConfig::try_from(&extracted_options)?.0;

            let topic = extracted_options
                .topic
                .expect("validated exists during purification");
            let group_id_prefix = extracted_options.group_id_prefix;

            let mut start_offsets = HashMap::new();
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

            let encoding = get_encoding(scx, format, &envelope, connection)?;

            let mut connection = KafkaSourceConnection {
                connection: kafka_connection,
                connection_id: connection_item.id(),
                options,
                topic,
                start_offsets,
                group_id_prefix,
                environment_id: scx.catalog.config().environment_id.clone(),
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

            let connection = SourceConnection::Kafka(connection);

            (connection, encoding, None)
        }
        CreateSourceConnection::Kinesis {
            connection: aws_connection,
            arn,
        } => {
            scx.require_unsafe_mode("CREATE SOURCE ... FROM KINESIS")?;
            let arn: AmazonResourceName = arn
                .parse()
                .map_err(|e| sql_err!("Unable to parse provided ARN: {:#?}", e))?;
            let stream_name = match arn.resource.strip_prefix("stream/") {
                Some(path) => path.to_owned(),
                _ => sql_bail!(
                    "Unable to parse stream name from resource path: {}",
                    arn.resource
                ),
            };

            let connection_item = scx.get_item_by_resolved_name(aws_connection)?;
            let aws = match connection_item.connection()? {
                Connection::Aws(aws) => aws.clone(),
                _ => sql_bail!("{} is not an AWS connection", connection_item.name()),
            };

            let encoding = get_encoding(scx, format, &envelope, connection)?;
            let connection = SourceConnection::Kinesis(KinesisSourceConnection {
                connection_id: connection_item.id(),
                stream_name,
                aws,
            });
            (connection, encoding, None)
        }
        CreateSourceConnection::S3 {
            connection: aws_connection,
            key_sources,
            pattern,
            compression,
        } => {
            scx.require_unsafe_mode("CREATE SOURCE ... FROM S3")?;

            let connection_item = scx.get_item_by_resolved_name(aws_connection)?;
            let aws = match connection_item.connection()? {
                Connection::Aws(aws) => aws.clone(),
                _ => sql_bail!("{} is not an AWS connection", connection_item.name()),
            };

            let mut converted_sources = Vec::new();
            for ks in key_sources {
                let dtks = match ks {
                    mz_sql_parser::ast::S3KeySource::Scan { bucket } => {
                        mz_storage::types::sources::S3KeySource::Scan {
                            bucket: bucket.clone(),
                        }
                    }
                    mz_sql_parser::ast::S3KeySource::SqsNotifications { queue } => {
                        mz_storage::types::sources::S3KeySource::SqsNotifications {
                            queue: queue.clone(),
                        }
                    }
                };
                converted_sources.push(dtks);
            }
            let encoding = get_encoding(scx, format, &envelope, connection)?;
            if matches!(encoding, SourceDataEncoding::KeyValue { .. }) {
                sql_bail!("S3 sources do not support key decoding");
            }
            let connection = SourceConnection::S3(S3SourceConnection {
                connection_id: connection_item.id(),
                key_sources: converted_sources,
                pattern: pattern
                    .as_ref()
                    .map(|p| {
                        GlobBuilder::new(p)
                            .literal_separator(true)
                            .backslash_escape(true)
                            .build()
                    })
                    .transpose()
                    .map_err(|e| sql_err!("parsing glob: {e}"))?,
                aws,
                compression: match compression {
                    Compression::Gzip => mz_storage::types::sources::Compression::Gzip,
                    Compression::None => mz_storage::types::sources::Compression::None,
                },
            });
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
                seen: _,
            } = options.clone().try_into()?;

            let details = details
                .as_ref()
                .ok_or_else(|| sql_err!("internal error: Postgres source missing details"))?;
            let details = hex::decode(details).map_err(|e| sql_err!("{}", e))?;
            let details =
                ProtoPostgresSourceDetails::decode(&*details).map_err(|e| sql_err!("{}", e))?;

            // Register the available subsources
            let mut available_subsources = HashMap::new();

            for (i, table) in details.tables.iter().enumerate() {
                let name = FullObjectName {
                    database: RawDatabaseSpecifier::Name(connection.database.clone()),
                    schema: table.namespace.clone(),
                    item: table.name.clone(),
                };
                // The zero-th output is the main output
                // TODO(petrosagg): these plus ones are an accident waiting to happen. Find a way
                // to handle the main source and the subsources uniformly
                available_subsources.insert(name, i + 1);
            }

            // Here we will generate the cast expressions required to convert the text encoded
            // columns into the appropriate target types, creating a Vec<MirScalarExpr> per table.
            // The postgres source reader will then eval each of those on the incoming rows based
            // on the target table
            let mut table_casts = vec![];
            for table in details.tables.iter() {
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
                    let ty = mz_pgrepr::Type::from_oid_and_typmod(column.type_oid, column.type_mod)
                        .map_err(|e| sql_err!("{}", e))?;
                    let data_type = scx.resolve_type(ty)?;
                    let scalar_type = query::scalar_type_from_sql(scx, &data_type)?;

                    let col_expr = HirScalarExpr::Column(ColumnRef {
                        level: 0,
                        column: i,
                    });

                    let cast_expr = plan_cast(
                        &cast_ecx,
                        CastContext::Explicit,
                        col_expr,
                        &scalar_type,
                    )?
                    .lower_uncorrelated()
                    .expect(
                        "lower_uncorrelated should not fail given that there is no correlation \
                            in the input col_expr",
                    );
                    column_casts.push(cast_expr);
                }
                table_casts.push(column_casts);
            }

            let connection = SourceConnection::Postgres(PostgresSourceConnection {
                connection,
                connection_id: connection_item.id(),
                table_casts,
                publication: publication.expect("validated exists during purification"),
                details: PostgresSourceDetails::from_proto(details)
                    .map_err(|e| sql_err!("{}", e))?,
            });

            // The postgres source only outputs data to its subsources. The catalog object
            // representing the source itself is just an empty relation with no columns
            let encoding = SourceDataEncoding::Single(DataEncoding::new(
                DataEncodingInner::RowCodec(RelationDesc::empty()),
            ));
            (connection, encoding, Some(available_subsources))
        }
        CreateSourceConnection::LoadGenerator { generator, options } => {
            use mz_storage::types::sources::LoadGenerator;

            let load_generator = match generator {
                mz_sql_parser::ast::LoadGenerator::Auction => LoadGenerator::Auction,
                mz_sql_parser::ast::LoadGenerator::Counter => LoadGenerator::Counter,
            };
            let generator = as_generator(&load_generator);

            let mut available_subsources = HashMap::new();
            for (i, (name, _)) in generator.views().iter().enumerate() {
                let name = FullObjectName {
                    database: RawDatabaseSpecifier::Name("mz_loadgenerator".to_owned()),
                    schema: "auction".to_owned(),
                    item: name.to_string(),
                };
                // The zero-th output is the main output
                // TODO(petrosagg): these plus ones are an accident waiting to happen. Find a way
                // to handle the main source and the subsources uniformly
                available_subsources.insert(name, i + 1);
            }

            let available_subsources = if available_subsources.is_empty() {
                None
            } else {
                Some(available_subsources)
            };

            let LoadGeneratorOptionExtracted { tick_interval, .. } = options.clone().try_into()?;
            let tick_micros = match tick_interval {
                Some(interval) => {
                    let micros: u64 = interval.as_microseconds().try_into()?;
                    Some(micros)
                }
                None => None,
            };
            let connection = SourceConnection::LoadGenerator(LoadGeneratorSourceConnection {
                load_generator,
                tick_micros,
            });
            (connection, generator.data_encoding(), available_subsources)
        }
    };
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
                DbzMode::Plain => {
                    UnplannedSourceEnvelope::Upsert(UpsertStyle::Debezium { after_idx })
                }
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
            UnplannedSourceEnvelope::Upsert(UpsertStyle::Default(key_envelope))
        }
        mz_sql_parser::ast::Envelope::CdcV2 => {
            scx.require_unsafe_mode("ENVELOPE MATERIALIZE")?;
            //TODO check that key envelope is not set
            match format {
                CreateSourceFormat::Bare(Format::Avro(_)) => {}
                _ => bail_unsupported!("non-Avro-encoded ENVELOPE MATERIALIZE"),
            }
            UnplannedSourceEnvelope::CdcV2
        }
    };

    let metadata_columns = external_connection.metadata_columns();
    let metadata_column_types = external_connection.metadata_column_types();
    let metadata_desc = included_column_desc(metadata_columns.clone());
    let (envelope, mut desc) = envelope.desc(key_desc, value_desc, metadata_desc)?;

    let CreateSourceOptionExtracted {
        remote,
        size,
        timeline,
        timestamp_interval,
        ignore_keys,
        seen: _,
    } = CreateSourceOptionExtracted::try_from(with_options.clone())?;

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
        scx.require_unsafe_mode("PRIMARY KEY NOT ENFORCED")?;

        let key_columns = columns
            .into_iter()
            .map(normalize::column_name)
            .collect::<Vec<_>>();

        let mut uniq = HashSet::new();
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

    let host_config = host_config(remote, size)?;

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

    let (available_subsources, requested_subsources) = match (available_subsources, subsources) {
        (Some(available_subsources), Some(CreateSourceSubsources::Subset(subsources))) => {
            let mut requested_subsources = vec![];
            for subsource in subsources {
                let (name, target) = match subsource {
                    CreateSourceSubsource::Resolved(name, target) => (name, target),
                    CreateSourceSubsource::Aliased(_, _) | CreateSourceSubsource::Bare(_) => {
                        sql_bail!(
                            "[internal error] subsources should be resolved during purification"
                        )
                    }
                };
                requested_subsources.push((name.clone(), target));
            }
            (available_subsources, requested_subsources)
        }
        (Some(_), None) => {
            // Multi-output sources must have a table selection clause
            sql_bail!("This is a multi-output source. Use `FOR TABLE (..)` or `FOR ALL TABLES` to select which ones to ingest");
        }
        (None, Some(_)) | (Some(_), Some(CreateSourceSubsources::All)) => {
            sql_bail!("[internal error] subsources should be resolved during purification")
        }
        (None, None) => (HashMap::<FullObjectName, usize>::new(), vec![]),
    };

    let mut subsource_exports = HashMap::new();
    for (name, target) in requested_subsources {
        let name = normalize::full_name(name)?;
        let idx = match available_subsources.get(&name) {
            Some(idx) => idx,
            None => sql_bail!("Requested non-existent subtable: {name}"),
        };

        let target_id = match target {
            ResolvedObjectName::Object { id, .. } => id,
            ResolvedObjectName::Cte { .. } | ResolvedObjectName::Error => {
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
        subsource_exports.insert(*target_id, *idx);
    }

    let if_not_exists = *if_not_exists;
    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name.clone())?)?;
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
        ingestion: Some(Ingestion {
            desc: source_desc,
            // Currently no source reads from another source
            source_imports: HashSet::new(),
            subsource_exports,
        }),
        desc,
    };

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        timeline,
        host_config,
    }))
}

pub fn plan_create_subsource(
    scx: &StatementContext,
    stmt: CreateSubsourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSubsourceStatement {
        name,
        columns,
        constraints,
        if_not_exists,
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

    for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary,
            } => {
                let mut key = vec![];
                for column in columns {
                    let column = normalize::column_name(column.clone());
                    match names.iter().position(|name| *name == column) {
                        None => sql_bail!("unknown column in constraint: {}", column),
                        Some(i) => {
                            key.push(i);
                            if *is_primary {
                                column_types[i].nullable = false;
                            }
                        }
                    }
                }
                keys.push(key);
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
    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name.clone())?)?;
    let create_sql = normalize::create_statement(scx, Statement::CreateSubsource(stmt))?;

    let typ = RelationType::new(column_types).with_keys(keys);
    let desc = RelationDesc::new(typ, names);

    let source = Source {
        create_sql,
        ingestion: None,
        desc,
    };

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        timeline: Timeline::EpochMilliseconds,
        host_config: StorageHostConfig::Undefined,
    }))
}

generate_extracted_config!(LoadGeneratorOption, (TickInterval, Interval));

fn typecheck_debezium(value_desc: &RelationDesc) -> Result<(usize, usize), PlanError> {
    let (before_idx, before_ty) = value_desc
        .get_by_name(&"before".into())
        .ok_or_else(|| sql_err!("'before' column missing from debezium input"))?;
    let (after_idx, after_ty) = value_desc
        .get_by_name(&"after".into())
        .ok_or_else(|| sql_err!("'after' column missing from debezium input"))?;
    if !matches!(before_ty.scalar_type, ScalarType::Record { .. }) {
        sql_bail!("'before' column must be of type record");
    }
    if before_ty != after_ty {
        sql_bail!("'before' type differs from 'after' column");
    }
    Ok((before_idx, after_idx))
}

fn get_encoding(
    scx: &StatementContext,
    format: &CreateSourceFormat<Aug>,
    envelope: &Envelope,
    connection: &CreateSourceConnection<Aug>,
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

    let force_nullable_keys = matches!(connection, CreateSourceConnection::Kafka(_))
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

fn host_config(
    remote: Option<String>,
    size: Option<String>,
) -> Result<StorageHostConfig, PlanError> {
    match (remote, size) {
        (None, None) => Ok(StorageHostConfig::Undefined),
        (None, Some(size)) => Ok(StorageHostConfig::Managed { size }),
        (Some(addr), None) => Ok(StorageHostConfig::Remote { addr }),
        (Some(_), Some(_)) => sql_bail!("only one of REMOTE and SIZE can be set"),
    }
}

generate_extracted_config!(AvroSchemaOption, (ConfluentWireFormat, bool, Default(true)));

#[derive(Debug)]
pub struct Schema {
    pub key_schema: Option<String>,
    pub value_schema: String,
    pub csr_connection: Option<mz_storage::types::connections::CsrConnection>,
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
                            sql_bail!("{} is not a schema registry connection", item.name())
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
                            sql_bail!("{} is not a schema registry connection", item.name())
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
                CsvColumns::Count(n) => ColumnSpec::Count(*n),
            };
            DataEncodingInner::Csv(CsvEncoding {
                columns,
                delimiter: match *delimiter as u32 {
                    0..=127 => *delimiter as u8,
                    _ => sql_bail!("CSV delimiter must be an ASCII character"),
                },
            })
        }
        Format::Json => bail_unsupported!("JSON sources"),
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
        DataEncodingInner::Bytes | DataEncodingInner::Text => false,
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
) -> Result<(QualifiedObjectName, View), PlanError> {
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
    } = query::plan_root_query(scx, query.clone(), QueryLifetime::Static)?;

    expr.bind_parameters(params)?;
    //TODO: materialize#724 - persist finishing information with the view?
    expr.finish(finishing);
    let relation_expr = expr.optimize_and_lower(&scx.into())?;

    let name = if temporary {
        scx.allocate_temporary_qualified_name(normalize::unresolved_object_name(name.to_owned())?)?
    } else {
        scx.allocate_qualified_name(normalize::unresolved_object_name(name.to_owned())?)?
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
    let partial_name = normalize::unresolved_object_name(definition.name.clone())?;
    let (name, view) = plan_view(scx, definition, params, *temporary)?;
    let replace = if *if_exists == IfExistsBehavior::Replace {
        if let Ok(item) = scx.catalog.resolve_item(&partial_name) {
            if view.expr.depends_on().contains(&item.id()) {
                sql_bail!(
                    "cannot replace view {0}: depended upon by new {0} definition",
                    scx.catalog.resolve_full_name(item.name())
                );
            }
            let cascade = false;
            plan_drop_item(scx, ObjectType::View, item, cascade)?
        } else {
            None
        }
    } else {
        None
    };
    Ok(Plan::CreateView(CreateViewPlan {
        name,
        view,
        replace,
        if_not_exists: *if_exists == IfExistsBehavior::Skip,
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
    let compute_instance = match &stmt.in_cluster {
        None => scx.resolve_compute_instance(None)?.id(),
        Some(in_cluster) => in_cluster.id,
    };
    stmt.in_cluster = Some(ResolvedClusterName {
        id: compute_instance,
        print_name: None,
    });

    let create_sql =
        normalize::create_statement(scx, Statement::CreateMaterializedView(stmt.clone()))?;

    let partial_name = normalize::unresolved_object_name(stmt.name)?;
    let name = scx.allocate_qualified_name(partial_name.clone())?;

    let query::PlannedQuery {
        mut expr,
        mut desc,
        finishing,
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
            if let Ok(item) = scx.catalog.resolve_item(&partial_name) {
                if expr.depends_on().contains(&item.id()) {
                    sql_bail!(
                        "cannot replace materialized view {0}: depended upon by new {0} definition",
                        scx.catalog.resolve_full_name(item.name())
                    );
                }
                let cascade = false;
                replace = plan_drop_item(scx, ObjectType::MaterializedView, item, cascade)?;
            }
        }
        IfExistsBehavior::Skip => if_not_exists = true,
        IfExistsBehavior::Error => (),
    }

    Ok(Plan::CreateMaterializedView(CreateMaterializedViewPlan {
        name,
        materialized_view: MaterializedView {
            create_sql,
            expr,
            column_names,
            compute_instance,
        },
        replace,
        if_not_exists,
    }))
}

pub fn describe_create_sink(
    _: &StatementContext,
    _: CreateSinkStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(
    CreateSinkOption,
    (Remote, String),
    (Size, String),
    (Snapshot, bool, Default(true))
);

pub fn plan_create_sink(
    scx: &StatementContext,
    stmt: CreateSinkStatement<Aug>,
) -> Result<Plan, PlanError> {
    let create_sql = normalize::create_statement(scx, Statement::CreateSink(stmt.clone()))?;
    let CreateSinkStatement {
        name,
        from,
        connection,
        format,
        envelope,
        if_not_exists,
        with_options,
    } = stmt;

    const SAFE_WITH_OPTIONS: &[CreateSinkOptionName] =
        &[CreateSinkOptionName::Size, CreateSinkOptionName::Snapshot];

    if with_options
        .iter()
        .any(|op| !SAFE_WITH_OPTIONS.contains(&op.name))
    {
        scx.require_unsafe_mode(&format!(
            "creating sinks with WITH options other than {}",
            comma_separated(SAFE_WITH_OPTIONS)
        ))?;
    }

    let envelope = match envelope {
        None => sql_bail!("ENVELOPE clause is required"),
        Some(Envelope::Debezium(mz_sql_parser::ast::DbzMode::Plain)) => SinkEnvelope::Debezium,
        Some(Envelope::Upsert) => SinkEnvelope::Upsert,
        Some(Envelope::CdcV2) => bail_unsupported!("CDCv2 sinks"),
        Some(Envelope::None) => bail_unsupported!("\"ENVELOPE NONE\" sinks"),
    };
    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name)?)?;
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
                let mut uniq = HashSet::new();
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
                            anyhow::bail!("Ambiguous column: {}", col);
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
        remote,
        size,
        snapshot,
        seen: _,
    } = with_options.try_into()?;

    let host_config = host_config(remote, size)?;

    Ok(Plan::CreateSink(CreateSinkPlan {
        name,
        sink: Sink {
            create_sql,
            from: from.id(),
            connection_builder,
            envelope,
        },
        with_snapshot: snapshot,
        if_not_exists,
        host_config,
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
        options: with_options,
    }: mz_sql_parser::ast::KafkaConnection<Aug>,
    format: Option<Format<Aug>>,
    relation_key_indices: Option<Vec<usize>>,
    key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    value_desc: RelationDesc,
    envelope: SinkEnvelope,
) -> Result<StorageSinkConnectionBuilder, PlanError> {
    let item = scx.get_item_by_resolved_name(&connection)?;
    // Get Kafka connection
    let connection = match item.connection()? {
        Connection::Kafka(connection) => connection.clone(),
        _ => sql_bail!("{} is not a kafka connection", item.name()),
    };

    if with_options
        .iter()
        .any(|mz_sql_parser::ast::KafkaConfigOption { name, .. }| {
            !matches!(name, KafkaConfigOptionName::Topic)
        })
    {
        scx.require_unsafe_mode("KAFKA CONNECTION options besides TOPIC")?;
    }

    kafka_util::validate_options_for_context(
        &with_options,
        kafka_util::KafkaOptionCheckContext::Sink,
    )?;

    let extracted_options: KafkaConfigOptionExtracted = with_options.try_into()?;
    let config_options = kafka_util::LibRdKafkaConfig::try_from(&extracted_options)?.0;
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
                    sql_bail!("{} is not a schema registry connection", item.name())
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
            );
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
            options: config_options,
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
        QualifiedObjectName {
            qualifiers: on.name().qualifiers.clone(),
            item: normalize::ident(name.clone()),
        }
    } else {
        let mut idx_name = QualifiedObjectName {
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

    let options = plan_index_options(scx, with_options.clone())?;
    let compute_instance = match in_cluster {
        None => scx.resolve_compute_instance(None)?.id(),
        Some(in_cluster) => in_cluster.id,
    };
    *in_cluster = Some(ResolvedClusterName {
        id: compute_instance,
        print_name: None,
    });

    // Normalize `stmt`.
    *name = Some(Ident::new(index_name.item.clone()));
    *key_parts = Some(filled_key_parts);
    let if_not_exists = *if_not_exists;
    if let ResolvedObjectName::Object { print_id, .. } = &mut stmt.on_name {
        *print_id = false;
    }
    let create_sql = normalize::create_statement(scx, Statement::CreateIndex(stmt))?;

    Ok(Plan::CreateIndex(CreateIndexPlan {
        name: index_name,
        index: Index {
            create_sql,
            on: on.id(),
            keys,
            compute_instance,
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

    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name)?)?;
    if scx.item_exists(&name) {
        sql_bail!("catalog item '{}' already exists", name);
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

pub fn describe_create_role(
    _: &StatementContext,
    _: CreateRoleStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_role(
    _: &StatementContext,
    CreateRoleStatement {
        name,
        is_user,
        options,
    }: CreateRoleStatement,
) -> Result<Plan, PlanError> {
    let mut login = None;
    let mut super_user = None;
    for option in options {
        match option {
            CreateRoleOption::Login | CreateRoleOption::NoLogin if login.is_some() => {
                sql_bail!("conflicting or redundant options");
            }
            CreateRoleOption::SuperUser | CreateRoleOption::NoSuperUser if super_user.is_some() => {
                sql_bail!("conflicting or redundant options");
            }
            CreateRoleOption::Login => login = Some(true),
            CreateRoleOption::NoLogin => login = Some(false),
            CreateRoleOption::SuperUser => super_user = Some(true),
            CreateRoleOption::NoSuperUser => super_user = Some(false),
        }
    }
    if is_user && login.is_none() {
        login = Some(true);
    }
    if login != Some(true) {
        bail_unsupported!("non-login users");
    }
    if super_user != Some(true) {
        bail_unsupported!("non-superusers");
    }
    Ok(Plan::CreateRole(CreateRolePlan {
        name: normalize::ident(name),
    }))
}

pub fn describe_create_cluster(
    _: &StatementContext,
    _: CreateClusterStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_cluster(
    scx: &StatementContext,
    CreateClusterStatement { name, options }: CreateClusterStatement<Aug>,
) -> Result<Plan, PlanError> {
    let mut replicas_definitions = None;

    for option in options {
        match option {
            ClusterOption::Replicas(replicas) => {
                if replicas_definitions.is_some() {
                    sql_bail!("REPLICAS specified more than once");
                }

                let mut defs = Vec::with_capacity(replicas.len());
                for ReplicaDefinition { name, options } in replicas {
                    defs.push((normalize::ident(name), plan_replica_config(scx, options)?));
                }
                replicas_definitions = Some(defs);
            }
        }
    }

    let replicas = match replicas_definitions {
        Some(r) => r,
        None => bail_unsupported!("CLUSTER without REPLICAS option"),
    };

    Ok(Plan::CreateComputeInstance(CreateComputeInstancePlan {
        name: normalize::ident(name),
        replicas,
    }))
}

const DEFAULT_INTROSPECTION_INTERVAL: Interval = Interval {
    micros: 1_000_000,
    months: 0,
    days: 0,
};

generate_extracted_config!(
    ReplicaOption,
    (AvailabilityZone, String),
    (Size, String),
    (Remote, Vec<String>),
    (Compute, Vec<String>),
    (Workers, u16),
    (IntrospectionInterval, OptionalInterval),
    (IntrospectionDebugging, bool)
);

fn plan_replica_config(
    scx: &StatementContext,
    options: Vec<ReplicaOption<Aug>>,
) -> Result<ComputeReplicaConfig, PlanError> {
    let ReplicaOptionExtracted {
        availability_zone,
        size,
        remote,
        workers,
        compute,
        introspection_interval,
        introspection_debugging,
        ..
    }: ReplicaOptionExtracted = options.try_into()?;

    if remote.is_some() {
        scx.require_unsafe_mode("REMOTE cluster replica option")?;
    }
    if compute.is_some() {
        scx.require_unsafe_mode("COMPUTE cluster replica option")?;
    }
    if workers.is_some() {
        scx.require_unsafe_mode("WORKERS cluster replica option")?;
    }

    let introspection_interval = introspection_interval
        .map(|OptionalInterval(i)| i)
        .unwrap_or(Some(DEFAULT_INTROSPECTION_INTERVAL));
    let introspection = match introspection_interval {
        Some(interval) => Some(ComputeReplicaIntrospectionConfig {
            interval: interval.duration()?,
            debugging: introspection_debugging.unwrap_or(false),
        }),
        None if introspection_debugging == Some(true) => {
            sql_bail!("INTROSPECTION DEBUGGING cannot be specified without INTROSPECTION INTERVAL")
        }
        None => None,
    };

    match (size, remote) {
        (None, Some(remote)) => {
            // REMOTE given, no SIZE
            if availability_zone.is_some() {
                sql_bail!("cannot specify AVAILABILITY ZONE and REMOTE");
            }
            // Unwrap REMOTE options
            let remote_addrs = remote.into_iter().collect::<BTreeSet<String>>();
            let compute_addrs = compute
                .unwrap_or_default()
                .into_iter()
                .collect::<BTreeSet<String>>();
            let workers = workers.unwrap_or(1);

            if remote_addrs.len() > 1 && (remote_addrs.len() != compute_addrs.len()) {
                sql_bail!(
                    "must specify as many REMOTE addresses as COMPUTE addresses for multi-process replicas"
                );
            }
            if compute_addrs.len() > remote_addrs.len() {
                sql_bail!(
                    "must specify as many REMOTE addresses as COMPUTE addresses for multi-process replicas"
                );
            }

            let workers = NonZeroUsize::new(workers.into())
                .ok_or_else(|| sql_err!("WORKERS must be greater 0"))?;
            Ok(ComputeReplicaConfig::Remote {
                addrs: remote_addrs,
                compute_addrs,
                workers,
                introspection,
            })
        }
        (Some(size), None) => {
            // SIZE given, no REMOTE
            if workers.is_some() {
                sql_bail!("cannot specify SIZE and WORKERS");
            }
            if compute.is_some() {
                sql_bail!("cannot specify SIZE and COMPUTE");
            }
            Ok(ComputeReplicaConfig::Managed {
                size,
                availability_zone,
                introspection,
            })
        }
        (_, _) => {
            // SIZE and REMOTE given, or none of them
            sql_bail!("only one of REMOTE or SIZE may be specified")
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
    let _ = scx
        .catalog
        .resolve_compute_instance(Some(&of_cluster.to_string()))?;
    Ok(Plan::CreateComputeReplica(CreateComputeReplicaPlan {
        name: normalize::ident(name),
        of_cluster: of_cluster.to_string(),
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

    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name.to_owned())?)?;
    let create_sql = normalize::create_statement(scx, Statement::CreateSecret(stmt.clone()))?;
    let secret_as = query::plan_secret_as(scx, value.clone())?;

    let secret = Secret {
        create_sql,
        secret_as,
    };

    let full_name = scx.catalog.resolve_full_name(&name);

    Ok(Plan::CreateSecret(CreateSecretPlan {
        name,
        secret,
        full_name,
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
    (Broker, String),
    (Brokers, Vec<String>),
    (ProgressTopic, String),
    (SslKey, with_options::Secret),
    (SslCertificate, StringOrSecret),
    (SslCertificateAuthority, StringOrSecret),
    (SaslMechanisms, String),
    (SaslUsername, StringOrSecret),
    (SaslPassword, with_options::Secret)
);

impl KafkaConnectionOptionExtracted {
    pub fn get_brokers(&self) -> Result<Vec<String>, PlanError> {
        let mut brokers = match (&self.broker, &self.brokers) {
            (Some(_), Some(_)) => sql_bail!("invalid CONNECTION: cannot set BROKER and BROKERS"),
            (None, None) => sql_bail!("invalid CONNECTION: must set either BROKER or BROKERS"),
            (Some(v), None) => vec![v.to_string()],
            (None, Some(v)) => v.to_vec(),
        };

        for broker in &mut brokers {
            // Normalize Kafka addresses
            *broker = KafkaAddrs::from_str(broker)
                .map_err(|e| sql_err!("parsing kafka broker: {e}"))?
                .to_string();
            if broker.contains(',') {
                sql_bail!("invalid CONNECTION: cannot specify multiple Kafka broker addresses in one string.\n\n
Instead, specify BROKERS using an array of strings, e.g. BROKERS ['kafka:9092', 'kafka:9093']");
            }
        }

        Ok(brokers)
    }
    pub fn ssl_config(&self) -> HashSet<KafkaConnectionOptionName> {
        use KafkaConnectionOptionName::*;
        HashSet::from([SslKey, SslCertificate])
    }
    pub fn sasl_config(&self) -> HashSet<KafkaConnectionOptionName> {
        use KafkaConnectionOptionName::*;
        HashSet::from([SaslMechanisms, SaslUsername, SaslPassword])
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

impl From<&KafkaConnectionOptionExtracted> for Option<SaslConfig> {
    fn from(k: &KafkaConnectionOptionExtracted) -> Self {
        if k.sasl_config().iter().all(|config| k.seen.contains(config)) {
            Some(SaslConfig {
                mechanisms: k.sasl_mechanisms.clone().unwrap(),
                username: k.sasl_username.clone().unwrap(),
                password: k.sasl_password.unwrap().into(),
                tls_root_cert: k.ssl_certificate_authority.clone(),
            })
        } else {
            None
        }
    }
}

impl TryFrom<&KafkaConnectionOptionExtracted> for Option<KafkaSecurity> {
    type Error = PlanError;
    fn try_from(value: &KafkaConnectionOptionExtracted) -> Result<Self, Self::Error> {
        let ssl_config = Option::<KafkaTlsConfig>::from(value).map(KafkaSecurity::from);
        let sasl_config = Option::<SaslConfig>::from(value).map(KafkaSecurity::from);

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

impl TryFrom<KafkaConnectionOptionExtracted> for KafkaConnection {
    type Error = PlanError;
    fn try_from(value: KafkaConnectionOptionExtracted) -> Result<Self, Self::Error> {
        Ok(KafkaConnection {
            brokers: value.get_brokers()?,
            security: (&value).try_into()?,
            progress_topic: value.progress_topic,
        })
    }
}

generate_extracted_config!(
    CsrConnectionOption,
    (Url, String),
    (SslKey, with_options::Secret),
    (SslCertificate, StringOrSecret),
    (SslCertificateAuthority, StringOrSecret),
    (Username, StringOrSecret),
    (Password, with_options::Secret)
);

impl TryFrom<CsrConnectionOptionExtracted> for mz_storage::types::connections::CsrConnection {
    type Error = PlanError;
    fn try_from(ccsr_options: CsrConnectionOptionExtracted) -> Result<Self, Self::Error> {
        let url = match ccsr_options.url {
            Some(url) => url
                .parse()
                .map_err(|e| sql_err!("parsing schema registry url: {e}"))?,
            None => sql_bail!("invalid CONNECTION: must specify URL"),
        };
        let cert = ccsr_options.ssl_certificate;
        let key = ccsr_options.ssl_key.map(|secret| secret.into());
        let tls_identity = match (cert, key) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some(TlsIdentity { cert, key }),
            _ => sql_bail!(
                "invalid CONNECTION: reading from SSL-auth Confluent Schema Registry requires both SSL KEY and SSL CERTIFICATE"
            ),
        };
        let http_auth = ccsr_options.username.map(|username| CsrConnectionHttpAuth {
            username,
            password: ccsr_options.password.map(|secret| secret.into()),
        });
        Ok(mz_storage::types::connections::CsrConnection {
            url,
            tls_root_cert: ccsr_options.ssl_certificate_authority,
            tls_identity,
            http_auth,
        })
    }
}

generate_extracted_config!(
    PostgresConnectionOption,
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
    ) -> Result<mz_storage::types::connections::PostgresConnection, PlanError> {
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

        // Validate that the SSH tunnel ID is indeed an SSH connection
        let ssh_tunnel_id = self.ssh_tunnel.map(|ssh_tunnel| ssh_tunnel.into());
        let ssh_tunnel = if let Some(ref ssh_tunnel) = ssh_tunnel_id {
            let ssh_tunnel = scx.catalog.get_item(ssh_tunnel);
            match ssh_tunnel.connection()? {
                Connection::Ssh(ssh) => Some(ssh.clone()),
                _ => sql_bail!("{} is not an SSH connection", ssh_tunnel.name().item),
            }
        } else {
            None
        };

        Ok(mz_storage::types::connections::PostgresConnection {
            database: self
                .database
                .ok_or_else(|| sql_err!("DATABASE option is required"))?,
            host: self
                .host
                .ok_or_else(|| sql_err!("HOST option is required"))?,
            password: self.password.map(|password| password.into()),
            port: self.port,
            ssh_tunnel_id,
            ssh_tunnel,
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

impl TryFrom<SshConnectionOptionExtracted> for mz_storage::types::connections::SshConnection {
    type Error = PlanError;

    fn try_from(options: SshConnectionOptionExtracted) -> Result<Self, Self::Error> {
        Ok(mz_storage::types::connections::SshConnection {
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
                // TODO(benesch): this should not treat an empty endpoint as
                // equivalent to a `NULL` endpoint, but making that change now
                // would break testdrive. AWS connections are all behind unsafe
                // mode right now, so no particular urgency to correct this.
                Some(endpoint) if !endpoint.is_empty() => {
                    let endpoint = http::Uri::from_str(&endpoint)
                        .map_err(|e| PlanError::Unstructured(e.to_string()))?;
                    Some(SerdeUri(endpoint))
                }
                _ => None,
            },
            region: options.region,
            role: options.role_arn.map(|arn| AwsAssumeRole { arn }),
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
            let k = KafkaConnectionOptionExtracted::try_from(with_options)?;
            Connection::Kafka(KafkaConnection::try_from(k)?)
        }
        CreateConnection::Csr { with_options } => {
            let c = CsrConnectionOptionExtracted::try_from(with_options)?;
            let connection = mz_storage::types::connections::CsrConnection::try_from(c)?;
            Connection::Csr(connection)
        }
        CreateConnection::Postgres { with_options } => {
            let c = PostgresConnectionOptionExtracted::try_from(with_options)?;
            let connection = c.to_connection(scx)?;
            Connection::Postgres(connection)
        }
        CreateConnection::Aws { with_options } => {
            let c = AwsConnectionOptionExtracted::try_from(with_options)?;
            let connection = AwsConfig::try_from(c)?;
            Connection::Aws(connection)
        }
        CreateConnection::Ssh { with_options } => {
            let c = SshConnectionOptionExtracted::try_from(with_options)?;
            let connection = mz_storage::types::connections::SshConnection::try_from(c)?;
            Connection::Ssh(connection)
        }
    };
    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name)?)?;
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

pub fn describe_drop_database(
    _: &StatementContext,
    _: DropDatabaseStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_database(
    scx: &StatementContext,
    DropDatabaseStatement {
        name,
        restrict,
        if_exists,
    }: DropDatabaseStatement,
) -> Result<Plan, PlanError> {
    let id = match scx.resolve_database(&name) {
        Ok(database) => {
            if restrict && database.has_schemas() {
                sql_bail!(
                    "database '{}' cannot be dropped with RESTRICT while it contains schemas",
                    name,
                );
            }
            Some(database.id())
        }
        // TODO(benesch/jkosh44): generate a notice indicating that the database does not exist.
        Err(_) if if_exists => None,
        Err(e) => return Err(e),
    };
    Ok(Plan::DropDatabase(DropDatabasePlan { id }))
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
        names,
        cascade,
        if_exists,
    }: DropObjectsStatement,
) -> Result<Plan, PlanError> {
    let mut items = vec![];
    for name in names {
        let name = normalize::unresolved_object_name(name)?;
        match scx.catalog.resolve_item(&name) {
            Ok(item) => items.push(item),
            Err(_) if if_exists => {
                // TODO(benesch/jkosh44): generate a notice indicating items do not exist.
            }
            Err(e) => return Err(e.into()),
        }
    }

    match object_type {
        ObjectType::Source
        | ObjectType::Table
        | ObjectType::View
        | ObjectType::MaterializedView
        | ObjectType::Index
        | ObjectType::Sink
        | ObjectType::Type
        | ObjectType::Secret
        | ObjectType::Connection => plan_drop_items(scx, object_type, &items, cascade),
        ObjectType::Role | ObjectType::Cluster | ObjectType::ClusterReplica => {
            unreachable!("handled through their respective plan_drop functions")
        }
        ObjectType::Object => unreachable!("cannot drop generic OBJECT, must provide object type"),
    }
}

pub fn describe_drop_schema(
    _: &StatementContext,
    _: DropSchemaStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_schema(
    scx: &StatementContext,
    DropSchemaStatement {
        name,
        cascade,
        if_exists,
    }: DropSchemaStatement,
) -> Result<Plan, PlanError> {
    match scx.resolve_schema(name) {
        Ok(schema) => {
            let database_id = match schema.database() {
                ResolvedDatabaseSpecifier::Ambient => sql_bail!(
                    "cannot drop schema {} because it is required by the database system",
                    schema.name().schema
                ),
                ResolvedDatabaseSpecifier::Id(id) => id,
            };
            if !cascade && schema.has_items() {
                let full_schema_name = FullSchemaName {
                    database: match schema.name().database {
                        ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
                        ResolvedDatabaseSpecifier::Id(id) => {
                            RawDatabaseSpecifier::Name(scx.get_database(&id).name().to_string())
                        }
                    },
                    schema: schema.name().schema.clone(),
                };
                sql_bail!(
                    "schema '{}' cannot be dropped without CASCADE while it contains objects",
                    full_schema_name
                );
            }
            let schema_id = match schema.id() {
                // This branch should be unreachable because the temporary schema is in the ambient
                // database, but this is just to protect against the case that ever changes.
                SchemaSpecifier::Temporary => sql_bail!(
                    "cannot drop schema {} because it is a temporary schema",
                    schema.name().schema,
                ),
                SchemaSpecifier::Id(id) => id,
            };
            Ok(Plan::DropSchema(DropSchemaPlan {
                id: Some((database_id.clone(), schema_id.clone())),
            }))
        }
        Err(_) if if_exists => {
            // TODO(benesch/jkosh44): generate a notice indicating that the
            // schema does not exist.
            Ok(Plan::DropSchema(DropSchemaPlan { id: None }))
        }
        Err(e) => Err(e),
    }
}

pub fn describe_drop_role(
    _: &StatementContext,
    _: DropRolesStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_role(
    scx: &StatementContext,
    DropRolesStatement { if_exists, names }: DropRolesStatement,
) -> Result<Plan, PlanError> {
    let mut out = vec![];
    for name in names {
        let name = if name.0.len() == 1 {
            normalize::ident(name.0.into_element())
        } else {
            sql_bail!("invalid role name {}", name.to_string().quoted())
        };
        if name == scx.catalog.active_user() {
            sql_bail!("current user cannot be dropped");
        }
        match scx.catalog.resolve_role(&name) {
            Ok(_) => out.push(name),
            Err(_) if if_exists => {
                // TODO(benesch): generate a notice indicating that the
                // role does not exist.
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(Plan::DropRoles(DropRolesPlan { names: out }))
}

pub fn describe_drop_cluster(
    _: &StatementContext,
    _: DropClustersStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_cluster(
    scx: &StatementContext,
    DropClustersStatement {
        if_exists,
        names,
        cascade,
    }: DropClustersStatement,
) -> Result<Plan, PlanError> {
    let mut out = vec![];
    for name in names {
        let name = if name.0.len() == 1 {
            name.0.into_element()
        } else {
            sql_bail!("invalid cluster name {}", name.to_string().quoted())
        };
        match scx.catalog.resolve_compute_instance(Some(name.as_str())) {
            Ok(instance) => {
                if !cascade && !instance.exports().is_empty() {
                    sql_bail!("cannot drop cluster with active indexes or materialized views");
                }
                out.push(name.into_string());
            }
            Err(_) if if_exists => {
                // TODO(benesch): generate a notice indicating that the
                // cluster does not exist.
            }
            Err(e) => return Err(e.into()),
        }
    }
    Ok(Plan::DropComputeInstances(DropComputeInstancesPlan {
        names: out,
    }))
}

pub fn describe_drop_cluster_replica(
    _: &StatementContext,
    _: DropClusterReplicasStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_cluster_replica(
    scx: &StatementContext,
    DropClusterReplicasStatement { if_exists, names }: DropClusterReplicasStatement,
) -> Result<Plan, PlanError> {
    let mut names_out = Vec::with_capacity(names.len());
    for QualifiedReplica { cluster, replica } in names {
        let instance = match scx.catalog.resolve_compute_instance(Some(cluster.as_str())) {
            Ok(instance) => instance,
            Err(_) if if_exists => continue,
            Err(e) => return Err(e.into()),
        };
        let replica_name = replica.into_string();
        // Check to see if name exists
        if instance.replica_names().contains(&replica_name) {
            names_out.push((instance.name().to_string(), replica_name));
        } else {
            // If "IF EXISTS" supplied, names allowed to be missing,
            // otherwise error.
            if !if_exists {
                // TODO(benesch): generate a notice indicating that the
                // replica does not exist.
                sql_bail!(
                    "CLUSTER {} has no CLUSTER REPLICA named {}",
                    instance.name(),
                    replica_name.quoted(),
                )
            }
        }
    }

    Ok(Plan::DropComputeReplicas(DropComputeReplicasPlan {
        names: names_out,
    }))
}

pub fn plan_drop_items(
    scx: &StatementContext,
    object_type: ObjectType,
    items: &[&dyn CatalogItem],
    cascade: bool,
) -> Result<Plan, PlanError> {
    let mut ids = vec![];
    for item in items {
        ids.extend(plan_drop_item(scx, object_type, *item, cascade)?);
    }
    Ok(Plan::DropItems(DropItemsPlan {
        items: ids,
        ty: object_type,
    }))
}

pub fn plan_drop_item(
    scx: &StatementContext,
    object_type: ObjectType,
    catalog_entry: &dyn CatalogItem,
    cascade: bool,
) -> Result<Option<GlobalId>, PlanError> {
    if catalog_entry.id().is_system() {
        sql_bail!(
            "cannot drop item {} because it is required by the database system",
            scx.catalog.resolve_full_name(catalog_entry.name()),
        );
    }
    let item_type = catalog_entry.item_type();

    // Return a more helpful error on `DROP VIEW <materialized-view>`.
    if object_type == ObjectType::View && item_type == CatalogItemType::MaterializedView {
        let name = scx
            .catalog
            .resolve_full_name(catalog_entry.name())
            .to_string();
        return Err(PlanError::DropViewOnMaterializedView(name));
    } else if object_type != item_type {
        sql_bail!(
            "{} is not of type {}",
            scx.catalog.resolve_full_name(catalog_entry.name()),
            object_type,
        );
    }
    if !cascade {
        let entry_id = catalog_entry.id();
        // When this item gets dropped it will also drop its subsources, so we need to check the
        // users of those
        let mut dropped_items = catalog_entry
            .subsources()
            .iter()
            .map(|id| scx.catalog.get_item(id))
            .collect_vec();
        dropped_items.push(catalog_entry);

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
                    sql_bail!(
                        "cannot drop {}: still depended upon by catalog item '{}'",
                        scx.catalog.resolve_full_name(catalog_entry.name()),
                        scx.catalog.resolve_full_name(dep.name())
                    );
                }
            }
        }
    }
    Ok(Some(catalog_entry.id()))
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

generate_extracted_config!(IndexOption, (LogicalCompactionWindow, OptionalInterval));

fn plan_index_options(
    scx: &StatementContext,
    with_opts: Vec<IndexOption<Aug>>,
) -> Result<Vec<crate::plan::IndexOption>, PlanError> {
    if !with_opts.is_empty() {
        // Index options are not durable.
        scx.require_unsafe_mode("INDEX OPTIONS")?;
    }

    let IndexOptionExtracted {
        logical_compaction_window,
        ..
    }: IndexOptionExtracted = with_opts.try_into()?;

    let mut out = Vec::with_capacity(1);

    if let Some(OptionalInterval(lcw)) = logical_compaction_window {
        scx.require_unsafe_mode("LOGICAL COMPACTION WINDOW")?;
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
    let index_name = normalize::unresolved_object_name(index_name)?;
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
            "{} is a {} not a index",
            scx.catalog.resolve_full_name(entry.name()),
            entry.item_type()
        )
    }
    let id = entry.id();

    match actions {
        AlterIndexAction::ResetOptions(options) => {
            Ok(Plan::AlterIndexResetOptions(AlterIndexResetOptionsPlan {
                id,
                options: options.into_iter().collect::<HashSet<IndexOptionName>>(),
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
    let name = normalize::unresolved_object_name(name)?;
    match scx.catalog.resolve_item(&name) {
        Ok(entry) => {
            let full_name = scx.catalog.resolve_full_name(entry.name());
            let item_type = entry.item_type();

            // Return a more helpful error on `ALTER VIEW <materialized-view>`.
            if object_type == ObjectType::View && item_type == CatalogItemType::MaterializedView {
                return Err(PlanError::AlterViewOnMaterializedView(
                    full_name.to_string(),
                ));
            } else if object_type != item_type {
                sql_bail!(
                    "{} is a {} not a {}",
                    full_name,
                    entry.item_type(),
                    object_type
                )
            }
            let proposed_name = QualifiedObjectName {
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
        Err(_) if if_exists => {
            // TODO(benesch/jkosh44): generate a notice indicating this
            // item does not exist.
            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
        Err(e) => Err(e.into()),
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
    let name = normalize::unresolved_object_name(name)?;
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
            "{} is a {} not a SECRET",
            scx.catalog.resolve_full_name(entry.name()),
            entry.item_type()
        )
    }
    let id = entry.id();
    let secret_as = query::plan_secret_as(scx, value)?;

    Ok(Plan::AlterSecret(AlterSecretPlan { id, secret_as }))
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
    let source_name = normalize::unresolved_object_name(source_name)?;
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
            "{} is a {} not a source",
            scx.catalog.resolve_full_name(entry.name()),
            entry.item_type()
        )
    }
    let id = entry.id();

    let mut size = AlterSourceItem::Unchanged;
    let mut remote = AlterSourceItem::Unchanged;
    match action {
        AlterSourceAction::SetOptions(options) => {
            let CreateSourceOptionExtracted {
                seen: _,
                remote: remote_opt,
                size: size_opt,
                timeline: timeline_opt,
                timestamp_interval: timestamp_interval_opt,
                ignore_keys: ignore_keys_opt,
            } = CreateSourceOptionExtracted::try_from(options)?;

            if let Some(value) = remote_opt {
                remote = AlterSourceItem::Set(value);
            }
            if let Some(value) = size_opt {
                size = AlterSourceItem::Set(value);
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
        }
        AlterSourceAction::ResetOptions(reset) => {
            for name in reset {
                match name {
                    CreateSourceOptionName::Remote => {
                        remote = AlterSourceItem::Reset;
                    }
                    CreateSourceOptionName::Size => {
                        size = AlterSourceItem::Reset;
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
                }
            }
        }
    };

    Ok(Plan::AlterSource(AlterSourcePlan { id, size, remote }))
}

pub fn describe_alter_system_set(
    _: &StatementContext,
    _: AlterSystemSetStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_system_set(
    _: &StatementContext,
    AlterSystemSetStatement { name, value }: AlterSystemSetStatement,
) -> Result<Plan, PlanError> {
    let name = name.to_string();
    if matches!(&value, SetVariableValue::Literal(value) if matches!(value, mz_sql_parser::ast::Value::Null))
    {
        sql_bail!("Unable to set system configuration '{}' to NULL", name)
    }
    Ok(Plan::AlterSystemSet(AlterSystemSetPlan { name, value }))
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
    let name = normalize::unresolved_object_name(name)?;
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
