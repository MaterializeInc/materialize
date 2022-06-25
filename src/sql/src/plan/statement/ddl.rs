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

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::Duration;

use anyhow::{anyhow, bail};
use aws_arn::ARN;
use chrono::{NaiveDate, NaiveDateTime};
use globset::GlobBuilder;
use itertools::Itertools;
use prost::Message;
use regex::Regex;
use tracing::{debug, warn};

use mz_dataflow_types::connections::{Connection, KafkaConnection};
use mz_dataflow_types::sinks::{
    KafkaSinkConnectionBuilder, KafkaSinkConnectionRetention, KafkaSinkFormat,
    PersistSinkConnectionBuilder, SinkConnectionBuilder, SinkEnvelope,
};
use mz_dataflow_types::sources::encoding::{
    included_column_desc, AvroEncoding, ColumnSpec, CsvEncoding, DataEncoding, DataEncodingInner,
    ProtobufEncoding, RegexEncoding, SourceDataEncoding, SourceDataEncodingInner,
};
use mz_dataflow_types::sources::{
    provide_default_metadata, DebeziumDedupProjection, DebeziumEnvelope, DebeziumMode,
    DebeziumSourceProjection, DebeziumTransactionMetadata, ExternalSourceConnection,
    IncludedColumnPos, KafkaSourceConnection, KeyEnvelope, KinesisSourceConnection, MzOffset,
    PostgresSourceConnection, PostgresSourceDetails, ProtoPostgresSourceDetails,
    PubNubSourceConnection, S3SourceConnection, SourceConnection, SourceEnvelope, Timeline,
    UnplannedSourceEnvelope, UpsertStyle,
};
use mz_expr::CollectionPlan;
use mz_interchange::avro::{self, AvroSchemaGenerator};
use mz_ore::collections::CollectionExt;
use mz_ore::str::StrExt;
use mz_postgres_util::desc::PostgresTableDesc;
use mz_repr::adt::interval::Interval;
use mz_repr::proto::RustType;
use mz_repr::strconv;
use mz_repr::{ColumnName, GlobalId, RelationDesc, RelationType, ScalarType};

use crate::ast::display::AstDisplay;
use crate::ast::{
    AlterIndexAction, AlterIndexStatement, AlterObjectRenameStatement, AlterSecretStatement,
    AvroSchema, AvroSchemaOption, AvroSchemaOptionName, ClusterOption, ColumnOption, Compression,
    CreateClusterReplicaStatement, CreateClusterStatement, CreateConnection,
    CreateConnectionStatement, CreateDatabaseStatement, CreateIndexStatement, CreateRoleOption,
    CreateRoleStatement, CreateSchemaStatement, CreateSecretStatement, CreateSinkConnection,
    CreateSinkStatement, CreateSourceConnection, CreateSourceFormat, CreateSourceStatement,
    CreateTableStatement, CreateTypeAs, CreateTypeStatement, CreateViewStatement,
    CreateViewsSourceTarget, CreateViewsStatement, CsrConnection, CsrConnectionAvro,
    CsrConnectionProto, CsrSeedCompiled, CsrSeedCompiledOrLegacy, CsvColumns, DbzMode,
    DbzTxMetadataOption, DropClusterReplicasStatement, DropClustersStatement,
    DropDatabaseStatement, DropObjectsStatement, DropRolesStatement, DropSchemaStatement, Envelope,
    Expr, Format, Ident, IfExistsBehavior, IndexOption, IndexOptionName, KafkaConsistency,
    KeyConstraint, ObjectType, Op, ProtobufSchema, QualifiedReplica, Query, ReplicaDefinition,
    ReplicaOption, ReplicaOptionName, Select, SelectItem, SetExpr, SourceIncludeMetadata,
    SourceIncludeMetadataType, Statement, SubscriptPosition, TableConstraint, TableFactor,
    TableWithJoins, UnresolvedDatabaseName, UnresolvedObjectName, Value, ViewDefinition,
    WithOptionValue,
};
use crate::catalog::{CatalogItem, CatalogItemType, CatalogType, CatalogTypeDetails};
use crate::kafka_util;
use crate::names::{
    self, Aug, FullSchemaName, QualifiedObjectName, RawDatabaseSpecifier, ResolvedClusterName,
    ResolvedDataType, ResolvedDatabaseSpecifier, ResolvedObjectName, SchemaSpecifier,
};
use crate::normalize::ident;
use crate::normalize::{self, SqlValueOrSecret};
use crate::plan::error::PlanError;
use crate::plan::query::QueryLifetime;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::with_options::{OptionalInterval, TryFromValue};
use crate::plan::{
    plan_utils, query, AlterIndexResetOptionsPlan, AlterIndexSetOptionsPlan, AlterItemRenamePlan,
    AlterNoopPlan, AlterSecretPlan, ComputeInstanceIntrospectionConfig, CreateComputeInstancePlan,
    CreateComputeInstanceReplicaPlan, CreateConnectionPlan, CreateDatabasePlan, CreateIndexPlan,
    CreateRolePlan, CreateSchemaPlan, CreateSecretPlan, CreateSinkPlan, CreateSourcePlan,
    CreateTablePlan, CreateTypePlan, CreateViewPlan, CreateViewsPlan,
    DropComputeInstanceReplicaPlan, DropComputeInstancesPlan, DropDatabasePlan, DropItemsPlan,
    DropRolesPlan, DropSchemaPlan, Index, Params, Plan, ReplicaConfig, Secret, Sink, Source, Table,
    Type, View,
};

pub fn describe_create_database(
    _: &StatementContext,
    _: CreateDatabaseStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_database(
    _: &StatementContext,
    CreateDatabaseStatement {
        name,
        if_not_exists,
    }: CreateDatabaseStatement,
) -> Result<Plan, anyhow::Error> {
    Ok(Plan::CreateDatabase(CreateDatabasePlan {
        name: normalize::ident(name.0),
        if_not_exists,
    }))
}

pub fn describe_create_schema(
    _: &StatementContext,
    _: CreateSchemaStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_schema(
    scx: &StatementContext,
    CreateSchemaStatement {
        mut name,
        if_not_exists,
    }: CreateSchemaStatement,
) -> Result<Plan, anyhow::Error> {
    if name.0.len() > 2 {
        bail!("schema name {} has more than two components", name);
    }
    let schema_name = normalize::ident(
        name.0
            .pop()
            .expect("names always have at least one component"),
    );
    let database_spec = match name.0.pop() {
        None => match scx.catalog.active_database() {
            Some(id) => ResolvedDatabaseSpecifier::Id(id.clone()),
            None => bail!("no database specified and no active database"),
        },
        Some(n) => match scx.resolve_database(&UnresolvedDatabaseName(n.clone())) {
            Ok(database) => ResolvedDatabaseSpecifier::Id(database.id()),
            Err(_) => bail!("invalid database {}", n.as_str()),
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
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_table(
    scx: &StatementContext,
    stmt: CreateTableStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let CreateTableStatement {
        name,
        columns,
        constraints,
        with_options,
        if_not_exists,
        temporary,
    } = &stmt;

    if !with_options.is_empty() {
        bail_unsupported!("WITH options");
    }

    let names: Vec<_> = columns
        .iter()
        .map(|c| normalize::column_name(c.name.clone()))
        .collect();

    if let Some(dup) = names.iter().duplicates().next() {
        bail!("column {} specified more than once", dup.as_str().quoted());
    }

    // Build initial relation type that handles declared data types
    // and NOT NULL constraints.
    let mut column_types = Vec::with_capacity(columns.len());
    let mut defaults = Vec::with_capacity(columns.len());
    let mut keys = Vec::new();

    for (i, c) in columns.into_iter().enumerate() {
        let aug_data_type = &c.data_type;
        let ty = query::scalar_type_from_sql(scx, &aug_data_type)?;
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
                        None => bail!("unknown column in constraint: {}", column),
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

    let create_sql = normalize::create_statement(&scx, Statement::CreateTable(stmt.clone()))?;
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
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_source(
    scx: &StatementContext,
    stmt: CreateSourceStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let CreateSourceStatement {
        name,
        col_names,
        connection,
        with_options,
        envelope,
        if_not_exists,
        materialized,
        format,
        key_constraint,
        include_metadata,
    } = &stmt;

    let envelope = envelope.clone().unwrap_or(Envelope::None);

    let with_options_original = with_options;
    let mut with_options = normalize::options(with_options_original)?;

    let ts_frequency = match with_options.remove("timestamp_frequency_ms") {
        Some(val) => match val.into() {
            Some(Value::Number(n)) => match n.parse::<u64>() {
                Ok(n) => Duration::from_millis(n),
                Err(_) => bail!("timestamp_frequency_ms must be an u64"),
            },
            _ => bail!("timestamp_frequency_ms must be an u64"),
        },
        None => scx.catalog.config().timestamp_frequency,
    };

    if !matches!(connection, CreateSourceConnection::Kafka { .. })
        && include_metadata
            .iter()
            .any(|sic| sic.ty == SourceIncludeMetadataType::Headers)
    {
        // TODO(guswynn): should this be `bail_unsupported!`?
        bail!("INCLUDE HEADERS with non-Kafka sources not supported");
    }
    if !matches!(connection, CreateSourceConnection::Kafka { .. }) && !include_metadata.is_empty() {
        bail_unsupported!("INCLUDE metadata with non-Kafka sources");
    }

    let (external_connection, encoding) = match connection {
        CreateSourceConnection::Kafka(kafka) => {
            let kafka_connection = match &kafka.connection {
                mz_sql_parser::ast::KafkaConnection::Inline { broker } => KafkaConnection {
                    broker: broker.parse()?,
                    options: kafka_util::extract_config(&mut with_options)?,
                },
                mz_sql_parser::ast::KafkaConnection::Reference { connection, .. } => {
                    let item = scx.get_item_by_resolved_name(&connection)?;
                    match item.connection()? {
                        Connection::Kafka(connection) => connection.clone(),
                        _ => bail!("{} is not a kafka connection", item.name()),
                    }
                }
            };

            let group_id_prefix = match with_options.remove("group_id_prefix") {
                None => None,
                Some(SqlValueOrSecret::Value(Value::String(s))) => Some(s),
                Some(_) => bail!("group_id_prefix must be a string"),
            };

            let parse_offset = |s: &str| match s.parse::<i64>() {
                // we parse an i64 here, because we don't yet support u64's in sql,
                // but put it into an internal MzOffset that holds a u64
                // TODO: make this an native u64 when https://github.com/MaterializeInc/materialize/issues/7629
                // is resolved.
                Ok(n) if n >= 0 => Ok(MzOffset {
                    offset: n.try_into().unwrap(),
                }),
                _ => bail!("start_offset must be a nonnegative integer"),
            };

            let mut start_offsets = HashMap::new();
            match with_options.remove("start_offset") {
                None => {
                    start_offsets.insert(0, MzOffset::from(0));
                }
                Some(SqlValueOrSecret::Value(Value::Number(n))) => {
                    start_offsets.insert(0, parse_offset(&n)?);
                }
                Some(SqlValueOrSecret::Value(Value::Array(vs))) => {
                    for (i, v) in vs.iter().enumerate() {
                        match v {
                            Value::Number(n) => {
                                start_offsets.insert(i32::try_from(i)?, parse_offset(n)?);
                            }
                            _ => bail!("start_offset value must be a number: {}", v),
                        }
                    }
                }
                Some(v) => bail!("invalid start_offset value: {}", v),
            }

            let encoding = get_encoding(scx, format, &envelope, &connection)?;

            let mut connection = KafkaSourceConnection {
                connection: kafka_connection,
                topic: kafka.topic.clone(),
                start_offsets,
                group_id_prefix,
                cluster_id: scx.catalog.config().cluster_id,
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
                bail!("INCLUDE HEADERS requires ENVELOPE UPSERT or no ENVELOPE");
            }

            if !include_metadata.is_empty()
                && matches!(envelope, Envelope::Debezium(DbzMode::Plain { .. }))
            {
                for kind in include_metadata {
                    if !matches!(kind.ty, SourceIncludeMetadataType::Key) {
                        bail!(
                            "INCLUDE {} with Debezium requires UPSERT semantics",
                            kind.ty
                        );
                    }
                }
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

            let connection = ExternalSourceConnection::Kafka(connection);

            (connection, encoding)
        }
        CreateSourceConnection::Kinesis { arn, .. } => {
            scx.require_unsafe_mode("CREATE SOURCE ... FROM KINESIS")?;
            let arn: ARN = arn
                .parse()
                .map_err(|e| anyhow!("Unable to parse provided ARN: {:#?}", e))?;
            let stream_name = match arn.resource.strip_prefix("stream/") {
                Some(path) => path.to_owned(),
                _ => bail!(
                    "Unable to parse stream name from resource path: {}",
                    arn.resource
                ),
            };

            let region = arn
                .region
                .ok_or_else(|| anyhow!("Provided ARN does not include an AWS region"))?;

            let aws = normalize::aws_config(&mut with_options, Some(region.into()))?;
            let encoding = get_encoding(scx, format, &envelope, &connection)?;
            let connection =
                ExternalSourceConnection::Kinesis(KinesisSourceConnection { stream_name, aws });
            (connection, encoding)
        }
        CreateSourceConnection::S3 {
            key_sources,
            pattern,
            compression,
        } => {
            scx.require_unsafe_mode("CREATE SOURCE ... FROM S3")?;
            let aws = normalize::aws_config(&mut with_options, None)?;
            let mut converted_sources = Vec::new();
            for ks in key_sources {
                let dtks = match ks {
                    mz_sql_parser::ast::S3KeySource::Scan { bucket } => {
                        mz_dataflow_types::sources::S3KeySource::Scan {
                            bucket: bucket.clone(),
                        }
                    }
                    mz_sql_parser::ast::S3KeySource::SqsNotifications { queue } => {
                        mz_dataflow_types::sources::S3KeySource::SqsNotifications {
                            queue: queue.clone(),
                        }
                    }
                };
                converted_sources.push(dtks);
            }
            let encoding = get_encoding(scx, format, &envelope, &connection)?;
            if matches!(encoding, SourceDataEncoding::KeyValue { .. }) {
                bail!("S3 sources do not support key decoding");
            }
            let connection = ExternalSourceConnection::S3(S3SourceConnection {
                key_sources: converted_sources,
                pattern: pattern
                    .as_ref()
                    .map(|p| {
                        GlobBuilder::new(p)
                            .literal_separator(true)
                            .backslash_escape(true)
                            .build()
                    })
                    .transpose()?,
                aws,
                compression: match compression {
                    Compression::Gzip => mz_dataflow_types::sources::Compression::Gzip,
                    Compression::None => mz_dataflow_types::sources::Compression::None,
                },
            });
            (connection, encoding)
        }
        CreateSourceConnection::Postgres {
            conn,
            publication,
            details,
        } => {
            let details = details
                .as_ref()
                .ok_or_else(|| anyhow!("internal error: Postgres source missing details"))?;
            let details = ProtoPostgresSourceDetails::decode(&*hex::decode(details)?)?;
            let connection = ExternalSourceConnection::Postgres(PostgresSourceConnection {
                conn: conn.clone(),
                publication: publication.clone(),
                details: PostgresSourceDetails::from_proto(details)?,
            });

            let encoding =
                SourceDataEncoding::Single(DataEncoding::new(DataEncodingInner::Postgres));
            (connection, encoding)
        }
        CreateSourceConnection::PubNub {
            subscribe_key,
            channel,
        } => {
            match format {
                CreateSourceFormat::None | CreateSourceFormat::Bare(Format::Text) => (),
                _ => bail!("CREATE SOURCE ... PUBNUB must specify FORMAT TEXT"),
            }
            let connection = ExternalSourceConnection::PubNub(PubNubSourceConnection {
                subscribe_key: subscribe_key.clone(),
                channel: channel.clone(),
            });
            (
                connection,
                SourceDataEncoding::Single(DataEncoding::new(DataEncodingInner::Text)),
            )
        }
    };
    let (key_desc, value_desc) = encoding.desc()?;

    let key_envelope = get_key_envelope(include_metadata, &envelope, &encoding)?;

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
        mz_sql_parser::ast::Envelope::None => {
            UnplannedSourceEnvelope::None(key_envelope.unwrap_or(KeyEnvelope::None))
        }
        mz_sql_parser::ast::Envelope::DifferentialRow => {
            assert!(key_envelope.is_none());
            UnplannedSourceEnvelope::DifferentialRow
        }
        mz_sql_parser::ast::Envelope::Debezium(mode) => {
            //TODO check that key envelope is not set
            let (before_idx, after_idx) = typecheck_debezium(&value_desc)?;

            match mode {
                DbzMode::Upsert => {
                    UnplannedSourceEnvelope::Upsert(UpsertStyle::Debezium { after_idx })
                }
                DbzMode::Plain { tx_metadata } => {
                    // TODO(#11668): Probably make this not a WITH option and integrate into the DBZ envelope?
                    let mut tx_metadata_source = None;
                    let mut tx_metadata_collection = None;

                    if !tx_metadata.is_empty() {
                        scx.require_unsafe_mode("ENVELOPE DEBEZIUM ... TRANSACTION METADATA")?;
                    }

                    for option in tx_metadata {
                        match option {
                            DbzTxMetadataOption::Source(source) => {
                                if tx_metadata_source.is_some() {
                                    bail!("TRANSACTION METADATA SOURCE specified more than once")
                                }
                                let item = scx.get_item_by_resolved_name(source)?;
                                if item.item_type() != CatalogItemType::Source {
                                    bail!(
                                        "provided TRANSACTION METADATA SOURCE {} is not a source",
                                        source.full_name_str(),
                                    );
                                }
                                tx_metadata_source = Some(item);
                            }
                            DbzTxMetadataOption::Collection(data_collection) => {
                                if tx_metadata_collection.is_some() {
                                    bail!(
                                        "TRANSACTION METADATA COLLECTION specified more than once"
                                    );
                                }
                                tx_metadata_collection =
                                    Some(String::try_from_value(data_collection.clone())?);
                            }
                        }
                    }

                    let tx_metadata = match (tx_metadata_source, tx_metadata_collection) {
                        (None, None) => None,
                        (Some(source), Some(collection)) => {
                            Some(typecheck_debezium_transaction_metadata(
                                scx,
                                source,
                                &value_desc,
                                collection,
                            )?)
                        }
                        _ => {
                            bail!(
                                "TRANSACTION METADATA requires both SOURCE and COLLECTION options"
                            );
                        }
                    };

                    let dedup_projection = typecheck_debezium_dedup(&value_desc, tx_metadata);

                    let dedup_mode = match with_options.remove("deduplication") {
                        None => match dedup_projection {
                            Ok(_) => Cow::from("ordered"),
                            Err(_) => Cow::from("none"),
                        },
                        Some(SqlValueOrSecret::Value(Value::String(s))) => Cow::from(s),
                        _ => bail!("deduplication option must be a string"),
                    };

                    match dedup_mode.as_ref() {
                        "ordered" => UnplannedSourceEnvelope::Debezium(DebeziumEnvelope {
                            before_idx,
                            after_idx,
                            mode: DebeziumMode::Ordered(dedup_projection?),
                        }),
                        "full" => UnplannedSourceEnvelope::Debezium(DebeziumEnvelope {
                            before_idx,
                            after_idx,
                            mode: DebeziumMode::Full(dedup_projection?),
                        }),
                        "none" => UnplannedSourceEnvelope::Debezium(DebeziumEnvelope {
                            before_idx,
                            after_idx,
                            mode: DebeziumMode::None,
                        }),
                        "full_in_range" => {
                            let parse_datetime = |s: &str| {
                                let formats = ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"];
                                for format in formats {
                                    if let Ok(dt) = NaiveDateTime::parse_from_str(s, format) {
                                        return Ok(dt);
                                    }
                                }
                                if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                                    return Ok(d.and_hms(0, 0, 0));
                                }

                                bail!(
                                    "UTC DateTime specifier '{}' should match \
                                    'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS' \
                                    or 'YYYY-MM-DD HH:MM:SS.FF",
                                    s
                                )
                            };

                            let dedup_start = match with_options.remove("deduplication_start") {
                                None => None,
                                Some(SqlValueOrSecret::Value(Value::String(start))) => Some(parse_datetime(&start)?),
                                _ => bail!("deduplication_start option must be a string"),
                            };

                            let dedup_end = match with_options.remove("deduplication_end") {
                                None => None,
                                Some(SqlValueOrSecret::Value(Value::String(end))) => Some(parse_datetime(&end)?),
                                _ => bail!("deduplication_end option must be a string"),
                            };

                            match dedup_start.zip(dedup_end) {
                                Some((start, end)) => {
                                    if start >= end {
                                        bail!(
                                            "Debezium deduplication start {} is not before end {}",
                                            start,
                                            end
                                        );
                                    }

                                    let pad_start =
                                        match with_options.remove("deduplication_pad_start") {
                                            None => None,
                                            Some(SqlValueOrSecret::Value(Value::String(pad_start))) => {
                                                Some(parse_datetime(&pad_start)?)
                                            }
                                            _ => bail!(
                                                "deduplication_pad_start option must be a string"
                                            ),
                                        };

                                    UnplannedSourceEnvelope::Debezium(DebeziumEnvelope {
                                        before_idx,
                                        after_idx,
                                        mode: DebeziumMode::FullInRange {
                                            start,
                                            end,
                                            pad_start,
                                            projection: dedup_projection?,
                                        }
                                    })
                                }
                                _ => bail!(
                                    "deduplication full_in_range requires both \
                                 'deduplication_start' and 'deduplication_end' parameters"
                                ),
                            }
                        }
                        _ => bail!(
                            "deduplication must be one of 'none', 'ordered', 'full' or 'full_in_range'."
                        ),
                    }
                }
            }
        }
        mz_sql_parser::ast::Envelope::Upsert => {
            if encoding.key_ref().is_none() {
                bail_unsupported!(format!("upsert requires a key/value format: {:?}", format));
            }
            //TODO(petrosagg): remove this check. it will be a breaking change
            let key_envelope = match encoding.key_ref() {
                Some(DataEncoding {
                    inner: DataEncodingInner::Avro(_),
                    ..
                }) => key_envelope.unwrap_or(KeyEnvelope::Flattened),
                _ => key_envelope.unwrap_or(KeyEnvelope::LegacyUpsert),
            };
            UnplannedSourceEnvelope::Upsert(UpsertStyle::Default(key_envelope))
        }
        mz_sql_parser::ast::Envelope::CdcV2 => {
            //TODO check that key envelope is not set
            match format {
                CreateSourceFormat::Bare(Format::Avro(_)) => {}
                _ => bail_unsupported!("non-Avro-encoded ENVELOPE MATERIALIZE"),
            }
            UnplannedSourceEnvelope::CdcV2
        }
    };

    // TODO(petrosagg): remove this inconsistency once INCLUDE (offset) syntax is implemented
    let include_defaults = provide_default_metadata(&envelope, encoding.value_ref());
    let metadata_columns = external_connection.metadata_columns(include_defaults);
    let metadata_column_types = external_connection.metadata_column_types(include_defaults);
    let metadata_desc = included_column_desc(metadata_columns.clone());
    let (envelope, mut desc) = envelope.desc(key_desc, value_desc, metadata_desc)?;

    // Append default metadata columns if column aliases were provided but do not include them.
    //
    // This is a confusing hack due to two combined facts:
    //
    // * we used to not allow users to refer to/alias the metadata columns because they were
    //   specified in render, instead of here in plan
    // * we don't follow postgres semantics and allow a shorter rename list than total column list
    //
    // TODO: probably we should just migrate to pg semantics and allow specifying fewer columns than
    // actually exist?
    let tmp_col;
    let col_names = if include_defaults
        && !col_names.is_empty()
        && metadata_columns.len() + col_names.len() == desc.arity()
    {
        let mut tmp = Vec::with_capacity(desc.arity());
        tmp.extend(col_names.iter().cloned());
        tmp.push(Ident::from(
            external_connection.default_metadata_column_name().unwrap(),
        ));
        tmp_col = tmp;
        &tmp_col
    } else {
        col_names
    };

    let ignore_source_keys = match with_options.remove("ignore_source_keys") {
        None => false,
        Some(SqlValueOrSecret::Value(Value::Boolean(b))) => b,
        Some(_) => bail!("ignore_source_keys must be a boolean"),
    };

    if ignore_source_keys {
        desc = desc.without_keys();
    }

    desc = plan_utils::maybe_rename_columns(format!("source {}", name), desc, &col_names)?;

    let names: Vec<_> = desc.iter_names().cloned().collect();
    if let Some(dup) = names.iter().duplicates().next() {
        bail!("column {} specified more than once", dup.as_str().quoted());
    }

    // Apply user-specified key constraint
    if let Some(KeyConstraint::PrimaryKeyNotEnforced { columns }) = key_constraint.clone() {
        let key_columns = columns
            .into_iter()
            .map(normalize::column_name)
            .collect::<Vec<_>>();

        let mut uniq = HashSet::new();
        for col in key_columns.iter() {
            if !uniq.insert(col) {
                bail!("Repeated column name in source key constraint: {}", col);
            }
        }

        let key_indices = key_columns
            .iter()
            .map(|col| -> anyhow::Result<usize> {
                let name_idx = desc
                    .get_by_name(col)
                    .map(|(idx, _type)| idx)
                    .ok_or_else(|| anyhow!("No such column in source key constraint: {}", col))?;
                if desc.get_unambiguous_name(name_idx).is_none() {
                    bail!("Ambiguous column in source key constraint: {}", col);
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

    let if_not_exists = *if_not_exists;
    let materialized = *materialized;
    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name.clone())?)?;
    let create_sql = normalize::create_statement(&scx, Statement::CreateSource(stmt))?;

    // Allow users to specify a timeline. If they do not, determine a default timeline for the source.
    let timeline = if let Some(timeline) = with_options.remove("timeline") {
        match timeline.into() {
            Some(Value::String(timeline)) => Timeline::User(timeline),
            Some(v) => bail!("unsupported timeline value {}", v.to_ast_string()),
            None => bail!("unsupported timeline value: secret"),
        }
    } else {
        match envelope {
            SourceEnvelope::CdcV2 => match with_options.remove("epoch_ms_timeline") {
                None => Timeline::External(name.to_string()),
                Some(SqlValueOrSecret::Value(Value::Boolean(true))) => Timeline::EpochMilliseconds,
                Some(v) => bail!("unsupported epoch_ms_timeline value {}", v),
            },
            _ => Timeline::EpochMilliseconds,
        }
    };

    let source = Source {
        create_sql,
        connection: SourceConnection::External {
            connection: external_connection,
            encoding,
            envelope,
            metadata_columns: metadata_column_types,
            ts_frequency,
            timeline,
        },
        desc,
    };

    normalize::ensure_empty_options(&with_options, "CREATE SOURCE")?;

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        materialized,
    }))
}

fn typecheck_debezium(value_desc: &RelationDesc) -> Result<(usize, usize), anyhow::Error> {
    let (before_idx, before_ty) = value_desc
        .get_by_name(&"before".into())
        .ok_or_else(|| anyhow!("'before' column missing from debezium input"))?;
    let (after_idx, after_ty) = value_desc
        .get_by_name(&"after".into())
        .ok_or_else(|| anyhow!("'after' column missing from debezium input"))?;
    if !matches!(before_ty.scalar_type, ScalarType::Record { .. }) {
        bail!("'before' column must be of type record");
    }
    if before_ty != after_ty {
        bail!("'before' type differs from 'after' column");
    }
    Ok((before_idx, after_idx))
}

fn typecheck_debezium_dedup(
    value_desc: &RelationDesc,
    tx_metadata: Option<DebeziumTransactionMetadata>,
) -> Result<DebeziumDedupProjection, anyhow::Error> {
    let (op_idx, op_ty) = value_desc
        .get_by_name(&"op".into())
        .ok_or_else(|| anyhow!("'op' column missing from debezium input"))?;
    if op_ty.scalar_type != ScalarType::String {
        bail!("'op' column must be of type string");
    };

    let (source_idx, source_ty) = value_desc
        .get_by_name(&"source".into())
        .ok_or_else(|| anyhow!("'source' column missing from debezium input"))?;

    let source_fields = match &source_ty.scalar_type {
        ScalarType::Record { fields, .. } => fields,
        _ => bail!("'source' column must be of type record"),
    };

    let snapshot = source_fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.0.as_str() == "snapshot");
    let snapshot_idx = match snapshot {
        Some((idx, (_, ty))) => match &ty.scalar_type {
            ScalarType::String | ScalarType::Bool => idx,
            _ => bail!("'snapshot' column must be a string or boolean"),
        },
        None => bail!("'snapshot' field missing from source record"),
    };

    let mut mysql = (None, None, None);
    let mut postgres = (None, None);
    let mut sqlserver = (None, None);

    for (idx, (name, ty)) in source_fields.iter().enumerate() {
        match name.as_str() {
            "file" => {
                mysql.0 = match &ty.scalar_type {
                    ScalarType::String => Some(idx),
                    t => bail!(r#""source"."file" must be of type string, found {:?}"#, t),
                }
            }
            "pos" => {
                mysql.1 = match &ty.scalar_type {
                    ScalarType::Int64 => Some(idx),
                    t => bail!(r#""source"."pos" must be of type bigint, found {:?}"#, t),
                }
            }
            "row" => {
                mysql.2 = match &ty.scalar_type {
                    ScalarType::Int32 => Some(idx),
                    t => bail!(r#""source"."file" must be of type int, found {:?}"#, t),
                }
            }
            "sequence" => {
                postgres.0 = match &ty.scalar_type {
                    ScalarType::String => Some(idx),
                    t => bail!(
                        r#""source"."sequence" must be of type string, found {:?}"#,
                        t
                    ),
                }
            }
            "lsn" => {
                postgres.1 = match &ty.scalar_type {
                    ScalarType::Int64 => Some(idx),
                    t => bail!(r#""source"."lsn" must be of type bigint, found {:?}"#, t),
                }
            }
            "change_lsn" => {
                sqlserver.0 = match &ty.scalar_type {
                    ScalarType::String => Some(idx),
                    t => bail!(
                        r#""source"."change_lsn" must be of type string, found {:?}"#,
                        t
                    ),
                }
            }
            "event_serial_no" => {
                sqlserver.1 = match &ty.scalar_type {
                    ScalarType::Int64 => Some(idx),
                    t => bail!(
                        r#""source"."event_serial_no" must be of type bigint, found {:?}"#,
                        t
                    ),
                }
            }
            _ => {}
        }
    }

    let source_projection = if let (Some(file), Some(pos), Some(row)) = mysql {
        DebeziumSourceProjection::MySql { file, pos, row }
    } else if let (Some(change_lsn), Some(event_serial_no)) = sqlserver {
        DebeziumSourceProjection::SqlServer {
            change_lsn,
            event_serial_no,
        }
    } else if let (Some(sequence), Some(lsn)) = postgres {
        DebeziumSourceProjection::Postgres { sequence, lsn }
    } else {
        bail!("unknown type of upstream database")
    };

    let (transaction_idx, _transaction_ty) = value_desc
        .get_by_name(&"transaction".into())
        .ok_or_else(|| anyhow!("'transaction' column missing from debezium input"))?;

    Ok(DebeziumDedupProjection {
        op_idx,
        source_idx,
        snapshot_idx,
        source_projection,
        transaction_idx,
        tx_metadata,
    })
}

fn typecheck_debezium_transaction_metadata(
    scx: &StatementContext,
    tx_metadata_source: &dyn CatalogItem,
    data_value_desc: &RelationDesc,
    tx_data_collection_name: String,
) -> Result<DebeziumTransactionMetadata, anyhow::Error> {
    let tx_value_desc =
        tx_metadata_source.desc(&scx.catalog.resolve_full_name(tx_metadata_source.name()))?;
    let (tx_status_idx, tx_status_ty) = tx_value_desc
        .get_by_name(&"status".into())
        .ok_or_else(|| anyhow!("'status' column missing from debezium transaction metadata"))?;
    let (tx_transaction_id_idx, tx_transaction_id_ty) = tx_value_desc
        .get_by_name(&"id".into())
        .ok_or_else(|| anyhow!("'id' column missing from debezium transaction metadata"))?;
    let (tx_data_collections_idx, tx_data_collections_ty) = tx_value_desc
        .get_by_name(&"data_collections".into())
        .ok_or_else(|| {
            anyhow!("'data_collections' column missing from debezium transaction metadata")
        })?;
    if tx_status_ty != &ScalarType::String.nullable(false) {
        bail!("'status' column must be of type non-nullable string");
    }
    if tx_transaction_id_ty != &ScalarType::String.nullable(false) {
        bail!("'id' column must be of type non-nullable string");
    }

    // Don't care about nullability of data_collections or subtypes
    let (tx_data_collections_data_collection, tx_data_collections_event_count) =
        match tx_data_collections_ty.scalar_type {
            ScalarType::Array(ref element_type)
            | ScalarType::List {
                ref element_type, ..
            } => match **element_type {
                ScalarType::Record { ref fields, .. } => {
                    let data_collections_data_collection = fields
                        .iter()
                        .enumerate()
                        .find(|(_, f)| f.0.as_str() == "data_collection");
                    let data_collections_event_count = fields
                        .iter()
                        .enumerate()
                        .find(|(_, f)| f.0.as_str() == "event_count");
                    (
                        data_collections_data_collection,
                        data_collections_event_count,
                    )
                }
                _ => bail!("'data_collections' array must contain records"),
            },
            _ => bail!("'data_collections' column must be of array or list type",),
        };

    let tx_data_collections_data_collection_idx = match tx_data_collections_data_collection {
        Some((idx, (_, ty))) => match ty.scalar_type {
            ScalarType::String => idx,
            _ => bail!("'data_collections.data_collection' must be of type string"),
        },
        _ => bail!("'data_collections.data_collection' missing from debezium transaction metadata"),
    };

    let tx_data_collections_event_count_idx = match tx_data_collections_event_count {
        Some((idx, (_, ty))) => match ty.scalar_type {
            ScalarType::Int16 | ScalarType::Int32 | ScalarType::Int64 => idx,
            _ => bail!("'data_collections.event_count' must be of type string"),
        },
        _ => bail!("'data_collections.event_count' missing from debezium transaction metadata"),
    };

    let (_data_transaction_idx, data_transaction_ty) = data_value_desc
        .get_by_name(&"transaction".into())
        .ok_or_else(|| anyhow!("'transaction' column missing from debezium input"))?;

    let data_transaction_id = match &data_transaction_ty.scalar_type {
        ScalarType::Record { fields, .. } => fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.0.as_str() == "id"),
        _ => bail!("'transaction' column must be of type record"),
    };

    let data_transaction_id_idx = match data_transaction_id {
        Some((idx, (_, ty))) => match &ty.scalar_type {
            ScalarType::String => idx,
            _ => bail!("'transaction.id' column must be of type string"),
        },
        None => bail!("'transaction.id' column missing from debezium input"),
    };

    Ok(DebeziumTransactionMetadata {
        tx_metadata_global_id: tx_metadata_source.id(),
        tx_status_idx,
        tx_transaction_id_idx,
        tx_data_collections_idx,
        tx_data_collections_data_collection_idx,
        tx_data_collections_event_count_idx,
        tx_data_collection_name,
        data_transaction_id_idx,
    })
}

fn get_encoding(
    scx: &StatementContext,
    format: &CreateSourceFormat<Aug>,
    envelope: &Envelope<Aug>,
    connection: &CreateSourceConnection<Aug>,
) -> Result<SourceDataEncoding, anyhow::Error> {
    let encoding = match format {
        CreateSourceFormat::None => bail!("Source format must be specified"),
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
        Envelope::Debezium(DbzMode::Upsert) | Envelope::Upsert
    );
    let is_keyvalue = matches!(encoding, SourceDataEncoding::KeyValue { .. });
    if requires_keyvalue && !is_keyvalue {
        bail!("ENVELOPE [DEBEZIUM] UPSERT requires that KEY FORMAT be specified");
    };

    Ok(encoding)
}

generate_extracted_config!(AvroSchemaOption, (ConfluentWireFormat, bool, Default(true)));

#[derive(Debug)]
pub struct Schema {
    pub key_schema: Option<String>,
    pub value_schema: String,
    pub csr_connection: Option<mz_dataflow_types::connections::CsrConnection>,
    pub confluent_wire_format: bool,
}

fn get_encoding_inner(
    scx: &StatementContext,
    format: &Format<Aug>,
) -> Result<SourceDataEncodingInner, anyhow::Error> {
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
                    schema: mz_sql_parser::ast::Schema::Inline(schema),
                    with_options,
                } => {
                    let AvroSchemaOptionExtracted {
                        confluent_wire_format,
                    } = with_options.clone().try_into()?;

                    Schema {
                        key_schema: None,
                        value_schema: schema.clone(),
                        csr_connection: None,
                        confluent_wire_format,
                    }
                }
                AvroSchema::InlineSchema {
                    schema: mz_sql_parser::ast::Schema::File(_),
                    ..
                } => {
                    unreachable!("File schema should already have been inlined")
                }
                AvroSchema::Csr {
                    csr_connection:
                        CsrConnectionAvro {
                            connection,
                            seed,
                            with_options: ccsr_options,
                        },
                } => {
                    let mut normalized_options = normalize::options(&ccsr_options)?;
                    let csr_connection = match connection {
                        CsrConnection::Inline { url } => kafka_util::generate_ccsr_connection(
                            url.parse()?,
                            &mut normalized_options,
                        )?,
                        CsrConnection::Reference { connection } => {
                            let item = scx.get_item_by_resolved_name(&connection)?;
                            match item.connection()? {
                                Connection::Csr(connection) => connection.clone(),
                                _ => bail!("{} is not a schema registry connection", item.name()),
                            }
                        }
                    };
                    normalize::ensure_empty_options(
                        &normalized_options,
                        "CONFLUENT SCHEMA REGISTRY",
                    )?;
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
                    CsrConnectionProto {
                        connection,
                        seed,
                        with_options: ccsr_options,
                    },
            } => {
                if let Some(CsrSeedCompiledOrLegacy::Compiled(CsrSeedCompiled { key, value })) =
                    seed
                {
                    // We validate to match the behavior of Avro CSR connections,
                    // even though we don't actually use the connection. (It
                    // was used during purification.)
                    let mut normalized_options = normalize::options(&ccsr_options)?;
                    let _ = match connection {
                        CsrConnection::Inline { url } => kafka_util::generate_ccsr_connection(
                            url.parse()?,
                            &mut normalized_options,
                        )?,
                        CsrConnection::Reference { connection } => {
                            let item = scx.get_item_by_resolved_name(&connection)?;
                            match item.connection()? {
                                Connection::Csr(connection) => connection.clone(),
                                _ => bail!("{} is not a schema registry connection", item.name()),
                            }
                        }
                    };
                    normalize::ensure_empty_options(
                        &normalized_options,
                        "CONFLUENT SCHEMA REGISTRY",
                    )?;
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
                schema,
            } => {
                let descriptors = match schema {
                    mz_sql_parser::ast::Schema::Inline(bytes) => strconv::parse_bytes(&bytes)?,
                    mz_sql_parser::ast::Schema::File(_) => {
                        unreachable!("File schema should already have been inlined")
                    }
                };

                DataEncodingInner::Protobuf(ProtobufEncoding {
                    descriptors,
                    message_name: message_name.to_owned(),
                    confluent_wire_format: false,
                })
            }
        },
        Format::Regex(regex) => {
            let regex = Regex::new(&regex)?;
            DataEncodingInner::Regex(RegexEncoding {
                regex: mz_repr::adt::regex::Regex(regex),
            })
        }
        Format::Csv { columns, delimiter } => {
            let columns = match columns {
                CsvColumns::Header { names } => {
                    if names.is_empty() {
                        bail!("[internal error] column spec should get names in purify")
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
                    _ => bail!("CSV delimiter must be an ASCII character"),
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
    envelope: &Envelope<Aug>,
    encoding: &SourceDataEncoding,
) -> Result<Option<KeyEnvelope>, anyhow::Error> {
    let key_definition = included_items
        .iter()
        .find(|i| i.ty == SourceIncludeMetadataType::Key);
    if matches!(envelope, Envelope::Debezium { .. }) && key_definition.is_some() {
        bail!("Cannot use INCLUDE KEY with ENVELOPE DEBEZIUM: Debezium values include all keys.");
    }
    if let Some(kd) = key_definition {
        Ok(Some(match (&kd.alias, encoding) {
            (Some(name), SourceDataEncoding::KeyValue { .. }) => {
                KeyEnvelope::Named(name.as_str().to_string())
            }
            (None, _) if matches!(envelope, Envelope::Upsert { .. }) => KeyEnvelope::LegacyUpsert,
            (None, SourceDataEncoding::KeyValue { key, .. }) => {
                // If the key is requested but comes from an unnamed type then it gets the name "key"
                //
                // Otherwise it gets the names of the columns in the type
                let is_composite = match key.inner {
                    DataEncodingInner::Postgres | DataEncodingInner::RowCodec(_) => {
                        bail!("{} sources cannot use INCLUDE KEY", key.op_name())
                    }
                    DataEncodingInner::Bytes | DataEncodingInner::Text => false,
                    DataEncodingInner::Avro(_)
                    | DataEncodingInner::Csv(_)
                    | DataEncodingInner::Protobuf(_)
                    | DataEncodingInner::Regex { .. } => true,
                };

                if is_composite {
                    KeyEnvelope::Flattened
                } else {
                    KeyEnvelope::Named("key".to_string())
                }
            }
            (_, SourceDataEncoding::Single(_)) => {
                // `kd.alias` == `None` means `INCLUDE KEY`
                // `kd.alias` == `Some(_) means INCLUDE KEY AS ___`
                // These both make sense with the same error message
                bail!(
                    "INCLUDE KEY requires specifying KEY FORMAT .. VALUE FORMAT, \
                        got bare FORMAT"
                );
            }
        }))
    } else {
        Ok(None)
    }
}

pub fn describe_create_view(
    _: &StatementContext,
    _: CreateViewStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_view(
    scx: &StatementContext,
    def: &mut ViewDefinition<Aug>,
    params: &Params,
    temporary: bool,
) -> Result<(QualifiedObjectName, View), anyhow::Error> {
    let create_sql = normalize::create_statement(
        scx,
        Statement::CreateView(CreateViewStatement {
            if_exists: IfExistsBehavior::Error,
            temporary,
            materialized: false,
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

    expr.bind_parameters(&params)?;
    //TODO: materialize#724 - persist finishing information with the view?
    expr.finish(finishing);
    let relation_expr = expr.optimize_and_lower(&scx.into())?;

    let name = if temporary {
        scx.allocate_temporary_qualified_name(normalize::unresolved_object_name(name.to_owned())?)?
    } else {
        scx.allocate_qualified_name(normalize::unresolved_object_name(name.to_owned())?)?
    };

    desc = plan_utils::maybe_rename_columns(format!("view {}", name), desc, &columns)?;
    let names: Vec<ColumnName> = desc.iter_names().cloned().collect();

    if let Some(dup) = names.iter().duplicates().next() {
        bail!("column {} specified more than once", dup.as_str().quoted());
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
) -> Result<Plan, anyhow::Error> {
    let CreateViewStatement {
        temporary,
        materialized,
        if_exists,
        definition,
    } = &mut stmt;
    let partial_name = normalize::unresolved_object_name(definition.name.clone())?;
    let (name, view) = plan_view(scx, definition, params, *temporary)?;
    let replace = if *if_exists == IfExistsBehavior::Replace {
        if let Ok(item) = scx.catalog.resolve_item(&partial_name) {
            if view.expr.depends_on().contains(&item.id()) {
                bail!(
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
        materialize: *materialized,
        if_not_exists: *if_exists == IfExistsBehavior::Skip,
    }))
}

pub fn describe_create_views(
    _: &StatementContext,
    _: CreateViewsStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_views(
    scx: &StatementContext,
    CreateViewsStatement {
        if_exists,
        materialized,
        temporary,
        source: source_name,
        targets,
    }: CreateViewsStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let source_connection = scx
        .get_item_by_resolved_name(&source_name)?
        .source_connection()?;
    match source_connection {
        SourceConnection::External {
            connection: ExternalSourceConnection::Postgres(PostgresSourceConnection { details, .. }),
            ..
        } => {
            let targets = targets.unwrap_or_else(|| {
                details
                    .tables
                    .iter()
                    .map(|t| {
                        let name = UnresolvedObjectName::qualified(&[&t.namespace, &t.name]);
                        CreateViewsSourceTarget {
                            name: name.clone(),
                            alias: Some(name),
                        }
                    })
                    .collect()
            });

            // An index from table_name -> schema_name -> PostgresTable
            let mut details_info_idx: HashMap<String, HashMap<String, PostgresTableDesc>> =
                HashMap::new();
            for table in &details.tables {
                details_info_idx
                    .entry(table.name.clone())
                    .or_default()
                    .entry(table.namespace.clone())
                    .or_insert_with(|| table.clone());
            }
            let mut views = Vec::with_capacity(targets.len());
            for target in targets {
                let view_name = target.alias.clone().unwrap_or_else(|| target.name.clone());
                let name = normalize::unresolved_object_name(target.name.clone())?;
                let schemas = details_info_idx
                    .get(&name.item)
                    .ok_or_else(|| anyhow!("table {} not found in upstream database", name))?;
                let table_desc = match &name.schema {
                    Some(schema) => schemas.get(schema).ok_or_else(|| {
                        anyhow!("schema {} does not exist in upstream database", schema)
                    })?,
                    None => schemas.values().exactly_one().or_else(|_| {
                        Err(anyhow!(
                            "table {} is ambiguous, consider specifying the schema",
                            name
                        ))
                    })?,
                };
                let mut projection = vec![];
                for (i, column) in table_desc.columns.iter().enumerate() {
                    let mut ty =
                        mz_pgrepr::Type::from_oid_and_typmod(column.type_oid, column.type_mod)?;
                    // Ignore precision constraints on date/time types until we support
                    // it. This should be safe enough because our types are wide enough
                    // to support the maximum possible precision.
                    //
                    // See: https://github.com/MaterializeInc/materialize/issues/10837
                    match &mut ty {
                        mz_pgrepr::Type::Interval { constraints } => *constraints = None,
                        mz_pgrepr::Type::Time { precision } => *precision = None,
                        mz_pgrepr::Type::TimeTz { precision } => *precision = None,
                        mz_pgrepr::Type::Timestamp { precision } => *precision = None,
                        mz_pgrepr::Type::TimestampTz { precision } => *precision = None,
                        _ => (),
                    }
                    // NOTE(benesch): this *looks* gross, but it is
                    // safe enough. The `fmt::Display`
                    // representation on `pgrepr::Type` promises to
                    // produce an unqualified type name that does
                    // not require quoting.
                    //
                    // TODO(benesch): converting `json` to `jsonb`
                    // is wrong. We ought to support the `json` type
                    // directly.
                    let mut ty = format!("pg_catalog.{}", ty);
                    if ty == "pg_catalog.json" {
                        ty = "pg_catalog.jsonb".into();
                    }
                    let data_type = mz_sql_parser::parser::parse_data_type(&ty)?;
                    let (data_type, _) = names::resolve(scx.catalog, data_type)?;
                    projection.push(SelectItem::Expr {
                        expr: Expr::Cast {
                            expr: Box::new(Expr::Subscript {
                                expr: Box::new(Expr::Identifier(vec![Ident::new("row_data")])),
                                positions: vec![SubscriptPosition {
                                    start: Some(Expr::Value(Value::Number(
                                        // LIST is one based
                                        (i + 1).to_string(),
                                    ))),
                                    end: None,
                                    explicit_slice: false,
                                }],
                            }),
                            data_type,
                        },
                        alias: Some(Ident::new(column.name.clone())),
                    });
                }
                let query = Query {
                    ctes: vec![],
                    body: SetExpr::Select(Box::new(Select {
                        distinct: None,
                        projection,
                        from: vec![TableWithJoins {
                            relation: TableFactor::Table {
                                name: source_name.clone(),
                                alias: None,
                            },
                            joins: vec![],
                        }],
                        selection: Some(Expr::Op {
                            op: Op::bare("="),
                            expr1: Box::new(Expr::Identifier(vec![Ident::new("oid")])),
                            expr2: Some(Box::new(Expr::Value(Value::Number(
                                table_desc.oid.to_string(),
                            )))),
                        }),
                        group_by: vec![],
                        having: None,
                        options: vec![],
                    })),
                    order_by: vec![],
                    limit: None,
                    offset: None,
                };

                let mut viewdef = ViewDefinition {
                    name: view_name,
                    columns: table_desc
                        .columns
                        .iter()
                        .map(|c| Ident::new(c.name.clone()))
                        .collect(),
                    query,
                };
                views.push(plan_view(scx, &mut viewdef, &Params::empty(), temporary)?);
            }
            Ok(Plan::CreateViews(CreateViewsPlan {
                views,
                if_not_exists: if_exists == IfExistsBehavior::Skip,
                materialize: materialized,
            }))
        }
        SourceConnection::External { connection, .. } => {
            bail!("cannot generate views from {} sources", connection.name())
        }
        SourceConnection::Local { .. } => {
            bail!("cannot generate views from local sources")
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn kafka_sink_builder(
    scx: &StatementContext,
    format: Option<Format<Aug>>,
    consistency: Option<KafkaConsistency<Aug>>,
    with_options: &mut BTreeMap<String, SqlValueOrSecret>,
    broker: String,
    topic_prefix: String,
    relation_key_indices: Option<Vec<usize>>,
    key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    value_desc: RelationDesc,
    envelope: SinkEnvelope,
    topic_suffix_nonce: String,
    root_dependencies: &[&dyn CatalogItem],
) -> Result<SinkConnectionBuilder, anyhow::Error> {
    let consistency_topic = match with_options.remove("consistency_topic") {
        None => None,
        Some(SqlValueOrSecret::Value(Value::String(topic))) => Some(topic),
        Some(_) => bail!("consistency_topic must be a string"),
    };
    if consistency_topic.is_some() && consistency.is_some() {
        // We're keeping consistency_topic around for backwards compatibility. Users
        // should not be able to specify consistency_topic and the newer CONSISTENCY options.
        bail!("Cannot specify consistency_topic and CONSISTENCY options simultaneously");
    }
    let reuse_topic = match with_options.remove("reuse_topic") {
        Some(SqlValueOrSecret::Value(Value::Boolean(b))) => b,
        None => false,
        Some(_) => bail!("reuse_topic must be a boolean"),
    };
    let config_options = kafka_util::extract_config(with_options)?;

    let avro_key_fullname = match with_options.remove("avro_key_fullname") {
        Some(SqlValueOrSecret::Value(Value::String(s))) => Some(s),
        None => None,
        Some(_) => bail!("avro_key_fullname must be a string"),
    };

    if key_desc_and_indices.is_none() && avro_key_fullname.is_some() {
        bail!("Cannot specify avro_key_fullname without a corresponding KEY field");
    }

    let avro_value_fullname = match with_options.remove("avro_value_fullname") {
        Some(SqlValueOrSecret::Value(Value::String(s))) => Some(s),
        None => None,
        Some(_) => bail!("avro_value_fullname must be a string"),
    };

    if key_desc_and_indices.is_some()
        && (avro_key_fullname.is_some() ^ avro_value_fullname.is_some())
    {
        bail!("Must specify both avro_key_fullname and avro_value_fullname when specifying generated schema names");
    }

    let format = match format {
        Some(Format::Avro(AvroSchema::Csr {
            csr_connection:
                CsrConnectionAvro {
                    connection: CsrConnection::Inline { url },
                    seed,
                    with_options,
                },
        })) => {
            if seed.is_some() {
                bail!("SEED option does not make sense with sinks");
            }
            let mut normalized_with_options = normalize::options(&with_options)?;
            let csr_connection =
                kafka_util::generate_ccsr_connection(url.parse()?, &mut normalized_with_options)?;
            normalize::ensure_empty_options(&normalized_with_options, "CONFLUENT SCHEMA REGISTRY")?;

            let include_transaction =
                reuse_topic || consistency_topic.is_some() || consistency.is_some();
            let schema_generator = AvroSchemaGenerator::new(
                avro_key_fullname.as_deref(),
                avro_value_fullname.as_deref(),
                key_desc_and_indices
                    .as_ref()
                    .map(|(desc, _indices)| desc.clone()),
                value_desc.clone(),
                matches!(envelope, SinkEnvelope::Debezium),
                include_transaction,
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

    let consistency_config = get_kafka_sink_consistency_config(
        &topic_prefix,
        &format,
        reuse_topic,
        consistency,
        consistency_topic,
    )?;

    let broker_addrs = broker.parse()?;

    let transitive_source_dependencies: Vec<_> = if reuse_topic {
        for item in root_dependencies.iter() {
            if item.item_type() == CatalogItemType::Source {
                if !item.source_connection()?.yields_stable_input() {
                    bail!(
                    "reuse_topic requires that sink input dependencies are replayable, {} is not",
                    scx.catalog.resolve_full_name(item.name())
                );
                }
            } else if item.item_type() != CatalogItemType::Source {
                bail!(
                    "reuse_topic requires that sink input dependencies are sources, {} is not",
                    scx.catalog.resolve_full_name(item.name())
                );
            };
        }

        root_dependencies.iter().map(|i| i.id()).collect()
    } else {
        Vec::new()
    };

    // Use the user supplied value for partition count, or default to -1 (broker default)
    let partition_count = match with_options.remove("partition_count") {
        None => -1,
        Some(SqlValueOrSecret::Value(Value::Number(n))) => n.parse::<i32>()?,
        Some(_) => bail!("partition count for sink topics must be an integer"),
    };

    if partition_count == 0 || partition_count < -1 {
        bail!(
            "partition count for sink topics must be a positive integer or -1 for broker default"
        );
    }

    // Use the user supplied value for replication factor, or default to -1 (broker default)
    let replication_factor = match with_options.remove("replication_factor") {
        None => -1,
        Some(SqlValueOrSecret::Value(Value::Number(n))) => n.parse::<i32>()?,
        Some(_) => bail!("replication factor for sink topics must be an integer"),
    };

    if replication_factor == 0 || replication_factor < -1 {
        bail!(
            "replication factor for sink topics must be a positive integer or -1 for broker default"
        );
    }

    let retention_duration = match with_options.remove("retention_ms") {
        None => None,
        Some(SqlValueOrSecret::Value(Value::Number(n))) => match n.parse::<i64>()? {
            -1 => Some(None),
            millis @ 0.. => Some(Some(Duration::from_millis(millis as u64))),
            _ => bail!("retention ms for sink topics must be greater than or equal to -1"),
        },
        Some(_) => bail!("retention ms for sink topics must be an integer"),
    };

    let retention_bytes = match with_options.remove("retention_bytes") {
        None => None,
        Some(SqlValueOrSecret::Value(Value::Number(n))) => Some(n.parse::<i64>()?),
        Some(_) => bail!("retention bytes for sink topics must be an integer"),
    };

    if retention_bytes.unwrap_or(0) < -1 {
        bail!("retention bytes for sink topics must be greater than or equal to -1");
    }
    let retention = KafkaSinkConnectionRetention {
        duration: retention_duration,
        bytes: retention_bytes,
    };

    let consistency_topic = consistency_config.clone().map(|config| config.0);
    let consistency_format = consistency_config.map(|config| config.1);

    Ok(SinkConnectionBuilder::Kafka(KafkaSinkConnectionBuilder {
        connection: KafkaConnection {
            broker: broker_addrs,
            options: config_options,
        },
        format,
        topic_prefix,
        consistency_topic_prefix: consistency_topic,
        consistency_format,
        topic_suffix_nonce,
        partition_count,
        replication_factor,
        fuel: 10000,
        relation_key_indices,
        key_desc_and_indices,
        value_desc,
        reuse_topic,
        transitive_source_dependencies,
        retention,
    }))
}

/// Determines the consistency configuration (topic and format) that should be used for a Kafka
/// sink based on the given configuration items.
///
/// This is slightly complicated because of a desire to maintain backwards compatibility with
/// previous ways of specifying consistency configuration. [`KafkaConsistency`] is the new way of
/// doing things, we support specifying just a topic name (via `consistency_topic`) for backwards
/// compatibility.
fn get_kafka_sink_consistency_config(
    topic_prefix: &str,
    sink_format: &KafkaSinkFormat,
    reuse_topic: bool,
    consistency: Option<KafkaConsistency<Aug>>,
    consistency_topic: Option<String>,
) -> Result<Option<(String, KafkaSinkFormat)>, anyhow::Error> {
    let result = match consistency {
        Some(KafkaConsistency {
            topic,
            topic_format,
        }) => match topic_format {
            Some(Format::Avro(AvroSchema::Csr {
                csr_connection:
                    CsrConnectionAvro {
                        connection: CsrConnection::Inline { url },
                        seed,
                        with_options,
                    },
            })) => {
                if seed.is_some() {
                    bail!("SEED option does not make sense with sinks");
                }
                let csr_connection = kafka_util::generate_ccsr_connection(
                    url.parse()?,
                    &mut normalize::options(&with_options)?,
                )?;

                Some((
                    topic,
                    KafkaSinkFormat::Avro {
                        key_schema: None,
                        value_schema: avro::get_debezium_transaction_schema().canonical_form(),
                        csr_connection,
                    },
                ))
            }
            None => {
                // If a CONSISTENCY FORMAT is not provided, default to the FORMAT of the sink.
                match sink_format {
                    format @ KafkaSinkFormat::Avro { .. } => Some((topic, format.clone())),
                    KafkaSinkFormat::Json => bail_unsupported!("CONSISTENCY FORMAT JSON"),
                }
            }
            Some(other) => bail_unsupported!(format!("CONSISTENCY FORMAT {}", &other)),
        },
        None => {
            // Support use of `consistency_topic` with option if the sink is Avro-formatted
            // for backwards compatibility.
            if reuse_topic | consistency_topic.is_some() {
                match sink_format {
                    KafkaSinkFormat::Avro {
                        csr_connection,
                        ..
                    } => {
                        let consistency_topic = match consistency_topic {
                            Some(topic) => topic,
                            None => {
                                let default_consistency_topic =
                                    format!("{}-consistency", topic_prefix);
                                debug!(
                                    "Using default consistency topic '{}' for topic '{}'",
                                    default_consistency_topic, topic_prefix
                                );
                                default_consistency_topic
                            }
                        };
                        Some((
                            consistency_topic,
                            KafkaSinkFormat::Avro {
                                key_schema: None,
                                value_schema: avro::get_debezium_transaction_schema()
                                    .canonical_form(),
                                csr_connection: csr_connection.clone(),
                            },
                        ))
                    }
                    KafkaSinkFormat::Json => bail!("For FORMAT JSON, you need to manually specify an Avro consistency topic using 'CONSISTENCY TOPIC consistency_topic CONSISTENCY FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY url'. The default of using a JSON consistency topic is not supported."),
                }
            } else {
                None
            }
        }
    };

    Ok(result)
}

fn persist_sink_builder(
    format: Option<Format<Aug>>,
    envelope: SinkEnvelope,
    value_desc: RelationDesc,
) -> Result<SinkConnectionBuilder, anyhow::Error> {
    if format.is_some() {
        bail!("CREATE SINK ... PERSIST cannot specify a format");
    }

    if envelope != SinkEnvelope::DifferentialRow {
        bail!("CREATE SINK ... PERSIST does not support specifying an ENVELOPE");
    }

    Ok(SinkConnectionBuilder::Persist(
        PersistSinkConnectionBuilder { value_desc },
    ))
}

pub fn describe_create_sink(
    scx: &StatementContext,
    _: CreateSinkStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    scx.require_unsafe_mode("CREATE SINK")?;
    Ok(StatementDesc::new(None))
}

pub fn plan_create_sink(
    scx: &StatementContext,
    mut stmt: CreateSinkStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    scx.require_unsafe_mode("CREATE SINK")?;
    let compute_instance = match &stmt.in_cluster {
        None => scx.resolve_compute_instance(None)?.id(),
        Some(in_cluster) => in_cluster.0,
    };
    stmt.in_cluster = Some(ResolvedClusterName(compute_instance));

    let create_sql = normalize::create_statement(scx, Statement::CreateSink(stmt.clone()))?;
    let CreateSinkStatement {
        name,
        from,
        in_cluster: _,
        connection,
        with_options,
        format,
        envelope,
        with_snapshot,
        as_of,
        if_not_exists,
    } = stmt;

    let envelope = match envelope {
        // The persist sink only works with its own special envelope, so make that the default.
        None if matches!(connection, CreateSinkConnection::Persist { .. }) => {
            SinkEnvelope::DifferentialRow
        }
        Some(Envelope::DifferentialRow) => unreachable!("not expressable in SQL"),
        // Other sinks default to ENVELOPE DEBEZIUM. Not sure that's good, though...
        None => SinkEnvelope::Debezium,
        Some(Envelope::Debezium(mz_sql_parser::ast::DbzMode::Plain { tx_metadata })) => {
            if !tx_metadata.is_empty() {
                bail_unsupported!("ENVELOPE DEBEZIUM ... TRANSACTION METADATA");
            }
            SinkEnvelope::Debezium
        }
        Some(Envelope::Upsert) => SinkEnvelope::Upsert,
        Some(Envelope::CdcV2) => bail_unsupported!("CDCv2 sinks"),
        Some(Envelope::Debezium(mz_sql_parser::ast::DbzMode::Upsert)) => {
            bail_unsupported!("UPSERT doesn't make sense for sinks")
        }
        Some(Envelope::None) => bail_unsupported!("\"ENVELOPE NONE\" sinks"),
    };
    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name)?)?;
    let from = scx.get_item_by_resolved_name(&from)?;
    let suffix_nonce = format!(
        "{}-{}",
        scx.catalog.config().start_time.timestamp(),
        scx.catalog.config().nonce
    );

    let mut with_options = normalize::options(&with_options)?;

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
                        bail!("Repeated column name in sink key: {}", col);
                    }
                }
                let indices = key_columns
                    .iter()
                    .map(|col| -> anyhow::Result<usize> {
                        let name_idx = desc
                            .get_by_name(col)
                            .map(|(idx, _type)| idx)
                            .ok_or_else(|| anyhow!("No such column: {}", col))?;
                        if desc.get_unambiguous_name(name_idx).is_none() {
                            bail!("Ambiguous column: {}", col);
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
        CreateSinkConnection::Persist { .. } => None,
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
        return Err(PlanError::UpsertSinkWithoutKey.into());
    }

    if as_of.is_some() {
        bail!("CREATE SINK ... AS OF is no longer supported");
    }

    let root_user_dependencies = get_root_dependencies(scx, &[from.id()]);

    let connection_builder = match connection {
        CreateSinkConnection::Kafka {
            broker,
            topic,
            consistency,
            ..
        } => kafka_sink_builder(
            scx,
            format,
            consistency,
            &mut with_options,
            broker,
            topic,
            relation_key_indices,
            key_desc_and_indices,
            desc.into_owned(),
            envelope,
            suffix_nonce,
            &root_user_dependencies,
        )?,
        CreateSinkConnection::Persist => persist_sink_builder(format, envelope, desc.into_owned())?,
    };

    normalize::ensure_empty_options(&with_options, "CREATE SINK")?;

    Ok(Plan::CreateSink(CreateSinkPlan {
        name,
        sink: Sink {
            create_sql,
            from: from.id(),
            connection_builder,
            envelope,
            compute_instance,
        },
        with_snapshot,
        if_not_exists,
    }))
}

fn invalid_upsert_key_err(desc: &RelationDesc, requested_user_key: &[ColumnName]) -> anyhow::Error {
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
    anyhow!("Invalid upsert key: {}, {}", requested_user_key, valid_keys)
}

fn key_constraint_err(desc: &RelationDesc, user_keys: &[ColumnName]) -> anyhow::Error {
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

    anyhow!(
        "Key constraint ({}) conflicts with existing key ({})",
        user_keys,
        existing_keys
    )
}

/// Returns only those `CatalogItem`s that don't have any other user
/// dependencies. Those are the root dependencies.
fn get_root_dependencies<'a>(
    scx: &'a StatementContext,
    depends_on: &[GlobalId],
) -> Vec<&'a dyn CatalogItem> {
    let mut result = Vec::new();
    let mut work_queue: Vec<&GlobalId> = Vec::new();
    let mut visited = HashSet::new();
    work_queue.extend(depends_on.iter().filter(|id| id.is_user()));

    while let Some(dep) = work_queue.pop() {
        let item = scx.get_item(&dep);
        let transitive_uses = item.uses().iter().filter(|id| id.is_user());
        let mut transitive_uses = transitive_uses.peekable();
        if let Some(_) = transitive_uses.peek() {
            for transitive_dep in transitive_uses {
                if visited.insert(transitive_dep) {
                    work_queue.push(transitive_dep);
                }
            }
        } else {
            // no transitive uses, so we must be a root dependency
            result.push(item);
        }
    }
    result
}

pub fn describe_create_index(
    _: &StatementContext,
    _: CreateIndexStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_index(
    scx: &StatementContext,
    mut stmt: CreateIndexStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let CreateIndexStatement {
        name,
        on_name,
        in_cluster,
        key_parts,
        with_options,
        if_not_exists,
    } = &mut stmt;
    let on = scx.get_item_by_resolved_name(&on_name)?;

    if CatalogItemType::View != on.item_type()
        && CatalogItemType::Source != on.item_type()
        && CatalogItemType::Table != on.item_type()
    {
        bail!(
            "index cannot be created on {} because it is a {}",
            on_name.full_name_str(),
            on.item_type()
        )
    }

    let on_desc = on.desc(&scx.catalog.resolve_full_name(on.name()))?;

    let filled_key_parts = match key_parts {
        Some(kp) => kp.to_vec(),
        None => {
            // `key_parts` is None if we're creating a "default" index, i.e.
            // creating the index as if the index had been created alongside the
            // view source, e.g. `CREATE MATERIALIZED...`
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
            idx_name.item += &format!("_{}_idx", index_name_col_suffix);
            idx_name.item = normalize::ident(Ident::new(&idx_name.item))
        }

        if !*if_not_exists {
            scx.catalog.find_available_name(idx_name)
        } else {
            idx_name
        }
    };

    let options = plan_index_options(with_options.clone())?;
    let compute_instance = match in_cluster {
        None => scx.resolve_compute_instance(None)?.id(),
        Some(in_cluster) => in_cluster.0,
    };
    *in_cluster = Some(ResolvedClusterName(compute_instance));

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
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_type(
    scx: &StatementContext,
    stmt: CreateTypeStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let create_sql = normalize::create_statement(scx, Statement::CreateType(stmt.clone()))?;
    let CreateTypeStatement { name, as_type, .. } = stmt;
    fn ensure_valid_data_type(
        scx: &StatementContext,
        data_type: &ResolvedDataType,
        as_type: &CreateTypeAs<Aug>,
        key: &str,
    ) -> Result<(), anyhow::Error> {
        let item = match data_type {
            ResolvedDataType::Named {
                id,
                full_name,
                modifiers,
                ..
            } => {
                if !modifiers.is_empty() {
                    bail!(
                        "CREATE TYPE ... AS {}option {} cannot accept type modifier on \
                                {}, you must use the default type",
                        as_type.to_string().quoted(),
                        key,
                        full_name
                    );
                }
                scx.catalog.get_item(&id)
            }
            d => bail!(
                "CREATE TYPE ... AS {}option {} can only use named data types, but \
                        found unnamed data type {}. Use CREATE TYPE to create a named type first",
                as_type.to_string().quoted(),
                key,
                d.to_ast_string(),
            ),
        };

        match scx.catalog.get_item(&item.id()).type_details() {
            None => bail!(
                "{} must be of class type, but received {} which is of class {}",
                key,
                scx.catalog.resolve_full_name(item.name()),
                item.item_type()
            ),
            Some(CatalogTypeDetails {
                typ: CatalogType::Char,
                ..
            }) if matches!(as_type, CreateTypeAs::List { .. }) => {
                bail_unsupported!("char list")
            }
            _ => {}
        }

        Ok(())
    }

    let mut depends_on = vec![];
    let mut record_fields = vec![];
    match &as_type {
        CreateTypeAs::List { with_options } | CreateTypeAs::Map { with_options } => {
            let mut with_options = normalize::option_objects(&with_options);
            let option_keys = match as_type {
                CreateTypeAs::List { .. } => vec!["element_type"],
                CreateTypeAs::Map { .. } => vec!["key_type", "value_type"],
                _ => vec![],
            };

            for key in option_keys {
                match with_options.remove(&key.to_string()) {
                    Some(WithOptionValue::DataType(data_type)) => {
                        ensure_valid_data_type(scx, &data_type, &as_type, key)?;
                        depends_on.extend(data_type.get_ids());
                    }
                    Some(_) => bail!("{} must be a data type", key),
                    None => bail!("{} parameter required", key),
                };
            }

            normalize::ensure_empty_options(&with_options, "CREATE TYPE")?;
        }
        CreateTypeAs::Record { ref column_defs } => {
            for column_def in column_defs {
                let data_type = &column_def.data_type;
                let key = ident(column_def.name.clone());
                ensure_valid_data_type(scx, data_type, &as_type, &key)?;
                depends_on.extend(data_type.get_ids());
                if let ResolvedDataType::Named { id, .. } = data_type {
                    record_fields.push((ColumnName::from(key.clone()), *id));
                } else {
                    bail!("field {} must be a named type", key)
                }
            }
        }
    };

    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name)?)?;
    if scx.item_exists(&name) {
        bail!("catalog item '{}' already exists", name);
    }

    let inner = match as_type {
        CreateTypeAs::List { .. } => CatalogType::List {
            // works because you can't create a nested List without intermediate custom types
            element_reference: *depends_on.get(0).expect("custom type to have element id"),
        },
        CreateTypeAs::Map { .. } => {
            // works because you can't create a nested Map without intermediate custom types
            let key_id = *depends_on.get(0).expect("key");
            let value_id = *depends_on.get(1).expect("value");
            let entry = scx.catalog.get_item(&key_id);
            match entry.type_details() {
                Some(CatalogTypeDetails {
                    typ: CatalogType::String,
                    ..
                }) => {}
                Some(_) => bail!(
                    "key_type must be text, got {}",
                    scx.catalog.resolve_full_name(entry.name())
                ),
                None => unreachable!("already guaranteed id correlates to a type"),
            }

            CatalogType::Map {
                key_reference: key_id,
                value_reference: value_id,
            }
        }
        CreateTypeAs::Record { .. } => CatalogType::Record {
            fields: record_fields,
        },
    };

    Ok(Plan::CreateType(CreateTypePlan {
        name,
        typ: Type { create_sql, inner },
    }))
}

pub fn describe_create_role(
    _: &StatementContext,
    _: CreateRoleStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_role(
    _: &StatementContext,
    CreateRoleStatement {
        name,
        is_user,
        options,
    }: CreateRoleStatement,
) -> Result<Plan, anyhow::Error> {
    let mut login = None;
    let mut super_user = None;
    for option in options {
        match option {
            CreateRoleOption::Login | CreateRoleOption::NoLogin if login.is_some() => {
                bail!("conflicting or redundant options");
            }
            CreateRoleOption::SuperUser | CreateRoleOption::NoSuperUser if super_user.is_some() => {
                bail!("conflicting or redundant options");
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
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_cluster(
    scx: &StatementContext,
    CreateClusterStatement { name, options }: CreateClusterStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let mut replicas_definitions = None;
    let mut introspection_debugging = None;
    let mut introspection_granularity: Option<Option<Interval>> = None;

    for option in options {
        match option {
            ClusterOption::IntrospectionDebugging(enabled) => {
                if introspection_debugging.is_some() {
                    bail!("INTROSPECTION DEBUGGING specified more than once");
                }
                introspection_debugging = Some(
                    bool::try_from_value(enabled)
                        .map_err(|e| anyhow!("invalid INTROSPECTION DEBUGGING: {}", e))?,
                );
            }
            ClusterOption::IntrospectionGranularity(interval) => {
                if introspection_granularity.is_some() {
                    bail!("INTROSPECTION GRANULARITY specified more than once");
                }
                introspection_granularity = Some(
                    OptionalInterval::try_from_value(interval)
                        .map_err(|e| anyhow!("invalid INTROSPECTION GRANULARITY: {}", e))?
                        .0,
                );
            }
            ClusterOption::Replicas(replicas) => {
                if replicas_definitions.is_some() {
                    bail!("REPLICAS specified more than once");
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

    let introspection_granularity =
        introspection_granularity.unwrap_or(Some(DEFAULT_INTROSPECTION_GRANULARITY));

    let config = match (introspection_debugging, introspection_granularity) {
        (None | Some(false), None) => None,
        (debugging, Some(granularity)) => Some(ComputeInstanceIntrospectionConfig {
            debugging: debugging.unwrap_or(false),
            granularity: granularity.duration()?,
        }),
        (Some(true), None) => {
            bail!("INTROSPECTION DEBUGGING cannot be specified without INTROSPECTION GRANULARITY")
        }
    };
    Ok(Plan::CreateComputeInstance(CreateComputeInstancePlan {
        name: normalize::ident(name),
        config,
        replicas,
    }))
}

const DEFAULT_INTROSPECTION_GRANULARITY: Interval = Interval {
    micros: 1_000_000,
    months: 0,
    days: 0,
};

generate_extracted_config!(
    ReplicaOption,
    (AvailabilityZone, String),
    (Size, String),
    (Remote, Vec<String>)
);

fn plan_replica_config(
    scx: &StatementContext,
    options: Vec<ReplicaOption<Aug>>,
) -> Result<ReplicaConfig, anyhow::Error> {
    let ReplicaOptionExtracted {
        availability_zone,
        size,
        remote,
    }: ReplicaOptionExtracted = options.try_into()?;

    if remote.is_some() {
        scx.require_unsafe_mode("REMOTE cluster replica option")?;
    }

    let remote_replicas = remote
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<String>>();

    match (remote_replicas.len() > 0, size) {
        (true, None) => {
            if availability_zone.is_some() {
                bail!("cannot specify AVAILABILITY ZONE and REMOTE");
            }
            Ok(ReplicaConfig::Remote {
                replicas: remote_replicas,
            })
        }
        (false, Some(size)) => Ok(ReplicaConfig::Managed {
            size,
            availability_zone,
        }),
        (false, None) => {
            bail!("one of REMOTE or SIZE must be specified")
        }
        (true, Some(_)) => {
            bail!("only one of REMOTE or SIZE may be specified")
        }
    }
}

pub fn describe_create_cluster_replica(
    _: &StatementContext,
    _: CreateClusterReplicaStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_cluster_replica(
    scx: &StatementContext,
    CreateClusterReplicaStatement {
        definition: ReplicaDefinition { name, options },
        of_cluster,
    }: CreateClusterReplicaStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let _ = scx
        .catalog
        .resolve_compute_instance(Some(&of_cluster.to_string()))?;
    Ok(Plan::CreateComputeInstanceReplica(
        CreateComputeInstanceReplicaPlan {
            name: normalize::ident(name),
            of_cluster: of_cluster.to_string(),
            config: plan_replica_config(scx, options)?,
        },
    ))
}

pub fn describe_create_secret(
    _: &StatementContext,
    _: CreateSecretStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_secret(
    scx: &StatementContext,
    stmt: CreateSecretStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    let CreateSecretStatement {
        name,
        if_not_exists,
        value,
    } = &stmt;

    let name = scx.allocate_qualified_name(normalize::unresolved_object_name(name.to_owned())?)?;
    let create_sql = normalize::create_statement(&scx, Statement::CreateSecret(stmt.clone()))?;
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
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_connection(
    scx: &StatementContext,
    stmt: CreateConnectionStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
    scx.require_unsafe_mode("CREATE CONNECTION")?;

    let create_sql = normalize::create_statement(&scx, Statement::CreateConnection(stmt.clone()))?;
    let CreateConnectionStatement {
        name,
        connection,
        if_not_exists,
    } = stmt;
    let connection = match connection {
        CreateConnection::Kafka {
            broker,
            with_options,
        } => {
            let mut with_options = normalize::options(&with_options)?;
            Connection::Kafka(KafkaConnection {
                broker: broker.parse()?,
                options: kafka_util::extract_config(&mut with_options)?,
            })
        }
        CreateConnection::Csr { url, with_options } => {
            let connection = kafka_util::generate_ccsr_connection(
                url.parse()?,
                &mut normalize::options(&with_options)?,
            )?;
            Connection::Csr(connection)
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
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_database(
    scx: &StatementContext,
    DropDatabaseStatement {
        name,
        restrict,
        if_exists,
    }: DropDatabaseStatement,
) -> Result<Plan, anyhow::Error> {
    let id = match scx.resolve_database(&name) {
        Ok(database) => {
            if restrict && database.has_schemas() {
                bail!(
                    "database '{}' cannot be dropped with RESTRICT while it contains schemas",
                    name,
                );
            }
            Some(database.id())
        }
        // TODO(benesch/jkosh44): generate a notice indicating that the database does not exist.
        Err(_) if if_exists => None,
        Err(e) => return Err(e.into()),
    };
    Ok(Plan::DropDatabase(DropDatabasePlan { id }))
}

pub fn describe_drop_objects(
    _: &StatementContext,
    _: DropObjectsStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_objects(
    scx: &StatementContext,
    DropObjectsStatement {
        materialized,
        object_type,
        names,
        cascade,
        if_exists,
    }: DropObjectsStatement,
) -> Result<Plan, anyhow::Error> {
    if materialized {
        bail!(
            "DROP MATERIALIZED {0} is not allowed, use DROP {0}",
            object_type
        );
    }

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
        | ObjectType::Index
        | ObjectType::Sink
        | ObjectType::Type
        | ObjectType::Secret
        | ObjectType::Connection => plan_drop_items(scx, object_type, items, cascade),
        ObjectType::Role | ObjectType::Cluster | ObjectType::ClusterReplica => {
            unreachable!("handled through their respective plan_drop functions")
        }
        ObjectType::Object => unreachable!("cannot drop generic OBJECT, must provide object type"),
    }
}

pub fn describe_drop_schema(
    _: &StatementContext,
    _: DropSchemaStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_schema(
    scx: &StatementContext,
    DropSchemaStatement {
        name,
        cascade,
        if_exists,
    }: DropSchemaStatement,
) -> Result<Plan, anyhow::Error> {
    match scx.resolve_schema(name) {
        Ok(schema) => {
            let database_id = match schema.database() {
                ResolvedDatabaseSpecifier::Ambient => bail!(
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
                bail!(
                    "schema '{}' cannot be dropped without CASCADE while it contains objects",
                    full_schema_name
                );
            }
            let schema_id = match schema.id() {
                // This branch should be unreachable because the temporary schema is in the ambient
                // database, but this is just to protect against the case that ever changes.
                SchemaSpecifier::Temporary => bail!(
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
        Err(e) => Err(e.into()),
    }
}

pub fn describe_drop_role(
    _: &StatementContext,
    _: DropRolesStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_role(
    scx: &StatementContext,
    DropRolesStatement { if_exists, names }: DropRolesStatement,
) -> Result<Plan, anyhow::Error> {
    let mut out = vec![];
    for name in names {
        let name = if name.0.len() == 1 {
            normalize::ident(name.0.into_element())
        } else {
            bail!("invalid role name {}", name.to_string().quoted())
        };
        if name == scx.catalog.active_user() {
            bail!("current user cannot be dropped");
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
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_cluster(
    scx: &StatementContext,
    DropClustersStatement {
        if_exists,
        names,
        cascade,
    }: DropClustersStatement,
) -> Result<Plan, anyhow::Error> {
    let mut out = vec![];
    for name in names {
        let name = if name.0.len() == 1 {
            name.0.into_element()
        } else {
            bail!("invalid cluster name {}", name.to_string().quoted())
        };
        match scx.catalog.resolve_compute_instance(Some(name.as_str())) {
            Ok(instance) => {
                if !(instance.indexes().is_empty() || instance.replica_names().is_empty())
                    && !cascade
                {
                    bail!("cannot drop cluster with active indexes, sinks, or replicas");
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
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_cluster_replica(
    scx: &StatementContext,
    DropClusterReplicasStatement { if_exists, names }: DropClusterReplicasStatement,
) -> Result<Plan, anyhow::Error> {
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
            names_out.push((instance.name().to_string(), replica_name))
        } else {
            // If "IF EXISTS" supplied, names allowed to be missing,
            // otherwise error.
            if !if_exists {
                // TODO(benesch): generate a notice indicating that the
                // replica does not exist.
                bail!(
                    "CLUSTER {} has no CLUSTER REPLICA named {}",
                    instance.name(),
                    replica_name.quoted(),
                )
            }
        }
    }

    Ok(Plan::DropComputeInstanceReplica(
        DropComputeInstanceReplicaPlan { names: names_out },
    ))
}

pub fn plan_drop_items(
    scx: &StatementContext,
    object_type: ObjectType,
    items: Vec<&dyn CatalogItem>,
    cascade: bool,
) -> Result<Plan, anyhow::Error> {
    let mut ids = vec![];
    for item in items {
        ids.extend(plan_drop_item(scx, object_type, item, cascade)?);
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
) -> Result<Option<GlobalId>, anyhow::Error> {
    if catalog_entry.id().is_system() {
        bail!(
            "cannot drop item {} because it is required by the database system",
            scx.catalog.resolve_full_name(catalog_entry.name()),
        );
    }
    if object_type != catalog_entry.item_type() {
        bail!(
            "{} is not of type {}",
            scx.catalog.resolve_full_name(catalog_entry.name()),
            object_type,
        );
    }
    if !cascade {
        for id in catalog_entry.used_by() {
            let dep = scx.catalog.get_item(id);
            match object_type {
                ObjectType::Type => bail!(
                    "cannot drop {}: still depended upon by catalog item '{}'",
                    scx.catalog.resolve_full_name(catalog_entry.name()),
                    scx.catalog.resolve_full_name(dep.name())
                ),
                _ => match dep.item_type() {
                    CatalogItemType::Func
                    | CatalogItemType::Table
                    | CatalogItemType::Source
                    | CatalogItemType::View
                    | CatalogItemType::Sink
                    | CatalogItemType::Type
                    | CatalogItemType::Secret
                    | CatalogItemType::Connection => {
                        bail!(
                            "cannot drop {}: still depended upon by catalog item '{}'",
                            scx.catalog.resolve_full_name(catalog_entry.name()),
                            scx.catalog.resolve_full_name(dep.name())
                        );
                    }
                    CatalogItemType::Index => (),
                },
            }
        }
    }
    Ok(Some(catalog_entry.id()))
}

pub fn describe_alter_index_options(
    _: &StatementContext,
    _: AlterIndexStatement<Aug>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(IndexOption, (LogicalCompactionWindow, OptionalInterval));

fn plan_index_options(
    with_opts: Vec<IndexOption<Aug>>,
) -> Result<Vec<crate::plan::IndexOption>, anyhow::Error> {
    let IndexOptionExtracted {
        logical_compaction_window,
    }: IndexOptionExtracted = with_opts.try_into()?;

    let mut out = Vec::with_capacity(1);

    if let Some(OptionalInterval(lcw)) = logical_compaction_window {
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
) -> Result<Plan, anyhow::Error> {
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
        bail!(
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
                options: plan_index_options(options)?,
            }))
        }
    }
}

pub fn describe_alter_object_rename(
    _: &StatementContext,
    _: AlterObjectRenameStatement,
) -> Result<StatementDesc, anyhow::Error> {
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
) -> Result<Plan, anyhow::Error> {
    let name = normalize::unresolved_object_name(name)?;
    match scx.catalog.resolve_item(&name) {
        Ok(entry) => {
            let full_name = scx.catalog.resolve_full_name(entry.name());
            if entry.item_type() != object_type {
                bail!(
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
                bail!("catalog item '{}' already exists", to_item_name);
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
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_secret(
    scx: &StatementContext,
    stmt: AlterSecretStatement<Aug>,
) -> Result<Plan, anyhow::Error> {
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
        bail!(
            "{} is a {} not a SECRET",
            scx.catalog.resolve_full_name(entry.name()),
            entry.item_type()
        )
    }
    let id = entry.id();
    let secret_as = query::plan_secret_as(scx, value)?;

    Ok(Plan::AlterSecret(AlterSecretPlan { id, secret_as }))
}
