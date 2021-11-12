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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail};
use aws_arn::ARN;
use globset::GlobBuilder;
use itertools::Itertools;
use log::{debug, error};
use regex::Regex;
use reqwest::Url;

use dataflow_types::{
    AvroEncoding, AvroOcfEncoding, AvroOcfSinkConnectorBuilder, BringYourOwn, ColumnSpec,
    Consistency, CsvEncoding, DataEncoding, DebeziumMode, ExternalSourceConnector,
    FileSourceConnector, KafkaSinkConnectorBuilder, KafkaSinkConnectorRetention, KafkaSinkFormat,
    KafkaSourceConnector, KeyEnvelope, KinesisSourceConnector, PostgresSourceConnector,
    ProtobufEncoding, PubNubSourceConnector, RegexEncoding, S3SourceConnector,
    SinkConnectorBuilder, SinkEnvelope, SourceConnector, SourceDataEncoding, SourceEnvelope,
    Timeline,
};
use expr::{func, GlobalId, MirRelationExpr, TableFunc, UnaryFunc};
use interchange::avro::{self, AvroSchemaGenerator, DebeziumDeduplicationStrategy};
use interchange::envelopes;
use ore::collections::CollectionExt;
use ore::str::StrExt;
use repr::{strconv, ColumnName, ColumnType, Datum, RelationDesc, RelationType, Row, ScalarType};
use sql_parser::ast::CsrSeedCompiledOrLegacy;

use crate::ast::display::AstDisplay;
use crate::ast::{
    AlterIndexAction, AlterIndexStatement, AlterObjectRenameStatement, AvroSchema, ColumnOption,
    Compression, CreateDatabaseStatement, CreateIndexStatement, CreateRoleOption,
    CreateRoleStatement, CreateSchemaStatement, CreateSinkConnector, CreateSinkStatement,
    CreateSourceConnector, CreateSourceFormat, CreateSourceStatement, CreateTableStatement,
    CreateTypeAs, CreateTypeStatement, CreateViewStatement, CreateViewsDefinitions,
    CreateViewsStatement, CsrConnectorAvro, CsrConnectorProto, CsrSeedCompiled, CsvColumns,
    DataType, DbzMode, DropDatabaseStatement, DropObjectsStatement, Envelope, Expr, Format, Ident,
    IfExistsBehavior, KafkaConsistency, KeyConstraint, ObjectType, ProtobufSchema, Raw,
    SourceIncludeMetadataType, SqlOption, Statement, UnresolvedObjectName, Value, ViewDefinition,
    WithOption,
};
use crate::catalog::{CatalogItem, CatalogItemType};
use crate::kafka_util;
use crate::names::{DatabaseSpecifier, FullName, SchemaName};
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::expr::{ColumnRef, HirScalarExpr, JoinKind};
use crate::plan::query::{resolve_names_data_type, QueryLifetime};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::{
    self, plan_utils, query, AlterIndexEnablePlan, AlterIndexResetOptionsPlan,
    AlterIndexSetOptionsPlan, AlterItemRenamePlan, AlterNoopPlan, CreateDatabasePlan,
    CreateIndexPlan, CreateRolePlan, CreateSchemaPlan, CreateSinkPlan, CreateSourcePlan,
    CreateTablePlan, CreateTypePlan, CreateViewPlan, CreateViewsPlan, DropDatabasePlan,
    DropItemsPlan, DropRolesPlan, DropSchemaPlan, HirRelationExpr, Index, IndexOption,
    IndexOptionName, Params, Plan, Sink, Source, Table, Type, TypeInner, View,
};
use crate::pure::Schema;

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
        name: normalize::ident(name),
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
    let database_name = match name.0.pop() {
        None => DatabaseSpecifier::Name(scx.catalog.default_database().into()),
        Some(n) => DatabaseSpecifier::Name(normalize::ident(n)),
    };
    Ok(Plan::CreateSchema(CreateSchemaPlan {
        database_name,
        schema_name,
        if_not_exists,
    }))
}

pub fn describe_create_table(
    _: &StatementContext,
    _: CreateTableStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_table(
    scx: &StatementContext,
    stmt: CreateTableStatement<Raw>,
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
    if !constraints.is_empty() {
        bail_unsupported!("CREATE TABLE with constraints")
    }

    let names: Vec<_> = columns
        .iter()
        .map(|c| normalize::column_name(c.name.clone()))
        .collect();

    if let Some(dup) = names.iter().duplicates().next() {
        bail!(
            "cannot CREATE TABLE: column {} specified more than once",
            dup.as_str().quoted()
        );
    }

    // Build initial relation type that handles declared data types
    // and NOT NULL constraints.
    let mut column_types = Vec::with_capacity(columns.len());
    let mut defaults = Vec::with_capacity(columns.len());
    let mut depends_on = Vec::new();

    for c in columns {
        let (aug_data_type, ids) = resolve_names_data_type(scx, c.data_type.clone())?;
        let ty = plan::scalar_type_from_sql(scx, &aug_data_type)?;
        let mut nullable = true;
        let mut default = Expr::null();
        for option in &c.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(expr) => {
                    // Ensure expression can be planned and yields the correct
                    // type.
                    let (_, expr_depends_on) = query::plan_default_expr(scx, expr, &ty)?;
                    depends_on.extend(expr_depends_on);
                    default = expr.clone();
                }
                other => {
                    bail_unsupported!(format!("CREATE TABLE with column constraint: {}", other))
                }
            }
        }
        column_types.push(ty.nullable(nullable));
        defaults.push(default);
        depends_on.extend(ids);
    }

    let typ = RelationType::new(column_types);

    let temporary = *temporary;
    let name = if temporary {
        scx.allocate_temporary_name(normalize::unresolved_object_name(name.to_owned())?)
    } else {
        scx.allocate_name(normalize::unresolved_object_name(name.to_owned())?)
    };
    let desc = RelationDesc::new(typ, names.into_iter().map(Some));

    let create_sql = normalize::create_statement(&scx, Statement::CreateTable(stmt.clone()))?;
    let table = Table {
        create_sql,
        desc,
        defaults,
        temporary,
        depends_on,
    };
    Ok(Plan::CreateTable(CreateTablePlan {
        name,
        table,
        if_not_exists: *if_not_exists,
    }))
}

pub fn describe_create_source(
    _: &StatementContext,
    _: CreateSourceStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

// Flatten one Debezium entry ("before" or "after")
// into its corresponding data fields, plus an extra column for the diff.
fn plan_dbz_flatten_one(
    input: HirRelationExpr,
    bare_column: usize,
    diff: i64,
    n_flattened_cols: usize,
) -> HirRelationExpr {
    HirRelationExpr::Map {
        input: Box::new(HirRelationExpr::Filter {
            input: Box::new(input),
            predicates: vec![HirScalarExpr::CallUnary {
                func: UnaryFunc::Not(func::Not),
                expr: Box::new(HirScalarExpr::CallUnary {
                    func: UnaryFunc::IsNull(func::IsNull),
                    expr: Box::new(HirScalarExpr::Column(ColumnRef {
                        level: 0,
                        column: bare_column,
                    })),
                }),
            }],
        }),
        scalars: (0..n_flattened_cols)
            .into_iter()
            .map(|idx| HirScalarExpr::CallUnary {
                func: UnaryFunc::RecordGet(idx),
                expr: Box::new(HirScalarExpr::Column(ColumnRef {
                    level: 0,
                    column: bare_column,
                })),
            })
            .chain(iter::once(HirScalarExpr::Literal(
                Row::pack(iter::once(Datum::Int64(diff))),
                ColumnType {
                    nullable: false,
                    scalar_type: ScalarType::Int64,
                },
            )))
            .collect(),
    }
}

fn plan_dbz_flatten(
    bare_desc: &RelationDesc,
    input: HirRelationExpr,
) -> Result<(HirRelationExpr, Vec<Option<ColumnName>>), anyhow::Error> {
    // This looks horrible, but it is basically pretty simple:
    // It aims to flatten rows of the shape
    // (before, after)
    // into rows whose columns are the individual fields of the before or after record,
    // plus a "diff" column whose value is -1 for before, and +1 for after.
    //
    // They will then be joined with `repeat(diff)` to get the correct stream out.
    let before_idx = bare_desc
        .iter_names()
        .position(|maybe_name| match maybe_name {
            Some(name) => name.as_str() == "before",
            None => false,
        })
        .ok_or_else(|| anyhow!("Debezium-formatted data must contain a `before` field."))?;
    let after_idx = bare_desc
        .iter_names()
        .position(|maybe_name| match maybe_name {
            Some(name) => name.as_str() == "after",
            None => false,
        })
        .ok_or_else(|| anyhow!("Debezium-formatted data must contain an `after` field."))?;
    let before_flattened_cols = match &bare_desc.typ().column_types[before_idx].scalar_type {
        ScalarType::Record { fields, .. } => fields.clone(),
        _ => unreachable!(), // This was verified in `Encoding::desc`
    };
    let after_flattened_cols = match &bare_desc.typ().column_types[after_idx].scalar_type {
        ScalarType::Record { fields, .. } => fields.clone(),
        _ => unreachable!(), // This was verified in `Encoding::desc`
    };
    assert!(before_flattened_cols == after_flattened_cols);
    let n_flattened_cols = before_flattened_cols.len();
    let old_arity = input.arity();
    let before_expr = plan_dbz_flatten_one(input.clone(), before_idx, -1, n_flattened_cols);
    let after_expr = plan_dbz_flatten_one(input, after_idx, 1, n_flattened_cols);
    let new_arity = before_expr.arity();
    assert!(new_arity == after_expr.arity());
    let before_expr = before_expr.project((old_arity..new_arity).collect());
    let after_expr = after_expr.project((old_arity..new_arity).collect());
    let united_expr = HirRelationExpr::Union {
        base: Box::new(before_expr),
        inputs: vec![after_expr],
    };
    let mut col_names = before_flattened_cols
        .into_iter()
        .map(|(name, _)| Some(name))
        .collect::<Vec<_>>();
    col_names.push(Some("diff".into()));
    Ok((united_expr, col_names))
}

fn plan_source_envelope(
    bare_desc: &RelationDesc,
    envelope: &SourceEnvelope,
    post_transform_key: Option<Vec<usize>>,
) -> Result<(MirRelationExpr, Vec<Option<ColumnName>>), anyhow::Error> {
    let get_expr = HirRelationExpr::Get {
        id: expr::Id::LocalBareSource,
        typ: bare_desc.typ().clone(),
    };
    let (hir_expr, column_names) = if let SourceEnvelope::Debezium(_, _) = envelope {
        // Debezium sources produce a diff in their last column.
        // Thus we need to select all rows but the last, which we repeat by.
        // I.e., for a source with four columns, we do
        // SELECT a.column1, a.column2, a.column3 FROM a, repeat(a.column4)
        //
        // [btv] - Maybe it would be better to write these in actual SQL and call into the planner,
        // rather than writing out the expr by hand? Then we would get some nice things; for
        // example, automatic tracking of column names.
        //
        // For this simple case, it probably doesn't matter

        let (flattened, mut column_names) = plan_dbz_flatten(bare_desc, get_expr)?;

        let diff_col = flattened.arity() - 1;
        let expr = HirRelationExpr::Join {
            left: Box::new(flattened),
            right: Box::new(HirRelationExpr::CallTable {
                func: TableFunc::Repeat,
                exprs: vec![HirScalarExpr::Column(ColumnRef {
                    level: 1,
                    column: diff_col,
                })],
            }),
            on: HirScalarExpr::literal_true(),
            kind: JoinKind::Inner { lateral: true },
        }
        .project((0..diff_col).collect());
        let expr = if let Some(post_transform_key) = post_transform_key {
            expr.declare_keys(vec![post_transform_key])
        } else {
            expr
        };
        column_names.pop();
        (expr, column_names)
    } else {
        (
            get_expr,
            bare_desc.iter_names().map(|name| name.cloned()).collect(),
        )
    };

    let mir_expr = hir_expr.lower();

    Ok((mir_expr, column_names))
}

pub fn plan_create_source(
    scx: &StatementContext,
    stmt: CreateSourceStatement<Raw>,
) -> Result<Plan, anyhow::Error> {
    let CreateSourceStatement {
        name,
        col_names,
        connector,
        with_options,
        envelope,
        if_not_exists,
        materialized,
        format,
        key_constraint,
        include_metadata,
    } = &stmt;

    let with_options_original = with_options;
    let mut with_options = normalize::options(with_options);

    let mut consistency = Consistency::RealTime;

    let ts_frequency = match with_options.remove("timestamp_frequency_ms") {
        Some(val) => match val {
            Value::Number(n) => match n.parse::<u64>() {
                Ok(n) => Duration::from_millis(n),
                Err(_) => bail!("timestamp_frequency_ms must be an u64"),
            },
            _ => bail!("timestamp_frequency_ms must be an u64"),
        },
        None => scx.catalog.config().timestamp_frequency,
    };
    if !matches!(connector, CreateSourceConnector::Kafka { .. }) && !include_metadata.is_empty() {
        bail_unsupported!("INCLUDE metadata with non-Kafka sources");
    }

    let (external_connector, encoding) = match connector {
        CreateSourceConnector::Kafka { broker, topic, .. } => {
            let config_options = kafka_util::extract_config(&mut with_options)?;

            consistency = match with_options.remove("consistency_topic") {
                None => Consistency::RealTime,
                Some(Value::String(topic)) => Consistency::BringYourOwn(BringYourOwn {
                    broker: broker.clone(),
                    topic,
                }),
                Some(_) => bail!("consistency_topic must be a string"),
            };

            let group_id_prefix = match with_options.remove("group_id_prefix") {
                None => None,
                Some(Value::String(s)) => Some(s),
                Some(_) => bail!("group_id_prefix must be a string"),
            };

            if with_options.contains_key("start_offset") && consistency != Consistency::RealTime {
                bail!("`start_offset` is not yet implemented for non-realtime consistency sources.")
            }
            let parse_offset = |s: &str| match s.parse::<i64>() {
                Ok(n) if n >= 0 => Ok(n),
                _ => bail!("start_offset must be a nonnegative integer"),
            };

            let mut start_offsets = HashMap::new();
            match with_options.remove("start_offset") {
                None => {
                    start_offsets.insert(0, 0);
                }
                Some(Value::Number(n)) => {
                    start_offsets.insert(0, parse_offset(&n)?);
                }
                Some(Value::Array(vs)) => {
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

            let encoding = get_encoding(format, envelope, with_options_original)?;

            let mut connector = KafkaSourceConnector {
                addrs: broker.parse()?,
                topic: topic.clone(),
                config_options,
                start_offsets,
                group_id_prefix,
                cluster_id: scx.catalog.config().cluster_id,
                include_key: None,
                include_timestamp: None,
                include_partition: None,
                include_topic: None,
            };

            for item in include_metadata {
                match item.ty {
                    SourceIncludeMetadataType::Key => {
                        connector.include_key =
                            Some(get_key_envelope(item.alias.clone(), envelope, &encoding)?)
                    }
                    SourceIncludeMetadataType::Timestamp => {
                        bail_unsupported!("INCLUDE TIMESTAMP")
                    }
                    SourceIncludeMetadataType::Partition => {
                        bail_unsupported!("INCLUDE PARTITION")
                    }
                    SourceIncludeMetadataType::Topic => {
                        bail_unsupported!("INCLUDE TOPIC")
                    }
                    SourceIncludeMetadataType::Offset => {
                        bail_unsupported!("INCLUDE OFFSET")
                    }
                }
            }

            if connector.include_key.is_none() && matches!(envelope, Envelope::Upsert) {
                connector.include_key = Some(KeyEnvelope::LegacyUpsert)
            }

            let connector = ExternalSourceConnector::Kafka(connector);

            if consistency != Consistency::RealTime
                && *envelope != sql_parser::ast::Envelope::Debezium(sql_parser::ast::DbzMode::Plain)
            {
                // TODO: does it make sense to support BYO with upsert? It doesn't seem obvious that
                // the timestamp topic will support the upsert semantics of the value topic
                bail!("BYO consistency only supported for plain Debezium Kafka sources");
            }

            (connector, encoding)
        }
        CreateSourceConnector::Kinesis { arn, .. } => {
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

            let aws_info = normalize::aws_connect_info(&mut with_options, Some(region.into()))?;
            let connector = ExternalSourceConnector::Kinesis(KinesisSourceConnector {
                stream_name,
                aws_info,
            });
            let encoding = get_encoding(format, envelope, with_options_original)?;
            (connector, encoding)
        }
        CreateSourceConnector::File { path, compression } => {
            let tail = match with_options.remove("tail") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("tail must be a boolean"),
            };
            consistency = match with_options.remove("consistency_topic") {
                None => Consistency::RealTime,
                Some(_) => bail!("BYO consistency not supported for file sources"),
            };

            let connector = ExternalSourceConnector::File(FileSourceConnector {
                path: path.clone().into(),
                compression: match compression {
                    Compression::Gzip => dataflow_types::Compression::Gzip,
                    Compression::None => dataflow_types::Compression::None,
                },
                tail,
            });
            let encoding = get_encoding(format, envelope, with_options_original)?;
            (connector, encoding)
        }
        CreateSourceConnector::S3 {
            key_sources,
            pattern,
            compression,
        } => {
            let aws_info = normalize::aws_connect_info(&mut with_options, None)?;
            let mut converted_sources = Vec::new();
            for ks in key_sources {
                let dtks = match ks {
                    sql_parser::ast::S3KeySource::Scan { bucket } => {
                        dataflow_types::S3KeySource::Scan {
                            bucket: bucket.clone(),
                        }
                    }
                    sql_parser::ast::S3KeySource::SqsNotifications { queue } => {
                        dataflow_types::S3KeySource::SqsNotifications {
                            queue: queue.clone(),
                        }
                    }
                };
                converted_sources.push(dtks);
            }
            let connector = ExternalSourceConnector::S3(S3SourceConnector {
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
                aws_info,
                compression: match compression {
                    Compression::Gzip => dataflow_types::Compression::Gzip,
                    Compression::None => dataflow_types::Compression::None,
                },
            });
            let encoding = get_encoding(format, envelope, with_options_original)?;
            (connector, encoding)
        }
        CreateSourceConnector::Postgres {
            conn,
            publication,
            slot,
        } => {
            let slot_name = slot
                .as_ref()
                .ok_or_else(|| anyhow!("Postgres sources must provide a slot name"))?;

            let connector = ExternalSourceConnector::Postgres(PostgresSourceConnector {
                conn: conn.clone(),
                publication: publication.clone(),
                slot_name: slot_name.clone(),
            });

            let encoding = SourceDataEncoding::Single(DataEncoding::Postgres);
            (connector, encoding)
        }
        CreateSourceConnector::PubNub {
            subscribe_key,
            channel,
        } => {
            match format {
                CreateSourceFormat::None | CreateSourceFormat::Bare(Format::Text) => (),
                _ => bail!("CREATE SOURCE ... PUBNUB must specify FORMAT TEXT"),
            }
            let connector = ExternalSourceConnector::PubNub(PubNubSourceConnector {
                subscribe_key: subscribe_key.clone(),
                channel: channel.clone(),
            });
            (connector, SourceDataEncoding::Single(DataEncoding::Text))
        }
        CreateSourceConnector::AvroOcf { path, .. } => {
            let tail = match with_options.remove("tail") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("tail must be a boolean"),
            };
            consistency = match with_options.remove("consistency_topic") {
                None => Consistency::RealTime,
                Some(Value::String(topic)) => Consistency::BringYourOwn(BringYourOwn {
                    broker: path.clone(),
                    topic,
                }),
                Some(_) => bail!("consistency_topic must be a string"),
            };

            if consistency != Consistency::RealTime {
                bail!("BYO consistency is not supported for Avro OCF sources");
            }

            let connector = ExternalSourceConnector::AvroOcf(FileSourceConnector {
                path: path.clone().into(),
                compression: dataflow_types::Compression::None,
                tail,
            });
            if !matches!(format, CreateSourceFormat::None) {
                bail!("avro ocf sources cannot specify a format");
            }
            let reader_schema = match with_options
                .remove("reader_schema")
                .expect("purification guarantees presence of reader_schema")
            {
                Value::String(s) => s,
                _ => bail!("reader_schema option must be a string"),
            };
            let encoding = SourceDataEncoding::Single(DataEncoding::AvroOcf(AvroOcfEncoding {
                reader_schema,
            }));
            (connector, encoding)
        }
    };

    // TODO (materialize#2537): cleanup format validation
    // Avro format validation is different for the Debezium envelope
    // vs the Upsert envelope.
    //
    // For the Debezium envelope, the key schema is not meant to be
    // used to decode records; it is meant to be a subset of the
    // value schema so we can identify what the primary key is.
    //
    // When using the Upsert envelope, we delete the key schema
    // from the value encoding because the key schema is not
    // necessarily a subset of the value schema. Also, we shift
    // the key schema, if it exists, over to the value schema position
    // in the Upsert envelope's key_format so it can be validated like
    // a schema used to decode records.

    // TODO: remove bails as more support for upsert is added.
    let envelope = match &envelope {
        sql_parser::ast::Envelope::None => SourceEnvelope::None,
        sql_parser::ast::Envelope::Debezium(mode) => {
            let is_avro = match encoding.value_ref() {
                DataEncoding::Avro(_) => true,
                DataEncoding::AvroOcf(_) => true,
                _ => false,
            };
            if !is_avro {
                bail!("non-Avro Debezium sources are not supported");
            }
            let dedup_strat = match with_options.remove("deduplication") {
                None => match mode {
                    sql_parser::ast::DbzMode::Plain => DebeziumDeduplicationStrategy::Ordered,
                    sql_parser::ast::DbzMode::Upsert => DebeziumDeduplicationStrategy::None,
                },
                Some(Value::String(s)) => {
                    match s.as_str() {
                        "none" => DebeziumDeduplicationStrategy::None,
                        "full" => DebeziumDeduplicationStrategy::Full,
                        "ordered" => DebeziumDeduplicationStrategy::Ordered,
                        "full_in_range" => {
                            match (
                                with_options.remove("deduplication_start"),
                                with_options.remove("deduplication_end"),
                            ) {
                                (Some(Value::String(start)), Some(Value::String(end))) => {
                                    let deduplication_pad_start = match with_options.remove("deduplication_pad_start") {
                                        Some(Value::String(start)) => Some(start),
                                        Some(v) => bail!("Expected string for deduplication_pad_start, got: {:?}", v),
                                        None => None
                                    };
                                    DebeziumDeduplicationStrategy::full_in_range(
                                        &start,
                                        &end,
                                        deduplication_pad_start.as_deref(),
                                    )
                                    .map_err(|e| {
                                        anyhow!("Unable to create deduplication strategy: {}", e)
                                    })?
                                }
                                (_, _) => bail!(
                                    "deduplication full_in_range requires both \
                                 'deduplication_start' and 'deduplication_end' parameters"
                                ),
                            }
                        }
                        _ => bail!(
                            "deduplication must be one of 'ordered' 'full', or 'full_in_range'."
                        ),
                    }
                }
                _ => bail!("deduplication must be one of 'ordered', 'full' or 'full_in_range'."),
            };
            let mode = match mode {
                sql_parser::ast::DbzMode::Plain => DebeziumMode::Plain,
                sql_parser::ast::DbzMode::Upsert => DebeziumMode::Upsert,
            };
            if mode == DebeziumMode::Upsert && dedup_strat != DebeziumDeduplicationStrategy::None {
                bail!("Debezium deduplication does not make sense with upsert sources");
            }
            SourceEnvelope::Debezium(dedup_strat, mode)
        }
        sql_parser::ast::Envelope::Upsert => match connector {
            // Currently the get_encoding function rewrites Formats with either a CSR config to be a
            // KeyValueDecoding, no other formats make sense.
            //
            // TODO(bwm): move key/value canonicalization entirely into the purify step, and turn
            // this and the related code in `get_encoding` into internal errors.
            CreateSourceConnector::Kafka { .. } => match format {
                CreateSourceFormat::KeyValue { .. } => SourceEnvelope::Upsert,
                CreateSourceFormat::Bare(Format::Avro(AvroSchema::Csr { .. })) => {
                    SourceEnvelope::Upsert
                }
                _ => bail_unsupported!(format!("upsert requires a key/value format: {:?}", format)),
            },
            _ => bail_unsupported!("upsert envelope for non-Kafka sources"),
        },
        sql_parser::ast::Envelope::CdcV2 => {
            if let CreateSourceConnector::AvroOcf { .. } = connector {
                // TODO[btv] - there is no fundamental reason not to support this eventually,
                // but OCF goes through a separate pipeline that it hasn't been implemented for.
                bail_unsupported!("ENVELOPE MATERIALIZE over OCF (Avro files)")
            }
            match format {
                CreateSourceFormat::Bare(Format::Avro(_)) => {}
                _ => bail_unsupported!("non-Avro-encoded ENVELOPE MATERIALIZE"),
            }
            SourceEnvelope::CdcV2
        }
    };

    if matches!(envelope, SourceEnvelope::Upsert) {
        match &encoding {
            SourceDataEncoding::Single(_) => {
                bail_unsupported!("upsert envelopes must have a key")
            }
            SourceDataEncoding::KeyValue { .. } => (),
        }
    }

    let mut bare_desc =
        if let ExternalSourceConnector::Kafka(KafkaSourceConnector { include_key, .. }) =
            &external_connector
        {
            encoding.desc(&envelope, include_key.as_ref())?
        } else {
            encoding.desc(&envelope, None)?
        };
    let ignore_source_keys = match with_options.remove("ignore_source_keys") {
        None => false,
        Some(Value::Boolean(b)) => b,
        Some(_) => bail!("ignore_source_keys must be a boolean"),
    };
    if ignore_source_keys {
        bare_desc = bare_desc.without_keys();
    }

    let post_transform_key = if let SourceEnvelope::Debezium(_, _) = &envelope {
        if let SourceDataEncoding::KeyValue {
            key: DataEncoding::Avro(AvroEncoding { schema, .. }),
            ..
        } = &encoding
        {
            if ignore_source_keys {
                None
            } else {
                match &bare_desc.typ().column_types[0].scalar_type {
                    ScalarType::Record { fields, .. } => {
                        let row_desc = RelationDesc::from_names_and_types(
                            fields.clone().into_iter().map(|(n, t)| (Some(n), t)),
                        );
                        let key_schema_indices = match avro::validate_key_schema(&schema, &row_desc)
                        {
                            Err(e) => bail!("Cannot use key due to error: {}", e),
                            Ok(indices) => Some(indices),
                        };
                        key_schema_indices
                    }
                    _ => {
                        error!("Not using key: expected `before` record in first column");
                        None
                    }
                }
            }
        } else {
            None
        }
    } else {
        None
    };

    bare_desc =
        plan_utils::maybe_rename_columns(format!("source {}", name), bare_desc, &col_names)?;

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
                let name_idx = bare_desc
                    .get_by_name(col)
                    .map(|(idx, _type)| idx)
                    .ok_or_else(|| anyhow!("No such column in source key constraint: {}", col))?;
                if bare_desc.get_unambiguous_name(name_idx).is_none() {
                    bail!("Ambiguous column in source key constraint: {}", col);
                }
                Ok(name_idx)
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !bare_desc.typ().keys.is_empty() {
            return Err(key_constraint_err(&bare_desc, &key_columns));
        } else {
            bare_desc = bare_desc.with_key(key_indices);
        }
    }

    // TODO(brennan): They should not depend on the envelope either. Figure out a way to
    // make all of this more tasteful.
    if !matches!(encoding.value_ref(), DataEncoding::Avro { .. })
        && !matches!(envelope, SourceEnvelope::Debezium(_, _))
    {
        for (name, ty) in external_connector.metadata_columns() {
            bare_desc = bare_desc.with_named_column(name, ty);
        }
    }

    let if_not_exists = *if_not_exists;
    let materialized = *materialized;
    let name = scx.allocate_name(normalize::unresolved_object_name(name.clone())?);
    let create_sql = normalize::create_statement(&scx, Statement::CreateSource(stmt))?;

    // Allow users to specify a timeline. If they do not, determine a default timeline for the source.
    let timeline = if let Some(timeline) = with_options.remove("timeline") {
        match timeline {
            Value::String(timeline) => Timeline::User(timeline),
            v => bail!("unsupported timeline value {}", v.to_ast_string()),
        }
    } else {
        match (&consistency, &envelope) {
            (_, SourceEnvelope::CdcV2) => match with_options.remove("epoch_ms_timeline") {
                None => Timeline::External(name.to_string()),
                Some(Value::Boolean(true)) => Timeline::EpochMilliseconds,
                Some(v) => bail!("unsupported epoch_ms_timeline value {}", v),
            },
            (Consistency::RealTime, _) => Timeline::EpochMilliseconds,
            (Consistency::BringYourOwn(byo), _) => Timeline::Counter(byo.clone()),
        }
    };

    let (expr, column_names) = plan_source_envelope(&bare_desc, &envelope, post_transform_key)?;
    let source = Source {
        create_sql,
        connector: SourceConnector::External {
            connector: external_connector,
            encoding,
            envelope,
            consistency,
            ts_frequency,
            timeline,
        },
        expr,
        bare_desc,
        column_names,
    };

    if !with_options.is_empty() {
        bail!(
            "unexpected parameters for CREATE SOURCE: {}",
            with_options.keys().join(",")
        )
    }

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source,
        if_not_exists,
        materialized,
    }))
}

fn get_encoding<T: sql_parser::ast::AstInfo>(
    format: &CreateSourceFormat<Raw>,
    envelope: &Envelope,
    with_options: &Vec<SqlOption<T>>,
) -> Result<SourceDataEncoding, anyhow::Error> {
    let encoding = match format {
        CreateSourceFormat::None => bail!("Source format must be specified"),
        CreateSourceFormat::Bare(format) => get_encoding_inner(format, with_options)?,
        CreateSourceFormat::KeyValue { key, value } => {
            let key = match get_encoding_inner(key, with_options)? {
                SourceDataEncoding::Single(key) => key,
                SourceDataEncoding::KeyValue { key, .. } => key,
            };
            let value = match get_encoding_inner(value, with_options)? {
                SourceDataEncoding::Single(value) => value,
                SourceDataEncoding::KeyValue { value, .. } => value,
            };
            SourceDataEncoding::KeyValue { key, value }
        }
    };

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

fn get_encoding_inner<T: sql_parser::ast::AstInfo>(
    format: &Format<Raw>,
    with_options: &Vec<SqlOption<T>>,
) -> Result<SourceDataEncoding, anyhow::Error> {
    // Avro/CSR can return a `SourceDataEncoding::KeyValue`
    Ok(SourceDataEncoding::Single(match format {
        Format::Bytes => DataEncoding::Bytes,
        Format::Avro(schema) => {
            let Schema {
                key_schema,
                value_schema,
                schema_registry_config,
                confluent_wire_format,
            } = match schema {
                // TODO(jldlaughlin): we need a way to pass in primary key information
                // when building a source from a string or file.
                AvroSchema::InlineSchema {
                    schema: sql_parser::ast::Schema::Inline(schema),
                    with_options,
                } => {
                    with_options! {
                        struct ConfluentMagic {
                            confluent_wire_format: bool,
                        }
                    }

                    Schema {
                        key_schema: None,
                        value_schema: schema.clone(),
                        schema_registry_config: None,
                        confluent_wire_format: ConfluentMagic::try_from(with_options.clone())?
                            .confluent_wire_format
                            .unwrap_or(true),
                    }
                }
                AvroSchema::InlineSchema {
                    schema: sql_parser::ast::Schema::File(_),
                    ..
                } => {
                    unreachable!("File schema should already have been inlined")
                }
                AvroSchema::Csr {
                    csr_connector:
                        CsrConnectorAvro {
                            url,
                            seed,
                            with_options: ccsr_options,
                        },
                } => {
                    let ccsr_config = kafka_util::generate_ccsr_client_config(
                        url.parse()?,
                        &kafka_util::extract_config(&mut normalize::options(with_options))?,
                        normalize::options(&ccsr_options),
                    )?;
                    if let Some(seed) = seed {
                        Schema {
                            key_schema: seed.key_schema.clone(),
                            value_schema: seed.value_schema.clone(),
                            schema_registry_config: Some(ccsr_config),
                            confluent_wire_format: true,
                        }
                    } else {
                        unreachable!("CSR seed resolution should already have been called: Avro")
                    }
                }
            };

            if let Some(key_schema) = key_schema {
                return Ok(SourceDataEncoding::KeyValue {
                    key: DataEncoding::Avro(AvroEncoding {
                        schema: key_schema,
                        schema_registry_config: schema_registry_config.clone(),
                        confluent_wire_format,
                    }),
                    value: DataEncoding::Avro(AvroEncoding {
                        schema: value_schema,
                        schema_registry_config,
                        confluent_wire_format,
                    }),
                });
            } else {
                DataEncoding::Avro(AvroEncoding {
                    schema: value_schema,
                    schema_registry_config,
                    confluent_wire_format,
                })
            }
        }
        Format::Protobuf(schema) => match schema {
            ProtobufSchema::Csr {
                csr_connector: CsrConnectorProto { seed, .. },
            } => {
                if let Some(CsrSeedCompiledOrLegacy::Compiled(CsrSeedCompiled { key, value })) =
                    seed
                {
                    let value = DataEncoding::Protobuf(ProtobufEncoding {
                        descriptors: strconv::parse_bytes(&value.schema)?,
                        message_name: value.message_name.clone(),
                    });
                    if let Some(key) = key {
                        return Ok(SourceDataEncoding::KeyValue {
                            key: DataEncoding::Protobuf(ProtobufEncoding {
                                descriptors: strconv::parse_bytes(&key.schema)?,
                                message_name: key.message_name.clone(),
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
                    sql_parser::ast::Schema::Inline(bytes) => strconv::parse_bytes(&bytes)?,
                    sql_parser::ast::Schema::File(_) => {
                        unreachable!("File schema should already have been inlined")
                    }
                };

                DataEncoding::Protobuf(ProtobufEncoding {
                    descriptors,
                    message_name: message_name.to_owned(),
                })
            }
        },
        Format::Regex(regex) => {
            let regex = Regex::new(&regex)?;
            DataEncoding::Regex(RegexEncoding { regex })
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
            DataEncoding::Csv(CsvEncoding {
                columns,
                delimiter: match *delimiter as u32 {
                    0..=127 => *delimiter as u8,
                    _ => bail!("CSV delimiter must be an ASCII character"),
                },
            })
        }
        Format::Json => bail_unsupported!("JSON sources"),
        Format::Text => DataEncoding::Text,
    }))
}

fn get_key_envelope(
    name: Option<Ident>,
    envelope: &Envelope,
    encoding: &SourceDataEncoding,
) -> Result<KeyEnvelope, anyhow::Error> {
    if matches!(envelope, Envelope::Debezium { .. }) {
        bail!("Cannot use INCLUDE KEY with ENVELOPE DEBEZIUM: Debezium values include all keys.");
    }
    Ok(match name {
        Some(name) => KeyEnvelope::Named(name.into_string()),
        None if matches!(envelope, Envelope::Upsert { .. }) => KeyEnvelope::LegacyUpsert,
        None => {
            // If the key is requested but comes from an unnamed type then it gets the name "key"
            //
            // Otherwise it gets the names of the columns in the type
            if let SourceDataEncoding::KeyValue { key, value: _ } = encoding {
                let is_composite = match key {
                    DataEncoding::AvroOcf { .. } | DataEncoding::Postgres => {
                        bail!("{} sources cannot use INCLUDE KEY", key.op_name())
                    }
                    DataEncoding::Bytes | DataEncoding::Text => false,
                    DataEncoding::Avro(_)
                    | DataEncoding::Csv(_)
                    | DataEncoding::Protobuf(_)
                    | DataEncoding::Regex { .. } => true,
                };

                if is_composite {
                    KeyEnvelope::Flattened
                } else {
                    KeyEnvelope::Named("key".to_string())
                }
            } else {
                bail!("INCLUDE KEY requires an explicit or implicit KEY FORMAT")
            }
        }
    })
}

pub fn describe_create_view(
    _: &StatementContext,
    _: CreateViewStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_view(
    scx: &StatementContext,
    def: &mut ViewDefinition<Raw>,
    params: &Params,
    temporary: bool,
) -> Result<(FullName, View), anyhow::Error> {
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
        with_options,
    } = def;

    if !with_options.is_empty() {
        bail_unsupported!("WITH options");
    }
    let query::PlannedQuery {
        mut expr,
        mut desc,
        finishing,
        depends_on,
    } = query::plan_root_query(scx, query.clone(), QueryLifetime::Static)?;
    expr.bind_parameters(&params)?;
    //TODO: materialize#724 - persist finishing information with the view?
    expr.finish(finishing);
    let relation_expr = expr.lower();

    let name = if temporary {
        scx.allocate_temporary_name(normalize::unresolved_object_name(name.to_owned())?)
    } else {
        scx.allocate_name(normalize::unresolved_object_name(name.to_owned())?)
    };

    desc = plan_utils::maybe_rename_columns(format!("view {}", name), desc, &columns)?;

    let view = View {
        create_sql,
        expr: relation_expr,
        column_names: desc.iter_names().map(|n| n.cloned()).collect(),
        temporary,
        depends_on,
    };

    Ok((name, view))
}

pub fn plan_create_view(
    scx: &StatementContext,
    mut stmt: CreateViewStatement<Raw>,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let CreateViewStatement {
        temporary,
        materialized,
        if_exists,
        definition,
    } = &mut stmt;
    let (name, view) = plan_view(scx, definition, params, *temporary)?;
    let replace = if *if_exists == IfExistsBehavior::Replace {
        if let Ok(item) = scx.catalog.resolve_item(&name.clone().into()) {
            if view.expr.global_uses().contains(&item.id()) {
                bail!(
                    "cannot replace view {0}: depended upon by new {0} definition",
                    item.name()
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
    _: CreateViewsStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_views(
    scx: &StatementContext,
    CreateViewsStatement {
        definitions,
        if_exists,
        materialized,
        temporary,
    }: CreateViewsStatement<Raw>,
) -> Result<Plan, anyhow::Error> {
    match definitions {
        CreateViewsDefinitions::Literal(view_definitions) => {
            let mut views = Vec::with_capacity(view_definitions.len());
            for mut definition in view_definitions {
                let view = plan_view(scx, &mut definition, &Params::empty(), temporary)?;
                views.push(view);
            }
            Ok(Plan::CreateViews(CreateViewsPlan {
                views,
                if_not_exists: if_exists == IfExistsBehavior::Skip,
                materialize: materialized,
            }))
        }
        CreateViewsDefinitions::Source { .. } => bail!("cannot create view from source"),
    }
}

#[allow(clippy::too_many_arguments)]
fn kafka_sink_builder(
    format: Option<Format<Raw>>,
    consistency: Option<KafkaConsistency<Raw>>,
    with_options: &mut BTreeMap<String, Value>,
    broker: String,
    topic_prefix: String,
    relation_key_indices: Option<Vec<usize>>,
    key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    value_desc: RelationDesc,
    topic_suffix_nonce: String,
    root_dependencies: &[&dyn CatalogItem],
) -> Result<SinkConnectorBuilder, anyhow::Error> {
    let consistency_topic = match with_options.remove("consistency_topic") {
        None => None,
        Some(Value::String(topic)) => Some(topic),
        Some(_) => bail!("consistency_topic must be a string"),
    };
    if consistency_topic.is_some() && consistency.is_some() {
        // We're keeping consistency_topic around for backwards compatibility. Users
        // should not be able to specify consistency_topic and the newer CONSISTENCY options.
        bail!("Cannot specify consistency_topic and CONSISTENCY options simultaneously");
    }
    let reuse_topic = match with_options.remove("reuse_topic") {
        Some(Value::Boolean(b)) => b,
        None => false,
        Some(_) => bail!("reuse_topic must be a boolean"),
    };
    let config_options = kafka_util::extract_config(with_options)?;

    let format = match format {
        Some(Format::Avro(AvroSchema::Csr {
            csr_connector:
                CsrConnectorAvro {
                    url,
                    seed,
                    with_options,
                },
        })) => {
            if seed.is_some() {
                bail!("SEED option does not make sense with sinks");
            }
            let schema_registry_url = url.parse::<Url>()?;
            let ccsr_with_options = normalize::options(&with_options);
            let ccsr_config = kafka_util::generate_ccsr_client_config(
                schema_registry_url.clone(),
                &config_options,
                ccsr_with_options,
            )?;

            let include_transaction =
                reuse_topic || consistency_topic.is_some() || consistency.is_some();
            let schema_generator = AvroSchemaGenerator::new(
                key_desc_and_indices
                    .as_ref()
                    .map(|(desc, _indices)| desc.clone()),
                value_desc.clone(),
                include_transaction,
            );
            let value_schema = schema_generator.value_writer_schema().to_string();
            let key_schema = schema_generator
                .key_writer_schema()
                .map(|key_schema| key_schema.to_string());

            KafkaSinkFormat::Avro {
                schema_registry_url,
                key_schema,
                value_schema,
                ccsr_config,
            }
        }
        Some(Format::Json) => KafkaSinkFormat::Json,
        Some(format) => bail_unsupported!(format!("sink format {:?}", format)),
        None => bail_unsupported!("sink without format"),
    };

    let consistency_config = get_kafka_sink_consistency_config(
        &topic_prefix,
        &format,
        &config_options,
        reuse_topic,
        consistency,
        consistency_topic,
    )?;

    let broker_addrs = broker.parse()?;

    let transitive_source_dependencies: Vec<_> = if reuse_topic {
        for item in root_dependencies.iter() {
            if item.item_type() == CatalogItemType::Source {
                if !item.source_connector()?.yields_stable_input() {
                    bail!(
                    "reuse_topic requires that sink input dependencies are replayable, {} is not",
                    item.name()
                );
                }
            } else if item.item_type() != CatalogItemType::Source {
                bail!(
                    "reuse_topic requires that sink input dependencies are sources, {} is not",
                    item.name()
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
        Some(Value::Number(n)) => n.parse::<i32>()?,
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
        Some(Value::Number(n)) => n.parse::<i32>()?,
        Some(_) => bail!("replication factor for sink topics must be an integer"),
    };

    if replication_factor == 0 || replication_factor < -1 {
        bail!(
            "replication factor for sink topics must be a positive integer or -1 for broker default"
        );
    }

    let retention_ms = match with_options.remove("retention_ms") {
        None => None,
        Some(Value::Number(n)) => Some(n.parse::<i64>()?),
        Some(_) => bail!("retention ms for sink topics must be an integer"),
    };

    if retention_ms.unwrap_or(0) < -1 {
        bail!("retention ms for sink topics must be greater than or equal to -1");
    }

    let retention_bytes = match with_options.remove("retention_bytes") {
        None => None,
        Some(Value::Number(n)) => Some(n.parse::<i64>()?),
        Some(_) => bail!("retention bytes for sink topics must be an integer"),
    };

    if retention_bytes.unwrap_or(0) < -1 {
        bail!("retention bytes for sink topics must be greater than or equal to -1");
    }
    let retention = KafkaSinkConnectorRetention {
        retention_ms,
        retention_bytes,
    };

    let consistency_topic = consistency_config.clone().map(|config| config.0);
    let consistency_format = consistency_config.map(|config| config.1);

    Ok(SinkConnectorBuilder::Kafka(KafkaSinkConnectorBuilder {
        broker_addrs,
        format,
        topic_prefix,
        consistency_topic_prefix: consistency_topic,
        consistency_format,
        topic_suffix_nonce,
        partition_count,
        replication_factor,
        fuel: 10000,
        config_options,
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
    config_options: &BTreeMap<String, String>,
    reuse_topic: bool,
    consistency: Option<KafkaConsistency<Raw>>,
    consistency_topic: Option<String>,
) -> Result<Option<(String, KafkaSinkFormat)>, anyhow::Error> {
    let result = match consistency {
        Some(KafkaConsistency {
            topic,
            topic_format,
        }) => match topic_format {
            Some(Format::Avro(AvroSchema::Csr {
                csr_connector:
                    CsrConnectorAvro {
                        url,
                        seed,
                        with_options,
                    },
            })) => {
                if seed.is_some() {
                    bail!("SEED option does not make sense with sinks");
                }
                let schema_registry_url = url.parse::<Url>()?;
                let ccsr_with_options = normalize::options(&with_options);
                let ccsr_config = kafka_util::generate_ccsr_client_config(
                    schema_registry_url.clone(),
                    config_options,
                    ccsr_with_options,
                )?;

                Some((
                    topic,
                    KafkaSinkFormat::Avro {
                        schema_registry_url,
                        key_schema: None,
                        value_schema: avro::get_debezium_transaction_schema().canonical_form(),
                        ccsr_config,
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
                        schema_registry_url,
                        ccsr_config,
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
                                schema_registry_url: schema_registry_url.clone(),
                                key_schema: None,
                                value_schema: avro::get_debezium_transaction_schema()
                                    .canonical_form(),
                                ccsr_config: ccsr_config.clone(),
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

fn avro_ocf_sink_builder(
    format: Option<Format<Raw>>,
    path: String,
    file_name_suffix: String,
    value_desc: RelationDesc,
) -> Result<SinkConnectorBuilder, anyhow::Error> {
    if format.is_some() {
        bail!("avro ocf sinks cannot specify a format");
    }

    let path = PathBuf::from(path);

    if path.is_dir() {
        bail!("avro ocf sink cannot write to a directory");
    }

    Ok(SinkConnectorBuilder::AvroOcf(AvroOcfSinkConnectorBuilder {
        path,
        file_name_suffix,
        value_desc,
    }))
}

pub fn describe_create_sink(
    _: &StatementContext,
    _: CreateSinkStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_sink(
    scx: &StatementContext,
    stmt: CreateSinkStatement<Raw>,
) -> Result<Plan, anyhow::Error> {
    let create_sql = normalize::create_statement(scx, Statement::CreateSink(stmt.clone()))?;
    let CreateSinkStatement {
        name,
        from,
        connector,
        with_options,
        format,
        envelope,
        with_snapshot,
        as_of,
        if_not_exists,
    } = stmt;

    let envelope = match envelope {
        None | Some(Envelope::Debezium(sql_parser::ast::DbzMode::Plain)) => SinkEnvelope::Debezium,
        Some(Envelope::Upsert) => SinkEnvelope::Upsert,
        Some(Envelope::CdcV2) => bail_unsupported!("CDCv2 sinks"),
        Some(Envelope::Debezium(sql_parser::ast::DbzMode::Upsert)) => {
            bail_unsupported!("UPSERT doesn't make sense for sinks")
        }
        Some(Envelope::None) => bail_unsupported!("\"ENVELOPE NONE\" sinks"),
    };
    let name = scx.allocate_name(normalize::unresolved_object_name(name)?);
    let from = scx.resolve_item(from)?;
    let suffix_nonce = format!(
        "{}-{}",
        scx.catalog.config().start_time.timestamp(),
        scx.catalog.config().nonce
    );

    let mut with_options = normalize::options(&with_options);

    let desc = from.desc()?;
    let key_indices = match &connector {
        CreateSinkConnector::Kafka { key, .. } => {
            if let Some(key) = key.clone() {
                let key = key
                    .into_iter()
                    .map(normalize::column_name)
                    .collect::<Vec<_>>();
                let mut uniq = HashSet::new();
                for col in key.iter() {
                    if !uniq.insert(col) {
                        bail!("Repeated column name in sink key: {}", col);
                    }
                }
                let indices = key
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
                if !is_valid_key && envelope == SinkEnvelope::Upsert {
                    return Err(invalid_upsert_key_err(&desc, &key));
                }
                Some(indices)
            } else {
                None
            }
        }
        CreateSinkConnector::AvroOcf { .. } => None,
    };

    // pick the first valid natural relation key, if any
    let relation_key_indices = desc.typ().keys.get(0).cloned();

    let key_desc_and_indices = key_indices.map(|key_indices| {
        let cols = desc.clone().into_iter().collect::<Vec<_>>();
        let (names, types): (Vec<_>, Vec<_>) =
            key_indices.iter().map(|&idx| cols[idx].clone()).unzip();
        let typ = RelationType::new(types);
        (RelationDesc::new(typ, names), key_indices)
    });

    if key_desc_and_indices.is_none() && envelope == SinkEnvelope::Upsert {
        return Err(PlanError::UpsertSinkWithoutKey.into());
    }

    let value_desc = match envelope {
        SinkEnvelope::Debezium => envelopes::dbz_desc(desc.clone()),
        SinkEnvelope::Upsert => desc.clone(),
    };

    if as_of.is_some() {
        bail!("CREATE SINK ... AS OF is no longer supported");
    }

    let mut depends_on = vec![from.id()];
    depends_on.extend(from.uses());

    let root_user_dependencies = get_root_dependencies(scx, &depends_on);

    let connector_builder = match connector {
        CreateSinkConnector::Kafka {
            broker,
            topic,
            consistency,
            ..
        } => kafka_sink_builder(
            format,
            consistency,
            &mut with_options,
            broker,
            topic,
            relation_key_indices,
            key_desc_and_indices,
            value_desc,
            suffix_nonce,
            &root_user_dependencies,
        )?,
        CreateSinkConnector::AvroOcf { path } => {
            avro_ocf_sink_builder(format, path, suffix_nonce, value_desc)?
        }
    };

    if !with_options.is_empty() {
        bail!(
            "unexpected parameters for CREATE SINK: {}",
            with_options.keys().join(",")
        )
    }

    Ok(Plan::CreateSink(CreateSinkPlan {
        name,
        sink: Sink {
            create_sql,
            from: from.id(),
            connector_builder,
            envelope,
            depends_on,
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
                    .map(|col| desc.get_name(*col).expect("known to exist").as_str())
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
                .map(|col| desc.get_name(*col).expect("known to exist").as_str())
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
        let item = scx.get_item_by_id(&dep);
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
    _: CreateIndexStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_index(
    scx: &StatementContext,
    mut stmt: CreateIndexStatement<Raw>,
) -> Result<Plan, anyhow::Error> {
    let CreateIndexStatement {
        name,
        on_name,
        key_parts,
        with_options,
        if_not_exists,
    } = &mut stmt;
    let on = scx.resolve_item(on_name.clone())?;

    if CatalogItemType::View != on.item_type()
        && CatalogItemType::Source != on.item_type()
        && CatalogItemType::Table != on.item_type()
    {
        bail!(
            "index cannot be created on {} because it is a {}",
            on.name(),
            on.item_type()
        )
    }

    let on_desc = on.desc()?;

    let filled_key_parts = match key_parts {
        Some(kp) => kp.to_vec(),
        None => {
            // `key_parts` is None if we're creating a "default" index, i.e.
            // creating the index as if the index had been created alongside the
            // view source, e.g. `CREATE MATERIALIZED...`
            on.desc()?
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
    let (keys, exprs_depend_on) = query::plan_index_exprs(scx, on_desc, filled_key_parts.clone())?;

    let index_name = if let Some(name) = name {
        FullName {
            database: on.name().database.clone(),
            schema: on.name().schema.clone(),
            item: normalize::ident(name.clone()),
        }
    } else {
        let mut idx_name = on.name().clone();
        if key_parts.is_none() {
            // We're trying to create the "default" index.
            idx_name.item += "_primary_idx";
        } else {
            // Use PG schema for automatically naming indexes:
            // `<table>_<_-separated indexed expressions>_idx`
            let index_name_col_suffix = keys
                .iter()
                .map(|k| match k {
                    expr::MirScalarExpr::Column(i) => match on_desc.get_unambiguous_name(*i) {
                        Some(col_name) => col_name.to_string(),
                        None => format!("{}", i + 1),
                    },
                    _ => "expr".to_string(),
                })
                .join("_");
            idx_name.item += &format!("_{}_idx", index_name_col_suffix);
            idx_name.item = normalize::ident(Ident::new(idx_name.item))
        }
        if !*if_not_exists {
            scx.catalog.find_available_name(idx_name)
        } else {
            idx_name
        }
    };

    let options = plan_index_options(with_options.clone())?;

    // Normalize `stmt`.
    *name = Some(Ident::new(index_name.item.clone()));
    *key_parts = Some(filled_key_parts);
    let if_not_exists = *if_not_exists;
    let create_sql = normalize::create_statement(scx, Statement::CreateIndex(stmt))?;
    let mut depends_on = vec![on.id()];
    depends_on.extend(exprs_depend_on);

    Ok(Plan::CreateIndex(CreateIndexPlan {
        name: index_name,
        index: Index {
            create_sql,
            on: on.id(),
            keys,
            depends_on,
        },
        options,
        if_not_exists,
    }))
}

pub fn describe_create_type(
    _: &StatementContext,
    _: CreateTypeStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_type(
    scx: &StatementContext,
    stmt: CreateTypeStatement<Raw>,
) -> Result<Plan, anyhow::Error> {
    let create_sql = normalize::create_statement(scx, Statement::CreateType(stmt.clone()))?;
    let CreateTypeStatement {
        name,
        as_type,
        with_options,
    } = stmt;

    let mut with_options = normalize::option_objects(&with_options);

    let option_keys = match as_type {
        CreateTypeAs::List => vec!["element_type"],
        CreateTypeAs::Map => vec!["key_type", "value_type"],
    };

    let mut ids = vec![];
    for key in option_keys {
        let item_name = match with_options.remove(&key.to_string()) {
            Some(SqlOption::DataType { data_type, .. }) => match data_type {
                DataType::Other { name, typ_mod } => {
                    if !typ_mod.is_empty() {
                        bail!(
                            "CREATE TYPE ... AS {}option {} cannot accept type modifier on \
                            {}, you must use the default type",
                            as_type.to_string().quoted(),
                            key,
                            name
                        )
                    }
                    name
                }
                d => bail!(
                    "CREATE TYPE ... AS {}option {} can only use named data types, but \
                    found unnamed data type {}. Use CREATE TYPE to create a named type first",
                    as_type.to_string().quoted(),
                    key,
                    d.to_ast_string(),
                ),
            },
            Some(_) => bail!("{} must be a data type", key),
            None => bail!("{} parameter required", key),
        };
        let item = scx
            .catalog
            .resolve_item(&normalize::unresolved_object_name(
                item_name.name().clone(),
            )?)?;
        let item_id = item.id();
        match scx.catalog.try_get_lossy_scalar_type_by_id(&item_id) {
            None => bail!(
                "{} must be of class type, but received {} which is of class {}",
                key,
                item.name(),
                item.item_type()
            ),
            Some(ScalarType::Char { .. }) if as_type == CreateTypeAs::List => {
                bail_unsupported!("char list")
            }
            _ => {}
        }
        ids.push(item_id);
    }

    if !with_options.is_empty() {
        bail!(
            "unexpected parameters for CREATE TYPE: {}",
            with_options.keys().join(",")
        )
    }

    let name = scx.allocate_name(normalize::unresolved_object_name(name)?);
    if scx.catalog.item_exists(&name) {
        bail!("catalog item {} already exists", name.to_string().quoted());
    }

    let inner = match as_type {
        CreateTypeAs::List => TypeInner::List {
            element_id: *ids.get(0).expect("custom type to have element id"),
        },
        CreateTypeAs::Map => {
            let key_id = *ids.get(0).expect("key");
            match scx.catalog.try_get_lossy_scalar_type_by_id(&key_id) {
                Some(ScalarType::String) => {}
                Some(t) => bail!(
                    "key_type must be text, got {}",
                    scx.humanize_scalar_type(&t)
                ),
                None => unreachable!("already guaranteed id correlates to a type"),
            }

            TypeInner::Map {
                key_id,
                value_id: *ids.get(1).expect("value"),
            }
        }
    };

    Ok(Plan::CreateType(CreateTypePlan {
        name,
        typ: Type {
            create_sql,
            inner,
            depends_on: ids,
        },
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
        if_exists,
        restrict,
    }: DropDatabaseStatement,
) -> Result<Plan, anyhow::Error> {
    let name = match scx.resolve_database_ident(name) {
        Ok(database) => {
            let name = String::from(database.name());
            if restrict && database.has_schemas() {
                bail!(
                    "database '{}' cannot be dropped with RESTRICT while it contains schemas",
                    database.name(),
                );
            }
            name
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the database
            // does not exist.
            //
            // TODO(benesch): adjust the type here so we can more clearly
            // indicate that we don't want to drop any database at all.
            String::new()
        }
        Err(err) => return Err(err.into()),
    };
    Ok(Plan::DropDatabase(DropDatabasePlan { name }))
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
        if_exists,
        names,
        cascade,
    }: DropObjectsStatement,
) -> Result<Plan, anyhow::Error> {
    if materialized {
        bail!(
            "DROP MATERIALIZED {0} is not allowed, use DROP {0}",
            object_type
        );
    }
    match object_type {
        ObjectType::Schema => plan_drop_schema(scx, if_exists, names, cascade),
        ObjectType::Source
        | ObjectType::Table
        | ObjectType::View
        | ObjectType::Index
        | ObjectType::Sink
        | ObjectType::Type => plan_drop_items(scx, object_type, if_exists, names, cascade),
        ObjectType::Role => plan_drop_role(scx, if_exists, names),
        ObjectType::Object => unreachable!("cannot drop generic OBJECT, must provide object type"),
    }
}

pub fn plan_drop_schema(
    scx: &StatementContext,
    if_exists: bool,
    names: Vec<UnresolvedObjectName>,
    cascade: bool,
) -> Result<Plan, anyhow::Error> {
    if names.len() != 1 {
        bail_unsupported!("DROP SCHEMA with multiple schemas");
    }
    match scx.resolve_schema(names.into_element()) {
        Ok(schema) => {
            if let DatabaseSpecifier::Ambient = schema.name().database {
                bail!(
                    "cannot drop schema {} because it is required by the database system",
                    schema.name()
                );
            }
            if !cascade && schema.has_items() {
                bail!(
                    "schema '{}' cannot be dropped without CASCADE while it contains objects",
                    schema.name(),
                );
            }
            Ok(Plan::DropSchema(DropSchemaPlan {
                name: schema.name().clone(),
            }))
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the
            // schema does not exist.
            // TODO(benesch): adjust the types here properly, rather than making
            // up a nonexistent database.
            Ok(Plan::DropSchema(DropSchemaPlan {
                name: SchemaName {
                    database: DatabaseSpecifier::Ambient,
                    schema: "noexist".into(),
                },
            }))
        }
        Err(e) => Err(e.into()),
    }
}

pub fn plan_drop_role(
    scx: &StatementContext,
    if_exists: bool,
    names: Vec<UnresolvedObjectName>,
) -> Result<Plan, anyhow::Error> {
    let mut out = vec![];
    for name in names {
        let name = if name.0.len() == 1 {
            normalize::ident(name.0.into_element())
        } else {
            bail!("invalid role name {}", name.to_string().quoted())
        };
        if name == scx.catalog.user() {
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

pub fn plan_drop_items(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    names: Vec<UnresolvedObjectName>,
    cascade: bool,
) -> Result<Plan, anyhow::Error> {
    let items = names
        .into_iter()
        .map(|n| scx.resolve_item(n))
        .collect::<Vec<_>>();
    let mut ids = vec![];
    for item in items {
        match item {
            Ok(item) => ids.extend(plan_drop_item(scx, object_type, item, cascade)?),
            Err(_) if if_exists => {
                // TODO(benesch): generate a notice indicating this
                // item does not exist.
            }
            Err(err) => return Err(err.into()),
        }
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
            catalog_entry.name(),
        );
    }
    if object_type != catalog_entry.item_type() {
        bail!("{} is not of type {}", catalog_entry.name(), object_type);
    }
    if !cascade {
        for id in catalog_entry.used_by() {
            let dep = scx.catalog.get_item_by_id(id);
            match object_type {
                ObjectType::Type => bail!(
                    "cannot drop {}: still depended upon by catalog item '{}'",
                    catalog_entry.name(),
                    dep.name()
                ),
                _ => match dep.item_type() {
                    CatalogItemType::Func
                    | CatalogItemType::Table
                    | CatalogItemType::Source
                    | CatalogItemType::View
                    | CatalogItemType::Sink
                    | CatalogItemType::Type => {
                        bail!(
                            "cannot drop {}: still depended upon by catalog item '{}'",
                            catalog_entry.name(),
                            dep.name()
                        );
                    }
                    CatalogItemType::Index => (),
                },
            }
        }
    }
    Ok(Some(catalog_entry.id()))
}

with_options! {
    struct IndexWithOptions {
        logical_compaction_window: String,
    }
}

pub fn describe_alter_index_options(
    _: &StatementContext,
    _: AlterIndexStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

fn plan_index_options(with_opts: Vec<WithOption>) -> Result<Vec<IndexOption>, anyhow::Error> {
    let with_opts = IndexWithOptions::try_from(with_opts)?;
    let mut out = vec![];

    match with_opts.logical_compaction_window.as_deref() {
        None => (),
        Some("off") => out.push(IndexOption::LogicalCompactionWindow(None)),
        Some(s) => {
            let window = Some(repr::util::parse_duration(s)?);
            out.push(IndexOption::LogicalCompactionWindow(window))
        }
    };

    Ok(out)
}

pub fn plan_alter_index_options(
    scx: &StatementContext,
    AlterIndexStatement {
        index_name,
        if_exists,
        action: actions,
    }: AlterIndexStatement,
) -> Result<Plan, anyhow::Error> {
    let entry = match scx.resolve_item(index_name) {
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
        bail!("{} is a {} not a index", entry.name(), entry.item_type())
    }
    let id = entry.id();

    match actions {
        AlterIndexAction::ResetOptions(options) => {
            let options = options
                .into_iter()
                .filter_map(|o| match normalize::ident(o).as_str() {
                    "logical_compaction_window" => Some(IndexOptionName::LogicalCompactionWindow),
                    // Follow Postgres and don't complain if unknown parameters
                    // are passed into `ALTER INDEX ... RESET`.
                    _ => None,
                })
                .collect();
            Ok(Plan::AlterIndexResetOptions(AlterIndexResetOptionsPlan {
                id,
                options,
            }))
        }
        AlterIndexAction::SetOptions(options) => {
            let options = plan_index_options(options)?;
            Ok(Plan::AlterIndexSetOptions(AlterIndexSetOptionsPlan {
                id,
                options,
            }))
        }
        AlterIndexAction::Enable => Ok(Plan::AlterIndexEnable(AlterIndexEnablePlan { id })),
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
        if_exists,
        to_item_name,
    }: AlterObjectRenameStatement,
) -> Result<Plan, anyhow::Error> {
    let id = match scx.resolve_item(name.clone()) {
        Ok(entry) => {
            if entry.item_type() != object_type {
                bail!("{} is a {} not a {}", name, entry.item_type(), object_type)
            }
            let mut proposed_name = name.0;
            let last = proposed_name.last_mut().unwrap();
            *last = to_item_name.clone();
            if scx
                .resolve_item(UnresolvedObjectName(proposed_name))
                .is_ok()
            {
                bail!("{} is already taken by item in schema", to_item_name)
            }
            entry.id()
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating this
            // item does not exist.
            return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
        }
        Err(err) => return Err(err.into()),
    };

    Ok(Plan::AlterItemRename(AlterItemRenamePlan {
        id,
        to_name: normalize::ident(to_item_name),
        object_type,
    }))
}
