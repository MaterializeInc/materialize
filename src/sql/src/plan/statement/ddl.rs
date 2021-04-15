// Copyright Materialize, Inc. All rights reserved.
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
use std::convert::TryFrom;
use std::iter;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail};
use aws_arn::ARN;
use expr::MirRelationExpr;
use expr::TableFunc;
use expr::UnaryFunc;
use globset::GlobBuilder;
use itertools::Itertools;
use log::error;
use log::warn;
use ore::str::StrExt;
use repr::ColumnName;
use repr::ColumnType;
use repr::Datum;
use repr::Row;

use reqwest::Url;

use dataflow_types::{
    AvroEncoding, AvroOcfEncoding, AvroOcfSinkConnectorBuilder, Consistency, CsvEncoding,
    DataEncoding, DebeziumMode, ExternalSourceConnector, FileSourceConnector,
    KafkaSinkConnectorBuilder, KafkaSourceConnector, KinesisSourceConnector,
    PostgresSourceConnector, ProtobufEncoding, PubNubSourceConnector, RegexEncoding,
    S3SourceConnector, SinkConnectorBuilder, SinkEnvelope, SourceConnector, SourceEnvelope,
};
use expr::GlobalId;
use interchange::avro::{self, DebeziumDeduplicationStrategy, Encoder};
use interchange::envelopes;
use ore::collections::CollectionExt;
use ore::iter::IteratorExt;
use regex::Regex;
use repr::{strconv, RelationDesc, RelationType, ScalarType};

use crate::ast::display::AstDisplay;
use crate::ast::{
    AlterIndexOptionsList, AlterIndexOptionsStatement, AlterObjectRenameStatement, AvroSchema,
    ColumnOption, Compression, Connector, CreateDatabaseStatement, CreateIndexStatement,
    CreateRoleOption, CreateRoleStatement, CreateSchemaStatement, CreateSinkStatement,
    CreateSourceStatement, CreateSourcesStatement, CreateTableStatement, CreateTypeAs,
    CreateTypeStatement, CreateViewStatement, DataType, DropDatabaseStatement,
    DropObjectsStatement, Envelope, Expr, Format, Ident, IfExistsBehavior, ObjectType, Raw,
    SqlOption, Statement, UnresolvedObjectName, Value, WithOption,
};
use crate::catalog::{CatalogItem, CatalogItemType};
use crate::kafka_util;
use crate::names::{DatabaseSpecifier, FullName, SchemaName};
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::expr::{ColumnRef, HirScalarExpr, JoinKind};
use crate::plan::query::{resolve_names_data_type, ExprContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::typeconv::{plan_hypothetical_cast, CastContext};
use crate::plan::{
    self, plan_utils, query, CreateSourcePlan, HirRelationExpr, Index, IndexOption,
    IndexOptionName, Params, Plan, QueryContext, Sink, Source, Table, Type, TypeInner, View,
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
    Ok(Plan::CreateDatabase {
        name: normalize::ident(name),
        if_not_exists,
    })
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
    Ok(Plan::CreateSchema {
        database_name,
        schema_name,
        if_not_exists,
    })
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
        unsupported!("WITH options");
    }
    if !constraints.is_empty() {
        unsupported!("CREATE TABLE with constraints")
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
                other => unsupported!(format!("CREATE TABLE with column constraint: {}", other)),
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
    };
    Ok(Plan::CreateTable {
        name,
        table,
        if_not_exists: *if_not_exists,
        depends_on,
    })
}

pub fn describe_create_source(
    _: &StatementContext,
    _: CreateSourceStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn describe_create_sources(
    _: &StatementContext,
    _: CreateSourcesStatement<Raw>,
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
                func: UnaryFunc::Not,
                expr: Box::new(HirScalarExpr::CallUnary {
                    func: UnaryFunc::IsNull,
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
        format,
        envelope,
        if_not_exists,
        materialized,
    } = &stmt;
    let get_encoding = |format: &Option<Format<Raw>>| {
        let format = format
            .as_ref()
            .ok_or_else(|| anyhow!("Source format must be specified"))?;

        Ok(match format {
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
                    AvroSchema::Schema {
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
                    AvroSchema::Schema {
                        schema: sql_parser::ast::Schema::File(_),
                        ..
                    } => {
                        unreachable!("File schema should already have been inlined")
                    }
                    AvroSchema::CsrUrl {
                        url,
                        seed,
                        with_options: ccsr_options,
                    } => {
                        let url: Url = url.parse()?;
                        let kafka_options =
                            kafka_util::extract_config(&mut normalize::options(with_options))?;
                        let ccsr_config = kafka_util::generate_ccsr_client_config(
                            url,
                            &kafka_options,
                            normalize::options(ccsr_options),
                        )?;

                        if let Some(seed) = seed {
                            Schema {
                                key_schema: seed.key_schema.clone(),
                                value_schema: seed.value_schema.clone(),
                                schema_registry_config: Some(ccsr_config),
                                confluent_wire_format: true,
                            }
                        } else {
                            unreachable!("CSR seed resolution should already have been called")
                        }
                    }
                };

                DataEncoding::Avro(AvroEncoding {
                    key_schema,
                    value_schema,
                    schema_registry_config,
                    confluent_wire_format,
                })
            }
            Format::Protobuf {
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
            Format::Regex(regex) => {
                let regex = Regex::new(regex)?;
                DataEncoding::Regex(RegexEncoding { regex })
            }
            Format::Csv {
                header_row,
                n_cols,
                delimiter,
            } => {
                let n_cols = if col_names.is_empty() {
                    match n_cols {
                        Some(n) => *n,
                        None => bail!(
                            "Cannot determine number of columns in CSV source; specify using \
                            CREATE SOURCE...FORMAT CSV WITH X COLUMNS"
                        ),
                    }
                } else {
                    col_names.len()
                };
                DataEncoding::Csv(CsvEncoding {
                    header_row: *header_row,
                    n_cols,
                    delimiter: match *delimiter as u32 {
                        0..=127 => *delimiter as u8,
                        _ => bail!("CSV delimiter must be an ASCII character"),
                    },
                })
            }
            Format::Json => unsupported!("JSON sources"),
            Format::Text => DataEncoding::Text,
        })
    };

    let mut with_options = normalize::options(with_options);

    let mut consistency = Consistency::RealTime;
    let mut ts_frequency = scx.catalog.config().timestamp_frequency;

    let (external_connector, mut encoding) = match connector {
        Connector::Kafka { broker, topic, .. } => {
            let config_options = kafka_util::extract_config(&mut with_options)?;

            consistency = match with_options.remove("consistency") {
                None => Consistency::RealTime,
                Some(Value::String(topic)) => Consistency::BringYourOwn(topic),
                Some(_) => bail!("consistency must be a string"),
            };

            let group_id_prefix = match with_options.remove("group_id_prefix") {
                None => None,
                Some(Value::String(s)) => Some(s),
                Some(_) => bail!("group_id_prefix must be a string"),
            };

            ts_frequency = extract_timestamp_frequency_option(
                scx.catalog.config().timestamp_frequency,
                &mut with_options,
            )?;

            // THIS IS EXPERIMENTAL - DO NOT DOCUMENT IT
            // until we have had time to think about what the right UX/design is on a non-urgent timeline!
            // By using this feature, you are opting in to not using updates or deletes in CDC sources, and
            // accepting panics if that constraint is violated.
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

            let enable_caching = match with_options.remove("cache") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("cache must be a bool!"),
            };

            if enable_caching && consistency != Consistency::RealTime {
                unsupported!("BYO source caching")
            }

            let connector = ExternalSourceConnector::Kafka(KafkaSourceConnector {
                addrs: broker.parse()?,
                topic: topic.clone(),
                config_options,
                start_offsets,
                group_id_prefix,
                cluster_id: scx.catalog.config().cluster_id,
                enable_caching,
                cached_files: None,
            });
            let encoding = get_encoding(format)?;

            if consistency != Consistency::RealTime
                && *envelope != sql_parser::ast::Envelope::Debezium(sql_parser::ast::DbzMode::Plain)
            {
                // TODO: does it make sense to support BYO with upsert? It doesn't seem obvious that
                // the timestamp topic will support the upsert semantics of the value topic
                bail!("BYO consistency only supported for plain Debezium Kafka sources");
            }

            (connector, encoding)
        }
        Connector::Kinesis { arn, .. } => {
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
            let encoding = get_encoding(format)?;
            (connector, encoding)
        }
        Connector::File { path, compression } => {
            let tail = match with_options.remove("tail") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("tail must be a boolean"),
            };
            consistency = match with_options.remove("consistency") {
                None => Consistency::RealTime,
                Some(_) => bail!("BYO consistency not supported for file sources"),
            };
            ts_frequency = extract_timestamp_frequency_option(
                scx.catalog.config().timestamp_frequency,
                &mut with_options,
            )?;

            let connector = ExternalSourceConnector::File(FileSourceConnector {
                path: path.clone().into(),
                compression: match compression {
                    Compression::Gzip => dataflow_types::Compression::Gzip,
                    Compression::None => dataflow_types::Compression::None,
                },
                tail,
            });
            let encoding = get_encoding(format)?;
            (connector, encoding)
        }
        Connector::S3 {
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
            let encoding = get_encoding(format)?;
            (connector, encoding)
        }
        Connector::Postgres {
            conn,
            publication,
            namespace,
            table,
            columns,
        } => {
            scx.require_experimental_mode("Postgres Sources")?;

            let qcx = QueryContext::root(scx, QueryLifetime::Static);
            let cast_desc = RelationDesc::empty();
            let ecx = ExprContext {
                qcx: &qcx,
                name: "postgres column cast",
                scope: &Scope::empty(None),
                relation_type: &cast_desc.typ(),
                allow_aggregates: false,
                allow_subqueries: true,
            };

            // Build the expected relation description
            let col_names: Vec<_> = columns
                .iter()
                .map(|c| Some(normalize::column_name(c.name.clone())))
                .collect();

            let mut col_types = vec![];
            let mut cast_exprs = vec![];
            for c in columns {
                if let Some(collation) = &c.collation {
                    unsupported!(format!(
                        "CREATE SOURCE FROM POSTGRES with column collation: {}",
                        collation
                    ));
                }

                let (aug_data_type, _ids) = resolve_names_data_type(scx, c.data_type.clone())?;
                let scalar_ty = plan::scalar_type_from_sql(scx, &aug_data_type)?;

                let mut nullable = true;
                for option in &c.options {
                    match &option.option {
                        ColumnOption::Null => (),
                        ColumnOption::NotNull => nullable = false,
                        other => unsupported!(format!(
                            "CREATE SOURCE FROM POSTGRES with column constraint: {}",
                            other
                        )),
                    }
                }

                let cast_expr = plan_hypothetical_cast(
                    &ecx,
                    CastContext::Explicit,
                    &ScalarType::String,
                    &scalar_ty,
                )
                .unwrap();

                col_types.push(scalar_ty.nullable(nullable));
                cast_exprs.push(cast_expr);
            }

            let desc = RelationDesc::new(RelationType::new(col_types), col_names);

            let connector = ExternalSourceConnector::Postgres(PostgresSourceConnector {
                conn: conn.clone(),
                publication: publication.clone(),
                namespace: namespace.clone(),
                table: table.clone(),
                cast_exprs,
            });

            (connector, DataEncoding::Postgres(desc))
        }
        Connector::PubNub {
            subscribe_key,
            channel,
        } => {
            match format {
                None | Some(Format::Text) => (),
                _ => bail!("CREATE SOURCE ... PUBNUB must specify FORMAT TEXT"),
            }
            let connector = ExternalSourceConnector::PubNub(PubNubSourceConnector {
                subscribe_key: subscribe_key.clone(),
                channel: channel.clone(),
            });
            (connector, DataEncoding::Text)
        }
        Connector::AvroOcf { path, .. } => {
            let tail = match with_options.remove("tail") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("tail must be a boolean"),
            };
            consistency = match with_options.remove("consistency") {
                None => Consistency::RealTime,
                Some(Value::String(topic)) => Consistency::BringYourOwn(topic),
                Some(_) => bail!("consistency must be a string"),
            };

            if consistency != Consistency::RealTime
                && *envelope != sql_parser::ast::Envelope::Debezium(sql_parser::ast::DbzMode::Plain)
            {
                bail!("BYO consistency only supported for Debezium Avro OCF sources");
            }

            ts_frequency = extract_timestamp_frequency_option(
                scx.catalog.config().timestamp_frequency,
                &mut with_options,
            )?;

            let connector = ExternalSourceConnector::AvroOcf(FileSourceConnector {
                path: path.clone().into(),
                compression: dataflow_types::Compression::None,
                tail,
            });
            if format.is_some() {
                bail!("avro ocf sources cannot specify a format");
            }
            let reader_schema = match with_options
                .remove("reader_schema")
                .expect("purification guarantees presence of reader_schema")
            {
                Value::String(s) => s,
                _ => bail!("reader_schema option must be a string"),
            };
            let encoding = DataEncoding::AvroOcf(AvroOcfEncoding { reader_schema });
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
        sql_parser::ast::Envelope::Upsert(key_format) => match connector {
            Connector::Kafka { .. } => {
                let mut key_encoding = if key_format.is_some() {
                    get_encoding(key_format)?
                } else {
                    encoding.clone()
                };
                match &mut key_encoding {
                    DataEncoding::Avro(AvroEncoding {
                        key_schema,
                        value_schema,
                        ..
                    }) => {
                        if key_schema.is_some() {
                            *value_schema = key_schema.take().unwrap();
                        }
                    }
                    DataEncoding::Bytes | DataEncoding::Text => {}
                    _ => unsupported!("format for upsert key"),
                }
                SourceEnvelope::Upsert(key_encoding)
            }
            _ => unsupported!("upsert envelope for non-Kafka sources"),
        },
        sql_parser::ast::Envelope::CdcV2 => {
            scx.require_experimental_mode("ENVELOPE MATERIALIZE")?;
            if let Connector::AvroOcf { .. } = connector {
                // TODO[btv] - there is no fundamental reason not to support this eventually,
                // but OCF goes through a separate pipeline that it hasn't been implemented for.
                unsupported!("ENVELOPE MATERIALIZE over OCF (Avro files)")
            }
            match format {
                Some(Format::Avro(_)) => {}
                _ => unsupported!("non-Avro-encoded ENVELOPE MATERIALIZE"),
            }
            SourceEnvelope::CdcV2
        }
    };

    if let SourceEnvelope::Upsert(key_encoding) = &envelope {
        match &mut encoding {
            DataEncoding::Avro(AvroEncoding { key_schema, .. }) => {
                *key_schema = None;
            }
            DataEncoding::Bytes | DataEncoding::Text | DataEncoding::Protobuf(_) => {
                if let DataEncoding::Avro(_) = &key_encoding {
                    unsupported!("Avro key for this format");
                }
            }
            _ => unsupported!("upsert envelope for this format"),
        }
    }

    let mut bare_desc = encoding.desc(&envelope)?;
    let ignore_source_keys = match with_options.remove("ignore_source_keys") {
        None => false,
        Some(Value::Boolean(b)) => b,
        Some(_) => bail!("ignore_source_keys must be a boolean"),
    };
    if ignore_source_keys {
        bare_desc = bare_desc.without_keys();
    }

    let post_transform_key = if let SourceEnvelope::Debezium(_, _) = &envelope {
        if let DataEncoding::Avro(AvroEncoding { key_schema, .. }) = &encoding {
            if ignore_source_keys {
                None
            } else {
                match &bare_desc.typ().column_types[0].scalar_type {
                    ScalarType::Record { fields, .. } => {
                        let row_desc = RelationDesc::from_names_and_types(
                            fields.clone().into_iter().map(|(n, t)| (Some(n), t)),
                        );
                        let key_schema_indices = key_schema.as_ref().and_then(|key_schema| {
                            avro::validate_key_schema(key_schema, &row_desc)
                                .map(Some)
                                .unwrap_or_else(|e| {
                                    warn!("Not using key due to error: {}", e);
                                    None
                                })
                        });
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

    // TODO(benesch): the available metadata columns should not depend
    // on the format.
    //
    // TODO(brennan): They should not depend on the envelope either. Figure out a way to
    // make all of this more tasteful.
    match (&encoding, &envelope) {
        (DataEncoding::Avro { .. }, _) | (_, SourceEnvelope::Debezium(_, _)) => (),
        _ => {
            for (name, ty) in external_connector.metadata_columns() {
                bare_desc = bare_desc.with_column(name, ty);
            }
        }
    }

    let if_not_exists = *if_not_exists;
    let materialized = *materialized;
    let name = scx.allocate_name(normalize::unresolved_object_name(name.clone())?);
    let create_sql = normalize::create_statement(&scx, Statement::CreateSource(stmt))?;

    let (expr, column_names) = plan_source_envelope(&bare_desc, &envelope, post_transform_key)?;
    let source = Source {
        create_sql,
        connector: SourceConnector::External {
            connector: external_connector,
            encoding,
            envelope,
            consistency,
            ts_frequency,
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

pub fn plan_create_sources(
    scx: &StatementContext,
    stmt: CreateSourcesStatement<Raw>,
) -> Result<Plan, anyhow::Error> {
    scx.require_experimental_mode("Postgres Sources")?;
    let CreateSourcesStatement { stmts, .. } = stmt;
    let mut planned = vec![];
    for stmt in stmts {
        planned.push(plan_create_source(scx, stmt)?);
    }

    unsupported!("CREATE SOURCES");
}

pub fn describe_create_view(
    _: &StatementContext,
    _: CreateViewStatement<Raw>,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_view(
    scx: &StatementContext,
    mut stmt: CreateViewStatement<Raw>,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let create_sql = normalize::create_statement(scx, Statement::CreateView(stmt.clone()))?;
    let CreateViewStatement {
        name,
        columns,
        query,
        temporary,
        materialized,
        if_exists,
        with_options,
    } = &mut stmt;
    if !with_options.is_empty() {
        unsupported!("WITH options");
    }
    let name = if *temporary {
        scx.allocate_temporary_name(normalize::unresolved_object_name(name.to_owned())?)
    } else {
        scx.allocate_name(normalize::unresolved_object_name(name.to_owned())?)
    };
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
    let replace = if *if_exists == IfExistsBehavior::Replace {
        if let Ok(item) = scx.catalog.resolve_item(&name.clone().into()) {
            if relation_expr.global_uses().contains(&item.id()) {
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
    desc = plan_utils::maybe_rename_columns(format!("view {}", name), desc, columns)?;
    let temporary = *temporary;
    let materialize = *materialized; // Normalize for `raw_sql` below.
    let if_not_exists = *if_exists == IfExistsBehavior::Skip;
    Ok(Plan::CreateView {
        name,
        view: View {
            create_sql,
            expr: relation_expr,
            column_names: desc.iter_names().map(|n| n.cloned()).collect(),
            temporary,
        },
        replace,
        materialize,
        if_not_exists,
        depends_on,
    })
}

#[allow(clippy::too_many_arguments)]
fn kafka_sink_builder(
    format: Option<Format<Raw>>,
    with_options: &mut BTreeMap<String, Value>,
    broker: String,
    topic_prefix: String,
    key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    value_desc: RelationDesc,
    topic_suffix_nonce: String,
    root_dependencies: &[&dyn CatalogItem],
) -> Result<SinkConnectorBuilder, anyhow::Error> {
    let (schema_registry_url, ccsr_with_options) = match format {
        Some(Format::Avro(AvroSchema::CsrUrl {
            url,
            seed,
            with_options,
        })) => {
            if seed.is_some() {
                bail!("SEED option does not make sense with sinks");
            }
            (url.parse::<Url>()?, normalize::options(&with_options))
        }
        _ => unsupported!("non-confluent schema registry avro sinks"),
    };

    let broker_addrs = broker.parse()?;

    let include_consistency = match with_options.remove("consistency") {
        Some(Value::Boolean(b)) => b,
        None => false,
        Some(_) => bail!("consistency must be a boolean"),
    };

    let exactly_once = match with_options.remove("exactly_once") {
        Some(Value::Boolean(b)) => b,
        None => false,
        Some(_) => bail!("exactly-once must be a boolean"),
    };

    if exactly_once && !include_consistency {
        bail!("exactly-once requires a consistency topic");
    }

    if exactly_once {
        for item in root_dependencies.iter() {
            if item.item_type() == CatalogItemType::Source
                && !item.source_connector()?.yields_stable_input()
            {
                bail!(
                    "all input sources of an exactly-once Kafka sink must be replayable, {} is not",
                    item.name()
                );
            } else if item.item_type() != CatalogItemType::Source {
                bail!(
                    "all inputs of an exactly-once Kafka sink must be sources, {} is not",
                    item.name()
                );
            };
        }
    }

    let encoder = Encoder::new(
        key_desc_and_indices
            .as_ref()
            .map(|(desc, _indices)| desc.clone()),
        value_desc.clone(),
        include_consistency,
    );
    let value_schema = encoder.value_writer_schema().to_string();
    let key_schema = encoder
        .key_writer_schema()
        .map(|key_schema| key_schema.to_string());

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

    let consistency_value_schema = if include_consistency {
        Some(avro::get_debezium_transaction_schema().canonical_form())
    } else {
        None
    };

    let config_options = kafka_util::extract_config(with_options)?;
    let ccsr_config = kafka_util::generate_ccsr_client_config(
        schema_registry_url.clone(),
        &config_options,
        ccsr_with_options,
    )?;
    Ok(SinkConnectorBuilder::Kafka(KafkaSinkConnectorBuilder {
        broker_addrs,
        schema_registry_url,
        value_schema,
        topic_prefix,
        topic_suffix_nonce,
        partition_count,
        replication_factor,
        fuel: 10000,
        consistency_value_schema,
        config_options,
        ccsr_config,
        key_schema,
        key_desc_and_indices,
        value_desc,
        exactly_once,
    }))
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
        Some(Envelope::Upsert(None)) => SinkEnvelope::Upsert,
        Some(Envelope::CdcV2) => unsupported!("CDCv2 sinks"),
        Some(Envelope::Debezium(sql_parser::ast::DbzMode::Upsert)) => {
            unsupported!("UPSERT doesn't make sense for sinks")
        }
        Some(Envelope::None) => unsupported!("\"ENVELOPE NONE\" sinks"),
        Some(Envelope::Upsert(Some(_))) => unsupported!("Upsert sinks with custom key encodings"),
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
        Connector::File { .. } => None,
        Connector::Kafka { key, .. } => {
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
                    .into_iter()
                    .map(|col| -> anyhow::Result<usize> {
                        let name_idx = desc
                            .get_by_name(&col)
                            .map(|(idx, _type)| idx)
                            .ok_or_else(|| anyhow!("No such column: {}", col))?;
                        if desc.get_unambiguous_name(name_idx).is_none() {
                            bail!("Ambiguous column: {}", col);
                        }
                        Ok(name_idx)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Some(indices)
            } else {
                None
            }
        }
        Connector::Kinesis { .. } => None,
        Connector::AvroOcf { .. } => None,
        Connector::S3 { .. } => None,
        Connector::Postgres { .. } => None,
        Connector::PubNub { .. } => None,
    };

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
        SinkEnvelope::Tail { .. } => {
            unreachable!("SinkEnvelope::Tail is only used when creating tails, not sinks")
        }
    };

    if as_of.is_some() {
        bail!("CREATE SINK ... AS OF is no longer supported");
    }

    let mut depends_on = vec![from.id()];
    depends_on.extend(from.uses());

    let root_user_dependencies = get_root_dependencies(scx, &depends_on);

    let connector_builder = match connector {
        Connector::File { .. } => unsupported!("file sinks"),
        Connector::Kafka { broker, topic, .. } => kafka_sink_builder(
            format,
            &mut with_options,
            broker,
            topic,
            key_desc_and_indices,
            value_desc,
            suffix_nonce,
            &root_user_dependencies,
        )?,
        Connector::Kinesis { .. } => unsupported!("Kinesis sinks"),
        Connector::AvroOcf { path } => {
            avro_ocf_sink_builder(format, path, suffix_nonce, value_desc)?
        }
        Connector::S3 { .. } => unsupported!("S3 sinks"),
        Connector::Postgres { .. } => unsupported!("Postgres sinks"),
        Connector::PubNub { .. } => unsupported!("PubNub sinks"),
    };

    if !with_options.is_empty() {
        bail!(
            "unexpected parameters for CREATE SINK: {}",
            with_options.keys().join(",")
        )
    }

    Ok(Plan::CreateSink {
        name,
        sink: Sink {
            create_sql,
            from: from.id(),
            connector_builder,
            envelope,
        },
        with_snapshot,
        if_not_exists,
        depends_on,
    })
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
        let mut idx_name_base = on.name().clone();
        if key_parts.is_none() {
            // We're trying to create the "default" index.
            idx_name_base.item += "_primary_idx";
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
            idx_name_base.item += &format!("_{}_idx", index_name_col_suffix);
            idx_name_base.item = normalize::ident(Ident::new(idx_name_base.item))
        }

        let mut index_name = idx_name_base.clone();
        let mut i = 0;

        let schema = SchemaName {
            database: on.name().database.clone(),
            schema: on.name().schema.clone(),
        };
        let mut cat_schema_iter = scx.catalog.list_items(&schema);

        // Search for an unused version of the name unless `if_not_exists`.
        while cat_schema_iter.any(|i| *i.name() == index_name) && !*if_not_exists {
            i += 1;
            index_name = idx_name_base.clone();
            index_name.item += &i.to_string();
            cat_schema_iter = scx.catalog.list_items(&schema);
        }

        index_name
    };

    let options = plan_index_options(with_options.clone())?;

    // Normalize `stmt`.
    *name = Some(Ident::new(index_name.item.clone()));
    *key_parts = Some(filled_key_parts);
    let if_not_exists = *if_not_exists;
    let create_sql = normalize::create_statement(scx, Statement::CreateIndex(stmt))?;
    let mut depends_on = vec![on.id()];
    depends_on.extend(exprs_depend_on);

    Ok(Plan::CreateIndex {
        name: index_name,
        index: Index {
            create_sql,
            on: on.id(),
            keys,
        },
        options,
        if_not_exists,
        depends_on,
    })
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
        if scx
            .catalog
            .try_get_lossy_scalar_type_by_id(&item_id)
            .is_none()
        {
            bail!(
                "{} must be of class type, but received {} which is of class {}",
                key,
                item.name(),
                item.item_type()
            );
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

    Ok(Plan::CreateType {
        name,
        typ: Type { create_sql, inner },
        depends_on: ids,
    })
}

fn extract_timestamp_frequency_option(
    default: Duration,
    with_options: &mut BTreeMap<String, Value>,
) -> Result<Duration, anyhow::Error> {
    match with_options.remove("timestamp_frequency_ms") {
        None => Ok(default),
        Some(Value::Number(n)) => match n.parse::<u64>() {
            Ok(n) => Ok(Duration::from_millis(n)),
            _ => bail!("timestamp_frequency_ms must be an u64"),
        },
        Some(_) => bail!("timestamp_frequency_ms must be an u64"),
    }
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
        unsupported!("non-login users");
    }
    if super_user != Some(true) {
        unsupported!("non-superusers");
    }
    Ok(Plan::CreateRole {
        name: normalize::ident(name),
    })
}

pub fn describe_drop_database(
    _: &StatementContext,
    _: DropDatabaseStatement,
) -> Result<StatementDesc, anyhow::Error> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_database(
    scx: &StatementContext,
    DropDatabaseStatement { name, if_exists }: DropDatabaseStatement,
) -> Result<Plan, anyhow::Error> {
    let name = match scx.resolve_database_ident(name) {
        Ok(database) => database.name().into(),
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
    Ok(Plan::DropDatabase { name })
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
        object_type,
        if_exists,
        names,
        cascade,
    }: DropObjectsStatement,
) -> Result<Plan, anyhow::Error> {
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
        unsupported!("DROP SCHEMA with multiple schemas");
    }
    match scx.resolve_schema(names.into_element()) {
        Ok(schema) => {
            if let DatabaseSpecifier::Ambient = schema.name().database {
                bail!(
                    "cannot drop schema {} because it is required by the database system",
                    schema.name()
                );
            }
            let mut items = scx.catalog.list_items(schema.name());
            if !cascade && items.next().is_some() {
                bail!(
                    "schema '{}' cannot be dropped without CASCADE while it contains objects",
                    schema.name(),
                );
            }
            Ok(Plan::DropSchema {
                name: schema.name().clone(),
            })
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the
            // schema does not exist.
            // TODO(benesch): adjust the types here properly, rather than making
            // up a nonexistent database.
            Ok(Plan::DropSchema {
                name: SchemaName {
                    database: DatabaseSpecifier::Ambient,
                    schema: "noexist".into(),
                },
            })
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
    Ok(Plan::DropRoles { names: out })
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
    Ok(Plan::DropItems {
        items: ids,
        ty: object_type,
    })
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
    _: AlterIndexOptionsStatement,
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
            let window = Some(parse_duration::parse(s)?);
            out.push(IndexOption::LogicalCompactionWindow(window))
        }
    };

    Ok(out)
}

pub fn plan_alter_index_options(
    scx: &StatementContext,
    AlterIndexOptionsStatement {
        index_name,
        if_exists,
        options,
    }: AlterIndexOptionsStatement,
) -> Result<Plan, anyhow::Error> {
    let entry = match scx.resolve_item(index_name) {
        Ok(index) => index,
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating this index does not
            // exist.
            return Ok(Plan::AlterNoop {
                object_type: ObjectType::Index,
            });
        }
        Err(e) => return Err(e.into()),
    };
    if entry.item_type() != CatalogItemType::Index {
        bail!("{} is a {} not a index", entry.name(), entry.item_type())
    }
    let id = entry.id();

    match options {
        AlterIndexOptionsList::Reset(options) => {
            let options = options
                .into_iter()
                .filter_map(|o| match normalize::ident(o).as_str() {
                    "logical_compaction_window" => Some(IndexOptionName::LogicalCompactionWindow),
                    // Follow Postgres and don't complain if unknown parameters
                    // are passed into `ALTER INDEX ... RESET`.
                    _ => None,
                })
                .collect();
            Ok(Plan::AlterIndexResetOptions { id, options })
        }
        AlterIndexOptionsList::Set(options) => {
            let options = plan_index_options(options)?;
            Ok(Plan::AlterIndexSetOptions { id, options })
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
            return Ok(Plan::AlterNoop { object_type });
        }
        Err(err) => return Err(err.into()),
    };

    Ok(Plan::AlterItemRename {
        id,
        to_name: normalize::ident(to_item_name),
        object_type,
    })
}
