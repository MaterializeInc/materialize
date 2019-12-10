// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL `Statement`s are the imperative, side-effecting part of SQL.
//!
//! This module turns SQL `Statement`s into `Plan`s - commands which will drive the dataflow layer

use std::convert::{TryFrom, TryInto};
use std::iter;
use std::net::{SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use failure::{bail, ResultExt};
use sqlparser::ast::{
    Ident, ObjectName, ObjectType, Query, SetVariableValue, ShowStatementFilter, SourceSchema,
    Stage, Statement, Value,
};
use url::Url;

use catalog::{Catalog, CatalogItem, RemoveMode};
use dataflow_types::{
    FileFormat, FileSourceConnector, Index, KafkaSinkConnector, KafkaSourceConnector, PeekWhen,
    RowSetFinishing, Sink, SinkConnector, Source, SourceConnector, View,
};
use expr as relationexpr;
use interchange::avro;
use ore::option::OptionExt;
use relationexpr::{EvalEnv, Id};
use repr::{ColumnType, Datum, QualName, RelationDesc, RelationType, Row, ScalarType};

use crate::expr::like::build_like_regex_from_string;
use crate::query::QueryLifetime;
use crate::session::Session;
use crate::{names, query, Params, Plan};

pub fn describe_statement(
    catalog: &Catalog,
    stmt: Statement,
) -> Result<(Option<RelationDesc>, Vec<ScalarType>), failure::Error> {
    Ok(match stmt {
        Statement::CreateIndex { .. }
        | Statement::CreateSource { .. }
        | Statement::CreateSink { .. }
        | Statement::CreateView { .. }
        | Statement::Drop { .. }
        | Statement::SetVariable { .. }
        | Statement::StartTransaction { .. }
        | Statement::Rollback { .. }
        | Statement::Commit { .. }
        | Statement::Tail { .. } => (None, vec![]),

        Statement::CreateSources { .. } => (
            Some(RelationDesc::empty().add_column("Topic", ScalarType::String)),
            vec![],
        ),

        Statement::Explain { stage, .. } => (
            Some(RelationDesc::empty().add_column(
                match stage {
                    Stage::Dataflow => "Dataflow",
                    Stage::Plan => "Plan",
                },
                ScalarType::String,
            )),
            vec![],
        ),

        Statement::ShowCreateView { .. } => (
            Some(
                RelationDesc::empty()
                    .add_column("View", ScalarType::String)
                    .add_column("Create View", ScalarType::String),
            ),
            vec![],
        ),

        Statement::ShowCreateSource { .. } => (
            Some(
                RelationDesc::empty()
                    .add_column("Source", ScalarType::String)
                    .add_column("Source URL", ScalarType::String),
            ),
            vec![],
        ),

        Statement::ShowColumns { .. } => (
            Some(
                RelationDesc::empty()
                    .add_column("Field", ScalarType::String)
                    .add_column("Nullable", ScalarType::String)
                    .add_column("Type", ScalarType::String),
            ),
            vec![],
        ),

        Statement::ShowObjects { object_type, .. } => (
            Some(
                RelationDesc::empty()
                    .add_column(object_type_as_plural_str(object_type), ScalarType::String),
            ),
            vec![],
        ),

        Statement::ShowVariable { variable, .. } => {
            if variable.value == unicase::Ascii::new("ALL") {
                (
                    Some(
                        RelationDesc::empty()
                            .add_column("name", ScalarType::String)
                            .add_column("setting", ScalarType::String)
                            .add_column("description", ScalarType::String),
                    ),
                    vec![],
                )
            } else {
                (
                    Some(RelationDesc::empty().add_column(variable.value, ScalarType::String)),
                    vec![],
                )
            }
        }

        Statement::Peek { name, .. } => {
            let sql_object = catalog.get(&name.try_into()?)?;
            (Some(sql_object.desc()?.clone()), vec![])
        }

        Statement::Query(query) => {
            // TODO(benesch): ideally we'd save `relation_expr` and `finishing`
            // somewhere, so we don't have to reanalyze the whole query when
            // `handle_statement` is called. This will require a complicated
            // dance when bind parameters are implemented, so punting for now.
            let (_relation_expr, desc, _finishing, param_types) =
                query::plan_root_query(catalog, *query, QueryLifetime::OneShot)?;
            (Some(desc), param_types)
        }

        _ => bail!("unsupported SQL statement: {:?}", stmt),
    })
}

/// Dispatch from arbitrary [`sqlparser::ast::Statement`]s to specific handle commands
pub fn handle_statement(
    catalog: &Catalog,
    session: &Session,
    stmt: Statement,
    params: &Params,
) -> Result<Plan, failure::Error> {
    match stmt {
        Statement::Peek { name, immediate } => handle_peek(catalog, name.try_into()?, immediate),
        Statement::Tail { name } => handle_tail(catalog, &name.try_into()?),
        Statement::StartTransaction { .. } => handle_start_transaction(),
        Statement::Commit { .. } => handle_commit_transaction(),
        Statement::Rollback { .. } => handle_rollback_transaction(),
        Statement::CreateSource { .. }
        | Statement::CreateSink { .. }
        | Statement::CreateView { .. }
        | Statement::CreateSources { .. }
        | Statement::CreateIndex { .. } => handle_create_dataflow(catalog, stmt, params),
        Statement::Drop { .. } => handle_drop_dataflow(catalog, stmt),
        Statement::Query(query) => handle_select(catalog, *query, params),
        Statement::SetVariable {
            local,
            variable,
            value,
        } => handle_set_variable(catalog, local, variable, value),
        Statement::ShowVariable { variable } => handle_show_variable(catalog, session, variable),
        Statement::ShowObjects {
            object_type: ot,
            filter,
        } => handle_show_objects(catalog, ot, filter.as_ref()),
        Statement::ShowColumns {
            extended,
            full,
            table_name,
            filter,
        } => handle_show_columns(
            catalog,
            extended,
            full,
            &table_name.try_into()?,
            filter.as_ref(),
        ),
        Statement::ShowCreateView { view_name } => {
            handle_show_create_view(catalog, view_name.try_into()?)
        }
        Statement::ShowCreateSource { source_name } => {
            handle_show_create_source(catalog, source_name)
        }
        Statement::Explain { stage, query } => handle_explain(catalog, stage, *query, params),

        _ => bail!("unsupported SQL statement: {:?}", stmt),
    }
}

fn handle_set_variable(
    _: &Catalog,
    local: bool,
    variable: Ident,
    value: SetVariableValue,
) -> Result<Plan, failure::Error> {
    if local {
        bail!("SET LOCAL ... is not supported");
    }
    Ok(Plan::SetVariable {
        name: variable.to_string(),
        value: match value {
            SetVariableValue::Literal(Value::SingleQuotedString(s)) => s,
            SetVariableValue::Literal(lit) => lit.to_string(),
            SetVariableValue::Ident(ident) => ident.value,
        },
    })
}

fn handle_show_variable(
    _: &Catalog,
    session: &Session,
    variable: Ident,
) -> Result<Plan, failure::Error> {
    if variable.value == unicase::Ascii::new("ALL") {
        Ok(Plan::SendRows(
            session
                .vars()
                .iter()
                .map(|v| {
                    Row::pack(&[
                        Datum::String(v.name()),
                        Datum::String(&v.value()),
                        Datum::String(v.description()),
                    ])
                })
                .collect(),
        ))
    } else {
        let variable = session.get(&variable.value)?;
        Ok(Plan::SendRows(vec![Row::pack(&[Datum::String(
            &variable.value(),
        )])]))
    }
}

fn handle_tail(catalog: &Catalog, from: &QualName) -> Result<Plan, failure::Error> {
    let entry = catalog.get(&from)?;
    Ok(Plan::Tail(entry.clone()))
}

fn handle_start_transaction() -> Result<Plan, failure::Error> {
    Ok(Plan::StartTransaction)
}

fn handle_commit_transaction() -> Result<Plan, failure::Error> {
    Ok(Plan::Commit)
}

fn handle_rollback_transaction() -> Result<Plan, failure::Error> {
    Ok(Plan::Rollback)
}

fn handle_show_objects(
    catalog: &Catalog,
    object_type: ObjectType,
    filter: Option<&ShowStatementFilter>,
) -> Result<Plan, failure::Error> {
    let like_regex = match filter {
        Some(ShowStatementFilter::Like(like_string)) => {
            build_like_regex_from_string(like_string.as_ref())?
        }
        Some(ShowStatementFilter::Where(_where_epr)) => bail!("SHOW ... WHERE is not supported"),
        None => build_like_regex_from_string(&String::from("%"))?,
    };

    let mut rows: Vec<Row> = catalog
        .iter()
        .filter(|entry| {
            object_type_matches(object_type, entry.item())
                && like_regex.is_match(&entry.name().to_string())
        })
        .map(|entry| Row::pack(&[Datum::from(&*entry.name().to_string())]))
        .collect();
    rows.sort_unstable_by(move |a, b| a.unpack_first().cmp(&b.unpack_first()));
    Ok(Plan::SendRows(rows))
}

/// Create an immediate result that describes all the columns for the given table
fn handle_show_columns(
    catalog: &Catalog,
    extended: bool,
    full: bool,
    table_name: &QualName,
    filter: Option<&ShowStatementFilter>,
) -> Result<Plan, failure::Error> {
    if extended {
        bail!("SHOW EXTENDED COLUMNS is not supported");
    }
    if full {
        bail!("SHOW FULL COLUMNS is not supported");
    }
    if filter.is_some() {
        bail!("SHOW COLUMNS ... { LIKE | WHERE } is not supported");
    }

    let column_descriptions: Vec<_> = catalog
        .get(&table_name.try_into()?)?
        .desc()?
        .iter()
        .map(|(name, typ)| {
            let name = name.map(|n| n.to_string());
            Row::pack(&[
                Datum::String(name.mz_as_deref().unwrap_or("?")),
                Datum::String(if typ.nullable { "YES" } else { "NO" }),
                Datum::String(&postgres_type_name(&typ.scalar_type)),
            ])
        })
        .collect();

    Ok(Plan::SendRows(column_descriptions))
}

fn handle_show_create_view(
    catalog: &Catalog,
    object_name: QualName,
) -> Result<Plan, failure::Error> {
    let name = object_name.try_into()?;
    let raw_sql = if let CatalogItem::View(view) = catalog.get(&name)?.item() {
        &view.raw_sql
    } else {
        bail!("'{}' is not a view", name);
    };
    Ok(Plan::SendRows(vec![Row::pack(&[
        Datum::String(&name.to_string()),
        Datum::String(raw_sql),
    ])]))
}

fn handle_show_create_source(
    catalog: &Catalog,
    object_name: ObjectName,
) -> Result<Plan, failure::Error> {
    let name = object_name.try_into()?;
    let source_url =
        if let CatalogItem::Source(Source { connector, .. }) = catalog.get(&name)?.item() {
            match connector {
                SourceConnector::Local => String::from("local://"),
                SourceConnector::Kafka(KafkaSourceConnector { addr, topic, .. }) => {
                    format!("kafka://{}/{}", addr, topic)
                }
                SourceConnector::File(c) => {
                    // TODO https://github.com/MaterializeInc/materialize/issues/1093
                    format!("file://{}", c.path.to_string_lossy())
                }
            }
        } else {
            bail!("{} is not a source", name);
        };
    Ok(Plan::SendRows(vec![Row::pack(&[
        Datum::String(&name.to_string()),
        Datum::String(&source_url),
    ])]))
}

fn handle_create_dataflow(
    catalog: &Catalog,
    mut stmt: Statement,
    params: &Params,
) -> Result<Plan, failure::Error> {
    match &mut stmt {
        Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            with_options,
        } => {
            if !with_options.is_empty() {
                bail!("WITH options are not yet supported");
            }
            let (mut relation_expr, mut desc, finishing) =
                handle_query(catalog, *query.clone(), params, QueryLifetime::Static)?;
            if !finishing.is_trivial() {
                //TODO: materialize#724 - persist finishing information with the view?
                relation_expr = relationexpr::RelationExpr::Project {
                    input: Box::new(relationexpr::RelationExpr::TopK {
                        input: Box::new(relation_expr),
                        group_key: vec![],
                        order_key: finishing.order_by,
                        limit: finishing.limit,
                        offset: finishing.offset,
                    }),
                    outputs: finishing.project,
                }
            }
            *materialized = false; // Normalize for `raw_sql` below.
            let typ = desc.typ();
            if !columns.is_empty() {
                if columns.len() != typ.column_types.len() {
                    bail!(
                        "VIEW definition has {} columns, but query has {} columns",
                        columns.len(),
                        typ.column_types.len()
                    )
                }
                for (i, name) in columns.iter().enumerate() {
                    desc.set_name(i, Some(names::ident_to_col_name(name.clone())));
                }
            }
            let name = QualName::try_from(&*name)?;
            let view = View {
                raw_sql: stmt.to_string(),
                relation_expr,
                desc,
                eval_env: EvalEnv::default(),
            };
            Ok(Plan::CreateView(name, view))
        }
        Statement::CreateSource {
            name,
            url,
            schema,
            with_options,
        } => {
            let name: QualName = (&*name).try_into()?;
            let source_url = parse_source_url(url)?;
            match source_url {
                SourceUrl::Kafka(KafkaUrl { addr, topic }) => {
                    if !with_options.is_empty() {
                        bail!("WITH options on Kafka sources are not yet supported");
                    }
                    if let Some(topic) = topic {
                        if let Some(schema) = schema {
                            let source = build_kafka_source(schema, addr, topic)?;
                            Ok(Plan::CreateSource(name, source))
                        } else {
                            bail!("Kafka sources require a schema.");
                        }
                    } else {
                        bail!("source URL missing topic path: {}", url);
                    }
                }
                SourceUrl::Path(path) => {
                    if schema.is_some() {
                        bail!("csv file sources do not support schemas.");
                    }
                    let mut format = None;
                    let mut n_cols: Option<usize> = None;
                    for with_op in with_options {
                        match with_op.name.value.as_str() {
                            "columns" => {
                                n_cols = Some(match &with_op.value {
                                    Value::Number(s) => s.parse()?,
                                    _ => bail!("`columns` must be a number."),
                                });
                            }
                            "format" => {
                                format = Some(match &with_op.value {
                                    Value::SingleQuotedString(s) => match s.as_ref() {
                                        "csv" => SourceFileFormat::Csv,
                                        _ => bail!("Unrecognized file format: {}", s),
                                    },
                                    _ => bail!("File format must be a string, e.g. 'csv'."),
                                });
                            }
                            _ => bail!("Unrecognized WITH option: {}", with_op.name.value),
                        }
                    }

                    let format = match format {
                        Some(f) => f,
                        None => bail!("File source requires a `format` WITH option."),
                    };
                    match format {
                        SourceFileFormat::Csv => {
                            let n_cols = match n_cols {
                                Some(n) => n,
                                None => bail!("Csv source requires a `columns` WITH option."),
                            };
                            let name = name.try_into()?;
                            let cols = iter::repeat(ColumnType::new(ScalarType::String))
                                .take(n_cols)
                                .collect();
                            let names = (1..=n_cols).map(|i| Some(format!("column{}", i)));
                            let source = Source {
                                connector: SourceConnector::File(FileSourceConnector {
                                    path: path.clone().try_into()?,
                                    format: FileFormat::Csv(n_cols),
                                }),
                                desc: RelationDesc::new(RelationType::new(cols), names),
                            };
                            Ok(Plan::CreateSource(name, source))
                        }
                    }
                }
            }
        }
        Statement::CreateSources {
            like,
            url,
            schema_registry,
            with_options,
        } => {
            if !with_options.is_empty() {
                bail!("WITH options are not yet supported");
            }
            // TODO(brennan): This shouldn't be synchronous either (see CreateSource above),
            // but for now we just want it working for demo purposes...
            let schema_registry_url: Url = schema_registry.parse()?;
            let ccsr_client = ccsr::Client::new(schema_registry_url.clone());
            let mut subjects = ccsr_client.list_subjects()?;
            if let Some(value) = like {
                let like_regex = build_like_regex_from_string(value)?;
                subjects.retain(|a| like_regex.is_match(a))
            }

            let names = subjects
                .iter()
                .filter_map(|s| {
                    let parts: Vec<&str> = s.rsplitn(2, '-').collect();
                    if parts.len() == 2 && parts[0] == "value" {
                        let topic_name = parts[1];
                        let sql_name = sanitize_kafka_topic_name(parts[1])
                            .parse()
                            .expect("sanitized kafka topic names should always be valid qualnames");
                        Some((topic_name, sql_name))
                    } else {
                        None
                    }
                })
                .filter(|(_tn, sn)| catalog.try_get(sn).is_none());
            let url: Url = url.parse()?;
            let (addr, topic) = parse_kafka_url(&url)?;
            if let Some(s) = topic {
                bail!(
                    "CREATE SOURCES statement should not take a topic path: {}",
                    s
                );
            }
            let sources = names
                .map(|(topic_name, sql_name)| {
                    Ok((
                        sql_name,
                        build_kafka_source(
                            &SourceSchema::Registry(schema_registry.to_owned()),
                            addr,
                            topic_name.to_owned(),
                        )?,
                    ))
                })
                .collect::<Result<Vec<_>, failure::Error>>()?;
            Ok(Plan::CreateSources(sources))
        }
        Statement::CreateSink {
            name,
            from,
            url,
            with_options,
        } => {
            if !with_options.is_empty() {
                bail!("WITH options are not yet supported");
            }
            let name = name.try_into()?;
            let from = from.try_into()?;
            let catalog_entry = catalog.get(&from)?;
            let (addr, topic) = parse_kafka_topic_url(url)?;
            let sink = Sink {
                from: (catalog_entry.id(), catalog_entry.desc()?.clone()),
                connector: SinkConnector::Kafka(KafkaSinkConnector {
                    addr,
                    topic,
                    schema_id: 0,
                }),
            };
            Ok(Plan::CreateSink(name, sink))
        }
        Statement::CreateIndex {
            name,
            on_name,
            key_parts,
        } => {
            let on_name = on_name.try_into()?;
            let (catalog_entry, keys, map_exprs) = query::plan_index(catalog, &on_name, key_parts)?;
            // TODO (andiwang) remove this when trace manager supports ScalarExpr keys
            if !map_exprs.is_empty() {
                bail!("function-based indexes are not supported yet");
            }
            let name = QualName::new_normalized(iter::once(name.clone()))?;
            let index = Index {
                on_id: catalog_entry.id(),
                relation_type: catalog_entry.desc()?.typ().clone(),
                keys,
                funcs: map_exprs
                    .into_iter()
                    .map(|x| x.lower_uncorrelated())
                    .collect(),
                eval_env: EvalEnv::default(),
            };
            Ok(Plan::CreateIndex(name, index))
        }
        other => bail!("Unsupported statement: {:?}", other),
    }
}

fn handle_drop_dataflow(catalog: &Catalog, stmt: Statement) -> Result<Plan, failure::Error> {
    let (object_type, if_exists, names, cascade) = match stmt {
        Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
        } => (object_type, if_exists, names, cascade),
        _ => unreachable!(),
    };
    //let names: Vec<String> = names.iter().map(QualName::try_into).?collect();
    for name in &names {
        match catalog.get(&name.try_into()?) {
            Ok(catalog_entry) => {
                if !object_type_matches(object_type, catalog_entry.item()) {
                    bail!("{} is not of type {}", name, object_type);
                }
            }
            Err(e) => {
                if !if_exists {
                    return Err(e);
                }
            }
        }
    }
    // This needs to have heterogenous drops, because cascades could drop multiple types.
    let mode = RemoveMode::from_cascade(cascade);
    let mut to_remove = vec![];
    for name in &names {
        catalog.plan_remove(&name.try_into()?, mode, &mut to_remove)?;
    }
    to_remove.sort();
    to_remove.dedup();
    Ok(match object_type {
        ObjectType::Source => Plan::DropItems(to_remove, ObjectType::Source),
        ObjectType::View => Plan::DropItems(to_remove, ObjectType::View),
        ObjectType::Index => Plan::DropItems(to_remove, ObjectType::Index),
        _ => bail!("unsupported SQL statement: DROP {}", object_type),
    })
}

fn handle_peek(catalog: &Catalog, name: QualName, immediate: bool) -> Result<Plan, failure::Error> {
    let name = name.try_into()?;
    let catalog_entry = catalog.get(&name)?.clone();
    let typ = catalog_entry.desc()?.typ();
    Ok(Plan::Peek {
        source: relationexpr::RelationExpr::Get {
            id: Id::Global(catalog_entry.id()),
            typ: typ.clone(),
        },
        when: if immediate {
            PeekWhen::Immediately
        } else {
            PeekWhen::EarliestSource
        },
        finishing: RowSetFinishing {
            offset: 0,
            limit: None,
            order_by: (0..typ.column_types.len())
                .map(|column| relationexpr::ColumnOrder {
                    column,
                    desc: false,
                })
                .collect(),
            project: (0..typ.column_types.len()).collect(),
        },
        eval_env: EvalEnv::default(),
    })
}

pub fn handle_select(
    catalog: &Catalog,
    query: Query,
    params: &Params,
) -> Result<Plan, failure::Error> {
    let (relation_expr, _, finishing) =
        handle_query(catalog, query, params, QueryLifetime::OneShot)?;
    Ok(Plan::Peek {
        source: relation_expr,
        when: PeekWhen::Immediately,
        finishing,
        eval_env: EvalEnv::default(),
    })
}

pub fn handle_explain(
    catalog: &Catalog,
    stage: Stage,
    query: Query,
    params: &Params,
) -> Result<Plan, failure::Error> {
    let (relation_expr, _desc, _finishing) =
        handle_query(catalog, query, params, QueryLifetime::OneShot)?;
    // Previouly we would bail here for ORDER BY and LIMIT; this has been relaxed to silently
    // report the plan without the ORDER BY and LIMIT decorations (which are done in post).
    if stage == Stage::Dataflow {
        Ok(Plan::SendRows(vec![Row::pack(&[Datum::String(
            &relation_expr.pretty_humanized(catalog),
        )])]))
    } else {
        Ok(Plan::ExplainPlan(relation_expr, EvalEnv::default()))
    }
}

/// Plans and decorrelates a `Query`. Like `query::plan_root_query`, but returns
/// an `::expr::RelationExpr`, which cannot include correlated expressions.
fn handle_query(
    catalog: &Catalog,
    query: Query,
    params: &Params,
    lifetime: QueryLifetime,
) -> Result<(relationexpr::RelationExpr, RelationDesc, RowSetFinishing), failure::Error> {
    let (mut expr, desc, finishing, _param_types) =
        query::plan_root_query(catalog, query, lifetime)?;
    expr.bind_parameters(&params);
    Ok((expr.decorrelate()?, desc, finishing))
}

fn build_kafka_source(
    schema: &SourceSchema,
    kafka_addr: SocketAddr,
    topic: String,
) -> Result<Source, failure::Error> {
    let (key_schema, value_schema, schema_registry_url) = match schema {
        // TODO(jldlaughlin): we need a way to pass in primary key information
        // when building a source from a string
        SourceSchema::Raw(schema) => (None, schema.to_owned(), None),

        SourceSchema::Registry(url) => {
            // TODO(benesch): we need to fetch this schema asynchronously to
            // avoid blocking the command processing thread.
            let url: Url = url.parse()?;
            let ccsr_client = ccsr::Client::new(url.clone());

            let value_schema_name = format!("{}-value", topic);
            let value_schema = ccsr_client
                .get_schema_by_subject(&value_schema_name)
                .with_context(|err| {
                    format!(
                        "fetching latest schema for subject '{}' from registry: {}",
                        value_schema_name, err
                    )
                })?;
            let key_schema = ccsr_client
                .get_schema_by_subject(&format!("{}-key", topic))
                .ok();
            (key_schema.map(|s| s.raw), value_schema.raw, Some(url))
        }
    };

    let mut desc = avro::validate_value_schema(&value_schema)?;
    if let Some(key_schema) = key_schema {
        let keys = avro::validate_key_schema(&key_schema, &desc)?;
        desc = desc.add_keys(keys);
    }

    Ok(Source {
        connector: SourceConnector::Kafka(KafkaSourceConnector {
            addr: kafka_addr,
            topic,
            raw_schema: value_schema,
            schema_registry_url,
        }),
        desc,
    })
}

enum SourceFileFormat {
    Csv,
}

struct KafkaUrl {
    addr: SocketAddr,
    topic: Option<String>,
}

enum SourceUrl {
    Kafka(KafkaUrl),
    Path(PathBuf),
}

fn parse_source_url(url: &str) -> Result<SourceUrl, failure::Error> {
    let url: Url = url.parse()?;
    let url = match url.scheme().to_lowercase().as_str() {
        "kafka" => {
            let (addr, topic) = parse_kafka_url(&url)?;
            SourceUrl::Kafka(KafkaUrl { addr, topic })
        }
        "file" => {
            if url.has_host() {
                bail!(
                    "No hostname allowed in file URL: {}. Found: {}",
                    url,
                    url.host_str().unwrap()
                );
            }
            let pb: PathBuf = url.path().parse()?;
            SourceUrl::Path(pb)
        }
        bad => bail!("Unrecognized source URL schema: {}", bad),
    };
    Ok(url)
}

fn parse_kafka_url(url: &Url) -> Result<(SocketAddr, Option<String>), failure::Error> {
    if url.scheme() != "kafka" {
        bail!("only kafka:// sources are supported: {}", url);
    } else if !url.has_host() {
        bail!("source URL missing hostname: {}", url)
    }
    let topic = match url.path_segments() {
        None => None,
        Some(segments) => {
            let segments: Vec<_> = segments.collect();
            if segments.is_empty() {
                None
            } else if segments.len() != 1 {
                bail!("source URL should have at most one path segment: {}", url);
            } else if segments[0].is_empty() {
                None
            } else {
                Some(segments[0].to_owned())
            }
        }
    };
    // We already checked for kafka scheme above, so it's safe to assume port
    // 9092.
    let addr = url
        .with_default_port(|_| Ok(9092))?
        .to_socket_addrs()?
        .next()
        .unwrap();
    Ok((addr, topic))
}

fn parse_kafka_topic_url(url: &str) -> Result<(SocketAddr, String), failure::Error> {
    let url: Url = url.parse()?;
    let (addr, topic) = parse_kafka_url(&url)?;
    if let Some(topic) = topic {
        Ok((addr, topic))
    } else {
        bail!("source URL missing topic path: {}", url)
    }
}

fn sanitize_kafka_topic_name(topic_name: &str) -> String {
    // Kafka topics can contain alphanumerics, dots (.), underscores (_), and
    // hyphens (-), and most Kafka topics contain at least one of these special
    // characters for namespacing, as in "mysql.tbl". Since non-quoted SQL
    // identifiers cannot contain dots or hyphens, if we were to use the topic
    // name directly as the name of the source, it would be impossible to refer
    // to in SQL without quoting. So we replace hyphens and dots with
    // underscores to make a valid SQL identifier.
    //
    // This scheme has the potential for collisions, but if you're creating
    // Kafka topics like "a.b", "a_b", and "a-b", well... that's why we let you
    // manually create sources with custom names using CREATE SOURCE.
    topic_name.replace("-", "_").replace(".", "_")
}

/// Whether a SQL object type can be interpreted as matching the type of the given catalog item.
/// For example, if `v` is a view, `DROP SOURCE v` should not work, since Source and View
/// are non-matching types.
///
/// For now tables are treated as a special kind of source in Materialize, so just
/// allow `TABLE` to refer to either.
fn object_type_matches(object_type: ObjectType, item: &CatalogItem) -> bool {
    match item {
        CatalogItem::Source { .. } => {
            object_type == ObjectType::Source || object_type == ObjectType::Table
        }
        CatalogItem::Sink { .. } => object_type == ObjectType::Sink,
        CatalogItem::View { .. } => object_type == ObjectType::View,
        CatalogItem::Index { .. } => object_type == ObjectType::Index,
    }
}

fn object_type_as_plural_str(object_type: ObjectType) -> &'static str {
    match object_type {
        ObjectType::Index => "INDEXES",
        ObjectType::Table => "TABLES",
        ObjectType::View => "VIEWS",
        ObjectType::Source => "SOURCES",
        ObjectType::Sink => "SINKS",
    }
}

/// Returns the name that PostgreSQL would use for this `ScalarType`. Note that
/// PostgreSQL does not have an explicit NULL type, so this function panics if
/// called with `ScalarType::Null`.
fn postgres_type_name(typ: &ScalarType) -> String {
    match typ {
        ScalarType::Null => panic!("postgres_type_name called on ScalarType::Null"),
        ScalarType::Bool => "bool".to_owned(),
        ScalarType::Int32 => "int4".to_owned(),
        ScalarType::Int64 => "int8".to_owned(),
        ScalarType::Float32 => "float4".to_owned(),
        ScalarType::Float64 => "float8".to_owned(),
        ScalarType::Decimal(_, _) => "numeric".to_owned(),
        ScalarType::Date => "date".to_owned(),
        ScalarType::Timestamp => "timestamp".to_owned(),
        ScalarType::TimestampTz => "timestamptz".to_owned(),
        ScalarType::Interval => "interval".to_owned(),
        ScalarType::Bytes => "bytea".to_owned(),
        ScalarType::String => "text".to_owned(),
    }
}
