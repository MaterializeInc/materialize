// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL `Statement`s are the imperative, side-effecting part of SQL.
//!
//! This module turns SQL `Statement`s into `Plan`s - commands which will drive the dataflow layer

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use aws_arn::{Resource, ARN};
use failure::{bail, format_err, ResultExt};
use itertools::Itertools;
use rusoto_core::Region;
use url::Url;

use catalog::names::{DatabaseSpecifier, FullName, PartialName};
use catalog::{Catalog, CatalogItem, SchemaType};
use dataflow_types::{
    AvroEncoding, AvroOcfSinkConnectorBuilder, Consistency, CsvEncoding, DataEncoding, Envelope,
    ExternalSourceConnector, FileSourceConnector, KafkaAuth, KafkaSinkConnectorBuilder,
    KafkaSourceConnector, KinesisSourceConnector, PeekWhen, ProtobufEncoding, SinkConnectorBuilder,
    SourceConnector,
};
use expr::{like_pattern, GlobalId, RowSetFinishing};
use interchange::avro::Encoder;
use ore::collections::CollectionExt;
use repr::strconv;
use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowArena, ScalarType};
use sql_parser::ast::{
    AvroSchema, Connector, CsrSeed, ExplainOptions, ExplainStage, Explainee, Format, Ident,
    IfExistsBehavior, ObjectName, ObjectType, Query, SetVariableValue, ShowStatementFilter,
    Statement, Value,
};

use crate::query::QueryLifetime;
use crate::{normalize, query, Index, Params, Plan, PlanSession, Sink, Source, View};
use regex::Regex;

use tokio::io::AsyncBufReadExt;

pub fn describe_statement(
    catalog: &Catalog,
    session: &dyn PlanSession,
    stmt: Statement,
) -> Result<(Option<RelationDesc>, Vec<ScalarType>), failure::Error> {
    let scx = &StatementContext { catalog, session };
    Ok(match stmt {
        Statement::CreateDatabase { .. }
        | Statement::CreateSchema { .. }
        | Statement::CreateIndex { .. }
        | Statement::CreateSource { .. }
        | Statement::CreateSink { .. }
        | Statement::CreateView { .. }
        | Statement::DropDatabase { .. }
        | Statement::DropObjects { .. }
        | Statement::SetVariable { .. }
        | Statement::StartTransaction { .. }
        | Statement::Rollback { .. }
        | Statement::Commit { .. } => (None, vec![]),

        Statement::Explain { stage, .. } => (
            Some(RelationDesc::empty().add_column(
                match stage {
                    ExplainStage::Sql => "Sql",
                    ExplainStage::RawPlan => "Raw Plan",
                    ExplainStage::DecorrelatedPlan => "Decorrelated Plan",
                    ExplainStage::OptimizedPlan{..} => "Optimized Plan",
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
                    .add_column("Create Source", ScalarType::String),
            ),
            vec![],
        ),

        Statement::ShowCreateSink { .. } => (
            Some(
                RelationDesc::empty()
                    .add_column("Sink", ScalarType::String)
                    .add_column("Create Sink", ScalarType::String),
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

        Statement::ShowIndexes { .. } => (
            Some(RelationDesc::new(
                RelationType::new(vec![
                    ColumnType::new(ScalarType::String),
                    ColumnType::new(ScalarType::String),
                    ColumnType::new(ScalarType::String).nullable(true),
                    ColumnType::new(ScalarType::String).nullable(true),
                    ColumnType::new(ScalarType::Bool),
                    ColumnType::new(ScalarType::Int64),
                ]),
                vec![
                    "Source_or_view",
                    "Key_name",
                    "Column_name",
                    "Expression",
                    "Null",
                    "Seq_in_index",
                ]
                .iter()
                .map(|s| Some(*s))
                .collect::<Vec<_>>(),
            )),
            vec![],
        ),

        Statement::ShowDatabases { .. } => (
            Some(RelationDesc::empty().add_column("Database", ScalarType::String)),
            vec![],
        ),

        Statement::ShowObjects {
            object_type,
            full,
            materialized,
            ..
        } => {
            let col_name = object_type_as_plural_str(object_type);
            (
                Some(if full {
                    let mut relation_desc = RelationDesc::empty()
                        .add_column(col_name, ScalarType::String)
                        .add_column("TYPE", ScalarType::String);
                    if ObjectType::View == object_type {
                        relation_desc = relation_desc.add_column("QUERYABLE", ScalarType::Bool);
                    }
                    if !materialized &&
                        (ObjectType::View == object_type || ObjectType::Source == object_type)
                    {
                        relation_desc = relation_desc.add_column("MATERIALIZED", ScalarType::Bool);
                    }
                    relation_desc
                } else {
                    RelationDesc::empty().add_column(col_name, ScalarType::String)
                }),
                vec![],
            )
        }
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

        Statement::Tail { name, .. } => {
            let name = scx.resolve_name(name)?;
            let sql_object = scx.catalog.get(&name)?;
            (Some(sql_object.desc()?.clone()), vec![])
        }

        Statement::Query(query) => {
            // TODO(benesch): ideally we'd save `relation_expr` and `finishing`
            // somewhere, so we don't have to reanalyze the whole query when
            // `handle_statement` is called. This will require a complicated
            // dance when bind parameters are implemented, so punting for now.
            let (_relation_expr, desc, _finishing, param_types) =
                query::plan_root_query(scx, *query, QueryLifetime::OneShot)?;
            (Some(desc), param_types)
        }
        Statement::CreateTable { .. } => bail!("CREATE TABLE statements are not supported. Try CREATE SOURCE or CREATE [MATERIALIZED] VIEW instead."),
        _ => bail!("unsupported SQL statement: {:?}", stmt),
    })
}

pub fn handle_statement(
    catalog: &Catalog,
    session: &dyn PlanSession,
    stmt: Statement,
    params: &Params,
) -> Result<Plan, failure::Error> {
    let scx = &StatementContext { catalog, session };
    match stmt {
        Statement::Tail { name } => handle_tail(scx, name),
        Statement::StartTransaction { .. } => Ok(Plan::StartTransaction),
        Statement::Commit { .. } => Ok(Plan::CommitTransaction),
        Statement::Rollback { .. } => Ok(Plan::AbortTransaction),
        Statement::CreateDatabase {
            name,
            if_not_exists,
        } => handle_create_database(scx, name, if_not_exists),
        Statement::CreateSchema {
            name,
            if_not_exists,
        } => handle_create_schema(scx, name, if_not_exists),
        Statement::CreateSource { .. } => handle_create_source(scx, stmt),
        Statement::CreateView { .. } => handle_create_view(scx, stmt, params),
        Statement::CreateSink { .. } => handle_create_sink(scx, stmt),
        Statement::CreateIndex { .. } => handle_create_index(scx, stmt),
        Statement::DropDatabase { name, if_exists } => handle_drop_database(scx, name, if_exists),
        Statement::DropObjects {
            object_type,
            if_exists,
            names,
            cascade,
        } => handle_drop_objects(scx, object_type, if_exists, names, cascade),
        Statement::Query(query) => handle_select(scx, *query, params),
        Statement::SetVariable {
            local,
            variable,
            value,
        } => handle_set_variable(scx, local, variable, value),
        Statement::ShowVariable { variable } => handle_show_variable(scx, variable),
        Statement::ShowDatabases { filter } => handle_show_databases(scx, filter.as_ref()),
        Statement::ShowObjects {
            extended,
            full,
            object_type: ot,
            from,
            materialized,
            filter,
        } => handle_show_objects(scx, extended, full, materialized, ot, from, filter),
        Statement::ShowIndexes {
            extended,
            table_name,
            filter,
        } => handle_show_indexes(scx, extended, table_name, filter.as_ref()),
        Statement::ShowColumns {
            extended,
            full,
            table_name,
            filter,
        } => handle_show_columns(scx, extended, full, table_name, filter.as_ref()),
        Statement::ShowCreateView { view_name } => handle_show_create_view(scx, view_name),
        Statement::ShowCreateSource { source_name } => handle_show_create_source(scx, source_name),
        Statement::ShowCreateSink { sink_name } => handle_show_create_sink(scx, sink_name),
        Statement::Explain {
            stage,
            explainee,
            options,
        } => handle_explain(scx, stage, explainee, options, params),

        _ => bail!("unsupported SQL statement: {:?}", stmt),
    }
}

fn handle_set_variable(
    _: &StatementContext,
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

fn handle_show_variable(_: &StatementContext, variable: Ident) -> Result<Plan, failure::Error> {
    if variable.value == unicase::Ascii::new("ALL") {
        Ok(Plan::ShowAllVariables)
    } else {
        Ok(Plan::ShowVariable(variable.value))
    }
}

fn handle_tail(scx: &StatementContext, from: ObjectName) -> Result<Plan, failure::Error> {
    let from = scx.resolve_name(from)?;
    let entry = scx.catalog.get(&from)?;
    match entry.item() {
        CatalogItem::Source(_) | CatalogItem::View(_) => Ok(Plan::Tail(entry.id())),
        CatalogItem::Index(_) | CatalogItem::Sink(_) => bail!(
            "'{}' cannot be tailed because it is a {}",
            from,
            entry.item().type_string()
        ),
    }
}

fn handle_show_databases(
    scx: &StatementContext,
    filter: Option<&ShowStatementFilter>,
) -> Result<Plan, failure::Error> {
    if filter.is_some() {
        bail!("SHOW DATABASES {LIKE | WHERE} is not yet supported");
    }
    Ok(Plan::SendRows(
        scx.catalog
            .databases()
            .map(|database| Row::pack(&[Datum::from(database)]))
            .collect(),
    ))
}

fn handle_show_objects(
    scx: &StatementContext,
    extended: bool,
    full: bool,
    materialized: bool,
    object_type: ObjectType,
    from: Option<ObjectName>,
    filter: Option<ShowStatementFilter>,
) -> Result<Plan, failure::Error> {
    let classify_id = |id| match id {
        GlobalId::System(_) => "SYSTEM",
        GlobalId::User(_) => "USER",
    };
    let make_row = |name: &str, class| {
        if full {
            Row::pack(&[Datum::from(name), Datum::from(class)])
        } else {
            Row::pack(&[Datum::from(name)])
        }
    };

    if let ObjectType::Schema = object_type {
        if filter.is_some() {
            bail!("SHOW SCHEMAS ... {LIKE | WHERE} is not supported");
        }

        let schemas = if let Some(from) = from {
            if from.0.len() != 1 {
                bail!(
                    "database name '{}' does not have exactly one component",
                    from
                );
            }
            let database_spec = DatabaseSpecifier::Name(normalize::ident(from.0[0].clone()));
            scx.catalog
                .get_schemas(&database_spec)
                .ok_or_else(|| format_err!("database '{:?}' does not exist", database_spec))?
        } else {
            scx.catalog
                .get_schemas(&scx.session.database())
                .ok_or_else(|| {
                    format_err!(
                        "session database '{}' does not exist",
                        scx.session.database()
                    )
                })?
        };

        let mut rows = vec![];
        for name in schemas.keys() {
            rows.push(make_row(name, "USER"));
        }
        if extended {
            let ambient_schemas = scx
                .catalog
                .get_schemas(&DatabaseSpecifier::Ambient)
                .expect("ambient database should always exist");
            for name in ambient_schemas.keys() {
                rows.push(make_row(name, "SYSTEM"));
            }
        }
        rows.sort_unstable_by(move |a, b| a.unpack_first().cmp(&b.unpack_first()));
        Ok(Plan::SendRows(rows))
    } else {
        let like_regex = match filter {
            Some(ShowStatementFilter::Like(pattern)) => like_pattern::build_regex(&pattern)?,
            Some(ShowStatementFilter::Where(_)) => bail!("SHOW ... WHERE is not supported"),
            None => like_pattern::build_regex("%")?,
        };

        let empty_schema = BTreeMap::new();
        let items = if let Some(mut from) = from {
            if from.0.len() > 2 {
                bail!(
                    "schema name '{}' cannot have more than two components",
                    from
                );
            }
            let schema_name = normalize::ident(from.0.pop().unwrap());
            let database_spec = from
                .0
                .pop()
                .map(|n| DatabaseSpecifier::Name(normalize::ident(n)))
                .unwrap_or_else(|| scx.session.database());
            &scx.catalog
                .database_resolver(database_spec)?
                .resolve_schema(&schema_name)
                .ok_or_else(|| format_err!("schema '{}' does not exist", schema_name))?
                .0
                .items
        } else {
            let resolver = scx.catalog.database_resolver(scx.session.database())?;
            scx.session
                .search_path()
                .iter()
                .filter_map(|schema_name| resolver.resolve_schema(schema_name))
                .find(|(_schema, typ)| *typ == SchemaType::Normal)
                .map_or_else(|| &empty_schema, |(schema, _typ)| &schema.items)
        };

        let filtered_items = items
            .iter()
            .map(|(name, id)| (name, scx.catalog.get_by_id(id)))
            .filter(|(_name, entry)| {
                object_type_matches(object_type, entry.item())
                    && like_regex.is_match(&entry.name().to_string())
            });

        if object_type == ObjectType::View || object_type == ObjectType::Source {
            Ok(Plan::ShowViews {
                ids: filtered_items
                    .map(|(name, entry)| (name.clone(), entry.id()))
                    .collect::<Vec<_>>(),
                full,
                show_queryable: object_type == ObjectType::View,
                limit_materialized: materialized,
            })
        } else {
            let mut rows = filtered_items
                .map(|(name, entry)| make_row(name, classify_id(entry.id())))
                .collect::<Vec<_>>();
            rows.sort_unstable_by(move |a, b| a.unpack_first().cmp(&b.unpack_first()));
            Ok(Plan::SendRows(rows))
        }
    }
}

fn handle_show_indexes(
    scx: &StatementContext,
    extended: bool,
    from_name: ObjectName,
    filter: Option<&ShowStatementFilter>,
) -> Result<Plan, failure::Error> {
    if extended {
        bail!("SHOW EXTENDED INDEXES is not supported")
    }
    if filter.is_some() {
        bail!("SHOW INDEXES ... WHERE is not supported");
    }
    let from_name = scx.resolve_name(from_name)?;
    let from_entry = scx.catalog.get(&from_name)?;
    if !object_type_matches(ObjectType::View, from_entry.item())
        && !object_type_matches(ObjectType::Source, from_entry.item())
    {
        bail!(
            "cannot show indexes on {} because it is a {}",
            from_name,
            from_entry.item().type_string()
        );
    }
    let rows = scx
        .catalog
        .iter()
        .filter(|entry| {
            object_type_matches(ObjectType::Index, entry.item())
                && entry.uses() == vec![from_entry.id()]
        })
        .flat_map(|entry| match entry.item() {
            CatalogItem::Index(catalog::Index {
                create_sql,
                keys,
                on,
                eval_env: _,
            }) => {
                let key_sqls = match crate::parse(create_sql.to_owned())
                    .expect("create_sql cannot be invalid")
                    .into_element()
                {
                    Statement::CreateIndex { key_parts, .. } => key_parts,
                    _ => unreachable!(),
                };
                let mut row_subset = Vec::new();
                for (i, (key_expr, key_sql)) in keys.iter().zip_eq(key_sqls).enumerate() {
                    let desc = scx.catalog.get_by_id(&on).desc().unwrap();
                    let key_sql = key_sql.to_string();
                    let arena = RowArena::new();
                    let (col_name, func) = match key_expr {
                        expr::ScalarExpr::Column(i) => {
                            let col_name = match desc.get_unambiguous_name(*i) {
                                Some(col_name) => col_name.to_string(),
                                None => format!("@{}", i + 1),
                            };
                            (Datum::String(arena.push_string(col_name)), Datum::Null)
                        }
                        _ => (Datum::Null, Datum::String(arena.push_string(key_sql))),
                    };
                    row_subset.push(Row::pack(&vec![
                        Datum::String(&from_entry.name().to_string()),
                        Datum::String(&entry.name().to_string()),
                        col_name,
                        func,
                        Datum::from(key_expr.typ(desc.typ()).nullable),
                        Datum::from((i + 1) as i64),
                    ]));
                }
                row_subset
            }
            _ => unreachable!(),
        })
        .collect();
    Ok(Plan::SendRows(rows))
}

/// Create an immediate result that describes all the columns for the given table
fn handle_show_columns(
    scx: &StatementContext,
    extended: bool,
    full: bool,
    table_name: ObjectName,
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

    let table_name = scx.resolve_name(table_name)?;
    let column_descriptions: Vec<_> = scx
        .catalog
        .get(&table_name)?
        .desc()?
        .iter()
        .map(|(name, typ)| {
            let name = name.map(|n| n.to_string());
            Row::pack(&[
                Datum::String(name.as_deref().unwrap_or("?")),
                Datum::String(if typ.nullable { "YES" } else { "NO" }),
                Datum::String(pgrepr::Type::from(typ.scalar_type).name()),
            ])
        })
        .collect();

    Ok(Plan::SendRows(column_descriptions))
}

fn handle_show_create_view(
    scx: &StatementContext,
    view_name: ObjectName,
) -> Result<Plan, failure::Error> {
    let view_name = scx.resolve_name(view_name)?;
    let create_sql = if let CatalogItem::View(view) = scx.catalog.get(&view_name)?.item() {
        &view.create_sql
    } else {
        bail!("'{}' is not a view", view_name);
    };
    Ok(Plan::SendRows(vec![Row::pack(&[
        Datum::String(&view_name.to_string()),
        Datum::String(create_sql),
    ])]))
}

fn handle_show_create_source(
    scx: &StatementContext,
    object_name: ObjectName,
) -> Result<Plan, failure::Error> {
    let name = scx.resolve_name(object_name)?;
    if let CatalogItem::Source(catalog::Source { create_sql, .. }) = scx.catalog.get(&name)?.item()
    {
        Ok(Plan::SendRows(vec![Row::pack(&[
            Datum::String(&name.to_string()),
            Datum::String(&create_sql),
        ])]))
    } else {
        bail!("{} is not a source", name);
    }
}

fn handle_show_create_sink(
    scx: &StatementContext,
    sink_name: ObjectName,
) -> Result<Plan, failure::Error> {
    let sink_name = scx.resolve_name(sink_name)?;
    let create_sql = if let CatalogItem::Sink(sink) = scx.catalog.get(&sink_name)?.item() {
        &sink.create_sql
    } else {
        bail!("'{}' is not a sink", sink_name);
    };
    Ok(Plan::SendRows(vec![Row::pack(&[
        Datum::String(&sink_name.to_string()),
        Datum::String(create_sql),
    ])]))
}

fn kafka_sink_builder(
    format: Option<Format>,
    mut broker: String,
    topic_prefix: String,
    desc: RelationDesc,
    topic_suffix: String,
) -> Result<SinkConnectorBuilder, failure::Error> {
    let schema_registry_url = match format {
        Some(Format::Avro(AvroSchema::CsrUrl { url, seed })) => {
            if seed.is_some() {
                bail!("SEED option does not make sense with sinks");
            }
            url.parse()?
        }
        _ => bail!("only confluent schema registry avro sinks are supported"),
    };

    if !broker.contains(':') {
        broker += ":9092";
    }
    let broker_url = broker.parse()?;

    let encoder = Encoder::new(desc);
    let value_schema = encoder.writer_schema().canonical_form();

    Ok(SinkConnectorBuilder::Kafka(KafkaSinkConnectorBuilder {
        broker_url,
        schema_registry_url,
        value_schema,
        topic_prefix,
        topic_suffix,
    }))
}

fn avro_ocf_sink_builder(
    format: Option<Format>,
    path: String,
    file_name_suffix: String,
) -> Result<SinkConnectorBuilder, failure::Error> {
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
    }))
}

fn handle_create_sink(scx: &StatementContext, stmt: Statement) -> Result<Plan, failure::Error> {
    let create_sql = normalize::create_statement(scx, stmt.clone())?;
    let (name, from, connector, format, if_not_exists) = match stmt {
        Statement::CreateSink {
            name,
            from,
            connector,
            format,
            if_not_exists,
        } => (name, from, connector, format, if_not_exists),
        _ => unreachable!(),
    };

    let name = scx.allocate_name(normalize::object_name(name)?);
    let from = scx.catalog.get(&scx.resolve_name(from)?)?;
    let suffix = format!(
        "{}-{}",
        scx.catalog
            .creation_time()
            .duration_since(UNIX_EPOCH)?
            .as_secs(),
        scx.catalog.nonce()
    );

    let connector_builder = match connector {
        Connector::File { .. } => bail!("file sinks are not yet supported"),
        Connector::Kafka { broker, topic } => {
            kafka_sink_builder(format, broker, topic, from.desc()?.clone(), suffix)?
        }
        Connector::Kinesis { .. } => bail!("Kinesis sinks are not yet supported"),
        Connector::AvroOcf { path } => avro_ocf_sink_builder(format, path, suffix)?,
    };

    Ok(Plan::CreateSink {
        name,
        sink: Sink {
            create_sql,
            from: from.id(),
            connector_builder,
        },
        if_not_exists,
    })
}

fn handle_create_index(scx: &StatementContext, stmt: Statement) -> Result<Plan, failure::Error> {
    let create_sql = normalize::create_statement(scx, stmt.clone())?;
    let (name, on_name, key_parts, if_not_exists) = match stmt {
        Statement::CreateIndex {
            name,
            on_name,
            key_parts,
            if_not_exists,
        } => (name, on_name, key_parts, if_not_exists),
        _ => unreachable!(),
    };
    let on_name = scx.resolve_name(on_name)?;
    let catalog_entry = scx.catalog.get(&on_name)?;
    let keys = query::plan_index_exprs(scx, catalog_entry.desc()?, &key_parts)?;
    if !object_type_matches(ObjectType::View, catalog_entry.item())
        && !object_type_matches(ObjectType::Source, catalog_entry.item())
    {
        bail!(
            "index cannot be created on {} because it is a {}",
            on_name,
            catalog_entry.item().type_string()
        );
    }
    Ok(Plan::CreateIndex {
        name: FullName {
            database: on_name.database.clone(),
            schema: on_name.schema.clone(),
            item: normalize::ident(name),
        },
        index: Index {
            create_sql,
            on: catalog_entry.id(),
            keys,
        },
        if_not_exists,
    })
}

fn handle_create_database(
    _scx: &StatementContext,
    name: Ident,
    if_not_exists: bool,
) -> Result<Plan, failure::Error> {
    Ok(Plan::CreateDatabase {
        name: normalize::ident(name),
        if_not_exists,
    })
}

fn handle_create_schema(
    scx: &StatementContext,
    mut name: ObjectName,
    if_not_exists: bool,
) -> Result<Plan, failure::Error> {
    if name.0.len() > 2 {
        bail!("schema name {} has more than two components", name);
    }
    let schema_name = normalize::ident(
        name.0
            .pop()
            .expect("names always have at least one component"),
    );
    let database_name = name
        .0
        .pop()
        .map(|n| DatabaseSpecifier::Name(normalize::ident(n)))
        .unwrap_or_else(|| scx.session.database());
    Ok(Plan::CreateSchema {
        database_name,
        schema_name,
        if_not_exists,
    })
}

fn handle_create_view(
    scx: &StatementContext,
    mut stmt: Statement,
    params: &Params,
) -> Result<Plan, failure::Error> {
    let create_sql = normalize::create_statement(scx, stmt.clone())?;
    let (name, columns, query, materialized, if_exists, with_options) = match &mut stmt {
        Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            if_exists,
            with_options,
        } => (name, columns, query, materialized, if_exists, with_options),
        _ => unreachable!(),
    };
    if !with_options.is_empty() {
        bail!("WITH options are not yet supported");
    }
    let name = scx.allocate_name(normalize::object_name(name.to_owned())?);
    let replace = if *if_exists == IfExistsBehavior::Replace {
        let if_exists = true;
        let cascade = false;
        handle_drop_item(scx, ObjectType::View, if_exists, &name, cascade)?
    } else {
        None
    };
    let (mut relation_expr, mut desc, finishing, _) =
        query::plan_root_query(scx, *query.clone(), QueryLifetime::Static)?;
    // TODO(jamii) can views even have parameters?
    relation_expr.bind_parameters(&params);
    //TODO: materialize#724 - persist finishing information with the view?
    relation_expr.finish(finishing);
    let relation_expr = relation_expr.decorrelate()?;
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
            desc.set_name(i, Some(normalize::column_name(name.clone())));
        }
    }
    let materialize = *materialized; // Normalize for `raw_sql` below.
    let if_not_exists = *if_exists == IfExistsBehavior::Skip;
    Ok(Plan::CreateView {
        name,
        view: View {
            create_sql,
            expr: relation_expr,
            desc,
        },
        replace,
        materialize,
        if_not_exists,
    })
}

async fn purify_format(
    format: &mut Option<Format>,
    connector: &mut Connector,
    col_names: &mut Vec<Ident>,
) -> Result<(), failure::Error> {
    match format {
        Some(Format::Avro(schema)) => match schema {
            AvroSchema::CsrUrl { url, seed } => {
                let topic = if let Connector::Kafka { topic, .. } = connector {
                    topic
                } else {
                    bail!("Confluent Schema Registry is only supported with Kafka sources")
                };
                if seed.is_none() {
                    let url = url.parse()?;
                    let Schema {
                        key_schema,
                        value_schema,
                        ..
                    } = get_remote_avro_schema(url, topic.clone()).await?;
                    *seed = Some(CsrSeed {
                        key_schema,
                        value_schema,
                    });
                }
            }
            AvroSchema::Schema(sql_parser::ast::Schema::File(path)) => {
                let value_schema = tokio::fs::read_to_string(path).await?;
                *schema = AvroSchema::Schema(sql_parser::ast::Schema::Inline(value_schema));
            }
            _ => {}
        },
        Some(Format::Protobuf { schema, .. }) => {
            if let sql_parser::ast::Schema::File(path) = schema {
                let descriptors = tokio::fs::read(path).await?;
                let mut buf = String::new();
                strconv::format_bytes(&mut buf, &descriptors);
                *schema = sql_parser::ast::Schema::Inline(buf);
            }
        }
        Some(Format::Csv {
            header_row,
            delimiter,
            ..
        }) => {
            if *header_row && col_names.is_empty() {
                match connector {
                    Connector::File { path } => {
                        let path = path.clone();
                        let f = tokio::fs::File::open(path).await?;
                        let f = tokio::io::BufReader::new(f);
                        let csv_header = f.lines().next_line().await?;
                        match csv_header {
                            Some(csv_header) => {
                                csv_header
                                    .split(*delimiter as char)
                                    .for_each(|v| col_names.push(Ident::from(v)));
                            }
                            None => bail!("CSV file expected header line, but is empty"),
                        }
                    }
                    _ => bail!("CSV format with headers only works with file connectors"),
                }
            }
        }
        _ => (),
    }
    Ok(())
}

pub async fn purify_statement(mut stmt: Statement) -> Result<Statement, failure::Error> {
    if let Statement::CreateSource {
        col_names,
        connector,
        format,
        with_options,
        envelope,
        ..
    } = &mut stmt
    {
        let with_options_map = normalize::with_options(with_options);

        match connector {
            Connector::Kafka { broker, .. } if !broker.contains(':') => {
                *broker += ":9092";
            }
            Connector::AvroOcf { path, .. } => {
                let path = path.clone();
                let f = tokio::fs::File::open(path).await?;
                let r = avro::Reader::new(f).await?;
                if !with_options_map.contains_key("reader_schema") {
                    let schema = serde_json::to_string(r.writer_schema()).unwrap();
                    with_options.push(sql_parser::ast::SqlOption {
                        name: sql_parser::ast::Ident::new("reader_schema"),
                        value: sql_parser::ast::Value::SingleQuotedString(schema),
                    });
                }
            }
            _ => (),
        }

        purify_format(format, connector, col_names).await?;
        if let sql_parser::ast::Envelope::Upsert(format) = envelope {
            purify_format(format, connector, col_names).await?;
        }

        // Tests that with_options are valid for generating auth and, only in
        // the case of creating SASL plaintext connections to Kerberized Kafka
        // clusters, tests the configuration.
        KafkaAuth::create_from_with_options(&mut with_options_map.clone(), true)?;
    }
    Ok(stmt)
}

fn handle_create_source(scx: &StatementContext, stmt: Statement) -> Result<Plan, failure::Error> {
    match &stmt {
        Statement::CreateSource {
            name,
            col_names,
            connector,
            with_options,
            format,
            envelope,
            if_not_exists,
            materialized,
        } => {
            let get_encoding = |format: &Option<Format>| {
                let format = format
                    .as_ref()
                    .ok_or_else(|| format_err!("Source format must be specified"))?;

                Ok(match format {
                    Format::Bytes => DataEncoding::Bytes,
                    Format::Avro(schema) => {
                        let Schema {
                            key_schema,
                            value_schema,
                            schema_registry_url,
                        } = match schema {
                            // TODO(jldlaughlin): we need a way to pass in primary key information
                            // when building a source from a string or file.
                            AvroSchema::Schema(sql_parser::ast::Schema::Inline(schema)) => Schema {
                                key_schema: None,
                                value_schema: schema.clone(),
                                schema_registry_url: None,
                            },
                            AvroSchema::Schema(sql_parser::ast::Schema::File(_)) => {
                                unreachable!("File schema should already have been inlined")
                            }
                            AvroSchema::CsrUrl { url: csr_url, seed } => {
                                let csr_url: Url = csr_url.parse()?;
                                if let Some(seed) = seed {
                                    Schema {
                                        key_schema: seed.key_schema.clone(),
                                        value_schema: seed.value_schema.clone(),
                                        schema_registry_url: Some(csr_url),
                                    }
                                } else {
                                    unreachable!(
                                        "CSR seed resolution should already have been called"
                                    )
                                }
                            }
                        };

                        DataEncoding::Avro(AvroEncoding {
                            key_schema,
                            value_schema,
                            schema_registry_url,
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
                        DataEncoding::Regex { regex }
                    }
                    Format::Csv {
                        header_row,
                        n_cols,
                        delimiter,
                    } => {
                        let n_cols = if col_names.is_empty() {
                            match n_cols {
                                Some(n) => *n,
                                None => bail!("Cannot determine number of columns in CSV source; specify using \
                                CREATE SOURCE...FORMAT CSV WITH X COLUMNS")
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
                    Format::Json => bail!("JSON sources are not supported yet."),
                    Format::Text => DataEncoding::Text,
                })
            };

            let mut with_options = normalize::with_options(with_options);

            let mut consistency = Consistency::RealTime;
            let (external_connector, mut encoding) = match connector {
                Connector::Kafka { broker, topic, .. } => {
                    let auth = KafkaAuth::create_from_with_options(&mut with_options, false)?;

                    consistency = match with_options.remove("consistency") {
                        None => Consistency::RealTime,
                        Some(Value::SingleQuotedString(topic)) => Consistency::BringYourOwn(topic),
                        Some(_) => bail!("consistency must be a string"),
                    };

                    let connector = ExternalSourceConnector::Kafka(KafkaSourceConnector {
                        url: broker.parse()?,
                        topic: topic.clone(),
                        auth,
                    });
                    let encoding = get_encoding(format)?;
                    (connector, encoding)
                }
                Connector::Kinesis { arn, .. } => {
                    let arn: ARN = match arn.parse() {
                        Ok(arn) => arn,
                        Err(e) => bail!("Unable to parse provided ARN: {:#?}", e),
                    };
                    let stream_name = match arn.resource {
                        Resource::Path(path) => {
                            if path.starts_with("stream/") {
                                String::from(&path["stream/".len()..])
                            } else {
                                bail!("Unable to parse stream name from resource path: {}", path);
                            }
                        }
                        _ => bail!("Unsupported AWS Resource type: {:#?}", arn.resource),
                    };

                    let region: Region = match arn.region {
                        Some(region) => match region.parse() {
                            Ok(region) => region,
                            Err(e) => {
                                // Region's fromstr doesn't support parsing custom regions.
                                // If a Kinesis stream's ARN indicates it exists in a custom
                                // region, support it iff a valid endpoint for the stream
                                // is also provided.
                                match with_options.remove("endpoint") {
                                    Some(Value::SingleQuotedString(endpoint)) => Region::Custom {
                                        name: region,
                                        endpoint,
                                    },
                                    _ => bail!(
                                        "Unable to parse AWS region: {}. If providing a custom \
                                         region, an `endpoint` option must also be provided",
                                        e
                                    ),
                                }
                            }
                        },
                        None => bail!("Provided ARN does not include an AWS region"),
                    };

                    // todo@jldlaughlin: We should support all (?) variants of AWS authentication.
                    // https://github.com/materializeinc/materialize/issues/1991
                    let access_key = match with_options.remove("access_key") {
                        Some(Value::SingleQuotedString(access_key)) => access_key,
                        _ => bail!("Kinesis sources require an `access_key` option"),
                    };
                    let secret_access_key = match with_options.remove("secret_access_key") {
                        Some(Value::SingleQuotedString(secret_access_key)) => secret_access_key,
                        _ => bail!("Kinesis sources require a `secret_access_key` option"),
                    };
                    let token = match with_options.remove("token") {
                        Some(Value::SingleQuotedString(token)) => Some(token),
                        _ => None,
                    };

                    let connector = ExternalSourceConnector::Kinesis(KinesisSourceConnector {
                        stream_name,
                        region,
                        access_key,
                        secret_access_key,
                        token,
                    });
                    let encoding = get_encoding(format)?;
                    (connector, encoding)
                }
                Connector::File { path, .. } => {
                    let tail = match with_options.remove("tail") {
                        None => false,
                        Some(Value::Boolean(b)) => b,
                        Some(_) => bail!("tail must be a boolean"),
                    };
                    let connector = ExternalSourceConnector::File(FileSourceConnector {
                        path: path.clone().into(),
                        tail,
                    });
                    let encoding = get_encoding(format)?;
                    (connector, encoding)
                }
                Connector::AvroOcf { path, .. } => {
                    let tail = match with_options.remove("tail") {
                        None => false,
                        Some(Value::Boolean(b)) => b,
                        Some(_) => bail!("tail must be a boolean"),
                    };
                    let connector = ExternalSourceConnector::AvroOcf(FileSourceConnector {
                        path: path.clone().into(),
                        tail,
                    });
                    if format.is_some() {
                        bail!("avro ocf sources cannot specify a format");
                    }
                    let reader_schema = match with_options
                        .remove("reader_schema")
                        .expect("purification guarantees presence of reader_schema")
                    {
                        Value::SingleQuotedString(s) => s,
                        _ => bail!("reader_schema option must be a string"),
                    };
                    let encoding = DataEncoding::AvroOcf { reader_schema };
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
            let envelope = match &envelope {
                sql_parser::ast::Envelope::None => dataflow_types::Envelope::None,
                sql_parser::ast::Envelope::Debezium => dataflow_types::Envelope::Debezium,
                sql_parser::ast::Envelope::Upsert(key_format) => match connector {
                    Connector::Kafka { .. } => {
                        let mut key_encoding = if key_format.is_some() {
                            get_encoding(key_format)?
                        } else {
                            encoding.clone()
                        };
                        if let DataEncoding::Avro(AvroEncoding {
                            key_schema,
                            value_schema,
                            ..
                        }) = &mut key_encoding
                        {
                            if key_schema.is_some() {
                                *value_schema = key_schema.take().unwrap();
                            }
                        }
                        dataflow_types::Envelope::Upsert(key_encoding)
                    }
                    _ => bail!("Upsert envelope for non-Kafka sources not supported yet"),
                },
            };

            if let dataflow_types::Envelope::Upsert(_) = envelope {
                // delete the key schema because 1) the format in the upsert is already
                // taking care of that 2) to prevent schema validation from looking for the
                // key columns in the value record
                if let DataEncoding::Avro(AvroEncoding { key_schema, .. }) = &mut encoding {
                    *key_schema = None;
                }
                // TODO: remove the bail once PR #2943 is in
                // the bail is meant to prevent someone from accidentally trying
                // the upsert envelope and then causing a panic.
                bail!("Upsert envelope is not supported yet");
            }

            let mut desc = encoding.desc(&envelope)?;

            let typ = desc.typ();
            if !col_names.is_empty() {
                if col_names.len() != typ.column_types.len() {
                    bail!(
                        "SOURCE definition has {} columns, but expected {} columns",
                        col_names.len(),
                        typ.column_types.len()
                    )
                }
                for (i, name) in col_names.iter().enumerate() {
                    desc.set_name(i, Some(normalize::column_name(name.clone())));
                }
            }

            // TODO(benesch): the available metadata columns should not depend
            // on the format.
            //
            // TODO(brennan): They should not depend on the envelope either. Figure out a way to
            // make all of this more tasteful.
            match (&encoding, &envelope) {
                (DataEncoding::Avro { .. }, _)
                | (DataEncoding::Protobuf { .. }, _)
                | (_, Envelope::Debezium) => (),
                _ => desc.add_cols(external_connector.metadata_columns()),
            }

            let if_not_exists = *if_not_exists;
            let materialized = *materialized;
            let name = scx.allocate_name(normalize::object_name(name.clone())?);
            let create_sql = normalize::create_statement(&scx, stmt)?;

            let source = Source {
                create_sql,
                connector: SourceConnector::External {
                    connector: external_connector,
                    encoding,
                    envelope,
                    consistency,
                },
                desc,
            };
            Ok(Plan::CreateSource {
                name,
                source,
                if_not_exists,
                materialized,
            })
        }
        other => bail!("Unsupported statement: {:?}", other),
    }
}

fn handle_drop_database(
    scx: &StatementContext,
    name: Ident,
    if_exists: bool,
) -> Result<Plan, failure::Error> {
    let name = normalize::ident(name);
    let spec = DatabaseSpecifier::Name(name.clone());
    match scx.catalog.database_resolver(spec) {
        Ok(_) => (),
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the database
            // does not exist.
        }
        Err(err) => return Err(err.into()),
    }
    Ok(Plan::DropDatabase { name })
}

fn handle_drop_objects(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    names: Vec<ObjectName>,
    cascade: bool,
) -> Result<Plan, failure::Error> {
    match object_type {
        ObjectType::Schema => handle_drop_schema(scx, if_exists, names, cascade),
        ObjectType::Source | ObjectType::View | ObjectType::Index | ObjectType::Sink => {
            handle_drop_items(scx, object_type, if_exists, names, cascade)
        }
        _ => bail!("unsupported SQL statement: DROP {}", object_type),
    }
}

fn handle_drop_schema(
    scx: &StatementContext,
    if_exists: bool,
    names: Vec<ObjectName>,
    cascade: bool,
) -> Result<Plan, failure::Error> {
    if names.len() != 1 {
        bail!("DROP SCHEMA with multiple schemas is not yet supported");
    }
    let mut name = names.into_element();
    let schema_name = normalize::ident(name.0.pop().unwrap());
    let database_name = name
        .0
        .pop()
        .map(|n| DatabaseSpecifier::Name(normalize::ident(n)))
        .unwrap_or_else(|| scx.session.database());
    match scx.catalog.database_resolver(database_name.clone()) {
        Ok(resolver) => {
            match resolver.resolve_schema(&schema_name) {
                None if if_exists => {
                    // TODO(benesch): generate a notice indicating that
                    // the schema does not exist.
                }
                None => bail!("schema '{}.{}' does not exist", database_name, schema_name),
                Some((_schema, SchemaType::Ambient)) => {
                    bail!(
                        "cannot drop schema {} because it is required by the database system",
                        schema_name
                    );
                }
                Some((schema, SchemaType::Normal)) if !cascade && !schema.items.is_empty() => {
                    bail!("schema '{}.{}' cannot be dropped without CASCADE while it contains objects", database_name, schema_name);
                }
                _ => (),
            }
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the
            // database does not exist.
        }
        Err(err) => return Err(err.into()),
    }
    Ok(Plan::DropSchema {
        database_name,
        schema_name,
    })
}

fn handle_drop_items(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    names: Vec<ObjectName>,
    cascade: bool,
) -> Result<Plan, failure::Error> {
    let names = names
        .into_iter()
        .map(|n| scx.resolve_name(n))
        .collect::<Vec<_>>();
    let mut ids = vec![];
    for name in names {
        match name {
            Ok(name) => ids.extend(handle_drop_item(
                scx,
                object_type,
                if_exists,
                &name,
                cascade,
            )?),
            Err(_) if if_exists => {
                // TODO(benesch): generate a notice indicating this
                // item does not exist.
            }
            Err(err) => return Err(err),
        }
    }
    Ok(Plan::DropItems {
        items: ids,
        ty: object_type,
    })
}

fn handle_drop_item(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: &FullName,
    cascade: bool,
) -> Result<Option<GlobalId>, failure::Error> {
    match scx.catalog.get(name) {
        Ok(catalog_entry) => {
            if catalog_entry.id().is_system() {
                bail!(
                    "cannot drop item {} because it is required by the database system",
                    name
                );
            }
            if !object_type_matches(object_type, catalog_entry.item()) {
                bail!("{} is not of type {}", name, object_type);
            }
            if !cascade {
                for id in catalog_entry.used_by() {
                    let dep = scx.catalog.get_by_id(id);
                    match dep.item() {
                        CatalogItem::Source(_) | CatalogItem::View(_) | CatalogItem::Sink(_) => {
                            bail!(
                                "cannot drop {}: still depended upon by catalog item '{}'",
                                catalog_entry.name(),
                                dep.name()
                            );
                        }
                        CatalogItem::Index(_) => (),
                    }
                }
            }
            Ok(Some(catalog_entry.id()))
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating this
            // item does not exist.
            Ok(None)
        }
        Err(err) => Err(err.into()),
    }
}

fn handle_select(
    scx: &StatementContext,
    query: Query,
    params: &Params,
) -> Result<Plan, failure::Error> {
    let (relation_expr, _, finishing) = handle_query(scx, query, params, QueryLifetime::OneShot)?;
    Ok(Plan::Peek {
        source: relation_expr,
        when: PeekWhen::Immediately,
        finishing,
        materialize: true,
    })
}

fn handle_explain(
    scx: &StatementContext,
    stage: ExplainStage,
    explainee: Explainee,
    options: ExplainOptions,
    _params: &Params,
) -> Result<Plan, failure::Error> {
    let is_view = if let Explainee::View(_) = explainee {
        true
    } else {
        false
    };
    let (sql, query) = match explainee {
        Explainee::View(name) => {
            let full_name = scx.resolve_name(name.clone())?;
            let entry = scx.catalog.get(&full_name)?;
            let view = match entry.item() {
                CatalogItem::View(view) => view,
                other => bail!(
                    "Expected {} to be a view, not a {}",
                    name,
                    other.type_string()
                ),
            };
            let parsed = crate::parse(view.create_sql.clone())
                .expect("Sql for existing view should be valid sql");
            let query = match parsed.into_last() {
                Statement::CreateView { query, .. } => query,
                _ => panic!("Sql for existing view should parse as a view"),
            };
            (view.create_sql.clone(), *query)
        }
        Explainee::Query(query) => (query.to_string(), query),
    };
    // Previouly we would bail here for ORDER BY and LIMIT; this has been relaxed to silently
    // report the plan without the ORDER BY and LIMIT decorations (which are done in post).
    let (mut sql_expr, _desc, finishing, _param_types) =
        query::plan_root_query(scx, query, QueryLifetime::OneShot)?;
    let finishing = if is_view {
        // views don't use a separate finishing
        sql_expr.finish(finishing);
        None
    } else if finishing.is_trivial() {
        None
    } else {
        Some(finishing)
    };
    let expr = sql_expr.clone().decorrelate();
    Ok(Plan::ExplainPlan {
        sql,
        raw_plan: sql_expr,
        decorrelated_plan: expr,
        row_set_finishing: finishing,
        stage,
        options,
    })
}

/// Plans and decorrelates a `Query`. Like `query::plan_root_query`, but returns
/// an `::expr::RelationExpr`, which cannot include correlated expressions.
fn handle_query(
    scx: &StatementContext,
    query: Query,
    params: &Params,
    lifetime: QueryLifetime,
) -> Result<(::expr::RelationExpr, RelationDesc, RowSetFinishing), failure::Error> {
    let (mut expr, desc, finishing, _param_types) = query::plan_root_query(scx, query, lifetime)?;
    expr.bind_parameters(&params);
    Ok((expr.decorrelate()?, desc, finishing))
}

#[derive(Debug)]
struct Schema {
    key_schema: Option<String>,
    value_schema: String,
    schema_registry_url: Option<Url>,
}

async fn get_remote_avro_schema(url: Url, topic: String) -> Result<Schema, failure::Error> {
    let ccsr_client = ccsr::AsyncClient::new(url.clone());

    let value_schema_name = format!("{}-value", topic);
    let value_schema = ccsr_client
        .get_schema_by_subject(&value_schema_name)
        .await
        .with_context(|err| {
            format!(
                "fetching latest schema for subject '{}' from registry: {}",
                value_schema_name, err
            )
        })?;
    let subject = format!("{}-key", topic);
    let key_schema = ccsr_client.get_schema_by_subject(&subject).await.ok();
    Ok(Schema {
        key_schema: key_schema.map(|s| s.raw),
        value_schema: value_schema.raw,
        schema_registry_url: Some(url),
    })
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
        ObjectType::Schema => "SCHEMAS",
        ObjectType::Index => "INDEXES",
        ObjectType::Table => "TABLES",
        ObjectType::View => "VIEWS",
        ObjectType::Source => "SOURCES",
        ObjectType::Sink => "SINKS",
    }
}

/// Immutable state that applies to the planning of an entire `Statement`.
#[derive(Debug)]
pub struct StatementContext<'a> {
    pub catalog: &'a Catalog,
    pub session: &'a dyn PlanSession,
}

impl<'a> StatementContext<'a> {
    pub fn allocate_name(&self, name: PartialName) -> FullName {
        FullName {
            database: match name.database {
                Some(name) => DatabaseSpecifier::Name(name),
                None => self.session.database(),
            },
            schema: name.schema.unwrap_or_else(|| "public".into()),
            item: name.item,
        }
    }

    pub fn resolve_name(&self, name: ObjectName) -> Result<FullName, failure::Error> {
        let name = normalize::object_name(name)?;
        Ok(self
            .catalog
            .resolve(self.session.database(), self.session.search_path(), &name)?)
    }
}
