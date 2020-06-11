// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use aws_arn::{Resource, ARN};
use failure::{bail, format_err};
use itertools::Itertools;
use lazy_static::lazy_static;
use regex::Regex;
use rusoto_core::Region;
use url::Url;

use dataflow_types::{
    AvroEncoding, AvroOcfSinkConnectorBuilder, Consistency, CsvEncoding, DataEncoding, Envelope,
    ExternalSourceConnector, FileSourceConnector, KafkaSinkConnectorBuilder, KafkaSourceConnector,
    KinesisSourceConnector, PeekWhen, ProtobufEncoding, SinkConnectorBuilder, SourceConnector,
};
use expr::{like_pattern, GlobalId, RowSetFinishing};
use interchange::avro::{DebeziumDeduplicationStrategy, Encoder};
use ore::collections::CollectionExt;
use repr::strconv;
use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowArena, ScalarType};
use sql_parser::ast::{
    AvroSchema, Connector, ExplainOptions, ExplainStage, Explainee, Format, Ident,
    IfExistsBehavior, ObjectName, ObjectType, Query, SetVariableValue, ShowStatementFilter,
    SqlOption, Statement, Value,
};

use crate::catalog::{Catalog, CatalogItemType};
use crate::kafka_util;
use crate::names::{DatabaseSpecifier, FullName, PartialName};
use crate::plan::query::QueryLifetime;
use crate::plan::{query, Index, Params, Plan, PlanContext, Sink, Source, View};
use crate::pure::Schema;
use crate::{normalize, unsupported};

lazy_static! {
    static ref SHOW_DATABASES_DESC: RelationDesc =
        { RelationDesc::empty().with_nonnull_column("Database", ScalarType::String) };
    static ref SHOW_INDEXES_DESC: RelationDesc = RelationDesc::new(
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
        .into_iter()
        .map(Some),
    );
    static ref SHOW_COLUMNS_DESC: RelationDesc = RelationDesc::empty()
        .with_nonnull_column("Field", ScalarType::String)
        .with_nonnull_column("Nullable", ScalarType::String)
        .with_nonnull_column("Type", ScalarType::String);
}

pub fn make_show_objects_desc(
    object_type: ObjectType,
    materialized: bool,
    full: bool,
) -> RelationDesc {
    let col_name = object_type_as_plural_str(object_type);
    if full {
        let mut relation_desc = RelationDesc::empty()
            .with_nonnull_column(col_name, ScalarType::String)
            .with_nonnull_column("TYPE", ScalarType::String);
        if ObjectType::View == object_type {
            relation_desc = relation_desc.with_nonnull_column("QUERYABLE", ScalarType::Bool);
        }
        if !materialized && (ObjectType::View == object_type || ObjectType::Source == object_type) {
            relation_desc = relation_desc.with_nonnull_column("MATERIALIZED", ScalarType::Bool);
        }
        relation_desc
    } else {
        RelationDesc::empty().with_nonnull_column(col_name, ScalarType::String)
    }
}

pub fn describe_statement(
    catalog: &dyn Catalog,
    stmt: Statement,
) -> Result<(Option<RelationDesc>, Vec<ScalarType>), failure::Error> {
    let pcx = &PlanContext::default();
    let scx = &StatementContext { catalog, pcx };
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

        Statement::Explain {
            stage, explainee, ..
        } => (
            Some(RelationDesc::empty().with_nonnull_column(
                match stage {
                    ExplainStage::Sql => "Sql",
                    ExplainStage::RawPlan => "Raw Plan",
                    ExplainStage::DecorrelatedPlan => "Decorrelated Plan",
                    ExplainStage::OptimizedPlan { .. } => "Optimized Plan",
                },
                ScalarType::String,
            )),
            match explainee {
                Explainee::Query(q) => {
                    describe_statement(catalog, Statement::Query(Box::new(q)))?.1
                }
                _ => vec![],
            },
        ),

        Statement::ShowCreateView { .. } => (
            Some(
                RelationDesc::empty()
                    .with_nonnull_column("View", ScalarType::String)
                    .with_nonnull_column("Create View", ScalarType::String),
            ),
            vec![],
        ),

        Statement::ShowCreateSource { .. } => (
            Some(
                RelationDesc::empty()
                    .with_nonnull_column("Source", ScalarType::String)
                    .with_nonnull_column("Create Source", ScalarType::String),
            ),
            vec![],
        ),

        Statement::ShowCreateSink { .. } => (
            Some(
                RelationDesc::empty()
                    .with_nonnull_column("Sink", ScalarType::String)
                    .with_nonnull_column("Create Sink", ScalarType::String),
            ),
            vec![],
        ),

        Statement::ShowColumns { .. } => (Some(SHOW_COLUMNS_DESC.clone()), vec![]),

        Statement::ShowIndexes { .. } => (Some(SHOW_INDEXES_DESC.clone()), vec![]),

        Statement::ShowDatabases { .. } => (Some(SHOW_DATABASES_DESC.clone()), vec![]),

        Statement::ShowObjects {
            object_type,
            full,
            materialized,
            ..
        } => (
            Some(make_show_objects_desc(object_type, materialized, full)),
            vec![],
        ),
        Statement::ShowVariable { variable, .. } => {
            if variable.as_str() == unicase::Ascii::new("ALL") {
                (
                    Some(
                        RelationDesc::empty()
                            .with_nonnull_column("name", ScalarType::String)
                            .with_nonnull_column("setting", ScalarType::String)
                            .with_nonnull_column("description", ScalarType::String),
                    ),
                    vec![],
                )
            } else {
                (
                    Some(
                        RelationDesc::empty()
                            .with_nonnull_column(variable.as_str(), ScalarType::String),
                    ),
                    vec![],
                )
            }
        }

        Statement::Tail { name, .. } => {
            let name = scx.resolve_item(name)?;
            let sql_object = scx.catalog.get_item(&name);
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
        Statement::CreateTable { .. } => bail!(
            "CREATE TABLE statements are not supported. \
             Try CREATE SOURCE or CREATE [MATERIALIZED] VIEW instead."
        ),
        _ => unsupported!(format!("{:?}", stmt)),
    })
}

pub fn handle_statement(
    pcx: &PlanContext,
    catalog: &dyn Catalog,
    stmt: Statement,
    params: &Params,
) -> Result<Plan, failure::Error> {
    let scx = &StatementContext { pcx, catalog };
    match stmt {
        Statement::Tail {
            name,
            with_snapshot,
            as_of,
        } => handle_tail(scx, name, with_snapshot, as_of),
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
        } => handle_show_objects(scx, extended, full, materialized, ot, from, filter.as_ref()),
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
        unsupported!("SET LOCAL");
    }
    Ok(Plan::SetVariable {
        name: variable.to_string(),
        value: match value {
            SetVariableValue::Literal(Value::SingleQuotedString(s)) => s,
            SetVariableValue::Literal(lit) => lit.to_string(),
            SetVariableValue::Ident(ident) => ident.value(),
        },
    })
}

fn handle_show_variable(_: &StatementContext, variable: Ident) -> Result<Plan, failure::Error> {
    if variable.as_str() == unicase::Ascii::new("ALL") {
        Ok(Plan::ShowAllVariables)
    } else {
        Ok(Plan::ShowVariable(variable.to_string()))
    }
}

fn handle_tail(
    scx: &StatementContext,
    from: ObjectName,
    with_snapshot: bool,
    as_of: Option<sql_parser::ast::Expr>,
) -> Result<Plan, failure::Error> {
    let from = scx.resolve_item(from)?;
    let entry = scx.catalog.get_item(&from);
    let ts = as_of.map(|e| query::eval_as_of(scx, e)).transpose()?;

    match entry.item_type() {
        CatalogItemType::Source | CatalogItemType::View => Ok(Plan::Tail {
            id: entry.id(),
            ts,
            with_snapshot,
        }),
        CatalogItemType::Index | CatalogItemType::Sink => bail!(
            "'{}' cannot be tailed because it is a {}",
            from,
            entry.item_type(),
        ),
    }
}

fn finish_show_where(
    scx: &StatementContext,
    filter: Option<&ShowStatementFilter>,
    rows: Vec<Vec<Datum>>,
    desc: &RelationDesc,
) -> Result<Plan, failure::Error> {
    let (r, finishing) = query::plan_show_where(scx, filter, rows, desc)?;

    Ok(Plan::Peek {
        source: r.decorrelate(),
        when: PeekWhen::Immediately,
        finishing,
        materialize: true,
    })
}

fn handle_show_databases(
    scx: &StatementContext,
    filter: Option<&ShowStatementFilter>,
) -> Result<Plan, failure::Error> {
    let rows = scx
        .catalog
        .list_databases()
        .map(|database| vec![Datum::from(database)])
        .collect();

    finish_show_where(scx, filter, rows, &SHOW_DATABASES_DESC)
}

fn handle_show_objects(
    scx: &StatementContext,
    extended: bool,
    full: bool,
    materialized: bool,
    object_type: ObjectType,
    from: Option<ObjectName>,
    filter: Option<&ShowStatementFilter>,
) -> Result<Plan, failure::Error> {
    let classify_id = |id| match id {
        GlobalId::System(_) => "SYSTEM",
        GlobalId::User(_) => "USER",
    };
    let arena = RowArena::new();
    let make_row = |name: &str, class: &str| {
        if full {
            vec![
                Datum::from(arena.push_string(name.to_string())),
                Datum::from(arena.push_string(class.to_string())),
            ]
        } else {
            vec![Datum::from(arena.push_string(name.to_string()))]
        }
    };

    if let ObjectType::Schema = object_type {
        let schemas = if let Some(from) = from {
            let database_name = scx.resolve_database(from)?;
            scx.catalog
                .list_schemas(&DatabaseSpecifier::Name(database_name))
        } else {
            scx.catalog.list_schemas(&scx.resolve_default_database()?)
        };

        let mut rows = vec![];
        for name in schemas {
            rows.push(make_row(name, "USER"));
        }
        if extended {
            let ambient_schemas = scx.catalog.list_schemas(&DatabaseSpecifier::Ambient);
            for name in ambient_schemas {
                rows.push(make_row(name, "SYSTEM"));
            }
        }
        // TODO(justin): it's unfortunate that we call make_show_objects_desc twice, I think we
        // should be able to restructure this so that it only gets called once.
        finish_show_where(
            scx,
            filter,
            rows,
            &make_show_objects_desc(object_type, materialized, full),
        )
    } else {
        let items = if let Some(from) = from {
            let (database_spec, schema_name) = scx.resolve_schema(from)?;
            scx.catalog.list_items(&database_spec, &schema_name)
        } else {
            scx.catalog
                .list_items(&scx.resolve_default_database()?, "public")
        };

        let filtered_items =
            items.filter(|entry| object_type_matches(object_type, entry.item_type()));

        if object_type == ObjectType::View || object_type == ObjectType::Source {
            // TODO(justin): we can't handle SHOW ... WHERE here yet because the coordinator adds
            // extra columns to this result that we don't have access to here yet. This could be
            // fixed by passing down this extra catalog info somehow.
            let like_regex = match filter {
                Some(ShowStatementFilter::Like(pattern)) => like_pattern::build_regex(&pattern)?,
                Some(ShowStatementFilter::Where(_)) => unsupported!("SHOW ... WHERE"),
                None => like_pattern::build_regex("%")?,
            };

            let filtered_items =
                filtered_items.filter(|entry| like_regex.is_match(&entry.name().item));
            Ok(Plan::ShowViews {
                ids: filtered_items
                    .map(|entry| (entry.name().item.clone(), entry.id()))
                    .collect::<Vec<_>>(),
                full,
                show_queryable: object_type == ObjectType::View,
                limit_materialized: materialized,
            })
        } else {
            let rows = filtered_items
                .map(|entry| make_row(&entry.name().item, classify_id(entry.id())))
                .collect::<Vec<_>>();

            finish_show_where(
                scx,
                filter,
                rows,
                &make_show_objects_desc(object_type, materialized, full),
            )
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
        unsupported!("SHOW EXTENDED INDEXES")
    }
    let from_name = scx.resolve_item(from_name)?;
    let from_entry = scx.catalog.get_item(&from_name);
    if !object_type_matches(ObjectType::View, from_entry.item_type())
        && !object_type_matches(ObjectType::Source, from_entry.item_type())
    {
        bail!(
            "cannot show indexes on {} because it is a {}",
            from_name,
            from_entry.item_type(),
        );
    }
    let arena = RowArena::new();
    let rows = from_entry
        .used_by()
        .iter()
        .map(|id| scx.catalog.get_item_by_id(id))
        .filter(|entry| {
            object_type_matches(ObjectType::Index, entry.item_type())
                && entry.uses() == vec![from_entry.id()]
        })
        .flat_map(|entry| {
            let (keys, on) = entry.index_details().expect("known to be an index");
            let key_sqls = match crate::parse::parse(entry.create_sql().to_owned())
                .expect("create_sql cannot be invalid")
                .into_element()
            {
                Statement::CreateIndex { key_parts, .. } => key_parts,
                _ => unreachable!(),
            };
            let mut row_subset = Vec::new();
            for (i, (key_expr, key_sql)) in keys.iter().zip_eq(key_sqls).enumerate() {
                let desc = scx.catalog.get_item_by_id(&on).desc().unwrap();
                let key_sql = key_sql.to_string();
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
                row_subset.push(vec![
                    Datum::String(arena.push_string(from_entry.name().to_string())),
                    Datum::String(arena.push_string(entry.name().to_string())),
                    col_name,
                    func,
                    Datum::from(key_expr.typ(desc.typ()).nullable),
                    Datum::from((i + 1) as i64),
                ]);
            }
            row_subset
        })
        .collect();

    finish_show_where(scx, filter, rows, &SHOW_INDEXES_DESC)
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
        unsupported!("SHOW EXTENDED COLUMNS");
    }
    if full {
        unsupported!("SHOW FULL COLUMNS");
    }

    let arena = RowArena::new();
    let table_name = scx.resolve_item(table_name)?;
    let rows: Vec<_> = scx
        .catalog
        .get_item(&table_name)
        .desc()?
        .iter()
        .map(|(name, typ)| {
            let name = name.map(|n| n.to_string());
            vec![
                Datum::String(name.map(|n| arena.push_string(n)).unwrap_or("?")),
                Datum::String(if typ.nullable { "YES" } else { "NO" }),
                Datum::String(pgrepr::Type::from(&typ.scalar_type).name()),
            ]
        })
        .collect();

    finish_show_where(scx, filter, rows, &SHOW_COLUMNS_DESC)
}

fn handle_show_create_view(
    scx: &StatementContext,
    name: ObjectName,
) -> Result<Plan, failure::Error> {
    let name = scx.resolve_item(name)?;
    let entry = scx.catalog.get_item(&name);
    if let CatalogItemType::View = entry.item_type() {
        Ok(Plan::SendRows(vec![Row::pack(&[
            Datum::String(&name.to_string()),
            Datum::String(entry.create_sql()),
        ])]))
    } else {
        bail!("{} is not a view", name);
    }
}

fn handle_show_create_source(
    scx: &StatementContext,
    name: ObjectName,
) -> Result<Plan, failure::Error> {
    let name = scx.resolve_item(name)?;
    let entry = scx.catalog.get_item(&name);
    if let CatalogItemType::Source = entry.item_type() {
        Ok(Plan::SendRows(vec![Row::pack(&[
            Datum::String(&name.to_string()),
            Datum::String(entry.create_sql()),
        ])]))
    } else {
        bail!("{} is not a source", name);
    }
}

fn handle_show_create_sink(
    scx: &StatementContext,
    name: ObjectName,
) -> Result<Plan, failure::Error> {
    let name = scx.resolve_item(name)?;
    let entry = scx.catalog.get_item(&name);
    if let CatalogItemType::Sink = entry.item_type() {
        Ok(Plan::SendRows(vec![Row::pack(&[
            Datum::String(&name.to_string()),
            Datum::String(entry.create_sql()),
        ])]))
    } else {
        bail!("'{}' is not a sink", name);
    }
}

fn kafka_sink_builder(
    format: Option<Format>,
    with_options: Vec<SqlOption>,
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
        _ => unsupported!("non-confluent schema registry avro sinks"),
    };

    if !broker.contains(':') {
        broker += ":9092";
    }
    let broker_url = broker.parse()?;

    let encoder = Encoder::new(desc);
    let value_schema = encoder.writer_schema().canonical_form();

    let mut with_options = normalize::with_options(&with_options);

    // Use the user supplied value for replication factor, or default to 1
    let replication_factor = match with_options.remove("replication_factor") {
        None => 1,
        Some(Value::Number(n)) => n.parse::<u32>()?,
        Some(_) => bail!("replication factor for sink topics has to be a positive integer"),
    };

    if replication_factor == 0 {
        bail!("replication factor for sink topics has to be greater than zero");
    }

    Ok(SinkConnectorBuilder::Kafka(KafkaSinkConnectorBuilder {
        broker_url,
        schema_registry_url,
        value_schema,
        topic_prefix,
        topic_suffix,
        replication_factor,
        fuel: 10000,
    }))
}

fn avro_ocf_sink_builder(
    format: Option<Format>,
    with_options: Vec<SqlOption>,
    path: String,
    file_name_suffix: String,
) -> Result<SinkConnectorBuilder, failure::Error> {
    if format.is_some() {
        bail!("avro ocf sinks cannot specify a format");
    }

    if !with_options.is_empty() {
        bail!("avro ocf sinks do not support WITH options");
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
    let (name, from, connector, with_options, format, if_not_exists) = match stmt {
        Statement::CreateSink {
            name,
            from,
            connector,
            with_options,
            format,
            if_not_exists,
        } => (name, from, connector, with_options, format, if_not_exists),
        _ => unreachable!(),
    };

    let name = scx.allocate_name(normalize::object_name(name)?);
    let from = scx.catalog.get_item(&scx.resolve_item(from)?);
    let suffix = format!(
        "{}-{}",
        scx.catalog
            .startup_time()
            .duration_since(UNIX_EPOCH)?
            .as_secs(),
        scx.catalog.nonce()
    );

    let connector_builder = match connector {
        Connector::File { .. } => unsupported!("file sinks"),
        Connector::Kafka { broker, topic } => kafka_sink_builder(
            format,
            with_options,
            broker,
            topic,
            from.desc()?.clone(),
            suffix,
        )?,
        Connector::Kinesis { .. } => unsupported!("Kinesis sinks"),
        Connector::AvroOcf { path } => avro_ocf_sink_builder(format, with_options, path, suffix)?,
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
    let on_name = scx.resolve_item(on_name)?;
    let catalog_entry = scx.catalog.get_item(&on_name);
    let keys = query::plan_index_exprs(scx, catalog_entry.desc()?, &key_parts)?;
    if !object_type_matches(ObjectType::View, catalog_entry.item_type())
        && !object_type_matches(ObjectType::Source, catalog_entry.item_type())
    {
        bail!(
            "index cannot be created on {} because it is a {}",
            on_name,
            catalog_entry.item_type()
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

fn handle_create_view(
    scx: &StatementContext,
    mut stmt: Statement,
    params: &Params,
) -> Result<Plan, failure::Error> {
    let create_sql = normalize::create_statement(scx, stmt.clone())?;
    let (name, columns, query, temporary, materialized, if_exists, with_options) = match &mut stmt {
        Statement::CreateView {
            name,
            columns,
            query,
            temporary,
            materialized,
            if_exists,
            with_options,
        } => (
            name,
            columns,
            query,
            temporary,
            materialized,
            if_exists,
            with_options,
        ),
        _ => unreachable!(),
    };
    if !with_options.is_empty() {
        unsupported!("WITH options");
    }
    let name = if *temporary {
        scx.allocate_temporary_name(normalize::object_name(name.to_owned())?)
    } else {
        scx.allocate_name(normalize::object_name(name.to_owned())?)
    };
    let replace = if *if_exists == IfExistsBehavior::Replace
        && scx.catalog.resolve_item(&name.clone().into()).is_ok()
    {
        let cascade = false;
        handle_drop_item(scx, ObjectType::View, &name, cascade)?
    } else {
        None
    };
    let (mut relation_expr, mut desc, finishing, _) =
        query::plan_root_query(scx, *query.clone(), QueryLifetime::Static)?;
    // TODO(jamii) can views even have parameters?
    relation_expr.bind_parameters(&params);
    //TODO: materialize#724 - persist finishing information with the view?
    relation_expr.finish(finishing);
    let relation_expr = relation_expr.decorrelate();
    desc = maybe_rename_columns(format!("view {}", name), desc, columns)?;
    let temporary = *temporary;
    let materialize = *materialized; // Normalize for `raw_sql` below.
    let if_not_exists = *if_exists == IfExistsBehavior::Skip;
    Ok(Plan::CreateView {
        name,
        view: View {
            create_sql,
            expr: relation_expr,
            desc,
            temporary,
        },
        replace,
        materialize,
        if_not_exists,
    })
}

fn extract_batch_size_option(
    with_options: &mut HashMap<String, Value>,
) -> Result<i64, failure::Error> {
    match with_options.remove("max_timestamp_batch_size") {
        None => Ok(0),
        Some(Value::Number(n)) => match n.parse::<i64>() {
            Ok(n) => {
                if n < 0 {
                    bail!("max_ts_batch must be greater than zero")
                } else {
                    Ok(n)
                }
            }
            _ => bail!("max_timestamp_batch_size must be an i64"),
        },
        Some(_) => bail!("max_timestamp_batch_size must be an i64"),
    }
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
                            schema_registry_config,
                        } = match schema {
                            // TODO(jldlaughlin): we need a way to pass in primary key information
                            // when building a source from a string or file.
                            AvroSchema::Schema(sql_parser::ast::Schema::Inline(schema)) => Schema {
                                key_schema: None,
                                value_schema: schema.clone(),
                                schema_registry_config: None,
                            },
                            AvroSchema::Schema(sql_parser::ast::Schema::File(_)) => {
                                unreachable!("File schema should already have been inlined")
                            }
                            AvroSchema::CsrUrl { url, seed } => {
                                let url: Url = url.parse()?;
                                let mut with_options_map = normalize::with_options(with_options);
                                let config_options =
                                    kafka_util::extract_config(&mut with_options_map)?;
                                let ccsr_config =
                                    kafka_util::generate_ccsr_client_config(url, &config_options)?;

                                if let Some(seed) = seed {
                                    Schema {
                                        key_schema: seed.key_schema.clone(),
                                        value_schema: seed.value_schema.clone(),
                                        schema_registry_config: Some(ccsr_config),
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
                            schema_registry_config,
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
                    Format::Json => unsupported!("JSON sources"),
                    Format::Text => DataEncoding::Text,
                })
            };

            let mut with_options = normalize::with_options(with_options);

            let mut consistency = Consistency::RealTime;
            let mut max_ts_batch = 0;
            let (external_connector, mut encoding) = match connector {
                Connector::Kafka { broker, topic, .. } => {
                    let config_options = kafka_util::extract_config(&mut with_options)?;

                    consistency = match with_options.remove("consistency") {
                        None => Consistency::RealTime,
                        Some(Value::SingleQuotedString(topic)) => Consistency::BringYourOwn(topic),
                        Some(_) => bail!("consistency must be a string"),
                    };

                    let group_id_prefix = match with_options.remove("group_id_prefix") {
                        None => None,
                        Some(Value::SingleQuotedString(s)) => Some(s),
                        Some(_) => bail!("group_id_prefix must be a string"),
                    };

                    max_ts_batch = extract_batch_size_option(&mut with_options)?;

                    // THIS IS EXPERIMENTAL - DO NOT DOCUMENT IT
                    // until we have had time to think about what the right UX/design is on a non-urgent timeline!
                    // In particular, we almost certainly want the offsets to be specified per-partition.
                    // The other major caveat is that by using this feature, you are opting in to
                    // not using updates or deletes in CDC sources, and accepting panics if that constraint is violated.
                    let start_offset_err = "start_offset must be a nonnegative integer";
                    let start_offset = match with_options.remove("start_offset") {
                        None => 0,
                        Some(Value::Number(n)) => match n.parse::<i64>() {
                            Ok(n) if n >= 0 => n,
                            _ => bail!(start_offset_err),
                        },
                        Some(_) => bail!(start_offset_err),
                    };

                    if start_offset != 0 && consistency != Consistency::RealTime {
                        bail!("`start_offset` is not yet implemented for BYO consistency sources.")
                    }

                    let connector = ExternalSourceConnector::Kafka(KafkaSourceConnector {
                        url: broker.parse()?,
                        topic: topic.clone(),
                        config_options,
                        start_offset,
                        group_id_prefix,
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
                        _ => unsupported!(format!("AWS Resource type: {:#?}", arn.resource)),
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
                    let access_key_id = match with_options.remove("access_key_id") {
                        Some(Value::SingleQuotedString(access_key_id)) => Some(access_key_id),
                        Some(_) => bail!("access_key_id must be a string"),
                        _ => None,
                    };
                    let secret_access_key = match with_options.remove("secret_access_key") {
                        Some(Value::SingleQuotedString(secret_access_key)) => {
                            Some(secret_access_key)
                        }
                        Some(_) => bail!("secret_access_key must be a string"),
                        _ => None,
                    };
                    let token = match with_options.remove("token") {
                        Some(Value::SingleQuotedString(token)) => Some(token),
                        Some(_) => bail!("token must be a string"),
                        _ => None,
                    };

                    let connector = ExternalSourceConnector::Kinesis(KinesisSourceConnector {
                        stream_name,
                        region,
                        access_key_id,
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
                    consistency = match with_options.remove("consistency") {
                        None => Consistency::RealTime,
                        Some(Value::SingleQuotedString(topic)) => Consistency::BringYourOwn(topic),
                        Some(_) => bail!("consistency must be a string"),
                    };
                    max_ts_batch = extract_batch_size_option(&mut with_options)?;

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
                    consistency = match with_options.remove("consistency") {
                        None => Consistency::RealTime,
                        Some(Value::SingleQuotedString(topic)) => Consistency::BringYourOwn(topic),
                        Some(_) => bail!("consistency must be a string"),
                    };

                    max_ts_batch = extract_batch_size_option(&mut with_options)?;

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

            // TODO: remove bails as more support for upsert is added.
            let envelope = match &envelope {
                sql_parser::ast::Envelope::None => dataflow_types::Envelope::None,
                sql_parser::ast::Envelope::Debezium => {
                    let dedup_strat = match with_options.remove("deduplication") {
                        None => DebeziumDeduplicationStrategy::Ordered,
                        Some(Value::SingleQuotedString(s)) => match s.as_str() {
                            "full" => DebeziumDeduplicationStrategy::Full,
                            "ordered" => DebeziumDeduplicationStrategy::Ordered,
                            _ => bail!("deduplication must be either 'full' or 'ordered'."),
                        },
                        _ => bail!("deduplication must be either 'full' or 'ordered'."),
                    };
                    dataflow_types::Envelope::Debezium(dedup_strat)
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
                        dataflow_types::Envelope::Upsert(key_encoding)
                    }
                    _ => unsupported!("upsert envelope for non-Kafka sources"),
                },
            };

            if let dataflow_types::Envelope::Upsert(key_encoding) = &envelope {
                match &mut encoding {
                    DataEncoding::Avro(AvroEncoding { key_schema, .. }) => {
                        *key_schema = None;
                    }
                    DataEncoding::Bytes | DataEncoding::Text => {
                        if let DataEncoding::Avro(_) = &key_encoding {
                            unsupported!("Avro key for this format");
                        }
                    }
                    _ => unsupported!("upsert envelope for this format"),
                }
            }

            let mut desc = encoding.desc(&envelope)?;
            let ignore_source_keys = match with_options.remove("ignore_source_keys") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("ignore_source_keys must be a boolean"),
            };
            if ignore_source_keys {
                desc = desc.without_keys();
            }

            desc = maybe_rename_columns(format!("source {}", name), desc, col_names)?;

            // TODO(benesch): the available metadata columns should not depend
            // on the format.
            //
            // TODO(brennan): They should not depend on the envelope either. Figure out a way to
            // make all of this more tasteful.
            match (&encoding, &envelope) {
                (DataEncoding::Avro { .. }, _)
                | (DataEncoding::Protobuf { .. }, _)
                | (_, Envelope::Debezium(_)) => (),
                _ => {
                    for (name, ty) in external_connector.metadata_columns() {
                        desc = desc.with_column(name, ty);
                    }
                }
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
                    max_ts_batch,
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
        other => unsupported!(format!("{:?}", other)),
    }
}

/// Renames the columns in `desc` with the names in `column_names` if
/// `column_names` is non-empty.
///
/// Returns an error if the length of `column_names` is not either zero or the
/// arity of `desc`.
fn maybe_rename_columns(
    context: impl fmt::Display,
    desc: RelationDesc,
    column_names: &[Ident],
) -> Result<RelationDesc, failure::Error> {
    if column_names.is_empty() {
        return Ok(desc);
    }

    if column_names.len() != desc.typ().column_types.len() {
        bail!(
            "{0} definition names {1} columns, but {0} has {2} columns",
            context,
            column_names.len(),
            desc.typ().column_types.len()
        )
    }

    let new_names = column_names
        .iter()
        .map(|n| Some(normalize::column_name(n.clone())));

    Ok(desc.with_names(new_names))
}

fn handle_drop_database(
    scx: &StatementContext,
    name: Ident,
    if_exists: bool,
) -> Result<Plan, failure::Error> {
    let name = match scx.resolve_database_ident(name) {
        Ok(name) => name,
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the database
            // does not exist.
            //
            // TODO(benesch): adjust the type here so we can more clearly
            // indicate that we don't want to drop any database at all.
            String::new()
        }
        Err(err) => return Err(err),
    };
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
        _ => unsupported!(format!("DROP {}", object_type)),
    }
}

fn handle_drop_schema(
    scx: &StatementContext,
    if_exists: bool,
    names: Vec<ObjectName>,
    cascade: bool,
) -> Result<Plan, failure::Error> {
    if names.len() != 1 {
        unsupported!("DROP SCHEMA with multiple schemas");
    }
    match scx.resolve_schema(names.into_element()) {
        Ok((database_spec, schema_name)) => {
            if let DatabaseSpecifier::Ambient = database_spec {
                bail!(
                    "cannot drop schema {} because it is required by the database system",
                    schema_name
                );
            }
            let mut items = scx.catalog.list_items(&database_spec, &schema_name);
            if !cascade && items.next().is_some() {
                bail!(
                    "schema '{}.{}' cannot be dropped without CASCADE while it contains objects",
                    database_spec,
                    schema_name
                );
            }
            Ok(Plan::DropSchema {
                database_name: database_spec,
                schema_name,
            })
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating that the
            // database does not exist.
            // TODO(benesch): adjust the types here properly, rather than making
            // up a nonexistent database.
            Ok(Plan::DropSchema {
                database_name: DatabaseSpecifier::Ambient,
                schema_name: "noexist".into(),
            })
        }
        Err(e) => Err(e),
    }
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
        .map(|n| scx.resolve_item(n))
        .collect::<Vec<_>>();
    let mut ids = vec![];
    for name in names {
        match name {
            Ok(name) => ids.extend(handle_drop_item(scx, object_type, &name, cascade)?),
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
    name: &FullName,
    cascade: bool,
) -> Result<Option<GlobalId>, failure::Error> {
    let catalog_entry = scx.catalog.get_item(name);
    if catalog_entry.id().is_system() {
        bail!(
            "cannot drop item {} because it is required by the database system",
            name
        );
    }
    if !object_type_matches(object_type, catalog_entry.item_type()) {
        bail!("{} is not of type {}", name, object_type);
    }
    if !cascade {
        for id in catalog_entry.used_by() {
            let dep = scx.catalog.get_item_by_id(id);
            match dep.item_type() {
                CatalogItemType::Source | CatalogItemType::View | CatalogItemType::Sink => {
                    bail!(
                        "cannot drop {}: still depended upon by catalog item '{}'",
                        catalog_entry.name(),
                        dep.name()
                    );
                }
                CatalogItemType::Index => (),
            }
        }
    }
    Ok(Some(catalog_entry.id()))
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
    params: &Params,
) -> Result<Plan, failure::Error> {
    let is_view = if let Explainee::View(_) = explainee {
        true
    } else {
        false
    };
    let (scx, sql, query) = match explainee {
        Explainee::View(name) => {
            let full_name = scx.resolve_item(name.clone())?;
            let entry = scx.catalog.get_item(&full_name);
            if entry.item_type() != CatalogItemType::View {
                bail!(
                    "Expected {} to be a view, not a {}",
                    name,
                    entry.item_type(),
                );
            }
            let parsed = crate::parse::parse(entry.create_sql().to_owned())
                .expect("Sql for existing view should be valid sql");
            let query = match parsed.into_last() {
                Statement::CreateView { query, .. } => query,
                _ => panic!("Sql for existing view should parse as a view"),
            };
            let scx = StatementContext {
                pcx: entry.plan_cx(),
                catalog: scx.catalog,
            };
            (scx, entry.create_sql().to_owned(), *query)
        }
        Explainee::Query(query) => (scx.clone(), query.to_string(), query),
    };
    // Previouly we would bail here for ORDER BY and LIMIT; this has been relaxed to silently
    // report the plan without the ORDER BY and LIMIT decorations (which are done in post).
    let (mut sql_expr, _desc, finishing, _param_types) =
        query::plan_root_query(&scx, query, QueryLifetime::OneShot)?;
    let finishing = if is_view {
        // views don't use a separate finishing
        sql_expr.finish(finishing);
        None
    } else if finishing.is_trivial() {
        None
    } else {
        Some(finishing)
    };
    sql_expr.bind_parameters(&params);
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
    Ok((expr.decorrelate(), desc, finishing))
}

/// Whether a SQL object type can be interpreted as matching the type of the given catalog item.
/// For example, if `v` is a view, `DROP SOURCE v` should not work, since Source and View
/// are non-matching types.
///
/// For now tables are treated as a special kind of source in Materialize, so just
/// allow `TABLE` to refer to either.
fn object_type_matches(object_type: ObjectType, item_type: CatalogItemType) -> bool {
    match item_type {
        CatalogItemType::Source => {
            object_type == ObjectType::Source || object_type == ObjectType::Table
        }
        CatalogItemType::Sink => object_type == ObjectType::Sink,
        CatalogItemType::View => object_type == ObjectType::View,
        CatalogItemType::Index => object_type == ObjectType::Index,
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
#[derive(Debug, Clone)]
pub struct StatementContext<'a> {
    pub pcx: &'a PlanContext,
    pub catalog: &'a dyn Catalog,
}

impl<'a> StatementContext<'a> {
    pub fn allocate_name(&self, name: PartialName) -> FullName {
        FullName {
            database: match name.database {
                Some(name) => DatabaseSpecifier::Name(name),
                None => DatabaseSpecifier::Name(self.catalog.default_database().into()),
            },
            schema: name.schema.unwrap_or_else(|| "public".into()),
            item: name.item,
        }
    }

    pub fn allocate_temporary_name(&self, name: PartialName) -> FullName {
        FullName {
            database: DatabaseSpecifier::Ambient,
            schema: name.schema.unwrap_or_else(|| "mz_temp".to_owned()),
            item: name.item,
        }
    }

    pub fn resolve_default_database(&self) -> Result<DatabaseSpecifier, failure::Error> {
        let name = self.catalog.default_database();
        self.catalog.resolve_database(name)?;
        Ok(DatabaseSpecifier::Name(name.into()))
    }

    pub fn resolve_database(&self, name: ObjectName) -> Result<String, failure::Error> {
        if name.0.len() != 1 {
            bail!(
                "database name '{}' does not have exactly one component",
                name
            );
        }
        self.resolve_database_ident(name.0.into_element())
    }

    pub fn resolve_database_ident(&self, name: Ident) -> Result<String, failure::Error> {
        let name = normalize::ident(name);
        self.catalog.resolve_database(&name)?;
        Ok(name)
    }

    pub fn resolve_schema(
        &self,
        mut name: ObjectName,
    ) -> Result<(DatabaseSpecifier, String), failure::Error> {
        if name.0.len() > 2 {
            bail!(
                "schema name '{}' cannot have more than two components",
                name
            );
        }
        let schema_name = normalize::ident(name.0.pop().unwrap());
        let database_spec = name.0.pop().map(normalize::ident);
        let database_spec = self.catalog.resolve_schema(database_spec, &schema_name)?;
        Ok((database_spec, schema_name))
    }

    pub fn resolve_item(&self, name: ObjectName) -> Result<FullName, failure::Error> {
        let name = normalize::object_name(name)?;
        Ok(self.catalog.resolve_item(&name)?)
    }
}
