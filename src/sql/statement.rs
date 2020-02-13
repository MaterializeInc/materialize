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

use itertools::join;
use std::collections::{BTreeMap, HashMap};
use std::iter;
use std::path::PathBuf;

use failure::{bail, format_err, ResultExt};
use itertools::Itertools;
use url::Url;

use catalog::names::{DatabaseSpecifier, FullName, PartialName};
use catalog::{Catalog, CatalogItem, SchemaType};
use dataflow_types::{
    AvroEncoding, Consistency, CsvEncoding, DataEncoding, Envelope, ExternalSourceConnector,
    FileSourceConnector, KafkaSinkConnector, KafkaSourceConnector, KinesisSourceConnector,
    PeekWhen, ProtobufEncoding, RowSetFinishing, SinkConnector, SourceConnector,
};
use expr::GlobalId;
use interchange::{avro, protobuf};
use ore::collections::CollectionExt;
use ore::future::MaybeFuture;
use repr::strconv;
use repr::{ColumnType, Datum, RelationDesc, RelationType, Row, RowArena, ScalarType};
use sql_parser::ast::{
    AvroSchema, Connector, CsrSeed, Format, Ident, ObjectName, ObjectType, Query, SetVariableValue,
    ShowStatementFilter, Stage, Statement, Value,
};

use crate::expr::like::build_like_regex_from_string;
use crate::query::QueryLifetime;
use crate::{normalize, query, Index, Params, Plan, PlanSession, Sink, Source, View};

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
                    "View",
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
                    if ObjectType::View == object_type && !materialized {
                        relation_desc = relation_desc
                            .add_column("QUERYABLE", ScalarType::Bool)
                            .add_column("MATERIALIZED", ScalarType::Bool);
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

fn handle_sync_statement(
    stmt: Statement,
    params: &Params,
    scx: &StatementContext,
) -> Result<Plan, failure::Error> {
    match stmt {
        Statement::CreateSource { .. } => unreachable!(),
        Statement::Tail { name } => handle_tail(scx, name),
        Statement::StartTransaction { .. } => handle_start_transaction(),
        Statement::Commit { .. } => handle_commit_transaction(),
        Statement::Rollback { .. } => handle_rollback_transaction(),
        Statement::CreateDatabase {
            name,
            if_not_exists,
        } => handle_create_database(scx, name, if_not_exists),
        Statement::CreateSchema {
            name,
            if_not_exists,
        } => handle_create_schema(scx, name, if_not_exists),
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
        Statement::Explain { stage, query } => handle_explain(scx, stage, *query, params),

        _ => bail!("unsupported SQL statement: {:?}", stmt),
    }
}

/// Dispatch from arbitrary [`sqlparser::ast::Statement`]s to specific handle commands
pub fn handle_statement(
    catalog: &Catalog,
    session: &dyn PlanSession,
    stmt: Statement,
    params: &Params,
) -> MaybeFuture<'static, Result<Plan, failure::Error>> {
    let scx = &StatementContext { catalog, session };
    match stmt {
        Statement::CreateSource { .. } => {
            let session = session.to_owned();
            MaybeFuture::Future(Box::pin(handle_create_dataflow(stmt, session)))
        }
        _ => handle_sync_statement(stmt, params, &scx).into(),
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
    if let CatalogItem::View(_) = entry.item() {
        Ok(Plan::Tail(entry.clone()))
    } else {
        bail!("'{}' is not a view", from);
    }
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
            Some(ShowStatementFilter::Like(pattern)) => build_like_regex_from_string(&pattern)?,
            Some(ShowStatementFilter::Where(_)) => bail!("SHOW ... WHERE is not supported"),
            None => build_like_regex_from_string("%")?,
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

        if object_type == ObjectType::View {
            Ok(Plan::ShowViews {
                ids: filtered_items
                    .map(|(name, entry)| (name.clone(), entry.id()))
                    .collect::<Vec<_>>(),
                full,
                materialized,
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
    if !object_type_matches(ObjectType::View, from_entry.item()) {
        bail!("{} is not a view", from_name);
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
    let source_url = if let CatalogItem::Source(catalog::Source { connector, .. }) =
        scx.catalog.get(&name)?.item()
    {
        match &connector.connector {
            ExternalSourceConnector::Kafka(KafkaSourceConnector { url, topic, .. }) => {
                format!("kafka://{}/{}", url, topic)
            }
            ExternalSourceConnector::Kinesis(KinesisSourceConnector {
                arn,
                access_key,
                secret_access_key,
                region,
            }) => format!(
                "kinesis -> arn: '{}' access_key: {}, secret_access_key: {}, region: {}",
                arn, access_key, secret_access_key, region
            ),
            ExternalSourceConnector::File(c) => {
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

    let (mut broker, topic) = match connector {
        Connector::File { .. } => bail!("file sinks are not yet supported"),
        Connector::Kafka {
            broker,
            topic,
            with_options,
        } => {
            if !with_options.is_empty() {
                bail!(
                    "Unexpected WITH options: {}",
                    join(with_options.into_iter().map(|o| o.name.value), ",")
                )
            }
            (broker, topic)
        }
        Connector::Kinesis { .. } => bail!("Kinesis sinks are not yet supported"),
    };

    let schema_registry_url = match format {
        Format::Avro(AvroSchema::CsrUrl { url, seed }) => {
            if seed.is_some() {
                bail!("SEED option does not make sense with sinks");
            }
            url
        }
        _ => bail!("only confluent schema registry avro sinks are supported"),
    };

    if !broker.contains(':') {
        broker += ":9092";
    }

    let url = broker.parse()?;

    let name = scx.allocate_name(normalize::object_name(name)?);
    let from = scx.resolve_name(from)?;
    let catalog_entry = scx.catalog.get(&from)?;

    // Validate that we can actually encode this stream as Avro.
    let relation_desc = catalog_entry.desc()?.clone();
    let _ = interchange::avro::encode_schema(&relation_desc)?;

    let sink = Sink {
        create_sql,
        from: catalog_entry.id(),
        connector: SinkConnector::Kafka(KafkaSinkConnector {
            url,
            topic,
            schema_registry_url: schema_registry_url.parse()?,
        }),
    };

    Ok(Plan::CreateSink {
        name,
        sink,
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
    if !object_type_matches(ObjectType::View, catalog_entry.item()) {
        bail!("{} is not a view", on_name);
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
    let (name, columns, query, materialized, replace, with_options) = match &mut stmt {
        Statement::CreateView {
            name,
            columns,
            query,
            materialized,
            replace,
            with_options,
        } => (name, columns, query, materialized, replace, with_options),
        _ => unreachable!(),
    };
    if !with_options.is_empty() {
        bail!("WITH options are not yet supported");
    }
    let name = scx.allocate_name(normalize::object_name(name.to_owned())?);
    let replace = if *replace {
        let if_exists = true;
        let cascade = false;
        handle_drop_item(scx, ObjectType::View, if_exists, &name, cascade)?
    } else {
        None
    };
    let (mut relation_expr, mut desc, finishing) =
        handle_query(scx, *query.clone(), params, QueryLifetime::Static)?;
    if !finishing.is_trivial() {
        //TODO: materialize#724 - persist finishing information with the view?
        relation_expr = expr::RelationExpr::Project {
            input: Box::new(expr::RelationExpr::TopK {
                input: Box::new(relation_expr),
                group_key: vec![],
                order_key: finishing.order_by,
                limit: finishing.limit,
                offset: finishing.offset,
            }),
            outputs: finishing.project,
        }
    }
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
    Ok(Plan::CreateView {
        name,
        view: View {
            create_sql,
            expr: relation_expr,
            desc,
        },
        replace,
        materialize,
    })
}

/// Remove dependencies on external state: inline schemas in files,
/// fetch schemas from registries, and so on, so that we can generate a
/// fully-specified `create-sql` to persist in the catalog.
///
/// The `Statement` returned from this function should be able to be planned
/// without any asynchronicity or non-determinism.
async fn purify_statement(mut stmt: Statement) -> Result<Statement, failure::Error> {
    if let Statement::CreateSource {
        connector, format, ..
    } = &mut stmt
    {
        let topic = if let Connector::Kafka { broker, topic, .. } = connector {
            if !broker.contains(':') {
                *broker += ":9092";
            }
            Some(topic)
        } else {
            None
        };

        match format {
            Format::Avro(schema) => match schema {
                AvroSchema::CsrUrl { url, seed } => {
                    let topic = if let Some(topic) = topic {
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
            Format::Protobuf { schema, .. } => {
                if let sql_parser::ast::Schema::File(path) = schema {
                    let descriptors = tokio::fs::read(path).await?;
                    let mut buf = String::new();
                    strconv::format_bytes(&mut buf, &descriptors);
                    *schema = sql_parser::ast::Schema::Inline(buf);
                }
            }
            _ => {}
        }
    }
    Ok(stmt)
}

fn handle_create_dataflow_pure(
    stmt: Statement,
    session: Box<dyn PlanSession + Send>,
) -> Result<Plan, failure::Error> {
    match &stmt {
        Statement::CreateSource {
            name,
            connector,
            format,
            envelope,
            if_not_exists,
        } => {
            let envelope = match envelope {
                sql_parser::ast::Envelope::None => dataflow_types::Envelope::None,
                sql_parser::ast::Envelope::Debezium => dataflow_types::Envelope::Debezium,
            };

            let mut source = match connector {
                Connector::Kafka {
                    broker,
                    topic,
                    with_options,
                } => {
                    let mut with_options: HashMap<_, _> = with_options
                        .iter()
                        .map(|op| (op.name.value.to_ascii_lowercase(), op.value.clone()))
                        .collect();
                    let consistency = match with_options.remove("consistency") {
                        None => Consistency::RealTime,
                        Some(Value::SingleQuotedString(topic)) => Consistency::BringYourOwn(topic),
                        Some(_) => bail!("consistency must be a string"),
                    };

                    let ssl_certificate_file = match with_options.remove("ssl_certificate_file") {
                        None => None,
                        Some(Value::SingleQuotedString(p)) => Some(p.into()),
                        Some(_) => bail!("ssl_certificate_file must be a string"),
                    };

                    if !with_options.is_empty() {
                        bail!(
                            "Unexpected WITH options: {}",
                            join(with_options.keys(), ",")
                        )
                    }
                    let url = broker.parse()?;
                    build_kafka_source(
                        url,
                        topic.clone(),
                        format,
                        envelope,
                        consistency,
                        ssl_certificate_file,
                    )?
                }
                Connector::Kinesis { arn, with_options } => {
                    let mut with_options: HashMap<_, _> = with_options
                        .iter()
                        .map(|op| (op.name.value.to_ascii_lowercase(), op.value.clone()))
                        .collect();
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
                    let region = match with_options.remove("region") {
                        Some(Value::SingleQuotedString(region)) => region,
                        _ => bail!("Kinesis sources require a `region` option"),
                    };
                    if !with_options.is_empty() {
                        bail!(
                            "Unexpected WITH options: {}",
                            join(with_options.keys(), ",")
                        )
                    }

                    match envelope {
                        dataflow_types::Envelope::None => {}
                        dataflow_types::Envelope::Debezium => {
                            bail!("Debezium-envelope Kinesis sources are not supported")
                        }
                    }

                    let (encoding, desc) = match format {
                        Format::Bytes => (
                            DataEncoding::Bytes,
                            RelationDesc::new(
                                RelationType::new(vec![ColumnType::new(ScalarType::Bytes)]),
                                vec![Some(String::from("record"))],
                            ),
                        ),
                        _ => bail!("Kinesis sources only support data as bytes"),
                    };

                    let source = Source {
                        create_sql: "<filled in later>".into(),
                        connector: SourceConnector {
                            connector: ExternalSourceConnector::Kinesis(KinesisSourceConnector {
                                arn: arn.clone(),
                                access_key,
                                secret_access_key,
                                region,
                            }),
                            encoding,
                            envelope: Envelope::None,
                            consistency: Consistency::RealTime,
                        },
                        desc,
                    };
                    dbg!(&source);

                    // todo@jldlaughlin: Actually return the source when implemented!
                    // We can bail gracefully in the planning stage (here),
                    // but can't once we actually try to build a dataflow.
                    bail!("Kinesis sources are not yet implemented")
                }
                Connector::File { path, with_options } => {
                    let mut with_options: HashMap<_, _> = with_options
                        .iter()
                        .map(|op| (op.name.value.to_ascii_lowercase(), op.value.clone()))
                        .collect();
                    let tail = match with_options.remove("tail") {
                        None => false,
                        Some(Value::Boolean(b)) => b,
                        Some(_) => bail!("tail must be a boolean"),
                    };
                    if !with_options.is_empty() {
                        bail!(
                            "Unexpected WITH options: {}",
                            join(with_options.keys(), ",")
                        )
                    }
                    let (encoding, desc) = match format {
                        Format::Bytes => (
                            DataEncoding::Bytes,
                            RelationDesc::new(
                                RelationType::new(vec![
                                    ColumnType::new(ScalarType::Bytes),
                                    ColumnType::new(ScalarType::Int64).nullable(true),
                                ]),
                                iter::once(Some(String::from("data")))
                                    .chain(iter::once(Some(String::from("mz_line_no")))),
                            ),
                        ),
                        Format::Avro(_) => bail!("Avro-format file sources are not yet supported"),
                        Format::Protobuf { .. } => {
                            bail!("Protobuf-format file sources are not yet supported")
                        }
                        Format::Regex(s) => {
                            let regex = match regex::Regex::new(&s) {
                                Ok(r) => r,
                                Err(e) => bail!("Error compiling regex: {}", e),
                            };
                            let names: Vec<_> = regex
                                .capture_names()
                                .enumerate()
                                // The first capture is the entire matched string.
                                // This will often not be useful, so skip it.
                                // If people want it they can just surround their
                                // entire regex in an explicit capture group.
                                .skip(1)
                                .map(|(i, ocn)| match ocn {
                                    None => Some(format!("column{}", i)),
                                    Some(ocn) => Some(String::from(ocn)),
                                })
                                .chain(iter::once(Some(String::from("mz_line_no"))))
                                .collect();
                            let n_cols = names.len() - 1;
                            if n_cols == 0 {
                                bail!("source regex must contain at least one capture group to be useful");
                            }
                            let cols =
                                iter::repeat(ColumnType::new(ScalarType::String).nullable(true))
                                    .take(n_cols)
                                    .chain(iter::once(
                                        ColumnType::new(ScalarType::Int64).nullable(true),
                                    ))
                                    .collect();
                            (
                                DataEncoding::Regex { regex },
                                RelationDesc::new(RelationType::new(cols), names),
                            )
                        }
                        Format::Csv { n_cols, delimiter } => {
                            let delimiter = match *delimiter as u32 {
                                0..=127 => *delimiter as u8,
                                _ => bail!("CSV delimiter must be an ASCII character"),
                            };
                            let cols = iter::repeat(ColumnType::new(ScalarType::String))
                                .take(*n_cols)
                                .chain(iter::once(
                                    ColumnType::new(ScalarType::Int64).nullable(true),
                                ))
                                .collect();
                            let names = (1..=*n_cols)
                                .map(|i| Some(format!("column{}", i)))
                                .chain(iter::once(Some(String::from("mz_line_no"))));
                            (
                                DataEncoding::Csv(CsvEncoding {
                                    n_cols: *n_cols,
                                    delimiter,
                                }),
                                RelationDesc::new(RelationType::new(cols), names),
                            )
                        }
                        Format::Json => bail!("JSON-format file sources are not yet supported"),
                        Format::Text => (
                            DataEncoding::Text,
                            RelationDesc::new(
                                RelationType::new(vec![
                                    ColumnType::new(ScalarType::String),
                                    ColumnType::new(ScalarType::Int64).nullable(true),
                                ]),
                                iter::once(Some(String::from("text")))
                                    .chain(iter::once(Some(String::from("mz_line_no")))),
                            ),
                        ),
                    };
                    match envelope {
                        dataflow_types::Envelope::None => {}
                        dataflow_types::Envelope::Debezium => {
                            bail!("Debezium-envelope file sources are not supported")
                        }
                    }
                    Source {
                        create_sql: "<filled in below>".into(),
                        connector: SourceConnector {
                            connector: ExternalSourceConnector::File(FileSourceConnector {
                                path: path.clone().into(),
                                tail,
                            }),
                            encoding,
                            envelope,
                            consistency: Consistency::RealTime,
                        },
                        desc,
                    }
                }
            };

            // TODO(benesch): figure out how to get the actual catalog in here.
            // Likely need to return a non-async func that takes the session and
            // catalog.
            let catalog = Catalog::dummy();
            let scx = StatementContext {
                catalog: &catalog,
                session: &*session,
            };
            let if_not_exists = *if_not_exists;
            let name = scx.allocate_name(normalize::object_name(name.clone())?);
            source.create_sql = normalize::create_statement(&scx, stmt)?;
            Ok(Plan::CreateSource {
                name,
                source,
                if_not_exists,
            })
        }
        other => bail!("Unsupported statement: {:?}", other),
    }
}

async fn handle_create_dataflow(
    stmt: Statement,
    session: Box<dyn PlanSession + Send>,
) -> Result<Plan, failure::Error> {
    let stmt = purify_statement(stmt).await?;
    handle_create_dataflow_pure(stmt, session)
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
        Err(err) => return Err(err),
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
        Err(err) => return Err(err),
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
        Err(err) => Err(err),
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
    stage: Stage,
    query: Query,
    params: &Params,
) -> Result<Plan, failure::Error> {
    let (relation_expr, _desc, _finishing) =
        handle_query(scx, query, params, QueryLifetime::OneShot)?;
    // Previouly we would bail here for ORDER BY and LIMIT; this has been relaxed to silently
    // report the plan without the ORDER BY and LIMIT decorations (which are done in post).
    if stage == Stage::Dataflow {
        Ok(Plan::SendRows(vec![Row::pack(&[Datum::String(
            &relation_expr.pretty_humanized(scx.catalog),
        )])]))
    } else {
        Ok(Plan::ExplainPlan(relation_expr))
    }
}

/// Plans and decorrelates a `Query`. Like `query::plan_root_query`, but returns
/// an `::expr::RelationExpr`, which cannot include correlated expressions.
fn handle_query(
    scx: &StatementContext,
    query: Query,
    params: &Params,
    lifetime: QueryLifetime,
) -> Result<(expr::RelationExpr, RelationDesc, RowSetFinishing), failure::Error> {
    let (mut expr, desc, finishing, _param_types) = query::plan_root_query(scx, query, lifetime)?;
    expr.bind_parameters(&params);
    Ok((expr.decorrelate()?, desc, finishing))
}

fn build_kafka_source(
    url: Url,
    topic: String,
    format: &Format,
    envelope: Envelope,
    consistency: Consistency,
    ssl_certificate_file: Option<PathBuf>,
) -> Result<Source, failure::Error> {
    match (format, envelope) {
        (Format::Avro(schema), Envelope::Debezium) => {
            build_kafka_avro_source(schema, url, topic, consistency, ssl_certificate_file)
        }
        (Format::Avro(_), _) => {
            // TODO(brennan) -- there's no reason not to support this
            bail!("Currently, only Avro in Debezium-envelope format is supported")
        }
        (
            Format::Protobuf {
                message_name,
                schema,
            },
            Envelope::None,
        ) => build_kafka_protobuf_source(
            schema,
            url,
            topic,
            message_name,
            consistency,
            ssl_certificate_file,
        ),
        (Format::Protobuf { .. }, Envelope::Debezium) => {
            bail!("Currently, Debezium-style envelopes are not supported for protobuf messages.")
        }
        _ => bail!("Currently, Kafka sources only support Avro and Protobuf formats."), // TODO(brennan)
    }
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

fn build_kafka_avro_source(
    schema: &AvroSchema,
    kafka_url: Url,
    topic: String,
    consistency: Consistency,
    ssl_certificate_file: Option<PathBuf>,
) -> Result<Source, failure::Error> {
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
                unreachable!("CSR seed resolution should already have been called")
            }
        }
    };

    let mut desc = avro::validate_value_schema(&value_schema)?;
    if let Some(key_schema) = key_schema {
        let keys = avro::validate_key_schema(&key_schema, &desc)?;
        desc = desc.add_keys(keys);
    }

    Ok(Source {
        create_sql: "<filled in later>".into(),
        connector: SourceConnector {
            connector: ExternalSourceConnector::Kafka(KafkaSourceConnector {
                url: kafka_url,
                topic,
                ssl_certificate_file,
            }),
            encoding: DataEncoding::Avro(AvroEncoding {
                raw_schema: value_schema,
                schema_registry_url,
            }),
            envelope: Envelope::Debezium,
            consistency,
        },
        desc,
    })
}

fn build_kafka_protobuf_source(
    schema: &sql_parser::ast::Schema,
    url: Url,
    topic: String,
    message_name: &str,
    consistency: Consistency,
    ssl_certificate_file: Option<PathBuf>,
) -> Result<Source, failure::Error> {
    let descriptors = match schema {
        sql_parser::ast::Schema::Inline(bytes) => strconv::parse_bytes(&bytes)?,
        sql_parser::ast::Schema::File(_) => {
            unreachable!("File schema should already have been inlined")
        }
    };
    let desc = protobuf::validate_descriptors(
        &message_name,
        &protobuf::decode_descriptors(&descriptors)?,
    )?;
    Ok(Source {
        create_sql: "<filled in later>".into(),
        connector: SourceConnector {
            connector: ExternalSourceConnector::Kafka(KafkaSourceConnector {
                url,
                topic,
                ssl_certificate_file,
            }),
            encoding: DataEncoding::Protobuf(ProtobufEncoding {
                descriptors,
                message_name: message_name.to_owned(),
            }),
            envelope: Envelope::None,
            consistency,
        },
        desc,
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
        self.catalog
            .resolve(self.session.database(), self.session.search_path(), &name)
    }
}
