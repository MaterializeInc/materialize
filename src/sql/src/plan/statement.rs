// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::fmt;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use aws_arn::{Resource, ARN};
use itertools::Itertools;
use kafka_util::{extract_config, generate_ccsr_client_config};
use regex::Regex;
use rusoto_core::Region;
use url::Url;

use dataflow_types::{
    AvroEncoding, AvroOcfEncoding, AvroOcfSinkConnectorBuilder, Consistency, CsvEncoding,
    DataEncoding, Envelope, ExternalSourceConnector, FileSourceConnector,
    KafkaSinkConnectorBuilder, KafkaSourceConnector, KinesisSourceConnector, ProtobufEncoding,
    RegexEncoding, SinkConnectorBuilder, SourceConnector,
};
use expr::{GlobalId, RowSetFinishing};
use interchange::avro::{self, DebeziumDeduplicationStrategy, Encoder};
use ore::collections::CollectionExt;
use ore::iter::IteratorExt;
use repr::{strconv, RelationDesc, RelationType, ScalarType};
use sql_parser::ast::display::AstDisplay;
use sql_parser::ast::{
    AlterIndexOptionsList, AlterIndexOptionsStatement, AlterObjectRenameStatement, AvroSchema,
    ColumnOption, Connector, CopyDirection, CopyRelation, CopyStatement, CopyTarget,
    CreateDatabaseStatement, CreateIndexStatement, CreateMapTypeStatement, CreateSchemaStatement,
    CreateSinkStatement, CreateSourceStatement, CreateTableStatement, CreateViewStatement,
    DiscardStatement, DiscardTarget, DropDatabaseStatement, DropObjectsStatement, ExplainStage,
    ExplainStatement, Explainee, Expr, Format, Ident, IfExistsBehavior, InsertStatement,
    ObjectName, ObjectType, Query, SelectStatement, SetVariableStatement, SetVariableValue,
    ShowVariableStatement, SqlOption, Statement, TailStatement, Value, WithOption, WithOptionValue,
};

use crate::catalog::{Catalog, CatalogItemType};
use crate::kafka_util;
use crate::names::{DatabaseSpecifier, FullName, PartialName, SchemaSpecifier};
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::query::QueryLifetime;
use crate::plan::{
    query, scalar_type_from_sql, AlterIndexLogicalCompactionWindow, CopyFormat, Index,
    LogicalCompactionWindow, Params, PeekWhen, Plan, PlanContext, Sink, Source, Table, Type,
    TypeInner, View,
};
use crate::pure::Schema;

mod show;

/// Describes the output of a SQL statement.
#[derive(Debug, Clone)]
pub struct StatementDesc {
    /// The shape of the rows produced by the statement, if the statement
    /// produces rows.
    pub relation_desc: Option<RelationDesc>,
    /// The determined types of the parameters in the statement, if any.
    pub param_types: Vec<pgrepr::Type>,
    /// Whether the statement is a `COPY` statement.
    pub is_copy: bool,
}

impl StatementDesc {
    pub fn new(relation_desc: Option<RelationDesc>) -> Self {
        StatementDesc {
            relation_desc,
            param_types: vec![],
            is_copy: false,
        }
    }

    /// Reports the number of columns in the statement's result set, or zero if
    /// the statement does not return rows.
    pub fn arity(&self) -> usize {
        self.relation_desc
            .as_ref()
            .map(|desc| desc.typ().column_types.len())
            .unwrap_or(0)
    }

    fn with_params(mut self, param_types: Vec<ScalarType>) -> Self {
        self.param_types = param_types.iter().map(pgrepr::Type::from).collect();
        self
    }

    fn with_pgrepr_params(mut self, param_types: Vec<pgrepr::Type>) -> Self {
        self.param_types = param_types;
        self
    }

    fn with_is_copy(mut self) -> Self {
        self.is_copy = true;
        self
    }
}

pub fn describe_statement(
    catalog: &dyn Catalog,
    stmt: Statement,
    param_types_in: &[Option<pgrepr::Type>],
) -> Result<StatementDesc, anyhow::Error> {
    let mut param_types = BTreeMap::new();
    for (i, ty) in param_types_in.iter().enumerate() {
        if let Some(ty) = ty {
            param_types.insert(i + 1, query::scalar_type_from_pg(ty)?);
        }
    }
    let scx = StatementContext {
        catalog,
        pcx: &PlanContext::default(),
        param_types: Rc::new(RefCell::new(param_types)),
    };
    Ok(match stmt {
        Statement::CreateDatabase(_)
        | Statement::CreateSchema(_)
        | Statement::CreateIndex(_)
        | Statement::CreateSource(_)
        | Statement::CreateTable(_)
        | Statement::CreateSink(_)
        | Statement::CreateView(_)
        | Statement::CreateMapType(_)
        | Statement::DropDatabase(_)
        | Statement::DropObjects(_)
        | Statement::SetVariable(_)
        | Statement::Discard(_)
        | Statement::StartTransaction(_)
        | Statement::Rollback(_)
        | Statement::Commit(_)
        | Statement::AlterObjectRename(_)
        | Statement::AlterIndexOptions(_) => StatementDesc::new(None),

        Statement::Explain(ExplainStatement {
            stage, explainee, ..
        }) => StatementDesc::new(Some(RelationDesc::empty().with_column(
            match stage {
                ExplainStage::RawPlan => "Raw Plan",
                ExplainStage::DecorrelatedPlan => "Decorrelated Plan",
                ExplainStage::OptimizedPlan { .. } => "Optimized Plan",
            },
            ScalarType::String.nullable(false),
        )))
        .with_pgrepr_params(match explainee {
            Explainee::Query(q) => {
                describe_statement(
                    catalog,
                    Statement::Select(SelectStatement {
                        query: q,
                        as_of: None,
                    }),
                    param_types_in,
                )?
                .param_types
            }
            _ => vec![],
        }),

        Statement::ShowCreateView(_) => StatementDesc::new(Some(
            RelationDesc::empty()
                .with_column("View", ScalarType::String.nullable(false))
                .with_column("Create View", ScalarType::String.nullable(false)),
        )),

        Statement::ShowCreateSource(_) => StatementDesc::new(Some(
            RelationDesc::empty()
                .with_column("Source", ScalarType::String.nullable(false))
                .with_column("Create Source", ScalarType::String.nullable(false)),
        )),

        Statement::ShowCreateTable(_) => StatementDesc::new(Some(
            RelationDesc::empty()
                .with_column("Table", ScalarType::String.nullable(false))
                .with_column("Create Table", ScalarType::String.nullable(false)),
        )),

        Statement::ShowCreateSink(_) => StatementDesc::new(Some(
            RelationDesc::empty()
                .with_column("Sink", ScalarType::String.nullable(false))
                .with_column("Create Sink", ScalarType::String.nullable(false)),
        )),

        Statement::ShowCreateIndex(_) => StatementDesc::new(Some(
            RelationDesc::empty()
                .with_column("Index", ScalarType::String.nullable(false))
                .with_column("Create Index", ScalarType::String.nullable(false)),
        )),

        Statement::ShowColumns(stmt) => show::show_columns(&scx, stmt)?.describe()?,
        Statement::ShowIndexes(stmt) => show::show_indexes(&scx, stmt)?.describe()?,
        Statement::ShowDatabases(stmt) => show::show_databases(&scx, stmt)?.describe()?,
        Statement::ShowObjects(stmt) => show::show_objects(&scx, stmt)?.describe()?,

        Statement::ShowVariable(ShowVariableStatement { variable, .. }) => {
            StatementDesc::new(Some(if variable.as_str() == unicase::Ascii::new("ALL") {
                RelationDesc::empty()
                    .with_column("name", ScalarType::String.nullable(false))
                    .with_column("setting", ScalarType::String.nullable(false))
                    .with_column("description", ScalarType::String.nullable(false))
            } else {
                RelationDesc::empty()
                    .with_column(variable.as_str(), ScalarType::String.nullable(false))
            }))
        }

        Statement::Tail(TailStatement { name, options, .. }) => {
            let name = scx.resolve_item(name)?;
            let sql_object = scx.catalog.get_item(&name);
            let options = TailOptions::try_from(options)?;
            const MAX_U64_DIGITS: u8 = 20;
            let mut desc = RelationDesc::empty().with_column(
                "timestamp",
                ScalarType::Decimal(MAX_U64_DIGITS, 0).nullable(false),
            );
            if options.progress.unwrap_or(false) {
                desc = desc.with_column("progressed", ScalarType::Bool.nullable(false));
            }
            let desc = desc
                .with_column("diff", ScalarType::Int64.nullable(true))
                .concat(sql_object.desc()?.clone());
            StatementDesc::new(Some(desc))
        }

        Statement::Copy(CopyStatement { relation, .. }) => match relation {
            CopyRelation::Table { .. } => bail!("unsupported COPY relation {:?}", relation),
            CopyRelation::Select(stmt) => {
                describe_statement(catalog, Statement::Select(stmt), param_types_in)?
            }
            CopyRelation::Tail(stmt) => {
                describe_statement(catalog, Statement::Tail(stmt), param_types_in)?
            }
        }
        .with_is_copy(),

        // TODO(benesch): currently, describing a `SELECT` or `INSERT` query
        // plans the whole query to determine its shape and parameter types,
        // and then throws away that plan. If we were smarter, we'd stash that
        // plan somewhere so we don't have to recompute it when the query is
        // executed.
        Statement::Select(SelectStatement { query, .. }) => {
            let (_relation_expr, desc, _finishing) =
                query::plan_root_query(&scx, query, QueryLifetime::OneShot)?;
            StatementDesc::new(Some(desc)).with_params(scx.finalize_param_types()?)
        }
        Statement::Insert(InsertStatement {
            table_name,
            columns,
            source,
            ..
        }) => {
            query::plan_insert_query(&scx, table_name, columns, source)?;
            StatementDesc::new(None).with_params(scx.finalize_param_types()?)
        }

        Statement::Update(_) => bail!("UPDATE statements are not supported"),
        Statement::Delete(_) => bail!("DELETE statements are not supported"),
        Statement::SetTransaction(_) => bail!("SET TRANSACTION statements are not supported"),
    })
}

pub fn handle_statement(
    pcx: &PlanContext,
    catalog: &dyn Catalog,
    stmt: Statement,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let param_types = params
        .types
        .iter()
        .enumerate()
        .map(|(i, ty)| (i + 1, ty.clone()))
        .collect();
    let scx = &StatementContext {
        pcx,
        catalog,
        param_types: Rc::new(RefCell::new(param_types)),
    };
    match stmt {
        Statement::CreateDatabase(stmt) => handle_create_database(scx, stmt),
        Statement::CreateIndex(stmt) => handle_create_index(scx, stmt),
        Statement::CreateSchema(stmt) => handle_create_schema(scx, stmt),
        Statement::CreateSink(stmt) => handle_create_sink(scx, stmt),
        Statement::CreateSource(stmt) => handle_create_source(scx, stmt),
        Statement::CreateTable(stmt) => handle_create_table(scx, stmt),
        Statement::CreateView(stmt) => handle_create_view(scx, stmt, params),
        Statement::CreateMapType(stmt) => handle_create_map_type(scx, stmt),
        Statement::DropDatabase(stmt) => handle_drop_database(scx, stmt),
        Statement::DropObjects(stmt) => handle_drop_objects(scx, stmt),
        Statement::AlterObjectRename(stmt) => handle_alter_object_rename(scx, stmt),
        Statement::AlterIndexOptions(stmt) => handle_alter_index_options(scx, stmt),

        Statement::ShowColumns(stmt) => show::show_columns(scx, stmt)?.handle(),
        Statement::ShowCreateTable(stmt) => show::handle_show_create_table(scx, stmt),
        Statement::ShowCreateSource(stmt) => show::handle_show_create_source(scx, stmt),
        Statement::ShowCreateView(stmt) => show::handle_show_create_view(scx, stmt),
        Statement::ShowCreateSink(stmt) => show::handle_show_create_sink(scx, stmt),
        Statement::ShowCreateIndex(stmt) => show::handle_show_create_index(scx, stmt),
        Statement::ShowDatabases(stmt) => show::show_databases(scx, stmt)?.handle(),
        Statement::ShowObjects(stmt) => show::show_objects(scx, stmt)?.handle(),
        Statement::ShowIndexes(stmt) => show::show_indexes(scx, stmt)?.handle(),

        Statement::SetVariable(stmt) => handle_set_variable(scx, stmt),
        Statement::ShowVariable(stmt) => handle_show_variable(scx, stmt),
        Statement::Discard(stmt) => handle_discard(scx, stmt),

        Statement::Explain(stmt) => handle_explain(scx, stmt, params),
        Statement::Select(stmt) => handle_select(scx, stmt, params, None),
        Statement::Tail(stmt) => handle_tail(scx, stmt, None),
        Statement::Copy(stmt) => handle_copy(scx, stmt),

        Statement::Insert(stmt) => handle_insert(scx, stmt, params),

        Statement::StartTransaction(_) => Ok(Plan::StartTransaction),
        Statement::Rollback(_) => Ok(Plan::AbortTransaction),
        Statement::Commit(_) => Ok(Plan::CommitTransaction),

        Statement::Update(_) => bail!("UPDATE statements are not supported"),
        Statement::Delete(_) => bail!("DELETE statements are not supported"),
        Statement::SetTransaction(_) => bail!("SET TRANSACTION statements are not supported"),
    }
}

fn handle_set_variable(
    _: &StatementContext,
    SetVariableStatement {
        local,
        variable,
        value,
    }: SetVariableStatement,
) -> Result<Plan, anyhow::Error> {
    if local {
        unsupported!("SET LOCAL");
    }
    Ok(Plan::SetVariable {
        name: variable.to_string(),
        value: match value {
            SetVariableValue::Literal(Value::String(s)) => s,
            SetVariableValue::Literal(lit) => lit.to_string(),
            SetVariableValue::Ident(ident) => ident.into_string(),
        },
    })
}

fn handle_show_variable(
    _: &StatementContext,
    ShowVariableStatement { variable }: ShowVariableStatement,
) -> Result<Plan, anyhow::Error> {
    if variable.as_str() == unicase::Ascii::new("ALL") {
        Ok(Plan::ShowAllVariables)
    } else {
        Ok(Plan::ShowVariable(variable.to_string()))
    }
}

fn handle_discard(
    _: &StatementContext,
    DiscardStatement { target }: DiscardStatement,
) -> Result<Plan, anyhow::Error> {
    match target {
        DiscardTarget::All => Ok(Plan::DiscardAll),
        DiscardTarget::Temp => Ok(Plan::DiscardTemp),
        DiscardTarget::Sequences => unsupported!("DISCARD SEQUENCES"),
        DiscardTarget::Plans => unsupported!("DISCARD PLANS"),
    }
}

fn handle_tail(
    scx: &StatementContext,
    TailStatement {
        name,
        options,
        as_of,
    }: TailStatement,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, anyhow::Error> {
    let from = scx.resolve_item(name)?;
    let entry = scx.catalog.get_item(&from);
    let ts = as_of.map(|e| query::eval_as_of(scx, e)).transpose()?;
    let options = TailOptions::try_from(options)?;

    match entry.item_type() {
        CatalogItemType::Table | CatalogItemType::Source | CatalogItemType::View => {
            Ok(Plan::Tail {
                id: entry.id(),
                ts,
                with_snapshot: options.snapshot.unwrap_or(true),
                copy_to,
                emit_progress: options.progress.unwrap_or(false),
                object_columns: entry.desc()?.arity(),
            })
        }
        CatalogItemType::Index | CatalogItemType::Sink | CatalogItemType::Type => bail!(
            "'{}' cannot be tailed because it is a {}",
            from,
            entry.item_type(),
        ),
    }
}

fn handle_copy(
    scx: &StatementContext,
    CopyStatement {
        relation,
        direction,
        target,
        options,
    }: CopyStatement,
) -> Result<Plan, anyhow::Error> {
    let options = CopyOptions::try_from(options)?;
    let format = if let Some(format) = options.format {
        match format.to_lowercase().as_str() {
            "text" => CopyFormat::Text,
            "csv" => CopyFormat::Csv,
            "binary" => CopyFormat::Binary,
            _ => bail!("unknown FORMAT: {}", format),
        }
    } else {
        CopyFormat::Text
    };
    match (&direction, &target) {
        (CopyDirection::To, CopyTarget::Stdout) => match relation {
            CopyRelation::Table { .. } => bail!("table with COPY TO unsupported"),
            CopyRelation::Select(stmt) => {
                Ok(handle_select(scx, stmt, &Params::empty(), Some(format))?)
            }
            CopyRelation::Tail(stmt) => Ok(handle_tail(scx, stmt, Some(format))?),
        },
        _ => bail!("COPY {} {} not supported", direction, target),
    }
}

fn handle_alter_object_rename(
    scx: &StatementContext,
    AlterObjectRenameStatement {
        name,
        object_type,
        if_exists,
        to_item_name,
    }: AlterObjectRenameStatement,
) -> Result<Plan, anyhow::Error> {
    let id = match scx.resolve_item(name.clone()) {
        Ok(from_name) => {
            let entry = scx.catalog.get_item(&from_name);
            if entry.item_type() != object_type {
                bail!("{} is a {} not a {}", name, entry.item_type(), object_type)
            }
            let mut proposed_name = name.0;
            let last = proposed_name.last_mut().unwrap();
            *last = to_item_name.clone();
            if scx.resolve_item(ObjectName(proposed_name)).is_ok() {
                bail!("{} is already taken by item in schema", to_item_name)
            }
            Some(entry.id())
        }
        Err(_) if if_exists => {
            // TODO(benesch): generate a notice indicating this
            // item does not exist.
            None
        }
        Err(err) => return Err(err.into()),
    };

    Ok(Plan::AlterItemRename {
        id,
        to_name: normalize::ident(to_item_name),
        object_type,
    })
}

fn handle_alter_index_options(
    scx: &StatementContext,
    AlterIndexOptionsStatement {
        index_name,
        if_exists,
        options,
    }: AlterIndexOptionsStatement,
) -> Result<Plan, anyhow::Error> {
    let alter_index = match scx.resolve_item(index_name) {
        Ok(name) => {
            let entry = scx.catalog.get_item(&name);
            if entry.item_type() != CatalogItemType::Index {
                bail!("{} is a {} not a index", name, entry.item_type())
            }

            let logical_compaction_window = match options {
                AlterIndexOptionsList::Reset(o) => {
                    let mut options: HashSet<_> =
                        o.iter().map(|x| normalize::ident(x.clone())).collect();
                    // Follow Postgres and don't complain if unknown parameters
                    // are passed into ALTER INDEX ... RESET
                    if options.remove("logical_compaction_window") {
                        Some(LogicalCompactionWindow::Default)
                    } else {
                        None
                    }
                }
                AlterIndexOptionsList::Set(o) => {
                    let mut options = normalize::options(&o);

                    let logical_compaction_window = match options
                        .remove("logical_compaction_window")
                    {
                        Some(Value::String(window)) => match window.as_str() {
                            "off" => Some(LogicalCompactionWindow::Off),
                            s => Some(LogicalCompactionWindow::Custom(parse_duration::parse(s)?)),
                        },
                        Some(_) => bail!("\"logical_compaction_window\" must be a string"),
                        None => None,
                    };

                    if !options.is_empty() {
                        bail!("unrecognized parameter: \"{}\". Only \"logical_compaction_window\" is currently supported.",
                              options.keys().next().expect("known to exist"))
                    }

                    logical_compaction_window
                }
            };

            if let Some(logical_compaction_window) = logical_compaction_window {
                Some(AlterIndexLogicalCompactionWindow {
                    index: entry.id(),
                    logical_compaction_window,
                })
            } else {
                None
            }
        }
        Err(_) if if_exists => {
            // TODO(rkhaitan): better message indicating that the index does not exist.
            None
        }
        Err(e) => return Err(e.into()),
    };

    Ok(Plan::AlterIndexLogicalCompactionWindow(alter_index))
}

fn kafka_sink_builder(
    format: Option<Format>,
    with_options: Vec<SqlOption>,
    broker: String,
    topic_prefix: String,
    desc: RelationDesc,
    topic_suffix: String,
    key_indices: Option<Vec<usize>>,
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

    let mut with_options = normalize::options(&with_options);
    let include_consistency = match with_options.remove("consistency") {
        Some(Value::Boolean(b)) => b,
        None => false,
        Some(_) => bail!("consistency must be a boolean"),
    };

    let encoder = Encoder::new(desc, include_consistency, key_indices.clone());
    let value_schema = encoder.writer_schema().canonical_form();
    let key_schema = encoder
        .key_writer_schema()
        .map(|key_schema| key_schema.canonical_form());

    // Use the user supplied value for replication factor, or default to 1
    let replication_factor = match with_options.remove("replication_factor") {
        None => 1,
        Some(Value::Number(n)) => n.parse::<u32>()?,
        Some(_) => bail!("replication factor for sink topics has to be a positive integer"),
    };

    if replication_factor == 0 {
        bail!("replication factor for sink topics has to be greater than zero");
    }

    let consistency_value_schema = if include_consistency {
        Some(avro::get_debezium_transaction_schema().canonical_form())
    } else {
        None
    };

    let config_options = extract_config(&with_options)?;

    let ccsr_config = generate_ccsr_client_config(
        schema_registry_url.clone(),
        &config_options,
        &ccsr_with_options,
    )?;
    Ok(SinkConnectorBuilder::Kafka(KafkaSinkConnectorBuilder {
        broker_addrs,
        schema_registry_url,
        value_schema,
        topic_prefix,
        topic_suffix,
        replication_factor,
        fuel: 10000,
        consistency_value_schema,
        config_options,
        ccsr_config,
        key_indices,
        key_schema,
    }))
}

fn avro_ocf_sink_builder(
    format: Option<Format>,
    with_options: Vec<SqlOption>,
    path: String,
    file_name_suffix: String,
) -> Result<SinkConnectorBuilder, anyhow::Error> {
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

fn handle_create_sink(
    scx: &StatementContext,
    stmt: CreateSinkStatement,
) -> Result<Plan, anyhow::Error> {
    let create_sql = normalize::create_statement(scx, Statement::CreateSink(stmt.clone()))?;
    let CreateSinkStatement {
        name,
        from,
        connector,
        with_options,
        format,
        with_snapshot,
        as_of,
        if_not_exists,
    } = stmt;
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

    let as_of = as_of.map(|e| query::eval_as_of(scx, e)).transpose()?;
    let connector_builder = match connector {
        Connector::File { .. } => unsupported!("file sinks"),
        Connector::Kafka { broker, topic, key } => {
            let desc = from.desc()?;
            let key_indices = if let Some(key) = key {
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
            };
            kafka_sink_builder(
                format,
                with_options,
                broker,
                topic,
                desc.clone(),
                suffix,
                key_indices,
            )?
        }
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
        with_snapshot,
        as_of,
        if_not_exists,
    })
}

fn handle_create_index(
    scx: &StatementContext,
    mut stmt: CreateIndexStatement,
) -> Result<Plan, anyhow::Error> {
    let CreateIndexStatement {
        name,
        on_name,
        key_parts,
        if_not_exists,
    } = &mut stmt;
    let on_name = scx.resolve_item(on_name.clone())?;
    let catalog_entry = scx.catalog.get_item(&on_name);

    if CatalogItemType::View != catalog_entry.item_type()
        && CatalogItemType::Source != catalog_entry.item_type()
        && CatalogItemType::Table != catalog_entry.item_type()
    {
        bail!(
            "index cannot be created on {} because it is a {}",
            on_name,
            catalog_entry.item_type()
        )
    }

    let on_desc = catalog_entry.desc()?;

    let filled_key_parts = match key_parts {
        Some(kp) => kp.to_vec(),
        None => {
            // `key_parts` is None if we're creating a "default" index, i.e.
            // creating the index as if the index had been created alongside the
            // view source, e.g. `CREATE MATERIALIZED...`
            catalog_entry
                .desc()?
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
    let keys = query::plan_index_exprs(scx, on_desc, filled_key_parts.clone())?;

    let index_name = if let Some(name) = name {
        FullName {
            database: on_name.database.clone(),
            schema: on_name.schema.clone(),
            item: normalize::ident(name.clone()),
        }
    } else {
        let mut idx_name_base = on_name.clone();
        if key_parts.is_none() {
            // We're trying to create the "default" index.
            idx_name_base.item += "_primary_idx";
        } else {
            // Use PG schema for automatically naming indexes:
            // `<table>_<_-separated indexed expressions>_idx`
            let index_name_col_suffix = keys
                .iter()
                .map(|k| match k {
                    expr::ScalarExpr::Column(i) => match on_desc.get_unambiguous_name(*i) {
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

        let mut cat_schema_iter = scx.catalog.list_items(&on_name.database, &on_name.schema);

        // Search for an unused version of the name unless `if_not_exists`.
        while cat_schema_iter.any(|i| *i.name() == index_name) && !*if_not_exists {
            i += 1;
            index_name = idx_name_base.clone();
            index_name.item += &i.to_string();
            cat_schema_iter = scx.catalog.list_items(&on_name.database, &on_name.schema);
        }

        index_name
    };

    // Normalize `stmt`.
    *name = Some(Ident::new(index_name.item.clone()));
    *key_parts = Some(filled_key_parts);
    let if_not_exists = *if_not_exists;
    let create_sql = normalize::create_statement(scx, Statement::CreateIndex(stmt))?;

    Ok(Plan::CreateIndex {
        name: index_name,
        index: Index {
            create_sql,
            on: catalog_entry.id(),
            keys,
        },
        if_not_exists,
    })
}

fn handle_create_database(
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

fn handle_create_schema(
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

fn handle_create_view(
    scx: &StatementContext,
    mut stmt: CreateViewStatement,
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
    let (mut relation_expr, mut desc, finishing) =
        query::plan_root_query(scx, query.clone(), QueryLifetime::Static)?;
    relation_expr.bind_parameters(&params)?;
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
            column_names: desc.iter_names().map(|n| n.cloned()).collect(),
            temporary,
        },
        replace,
        materialize,
        if_not_exists,
    })
}

fn handle_create_map_type(
    scx: &StatementContext,
    stmt: CreateMapTypeStatement,
) -> Result<Plan, anyhow::Error> {
    let create_sql = normalize::create_statement(scx, Statement::CreateMapType(stmt.clone()))?;
    let CreateMapTypeStatement { name, with_options } = stmt;

    let mut with_options = normalize::option_objects(&with_options);
    let key_name = match with_options.remove("key_type") {
        Some(SqlOption::Value {
            value: Value::String(val),
            ..
        }) => ObjectName(vec![Ident::new(val)]),
        Some(SqlOption::ObjectName { object_name, .. }) => object_name,
        Some(_) => bail!("key_type must be a string or identifier"),
        None => bail!("key_type parameter required"),
    };
    let key = scx
        .catalog
        .resolve_item(&normalize::object_name(key_name)?)?;
    if key.item.to_uppercase().as_str() != "TEXT" {
        bail!("key_type must be text")
    }
    let key_id = scx.catalog.get_item(&key).id();

    let value_name = match with_options.remove("value_type") {
        Some(SqlOption::Value {
            value: Value::String(val),
            ..
        }) => ObjectName(vec![Ident::new(val)]),
        Some(SqlOption::ObjectName { object_name, .. }) => object_name,
        Some(_) => bail!("value_type must be a string or identifier"),
        None => bail!("value_type parameter required"),
    };
    let value = scx
        .catalog
        .resolve_item(&normalize::object_name(value_name)?)?;
    let value_id = scx.catalog.get_item(&value).id();

    if !with_options.is_empty() {
        bail!(
            "unexpected parameters for CREATE TYPE: {}",
            with_options.keys().join(",")
        )
    }

    let name = scx.allocate_name(normalize::object_name(name)?);
    if scx.catalog.type_exists(&name) {
        bail!("type \"{}\" already exists", name.to_string());
    }

    Ok(Plan::CreateType {
        name,
        typ: Type {
            create_sql,
            inner: TypeInner::Map { key_id, value_id },
        },
    })
}

fn extract_timestamp_frequency_option(
    with_options: &mut HashMap<String, Value>,
) -> Result<Duration, anyhow::Error> {
    match with_options.remove("timestamp_frequency_ms") {
        None => Ok(Duration::from_secs(1)),
        Some(Value::Number(n)) => match n.parse::<u64>() {
            Ok(n) => Ok(Duration::from_millis(n)),
            _ => bail!("timestamp_frequency_ms must be an u64"),
        },
        Some(_) => bail!("timestamp_frequency_ms must be an u64"),
    }
}

fn handle_create_source(
    scx: &StatementContext,
    stmt: CreateSourceStatement,
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
    let get_encoding = |format: &Option<Format>| {
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
                    AvroSchema::CsrUrl {
                        url,
                        seed,
                        with_options: ccsr_options,
                    } => {
                        let url: Url = url.parse()?;
                        let kafka_options =
                            kafka_util::extract_config(&normalize::options(with_options))?;
                        let ccsr_config = kafka_util::generate_ccsr_client_config(
                            url,
                            &kafka_options,
                            &normalize::options(ccsr_options),
                        )?;

                        if let Some(seed) = seed {
                            Schema {
                                key_schema: seed.key_schema.clone(),
                                value_schema: seed.value_schema.clone(),
                                schema_registry_config: Some(ccsr_config),
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
    let mut ts_frequency = Duration::from_secs(1);

    let (external_connector, mut encoding) = match connector {
        Connector::Kafka { broker, topic, .. } => {
            let config_options = kafka_util::extract_config(&with_options)?;

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

            ts_frequency = extract_timestamp_frequency_option(&mut with_options)?;

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

            let enable_caching = match with_options.remove("cache") {
                None => false,
                Some(Value::Boolean(b)) => b,
                Some(_) => bail!("cache must be a bool!"),
            };

            if enable_caching && consistency != Consistency::RealTime {
                unsupported!("BYO source caching")
            }

            let mut start_offsets = HashMap::new();
            start_offsets.insert(0, start_offset);

            let connector = ExternalSourceConnector::Kafka(KafkaSourceConnector {
                addrs: broker.parse()?,
                topic: topic.clone(),
                config_options,
                start_offsets,
                group_id_prefix,
                enable_caching,
                cached_files: None,
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
                    if let Some(path) = path.strip_prefix("stream/") {
                        path.to_owned()
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
                            Some(Value::String(endpoint)) => Region::Custom {
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
                Some(Value::String(access_key_id)) => Some(access_key_id),
                Some(_) => bail!("access_key_id must be a string"),
                _ => None,
            };
            let secret_access_key = match with_options.remove("secret_access_key") {
                Some(Value::String(secret_access_key)) => Some(secret_access_key),
                Some(_) => bail!("secret_access_key must be a string"),
                _ => None,
            };
            let token = match with_options.remove("token") {
                Some(Value::String(token)) => Some(token),
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
                Some(Value::String(topic)) => Consistency::BringYourOwn(topic),
                Some(_) => bail!("consistency must be a string"),
            };
            ts_frequency = extract_timestamp_frequency_option(&mut with_options)?;

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
                Some(Value::String(topic)) => Consistency::BringYourOwn(topic),
                Some(_) => bail!("consistency must be a string"),
            };

            ts_frequency = extract_timestamp_frequency_option(&mut with_options)?;

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
        sql_parser::ast::Envelope::None => dataflow_types::Envelope::None,
        sql_parser::ast::Envelope::Debezium => {
            let dedup_strat = match with_options.remove("deduplication") {
                None => DebeziumDeduplicationStrategy::Ordered,
                Some(Value::String(s)) => {
                    match s.as_str() {
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
            dataflow_types::Envelope::CdcV2
        }
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

    desc = maybe_rename_columns(format!("source {}", name), desc, &col_names)?;

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
    let create_sql = normalize::create_statement(&scx, Statement::CreateSource(stmt))?;

    let source = Source {
        create_sql,
        connector: SourceConnector::External {
            connector: external_connector,
            encoding,
            envelope,
            consistency,
            ts_frequency,
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

fn handle_create_table(
    scx: &StatementContext,
    stmt: CreateTableStatement,
) -> Result<Plan, anyhow::Error> {
    let CreateTableStatement {
        name,
        columns,
        constraints,
        with_options,
        if_not_exists,
    } = &stmt;

    if !with_options.is_empty() {
        unsupported!("WITH options");
    }
    if !constraints.is_empty() {
        unsupported!("CREATE TABLE with constraints")
    }

    let names: Vec<_> = columns
        .iter()
        .map(|c| Some(normalize::column_name(c.name.clone())))
        .collect();

    if names.iter().has_duplicates() {
        bail!("cannot CREATE TABLE with duplicate column names");
    }

    // Build initial relation type that handles declared data types
    // and NOT NULL constraints.
    let typ = RelationType::new(
        columns
            .iter()
            .map(|c| {
                let ty = scalar_type_from_sql(&c.data_type)?;
                let mut nullable = true;
                for option in c.options.iter() {
                    match &option.option {
                        ColumnOption::NotNull => nullable = false,
                        other => {
                            unsupported!(format!("CREATE TABLE with column constraint: {}", other))
                        }
                    }
                }
                Ok(ty.nullable(nullable))
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?,
    );

    let name = scx.allocate_name(normalize::object_name(name.clone())?);
    let desc = RelationDesc::new(typ, names);

    let create_sql = normalize::create_statement(&scx, Statement::CreateTable(stmt.clone()))?;
    let table = Table { create_sql, desc };
    Ok(Plan::CreateTable {
        name,
        table,
        if_not_exists: *if_not_exists,
    })
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
) -> Result<RelationDesc, anyhow::Error> {
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
    DropDatabaseStatement { name, if_exists }: DropDatabaseStatement,
) -> Result<Plan, anyhow::Error> {
    let name = match scx.resolve_database_ident(name) {
        Ok((name, _id)) => name,
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

fn handle_drop_objects(
    scx: &StatementContext,
    DropObjectsStatement {
        object_type,
        if_exists,
        names,
        cascade,
    }: DropObjectsStatement,
) -> Result<Plan, anyhow::Error> {
    match object_type {
        ObjectType::Schema => handle_drop_schema(scx, if_exists, names, cascade),
        ObjectType::Source
        | ObjectType::Table
        | ObjectType::View
        | ObjectType::Index
        | ObjectType::Sink
        | ObjectType::Type => handle_drop_items(scx, object_type, if_exists, names, cascade),
    }
}

fn handle_drop_schema(
    scx: &StatementContext,
    if_exists: bool,
    names: Vec<ObjectName>,
    cascade: bool,
) -> Result<Plan, anyhow::Error> {
    if names.len() != 1 {
        unsupported!("DROP SCHEMA with multiple schemas");
    }
    match scx.resolve_schema(names.into_element()) {
        Ok((database_spec, schema_spec)) => {
            if let DatabaseSpecifier::Ambient = database_spec {
                bail!(
                    "cannot drop schema {} because it is required by the database system",
                    schema_spec.name
                );
            }
            let mut items = scx.catalog.list_items(&database_spec, &schema_spec.name);
            if !cascade && items.next().is_some() {
                bail!(
                    "schema '{}.{}' cannot be dropped without CASCADE while it contains objects",
                    database_spec,
                    schema_spec.name
                );
            }
            Ok(Plan::DropSchema {
                database_name: database_spec,
                schema_name: schema_spec.name,
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
        Err(e) => Err(e.into()),
    }
}

fn handle_drop_items(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    names: Vec<ObjectName>,
    cascade: bool,
) -> Result<Plan, anyhow::Error> {
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
            Err(err) => return Err(err.into()),
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
) -> Result<Option<GlobalId>, anyhow::Error> {
    let catalog_entry = scx.catalog.get_item(name);
    if catalog_entry.id().is_system() {
        bail!(
            "cannot drop item {} because it is required by the database system",
            name
        );
    }
    if object_type != catalog_entry.item_type() {
        bail!("{} is not of type {}", name, object_type);
    }
    if !cascade {
        for id in catalog_entry.used_by() {
            let dep = scx.catalog.get_item_by_id(id);
            match dep.item_type() {
                CatalogItemType::Table
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
            }
        }
    }
    Ok(Some(catalog_entry.id()))
}

fn handle_insert(
    scx: &StatementContext,
    InsertStatement {
        table_name,
        columns,
        source,
    }: InsertStatement,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let (id, mut expr) = query::plan_insert_query(scx, table_name, columns, source)?;
    expr.bind_parameters(&params)?;
    let expr = expr.decorrelate();

    Ok(Plan::Insert { id, values: expr })
}

fn handle_select(
    scx: &StatementContext,
    SelectStatement { query, as_of }: SelectStatement,
    params: &Params,
    copy_to: Option<CopyFormat>,
) -> Result<Plan, anyhow::Error> {
    let (relation_expr, _, finishing) = handle_query(scx, query, params, QueryLifetime::OneShot)?;
    let when = match as_of.map(|e| query::eval_as_of(scx, e)).transpose()? {
        Some(ts) => PeekWhen::AtTimestamp(ts),
        None => PeekWhen::Immediately,
    };

    Ok(Plan::Peek {
        source: relation_expr,
        when,
        finishing,
        copy_to,
    })
}

fn handle_explain(
    scx: &StatementContext,
    ExplainStatement {
        stage,
        explainee,
        options,
    }: ExplainStatement,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    let is_view = matches!(explainee, Explainee::View(_));
    let (scx, query) = match explainee {
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
            let parsed = crate::parse::parse(entry.create_sql())
                .expect("Sql for existing view should be valid sql");
            let query = match parsed.into_last() {
                Statement::CreateView(CreateViewStatement { query, .. }) => query,
                _ => panic!("Sql for existing view should parse as a view"),
            };
            let scx = StatementContext {
                pcx: entry.plan_cx(),
                catalog: scx.catalog,
                param_types: scx.param_types.clone(),
            };
            (scx, query)
        }
        Explainee::Query(query) => (scx.clone(), query),
    };
    // Previouly we would bail here for ORDER BY and LIMIT; this has been relaxed to silently
    // report the plan without the ORDER BY and LIMIT decorations (which are done in post).
    let (mut sql_expr, desc, finishing) =
        query::plan_root_query(&scx, query, QueryLifetime::OneShot)?;
    let finishing = if is_view {
        // views don't use a separate finishing
        sql_expr.finish(finishing);
        None
    } else if finishing.is_trivial(desc.arity()) {
        None
    } else {
        Some(finishing)
    };
    sql_expr.bind_parameters(&params)?;
    let expr = sql_expr.clone().decorrelate();
    Ok(Plan::ExplainPlan {
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
) -> Result<(::expr::RelationExpr, RelationDesc, RowSetFinishing), anyhow::Error> {
    let (mut expr, desc, finishing) = query::plan_root_query(scx, query, lifetime)?;
    expr.bind_parameters(&params)?;
    Ok((expr.decorrelate(), desc, finishing))
}

/// Whether a SQL object type can be interpreted as matching the type of the given catalog item.
/// For example, if `v` is a view, `DROP SOURCE v` should not work, since Source and View
/// are non-matching types.
///
/// For now tables are treated as a special kind of source in Materialize, so just
/// allow `TABLE` to refer to either.
impl PartialEq<ObjectType> for CatalogItemType {
    fn eq(&self, other: &ObjectType) -> bool {
        match (self, other) {
            (CatalogItemType::Source, ObjectType::Source)
            | (CatalogItemType::Table, ObjectType::Table)
            | (CatalogItemType::Sink, ObjectType::Sink)
            | (CatalogItemType::View, ObjectType::View)
            | (CatalogItemType::Index, ObjectType::Index)
            | (CatalogItemType::Type, ObjectType::Type) => true,
            (_, _) => false,
        }
    }
}

impl PartialEq<CatalogItemType> for ObjectType {
    fn eq(&self, other: &CatalogItemType) -> bool {
        other == self
    }
}

/// Immutable state that applies to the planning of an entire `Statement`.
#[derive(Debug, Clone)]
pub struct StatementContext<'a> {
    pub pcx: &'a PlanContext,
    pub catalog: &'a dyn Catalog,
    /// The types of the parameters in the query. This is filled in as planning
    /// occurs.
    pub param_types: Rc<RefCell<BTreeMap<usize, ScalarType>>>,
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

    pub fn resolve_default_database(&self) -> Result<(String, i64), PlanError> {
        let name = self.catalog.default_database();
        let id = self.catalog.resolve_database(name)?;
        Ok((name.into(), id))
    }

    pub fn resolve_default_schema(&self) -> Result<SchemaSpecifier, PlanError> {
        Ok(self
            .resolve_schema(ObjectName(vec![Ident::new("public")]))?
            .1)
    }

    pub fn resolve_database(&self, name: ObjectName) -> Result<(String, i64), PlanError> {
        if name.0.len() != 1 {
            return Err(PlanError::OverqualifiedDatabaseName(name.to_string()));
        }
        self.resolve_database_ident(name.0.into_element())
    }

    pub fn resolve_database_ident(&self, name: Ident) -> Result<(String, i64), PlanError> {
        let name = normalize::ident(name);
        let id = self.catalog.resolve_database(&name)?;
        Ok((name, id))
    }

    pub fn resolve_schema(
        &self,
        mut name: ObjectName,
    ) -> Result<(DatabaseSpecifier, SchemaSpecifier), PlanError> {
        if name.0.len() > 2 {
            return Err(PlanError::OverqualifiedSchemaName(name.to_string()));
        }
        let schema_name = normalize::ident(name.0.pop().unwrap());
        let database_spec = name.0.pop().map(normalize::ident);
        Ok(self.catalog.resolve_schema(database_spec, &schema_name)?)
    }

    pub fn resolve_item(&self, name: ObjectName) -> Result<FullName, PlanError> {
        let name = normalize::object_name(name)?;
        Ok(self.catalog.resolve_item(&name)?)
    }

    pub fn experimental_mode(&self) -> bool {
        self.catalog.experimental_mode()
    }

    pub fn require_experimental_mode(&self, feature_name: &str) -> Result<(), anyhow::Error> {
        if !self.experimental_mode() {
            bail!(
                "{} requires experimental mode; see \
                https://materialize.com/docs/cli/#experimental-mode",
                feature_name
            )
        }
        Ok(())
    }

    pub fn finalize_param_types(self) -> Result<Vec<ScalarType>, anyhow::Error> {
        let param_types = Rc::try_unwrap(self.param_types).unwrap().into_inner();
        let mut out = vec![];
        for (i, (n, typ)) in param_types.into_iter().enumerate() {
            if n != i + 1 {
                bail!("unable to infer type for parameter ${}", i + 1);
            }
            out.push(typ);
        }
        Ok(out)
    }
}

macro_rules! with_option_type {
    ($name:ident, String) => {
        if let Some(WithOptionValue::Value(Value::String(value))) = $name {
            value
        } else if let Some(WithOptionValue::ObjectName(name)) = $name {
            name.to_ast_string()
        } else {
            bail!("expected String");
        }
    };
    ($name:ident, bool) => {
        if let Some(WithOptionValue::Value(Value::Boolean(value))) = $name {
            value
        } else if $name.is_none() {
            // Bools, if they have no '= value', are true.
            true
        } else {
            bail!("expected bool");
        }
    };
}

/// This macro accepts a struct definition and will generate it and a `try_from`
/// method that takes a `Vec<WithOption>` which will extract and type check
/// options based on the struct field names and types. Field names must match
/// exactly the lowercased option name. Supported types are:
///
/// - `String`: expects a SQL string (`WITH (name = "value")`) or identifier
///   (`WITH (name = text)`).
/// - `bool`: expects either a SQL bool (`WITH (name = true)`) or a valueless
///   option which will be interpreted as true: (`WITH (name)`.
macro_rules! with_options {
  (struct $name:ident {
        $($field_name:ident: $field_type:ident,)*
    }) => {
        struct $name {
            $($field_name: Option<$field_type>,)*
        }

        impl TryFrom<Vec<WithOption>> for $name {
            type Error = anyhow::Error;

            fn try_from(mut options: Vec<WithOption>) -> Result<Self, Self::Error> {
                let v = Self {
                    $($field_name: {
                        match options.iter().position(|opt| opt.key.as_str() == stringify!($field_name)) {
                            None => None,
                            Some(pos) => {
                                let value: Option<WithOptionValue> = options.swap_remove(pos).value;
                                let value: $field_type = with_option_type!(value, $field_type);
                                Some(value)
                            },
                        }
                    },
                    )*
                };
                if !options.is_empty() {
                    bail!("unexpected options");
                }
                Ok(v)
            }
        }
    }
}

with_options! { struct CopyOptions {
    format: String,
} }

with_options! { struct TailOptions {
    snapshot: bool,
    progress: bool,
} }
