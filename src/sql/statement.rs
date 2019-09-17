// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL `Statement`s are the imperative, side-effecting part of SQL.
//! This module turns SQL `Statement`s into `Plan`s - commands which will drive the dataflow layer

use super::expr::like::build_like_regex_from_string;
use super::scope::Scope;
use super::session::Session;
use super::store::{DataflowStore, RemoveMode};
use super::{extract_sql_object_name, Plan, Planner};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    ColumnOrder, Dataflow, KafkaSinkConnector, KafkaSourceConnector, PeekWhen, RowSetFinishing,
    Sink, SinkConnector, Source, SourceConnector, View,
};
use failure::{bail, ResultExt};
use interchange::avro;
use ore::collections::CollectionExt;
use ore::option::OptionExt;
use repr::{ColumnType, Datum, RelationType, ScalarType};
use sqlparser::ast::{
    Ident, ObjectName, ObjectType, Query, SetVariableValue, ShowStatementFilter, SourceSchema,
    Stage, Statement, Value,
};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser as SqlParser;
use std::iter::FromIterator;
use std::net::{SocketAddr, ToSocketAddrs};
use url::Url;

impl Planner {
    pub fn new(logging_config: Option<&LoggingConfig>) -> Planner {
        Planner {
            dataflows: DataflowStore::new(logging_config),
        }
    }

    /// Parses and plans a raw SQL query. See the documentation for
    /// [`Result<Plan, failure::Error>`] for details about the meaning of the return type.
    pub fn handle_command(
        &mut self,
        session: &mut Session,
        sql: String,
    ) -> Result<Plan, failure::Error> {
        let stmts = SqlParser::parse_sql(&AnsiDialect {}, sql)?;
        match stmts.len() {
            0 => Ok(Plan::EmptyQuery),
            1 => self.handle_statement(session, stmts.into_element()),
            _ => bail!("expected one statement, but got {}", stmts.len()),
        }
    }

    fn handle_statement(
        &mut self,
        session: &mut Session,
        mut stmt: Statement,
    ) -> Result<Plan, failure::Error> {
        super::transform::transform(&mut stmt);
        match stmt {
            Statement::Peek { name, immediate } => self.handle_peek(name, immediate),
            Statement::Tail { name } => self.handle_tail(name),
            Statement::CreateSource { .. }
            | Statement::CreateSink { .. }
            | Statement::CreateView { .. }
            | Statement::CreateSources { .. } => self.handle_create_dataflow(stmt),
            Statement::Drop { .. } => self.handle_drop_dataflow(stmt),
            Statement::Query(query) => self.handle_select(*query),
            Statement::SetVariable {
                local,
                variable,
                value,
            } => self.handle_set_variable(session, local, variable, value),
            Statement::ShowVariable { variable } => self.handle_show_variable(session, variable),
            Statement::ShowObjects { object_type: ot } => self.handle_show_objects(ot),
            Statement::ShowColumns {
                extended,
                full,
                table_name,
                filter,
            } => self.handle_show_columns(extended, full, &table_name, filter.as_ref()),
            Statement::Explain { stage, query } => self.handle_explain(stage, *query),

            _ => bail!("unsupported SQL statement: {:?}", stmt),
        }
    }

    fn handle_set_variable(
        &mut self,
        session: &mut Session,
        local: bool,
        variable: Ident,
        value: SetVariableValue,
    ) -> Result<Plan, failure::Error> {
        if local {
            bail!("SET LOCAL ... is not supported");
        }
        session.set(
            &variable,
            &match value {
                SetVariableValue::Literal(Value::SingleQuotedString(s)) => s,
                SetVariableValue::Literal(lit) => lit.to_string(),
                SetVariableValue::Ident(ident) => ident,
            },
        )?;
        Ok(Plan::DidSetVariable)
    }

    fn handle_show_variable(
        &mut self,
        session: &Session,
        variable: Ident,
    ) -> Result<Plan, failure::Error> {
        if variable == unicase::Ascii::new("ALL") {
            Ok(Plan::SendRows {
                typ: RelationType {
                    column_types: vec![
                        ColumnType::new(ScalarType::String).name("name"),
                        ColumnType::new(ScalarType::String).name("setting"),
                        ColumnType::new(ScalarType::String).name("description"),
                    ],
                },
                rows: session
                    .vars()
                    .iter()
                    .map(|v| vec![v.name().into(), v.value().into(), v.description().into()])
                    .collect(),
            })
        } else {
            let variable = session.get(&variable)?;
            Ok(Plan::SendRows {
                typ: RelationType {
                    column_types: vec![ColumnType::new(ScalarType::String).name(variable.name())],
                },
                rows: vec![vec![variable.value().into()]],
            })
        }
    }

    fn handle_tail(&mut self, from: ObjectName) -> Result<Plan, failure::Error> {
        let from = extract_sql_object_name(&from)?;
        let dataflow = self.dataflows.get(&from)?;
        Ok(Plan::Tail(dataflow.clone()))
    }

    fn handle_show_objects(&mut self, object_type: ObjectType) -> Result<Plan, failure::Error> {
        let mut rows: Vec<Vec<Datum>> = self
            .dataflows
            .iter()
            .filter(|(_k, v)| object_type_matches(object_type, &v))
            .map(|(k, _v)| vec![Datum::from(k.to_owned())])
            .collect();
        rows.sort_unstable();
        Ok(Plan::SendRows {
            typ: RelationType {
                column_types: vec![ColumnType::new(ScalarType::String)
                    .name(object_type_as_plural_str(object_type).to_owned())],
            },
            rows,
        })
    }

    /// Create an immediate result that describes all the columns for the given table
    fn handle_show_columns(
        &mut self,
        extended: bool,
        full: bool,
        table_name: &ObjectName,
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

        let column_descriptions: Vec<_> = self
            .dataflows
            .get_type(&table_name.to_string())?
            .column_types
            .iter()
            .map(|colty| {
                vec![
                    colty.name.as_deref().unwrap_or("?").into(),
                    if colty.nullable { "YES" } else { "NO" }.into(),
                    colty.scalar_type.to_string().into(),
                ]
            })
            .collect();

        let col_name = |s: &str| ColumnType::new(ScalarType::String).name(s.to_string());
        Ok(Plan::SendRows {
            typ: RelationType {
                column_types: vec![col_name("Field"), col_name("Nullable"), col_name("Type")],
            },
            rows: column_descriptions,
        })
    }

    fn handle_create_dataflow(&mut self, stmt: Statement) -> Result<Plan, failure::Error> {
        match &stmt {
            Statement::CreateView {
                name,
                columns,
                query,
                materialized: _,
                with_options,
            } => {
                if !with_options.is_empty() {
                    bail!("WITH options are not yet supported");
                }
                let (relation_expr, transform) = self.plan_query(query, &Scope::empty(None))?;
                if transform != Default::default() {
                    bail!("ORDER BY and LIMIT are not yet supported in view definitions.");
                }
                let relation_expr = relation_expr.decorrelate()?;
                let mut typ = relation_expr.typ();
                if !columns.is_empty() {
                    if columns.len() != typ.column_types.len() {
                        bail!(
                            "VIEW definition has {} columns, but query has {} columns",
                            columns.len(),
                            typ.column_types.len()
                        )
                    }
                    for (typ, name) in typ.column_types.iter_mut().zip(columns) {
                        typ.name = Some(name.clone());
                    }
                }
                let view = View {
                    name: extract_sql_object_name(name)?,
                    relation_expr,
                    typ,
                    as_of: None,
                };
                self.dataflows.insert(Dataflow::View(view.clone()))?;
                Ok(Plan::CreateView(view))
            }
            Statement::CreateSource {
                name,
                url,
                schema,
                with_options,
            } => {
                if !with_options.is_empty() {
                    bail!("WITH options are not yet supported");
                }
                let name = extract_sql_object_name(name)?;
                let (addr, topic) = parse_kafka_topic_url(url)?;
                let source = build_source(schema, addr, name, topic)?;
                self.dataflows.insert(Dataflow::Source(source.clone()))?;
                Ok(Plan::CreateSource(source))
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
                            let sql_name = sanitize_kafka_topic_name(parts[1]);
                            Some((topic_name, sql_name))
                        } else {
                            None
                        }
                    })
                    .filter(|(_tn, sn)| self.dataflows.try_get(sn).is_none());
                let (addr, topic) = parse_kafka_url(url)?;
                if let Some(s) = topic {
                    bail!(
                        "CREATE SOURCES statement should not take a topic path: {}",
                        s
                    );
                }
                let sources = names
                    .map(|(topic_name, sql_name)| {
                        Ok(build_source(
                            &SourceSchema::Registry(schema_registry.to_owned()),
                            addr,
                            sql_name,
                            topic_name.to_owned(),
                        )?)
                    })
                    .collect::<Result<Vec<_>, failure::Error>>()?;
                for source in &sources {
                    self.dataflows.insert(Dataflow::Source(source.clone()))?;
                }
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
                let name = extract_sql_object_name(name)?;
                let from = extract_sql_object_name(from)?;
                let dataflow = self.dataflows.get(&from)?;
                let (addr, topic) = parse_kafka_topic_url(url)?;
                let sink = Sink {
                    name,
                    from: (from, dataflow.typ().clone()),
                    connector: SinkConnector::Kafka(KafkaSinkConnector {
                        addr,
                        topic,
                        schema_id: 0,
                    }),
                };
                self.dataflows.insert(Dataflow::Sink(sink.clone()))?;
                Ok(Plan::CreateSink(sink))
            }
            other => bail!("Unsupported statement: {:?}", other),
        }
    }

    fn handle_drop_dataflow(&mut self, stmt: Statement) -> Result<Plan, failure::Error> {
        let (object_type, if_exists, names, cascade) = match stmt {
            Statement::Drop {
                object_type,
                if_exists,
                names,
                cascade,
            } => (object_type, if_exists, names, cascade),
            _ => unreachable!(),
        };
        let names: Vec<String> = Result::from_iter(names.iter().map(extract_sql_object_name))?;
        for name in &names {
            match self.dataflows.get(name) {
                Ok(dataflow) => {
                    if !object_type_matches(object_type, dataflow) {
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
        let mode = RemoveMode::from_cascade(cascade);
        let mut removed = vec![];
        for name in &names {
            self.dataflows.remove(name, mode, &mut removed)?;
        }
        let removed = removed.iter().map(|d| d.name().to_owned()).collect();
        Ok(match object_type {
            ObjectType::Source => Plan::DropSources(removed),
            ObjectType::View => Plan::DropViews(removed),
            _ => bail!("unsupported SQL statement: DROP {}", object_type),
        })
    }

    fn handle_peek(&mut self, name: ObjectName, immediate: bool) -> Result<Plan, failure::Error> {
        let name = name.to_string();
        let dataflow = self.dataflows.get(&name)?.clone();
        let typ = dataflow.typ();
        Ok(Plan::Peek {
            source: ::expr::RelationExpr::Get {
                name: dataflow.name().to_owned(),
                typ: typ.clone(),
            },
            when: if immediate {
                PeekWhen::Immediately
            } else {
                PeekWhen::EarliestSource
            },
            transform: RowSetFinishing {
                limit: None,
                order_by: (0..typ.column_types.len())
                    .map(|column| ColumnOrder {
                        column,
                        desc: false,
                    })
                    .collect(),
            },
        })
    }

    pub fn handle_select(&mut self, query: Query) -> Result<Plan, failure::Error> {
        let (relation_expr, transform) = self.plan_query(&query, &Scope::empty(None))?;
        let relation_expr = relation_expr.decorrelate()?;
        Ok(Plan::Peek {
            source: relation_expr,
            when: PeekWhen::Immediately,
            transform,
        })
    }

    pub fn handle_explain(&mut self, stage: Stage, query: Query) -> Result<Plan, failure::Error> {
        let (relation_expr, transform) = self.plan_query(&query, &Scope::empty(None))?;
        let relation_expr = relation_expr.decorrelate()?;
        if transform != Default::default() {
            bail!("Explaining ORDER BY and LIMIT queries is not yet supported.");
        }
        if stage == Stage::Dataflow {
            Ok(Plan::SendRows {
                typ: RelationType {
                    column_types: vec![ColumnType::new(ScalarType::String).name("Dataflow")],
                },
                rows: vec![vec![Datum::from(relation_expr.pretty())]],
            })
        } else {
            Ok(Plan::ExplainPlan {
                typ: RelationType {
                    column_types: vec![ColumnType::new(ScalarType::String).name("Dataflow")],
                },
                relation_expr,
            })
        }
    }
}

fn build_source(
    schema: &SourceSchema,
    kafka_addr: SocketAddr,
    name: String,
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

    let typ = avro::validate_value_schema(&value_schema)?;
    let pkey_indices = match key_schema {
        Some(key_schema) => avro::validate_key_schema(&key_schema, &typ)?,
        None => Vec::new(),
    };

    Ok(Source {
        name,
        connector: SourceConnector::Kafka(KafkaSourceConnector {
            addr: kafka_addr,
            topic,
            raw_schema: value_schema,
            schema_registry_url,
        }),
        typ,
        pkey_indices,
    })
}

fn parse_kafka_url(url: &str) -> Result<(SocketAddr, Option<String>), failure::Error> {
    let url: Url = url.parse()?;
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
    let (addr, topic) = parse_kafka_url(url)?;
    if let Some(topic) = topic {
        Ok((addr, topic))
    } else {
        bail!("source URL missing topic path: {}")
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

/// Whether a SQL object type can be interpreted as matching the type of the given Dataflow.
/// For example, if `v` is a view, `DROP SOURCE v` should not work, since Source and View
/// are non-matching types.
///
/// For now tables are treated as a special kind of source in Materialize, so just
/// allow `TABLE` to refer to either.
fn object_type_matches(object_type: ObjectType, dataflow: &Dataflow) -> bool {
    match dataflow {
        Dataflow::Source { .. } => {
            object_type == ObjectType::Source || object_type == ObjectType::Table
        }
        Dataflow::Sink { .. } => object_type == ObjectType::Sink,
        Dataflow::View { .. } => object_type == ObjectType::View,
    }
}

fn object_type_as_plural_str(object_type: ObjectType) -> &'static str {
    match object_type {
        ObjectType::Table => "TABLES",
        ObjectType::View => "VIEWS",
        ObjectType::Source => "SOURCES",
        ObjectType::Sink => "SINKS",
    }
}
