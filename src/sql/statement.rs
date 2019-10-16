// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL `Statement`s are the imperative, side-effecting part of SQL.
//!
//! This module turns SQL `Statement`s into `Plan`s - commands which will drive the dataflow layer

use std::iter::FromIterator;
use std::net::{SocketAddr, ToSocketAddrs};

use failure::{bail, ResultExt};
use sqlparser::ast::{
    Ident, ObjectName, ObjectType, Query, SetVariableValue, ShowStatementFilter, SourceSchema,
    Stage, Statement, Value,
};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser as SqlParser;
use url::Url;

use crate::expr::like::build_like_regex_from_string;
use crate::scope::Scope;
use crate::session::{PreparedStatement, Session};
use crate::store::{DataflowStore, RemoveMode};
use crate::{extract_sql_object_name, Plan, Planner};
use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    Dataflow, KafkaSinkConnector, KafkaSourceConnector, PeekWhen, RowSetFinishing, Sink,
    SinkConnector, Source, SourceConnector, View,
};
use expr::{ColumnOrder, RelationExpr};
use interchange::avro;
use ore::collections::CollectionExt;
use ore::option::OptionExt;
use repr::{Datum, RelationDesc, RelationType, ScalarType};

impl Planner {
    pub fn new(logging_config: Option<&LoggingConfig>) -> Planner {
        Planner {
            dataflows: DataflowStore::new(logging_config),
        }
    }

    /// Parses and plans several raw SQL queries.
    pub fn handle_commands(
        &mut self,
        session: &mut Session,
        sql: String,
    ) -> Result<Vec<Plan>, failure::Error> {
        let stmts = SqlParser::parse_sql(&AnsiDialect {}, sql)?;
        stmts
            .into_iter()
            .map(|stmt| self.handle_statement(session, stmt))
            .collect::<Result<_, _>>()
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

    /// Convert some raw_sql into a parsed statement, and associate it with the current Session
    pub fn handle_parse_command(
        &mut self,
        session: &mut Session,
        sql: String,
        name: String,
    ) -> Result<Plan, failure::Error> {
        let stmt = SqlParser::parse_sql(&AnsiDialect {}, sql.clone())?;
        if stmt.len() != 1 {
            bail!("cannot parse zero or multiple queries: {}", sql);
        }
        self.handle_parse_statement(session, stmt.into_element(), name, sql)
    }

    pub fn handle_execute_command(
        &mut self,
        session: &Session,
        portal_name: &str,
    ) -> Result<Plan, failure::Error> {
        let portal = session
            .get_portal(portal_name)
            .ok_or_else(|| failure::format_err!("portal does not exist {:?}", portal_name))?;
        let prepared = session
            .get_prepared_statement(&portal.statement_name)
            .ok_or_else(|| {
                failure::format_err!(
                    "statement for portal does not exist portal={:?} statement={:?}",
                    portal_name,
                    portal.statement_name
                )
            })?;
        Ok(Plan::Peek {
            source: prepared.source().clone(),
            desc: prepared.desc().clone(),
            when: PeekWhen::Immediately,
            finishing: prepared.finishing().clone(),
        })
    }

    /// Dispatch from arbitrary [`sqlparser::ast::Statement`]s to specific handle commands
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
                desc: RelationDesc::empty()
                    .add_column("name", ScalarType::String)
                    .add_column("setting", ScalarType::String)
                    .add_column("description", ScalarType::String),
                rows: session
                    .vars()
                    .iter()
                    .map(|v| vec![v.name().into(), v.value().into(), v.description().into()])
                    .collect(),
            })
        } else {
            let variable = session.get(&variable)?;
            Ok(Plan::SendRows {
                desc: RelationDesc::empty().add_column(variable.name(), ScalarType::String),
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
            desc: RelationDesc::empty()
                .add_column(object_type_as_plural_str(object_type), ScalarType::String),
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
            .get_desc(&table_name.to_string())?
            .iter()
            .map(|(name, typ)| {
                vec![
                    name.mz_as_deref().unwrap_or("?").into(),
                    if typ.nullable { "YES" } else { "NO" }.into(),
                    typ.scalar_type.to_string().into(),
                ]
            })
            .collect();

        Ok(Plan::SendRows {
            desc: RelationDesc::empty()
                .add_column("Field", ScalarType::String)
                .add_column("Nullable", ScalarType::String)
                .add_column("Type", ScalarType::String),
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
                let (mut relation_expr, mut desc, finishing) = self.plan_toplevel_query(&query)?;
                if !finishing.is_trivial() {
                    //TODO: materialize#724 - persist finishing information with the view?
                    relation_expr = RelationExpr::Project {
                        input: Box::new(RelationExpr::TopK {
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
                        desc.set_name(i, Some(name.into()));
                    }
                }
                let view = View {
                    name: extract_sql_object_name(name)?,
                    relation_expr,
                    desc,
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
                    from: (from, dataflow.desc().clone()),
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
            desc: dataflow.desc().clone(),
            when: if immediate {
                PeekWhen::Immediately
            } else {
                PeekWhen::EarliestSource
            },
            finishing: RowSetFinishing {
                filter: vec![],
                offset: 0,
                limit: None,
                order_by: (0..typ.column_types.len())
                    .map(|column| ColumnOrder {
                        column,
                        desc: false,
                    })
                    .collect(),
                project: (0..typ.column_types.len()).collect(),
            },
        })
    }

    pub fn handle_select(&mut self, query: Query) -> Result<Plan, failure::Error> {
        let (relation_expr, desc, finishing) = self.plan_toplevel_query(&query)?;
        Ok(Plan::Peek {
            source: relation_expr,
            desc,
            when: PeekWhen::Immediately,
            finishing,
        })
    }

    /// Convert a parse statement into a [`Plan::PreparedStatement`] and put it in the Session
    fn handle_parse_statement(
        &mut self,
        session: &mut Session,
        mut stmt: Statement,
        name: String,
        sql: String,
    ) -> Result<Plan, failure::Error> {
        super::transform::transform(&mut stmt);
        match stmt {
            Statement::Query(query) => {
                let (relation_expr, desc, finishing) = self.plan_toplevel_query(&query)?;
                session.set_prepared_statement(
                    name.clone(),
                    PreparedStatement::new(sql, relation_expr, desc, finishing),
                );
                Ok(Plan::Parsed { name })
            }
            _ => bail!("PARSE unsupported for sql statement: {:?}", sql),
        }
    }

    pub fn handle_explain(&mut self, stage: Stage, query: Query) -> Result<Plan, failure::Error> {
        let (relation_expr, _desc, _finishing) = self.plan_toplevel_query(&query)?;
        // Previouly we would bail here for ORDER BY and LIMIT; this has been relaxed to silently
        // report the plan without the ORDER BY and LIMIT decorations (which are done in post).
        if stage == Stage::Dataflow {
            Ok(Plan::SendRows {
                desc: RelationDesc::empty().add_column("Dataflow", ScalarType::String),
                rows: vec![vec![Datum::from(relation_expr.pretty())]],
            })
        } else {
            Ok(Plan::ExplainPlan {
                desc: RelationDesc::empty().add_column("Dataflow", ScalarType::String),
                relation_expr,
            })
        }
    }

    /// Plans and decorrelates a query once we've planned all nested RelationExprs.
    /// Decorrelation converts a sql::RelationExpr (which can include correlations)
    /// to an expr::RelationExpr (which cannot include correlations).
    ///
    /// Note that the returned `RelationDesc` describes the expression after
    /// applying the returned `RowSetFinishing`.
    fn plan_toplevel_query(
        &mut self,
        query: &Query,
    ) -> Result<(RelationExpr, RelationDesc, RowSetFinishing), failure::Error> {
        let (expr, scope, finishing) =
            self.plan_query(query, &Scope::empty(None), &RelationType::empty())?;
        let expr = expr.decorrelate()?;
        let typ = expr.typ();
        let typ = RelationType::new(
            finishing
                .project
                .iter()
                .map(|i| typ.column_types[*i])
                .collect(),
        );
        let desc = RelationDesc::new(typ, scope.column_names());
        Ok((expr, desc, finishing))
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

    let mut desc = avro::validate_value_schema(&value_schema)?;
    if let Some(key_schema) = key_schema {
        let keys = avro::validate_key_schema(&key_schema, &desc)?;
        desc = desc.add_keys(keys);
    }

    Ok(Source {
        name,
        connector: SourceConnector::Kafka(KafkaSourceConnector {
            addr: kafka_addr,
            topic,
            raw_schema: value_schema,
            schema_registry_url,
        }),
        desc,
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
