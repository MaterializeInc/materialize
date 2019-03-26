// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use failure::{bail, format_err};
use futures::{future, Future, Stream};
use lazy_static::lazy_static;
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::visit;
use sqlparser::sqlast::visit::Visit;
use sqlparser::sqlast::{
    ASTNode, SQLObjectName, SQLQuery, SQLSelect, SQLSelectItem, SQLSetExpr, SQLStatement,
    TableFactor, Value,
};
use sqlparser::sqlparser::Parser as SQLParser;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::dataflow::server::{Command, CommandSender};
use crate::dataflow::{Connector, Dataflow, Expr, Plan, Source, View};
use crate::repr::{Datum, Schema, Type};
use crate::server::{ConnState, ServerState};
use metastore::MetaStore;
use ore::future::FutureExt;

lazy_static! {
    static ref DUAL_SCHEMA: Schema = Schema {
        name: Some("dual".into()),
        nullable: false,
        typ: Type::Tuple(vec![Schema {
            name: Some("x".into()),
            nullable: false,
            typ: Type::String,
        }]),
    };
}

pub enum QueryResponse {
    CreatedDataSource,
    CreatedView,
    StreamingRows {
        schema: Schema,
        rows: Box<dyn Stream<Item = Datum, Error = failure::Error> + Send>,
    },
}

pub fn handle_query(
    stmt: String,
    conn_state: &ConnState,
) -> impl Future<Item = QueryResponse, Error = failure::Error> {
    let meta_store = conn_state.meta_store.clone();
    let cmd_tx = conn_state.cmd_tx.clone();
    let server_state = conn_state.server_state.clone();
    future::lazy(|| {
        let mut stmts = SQLParser::parse_sql(&AnsiSqlDialect {}, stmt)?;
        if stmts.len() != 1 {
            bail!("expected one statement, but got {}", stmts.len());
        }
        Ok(stmts.remove(0))
    })
    .and_then(|stmt| match stmt {
        SQLStatement::SQLPeek { mut name } => {
            let name = std::mem::replace(&mut name, SQLObjectName(Vec::new()));
            handle_peek(name, cmd_tx, server_state, meta_store).left()
        }
        SQLStatement::SQLTail { .. } => unimplemented!(),
        SQLStatement::SQLCreateDataSource { .. } | SQLStatement::SQLCreateView { .. } => {
            handle_create_dataflow(stmt, meta_store).right()
        }
        _ => unimplemented!(),
    })
}

fn handle_create_dataflow(
    stmt: SQLStatement,
    meta_store: MetaStore<Dataflow>,
) -> impl Future<Item = QueryResponse, Error = failure::Error> {
    future::lazy(move || {
        let mut visitor = ObjectNameVisitor::new();
        visitor.visit_statement(&stmt);
        visitor
            .into_result()
            .map(|object_names| (stmt, object_names))
    })
    .and_then(|(stmt, object_names)| {
        meta_store
            .read_dataflows(object_names)
            .map(|dataflows| (stmt, dataflows))
            .map(|(stmt, dataflows)| (meta_store, stmt, dataflows))
    })
    .and_then(|(meta_store, stmt, dataflows)| {
        let parser = Parser::new(
            dataflows
                .into_iter()
                .map(|(n, d)| (n, (0, d.schema().to_owned()))),
        );
        let dataflow = match parser.parse_statement(&stmt) {
            Ok(dataflow) => dataflow,
            Err(err) => return future::err(err).left(),
        };
        meta_store
            .new_dataflow(dataflow.name(), &dataflow)
            .map(|_| stmt)
            .right()
    })
    .and_then(|stmt| {
        Ok(match stmt {
            SQLStatement::SQLCreateDataSource { .. } => QueryResponse::CreatedDataSource,
            SQLStatement::SQLCreateView { .. } => QueryResponse::CreatedView,
            _ => unreachable!(),
        })
    })
}

fn handle_peek(
    name: SQLObjectName,
    cmd_tx: CommandSender,
    server_state: Arc<RwLock<ServerState>>,
    meta_store: metastore::MetaStore<Dataflow>,
) -> impl Future<Item = QueryResponse, Error = failure::Error> {
    let name = name.to_string();
    let names = vec![name.clone()];
    meta_store
        .read_dataflows(names)
        .and_then(move |mut dataflows| {
            let dataflow = dataflows.remove(&name).unwrap();
            let uuid = uuid::Uuid::new_v4();
            let (tx, rx) = futures::sync::mpsc::unbounded();
            {
                let mut server_state = server_state.write().unwrap();
                server_state.peek_results.insert(uuid, (tx, 4 /* XXX */));
            }
            cmd_tx.send(Command::Peek(name.to_string(), uuid)).unwrap();
            future::ok(QueryResponse::StreamingRows {
                schema: dataflow.schema().to_owned(),
                rows: Box::new(rx.map_err(|_| format_err!("unreachable"))),
            })
        })
}

struct ObjectNameVisitor {
    recording: bool,
    object_names: Vec<String>,
    err: Option<failure::Error>,
}

impl ObjectNameVisitor {
    pub fn new() -> ObjectNameVisitor {
        ObjectNameVisitor {
            recording: false,
            object_names: Vec::new(),
            err: None,
        }
    }

    pub fn into_result(self) -> Result<Vec<String>, failure::Error> {
        match self.err {
            Some(err) => Err(err),
            None => Ok(self.object_names),
        }
    }
}

impl<'ast> Visit<'ast> for ObjectNameVisitor {
    fn visit_query(&mut self, query: &'ast SQLQuery) {
        self.recording = true;
        visit::visit_query(self, query);
    }

    fn visit_object_name(&mut self, object_name: &'ast SQLObjectName) {
        if self.err.is_some() || !self.recording {
            return;
        }
        if object_name.0.len() != 1 {
            self.err = Some(format_err!(
                "qualified names are not yet supported: {}",
                object_name.to_string()
            ))
        } else {
            self.object_names.push(object_name.0[0].to_owned())
        }
    }
}

#[allow(dead_code)]
pub struct Parser {
    dataflows: HashMap<String, (usize, Schema)>,
}

#[allow(dead_code)]
impl Parser {
    pub fn new<I>(iter: I) -> Parser
    where
        I: IntoIterator<Item = (String, (usize, Schema))>,
    {
        Parser {
            dataflows: iter.into_iter().collect(),
        }
    }

    pub fn parse_statement(&self, stmt: &SQLStatement) -> Result<Dataflow, failure::Error> {
        match stmt {
            SQLStatement::SQLCreateView {
                name,
                query,
                materialized: true,
            } => {
                let (plan, schema) = self.parse_view_query(query)?;
                Ok(Dataflow::View(View {
                    name: self.parse_sql_object_name(name)?,
                    plan,
                    schema,
                }))
            }
            SQLStatement::SQLCreateDataSource { name, url, schema } => {
                use std::net::ToSocketAddrs;

                let url: url::Url = url.parse()?;
                if url.scheme() != "kafka" {
                    bail!("only kafka:// data sources are supported: {}", url);
                } else if !url.has_host() {
                    bail!("data source URL missing hostname: {}", url)
                }
                let topic = match url.path_segments() {
                    None => bail!("data source URL missing topic path: {}"),
                    Some(segments) => {
                        let segments: Vec<_> = segments.collect();
                        if segments.len() != 1 {
                            bail!(
                                "data source URL should have exactly one path segment: {}",
                                url
                            );
                        }
                        segments[0].to_owned()
                    }
                };
                let url = url.with_default_port(|_| Ok(9092))?; // we already checked for kafka scheme above, so safe to assume scheme

                Ok(Dataflow::Source(Source {
                    name: self.parse_sql_object_name(name)?,
                    connector: Connector::Kafka {
                        addr: url.to_socket_addrs()?.next().unwrap(),
                        topic,
                    },
                    schema: crate::interchange::avro::parse_schema(schema)?,
                    raw_schema: schema.clone(),
                }))
            }
            _ => bail!("only CREATE MATERIALIZED VIEW AS allowed"),
        }
    }

    fn parse_view_query(&self, q: &SQLQuery) -> Result<(Plan, Schema), failure::Error> {
        if !q.ctes.is_empty() {
            bail!("CTEs are not yet supported");
        }
        if q.limit.is_some() {
            bail!("LIMIT is not supported in a view definition");
        }
        if q.order_by.is_some() {
            bail!("ORDER BY is not supported in a view definition");
        }
        match &q.body {
            SQLSetExpr::Select(select) => self.parse_view_select(select),
            _ => bail!("set operations are not yet supported"),
        }
    }

    fn parse_view_select(&self, s: &SQLSelect) -> Result<(Plan, Schema), failure::Error> {
        if s.having.is_some() {
            bail!("HAVING is not yet supported");
        } else if s.group_by.is_some() {
            bail!("GROUP BY is not yet supported");
        } else if !s.joins.is_empty() {
            bail!("JOIN is not yet supported");
        }

        let (plan, schema) = match &s.relation {
            Some(TableFactor::Table { name, .. }) => {
                let name = self.parse_sql_object_name(name)?;
                let schema = match self.dataflows.get(&name) {
                    None => bail!("no dataflow named {}", name),
                    Some((_version, schema)) => schema,
                };
                (Plan::Source(name), schema)
            }
            Some(TableFactor::Derived { .. }) => {
                bail!("nested subqueries are not yet supported");
            }
            None => {
                // https://en.wikipedia.org/wiki/DUAL_table
                (Plan::Source("dual".into()), &*DUAL_SCHEMA)
            }
        };

        let mut outputs = Vec::new();
        let mut pschema = Vec::new();
        for p in &s.projection {
            let (name, expr, typ) = self.parse_select_item(p, schema)?;
            outputs.push(expr);
            pschema.push(Schema {
                name: name.map(|s| s.to_owned()),
                nullable: false,
                typ,
            });
        }

        let plan = Plan::Project {
            outputs,
            input: Box::new(plan),
        };

        Ok((
            plan,
            Schema {
                name: None,
                nullable: false,
                typ: Type::Tuple(pschema),
            },
        ))
    }

    fn parse_select_item<'a>(
        &self,
        s: &'a SQLSelectItem,
        schema: &Schema,
    ) -> Result<(Option<&'a str>, Expr, Type), failure::Error> {
        match s {
            SQLSelectItem::UnnamedExpression(e) => self.parse_expr(e, schema),
            _ => bail!(
                "complicated select items are not yet supported: {}",
                s.to_string()
            ),
        }
    }

    fn parse_expr<'a>(
        &self,
        e: &'a ASTNode,
        schema: &Schema,
    ) -> Result<(Option<&'a str>, Expr, Type), failure::Error> {
        match e {
            ASTNode::SQLIdentifier(name) => {
                let i = match &schema.typ {
                    Type::Tuple(t) => t.iter().position(|f| f.name == Some(name.clone())),
                    _ => unimplemented!(),
                };
                let i = match i {
                    Some(i) => i,
                    None => bail!("unknown column {}", name),
                };
                let expr = Expr::Column(i);
                let typ = match &schema.typ {
                    Type::Tuple(t) => t[i].typ.clone(),
                    _ => unreachable!(),
                };
                Ok((Some(name), expr, typ))
            }
            ASTNode::SQLValue(val) => match val {
                Value::Long(i) => Ok((None, Expr::Literal(Datum::Int64(*i)), Type::Int64)),
                Value::SingleQuotedString(s) => Ok((
                    None,
                    Expr::Literal(Datum::String(s.to_string())),
                    Type::String,
                )),
                _ => bail!(
                    "complicated literals are not yet supported: {}",
                    val.to_string()
                ),
            },
            _ => bail!(
                "complicated expressions are not yet supported: {}",
                e.to_string()
            ),
        }
    }

    fn parse_sql_object_name(&self, n: &SQLObjectName) -> Result<String, failure::Error> {
        if n.0.len() != 1 {
            bail!("qualified names are not yet supported: {}", n.to_string())
        }
        Ok(n.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_view() -> Result<(), failure::Error> {
        let schema = Schema {
            name: None,
            nullable: false,
            typ: Type::Tuple(vec![
                Schema { name: None, nullable: false, typ: Type::Int64 },
                Schema { name: Some("a".into()), nullable: false, typ: Type::String },
                Schema { name: Some("b".into()), nullable: false, typ: Type::String },
            ]),
        };
        let version = 1;
        let parser = Parser::new(vec![("src".into(), (version, schema))]);

        let stmts = SQLParser::parse_sql(
            &AnsiSqlDialect {},
            "CREATE MATERIALIZED VIEW v AS SELECT b FROM src".into(),
        )?;
        let dataflow = parser.parse_statement(&stmts[0])?;
        assert_eq!(
            dataflow,
            Dataflow::View(View {
                name: "v".into(),
                plan: Plan::Project {
                    outputs: vec![Expr::Column(2)],
                    input: Box::new(Plan::Source("src".into())),
                },
                schema: Schema {
                    name: None,
                    nullable: false,
                    typ: Type::Tuple(vec![
                        Schema { name: Some("b".into()), nullable: false, typ: Type::String },
                    ]),
                }
            })
        );

        Ok(())
    }

    #[test]
    fn test_basic_source() -> Result<(), failure::Error> {
        let parser = Parser::new(vec![]);

        let raw_schema = r#"{
    "type": "record",
    "name": "foo",
    "fields": [
        {"name": "a", "type": "long", "default": 42},
        {"name": "b", "type": "string"}
    ]
}"#;

        let stmts = SQLParser::parse_sql(
            &AnsiSqlDialect {},
            format!("CREATE DATA SOURCE s FROM 'kafka://localhost/topic' USING SCHEMA '{}'", raw_schema)
        )?;
        let dataflow = parser.parse_statement(&stmts[0])?;
        assert_eq!(
            dataflow,
            Dataflow::Source(Source {
                name: "s".into(),
                connector: Connector::Kafka {
                    addr: "[::1]:9092".parse()?,
                    topic: "topic".into(),
                },
                schema: Schema {
                    name: None,
                    nullable: false,
                    typ: Type::Tuple(vec![
                        Schema { name: Some("a".into()), nullable: false, typ: Type::Int64 },
                        Schema { name: Some("b".into()), nullable: false, typ: Type::String },
                    ]),
                },
                raw_schema: raw_schema.to_owned(),
            })
        );

        Ok(())
    }
}
