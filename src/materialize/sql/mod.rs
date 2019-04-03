// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

//! SQL–dataflow translation.

use failure::{bail, format_err};
use futures::{future, Future, Stream};
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::visit;
use sqlparser::sqlast::visit::Visit;
use sqlparser::sqlast::{
    ASTNode, SQLObjectName, SQLOperator, SQLQuery, SQLSelect, SQLSelectItem, SQLSetExpr,
    SQLStatement, TableFactor, Value,
};
use sqlparser::sqlparser::Parser as SQLParser;
use std::sync::{Arc, RwLock};

use self::catalog::{NameResolver, TableCollection};
use crate::dataflow::func::{BinaryFunc, UnaryFunc};
use crate::dataflow::server::{Command, CommandSender};
use crate::dataflow::{Connector, Dataflow, Expr, Plan, Source, View};
use crate::repr::{Datum, FType, Type};
use crate::server::{ConnState, ServerState};
use metastore::MetaStore;
use ore::future::FutureExt;

mod catalog;

pub enum QueryResponse {
    CreatedDataSource,
    CreatedView,
    StreamingRows {
        typ: Type,
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
        let parser = Parser::new(dataflows.into_iter().map(|(n, d)| (n, d.typ().to_owned())));
        let dataflow = match parser.parse_statement(&stmt) {
            Ok(dataflow) => dataflow,
            Err(err) => return future::err(err).left(),
        };
        meta_store
            .create_dataflow(dataflow.name(), &dataflow)
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
                typ: dataflow.typ().to_owned(),
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
    dataflows: TableCollection,
}

#[allow(dead_code)]
impl Parser {
    pub fn new<I>(iter: I) -> Parser
    where
        I: IntoIterator<Item = (String, Type)>,
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
                let (plan, typ) = self.parse_view_query(query)?;
                Ok(Dataflow::View(View {
                    name: self.parse_sql_object_name(name)?,
                    plan,
                    typ,
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
                    typ: crate::interchange::avro::parse_schema(schema)?,
                    raw_schema: schema.clone(),
                }))
            }
            _ => bail!("only CREATE MATERIALIZED VIEW AS allowed"),
        }
    }

    fn parse_view_query(&self, q: &SQLQuery) -> Result<(Plan, Type), failure::Error> {
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

    fn parse_view_select(&self, s: &SQLSelect) -> Result<(Plan, Type), failure::Error> {
        if s.having.is_some() {
            bail!("HAVING is not yet supported");
        } else if s.group_by.is_some() {
            bail!("GROUP BY is not yet supported");
        } else if !s.joins.is_empty() {
            bail!("JOIN is not yet supported");
        }

        let mut nr = NameResolver::new(&self.dataflows);

        // Step 1. Handle FROM clause, including joins.
        let plan = match &s.relation {
            Some(TableFactor::Table { name, .. }) => {
                let name = self.parse_sql_object_name(name)?;
                nr.import_table(&name);
                Plan::Source(name)
            }
            Some(TableFactor::Derived { .. }) => {
                bail!("nested subqueries are not yet supported");
            }
            None => {
                // https://en.wikipedia.org/wiki/DUAL_table
                nr.import_table("$dual");
                Plan::Source("$dual".into())
            }
        };
        // TODO(benesch): joins.

        // Step 2. Handle WHERE clause.
        let plan = match &s.selection {
            Some(expr) => {
                let (expr, typ) = self.parse_expr(expr, &nr)?;
                if typ.ftype != FType::Bool {
                    bail!("WHERE clause must have boolean type, not {:?}", typ.ftype);
                }
                Plan::Filter {
                    predicate: expr,
                    input: Box::new(plan),
                }
            }
            None => plan,
        };

        // Step 3. Handle GROUP BY clause.
        // TODO(benesch)

        // Step 4. Handle HAVING clause.
        // TODO(benesch)

        // Step 5. Handle projections.
        let mut outputs = Vec::new();
        let mut pschema = Vec::new();
        for p in &s.projection {
            let (expr, typ) = self.parse_select_item(p, &nr)?;
            outputs.push(expr);
            pschema.push(typ);
        }

        let plan = Plan::Project {
            outputs,
            input: Box::new(plan),
        };

        Ok((
            plan,
            Type {
                name: None,
                nullable: false,
                ftype: FType::Tuple(pschema),
            },
        ))
    }

    fn parse_select_item<'a>(
        &self,
        s: &'a SQLSelectItem,
        nr: &NameResolver,
    ) -> Result<(Expr, Type), failure::Error> {
        match s {
            SQLSelectItem::UnnamedExpression(e) => self.parse_expr(e, nr),
            _ => bail!(
                "complicated select items are not yet supported: {}",
                s.to_string()
            ),
        }
    }

    fn parse_expr<'a>(
        &self,
        e: &'a ASTNode,
        nr: &NameResolver,
    ) -> Result<(Expr, Type), failure::Error> {
        match e {
            ASTNode::SQLIdentifier(name) => {
                let (i, typ) = nr.resolve_column(name)?;
                let expr = Expr::Column(i, Box::new(Expr::Ambient));
                Ok((expr, typ))
            }
            ASTNode::SQLValue(val) => self.parse_literal(val),
            // TODO(benesch): why isn't IS [NOT] NULL a unary op?
            ASTNode::SQLIsNull(expr) => self.parse_is_null_expr(expr, false, nr),
            ASTNode::SQLIsNotNull(expr) => self.parse_is_null_expr(expr, true, nr),
            // TODO(benesch): "SQLUnary" but "SQLBinaryExpr"?
            ASTNode::SQLUnary { operator, expr } => self.parse_unary_expr(operator, expr, nr),
            ASTNode::SQLBinaryExpr { op, left, right } => {
                self.parse_binary_expr(op, left, right, nr)
            }
            ASTNode::SQLNested(expr) => self.parse_expr(expr, nr),
            _ => bail!(
                "complicated expressions are not yet supported: {}",
                e.to_string()
            ),
        }
    }

    fn parse_is_null_expr<'a>(
        &self,
        inner: &'a ASTNode,
        not: bool,
        nr: &NameResolver,
    ) -> Result<(Expr, Type), failure::Error> {
        let (expr, _) = self.parse_expr(inner, nr)?;
        let mut expr = Expr::CallUnary {
            func: UnaryFunc::IsNull,
            expr: Box::new(expr),
        };
        if not {
            expr = Expr::CallUnary {
                func: UnaryFunc::Not,
                expr: Box::new(expr),
            }
        }
        let typ = Type {
            name: None,
            nullable: false,
            ftype: FType::Bool,
        };
        Ok((expr, typ))
    }

    fn parse_unary_expr<'a>(
        &self,
        op: &'a SQLOperator,
        expr: &'a ASTNode,
        nr: &NameResolver,
    ) -> Result<(Expr, Type), failure::Error> {
        let (expr, typ) = self.parse_expr(expr, nr)?;
        let (func, ftype) = match op {
            SQLOperator::Not => (UnaryFunc::Not, FType::Bool),
            SQLOperator::Plus => return Ok((expr, typ)), // no-op
            SQLOperator::Minus => match typ.ftype {
                FType::Int32 => (UnaryFunc::NegInt32, FType::Int32),
                FType::Int64 => (UnaryFunc::NegInt64, FType::Int64),
                FType::Float32 => (UnaryFunc::NegFloat32, FType::Float32),
                FType::Float64 => (UnaryFunc::NegFloat64, FType::Float64),
                _ => bail!("cannot negate {:?}", typ.ftype),
            },
            // These are the only unary operators.
            //
            // TODO(benesch): SQLOperator should be split into UnarySQLOperator
            // and BinarySQLOperator so that the compiler can check
            // exhaustiveness.
            _ => unreachable!(),
        };
        let expr = Expr::CallUnary {
            func,
            expr: Box::new(expr),
        };
        let typ = Type {
            name: None,
            nullable: typ.nullable,
            ftype,
        };
        Ok((expr, typ))
    }

    fn parse_binary_expr<'a>(
        &self,
        op: &'a SQLOperator,
        left: &'a ASTNode,
        right: &'a ASTNode,
        nr: &NameResolver,
    ) -> Result<(Expr, Type), failure::Error> {
        let (lexpr, ltype) = self.parse_expr(left, nr)?;
        let (rexpr, rtype) = self.parse_expr(right, nr)?;
        let (func, ftype) = match op {
            SQLOperator::Plus => match (&ltype.ftype, &rtype.ftype) {
                (FType::Int32, FType::Int32) => (BinaryFunc::AddInt32, FType::Int32),
                (FType::Int64, FType::Int64) => (BinaryFunc::AddInt64, FType::Int64),
                (FType::Float32, FType::Float32) => (BinaryFunc::AddFloat32, FType::Float32),
                (FType::Float64, FType::Float64) => (BinaryFunc::AddFloat64, FType::Float64),
                _ => bail!("no overload for {:?} + {:?}", ltype.ftype, rtype.ftype),
            },
            SQLOperator::Minus => match (&ltype.ftype, &rtype.ftype) {
                (FType::Int32, FType::Int32) => (BinaryFunc::SubInt32, FType::Int32),
                (FType::Int64, FType::Int64) => (BinaryFunc::SubInt64, FType::Int64),
                (FType::Float32, FType::Float32) => (BinaryFunc::SubFloat32, FType::Float32),
                (FType::Float64, FType::Float64) => (BinaryFunc::SubFloat64, FType::Float64),
                _ => bail!("no overload for {:?} - {:?}", ltype.ftype, rtype.ftype),
            },
            SQLOperator::Multiply => match (&ltype.ftype, &rtype.ftype) {
                (FType::Int32, FType::Int32) => (BinaryFunc::MulInt32, FType::Int32),
                (FType::Int64, FType::Int64) => (BinaryFunc::MulInt64, FType::Int64),
                (FType::Float32, FType::Float32) => (BinaryFunc::MulFloat32, FType::Float32),
                (FType::Float64, FType::Float64) => (BinaryFunc::MulFloat64, FType::Float64),
                _ => bail!("no overload for {:?} - {:?}", ltype.ftype, rtype.ftype),
            },
            SQLOperator::Divide => match (&ltype.ftype, &rtype.ftype) {
                (FType::Int32, FType::Int32) => (BinaryFunc::DivInt32, FType::Int32),
                (FType::Int64, FType::Int64) => (BinaryFunc::DivInt64, FType::Int64),
                (FType::Float32, FType::Float32) => (BinaryFunc::DivFloat32, FType::Float32),
                (FType::Float64, FType::Float64) => (BinaryFunc::DivFloat64, FType::Float64),
                _ => bail!("no overload for {:?} - {:?}", ltype.ftype, rtype.ftype),
            },
            SQLOperator::Modulus => match (&ltype.ftype, &rtype.ftype) {
                (FType::Int32, FType::Int32) => (BinaryFunc::ModInt32, FType::Int32),
                (FType::Int64, FType::Int64) => (BinaryFunc::ModInt64, FType::Int64),
                (FType::Float32, FType::Float32) => (BinaryFunc::ModFloat32, FType::Float32),
                (FType::Float64, FType::Float64) => (BinaryFunc::ModFloat64, FType::Float64),
                _ => bail!("no overload for {:?} - {:?}", ltype.ftype, rtype.ftype),
            },
            SQLOperator::Lt | SQLOperator::LtEq | SQLOperator::Gt | SQLOperator::GtEq => {
                if ltype.ftype != rtype.ftype {
                    bail!("{:?} and {:?} are not comparable", ltype.ftype, rtype.ftype)
                }
                let func = match op {
                    SQLOperator::Lt => BinaryFunc::Lt,
                    SQLOperator::LtEq => BinaryFunc::Lte,
                    SQLOperator::Gt => BinaryFunc::Gt,
                    SQLOperator::GtEq => BinaryFunc::Gte,
                    _ => unreachable!(),
                };
                (func, FType::Bool)
            }
            _ => unimplemented!(),
        };
        let expr = Expr::CallBinary {
            func,
            expr1: Box::new(lexpr),
            expr2: Box::new(rexpr),
        };
        let typ = Type {
            name: None,
            nullable: ltype.nullable || rtype.nullable,
            ftype,
        };
        Ok((expr, typ))
    }

    fn parse_literal<'a>(&self, l: &'a Value) -> Result<(Expr, Type), failure::Error> {
        let (datum, ftype) = match l {
            Value::Long(i) => (Datum::Int64(*i), FType::Int64),
            Value::Double(f) => (Datum::Float64((*f).into()), FType::Float64),
            Value::SingleQuotedString(s) => (Datum::String(s.clone()), FType::String),
            Value::NationalStringLiteral(_) => {
                bail!("n'' string literals are not supported: {}", l.to_string())
            }
            Value::Boolean(b) => match b {
                false => (Datum::False, FType::Bool),
                true => (Datum::True, FType::Bool),
            },
            Value::Null => (Datum::Null, FType::Null),
            Value::Date(_) | Value::DateTime(_) | Value::Timestamp(_) | Value::Time(_) => {
                bail!("date/time types are not yet supported: {}", l.to_string())
            }
            Value::Uuid(_) => bail!("uuid types are not yet supported: {}", l.to_string()),
        };
        let nullable = datum == Datum::Null;
        let expr = Expr::Literal(datum);
        let typ = Type {
            name: None,
            nullable,
            ftype,
        };
        Ok((expr, typ))
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
        let typ = Type {
            name: None,
            nullable: false,
            ftype: FType::Tuple(vec![
                Type {
                    name: None,
                    nullable: false,
                    ftype: FType::Int64,
                },
                Type {
                    name: Some("a".into()),
                    nullable: false,
                    ftype: FType::String,
                },
                Type {
                    name: Some("b".into()),
                    nullable: false,
                    ftype: FType::String,
                },
            ]),
        };
        let parser = Parser::new(vec![("src".into(), typ)]);

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
                    outputs: vec![Expr::Column(2, Box::new(Expr::Ambient))],
                    input: Box::new(Plan::Source("src".into())),
                },
                typ: Type {
                    name: None,
                    nullable: false,
                    ftype: FType::Tuple(vec![Type {
                        name: Some("b".into()),
                        nullable: false,
                        ftype: FType::String
                    },]),
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
            format!(
                "CREATE DATA SOURCE s FROM 'kafka://127.0.0.1/topic' USING SCHEMA '{}'",
                raw_schema
            ),
        )?;
        let dataflow = parser.parse_statement(&stmts[0])?;
        assert_eq!(
            dataflow,
            Dataflow::Source(Source {
                name: "s".into(),
                connector: Connector::Kafka {
                    addr: "127.0.0.1:9092".parse()?,
                    topic: "topic".into(),
                },
                typ: Type {
                    name: None,
                    nullable: false,
                    ftype: FType::Tuple(vec![
                        Type {
                            name: Some("a".into()),
                            nullable: false,
                            ftype: FType::Int64
                        },
                        Type {
                            name: Some("b".into()),
                            nullable: false,
                            ftype: FType::String
                        },
                    ]),
                },
                raw_schema: raw_schema.to_owned(),
            })
        );

        Ok(())
    }
}
