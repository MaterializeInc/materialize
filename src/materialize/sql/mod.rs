// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL-dataflow translation.

use failure::{bail, format_err};
use futures::{future, Future, Stream};
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::visit;
use sqlparser::sqlast::visit::Visit;
use sqlparser::sqlast::{
    ASTNode, JoinConstraint, JoinOperator, SQLIdent, SQLObjectName, SQLOperator, SQLQuery,
    SQLSelect, SQLSelectItem, SQLSetExpr, SQLSetOperator, SQLStatement, TableFactor, Value,
};
use sqlparser::sqlparser::Parser as SQLParser;
use std::sync::{Arc, RwLock};

use self::catalog::{NameResolver, Side, TableCollection};
use crate::dataflow::func::{AggregateFunc, BinaryFunc, UnaryFunc};
use crate::dataflow::server::{Command, CommandSender};
use crate::dataflow::{Aggregate, Connector, Dataflow, Expr, Plan, Source, View};
use crate::repr::{Datum, FType, Type};
use crate::server::{ConnState, ServerState};
use metastore::MetaStore;
use ore::future::FutureExt;

mod catalog;

pub enum QueryResponse {
    CreatedDataSource,
    CreatedView,
    DroppedDataSource,
    DroppedView,
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
            handle_peek(name, cmd_tx, server_state, meta_store)
                .left()
                .left()
        }
        SQLStatement::SQLTail { .. } => unimplemented!(),
        SQLStatement::SQLCreateDataSource { .. } | SQLStatement::SQLCreateView { .. } => {
            handle_create_dataflow(stmt, meta_store).left().right()
        }
        SQLStatement::SQLDropDataSource { .. } | SQLStatement::SQLDropView { .. } => {
            handle_drop_dataflow(stmt, meta_store).right().left()
        }
        _ => future::err(format_err!("unsupported SQL query: {:?}", stmt))
            .right()
            .right(),
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

fn handle_drop_dataflow(
    stmt: SQLStatement,
    meta_store: MetaStore<Dataflow>,
) -> impl Future<Item = QueryResponse, Error = failure::Error> {
    let (response, object_name) = match stmt {
        SQLStatement::SQLDropDataSource { name, .. } => (QueryResponse::DroppedDataSource, name),
        SQLStatement::SQLDropView { name, .. } => (QueryResponse::DroppedView, name),
        _ => unreachable!(),
    };
    future::lazy(move || extract_sql_object_name(&object_name))
        .and_then(move |name| meta_store.delete_dataflow(&name))
        .map(move |_| response)
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

struct AggregateFragment {
    name: String,
    id: *const SQLIdent,
    expr: ASTNode,
}

struct AggregateFuncVisitor {
    aggs: Vec<AggregateFragment>,
    within: bool,
    err: Option<failure::Error>,
}

impl AggregateFuncVisitor {
    fn new() -> AggregateFuncVisitor {
        AggregateFuncVisitor {
            aggs: Vec::new(),
            within: false,
            err: None,
        }
    }

    fn into_result(self) -> Result<Vec<AggregateFragment>, failure::Error> {
        match self.err {
            Some(err) => Err(err),
            None => Ok(self.aggs),
        }
    }
}

impl<'ast> Visit<'ast> for AggregateFuncVisitor {
    fn visit_function(&mut self, ident: &'ast SQLIdent, args: &'ast Vec<ASTNode>) {
        match ident.to_lowercase().as_ref() {
            "avg" | "sum" | "min" | "max" | "count" => {
                if self.within {
                    self.err = Some(format_err!("nested aggregate functions are not allowed"));
                    return;
                }
                if args.len() != 1 {
                    self.err = Some(format_err!("{} function only takes one argument", ident));
                    return;
                }
                self.aggs.push(AggregateFragment {
                    name: ident.to_owned(),
                    id: ident as *const _,
                    expr: args[0].clone(),
                });
            }
            _ => (),
        }
        self.within = true;
        visit::visit_function(self, ident, args);
        self.within = false;
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
                    name: extract_sql_object_name(name)?,
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
                    name: extract_sql_object_name(name)?,
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
        self.parse_set_expr(&q.body)
    }

    fn parse_set_expr(&self, q: &SQLSetExpr) -> Result<(Plan, Type), failure::Error> {
        match q {
            SQLSetExpr::Select(select) => self.parse_view_select(select),
            SQLSetExpr::SetOperation {
                op: SQLSetOperator::Union,
                all,
                left,
                right,
            } => {
                let (left_plan, left_type) = self.parse_set_expr(left)?;
                let (right_plan, right_type) = self.parse_set_expr(right)?;

                let plan = Plan::UnionAll(vec![left_plan, right_plan]);
                let plan = if *all {
                    plan
                } else {
                    Plan::Distinct(Box::new(plan))
                };

                // left and right must have the same number of columns and the same column types
                // column names are taken from left, as in postgres
                let ftype = match (left_type.ftype, right_type.ftype) {
                    (FType::Tuple(left_types), FType::Tuple(right_types)) => {
                        if left_types.len() != right_types.len() {
                            bail!("Each UNION should have the same number of columns: {:?} UNION {:?}", left, right);
                        }
                        for (left_col_type, right_col_type) in
                            left_types.iter().zip(right_types.iter())
                        {
                            if left_col_type.ftype != right_col_type.ftype {
                                bail!(
                                    "Each UNION should have the column types: {:?} UNION {:?}",
                                    left,
                                    right
                                );
                            }
                        }
                        let types = left_types
                            .iter()
                            .zip(right_types.iter())
                            .map(|(left_col_type, right_col_type)| {
                                if left_col_type.ftype != right_col_type.ftype {
                                    bail!(
                                        "Each UNION should have the column types: {:?} UNION {:?}",
                                        left,
                                        right
                                    );
                                } else {
                                    Ok(Type {
                                        name: left_col_type.name.clone(),
                                        nullable: left_col_type.nullable || right_col_type.nullable,
                                        ftype: left_col_type.ftype.clone(),
                                    })
                                }
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        FType::Tuple(types)
                    }
                    (_, _) => panic!(
                        "Union on non-tuple types shouldn't be possible - {:?} UNION {:?}",
                        left, right
                    ),
                };

                Ok((
                    plan,
                    Type {
                        name: None,
                        nullable: false,
                        ftype,
                    },
                ))
            }
            _ => bail!("set operations are not yet supported"),
        }
    }

    fn parse_view_select(&self, s: &SQLSelect) -> Result<(Plan, Type), failure::Error> {
        let mut nr = NameResolver::new(&self.dataflows);

        // Step 1. Handle FROM clause, including joins.
        let mut plan = match &s.relation {
            Some(TableFactor::Table { name, .. }) => {
                let name = extract_sql_object_name(name)?;
                nr.import_table(&name);
                Plan::Source(name)
            }
            Some(TableFactor::Derived { .. }) => {
                bail!("subqueries are not yet supported");
            }
            None => {
                // https://en.wikipedia.org/wiki/DUAL_table
                nr.import_table("$dual");
                Plan::Source("$dual".into())
            }
        };
        for join in &s.joins {
            match &join.relation {
                TableFactor::Table { name, .. } => {
                    let name = extract_sql_object_name(&name)?;
                    nr.import_table(&name);
                    let ((left_key, right_key), (include_left_outer, include_right_outer)) =
                        match &join.join_operator {
                            JoinOperator::Inner(constraint) => {
                                (self.parse_join_constraint(constraint, &nr)?, (false, false))
                            }
                            JoinOperator::LeftOuter(constraint) => {
                                (self.parse_join_constraint(constraint, &nr)?, (true, false))
                            }
                            JoinOperator::RightOuter(constraint) => {
                                (self.parse_join_constraint(constraint, &nr)?, (false, true))
                            }
                            JoinOperator::FullOuter(constraint) => {
                                (self.parse_join_constraint(constraint, &nr)?, (true, true))
                            }
                            JoinOperator::Implicit => {
                                bail!("multiple from tables are not yet supported")
                            }
                            JoinOperator::Cross => {
                                ((Expr::Tuple(vec![]), Expr::Tuple(vec![])), (false, false))
                            }
                        };
                    if include_left_outer {
                        nr.make_nullable(Side::Left);
                    }
                    if include_right_outer {
                        nr.make_nullable(Side::Right);
                    }
                    plan = Plan::Join {
                        left_key,
                        right_key,
                        left: Box::new(plan),
                        right: Box::new(Plan::Source(name)),
                        include_left_outer: if include_left_outer {
                            Some(nr.num_columns(Side::Left))
                        } else {
                            None
                        },
                        include_right_outer: if include_right_outer {
                            Some(nr.num_columns(Side::Right))
                        } else {
                            None
                        },
                    }
                }
                TableFactor::Derived { .. } => {
                    bail!("subqueries are not yet supported");
                }
            }
        }

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
        let mut agg_visitor = AggregateFuncVisitor::new();
        for p in &s.projection {
            agg_visitor.visit_select_item(p);
        }
        let agg_frags = agg_visitor.into_result()?;
        let plan = if !agg_frags.is_empty() {
            let mut aggs = Vec::new();
            for frag in agg_frags {
                let (expr, typ) = self.parse_expr(&frag.expr, &nr)?;
                let func = match (frag.name.as_ref(), &typ.ftype) {
                    ("avg", FType::Int32) => AggregateFunc::AvgInt32,
                    ("avg", FType::Int64) => AggregateFunc::AvgInt64,
                    ("avg", FType::Float32) => AggregateFunc::AvgFloat32,
                    ("avg", FType::Float64) => AggregateFunc::AvgFloat64,
                    ("max", FType::Int32) => AggregateFunc::MaxInt32,
                    ("max", FType::Int64) => AggregateFunc::MaxInt64,
                    ("max", FType::Float32) => AggregateFunc::MaxFloat32,
                    ("max", FType::Float64) => AggregateFunc::MaxFloat64,
                    ("min", FType::Int32) => AggregateFunc::MinInt32,
                    ("min", FType::Int64) => AggregateFunc::MinInt64,
                    ("min", FType::Float32) => AggregateFunc::MinFloat32,
                    ("min", FType::Float64) => AggregateFunc::MinFloat64,
                    ("sum", FType::Int32) => AggregateFunc::SumInt32,
                    ("sum", FType::Int64) => AggregateFunc::SumInt64,
                    ("sum", FType::Float32) => AggregateFunc::SumFloat32,
                    ("sum", FType::Float64) => AggregateFunc::SumFloat64,
                    ("count", _) => AggregateFunc::Count,
                    _ => unimplemented!(),
                };
                aggs.push(Aggregate { func, expr });
                nr.add_func(
                    frag.id,
                    Type {
                        name: None,
                        nullable: true,
                        ftype: typ.ftype,
                    },
                );
            }

            let mut key_exprs = Vec::new();
            let mut retained_columns = Vec::new();
            for expr in &s.group_by {
                let (expr, typ) = self.parse_expr(&expr, &nr)?;
                retained_columns.push(typ);
                key_exprs.push(expr);
            }
            let key_expr = Expr::Tuple(key_exprs);

            nr.reset(retained_columns);
            Plan::Aggregate {
                key: key_expr,
                aggs,
                input: Box::new(plan),
            }
        } else {
            plan
        };

        // Step 4. Handle HAVING clause.
        let plan = match &s.having {
            Some(expr) => {
                let (expr, typ) = self.parse_expr(expr, &nr)?;
                if typ.ftype != FType::Bool {
                    bail!("HAVING clause must have boolean type, not {:?}", typ.ftype);
                }
                Plan::Filter {
                    predicate: expr,
                    input: Box::new(plan),
                }
            }
            None => plan,
        };

        // Step 5. Handle projections.
        let mut outputs = Vec::new();
        let mut proj_type = Vec::new();
        for p in &s.projection {
            let (expr, typ) = self.parse_select_item(p, &nr)?;
            outputs.push(expr);
            proj_type.push(typ);
        }
        let plan = Plan::Project {
            outputs,
            input: Box::new(plan),
        };

        // Step 6. Handle DISTINCT.
        let plan = if s.distinct {
            Plan::Distinct(Box::new(plan))
        } else {
            plan
        };

        Ok((
            plan,
            Type {
                name: None,
                nullable: false,
                ftype: FType::Tuple(proj_type),
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

    fn parse_join_constraint<'a>(
        &self,
        constraint: &'a JoinConstraint,
        nr: &NameResolver,
    ) -> Result<(Expr, Expr), failure::Error> {
        match constraint {
            JoinConstraint::On(expr) => self.parse_join_on_expr(expr, nr),
            JoinConstraint::Natural => bail!("natural joins are not yet supported"),
            JoinConstraint::Using(_) => bail!("using joins are not yet supported"),
        }
    }

    fn parse_join_on_expr<'a>(
        &self,
        expr: &'a ASTNode,
        nr: &NameResolver,
    ) -> Result<(Expr, Expr), failure::Error> {
        fn extract(e: &ASTNode, nr: &NameResolver) -> Result<(usize, Type, Side), failure::Error> {
            match e {
                ASTNode::SQLIdentifier(name) => {
                    let (pos, typ) = nr.resolve_column(name)?;
                    Ok((pos, typ, nr.side(pos)))
                }
                _ => bail!(
                    "ON clause contained unsupported complicated expression: {:?}",
                    e
                ),
            }
        };

        fn work(
            e: &ASTNode,
            left_keys: &mut Vec<Expr>,
            right_keys: &mut Vec<Expr>,
            nr: &NameResolver,
        ) -> Result<(), failure::Error> {
            match unnest(e) {
                ASTNode::SQLBinaryExpr { left, op, right } => match op {
                    SQLOperator::And => {
                        work(left, left_keys, right_keys, nr)?;
                        work(right, left_keys, right_keys, nr)
                    }
                    SQLOperator::Eq => {
                        let (lpos, ltype, lside) = extract(left, nr)?;
                        let (rpos, rtype, rside) = extract(right, nr)?;
                        let (lpos, ltype, rpos, rtype) = match (lside, rside) {
                            (Side::Left, Side::Left) | (Side::Right, Side::Right) => {
                                bail!("ON clause compares two columns from the same table");
                            }
                            (Side::Left, Side::Right) => (lpos, ltype, rpos, rtype),
                            (Side::Right, Side::Left) => (rpos, rtype, lpos, ltype),
                        };
                        if ltype.ftype != rtype.ftype {
                            bail!("cannot compare {:?} and {:?}", ltype.ftype, rtype.ftype);
                        }
                        let lexpr = Expr::Column(lpos, Box::new(Expr::Ambient));
                        let rexpr = Expr::Column(nr.adjust_rhs(rpos), Box::new(Expr::Ambient));
                        left_keys.push(lexpr);
                        right_keys.push(rexpr);
                        Ok(())
                    }
                    _ => bail!("ON clause contained non-equality operator: {:?}", op),
                },
                _ => bail!(
                    "ON clause contained unsupported complicated expression: {:?}",
                    e
                ),
            }
        };

        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        work(expr, &mut left_keys, &mut right_keys, nr)?;

        Ok((Expr::Tuple(left_keys), Expr::Tuple(right_keys)))
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
            ASTNode::SQLFunction { id, args } => self.parse_function(id, args, nr),
            _ => bail!(
                "complicated expressions are not yet supported: {}",
                e.to_string()
            ),
        }
    }

    #[allow(clippy::ptr_arg)]
    fn parse_function<'a>(
        &self,
        ident: &'a SQLIdent,
        _args: &'a [ASTNode],
        nr: &NameResolver,
    ) -> Result<(Expr, Type), failure::Error> {
        let (i, typ) = nr.resolve_func(ident)?;
        let expr = Expr::Column(i, Box::new(Expr::Ambient));
        Ok((expr, typ))
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
}

fn extract_sql_object_name(n: &SQLObjectName) -> Result<String, failure::Error> {
    if n.0.len() != 1 {
        bail!("qualified names are not yet supported: {}", n.to_string())
    }
    Ok(n.to_string())
}

fn unnest(expr: &ASTNode) -> &ASTNode {
    match expr {
        ASTNode::SQLNested(expr) => unnest(expr),
        _ => expr,
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

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

    #[test]
    fn test_basic_sum() -> Result<(), failure::Error> {
        let typ = Type {
            name: None,
            nullable: false,
            ftype: FType::Tuple(vec![
                Type {
                    name: Some("a".into()),
                    nullable: false,
                    ftype: FType::Int64,
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
            "CREATE MATERIALIZED VIEW v AS SELECT 1 + sum(a + 1) FROM src GROUP BY b".into(),
        )?;
        let dataflow = parser.parse_statement(&stmts[0])?;
        assert_eq!(
            dataflow,
            Dataflow::View(View {
                name: "v".into(),
                plan: Plan::Project {
                    outputs: vec![Expr::CallBinary {
                        func: BinaryFunc::AddInt64,
                        expr1: Box::new(Expr::Literal(Datum::Int64(1))),
                        expr2: Box::new(Expr::Column(1, Box::new(Expr::Ambient))),
                    }],
                    input: Box::new(Plan::Aggregate {
                        key: Expr::Tuple(vec![Expr::Column(1, Box::new(Expr::Ambient))]),
                        aggs: vec![Aggregate {
                            func: AggregateFunc::SumInt64,
                            expr: Expr::CallBinary {
                                func: BinaryFunc::AddInt64,
                                expr1: Box::new(Expr::Column(0, Box::new(Expr::Ambient))),
                                expr2: Box::new(Expr::Literal(Datum::Int64(1))),
                            }
                        }],
                        input: Box::new(Plan::Source("src".into())),
                    }),
                },
                typ: Type {
                    name: None,
                    nullable: false,
                    ftype: FType::Tuple(vec![Type {
                        name: None,
                        nullable: true,
                        ftype: FType::Int64,
                    }]),
                }
            })
        );

        Ok(())
    }

    #[test]
    fn test_basic_join() -> Result<(), failure::Error> {
        let src1_type = Type {
            name: None,
            nullable: false,
            ftype: FType::Tuple(vec![
                Type {
                    name: Some("a".into()),
                    nullable: false,
                    ftype: FType::Int64,
                },
                Type {
                    name: Some("b".into()),
                    nullable: false,
                    ftype: FType::Int64,
                },
            ]),
        };
        let src2_type = Type {
            name: None,
            nullable: false,
            ftype: FType::Tuple(vec![
                Type {
                    name: Some("c".into()),
                    nullable: false,
                    ftype: FType::Int64,
                },
                Type {
                    name: Some("d".into()),
                    nullable: false,
                    ftype: FType::Int64,
                },
            ]),
        };
        let parser = Parser::new(vec![("src1".into(), src1_type), ("src2".into(), src2_type)]);

        let stmts = SQLParser::parse_sql(
            &AnsiSqlDialect {},
            "CREATE MATERIALIZED VIEW v AS SELECT a, b, d FROM src1 JOIN src2 ON c = b".into(),
        )?;
        let dataflow = parser.parse_statement(&stmts[0])?;
        assert_eq!(
            dataflow,
            Dataflow::View(View {
                name: "v".into(),
                plan: Plan::Project {
                    outputs: vec![
                        Expr::Column(0, Box::new(Expr::Ambient)),
                        Expr::Column(1, Box::new(Expr::Ambient)),
                        Expr::Column(3, Box::new(Expr::Ambient)),
                    ],
                    input: Box::new(Plan::Join {
                        left_key: Expr::Tuple(vec![Expr::Column(1, Box::new(Expr::Ambient))]),
                        right_key: Expr::Tuple(vec![Expr::Column(0, Box::new(Expr::Ambient))]),
                        left: Box::new(Plan::Source("src1".into())),
                        right: Box::new(Plan::Source("src2".into())),
                        include_left_outer: None,
                        include_right_outer: None,
                    }),
                },
                typ: Type {
                    name: None,
                    nullable: false,
                    ftype: FType::Tuple(vec![
                        Type {
                            name: Some("a".into()),
                            nullable: false,
                            ftype: FType::Int64,
                        },
                        Type {
                            name: Some("b".into()),
                            nullable: false,
                            ftype: FType::Int64,
                        },
                        Type {
                            name: Some("d".into()),
                            nullable: false,
                            ftype: FType::Int64,
                        },
                    ]),
                }
            })
        );

        Ok(())
    }

    #[test]
    fn test_basic_union() -> Result<(), failure::Error> {
        let src1_type = Type {
            name: None,
            nullable: false,
            ftype: FType::Tuple(vec![
                Type {
                    name: Some("a".into()),
                    nullable: false,
                    ftype: FType::Int64,
                },
                Type {
                    name: Some("b".into()),
                    nullable: false,
                    ftype: FType::Int64,
                },
            ]),
        };
        let src2_type = Type {
            name: None,
            nullable: false,
            ftype: FType::Tuple(vec![
                Type {
                    name: Some("a".into()),
                    nullable: false,
                    ftype: FType::Int64,
                },
                Type {
                    name: Some("b".into()),
                    nullable: false,
                    ftype: FType::Int64,
                },
            ]),
        };
        let parser = Parser::new(vec![("src1".into(), src1_type), ("src2".into(), src2_type)]);

        let stmts = SQLParser::parse_sql(
            &AnsiSqlDialect {},
            "CREATE MATERIALIZED VIEW v AS SELECT a, b FROM src1 UNION ALL SELECT a, b FROM src2"
                .into(),
        )?;
        let dataflow = parser.parse_statement(&stmts[0])?;
        assert_eq!(
            dataflow,
            Dataflow::View(View {
                name: "v".into(),
                plan: Plan::UnionAll(vec![
                    Plan::Project {
                        outputs: vec![
                            Expr::Column(0, Box::new(Expr::Ambient)),
                            Expr::Column(1, Box::new(Expr::Ambient)),
                        ],
                        input: Box::new(Plan::Source("src1".into()))
                    },
                    Plan::Project {
                        outputs: vec![
                            Expr::Column(0, Box::new(Expr::Ambient)),
                            Expr::Column(1, Box::new(Expr::Ambient)),
                        ],
                        input: Box::new(Plan::Source("src2".into()))
                    },
                ]),
                typ: Type {
                    name: None,
                    nullable: false,
                    ftype: FType::Tuple(vec![
                        Type {
                            name: Some("a".into()),
                            nullable: false,
                            ftype: FType::Int64,
                        },
                        Type {
                            name: Some("b".into()),
                            nullable: false,
                            ftype: FType::Int64,
                        },
                    ]),
                }
            })
        );

        Ok(())
    }
}
