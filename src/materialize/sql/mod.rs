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
    SQLSelect, SQLSelectItem, SQLSetExpr, SQLSetOperator, SQLStatement, SQLType, TableFactor,
    Value,
};
use sqlparser::sqlparser::Parser as SQLParser;
use std::collections::{HashMap, HashSet};
use std::iter::FromIterator;
use std::sync::Arc;

use crate::dataflow::func::{AggregateFunc, BinaryFunc, UnaryFunc};
use crate::dataflow::server::{Command, CommandSender};
use crate::dataflow::{
    Aggregate, Connector, Dataflow, Expr, KafkaConnector, LocalConnector, Plan, Source, View,
};
use crate::repr::{Datum, FType, Type};
use crate::server::{ConnState, ServerState};
use metastore::MetaStore;
use ore::future::FutureExt;
use plan::SQLPlan;

mod plan;

pub enum QueryResponse {
    CreatedDataSource,
    CreatedView,
    CreatedTable,
    DroppedDataSource,
    DroppedView,
    DroppedTable,
    Inserted(usize),
    StreamingRows {
        typ: Type,
        rows: Box<dyn Stream<Item = Datum, Error = failure::Error> + Send>,
    },
}

pub fn handle_query(
    stmt: String,
    conn_state: &ConnState,
) -> impl Future<Item = QueryResponse, Error = failure::Error> {
    let ConnState {
        meta_store,
        cmd_tx,
        server_state,
    } = conn_state.clone();
    future::lazy(|| {
        let mut stmts = SQLParser::parse_sql(&AnsiSqlDialect {}, stmt)?;
        if stmts.len() != 1 {
            bail!("expected one statement, but got {}", stmts.len());
        }
        Ok(stmts.remove(0))
    })
    .and_then(|stmt| handle_statement(stmt, meta_store, cmd_tx, server_state))
}

fn handle_statement(
    stmt: SQLStatement,
    meta_store: MetaStore<Dataflow>,
    cmd_tx: crate::dataflow::server::CommandSender,
    server_state: Arc<ServerState>,
) -> Box<dyn Future<Item = QueryResponse, Error = failure::Error> + Send> {
    match stmt {
        SQLStatement::SQLPeek { name } => {
            Box::new(handle_peek(name, meta_store, cmd_tx, server_state))
        }
        SQLStatement::SQLTail { .. } => {
            Box::new(future::err(format_err!("TAIL is not implemented yet")))
        }
        SQLStatement::SQLCreateDataSource { .. }
        | SQLStatement::SQLCreateView { .. }
        | SQLStatement::SQLCreateTable { .. } => Box::new(handle_create_dataflow(stmt, meta_store)),
        SQLStatement::SQLDropDataSource { .. } | SQLStatement::SQLDropView { .. } => {
            Box::new(handle_drop_dataflow(stmt, meta_store))
        }

        // these are intended mostly for testing:
        SQLStatement::SQLSelect(query) => {
            Box::new(handle_select(query, meta_store, cmd_tx, server_state))
        }
        // SQLStatement::DropTable{name, columns} => {}
        SQLStatement::SQLInsert {
            table_name,
            columns,
            values,
        } => Box::new(handle_insert(table_name, columns, values, meta_store)),
        _ => Box::new(future::err(format_err!(
            "unsupported SQL query: {:?}",
            stmt
        ))),
    }
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
                .iter()
                .map(|(n, d)| (n.to_owned(), d.inner.typ().to_owned())),
        );
        let dataflow = match parser.parse_statement(&stmt) {
            Ok(dataflow) => dataflow,
            Err(err) => return future::err(err).left(),
        };
        let name = dataflow.name().to_owned();
        meta_store
            .create_dataflow(&name, dataflow, dataflows.into_iter().map(|(_, v)| v))
            .map(|_| stmt)
            .right()
    })
    .and_then(|stmt| {
        Ok(match stmt {
            SQLStatement::SQLCreateDataSource { .. } => QueryResponse::CreatedDataSource,
            SQLStatement::SQLCreateView { .. } => QueryResponse::CreatedView,
            SQLStatement::SQLCreateTable { .. } => QueryResponse::CreatedTable,
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
    meta_store: metastore::MetaStore<Dataflow>,
    cmd_tx: CommandSender,
    server_state: Arc<ServerState>,
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
                let mut peek_results = server_state.peek_results.write().unwrap();
                peek_results.insert(uuid, (tx, 4 /* XXX */));
            }
            let ts = server_state.clock.now();
            cmd_tx
                .send(Command::Peek(name.to_string(), uuid, ts))
                .unwrap();
            future::ok(QueryResponse::StreamingRows {
                typ: dataflow.inner.typ().to_owned(),
                rows: Box::new(rx.map_err(|_| format_err!("unreachable"))),
            })
        })
}

fn handle_select(
    query: SQLQuery,
    meta_store: metastore::MetaStore<Dataflow>,
    cmd_tx: CommandSender,
    server_state: Arc<ServerState>,
) -> impl Future<Item = QueryResponse, Error = failure::Error> {
    let id: u64 = rand::random();
    let name = SQLObjectName(vec![format!("<temp_{}>", id)]);
    handle_statement(
        SQLStatement::SQLCreateView {
            name: name.clone(),
            query,
            materialized: true,
        },
        meta_store.clone(),
        cmd_tx.clone(),
        server_state.clone(),
    )
    .and_then(move |_| {
        handle_statement(
            SQLStatement::SQLPeek { name: name.clone() },
            meta_store.clone(),
            cmd_tx.clone(),
            server_state.clone(),
        )
        .and_then(move |query_response| {
            handle_statement(
                SQLStatement::SQLDropView {
                    name: name.clone(),
                    materialized: true,
                },
                meta_store,
                cmd_tx,
                server_state,
            )
            .map(move |_| query_response)
        })
    })
}

fn handle_insert(
    name: SQLObjectName,
    columns: Vec<SQLIdent>,
    values: Vec<Vec<ASTNode>>,
    meta_store: metastore::MetaStore<Dataflow>,
) -> impl Future<Item = QueryResponse, Error = failure::Error> {
    let name = name.to_string();
    meta_store
        .read_dataflows(vec![name.clone()])
        .and_then(move |dataflows| match dataflows[&name].inner {
            Dataflow::Source(ref src) => handle_insert_source(src, columns, values),
            Dataflow::View(_) => bail!("Can only insert into tables - {} is a view", name),
        })
}

fn handle_insert_source(
    source: &Source,
    columns: Vec<SQLIdent>,
    values: Vec<Vec<ASTNode>>,
) -> Result<QueryResponse, failure::Error> {
    let types = match &source.typ {
        Type {
            ftype: FType::Tuple(types),
            ..
        } => types,
        _ => bail!(
            "Can only insert into tables of tuple type - {} has type {:?}",
            source.name,
            source.typ
        ),
    };

    let sender = match &source.connector {
        Connector::Local(connector) => connector.get_sender(),
        connector => bail!(
            "Can only insert into tables - {} is a data source: {:?}",
            source.name,
            connector
        ),
    };

    if HashSet::<&String>::from_iter(&columns).len() != columns.len() {
        bail!(
            "Duplicate column in INSERT INTO ... COLUMNS ({})",
            columns.join(", ")
        );
    }
    let expected_columns = types
        .iter()
        .map(|typ| typ.name.clone().expect("Table columns should all be named"))
        .collect::<Vec<_>>();
    if HashSet::<&String>::from_iter(&columns).len()
        != HashSet::<&String>::from_iter(&expected_columns).len()
    {
        bail!(
            "Missing column in INSERT INTO ... COLUMNS ({}), expected {}",
            columns.join(", "),
            expected_columns.join(", ")
        );
    }
    let permutation = expected_columns
        .iter()
        .map(|name| columns.iter().position(|name2| name == name2).unwrap())
        .collect::<Vec<_>>();
    let n = values.len();
    let datums = values
        .into_iter()
        .map(|asts| {
            let permuted_asts = permutation.iter().map(|i| asts[*i].clone());
            let datums = permuted_asts
                .zip(types.iter())
                .map(|(ast, typ)| {
                    Ok(match ast {
                        ASTNode::SQLValue(value) => match (value, &typ.ftype) {
                            (Value::Null, _) => {
                                if typ.nullable {
                                    Datum::Null
                                } else {
                                    bail!("Tried to insert null into non-nullable column")
                                }
                            }
                            (Value::Long(l), FType::Int64) => Datum::Int64(l),
                            (Value::Double(f), FType::Float64) => Datum::Float64(f.into()),
                            (Value::SingleQuotedString(s), FType::String)
                            | (Value::NationalStringLiteral(s), FType::String) => Datum::String(s),
                            (Value::Boolean(b), FType::Bool) => {
                                if b {
                                    Datum::True
                                } else {
                                    Datum::False
                                }
                            }
                            (value, ftype) => bail!(
                                "Don't know how to insert value {:?} into column of type {:?}",
                                value,
                                ftype
                            ),
                        },
                        other => bail!("Can only insert plain values, not {:?}", other),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Datum::Tuple(datums))
        })
        .collect::<Result<Vec<_>, failure::Error>>()?;
    for datum in datums {
        sender.send(datum).map_err(|e| format_err!("{}", e))?;
    }
    Ok(QueryResponse::Inserted(n))
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

pub enum Side {
    Left,
    Right,
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        match self {
            Side::Left => write!(f, "left"),
            Side::Right => write!(f, "right"),
        }
    }
}

#[allow(dead_code)]
pub struct Parser {
    dataflows: HashMap<String, Type>,
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
                    connector: Connector::Kafka(KafkaConnector {
                        addr: url.to_socket_addrs()?.next().unwrap(),
                        topic,
                        raw_schema: schema.clone(),
                    }),
                    typ: crate::interchange::avro::parse_schema(schema)?,
                }))
            }
            SQLStatement::SQLCreateTable { name, columns } => {
                let types = columns
                    .iter()
                    .map(|column| {
                        Ok(Type {
                            name: Some(column.name.clone()),
                            ftype: match &column.data_type {
                                SQLType::Char(_) | SQLType::Varchar(_) | SQLType::Text => {
                                    FType::String
                                }
                                SQLType::SmallInt | SQLType::Int | SQLType::BigInt => FType::Int64,
                                SQLType::Float(_) | SQLType::Real | SQLType::Double => {
                                    FType::Float64
                                }
                                other => bail!("Unexpected SQL type: {:?}", other),
                            },
                            nullable: column.allow_null,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let typ = Type {
                    name: None,
                    ftype: FType::Tuple(types),
                    nullable: false,
                };
                Ok(Dataflow::Source(Source {
                    name: extract_sql_object_name(name)?,
                    connector: Connector::Local(LocalConnector::new()),
                    typ,
                }))
            }
            other => bail!("Unsupported statement: {:?}", other),
        }
    }

    pub fn parse_view_query(&self, q: &SQLQuery) -> Result<(Plan, Type), failure::Error> {
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
                                    "Each UNION should have the same column types: {:?} UNION {:?}",
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
                                        "Each UNION should have the same column types: {:?} UNION {:?}",
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
        // Step 1. Handle FROM clause, including joins.
        let mut plan = match &s.relation {
            Some(TableFactor::Table { name, alias }) => {
                let name = extract_sql_object_name(name)?;
                let types = match self.dataflows.get(&name) {
                    Some(Type {
                        ftype: FType::Tuple(types),
                        ..
                    }) => types.clone(),
                    Some(typ) => panic!("Table {} has non-tuple type {:?}", name, typ),
                    None => bail!("no table named {} in scope", name),
                };
                let mut plan = SQLPlan::from_source(&name, types);
                if let Some(alias) = alias {
                    plan = plan.alias_table(alias);
                }
                plan
            }
            Some(TableFactor::Derived { .. }) => {
                bail!("subqueries are not yet supported");
            }
            None => {
                // https://en.wikipedia.org/wiki/DUAL_table
                let types = vec![Type {
                    name: Some("x".into()),
                    nullable: false,
                    ftype: FType::String,
                }];
                SQLPlan::from_source("dual", types)
            }
        };
        let mut selection = s.selection.clone();
        for join in &s.joins {
            match &join.relation {
                TableFactor::Table { name, alias } => {
                    let name = extract_sql_object_name(&name)?;
                    let types = match self.dataflows.get(&name) {
                        Some(Type {
                            ftype: FType::Tuple(types),
                            ..
                        }) => types.clone(),
                        Some(typ) => panic!("Table {} has non-tuple type {:?}", name, typ),
                        None => bail!("no table named {} in scope", name),
                    };
                    let mut right = SQLPlan::from_source(&name, types);
                    if let Some(alias) = alias {
                        right = right.alias_table(alias);
                    }
                    plan =
                        self.parse_join_operator(&join.join_operator, &mut selection, plan, right)?;
                }
                TableFactor::Derived { .. } => {
                    bail!("subqueries are not yet supported");
                }
            }
        }

        // Step 2. Handle WHERE clause.
        if let Some(selection) = selection {
            let (expr, typ) = self.parse_expr(&selection, &plan)?;
            if typ.ftype != FType::Bool {
                bail!("WHERE clause must have boolean type, not {:?}", typ.ftype);
            }
            plan = plan.filter(expr);
        }

        // Step 3. Handle GROUP BY clause.
        let mut agg_visitor = AggregateFuncVisitor::new();
        for p in &s.projection {
            agg_visitor.visit_select_item(p);
        }
        let agg_frags = agg_visitor.into_result()?;
        if !agg_frags.is_empty() {
            let mut aggs = Vec::new();
            for frag in agg_frags {
                let (expr, typ) = self.parse_expr(&frag.expr, &plan)?;
                let func = AggregateFunc::from_name_and_ftype(frag.name.as_ref(), &typ.ftype)?;
                aggs.push((frag.id, Aggregate { func, expr }, typ));
            }

            let mut key_exprs = Vec::new();
            let mut retained_columns = Vec::new();
            for expr in &s.group_by {
                let (expr, typ) = self.parse_expr(&expr, &plan)?;
                retained_columns.push(typ);
                key_exprs.push(expr);
            }
            let key_expr = Expr::Tuple(key_exprs);

            plan = plan.aggregate((key_expr, retained_columns), aggs);
        }

        // Step 4. Handle HAVING clause.
        if let Some(having) = &s.having {
            let (expr, typ) = self.parse_expr(having, &plan)?;
            if typ.ftype != FType::Bool {
                bail!("HAVING clause must have boolean type, not {:?}", typ.ftype);
            }
            plan = plan.filter(expr);
        }

        // Step 5. Handle projections.
        let mut outputs = Vec::new();
        for p in &s.projection {
            for (expr, typ) in self.parse_select_item(p, &plan)? {
                outputs.push((expr, typ));
            }
        }
        plan = plan.project(outputs);

        // Step 6. Handle DISTINCT.
        if s.distinct {
            plan = plan.distinct();
        }

        Ok(plan.finish())
    }

    fn parse_select_item<'a>(
        &self,
        s: &'a SQLSelectItem,
        plan: &SQLPlan,
    ) -> Result<Vec<(Expr, Type)>, failure::Error> {
        match s {
            SQLSelectItem::UnnamedExpression(e) => Ok(vec![self.parse_expr(e, plan)?]),
            SQLSelectItem::ExpressionWithAlias(e, alias) => {
                let (expr, mut typ) = self.parse_expr(e, plan)?;
                typ.name = Some(alias.clone());
                Ok(vec![(expr, typ)])
            }
            SQLSelectItem::Wildcard => Ok(plan
                .get_all_column_types()
                .into_iter()
                .enumerate()
                .map(|(i, typ)| (Expr::Column(i, Box::new(Expr::Ambient)), typ.clone()))
                .collect()),
            SQLSelectItem::QualifiedWildcard(name) => Ok(plan
                .get_table_column_types(&extract_sql_object_name(name)?)
                .into_iter()
                .map(|(i, typ)| (Expr::Column(i, Box::new(Expr::Ambient)), typ.clone()))
                .collect()),
        }
    }

    fn parse_join_operator(
        &self,
        operator: &JoinOperator,
        selection: &mut Option<ASTNode>,
        left: SQLPlan,
        right: SQLPlan,
    ) -> Result<SQLPlan, failure::Error> {
        match operator {
            JoinOperator::Inner(constraint) => {
                self.parse_join_constraint(constraint, left, right, false, false)
            }
            JoinOperator::LeftOuter(constraint) => {
                self.parse_join_constraint(constraint, left, right, true, false)
            }
            JoinOperator::RightOuter(constraint) => {
                self.parse_join_constraint(constraint, left, right, false, true)
            }
            JoinOperator::FullOuter(constraint) => {
                self.parse_join_constraint(constraint, left, right, true, true)
            }
            JoinOperator::Implicit => {
                let (left_key, right_key, new_selection) =
                    self.parse_implicit_join_expr(selection, &left, &right)?;
                *selection = new_selection;
                Ok(left.join_on(right, left_key, right_key, false, false))
            }
            JoinOperator::Cross => Ok(left.join_on(
                right,
                Expr::Tuple(vec![]),
                Expr::Tuple(vec![]),
                false,
                false,
            )),
        }
    }

    fn parse_join_constraint<'a>(
        &self,
        constraint: &'a JoinConstraint,
        left: SQLPlan,
        right: SQLPlan,
        include_left_outer: bool,
        include_right_outer: bool,
    ) -> Result<SQLPlan, failure::Error> {
        match constraint {
            JoinConstraint::On(expr) => {
                let (left_key, right_key) = self.parse_join_on_expr(expr, &left, &right)?;
                Ok(left.join_on(
                    right,
                    left_key,
                    right_key,
                    include_left_outer,
                    include_right_outer,
                ))
            }
            JoinConstraint::Natural => {
                Ok(left.join_natural(right, include_left_outer, include_right_outer))
            }
            JoinConstraint::Using(column_names) => {
                Ok(left.join_using(right, column_names, include_left_outer, include_right_outer)?)
            }
        }
    }

    fn resolve_name(
        &self,
        name: &ASTNode,
        left: &SQLPlan,
        right: &SQLPlan,
    ) -> Result<(usize, Type, Side), failure::Error> {
        match name {
            ASTNode::SQLIdentifier(column_name) => {
                match (
                    left.resolve_column(column_name),
                    right.resolve_column(column_name),
                ) {
                    (Ok(_), Ok(_)) => bail!("column name {} is ambiguous", column_name),
                    (Ok((pos, typ)), Err(_)) => Ok((pos, typ.clone(), Side::Left)),
                    (Err(_), Ok((pos, typ))) => Ok((pos, typ.clone(), Side::Right)),
                    (Err(left_err), Err(right_err)) => bail!(
                        "{} on left of join, {} on right of join",
                        left_err,
                        right_err
                    ),
                }
            }
            ASTNode::SQLCompoundIdentifier(names) if names.len() == 2 => {
                let table_name = &names[0];
                let column_name = &names[1];
                match (
                    left.resolve_table_column(table_name, column_name),
                    right.resolve_table_column(table_name, column_name),
                ) {
                    (Ok(_), Ok(_)) => {
                        bail!("column name {}.{} is ambiguous", table_name, column_name)
                    }
                    (Ok((pos, typ)), Err(_)) => Ok((pos, typ.clone(), Side::Left)),
                    (Err(_), Ok((pos, typ))) => Ok((pos, typ.clone(), Side::Right)),
                    (Err(left_err), Err(right_err)) => bail!(
                        "{} on left of join, {} on right of join",
                        left_err,
                        right_err
                    ),
                }
            }
            _ => bail!(
                "cannot resolve unsupported complicated expression: {:?}",
                name
            ),
        }
    }

    fn parse_eq_expr(
        &self,
        left: &ASTNode,
        right: &ASTNode,
        left_plan: &SQLPlan,
        right_plan: &SQLPlan,
    ) -> Result<(Expr, Expr), failure::Error> {
        let (lpos, ltype, lside) = self.resolve_name(left, left_plan, right_plan)?;
        let (rpos, rtype, rside) = self.resolve_name(right, left_plan, right_plan)?;
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
        Ok((
            Expr::Column(lpos, Box::new(Expr::Ambient)),
            Expr::Column(rpos, Box::new(Expr::Ambient)),
        ))
    }

    fn parse_join_on_expr(
        &self,
        expr: &ASTNode,
        left_plan: &SQLPlan,
        right_plan: &SQLPlan,
    ) -> Result<(Expr, Expr), failure::Error> {
        let mut exprs = vec![expr];
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();

        while let Some(expr) = exprs.pop() {
            match unnest(expr) {
                ASTNode::SQLBinaryExpr { left, op, right } => match op {
                    SQLOperator::And => {
                        exprs.push(left);
                        exprs.push(right);
                    }
                    SQLOperator::Eq => {
                        let (left_expr, right_expr) =
                            self.parse_eq_expr(left, right, left_plan, right_plan)?;
                        left_keys.push(left_expr);
                        right_keys.push(right_expr);
                    }
                    _ => bail!("ON clause contained non-equality operator: {:?}", op),
                },
                _ => bail!(
                    "ON clause contained unsupported complicated expression: {:?}",
                    expr
                ),
            }
        }

        Ok((Expr::Tuple(left_keys), Expr::Tuple(right_keys)))
    }

    // This is basically the same as parse_join_on_expr, except that we allow `expr` to contain irrelevant constraints that need to be saved for later
    fn parse_implicit_join_expr(
        &self,
        expr: &Option<ASTNode>,
        left_plan: &SQLPlan,
        right_plan: &SQLPlan,
    ) -> Result<(Expr, Expr, Option<ASTNode>), failure::Error> {
        let mut exprs = expr.iter().collect::<Vec<_>>();
        let mut left_keys = Vec::new();
        let mut right_keys = Vec::new();
        let mut left_over = vec![];

        while let Some(expr) = exprs.pop() {
            match unnest(expr) {
                ASTNode::SQLBinaryExpr { left, op, right } => match op {
                    SQLOperator::And => {
                        exprs.push(left);
                        exprs.push(right);
                    }
                    SQLOperator::Eq => {
                        match self.parse_eq_expr(left, right, left_plan, right_plan) {
                            Ok((left_expr, right_expr)) => {
                                left_keys.push(left_expr);
                                right_keys.push(right_expr);
                            }
                            Err(_) => left_over.push(expr),
                        }
                    }
                    _ => left_over.push(expr),
                },
                _ => left_over.push(expr),
            }
        }

        let mut left_over_iter = left_over.into_iter();
        let left_over = left_over_iter.next().map(|expr| {
            left_over_iter.fold(expr.clone(), |e1, e2| ASTNode::SQLBinaryExpr {
                left: Box::new(e1.clone()),
                op: SQLOperator::And,
                right: Box::new(e2.clone()),
            })
        });

        Ok((Expr::Tuple(left_keys), Expr::Tuple(right_keys), left_over))
    }

    fn parse_expr<'a>(
        &self,
        e: &'a ASTNode,
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        match e {
            ASTNode::SQLIdentifier(name) => {
                let (i, typ) = plan.resolve_column(name)?;
                let expr = Expr::Column(i, Box::new(Expr::Ambient));
                Ok((expr, typ.clone()))
            }
            ASTNode::SQLCompoundIdentifier(names) if names.len() == 2 => {
                let (i, typ) = plan.resolve_table_column(&names[0], &names[1])?;
                let expr = Expr::Column(i, Box::new(Expr::Ambient));
                Ok((expr, typ.clone()))
            }
            ASTNode::SQLValue(val) => self.parse_literal(val),
            // TODO(benesch): why isn't IS [NOT] NULL a unary op?
            ASTNode::SQLIsNull(expr) => self.parse_is_null_expr(expr, false, plan),
            ASTNode::SQLIsNotNull(expr) => self.parse_is_null_expr(expr, true, plan),
            // TODO(benesch): "SQLUnary" but "SQLBinaryExpr"?
            ASTNode::SQLUnary { operator, expr } => self.parse_unary_expr(operator, expr, plan),
            ASTNode::SQLBinaryExpr { op, left, right } => {
                self.parse_binary_expr(op, left, right, plan)
            }
            ASTNode::SQLNested(expr) => self.parse_expr(expr, plan),
            ASTNode::SQLFunction { id, args } => self.parse_function(id, args, plan),
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
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        if AggregateFunc::is_aggregate_func(ident) {
            let (i, typ) = plan.resolve_func(ident);
            let expr = Expr::Column(i, Box::new(Expr::Ambient));
            Ok((expr, typ.clone()))
        } else {
            bail!("Unsupported function: {}", ident)
        }
    }

    fn parse_is_null_expr<'a>(
        &self,
        inner: &'a ASTNode,
        not: bool,
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        let (expr, _) = self.parse_expr(inner, plan)?;
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
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        let (expr, typ) = self.parse_expr(expr, plan)?;
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
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        let (lexpr, ltype) = self.parse_expr(left, plan)?;
        let (rexpr, rtype) = self.parse_expr(right, plan)?;
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
                _ => bail!("no overload for {:?} * {:?}", ltype.ftype, rtype.ftype),
            },
            SQLOperator::Divide => match (&ltype.ftype, &rtype.ftype) {
                (FType::Int32, FType::Int32) => (BinaryFunc::DivInt32, FType::Int32),
                (FType::Int64, FType::Int64) => (BinaryFunc::DivInt64, FType::Int64),
                (FType::Float32, FType::Float32) => (BinaryFunc::DivFloat32, FType::Float32),
                (FType::Float64, FType::Float64) => (BinaryFunc::DivFloat64, FType::Float64),
                _ => bail!("no overload for {:?} / {:?}", ltype.ftype, rtype.ftype),
            },
            SQLOperator::Modulus => match (&ltype.ftype, &rtype.ftype) {
                (FType::Int32, FType::Int32) => (BinaryFunc::ModInt32, FType::Int32),
                (FType::Int64, FType::Int64) => (BinaryFunc::ModInt64, FType::Int64),
                (FType::Float32, FType::Float32) => (BinaryFunc::ModFloat32, FType::Float32),
                (FType::Float64, FType::Float64) => (BinaryFunc::ModFloat64, FType::Float64),
                _ => bail!("no overload for {:?} % {:?}", ltype.ftype, rtype.ftype),
            },
            SQLOperator::Lt
            | SQLOperator::LtEq
            | SQLOperator::Gt
            | SQLOperator::GtEq
            | SQLOperator::Eq
            | SQLOperator::NotEq => {
                if ltype.ftype != rtype.ftype {
                    bail!("{:?} and {:?} are not comparable", ltype.ftype, rtype.ftype)
                }
                let func = match op {
                    SQLOperator::Lt => BinaryFunc::Lt,
                    SQLOperator::LtEq => BinaryFunc::Lte,
                    SQLOperator::Gt => BinaryFunc::Gt,
                    SQLOperator::GtEq => BinaryFunc::Gte,
                    SQLOperator::Eq => BinaryFunc::Eq,
                    SQLOperator::NotEq => BinaryFunc::NotEq,
                    _ => unreachable!(),
                };
                (func, FType::Bool)
            }
            other => bail!("Function {:?} is not supported yet", other),
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
                connector: Connector::Kafka(KafkaConnector {
                    addr: "127.0.0.1:9092".parse()?,
                    topic: "topic".into(),
                    raw_schema: raw_schema.to_owned(),
                }),
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
                        nullable: false,
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
