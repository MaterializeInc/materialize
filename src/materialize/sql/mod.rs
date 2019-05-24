// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL-dataflow translation.

use failure::{bail, format_err};
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast;
use sqlparser::sqlast::visit;
use sqlparser::sqlast::visit::Visit;
use sqlparser::sqlast::{
    ASTNode, DataSourceSchema, JoinConstraint, JoinOperator, SQLIdent, SQLObjectName, SQLOperator,
    SQLQuery, SQLSelect, SQLSelectItem, SQLSetExpr, SQLSetOperator, SQLStatement, SQLType,
    SQLWindowSpec, TableFactor, Value,
};

use sqlparser::sqlparser::Parser as SQLParser;
use std::collections::HashSet;
use std::iter::FromIterator;
use std::net::{SocketAddr, ToSocketAddrs};
use url::Url;

use crate::dataflow::func::{AggregateFunc, BinaryFunc, UnaryFunc, VariadicFunc};
use crate::dataflow::{
    Aggregate, Dataflow, Expr, KafkaSinkConnector, KafkaSourceConnector, LocalSourceConnector,
    Plan, Sink, SinkConnector, Source, SourceConnector, View,
};
use crate::glue::*;
use crate::interchange::avro;
use crate::repr::{Datum, FType, Type};
use ore::collections::CollectionExt;
use plan::SQLPlan;
use store::{DataflowStore, RemoveMode};

mod multiway_plan;
mod plan;
mod store;

#[derive(Debug, Default)]
pub struct Planner {
    dataflows: DataflowStore,
}

pub type PlannerResult = Result<(SqlResponse, Option<DataflowCommand>), failure::Error>;

impl Planner {
    pub fn handle_command(&mut self, sql: String) -> PlannerResult {
        let stmts = SQLParser::parse_sql(&AnsiSqlDialect {}, sql)?;
        match stmts.len() {
            0 => Ok((SqlResponse::EmptyQuery, None)),
            1 => self.handle_statement(stmts.into_element()),
            _ => bail!("expected one statement, but got {}", stmts.len()),
        }
    }

    fn handle_statement(&mut self, stmt: SQLStatement) -> PlannerResult {
        match stmt {
            SQLStatement::SQLPeek { name } => self.handle_peek(name),
            SQLStatement::SQLTail { .. } => bail!("TAIL is not implemented yet"),
            SQLStatement::SQLCreateDataSource { .. }
            | SQLStatement::SQLCreateDataSink { .. }
            | SQLStatement::SQLCreateView { .. }
            | SQLStatement::SQLCreateTable { .. } => self.handle_create_dataflow(stmt),
            SQLStatement::SQLDropDataSource { .. }
            | SQLStatement::SQLDropView { .. }
            | SQLStatement::SQLDropTable { .. } => self.handle_drop_dataflow(stmt),

            // these are intended mostly for testing:
            SQLStatement::SQLQuery(query) => self.handle_select(*query),
            SQLStatement::SQLInsert {
                table_name,
                columns,
                values,
            } => self.handle_insert(table_name, columns, values),

            _ => bail!("unsupported SQL statement: {:?}", stmt),
        }
    }

    fn handle_create_dataflow(&mut self, stmt: SQLStatement) -> PlannerResult {
        let dataflow = self.plan_statement(&stmt)?;
        let sql_response = match stmt {
            SQLStatement::SQLCreateDataSource { .. } => SqlResponse::CreatedDataSource,
            SQLStatement::SQLCreateDataSink { .. } => SqlResponse::CreatedDataSink,
            SQLStatement::SQLCreateView { .. } => SqlResponse::CreatedView,
            SQLStatement::SQLCreateTable { .. } => SqlResponse::CreatedTable,
            _ => unreachable!(),
        };

        self.dataflows.insert(dataflow.clone())?;
        Ok((
            sql_response,
            Some(DataflowCommand::CreateDataflow(dataflow)),
        ))
    }

    fn handle_drop_dataflow(&mut self, stmt: SQLStatement) -> PlannerResult {
        // TODO(benesch): DROP <TYPE> should error if the named object is not
        // of the correct type (#38).
        let (sql_response, drop) = match stmt {
            SQLStatement::SQLDropDataSource(drop) => (SqlResponse::DroppedDataSource, drop),
            SQLStatement::SQLDropTable(drop) => (SqlResponse::DroppedTable, drop),
            SQLStatement::SQLDropView(drop) => (SqlResponse::DroppedView, drop),
            _ => unreachable!(),
        };
        let names: Vec<String> = Result::from_iter(drop.names.iter().map(extract_sql_object_name))?;
        if !drop.if_exists {
            // Without IF EXISTS, we need to verify that every named
            // dataflow exists before proceeding with the drop
            // implementation.
            for name in &names {
                let _ = self.dataflows.get(name)?;
            }
        }
        let mode = RemoveMode::from_cascade(drop.cascade);
        for name in &names {
            self.dataflows.remove(name, mode)?;
        }
        Ok((sql_response, Some(DataflowCommand::DropDataflows(names))))
    }

    fn handle_peek(&mut self, name: SQLObjectName) -> PlannerResult {
        let name = name.to_string();
        let typ = self.dataflows.get_type(&name)?.clone();

        Ok((
            SqlResponse::Peeking { typ },
            Some(DataflowCommand::PeekExisting(name)),
        ))
    }

    fn handle_select(&mut self, query: SQLQuery) -> PlannerResult {
        let id: u64 = rand::random();
        let name = format!("<temp_{}>", id);
        let dataflow = self.plan_statement(&SQLStatement::SQLCreateView {
            name: SQLObjectName(vec![name.clone()]),
            query: Box::new(query),
            materialized: true,
            with_options: vec![],
        })?;
        // Safe to unwrap dataflow.typ() below, as planning a view always yields
        // a dataflow with a type.
        let typ = dataflow.typ().unwrap().clone();

        Ok((
            SqlResponse::Peeking { typ },
            Some(DataflowCommand::PeekTransient(dataflow)),
        ))
    }

    fn handle_insert(
        &mut self,
        name: SQLObjectName,
        columns: Vec<SQLIdent>,
        values: Vec<Vec<ASTNode>>,
    ) -> PlannerResult {
        let name = name.to_string();
        let typ = match self.dataflows.get(&name)? {
            Dataflow::Source(Source {
                connector: SourceConnector::Local(_),
                typ,
                ..
            }) => typ,
            other => bail!("Can only insert into tables - {} is a {:?}", name, other),
        };
        let types = match &typ {
            Type {
                ftype: FType::Tuple(types),
                ..
            } => types,
            _ => bail!(
                "Can only insert into tables of tuple type - {} has type {:?}",
                name,
                typ
            ),
        };

        let permutation = if columns.is_empty() {
            // if not specified, just insert in natural order
            (0..types.len()).collect::<Vec<_>>()
        } else {
            // otherwise, check that we have a sensible list of columns
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
            expected_columns
                .iter()
                .map(|name| columns.iter().position(|name2| name == name2).unwrap())
                .collect::<Vec<_>>()
        };
        let datums = values
            .into_iter()
            .map(|asts| {
                let permuted_asts = permutation.iter().map(|i| asts[*i].clone());
                let datums = permuted_asts
                    .zip(types.iter())
                    .map(|(ast, typ)| Datum::from_sql(ast, typ))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Datum::Tuple(datums))
            })
            .collect::<Result<Vec<_>, failure::Error>>()?;

        Ok((
            SqlResponse::Inserted(datums.len()),
            Some(DataflowCommand::Insert(name, datums)),
        ))
    }
}

impl Datum {
    pub fn from_sql(ast: ASTNode, typ: &Type) -> Result<Self, failure::Error> {
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
    }
}

struct AggregateFragment {
    name: String,
    id: *const SQLObjectName,
    expr: ASTNode,
    distinct: bool,
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
    fn visit_function(
        &mut self,
        name: &'ast SQLObjectName,
        args: &'ast Vec<ASTNode>,
        over: Option<&'ast SQLWindowSpec>,
        all: bool,
        distinct: bool,
    ) {
        if over.is_some() {
            self.err = Some(format_err!("window functions are not yet supported"));
            return;
        }
        let name_str = name.to_string().to_lowercase();
        let old_within = self.within;
        match name_str.as_ref() {
            "avg" | "sum" | "min" | "max" | "count" => {
                if self.within {
                    self.err = Some(format_err!("nested aggregate functions are not allowed"));
                    return;
                }
                if args.len() != 1 {
                    self.err = Some(format_err!("{} function only takes one argument", name_str));
                    return;
                }
                self.aggs.push(AggregateFragment {
                    name: name_str.clone(),
                    id: name as *const _,
                    expr: args[0].clone(),
                    distinct,
                });
                self.within = true;
            }
            _ => (),
        }
        visit::visit_function(self, name, args, over, all, distinct);
        self.within = old_within;
    }

    fn visit_subquery(&mut self, _subquery: &'ast SQLQuery) {
        // don't go into subqueries
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

impl Planner {
    pub fn plan_statement(&self, stmt: &SQLStatement) -> Result<Dataflow, failure::Error> {
        match stmt {
            SQLStatement::SQLCreateView {
                name,
                query,
                materialized: true,
                with_options,
            } => {
                if !with_options.is_empty() {
                    bail!("WITH options are not yet supported");
                }
                let (plan, typ) = self.plan_view_query(query)?;
                Ok(Dataflow::View(View {
                    name: extract_sql_object_name(name)?,
                    plan,
                    typ,
                }))
            }
            SQLStatement::SQLCreateDataSource {
                name,
                url,
                schema,
                with_options,
            } => {
                if !with_options.is_empty() {
                    bail!("WITH options are not yet supported");
                }
                let name = extract_sql_object_name(name)?;
                let (addr, topic) = parse_kafka_url(url)?;
                let (raw_schema, schema_registry_url) = match schema {
                    DataSourceSchema::Raw(schema) => (schema.to_owned(), None),
                    DataSourceSchema::Registry(url) => {
                        // TODO(benesch): we need to fetch this schema
                        // asynchronously to avoid blocking the command
                        // processing thread.
                        let url: Url = url.parse()?;
                        let ccsr_client = ccsr::Client::new(url.clone());
                        let res = ccsr_client.get_schema_by_subject(&format!("{}-value", topic))?;
                        (res.raw, Some(url))
                    }
                };
                let typ = avro::parse_schema(&raw_schema)?;
                Ok(Dataflow::Source(Source {
                    name,
                    connector: SourceConnector::Kafka(KafkaSourceConnector {
                        addr,
                        topic,
                        raw_schema,
                        schema_registry_url,
                    }),
                    typ,
                }))
            }
            SQLStatement::SQLCreateDataSink {
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
                let _ = self.dataflows.get(&from)?;
                let (addr, topic) = parse_kafka_url(url)?;
                Ok(Dataflow::Sink(Sink {
                    name,
                    from,
                    connector: SinkConnector::Kafka(KafkaSinkConnector {
                        addr,
                        topic,
                        schema_id: 0,
                    }),
                }))
            }
            SQLStatement::SQLCreateTable {
                name,
                columns,
                with_options,
                external,
                file_format,
                location,
            } => {
                if *external || file_format.is_some() || location.is_some() {
                    bail!("EXTERNAL tables are not supported");
                }
                if !with_options.is_empty() {
                    bail!("WITH options are not supported");
                }
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
                    connector: SourceConnector::Local(LocalSourceConnector {}),
                    typ,
                }))
            }
            other => bail!("Unsupported statement: {:?}", other),
        }
    }

    pub fn plan_view_query(&self, q: &SQLQuery) -> Result<(Plan, Type), failure::Error> {
        if !q.ctes.is_empty() {
            bail!("CTEs are not yet supported");
        }
        if q.limit.is_some() {
            bail!("LIMIT is not supported in a view definition");
        }
        if !q.order_by.is_empty() {
            bail!("ORDER BY is not supported in a view definition");
        }
        self.plan_set_expr(&q.body)
    }

    fn plan_set_expr(&self, q: &SQLSetExpr) -> Result<(Plan, Type), failure::Error> {
        match q {
            SQLSetExpr::Select(select) => self.plan_view_select(select),
            SQLSetExpr::SetOperation {
                op: SQLSetOperator::Union,
                all,
                left,
                right,
            } => {
                let (left_plan, left_type) = self.plan_set_expr(left)?;
                let (right_plan, right_type) = self.plan_set_expr(right)?;

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

    fn plan_view_select(&self, s: &SQLSelect) -> Result<(Plan, Type), failure::Error> {
        // Step 1. Handle FROM clause, including joins.
        let none = None;
        let (from_name, from_alias) = match &s.relation {
            Some(TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            }) => {
                if !args.is_empty() {
                    bail!("table arguments are not supported");
                }
                if !with_hints.is_empty() {
                    bail!("WITH hints are not supported");
                }
                (extract_sql_object_name(name)?, alias)
            }
            Some(TableFactor::Derived { .. }) => {
                bail!("subqueries are not yet supported");
            }
            None => ("dual".into(), &none),
        };
        let plan = {
            let types = match self.dataflows.get_type(&from_name)? {
                Type {
                    ftype: FType::Tuple(types),
                    ..
                } => types.clone(),
                typ => panic!("table {} has non-tuple type {:?}", from_name, typ),
            };
            let mut plan = SQLPlan::from_source(&from_name, types);
            if let Some(alias) = from_alias {
                plan = plan.alias_table(alias);
            }
            plan
        };

        let (mut plan, selection) =
            self.plan_multiple_joins(plan, &s.joins[..], s.selection.clone())?;

        // if we have a `SELECT *` later, this is what it selects
        let named_columns = plan.named_columns();

        // Step 2. Handle WHERE clause.
        if let Some(selection) = selection {
            let ctx = &ExprContext {
                scope: "WHERE clause",
                allow_aggregates: false,
            };
            let (expr, typ) = self.plan_expr(ctx, &selection, &plan)?;
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
        if let Some(having) = &s.having {
            agg_visitor.visit_expr(having);
        }
        let agg_frags = agg_visitor.into_result()?;
        if !agg_frags.is_empty() || !s.group_by.is_empty() {
            let ctx = &ExprContext {
                scope: "GROUP BY clause",
                allow_aggregates: false,
            };
            let mut aggs = Vec::new();
            for frag in agg_frags {
                let (expr, func, ftype) = match (&*frag.name, &frag.expr) {
                    // COUNT(*) is a special case that doesn't compose well
                    ("count", ASTNode::SQLWildcard) => {
                        (Expr::Ambient, AggregateFunc::CountAll, FType::Int64)
                    }
                    _ => {
                        let (expr, typ) = self.plan_expr(ctx, &frag.expr, &plan)?;
                        let (func, ftype) =
                            AggregateFunc::from_name_and_ftype(&frag.name, &typ.ftype)?;
                        (expr, func, ftype)
                    }
                };
                aggs.push((
                    frag.id,
                    Aggregate {
                        func,
                        expr,
                        distinct: frag.distinct,
                    },
                    Type {
                        // TODO(jamii) name should be format("{}", expr) eg "count(*)"
                        name: None,
                        nullable: func.is_nullable(),
                        ftype,
                    },
                ));
            }

            let mut key_exprs = Vec::new();
            let mut key_columns = Vec::new();
            for expr in &s.group_by {
                // we have to remember the names of GROUP BY exprs so we can SELECT them later
                let name = match expr {
                    ASTNode::SQLIdentifier(column_name) => {
                        let (_, name, _) = plan.resolve_column(column_name)?;
                        name.clone()
                    }
                    ASTNode::SQLCompoundIdentifier(names) if names.len() == 2 => {
                        let (_, name, _) = plan.resolve_table_column(&names[0], &names[1])?;
                        name.clone()
                    }
                    // TODO(jamii) for complex exprs, we need to remember the expr itself so we can do eg `SELECT (a+1)+1 FROM .. GROUP BY a+1` :(
                    _ => plan::Name::none(),
                };
                let (expr, typ) = self.plan_expr(ctx, &expr, &plan)?;
                // repeated exprs in GROUP BY confuse name resolution later, and dropping them doesn't change the results
                if !key_exprs.contains(&expr) {
                    key_columns.push((name, typ));
                    key_exprs.push(expr);
                }
            }
            let key_expr = Expr::Tuple(key_exprs);

            plan = plan.aggregate(key_expr, key_columns, aggs);
        }

        // Step 4. Handle HAVING clause.
        if let Some(having) = &s.having {
            let ctx = &ExprContext {
                scope: "HAVING clause",
                allow_aggregates: true,
            };
            let (expr, typ) = self.plan_expr(ctx, having, &plan)?;
            if typ.ftype != FType::Bool {
                bail!("HAVING clause must have boolean type, not {:?}", typ.ftype);
            }
            plan = plan.filter(expr);
        }

        // Step 5. Handle projections.
        let mut outputs = Vec::new();
        for p in &s.projection {
            for (expr, typ) in self.plan_select_item(p, &plan, &named_columns)? {
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

    fn plan_select_item<'a>(
        &self,
        s: &'a SQLSelectItem,
        plan: &SQLPlan,
        named_columns: &[(String, String)],
    ) -> Result<Vec<(Expr, Type)>, failure::Error> {
        let ctx = &ExprContext {
            scope: "SELECT projection",
            allow_aggregates: true,
        };
        match s {
            SQLSelectItem::UnnamedExpression(e) => Ok(vec![self.plan_expr(ctx, e, plan)?]),
            SQLSelectItem::ExpressionWithAlias { expr, alias } => {
                let (expr, mut typ) = self.plan_expr(ctx, expr, plan)?;
                typ.name = Some(alias.clone());
                Ok(vec![(expr, typ)])
            }
            SQLSelectItem::Wildcard => named_columns
                .iter()
                .map(|(table_name, column_name)| {
                    let (pos, _, typ) = plan.resolve_table_column(table_name, column_name)?;
                    Ok((Expr::Column(pos, Box::new(Expr::Ambient)), typ.clone()))
                })
                .collect::<Result<Vec<_>, _>>(),
            SQLSelectItem::QualifiedWildcard(name) => {
                let name = extract_sql_object_name(name)?;
                named_columns
                    .iter()
                    .filter(|(table_name, _)| *table_name == name)
                    .map(|(table_name, column_name)| {
                        let (pos, _, typ) = plan.resolve_table_column(table_name, column_name)?;
                        Ok((Expr::Column(pos, Box::new(Expr::Ambient)), typ.clone()))
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
        }
    }

    fn plan_multiple_joins(
        &self,
        mut plan: SQLPlan,
        joins: &[sqlast::Join],
        mut selection: Option<ASTNode>,
    ) -> Result<(SQLPlan, Option<ASTNode>), failure::Error> {
        if joins.is_empty() {
            Ok((plan, selection))
        } else {
            // Assemble participating tables.
            let mut tables = Vec::new();
            tables.push(plan.clone());

            for join in joins.iter() {
                match &join.relation {
                    TableFactor::Table {
                        name,
                        alias,
                        args,
                        with_hints,
                    } => {
                        if !args.is_empty() {
                            bail!("table arguments are not supported");
                        }
                        if !with_hints.is_empty() {
                            bail!("WITH hints are not supported");
                        }
                        let name = extract_sql_object_name(&name)?;
                        let types = match self.dataflows.get_type(&name)? {
                            Type {
                                ftype: FType::Tuple(types),
                                ..
                            } => types.clone(),
                            typ => panic!("Table {} has non-tuple type {:?}", name, typ),
                        };
                        let mut right = SQLPlan::from_source(&name, types);
                        if let Some(alias) = alias {
                            right = right.alias_table(alias);
                        }
                        tables.push(right);
                    }
                    TableFactor::Derived { .. } => {
                        bail!("subqueries are not yet supported");
                    }
                }
            }

            // Assert that we have the right number of tables at hand (e.g. that we didn't
            // miss something in that loop above with the special cases).
            assert!(tables.len() == joins.len() + 1);

            // Extract all ASTNode join constraints

            let attempt =
                multiway_plan::plan_multiple_joins(&tables[..], &joins[..], selection.clone());

            if let Ok((plan, selection)) = attempt {
                // println!("!!!!!!!!");
                Ok((plan, selection))
            } else {
                // println!("Bailed: {:?}", attempt);
                for (index, join) in joins.iter().enumerate() {
                    plan = self.plan_join_operator(
                        &join.join_operator,
                        &mut selection,
                        plan,
                        tables[index + 1].clone(),
                    )?;
                }
                Ok((plan, selection))
            }
        }
    }

    fn plan_join_operator(
        &self,
        operator: &JoinOperator,
        selection: &mut Option<ASTNode>,
        left: SQLPlan,
        right: SQLPlan,
    ) -> Result<SQLPlan, failure::Error> {
        match operator {
            JoinOperator::Inner(constraint) => {
                self.plan_join_constraint(selection, constraint, left, right, false, false)
            }
            JoinOperator::LeftOuter(constraint) => {
                self.plan_join_constraint(selection, constraint, left, right, true, false)
            }
            JoinOperator::RightOuter(constraint) => {
                self.plan_join_constraint(selection, constraint, left, right, false, true)
            }
            JoinOperator::FullOuter(constraint) => {
                self.plan_join_constraint(selection, constraint, left, right, true, true)
            }
            JoinOperator::Implicit => {
                let (left_key, right_key, new_selection) =
                    self.plan_join_expr(selection.as_ref(), &left, &right)?;
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

    fn plan_join_constraint<'a>(
        &self,
        selection: &mut Option<ASTNode>,
        constraint: &'a JoinConstraint,
        left: SQLPlan,
        right: SQLPlan,
        include_left_outer: bool,
        include_right_outer: bool,
    ) -> Result<SQLPlan, failure::Error> {
        match constraint {
            JoinConstraint::On(expr) => {
                let (left_key, right_key, left_over_expr) =
                    self.plan_join_expr(Some(expr), &left, &right)?;
                if let Some(left_over_expr) = left_over_expr {
                    if let Some(existing_selection) = selection.take() {
                        *selection = Some(ASTNode::SQLBinaryExpr {
                            left: Box::new(left_over_expr),
                            op: SQLOperator::And,
                            right: Box::new(existing_selection),
                        });
                    } else {
                        *selection = Some(left_over_expr);
                    }
                }
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
                    (Ok((pos, _, typ)), Err(_)) => Ok((pos, typ.clone(), Side::Left)),
                    (Err(_), Ok((pos, _, typ))) => Ok((pos, typ.clone(), Side::Right)),
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
                    (Ok((pos, _, typ)), Err(_)) => Ok((pos, typ.clone(), Side::Left)),
                    (Err(_), Ok((pos, _, typ))) => Ok((pos, typ.clone(), Side::Right)),
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

    fn plan_eq_expr(
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

    fn plan_join_expr(
        &self,
        expr: Option<&ASTNode>,
        left_plan: &SQLPlan,
        right_plan: &SQLPlan,
    ) -> Result<(Expr, Expr, Option<ASTNode>), failure::Error> {
        let mut exprs = expr.into_iter().collect::<Vec<&ASTNode>>();
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
                        match self.plan_eq_expr(left, right, left_plan, right_plan) {
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

    fn plan_expr<'a>(
        &self,
        ctx: &ExprContext,
        e: &'a ASTNode,
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        match e {
            ASTNode::SQLIdentifier(name) => {
                let (i, _, typ) = plan.resolve_column(name)?;
                let expr = Expr::Column(i, Box::new(Expr::Ambient));
                Ok((expr, typ.clone()))
            }
            ASTNode::SQLCompoundIdentifier(names) if names.len() == 2 => {
                let (i, _, typ) = plan.resolve_table_column(&names[0], &names[1])?;
                let expr = Expr::Column(i, Box::new(Expr::Ambient));
                Ok((expr, typ.clone()))
            }
            ASTNode::SQLValue(val) => self.plan_literal(val),
            // TODO(benesch): why isn't IS [NOT] NULL a unary op?
            ASTNode::SQLIsNull(expr) => self.plan_is_null_expr(ctx, expr, false, plan),
            ASTNode::SQLIsNotNull(expr) => self.plan_is_null_expr(ctx, expr, true, plan),
            // TODO(benesch): "SQLUnary" but "SQLBinaryExpr"?
            ASTNode::SQLUnary { operator, expr } => self.plan_unary_expr(ctx, operator, expr, plan),
            ASTNode::SQLBinaryExpr { op, left, right } => {
                self.plan_binary_expr(ctx, op, left, right, plan)
            }
            ASTNode::SQLBetween {
                expr,
                low,
                high,
                negated,
            } => self.plan_between(ctx, expr, low, high, *negated, plan),
            ASTNode::SQLCase {
                operand,
                conditions,
                results,
                else_result,
            } => self.plan_case(ctx, operand, conditions, results, else_result, plan),
            ASTNode::SQLNested(expr) => self.plan_expr(ctx, expr, plan),
            ASTNode::SQLFunction {
                name,
                args,
                over,
                all,
                distinct,
            } => self.plan_function(ctx, name, args, over, *all, *distinct, plan),
            _ => bail!(
                "complicated expressions are not yet supported: {}",
                e.to_string()
            ),
        }
    }

    #[allow(clippy::ptr_arg)]
    #[allow(clippy::too_many_arguments)]
    fn plan_function<'a>(
        &self,
        ctx: &ExprContext,
        name: &'a SQLObjectName,
        args: &'a [ASTNode],
        _over: &'a Option<SQLWindowSpec>,
        _all: bool,
        _distinct: bool,
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        let ident = name.to_string().to_lowercase();

        if AggregateFunc::is_aggregate_func(&ident) {
            if !ctx.allow_aggregates {
                bail!("aggregate functions are not allowed in {}", ctx.scope);
            }
            let (i, typ) = plan.resolve_func(name);
            let expr = Expr::Column(i, Box::new(Expr::Ambient));
            return Ok((expr, typ.clone()));
        }

        match ident.as_str() {
            "abs" => {
                if args.len() != 1 {
                    bail!("abs expects one argument, got {}", args.len());
                }
                let (expr, typ) = self.plan_expr(ctx, args.into_element(), plan)?;
                let func = match typ.ftype {
                    FType::Int32 => UnaryFunc::AbsInt32,
                    FType::Int64 => UnaryFunc::AbsInt64,
                    FType::Float32 => UnaryFunc::AbsFloat32,
                    FType::Float64 => UnaryFunc::AbsFloat64,
                    _ => bail!("abs does not accept arguments of type {:?}", typ),
                };
                let expr = Expr::CallUnary {
                    func,
                    expr: Box::new(expr),
                };
                Ok((expr, typ))
            }

            "coalesce" => {
                if args.is_empty() {
                    bail!("coalesce requires at least one argument");
                }
                let mut exprs = Vec::new();
                let mut result_type: Option<Type> = None;
                for arg in args {
                    let (expr, typ) = self.plan_expr(ctx, arg, plan)?;
                    match &result_type {
                        Some(result_type) => {
                            if result_type.ftype != typ.ftype {
                                bail!(
                                    "COALESCE does not have uniform argument type: {:?} vs {:?}",
                                    result_type.ftype,
                                    typ.ftype
                                )
                            }
                        }
                        None => result_type = Some(typ),
                    }
                    exprs.push(expr);
                }
                // args is known to be non-empty, and therefore result_type must
                // be non-None after the loop above.
                let result_type = result_type.unwrap();
                let expr = Expr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs,
                };
                Ok((expr, result_type))
            }

            _ => bail!("unsupported function: {}", ident),
        }
    }

    fn plan_is_null_expr<'a>(
        &self,
        ctx: &ExprContext,
        inner: &'a ASTNode,
        not: bool,
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        let (expr, _) = self.plan_expr(ctx, inner, plan)?;
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

    fn plan_unary_expr<'a>(
        &self,
        ctx: &ExprContext,
        op: &'a SQLOperator,
        expr: &'a ASTNode,
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        let (expr, typ) = self.plan_expr(ctx, expr, plan)?;
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

    fn plan_binary_expr<'a>(
        &self,
        ctx: &ExprContext,
        op: &'a SQLOperator,
        left: &'a ASTNode,
        right: &'a ASTNode,
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        let (lexpr, ltype) = self.plan_expr(ctx, left, plan)?;
        let (rexpr, rtype) = self.plan_expr(ctx, right, plan)?;
        let (func, ftype) = match op {
            SQLOperator::And | SQLOperator::Or => {
                if ltype.ftype != FType::Bool && ltype.ftype != FType::Null {
                    bail!(
                        "Cannot apply operator {:?} to non-boolean type {:?}",
                        op,
                        ltype.ftype
                    )
                }
                if rtype.ftype != FType::Bool && rtype.ftype != FType::Null {
                    bail!(
                        "Cannot apply operator {:?} to non-boolean type {:?}",
                        op,
                        rtype.ftype
                    )
                }
                let func = match op {
                    SQLOperator::And => BinaryFunc::And,
                    SQLOperator::Or => BinaryFunc::Or,
                    _ => unreachable!(),
                };
                (func, FType::Bool)
            }
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
                if ltype.ftype != rtype.ftype
                    && ltype.ftype != FType::Null
                    && rtype.ftype != FType::Null
                {
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
        let is_integer_div = match &func {
            BinaryFunc::DivInt32 | BinaryFunc::DivInt64 => true,
            _ => false,
        };
        let expr = Expr::CallBinary {
            func,
            expr1: Box::new(lexpr),
            expr2: Box::new(rexpr),
        };
        let typ = Type {
            name: None,
            nullable: ltype.nullable || rtype.nullable || is_integer_div,
            ftype,
        };
        Ok((expr, typ))
    }

    fn plan_between<'a>(
        &self,
        ctx: &ExprContext,
        expr: &'a ASTNode,
        low: &'a ASTNode,
        high: &'a ASTNode,
        negated: bool,
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        let low = ASTNode::SQLBinaryExpr {
            left: Box::new(expr.clone()),
            op: if negated {
                SQLOperator::Lt
            } else {
                SQLOperator::GtEq
            },
            right: Box::new(low.clone()),
        };
        let high = ASTNode::SQLBinaryExpr {
            left: Box::new(expr.clone()),
            op: if negated {
                SQLOperator::Gt
            } else {
                SQLOperator::LtEq
            },
            right: Box::new(high.clone()),
        };
        let both = ASTNode::SQLBinaryExpr {
            left: Box::new(low),
            op: if negated {
                SQLOperator::Or
            } else {
                SQLOperator::And
            },
            right: Box::new(high),
        };
        self.plan_expr(ctx, &both, plan)
    }

    fn plan_case<'a>(
        &self,
        ctx: &ExprContext,
        operand: &'a Option<Box<ASTNode>>,
        conditions: &'a [ASTNode],
        results: &'a [ASTNode],
        else_result: &'a Option<Box<ASTNode>>,
        plan: &SQLPlan,
    ) -> Result<(Expr, Type), failure::Error> {
        let mut case_exprs = Vec::new();
        let mut result_type: Option<Type> = None;
        for (c, r) in conditions.iter().zip(results) {
            let c = match operand {
                Some(operand) => ASTNode::SQLBinaryExpr {
                    left: operand.clone(),
                    op: SQLOperator::Eq,
                    right: Box::new(c.clone()),
                },
                None => c.clone(),
            };
            let (cexpr, ctype) = self.plan_expr(ctx, &c, plan)?;
            if ctype.ftype != FType::Bool {
                bail!("CASE expression has non-boolean type {:?}", ctype.ftype);
            }
            let (rexpr, rtype) = self.plan_expr(ctx, r, plan)?;
            match &result_type {
                Some(result_type) => {
                    if result_type.ftype != rtype.ftype {
                        bail!(
                            "CASE expression does not have uniform result type: {:?} vs {:?}",
                            result_type.ftype,
                            rtype.ftype
                        );
                    }
                }
                None => result_type = Some(rtype),
            }
            case_exprs.push((cexpr, rexpr));
        }
        // conditions and results must be non-empty, which implies that
        // result_type must be non-None.
        let result_type = result_type.unwrap();
        let mut expr = match else_result {
            Some(else_result) => {
                let (expr, typ) = self.plan_expr(ctx, else_result, plan)?;
                if typ.ftype != result_type.ftype {
                    bail!(
                        "CASE expression does not have uniform result type: {:?} vs {:?}",
                        result_type.ftype,
                        typ.ftype
                    );
                }
                expr
            }
            None => Expr::Literal(Datum::Null),
        };
        for (cexpr, rexpr) in case_exprs.into_iter().rev() {
            expr = Expr::If {
                cond: Box::new(cexpr),
                then: Box::new(rexpr),
                els: Box::new(expr),
            }
        }
        Ok((expr, result_type))
    }

    fn plan_literal<'a>(&self, l: &'a Value) -> Result<(Expr, Type), failure::Error> {
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

struct ExprContext {
    scope: &'static str,
    allow_aggregates: bool,
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

fn parse_kafka_url(url: &str) -> Result<(SocketAddr, String), failure::Error> {
    let url: Url = url.parse()?;
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
    // We already checked for kafka scheme above, so it's safe to assume port
    // 9092.
    let addr = url
        .with_default_port(|_| Ok(9092))?
        .to_socket_addrs()?
        .next()
        .unwrap();
    Ok((addr, topic))
}

impl Planner {
    pub fn mock<I>(dataflows: I) -> Self
    where
        I: IntoIterator<Item = (String, Type)>,
    {
        Planner {
            dataflows: dataflows
                .into_iter()
                .map(|(name, typ)| {
                    Dataflow::Source(Source {
                        name,
                        connector: SourceConnector::Local(LocalSourceConnector {}),
                        typ,
                    })
                })
                .collect(),
        }
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
        let planner = Planner::mock(vec![("src".into(), typ)]);

        let stmts = SQLParser::parse_sql(
            &AnsiSqlDialect {},
            "CREATE MATERIALIZED VIEW v AS SELECT b FROM src".into(),
        )?;
        let dataflow = planner.plan_statement(&stmts[0])?;
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
        let planner = Planner::mock(vec![]);

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
        let dataflow = planner.plan_statement(&stmts[0])?;
        assert_eq!(
            dataflow,
            Dataflow::Source(Source {
                name: "s".into(),
                connector: SourceConnector::Kafka(KafkaSourceConnector {
                    addr: "127.0.0.1:9092".parse()?,
                    topic: "topic".into(),
                    raw_schema: raw_schema.to_owned(),
                    schema_registry_url: None,
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
        let planner = Planner::mock(vec![("src".into(), typ)]);

        let stmts = SQLParser::parse_sql(
            &AnsiSqlDialect {},
            "CREATE MATERIALIZED VIEW v AS SELECT 1 + sum(a + 1) FROM src GROUP BY b".into(),
        )?;
        let dataflow = planner.plan_statement(&stmts[0])?;
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
                            },
                            distinct: false,
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

    // #[test]
    // fn test_basic_join() -> Result<(), failure::Error> {
    //     let src1_type = Type {
    //         name: None,
    //         nullable: false,
    //         ftype: FType::Tuple(vec![
    //             Type {
    //                 name: Some("a".into()),
    //                 nullable: false,
    //                 ftype: FType::Int64,
    //             },
    //             Type {
    //                 name: Some("b".into()),
    //                 nullable: false,
    //                 ftype: FType::Int64,
    //             },
    //         ]),
    //     };
    //     let src2_type = Type {
    //         name: None,
    //         nullable: false,
    //         ftype: FType::Tuple(vec![
    //             Type {
    //                 name: Some("c".into()),
    //                 nullable: false,
    //                 ftype: FType::Int64,
    //             },
    //             Type {
    //                 name: Some("d".into()),
    //                 nullable: false,
    //                 ftype: FType::Int64,
    //             },
    //         ]),
    //     };
    //     let planner = Planner::mock(vec![("src1".into(), src1_type), ("src2".into(), src2_type)]);

    //     let stmts = SQLParser::parse_sql(
    //         &AnsiSqlDialect {},
    //         "CREATE MATERIALIZED VIEW v AS SELECT a, b, d FROM src1 JOIN src2 ON c = b".into(),
    //     )?;
    //     let dataflow = planner.plan_statement(&stmts[0])?;
    //     assert_eq!(
    //         dataflow,
    //         Dataflow::View(View {
    //             name: "v".into(),
    //             plan: Plan::Project {
    //                 outputs: vec![
    //                     Expr::Column(0, Box::new(Expr::Ambient)),
    //                     Expr::Column(1, Box::new(Expr::Ambient)),
    //                     Expr::Column(3, Box::new(Expr::Ambient)),
    //                 ],
    //                 input: Box::new(Plan::Join {
    //                     left_key: Expr::Tuple(vec![Expr::Column(1, Box::new(Expr::Ambient))]),
    //                     right_key: Expr::Tuple(vec![Expr::Column(0, Box::new(Expr::Ambient))]),
    //                     left: Box::new(Plan::Source("src1".into())),
    //                     right: Box::new(Plan::Source("src2".into())),
    //                     include_left_outer: None,
    //                     include_right_outer: None,
    //                 }),
    //             },
    //             typ: Type {
    //                 name: None,
    //                 nullable: false,
    //                 ftype: FType::Tuple(vec![
    //                     Type {
    //                         name: Some("a".into()),
    //                         nullable: false,
    //                         ftype: FType::Int64,
    //                     },
    //                     Type {
    //                         name: Some("b".into()),
    //                         nullable: false,
    //                         ftype: FType::Int64,
    //                     },
    //                     Type {
    //                         name: Some("d".into()),
    //                         nullable: false,
    //                         ftype: FType::Int64,
    //                     },
    //                 ]),
    //             }
    //         })
    //     );

    //     Ok(())
    // }

    #[test]
    fn test_basic_multiwayjoin() -> Result<(), failure::Error> {
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
        let planner = Planner::mock(vec![("src1".into(), src1_type), ("src2".into(), src2_type)]);

        let stmts = SQLParser::parse_sql(
            &AnsiSqlDialect {},
            "CREATE MATERIALIZED VIEW v AS SELECT a, b, d FROM src1 JOIN src2 ON src2.c = src1.b"
                .into(),
        )?;
        let dataflow = planner.plan_statement(&stmts[0])?;
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
                    input: Box::new(Plan::MultiwayJoin {
                        plans: vec![Plan::Source("src1".into()), Plan::Source("src2".into())],
                        arities: vec![2, 2],
                        equalities: vec![((1, 0), (0, 1))],
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
        let planner = Planner::mock(vec![("src1".into(), src1_type), ("src2".into(), src2_type)]);

        let stmts = SQLParser::parse_sql(
            &AnsiSqlDialect {},
            "CREATE MATERIALIZED VIEW v AS SELECT a, b FROM src1 UNION ALL SELECT a, b FROM src2"
                .into(),
        )?;
        let dataflow = planner.plan_statement(&stmts[0])?;
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
