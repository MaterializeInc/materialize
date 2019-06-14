// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL-dataflow translation.

use failure::{bail, format_err};
use itertools::Itertools;
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::visit::{self, Visit};
use sqlparser::sqlast::{
    ASTNode, JoinConstraint, JoinOperator, SQLBinaryOperator, SQLFunction, SQLIdent, SQLObjectName,
    SQLObjectType, SQLQuery, SQLSelect, SQLSelectItem, SQLSetExpr, SQLSetOperator, SQLStatement,
    SQLType, SQLUnaryOperator, SQLValues, SourceSchema, TableAlias, TableConstraint, TableFactor,
    TableWithJoins, Value,
};
use sqlparser::sqlparser::Parser as SQLParser;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::iter::FromIterator;
use std::net::{SocketAddr, ToSocketAddrs};
use url::Url;

use crate::dataflow::func::{AggregateFunc, BinaryFunc, UnaryFunc, VariadicFunc};
use crate::dataflow::{
    AggregateExpr, Dataflow, KafkaSinkConnector, KafkaSourceConnector, LocalSourceConnector,
    RelationExpr, ScalarExpr, Sink, SinkConnector, Source, SourceConnector, View,
};
use crate::glue::*;
use crate::interchange::avro;
use crate::repr::{ColumnType, Datum, RelationType, ScalarType};
use ore::collections::CollectionExt;
use ore::iter::{FallibleIteratorExt, IteratorExt};
use ore::option::OptionExt;
use store::{DataflowStore, RemoveMode};

mod store;

/// Converts raw SQL queries into dataflow commands.
#[derive(Debug, Default)]
pub struct Planner {
    dataflows: DataflowStore,
}

/// The result of planning a SQL query.
///
/// The `Ok` variant bundles a [`SQLResponse`] and an optional
/// [`DataflowCommand`]. The `SQLResponse` is meant for the end user, while the
/// `DataflowCommand` is meant to drive a running dataflow server. Typically the
/// `SQLResponse` can be sent directly to the client, though
/// [`SQLResponse::Peeking`] requires special handling in order to route results
/// from the dataflow server to the client.
pub type PlannerResult = Result<(SqlResponse, Option<DataflowCommand>), failure::Error>;

impl Planner {
    /// Parses and plans a raw SQL query. See the documentation for
    /// [`PlannerResult`] for details about the meaning of the return time.
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
            SQLStatement::SQLCreateSource { .. }
            | SQLStatement::SQLCreateSink { .. }
            | SQLStatement::SQLCreateView { .. }
            | SQLStatement::SQLCreateTable { .. } => self.handle_create_dataflow(stmt),
            SQLStatement::SQLDrop { .. } => self.handle_drop_dataflow(stmt),

            // these are intended mostly for testing:
            SQLStatement::SQLQuery(query) => self.handle_select(*query),
            SQLStatement::SQLInsert {
                table_name,
                columns,
                source,
            } => self.handle_insert(table_name, columns, *source),

            _ => bail!("unsupported SQL statement: {:?}", stmt),
        }
    }

    fn handle_create_dataflow(&mut self, stmt: SQLStatement) -> PlannerResult {
        let dataflow = self.plan_statement(&stmt)?;
        let sql_response = match stmt {
            SQLStatement::SQLCreateSource { .. } => SqlResponse::CreatedSource,
            SQLStatement::SQLCreateSink { .. } => SqlResponse::CreatedSink,
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
        let (object_type, if_exists, names, cascade) = match stmt {
            SQLStatement::SQLDrop {
                object_type,
                if_exists,
                names,
                cascade,
            } => (object_type, if_exists, names, cascade),
            _ => unreachable!(),
        };
        // TODO(benesch): DROP <TYPE> should error if the named object is not
        // of the correct type (#38).
        let names: Vec<String> = Result::from_iter(names.iter().map(extract_sql_object_name))?;
        if !if_exists {
            // Without IF EXISTS, we need to verify that every named
            // dataflow exists before proceeding with the drop
            // implementation.
            for name in &names {
                let _ = self.dataflows.get(name)?;
            }
        }
        let mode = RemoveMode::from_cascade(cascade);
        let mut removed = vec![];
        for name in &names {
            self.dataflows.remove(name, mode, &mut removed)?;
        }
        let sql_response = match object_type {
            SQLObjectType::Source => SqlResponse::DroppedSource,
            SQLObjectType::Table => SqlResponse::DroppedTable,
            SQLObjectType::View => SqlResponse::DroppedView,
            _ => unreachable!(),
        };
        Ok((sql_response, Some(DataflowCommand::DropDataflows(removed))))
    }

    fn handle_peek(&mut self, name: SQLObjectName) -> PlannerResult {
        let name = name.to_string();
        let dataflow = self.dataflows.get(&name)?.clone();
        Ok((
            SqlResponse::Peeking {
                typ: dataflow.typ().clone(),
            },
            Some(DataflowCommand::PeekExisting {
                dataflow,
                when: PeekWhen::Immediately,
            }),
        ))
    }

    fn handle_select(&mut self, query: SQLQuery) -> PlannerResult {
        let id: u64 = rand::random();
        let name = format!("<temp_{}>", id);
        let dataflow = self.plan_statement(&SQLStatement::SQLCreateView {
            name: SQLObjectName(vec![name.clone()]),
            columns: vec![],
            query: Box::new(query),
            materialized: true,
            with_options: vec![],
        })?;
        if let Dataflow::View(view) = dataflow {
            Ok((
                SqlResponse::Peeking {
                    typ: view.typ.clone(),
                },
                Some(DataflowCommand::PeekTransient {
                    view,
                    when: PeekWhen::AfterFlush,
                }),
            ))
        } else {
            panic!("Got a non-view dataflow for a SELECT: {:?}", dataflow);
        }
    }

    fn handle_insert(
        &mut self,
        name: SQLObjectName,
        columns: Vec<SQLIdent>,
        source: SQLQuery,
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
        let permutation = if columns.is_empty() {
            // if not specified, just insert in natural order
            (0..typ.column_types.len()).collect::<Vec<_>>()
        } else {
            // otherwise, check that we have a sensible list of columns
            if HashSet::<&String>::from_iter(&columns).len() != columns.len() {
                bail!(
                    "Duplicate column in INSERT INTO ... COLUMNS ({})",
                    columns.join(", ")
                );
            }
            let expected_columns = typ
                .column_types
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

        if !source.ctes.is_empty() || !source.order_by.is_empty() || source.limit.is_some() {
            bail!("complicated INSERTs are not supported");
        }

        let datums = match source.body {
            SQLSetExpr::Values(SQLValues(values)) => {
                assert!(!values.is_empty());
                if !values.iter().map(Vec::len).all_equal() {
                    bail!("VALUES lists must all be the same length");
                }
                if values[0].len() != permutation.len() {
                    bail!(
                        "INSERT has {} expression(s) but {} target column(s)",
                        values[0].len(),
                        permutation.len()
                    );
                }
                values
                    .into_iter()
                    .map(|asts| {
                        let permuted_asts = permutation.iter().map(|i| asts[*i].clone());
                        let datums = permuted_asts
                            .zip(typ.column_types.iter())
                            .map(|(ast, typ)| Datum::from_sql(ast, typ))
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok(datums)
                    })
                    .collect::<Result<Vec<_>, failure::Error>>()?
            }
            _ => bail!("complicated INSERTs are not supported"),
        };

        Ok((
            SqlResponse::Inserted(datums.len()),
            Some(DataflowCommand::Insert(name, datums)),
        ))
    }
}

impl Datum {
    pub fn from_sql(ast: ASTNode, typ: &ColumnType) -> Result<Self, failure::Error> {
        Ok(match ast {
            ASTNode::SQLValue(value) => match (value, &typ.scalar_type) {
                (Value::Null, _) => {
                    if typ.nullable {
                        Datum::Null
                    } else {
                        bail!("Tried to insert null into non-nullable column")
                    }
                }
                (Value::Long(l), ScalarType::Int64) => Datum::Int64(l as i64), // TODO(benesch): safe conversion
                (Value::Double(f), ScalarType::Float64) => Datum::Float64(f),
                (Value::SingleQuotedString(s), ScalarType::String)
                | (Value::NationalStringLiteral(s), ScalarType::String) => Datum::String(s),
                (Value::Boolean(b), ScalarType::Bool) => {
                    if b {
                        Datum::True
                    } else {
                        Datum::False
                    }
                }
                (value, scalar_type) => bail!(
                    "Don't know how to insert value {:?} into column of type {:?}",
                    value,
                    scalar_type
                ),
            },
            other => bail!("Can only insert plain values, not {:?}", other),
        })
    }
}

impl Planner {
    fn plan_statement(&self, stmt: &SQLStatement) -> Result<Dataflow, failure::Error> {
        match stmt {
            SQLStatement::SQLCreateView {
                name,
                columns,
                query,
                materialized: _,
                with_options,
            } => {
                if !with_options.is_empty() {
                    bail!("WITH options are not yet supported");
                }
                let relation_expr = self.plan_view_query(query)?;
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
                Ok(Dataflow::View(View {
                    name: extract_sql_object_name(name)?,
                    relation_expr,
                    typ,
                }))
            }
            SQLStatement::SQLCreateSource {
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
                    SourceSchema::Raw(schema) => (schema.to_owned(), None),
                    SourceSchema::Registry(url) => {
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
            SQLStatement::SQLCreateSink {
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
                let (addr, topic) = parse_kafka_url(url)?;
                Ok(Dataflow::Sink(Sink {
                    name,
                    from: (from, dataflow.typ().clone()),
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
                constraints,
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
                for constraint in constraints {
                    match constraint {
                        TableConstraint::Check { .. } => {
                            bail!("CHECK constraints are not supported")
                        }
                        TableConstraint::ForeignKey { .. } => {
                            bail!("FOREIGN KEY constraints are not supported")
                        }
                        // TODO(benesch): we should probably actually enforce
                        // this, if we're going to allow it.
                        TableConstraint::Unique { .. } => (),
                    }
                }
                let types = columns
                    .iter()
                    .map(|column| {
                        Ok(ColumnType {
                            name: Some(column.name.clone()),
                            scalar_type: match &column.data_type {
                                SQLType::Char(_) | SQLType::Varchar(_) | SQLType::Text => {
                                    ScalarType::String
                                }
                                SQLType::SmallInt | SQLType::Int | SQLType::BigInt => {
                                    ScalarType::Int64
                                }
                                SQLType::Float(_) | SQLType::Real | SQLType::Double => {
                                    ScalarType::Float64
                                }
                                SQLType::Decimal(scale, precision) => ScalarType::Decimal(
                                    scale.unwrap_or(0) as usize,
                                    precision.unwrap_or(0) as usize,
                                ),
                                SQLType::Date => ScalarType::Date,
                                SQLType::Timestamp => ScalarType::Timestamp,
                                SQLType::Time => ScalarType::Time,
                                other => bail!("Unexpected SQL type: {:?}", other),
                            },
                            nullable: true,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Dataflow::Source(Source {
                    name: extract_sql_object_name(name)?,
                    connector: SourceConnector::Local(LocalSourceConnector {}),
                    typ: RelationType {
                        column_types: types,
                    },
                }))
            }
            other => bail!("Unsupported statement: {:?}", other),
        }
    }

    fn plan_view_query(&self, q: &SQLQuery) -> Result<RelationExpr, failure::Error> {
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

    fn plan_set_expr(&self, q: &SQLSetExpr) -> Result<RelationExpr, failure::Error> {
        match q {
            SQLSetExpr::Select(select) => self.plan_view_select(select),
            SQLSetExpr::SetOperation {
                op: SQLSetOperator::Union,
                all,
                left,
                right,
            } => {
                let relation_expr = RelationExpr::Union {
                    left: Box::new(self.plan_set_expr(left)?),
                    right: Box::new(self.plan_set_expr(right)?),
                };
                let relation_expr = if *all {
                    relation_expr
                } else {
                    RelationExpr::Distinct {
                        input: Box::new(relation_expr),
                    }
                };
                Ok(relation_expr)
            }
            _ => bail!("set operations are not yet supported"),
        }
    }

    fn plan_view_select(&self, s: &SQLSelect) -> Result<RelationExpr, failure::Error> {
        // Step 1. Handle FROM clause, including joins.
        let (mut relation_expr, from_scope) = s
            .from
            .iter()
            .map(|twj| self.plan_table_with_joins(twj))
            .fallible()
            .fold1(|(left, left_scope), (right, right_scope)| {
                self.plan_join_operator(&JoinOperator::Cross, left, left_scope, right, right_scope)
            })
            .unwrap_or_else(|| {
                let name = "dual";
                let typ = self.dataflows.get_type(&name)?;
                Ok((
                    RelationExpr::Get {
                        name: name.to_owned(),
                        typ: typ.clone(),
                    },
                    Scope::from_source(name, typ.clone()),
                ))
            })?;

        // Step 2. Handle WHERE clause.
        if let Some(selection) = &s.selection {
            let ctx = &ExprContext {
                name: "WHERE clause",
                scope: &from_scope,
                aggregate_context: None,
            };
            let (expr, typ) = self.plan_expr(ctx, &selection)?;
            if typ.scalar_type != ScalarType::Bool {
                bail!(
                    "WHERE clause must have boolean type, not {:?}",
                    typ.scalar_type
                );
            }
            relation_expr = relation_expr.filter(vec![expr]);
        }

        // Step 3. Handle GROUP BY clause.
        let (group_scope, aggregate_context) = {
            // gather group columns
            let ctx = &ExprContext {
                name: "GROUP BY clause",
                scope: &from_scope,
                aggregate_context: None,
            };
            let mut group_key = vec![];
            let mut group_exprs = vec![];
            let mut group_scope = Scope { items: vec![] };
            for expr in &s.group_by {
                let (expr, typ) = self.plan_expr(ctx, &expr)?;
                match &expr {
                    ScalarExpr::Column(i) => {
                        // repeated exprs in GROUP BY confuse name resolution later, and dropping them doesn't change the results
                        if !group_key.contains(i) {
                            group_key.push(*i);
                            group_scope.items.push(from_scope.items[*i].clone());
                        }
                    }
                    _ => {
                        group_key.push(from_scope.len() + group_exprs.len());
                        group_exprs.push((expr, typ.clone()));
                        group_scope.items.push(ScopeItem {
                            table_name: None,
                            column_name: None,
                            typ,
                        });
                    }
                }
            }
            // gather aggregates
            let mut aggregate_visitor = AggregateFuncVisitor::new();
            for p in &s.projection {
                aggregate_visitor.visit_select_item(p);
            }
            if let Some(having) = &s.having {
                aggregate_visitor.visit_expr(having);
            }
            let ctx = &ExprContext {
                name: "aggregate function",
                scope: &from_scope,
                aggregate_context: None,
            };
            let mut aggregate_context = HashMap::new();
            let mut aggregates = vec![];
            for sql_function in aggregate_visitor.into_result()? {
                let (expr, typ) = self.plan_aggregate(ctx, sql_function)?;
                aggregate_context.insert(
                    sql_function,
                    (group_key.len() + aggregates.len(), typ.clone()),
                );
                aggregates.push((expr, typ.clone()));
                group_scope.items.push(ScopeItem {
                    table_name: None,
                    column_name: None,
                    typ,
                });
            }
            if !aggregates.is_empty() || !group_key.is_empty() {
                // apply GROUP BY / aggregates
                relation_expr = relation_expr
                    .map(group_exprs)
                    .reduce(group_key.clone(), aggregates.clone());
                if group_key.is_empty() {
                    relation_expr = RelationExpr::OrDefault {
                        input: Box::new(relation_expr),
                        default: aggregates
                            .iter()
                            .map(|(agg, _)| agg.func.default())
                            .collect(),
                    }
                }
                (group_scope, aggregate_context)
            } else {
                // if no GROUP BY or aggregates, all columns remain in scope
                (from_scope.clone(), aggregate_context)
            }
        };

        // Step 4. Handle HAVING clause.
        if let Some(having) = &s.having {
            let ctx = &ExprContext {
                name: "HAVING clause",
                scope: &group_scope,
                aggregate_context: Some(&aggregate_context),
            };
            let (expr, typ) = self.plan_expr(ctx, having)?;
            if typ.scalar_type != ScalarType::Bool {
                bail!(
                    "HAVING clause must have boolean type, not {:?}",
                    typ.scalar_type
                );
            }
            relation_expr = relation_expr.filter(vec![expr]);
        }

        // Step 5. Handle projections.
        {
            let mut project_exprs = vec![];
            let mut project_key = vec![];
            for p in &s.projection {
                let ctx = &ExprContext {
                    name: "SELECT clause",
                    scope: &group_scope,
                    aggregate_context: Some(&aggregate_context),
                };
                for (expr, typ) in self.plan_select_item(ctx, p, &from_scope)? {
                    match &expr {
                        ScalarExpr::Column(i) => {
                            project_key.push(*i);
                        }
                        _ => {
                            project_key.push(group_scope.len() + project_exprs.len());
                            project_exprs.push((expr, typ));
                        }
                    }
                }
            }
            relation_expr = relation_expr.map(project_exprs).project(project_key);
        }

        // Step 6. Handle DISTINCT.
        if s.distinct {
            relation_expr = relation_expr.distinct();
        }

        Ok(relation_expr)
    }

    fn plan_table_with_joins<'a>(
        &self,
        table_with_joins: &'a TableWithJoins,
    ) -> Result<(RelationExpr, Scope), failure::Error> {
        let (mut left, mut left_scope) = self.plan_table_factor(&table_with_joins.relation)?;
        for join in &table_with_joins.joins {
            let (right, right_scope) = self.plan_table_factor(&join.relation)?;
            let (new_left, new_left_scope) =
                self.plan_join_operator(&join.join_operator, left, left_scope, right, right_scope)?;
            left = new_left;
            left_scope = new_left_scope;
        }
        Ok((left, left_scope))
    }

    fn plan_select_item<'a>(
        &self,
        ctx: &ExprContext,
        s: &'a SQLSelectItem,
        select_all_scope: &Scope,
    ) -> Result<Vec<(ScalarExpr, ColumnType)>, failure::Error> {
        match s {
            SQLSelectItem::UnnamedExpression(e) => Ok(vec![self.plan_expr(ctx, e)?]),
            SQLSelectItem::ExpressionWithAlias { expr, alias } => {
                let (expr, mut typ) = self.plan_expr(ctx, expr)?;
                typ.name = Some(alias.clone());
                Ok(vec![(expr, typ)])
            }
            SQLSelectItem::Wildcard => select_all_scope
                .items
                .iter()
                .filter(|item| item.table_name.is_some() && item.column_name.is_some())
                .map(|item| {
                    let (pos, item) = ctx.scope.resolve_table_column(
                        item.table_name.as_ref().unwrap(),
                        item.column_name.as_ref().unwrap(),
                    )?;
                    Ok((ScalarExpr::Column(pos), item.typ.clone()))
                })
                .collect::<Result<Vec<_>, _>>(),
            SQLSelectItem::QualifiedWildcard(table_name) => {
                let table_name = extract_sql_object_name(table_name)?;
                select_all_scope
                    .items
                    .iter()
                    .filter(|item| {
                        item.table_name.as_ref() == Some(&table_name) && item.column_name.is_some()
                    })
                    .map(|item| {
                        let (pos, item) = ctx.scope.resolve_table_column(
                            item.table_name.as_ref().unwrap(),
                            item.column_name.as_ref().unwrap(),
                        )?;
                        Ok((ScalarExpr::Column(pos), item.typ.clone()))
                    })
                    .collect::<Result<Vec<_>, _>>()
            }
        }
    }

    fn plan_table_factor<'a>(
        &self,
        table_factor: &'a TableFactor,
    ) -> Result<(RelationExpr, Scope), failure::Error> {
        match table_factor {
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
                let name = extract_sql_object_name(name)?;
                let typ = self.dataflows.get_type(&name)?;
                let expr = RelationExpr::Get {
                    name: name.clone(),
                    typ: typ.clone(),
                };
                let mut scope = Scope::from_source(&name, typ.clone());
                if let Some(TableAlias { name, columns }) = alias {
                    if !columns.is_empty() {
                        bail!("aliasing columns is not yet supported");
                    }
                    scope = scope.alias_table(&name);
                }
                Ok((expr, scope))
            }
            TableFactor::Derived { .. } => {
                bail!("subqueries are not yet supported");
            }
            TableFactor::NestedJoin(table_with_joins) => {
                self.plan_table_with_joins(table_with_joins)
            }
        }
    }

    fn plan_join_operator(
        &self,
        operator: &JoinOperator,
        left: RelationExpr,
        left_scope: Scope,
        right: RelationExpr,
        right_scope: Scope,
    ) -> Result<(RelationExpr, Scope), failure::Error> {
        match operator {
            JoinOperator::Inner(constraint) => {
                self.plan_join_constraint(&constraint, left, left_scope, right, right_scope)
            }
            JoinOperator::LeftOuter(constraint) => RelationExpr::let_(left, |left| {
                let (both, both_scope) = self.plan_join_constraint(
                    &constraint,
                    left.clone(),
                    left_scope,
                    right,
                    right_scope,
                )?;
                RelationExpr::let_(both, |both| {
                    let both_and_outer = both.clone().union(both.left_outer(left));
                    Ok((both_and_outer, both_scope))
                })
            }),
            JoinOperator::RightOuter(constraint) => RelationExpr::let_(right, |right| {
                let (both, both_scope) = self.plan_join_constraint(
                    &constraint,
                    left,
                    left_scope,
                    right.clone(),
                    right_scope,
                )?;
                RelationExpr::let_(both, |both| {
                    let both_and_outer = both.clone().union(both.right_outer(right));
                    Ok((both_and_outer, both_scope))
                })
            }),
            JoinOperator::FullOuter(constraint) => RelationExpr::let_(left, |left| {
                RelationExpr::let_(right, |right| {
                    let (both, both_scope) = self.plan_join_constraint(
                        &constraint,
                        left.clone(),
                        left_scope,
                        right.clone(),
                        right_scope,
                    )?;
                    RelationExpr::let_(both, |both| {
                        let both_and_outer = both
                            .clone()
                            .union(both.clone().left_outer(left))
                            .union(both.clone().right_outer(right));
                        Ok((both_and_outer, both_scope))
                    })
                })
            }),
            JoinOperator::Cross => Ok((left.product(right), left_scope.product(right_scope))),
        }
    }

    fn plan_join_constraint<'a>(
        &self,
        constraint: &'a JoinConstraint,
        left: RelationExpr,
        left_scope: Scope,
        right: RelationExpr,
        right_scope: Scope,
    ) -> Result<(RelationExpr, Scope), failure::Error> {
        match constraint {
            JoinConstraint::On(expr) => {
                let product = left.product(right);
                let product_scope = left_scope.product(right_scope);
                let ctx = &ExprContext {
                    name: "ON clause",
                    scope: &product_scope,
                    aggregate_context: None,
                };
                let (predicate, _) = self.plan_expr(ctx, expr)?;
                Ok((product.filter(vec![predicate]), product_scope))
            }
            JoinConstraint::Using(column_names) => {
                let (predicate, project_key) =
                    self.plan_using_constraint(&column_names, &left_scope, &right_scope)?;
                Ok((
                    left.product(right)
                        .filter(vec![predicate])
                        .project(project_key.clone()),
                    left_scope.product(right_scope).project(&project_key),
                ))
            }
            JoinConstraint::Natural => {
                let mut column_names = HashSet::new();
                for item in right_scope.items.iter() {
                    if let Some(column_name) = &item.column_name {
                        if left_scope.resolve_column(column_name).is_ok() {
                            column_names.insert(column_name.clone());
                        }
                    }
                }
                let column_names = column_names.into_iter().collect::<Vec<_>>();
                let (predicate, project_key) =
                    self.plan_using_constraint(&column_names, &left_scope, &right_scope)?;
                Ok((
                    left.product(right)
                        .filter(vec![predicate])
                        .project(project_key.clone()),
                    left_scope.product(right_scope).project(&project_key),
                ))
            }
        }
    }

    fn plan_using_constraint(
        &self,
        column_names: &[String],
        left: &Scope,
        right: &Scope,
    ) -> Result<(ScalarExpr, Vec<usize>), failure::Error> {
        let mut exprs = vec![];
        let mut dropped_columns = HashSet::new();
        for column_name in column_names {
            let (l, _) = left.resolve_column(column_name)?;
            let (r, _) = right.resolve_column(column_name)?;
            exprs.push(ScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1: Box::new(ScalarExpr::Column(l)),
                expr2: Box::new(ScalarExpr::Column(left.len() + r)),
            });
            dropped_columns.insert(r);
        }
        let expr = exprs
            .into_iter()
            .fold(ScalarExpr::Literal(Datum::True), |a, b| {
                ScalarExpr::CallBinary {
                    func: BinaryFunc::And,
                    expr1: Box::new(a),
                    expr2: Box::new(b),
                }
            });
        let project_key = (0..left.len())
            .chain(
                (0..right.len())
                    // drop columns on the right that were joined
                    .filter(|r| !dropped_columns.contains(r))
                    .map(|r| left.len() + r),
            )
            .collect::<Vec<_>>();
        Ok((expr, project_key))
    }

    fn plan_expr<'a>(
        &self,
        ctx: &ExprContext,
        e: &'a ASTNode,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        match e {
            ASTNode::SQLIdentifier(name) => {
                let (i, item) = ctx.scope.resolve_column(name)?;
                let expr = ScalarExpr::Column(i);
                Ok((expr, item.typ.clone()))
            }
            ASTNode::SQLCompoundIdentifier(names) if names.len() == 2 => {
                let (i, item) = ctx.scope.resolve_table_column(&names[0], &names[1])?;
                let expr = ScalarExpr::Column(i);
                Ok((expr, item.typ.clone()))
            }
            ASTNode::SQLValue(val) => self.plan_literal(val),
            // TODO(benesch): why isn't IS [NOT] NULL a unary op?
            ASTNode::SQLIsNull(expr) => self.plan_is_null_expr(ctx, expr, false),
            ASTNode::SQLIsNotNull(expr) => self.plan_is_null_expr(ctx, expr, true),
            ASTNode::SQLUnaryOp { op, expr } => self.plan_unary_op(ctx, op, expr),
            ASTNode::SQLBinaryOp { op, left, right } => self.plan_binary_op(ctx, op, left, right),
            ASTNode::SQLBetween {
                expr,
                low,
                high,
                negated,
            } => self.plan_between(ctx, expr, low, high, *negated),
            ASTNode::SQLInList {
                expr,
                list,
                negated,
            } => self.plan_in_list(ctx, expr, list, *negated),
            ASTNode::SQLCase {
                operand,
                conditions,
                results,
                else_result,
            } => self.plan_case(ctx, operand, conditions, results, else_result),
            ASTNode::SQLNested(expr) => self.plan_expr(ctx, expr),
            ASTNode::SQLCast { expr, data_type } => self.plan_cast(ctx, expr, data_type),
            ASTNode::SQLFunction(func) => self.plan_function(ctx, func),
            _ => bail!(
                "complicated expressions are not yet supported: {}",
                e.to_string()
            ),
        }
    }

    fn plan_cast<'a>(
        &self,
        ctx: &ExprContext,
        expr: &'a ASTNode,
        data_type: &'a SQLType,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let to_scalar_type = match data_type {
            SQLType::Varchar(_) => ScalarType::String,
            SQLType::Text => ScalarType::String,
            SQLType::Bytea => ScalarType::Bytes,
            SQLType::Float(_) => ScalarType::Float64,
            SQLType::Real => ScalarType::Float64,
            SQLType::Double => ScalarType::Float64,
            SQLType::SmallInt => ScalarType::Int32,
            SQLType::Int => ScalarType::Int64,
            SQLType::BigInt => ScalarType::Int64,
            SQLType::Boolean => ScalarType::Bool,
            _ => bail!("CAST ... AS {} is not yet supported", data_type.to_string()),
        };
        let (expr, from_type) = self.plan_expr(ctx, expr)?;
        let func = match (&from_type.scalar_type, &to_scalar_type) {
            (ScalarType::Int32, ScalarType::Float32) => Some(UnaryFunc::CastInt32ToFloat32),
            (ScalarType::Int32, ScalarType::Float64) => Some(UnaryFunc::CastInt32ToFloat64),
            (ScalarType::Int64, ScalarType::Int32) => Some(UnaryFunc::CastInt64ToInt32),
            (ScalarType::Int64, ScalarType::Float32) => Some(UnaryFunc::CastInt64ToFloat32),
            (ScalarType::Int64, ScalarType::Float64) => Some(UnaryFunc::CastInt64ToFloat64),
            (ScalarType::Float32, ScalarType::Int64) => Some(UnaryFunc::CastFloat32ToInt64),
            (ScalarType::Float32, ScalarType::Float64) => Some(UnaryFunc::CastFloat32ToFloat64),
            (ScalarType::Float64, ScalarType::Int64) => Some(UnaryFunc::CastFloat64ToInt64),
            (ScalarType::Null, _) => None,
            (from, to) => {
                if from != to {
                    bail!("CAST does not support casting from {:?} to {:?}", from, to);
                }
                None
            }
        };
        let expr = match func {
            Some(func) => ScalarExpr::CallUnary {
                func,
                expr: Box::new(expr),
            },
            None => expr,
        };
        let to_type = ColumnType {
            name: None,
            nullable: from_type.nullable,
            scalar_type: to_scalar_type,
        };
        Ok((expr, to_type))
    }

    fn plan_aggregate(
        &self,
        ctx: &ExprContext,
        sql_func: &SQLFunction,
    ) -> Result<(AggregateExpr, ColumnType), failure::Error> {
        let ident = sql_func.name.to_string().to_lowercase();
        assert!(AggregateFunc::is_aggregate_func(&ident));

        if sql_func.over.is_some() {
            bail!("window functions are not yet supported");
        }

        if sql_func.args.len() != 1 {
            bail!("{} function only takes one argument", ident);
        }

        let arg = &sql_func.args[0];
        let (expr, func, scalar_type) = match (&*ident, arg) {
            // COUNT(*) is a special case that doesn't compose well
            ("count", ASTNode::SQLWildcard) => (
                ScalarExpr::Literal(Datum::Null),
                AggregateFunc::CountAll,
                ScalarType::Int64,
            ),
            _ => {
                let (expr, typ) = self.plan_expr(ctx, arg)?;
                let (func, scalar_type) =
                    AggregateFunc::from_name_and_scalar_type(&ident, &typ.scalar_type)?;
                (expr, func, scalar_type)
            }
        };
        let typ = ColumnType {
            name: None,
            nullable: func.is_nullable(),
            scalar_type,
        };
        Ok((
            AggregateExpr {
                func,
                expr,
                distinct: sql_func.distinct,
            },
            typ,
        ))
    }

    fn plan_function<'a>(
        &self,
        ctx: &ExprContext,
        sql_func: &'a SQLFunction,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let ident = sql_func.name.to_string().to_lowercase();
        if AggregateFunc::is_aggregate_func(&ident) {
            if let Some(agg_cx) = ctx.aggregate_context {
                let (i, typ) = agg_cx.get(sql_func).ok_or_else(|| {
                    format_err!(
                        "Internal error: encountered unplanned aggregate function: {:?}",
                        sql_func,
                    )
                })?;
                Ok((ScalarExpr::Column(*i), typ.clone()))
            } else {
                bail!("aggregate functions are not allowed in {}", ctx.name);
            }
        } else {
            match ident.as_str() {
                "abs" => {
                    if sql_func.args.len() != 1 {
                        bail!("abs expects one argument, got {}", sql_func.args.len());
                    }
                    let (expr, typ) = self.plan_expr(ctx, &sql_func.args[0])?;
                    let func = match typ.scalar_type {
                        ScalarType::Int32 => UnaryFunc::AbsInt32,
                        ScalarType::Int64 => UnaryFunc::AbsInt64,
                        ScalarType::Float32 => UnaryFunc::AbsFloat32,
                        ScalarType::Float64 => UnaryFunc::AbsFloat64,
                        _ => bail!("abs does not accept arguments of type {:?}", typ),
                    };
                    let expr = ScalarExpr::CallUnary {
                        func,
                        expr: Box::new(expr),
                    };
                    Ok((expr, typ))
                }

                "coalesce" => {
                    if sql_func.args.is_empty() {
                        bail!("coalesce requires at least one argument");
                    }
                    let mut exprs = Vec::new();
                    for arg in &sql_func.args {
                        exprs.push(self.plan_expr(ctx, arg)?);
                    }
                    let (exprs, typ) = try_coalesce_types(exprs, "coalesce")?;
                    let expr = ScalarExpr::CallVariadic {
                        func: VariadicFunc::Coalesce,
                        exprs,
                    };
                    Ok((expr, typ))
                }

                "nullif" => {
                    if sql_func.args.len() != 2 {
                        bail!("nullif requires exactly two arguments");
                    }
                    let cond = ASTNode::SQLBinaryOp {
                        left: Box::new(sql_func.args[0].clone()),
                        op: SQLBinaryOperator::Eq,
                        right: Box::new(sql_func.args[1].clone()),
                    };
                    let (cond_expr, _) = self.plan_expr(ctx, &cond)?;
                    let (else_expr, else_type) = self.plan_expr(ctx, &sql_func.args[0])?;
                    let expr = ScalarExpr::If {
                        cond: Box::new(cond_expr),
                        then: Box::new(ScalarExpr::Literal(Datum::Null)),
                        els: Box::new(else_expr),
                    };
                    let typ = ColumnType {
                        name: None,
                        nullable: true,
                        scalar_type: else_type.scalar_type,
                    };
                    Ok((expr, typ))
                }

                _ => bail!("unsupported function: {}", ident),
            }
        }
    }

    fn plan_is_null_expr<'a>(
        &self,
        ctx: &ExprContext,
        inner: &'a ASTNode,
        not: bool,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (expr, _) = self.plan_expr(ctx, inner)?;
        let mut expr = ScalarExpr::CallUnary {
            func: UnaryFunc::IsNull,
            expr: Box::new(expr),
        };
        if not {
            expr = ScalarExpr::CallUnary {
                func: UnaryFunc::Not,
                expr: Box::new(expr),
            }
        }
        let typ = ColumnType {
            name: None,
            nullable: false,
            scalar_type: ScalarType::Bool,
        };
        Ok((expr, typ))
    }

    fn plan_unary_op<'a>(
        &self,
        ctx: &ExprContext,
        op: &'a SQLUnaryOperator,
        expr: &'a ASTNode,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (expr, typ) = self.plan_expr(ctx, expr)?;
        let (func, scalar_type) = match op {
            SQLUnaryOperator::Not => (UnaryFunc::Not, ScalarType::Bool),
            SQLUnaryOperator::Plus => return Ok((expr, typ)), // no-op
            SQLUnaryOperator::Minus => match typ.scalar_type {
                ScalarType::Int32 => (UnaryFunc::NegInt32, ScalarType::Int32),
                ScalarType::Int64 => (UnaryFunc::NegInt64, ScalarType::Int64),
                ScalarType::Float32 => (UnaryFunc::NegFloat32, ScalarType::Float32),
                ScalarType::Float64 => (UnaryFunc::NegFloat64, ScalarType::Float64),
                _ => bail!("cannot negate {:?}", typ.scalar_type),
            },
        };
        let expr = ScalarExpr::CallUnary {
            func,
            expr: Box::new(expr),
        };
        let typ = ColumnType {
            name: None,
            nullable: typ.nullable,
            scalar_type,
        };
        Ok((expr, typ))
    }

    fn plan_binary_op<'a>(
        &self,
        ctx: &ExprContext,
        op: &'a SQLBinaryOperator,
        left: &'a ASTNode,
        right: &'a ASTNode,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (mut lexpr, mut ltype) = self.plan_expr(ctx, left)?;
        let (mut rexpr, mut rtype) = self.plan_expr(ctx, right)?;

        if op == &SQLBinaryOperator::Plus
            || op == &SQLBinaryOperator::Minus
            || op == &SQLBinaryOperator::Multiply
            || op == &SQLBinaryOperator::Divide
            || op == &SQLBinaryOperator::Lt
            || op == &SQLBinaryOperator::LtEq
            || op == &SQLBinaryOperator::Gt
            || op == &SQLBinaryOperator::GtEq
            || op == &SQLBinaryOperator::Eq
            || op == &SQLBinaryOperator::NotEq
        {
            let ctx = op.to_string();
            let (mut exprs, typ) = try_coalesce_types(vec![(lexpr, ltype), (rexpr, rtype)], ctx)?;
            assert_eq!(exprs.len(), 2);
            rexpr = exprs.pop().unwrap();
            lexpr = exprs.pop().unwrap();
            rtype = typ.clone();
            ltype = typ;
        }

        let (func, scalar_type) = match op {
            SQLBinaryOperator::And | SQLBinaryOperator::Or => {
                if ltype.scalar_type != ScalarType::Bool && ltype.scalar_type != ScalarType::Null {
                    bail!(
                        "Cannot apply operator {:?} to non-boolean type {:?}",
                        op,
                        ltype.scalar_type
                    )
                }
                if rtype.scalar_type != ScalarType::Bool && rtype.scalar_type != ScalarType::Null {
                    bail!(
                        "Cannot apply operator {:?} to non-boolean type {:?}",
                        op,
                        rtype.scalar_type
                    )
                }
                let func = match op {
                    SQLBinaryOperator::And => BinaryFunc::And,
                    SQLBinaryOperator::Or => BinaryFunc::Or,
                    _ => unreachable!(),
                };
                (func, ScalarType::Bool)
            }
            SQLBinaryOperator::Plus => match (&ltype.scalar_type, &rtype.scalar_type) {
                (ScalarType::Int32, ScalarType::Int32) => (BinaryFunc::AddInt32, ScalarType::Int32),
                (ScalarType::Int64, ScalarType::Int64) => (BinaryFunc::AddInt64, ScalarType::Int64),
                (ScalarType::Float32, ScalarType::Float32) => {
                    (BinaryFunc::AddFloat32, ScalarType::Float32)
                }
                (ScalarType::Float64, ScalarType::Float64) => {
                    (BinaryFunc::AddFloat64, ScalarType::Float64)
                }
                _ => bail!(
                    "no overload for {:?} + {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
            SQLBinaryOperator::Minus => match (&ltype.scalar_type, &rtype.scalar_type) {
                (ScalarType::Int32, ScalarType::Int32) => (BinaryFunc::SubInt32, ScalarType::Int32),
                (ScalarType::Int64, ScalarType::Int64) => (BinaryFunc::SubInt64, ScalarType::Int64),
                (ScalarType::Float32, ScalarType::Float32) => {
                    (BinaryFunc::SubFloat32, ScalarType::Float32)
                }
                (ScalarType::Float64, ScalarType::Float64) => {
                    (BinaryFunc::SubFloat64, ScalarType::Float64)
                }
                _ => bail!(
                    "no overload for {:?} - {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
            SQLBinaryOperator::Multiply => match (&ltype.scalar_type, &rtype.scalar_type) {
                (ScalarType::Int32, ScalarType::Int32) => (BinaryFunc::MulInt32, ScalarType::Int32),
                (ScalarType::Int64, ScalarType::Int64) => (BinaryFunc::MulInt64, ScalarType::Int64),
                (ScalarType::Float32, ScalarType::Float32) => {
                    (BinaryFunc::MulFloat32, ScalarType::Float32)
                }
                (ScalarType::Float64, ScalarType::Float64) => {
                    (BinaryFunc::MulFloat64, ScalarType::Float64)
                }
                _ => bail!(
                    "no overload for {:?} * {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
            SQLBinaryOperator::Divide => match (&ltype.scalar_type, &rtype.scalar_type) {
                (ScalarType::Int32, ScalarType::Int32) => (BinaryFunc::DivInt32, ScalarType::Int32),
                (ScalarType::Int64, ScalarType::Int64) => (BinaryFunc::DivInt64, ScalarType::Int64),
                (ScalarType::Float32, ScalarType::Float32) => {
                    (BinaryFunc::DivFloat32, ScalarType::Float32)
                }
                (ScalarType::Float64, ScalarType::Float64) => {
                    (BinaryFunc::DivFloat64, ScalarType::Float64)
                }
                _ => bail!(
                    "no overload for {:?} / {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
            SQLBinaryOperator::Modulus => match (&ltype.scalar_type, &rtype.scalar_type) {
                (ScalarType::Int32, ScalarType::Int32) => (BinaryFunc::ModInt32, ScalarType::Int32),
                (ScalarType::Int64, ScalarType::Int64) => (BinaryFunc::ModInt64, ScalarType::Int64),
                (ScalarType::Float32, ScalarType::Float32) => {
                    (BinaryFunc::ModFloat32, ScalarType::Float32)
                }
                (ScalarType::Float64, ScalarType::Float64) => {
                    (BinaryFunc::ModFloat64, ScalarType::Float64)
                }
                _ => bail!(
                    "no overload for {:?} % {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
            SQLBinaryOperator::Lt
            | SQLBinaryOperator::LtEq
            | SQLBinaryOperator::Gt
            | SQLBinaryOperator::GtEq
            | SQLBinaryOperator::Eq
            | SQLBinaryOperator::NotEq => {
                if ltype.scalar_type != rtype.scalar_type
                    && ltype.scalar_type != ScalarType::Null
                    && rtype.scalar_type != ScalarType::Null
                {
                    bail!(
                        "{:?} and {:?} are not comparable",
                        ltype.scalar_type,
                        rtype.scalar_type
                    )
                }
                let func = match op {
                    SQLBinaryOperator::Lt => BinaryFunc::Lt,
                    SQLBinaryOperator::LtEq => BinaryFunc::Lte,
                    SQLBinaryOperator::Gt => BinaryFunc::Gt,
                    SQLBinaryOperator::GtEq => BinaryFunc::Gte,
                    SQLBinaryOperator::Eq => BinaryFunc::Eq,
                    SQLBinaryOperator::NotEq => BinaryFunc::NotEq,
                    _ => unreachable!(),
                };
                (func, ScalarType::Bool)
            }
            other => bail!("Function {:?} is not supported yet", other),
        };
        let is_integer_div = match &func {
            BinaryFunc::DivInt32 | BinaryFunc::DivInt64 => true,
            _ => false,
        };
        let expr = ScalarExpr::CallBinary {
            func,
            expr1: Box::new(lexpr),
            expr2: Box::new(rexpr),
        };
        let typ = ColumnType {
            name: None,
            nullable: ltype.nullable || rtype.nullable || is_integer_div,
            scalar_type,
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
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let low = ASTNode::SQLBinaryOp {
            left: Box::new(expr.clone()),
            op: if negated {
                SQLBinaryOperator::Lt
            } else {
                SQLBinaryOperator::GtEq
            },
            right: Box::new(low.clone()),
        };
        let high = ASTNode::SQLBinaryOp {
            left: Box::new(expr.clone()),
            op: if negated {
                SQLBinaryOperator::Gt
            } else {
                SQLBinaryOperator::LtEq
            },
            right: Box::new(high.clone()),
        };
        let both = ASTNode::SQLBinaryOp {
            left: Box::new(low),
            op: if negated {
                SQLBinaryOperator::Or
            } else {
                SQLBinaryOperator::And
            },
            right: Box::new(high),
        };
        self.plan_expr(ctx, &both)
    }

    fn plan_in_list<'a>(
        &self,
        ctx: &ExprContext,
        expr: &'a ASTNode,
        list: &'a [ASTNode],
        negated: bool,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let mut cond = ASTNode::SQLValue(Value::Boolean(false));
        for l in list {
            cond = ASTNode::SQLBinaryOp {
                left: Box::new(cond),
                op: SQLBinaryOperator::Or,
                right: Box::new(ASTNode::SQLBinaryOp {
                    left: Box::new(expr.clone()),
                    op: SQLBinaryOperator::Eq,
                    right: Box::new(l.clone()),
                }),
            }
        }
        if negated {
            cond = ASTNode::SQLUnaryOp {
                op: SQLUnaryOperator::Not,
                expr: Box::new(cond),
            }
        }
        self.plan_expr(ctx, &cond)
    }

    fn plan_case<'a>(
        &self,
        ctx: &ExprContext,
        operand: &'a Option<Box<ASTNode>>,
        conditions: &'a [ASTNode],
        results: &'a [ASTNode],
        else_result: &'a Option<Box<ASTNode>>,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let mut cond_exprs = Vec::new();
        let mut result_exprs = Vec::new();
        for (c, r) in conditions.iter().zip(results) {
            let c = match operand {
                Some(operand) => ASTNode::SQLBinaryOp {
                    left: operand.clone(),
                    op: SQLBinaryOperator::Eq,
                    right: Box::new(c.clone()),
                },
                None => c.clone(),
            };
            let (cexpr, ctype) = self.plan_expr(ctx, &c)?;
            if ctype.scalar_type != ScalarType::Bool {
                bail!(
                    "CASE expression has non-boolean type {:?}",
                    ctype.scalar_type
                );
            }
            cond_exprs.push(cexpr);
            let (rexpr, rtype) = self.plan_expr(ctx, r)?;
            result_exprs.push((rexpr, rtype));
        }
        let (else_expr, else_type) = match else_result {
            Some(else_result) => self.plan_expr(ctx, else_result)?,
            None => {
                let expr = ScalarExpr::Literal(Datum::Null);
                let typ = ColumnType {
                    name: None,
                    nullable: false,
                    scalar_type: ScalarType::Null,
                };
                (expr, typ)
            }
        };
        result_exprs.push((else_expr, else_type));
        let (mut result_exprs, typ) = try_coalesce_types(result_exprs, "CASE")?;
        let mut expr = result_exprs.pop().unwrap();
        assert_eq!(cond_exprs.len(), result_exprs.len());
        for (cexpr, rexpr) in cond_exprs.into_iter().zip(result_exprs).rev() {
            expr = ScalarExpr::If {
                cond: Box::new(cexpr),
                then: Box::new(rexpr),
                els: Box::new(expr),
            }
        }
        Ok((expr, typ))
    }

    fn plan_literal<'a>(&self, l: &'a Value) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (datum, scalar_type) = match l {
            Value::Long(i) => (Datum::Int64(*i as i64), ScalarType::Int64), // TODO(benesch): safe conversion
            Value::Double(f) => (Datum::Float64(*f), ScalarType::Float64),
            Value::SingleQuotedString(s) => (Datum::String(s.clone()), ScalarType::String),
            Value::NationalStringLiteral(_) => {
                bail!("n'' string literals are not supported: {}", l.to_string())
            }
            Value::HexStringLiteral(_) => {
                bail!("x'' string literals are not supported: {}", l.to_string())
            }
            Value::Boolean(b) => match b {
                false => (Datum::False, ScalarType::Bool),
                true => (Datum::True, ScalarType::Bool),
            },
            Value::Date(_) => bail!("DATE literals are not supported: {}", l.to_string()),
            Value::Time(_) => bail!("TIME literals are not supported: {}", l.to_string()),
            Value::Timestamp(_) => bail!("TIMESTAMP literals are not supported: {}", l.to_string()),
            Value::Interval { .. } => {
                bail!("INTERVAL literals are not supported: {}", l.to_string())
            }
            Value::Null => (Datum::Null, ScalarType::Null),
        };
        let nullable = datum == Datum::Null;
        let expr = ScalarExpr::Literal(datum);
        let typ = ColumnType {
            name: None,
            nullable,
            scalar_type,
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

// When types don't match exactly, SQL has some poorly-documented type promotion
// rules. For now, just promote integers into floats, and small floats into
// bigger floats.
fn try_coalesce_types<C>(
    exprs: Vec<(ScalarExpr, ColumnType)>,
    context: C,
) -> Result<(Vec<ScalarExpr>, ColumnType), failure::Error>
where
    C: fmt::Display,
{
    assert!(!exprs.is_empty());

    let scalar_type_prec = |scalar_type: &ScalarType| match scalar_type {
        ScalarType::Null => 0,
        ScalarType::Int32 => 1,
        ScalarType::Int64 => 2,
        ScalarType::Float32 => 3,
        ScalarType::Float64 => 4,
        _ => 5,
    };
    let max_scalar_type = exprs
        .iter()
        .map(|(_expr, typ)| &typ.scalar_type)
        .max_by_key(|scalar_type| scalar_type_prec(scalar_type))
        .unwrap()
        .clone();
    let nullable = exprs.iter().any(|(_expr, typ)| typ.nullable);
    let mut out = Vec::new();
    for (mut expr, typ) in exprs {
        let func = match (&typ.scalar_type, &max_scalar_type) {
            (ScalarType::Int32, ScalarType::Float32) => Some(UnaryFunc::CastInt32ToFloat32),
            (ScalarType::Int32, ScalarType::Float64) => Some(UnaryFunc::CastInt32ToFloat64),
            (ScalarType::Int64, ScalarType::Float32) => Some(UnaryFunc::CastInt64ToFloat32),
            (ScalarType::Int64, ScalarType::Float64) => Some(UnaryFunc::CastInt64ToFloat64),
            (ScalarType::Float32, ScalarType::Float64) => Some(UnaryFunc::CastFloat32ToFloat64),
            (ScalarType::Null, _) => None,
            (from, to) if from == to => None,
            (from, to) => bail!(
                "{} does not have uniform type: {:?} vs {:?}",
                context,
                from,
                to,
            ),
        };
        if let Some(func) = func {
            expr = ScalarExpr::CallUnary {
                func,
                expr: Box::new(expr),
            }
        }
        out.push(expr);
    }
    let typ = ColumnType {
        name: None,
        nullable,
        scalar_type: max_scalar_type,
    };
    Ok((out, typ))
}

fn parse_kafka_url(url: &str) -> Result<(SocketAddr, String), failure::Error> {
    let url: Url = url.parse()?;
    if url.scheme() != "kafka" {
        bail!("only kafka:// sources are supported: {}", url);
    } else if !url.has_host() {
        bail!("source URL missing hostname: {}", url)
    }
    let topic = match url.path_segments() {
        None => bail!("source URL missing topic path: {}"),
        Some(segments) => {
            let segments: Vec<_> = segments.collect();
            if segments.len() != 1 {
                bail!("source URL should have exactly one path segment: {}", url);
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

struct AggregateFuncVisitor<'ast> {
    aggs: Vec<&'ast SQLFunction>,
    within_aggregate: bool,
    err: Option<failure::Error>,
}

impl<'ast> AggregateFuncVisitor<'ast> {
    fn new() -> AggregateFuncVisitor<'ast> {
        AggregateFuncVisitor {
            aggs: Vec::new(),
            within_aggregate: false,
            err: None,
        }
    }

    fn into_result(self) -> Result<Vec<&'ast SQLFunction>, failure::Error> {
        match self.err {
            Some(err) => Err(err),
            None => Ok(self.aggs),
        }
    }
}

impl<'ast> Visit<'ast> for AggregateFuncVisitor<'ast> {
    fn visit_function(&mut self, func: &'ast SQLFunction) {
        let name_str = func.name.to_string().to_lowercase();
        let old_within_aggregate = self.within_aggregate;
        if AggregateFunc::is_aggregate_func(&name_str) {
            if self.within_aggregate {
                self.err = Some(format_err!("nested aggregate functions are not allowed"));
                return;
            }
            self.aggs.push(func);
            self.within_aggregate = true;
        }
        visit::visit_function(self, func);
        self.within_aggregate = old_within_aggregate;
    }

    fn visit_subquery(&mut self, _subquery: &'ast SQLQuery) {
        // don't go into subqueries
    }
}

struct ExprContext<'a> {
    name: &'static str,
    scope: &'a Scope,
    /// If None, aggregates are not allowed in this context
    /// If Some(nodes), nodes.get(aggregate) tells us which column to find the aggregate in
    aggregate_context: Option<&'a HashMap<&'a SQLFunction, (usize, ColumnType)>>,
}

#[derive(Debug, Clone)]
struct ScopeItem {
    table_name: Option<String>,
    column_name: Option<String>,
    typ: ColumnType,
}

#[derive(Debug, Clone)]
struct Scope {
    items: Vec<ScopeItem>,
}

impl Scope {
    fn from_source(table_name: &str, typ: RelationType) -> Self {
        Scope {
            items: typ
                .column_types
                .into_iter()
                .map(|typ| ScopeItem {
                    table_name: Some(table_name.to_owned()),
                    column_name: typ.name.clone(),
                    typ,
                })
                .collect(),
        }
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn alias_table(mut self, table_name: &str) -> Self {
        for item in &mut self.items {
            item.table_name = Some(table_name.to_owned());
        }
        self
    }

    fn resolve_column<'a>(
        &'a self,
        column_name: &str,
    ) -> Result<(usize, &'a ScopeItem), failure::Error> {
        let mut results = self
            .items
            .iter()
            .enumerate()
            .filter(|(_, item)| item.column_name.as_deref() == Some(column_name));
        match (results.next(), results.next()) {
            (None, None) => bail!("no column named {} in scope", column_name),
            (Some((i, item)), None) => Ok((i, item)),
            (Some(_), Some(_)) => bail!("column name {} is ambiguous", column_name),
            _ => unreachable!(),
        }
    }

    fn resolve_table_column<'a>(
        &'a self,
        table_name: &str,
        column_name: &str,
    ) -> Result<(usize, &'a ScopeItem), failure::Error> {
        let mut results = self.items.iter().enumerate().filter(|(_, item)| {
            item.table_name.as_deref() == Some(table_name)
                && item.column_name.as_deref() == Some(column_name)
        });
        match (results.next(), results.next()) {
            (None, None) => bail!("no column named {}.{} in scope", table_name, column_name),
            (Some((i, item)), None) => Ok((i, item)),
            (Some(_), Some(_)) => bail!("column name {}.{} is ambiguous", table_name, column_name),
            _ => unreachable!(),
        }
    }

    fn product(self, right: Self) -> Self {
        Scope {
            items: self
                .items
                .into_iter()
                .chain(right.items.into_iter())
                .collect(),
        }
    }

    fn project(&self, columns: &[usize]) -> Self {
        Scope {
            items: columns.iter().map(|&i| self.items[i].clone()).collect(),
        }
    }
}

impl RelationExpr {
    fn let_<F>(value: RelationExpr, f: F) -> Result<(Self, Scope), failure::Error>
    where
        F: FnOnce(Self) -> Result<(Self, Scope), failure::Error>,
    {
        let name = format!("tmp_{}", Uuid::new_v4());
        let typ = value.typ();
        let (body, scope) = f(RelationExpr::Get {
            name: name.clone(),
            typ,
        })?;
        let expr = RelationExpr::Let {
            name: name.clone(),
            value: Box::new(value),
            body: Box::new(body),
        };
        Ok((expr, scope))
    }
}
