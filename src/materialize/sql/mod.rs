// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL-dataflow translation.

use failure::{bail, ensure, format_err};
use sqlparser::ast::visit::{self, Visit};
use sqlparser::ast::{
    BinaryOperator, DataType, Expr, Function, JoinConstraint, JoinOperator, ObjectName, ObjectType,
    Query, Select, SelectItem, SetExpr, SetOperator, SourceSchema, Statement, TableAlias,
    TableFactor, TableWithJoins, UnaryOperator, Value, Values,
};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser as SqlParser;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::iter::FromIterator;
use std::net::{SocketAddr, ToSocketAddrs};
use url::Url;

use crate::dataflow::func::{AggregateFunc, BinaryFunc, UnaryFunc, VariadicFunc};
use crate::dataflow::{
    AggregateExpr, Dataflow, KafkaSinkConnector, KafkaSourceConnector, RelationExpr, ScalarExpr,
    Sink, SinkConnector, Source, SourceConnector, View,
};
use crate::glue::*;
use crate::interchange::avro;
use crate::repr::decimal::MAX_DECIMAL_PRECISION;
use crate::repr::{ColumnType, Datum, RelationType, ScalarType};
use ore::collections::CollectionExt;
use ore::iter::{FallibleIteratorExt, IteratorExt};
use ore::option::OptionExt;
use store::{DataflowStore, RemoveMode};

pub mod store;

/// Converts raw SQL queries into dataflow commands.
#[derive(Debug, Default)]
pub struct Planner {
    pub dataflows: DataflowStore,
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

impl Planner {
    /// Parses and plans a raw SQL query. See the documentation for
    /// [`PlannerResult`] for details about the meaning of the return type.
    pub fn handle_command(&mut self, sql: String) -> PlannerResult {
        let stmts = SqlParser::parse_sql(&AnsiDialect {}, sql)?;
        match stmts.len() {
            0 => Ok((SqlResponse::EmptyQuery, None)),
            1 => self.handle_statement(stmts.into_element()),
            _ => bail!("expected one statement, but got {}", stmts.len()),
        }
    }

    fn handle_statement(&mut self, stmt: Statement) -> PlannerResult {
        match stmt {
            Statement::Peek { name, immediate } => self.handle_peek(name, immediate),
            Statement::Tail { .. } => bail!("TAIL is not implemented yet"),
            Statement::CreateSource { .. }
            | Statement::CreateSink { .. }
            | Statement::CreateView { .. }
            | Statement::CreateSources { .. } => self.handle_create_dataflow(stmt),
            Statement::Drop { .. } => self.handle_drop_dataflow(stmt),
            Statement::Query(query) => self.handle_select(*query),
            Statement::Show { object_type: ot } => self.handle_show_objects(ot),
            Statement::ShowColumns { table_name } => self.handle_show_columns(&table_name),

            _ => bail!("unsupported SQL statement: {:?}", stmt),
        }
    }

    fn handle_show_objects(&mut self, object_type: ObjectType) -> PlannerResult {
        let rows: Vec<Vec<Datum>> = self
            .dataflows
            .iter()
            .filter(|(_k, v)| object_type_matches(object_type, &v))
            .map(|(k, _v)| vec![Datum::from(k.to_owned())])
            .collect();
        Ok((
            SqlResponse::SendRows {
                typ: RelationType {
                    column_types: vec![ColumnType::new(ScalarType::String)
                        .name(object_type_as_plural_str(object_type).to_owned())],
                },
                rows,
            },
            None,
        ))
    }

    /// Create an immediate result that describes all the columns for the given table
    pub fn handle_show_columns(&mut self, table_name: &ObjectName) -> PlannerResult {
        use Datum::String as DString;

        let column_descriptions: Vec<_> = self
            .dataflows
            .get_type(&table_name.to_string())?
            .column_types
            .iter()
            .map(|colty| {
                vec![
                    DString(colty.name.as_ref().map(|n| &**n).unwrap_or("?").to_string()),
                    DString(if colty.nullable { "YES" } else { "NO" }.to_string()),
                    DString(colty.scalar_type.to_string()),
                ]
            })
            .collect();

        let col_name = |s: &str| ColumnType::new(ScalarType::String).name(s.to_string());
        Ok((
            SqlResponse::SendRows {
                typ: RelationType {
                    column_types: vec![col_name("Field"), col_name("Nullable"), col_name("Type")],
                },
                rows: column_descriptions,
            },
            None,
        ))
    }

    fn handle_create_dataflow(&mut self, stmt: Statement) -> PlannerResult {
        let dataflows = self.plan_statement(&stmt)?;
        let sql_response = match stmt {
            Statement::CreateSource { .. } => SqlResponse::CreatedSource,
            Statement::CreateSink { .. } => SqlResponse::CreatedSink,
            Statement::CreateView { .. } => SqlResponse::CreatedView,
            Statement::CreateSources { .. } => SqlResponse::SendRows {
                typ: RelationType {
                    column_types: vec![ColumnType::new(ScalarType::String).name("Topic")],
                },
                rows: dataflows
                    .iter()
                    .map(|df| vec![Datum::from(df.name().to_owned())])
                    .collect(),
            },
            _ => unreachable!(),
        };
        for dataflow in &dataflows {
            self.dataflows.insert(dataflow.clone())?;
        }
        Ok((
            sql_response,
            Some(DataflowCommand::CreateDataflows(dataflows)),
        ))
    }

    fn handle_drop_dataflow(&mut self, stmt: Statement) -> PlannerResult {
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
        let sql_response = match object_type {
            ObjectType::Source => SqlResponse::DroppedSource,
            ObjectType::View => SqlResponse::DroppedView,
            _ => bail!("unsupported SQL statement: DROP {}", object_type),
        };
        let removed = removed.iter().map(|d| d.name().to_owned()).collect();
        Ok((sql_response, Some(DataflowCommand::DropDataflows(removed))))
    }

    fn handle_peek(&mut self, name: ObjectName, immediate: bool) -> PlannerResult {
        let name = name.to_string();
        let dataflow = self.dataflows.get(&name)?.clone();
        Ok((
            SqlResponse::Peeking {
                typ: dataflow.typ().clone(),
            },
            Some(DataflowCommand::Peek {
                source: RelationExpr::Get {
                    name: dataflow.name().to_owned(),
                    typ: dataflow.typ().clone(),
                },
                when: if immediate {
                    PeekWhen::Immediately
                } else {
                    PeekWhen::EarliestSource
                },
            }),
        ))
    }

    pub fn handle_select(&mut self, query: Query) -> PlannerResult {
        let relation_expr = self.plan_query(&query)?;
        Ok((
            SqlResponse::Peeking {
                typ: relation_expr.typ(),
            },
            Some(DataflowCommand::Peek {
                source: relation_expr,
                when: PeekWhen::Immediately,
            }),
        ))
    }
}

impl ScalarType {
    pub fn from_sql(data_type: &DataType) -> Result<Self, failure::Error> {
        // NOTE this needs to stay in sync with sqllogictest::postgres::get_column
        Ok(match data_type {
            DataType::Boolean => ScalarType::Bool,
            DataType::Custom(name) if name.to_string().to_lowercase() == "bool" => ScalarType::Bool,
            DataType::Char(_) | DataType::Varchar(_) | DataType::Text => ScalarType::String,
            DataType::Custom(name) if name.to_string().to_lowercase() == "string" => {
                ScalarType::String
            }
            DataType::SmallInt => ScalarType::Int32,
            DataType::Int | DataType::BigInt => ScalarType::Int64,
            DataType::Float(_) | DataType::Real | DataType::Double => ScalarType::Float64,
            DataType::Decimal(precision, scale) => {
                let precision = precision.unwrap_or(MAX_DECIMAL_PRECISION.into());
                let scale = scale.unwrap_or(0);
                if precision > MAX_DECIMAL_PRECISION.into() {
                    bail!(
                        "decimal precision {} exceeds maximum precision {}",
                        precision,
                        MAX_DECIMAL_PRECISION
                    );
                }
                if scale > precision {
                    bail!("decimal scale {} exceeds precision {}", scale, precision);
                }
                ScalarType::Decimal(precision as u8, scale as u8)
            }
            DataType::Date => ScalarType::Date,
            DataType::Timestamp => ScalarType::Timestamp,
            DataType::Time => ScalarType::Time,
            DataType::Bytea => ScalarType::Bytes,
            other => bail!("Unexpected SQL type: {:?}", other),
        })
    }
}

fn build_source(
    schema: &SourceSchema,
    kafka_addr: SocketAddr,
    name: String,
    topic: String,
) -> Result<Source, failure::Error> {
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
    let typ = avro::validate_schema(&raw_schema)?;
    Ok(Source {
        name,
        connector: SourceConnector::Kafka(KafkaSourceConnector {
            addr: kafka_addr,
            topic,
            raw_schema,
            schema_registry_url,
        }),
        typ,
    })
}

impl Planner {
    fn plan_statement(&self, stmt: &Statement) -> Result<Vec<Dataflow>, failure::Error> {
        match stmt {
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
                let relation_expr = self.plan_query(query)?;
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
                Ok(vec![Dataflow::View(View {
                    name: extract_sql_object_name(name)?,
                    relation_expr,
                    typ,
                })])
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

                Ok(vec![Dataflow::Source(build_source(
                    schema, addr, name, topic,
                )?)])
            }
            Statement::CreateSources {
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
                let subjects = ccsr_client.list_subjects()?;
                let topic_names = subjects
                    .iter()
                    .filter_map(|s| {
                        let parts: Vec<&str> = s.rsplitn(2, '-').collect();
                        if parts.len() == 2 && parts[0] == "value" {
                            Some(parts[1])
                        } else {
                            None
                        }
                    })
                    .filter(|topic| self.dataflows.try_get(topic).is_none());
                let (addr, topic) = parse_kafka_url(url)?;
                if let Some(s) = topic {
                    bail!(
                        "CREATE SOURCES statement should not take a topic path: {}",
                        s
                    );
                }
                let results: Result<Vec<_>, failure::Error> = topic_names
                    .map(|name| {
                        Ok(Dataflow::Source(build_source(
                            &SourceSchema::Registry(schema_registry.to_owned()),
                            addr,
                            name.to_owned(),
                            name.to_owned(),
                        )?))
                    })
                    .collect();

                Ok(results?)
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
                Ok(vec![Dataflow::Sink(Sink {
                    name,
                    from: (from, dataflow.typ().clone()),
                    connector: SinkConnector::Kafka(KafkaSinkConnector {
                        addr,
                        topic,
                        schema_id: 0,
                    }),
                })])
            }
            other => bail!("Unsupported statement: {:?}", other),
        }
    }

    pub fn plan_query(&self, q: &Query) -> Result<RelationExpr, failure::Error> {
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

    fn plan_set_expr(&self, q: &SetExpr) -> Result<RelationExpr, failure::Error> {
        match q {
            SetExpr::Select(select) => self.plan_view_select(select),
            SetExpr::SetOperation {
                op: SetOperator::Union,
                all,
                left,
                right,
            } => {
                let left_expr = self.plan_set_expr(left)?;
                let right_expr = self.plan_set_expr(right)?;

                // TODO(jamii) this type-checking is redundant with RelationExpr::typ, but currently it seems that we need both because RelationExpr::typ is not allowed to return errors
                let left_types = &left_expr.typ().column_types;
                let right_types = &right_expr.typ().column_types;
                if left_types.len() != right_types.len() {
                    bail!(
                        "Each UNION should have the same number of columns: {:?} UNION {:?}",
                        left,
                        right
                    );
                }
                for (left_col_type, right_col_type) in left_types.iter().zip(right_types.iter()) {
                    left_col_type.union(right_col_type)?;
                }

                let relation_expr = RelationExpr::Union {
                    left: Box::new(left_expr),
                    right: Box::new(right_expr),
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
            SetExpr::Values(Values(values)) => {
                ensure!(
                    !values.is_empty(),
                    "Can't infer a type for empty VALUES expression"
                );
                let ctx = &ExprContext {
                    name: "values",
                    scope: &Scope::empty(),
                    aggregate_context: None,
                };
                let mut expr: Option<RelationExpr> = None;
                let mut types: Option<Vec<ColumnType>> = None;
                for row in values {
                    let mut value_exprs = vec![];
                    for value in row.iter() {
                        value_exprs.push(self.plan_expr(ctx, value)?);
                    }
                    types = if let Some(types) = types {
                        if types.len() != value_exprs.len() {
                            bail!(
                                "VALUES expression has varying number of columns: {}",
                                q.to_string()
                            );
                        }
                        Some(
                            types
                                .iter()
                                .zip(value_exprs.iter())
                                .map(|(left_typ, (_, right_typ))| left_typ.union(right_typ))
                                .collect::<Result<Vec<_>, _>>()?,
                        )
                    } else {
                        Some(
                            value_exprs
                                .iter()
                                .map(|(_, right_typ)| right_typ.clone())
                                .collect(),
                        )
                    };

                    let row_expr = RelationExpr::Constant {
                        rows: vec![vec![]],
                        typ: RelationType {
                            column_types: vec![],
                        },
                    }
                    .map(value_exprs);
                    expr = if let Some(expr) = expr {
                        Some(expr.union(row_expr))
                    } else {
                        Some(row_expr)
                    };
                }
                Ok(expr.unwrap())
            }
            _ => bail!("set operations are not yet supported"),
        }
    }

    fn plan_view_select(&self, s: &Select) -> Result<RelationExpr, failure::Error> {
        // Step 1. Handle FROM clause, including joins.
        let (mut relation_expr, from_scope) = s
            .from
            .iter()
            .map(|twj| self.plan_table_with_joins(twj))
            .fallible()
            .fold1(|(left, left_scope), (right, right_scope)| {
                self.plan_join_operator(
                    &JoinOperator::CrossJoin,
                    left,
                    left_scope,
                    right,
                    right_scope,
                )
            })
            .unwrap_or_else(|| {
                let typ = RelationType::new(vec![ColumnType::new(ScalarType::String)]);
                Ok((
                    RelationExpr::Constant {
                        rows: vec![vec![Datum::String("X".into())]],
                        typ: typ.clone(),
                    },
                    Scope::from_source("dual", typ),
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
            if typ.scalar_type != ScalarType::Bool && typ.scalar_type != ScalarType::Null {
                bail!(
                    "WHERE clause must have boolean type, not {:?}",
                    typ.scalar_type
                );
            }
            relation_expr = relation_expr.filter(vec![expr]);
        }

        // Step 3. Handle GROUP BY clause.
        let (group_scope, aggregate_context, select_all_mapping) = {
            // gather group columns
            let ctx = &ExprContext {
                name: "GROUP BY clause",
                scope: &from_scope,
                aggregate_context: None,
            };
            let mut group_key = vec![];
            let mut group_exprs = vec![];
            let mut group_scope = Scope { items: vec![] };
            let mut select_all_mapping = HashMap::new();
            for expr in &s.group_by {
                let (expr, typ) = self.plan_expr(ctx, &expr)?;
                match &expr {
                    ScalarExpr::Column(i) => {
                        // repeated exprs in GROUP BY confuse name resolution later, and dropping them doesn't change the results
                        if !group_key.contains(i) {
                            group_key.push(*i);
                            select_all_mapping.insert(*i, group_scope.len());
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
            if !aggregates.is_empty() || !group_key.is_empty() || s.having.is_some() {
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
                (group_scope, aggregate_context, select_all_mapping)
            } else {
                // if no GROUP BY, aggregates or having then all columns remain in scope
                (
                    from_scope.clone(),
                    aggregate_context,
                    (0..from_scope.len()).map(|i| (i, i)).collect(),
                )
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
                for (expr, typ) in
                    self.plan_select_item(ctx, p, &from_scope, &select_all_mapping)?
                {
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
        s: &'a SelectItem,
        select_all_scope: &Scope,
        select_all_mapping: &HashMap<usize, usize>,
    ) -> Result<Vec<(ScalarExpr, ColumnType)>, failure::Error> {
        match s {
            SelectItem::UnnamedExpr(e) => Ok(vec![self.plan_expr(ctx, e)?]),
            SelectItem::ExprWithAlias { expr, alias } => {
                let (expr, mut typ) = self.plan_expr(ctx, expr)?;
                typ.name = Some(alias.clone());
                Ok(vec![(expr, typ)])
            }
            SelectItem::Wildcard => select_all_scope
                .items
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let j = select_all_mapping.get(&i).ok_or_else(|| {
                        format_err!(
                            "no column named {}{} in scope",
                            item.table_name
                                .as_deref()
                                .map(|s| format!("{}.", s))
                                .unwrap_or_else(|| "".to_owned()),
                            item.column_name.as_deref().unwrap_or("")
                        )
                    })?;
                    Ok((ScalarExpr::Column(*j), item.typ.clone()))
                })
                .collect::<Result<Vec<_>, _>>(),
            SelectItem::QualifiedWildcard(table_name) => {
                let table_name = extract_sql_object_name(table_name)?;
                select_all_scope
                    .items
                    .iter()
                    .enumerate()
                    .filter(|(_, item)| item.table_name.as_ref() == Some(&table_name))
                    .map(|(i, item)| {
                        let j = select_all_mapping.get(&i).ok_or_else(|| {
                            format_err!(
                                "no column named {}{} in scope",
                                item.table_name
                                    .as_deref()
                                    .map(|s| format!("{}.", s))
                                    .unwrap_or_else(|| "".to_owned()),
                                item.column_name.as_deref().unwrap_or("")
                            )
                        })?;
                        Ok((ScalarExpr::Column(*j), item.typ.clone()))
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
            JoinOperator::Inner(constraint) => self.plan_join_constraint(
                &constraint,
                left,
                left_scope,
                right,
                right_scope,
                |both, both_scope| Ok((both, both_scope)),
            ),
            JoinOperator::LeftOuter(constraint) => left.let_(|left| {
                self.plan_join_constraint(
                    &constraint,
                    left.clone(),
                    left_scope,
                    right,
                    right_scope,
                    |both, both_scope| {
                        both.let_(|both| {
                            Ok((both.clone().union(both.left_outer(left)), both_scope))
                        })
                    },
                )
            }),
            JoinOperator::RightOuter(constraint) => right.let_(|right| {
                self.plan_join_constraint(
                    &constraint,
                    left,
                    left_scope,
                    right.clone(),
                    right_scope,
                    |both, both_scope| {
                        both.let_(|both| {
                            Ok((both.clone().union(both.right_outer(right)), both_scope))
                        })
                    },
                )
            }),
            JoinOperator::FullOuter(constraint) => left.let_(|left| {
                right.let_(|right| {
                    self.plan_join_constraint(
                        &constraint,
                        left.clone(),
                        left_scope,
                        right.clone(),
                        right_scope,
                        |both, both_scope| {
                            both.let_(|both| {
                                Ok((
                                    both.clone()
                                        .union(both.clone().left_outer(left))
                                        .union(both.clone().right_outer(right)),
                                    both_scope,
                                ))
                            })
                        },
                    )
                })
            }),
            JoinOperator::CrossJoin => Ok((left.product(right), left_scope.product(right_scope))),
            // The remaining join types are MSSQL-specific. We are unlikely to
            // ever support them. The standard SQL equivalent is LATERAL, which
            // we are not capable of even parsing at the moment.
            JoinOperator::CrossApply => bail!("CROSS APPLY is not supported"),
            JoinOperator::OuterApply => bail!("OUTER APPLY is not supported"),
        }
    }

    fn plan_join_constraint<'a, F>(
        &self,
        constraint: &'a JoinConstraint,
        left: RelationExpr,
        left_scope: Scope,
        right: RelationExpr,
        right_scope: Scope,
        with_both: F,
    ) -> Result<(RelationExpr, Scope), failure::Error>
    where
        F: FnOnce(RelationExpr, Scope) -> Result<(RelationExpr, Scope), failure::Error>,
    {
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
                with_both(product.filter(vec![predicate]), product_scope)
            }
            JoinConstraint::Using(column_names) => self.plan_using_constraint(
                &column_names,
                left,
                left_scope,
                right,
                right_scope,
                with_both,
            ),
            JoinConstraint::Natural => {
                let mut column_names = vec![];
                for item in left_scope.items.iter() {
                    if let Some(column_name) = &item.column_name {
                        if left_scope.resolve_column(column_name).is_ok()
                            && right_scope.resolve_column(column_name).is_ok()
                        {
                            column_names.push(column_name.clone());
                        }
                    }
                }
                self.plan_using_constraint(
                    &column_names,
                    left,
                    left_scope,
                    right,
                    right_scope,
                    with_both,
                )
            }
        }
    }

    // See page 440 of ANSI SQL 2016 spec for details on scoping of using/natural joins
    fn plan_using_constraint<F>(
        &self,
        column_names: &[String],
        left: RelationExpr,
        left_scope: Scope,
        right: RelationExpr,
        right_scope: Scope,
        with_both: F,
    ) -> Result<(RelationExpr, Scope), failure::Error>
    where
        F: FnOnce(RelationExpr, Scope) -> Result<(RelationExpr, Scope), failure::Error>,
    {
        let mut join_exprs = vec![];
        let mut map_exprs = vec![];
        let mut new_items = vec![];
        let mut dropped_columns = HashSet::new();
        for column_name in column_names {
            let (l, l_item) = left_scope.resolve_column(column_name)?;
            let (r, r_item) = right_scope.resolve_column(column_name)?;
            let typ = l_item.typ.union(&r_item.typ)?;
            join_exprs.push(ScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1: Box::new(ScalarExpr::Column(l)),
                expr2: Box::new(ScalarExpr::Column(left_scope.len() + r)),
            });
            map_exprs.push((
                ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![
                        ScalarExpr::Column(l),
                        ScalarExpr::Column(left_scope.len() + r),
                    ],
                },
                typ.clone(),
            ));
            new_items.push(ScopeItem {
                table_name: None,
                column_name: typ.name.clone(),
                typ,
            });
            dropped_columns.insert(l);
            dropped_columns.insert(left_scope.len() + r);
        }
        let project_key =
            // coalesced join columns
            (0..map_exprs.len())
            .map(|i| left_scope.len() + right_scope.len() + i)
            // other columns that weren't joined
            .chain(
                (0..(left_scope.len() + right_scope.len()))
                    .filter(|i| !dropped_columns.contains(i)),
            )
            .collect::<Vec<_>>();
        let both = left.product(right).filter(join_exprs);
        let both_scope = left_scope.product(right_scope);
        let (both, mut both_scope) = with_both(both, both_scope)?;
        both_scope.items.extend(new_items);
        let both_scope = both_scope.project(&project_key);
        let both = both.map(map_exprs).project(project_key);
        Ok((both, both_scope))
    }

    fn plan_expr<'a>(
        &self,
        ctx: &ExprContext,
        e: &'a Expr,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        match e {
            Expr::Identifier(name) => {
                let (i, item) = ctx.scope.resolve_column(name)?;
                let expr = ScalarExpr::Column(i);
                Ok((expr, item.typ.clone()))
            }
            Expr::CompoundIdentifier(names) if names.len() == 2 => {
                let (i, item) = ctx.scope.resolve_table_column(&names[0], &names[1])?;
                let expr = ScalarExpr::Column(i);
                Ok((expr, item.typ.clone()))
            }
            Expr::Value(val) => self.plan_literal(val),
            // TODO(benesch): why isn't IS [NOT] NULL a unary op?
            Expr::IsNull(expr) => self.plan_is_null_expr(ctx, expr, false),
            Expr::IsNotNull(expr) => self.plan_is_null_expr(ctx, expr, true),
            Expr::UnaryOp { op, expr } => self.plan_unary_op(ctx, op, expr),
            Expr::BinaryOp { op, left, right } => self.plan_binary_op(ctx, op, left, right),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => self.plan_between(ctx, expr, low, high, *negated),
            Expr::InList {
                expr,
                list,
                negated,
            } => self.plan_in_list(ctx, expr, list, *negated),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => self.plan_case(ctx, operand, conditions, results, else_result),
            Expr::Nested(expr) => self.plan_expr(ctx, expr),
            Expr::Cast { expr, data_type } => self.plan_cast(ctx, expr, data_type),
            Expr::Function(func) => self.plan_function(ctx, func),
            _ => bail!(
                "complicated expressions are not yet supported: {}",
                e.to_string()
            ),
        }
    }

    fn plan_cast<'a>(
        &self,
        ctx: &ExprContext,
        expr: &'a Expr,
        data_type: &'a DataType,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let to_scalar_type = ScalarType::from_sql(data_type)?;
        let (expr, from_type) = self.plan_expr(ctx, expr)?;
        plan_cast_internal("CAST", expr, &from_type, to_scalar_type)
    }

    fn plan_aggregate(
        &self,
        ctx: &ExprContext,
        sql_func: &Function,
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
            ("count", Expr::Wildcard) => (
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
        let typ = ColumnType::new(scalar_type)
            .name(ident.clone())
            .nullable(func.is_nullable());
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
        sql_func: &'a Function,
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
                    let cond = Expr::BinaryOp {
                        left: Box::new(sql_func.args[0].clone()),
                        op: BinaryOperator::Eq,
                        right: Box::new(sql_func.args[1].clone()),
                    };
                    let (cond_expr, _) = self.plan_expr(ctx, &cond)?;
                    let (else_expr, else_type) = self.plan_expr(ctx, &sql_func.args[0])?;
                    let expr = ScalarExpr::If {
                        cond: Box::new(cond_expr),
                        then: Box::new(ScalarExpr::Literal(Datum::Null)),
                        els: Box::new(else_expr),
                    };
                    let typ = ColumnType::new(else_type.scalar_type).nullable(true);
                    Ok((expr, typ))
                }

                _ => bail!("unsupported function: {}", ident),
            }
        }
    }

    fn plan_is_null_expr<'a>(
        &self,
        ctx: &ExprContext,
        inner: &'a Expr,
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
        let typ = ColumnType::new(ScalarType::Bool);
        Ok((expr, typ))
    }

    fn plan_unary_op<'a>(
        &self,
        ctx: &ExprContext,
        op: &'a UnaryOperator,
        expr: &'a Expr,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (expr, typ) = self.plan_expr(ctx, expr)?;
        let (func, scalar_type) = match op {
            UnaryOperator::Not => (UnaryFunc::Not, ScalarType::Bool),
            UnaryOperator::Plus => return Ok((expr, typ)), // no-op
            UnaryOperator::Minus => match typ.scalar_type {
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
        let typ = ColumnType::new(scalar_type).nullable(typ.nullable);
        Ok((expr, typ))
    }

    fn plan_binary_op<'a>(
        &self,
        ctx: &ExprContext,
        op: &'a BinaryOperator,
        left: &'a Expr,
        right: &'a Expr,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (mut lexpr, mut ltype) = self.plan_expr(ctx, left)?;
        let (mut rexpr, mut rtype) = self.plan_expr(ctx, right)?;

        // Arithmetic operations follow Snowflake's rules for precision/scale
        // conversions. [0]
        //
        // [0]: https://docs.snowflake.net/manuals/sql-reference/operators-arithmetic.html
        match (&op, &ltype.scalar_type, &rtype.scalar_type) {
            (BinaryOperator::Plus, ScalarType::Decimal(p1, s1), ScalarType::Decimal(p2, s2))
            | (BinaryOperator::Minus, ScalarType::Decimal(p1, s1), ScalarType::Decimal(p2, s2)) => {
                let p = cmp::max(p1, p2) + 1;
                let so = cmp::max(s1, s2);
                let lexpr = rescale_decimal(lexpr, *s1, *so);
                let rexpr = rescale_decimal(rexpr, *s2, *so);
                let func = if op == &BinaryOperator::Plus {
                    BinaryFunc::AddDecimal
                } else {
                    BinaryFunc::SubDecimal
                };
                let expr = lexpr.call_binary(rexpr, func);
                let typ = ColumnType::new(ScalarType::Decimal(p, *so))
                    .nullable(ltype.nullable || rtype.nullable);
                return Ok((expr, typ));
            }
            (
                BinaryOperator::Multiply,
                ScalarType::Decimal(p1, s1),
                ScalarType::Decimal(p2, s2),
            ) => {
                let so = cmp::max(cmp::max(cmp::min(s1 + s2, 12), *s1), *s2);
                let si = s1 + s2;
                let expr = lexpr.call_binary(rexpr, BinaryFunc::MulDecimal);
                let expr = rescale_decimal(expr, si, so);
                let p = (p1 - s1) + (p2 - s2) + so;
                let typ = ColumnType::new(ScalarType::Decimal(p, so))
                    .nullable(ltype.nullable || rtype.nullable);
                return Ok((expr, typ));
            }
            (BinaryOperator::Divide, ScalarType::Decimal(p1, s1), ScalarType::Decimal(_, s2)) => {
                let s = cmp::max(cmp::min(12, s1 + 6), *s1);
                let si = cmp::max(s + 1, *s2);
                lexpr = rescale_decimal(lexpr, *s1, si);
                let expr = lexpr.call_binary(rexpr, BinaryFunc::DivDecimal);
                let expr = rescale_decimal(expr, si - s2, s);
                let p = (p1 - s1) + s2 + s;
                let typ = ColumnType::new(ScalarType::Decimal(p, s)).nullable(true);
                return Ok((expr, typ));
            }
            _ => (),
        }

        if op == &BinaryOperator::Plus
            || op == &BinaryOperator::Minus
            || op == &BinaryOperator::Multiply
            || op == &BinaryOperator::Divide
            || op == &BinaryOperator::Lt
            || op == &BinaryOperator::LtEq
            || op == &BinaryOperator::Gt
            || op == &BinaryOperator::GtEq
            || op == &BinaryOperator::Eq
            || op == &BinaryOperator::NotEq
        {
            let ctx = op.to_string();
            let (mut exprs, typ) = try_coalesce_types(vec![(lexpr, ltype), (rexpr, rtype)], &ctx)?;
            assert_eq!(exprs.len(), 2);
            rexpr = exprs.pop().unwrap();
            lexpr = exprs.pop().unwrap();
            rtype = typ.clone();
            ltype = typ;
        }

        let (func, scalar_type) = match op {
            BinaryOperator::And | BinaryOperator::Or => {
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
                    BinaryOperator::And => BinaryFunc::And,
                    BinaryOperator::Or => BinaryFunc::Or,
                    _ => unreachable!(),
                };
                (func, ScalarType::Bool)
            }
            BinaryOperator::Plus => match (&ltype.scalar_type, &rtype.scalar_type) {
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
            BinaryOperator::Minus => match (&ltype.scalar_type, &rtype.scalar_type) {
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
            BinaryOperator::Multiply => match (&ltype.scalar_type, &rtype.scalar_type) {
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
            BinaryOperator::Divide => match (&ltype.scalar_type, &rtype.scalar_type) {
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
            BinaryOperator::Modulus => match (&ltype.scalar_type, &rtype.scalar_type) {
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
            BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Eq
            | BinaryOperator::NotEq => {
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
                    BinaryOperator::Lt => BinaryFunc::Lt,
                    BinaryOperator::LtEq => BinaryFunc::Lte,
                    BinaryOperator::Gt => BinaryFunc::Gt,
                    BinaryOperator::GtEq => BinaryFunc::Gte,
                    BinaryOperator::Eq => BinaryFunc::Eq,
                    BinaryOperator::NotEq => BinaryFunc::NotEq,
                    _ => unreachable!(),
                };
                (func, ScalarType::Bool)
            }
            BinaryOperator::Like | BinaryOperator::NotLike => {
                if (ltype.scalar_type != ScalarType::String
                    && ltype.scalar_type != ScalarType::Null)
                    || (rtype.scalar_type != ScalarType::String
                        && rtype.scalar_type != ScalarType::Null)
                {
                    bail!(
                        "LIKE operator requires two string operators, found: {:?} and {:?}",
                        ltype,
                        rtype
                    );
                } else {
                    let mut expr = ScalarExpr::CallBinary {
                        func: BinaryFunc::MatchRegex,
                        expr1: Box::new(lexpr),
                        expr2: Box::new(ScalarExpr::CallUnary {
                            func: UnaryFunc::BuildLikeRegex,
                            expr: Box::new(rexpr),
                        }),
                    };
                    if let BinaryOperator::NotLike = op {
                        expr = ScalarExpr::CallUnary {
                            func: UnaryFunc::Not,
                            expr: Box::new(expr),
                        };
                    }
                    let typ = ColumnType::new(ScalarType::Bool)
                        .nullable(ltype.nullable || rtype.nullable);
                    return Ok((expr, typ));
                }
            }
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
        let typ = ColumnType::new(scalar_type)
            .nullable(ltype.nullable || rtype.nullable || is_integer_div);
        Ok((expr, typ))
    }

    fn plan_between<'a>(
        &self,
        ctx: &ExprContext,
        expr: &'a Expr,
        low: &'a Expr,
        high: &'a Expr,
        negated: bool,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let low = Expr::BinaryOp {
            left: Box::new(expr.clone()),
            op: if negated {
                BinaryOperator::Lt
            } else {
                BinaryOperator::GtEq
            },
            right: Box::new(low.clone()),
        };
        let high = Expr::BinaryOp {
            left: Box::new(expr.clone()),
            op: if negated {
                BinaryOperator::Gt
            } else {
                BinaryOperator::LtEq
            },
            right: Box::new(high.clone()),
        };
        let both = Expr::BinaryOp {
            left: Box::new(low),
            op: if negated {
                BinaryOperator::Or
            } else {
                BinaryOperator::And
            },
            right: Box::new(high),
        };
        self.plan_expr(ctx, &both)
    }

    fn plan_in_list<'a>(
        &self,
        ctx: &ExprContext,
        expr: &'a Expr,
        list: &'a [Expr],
        negated: bool,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let mut cond = Expr::Value(Value::Boolean(false));
        for l in list {
            cond = Expr::BinaryOp {
                left: Box::new(cond),
                op: BinaryOperator::Or,
                right: Box::new(Expr::BinaryOp {
                    left: Box::new(expr.clone()),
                    op: BinaryOperator::Eq,
                    right: Box::new(l.clone()),
                }),
            }
        }
        if negated {
            cond = Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(cond),
            }
        }
        self.plan_expr(ctx, &cond)
    }

    fn plan_case<'a>(
        &self,
        ctx: &ExprContext,
        operand: &'a Option<Box<Expr>>,
        conditions: &'a [Expr],
        results: &'a [Expr],
        else_result: &'a Option<Box<Expr>>,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let mut cond_exprs = Vec::new();
        let mut result_exprs = Vec::new();
        for (c, r) in conditions.iter().zip(results) {
            let c = match operand {
                Some(operand) => Expr::BinaryOp {
                    left: operand.clone(),
                    op: BinaryOperator::Eq,
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
                let typ = ColumnType::new(ScalarType::Null);
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
            Value::Decimal(d) => {
                // Our SQL parser parses negative numbers as a unary negation
                // of a positive literal, so we don't need to worry about
                // negative numbers here.
                assert_ne!(d.sign(), num_bigint::Sign::Minus);
                let (bigint, scale) = d.as_bigint_and_exponent();
                let precision = cmp::max(d.digits(), scale as u64);
                if precision > 38 {
                    bail!(
                        "decimal literal has precision {}, which exceeds maximum precision of 38",
                        precision
                    );
                }
                let mut significand: i128 = 0;
                for digit in bigint.to_radix_be(2).1 {
                    significand = (significand << 1) + i128::from(digit);
                }
                (
                    Datum::from(significand),
                    ScalarType::Decimal(precision as u8, scale as u8),
                )
            }
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
        let typ = ColumnType::new(scalar_type).nullable(nullable);
        Ok((expr, typ))
    }
}

fn extract_sql_object_name(n: &ObjectName) -> Result<String, failure::Error> {
    if n.0.len() != 1 {
        bail!("qualified names are not yet supported: {}", n.to_string())
    }
    Ok(n.to_string())
}

// When types don't match exactly, SQL has some poorly-documented type promotion
// rules. For now, just promote integers into decimals or floats, decimals into
// floats, and small Xs into bigger Xs.
fn try_coalesce_types<C>(
    exprs: Vec<(ScalarExpr, ColumnType)>,
    context: C,
) -> Result<(Vec<ScalarExpr>, ColumnType), failure::Error>
where
    C: fmt::Display + Copy,
{
    assert!(!exprs.is_empty());

    let scalar_type_prec = |scalar_type: &ScalarType| match scalar_type {
        ScalarType::Null => 0,
        ScalarType::Int32 => 1,
        ScalarType::Int64 => 2,
        ScalarType::Decimal(_, _) => 3,
        ScalarType::Float32 => 4,
        ScalarType::Float64 => 5,
        _ => 6,
    };
    let max_scalar_type = exprs
        .iter()
        .map(|(_expr, typ)| &typ.scalar_type)
        .max_by_key(|scalar_type| scalar_type_prec(scalar_type))
        .unwrap()
        .clone();
    let nullable = exprs.iter().any(|(_expr, typ)| typ.nullable);
    let mut out = Vec::new();
    let out_typ = ColumnType::new(max_scalar_type).nullable(nullable);
    for (expr, typ) in exprs {
        match plan_cast_internal(context, expr, &typ, out_typ.scalar_type.clone()) {
            Ok((expr, _)) => out.push(expr),
            Err(_) => bail!(
                "{} does not have uniform type: {:?} vs {:?}",
                context,
                typ,
                out_typ
            ),
        }
    }
    Ok((out, out_typ))
}

fn plan_cast_internal<C>(
    context: C,
    expr: ScalarExpr,
    from_type: &ColumnType,
    to_scalar_type: ScalarType,
) -> Result<(ScalarExpr, ColumnType), failure::Error>
where
    C: fmt::Display + Copy,
{
    use ScalarType::*;
    use UnaryFunc::*;
    let to_type = ColumnType::new(to_scalar_type).nullable(from_type.nullable);
    let expr = match (&from_type.scalar_type, &to_type.scalar_type) {
        (Int32, Float32) => expr.call_unary(CastInt32ToFloat32),
        (Int32, Float64) => expr.call_unary(CastInt32ToFloat64),
        (Int64, Int32) => expr.call_unary(CastInt64ToInt32),
        (Int32, Decimal(_, s)) => rescale_decimal(expr.call_unary(CastInt32ToDecimal), 0, *s),
        (Int64, Decimal(_, s)) => rescale_decimal(expr.call_unary(CastInt64ToDecimal), 0, *s),
        (Int64, Float32) => expr.call_unary(CastInt64ToFloat32),
        (Int64, Float64) => expr.call_unary(CastInt64ToFloat64),
        (Float32, Int64) => expr.call_unary(CastFloat32ToInt64),
        (Float32, Float64) => expr.call_unary(CastFloat32ToFloat64),
        (Float64, Int64) => expr.call_unary(CastFloat64ToInt64),
        (Decimal(_, s), Int32) => rescale_decimal(expr, *s, 0).call_unary(CastDecimalToInt32),
        (Decimal(_, s), Int64) => rescale_decimal(expr, *s, 0).call_unary(CastDecimalToInt64),
        (Decimal(_, s), Float32) => {
            let factor = 10_f32.powi(i32::from(*s));
            let factor = ScalarExpr::Literal(Datum::from(factor));
            expr.call_unary(CastDecimalToFloat32)
                .call_binary(factor, BinaryFunc::DivFloat32)
        }
        (Decimal(_, s), Float64) => {
            let factor = 10_f64.powi(i32::from(*s));
            let factor = ScalarExpr::Literal(Datum::from(factor));
            expr.call_unary(CastDecimalToFloat64)
                .call_binary(factor, BinaryFunc::DivFloat64)
        }
        (Decimal(_, s1), Decimal(_, s2)) => rescale_decimal(expr, *s1, *s2),
        (Null, _) => expr,
        (from, to) if from == to => expr,
        (from, to) => {
            bail!(
                "{} does not support casting from {:?} to {:?}",
                context,
                from,
                to
            );
        }
    };
    Ok((expr, to_type))
}

fn rescale_decimal(expr: ScalarExpr, s1: u8, s2: u8) -> ScalarExpr {
    if s2 > s1 {
        let factor = 10_i128.pow(u32::from(s2 - s1));
        let factor = ScalarExpr::Literal(Datum::from(factor));
        expr.call_binary(factor, BinaryFunc::MulDecimal)
    } else if s1 > s2 {
        let factor = 10_i128.pow(u32::from(s1 - s2));
        let factor = ScalarExpr::Literal(Datum::from(factor));
        expr.call_binary(factor, BinaryFunc::DivDecimal)
    } else {
        expr
    }
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

struct AggregateFuncVisitor<'ast> {
    aggs: Vec<&'ast Function>,
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

    fn into_result(self) -> Result<Vec<&'ast Function>, failure::Error> {
        match self.err {
            Some(err) => Err(err),
            None => Ok(self.aggs),
        }
    }
}

impl<'ast> Visit<'ast> for AggregateFuncVisitor<'ast> {
    fn visit_function(&mut self, func: &'ast Function) {
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

    fn visit_subquery(&mut self, _subquery: &'ast Query) {
        // don't go into subqueries
    }
}

struct ExprContext<'a> {
    name: &'static str,
    scope: &'a Scope,
    /// If None, aggregates are not allowed in this context
    /// If Some(nodes), nodes.get(aggregate) tells us which column to find the aggregate in
    aggregate_context: Option<&'a HashMap<&'a Function, (usize, ColumnType)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
    fn empty() -> Self {
        Scope { items: vec![] }
    }

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
    fn let_<F>(self: RelationExpr, f: F) -> Result<(Self, Scope), failure::Error>
    where
        F: FnOnce(Self) -> Result<(Self, Scope), failure::Error>,
    {
        let name = format!("tmp_{}", Uuid::new_v4());
        let typ = self.typ();
        let (body, scope) = f(RelationExpr::Get {
            name: name.clone(),
            typ,
        })?;
        let expr = RelationExpr::Let {
            name: name.clone(),
            value: Box::new(self),
            body: Box::new(body),
        };
        Ok((expr, scope))
    }
}
