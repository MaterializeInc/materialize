// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL-dataflow translation.

#![deny(missing_debug_implementations)]

use std::convert::TryInto;

use dataflow_types::logging::LoggingConfig;
use dataflow_types::{
    ColumnOrder, Dataflow, KafkaSinkConnector, KafkaSourceConnector, PeekWhen, RowSetFinishing,
    Sink, SinkConnector, Source, SourceConnector, View,
};
use failure::{bail, ensure, format_err, ResultExt};
use interchange::avro;
use ore::collections::CollectionExt;
use ore::iter::{FallibleIteratorExt, IteratorExt};
use ore::option::OptionExt;
use repr::decimal::MAX_DECIMAL_PRECISION;
use repr::{ColumnType, Datum, RelationType, ScalarType};
use sqlparser::ast::visit::{self, Visit};
use sqlparser::ast::{
    BinaryOperator, DataType, Expr, Function, Ident, JoinConstraint, JoinOperator, ObjectName,
    ObjectType, ParsedDate, ParsedTimestamp, Query, Select, SelectItem, SetExpr, SetOperator,
    SetVariableValue, ShowStatementFilter, SourceSchema, Stage, Statement, TableAlias, TableFactor,
    TableWithJoins, UnaryOperator, Value, Values,
};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser as SqlParser;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::iter::FromIterator;
use std::net::{SocketAddr, ToSocketAddrs};
use store::{DataflowStore, RemoveMode};
use url::Url;
use uuid::Uuid;

mod expr;
mod session;
pub mod store;
mod transform;

use self::expr::like::build_like_regex_from_string;
use self::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, ColumnRef, JoinKind, RelationExpr, ScalarExpr,
    UnaryFunc, VariadicFunc,
};
pub use session::Session;

/// Instructions for executing a SQL query.
#[derive(Debug)]
pub enum Plan {
    CreateSource(Source),
    CreateSources(Vec<Source>),
    CreateSink(Sink),
    CreateView(View),
    DropSources(Vec<String>),
    DropViews(Vec<String>),
    EmptyQuery,
    DidSetVariable,
    Peek {
        source: ::expr::RelationExpr,
        when: PeekWhen,
        transform: RowSetFinishing,
    },
    Tail(Dataflow),
    SendRows {
        typ: RelationType,
        rows: Vec<Vec<Datum>>,
    },
    ExplainPlan {
        typ: RelationType,
        relation_expr: ::expr::RelationExpr,
    },
}

/// Converts raw SQL queries into [`Plan`]s.
#[derive(Debug)]
pub struct Planner {
    pub dataflows: DataflowStore,
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
        transform::transform(&mut stmt);
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

    fn plan_query(
        &self,
        q: &Query,
        outer_scope: &Scope,
    ) -> Result<(RelationExpr, RowSetFinishing), failure::Error> {
        if !q.ctes.is_empty() {
            bail!("CTEs are not yet supported");
        }
        let limit = match &q.limit {
            None => None,
            Some(Expr::Value(Value::Number(x))) => Some(x.parse()?),
            _ => bail!("LIMIT must be an integer constant"),
        };
        let expr = self.plan_set_expr(&q.body, outer_scope)?;
        let output_typ = expr.typ();
        // This is O(m*n) where m is the number of columns and n is the number of order keys.
        // If this ever becomes a bottleneck (which I doubt) it is easy enough to make faster...
        let order: Result<Vec<ColumnOrder>, failure::Error> = q
            .order_by
            .iter()
            .map(|obe| match &obe.expr {
                Expr::Identifier(col_name) => output_typ
                    .column_types
                    .iter()
                    .enumerate()
                    .find(|(_idx, ct)| ct.name.as_ref() == Some(col_name))
                    .map(|(idx, _ct)| ColumnOrder {
                        column: idx,
                        desc: match obe.asc {
                            None => false,
                            Some(asc) => !asc,
                        },
                    })
                    .ok_or_else(|| format_err!("ORDER BY key must be an output column name.")),
                Expr::Value(Value::Number(n)) => {
                    let n = n.parse::<usize>().with_context(|err| {
                        format_err!(
                            "unable to parse column reference in ORDER BY: {}: {}",
                            err,
                            n
                        )
                    })?;
                    let max = output_typ.column_types.len();
                    if n < 1 || n > max {
                        bail!(
                            "column reference {} in ORDER BY is out of range (1 - {})",
                            n,
                            max
                        );
                    }
                    Ok(ColumnOrder {
                        column: n - 1,
                        desc: match obe.asc {
                            None => false,
                            Some(asc) => !asc,
                        },
                    })
                }
                _ => Err(format_err!(
                    "Arbitrary expressions for ORDER BY keys are not yet supported."
                )),
            })
            .collect();
        let transform = RowSetFinishing {
            order_by: order?,
            limit,
        };
        Ok((expr, transform))
    }

    fn plan_set_expr(
        &self,
        q: &SetExpr,
        outer_scope: &Scope,
    ) -> Result<RelationExpr, failure::Error> {
        match q {
            SetExpr::Select(select) => self.plan_view_select(select, outer_scope),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                let left_expr = self.plan_set_expr(left, outer_scope)?;
                let right_expr = self.plan_set_expr(right, outer_scope)?;

                // TODO(jamii) this type-checking is redundant with RelationExpr::typ, but currently it seems that we need both because RelationExpr::typ is not allowed to return errors
                let left_types = &left_expr.typ().column_types;
                let right_types = &right_expr.typ().column_types;
                if left_types.len() != right_types.len() {
                    bail!(
                        "set operation {:?} with {:?} and {:?} columns not supported",
                        op,
                        left_types.len(),
                        right_types.len(),
                    );
                }
                for (left_col_type, right_col_type) in left_types.iter().zip(right_types.iter()) {
                    left_col_type.union(right_col_type)?;
                }

                let relation_expr = match op {
                    SetOperator::Union => {
                        if *all {
                            left_expr.union(right_expr)
                        } else {
                            left_expr.union(right_expr).distinct()
                        }
                    }
                    SetOperator::Except => {
                        if *all {
                            left_expr.union(right_expr.negate()).threshold()
                        } else {
                            left_expr
                                .distinct()
                                .union(right_expr.distinct().negate())
                                .threshold()
                        }
                    }
                    SetOperator::Intersect => {
                        // TODO: Let's not duplicate the left-hand expression into TWO dataflows!
                        // Though we believe that render() does The Right Thing (TM)
                        // Also note that we do *not* need another threshold() at the end of the method chain
                        // because the right-hand side of the outer union only produces existing records,
                        // i.e., the record counts for differential data flow definitely remain non-negative.
                        let left_clone = left_expr.clone();
                        if *all {
                            left_expr
                                .union(left_clone.union(right_expr.negate()).threshold().negate())
                        } else {
                            left_expr
                                .union(left_clone.union(right_expr.negate()).threshold().negate())
                                .distinct()
                        }
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
                    scope: &Scope::empty(Some(outer_scope.clone())),
                    aggregate_context: None,
                };
                let mut expr: Option<RelationExpr> = None;
                let mut types: Option<Vec<ColumnType>> = None;
                for row in values {
                    let mut value_exprs = vec![];
                    for (i, value) in row.iter().enumerate() {
                        let (expr, mut typ) = self.plan_expr(ctx, value)?;
                        typ.name = Some(format!("column{}", i + 1));
                        value_exprs.push((expr, typ));
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
            SetExpr::Query(query) => {
                let (expr, transform) = self.plan_query(query, outer_scope)?;
                if transform != Default::default() {
                    bail!("ORDER BY and LIMIT are not yet supported in subqueries");
                }
                Ok(expr)
            }
        }
    }

    fn plan_view_select(
        &self,
        s: &Select,
        outer_scope: &Scope,
    ) -> Result<RelationExpr, failure::Error> {
        // Step 1. Handle FROM clause, including joins.
        let (mut relation_expr, from_scope) = s
            .from
            .iter()
            .map(|twj| self.plan_table_with_joins(twj, outer_scope))
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
                let typ = RelationType::new(vec![ColumnType::new(ScalarType::String).name("x")]);
                Ok((
                    RelationExpr::Constant {
                        rows: vec![vec![Datum::String("X".into())]],
                        typ: typ.clone(),
                    },
                    Scope::from_source(Some("dual"), typ, Some(outer_scope.clone())),
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
            let mut group_scope = Scope::empty(Some(outer_scope.clone()));
            let mut select_all_mapping = HashMap::new();
            for expr in &s.group_by {
                let (expr, typ) = self.plan_expr(ctx, &expr)?;
                match &expr {
                    ScalarExpr::Column(ColumnRef::Inner(i)) => {
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
                        group_scope.items.push(ScopeItem { names: vec![], typ });
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
                group_scope.items.push(ScopeItem { names: vec![], typ });
            }
            if !aggregates.is_empty() || !group_key.is_empty() || s.having.is_some() {
                // apply GROUP BY / aggregates
                relation_expr = relation_expr
                    .map(group_exprs)
                    .reduce(group_key.clone(), aggregates.clone());
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
            let relation_type = relation_expr.typ();
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
                        ScalarExpr::Column(ColumnRef::Inner(i))
                            if typ.name == relation_type.column_types[*i].name =>
                        {
                            // Note that if the column name changed (i.e.,
                            // because the select item was aliased), then we
                            // can't take this fast path, or we'll lose the
                            // alias. Hence the guard on this match arm.
                            //
                            // TODO(benesch): this is a dumb restriction. If
                            // this optimization is actually important, perhaps
                            // it should become a proper query transformation
                            // and we shouldn't bother trying to do it here.
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
        outer_scope: &Scope,
    ) -> Result<(RelationExpr, Scope), failure::Error> {
        let (mut left, mut left_scope) =
            self.plan_table_factor(&table_with_joins.relation, outer_scope)?;
        for join in &table_with_joins.joins {
            let (right, right_scope) = self.plan_table_factor(&join.relation, outer_scope)?;
            let (new_left, new_left_scope) =
                self.plan_join_operator(&join.join_operator, left, left_scope, right, right_scope)?;
            left = new_left;
            left_scope = new_left_scope;
        }
        Ok((left, left_scope))
    }

    fn plan_table_factor<'a>(
        &self,
        table_factor: &'a TableFactor,
        outer_scope: &Scope,
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
                let alias = if let Some(TableAlias { name, columns }) = alias {
                    if !columns.is_empty() {
                        bail!("aliasing columns is not yet supported");
                    }
                    name.to_owned()
                } else {
                    name
                };
                let scope =
                    Scope::from_source(Some(&alias), typ.clone(), Some(outer_scope.clone()));
                Ok((expr, scope))
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    bail!("LATERAL derived tables are not yet supported");
                }
                let (expr, finishing) = self.plan_query(&subquery, &Scope::empty(None))?;
                if finishing != Default::default() {
                    bail!("ORDER BY and LIMIT are not yet supported in subqueries");
                }
                let alias = if let Some(TableAlias { name, columns }) = alias {
                    if !columns.is_empty() {
                        bail!("aliasing columns is not yet supported");
                    }
                    Some(name.as_str())
                } else {
                    None
                };
                let scope = Scope::from_source(alias, expr.typ(), Some(outer_scope.clone()));
                Ok((expr, scope))
            }
            TableFactor::NestedJoin(table_with_joins) => {
                self.plan_table_with_joins(table_with_joins, outer_scope)
            }
        }
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
                        format_err!("internal error: unable to resolve scope item {:?}", item)
                    })?;
                    Ok((ScalarExpr::Column(ColumnRef::Inner(*j)), item.typ.clone()))
                })
                .collect::<Result<Vec<_>, _>>(),
            SelectItem::QualifiedWildcard(table_name) => {
                let table_name = Some(extract_sql_object_name(table_name)?);
                select_all_scope
                    .items
                    .iter()
                    .enumerate()
                    .filter_map(|(i, item)| {
                        item.names
                            .iter()
                            .find(|name| name.table_name == table_name)
                            .map(|_name| (i, item))
                    })
                    .map(|(i, item)| {
                        let j = select_all_mapping.get(&i).ok_or_else(|| {
                            format_err!("internal error: unable to resolve scope item {:?}", item)
                        })?;
                        Ok((ScalarExpr::Column(ColumnRef::Inner(*j)), item.typ.clone()))
                    })
                    .collect::<Result<Vec<_>, _>>()
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
                JoinKind::Inner,
            ),
            JoinOperator::LeftOuter(constraint) => self.plan_join_constraint(
                &constraint,
                left,
                left_scope,
                right,
                right_scope,
                JoinKind::LeftOuter,
            ),
            JoinOperator::RightOuter(constraint) => self.plan_join_constraint(
                &constraint,
                left,
                left_scope,
                right,
                right_scope,
                JoinKind::RightOuter,
            ),
            JoinOperator::FullOuter(constraint) => self.plan_join_constraint(
                &constraint,
                left,
                left_scope,
                right,
                right_scope,
                JoinKind::FullOuter,
            ),
            JoinOperator::CrossJoin => Ok((left.product(right), left_scope.product(right_scope))),
            // The remaining join types are MSSQL-specific. We are unlikely to
            // ever support them. The standard SQL equivalent is LATERAL, which
            // we are not capable of even parsing at the moment.
            JoinOperator::CrossApply => bail!("CROSS APPLY is not supported"),
            JoinOperator::OuterApply => bail!("OUTER APPLY is not supported"),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn plan_join_constraint<'a>(
        &self,
        constraint: &'a JoinConstraint,
        left: RelationExpr,
        left_scope: Scope,
        right: RelationExpr,
        right_scope: Scope,
        kind: JoinKind,
    ) -> Result<(RelationExpr, Scope), failure::Error> {
        match constraint {
            JoinConstraint::On(expr) => {
                let mut product_scope = left_scope.product(right_scope);
                let ctx = &ExprContext {
                    name: "ON clause",
                    scope: &product_scope,
                    aggregate_context: None,
                };
                let (on, _) = self.plan_expr(ctx, expr)?;
                for (l, r) in find_trivial_column_equivalences(&on) {
                    // When we can statically prove that two columns are
                    // equivalent after a join, the right column becomes
                    // unnamable and the left column assumes both names. This
                    // permits queries like
                    //
                    //     SELECT rhs.a FROM lhs JOIN rhs ON lhs.a = rhs.a
                    //     GROUP BY lhs.a
                    //
                    // which otherwise would fail because rhs.a appears to be
                    // a column that does not appear in the GROUP BY.
                    //
                    // Note that this is a MySQL-ism; PostgreSQL does not do
                    // this sort of equivalence detection for ON constraints.
                    let right_names =
                        std::mem::replace(&mut product_scope.items[r].names, Vec::new());
                    product_scope.items[l].names.extend(right_names);
                }
                let joined = RelationExpr::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on,
                    kind,
                };
                Ok((joined, product_scope))
            }
            JoinConstraint::Using(column_names) => self.plan_using_constraint(
                &column_names,
                left,
                left_scope,
                right,
                right_scope,
                kind,
            ),
            JoinConstraint::Natural => {
                let mut column_names = vec![];
                for item in left_scope.items.iter() {
                    for name in &item.names {
                        if let Some(column_name) = &name.column_name {
                            if left_scope.resolve_column(column_name).is_ok()
                                && right_scope.resolve_column(column_name).is_ok()
                            {
                                column_names.push(column_name.clone());
                                break;
                            }
                        }
                    }
                }
                self.plan_using_constraint(
                    &column_names,
                    left,
                    left_scope,
                    right,
                    right_scope,
                    kind,
                )
            }
        }
    }

    // See page 440 of ANSI SQL 2016 spec for details on scoping of using/natural joins
    #[allow(clippy::too_many_arguments)]
    fn plan_using_constraint(
        &self,
        column_names: &[String],
        left: RelationExpr,
        left_scope: Scope,
        right: RelationExpr,
        right_scope: Scope,
        kind: JoinKind,
    ) -> Result<(RelationExpr, Scope), failure::Error> {
        let mut join_exprs = vec![];
        let mut map_exprs = vec![];
        let mut new_items = vec![];
        let mut dropped_columns = HashSet::new();
        for column_name in column_names {
            let (l, l_item) = left_scope.resolve_column(column_name)?;
            let (r, r_item) = right_scope.resolve_column(column_name)?;
            let l = match l {
                ColumnRef::Inner(l) => l,
                ColumnRef::Outer(_) => bail!(
                    "Internal error: name {} in USING resolved to outer column",
                    column_name
                ),
            };
            let r = match r {
                ColumnRef::Inner(r) => r,
                ColumnRef::Outer(_) => bail!(
                    "Internal error: name {} in USING resolved to outer column",
                    column_name
                ),
            };
            let typ = l_item.typ.union(&r_item.typ)?;
            join_exprs.push(ScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1: Box::new(ScalarExpr::Column(ColumnRef::Inner(l))),
                expr2: Box::new(ScalarExpr::Column(ColumnRef::Inner(left_scope.len() + r))),
            });
            map_exprs.push((
                ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![
                        ScalarExpr::Column(ColumnRef::Inner(l)),
                        ScalarExpr::Column(ColumnRef::Inner(left_scope.len() + r)),
                    ],
                },
                typ.clone(),
            ));
            let mut names = l_item.names.clone();
            names.extend(r_item.names.clone());
            new_items.push(ScopeItem { names, typ });
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
        let mut both_scope = left_scope.product(right_scope);
        both_scope.items.extend(new_items);
        let both_scope = both_scope.project(&project_key);
        let both = RelationExpr::Join {
            left: Box::new(left),
            right: Box::new(right),
            on: join_exprs
                .into_iter()
                .fold(ScalarExpr::Literal(Datum::True), |expr1, expr2| {
                    ScalarExpr::CallBinary {
                        func: BinaryFunc::And,
                        expr1: Box::new(expr1),
                        expr2: Box::new(expr2),
                    }
                }),
            kind,
        }
        .map(map_exprs)
        .project(project_key);
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
            Expr::Exists(query) => {
                let (expr, transform) = self.plan_query(query, &ctx.scope)?;
                if transform != Default::default() {
                    bail!("ORDER BY and LIMIT are not yet supported in subqueries");
                }
                Ok((expr.exists(), ColumnType::new(ScalarType::Bool)))
            }
            Expr::Subquery(query) => {
                let (expr, transform) = self.plan_query(query, &ctx.scope)?;
                if transform != Default::default() {
                    bail!("ORDER BY and LIMIT are not yet supported in subqueries");
                }
                let column_types = expr.typ().column_types;
                if column_types.len() != 1 {
                    bail!(
                        "Expected subselect to return 1 column, got {} columns",
                        column_types.len()
                    );
                }
                let mut column_type = column_types.into_element();
                column_type.nullable = true;
                Ok((expr.select(), column_type))
            }
            Expr::Any {
                left,
                op,
                right,
                some: _,
            } => self.plan_any_or_all(ctx, left, op, right, AggregateFunc::Any),
            Expr::All { left, op, right } => {
                self.plan_any_or_all(ctx, left, op, right, AggregateFunc::All)
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                use BinaryOperator::{Eq, NotEq};
                if *negated {
                    // `<expr> NOT IN (<subquery>)` is equivalent to
                    // `<expr> <> ALL (<subquery>)`.
                    self.plan_any_or_all(ctx, expr, &NotEq, subquery, AggregateFunc::All)
                } else {
                    // `<expr> IN (<subquery>)` is equivalent to
                    // `<expr> = ANY (<subquery>)`.
                    self.plan_any_or_all(ctx, expr, &Eq, subquery, AggregateFunc::Any)
                }
            }
            _ => bail!(
                "complicated expressions are not yet supported: {}",
                e.to_string()
            ),
        }
    }

    fn plan_any_or_all<'a>(
        &self,
        ctx: &ExprContext,
        left: &'a Expr,
        op: &'a BinaryOperator,
        right: &'a Query,
        func: AggregateFunc,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        // plan right
        let (right, transform) = self.plan_query(right, &ctx.scope)?;
        if transform != Default::default() {
            bail!("ORDER BY and LIMIT are not yet supported in subqueries");
        }
        let column_types = right.typ().column_types;
        if column_types.len() != 1 {
            bail!(
                "Expected subquery of ANY to return 1 column, got {} columns",
                column_types.len()
            );
        }
        let right_type = column_types.into_element();

        // plan left and op
        // this is a bit of a hack - we want to plan `op` as if the original expr was `(SELECT ANY/ALL(left op right[1]) FROM right)`
        let mut scope = Scope::empty(Some(ctx.scope.clone()));
        let right_name = format!("right_{}", Uuid::new_v4());
        scope.items.push(ScopeItem {
            names: vec![ScopeItemName {
                table_name: right_name.clone(),
                column_name: Some(right_name.clone()),
            }],
            typ: right_type,
        });
        let any_ctx = ExprContext {
            name: "WHERE clause",
            scope: &scope,
            aggregate_context: None,
        };
        let (op_expr, op_type) =
            self.plan_binary_op(&any_ctx, op, left, &Expr::Identifier(right_name))?;

        // plan subquery
        let right_arity = right.arity();
        let expr = right
            .map(vec![(op_expr, op_type.clone())])
            .reduce(
                vec![],
                vec![(
                    AggregateExpr {
                        func,
                        expr: Box::new(ScalarExpr::Column(ColumnRef::Inner(right_arity))),
                        distinct: true,
                    },
                    op_type.clone(),
                )],
            )
            .select();
        Ok((
            expr,
            ColumnType {
                name: None,
                nullable: op_type.nullable,
                scalar_type: ScalarType::Bool,
            },
        ))
    }

    fn plan_cast<'a>(
        &self,
        ctx: &ExprContext,
        expr: &'a Expr,
        data_type: &'a DataType,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let to_scalar_type = scalar_type_from_sql(data_type)?;
        let (expr, from_type) = self.plan_expr(ctx, expr)?;
        plan_cast_internal("CAST", expr, &from_type, to_scalar_type)
    }

    fn plan_aggregate(
        &self,
        ctx: &ExprContext,
        sql_func: &Function,
    ) -> Result<(AggregateExpr, ColumnType), failure::Error> {
        let ident = sql_func.name.to_string().to_lowercase();
        assert!(is_aggregate_func(&ident));

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
                let (func, scalar_type) = find_agg_func(&ident, &typ.scalar_type)?;
                (expr, func, scalar_type)
            }
        };
        let typ = ColumnType::new(scalar_type)
            .name(ident.clone())
            .nullable(func.is_nullable());
        Ok((
            AggregateExpr {
                func,
                expr: Box::new(expr),
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
        if is_aggregate_func(&ident) {
            if let Some(agg_cx) = ctx.aggregate_context {
                let (i, typ) = agg_cx.get(sql_func).ok_or_else(|| {
                    format_err!(
                        "Internal error: encountered unplanned aggregate function: {:?}",
                        sql_func,
                    )
                })?;
                Ok((ScalarExpr::Column(ColumnRef::Inner(*i)), typ.clone()))
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

    /// Figure out what the Expression should be, and the value of the result
    fn plan_binary_op<'a>(
        &self,
        ctx: &ExprContext,
        op: &'a BinaryOperator,
        left: &'a Expr,
        right: &'a Expr,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (mut lexpr, lty) = self.plan_expr(ctx, left)?;
        let (mut rexpr, rty) = self.plan_expr(ctx, right)?;

        let both_decimals = match (&lty.scalar_type, &rty.scalar_type) {
            (ScalarType::Decimal(_, _), ScalarType::Decimal(_, _)) => true,
            _ => false,
        };

        let (mut ltype, mut rtype, timelike) = match (&lty.scalar_type, &rty.scalar_type) {
            (ScalarType::Date, ScalarType::Interval) => (lty, rty, true),
            (ScalarType::Timestamp, ScalarType::Interval) => (lty, rty, true),
            // for intervals on the left, flip them around
            (ScalarType::Interval, ScalarType::Date) => (rty, lty, true),
            (ScalarType::Interval, ScalarType::Timestamp) => (rty, lty, true),
            (_, _) => (lty, rty, false),
        };

        let is_cmp = op == &BinaryOperator::Lt
            || op == &BinaryOperator::LtEq
            || op == &BinaryOperator::Gt
            || op == &BinaryOperator::GtEq
            || op == &BinaryOperator::Eq
            || op == &BinaryOperator::NotEq;

        let is_arithmetic = op == &BinaryOperator::Plus
            || op == &BinaryOperator::Minus
            || op == &BinaryOperator::Multiply
            || op == &BinaryOperator::Divide
            || op == &BinaryOperator::Modulus;

        // For arithmetic there are two type categories where we skip coalescing:
        //
        // * both inputs are already decimals: it could result in a rescale
        //   if the decimals have different precisions, because we tightly
        //   control the rescale when planning the arithmetic operation (below).
        //   E.g., decimal multiplication does not need to rescale its inputs,
        //   even when the inputs have different scales.
        // * inputs are timelike: math is non commutative, there are very
        //   specific rules about what makes sense, defined in the specific
        //   comparisons below
        if is_cmp || (is_arithmetic && !(both_decimals || timelike)) {
            let ctx = op.to_string();
            let (mut exprs, typ) = try_coalesce_types(vec![(lexpr, ltype), (rexpr, rtype)], &ctx)?;
            assert_eq!(exprs.len(), 2);
            rexpr = exprs.pop().unwrap();
            lexpr = exprs.pop().unwrap();
            rtype = typ.clone();
            ltype = typ;
        }

        // Arithmetic operations follow Snowflake's rules for precision/scale
        // conversions. [0]
        //
        // [0]: https://docs.snowflake.net/manuals/sql-reference/operators-arithmetic.html
        match (&op, &ltype.scalar_type, &rtype.scalar_type) {
            (BinaryOperator::Plus, ScalarType::Decimal(p1, s1), ScalarType::Decimal(p2, s2))
            | (BinaryOperator::Minus, ScalarType::Decimal(p1, s1), ScalarType::Decimal(p2, s2))
            | (BinaryOperator::Modulus, ScalarType::Decimal(p1, s1), ScalarType::Decimal(p2, s2)) =>
            {
                let p = cmp::max(p1, p2) + 1;
                let so = cmp::max(s1, s2);
                let lexpr = rescale_decimal(lexpr, *s1, *so);
                let rexpr = rescale_decimal(rexpr, *s2, *so);
                let func = if op == &BinaryOperator::Plus {
                    BinaryFunc::AddDecimal
                } else if op == &BinaryOperator::Minus {
                    BinaryFunc::SubDecimal
                } else {
                    BinaryFunc::ModDecimal
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
                let expr = rescale_decimal(expr, si - *s2, s);
                let p = (p1 - s1) + s2 + s;
                let typ = ColumnType::new(ScalarType::Decimal(p, s)).nullable(true);
                return Ok((expr, typ));
            }
            _ => (),
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
                (ScalarType::Date, ScalarType::Interval) => {
                    (BinaryFunc::AddTimelikeWithInterval, ScalarType::Date)
                }
                (ScalarType::Timestamp, ScalarType::Interval) => {
                    (BinaryFunc::AddTimelikeWithInterval, ScalarType::Date)
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
                (ScalarType::Date, ScalarType::Interval) => {
                    (BinaryFunc::SubTimelikeWithInterval, ScalarType::Timestamp)
                }
                (ScalarType::Timestamp, ScalarType::Interval) => {
                    (BinaryFunc::SubTimelikeWithInterval, ScalarType::Timestamp)
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
            Value::Number(s) => {
                let mut significand: i128 = 0;
                let mut precision = 0;
                let mut scale = 0;
                let mut seen_decimal = false;
                for c in s.chars() {
                    if c == '.' {
                        if seen_decimal {
                            bail!("more than one decimal point in numeric literal: {}", s)
                        }
                        seen_decimal = true;
                        continue;
                    }

                    precision += 1;
                    if seen_decimal {
                        scale += 1;
                    }

                    let digit = c
                        .to_digit(10)
                        .ok_or_else(|| format_err!("invalid digit in numeric literal: {}", s))?;
                    significand = significand
                        .checked_mul(10)
                        .ok_or_else(|| format_err!("numeric literal overflows i128: {}", s))?;
                    significand = significand
                        .checked_add(i128::from(digit))
                        .ok_or_else(|| format_err!("numeric literal overflows i128: {}", s))?;
                }
                if precision > MAX_DECIMAL_PRECISION {
                    bail!("numeric literal exceeds maximum precision: {}", s)
                } else if scale == 0 {
                    match significand.try_into() {
                        Ok(n) => (Datum::Int64(n), ScalarType::Int64),
                        Err(_) => (
                            Datum::from(significand),
                            ScalarType::Decimal(precision as u8, scale as u8),
                        ),
                    }
                } else {
                    (
                        Datum::from(significand),
                        ScalarType::Decimal(precision as u8, scale as u8),
                    )
                }
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
            Value::Date(_, ParsedDate { year, month, day }) => (
                Datum::from_ymd(
                    (*year)
                        .try_into()
                        .map_err(|e| format_err!("Year is too large {}: {}", year, e))?,
                    *month,
                    *day,
                )?,
                ScalarType::Date,
            ),
            Value::Timestamp(
                _,
                ParsedTimestamp {
                    year,
                    month,
                    day,
                    hour,
                    minute,
                    second,
                    nano,
                },
            ) => (
                Datum::from_ymd_hms_nano(
                    (*year)
                        .try_into()
                        .map_err(|e| format_err!("Year is too large {}: {}", year, e))?,
                    *month,
                    *day,
                    *hour,
                    *minute,
                    *second,
                    *nano,
                )?,
                ScalarType::Timestamp,
            ),
            Value::Time(_) => bail!("TIME literals are not supported: {}", l.to_string()),
            Value::Interval(iv) => {
                iv.fields_match_precision()?;
                let i = iv.computed_permissive()?;
                (Datum::Interval(i.into()), ScalarType::Interval)
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

fn find_trivial_column_equivalences(expr: &ScalarExpr) -> Vec<(usize, usize)> {
    use BinaryFunc::*;
    use ScalarExpr::*;
    let mut exprs = vec![expr];
    let mut equivalences = vec![];
    while let Some(expr) = exprs.pop() {
        match expr {
            CallBinary {
                func: Eq,
                expr1,
                expr2,
            } => {
                if let (Column(ColumnRef::Inner(l)), Column(ColumnRef::Inner(r))) =
                    (&**expr1, &**expr2)
                {
                    equivalences.push((*l, *r));
                }
            }
            CallBinary {
                func: And,
                expr1,
                expr2,
            } => {
                exprs.push(expr1);
                exprs.push(expr2);
            }
            _ => (),
        }
    }
    equivalences
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
    let out_typ = find_output_type(&exprs.iter().map(|(_, typ)| typ).collect::<Vec<_>>())?;
    let mut out = Vec::new();
    for (expr, typ) in exprs {
        match plan_cast_internal(context, expr, &typ, out_typ.scalar_type.clone()) {
            Ok((expr, _)) => out.push(expr),
            Err(_) => bail!(
                "{} does not have uniform type: {:?} vs {:?}",
                context,
                typ,
                out_typ,
            ),
        }
    }
    Ok((out, out_typ))
}

/// Find a type that we can expect the output of a sequence of expressions to be
///
/// There aren't any real guarantees about what we output, except that it's
/// possible that we'll be able to build this result.
///
/// # Examples
///
/// - `1i32 + 2i64` -> `i64`
/// - `1i64 + Decimal(2)` -> `Decimal`
fn find_output_type(col_typs: &[&ColumnType]) -> Result<ColumnType, failure::Error> {
    let scalar_type_prec = |scalar_type: &&ScalarType| match scalar_type {
        ScalarType::Null => 0,
        ScalarType::Int32 => 1,
        ScalarType::Int64 => 2,
        ScalarType::Decimal(_, _) => 3,
        ScalarType::Float32 => 4,
        ScalarType::Float64 => 5,
        _ => 6,
    };
    let nullable = col_typs.iter().any(|typ| typ.nullable);
    let max = col_typs
        .iter()
        .map(|typ| &typ.scalar_type)
        .max_by_key(scalar_type_prec);
    Ok(ColumnType::new(max.unwrap().clone()).nullable(nullable))
}

/// Figure out whether we need to cast a value in order for an operation to succeed
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
        (Int32, Int64) => expr.call_unary(CastInt32ToInt64),
        (Int32, Decimal(_, s)) => rescale_decimal(expr.call_unary(CastInt32ToDecimal), 0, *s),
        (Int64, Decimal(_, s)) => rescale_decimal(expr.call_unary(CastInt64ToDecimal), 0, *s),
        (Int64, Float32) => expr.call_unary(CastInt64ToFloat32),
        (Int64, Float64) => expr.call_unary(CastInt64ToFloat64),
        (Int64, Int32) => expr.call_unary(CastInt64ToInt32),
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

pub fn scalar_type_from_sql(data_type: &DataType) -> Result<ScalarType, failure::Error> {
    // NOTE this needs to stay in sync with sqllogictest::postgres::get_column
    Ok(match data_type {
        DataType::Boolean => ScalarType::Bool,
        DataType::Custom(name) if name.to_string().to_lowercase() == "bool" => ScalarType::Bool,
        DataType::Char(_) | DataType::Varchar(_) | DataType::Text => ScalarType::String,
        DataType::Custom(name) if name.to_string().to_lowercase() == "string" => ScalarType::String,
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
        DataType::Interval => ScalarType::Interval,
        DataType::Time => ScalarType::Time,
        DataType::Bytea => ScalarType::Bytes,
        other @ DataType::Array(_)
        | other @ DataType::Binary(..)
        | other @ DataType::Blob(_)
        | other @ DataType::Clob(_)
        | other @ DataType::Custom(_)
        | other @ DataType::Regclass
        | other @ DataType::Uuid
        | other @ DataType::Varbinary(_) => bail!("Unexpected SQL type: {:?}", other),
    })
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
        if is_aggregate_func(&name_str) {
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

#[derive(Debug)]
struct ExprContext<'a> {
    name: &'static str,
    scope: &'a Scope,
    /// If None, aggregates are not allowed in this context
    /// If Some(nodes), nodes.get(aggregate) tells us which column to find the aggregate in
    aggregate_context: Option<&'a HashMap<&'a Function, (usize, ColumnType)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopeItemName {
    table_name: Option<String>,
    column_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ScopeItem {
    names: Vec<ScopeItemName>,
    typ: ColumnType,
}

#[derive(Debug, Clone)]
struct Scope {
    // items in this query
    items: Vec<ScopeItem>,
    // items inherited from an enclosing query
    outer_items: Vec<ScopeItem>,
}

#[derive(Debug)]
enum Resolution<'a> {
    NotFound,
    Found((usize, &'a ScopeItem)),
    Ambiguous,
}

impl Scope {
    fn empty(outer_scope: Option<Scope>) -> Self {
        Scope {
            items: vec![],
            outer_items: if let Some(outer_scope) = outer_scope {
                outer_scope
                    .outer_items
                    .into_iter()
                    .chain(outer_scope.items.into_iter())
                    .collect()
            } else {
                vec![]
            },
        }
    }

    fn from_source(
        table_name: Option<&str>,
        typ: RelationType,
        outer_scope: Option<Scope>,
    ) -> Self {
        let mut scope = Scope::empty(outer_scope);
        scope.items = typ
            .column_types
            .into_iter()
            .map(|typ| ScopeItem {
                names: vec![ScopeItemName {
                    table_name: table_name.owned(),
                    column_name: typ.name.clone(),
                }],
                typ,
            })
            .collect();
        scope
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    fn resolve<'a, Matches>(
        &'a self,
        matches: Matches,
        name_in_error: &str,
    ) -> Result<(ColumnRef, &'a ScopeItem), failure::Error>
    where
        Matches: Fn(&ScopeItemName) -> bool,
    {
        let resolve_over = |items: &'a [ScopeItem]| {
            let mut results = items
                .iter()
                .enumerate()
                .map(|(pos, item)| item.names.iter().map(move |name| (pos, item, name)))
                .flatten()
                .filter(|(_, _, name)| (matches)(name));
            match results.next() {
                None => Resolution::NotFound,
                Some((pos, item, _name)) => {
                    if results.find(|(pos2, _item, _name)| pos != *pos2).is_none() {
                        Resolution::Found((pos, item))
                    } else {
                        Resolution::Ambiguous
                    }
                }
            }
        };
        match resolve_over(&self.items) {
            Resolution::NotFound => match resolve_over(&self.outer_items) {
                Resolution::NotFound => bail!("No column named {} in scope", name_in_error),
                Resolution::Found((pos, item)) => Ok((ColumnRef::Outer(pos), item)),
                Resolution::Ambiguous => bail!("Column name {} is ambiguous", name_in_error),
            },
            Resolution::Found((pos, item)) => Ok((ColumnRef::Inner(pos), item)),
            Resolution::Ambiguous => bail!("Column name {} is ambiguous", name_in_error),
        }
    }

    fn resolve_column<'a>(
        &'a self,
        column_name: &str,
    ) -> Result<(ColumnRef, &'a ScopeItem), failure::Error> {
        self.resolve(
            |item: &ScopeItemName| item.column_name.as_deref() == Some(column_name),
            column_name,
        )
    }

    fn resolve_table_column<'a>(
        &'a self,
        table_name: &str,
        column_name: &str,
    ) -> Result<(ColumnRef, &'a ScopeItem), failure::Error> {
        self.resolve(
            |item: &ScopeItemName| {
                item.table_name.as_deref() == Some(table_name)
                    && item.column_name.as_deref() == Some(column_name)
            },
            &format!("{}.{}", table_name, column_name),
        )
    }

    fn product(self, right: Self) -> Self {
        assert!(self.outer_items == right.outer_items);
        Scope {
            items: self
                .items
                .into_iter()
                .chain(right.items.into_iter())
                .collect(),
            outer_items: self.outer_items,
        }
    }

    fn project(&self, columns: &[usize]) -> Self {
        Scope {
            items: columns.iter().map(|&i| self.items[i].clone()).collect(),
            outer_items: self.outer_items.clone(),
        }
    }
}

fn is_aggregate_func(name: &str) -> bool {
    match name {
        // avg is handled by transform::AvgFuncRewriter.
        "max" | "min" | "sum" | "count" => true,
        _ => false,
    }
}

fn find_agg_func(
    name: &str,
    scalar_type: &ScalarType,
) -> Result<(AggregateFunc, ScalarType), failure::Error> {
    let func = match (name, scalar_type) {
        ("max", ScalarType::Int32) => AggregateFunc::MaxInt32,
        ("max", ScalarType::Int64) => AggregateFunc::MaxInt64,
        ("max", ScalarType::Float32) => AggregateFunc::MaxFloat32,
        ("max", ScalarType::Float64) => AggregateFunc::MaxFloat64,
        ("max", ScalarType::Bool) => AggregateFunc::MaxBool,
        ("max", ScalarType::String) => AggregateFunc::MaxString,
        ("max", ScalarType::Null) => AggregateFunc::MaxNull,
        ("min", ScalarType::Int32) => AggregateFunc::MinInt32,
        ("min", ScalarType::Int64) => AggregateFunc::MinInt64,
        ("min", ScalarType::Float32) => AggregateFunc::MinFloat32,
        ("min", ScalarType::Float64) => AggregateFunc::MinFloat64,
        ("min", ScalarType::Bool) => AggregateFunc::MinBool,
        ("min", ScalarType::String) => AggregateFunc::MinString,
        ("min", ScalarType::Null) => AggregateFunc::MinNull,
        ("sum", ScalarType::Int32) => AggregateFunc::SumInt32,
        ("sum", ScalarType::Int64) => AggregateFunc::SumInt64,
        ("sum", ScalarType::Float32) => AggregateFunc::SumFloat32,
        ("sum", ScalarType::Float64) => AggregateFunc::SumFloat64,
        ("sum", ScalarType::Decimal(_, _)) => AggregateFunc::SumDecimal,
        ("sum", ScalarType::Null) => AggregateFunc::SumNull,
        ("count", _) => AggregateFunc::Count,
        other => bail!("Unimplemented function/type combo: {:?}", other),
    };
    let scalar_type = match (name, scalar_type) {
        ("count", _) => ScalarType::Int64,
        ("max", _) | ("min", _) | ("sum", _) => scalar_type.clone(),
        other => bail!("Unknown aggregate function: {:?}", other),
    };
    Ok((func, scalar_type))
}

#[cfg(test)]
mod test {
    use super::*;

    fn ct(s: ScalarType) -> ColumnType {
        ColumnType::new(s)
    }

    #[test]
    fn find_output_type_chooses_higher_precision() {
        use ScalarType::*;
        let col_expected = &[
            ([&ct(Int32), &ct(Int64)], ct(Int64)),
            ([&ct(Int64), &ct(Int32)], ct(Int64)),
            ([&ct(Int64), &ct(Decimal(10, 10))], ct(Decimal(10, 10))),
            ([&ct(Int64), &ct(Float32)], ct(Float32)),
            ([&ct(Float32), &ct(Float64)], ct(Float64)),
        ];

        for (cols, expected) in col_expected {
            assert_eq!(find_output_type(cols).unwrap(), *expected);
        }
    }
}
