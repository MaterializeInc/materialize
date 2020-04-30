// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL `Query`s are the declarative, computational part of SQL.
//! This module turns `Query`s into `RelationExpr`s - a more explicit, algebraic way of describing computation.

//! Functions named plan_* are typically responsible for handling a single node of the SQL ast. Eg `plan_query` is responsible for handling `sqlparser::ast::Query`.
//! plan_* functions which correspond to operations on relations typically return a `RelationExpr`.
//! plan_* functions which correspond to operations on scalars typically return a `ScalarExpr` and a `ScalarType`. (The latter is because it's not always possible to infer from a `ScalarExpr` what the intended type is - notably in the case of decimals where the scale/precision are encoded only in the type).

//! Aggregates are particularly twisty.
//! In SQL, a GROUP BY turns any columns not in the group key into vectors of values. Then anywhere later in the scope, an aggregate function can be applied to that group. Inside the arguments of an aggregate function, other normal functions are applied element-wise over the vectors. Thus `SELECT sum(foo.x + foo.y) FROM foo GROUP BY x` means adding the scalar `x` to the vector `y` and summing the results.
//! In `RelationExpr`, aggregates can only be applied immediately at the time of grouping.
//! To deal with this, whenever we see a SQL GROUP BY we look ahead for aggregates and precompute them in the `RelationExpr::Reduce`. When we reach the same aggregates during normal planning later on, we look them up in an `ExprContext` to find the precomputed versions.

use std::cell::RefCell;
use std::cmp::{self, Ordering};
use std::collections::{btree_map, BTreeMap, HashSet};
use std::convert::TryInto;
use std::fmt;
use std::iter;
use std::mem;
use std::rc::Rc;

use failure::{bail, ensure, format_err, ResultExt};
use sql_parser::ast::visit::{self, Visit};
use sql_parser::ast::{
    BinaryOperator, DataType, Expr, ExtractField, Function, FunctionArgs, Ident, JoinConstraint,
    JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr, SetOperator, ShowStatementFilter,
    TableAlias, TableFactor, TableWithJoins, UnaryOperator, Value, Values,
};
use uuid::Uuid;

use ::expr::{DateTruncTo, Id, RowSetFinishing};
use catalog::names::PartialName;
use dataflow_types::Timestamp;
use repr::decimal::{Decimal, MAX_DECIMAL_PRECISION};
use repr::{
    strconv, ColumnName, ColumnType, Datum, RelationDesc, RelationType, RowArena, ScalarType,
};

use super::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, ColumnOrder, ColumnRef, JoinKind, NullaryFunc,
    RelationExpr, ScalarExpr, UnaryFunc, UnaryTableFunc, VariadicFunc,
};
use super::normalize;
use super::scope::{Scope, ScopeItem, ScopeItemName};
use super::statement::StatementContext;

/// Plans a top-level query, returning the `RelationExpr` describing the query
/// plan, the `RelationDesc` describing the shape of the result set, a
/// `RowSetFinishing` describing post-processing that must occur before results
/// are sent to the client, and the types of the parameters in the query, if any
/// were present.
///
/// Note that the returned `RelationDesc` describes the expression after
/// applying the returned `RowSetFinishing`.
pub fn plan_root_query(
    scx: &StatementContext,
    mut query: Query,
    lifetime: QueryLifetime,
) -> Result<(RelationExpr, RelationDesc, RowSetFinishing, Vec<ScalarType>), failure::Error> {
    crate::transform::transform(&mut query);
    let qcx = QueryContext::root(scx, lifetime);
    let (expr, scope, finishing) = plan_query(&qcx, &query)?;
    let typ = qcx.relation_type(&expr);
    let typ = RelationType::new(
        finishing
            .project
            .iter()
            .map(|i| typ.column_types[*i].clone())
            .collect(),
    );

    let desc = RelationDesc::new(typ, scope.column_names());
    let mut param_types = vec![];
    for (i, (n, typ)) in qcx.unwrap_param_types().into_iter().enumerate() {
        if n != i + 1 {
            bail!("unable to infer type for parameter ${}", i + 1);
        }
        param_types.push(typ);
    }
    Ok((expr, desc, finishing, param_types))
}

/// Plans a SHOW statement that might have a WHERE or LIKE clause attached to it. A LIKE clause is
/// treated as a WHERE applied to the first column in the result set.
pub fn plan_show_where(
    scx: &StatementContext,
    filter: Option<&ShowStatementFilter>,
    rows: Vec<Vec<Datum>>,
    desc: &RelationDesc,
) -> Result<(RelationExpr, RowSetFinishing), failure::Error> {
    let names: Vec<Option<String>> = desc
        .iter_names()
        .map(|name| name.map(|x| x.as_str().into()))
        .collect();

    let num_cols = names.len();
    let mut row_expr = RelationExpr::constant(rows, desc.typ().clone());

    if let Some(f) = filter {
        let owned;
        let predicate = match &f {
            ShowStatementFilter::Like(s) => {
                owned = Expr::BinaryOp {
                    left: Box::new(Expr::Identifier(vec![Ident::new(
                        names[0].clone().unwrap(),
                    )])),
                    op: BinaryOperator::Like,
                    right: Box::new(Expr::Value(Value::SingleQuotedString(s.into()))),
                };
                &owned
            }
            ShowStatementFilter::Where(selection) => selection,
        };
        let qcx = QueryContext::root(scx, QueryLifetime::OneShot);
        let scope = Scope::from_source(None, names, None);
        let ecx = ExprContext {
            qcx: &qcx,
            name: "SHOW WHERE clause",
            scope: &scope,
            relation_type: &qcx.relation_type(&row_expr),
            allow_aggregates: false,
            allow_subqueries: true,
        };
        let expr = plan_expr(&ecx, &predicate, Some(ScalarType::Bool))?;
        let typ = ecx.column_type(&expr);
        if typ.scalar_type != ScalarType::Bool && typ.scalar_type != ScalarType::Unknown {
            bail!(
                "WHERE clause must have boolean type, not {:?}",
                typ.scalar_type
            );
        }
        row_expr = row_expr.filter(vec![expr]);
    }

    Ok((
        row_expr,
        RowSetFinishing {
            order_by: (0..num_cols)
                .map(|c| ColumnOrder {
                    column: c,
                    desc: false,
                })
                .collect(),
            limit: None,
            offset: 0,
            project: (0..num_cols).collect(),
        },
    ))
}

/// Evaluates an expression in the AS OF position of a TAIL statement.
pub fn eval_as_of<'a>(scx: &'a StatementContext, expr: Expr) -> Result<Timestamp, failure::Error> {
    let scope = Scope::from_source(
        None,
        iter::empty::<Option<ColumnName>>(),
        Some(Scope::empty(None)),
    );
    let desc = RelationDesc::empty();
    let qcx = &QueryContext::root(scx, QueryLifetime::OneShot);
    let ecx = &ExprContext {
        qcx: &qcx,
        name: "AS OF",
        scope: &scope,
        relation_type: &desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
    };

    let ex = plan_expr(ecx, &expr, None)?.lower_uncorrelated();
    let temp_storage = &RowArena::new();
    let evaled = ex.eval(&[], temp_storage)?;

    Ok(match ex.typ(desc.typ()).scalar_type {
        ScalarType::Decimal(_, 0) => evaled.unwrap_decimal().as_i128().try_into()?,
        ScalarType::Decimal(_, _) => {
            bail!("decimal with fractional component is not a valid timestamp")
        }
        ScalarType::Int32 => evaled.unwrap_int32().try_into()?,
        ScalarType::Int64 => evaled.unwrap_int64().try_into()?,
        ScalarType::TimestampTz => evaled.unwrap_timestamptz().timestamp_millis().try_into()?,
        ScalarType::Timestamp => evaled.unwrap_timestamp().timestamp_millis().try_into()?,
        _ => bail!("can't use {} as a timestamp for AS OF", ex.typ(desc.typ())),
    })
}

pub fn plan_index_exprs<'a>(
    scx: &'a StatementContext,
    on_desc: &RelationDesc,
    exprs: &[Expr],
) -> Result<Vec<::expr::ScalarExpr>, failure::Error> {
    let scope = Scope::from_source(None, on_desc.iter_names(), Some(Scope::empty(None)));
    let qcx = &QueryContext::root(scx, QueryLifetime::Static);
    let ecx = &ExprContext {
        qcx: &qcx,
        name: "CREATE INDEX",
        scope: &scope,
        relation_type: on_desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
    };
    let mut out = vec![];
    for expr in exprs {
        let (expr, _) =
            plan_expr_or_col_index(ecx, expr, Some(ScalarType::String), "CREATE INDEX")?;
        out.push(expr.lower_uncorrelated());
    }
    Ok(out)
}

fn plan_expr_or_col_index<'a>(
    ecx: &ExprContext,
    e: &'a Expr,
    type_hint: Option<ScalarType>,
    clause_name: &str,
) -> Result<(ScalarExpr, Option<ScopeItemName>), failure::Error> {
    match e {
        Expr::Value(Value::Number(n)) => {
            let n = n.parse::<usize>().with_context(|err| {
                format_err!(
                    "unable to parse column reference in {}: {}: {}",
                    clause_name,
                    err,
                    n
                )
            })?;
            let max = ecx.relation_type.column_types.len();
            if n < 1 || n > max {
                bail!(
                    "column reference {} in {} is out of range (1 - {})",
                    n,
                    clause_name,
                    max
                );
            }
            Some(Ok((
                ScalarExpr::Column(ColumnRef {
                    level: 0,
                    column: n - 1,
                }),
                None,
            )))
        }
        _ => None,
    }
    .unwrap_or_else(|| plan_expr_returning_name(ecx, e, type_hint))
}

fn plan_query(
    qcx: &QueryContext,
    q: &Query,
) -> Result<(RelationExpr, Scope, RowSetFinishing), failure::Error> {
    if !q.ctes.is_empty() {
        bail!("CTEs are not yet supported");
    }
    let limit = match &q.limit {
        None => None,
        Some(Expr::Value(Value::Number(x))) => Some(x.parse()?),
        _ => bail!("LIMIT must be an integer constant"),
    };
    let offset = match &q.offset {
        None => 0,
        Some(Expr::Value(Value::Number(x))) => x.parse()?,
        _ => bail!("OFFSET must be an integer constant"),
    };
    let (expr, scope) = plan_set_expr(qcx, &q.body)?;
    let output_typ = qcx.relation_type(&expr);
    let mut order_by = vec![];
    let mut map_exprs = vec![];
    for obe in &q.order_by {
        let ecx = &ExprContext {
            qcx,
            name: "ORDER BY clause",
            scope: &scope,
            relation_type: &output_typ,
            allow_aggregates: true,
            allow_subqueries: true,
        };
        let (expr, _maybe_name) =
            plan_expr_or_col_index(ecx, &obe.expr, Some(ScalarType::String), "ORDER BY")?;
        // If the expression is a reference to an existing column,
        // do not introduce a new column to support it.
        if let ScalarExpr::Column(ColumnRef { level: 0, column }) = expr {
            order_by.push(ColumnOrder {
                column,
                desc: match obe.asc {
                    None => false,
                    Some(asc) => !asc,
                },
            });
        } else {
            let idx = output_typ.column_types.len() + map_exprs.len();
            map_exprs.push(expr);
            order_by.push(ColumnOrder {
                column: idx,
                desc: match obe.asc {
                    None => false,
                    Some(asc) => !asc,
                },
            });
        }
    }

    let finishing = RowSetFinishing {
        order_by,
        limit,
        project: (0..output_typ.column_types.len()).collect(),
        offset,
    };
    Ok((expr.map(map_exprs), scope, finishing))
}

fn plan_subquery(qcx: &QueryContext, q: &Query) -> Result<(RelationExpr, Scope), failure::Error> {
    let (mut expr, scope, finishing) = plan_query(qcx, q)?;
    if finishing.limit.is_some() || finishing.offset > 0 {
        expr = RelationExpr::TopK {
            input: Box::new(expr),
            group_key: vec![],
            order_key: finishing.order_by,
            limit: finishing.limit,
            offset: finishing.offset,
        };
    }
    Ok((
        RelationExpr::Project {
            input: Box::new(expr),
            outputs: finishing.project,
        },
        scope,
    ))
}

fn plan_set_expr(qcx: &QueryContext, q: &SetExpr) -> Result<(RelationExpr, Scope), failure::Error> {
    match q {
        SetExpr::Select(select) => plan_view_select(qcx, select),
        SetExpr::SetOperation {
            op,
            all,
            left,
            right,
        } => {
            let (left_expr, left_scope) = plan_set_expr(qcx, left)?;
            let (right_expr, _right_scope) = plan_set_expr(qcx, right)?;

            // TODO(jamii) this type-checking is redundant with RelationExpr::typ, but currently it seems that we need both because RelationExpr::typ is not allowed to return errors
            let left_types = qcx.relation_type(&left_expr).column_types;
            let right_types = qcx.relation_type(&right_expr).column_types;
            if left_types.len() != right_types.len() {
                bail!(
                    "each {} query must have the same number of columns: {} vs {}",
                    op,
                    left_types.len(),
                    right_types.len(),
                );
            }
            for (left_col_type, right_col_type) in left_types.iter().zip(right_types.iter()) {
                if left_col_type.union(right_col_type).is_err() {
                    bail!(
                        "{} types {} and {} cannot be matched",
                        op,
                        left_col_type.scalar_type,
                        right_col_type.scalar_type
                    );
                }
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
                        left_expr.union(left_clone.union(right_expr.negate()).threshold().negate())
                    } else {
                        left_expr
                            .union(left_clone.union(right_expr.negate()).threshold().negate())
                            .distinct()
                    }
                }
            };
            let scope = Scope::from_source(
                None,
                // Column names are taken from the left, as in Postgres.
                left_scope.column_names(),
                Some(qcx.outer_scope.clone()),
            );

            Ok((relation_expr, scope))
        }
        SetExpr::Values(Values(values)) => {
            ensure!(
                !values.is_empty(),
                "Can't infer a type for empty VALUES expression"
            );
            let ecx = &ExprContext {
                qcx,
                name: "values",
                scope: &Scope::empty(Some(qcx.outer_scope.clone())),
                relation_type: &RelationType::empty(),
                allow_aggregates: false,
                allow_subqueries: true,
            };
            let mut expr: Option<RelationExpr> = None;
            let mut types: Option<Vec<ColumnType>> = None;
            for row in values {
                let mut value_exprs = vec![];
                let mut value_types = vec![];
                for value in row {
                    let expr = plan_expr(ecx, value, Some(ScalarType::String))?;
                    value_types.push(ecx.column_type(&expr));
                    value_exprs.push(expr);
                }
                types = if let Some(types) = types {
                    if types.len() != value_exprs.len() {
                        bail!("VALUES expression has varying number of columns: {}", q);
                    }
                    Some(
                        types
                            .iter()
                            .zip(value_types.iter())
                            .map(|(left_typ, right_typ)| left_typ.union(right_typ))
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                } else {
                    Some(value_types)
                };

                let row_expr = RelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
                    .map(value_exprs);
                expr = if let Some(expr) = expr {
                    Some(expr.union(row_expr))
                } else {
                    Some(row_expr)
                };
            }
            let mut scope = Scope::empty(Some(qcx.outer_scope.clone()));
            for i in 0..types.unwrap().len() {
                let name = Some(format!("column{}", i + 1).into());
                scope.items.push(ScopeItem::from_column_name(name));
            }
            Ok((expr.unwrap(), scope))
        }
        SetExpr::Query(query) => {
            let (expr, scope) = plan_subquery(qcx, query)?;
            Ok((expr, scope))
        }
    }
}

fn plan_view_select(
    qcx: &QueryContext,
    s: &Select,
) -> Result<(RelationExpr, Scope), failure::Error> {
    // Step 1. Handle FROM clause, including joins.
    let (left, left_scope) = {
        let typ = RelationType::new(vec![]);
        (
            RelationExpr::constant(vec![vec![]], typ),
            Scope::from_source(
                None,
                iter::empty::<Option<ColumnName>>(),
                Some(qcx.outer_scope.clone()),
            ),
        )
    };
    let (mut relation_expr, from_scope) =
        s.from.iter().fold(Ok((left, left_scope)), |l, twj| {
            let (left, left_scope) = l?;
            plan_table_with_joins(qcx, left, left_scope, &JoinOperator::CrossJoin, twj)
        })?;

    // Step 2. Handle WHERE clause.
    if let Some(selection) = &s.selection {
        let ecx = &ExprContext {
            qcx,
            name: "WHERE clause",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: false,
            allow_subqueries: true,
        };
        let expr = plan_expr(ecx, &selection, Some(ScalarType::Bool))?;
        let typ = ecx.column_type(&expr);
        if typ.scalar_type != ScalarType::Bool && typ.scalar_type != ScalarType::Unknown {
            bail!(
                "WHERE clause must have boolean type, not {:?}",
                typ.scalar_type
            );
        }
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 3. Handle GROUP BY clause.
    let (group_scope, select_all_mapping) = {
        // gather group columns
        let ecx = &ExprContext {
            qcx,
            name: "GROUP BY clause",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: false,
            allow_subqueries: true,
        };
        let mut group_key = vec![];
        let mut group_exprs = vec![];
        let mut group_scope = Scope::empty(Some(qcx.outer_scope.clone()));
        let mut select_all_mapping = BTreeMap::new();
        for group_expr in &s.group_by {
            let (expr, maybe_name) =
                plan_expr_or_col_index(ecx, group_expr, Some(ScalarType::String), "GROUP BY")?;
            let new_column = group_key.len();
            // repeated exprs in GROUP BY confuse name resolution later, and dropping them doesn't change the result
            if group_exprs
                .iter()
                .find(|existing_expr| **existing_expr == expr)
                .is_none()
            {
                let scope_item = if let ScalarExpr::Column(ColumnRef {
                    level: 0,
                    column: old_column,
                }) = &expr
                {
                    // If we later have `SELECT foo.*` then we have to find all the `foo` items in `from_scope` and figure out where they ended up in `group_scope`.
                    // This is really hard to do right using SQL name resolution, so instead we just track the movement here.
                    select_all_mapping.insert(*old_column, new_column);
                    let mut scope_item = ecx.scope.items[*old_column].clone();
                    scope_item.expr = Some(group_expr.clone());
                    scope_item
                } else {
                    ScopeItem {
                        names: maybe_name.into_iter().collect(),
                        expr: Some(group_expr.clone()),
                        nameable: true,
                    }
                };

                group_key.push(from_scope.len() + group_exprs.len());
                group_exprs.push(expr);
                group_scope.items.push(scope_item);
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
        let ecx = &ExprContext {
            qcx,
            name: "aggregate function",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr.clone().map(group_exprs.clone())),
            allow_aggregates: false,
            allow_subqueries: true,
        };
        let mut aggregates = vec![];
        for sql_function in aggregate_visitor.into_result()? {
            aggregates.push(plan_aggregate(ecx, sql_function)?);
            group_scope.items.push(ScopeItem {
                names: vec![ScopeItemName {
                    table_name: None,
                    column_name: Some(sql_function.name.to_string().into()),
                }],
                expr: Some(Expr::Function(sql_function.clone())),
                nameable: true,
            });
        }
        if !aggregates.is_empty() || !group_key.is_empty() || s.having.is_some() {
            // apply GROUP BY / aggregates
            relation_expr = relation_expr.map(group_exprs).reduce(group_key, aggregates);
            (group_scope, select_all_mapping)
        } else {
            // if no GROUP BY, aggregates or having then all columns remain in scope
            (
                from_scope.clone(),
                (0..from_scope.len()).map(|i| (i, i)).collect(),
            )
        }
    };

    // Step 4. Handle HAVING clause.
    if let Some(having) = &s.having {
        let ecx = &ExprContext {
            qcx,
            name: "HAVING clause",
            scope: &group_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: true,
            allow_subqueries: true,
        };
        let expr = plan_expr(ecx, having, Some(ScalarType::Bool))?;
        let typ = ecx.column_type(&expr);
        if typ.scalar_type != ScalarType::Bool {
            bail!(
                "HAVING clause must have boolean type, not {:?}",
                typ.scalar_type
            );
        }
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 5. Handle projections.
    let project_scope = {
        let mut project_exprs = vec![];
        let mut project_key = vec![];
        let mut project_scope = Scope::empty(Some(qcx.outer_scope.clone()));
        for p in &s.projection {
            let ecx = &ExprContext {
                qcx,
                name: "SELECT clause",
                scope: &group_scope,
                relation_type: &qcx.relation_type(&relation_expr),
                allow_aggregates: true,
                allow_subqueries: true,
            };
            for (expr, scope_item) in plan_select_item(ecx, p, &from_scope, &select_all_mapping)? {
                project_key.push(group_scope.len() + project_exprs.len());
                project_exprs.push(expr);
                project_scope.items.push(scope_item);
            }
        }
        relation_expr = relation_expr.map(project_exprs).project(project_key);
        project_scope
    };

    // Step 6. Handle DISTINCT.
    if s.distinct {
        relation_expr = relation_expr.distinct();
    }

    Ok((relation_expr, project_scope))
}

fn plan_table_with_joins<'a>(
    qcx: &QueryContext,
    left: RelationExpr,
    left_scope: Scope,
    join_operator: &JoinOperator,
    table_with_joins: &'a TableWithJoins,
) -> Result<(RelationExpr, Scope), failure::Error> {
    let (mut left, mut left_scope) = plan_table_factor(
        qcx,
        left,
        left_scope,
        join_operator,
        &table_with_joins.relation,
    )?;
    for join in &table_with_joins.joins {
        let (new_left, new_left_scope) =
            plan_table_factor(qcx, left, left_scope, &join.join_operator, &join.relation)?;
        left = new_left;
        left_scope = new_left_scope;
    }
    Ok((left, left_scope))
}

fn plan_table_factor<'a>(
    qcx: &QueryContext,
    left: RelationExpr,
    left_scope: Scope,
    join_operator: &JoinOperator,
    table_factor: &'a TableFactor,
) -> Result<(RelationExpr, Scope), failure::Error> {
    match table_factor {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
        } => {
            if !with_hints.is_empty() {
                bail!("WITH hints are not supported");
            }
            let alias = if let Some(TableAlias { name, columns }) = alias {
                if !columns.is_empty() {
                    bail!("aliasing columns is not yet supported");
                }
                PartialName {
                    database: None,
                    schema: None,
                    item: normalize::ident(name.clone()),
                }
            } else {
                normalize::object_name(name.clone())?
            };
            if let Some(args) = args {
                let ecx = &ExprContext {
                    qcx,
                    name: "FROM table function",
                    scope: &left_scope,
                    relation_type: &qcx.relation_type(&left),
                    allow_aggregates: false,
                    allow_subqueries: true,
                };
                plan_table_function(ecx, left, &name, Some(alias), args)
            } else {
                let name = qcx.scx.resolve_name(name.clone())?;
                let item = qcx.scx.catalog.get(&name)?;
                let expr = RelationExpr::Get {
                    id: Id::Global(item.id()),
                    typ: item.desc()?.typ().clone(),
                };
                let scope = Scope::from_source(
                    Some(alias),
                    item.desc()?.iter_names(),
                    Some(qcx.outer_scope.clone()),
                );
                plan_join_operator(qcx, &join_operator, left, left_scope, expr, scope)
            }
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            if *lateral {
                bail!("LATERAL derived tables are not yet supported");
            }
            let (expr, scope) = plan_subquery(&qcx, &subquery)?;
            let alias = if let Some(TableAlias { name, columns }) = alias {
                if !columns.is_empty() {
                    bail!("aliasing columns is not yet supported");
                }
                Some(PartialName {
                    database: None,
                    schema: None,
                    item: normalize::ident(name.clone()),
                })
            } else {
                None
            };
            let scope =
                Scope::from_source(alias, scope.column_names(), Some(qcx.outer_scope.clone()));
            plan_join_operator(qcx, &join_operator, left, left_scope, expr, scope)
        }
        TableFactor::NestedJoin(table_with_joins) => {
            plan_table_with_joins(qcx, left, left_scope, join_operator, table_with_joins)
        }
    }
}

fn plan_table_function(
    ecx: &ExprContext,
    left: RelationExpr,
    name: &ObjectName,
    alias: Option<PartialName>,
    args: &FunctionArgs,
) -> Result<(RelationExpr, Scope), failure::Error> {
    let ident = &*normalize::function_name(name.clone())?;
    if !is_table_func(ident) {
        // so we don't forget to add names over there
        bail!("{} is not a table function", ident);
    }
    let args = match args {
        FunctionArgs::Star => bail!("{} does not accept * as an argument", ident),
        FunctionArgs::Args(args) => args,
    };
    match (ident, args.as_slice()) {
        ("jsonb_each", [expr])
        | ("jsonb_object_keys", [expr])
        | ("jsonb_array_elements", [expr])
        | ("jsonb_each_text", [expr])
        | ("jsonb_array_elements_text", [expr]) => {
            let expr = plan_expr(ecx, expr, Some(ScalarType::Jsonb))?;
            match ecx.column_type(&expr).scalar_type {
                ScalarType::Jsonb => {
                    let func = match ident {
                        "jsonb_each" | "jsonb_each_text" => UnaryTableFunc::JsonbEach,
                        "jsonb_object_keys" => UnaryTableFunc::JsonbObjectKeys,
                        "jsonb_array_elements" | "jsonb_array_elements_text" => {
                            UnaryTableFunc::JsonbArrayElements
                        }
                        _ => unreachable!(),
                    };
                    let mut call = RelationExpr::FlatMapUnary {
                        input: Box::new(left),
                        func,
                        expr,
                    };

                    if let "jsonb_each_text" = ident {
                        // convert value column to text, leave key column as is
                        let num_old_columns = ecx.scope.len();
                        call = call
                            .map(vec![ScalarExpr::Column(ColumnRef {
                                level: 0,
                                column: num_old_columns + 1,
                            })
                            .call_unary(UnaryFunc::JsonbStringifyUnlessString)])
                            .project(
                                (0..num_old_columns)
                                    .chain(vec![num_old_columns, num_old_columns + 2])
                                    .collect(),
                            );
                    }
                    if let "jsonb_array_elements_text" = ident {
                        // convert value column to text
                        let num_old_columns = ecx.scope.len();
                        call = call
                            .map(vec![ScalarExpr::Column(ColumnRef {
                                level: 0,
                                column: num_old_columns,
                            })
                            .call_unary(UnaryFunc::JsonbStringifyUnlessString)])
                            .project(
                                (0..num_old_columns)
                                    .chain(vec![num_old_columns + 1])
                                    .collect(),
                            );
                    }

                    let column_names: &[&str] = match ident {
                        "jsonb_each" | "jsonb_each_text" => &["key", "value"],
                        "jsonb_object_keys" => &["jsonb_object_keys"],
                        "jsonb_array_elements" | "jsonb_array_elements_text" => &["value"],
                        _ => unreachable!(),
                    };
                    let scope = Scope::from_source(
                        alias,
                        column_names
                            .iter()
                            .map(|name| Some(ColumnName::from(&**name))),
                        Some(ecx.qcx.outer_scope.clone()),
                    );
                    Ok((call, ecx.scope.clone().product(scope)))
                }
                other => bail!("No overload of {} for {}", ident, other),
            }
        }
        ("jsonb_each", _)
        | ("jsonb_object_keys", _)
        | ("jsonb_array_elements", _)
        | ("jsonb_each_text", _)
        | ("jsonb_array_elements_text", _) => bail!("{}() requires exactly one argument", ident),
        ("regexp_extract", [regex, haystack]) => {
            let expr = plan_expr(ecx, haystack, None)?;
            if ecx.column_type(&expr).scalar_type != ScalarType::String {
                bail!("Datum to search must be a string");
            }
            let regex = match &regex {
                Expr::Value(Value::SingleQuotedString(s)) => s,
                _ => bail!("Regex must be a string literal."),
            };

            let ar = expr::AnalyzedRegex::new(regex)?;
            let colnames: Vec<_> = ar
                .capture_groups_iter()
                .map(|cgd| {
                    cgd.name
                        .clone()
                        .unwrap_or_else(|| format!("column{}", cgd.index))
                })
                .collect();
            let call = RelationExpr::FlatMapUnary {
                input: Box::new(left),
                func: UnaryTableFunc::RegexpExtract(ar),
                expr,
            };
            let scope = Scope::from_source(
                alias,
                colnames.iter().map(|name| Some(ColumnName::from(&**name))),
                Some(ecx.qcx.outer_scope.clone()),
            );
            Ok((call, ecx.scope.clone().product(scope)))
        }
        ("csv_extract", [n_cols, expr]) => {
            let bad_ncols_bail = "csv_extract number of columns must be a positive integer literal";
            let n_cols: usize = match n_cols {
                Expr::Value(Value::Number(s)) => s.parse()?,
                _ => bail!(bad_ncols_bail),
            };
            if n_cols == 0 {
                bail!(bad_ncols_bail);
            }
            let expr = plan_expr(ecx, expr, None)?;
            let st = ecx.column_type(&expr).scalar_type;
            if st != ScalarType::Bytes && st != ScalarType::String {
                bail!("Datum to decode as CSV must be a string")
            }
            let colnames: Vec<_> = (1..=n_cols).map(|i| format!("column{}", i)).collect();
            let call = RelationExpr::FlatMapUnary {
                input: Box::new(left),
                func: UnaryTableFunc::CsvExtract(n_cols),
                expr,
            };
            let scope = Scope::from_source(
                alias,
                colnames.iter().map(|name| Some(ColumnName::from(&**name))),
                Some(ecx.qcx.outer_scope.clone()),
            );
            Ok((call, ecx.scope.clone().product(scope)))
        }
        ("regexp_extract", _) | ("csv_extract", _) => {
            bail!("{}() requires exactly two arguments", ident)
        }
        _ => bail!("unsupported table function: {}", ident),
    }
}

fn plan_select_item<'a>(
    ecx: &ExprContext,
    s: &'a SelectItem,
    select_all_scope: &Scope,
    select_all_mapping: &BTreeMap<usize, usize>,
) -> Result<Vec<(ScalarExpr, ScopeItem)>, failure::Error> {
    match s {
        SelectItem::UnnamedExpr(sql_expr) => {
            let (expr, maybe_name) =
                plan_expr_returning_name(ecx, sql_expr, Some(ScalarType::String))?;
            let scope_item = ScopeItem {
                names: maybe_name.into_iter().collect(),
                expr: Some(sql_expr.clone()),
                nameable: true,
            };
            Ok(vec![(expr, scope_item)])
        }
        SelectItem::ExprWithAlias {
            expr: sql_expr,
            alias,
        } => {
            let (expr, maybe_name) =
                plan_expr_returning_name(ecx, sql_expr, Some(ScalarType::String))?;
            let scope_item = ScopeItem {
                names: iter::once(ScopeItemName {
                    table_name: None,
                    column_name: Some(normalize::column_name(alias.clone())),
                })
                .chain(maybe_name.into_iter())
                .collect(),
                expr: Some(sql_expr.clone()),
                nameable: true,
            };
            Ok(vec![(expr, scope_item)])
        }
        SelectItem::Wildcard => {
            let out = select_all_scope
                .items
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let expr = ScalarExpr::Column(ColumnRef {
                        level: 0,
                        column: *select_all_mapping.get(&i).ok_or_else(|| {
                            format_err!("internal error: unable to resolve scope item {:?}", item)
                        })?,
                    });
                    let mut out_item = item.clone();
                    out_item.expr = None;
                    Ok((expr, out_item))
                })
                .collect::<Result<Vec<_>, failure::Error>>()?;
            if out.is_empty() {
                bail!("SELECT * with no tables specified is not valid");
            }
            Ok(out)
        }
        SelectItem::QualifiedWildcard(table_name) => {
            let table_name = normalize::object_name(table_name.clone())?;
            let out = select_all_scope
                .items
                .iter()
                .enumerate()
                .filter(|(_i, item)| item.is_from_table(&table_name))
                .map(|(i, item)| {
                    let expr = ScalarExpr::Column(ColumnRef {
                        level: 0,
                        column: *select_all_mapping.get(&i).ok_or_else(|| {
                            format_err!("internal error: unable to resolve scope item {:?}", item)
                        })?,
                    });
                    let mut out_item = item.clone();
                    out_item.expr = None;
                    Ok((expr, out_item))
                })
                .collect::<Result<Vec<_>, failure::Error>>()?;
            if out.is_empty() {
                bail!("no table named '{}' in scope", table_name);
            }
            Ok(out)
        }
    }
}

fn plan_join_operator(
    qcx: &QueryContext,
    operator: &JoinOperator,
    left: RelationExpr,
    left_scope: Scope,
    right: RelationExpr,
    right_scope: Scope,
) -> Result<(RelationExpr, Scope), failure::Error> {
    match operator {
        JoinOperator::Inner(constraint) => plan_join_constraint(
            qcx,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            JoinKind::Inner,
        ),
        JoinOperator::LeftOuter(constraint) => plan_join_constraint(
            qcx,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            JoinKind::LeftOuter,
        ),
        JoinOperator::RightOuter(constraint) => plan_join_constraint(
            qcx,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            JoinKind::RightOuter,
        ),
        JoinOperator::FullOuter(constraint) => plan_join_constraint(
            qcx,
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
    qcx: &QueryContext,
    constraint: &'a JoinConstraint,
    left: RelationExpr,
    left_scope: Scope,
    right: RelationExpr,
    right_scope: Scope,
    kind: JoinKind,
) -> Result<(RelationExpr, Scope), failure::Error> {
    let (expr, scope) = match constraint {
        JoinConstraint::On(expr) => {
            let mut product_scope = left_scope.product(right_scope);
            let ecx = &ExprContext {
                qcx,
                name: "ON clause",
                scope: &product_scope,
                relation_type: &RelationType::new(
                    qcx.relation_type(&left)
                        .column_types
                        .into_iter()
                        .chain(qcx.relation_type(&right).column_types)
                        .collect(),
                ),
                allow_aggregates: false,
                allow_subqueries: true,
            };
            let on = plan_expr(ecx, expr, Some(ScalarType::Bool))?;
            if kind == JoinKind::Inner {
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
                    product_scope.items[r].nameable = false;
                    let right_names = product_scope.items[r].names.clone();
                    product_scope.items[l].names.extend(right_names);
                }
            }
            let joined = RelationExpr::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                kind,
            };
            (joined, product_scope)
        }
        JoinConstraint::Using(column_names) => plan_using_constraint(
            qcx,
            &column_names
                .iter()
                .map(|ident| normalize::column_name(ident.clone()))
                .collect::<Vec<_>>(),
            left,
            left_scope,
            right,
            right_scope,
            kind,
        )?,
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
            plan_using_constraint(
                qcx,
                &column_names,
                left,
                left_scope,
                right,
                right_scope,
                kind,
            )?
        }
    };
    Ok((expr, scope))
}

// See page 440 of ANSI SQL 2016 spec for details on scoping of using/natural joins
#[allow(clippy::too_many_arguments)]
fn plan_using_constraint(
    qcx: &QueryContext,
    column_names: &[ColumnName],
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
        let (l, _) = left_scope.resolve_column(column_name)?;
        let (r, _) = right_scope.resolve_column(column_name)?;
        let l = match l {
            ColumnRef {
                level: 0,
                column: l,
            } => l,
            _ => bail!(
                "Internal error: name {} in USING resolved to outer column",
                column_name
            ),
        };
        let r = match r {
            ColumnRef {
                level: 0,
                column: r,
            } => r,
            _ => bail!(
                "Internal error: name {} in USING resolved to outer column",
                column_name
            ),
        };
        let l_type = &qcx.relation_type(&left).column_types[l];
        let r_type = &qcx.relation_type(&right).column_types[r];
        if l_type.scalar_type != r_type.scalar_type
            && l_type.scalar_type != ScalarType::Unknown
            && r_type.scalar_type != ScalarType::Unknown
        {
            bail!(
                "{:?} and {:?} are not comparable (in NATURAL/USING join on {})",
                l_type.scalar_type,
                r_type.scalar_type,
                column_name
            );
        }
        join_exprs.push(ScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1: Box::new(ScalarExpr::Column(ColumnRef {
                level: 0,
                column: l,
            })),
            expr2: Box::new(ScalarExpr::Column(ColumnRef {
                level: 0,
                column: left_scope.len() + r,
            })),
        });
        map_exprs.push(ScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce,
            exprs: vec![
                ScalarExpr::Column(ColumnRef {
                    level: 0,
                    column: l,
                }),
                ScalarExpr::Column(ColumnRef {
                    level: 0,
                    column: left_scope.len() + r,
                }),
            ],
        });
        let mut names = left_scope.items[l].names.clone();
        names.extend(right_scope.items[r].names.clone());
        new_items.push(ScopeItem {
            names,
            expr: None,
            nameable: true,
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
    let mut both_scope = left_scope.product(right_scope);
    both_scope.items.extend(new_items);
    let both_scope = both_scope.project(&project_key);
    let both = RelationExpr::Join {
        left: Box::new(left),
        right: Box::new(right),
        on: join_exprs
            .into_iter()
            .fold(ScalarExpr::literal_true(), |expr1, expr2| {
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

/// Reports whether `e` has an unknown type, due to a query parameter whose type
/// has not yet been constraint.
fn expr_has_unknown_type(ecx: &ExprContext, expr: &Expr) -> bool {
    if let Expr::Parameter(n) = unnest(expr) {
        !ecx.qcx.param_types.borrow().contains_key(n)
    } else {
        false
    }
}

fn plan_expr<'a>(
    ecx: &ExprContext,
    e: &'a Expr,
    type_hint: Option<ScalarType>,
) -> Result<ScalarExpr, failure::Error> {
    let (expr, _scope_item) = plan_expr_returning_name(ecx, e, type_hint)?;
    Ok(expr)
}

fn plan_expr_returning_name<'a>(
    ecx: &'a ExprContext,
    e: &Expr,
    type_hint: Option<ScalarType>,
) -> Result<(ScalarExpr, Option<ScopeItemName>), failure::Error> {
    if let Some((i, name)) = ecx.scope.resolve_expr(e) {
        // surprise - we already calculated this expr before
        Ok((ScalarExpr::Column(i), name.cloned()))
    } else {
        Ok(match e {
            Expr::Identifier(names) => {
                let mut names = names.clone();
                let col_name = normalize::column_name(names.pop().unwrap());
                let (i, name) = if names.is_empty() {
                    ecx.scope.resolve_column(&col_name)?
                } else {
                    let table_name = normalize::object_name(ObjectName(names))?;
                    ecx.scope.resolve_table_column(&table_name, &col_name)?
                };
                (ScalarExpr::Column(i), Some(name.clone()))
            }
            Expr::Value(val) => (plan_literal(val)?, None),
            Expr::QualifiedWildcard(_) => bail!("wildcard in invalid position"),
            Expr::Parameter(n) => {
                if !ecx.allow_subqueries {
                    bail!("{} does not allow subqueries", ecx.name)
                }
                if *n == 0 || *n > 65536 {
                    bail!("there is no parameter ${}", n);
                }
                match ecx.qcx.param_types.borrow_mut().entry(*n) {
                    btree_map::Entry::Occupied(_) => (),
                    btree_map::Entry::Vacant(v) => {
                        if let Some(typ) = type_hint {
                            v.insert(typ);
                        } else {
                            bail!("unable to infer type for parameter ${}", n);
                        }
                    }
                }
                (ScalarExpr::Parameter(*n), None)
            }
            // TODO(benesch): why isn't IS [NOT] NULL a unary op?
            Expr::IsNull(expr) => (plan_is_null_expr(ecx, expr, false)?, None),
            Expr::IsNotNull(expr) => (plan_is_null_expr(ecx, expr, true)?, None),
            Expr::UnaryOp { op, expr } => (plan_unary_op(ecx, op, expr)?, None),
            Expr::BinaryOp { op, left, right } => (plan_binary_op(ecx, op, left, right)?, None),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => (plan_between(ecx, expr, low, high, *negated)?, None),
            Expr::InList {
                expr,
                list,
                negated,
            } => (plan_in_list(ecx, expr, list, *negated)?, None),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => (
                plan_case(ecx, operand, conditions, results, else_result)?,
                None,
            ),
            Expr::Nested(expr) => (plan_expr(ecx, expr, type_hint)?, None),
            Expr::Cast { expr, data_type } => plan_cast(ecx, expr, data_type)?,
            Expr::Function(func) => {
                let expr = plan_function(ecx, func)?;
                let name = ScopeItemName {
                    table_name: None,
                    column_name: Some(normalize::column_name(func.name.0.last().unwrap().clone())),
                };
                (expr, Some(name))
            }
            Expr::Exists(query) => {
                if !ecx.allow_subqueries {
                    bail!("{} does not allow subqueries", ecx.name)
                }
                let qcx = ecx.derived_query_context();
                let (expr, _scope) = plan_subquery(&qcx, query)?;
                (expr.exists(), None)
            }
            Expr::Subquery(query) => {
                if !ecx.allow_subqueries {
                    bail!("{} does not allow subqueries", ecx.name)
                }
                let qcx = ecx.derived_query_context();
                let (expr, _scope) = plan_subquery(&qcx, query)?;
                let column_types = qcx.relation_type(&expr).column_types;
                if column_types.len() != 1 {
                    bail!(
                        "Expected subselect to return 1 column, got {} columns",
                        column_types.len()
                    );
                }
                (expr.select(), None)
            }
            Expr::Any {
                left,
                op,
                right,
                some: _,
            } => (
                plan_any_or_all(ecx, left, op, right, AggregateFunc::Any)?,
                None,
            ),
            Expr::All { left, op, right } => (
                plan_any_or_all(ecx, left, op, right, AggregateFunc::All)?,
                None,
            ),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if !ecx.allow_subqueries {
                    bail!("{} does not allow subqueries", ecx.name)
                }
                use BinaryOperator::{Eq, NotEq};
                if *negated {
                    // `<expr> NOT IN (<subquery>)` is equivalent to
                    // `<expr> <> ALL (<subquery>)`.
                    (
                        plan_any_or_all(ecx, expr, &NotEq, subquery, AggregateFunc::All)?,
                        None,
                    )
                } else {
                    // `<expr> IN (<subquery>)` is equivalent to
                    // `<expr> = ANY (<subquery>)`.
                    (
                        plan_any_or_all(ecx, expr, &Eq, subquery, AggregateFunc::Any)?,
                        None,
                    )
                }
            }
            Expr::Extract { field, expr } => {
                // No type hint passed to `plan_expr`, because `expr` can be
                // any date type. PostgreSQL is also unable to infer parameter
                // types in this position.
                let mut expr = plan_expr(ecx, expr, None)?;
                let mut typ = ecx.column_type(&expr);
                if let ScalarType::Date = typ.scalar_type {
                    expr = plan_cast_internal(
                        ecx,
                        CastContext::Implicit("EXTRACT"),
                        expr,
                        ScalarType::Timestamp,
                    )?;
                    typ = ecx.column_type(&expr);
                }
                let func = match &typ.scalar_type {
                    ScalarType::Interval => match field {
                        ExtractField::Year => UnaryFunc::ExtractIntervalYear,
                        ExtractField::Month => UnaryFunc::ExtractIntervalMonth,
                        ExtractField::Day => UnaryFunc::ExtractIntervalDay,
                        ExtractField::Hour => UnaryFunc::ExtractIntervalHour,
                        ExtractField::Minute => UnaryFunc::ExtractIntervalMinute,
                        ExtractField::Second => UnaryFunc::ExtractIntervalSecond,
                        ExtractField::Epoch => UnaryFunc::ExtractIntervalEpoch,
                        ExtractField::DayOfWeek
                        | ExtractField::IsoDayOfWeek
                        | ExtractField::Quarter => {
                            failure::bail!("invalid extract field for INTERVAL: {}", field)
                        }
                        _ => failure::bail!(
                            "EXTRACT({} ..) for INTERVAL is not yet implemented",
                            field
                        ),
                    },
                    ScalarType::Timestamp => match field {
                        ExtractField::Year => UnaryFunc::ExtractTimestampYear,
                        ExtractField::Quarter => UnaryFunc::ExtractTimestampQuarter,
                        ExtractField::Month => UnaryFunc::ExtractTimestampMonth,
                        ExtractField::Day => UnaryFunc::ExtractTimestampDay,
                        ExtractField::Hour => UnaryFunc::ExtractTimestampHour,
                        ExtractField::Minute => UnaryFunc::ExtractTimestampMinute,
                        ExtractField::Second => UnaryFunc::ExtractTimestampSecond,
                        ExtractField::WeekOfYear => UnaryFunc::ExtractTimestampWeek,
                        ExtractField::DayOfYear => UnaryFunc::ExtractTimestampDayOfYear,
                        ExtractField::DayOfWeek => UnaryFunc::ExtractTimestampDayOfWeek,
                        ExtractField::IsoDayOfWeek => UnaryFunc::ExtractTimestampIsoDayOfWeek,
                        ExtractField::Epoch => UnaryFunc::ExtractTimestampEpoch,
                        _ => failure::bail!(
                            "EXTRACT({} ..) for timestamp is not yet implemented",
                            field
                        ),
                    },
                    ScalarType::TimestampTz => match field {
                        ExtractField::Year => UnaryFunc::ExtractTimestampTzYear,
                        ExtractField::Quarter => UnaryFunc::ExtractTimestampTzQuarter,
                        ExtractField::Month => UnaryFunc::ExtractTimestampTzMonth,
                        ExtractField::Day => UnaryFunc::ExtractTimestampTzDay,
                        ExtractField::Hour => UnaryFunc::ExtractTimestampTzHour,
                        ExtractField::Minute => UnaryFunc::ExtractTimestampTzMinute,
                        ExtractField::Second => UnaryFunc::ExtractTimestampTzSecond,
                        ExtractField::WeekOfYear => UnaryFunc::ExtractTimestampTzWeek,
                        ExtractField::DayOfYear => UnaryFunc::ExtractTimestampTzDayOfYear,
                        ExtractField::DayOfWeek => UnaryFunc::ExtractTimestampTzDayOfWeek,
                        ExtractField::IsoDayOfWeek => UnaryFunc::ExtractTimestampTzIsoDayOfWeek,
                        ExtractField::Epoch => UnaryFunc::ExtractTimestampTzEpoch,
                        _ => failure::bail!(
                            "EXTRACT({} ..) for timestamp tz is not yet implemented",
                            field
                        ),
                    },
                    other => bail!(
                        "EXTRACT expects timestamp, interval, or date input, got {:?}",
                        other
                    ),
                };
                (expr.call_unary(func), None)
            }
            Expr::Collate { .. } => bail!("COLLATE is not yet supported"),
            Expr::List(exprs) => {
                let elem_type_hint = if let Some(ScalarType::List(elem_type_hint)) = type_hint {
                    Some(*elem_type_hint)
                } else {
                    None
                };
                let exprs = exprs
                    .iter()
                    .map(|expr| plan_expr(ecx, expr, elem_type_hint.clone()))
                    .collect::<Result<Vec<_>, _>>()?;
                let elem_types = exprs
                    .iter()
                    .map(|expr| ecx.scalar_type(expr))
                    .collect::<Vec<_>>();
                let elem_type = if let Some(elem_type) = elem_types.iter().next() {
                    &elem_type
                } else if let Some(elem_type_hint) = &elem_type_hint {
                    elem_type_hint
                } else {
                    bail!("Cannot assign type to this empty list")
                };
                if let Some(pos) = elem_types
                    .iter()
                    .position(|est| (est != elem_type) && (*est != ScalarType::Unknown))
                {
                    bail!("Cannot create list with mixed types. Element 1 has type {} but element {} has type {}", elem_type, pos+1, &elem_types[pos])
                }
                (
                    ScalarExpr::CallVariadic {
                        func: VariadicFunc::ListCreate {
                            elem_type: elem_type.clone(),
                        },
                        exprs,
                    },
                    Some(ScopeItemName {
                        table_name: None,
                        column_name: Some(ColumnName::from("list")),
                    }),
                )
            }
        })
    }
}

// Plans a list of expressions such that all input expressions will be cast to
// the same type. If successful, returns a new list of expressions in the same
// order as the input, where each expression has the appropriate casts to make
// them all of a uniform type.
//
// When types don't match exactly, SQL has some poorly-documented type promotion
// rules. For now, just promote integers into decimals or floats, decimals into
// floats, dates into timestamps, and small Xs into bigger Xs.
fn plan_homogeneous_exprs(
    name: &str,
    ecx: &ExprContext,
    exprs: &[impl std::borrow::Borrow<Expr>],
    type_hint: Option<ScalarType>,
) -> Result<Vec<ScalarExpr>, failure::Error> {
    assert!(!exprs.is_empty());

    let mut pending = vec![None; exprs.len()];

    // Compute the types of all known expressions.
    for (i, expr) in exprs.iter().enumerate() {
        if expr_has_unknown_type(ecx, expr.borrow()) {
            continue;
        }
        let expr = plan_expr(ecx, expr.borrow(), None)?;
        let typ = ecx.column_type(&expr);
        pending[i] = Some((expr, typ));
    }

    // Determine the best target type. If all the expressions were parameters,
    // fall back to `type_hint`.
    let best_target_type = best_target_type(
        pending
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(_expr, typ)| typ.scalar_type.clone())),
    )
    .or(type_hint);

    // Plan all the parameter expressions with `best_target_type` as the type
    // hint.
    for (i, slot) in pending.iter_mut().enumerate() {
        if slot.is_none() {
            let expr = plan_expr(ecx, exprs[i].borrow(), best_target_type.clone())?;
            let typ = ecx.column_type(&expr);
            *slot = Some((expr, typ));
        }
    }

    // Try to cast all expressions to `best_target_type`.
    let mut out = Vec::new();
    for (expr, typ) in pending.into_iter().map(Option::unwrap) {
        match plan_cast_internal(
            ecx,
            CastContext::Implicit(name),
            expr,
            best_target_type.clone().unwrap(),
        ) {
            Ok(expr) => out.push(expr),
            Err(_) => bail!(
                "{} does not have uniform type: {:?} vs {:?}",
                name,
                typ.scalar_type,
                best_target_type,
            ),
        }
    }
    Ok(out)
}

fn plan_any_or_all<'a>(
    ecx: &ExprContext,
    left: &'a Expr,
    op: &'a BinaryOperator,
    right: &'a Query,
    func: AggregateFunc,
) -> Result<ScalarExpr, failure::Error> {
    if !ecx.allow_subqueries {
        bail!("{} does not allow subqueries", ecx.name)
    }
    let qcx = ecx.derived_query_context();
    // plan right

    let (right, _scope) = plan_subquery(&qcx, right)?;
    let column_types = qcx.relation_type(&right).column_types;
    if column_types.len() != 1 {
        bail!(
            "Expected subquery of ANY to return 1 column, got {} columns",
            column_types.len()
        );
    }

    // plan left and op
    // this is a bit of a hack - we want to plan `op` as if the original expr was `(SELECT ANY/ALL(left op right[1]) FROM right)`
    let mut scope = Scope::empty(Some(ecx.scope.clone()));
    let right_name = format!("right_{}", Uuid::new_v4());
    scope.items.push(ScopeItem {
        names: vec![ScopeItemName {
            table_name: None,
            column_name: Some(ColumnName::from(right_name.clone())),
        }],
        expr: None,
        nameable: true,
    });
    let any_ecx = ExprContext {
        qcx: &qcx,
        name: "WHERE clause",
        scope: &scope,
        relation_type: &qcx.relation_type(&right),
        allow_aggregates: false,
        allow_subqueries: true,
    };
    let op_expr = plan_binary_op(
        &any_ecx,
        op,
        left,
        &Expr::Identifier(vec![Ident::new(right_name)]),
    )?;

    // plan subquery
    let expr = right
        .reduce(
            vec![],
            vec![AggregateExpr {
                func,
                expr: Box::new(op_expr),
                distinct: false,
            }],
        )
        .select();
    Ok(expr)
}

fn plan_cast<'a>(
    ecx: &ExprContext,
    expr: &'a Expr,
    data_type: &'a DataType,
) -> Result<(ScalarExpr, Option<ScopeItemName>), failure::Error> {
    let to_scalar_type = scalar_type_from_sql(data_type)?;
    let (expr, maybe_name) = plan_expr_returning_name(ecx, expr, Some(to_scalar_type.clone()))?;
    Ok((
        plan_cast_internal(ecx, CastContext::Explicit, expr, to_scalar_type)?,
        maybe_name,
    ))
}

fn plan_aggregate(ecx: &ExprContext, sql_func: &Function) -> Result<AggregateExpr, failure::Error> {
    let name = normalize::function_name(sql_func.name.clone())?;
    assert!(is_aggregate_func(&name));

    if sql_func.over.is_some() {
        bail!("window functions are not yet supported");
    }

    let (mut expr, mut func) = match (name.as_str(), &sql_func.args) {
        // COUNT(*) is a special case that doesn't compose well
        ("count", FunctionArgs::Star) => (
            // Ok to use `ScalarType::Unknown` here because this expression
            // can't ever escape the surrounding reduce.
            ScalarExpr::literal_null(ScalarType::Unknown),
            AggregateFunc::CountAll,
        ),
        (_, FunctionArgs::Args(args)) if args.len() == 1 => {
            let arg = &args[0];
            // No type hint passed to `plan_expr`, because all aggregates accept
            // multiple input types. PostgreSQL is also unable to infer
            // parameter types in this position.
            let expr = plan_expr(ecx, arg, None)?;
            let typ = ecx.column_type(&expr);
            match find_agg_func(&name, typ.scalar_type)? {
                AggregateFunc::JsonbAgg => {
                    // We need to transform input into jsonb in order to
                    // match Postgres' behavior here.
                    let expr = plan_to_jsonb(ecx, "jsonb_agg", expr)?;
                    (expr, AggregateFunc::JsonbAgg)
                }
                func => (expr, func),
            }
        }
        _ => bail!("{} function requires exactly one non-star argument", name),
    };
    if let Some(filter) = &sql_func.filter {
        // If a filter is present, as in
        //
        //     <agg>(<expr>) FILTER (WHERE <cond>)
        //
        // we plan it by essentially rewriting the expression to
        //
        //     <agg>(CASE WHEN <cond> THEN <expr> ELSE NULL)
        //
        // as aggregate functions ignore NULL. The only exception is `count(*)`,
        // which includes NULLs in its count; we handle that specially by
        // rewriting to:
        //
        //     count(CASE WHEN <cond> THEN TRUE ELSE NULL)
        //
        // (Note the `TRUE` in in place of `<expr>`.)
        let cond = plan_expr(&ecx.with_name("FILTER"), filter, Some(ScalarType::Bool))?;
        let cond_typ = ecx.column_type(&cond);
        if cond_typ.scalar_type != ScalarType::Bool && cond_typ.scalar_type != ScalarType::Unknown {
            bail!(
                "WHERE expression in FILTER must have boolean type, not {:?}",
                cond_typ
            );
        }
        let expr_typ = ecx.scalar_type(&expr);
        if func == AggregateFunc::CountAll {
            func = AggregateFunc::Count;
            expr = ScalarExpr::literal_true();
        }
        expr = ScalarExpr::If {
            cond: Box::new(cond),
            then: Box::new(expr),
            els: Box::new(ScalarExpr::literal_null(expr_typ)),
        };
    }
    Ok(AggregateExpr {
        func,
        expr: Box::new(expr),
        distinct: sql_func.distinct,
    })
}

fn plan_function<'a>(
    ecx: &ExprContext,
    sql_func: &'a Function,
) -> Result<ScalarExpr, failure::Error> {
    let name = normalize::function_name(sql_func.name.clone())?;
    let ident = &*name.to_string();

    if is_aggregate_func(&name) {
        if ecx.allow_aggregates {
            // should already have been caught by `scope.resolve_expr` in `plan_expr`
            bail!(
                "Internal error: encountered unplanned aggregate function: {:?}",
                sql_func,
            )
        } else {
            bail!("aggregate functions are not allowed in {}", ecx.name);
        }
    }

    if sql_func.over.is_some() {
        bail!("OVER specified but {}() is not a window function", ident);
    }
    if sql_func.filter.is_some() {
        bail!(
            "FILTER specified but {}() is not an aggregate function",
            ident
        );
    }
    let args = match &sql_func.args {
        FunctionArgs::Star => bail!(
            "* argument is invalid with non-aggregate function {}",
            ident
        ),
        FunctionArgs::Args(args) => args,
    };
    match ident {
        "abs" => {
            if args.len() != 1 {
                bail!("abs expects one argument, got {}", args.len());
            }
            let expr = plan_expr(ecx, &args[0], Some(ScalarType::Float64))?;
            let typ = ecx.column_type(&expr);
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
            Ok(expr)
        }

        "ascii" => {
            if args.len() != 1 {
                bail!("ascii expects one argument, got {}", args.len());
            }
            let expr = plan_expr(ecx, &args[0], Some(ScalarType::String))?;
            let typ = ecx.column_type(&expr);
            if typ.scalar_type != ScalarType::String && typ.scalar_type != ScalarType::Unknown {
                bail!("ascii does not accept arguments of type {:?}", typ);
            }
            let expr = ScalarExpr::CallUnary {
                func: UnaryFunc::Ascii,
                expr: Box::new(expr),
            };
            Ok(expr)
        }

        "ceil" => {
            if args.len() != 1 {
                bail!("ceil expects 1 argument, got {}", args.len());
            }
            let expr = plan_expr(ecx, &args[0], None)?;
            let expr = promote_number_floatdec(ecx, "ceil", expr)?;
            Ok(match ecx.column_type(&expr).scalar_type {
                ScalarType::Float32 => expr.call_unary(UnaryFunc::CeilFloat32),
                ScalarType::Float64 => expr.call_unary(UnaryFunc::CeilFloat64),
                ScalarType::Decimal(_, s) => expr.call_unary(UnaryFunc::CeilDecimal(s)),
                _ => unreachable!(),
            })
        }

        "coalesce" => {
            if args.is_empty() {
                bail!("coalesce requires at least one argument");
            }
            let expr = ScalarExpr::CallVariadic {
                func: VariadicFunc::Coalesce,
                exprs: plan_homogeneous_exprs("coalesce", ecx, &args, Some(ScalarType::String))?,
            };
            Ok(expr)
        }

        "concat" => {
            if args.is_empty() {
                bail!("concat requires at least one argument");
            }
            let mut exprs = Vec::new();
            for arg in args {
                let expr = plan_expr(ecx, arg, Some(ScalarType::String))?;
                let expr = plan_cast_internal(
                    ecx,
                    CastContext::Implicit("concat"),
                    expr,
                    ScalarType::String,
                )?;
                exprs.push(expr);
            }
            let expr = ScalarExpr::CallVariadic {
                func: VariadicFunc::Concat,
                exprs,
            };
            Ok(expr)
        }

        "current_timestamp" | "now" => {
            if !args.is_empty() {
                bail!("{} does not take any arguments", ident);
            }
            match ecx.qcx.lifetime {
                QueryLifetime::OneShot => Ok(ScalarExpr::literal(
                    Datum::from(ecx.qcx.scx.pcx.wall_time),
                    ColumnType::new(ScalarType::TimestampTz),
                )),
                QueryLifetime::Static => bail!("{} cannot be used in static queries", ident),
            }
        }

        "date_trunc" => {
            if args.len() != 2 {
                bail!("date_trunc() requires exactly two arguments");
            }

            let precision_field = plan_expr(ecx, &args[0], Some(ScalarType::String))?;
            let typ = ecx.column_type(&precision_field);
            if typ.scalar_type != ScalarType::String {
                bail!("date_trunc() can only be formatted with strings");
            }

            // If the precision field happens to be a literal, we can do
            // some early validation.
            if let ScalarExpr::Literal(row, _) = &precision_field {
                let datum = row.unpack_first();
                let precision_str = datum.unwrap_str();
                let _ = precision_str.parse::<DateTruncTo>()?;
            }

            let source_timestamp = plan_expr(ecx, &args[1], Some(ScalarType::TimestampTz))?;
            let typ = ecx.column_type(&source_timestamp);

            let expr = match typ.scalar_type {
                ScalarType::Timestamp => ScalarExpr::CallBinary {
                    func: BinaryFunc::DateTruncTimestamp,
                    expr1: Box::new(precision_field),
                    expr2: Box::new(source_timestamp),
                },
                ScalarType::TimestampTz => ScalarExpr::CallBinary {
                    func: BinaryFunc::DateTruncTimestampTz,
                    expr1: Box::new(precision_field),
                    expr2: Box::new(source_timestamp),
                },
                _ => bail!(
                    "date_trunc() is currently only implemented for TIMESTAMPs and TIMESTAMPTZs"
                ),
            };

            Ok(expr)
        }

        "floor" => {
            if args.len() != 1 {
                bail!("floor expects 1 argument, got {}", args.len());
            }
            let expr = plan_expr(ecx, &args[0], None)?;
            let expr = promote_number_floatdec(ecx, "floor", expr)?;
            Ok(match ecx.column_type(&expr).scalar_type {
                ScalarType::Float32 => expr.call_unary(UnaryFunc::FloorFloat32),
                ScalarType::Float64 => expr.call_unary(UnaryFunc::FloorFloat64),
                ScalarType::Decimal(_, s) => expr.call_unary(UnaryFunc::FloorDecimal(s)),
                _ => unreachable!(),
            })
        }

        // Promotes a numeric type to the smallest fractional type that
        // can represent it. This is primarily useful for the avg
        // aggregate function, so that the avg of an integer column does
        // not get truncated to an integer, which would be surprising to
        // users (#549).
        "internal_avg_promotion" => {
            if args.len() != 1 {
                bail!("internal.avg_promotion requires exactly one argument");
            }
            let expr = plan_expr(ecx, &args[0], None)?;
            let typ = ecx.column_type(&expr);
            let output_type = match &typ.scalar_type {
                ScalarType::Unknown => ScalarType::Unknown,
                ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
                ScalarType::Decimal(p, s) => ScalarType::Decimal(*p, *s),
                ScalarType::Int32 => ScalarType::Decimal(10, 0),
                ScalarType::Int64 => ScalarType::Decimal(19, 0),
                _ => bail!("internal.avg_promotion called with unexpected argument"),
            };
            plan_cast_internal(
                ecx,
                CastContext::Implicit("internal.avg_promotion"),
                expr,
                output_type,
            )
        }

        "jsonb_array_length" | "jsonb_typeof" | "jsonb_strip_nulls" | "jsonb_pretty" => {
            if args.len() != 1 {
                bail!("{}() requires exactly two arguments", ident);
            }
            let jsonb = plan_expr(ecx, &args[0], Some(ScalarType::Jsonb))?;
            let typ = ecx.column_type(&jsonb);
            if typ.scalar_type != ScalarType::Jsonb && typ.scalar_type != ScalarType::Unknown {
                bail!(
                    "{}() requires jsonb as it's first argument, but got {}",
                    ident,
                    typ.scalar_type
                );
            }
            let expr = ScalarExpr::CallUnary {
                func: match ident {
                    "jsonb_array_length" => UnaryFunc::JsonbArrayLength,
                    "jsonb_typeof" => UnaryFunc::JsonbTypeof,
                    "jsonb_strip_nulls" => UnaryFunc::JsonbStripNulls,
                    "jsonb_pretty" => UnaryFunc::JsonbPretty,
                    _ => unreachable!(),
                },
                expr: Box::new(jsonb),
            };
            Ok(expr)
        }

        "jsonb_build_array" => {
            let args = args
                .iter()
                .map(|arg| {
                    Ok(plan_to_jsonb(
                        ecx,
                        "jsonb_build_array",
                        plan_expr(ecx, arg, None)?,
                    )?)
                })
                .collect::<Result<Vec<_>, failure::Error>>()?;
            let expr = ScalarExpr::CallVariadic {
                func: VariadicFunc::JsonbBuildArray,
                exprs: args,
            };
            Ok(expr)
        }

        "jsonb_build_object" => {
            if args.len() % 2 != 0 {
                bail!("jsonb_build_object() requires an even number of arguments");
            }
            let args = args
                .iter()
                .enumerate()
                .map(|(i, arg)| {
                    Ok(if i % 2 == 0 {
                        plan_cast_internal(
                            ecx,
                            CastContext::Explicit,
                            plan_expr(ecx, arg, None)?,
                            ScalarType::String,
                        )?
                    } else {
                        plan_to_jsonb(ecx, "jsonb_build_object", plan_expr(ecx, arg, None)?)?
                    })
                })
                .collect::<Result<Vec<_>, failure::Error>>()?;
            let expr = ScalarExpr::CallVariadic {
                func: VariadicFunc::JsonbBuildObject,
                exprs: args,
            };
            Ok(expr)
        }

        "round" => {
            if args.is_empty() || args.len() > 2 {
                bail!("round expects 1 or 2 arguments, got {}", args.len());
            }

            if args.len() == 1 {
                // When there is only one argument, the argument can be
                // any numeric type, with integers promoted to decimals.
                let expr1 = plan_expr(ecx, &args[0], None)?;
                let expr1 = promote_number_floatdec(ecx, "round argument", expr1)?;
                Ok(match ecx.column_type(&expr1).scalar_type {
                    ScalarType::Float32 => expr1.call_unary(UnaryFunc::RoundFloat32),
                    ScalarType::Float64 => expr1.call_unary(UnaryFunc::RoundFloat64),
                    ScalarType::Decimal(_, s) => {
                        let zero = ScalarExpr::literal(
                            Datum::Int64(0),
                            ColumnType::new(ScalarType::Int64),
                        );
                        expr1.call_binary(zero, BinaryFunc::RoundDecimal(s))
                    }
                    _ => unreachable!(),
                })
            } else {
                // When there are two arguments, the first argument has to
                // be a decimal.
                let expr1 = plan_expr(ecx, &args[0], None)?;
                let (expr1, scale) = promote_int_decimal(ecx, "first round argument", expr1)?;
                let expr2 = plan_expr(ecx, &args[1], None)?;
                let expr2 = promote_int_int64(ecx, "second round argument", expr2)?;
                Ok(expr1.call_binary(expr2, BinaryFunc::RoundDecimal(scale)))
            }
        }

        "length" => {
            if args.is_empty() || args.len() > 2 {
                bail!("length expects one or two arguments, got {:?}", args.len());
            }

            let mut exprs = Vec::new();
            let expr1 = plan_expr(ecx, &args[0], Some(ScalarType::String))?;
            let typ1 = ecx.column_type(&expr1);
            match typ1.scalar_type {
                ScalarType::String | ScalarType::Unknown => {
                    exprs.push(expr1);

                    if args.len() == 2 {
                        let expr2 = plan_expr(ecx, &args[1], Some(ScalarType::String))?;
                        let typ2 = ecx.column_type(&expr2);
                        if typ2.scalar_type != ScalarType::String
                            && typ2.scalar_type != ScalarType::Unknown
                        {
                            bail!("length second argument has non-string type {:?}", typ1);
                        }
                        exprs.push(expr2);
                    }
                    let expr = ScalarExpr::CallVariadic {
                        func: VariadicFunc::LengthString,
                        exprs,
                    };
                    Ok(expr)
                }
                ScalarType::Bytes => {
                    if args.len() != 1 {
                        bail!(
                            "length expects only one argument when first argument \
                                has type bytea, got {:?}",
                            args.len(),
                        );
                    }
                    Ok(expr1.call_unary(UnaryFunc::LengthBytes))
                }
                _ => bail!("length first argument has non-string type {:?}", typ1),
            }
        }

        "make_timestamp" => {
            if args.len() != 6 {
                bail!("make_timestamp expects six arguments, got {}", args.len());
            }

            let mut exprs = Vec::new();
            for arg in &args[..5] {
                let expr = plan_expr(ecx, arg, Some(ScalarType::Int64))?;
                let expr = promote_int_int64(ecx, "make_timestamp", expr)?;
                exprs.push(expr);
            }
            {
                let expr = plan_expr(ecx, &args[5], Some(ScalarType::Float64))?;
                let expr = promote_decimal_float64(ecx, "make_timestamp", expr)?;
                exprs.push(expr);
            }
            let expr = ScalarExpr::CallVariadic {
                func: VariadicFunc::MakeTimestamp,
                exprs,
            };
            Ok(expr)
        }

        "mod" => {
            if args.len() != 2 {
                bail!("mod requires exactly two arguments");
            }
            plan_binary_op(ecx, &BinaryOperator::Modulus, &args[0], &args[1])
        }

        "mz_logical_timestamp" => {
            if !args.is_empty() {
                bail!("mz_logical_timestamp does not take any arguments");
            }
            match ecx.qcx.lifetime {
                QueryLifetime::OneShot => {
                    Ok(ScalarExpr::CallNullary(NullaryFunc::MzLogicalTimestamp))
                }
                QueryLifetime::Static => bail!("{} cannot be used in static queries", ident),
            }
        }

        "nullif" => {
            if args.len() != 2 {
                bail!("nullif requires exactly two arguments");
            }
            let cond = Expr::BinaryOp {
                left: Box::new(args[0].clone()),
                op: BinaryOperator::Eq,
                right: Box::new(args[1].clone()),
            };
            let cond_expr = plan_expr(ecx, &cond, None)?;
            let else_expr = plan_expr(ecx, &args[0], None)?;
            let expr = ScalarExpr::If {
                cond: Box::new(cond_expr),
                then: Box::new(ScalarExpr::literal_null(ecx.scalar_type(&else_expr))),
                els: Box::new(else_expr),
            };
            Ok(expr)
        }

        "sqrt" => {
            if args.len() != 1 {
                bail!("sqrt expects 1 argument, got {}", args.len());
            }
            let expr = plan_expr(ecx, &args[0], None)?;
            let expr = promote_number_floatdec(ecx, "sqrt", expr)?;
            Ok(match ecx.column_type(&expr).scalar_type {
                ScalarType::Float32 => expr.call_unary(UnaryFunc::SqrtFloat32),
                ScalarType::Float64 => expr.call_unary(UnaryFunc::SqrtFloat64),
                ScalarType::Decimal(p, s) => {
                    // TODO(benesch): proper sqrt support for decimals. For
                    // now we cast to an f64 and back, which is semi-ok
                    // because sqrt is an inherently imprecise operation.
                    let expr = plan_cast_internal(
                        ecx,
                        CastContext::Implicit("sqrt"),
                        expr,
                        ScalarType::Float64,
                    )?;
                    let expr = expr.call_unary(UnaryFunc::SqrtFloat64);
                    plan_cast_internal(
                        ecx,
                        CastContext::Implicit("sqrt"),
                        expr,
                        ScalarType::Decimal(p, s),
                    )?
                }
                _ => unreachable!(),
            })
        }

        "substr" => {
            let func = Function {
                name: ObjectName(vec![Ident::new("substring")]),
                args: sql_func.args.clone(),
                filter: sql_func.filter.clone(),
                over: sql_func.over.clone(),
                distinct: sql_func.distinct,
            };
            plan_function(ecx, &func)
        }

        "substring" => {
            if args.len() < 2 || args.len() > 3 {
                bail!(
                    "substring expects two or three arguments, got {:?}",
                    args.len()
                );
            }
            let mut exprs = Vec::new();
            let expr1 = plan_expr(ecx, &args[0], Some(ScalarType::String))?;
            let typ1 = ecx.column_type(&expr1);
            if typ1.scalar_type != ScalarType::String && typ1.scalar_type != ScalarType::Unknown {
                bail!("substring first argument has non-string type {:?}", typ1);
            }
            exprs.push(expr1);

            let expr2 = plan_expr(ecx, &args[1], Some(ScalarType::Int64))?;
            let expr2 = promote_int_int64(ecx, "substring start", expr2)?;
            exprs.push(expr2);
            if args.len() == 3 {
                let expr3 = plan_expr(ecx, &args[2], Some(ScalarType::Int64))?;
                let expr3 = promote_int_int64(ecx, "substring length", expr3)?;
                exprs.push(expr3);
            }
            let expr = ScalarExpr::CallVariadic {
                func: VariadicFunc::Substr,
                exprs,
            };
            Ok(expr)
        }

        "replace" => {
            if args.len() != 3 {
                bail!(
                    "replace expects exactly three arguments, got {:?}",
                    args.len()
                )
            }

            let mut exprs = Vec::new();
            let original_string = plan_expr(ecx, &args[0], Some(ScalarType::String))?;
            let original_string_typ = ecx.column_type(&original_string);
            // todo: function that will do these steps for us?
            if original_string_typ.scalar_type != ScalarType::String {
                bail!(
                    "replace first argument has non-string type {:?}",
                    original_string_typ
                );
            }
            exprs.push(original_string);

            let from = plan_expr(ecx, &args[1], Some(ScalarType::String))?;
            let from_typ = ecx.column_type(&from);
            if from_typ.scalar_type != ScalarType::String {
                bail!("replace second argument has non-string type {:?}", from_typ);
            }
            exprs.push(from);

            let to = plan_expr(ecx, &args[2], Some(ScalarType::String))?;
            let to_typ = ecx.column_type(&to);
            if to_typ.scalar_type != ScalarType::String {
                bail!("replace third argument has non-string type {:?}", to_typ);
            }
            exprs.push(to);

            let expr = ScalarExpr::CallVariadic {
                func: VariadicFunc::Replace,
                exprs,
            };

            Ok(expr)
        }

        "to_char" => {
            if args.len() != 2 {
                bail!("to_char requires exactly two arguments");
            }

            let ts_expr = plan_expr(ecx, &args[0], Some(ScalarType::TimestampTz))?;
            let ts_type = ecx.column_type(&ts_expr);
            match ts_type.scalar_type {
                ScalarType::Timestamp | ScalarType::TimestampTz | ScalarType::Unknown => (),
                other => bail!("to_char requires a timestamp or timestamptz as its first argument, but got: {}", other)
            }

            let fmt_expr = plan_expr(ecx, &args[1], Some(ScalarType::String))?;
            let fmt_typ = ecx.column_type(&fmt_expr);
            if fmt_typ.scalar_type != ScalarType::String
                && fmt_typ.scalar_type != ScalarType::Unknown
            {
                bail!(
                    "to_char requires a string as its second arugment, but got: {}",
                    fmt_typ.scalar_type
                );
            }

            Ok(ScalarExpr::CallBinary {
                func: if ts_type.scalar_type == ScalarType::Timestamp {
                    BinaryFunc::ToCharTimestamp
                } else {
                    BinaryFunc::ToCharTimestampTz
                },
                expr1: Box::new(ts_expr),
                expr2: Box::new(fmt_expr),
            })
        }

        "to_jsonb" => {
            if args.len() != 1 {
                bail!("{}() requires exactly two arguments", ident);
            }
            let arg = plan_expr(ecx, &args[0], None)?;
            // > Returns the value as json or jsonb. Arrays and composites
            // > are converted (recursively) to arrays and objects;
            // > otherwise, if there is a cast from the type to json, the
            // > cast function will be used to perform the conversion;
            // > otherwise, a scalar value is produced. For any scalar type
            // > other than a number, a Boolean, or a null value, the text
            // > representation will be used, in such a fashion that it is a
            // > valid json or jsonb value.
            //
            // https://www.postgresql.org/docs/current/functions-json.html
            let expr = plan_to_jsonb(ecx, "to_jsonb", arg)?;
            Ok(expr)
        }

        "to_timestamp" => {
            if args.len() != 1 {
                bail!("to_timestamp requires exactly one argument");
            }
            let expr = plan_expr(ecx, &args[0], Some(ScalarType::Float64))?;
            let expr = promote_number_float64(ecx, "to_timestamp", expr)?;
            Ok(expr.call_unary(UnaryFunc::ToTimestamp))
        }

        "convert_from" => {
            if args.len() != 2 {
                bail!("convert_from requires exactly two arguments");
            }

            let str_expr = plan_expr(ecx, &args[0], Some(ScalarType::Bytes))?;
            let str_type = ecx.column_type(&str_expr);
            if str_type.scalar_type != ScalarType::Bytes {
                bail!(
                    "convert_from requires a bytea value as its first argument, but got: {}",
                    str_type.scalar_type
                );
            }

            let enc_expr = plan_expr(ecx, &args[1], Some(ScalarType::String))?;
            let enc_type = ecx.column_type(&enc_expr);
            if enc_type.scalar_type != ScalarType::String {
                bail!(
                    "convert_from requires a string as its second argument, but got: {}",
                    enc_type.scalar_type
                );
            }

            Ok(ScalarExpr::CallBinary {
                func: BinaryFunc::ConvertFrom,
                expr1: Box::new(str_expr),
                expr2: Box::new(enc_expr),
            })
        }

        _ => {
            if is_table_func(&name) {
                bail!(
                    "table functions in scalar position are not supported: {}",
                    ident
                )
            } else {
                bail!("unsupported function: {}", ident)
            }
        }
    }
}

fn plan_to_jsonb(
    ecx: &ExprContext,
    name: &str,
    arg: ScalarExpr,
) -> Result<ScalarExpr, failure::Error> {
    let typ = ecx.column_type(&arg).scalar_type;
    Ok(match typ {
        ScalarType::Jsonb => arg,
        ScalarType::String | ScalarType::Float64 | ScalarType::Bool | ScalarType::Unknown => {
            arg.call_unary(UnaryFunc::CastJsonbOrNullToJsonb)
        }
        ScalarType::Int32 => arg
            .call_unary(UnaryFunc::CastInt32ToFloat64)
            .call_unary(UnaryFunc::CastJsonbOrNullToJsonb),
        ScalarType::Int64 | ScalarType::Float32 | ScalarType::Decimal(..) => {
            // TODO(jamii) this is awaiting a decision about how to represent numbers in jsonb
            bail!(
                "{}() doesn't currently support {} arguments, try adding ::float to the argument for now",
                name,
                typ
            )
        }
        _ => plan_cast_internal(ecx, CastContext::Implicit(name), arg, ScalarType::String)?
            .call_unary(UnaryFunc::CastJsonbOrNullToJsonb),
    })
}

fn plan_is_null_expr<'a>(
    ecx: &ExprContext,
    inner: &'a Expr,
    not: bool,
) -> Result<ScalarExpr, failure::Error> {
    // No type hint passed to `plan_expr`. In other situations where any type
    // will do, PostgreSQL uses `ScalarType::String`, but for some reason it
    // does not here.
    let expr = plan_expr(ecx, inner, None)?;
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
    Ok(expr)
}

fn plan_unary_op<'a>(
    ecx: &ExprContext,
    op: &'a UnaryOperator,
    expr: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let type_hint = match op {
        UnaryOperator::Not => ScalarType::Bool,
        UnaryOperator::Plus | UnaryOperator::Minus => ScalarType::Float64,
    };
    let expr = plan_expr(ecx, expr, Some(type_hint))?;
    let typ = ecx.column_type(&expr);
    let func = match op {
        UnaryOperator::Not => match typ.scalar_type {
            ScalarType::Bool => UnaryFunc::Not,
            _ => bail!(
                "Cannot apply operator Not to non-boolean type {:?}",
                typ.scalar_type
            ),
        },
        UnaryOperator::Plus => return Ok(expr), // no-op
        UnaryOperator::Minus => match typ.scalar_type {
            ScalarType::Int32 => UnaryFunc::NegInt32,
            ScalarType::Int64 => UnaryFunc::NegInt64,
            ScalarType::Float32 => UnaryFunc::NegFloat32,
            ScalarType::Float64 => UnaryFunc::NegFloat64,
            ScalarType::Decimal(_, _) => UnaryFunc::NegDecimal,
            ScalarType::Interval => UnaryFunc::NegInterval,
            _ => bail!("cannot negate {:?}", typ.scalar_type),
        },
    };
    let expr = ScalarExpr::CallUnary {
        func,
        expr: Box::new(expr),
    };
    Ok(expr)
}

fn plan_binary_op<'a>(
    ecx: &ExprContext,
    op: &'a BinaryOperator,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    use BinaryOperator::*;
    match op {
        And => plan_boolean_op(ecx, BooleanOp::And, left, right),
        Or => plan_boolean_op(ecx, BooleanOp::Or, left, right),

        Plus => plan_arithmetic_op(ecx, ArithmeticOp::Plus, left, right),
        Minus => plan_arithmetic_op(ecx, ArithmeticOp::Minus, left, right),
        Multiply => plan_arithmetic_op(ecx, ArithmeticOp::Multiply, left, right),
        Divide => plan_arithmetic_op(ecx, ArithmeticOp::Divide, left, right),
        Modulus => plan_arithmetic_op(ecx, ArithmeticOp::Modulo, left, right),

        Lt => plan_comparison_op(ecx, ComparisonOp::Lt, left, right),
        LtEq => plan_comparison_op(ecx, ComparisonOp::LtEq, left, right),
        Gt => plan_comparison_op(ecx, ComparisonOp::Gt, left, right),
        GtEq => plan_comparison_op(ecx, ComparisonOp::GtEq, left, right),
        Eq => plan_comparison_op(ecx, ComparisonOp::Eq, left, right),
        NotEq => plan_comparison_op(ecx, ComparisonOp::NotEq, left, right),

        Like => plan_like(ecx, left, right, false),
        NotLike => plan_like(ecx, left, right, true),

        JsonGet => plan_json_op(ecx, JsonOp::Get, left, right),
        JsonGetAsText => plan_json_op(ecx, JsonOp::GetAsText, left, right),
        JsonGetPath => plan_json_op(ecx, JsonOp::GetPath, left, right),
        JsonGetPathAsText => plan_json_op(ecx, JsonOp::GetPathAsText, left, right),
        JsonContainsJson => plan_json_op(ecx, JsonOp::ContainsJson, left, right),
        JsonContainedInJson => plan_json_op(ecx, JsonOp::ContainedInJson, left, right),
        JsonContainsField => plan_json_op(ecx, JsonOp::ContainsField, left, right),
        JsonContainsAnyFields => plan_json_op(ecx, JsonOp::ContainsAnyFields, left, right),
        JsonContainsAllFields => plan_json_op(ecx, JsonOp::ContainsAllFields, left, right),
        JsonConcat => plan_json_op(ecx, JsonOp::Concat, left, right),
        JsonDeletePath => plan_json_op(ecx, JsonOp::DeletePath, left, right),
        JsonContainsPath => plan_json_op(ecx, JsonOp::ContainsPath, left, right),
        JsonApplyPathPredicate => plan_json_op(ecx, JsonOp::ApplyPathPredicate, left, right),
    }
}

fn plan_boolean_op<'a>(
    ecx: &ExprContext,
    op: BooleanOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let lexpr = plan_expr(ecx, left, Some(ScalarType::Bool))?;
    let rexpr = plan_expr(ecx, right, Some(ScalarType::Bool))?;
    let ltype = ecx.column_type(&lexpr);
    let rtype = ecx.column_type(&rexpr);

    if ltype.scalar_type != ScalarType::Bool && ltype.scalar_type != ScalarType::Unknown {
        bail!(
            "Cannot apply operator {:?} to non-boolean type {:?}",
            op,
            ltype.scalar_type
        )
    }
    if rtype.scalar_type != ScalarType::Bool && rtype.scalar_type != ScalarType::Unknown {
        bail!(
            "Cannot apply operator {:?} to non-boolean type {:?}",
            op,
            rtype.scalar_type
        )
    }
    let func = match op {
        BooleanOp::And => BinaryFunc::And,
        BooleanOp::Or => BinaryFunc::Or,
    };
    let expr = lexpr.call_binary(rexpr, func);
    Ok(expr)
}

fn plan_arithmetic_op<'a>(
    ecx: &ExprContext,
    op: ArithmeticOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    use ArithmeticOp::*;
    use BinaryFunc::*;
    use ScalarType::*;

    // Step 1. Plan inner expressions.
    let left_unknown = expr_has_unknown_type(ecx, left);
    let right_unknown = expr_has_unknown_type(ecx, right);
    let (mut lexpr, mut ltype, mut rexpr, mut rtype) = match (left_unknown, right_unknown) {
        // Both inputs are parameters of unknown types.
        (true, true) => {
            // We have no clues as to which operator to select, so bail. To
            // avoid duplicating the logic for generating the correct error
            // message, we just plan the left expression with no type hint,
            // which we know will fail. (We could just as well plan the right
            // expression.)
            plan_expr(ecx, left, None)?;
            unreachable!();
        }

        // Neither input is a parameter of an unknown type.
        (false, false) => {
            let mut lexpr = plan_expr(ecx, left, None)?;
            let mut rexpr = plan_expr(ecx, right, None)?;
            let mut ltype = ecx.column_type(&lexpr);
            let mut rtype = ecx.column_type(&rexpr);

            // When both inputs are already decimals, we skip coalescing, which
            // could result in a rescale if the decimals have different
            // precisions, because we tightly control the rescale when planning
            // the arithmetic operation (below). E.g., decimal multiplication
            // does not need to rescale its inputs, even when the inputs have
            // different scales.
            //
            // Similarly, if either input is a timelike or JSON, we skip
            // coalescing. Timelike values have well-defined operations with
            // intervals, and JSON values have well-defined operations with
            // strings and integers, so attempting to homogeneous the types will
            // cause us to miss out on the overload.
            let coalesce = match (&ltype.scalar_type, &rtype.scalar_type) {
                (Decimal(_, _), Decimal(_, _))
                | (Date, _)
                | (Time, _)
                | (Timestamp, _)
                | (TimestampTz, _)
                | (_, Date)
                | (_, Time)
                | (_, Timestamp)
                | (_, TimestampTz)
                | (Jsonb, _)
                | (_, Jsonb) => false,
                _ => true,
            };
            if coalesce {
                // Try to find a common type that can represent both inputs.
                let op_string = op.to_string();
                let best_target_type =
                    best_target_type(vec![ltype.scalar_type.clone(), rtype.scalar_type.clone()])
                        .unwrap();
                lexpr = match plan_cast_internal(
                    ecx,
                    CastContext::Implicit(&op_string),
                    lexpr,
                    best_target_type.clone(),
                ) {
                    Ok(expr) => expr,
                    Err(_) => bail!(
                        "{} does not have uniform type: {:?} vs {:?}",
                        &op_string,
                        ltype.scalar_type,
                        best_target_type,
                    ),
                };
                rexpr = match plan_cast_internal(
                    ecx,
                    CastContext::Implicit(&op_string),
                    rexpr,
                    best_target_type.clone(),
                ) {
                    Ok(expr) => expr,
                    Err(_) => bail!(
                        "{} does not have uniform type: {:?} vs {:?}",
                        &op_string,
                        rtype.scalar_type,
                        best_target_type,
                    ),
                };
                rtype = ecx.column_type(&rexpr);
                ltype = ecx.column_type(&lexpr);
            }

            (lexpr, ltype, rexpr, rtype)
        }

        // One of the inputs is a parameter of unknown type, but the other is
        // not.
        (swap @ false, true) | (swap @ true, false) => {
            // Knowing the type of one input is often sufficiently constraining
            // to allow us to infer the type of the unknown input.

            // First, adjust so the known input is on the left.
            let (left, right) = if swap { (right, left) } else { (left, right) };

            // Determine the known type.
            let lexpr = plan_expr(ecx, left, None)?;
            let ltype = ecx.column_type(&lexpr);

            // Infer the unknown type. Dates and timestamps want an interval for
            // arithmetic. JSON wants an int *or* a string for the removal
            // (minus) operator, but according to Postgres we can just pick
            // string. Everything else wants its own type.
            let type_hint = match ltype.scalar_type.clone() {
                Date | Timestamp | TimestampTz => Interval,
                Jsonb => String,
                other => other,
            };
            let rexpr = plan_expr(ecx, right, Some(type_hint))?;
            let rtype = ecx.column_type(&rexpr);

            // Swap back, if necessary.
            if swap {
                (rexpr, rtype, lexpr, ltype)
            } else {
                (lexpr, ltype, rexpr, rtype)
            }
        }
    };

    // Step 2a. Plan the arithmetic operation for decimals.
    //
    // Decimal arithmetic requires special support from the planner, because
    // the precision and scale of the decimal is erased in the dataflow
    // layer. Operations follow Snowflake's rules for precision/scale
    // conversions. [0]
    //
    // [0]: https://docs.snowflake.net/manuals/sql-reference/operators-arithmetic.html
    match (&op, &ltype.scalar_type, &rtype.scalar_type) {
        (Plus, Decimal(_, s1), Decimal(_, s2))
        | (Minus, Decimal(_, s1), Decimal(_, s2))
        | (Modulo, Decimal(_, s1), Decimal(_, s2)) => {
            let so = cmp::max(s1, s2);
            let lexpr = rescale_decimal(lexpr, *s1, *so);
            let rexpr = rescale_decimal(rexpr, *s2, *so);
            let func = match op {
                Plus => AddDecimal,
                Minus => SubDecimal,
                Modulo => ModDecimal,
                _ => unreachable!(),
            };
            let expr = lexpr.call_binary(rexpr, func);
            return Ok(expr);
        }
        (Multiply, Decimal(_, s1), Decimal(_, s2)) => {
            let so = cmp::max(cmp::max(cmp::min(s1 + s2, 12), *s1), *s2);
            let si = s1 + s2;
            let expr = lexpr.call_binary(rexpr, MulDecimal);
            let expr = rescale_decimal(expr, si, so);
            return Ok(expr);
        }
        (Divide, Decimal(_, s1), Decimal(_, s2)) => {
            let s = cmp::max(cmp::min(12, s1 + 6), *s1);
            let si = cmp::max(s + 1, *s2);
            lexpr = rescale_decimal(lexpr, *s1, si);
            let expr = lexpr.call_binary(rexpr, DivDecimal);
            let expr = rescale_decimal(expr, si - s2, s);
            return Ok(expr);
        }
        _ => (),
    }

    // Step 2b. Plan the arithmetic operation for all other types.
    let func = match op {
        Plus => match (&ltype.scalar_type, &rtype.scalar_type) {
            (Int32, Int32) => AddInt32,
            (Int64, Int64) => AddInt64,
            (Float32, Float32) => AddFloat32,
            (Float64, Float64) => AddFloat64,
            (Interval, Interval) => AddInterval,
            (Timestamp, Interval) => AddTimestampInterval,
            (Interval, Timestamp) => {
                mem::swap(&mut lexpr, &mut rexpr);
                mem::swap(&mut ltype, &mut rtype);
                AddTimestampInterval
            }
            (TimestampTz, Interval) => AddTimestampTzInterval,
            (Interval, TimestampTz) => {
                mem::swap(&mut lexpr, &mut rexpr);
                mem::swap(&mut ltype, &mut rtype);
                AddTimestampTzInterval
            }
            (Date, Interval) => AddDateInterval,
            (Interval, Date) => {
                mem::swap(&mut lexpr, &mut rexpr);
                mem::swap(&mut ltype, &mut rtype);
                AddDateInterval
            }
            (Date, Time) => AddDateTime,
            (Time, Date) => {
                mem::swap(&mut lexpr, &mut rexpr);
                mem::swap(&mut ltype, &mut rtype);
                AddDateTime
            }
            (Time, Interval) => AddTimeInterval,
            (Interval, Time) => {
                mem::swap(&mut lexpr, &mut rexpr);
                mem::swap(&mut ltype, &mut rtype);
                AddTimeInterval
            }
            _ => bail!(
                "no overload for {} + {}",
                ltype.scalar_type,
                rtype.scalar_type
            ),
        },
        Minus => match (&ltype.scalar_type, &rtype.scalar_type) {
            (Int32, Int32) => SubInt32,
            (Int64, Int64) => SubInt64,
            (Float32, Float32) => SubFloat32,
            (Float64, Float64) => SubFloat64,
            (Interval, Interval) => SubInterval,
            (Timestamp, Timestamp) => SubTimestamp,
            (TimestampTz, TimestampTz) => SubTimestampTz,
            (Timestamp, Interval) => SubTimestampInterval,
            (TimestampTz, Interval) => SubTimestampTzInterval,
            (Date, Date) => SubDate,
            (Date, Interval) => SubDateInterval,
            (Time, Time) => SubTime,
            (Time, Interval) => SubTimeInterval,
            (Jsonb, Int32) => {
                rexpr = rexpr.call_unary(UnaryFunc::CastInt32ToInt64);
                JsonbDeleteInt64
            }
            (Jsonb, Int64) => JsonbDeleteInt64,
            (Jsonb, String) => JsonbDeleteString,
            // TODO(jamii) there should be corresponding overloads for
            // Array(Int64) and Array(String)
            _ => bail!(
                "no overload for {} - {}",
                ltype.scalar_type,
                rtype.scalar_type
            ),
        },
        Multiply => match (&ltype.scalar_type, &rtype.scalar_type) {
            (Int32, Int32) => MulInt32,
            (Int64, Int64) => MulInt64,
            (Float32, Float32) => MulFloat32,
            (Float64, Float64) => MulFloat64,
            _ => bail!(
                "no overload for {} * {}",
                ltype.scalar_type,
                rtype.scalar_type
            ),
        },
        Divide => match (&ltype.scalar_type, &rtype.scalar_type) {
            (Int32, Int32) => DivInt32,
            (Int64, Int64) => DivInt64,
            (Float32, Float32) => DivFloat32,
            (Float64, Float64) => DivFloat64,
            _ => bail!(
                "no overload for {} / {}",
                ltype.scalar_type,
                rtype.scalar_type
            ),
        },
        Modulo => match (&ltype.scalar_type, &rtype.scalar_type) {
            (Int32, Int32) => ModInt32,
            (Int64, Int64) => ModInt64,
            (Float32, Float32) => ModFloat32,
            (Float64, Float64) => ModFloat64,
            _ => bail!(
                "no overload for {} % {}",
                ltype.scalar_type,
                rtype.scalar_type
            ),
        },
    };
    let expr = lexpr.call_binary(rexpr, func);
    Ok(expr)
}

fn plan_comparison_op<'a>(
    ecx: &ExprContext,
    op: ComparisonOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let op_name = op.to_string();
    let mut exprs =
        plan_homogeneous_exprs(&op_name, ecx, &[left, right], Some(ScalarType::String))?;
    assert_eq!(exprs.len(), 2);
    let rexpr = exprs.pop().unwrap();
    let lexpr = exprs.pop().unwrap();
    let rtype = ecx.column_type(&rexpr);
    let ltype = ecx.column_type(&lexpr);

    if ltype.scalar_type != rtype.scalar_type
        && ltype.scalar_type != ScalarType::Unknown
        && rtype.scalar_type != ScalarType::Unknown
    {
        bail!(
            "{:?} and {:?} are not comparable",
            ltype.scalar_type,
            rtype.scalar_type
        )
    }

    let func = match op {
        ComparisonOp::Lt => BinaryFunc::Lt,
        ComparisonOp::LtEq => BinaryFunc::Lte,
        ComparisonOp::Gt => BinaryFunc::Gt,
        ComparisonOp::GtEq => BinaryFunc::Gte,
        ComparisonOp::Eq => BinaryFunc::Eq,
        ComparisonOp::NotEq => BinaryFunc::NotEq,
    };
    Ok(lexpr.call_binary(rexpr, func))
}

fn plan_like<'a>(
    ecx: &ExprContext,
    left: &'a Expr,
    right: &'a Expr,
    negate: bool,
) -> Result<ScalarExpr, failure::Error> {
    let lexpr = plan_expr(ecx, left, Some(ScalarType::String))?;
    let ltype = ecx.column_type(&lexpr);
    let rexpr = plan_expr(ecx, right, Some(ScalarType::String))?;
    let rtype = ecx.column_type(&rexpr);

    if (ltype.scalar_type != ScalarType::String && ltype.scalar_type != ScalarType::Unknown)
        || (rtype.scalar_type != ScalarType::String && rtype.scalar_type != ScalarType::Unknown)
    {
        bail!(
            "LIKE operator requires two string operators, found: {:?} and {:?}",
            ltype,
            rtype
        );
    }

    let mut expr = ScalarExpr::CallBinary {
        func: BinaryFunc::MatchLikePattern,
        expr1: Box::new(lexpr),
        expr2: Box::new(rexpr),
    };
    if negate {
        expr = ScalarExpr::CallUnary {
            func: UnaryFunc::Not,
            expr: Box::new(expr),
        };
    }
    Ok(expr)
}

fn plan_json_op(
    ecx: &ExprContext,
    op: JsonOp,
    left: &Expr,
    right: &Expr,
) -> Result<ScalarExpr, failure::Error> {
    use JsonOp::*;
    use ScalarType::*;

    let lexpr = plan_expr(ecx, left, Some(ScalarType::String))?;
    let ltype = ecx.column_type(&lexpr);
    let rexpr = plan_expr(ecx, right, Some(ScalarType::String))?;
    let rtype = ecx.column_type(&rexpr);

    Ok(match (op, &ltype.scalar_type, &rtype.scalar_type) {
        (Get, Jsonb, Int32) => lexpr.call_binary(
            rexpr.call_unary(UnaryFunc::CastInt32ToInt64),
            BinaryFunc::JsonbGetInt64,
        ),
        (Get, Jsonb, Int64) => lexpr.call_binary(rexpr, BinaryFunc::JsonbGetInt64),
        (Get, Jsonb, String) => lexpr.call_binary(rexpr, BinaryFunc::JsonbGetString),
        (Get, _, _) => bail!("No overload for {} {} {}", ltype, op, rtype),

        (GetAsText, Jsonb, Int32) => lexpr
            .call_binary(
                rexpr.call_unary(UnaryFunc::CastInt32ToInt64),
                BinaryFunc::JsonbGetInt64,
            )
            .call_unary(UnaryFunc::JsonbStringifyUnlessString),
        (GetAsText, Jsonb, Int64) => lexpr
            .call_binary(rexpr, BinaryFunc::JsonbGetInt64)
            .call_unary(UnaryFunc::JsonbStringifyUnlessString),
        (GetAsText, Jsonb, String) => lexpr
            .call_binary(rexpr, BinaryFunc::JsonbGetString)
            .call_unary(UnaryFunc::JsonbStringifyUnlessString),
        (GetAsText, _, _) => bail!("No overload for {} {} {}", ltype, op, rtype),

        (ContainsField, Jsonb, String) => lexpr.call_binary(rexpr, BinaryFunc::JsonbContainsString),
        (ContainsField, _, _) => bail!("No overload for {} {} {}", ltype, op, rtype),

        (Concat, Jsonb, Jsonb) => lexpr.call_binary(rexpr, BinaryFunc::JsonbConcat),
        (Concat, String, _) | (Concat, _, String) | (Concat, Unknown, Unknown) => {
            // These are philosophically internal casts, but PostgreSQL
            // considers them to be explicit (perhaps for historical reasons),
            // so we do too.
            let lexpr = plan_cast_internal(ecx, CastContext::Explicit, lexpr, String)?;
            let rexpr = plan_cast_internal(ecx, CastContext::Explicit, rexpr, String)?;
            lexpr.call_binary(rexpr, BinaryFunc::TextConcat)
        }
        (Concat, _, _) => bail!("No overload for {} {} {}", ltype, op, rtype),

        (ContainsJson, Jsonb, Jsonb) => lexpr.call_binary(rexpr, BinaryFunc::JsonbContainsJsonb),
        (ContainsJson, Jsonb, String) => lexpr.call_binary(
            rexpr.call_unary(UnaryFunc::CastStringToJsonb),
            BinaryFunc::JsonbContainsJsonb,
        ),
        (ContainsJson, String, Jsonb) => lexpr
            .call_unary(UnaryFunc::CastStringToJsonb)
            .call_binary(rexpr, BinaryFunc::JsonbContainsJsonb),
        (ContainsJson, _, _) => bail!("No overload for {} {} {}", ltype, op, rtype),

        (ContainedInJson, Jsonb, Jsonb) => rexpr.call_binary(lexpr, BinaryFunc::JsonbContainsJsonb),
        (ContainedInJson, String, Jsonb) => rexpr.call_binary(
            lexpr.call_unary(UnaryFunc::CastStringToJsonb),
            BinaryFunc::JsonbContainsJsonb,
        ),
        (ContainedInJson, Jsonb, String) => rexpr
            .call_unary(UnaryFunc::CastStringToJsonb)
            .call_binary(lexpr, BinaryFunc::JsonbContainsJsonb),
        (ContainedInJson, _, _) => bail!("No overload for {} {} {}", ltype, op, rtype),

        // TODO(jamii) these require sql arrays
        (ContainsAnyFields, _, _) => bail!("Unsupported json operator: {}", op),
        (ContainsAllFields, _, _) => bail!("Unsupported json operator: {}", op),

        // TODO(jamii) these require json paths
        (GetPath, _, _) => bail!("Unsupported json operator: {}", op),
        (GetPathAsText, _, _) => bail!("Unsupported json operator: {}", op),
        (DeletePath, _, _) => bail!("Unsupported json operator: {}", op),
        (ContainsPath, _, _) => bail!("Unsupported json operator: {}", op),
        (ApplyPathPredicate, _, _) => bail!("Unsupported json operator: {}", op),
    })
}

fn plan_between<'a>(
    ecx: &ExprContext,
    expr: &'a Expr,
    low: &'a Expr,
    high: &'a Expr,
    negated: bool,
) -> Result<ScalarExpr, failure::Error> {
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
    plan_expr(ecx, &both, None)
}

fn plan_in_list<'a>(
    ecx: &ExprContext,
    expr: &'a Expr,
    list: &'a [Expr],
    negated: bool,
) -> Result<ScalarExpr, failure::Error> {
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
    plan_expr(ecx, &cond, None)
}

fn plan_case<'a>(
    ecx: &ExprContext,
    operand: &'a Option<Box<Expr>>,
    conditions: &'a [Expr],
    results: &'a [Expr],
    else_result: &'a Option<Box<Expr>>,
) -> Result<ScalarExpr, failure::Error> {
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
        let cexpr = plan_expr(ecx, &c, Some(ScalarType::Bool))?;
        let ctype = ecx.column_type(&cexpr);
        if ctype.scalar_type != ScalarType::Bool {
            bail!(
                "CASE expression has non-boolean type {:?}",
                ctype.scalar_type
            );
        }
        cond_exprs.push(cexpr);
        result_exprs.push(r);
    }
    result_exprs.push(match else_result {
        Some(else_result) => else_result,
        None => &Expr::Value(Value::Null),
    });
    let mut result_exprs =
        plan_homogeneous_exprs("CASE", ecx, &result_exprs, Some(ScalarType::String))?;
    let mut expr = result_exprs.pop().unwrap();
    assert_eq!(cond_exprs.len(), result_exprs.len());
    for (cexpr, rexpr) in cond_exprs.into_iter().zip(result_exprs).rev() {
        expr = ScalarExpr::If {
            cond: Box::new(cexpr),
            then: Box::new(rexpr),
            els: Box::new(expr),
        }
    }
    Ok(expr)
}

fn plan_literal<'a>(l: &'a Value) -> Result<ScalarExpr, failure::Error> {
    let (datum, scalar_type) = sql_value_to_datum(l)?;
    let nullable = datum == Datum::Null;
    let typ = ColumnType::new(scalar_type).nullable(nullable);
    let expr = ScalarExpr::literal(datum, typ);
    Ok(expr)
}

fn sql_value_to_datum<'a>(l: &'a Value) -> Result<(Datum<'a>, ScalarType), failure::Error> {
    Ok(match l {
        Value::Number(s) => {
            let d: Decimal = s.parse()?;
            if d.scale() == 0 {
                match d.significand().try_into() {
                    Ok(n) => (Datum::Int32(n), ScalarType::Int32),
                    Err(_) => (
                        Datum::from(d.significand()),
                        ScalarType::Decimal(MAX_DECIMAL_PRECISION, d.scale()),
                    ),
                }
            } else {
                (
                    Datum::from(d.significand()),
                    ScalarType::Decimal(MAX_DECIMAL_PRECISION, d.scale()),
                )
            }
        }
        Value::SingleQuotedString(s) => (Datum::String(s), ScalarType::String),
        Value::HexStringLiteral(_) => bail!("x'' string literals are not supported: {}", l),
        Value::Boolean(b) => match b {
            false => (Datum::False, ScalarType::Bool),
            true => (Datum::True, ScalarType::Bool),
        },
        Value::Date(d) => (Datum::Date(strconv::parse_date(d)?), ScalarType::Date),
        Value::Timestamp(ts) => (
            Datum::Timestamp(strconv::parse_timestamp(ts)?),
            ScalarType::Timestamp,
        ),
        Value::TimestampTz(ts) => (
            Datum::TimestampTz(strconv::parse_timestamptz(ts)?),
            ScalarType::TimestampTz,
        ),
        Value::Time(t) => (Datum::Time(strconv::parse_time(t)?), ScalarType::Time),
        Value::Interval(iv) => {
            let mut i = strconv::parse_interval_w_disambiguator(&iv.value, iv.precision_low)?;
            i.truncate_high_fields(iv.precision_high);
            i.truncate_low_fields(iv.precision_low, iv.fsec_max_precision)?;
            (Datum::Interval(i), ScalarType::Interval)
        }
        Value::Null => (Datum::Null, ScalarType::Unknown),
    })
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum BooleanOp {
    And,
    Or,
}

enum ComparisonOp {
    Lt,
    LtEq,
    Gt,
    GtEq,
    Eq,
    NotEq,
}

impl fmt::Display for ComparisonOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ComparisonOp::Lt => f.write_str("<"),
            ComparisonOp::LtEq => f.write_str("<="),
            ComparisonOp::Gt => f.write_str(">"),
            ComparisonOp::GtEq => f.write_str(">="),
            ComparisonOp::Eq => f.write_str("="),
            ComparisonOp::NotEq => f.write_str("<>"),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum ArithmeticOp {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
}

impl fmt::Display for ArithmeticOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ArithmeticOp::Plus => f.write_str("+"),
            ArithmeticOp::Minus => f.write_str("-"),
            ArithmeticOp::Multiply => f.write_str("*"),
            ArithmeticOp::Divide => f.write_str("/"),
            ArithmeticOp::Modulo => f.write_str("%"),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum JsonOp {
    Get,
    GetAsText,
    GetPath,
    GetPathAsText,
    ContainsJson,
    ContainedInJson,
    ContainsField,
    ContainsAnyFields,
    ContainsAllFields,
    Concat,
    DeletePath,
    ContainsPath,
    ApplyPathPredicate,
}

impl fmt::Display for JsonOp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use JsonOp::*;
        match self {
            Get => f.write_str("->"),
            GetAsText => f.write_str("->>"),
            GetPath => f.write_str("#>"),
            GetPathAsText => f.write_str("#>>"),
            ContainsJson => f.write_str("@>"),
            ContainedInJson => f.write_str("<@"),
            ContainsField => f.write_str("?"),
            ContainsAnyFields => f.write_str("?|"),
            ContainsAllFields => f.write_str("?&"),
            Concat => f.write_str("||"),
            DeletePath => f.write_str("#-"),
            ContainsPath => f.write_str("@?"),
            ApplyPathPredicate => f.write_str("@@"),
        }
    }
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
                if let (
                    Column(ColumnRef {
                        level: 0,
                        column: l,
                    }),
                    Column(ColumnRef {
                        level: 0,
                        column: r,
                    }),
                ) = (&**expr1, &**expr2)
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

fn unnest(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(expr) => unnest(expr),
        _ => expr,
    }
}

fn best_target_type(iter: impl IntoIterator<Item = ScalarType>) -> Option<ScalarType> {
    iter.into_iter()
        .max_by_key(|scalar_type| match scalar_type {
            ScalarType::Unknown => 0,
            ScalarType::Int32 => 1,
            ScalarType::Int64 => 2,
            ScalarType::Decimal(_, _) => 3,
            ScalarType::Float32 => 4,
            ScalarType::Float64 => 5,
            ScalarType::Date => 6,
            ScalarType::Timestamp => 7,
            ScalarType::TimestampTz => 8,
            _ => 9,
        })
}

enum CastContext<'a> {
    Explicit,
    Implicit(&'a str),
}

impl<'a> fmt::Display for CastContext<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CastContext::Explicit => f.write_str("CAST"),
            CastContext::Implicit(s) => write!(f, "{}", s),
        }
    }
}

/// Plans a cast between two `RelationExpr`s of different types. If it is
/// impossible to cast between the two types, an error is returned.
///
/// Note that `plan_cast_internal` only understands [`ScalarType`]s. If you need
/// to cast between SQL [`DataType`]s, see [`Planner::plan_cast`].
fn plan_cast_internal<'a>(
    ecx: &ExprContext<'a>,
    ccx: CastContext<'a>,
    expr: ScalarExpr,
    to_scalar_type: ScalarType,
) -> Result<ScalarExpr, failure::Error> {
    use ScalarType::*;
    use UnaryFunc::*;
    let from_scalar_type = ecx.column_type(&expr).scalar_type;
    let expr = match (from_scalar_type, to_scalar_type.clone()) {
        (Bool, String) => expr.call_unary(match ccx {
            CastContext::Explicit => CastBoolToStringExplicit,
            CastContext::Implicit(_) => CastBoolToStringImplicit,
        }),
        (Int32, Bool) => expr.call_unary(CastInt32ToBool),
        (Int32, Float32) => expr.call_unary(CastInt32ToFloat32),
        (Int32, Float64) => expr.call_unary(CastInt32ToFloat64),
        (Int32, Int64) => expr.call_unary(CastInt32ToInt64),
        (Int32, Decimal(_, s)) => rescale_decimal(expr.call_unary(CastInt32ToDecimal), 0, s),
        (Int32, String) => expr.call_unary(CastInt32ToString),
        (Int64, Bool) => expr.call_unary(CastInt64ToBool),
        (Int64, Decimal(_, s)) => rescale_decimal(expr.call_unary(CastInt64ToDecimal), 0, s),
        (Int64, Float32) => expr.call_unary(CastInt64ToFloat32),
        (Int64, Float64) => expr.call_unary(CastInt64ToFloat64),
        (Int64, Int32) => expr.call_unary(CastInt64ToInt32),
        (Int64, String) => expr.call_unary(CastInt64ToString),
        (Float32, Int64) => expr.call_unary(CastFloat32ToInt64),
        (Float32, Float64) => expr.call_unary(CastFloat32ToFloat64),
        (Float32, Decimal(_, s)) => {
            let s = ScalarExpr::literal(Datum::from(s as i32), ColumnType::new(to_scalar_type));
            expr.call_binary(s, BinaryFunc::CastFloat32ToDecimal)
        }
        (Float32, String) => expr.call_unary(CastFloat32ToString),
        (Float64, Int32) => expr.call_unary(CastFloat64ToInt32),
        (Float64, Int64) => expr.call_unary(CastFloat64ToInt64),
        (Float64, Decimal(_, s)) => {
            let s = ScalarExpr::literal(Datum::from(s as i32), ColumnType::new(to_scalar_type));
            expr.call_binary(s, BinaryFunc::CastFloat64ToDecimal)
        }
        (Float64, String) => expr.call_unary(CastFloat64ToString),
        (Decimal(_, s), Int32) => rescale_decimal(expr, s, 0).call_unary(CastDecimalToInt32),
        (Decimal(_, s), Int64) => rescale_decimal(expr, s, 0).call_unary(CastDecimalToInt64),
        (Decimal(_, s), Float32) => {
            let factor = 10_f32.powi(i32::from(s));
            let factor = ScalarExpr::literal(Datum::from(factor), ColumnType::new(to_scalar_type));
            expr.call_unary(CastSignificandToFloat32)
                .call_binary(factor, BinaryFunc::DivFloat32)
        }
        (Decimal(_, s), Float64) => {
            let factor = 10_f64.powi(i32::from(s));
            let factor = ScalarExpr::literal(Datum::from(factor), ColumnType::new(to_scalar_type));
            expr.call_unary(CastSignificandToFloat64)
                .call_binary(factor, BinaryFunc::DivFloat64)
        }
        (Decimal(_, s1), Decimal(_, s2)) => rescale_decimal(expr, s1, s2),
        (Decimal(_, s), String) => expr.call_unary(UnaryFunc::CastDecimalToString(s)),
        (Date, Timestamp) => expr.call_unary(CastDateToTimestamp),
        (Date, TimestampTz) => expr.call_unary(CastDateToTimestampTz),
        (Date, String) => expr.call_unary(CastDateToString),
        (Time, String) => expr.call_unary(CastTimeToString),
        (Time, Interval) => expr.call_unary(CastTimeToInterval),
        (Timestamp, Date) => expr.call_unary(CastTimestampToDate),
        (Timestamp, TimestampTz) => expr.call_unary(CastTimestampToTimestampTz),
        (Timestamp, String) => expr.call_unary(CastTimestampToString),
        (TimestampTz, Date) => expr.call_unary(CastTimestampTzToDate),
        (TimestampTz, Timestamp) => expr.call_unary(CastTimestampTzToTimestamp),
        (TimestampTz, String) => expr.call_unary(CastTimestampTzToString),
        (Interval, String) => expr.call_unary(CastIntervalToString),
        (Interval, Time) => expr.call_unary(CastIntervalToTime),
        (Bytes, String) => expr.call_unary(CastBytesToString),
        (Jsonb, String) => expr.call_unary(JsonbStringify),
        (Jsonb, Float64) => expr.call_unary(CastJsonbToFloat64),
        (Jsonb, Bool) => expr.call_unary(CastJsonbToBool),
        (Jsonb, Int32) | (Jsonb, Int64) | (Jsonb, Float32) | (Jsonb, Decimal(..)) => {
            plan_cast_internal(
                ecx,
                ccx,
                expr.call_unary(CastJsonbToFloat64),
                to_scalar_type,
            )?
        }
        (Jsonb, Date) | (Jsonb, Timestamp) | (Jsonb, TimestampTz) | (Jsonb, Interval) => {
            plan_cast_internal(ecx, ccx, expr.call_unary(CastJsonbToString), to_scalar_type)?
        }
        (String, Bool) => expr.call_unary(CastStringToBool),
        (String, Int32) => expr.call_unary(CastStringToInt32),
        (String, Int64) => expr.call_unary(CastStringToInt64),
        (String, Float32) => expr.call_unary(CastStringToFloat32),
        (String, Float64) => expr.call_unary(CastStringToFloat64),
        (String, Decimal(_, s)) => expr.call_unary(CastStringToDecimal(s)),
        (String, Date) => expr.call_unary(CastStringToDate),
        (String, Time) => expr.call_unary(CastStringToTime),
        (String, Timestamp) => expr.call_unary(CastStringToTimestamp),
        (String, TimestampTz) => expr.call_unary(CastStringToTimestampTz),
        (String, Interval) => expr.call_unary(CastStringToInterval),
        (String, Bytes) => expr.call_unary(CastStringToBytes),
        (String, Jsonb) => expr.call_unary(CastStringToJsonb),
        (Unknown, _) => {
            ScalarExpr::literal(Datum::Null, ColumnType::new(to_scalar_type).nullable(true))
        }
        (from, to) if from == to => expr,
        (from, to) => {
            bail!(
                "{} does not support casting from {:?} to {:?}",
                ccx,
                from,
                to
            );
        }
    };
    Ok(expr)
}

fn promote_int_decimal<'a>(
    ecx: &ExprContext<'a>,
    name: &str,
    expr: ScalarExpr,
) -> Result<(ScalarExpr, u8), failure::Error> {
    match ecx.column_type(&expr).scalar_type {
        ScalarType::Decimal(_, s) => Ok((expr, s)),
        ScalarType::Unknown | ScalarType::Int32 | ScalarType::Int64 => {
            let scale = 0;
            let expr = plan_cast_internal(
                ecx,
                CastContext::Implicit(name),
                expr,
                ScalarType::Decimal(0, scale),
            )?;
            Ok((expr, scale))
        }
        other => bail!("{} has non-integer type {:?}", name, other),
    }
}

fn promote_number_floatdec<'a>(
    ecx: &ExprContext<'a>,
    name: &str,
    expr: ScalarExpr,
) -> Result<ScalarExpr, failure::Error> {
    Ok(match ecx.column_type(&expr).scalar_type {
        ScalarType::Float32 | ScalarType::Float64 | ScalarType::Decimal(_, _) => expr,
        ScalarType::Unknown | ScalarType::Int32 | ScalarType::Int64 => {
            plan_cast_internal(ecx, CastContext::Implicit(name), expr, ScalarType::Float64)?
        }
        other => bail!("{} has non-numeric type {:?}", name, other),
    })
}

fn promote_number_float64<'a>(
    ecx: &ExprContext<'a>,
    name: &str,
    expr: ScalarExpr,
) -> Result<ScalarExpr, failure::Error> {
    Ok(match ecx.column_type(&expr).scalar_type {
        ScalarType::Float64 => expr,
        ScalarType::Unknown
        | ScalarType::Int32
        | ScalarType::Int64
        | ScalarType::Decimal(_, _)
        | ScalarType::Float32 => {
            plan_cast_internal(ecx, CastContext::Implicit(name), expr, ScalarType::Float64)?
        }
        other => bail!("{} has non-numeric type {:?}", name, other),
    })
}

fn promote_int_int64<'a>(
    ecx: &ExprContext<'a>,
    name: &str,
    expr: ScalarExpr,
) -> Result<ScalarExpr, failure::Error> {
    Ok(match ecx.column_type(&expr).scalar_type {
        ScalarType::Int64 => expr,
        ScalarType::Unknown | ScalarType::Int32 => {
            plan_cast_internal(ecx, CastContext::Implicit(name), expr, ScalarType::Int64)?
        }
        other => bail!("{} has non-integer type {:?}", name, other,),
    })
}

fn promote_decimal_float64<'a>(
    ecx: &ExprContext<'a>,
    name: &str,
    expr: ScalarExpr,
) -> Result<ScalarExpr, failure::Error> {
    Ok(match ecx.column_type(&expr).scalar_type {
        ScalarType::Unknown | ScalarType::Float64 => expr,
        ScalarType::Float32 | ScalarType::Decimal(_, _) => {
            plan_cast_internal(ecx, CastContext::Implicit(name), expr, ScalarType::Float64)?
        }
        other => bail!("{} has non-decimal type {:?}", name, other,),
    })
}

fn rescale_decimal(expr: ScalarExpr, s1: u8, s2: u8) -> ScalarExpr {
    match s1.cmp(&s2) {
        Ordering::Less => {
            let typ = ColumnType::new(ScalarType::Decimal(38, s2 - s1));
            let factor = 10_i128.pow(u32::from(s2 - s1));
            let factor = ScalarExpr::literal(Datum::from(factor), typ);
            expr.call_binary(factor, BinaryFunc::MulDecimal)
        }
        Ordering::Equal => expr,
        Ordering::Greater => {
            let typ = ColumnType::new(ScalarType::Decimal(38, s1 - s2));
            let factor = 10_i128.pow(u32::from(s1 - s2));
            let factor = ScalarExpr::literal(Datum::from(factor), typ);
            expr.call_binary(factor, BinaryFunc::DivDecimal)
        }
    }
}

pub fn scalar_type_from_sql(data_type: &DataType) -> Result<ScalarType, failure::Error> {
    // NOTE this needs to stay in sync with symbiosis::push_column
    Ok(match data_type {
        DataType::Boolean => ScalarType::Bool,
        DataType::Char(_) | DataType::Varchar(_) | DataType::Text => ScalarType::String,
        DataType::SmallInt | DataType::Int => ScalarType::Int32,
        DataType::BigInt => ScalarType::Int64,
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
        DataType::Time => ScalarType::Time,
        DataType::Timestamp => ScalarType::Timestamp,
        DataType::TimestampTz => ScalarType::TimestampTz,
        DataType::Interval => ScalarType::Interval,
        DataType::Bytea => ScalarType::Bytes,
        DataType::Jsonb => ScalarType::Jsonb,
        DataType::List(elem_type) => ScalarType::List(Box::new(scalar_type_from_sql(elem_type)?)),
        other @ DataType::Binary(..)
        | other @ DataType::Blob(_)
        | other @ DataType::Clob(_)
        | other @ DataType::Regclass
        | other @ DataType::TimeTz
        | other @ DataType::Uuid
        | other @ DataType::Varbinary(_) => bail!("Unexpected SQL type: {:?}", other),
    })
}

/// This is used to collect aggregates from within an `Expr`.
/// See the explanation of aggregate handling at the top of the file for more details.
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
            None => {
                // dedup aggs while preserving the order
                // (we don't care what the order is, but it has to be reproducible so that EXPLAIN PLAN tests work)
                let mut seen = HashSet::new();
                Ok(self
                    .aggs
                    .into_iter()
                    .filter(move |agg| seen.insert(&**agg))
                    .collect())
            }
        }
    }
}

impl<'ast> Visit<'ast> for AggregateFuncVisitor<'ast> {
    fn visit_function(&mut self, func: &'ast Function) {
        let old_within_aggregate = self.within_aggregate;
        if let Ok(name) = normalize::function_name(func.name.clone()) {
            if is_aggregate_func(&name) {
                if self.within_aggregate {
                    self.err = Some(format_err!("nested aggregate functions are not allowed"));
                    return;
                }
                self.aggs.push(func);
                self.within_aggregate = true;
            }
        }
        visit::visit_function(self, func);
        self.within_aggregate = old_within_aggregate;
    }

    fn visit_subquery(&mut self, _subquery: &'ast Query) {
        // don't go into subqueries
    }
}

/// Specifies how long a query will live. This impacts whether the query is
/// allowed to reason about the time at which it is running, e.g., by calling
/// the `now()` function.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum QueryLifetime {
    /// The query's result will be computed at one point in time.
    OneShot,
    /// The query's result will be maintained indefinitely.
    Static,
}

/// The state required when planning a `Query`.
#[derive(Debug)]
struct QueryContext<'a> {
    /// The context for the containing `Statement`.
    scx: &'a StatementContext<'a>,
    /// The lifetime that the planned query will have.
    lifetime: QueryLifetime,
    /// The scope of the outer relation expression.
    outer_scope: Scope,
    /// The type of the outer relation expressions.
    outer_relation_types: Vec<RelationType>,
    /// The types of the parameters in the query. This is filled in as planning
    /// occurs.
    param_types: Rc<RefCell<BTreeMap<usize, ScalarType>>>,
}

impl<'a> QueryContext<'a> {
    fn root(scx: &'a StatementContext, lifetime: QueryLifetime) -> QueryContext<'a> {
        QueryContext {
            scx,
            lifetime,
            outer_scope: Scope::empty(None),
            outer_relation_types: vec![],
            param_types: Rc::new(RefCell::new(BTreeMap::new())),
        }
    }

    fn unwrap_param_types(self) -> BTreeMap<usize, ScalarType> {
        Rc::try_unwrap(self.param_types).unwrap().into_inner()
    }

    fn relation_type(&self, expr: &RelationExpr) -> RelationType {
        expr.typ(&self.outer_relation_types, &self.param_types.borrow())
    }
}

/// A bundle of unrelated things that we need for planning `Expr`s.
#[derive(Debug, Clone)]
struct ExprContext<'a> {
    qcx: &'a QueryContext<'a>,
    /// The name of this kind of expression eg "WHERE clause". Used only for error messages.
    name: &'static str,
    /// The context for the `Query` that contains this `Expr`.
    /// The current scope.
    scope: &'a Scope,
    /// The type of the current relation expression upon which this scalar
    /// expression will be evaluated.
    relation_type: &'a RelationType,
    /// Are aggregate functions allowed in this context
    allow_aggregates: bool,
    /// Are subqueries allowed in this context
    allow_subqueries: bool,
}

impl<'a> ExprContext<'a> {
    fn with_name(&self, name: &'static str) -> ExprContext<'a> {
        let mut ecx = self.clone();
        ecx.name = name;
        ecx
    }

    fn column_type(&self, expr: &ScalarExpr) -> ColumnType {
        expr.typ(
            &self.qcx.outer_relation_types,
            &self.relation_type,
            &self.qcx.param_types.borrow(),
        )
    }

    fn scalar_type(&self, expr: &ScalarExpr) -> ScalarType {
        self.column_type(expr).scalar_type
    }

    fn derived_query_context(&self) -> QueryContext {
        QueryContext {
            scx: self.qcx.scx,
            lifetime: self.qcx.lifetime,
            outer_scope: self.scope.clone(),
            outer_relation_types: self
                .qcx
                .outer_relation_types
                .iter()
                .chain(std::iter::once(self.relation_type))
                .cloned()
                .collect(),
            param_types: self.qcx.param_types.clone(),
        }
    }
}

fn is_table_func(name: &str) -> bool {
    match name {
        "jsonb_each"
        | "jsonb_object_keys"
        | "jsonb_array_elements"
        | "jsonb_each_text"
        | "jsonb_array_elements_text"
        | "regexp_extract"
        | "csv_extract" => true,
        _ => false,
    }
}

fn is_aggregate_func(name: &str) -> bool {
    match name {
        // avg is handled by transform::AvgFuncRewriter.
        "max" | "min" | "sum" | "count" | "jsonb_agg" => true,
        _ => false,
    }
}

fn find_agg_func(name: &str, scalar_type: ScalarType) -> Result<AggregateFunc, failure::Error> {
    Ok(match (name, scalar_type) {
        ("max", ScalarType::Int32) => AggregateFunc::MaxInt32,
        ("max", ScalarType::Int64) => AggregateFunc::MaxInt64,
        ("max", ScalarType::Float32) => AggregateFunc::MaxFloat32,
        ("max", ScalarType::Float64) => AggregateFunc::MaxFloat64,
        ("max", ScalarType::Decimal(_, _)) => AggregateFunc::MaxDecimal,
        ("max", ScalarType::Bool) => AggregateFunc::MaxBool,
        ("max", ScalarType::String) => AggregateFunc::MaxString,
        ("max", ScalarType::Date) => AggregateFunc::MaxDate,
        ("max", ScalarType::Timestamp) => AggregateFunc::MaxTimestamp,
        ("max", ScalarType::TimestampTz) => AggregateFunc::MaxTimestampTz,
        ("max", ScalarType::Unknown) => AggregateFunc::MaxNull,
        ("min", ScalarType::Int32) => AggregateFunc::MinInt32,
        ("min", ScalarType::Int64) => AggregateFunc::MinInt64,
        ("min", ScalarType::Float32) => AggregateFunc::MinFloat32,
        ("min", ScalarType::Float64) => AggregateFunc::MinFloat64,
        ("min", ScalarType::Decimal(_, _)) => AggregateFunc::MinDecimal,
        ("min", ScalarType::Bool) => AggregateFunc::MinBool,
        ("min", ScalarType::String) => AggregateFunc::MinString,
        ("min", ScalarType::Date) => AggregateFunc::MinDate,
        ("min", ScalarType::Timestamp) => AggregateFunc::MinTimestamp,
        ("min", ScalarType::TimestampTz) => AggregateFunc::MinTimestampTz,
        ("min", ScalarType::Unknown) => AggregateFunc::MinNull,
        ("sum", ScalarType::Int32) => AggregateFunc::SumInt32,
        ("sum", ScalarType::Int64) => AggregateFunc::SumInt64,
        ("sum", ScalarType::Float32) => AggregateFunc::SumFloat32,
        ("sum", ScalarType::Float64) => AggregateFunc::SumFloat64,
        ("sum", ScalarType::Decimal(_, _)) => AggregateFunc::SumDecimal,
        ("sum", ScalarType::Unknown) => AggregateFunc::SumNull,
        ("count", _) => AggregateFunc::Count,
        ("jsonb_agg", _) => AggregateFunc::JsonbAgg,
        other => bail!("Unimplemented function/type combo: {:?}", other),
    })
}
