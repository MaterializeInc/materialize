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

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;
use std::iter;
use std::mem;
use std::rc::Rc;

use anyhow::{anyhow, bail, ensure, Context};
use sql_parser::ast::visit::{self, Visit};
use sql_parser::ast::{
    BinaryOperator, DataType, Expr, Function, FunctionArgs, Ident, JoinConstraint, JoinOperator,
    ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr, SetOperator, ShowStatementFilter,
    TableAlias, TableFactor, TableWithJoins, Value, Values,
};

use ::expr::{Id, RowSetFinishing};
use dataflow_types::Timestamp;
use repr::adt::decimal::{Decimal, MAX_DECIMAL_PRECISION};
use repr::{
    strconv, ColumnName, ColumnType, Datum, RelationDesc, RelationType, RowArena, ScalarType,
};

use crate::names::PartialName;
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, CoercibleScalarExpr, ColumnOrder, ColumnRef,
    JoinKind, RelationExpr, ScalarExpr, ScalarTypeable, UnaryFunc, VariadicFunc,
};
use crate::plan::func;
use crate::plan::scope::{Scope, ScopeItem, ScopeItemName};
use crate::plan::statement::StatementContext;
use crate::plan::transform_ast;
use crate::plan::typeconv::{self, CastTo, CoerceTo};

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
) -> Result<(RelationExpr, RelationDesc, RowSetFinishing, Vec<ScalarType>), anyhow::Error> {
    transform_ast::transform_query(&mut query)?;
    let qcx = QueryContext::root(scx, lifetime);
    let (mut expr, scope, mut finishing) = plan_query(&qcx, &query)?;

    // Check if the `finishing.order_by` can be applied after
    // `finishing.project`, and push `finishing.project` onto `expr` if so. This
    // allows data to be projected down on the workers rather than the
    // coordinator. It also improves the optimizer's demand analysis, as the
    // optimizer can only reason about demand information in `expr` (i.e., it
    // can't see `finishing.project`).
    let mut unproject = vec![None; expr.arity()];
    for (out_i, in_i) in finishing.project.iter().copied().enumerate() {
        unproject[in_i] = Some(out_i);
    }
    if finishing
        .order_by
        .iter()
        .all(|ob| unproject[ob.column].is_some())
    {
        let trivial_project = (0..finishing.project.len()).collect();
        expr = expr.project(mem::replace(&mut finishing.project, trivial_project));
        for ob in &mut finishing.order_by {
            ob.column = unproject[ob.column].unwrap();
        }
    }

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
) -> Result<(RelationExpr, RowSetFinishing), anyhow::Error> {
    let names: Vec<Option<String>> = desc
        .iter_names()
        .map(|name| name.map(|x| x.as_str().into()))
        .collect();

    let num_cols = names.len();
    let mut row_expr = RelationExpr::constant(rows, desc.typ().clone());

    if let Some(f) = filter {
        let mut predicate = match &f {
            ShowStatementFilter::Like(s) => Expr::BinaryOp {
                left: Box::new(Expr::Identifier(vec![Ident::new(
                    names[0].clone().unwrap(),
                )])),
                op: BinaryOperator::Like,
                right: Box::new(Expr::Value(Value::String(s.into()))),
            },
            ShowStatementFilter::Where(selection) => selection.clone(),
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
        transform_ast::transform_expr(&mut predicate)?;
        let expr = plan_expr(&ecx, &predicate)?.type_as(&ecx, ScalarType::Bool)?;
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
pub fn eval_as_of<'a>(
    scx: &'a StatementContext,
    mut expr: Expr,
) -> Result<Timestamp, anyhow::Error> {
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

    transform_ast::transform_expr(&mut expr)?;
    let ex = plan_expr(ecx, &expr)?
        .type_as_any(ecx)?
        .lower_uncorrelated()?;
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
    exprs: Vec<Expr>,
) -> Result<Vec<::expr::ScalarExpr>, anyhow::Error> {
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
    for mut expr in exprs {
        transform_ast::transform_expr(&mut expr)?;
        let expr = plan_expr_or_col_index(ecx, &expr)?;
        out.push(expr.lower_uncorrelated()?);
    }
    Ok(out)
}

fn plan_expr_or_col_index(ecx: &ExprContext, e: &Expr) -> Result<ScalarExpr, anyhow::Error> {
    match check_col_index(&ecx.name, e, ecx.relation_type.column_types.len())? {
        Some(column) => Ok(ScalarExpr::Column(ColumnRef { level: 0, column })),
        _ => plan_expr(ecx, e)?.type_as_any(ecx),
    }
}

fn check_col_index(name: &str, e: &Expr, max: usize) -> Result<Option<usize>, anyhow::Error> {
    match e {
        Expr::Value(Value::Number(n)) => {
            let n = n
                .parse::<usize>()
                .with_context(|| anyhow!("unable to parse column reference in {}: {}", name, n))?;
            if n < 1 || n > max {
                bail!(
                    "column reference {} in {} is out of range (1 - {})",
                    n,
                    name,
                    max
                );
            }
            Ok(Some(n - 1))
        }
        _ => Ok(None),
    }
}

fn plan_query(
    qcx: &QueryContext,
    q: &Query,
) -> Result<(RelationExpr, Scope, RowSetFinishing), anyhow::Error> {
    if !q.ctes.is_empty() {
        unsupported!(2617, "CTEs");
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
    match &q.body {
        SetExpr::Select(s) if !s.distinct => {
            let plan = plan_view_select_intrusive(
                qcx,
                &s.projection,
                &s.from,
                s.selection.as_ref(),
                &s.group_by,
                s.having.as_ref(),
                &q.order_by,
            )?;
            let finishing = RowSetFinishing {
                order_by: plan.order_by,
                project: plan.project,
                limit,
                offset,
            };
            Ok((plan.expr, plan.scope, finishing))
        }
        _ => {
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
                let expr = plan_expr_or_col_index(ecx, &obe.expr)?;
                // If the expression is a reference to an existing column,
                // do not introduce a new column to support it.
                let column = if let ScalarExpr::Column(ColumnRef { level: 0, column }) = expr {
                    column
                } else {
                    map_exprs.push(expr);
                    output_typ.column_types.len() + map_exprs.len() - 1
                };
                order_by.push(ColumnOrder {
                    column,
                    desc: !obe.asc.unwrap_or(true),
                });
            }
            let finishing = RowSetFinishing {
                order_by,
                limit,
                project: (0..output_typ.column_types.len()).collect(),
                offset,
            };
            Ok((expr.map(map_exprs), scope, finishing))
        }
    }
}

fn plan_subquery(qcx: &QueryContext, q: &Query) -> Result<(RelationExpr, Scope), anyhow::Error> {
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
    Ok((expr.project(finishing.project), scope))
}

fn plan_set_expr(qcx: &QueryContext, q: &SetExpr) -> Result<(RelationExpr, Scope), anyhow::Error> {
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

            let ncols = values[0].len();
            let nrows = values.len();

            // Arrange input expressions by columns, not rows, so that we can
            // call `plan_homogeneous_exprs` on each column.
            let mut cols = vec![vec![]; ncols];
            for row in values {
                if row.len() != ncols {
                    bail!(
                        "VALUES expression has varying number of columns: {} vs {}",
                        row.len(),
                        ncols
                    );
                }
                for (i, v) in row.iter().enumerate() {
                    cols[i].push(v);
                }
            }

            // Plan each column.
            let mut col_iters = Vec::with_capacity(ncols);
            let mut col_types = Vec::with_capacity(ncols);
            for col in cols {
                let col = plan_homogeneous_exprs("VALUES", ecx, &col)?;
                col_types.push(ecx.column_type(&col[0]));
                col_iters.push(col.into_iter());
            }

            // Build constant relation.
            let typ = RelationType::new(col_types);
            let mut out = RelationExpr::constant(vec![], typ);
            for _ in 0..nrows {
                let row: Vec<_> = (0..ncols).map(|i| col_iters[i].next().unwrap()).collect();
                let empty = RelationExpr::constant(vec![vec![]], RelationType::new(vec![]));
                out = out.union(empty.map(row));
            }

            // Build column names.
            let mut scope = Scope::empty(Some(qcx.outer_scope.clone()));
            for i in 0..ncols {
                let name = Some(format!("column{}", i + 1).into());
                scope.items.push(ScopeItem::from_column_name(name));
            }

            Ok((out, scope))
        }
        SetExpr::Query(query) => {
            let (expr, scope) = plan_subquery(qcx, query)?;
            Ok((expr, scope))
        }
    }
}

fn plan_join_identity(qcx: &QueryContext) -> (RelationExpr, Scope) {
    let typ = RelationType::new(vec![]);
    let expr = RelationExpr::constant(vec![vec![]], typ);
    let scope = Scope::from_source(
        None,
        iter::empty::<Option<ColumnName>>(),
        Some(qcx.outer_scope.clone()),
    );
    (expr, scope)
}

/// Describes how to execute a SELECT query.
///
/// `order_by` describes how to order the rows in `expr` *before* applying the
/// projection. The `scope` describes the columns in `expr` *after* the
/// projection has been applied.
#[derive(Debug)]
struct SelectPlan {
    expr: RelationExpr,
    scope: Scope,
    order_by: Vec<ColumnOrder>,
    project: Vec<usize>,
}

/// Plans a SELECT query with an intrusive ORDER BY clause.
///
/// Normally, the ORDER BY clause occurs after the columns specified in the
/// SELECT list have been projected. In a query like
///
///   CREATE TABLE (a int, b int)
///   (SELECT a FROM t) UNION (SELECT a FROM t) ORDER BY a
///
/// it is valid to refer to `a`, because it is explicitly selected, but it would
/// not be valid to refer to unselected column `b`.
///
/// But PostgreSQL extends the standard to permit queries like
///
///   SELECT a FROM t ORDER BY b
///
/// where expressions in the ORDER BY clause can refer to *both* input columns
/// and output columns.
///
/// This function handles queries of the latter class. For queries of the
/// former class, see `plan_view_select`.
fn plan_view_select_intrusive(
    qcx: &QueryContext,
    projection: &[SelectItem],
    from: &[TableWithJoins],
    selection: Option<&Expr>,
    group_by: &[Expr],
    having: Option<&Expr>,
    order_by_exprs: &[OrderByExpr],
) -> Result<SelectPlan, anyhow::Error> {
    // Step 1. Handle FROM clause, including joins.
    let (mut relation_expr, from_scope) =
        from.iter().fold(Ok(plan_join_identity(qcx)), |l, twj| {
            let (left, left_scope) = l?;
            plan_table_with_joins(qcx, left, left_scope, &JoinOperator::CrossJoin, twj)
        })?;

    // Step 2. Handle WHERE clause.
    if let Some(selection) = &selection {
        let ecx = &ExprContext {
            qcx,
            name: "WHERE clause",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: false,
            allow_subqueries: true,
        };
        let expr = plan_expr(ecx, &selection)?.type_as(ecx, ScalarType::Bool)?;
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 3. Gather aggregates.
    let aggregates = {
        let mut aggregate_visitor = AggregateFuncVisitor::new();
        for p in projection {
            aggregate_visitor.visit_select_item(p);
        }
        for o in order_by_exprs {
            aggregate_visitor.visit_order_by_expr(o);
        }
        if let Some(having) = having {
            aggregate_visitor.visit_expr(having);
        }
        aggregate_visitor.into_result()?
    };

    // Step 4. Expand SELECT clause.
    let projection = {
        let ecx = &ExprContext {
            qcx,
            name: "SELECT clause",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: true,
            allow_subqueries: true,
        };
        let mut out = vec![];
        for si in projection {
            if *si == SelectItem::Wildcard && from.is_empty() {
                bail!("SELECT * with no tables specified is not valid");
            }
            out.extend(expand_select_item(&ecx, si)?);
        }
        out
    };

    // Step 5. Handle GROUP BY clause.
    let (group_scope, select_all_mapping) = {
        // Compute GROUP BY expressions.
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
        for group_expr in group_by {
            let expr = plan_group_by_expr(ecx, group_expr, &projection)?;
            let new_column = group_key.len();
            // Repeated expressions in GROUP BY confuse name resolution later,
            // and dropping them doesn't change the result.
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
                    // If we later have `SELECT foo.*` then we have to find all
                    // the `foo` items in `from_scope` and figure out where they
                    // ended up in `group_scope`. This is really hard to do
                    // right using SQL name resolution, so instead we just track
                    // the movement here.
                    select_all_mapping.insert(*old_column, new_column);
                    let mut scope_item = ecx.scope.items[*old_column].clone();
                    scope_item.expr = Some(group_expr.clone());
                    scope_item
                } else {
                    ScopeItem {
                        names: invent_column_name(group_expr).into_iter().collect(),
                        expr: Some(group_expr.clone()),
                        nameable: true,
                    }
                };

                group_key.push(from_scope.len() + group_exprs.len());
                group_exprs.push(expr);
                group_scope.items.push(scope_item);
            }
        }

        // Plan aggregates.
        let ecx = &ExprContext {
            qcx,
            name: "aggregate function",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr.clone().map(group_exprs.clone())),
            allow_aggregates: false,
            allow_subqueries: true,
        };
        let mut agg_exprs = vec![];
        for sql_function in aggregates {
            agg_exprs.push(plan_aggregate(ecx, sql_function)?);
            group_scope.items.push(ScopeItem {
                names: vec![ScopeItemName {
                    table_name: None,
                    column_name: Some(sql_function.name.to_string().into()),
                    priority: false,
                }],
                expr: Some(Expr::Function(sql_function.clone())),
                nameable: true,
            });
        }
        if !agg_exprs.is_empty() || !group_key.is_empty() || having.is_some() {
            // apply GROUP BY / aggregates
            relation_expr = relation_expr.map(group_exprs).reduce(group_key, agg_exprs);
            (group_scope, select_all_mapping)
        } else {
            // if no GROUP BY, aggregates or having then all columns remain in scope
            (
                from_scope.clone(),
                (0..from_scope.len()).map(|i| (i, i)).collect(),
            )
        }
    };

    // Step 6. Handle HAVING clause.
    if let Some(having) = having {
        let ecx = &ExprContext {
            qcx,
            name: "HAVING clause",
            scope: &group_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: true,
            allow_subqueries: true,
        };
        let expr = plan_expr(ecx, having)?.type_as(ecx, ScalarType::Bool)?;
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 7. Handle SELECT clause.
    let (project_key, map_scope) = {
        let mut new_exprs = vec![];
        let mut project_key = vec![];
        let mut map_scope = group_scope.clone();
        let ecx = &ExprContext {
            qcx,
            name: "SELECT clause",
            scope: &group_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: true,
            allow_subqueries: true,
        };
        for (select_item, column_name) in projection {
            let expr = match select_item {
                ExpandedSelectItem::InputOrdinal(i) => {
                    if let Some(column) = select_all_mapping.get(&i).copied() {
                        ScalarExpr::Column(ColumnRef { level: 0, column })
                    } else {
                        bail!("column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function", from_scope.items[i].short_display_name());
                    }
                }
                ExpandedSelectItem::Expr(expr) => plan_expr(ecx, &expr)?.type_as_any(ecx)?,
            };
            if let ScalarExpr::Column(ColumnRef { level: 0, column }) = expr {
                project_key.push(column);
                // Mark the output name as prioritized, so that they shadow any
                // input columns of the same name.
                if let Some(column_name) = column_name {
                    map_scope.items[column].names.insert(
                        0,
                        ScopeItemName {
                            table_name: None,
                            column_name: Some(column_name),
                            priority: true,
                        },
                    );
                }
            } else {
                project_key.push(group_scope.len() + new_exprs.len());
                new_exprs.push(expr);
                map_scope.items.push(ScopeItem {
                    names: column_name
                        .into_iter()
                        .map(|column_name| ScopeItemName {
                            table_name: None,
                            column_name: Some(column_name),
                            priority: true,
                        })
                        .collect(),
                    expr: None,
                    nameable: true,
                });
            }
        }
        relation_expr = relation_expr.map(new_exprs);
        (project_key, map_scope)
    };

    // Step 8. Handle intrusive ORDER BY.
    let order_by = {
        let mut order_by = vec![];
        let mut new_exprs = vec![];
        for obe in order_by_exprs {
            let ecx = &ExprContext {
                qcx,
                name: "ORDER BY clause",
                scope: &map_scope,
                relation_type: &qcx.relation_type(&relation_expr),
                allow_aggregates: true,
                allow_subqueries: true,
            };

            let expr = match check_col_index(&ecx.name, &obe.expr, project_key.len())? {
                Some(i) => ScalarExpr::Column(ColumnRef {
                    level: 0,
                    column: project_key[i],
                }),
                None => plan_expr(ecx, &obe.expr)?.type_as_any(ecx)?,
            };

            // If the expression is a reference to an existing column, do not
            // introduce a new column to support it.
            let column = if let ScalarExpr::Column(ColumnRef { level: 0, column }) = expr {
                column
            } else {
                new_exprs.push(expr);
                map_scope.len() + new_exprs.len() - 1
            };
            order_by.push(ColumnOrder {
                column,
                desc: !obe.asc.unwrap_or(true),
            });
        }
        relation_expr = relation_expr.map(new_exprs);
        order_by
    };

    Ok(SelectPlan {
        expr: relation_expr,
        scope: map_scope.project(&project_key).scrub(),
        order_by,
        project: project_key,
    })
}

/// Plans an expression in a `GROUP BY` clause.
///
/// For historical reasons, PostgreSQL allows `GROUP BY` expressions to refer to
/// names/expressions defined in the `SELECT` clause. These special cases are
/// handled by this function; see comments within the implementation for
/// details.
fn plan_group_by_expr(
    ecx: &ExprContext,
    group_expr: &Expr,
    projection: &[(ExpandedSelectItem, Option<ColumnName>)],
) -> Result<ScalarExpr, anyhow::Error> {
    let plan_projection = |column: usize| match &projection[column].0 {
        ExpandedSelectItem::InputOrdinal(column) => Ok(ScalarExpr::Column(ColumnRef {
            level: 0,
            column: *column,
        })),
        ExpandedSelectItem::Expr(expr) => Ok(plan_expr(&ecx, expr)?.type_as_any(ecx)?),
    };

    // Check if the expression is a numeric literal, as in `GROUP BY 1`. This is
    // a special case that means to use the ith item in the SELECT clause.
    if let Some(column) = check_col_index(&ecx.name, group_expr, projection.len())? {
        return plan_projection(column);
    }

    // Check if the expression is a simple identifier, as in `GROUP BY foo`.
    // The `foo` can refer to *either* an input column or an output column. If
    // both exist, the input column is preferred.
    match group_expr {
        Expr::Identifier(names) if names.len() == 1 => match plan_identifier(ecx, names) {
            Err(e @ PlanError::UnknownColumn(_)) => {
                // The expression was a simple identifier that did not match an
                // input column. See if it matches an output column.
                let name = Some(normalize::column_name(names[0].clone()));
                let mut iter = projection.iter().map(|(_expr, name)| name);
                if let Some(column) = iter.position(|n| *n == name) {
                    if iter.any(|n| *n == name) {
                        Err(PlanError::AmbiguousColumn(names[0].to_string()).into())
                    } else {
                        plan_projection(column)
                    }
                } else {
                    // The name didn't match an output column either. Return the
                    // "unknown column" error.
                    Err(e.into())
                }
            }
            res => Ok(res?),
        },
        _ => plan_expr(&ecx, group_expr)?.type_as_any(ecx),
    }
}

fn plan_view_select(
    qcx: &QueryContext,
    s: &Select,
) -> Result<(RelationExpr, Scope), anyhow::Error> {
    let order_by_exprs = &[];
    let plan = plan_view_select_intrusive(
        qcx,
        &s.projection,
        &s.from,
        s.selection.as_ref(),
        &s.group_by,
        s.having.as_ref(),
        order_by_exprs,
    )?;
    // We didn't provide any `order_by_exprs`, so `plan_view_select_intrusive`
    // should not have planned any ordering.
    assert!(plan.order_by.is_empty());

    let mut expr = plan.expr.project(plan.project);
    if s.distinct {
        expr = expr.distinct();
    }
    Ok((expr, plan.scope))
}

fn plan_table_with_joins<'a>(
    qcx: &QueryContext,
    left: RelationExpr,
    left_scope: Scope,
    join_operator: &JoinOperator,
    table_with_joins: &'a TableWithJoins,
) -> Result<(RelationExpr, Scope), anyhow::Error> {
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
) -> Result<(RelationExpr, Scope), anyhow::Error> {
    match table_factor {
        TableFactor::Table { name, alias, args } => {
            if let Some(args) = args {
                let ecx = &ExprContext {
                    qcx,
                    name: "FROM table function",
                    scope: &left_scope,
                    relation_type: &qcx.relation_type(&left),
                    allow_aggregates: false,
                    allow_subqueries: true,
                };
                plan_table_function(ecx, left, &name, alias.as_ref(), args)
            } else {
                let name = qcx.scx.resolve_item(name.clone())?;
                let item = qcx.scx.catalog.get_item(&name);
                let expr = RelationExpr::Get {
                    id: Id::Global(item.id()),
                    typ: item.desc()?.typ().clone(),
                };
                let column_names = item.desc()?.iter_names().map(|n| n.cloned()).collect();
                let scope = plan_table_alias(qcx, alias.as_ref(), Some(name.into()), column_names)?;
                plan_join_operator(qcx, &join_operator, left, left_scope, expr, scope)
            }
        }
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            if *lateral {
                unsupported!(3111, "LATERAL derived tables");
            }
            let (expr, scope) = plan_subquery(&qcx, &subquery)?;
            let table_name = None;
            let column_names = scope.column_names().map(|n| n.cloned()).collect();
            let scope = plan_table_alias(qcx, alias.as_ref(), table_name, column_names)?;
            plan_join_operator(qcx, &join_operator, left, left_scope, expr, scope)
        }
        TableFactor::NestedJoin(table_with_joins) => {
            let (identity, identity_scope) = plan_join_identity(qcx);
            let (expr, scope) = plan_table_with_joins(
                qcx,
                identity,
                identity_scope,
                &JoinOperator::CrossJoin,
                table_with_joins,
            )?;
            plan_join_operator(qcx, &join_operator, left, left_scope, expr, scope)
        }
    }
}

fn plan_table_function(
    ecx: &ExprContext,
    left: RelationExpr,
    name: &ObjectName,
    alias: Option<&TableAlias>,
    args: &FunctionArgs,
) -> Result<(RelationExpr, Scope), anyhow::Error> {
    let ident = normalize::function_name(name.clone())?;
    let args = match args {
        FunctionArgs::Star => bail!("{} does not accept * as an argument", ident),
        FunctionArgs::Args(args) => args,
    };
    let tf = func::select_table_func(ecx, &*ident, args)?;
    let call = RelationExpr::FlatMap {
        input: Box::new(left),
        func: tf.func,
        exprs: tf.exprs,
    };
    let name = PartialName {
        database: None,
        schema: None,
        item: ident,
    };
    let scope = plan_table_alias(ecx.qcx, alias, Some(name), tf.column_names)?;
    Ok((call, ecx.scope.clone().product(scope)))
}

fn plan_table_alias(
    qcx: &QueryContext,
    alias: Option<&TableAlias>,
    inherent_table_name: Option<PartialName>,
    inherent_column_names: Vec<Option<ColumnName>>,
) -> Result<Scope, anyhow::Error> {
    let table_name = match alias {
        None => inherent_table_name,
        Some(TableAlias { name, .. }) => Some(PartialName {
            database: None,
            schema: None,
            item: normalize::ident(name.to_owned()),
        }),
    };
    let column_names = match alias {
        None => inherent_column_names,
        Some(TableAlias {
            columns,
            strict: false,
            ..
        }) if columns.is_empty() => inherent_column_names,

        Some(TableAlias {
            columns, strict, ..
        }) if (columns.len() > inherent_column_names.len())
            || (*strict && columns.len() != inherent_column_names.len()) =>
        {
            bail!(
                "{} has {} columns available but {} columns specified",
                table_name
                    .map(|n| n.to_string())
                    .as_deref()
                    .unwrap_or("subquery"),
                inherent_column_names.len(),
                columns.len()
            );
        }

        Some(TableAlias { columns, .. }) => columns
            .iter()
            .cloned()
            .map(|n| Some(normalize::column_name(n)))
            .chain(inherent_column_names.into_iter().skip(columns.len()))
            .collect(),
    };
    Ok(Scope::from_source(
        table_name,
        column_names,
        Some(qcx.outer_scope.clone()),
    ))
}

fn invent_column_name(expr: &Expr) -> Option<ScopeItemName> {
    let name = match expr {
        Expr::Identifier(names) => names.last().map(|n| normalize::column_name(n.clone())),
        Expr::Function(func) => func
            .name
            .0
            .last()
            .map(|n| normalize::column_name(n.clone())),
        Expr::Coalesce { .. } => Some("coalesce".into()),
        Expr::List { .. } => Some("list".into()),
        Expr::Cast { expr, .. } => return invent_column_name(expr),
        Expr::FieldAccess { field, .. } => Some(normalize::column_name(field.clone())),
        _ => return None,
    };
    name.map(|n| ScopeItemName {
        table_name: None,
        column_name: Some(n),
        priority: false,
    })
}

enum ExpandedSelectItem<'a> {
    InputOrdinal(usize),
    Expr(Cow<'a, Expr>),
}

fn expand_select_item<'a>(
    ecx: &ExprContext,
    s: &'a SelectItem,
) -> Result<Vec<(ExpandedSelectItem<'a>, Option<ColumnName>)>, anyhow::Error> {
    match s {
        SelectItem::Expr {
            expr: Expr::QualifiedWildcard(table_name),
            alias: _,
        } => {
            let table_name = normalize::object_name(ObjectName(table_name.clone()))?;
            let out: Vec<_> = ecx
                .scope
                .items
                .iter()
                .enumerate()
                .filter(|(_i, item)| item.is_from_table(&table_name))
                .map(|(i, item)| {
                    let name = item.names.get(0).and_then(|n| n.column_name.clone());
                    (ExpandedSelectItem::InputOrdinal(i), name)
                })
                .collect();
            if out.is_empty() {
                bail!("no table named '{}' in scope", table_name);
            }
            Ok(out)
        }
        SelectItem::Expr {
            expr: Expr::WildcardAccess(sql_expr),
            alias: _,
        } => {
            // A bit silly to have to plan the expression here just to get its
            // type, since we throw away the planned expression, but fixing this
            // requires a separate semantic analysis phase. Luckily this is an
            // uncommon operation and the PostgreSQL docs have a warning that
            // this operation is slow in Postgres too.
            let expr = plan_expr(ecx, sql_expr)?.type_as_any(ecx)?;
            let fields = match ecx.scalar_type(&expr) {
                ScalarType::Record { fields, .. } => fields,
                ty => bail!("type {} is not composite", ty),
            };
            let items = fields
                .iter()
                .map(|(name, _ty)| {
                    let item = ExpandedSelectItem::Expr(Cow::Owned(Expr::FieldAccess {
                        expr: sql_expr.clone(),
                        field: Ident::new(name.as_str()),
                    }));
                    (item, Some(name.clone()))
                })
                .collect();
            Ok(items)
        }
        SelectItem::Wildcard => {
            let items: Vec<_> = ecx
                .scope
                .items
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    let name = item.names.get(0).and_then(|n| n.column_name.clone());
                    (ExpandedSelectItem::InputOrdinal(i), name)
                })
                .collect();
            Ok(items)
        }
        SelectItem::Expr { expr, alias } => {
            let name = alias
                .clone()
                .map(normalize::column_name)
                .or_else(|| invent_column_name(&expr).and_then(|n| n.column_name));
            Ok(vec![(ExpandedSelectItem::Expr(Cow::Borrowed(expr)), name)])
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
) -> Result<(RelationExpr, Scope), anyhow::Error> {
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
) -> Result<(RelationExpr, Scope), anyhow::Error> {
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
            let on = plan_expr(ecx, expr)?.type_as(ecx, ScalarType::Bool)?;
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
) -> Result<(RelationExpr, Scope), anyhow::Error> {
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
        if l_type.scalar_type != r_type.scalar_type {
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

pub fn plan_expr<'a>(ecx: &'a ExprContext, e: &Expr) -> Result<CoercibleScalarExpr, anyhow::Error> {
    if let Some(i) = ecx.scope.resolve_expr(e) {
        // We've already calculated this expression.
        return Ok(ScalarExpr::Column(i).into());
    }

    Ok(match e {
        // Names.
        Expr::Identifier(names) | Expr::QualifiedWildcard(names) => {
            plan_identifier(ecx, names)?.into()
        }

        // Literals.
        Expr::Value(val) => plan_literal(val)?,
        Expr::Parameter(n) => {
            if !ecx.allow_subqueries {
                bail!("{} does not allow subqueries", ecx.name)
            }
            if *n == 0 || *n > 65536 {
                bail!("there is no parameter ${}", n);
            }
            if ecx.qcx.param_types.borrow().contains_key(n) {
                ScalarExpr::Parameter(*n).into()
            } else {
                CoercibleScalarExpr::Parameter(*n)
            }
        }
        Expr::List(exprs) => {
            let mut out = vec![];
            for e in exprs {
                out.push(plan_expr(ecx, e)?);
            }
            CoercibleScalarExpr::LiteralList(out)
        }
        Expr::Row { exprs } => {
            let mut out = vec![];
            for e in exprs {
                out.push(plan_expr(ecx, e)?);
            }
            CoercibleScalarExpr::LiteralRecord(out)
        }

        // Generalized functions, operators, and casts.
        Expr::UnaryOp { op, expr } => func::plan_unary_op(ecx, op, expr)?.into(),
        Expr::BinaryOp { op, left, right } => func::plan_binary_op(ecx, op, left, right)?.into(),
        Expr::Cast { expr, data_type } => {
            let to_scalar_type = scalar_type_from_sql(data_type)?;
            let expr = plan_expr(ecx, expr)?;
            let expr = typeconv::plan_coerce(ecx, expr, CoerceTo::Plain(to_scalar_type.clone()))?;
            typeconv::plan_cast("CAST", ecx, expr, CastTo::Explicit(to_scalar_type))?.into()
        }
        Expr::Function(func) => plan_function(ecx, func)?.into(),

        // Special functions and operators.
        Expr::IsNull { expr, negated } => plan_is_null_expr(ecx, expr, *negated)?.into(),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => plan_case(ecx, operand, conditions, results, else_result)?.into(),
        Expr::Coalesce { exprs } => {
            assert!(!exprs.is_empty()); // `COALESCE()` is a syntax error
            let expr = ScalarExpr::CallVariadic {
                func: VariadicFunc::Coalesce,
                exprs: plan_homogeneous_exprs("coalesce", ecx, exprs)?,
            };
            expr.into()
        }
        Expr::FieldAccess { expr, field } => {
            let field = normalize::column_name(field.clone());
            let expr = plan_expr(ecx, expr)?.type_as_any(ecx)?;
            let ty = ecx.scalar_type(&expr);
            let i = match &ty {
                ScalarType::Record { fields, .. } => {
                    fields.iter().position(|(name, _ty)| *name == field)
                }
                ty => bail!(
                    "column notation applied to type {}, which is not a composite type",
                    ty
                ),
            };
            match i {
                None => bail!("field {} not found in data type {}", field, ty),
                Some(i) => expr.call_unary(UnaryFunc::RecordGet(i)).into(),
            }
        }
        Expr::WildcardAccess(expr) => plan_expr(ecx, expr)?,
        Expr::SubscriptIndex { expr, subscript } => {
            let expr = plan_expr(ecx, expr)?.type_as_any(ecx)?;
            let ty = ecx.scalar_type(&expr);
            match &ty {
                ScalarType::List(_) => {}
                ty => bail!("cannot subscript type {}", ty),
            };

            expr.call_binary(
                plan_expr(ecx, subscript)?.explicit_cast_to(
                    "subscript (indexing)",
                    ecx,
                    ScalarType::Int64,
                )?,
                BinaryFunc::ListIndex,
            )
            .into()
        }

        Expr::SubscriptSlice { expr, positions } => {
            assert_ne!(
                positions.len(),
                0,
                "subscript expression must contain at least one position"
            );
            if positions.len() > 1 && !ecx.qcx.scx.experimental_mode() {
                bail!(
                    "multi-dimensional slicing requires experimental mode; see \
                https://materialize.io/docs/cli/#experimental-mode"
                )
            };
            let expr = plan_expr(ecx, expr)?.type_as_any(ecx)?;
            let ty = ecx.scalar_type(&expr);
            match &ty {
                ScalarType::List(_) => {
                    let pos_len = positions.len();
                    let n_dims = ty.unwrap_list_n_dims();
                    if pos_len > n_dims {
                        bail!(
                            "cannot slice on {} dimensions; list only has {} dimension{}",
                            pos_len,
                            n_dims,
                            if n_dims == 1 { "" } else { "s" }
                        )
                    }
                }
                ty => bail!("cannot subscript type {}", ty),
            };

            let mut exprs = vec![expr];
            let op_str = "subscript (slicing)";

            for p in positions {
                let start = if let Some(start) = &p.start {
                    plan_expr(ecx, start)?.explicit_cast_to(op_str, ecx, ScalarType::Int64)?
                } else {
                    ScalarExpr::literal(Datum::Int64(1), ColumnType::new(ScalarType::Int64, true))
                };

                let end = if let Some(end) = &p.end {
                    plan_expr(ecx, end)?.explicit_cast_to(op_str, ecx, ScalarType::Int64)?
                } else {
                    ScalarExpr::literal(
                        Datum::Int64(i64::MAX - 1),
                        ColumnType::new(ScalarType::Int64, true),
                    )
                };

                exprs.push(start);
                exprs.push(end);
            }

            ScalarExpr::CallVariadic {
                func: VariadicFunc::ListSlice,
                exprs,
            }
            .into()
        }

        // Subqueries.
        Expr::Exists(query) => {
            if !ecx.allow_subqueries {
                bail!("{} does not allow subqueries", ecx.name)
            }
            let qcx = ecx.derived_query_context();
            let (expr, _scope) = plan_subquery(&qcx, query)?;
            expr.exists().into()
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
            expr.select().into()
        }

        Expr::Collate { .. } => unsupported!("COLLATE"),
        Expr::Nested(_) => unreachable!("Expr::Nested not desugared"),
        Expr::InList { .. } => unreachable!("Expr::InList not desugared"),
        Expr::InSubquery { .. } => unreachable!("Expr::InSubquery not desugared"),
        Expr::Any { .. } => unreachable!("Expr::Any not desugared"),
        Expr::All { .. } => unreachable!("Expr::All not desugared"),
        Expr::Between { .. } => unreachable!("Expr::Between not desugared"),
    })
}

/// Plans a list of expressions such that all input expressions will be cast to
/// the same type. If successful, returns a new list of expressions in the same
/// order as the input, where each expression has the appropriate casts to make
/// them all of a uniform type.
///
/// Note that this is our implementation of Postres' type conversion for
/// ["`UNION`, `CASE`, and Related Constructs"][union-type-conv], though it isn't
/// yet used in all of those cases.
///
/// [union-type-conv]:
/// https://www.postgresql.org/docs/12/typeconv-union-case.html
pub fn plan_homogeneous_exprs(
    name: &str,
    ecx: &ExprContext,
    exprs: &[impl std::borrow::Borrow<Expr>],
) -> Result<Vec<ScalarExpr>, anyhow::Error> {
    assert!(!exprs.is_empty());

    let mut cexprs = Vec::new();
    for expr in exprs.iter() {
        let cexpr = super::query::plan_expr(ecx, expr.borrow())?;
        cexprs.push(cexpr);
    }

    let types: Vec<_> = cexprs
        .iter()
        .map(|e| ecx.column_type(e).map(|t| t.scalar_type))
        .collect();

    let target = match typeconv::guess_best_common_type(&types) {
        Some(t) => t,
        None => bail!("Cannot determine homogenous type for arguments to {}", name),
    };

    // Try to cast all expressions to `target`.
    let mut out = Vec::new();
    for cexpr in cexprs {
        let arg = typeconv::plan_coerce(ecx, cexpr, CoerceTo::Plain(target.clone()))?;

        match typeconv::plan_cast(name, ecx, arg.clone(), CastTo::Implicit(target.clone())) {
            Ok(expr) => out.push(expr),
            Err(_) => bail!(
                "{} does not have uniform type: {:?} vs {:?}",
                name,
                ecx.scalar_type(&arg),
                target,
            ),
        }
    }
    Ok(out)
}

fn plan_aggregate(ecx: &ExprContext, sql_func: &Function) -> Result<AggregateExpr, anyhow::Error> {
    let name = normalize::function_name(sql_func.name.clone())?;
    assert!(func::is_aggregate_func(&name));

    if sql_func.over.is_some() {
        unsupported!(213, "window functions");
    }

    // We follow PostgreSQL's rule here for mapping `count(*)` into the
    // generalized function selection framework. The rule is simple: the user
    // must type `count(*)`, but the function selection framework sees an empty
    // parameter list, as if the user had typed `count()`. But if the user types
    // `count()` directly, that is an error. Like PostgreSQL, we apply these
    // rules to all aggregates, not just `count`, since we may one day support
    // user-defined aggregates, including user-defined aggregates that take no
    // parameters.
    let args = match &sql_func.args {
        FunctionArgs::Star => &[][..],
        FunctionArgs::Args(args) if args.is_empty() => {
            bail!(
                "{}(*) must be used to call a parameterless aggregate function",
                name
            );
        }
        FunctionArgs::Args(args) => args,
    };
    let (mut expr, mut func) = func::select_aggregate_func(ecx, &name, args)?;
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
        let cond = plan_expr(&ecx.with_name("FILTER"), filter)?.type_as(ecx, ScalarType::Bool)?;
        if func == AggregateFunc::CountAll {
            func = AggregateFunc::Count;
            expr = ScalarExpr::literal_true();
        }
        let expr_typ = ecx.scalar_type(&expr);
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

fn plan_identifier(ecx: &ExprContext, names: &[Ident]) -> Result<ScalarExpr, PlanError> {
    let mut names = names.to_vec();
    let col_name = normalize::column_name(names.pop().unwrap());

    // If the name is qualified, it must refer to a column in a table.
    if !names.is_empty() {
        let table_name = normalize::object_name(ObjectName(names))?;
        let (i, _name) = ecx.scope.resolve_table_column(&table_name, &col_name)?;
        return Ok(ScalarExpr::Column(i));
    }

    // If the name is unqualified, first check if it refers to a column.
    match ecx.scope.resolve_column(&col_name) {
        Ok((i, _name)) => return Ok(ScalarExpr::Column(i)),
        Err(PlanError::UnknownColumn(_)) => (),
        Err(e) => return Err(e),
    }

    // The name doesn't refer to a column. Check if it refers to a table. If it
    // does, the expression is a record containing all the columns of the table.
    //
    // NOTE(benesch): `Scope` doesn't have an API for asking whether a given
    // table is in scope, so we instead look at every column in the scope and
    // ask what table it came from. Stupid, but it works.
    let (exprs, field_names): (Vec<_>, Vec<_>) = ecx
        .scope
        .items
        .iter()
        .enumerate()
        .filter(|(_i, item)| {
            item.is_from_table(&PartialName {
                database: None,
                schema: None,
                item: col_name.as_str().to_owned(),
            })
        })
        .enumerate()
        .map(|(i, (column, item))| {
            let expr = ScalarExpr::Column(ColumnRef { level: 0, column });
            let name = item
                .names
                .first()
                .and_then(|n| n.column_name.clone())
                .unwrap_or_else(|| ColumnName::from(format!("f{}", i + 1)));
            (expr, name)
        })
        .unzip();
    if !exprs.is_empty() {
        return Ok(ScalarExpr::CallVariadic {
            func: VariadicFunc::RecordCreate { field_names },
            exprs,
        });
    }

    Err(PlanError::UnknownColumn(col_name.to_string()))
}

fn plan_function<'a>(
    ecx: &ExprContext,
    sql_func: &'a Function,
) -> Result<ScalarExpr, anyhow::Error> {
    let name = normalize::function_name(sql_func.name.clone())?;
    let ident = &*name;

    if func::is_aggregate_func(&name) {
        if ecx.allow_aggregates {
            // should already have been caught by `scope.resolve_expr` in `plan_expr`
            bail!(
                "Internal error: encountered unplanned aggregate function: {:?}",
                sql_func,
            )
        } else {
            bail!("aggregate functions are not allowed in {}", ecx.name);
        }
    } else if func::is_table_func(&name) {
        unsupported!(
            1546,
            format!("table function ({}) in scalar position", ident)
        );
    }

    if sql_func.over.is_some() {
        unsupported!(213, "window functions");
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

    func::select_scalar_func(ecx, ident, args)
}

fn plan_is_null_expr<'a>(
    ecx: &ExprContext,
    inner: &'a Expr,
    not: bool,
) -> Result<ScalarExpr, anyhow::Error> {
    // PostgreSQL can plan `NULL IS NULL` but not `$1 IS NULL`. This is at odds
    // with our type coercion rules, which treat `NULL` literals and
    // unconstrained parameters identically. Providing a type hint of string
    // means we wind up supporting both.
    let expr = plan_expr(ecx, inner)?.type_as_any(ecx)?;
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

fn plan_case<'a>(
    ecx: &ExprContext,
    operand: &'a Option<Box<Expr>>,
    conditions: &'a [Expr],
    results: &'a [Expr],
    else_result: &'a Option<Box<Expr>>,
) -> Result<ScalarExpr, anyhow::Error> {
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
        let cexpr = plan_expr(ecx, &c)?.type_as(ecx, ScalarType::Bool)?;
        cond_exprs.push(cexpr);
        result_exprs.push(r);
    }
    result_exprs.push(match else_result {
        Some(else_result) => else_result,
        None => &Expr::Value(Value::Null),
    });
    let mut result_exprs = plan_homogeneous_exprs("CASE", ecx, &result_exprs)?;
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

fn plan_literal<'a>(l: &'a Value) -> Result<CoercibleScalarExpr, anyhow::Error> {
    let (datum, scalar_type) = match l {
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
        Value::HexString(_) => unsupported!(3114, "hex string literals"),
        Value::Boolean(b) => match b {
            false => (Datum::False, ScalarType::Bool),
            true => (Datum::True, ScalarType::Bool),
        },
        Value::Interval(iv) => {
            let mut i = strconv::parse_interval_w_disambiguator(&iv.value, iv.precision_low)?;
            i.truncate_high_fields(iv.precision_high);
            i.truncate_low_fields(iv.precision_low, iv.fsec_max_precision)?;
            (Datum::Interval(i), ScalarType::Interval)
        }
        Value::String(s) => return Ok(CoercibleScalarExpr::LiteralString(s.clone())),
        Value::Null => return Ok(CoercibleScalarExpr::LiteralNull),
    };
    let nullable = datum == Datum::Null;
    let typ = ColumnType::new(scalar_type, nullable);
    let expr = ScalarExpr::literal(datum, typ);
    Ok(expr.into())
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

pub fn scalar_type_from_sql(data_type: &DataType) -> Result<ScalarType, anyhow::Error> {
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
    err: Option<anyhow::Error>,
}

impl<'ast> AggregateFuncVisitor<'ast> {
    fn new() -> AggregateFuncVisitor<'ast> {
        AggregateFuncVisitor {
            aggs: Vec::new(),
            within_aggregate: false,
            err: None,
        }
    }

    fn into_result(self) -> Result<Vec<&'ast Function>, anyhow::Error> {
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
        if let Ok(name) = normalize::function_name(func.name.clone()) {
            if func::is_aggregate_func(&name) {
                if self.within_aggregate {
                    self.err = Some(anyhow!("nested aggregate functions are not allowed"));
                    return;
                }
                self.aggs.push(func);
                let Function {
                    name: _,
                    args,
                    filter,
                    over: _,
                    distinct: _,
                } = func;
                if let Some(filter) = filter {
                    self.visit_expr(filter);
                }
                let old_within_aggregate = self.within_aggregate;
                self.within_aggregate = true;
                self.visit_function_args(args);
                self.within_aggregate = old_within_aggregate;
                return;
            }
        }
        visit::visit_function(self, func);
    }

    fn visit_query(&mut self, _query: &'ast Query) {
        // Don't go into subqueries.
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
pub struct QueryContext<'a> {
    /// The context for the containing `Statement`.
    pub scx: &'a StatementContext<'a>,
    /// The lifetime that the planned query will have.
    pub lifetime: QueryLifetime,
    /// The scope of the outer relation expression.
    pub outer_scope: Scope,
    /// The type of the outer relation expressions.
    pub outer_relation_types: Vec<RelationType>,
    /// The types of the parameters in the query. This is filled in as planning
    /// occurs.
    pub param_types: Rc<RefCell<BTreeMap<usize, ScalarType>>>,
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
pub struct ExprContext<'a> {
    pub qcx: &'a QueryContext<'a>,
    /// The name of this kind of expression eg "WHERE clause". Used only for error messages.
    pub name: &'static str,
    /// The context for the `Query` that contains this `Expr`.
    /// The current scope.
    pub scope: &'a Scope,
    /// The type of the current relation expression upon which this scalar
    /// expression will be evaluated.
    pub relation_type: &'a RelationType,
    /// Are aggregate functions allowed in this context
    pub allow_aggregates: bool,
    /// Are subqueries allowed in this context
    pub allow_subqueries: bool,
}

impl<'a> ExprContext<'a> {
    fn with_name(&self, name: &'static str) -> ExprContext<'a> {
        let mut ecx = self.clone();
        ecx.name = name;
        ecx
    }

    pub fn column_type<E>(&self, expr: &E) -> E::Type
    where
        E: ScalarTypeable,
    {
        expr.typ(
            &self.qcx.outer_relation_types,
            &self.relation_type,
            &self.qcx.param_types.borrow(),
        )
    }

    pub fn scalar_type(&self, expr: &ScalarExpr) -> ScalarType {
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
