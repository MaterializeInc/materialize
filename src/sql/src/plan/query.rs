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
use std::collections::{BTreeMap, HashSet};
use std::convert::TryInto;
use std::iter;
use std::rc::Rc;

use failure::{bail, ensure, format_err, ResultExt};
use sql_parser::ast::visit::{self, Visit};
use sql_parser::ast::{
    BinaryOperator, DataType, Expr, ExtractField, Function, FunctionArgs, Ident, JoinConstraint,
    JoinOperator, ObjectName, Query, Select, SelectItem, SetExpr, SetOperator, ShowStatementFilter,
    TableAlias, TableFactor, TableWithJoins, TrimSide, UnaryOperator, Value, Values,
};
use uuid::Uuid;

use ::expr::{Id, RowSetFinishing};
use dataflow_types::Timestamp;
use repr::adt::decimal::{Decimal, MAX_DECIMAL_PRECISION};
use repr::{
    strconv, ColumnName, ColumnType, Datum, RelationDesc, RelationType, RowArena, ScalarType,
};

use crate::names::PartialName;
use crate::plan::cast;
use crate::plan::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, CoercibleScalarExpr, ColumnOrder, ColumnRef,
    JoinKind, NullaryFunc, RelationExpr, ScalarExpr, ScalarTypeable, TableFunc, UnaryFunc,
    VariadicFunc,
};
use crate::plan::func;
use crate::plan::scope::{Scope, ScopeItem, ScopeItemName};
use crate::plan::statement::StatementContext;
use crate::plan::transform;
use crate::{normalize, unsupported};

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
    transform::transform(&mut query);
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
        if typ.scalar_type != ScalarType::Bool {
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
        if typ.scalar_type != ScalarType::Bool {
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
                unsupported!("WITH hints");
            }
            let alias = if let Some(TableAlias { name, columns }) = alias {
                if !columns.is_empty() {
                    unsupported!(3112, "aliasing columns");
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
                let name = qcx.scx.resolve_item(name.clone())?;
                let item = qcx.scx.catalog.get_item(&name);
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
                unsupported!(3111, "LATERAL derived tables");
            }
            let (expr, scope) = plan_subquery(&qcx, &subquery)?;
            let alias = if let Some(TableAlias { name, columns }) = alias {
                if !columns.is_empty() {
                    unsupported!(3112, "aliasing columns");
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
    use cast::CastContext::Internal;
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
        ("generate_series", [start, stop]) => {
            // If both start and stop are Int32s, we use the Int32 version. Otherwise, promote both
            // arguments.
            let mut start = plan_expr(ecx, start, Some(ScalarType::Int64))?;
            let mut stop = plan_expr(ecx, stop, Some(ScalarType::Int64))?;
            let typ = match (
                ecx.column_type(&start).scalar_type,
                ecx.column_type(&stop).scalar_type,
            ) {
                (ScalarType::Int32, ScalarType::Int32) => ScalarType::Int32,
                _ => {
                    start = plan_cast_internal(
                        Internal("first generate_series argument"),
                        ecx,
                        start,
                        cast::CastTo::Explicit(ScalarType::Int64),
                    )?;
                    stop = plan_cast_internal(
                        Internal("second generate_series argument"),
                        ecx,
                        stop,
                        cast::CastTo::Explicit(ScalarType::Int64),
                    )?;
                    ScalarType::Int64
                }
            };

            let call = RelationExpr::FlatMap {
                input: Box::new(left),
                func: TableFunc::GenerateSeries(typ),
                exprs: vec![start, stop],
            };

            let scope = Scope::from_source(
                alias,
                vec![Some("generate_series")],
                Some(ecx.qcx.outer_scope.clone()),
            );

            Ok((call, ecx.scope.clone().product(scope)))
        }
        ("jsonb_each", [expr])
        | ("jsonb_object_keys", [expr])
        | ("jsonb_array_elements", [expr])
        | ("jsonb_each_text", [expr])
        | ("jsonb_array_elements_text", [expr]) => {
            let expr = plan_expr(ecx, expr, Some(ScalarType::Jsonb))?;
            match ecx.column_type(&expr).scalar_type {
                ScalarType::Jsonb => {
                    let func = match ident {
                        "jsonb_each" | "jsonb_each_text" => TableFunc::JsonbEach,
                        "jsonb_object_keys" => TableFunc::JsonbObjectKeys,
                        "jsonb_array_elements" | "jsonb_array_elements_text" => {
                            TableFunc::JsonbArrayElements
                        }
                        _ => unreachable!(),
                    };
                    let mut call = RelationExpr::FlatMap {
                        input: Box::new(left),
                        func,
                        exprs: vec![expr],
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
            let call = RelationExpr::FlatMap {
                input: Box::new(left),
                func: TableFunc::RegexpExtract(ar),
                exprs: vec![expr],
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
            let call = RelationExpr::FlatMap {
                input: Box::new(left),
                func: TableFunc::CsvExtract(n_cols),
                exprs: vec![expr],
            };
            let scope = Scope::from_source(
                alias,
                colnames.iter().map(|name| Some(ColumnName::from(&**name))),
                Some(ecx.qcx.outer_scope.clone()),
            );
            Ok((call, ecx.scope.clone().product(scope)))
        }
        ("regexp_extract", _) | ("csv_extract", _) | ("generate_series", _) => {
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
        JoinOperator::CrossApply => unsupported!("CROSS APPLY"),
        JoinOperator::OuterApply => unsupported!("OUTER APPLY"),
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

pub fn plan_expr<'a>(
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
    let (expr, name) = plan_coercible_expr(ecx, e)?;
    let coerce_to = match type_hint {
        None => CoerceTo::Nothing,
        Some(type_hint) => CoerceTo::Plain(type_hint),
    };
    Ok((plan_coerce(ecx, expr, coerce_to)?, name))
}

/// Controls coercion behavior for `plan_coerce`.
///
/// Note that `CoerceTo` cannot affect already coerced elements, i.e.,
/// the [`CoercibleScalarExpr::Coerced`] variant.
#[derive(Clone, Debug)]
pub enum CoerceTo {
    /// No coercion preference.
    Nothing,
    /// Coerce to the specified scalar type.
    Plain(ScalarType),
    /// Coerce using special JSONB coercion rules. The following table
    /// summarizes the differences between the normal and special JSONB
    /// conversion rules.
    ///
    /// +--------------+---------------+--------------------+-------------------------+
    /// |              | NULL          | 'literal'          | '"literal"'             |
    /// +--------------|---------------|--------------------|-------------------------|
    /// | Plain(Jsonb) | NULL::jsonb   | <error: bad json>  | '"literal"'::jsonb      |
    /// | JsonbAny     | 'null'::jsonb | '"literal"'::jsonb | '"\"literal\""'::jsonb  |
    /// +--------------+---------------+--------------------+-------------------------+
    JsonbAny,
}

pub fn plan_coerce<'a>(
    ecx: &'a ExprContext,
    e: CoercibleScalarExpr,
    coerce_to: CoerceTo,
) -> Result<ScalarExpr, failure::Error> {
    use CoerceTo::*;
    use CoercibleScalarExpr::*;

    Ok(match (e, coerce_to) {
        (Coerced(e), _) => e,

        (LiteralNull, Nothing) => bail!("unable to infer type for NULL"),
        (LiteralNull, Plain(typ)) => ScalarExpr::literal_null(typ),
        (LiteralNull, JsonbAny) => {
            ScalarExpr::literal(Datum::JsonNull, ColumnType::new(ScalarType::Jsonb))
        }

        (LiteralString(s), Nothing) => {
            ScalarExpr::literal(Datum::String(&s), ColumnType::new(ScalarType::String))
        }
        (LiteralString(s), Plain(typ)) => {
            let lit = ScalarExpr::literal(Datum::String(&s), ColumnType::new(ScalarType::String));
            plan_cast_internal(
                cast::CastContext::Internal("string literal"),
                ecx,
                lit,
                cast::CastTo::Explicit(typ),
            )?
        }
        (LiteralString(s), JsonbAny) => {
            ScalarExpr::literal(Datum::String(&s), ColumnType::new(ScalarType::Jsonb))
        }

        (LiteralList(exprs), coerce_to) => {
            let coerce_elem_to = match &coerce_to {
                Plain(ScalarType::List(typ)) => Plain((**typ).clone()),
                Nothing | Plain(_) => {
                    let typ = exprs
                        .iter()
                        .find_map(|e| ecx.column_type(e).map(|t| t.scalar_type));
                    CoerceTo::Plain(typ.unwrap_or(ScalarType::String))
                }
                JsonbAny => bail!("cannot coerce list literal to jsonb type"),
            };
            let mut out = vec![];
            for e in exprs {
                out.push(plan_coerce(ecx, e, coerce_elem_to.clone())?);
            }
            let typ = if !out.is_empty() {
                ecx.scalar_type(&out[0])
            } else if let Plain(ScalarType::List(ty)) = coerce_to {
                *ty
            } else {
                bail!("unable to infer type for empty list")
            };
            for (i, e) in out.iter().enumerate() {
                let t = ecx.scalar_type(&e);
                if t != typ {
                    bail!(
                        "Cannot create list with mixed types. \
                        Element 1 has type {} but element {} has type {}",
                        typ,
                        i + 1,
                        t,
                    )
                }
            }
            ScalarExpr::CallVariadic {
                func: VariadicFunc::ListCreate { elem_type: typ },
                exprs: out,
            }
        }

        (Parameter(n), coerce_to) => {
            let typ = match coerce_to {
                CoerceTo::Nothing => bail!("unable to infer type for parameter ${}", n),
                CoerceTo::Plain(typ) => typ,
                CoerceTo::JsonbAny => ScalarType::Jsonb,
            };
            let prev = ecx.qcx.param_types.borrow_mut().insert(n, typ);
            assert!(prev.is_none());
            ScalarExpr::Parameter(n)
        }
    })
}

pub fn plan_coercible_expr<'a>(
    ecx: &'a ExprContext,
    e: &Expr,
) -> Result<(CoercibleScalarExpr, Option<ScopeItemName>), failure::Error> {
    if let Some((i, name)) = ecx.scope.resolve_expr(e) {
        // surprise - we already calculated this expr before
        Ok((ScalarExpr::Column(i).into(), name.cloned()))
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
                (ScalarExpr::Column(i).into(), Some(name.clone()))
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
                if ecx.qcx.param_types.borrow().contains_key(n) {
                    (ScalarExpr::Parameter(*n).into(), None)
                } else {
                    (CoercibleScalarExpr::Parameter(*n), None)
                }
            }
            // TODO(benesch): why isn't IS [NOT] NULL a unary op?
            Expr::IsNull(expr) => (plan_is_null_expr(ecx, expr, false)?.into(), None),
            Expr::IsNotNull(expr) => (plan_is_null_expr(ecx, expr, true)?.into(), None),
            Expr::UnaryOp { op, expr } => (plan_unary_op(ecx, op, expr)?.into(), None),
            Expr::BinaryOp { op, left, right } => {
                (func::plan_binary_op(ecx, op, left, right)?.into(), None)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => (plan_between(ecx, expr, low, high, *negated)?.into(), None),
            Expr::InList {
                expr,
                list,
                negated,
            } => (plan_in_list(ecx, expr, list, *negated)?.into(), None),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => (
                plan_case(ecx, operand, conditions, results, else_result)?.into(),
                None,
            ),
            Expr::Nested(expr) => (plan_coercible_expr(ecx, expr)?.0, None),
            Expr::Cast { expr, data_type } => {
                let (expr, name) = plan_cast(ecx, expr, data_type)?;
                (expr.into(), name)
            }
            Expr::Function(func) => {
                let expr = plan_function(ecx, func)?.into();
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
                (expr.exists().into(), None)
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
                (expr.select().into(), None)
            }
            Expr::Any {
                left,
                op,
                right,
                some: _,
            } => (
                plan_any_or_all(ecx, left, op, right, AggregateFunc::Any)?.into(),
                None,
            ),
            Expr::All { left, op, right } => (
                plan_any_or_all(ecx, left, op, right, AggregateFunc::All)?.into(),
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
                        plan_any_or_all(ecx, expr, &NotEq, subquery, AggregateFunc::All)?.into(),
                        None,
                    )
                } else {
                    // `<expr> IN (<subquery>)` is equivalent to
                    // `<expr> = ANY (<subquery>)`.
                    (
                        plan_any_or_all(ecx, expr, &Eq, subquery, AggregateFunc::Any)?.into(),
                        None,
                    )
                }
            }
            Expr::Trim { side, exprs } => {
                let ident = match side {
                    TrimSide::Both => "btrim",
                    TrimSide::Leading => "ltrim",
                    TrimSide::Trailing => "rtrim",
                };

                (
                    CoercibleScalarExpr::Coerced(func::select_scalar_func(ecx, ident, exprs)?),
                    None,
                )
            }
            Expr::Extract { field, expr } => {
                // No type hint passed to `plan_expr`, because `expr` can be
                // any date type. PostgreSQL is also unable to infer parameter
                // types in this position.
                let mut expr = plan_expr(ecx, expr, None)?;
                let mut typ = ecx.scalar_type(&expr);
                if let ScalarType::Date = typ {
                    expr = plan_cast_internal(
                        cast::CastContext::Internal("EXTRACT"),
                        ecx,
                        expr,
                        cast::CastTo::Explicit(ScalarType::Timestamp),
                    )?;
                    typ = ecx.scalar_type(&expr);
                }
                let func = match &typ {
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
                (expr.call_unary(func).into(), None)
            }
            Expr::Collate { .. } => unsupported!("COLLATE"),
            Expr::Coalesce { exprs } => {
                assert!(!exprs.is_empty()); // `COALESCE()` is a syntax error
                let expr = ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: plan_homogeneous_exprs("coalesce", ecx, exprs)?,
                };
                let name = ScopeItemName {
                    table_name: None,
                    column_name: Some("coalesce".into()),
                };
                (expr.into(), Some(name))
            }
            Expr::List(exprs) => {
                let mut out = vec![];
                for e in exprs {
                    out.push(plan_coercible_expr(ecx, e)?.0);
                }
                (
                    CoercibleScalarExpr::LiteralList(out),
                    Some(ScopeItemName {
                        table_name: None,
                        column_name: Some(ColumnName::from("list")),
                    }),
                )
            }
        })
    }
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
) -> Result<Vec<ScalarExpr>, failure::Error> {
    assert!(!exprs.is_empty());

    let mut cexprs = Vec::new();
    for expr in exprs.iter() {
        let cexpr = super::query::plan_coercible_expr(ecx, expr.borrow())?.0;
        cexprs.push(cexpr);
    }

    let types: Vec<_> = cexprs
        .iter()
        .map(|e| ecx.column_type(e).map(|t| t.scalar_type))
        .collect();

    let target = match cast::guess_best_common_type(&types) {
        Some(t) => t,
        None => bail!("Cannot determine homogenous type for arguments to {}", name),
    };

    // Try to cast all expressions to `target`.
    let mut out = Vec::new();
    for cexpr in cexprs {
        let arg = super::query::plan_coerce(ecx, cexpr, CoerceTo::Plain(target.clone()))?;

        match plan_cast_internal(
            cast::CastContext::Internal(name),
            ecx,
            arg.clone(),
            cast::CastTo::Implicit(target.clone()),
        ) {
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

    if !column_types[0].nullable {
        // This transformation allows us to exploit some structure that is easy to spot at this
        // level, but trickier at the dataflow layer, so we do it here. However, it's only valid if
        // the column from the right subquery is non-nullable.
        //
        // Planning ANY:
        // SELECT abc.a = ANY(SELECT xyz.x FROM xyz) FROM abc
        // =>
        // SELECT EXISTS(SELECT xyz.x FROM xyz WHERE abc.a = xyz.x) FROM abc
        //
        // Planning ALL (the same, but applying De Morgan's law):
        // SELECT abc.a != ALL(SELECT xyz.x FROM xyz) FROM abc
        // =>
        // SELECT NOT EXISTS(SELECT xyz.x FROM xyz WHERE abc.a = xyz.x) FROM abc

        let mut cond = func::plan_binary_op(
            &any_ecx,
            op,
            left,
            &Expr::Identifier(vec![Ident::new(right_name)]),
        )?;
        if func == AggregateFunc::All {
            cond = cond.call_unary(UnaryFunc::Not);
        }

        let mut exists = right.filter(vec![cond]).exists();
        if func == AggregateFunc::All {
            exists = exists.call_unary(UnaryFunc::Not);
        }
        Ok(ScalarExpr::If {
            cond: Box::new(plan_is_null_expr(ecx, left, false)?),
            then: Box::new(ScalarExpr::literal_null(ecx.scalar_type(&exists))),
            els: Box::new(exists),
        })
    } else {
        let op_expr = func::plan_binary_op(
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
}

fn plan_cast<'a>(
    ecx: &ExprContext,
    expr: &'a Expr,
    data_type: &'a DataType,
) -> Result<(ScalarExpr, Option<ScopeItemName>), failure::Error> {
    let to_scalar_type = scalar_type_from_sql(data_type)?;
    let (expr, maybe_name) = plan_expr_returning_name(ecx, expr, Some(to_scalar_type.clone()))?;
    Ok((
        plan_cast_internal(
            cast::CastContext::CastFunc,
            ecx,
            expr,
            cast::CastTo::Explicit(to_scalar_type),
        )?,
        maybe_name,
    ))
}

fn plan_aggregate(ecx: &ExprContext, sql_func: &Function) -> Result<AggregateExpr, failure::Error> {
    let name = normalize::function_name(sql_func.name.clone())?;
    assert!(is_aggregate_func(&name));

    if sql_func.over.is_some() {
        unsupported!(213, "window functions");
    }

    let (mut expr, mut func) = match (name.as_str(), &sql_func.args) {
        // COUNT(*) is a special case that doesn't compose well
        ("count", FunctionArgs::Star) => (
            // The scalar type of the null doesn't matter because this
            // expression can't escape the surrounding reduce.
            ScalarExpr::literal_null(ScalarType::String),
            AggregateFunc::CountAll,
        ),
        (_, FunctionArgs::Args(args)) if args.len() == 1 => {
            let arg = &args[0];
            // TODO(benesch, sploiselle): hook up the generalized function
            // selection mechanism.
            let type_hint = match &*name {
                "min" | "max" | "count" => Some(ScalarType::String),
                _ => None,
            };
            let expr = plan_expr(ecx, arg, type_hint)?;
            let typ = ecx.column_type(&expr);
            match find_agg_func(&name, typ.scalar_type)? {
                AggregateFunc::JsonbAgg => {
                    // We need to transform input into jsonb in order to
                    // match Postgres' behavior here.
                    let expr = plan_cast_internal(
                        cast::CastContext::Internal("jsonb_agg"),
                        ecx,
                        expr,
                        cast::CastTo::JsonbAny,
                    )?;
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
        if cond_typ.scalar_type != ScalarType::Bool {
            bail!(
                "WHERE expression in FILTER must have boolean type, not {:?}",
                cond_typ
            );
        }
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

    // The functions matched here on string literals do not yet
    // work with our generalized function selection (`sql::func::select_scalar_func`).
    match ident {
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
                ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
                ScalarType::Decimal(p, s) => ScalarType::Decimal(*p, *s),
                ScalarType::Int32 => ScalarType::Decimal(10, 0),
                ScalarType::Int64 => ScalarType::Decimal(19, 0),
                _ => bail!("internal.avg_promotion called with unexpected argument"),
            };
            plan_cast_internal(
                cast::CastContext::Internal("internal.avg_promotion"),
                ecx,
                expr,
                cast::CastTo::Explicit(output_type),
            )
        }

        "mod" => func::plan_binary_op(ecx, &BinaryOperator::Modulus, &args[0], &args[1]),

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

        _ => {
            if is_table_func(&name) {
                unsupported!(
                    1546,
                    format!("table function ({}) in scalar position", ident)
                );
            } else {
                func::select_scalar_func(ecx, ident, args)
            }
        }
    }
}

fn plan_is_null_expr<'a>(
    ecx: &ExprContext,
    inner: &'a Expr,
    not: bool,
) -> Result<ScalarExpr, failure::Error> {
    // PostgreSQL can plan `NULL IS NULL` but not `$1 IS NULL`. This is at odds
    // with our type coercion rules, which treat `NULL` literals and
    // unconstrained parameters identically. Providing a type hint of string
    // means we wind up supporting both.
    let expr = plan_expr(ecx, inner, Some(ScalarType::String))?;
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

fn plan_literal<'a>(l: &'a Value) -> Result<CoercibleScalarExpr, failure::Error> {
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
        Value::HexStringLiteral(_) => unsupported!(3114, "hex string literals"),
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
        Value::SingleQuotedString(s) => return Ok(CoercibleScalarExpr::LiteralString(s.clone())),
        Value::Null => return Ok(CoercibleScalarExpr::LiteralNull),
    };
    let nullable = datum == Datum::Null;
    let typ = ColumnType::new(scalar_type).nullable(nullable);
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

/// Plans a cast between [`ScalarType`]s, specifying which types of casts are
/// permitted using [`cast::CastTo`].
///
/// Note that `plan_cast_internal` only understands [`ScalarType`]s. If you need
/// to cast between SQL [`DataType`]s, see [`Planner::plan_cast`].
///
/// # Errors
///
/// If a cast between the `ScalarExpr`'s base type and the specified type is:
/// - Not possible, e.g. `Bytes` to `Decimal`
/// - Not permitted, e.g. implicitly casting from `Float64` to `Float32`.
pub fn plan_cast_internal<'a>(
    ccx: cast::CastContext,
    ecx: &ExprContext<'a>,
    expr: ScalarExpr,
    cast_to: cast::CastTo,
) -> Result<ScalarExpr, failure::Error> {
    let from_scalar_type = ecx.scalar_type(&expr);

    let cast_op = match cast::get_cast(ccx.clone(), &from_scalar_type, &cast_to) {
        Some(cast_op) => cast_op,
        None => bail!(
            "{} does not support {}casting from {} to {}",
            ccx,
            if let cast::CastTo::Implicit(_) = cast_to {
                "implicitly "
            } else {
                ""
            },
            from_scalar_type,
            cast_to
        ),
    };

    Ok(cast_op.gen_expr(ecx, expr, cast_to))
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
        if let Ok(name) = normalize::function_name(func.name.clone()) {
            if is_aggregate_func(&name) {
                if self.within_aggregate {
                    self.err = Some(format_err!("nested aggregate functions are not allowed"));
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
pub struct ExprContext<'a> {
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

fn is_table_func(name: &str) -> bool {
    match name {
        "csv_extract"
        | "generate_series"
        | "jsonb_array_elements"
        | "jsonb_array_elements_text"
        | "jsonb_each"
        | "jsonb_each_text"
        | "jsonb_object_keys"
        | "regexp_extract" => true,
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
        ("sum", ScalarType::Int32) => AggregateFunc::SumInt32,
        ("sum", ScalarType::Int64) => AggregateFunc::SumInt64,
        ("sum", ScalarType::Float32) => AggregateFunc::SumFloat32,
        ("sum", ScalarType::Float64) => AggregateFunc::SumFloat64,
        ("sum", ScalarType::Decimal(_, _)) => AggregateFunc::SumDecimal,
        ("count", _) => AggregateFunc::Count,
        ("jsonb_agg", _) => AggregateFunc::JsonbAgg,
        other => bail!("Unimplemented function/type combo: {:?}", other),
    })
}
