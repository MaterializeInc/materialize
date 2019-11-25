// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL `Query`s are the declarative, computational part of SQL.
//! This module turns `Query`s into `RelationExpr`s - a more explicit, algebraic way of describing computation.

//! Functions named plan_* are typically responsible for handling a single node of the SQL ast. Eg `plan_query` is responsible for handling `sqlparser::ast::Query`.
//! plan_* functions which correspond to operations on relations typically return a `RelationExpr`.
//! plan_* functions which correspond to operations on scalars typically return a `ScalarExpr` and a `ScalarType`. (The latter is because it's not always possible to infer from a `ScalarExpr` what the intended type is - notably in the case of decimals where the scale/precision are encoded only in the type).

//! Aggregates are particularly twisty.
//! In SQL, a GROUP BY turns any columns not in the group key into vectors of values. Then anywhere later in the scope, an aggregate function can be applied to that group. Inside the arguments of an aggregate function, other normal functions are applied element-wise over the vectors. Thus `SELECT sum(foo.x + foo.y) FROM foo GROUP BY x` means adding the scalar `x` to the vector `y` and summing the results.
//! In `RelationExpr`, aggregates can only be applied immediately at the time of grouping.
//! To deal with this, whenever we see a SQL GROUP BY we look ahead for aggregates and precompute them in the `RelationExpr::Reduce`. When we reach the same aggregates during normal planning later on, we look them up in an `ExprContext` to find the precomputed versions.

use chrono::{DateTime, Utc};
use std::cell::RefCell;
use std::cmp;
use std::collections::{btree_map, BTreeMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::iter;
use std::mem;
use std::rc::Rc;

use failure::{bail, ensure, format_err, ResultExt};
use sqlparser::ast::visit::{self, Visit};
use sqlparser::ast::{
    BinaryOperator, DataType, DateTimeField, Expr, Function, Ident, JoinConstraint, JoinOperator,
    ObjectName, ParsedDate, ParsedTimestamp, Query, Select, SelectItem, SetExpr, SetOperator,
    TableAlias, TableFactor, TableWithJoins, UnaryOperator, Value, Values,
};
use uuid::Uuid;

use ::expr::Id;
use catalog::{Catalog, CatalogEntry};
use dataflow_types::RowSetFinishing;
use ore::iter::{FallibleIteratorExt, IteratorExt};
use repr::decimal::MAX_DECIMAL_PRECISION;
use repr::{ColumnName, ColumnType, Datum, QualName, RelationDesc, RelationType, Row, ScalarType};

use super::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, ColumnOrder, ColumnRef, JoinKind, RelationExpr,
    ScalarExpr, UnaryFunc, VariadicFunc,
};
use super::names;
use super::scope::{Scope, ScopeItem, ScopeItemName};

/// Plans a top-level query, returning the `RelationExpr` describing the query
/// plan, the `RelationDesc` describing the shape of the result set, a
/// `RowSetFinishing` describing post-processing that must occur before results
/// are sent to the client, and the types of the parameters in the query, if any
/// were present.
///
/// Note that the returned `RelationDesc` describes the expression after
/// applying the returned `RowSetFinishing`.
pub fn plan_root_query(
    catalog: &Catalog,
    mut query: Query,
) -> Result<(RelationExpr, RelationDesc, RowSetFinishing, Vec<ScalarType>), failure::Error> {
    crate::transform::transform(&mut query);
    let qcx = QueryContext::root();
    let (expr, scope, finishing) = plan_query(catalog, &qcx, &query)?;
    let typ = qcx.relation_type(&expr);
    let typ = RelationType::new(
        finishing
            .project
            .iter()
            .map(|i| typ.column_types[*i])
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

fn plan_expr_or_col_index<'a>(
    catalog: &Catalog,
    ecx: &ExprContext,
    e: &'a Expr,
    type_hint: Option<ScalarType>,
    clause_name: &str,
) -> Result<ScalarExpr, failure::Error> {
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
            Some(Ok(ScalarExpr::Column(ColumnRef::Inner(n - 1))))
        }
        _ => None,
    }
    .unwrap_or_else(|| plan_expr(catalog, ecx, e, type_hint))
}

fn plan_query(
    catalog: &Catalog,
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
    let (expr, scope) = plan_set_expr(catalog, qcx, &q.body)?;
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
        let expr = plan_expr_or_col_index(
            catalog,
            ecx,
            &obe.expr,
            Some(ScalarType::String),
            "ORDER BY",
        )?;
        // If the expression is a reference to an existing column,
        // do not introduce a new column to support it.
        if let ScalarExpr::Column(ColumnRef::Inner(column)) = expr {
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

fn plan_subquery(
    catalog: &Catalog,
    qcx: &QueryContext,
    q: &Query,
) -> Result<(RelationExpr, Scope), failure::Error> {
    let (mut expr, scope, finishing) = plan_query(catalog, qcx, q)?;
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

fn plan_set_expr(
    catalog: &Catalog,
    qcx: &QueryContext,
    q: &SetExpr,
) -> Result<(RelationExpr, Scope), failure::Error> {
    match q {
        SetExpr::Select(select) => plan_view_select(catalog, qcx, select),
        SetExpr::SetOperation {
            op,
            all,
            left,
            right,
        } => {
            let (left_expr, left_scope) = plan_set_expr(catalog, qcx, left)?;
            let (right_expr, _right_scope) = plan_set_expr(catalog, qcx, right)?;

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
                if left_col_type.union(*right_col_type).is_err() {
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
            let tn: Option<QualName> = None;
            let scope = Scope::from_source(
                tn,
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
                    let expr = plan_expr(catalog, ecx, value, Some(ScalarType::String))?;
                    value_types.push(ecx.column_type(&expr));
                    value_exprs.push(expr);
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
                            .zip(value_types.iter())
                            .map(|(left_typ, right_typ)| left_typ.union(*right_typ))
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
            let (expr, scope) = plan_subquery(catalog, qcx, query)?;
            Ok((expr, scope))
        }
    }
}

fn plan_view_select(
    catalog: &Catalog,
    qcx: &QueryContext,
    s: &Select,
) -> Result<(RelationExpr, Scope), failure::Error> {
    // Step 1. Handle FROM clause, including joins.
    let (mut relation_expr, from_scope) = s
        .from
        .iter()
        .map(|twj| plan_table_with_joins(catalog, qcx, twj))
        .fallible()
        .fold1(|(left, left_scope), (right, right_scope)| {
            plan_join_operator(
                catalog,
                qcx,
                &JoinOperator::CrossJoin,
                left,
                left_scope,
                right,
                right_scope,
            )
        })
        .unwrap_or_else(|| {
            let typ = RelationType::new(vec![]);
            let tn: Option<QualName> = None;
            Ok((
                RelationExpr::constant(vec![vec![]], typ.clone()),
                Scope::from_source(
                    tn,
                    iter::empty::<Option<ColumnName>>(),
                    Some(qcx.outer_scope.clone()),
                ),
            ))
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
        let expr = plan_expr(catalog, ecx, &selection, Some(ScalarType::Bool))?;
        let typ = ecx.column_type(&expr);
        if typ.scalar_type != ScalarType::Bool && typ.scalar_type != ScalarType::Null {
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
            let expr = plan_expr_or_col_index(
                catalog,
                ecx,
                group_expr,
                Some(ScalarType::String),
                "GROUP BY",
            )?;
            let new_column = group_key.len();
            // repeated exprs in GROUP BY confuse name resolution later, and dropping them doesn't change the result
            if group_exprs
                .iter()
                .find(|existing_expr| **existing_expr == expr)
                .is_none()
            {
                let mut scope_item = if let ScalarExpr::Column(ColumnRef::Inner(old_column)) = &expr
                {
                    // If we later have `SELECT foo.*` then we have to find all the `foo` items in `from_scope` and figure out where they ended up in `group_scope`.
                    // This is really hard to do right using SQL name resolution, so instead we just track the movement here.
                    select_all_mapping.insert(*old_column, new_column);
                    ecx.scope.items[*old_column].clone()
                } else {
                    ScopeItem::from_column_name(None)
                };
                scope_item.expr = Some(group_expr.clone());

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
            aggregates.push(plan_aggregate(catalog, ecx, sql_function)?);
            group_scope.items.push(ScopeItem {
                names: vec![ScopeItemName {
                    table_name: None,
                    column_name: Some(sql_function.name.to_string().into()),
                }],
                expr: Some(Expr::Function(sql_function.clone())),
            });
        }
        if !aggregates.is_empty() || !group_key.is_empty() || s.having.is_some() {
            // apply GROUP BY / aggregates
            relation_expr = relation_expr
                .map(group_exprs)
                .reduce(group_key.clone(), aggregates.clone());
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
        let expr = plan_expr(catalog, ecx, having, Some(ScalarType::Bool))?;
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
            for (expr, scope_item) in
                plan_select_item(catalog, ecx, p, &from_scope, &select_all_mapping)?
            {
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

pub fn plan_index<'a>(
    catalog: &'a Catalog,
    on_name: &QualName,
    key_parts: &[Expr],
) -> Result<(&'a CatalogEntry, Vec<usize>, Vec<ScalarExpr>), failure::Error> {
    let item = catalog.get(on_name)?;
    let desc = item.desc()?;
    let scope = Scope::from_source(Some(on_name), desc.iter_names(), Some(Scope::empty(None)));
    let qcx = &QueryContext::root();
    let ecx = &ExprContext {
        qcx: &qcx,
        name: "CREATE INDEX",
        scope: &scope,
        relation_type: desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
    };

    let mut map_exprs = vec![];
    let mut arrange_cols = vec![];
    let mut func_count = key_parts.len();

    for key_part in key_parts {
        let expr = plan_expr(catalog, ecx, key_part, Some(ScalarType::String))?;
        if let ScalarExpr::Column(ColumnRef::Inner(i)) = &expr {
            arrange_cols.push(*i);
        } else {
            map_exprs.push(expr);
            arrange_cols.push(func_count);
            func_count += 1;
        }
    }
    Ok((item, arrange_cols, map_exprs))
}

fn plan_table_with_joins<'a>(
    catalog: &Catalog,
    qcx: &QueryContext,
    table_with_joins: &'a TableWithJoins,
) -> Result<(RelationExpr, Scope), failure::Error> {
    let (mut left, mut left_scope) = plan_table_factor(catalog, qcx, &table_with_joins.relation)?;
    for join in &table_with_joins.joins {
        let (right, right_scope) = plan_table_factor(catalog, qcx, &join.relation)?;
        let (new_left, new_left_scope) = plan_join_operator(
            catalog,
            qcx,
            &join.join_operator,
            left,
            left_scope,
            right,
            right_scope,
        )?;
        left = new_left;
        left_scope = new_left_scope;
    }
    Ok((left, left_scope))
}

fn plan_table_factor<'a>(
    catalog: &Catalog,
    qcx: &QueryContext,
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
            let name = QualName::try_from(name.clone())?;
            let item = catalog.get(&name)?;
            let expr = RelationExpr::Get {
                id: Id::Global(item.id()),
                typ: item.desc()?.typ().clone(),
            };
            let alias: QualName = if let Some(TableAlias { name, columns }) = alias {
                if !columns.is_empty() {
                    bail!("aliasing columns is not yet supported");
                }
                name.clone().try_into()?
            } else {
                name
            };
            let scope = Scope::from_source(
                Some(&alias),
                item.desc()?.iter_names(),
                Some(qcx.outer_scope.clone()),
            );
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
            let (expr, scope) = plan_subquery(catalog, &qcx, &subquery)?;
            let alias: Option<QualName> = if let Some(TableAlias { name, columns }) = alias {
                if !columns.is_empty() {
                    bail!("aliasing columns is not yet supported");
                }
                let qn: QualName = name.try_into()?;
                Some(qn)
            } else {
                None
            };
            let scope =
                Scope::from_source(alias, scope.column_names(), Some(qcx.outer_scope.clone()));
            Ok((expr, scope))
        }
        TableFactor::NestedJoin(table_with_joins) => {
            plan_table_with_joins(catalog, qcx, table_with_joins)
        }
    }
}

fn plan_select_item<'a>(
    catalog: &Catalog,
    ecx: &ExprContext,
    s: &'a SelectItem,
    select_all_scope: &Scope,
    select_all_mapping: &BTreeMap<usize, usize>,
) -> Result<Vec<(ScalarExpr, ScopeItem)>, failure::Error> {
    match s {
        SelectItem::UnnamedExpr(sql_expr) => {
            let expr = plan_expr(catalog, ecx, sql_expr, Some(ScalarType::String))?;
            let mut scope_item = if let ScalarExpr::Column(ColumnRef::Inner(i)) = &expr {
                ecx.scope.items[*i].clone()
            } else {
                ScopeItem::from_column_name(None)
            };
            scope_item.expr = Some(sql_expr.clone());
            Ok(vec![(expr, scope_item)])
        }
        SelectItem::ExprWithAlias {
            expr: sql_expr,
            alias,
        } => {
            let expr = plan_expr(catalog, ecx, sql_expr, Some(ScalarType::String))?;
            let mut scope_item = if let ScalarExpr::Column(ColumnRef::Inner(i)) = &expr {
                ecx.scope.items[*i].clone()
            } else {
                ScopeItem::from_column_name(None)
            };
            scope_item.names.insert(
                0,
                ScopeItemName {
                    table_name: None,
                    column_name: Some(names::ident_to_col_name(alias.clone())),
                },
            );
            scope_item.expr = Some(sql_expr.clone());
            Ok(vec![(expr, scope_item)])
        }
        SelectItem::Wildcard => select_all_scope
            .items
            .iter()
            .enumerate()
            .map(|(i, item)| {
                let j = select_all_mapping.get(&i).ok_or_else(|| {
                    format_err!("internal error: unable to resolve scope item {:?}", item)
                })?;
                let mut scope_item = item.clone();
                scope_item.expr = None;
                Ok((ScalarExpr::Column(ColumnRef::Inner(*j)), scope_item))
            })
            .collect::<Result<Vec<_>, _>>(),
        SelectItem::QualifiedWildcard(table_name) => {
            let table_name = Some(QualName::try_from(table_name)?);
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
                    let mut scope_item = item.clone();
                    scope_item.expr = None;
                    Ok((ScalarExpr::Column(ColumnRef::Inner(*j)), scope_item))
                })
                .collect::<Result<Vec<_>, _>>()
        }
    }
}

fn plan_join_operator(
    catalog: &Catalog,
    qcx: &QueryContext,
    operator: &JoinOperator,
    left: RelationExpr,
    left_scope: Scope,
    right: RelationExpr,
    right_scope: Scope,
) -> Result<(RelationExpr, Scope), failure::Error> {
    match operator {
        JoinOperator::Inner(constraint) => plan_join_constraint(
            catalog,
            qcx,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            JoinKind::Inner,
        ),
        JoinOperator::LeftOuter(constraint) => plan_join_constraint(
            catalog,
            qcx,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            JoinKind::LeftOuter,
        ),
        JoinOperator::RightOuter(constraint) => plan_join_constraint(
            catalog,
            qcx,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            JoinKind::RightOuter,
        ),
        JoinOperator::FullOuter(constraint) => plan_join_constraint(
            catalog,
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
    catalog: &Catalog,
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
            let on = plan_expr(catalog, ecx, expr, Some(ScalarType::Bool))?;
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
                let right_names = std::mem::replace(&mut product_scope.items[r].names, Vec::new());
                product_scope.items[l].names.extend(right_names);
            }
            let joined = RelationExpr::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                kind: kind.clone(),
            };
            (joined, product_scope)
        }
        JoinConstraint::Using(column_names) => plan_using_constraint(
            catalog,
            &column_names
                .iter()
                .map(|ident| names::ident_to_col_name(ident.clone()))
                .collect::<Vec<_>>(),
            left,
            left_scope,
            right,
            right_scope,
            kind.clone(),
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
                catalog,
                &column_names,
                left,
                left_scope,
                right,
                right_scope,
                kind.clone(),
            )?
        }
    };
    Ok((expr, scope))
}

// See page 440 of ANSI SQL 2016 spec for details on scoping of using/natural joins
#[allow(clippy::too_many_arguments)]
fn plan_using_constraint(
    _: &Catalog,
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
        join_exprs.push(ScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1: Box::new(ScalarExpr::Column(ColumnRef::Inner(l))),
            expr2: Box::new(ScalarExpr::Column(ColumnRef::Inner(left_scope.len() + r))),
        });
        map_exprs.push(ScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce,
            exprs: vec![
                ScalarExpr::Column(ColumnRef::Inner(l)),
                ScalarExpr::Column(ColumnRef::Inner(left_scope.len() + r)),
            ],
        });
        let mut names = l_item.names.clone();
        names.extend(r_item.names.clone());
        new_items.push(ScopeItem { names, expr: None });
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
    catalog: &Catalog,
    ecx: &ExprContext,
    e: &'a Expr,
    type_hint: Option<ScalarType>,
) -> Result<ScalarExpr, failure::Error> {
    if let Some((i, _)) = ecx.scope.resolve_expr(e) {
        // surprise - we already calculated this expr before
        Ok(ScalarExpr::Column(i))
    } else {
        match e {
            Expr::Identifier(name) => {
                let (i, _) = ecx
                    .scope
                    .resolve_column(&names::ident_to_col_name(name.clone()))?;
                Ok(ScalarExpr::Column(i))
            }
            Expr::CompoundIdentifier(names) => {
                if names.len() == 2 {
                    let (i, _) = ecx.scope.resolve_table_column(
                        &(&names[0]).try_into()?,
                        &names::ident_to_col_name(names[1].clone()),
                    )?;
                    Ok(ScalarExpr::Column(i))
                } else {
                    bail!(
                        "compound identifier {} with more than two identifiers is not supported",
                        e
                    );
                }
            }
            Expr::Value(val) => plan_literal(catalog, val),
            Expr::Wildcard { .. } | Expr::QualifiedWildcard(_) => {
                bail!("wildcard in invalid position")
            }
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
                Ok(ScalarExpr::Parameter(*n))
            }
            // TODO(benesch): why isn't IS [NOT] NULL a unary op?
            Expr::IsNull(expr) => plan_is_null_expr(catalog, ecx, expr, false),
            Expr::IsNotNull(expr) => plan_is_null_expr(catalog, ecx, expr, true),
            Expr::UnaryOp { op, expr } => plan_unary_op(catalog, ecx, op, expr),
            Expr::BinaryOp { op, left, right } => plan_binary_op(catalog, ecx, op, left, right),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => plan_between(catalog, ecx, expr, low, high, *negated),
            Expr::InList {
                expr,
                list,
                negated,
            } => plan_in_list(catalog, ecx, expr, list, *negated),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => plan_case(catalog, ecx, operand, conditions, results, else_result),
            Expr::Nested(expr) => plan_expr(catalog, ecx, expr, type_hint),
            Expr::Cast { expr, data_type } => plan_cast(catalog, ecx, expr, data_type),
            Expr::Function(func) => plan_function(catalog, ecx, func),
            Expr::Exists(query) => {
                if !ecx.allow_subqueries {
                    bail!("{} does not allow subqueries", ecx.name)
                }
                let qcx = ecx.derived_query_context();
                let (expr, _scope) = plan_subquery(catalog, &qcx, query)?;
                Ok(expr.exists())
            }
            Expr::Subquery(query) => {
                if !ecx.allow_subqueries {
                    bail!("{} does not allow subqueries", ecx.name)
                }
                let qcx = ecx.derived_query_context();
                let (expr, _scope) = plan_subquery(catalog, &qcx, query)?;
                let column_types = qcx.relation_type(&expr).column_types;
                if column_types.len() != 1 {
                    bail!(
                        "Expected subselect to return 1 column, got {} columns",
                        column_types.len()
                    );
                }
                Ok(expr.select())
            }
            Expr::Any {
                left,
                op,
                right,
                some: _,
            } => plan_any_or_all(catalog, ecx, left, op, right, AggregateFunc::Any),
            Expr::All { left, op, right } => {
                plan_any_or_all(catalog, ecx, left, op, right, AggregateFunc::All)
            }
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
                    plan_any_or_all(catalog, ecx, expr, &NotEq, subquery, AggregateFunc::All)
                } else {
                    // `<expr> IN (<subquery>)` is equivalent to
                    // `<expr> = ANY (<subquery>)`.
                    plan_any_or_all(catalog, ecx, expr, &Eq, subquery, AggregateFunc::Any)
                }
            }
            Expr::Extract { field, expr } => {
                // No type hint passed to `plan_expr`, because `expr` can be
                // any date type. PostgreSQL is also unable to infer parameter
                // types in this position.
                let mut expr = plan_expr(catalog, ecx, expr, None)?;
                let mut typ = ecx.column_type(&expr);
                if let ScalarType::Date = typ.scalar_type {
                    expr = plan_cast_internal(ecx, "EXTRACT", expr, ScalarType::Timestamp)?;
                    typ = ecx.column_type(&expr);
                }
                let func = match &typ.scalar_type {
                    ScalarType::Interval => match field {
                        DateTimeField::Year => UnaryFunc::ExtractIntervalYear,
                        DateTimeField::Month => UnaryFunc::ExtractIntervalMonth,
                        DateTimeField::Day => UnaryFunc::ExtractIntervalDay,
                        DateTimeField::Hour => UnaryFunc::ExtractIntervalHour,
                        DateTimeField::Minute => UnaryFunc::ExtractIntervalMinute,
                        DateTimeField::Second => UnaryFunc::ExtractIntervalSecond,
                    },
                    ScalarType::Timestamp => match field {
                        DateTimeField::Year => UnaryFunc::ExtractTimestampYear,
                        DateTimeField::Month => UnaryFunc::ExtractTimestampMonth,
                        DateTimeField::Day => UnaryFunc::ExtractTimestampDay,
                        DateTimeField::Hour => UnaryFunc::ExtractTimestampHour,
                        DateTimeField::Minute => UnaryFunc::ExtractTimestampMinute,
                        DateTimeField::Second => UnaryFunc::ExtractTimestampSecond,
                    },
                    ScalarType::TimestampTz => match field {
                        DateTimeField::Year => UnaryFunc::ExtractTimestampTzYear,
                        DateTimeField::Month => UnaryFunc::ExtractTimestampTzMonth,
                        DateTimeField::Day => UnaryFunc::ExtractTimestampTzDay,
                        DateTimeField::Hour => UnaryFunc::ExtractTimestampTzHour,
                        DateTimeField::Minute => UnaryFunc::ExtractTimestampTzMinute,
                        DateTimeField::Second => UnaryFunc::ExtractTimestampTzSecond,
                    },
                    other => bail!(
                        "EXTRACT expects timestamp, interval, or date input, got {:?}",
                        other
                    ),
                };
                Ok(expr.call_unary(func))
            }
            Expr::Collate { .. } => bail!("COLLATE is not yet supported"),
        }
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
fn plan_homogeneous_exprs<S>(
    name: S,
    catalog: &Catalog,
    ecx: &ExprContext,
    exprs: &[impl std::borrow::Borrow<Expr>],
    type_hint: Option<ScalarType>,
) -> Result<Vec<ScalarExpr>, failure::Error>
where
    S: fmt::Display + Copy,
{
    assert!(!exprs.is_empty());

    let mut pending = vec![None; exprs.len()];

    // Compute the types of all known expressions.
    for (i, expr) in exprs.iter().enumerate() {
        if expr_has_unknown_type(ecx, expr.borrow()) {
            continue;
        }
        let expr = plan_expr(catalog, ecx, expr.borrow(), None)?;
        let typ = ecx.column_type(&expr);
        pending[i] = Some((expr, typ));
    }

    // Determine the best target type. If all the expressions were parameters,
    // fall back to `type_hint`.
    let best_target_type = best_target_type(
        pending
            .iter()
            .filter_map(|slot| slot.as_ref().map(|(_expr, typ)| typ.scalar_type)),
    )
    .or(type_hint);

    // Plan all the parameter expressions with `best_target_type` as the type
    // hint.
    for (i, slot) in pending.iter_mut().enumerate() {
        if slot.is_none() {
            let expr = plan_expr(catalog, ecx, exprs[i].borrow(), best_target_type)?;
            let typ = ecx.column_type(&expr);
            *slot = Some((expr, typ));
        }
    }

    // Try to cast all expressions to `best_target_type`.
    let mut out = Vec::new();
    for (expr, typ) in pending.into_iter().map(Option::unwrap) {
        match plan_cast_internal(ecx, name, expr, best_target_type.unwrap()) {
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
    catalog: &Catalog,
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

    let (right, _scope) = plan_subquery(catalog, &qcx, right)?;
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
        catalog,
        &any_ecx,
        op,
        left,
        &Expr::Identifier(Ident::new(right_name)),
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
    catalog: &Catalog,
    ecx: &ExprContext,
    expr: &'a Expr,
    data_type: &'a DataType,
) -> Result<ScalarExpr, failure::Error> {
    let to_scalar_type = scalar_type_from_sql(data_type)?;
    let expr = plan_expr(catalog, ecx, expr, Some(to_scalar_type))?;
    plan_cast_internal(ecx, "CAST", expr, to_scalar_type)
}

fn plan_aggregate(
    catalog: &Catalog,
    ecx: &ExprContext,
    sql_func: &Function,
) -> Result<AggregateExpr, failure::Error> {
    let name = QualName::try_from(&sql_func.name)?;
    let ident = name.as_ident_str()?;
    assert!(is_aggregate_func(&name));

    if sql_func.over.is_some() {
        bail!("window functions are not yet supported");
    }

    if sql_func.args.len() != 1 {
        bail!("{} function only takes one argument", ident);
    }

    let arg = &sql_func.args[0];
    let (expr, func) = match (ident, arg) {
        // COUNT(*) is a special case that doesn't compose well
        ("count", Expr::Wildcard) => (ScalarExpr::literal_null(), AggregateFunc::CountAll),
        _ => {
            // No type hint passed to `plan_expr`, because all aggregates accept
            // multiple input types. PostgreSQL is also unable to infer
            // parameter types in this position.
            let expr = plan_expr(catalog, ecx, arg, None)?;
            let typ = ecx.column_type(&expr);
            let func = find_agg_func(&ident, typ.scalar_type)?;
            (expr, func)
        }
    };
    Ok(AggregateExpr {
        func,
        expr: Box::new(expr),
        distinct: sql_func.distinct,
    })
}

fn plan_function<'a>(
    catalog: &Catalog,
    ecx: &ExprContext,
    sql_func: &'a Function,
) -> Result<ScalarExpr, failure::Error> {
    let name = QualName::try_from(&sql_func.name)?;
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
    } else {
        match ident {
            "abs" => {
                if sql_func.args.len() != 1 {
                    bail!("abs expects one argument, got {}", sql_func.args.len());
                }
                let expr = plan_expr(catalog, ecx, &sql_func.args[0], Some(ScalarType::Float64))?;
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
                if sql_func.args.len() != 1 {
                    bail!("ascii expects one argument, got {}", sql_func.args.len());
                }
                let expr = plan_expr(catalog, ecx, &sql_func.args[0], Some(ScalarType::String))?;
                let typ = ecx.column_type(&expr);
                if typ.scalar_type != ScalarType::String && typ.scalar_type != ScalarType::Null {
                    bail!("ascii does not accept arguments of type {:?}", typ);
                }
                let expr = ScalarExpr::CallUnary {
                    func: UnaryFunc::Ascii,
                    expr: Box::new(expr),
                };
                Ok(expr)
            }

            "coalesce" => {
                if sql_func.args.is_empty() {
                    bail!("coalesce requires at least one argument");
                }
                let expr = ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: plan_homogeneous_exprs(
                        "coalesce",
                        catalog,
                        ecx,
                        &sql_func.args,
                        Some(ScalarType::String),
                    )?,
                };
                Ok(expr)
            }

            "current_timestamp" | "now" => Ok(ScalarExpr::Literal(
                Row::pack(&[Datum::TimestampTz(ecx.qcx.current_timestamp)]),
                ColumnType::new(ScalarType::TimestampTz),
            )),

            "mod" => {
                if sql_func.args.len() != 2 {
                    bail!("mod requires exactly two arguments");
                }
                plan_binary_op(
                    catalog,
                    ecx,
                    &BinaryOperator::Modulus,
                    &sql_func.args[0],
                    &sql_func.args[1],
                )
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
                let cond_expr = plan_expr(catalog, ecx, &cond, None)?;
                let else_expr = plan_expr(catalog, ecx, &sql_func.args[0], None)?;
                let expr = ScalarExpr::If {
                    cond: Box::new(cond_expr),
                    then: Box::new(ScalarExpr::literal_null()),
                    els: Box::new(else_expr),
                };
                Ok(expr)
            }

            "substr" => {
                let func = Function {
                    name: ObjectName(vec![Ident::new("substring")]),
                    args: sql_func.args.clone(),
                    over: sql_func.over.clone(),
                    distinct: sql_func.distinct,
                };
                plan_function(catalog, ecx, &func)
            }

            "substring" => {
                if sql_func.args.len() < 2 || sql_func.args.len() > 3 {
                    bail!(
                        "substring expects two or three arguments, got {:?}",
                        sql_func.args.len()
                    );
                }
                let mut exprs = Vec::new();
                let expr1 = plan_expr(catalog, ecx, &sql_func.args[0], Some(ScalarType::String))?;
                let typ1 = ecx.column_type(&expr1);
                if typ1.scalar_type != ScalarType::String && typ1.scalar_type != ScalarType::Null {
                    bail!("substring first argument has non-string type {:?}", typ1);
                }
                exprs.push(expr1);

                let expr2 = plan_expr(catalog, ecx, &sql_func.args[1], Some(ScalarType::Int64))?;
                let expr2 = promote_int_int64(ecx, "substring start", expr2)?;
                exprs.push(expr2);
                if sql_func.args.len() == 3 {
                    let expr3 =
                        plan_expr(catalog, ecx, &sql_func.args[2], Some(ScalarType::Int64))?;
                    let expr3 = promote_int_int64(ecx, "substring length", expr3)?;
                    exprs.push(expr3);
                }
                let expr = ScalarExpr::CallVariadic {
                    func: VariadicFunc::Substr,
                    exprs,
                };
                Ok(expr)
            }

            "length" => {
                if sql_func.args.is_empty() || sql_func.args.len() > 2 {
                    bail!(
                        "length expects one or two arguments, got {:?}",
                        sql_func.args.len()
                    );
                }

                let mut exprs = Vec::new();
                let expr1 = plan_expr(catalog, ecx, &sql_func.args[0], Some(ScalarType::String))?;
                let typ1 = ecx.column_type(&expr1);
                if typ1.scalar_type != ScalarType::String && typ1.scalar_type != ScalarType::Null {
                    bail!("length first argument has non-string type {:?}", typ1);
                }
                exprs.push(expr1);

                if sql_func.args.len() == 2 {
                    let expr2 =
                        plan_expr(catalog, ecx, &sql_func.args[1], Some(ScalarType::String))?;
                    let typ2 = ecx.column_type(&expr2);
                    if typ2.scalar_type != ScalarType::String
                        && typ2.scalar_type != ScalarType::Null
                    {
                        bail!("length second argument has non-string type {:?}", typ1);
                    }
                    exprs.push(expr2);
                }
                let expr = ScalarExpr::CallVariadic {
                    func: VariadicFunc::Length,
                    exprs,
                };
                Ok(expr)
            }

            // Promotes a numeric type to the smallest fractional type that
            // can represent it. This is primarily useful for the avg
            // aggregate function, so that the avg of an integer column does
            // not get truncated to an integer, which would be surprising to
            // users (#549).
            "internal.avg_promotion" => {
                if sql_func.args.len() != 1 {
                    bail!("internal.avg_promotion requires exactly one argument");
                }
                let expr = plan_expr(catalog, ecx, &sql_func.args[0], None)?;
                let typ = ecx.column_type(&expr);
                let output_type = match &typ.scalar_type {
                    ScalarType::Null => ScalarType::Null,
                    ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
                    ScalarType::Decimal(p, s) => ScalarType::Decimal(*p, *s),
                    ScalarType::Int32 => ScalarType::Decimal(10, 0),
                    ScalarType::Int64 => ScalarType::Decimal(19, 0),
                    _ => bail!("internal.avg_promotion called with unexpected argument"),
                };
                plan_cast_internal(ecx, "internal.avg_promotion", expr, output_type)
            }

            // Currently only implement this specific case from Metabase:
            // to_char(current_timestamp(), 'YYYY-MM-DD HH24:MI:SS.MS TZ')
            "to_char" => {
                if sql_func.args.len() != 2 {
                    bail!("to_char requires exactly two arguments");
                }

                // &sql_func.args[0] should be current_timestamp()/now()
                let timestamp_func = plan_expr(
                    catalog,
                    ecx,
                    &sql_func.args[0],
                    Some(ScalarType::TimestampTz),
                )?;
                let typ = ecx.column_type(&timestamp_func);
                if typ.scalar_type != ScalarType::TimestampTz && typ.scalar_type != ScalarType::Null
                {
                    bail!("to_char() currently only implemented for timestamps");
                }

                let format_string =
                    plan_expr(catalog, ecx, &sql_func.args[1], Some(ScalarType::String))?;
                let typ = ecx.column_type(&format_string);
                if typ.scalar_type != ScalarType::String && typ.scalar_type != ScalarType::Null {
                    bail!("to_char() requires a format string as the second parameter");
                }

                let expr = ScalarExpr::CallBinary {
                    func: BinaryFunc::ToChar,
                    expr1: Box::new(timestamp_func),
                    expr2: Box::new(format_string),
                };
                Ok(expr)
            }

            "date_trunc" => {
                if sql_func.args.len() != 2 {
                    bail!("date_trunc() requires exactly two arguments");
                }

                let precision_field =
                    plan_expr(catalog, ecx, &sql_func.args[0], Some(ScalarType::String))?;
                let typ = ecx.column_type(&precision_field);
                if typ.scalar_type != ScalarType::String {
                    bail!("date_trunc() can only be formatted with strings");
                }

                let source_timestamp =
                    plan_expr(catalog, ecx, &sql_func.args[1], Some(ScalarType::Timestamp))?;
                let typ = ecx.column_type(&source_timestamp);
                if typ.scalar_type != ScalarType::Timestamp {
                    bail!("date_trunc() is currently only implemented for TIMESTAMPs");
                }

                let expr = ScalarExpr::CallBinary {
                    func: BinaryFunc::DateTrunc,
                    expr1: Box::new(precision_field),
                    expr2: Box::new(source_timestamp),
                };

                Ok(expr)
            }

            _ => bail!("unsupported function: {}", ident),
        }
    }
}

fn plan_is_null_expr<'a>(
    catalog: &Catalog,
    ecx: &ExprContext,
    inner: &'a Expr,
    not: bool,
) -> Result<ScalarExpr, failure::Error> {
    // No type hint passed to `plan_expr`. In other situations where any type
    // will do, PostgreSQL uses `ScalarType::String`, but for some reason it
    // does not here.
    let expr = plan_expr(catalog, ecx, inner, None)?;
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
    catalog: &Catalog,
    ecx: &ExprContext,
    op: &'a UnaryOperator,
    expr: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let type_hint = match op {
        UnaryOperator::Not => ScalarType::Bool,
        UnaryOperator::Plus | UnaryOperator::Minus => ScalarType::Float64,
    };
    let expr = plan_expr(catalog, ecx, expr, Some(type_hint))?;
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
    catalog: &Catalog,
    ecx: &ExprContext,
    op: &'a BinaryOperator,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    use BinaryOperator::*;
    match op {
        And => plan_boolean_op(catalog, ecx, BooleanOp::And, left, right),
        Or => plan_boolean_op(catalog, ecx, BooleanOp::Or, left, right),

        Plus => plan_arithmetic_op(catalog, ecx, ArithmeticOp::Plus, left, right),
        Minus => plan_arithmetic_op(catalog, ecx, ArithmeticOp::Minus, left, right),
        Multiply => plan_arithmetic_op(catalog, ecx, ArithmeticOp::Multiply, left, right),
        Divide => plan_arithmetic_op(catalog, ecx, ArithmeticOp::Divide, left, right),
        Modulus => plan_arithmetic_op(catalog, ecx, ArithmeticOp::Modulo, left, right),

        Lt => plan_comparison_op(catalog, ecx, ComparisonOp::Lt, left, right),
        LtEq => plan_comparison_op(catalog, ecx, ComparisonOp::LtEq, left, right),
        Gt => plan_comparison_op(catalog, ecx, ComparisonOp::Gt, left, right),
        GtEq => plan_comparison_op(catalog, ecx, ComparisonOp::GtEq, left, right),
        Eq => plan_comparison_op(catalog, ecx, ComparisonOp::Eq, left, right),
        NotEq => plan_comparison_op(catalog, ecx, ComparisonOp::NotEq, left, right),

        Like => plan_like(catalog, ecx, left, right, false),
        NotLike => plan_like(catalog, ecx, left, right, true),
    }
}

fn plan_boolean_op<'a>(
    catalog: &Catalog,
    ecx: &ExprContext,
    op: BooleanOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let lexpr = plan_expr(catalog, ecx, left, Some(ScalarType::Bool))?;
    let rexpr = plan_expr(catalog, ecx, right, Some(ScalarType::Bool))?;
    let ltype = ecx.column_type(&lexpr);
    let rtype = ecx.column_type(&rexpr);

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
        BooleanOp::And => BinaryFunc::And,
        BooleanOp::Or => BinaryFunc::Or,
    };
    let expr = lexpr.call_binary(rexpr, func);
    Ok(expr)
}

fn plan_arithmetic_op<'a>(
    catalog: &Catalog,
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
            plan_expr(catalog, ecx, left, None)?;
            unreachable!();
        }

        // Neither input is a parameter of an unknown type.
        (false, false) => {
            let mut lexpr = plan_expr(catalog, ecx, left, None)?;
            let mut rexpr = plan_expr(catalog, ecx, right, None)?;
            let mut ltype = ecx.column_type(&lexpr);
            let mut rtype = ecx.column_type(&rexpr);

            let both_decimals = match (&ltype.scalar_type, &rtype.scalar_type) {
                (Decimal(_, _), Decimal(_, _)) => true,
                _ => false,
            };
            let timelike_and_interval = match (&ltype.scalar_type, &rtype.scalar_type) {
                (Date, Interval)
                | (Timestamp, Interval)
                | (TimestampTz, Interval)
                | (Interval, Date)
                | (Interval, Timestamp)
                | (Interval, TimestampTz) => true,
                _ => false,
            };
            if both_decimals || timelike_and_interval {
                // When both inputs are already decimals, we skip coalescing,
                // which could result in a rescale if the decimals have
                // different precisions, because we tightly control the rescale
                // when planning the arithmetic operation (below). E.g., decimal
                // multiplication does not need to rescale its inputs, even when
                // the inputs have different scales.
                //
                // Similarly, if the inputs are a timelike and an interval, we
                // skip coalescing, because adding and subtracting intervals and
                // timelikes is well defined.
            } else {
                // Otherwise, try to find a common type that can represent both
                // inputs.
                let best_target_type =
                    best_target_type(vec![ltype.scalar_type, rtype.scalar_type]).unwrap();
                lexpr = match plan_cast_internal(ecx, &op, lexpr, best_target_type) {
                    Ok(expr) => expr,
                    Err(_) => bail!(
                        "{} does not have uniform type: {:?} vs {:?}",
                        &op,
                        ltype.scalar_type,
                        best_target_type,
                    ),
                };
                rexpr = match plan_cast_internal(ecx, &op, rexpr, best_target_type) {
                    Ok(expr) => expr,
                    Err(_) => bail!(
                        "{} does not have uniform type: {:?} vs {:?}",
                        &op,
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
            // Knowing the type of one input is sufficiently constraining to
            // allow us to infer the type of the unknown input.

            // First, adjust so the known input is on the left.
            let (left, right) = if swap { (right, left) } else { (left, right) };

            // Determine the known type.
            let lexpr = plan_expr(catalog, ecx, left, None)?;
            let ltype = ecx.column_type(&lexpr);

            // Infer the unknown type. Dates and timestamps want an interval for
            // arithmetic. Everything else wants its own type.
            let type_hint = match ltype.scalar_type {
                Date | Timestamp | TimestampTz => Interval,
                other => other,
            };
            let rexpr = plan_expr(catalog, ecx, right, Some(type_hint))?;
            let rtype = ecx.column_type(&rexpr);

            (lexpr, ltype, rexpr, rtype)
        }
    };

    // Step 2. Promote dates to intervals.
    //
    // Dates can't be added to intervals, but timestamps can, and dates are
    // trivially promotable to intervals.
    if let Date = &ltype.scalar_type {
        let expr = plan_cast_internal(ecx, &op, lexpr, Timestamp)?;
        ltype = ecx.column_type(&expr);
        lexpr = expr;
    }
    if let Date = &rtype.scalar_type {
        let expr = plan_cast_internal(ecx, &op, rexpr, Timestamp)?;
        rtype = ecx.column_type(&expr);
        rexpr = expr;
    }

    // Step 3a. Plan the arithmetic operation for decimals.
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

    // Step 3b. Plan the arithmetic operation for all other types.
    let func = match op {
        Plus => match (&ltype.scalar_type, &rtype.scalar_type) {
            (Int32, Int32) => AddInt32,
            (Int64, Int64) => AddInt64,
            (Float32, Float32) => AddFloat32,
            (Float64, Float64) => AddFloat64,
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
            (Timestamp, Interval) => SubTimestampInterval,
            (TimestampTz, Interval) => SubTimestampTzInterval,
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
    catalog: &Catalog,
    ecx: &ExprContext,
    op: ComparisonOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let mut exprs =
        plan_homogeneous_exprs(&op, catalog, ecx, &[left, right], Some(ScalarType::String))?;
    assert_eq!(exprs.len(), 2);
    let rexpr = exprs.pop().unwrap();
    let lexpr = exprs.pop().unwrap();
    let rtype = ecx.column_type(&rexpr);
    let ltype = ecx.column_type(&lexpr);

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
    catalog: &Catalog,
    ecx: &ExprContext,
    left: &'a Expr,
    right: &'a Expr,
    negate: bool,
) -> Result<ScalarExpr, failure::Error> {
    let lexpr = plan_expr(catalog, ecx, left, Some(ScalarType::String))?;
    let ltype = ecx.column_type(&lexpr);
    let rexpr = plan_expr(catalog, ecx, right, Some(ScalarType::String))?;
    let rtype = ecx.column_type(&rexpr);

    if (ltype.scalar_type != ScalarType::String && ltype.scalar_type != ScalarType::Null)
        || (rtype.scalar_type != ScalarType::String && rtype.scalar_type != ScalarType::Null)
    {
        bail!(
            "LIKE operator requires two string operators, found: {:?} and {:?}",
            ltype,
            rtype
        );
    }

    let mut expr = ScalarExpr::CallBinary {
        func: BinaryFunc::MatchRegex,
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

fn plan_between<'a>(
    catalog: &Catalog,
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
    plan_expr(catalog, ecx, &both, None)
}

fn plan_in_list<'a>(
    catalog: &Catalog,
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
    plan_expr(catalog, ecx, &cond, None)
}

fn plan_case<'a>(
    catalog: &Catalog,
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
        let cexpr = plan_expr(catalog, ecx, &c, Some(ScalarType::Bool))?;
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
    let mut result_exprs = plan_homogeneous_exprs(
        "CASE",
        catalog,
        ecx,
        &result_exprs,
        Some(ScalarType::String),
    )?;
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

fn plan_literal<'a>(_: &Catalog, l: &'a Value) -> Result<ScalarExpr, failure::Error> {
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
        Value::SingleQuotedString(s) => (Datum::cow_from_str(s), ScalarType::String),
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
                ..
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
        Value::TimestampTz(
            _,
            ParsedTimestamp {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nano,
                timezone_offset_second,
            },
        ) => (
            Datum::from_ymd_hms_nano_tz_offset(
                (*year)
                    .try_into()
                    .map_err(|e| format_err!("Year is too large {}: {}", year, e))?,
                *month,
                *day,
                *hour,
                *minute,
                *second,
                *nano,
                *timezone_offset_second,
            )?,
            ScalarType::TimestampTz,
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
    let typ = ColumnType::new(scalar_type).nullable(nullable);
    let expr = ScalarExpr::literal(datum, typ);
    Ok(expr)
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum BooleanOp {
    And,
    Or,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
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

fn unnest(expr: &Expr) -> &Expr {
    match expr {
        Expr::Nested(expr) => unnest(expr),
        _ => expr,
    }
}

fn best_target_type(iter: impl IntoIterator<Item = ScalarType>) -> Option<ScalarType> {
    iter.into_iter()
        .max_by_key(|scalar_type| match scalar_type {
            ScalarType::Null => 0,
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

/// Plans a cast between two `RelationExpr`s of different types. If it is
/// impossible to cast between the two types, an error is returned.
///
/// Note that `plan_cast_internal` only understands [`ScalarType`]s. If you need
/// to cast between SQL [`DataType`]s, see [`Planner::plan_cast`].
fn plan_cast_internal<'a, S>(
    ecx: &ExprContext<'a>,
    name: S,
    expr: ScalarExpr,
    to_scalar_type: ScalarType,
) -> Result<ScalarExpr, failure::Error>
where
    S: fmt::Display + Copy,
{
    use ScalarType::*;
    use UnaryFunc::*;
    let from_scalar_type = ecx.column_type(&expr).scalar_type;
    let expr = match (from_scalar_type, to_scalar_type) {
        (Int32, Float32) => expr.call_unary(CastInt32ToFloat32),
        (Int32, Float64) => expr.call_unary(CastInt32ToFloat64),
        (Int32, Int64) => expr.call_unary(CastInt32ToInt64),
        (Int32, Decimal(_, s)) => rescale_decimal(expr.call_unary(CastInt32ToDecimal), 0, s),
        (Int64, Decimal(_, s)) => rescale_decimal(expr.call_unary(CastInt64ToDecimal), 0, s),
        (Int64, Float32) => expr.call_unary(CastInt64ToFloat32),
        (Int64, Float64) => expr.call_unary(CastInt64ToFloat64),
        (Int64, Int32) => expr.call_unary(CastInt64ToInt32),
        (Float32, Int64) => expr.call_unary(CastFloat32ToInt64),
        (Float32, Float64) => expr.call_unary(CastFloat32ToFloat64),
        (Float64, Int64) => expr.call_unary(CastFloat64ToInt64),
        (Decimal(_, s), Int32) => rescale_decimal(expr, s, 0).call_unary(CastDecimalToInt32),
        (Decimal(_, s), Int64) => rescale_decimal(expr, s, 0).call_unary(CastDecimalToInt64),
        (Decimal(_, s), Float32) => {
            let factor = 10_f32.powi(i32::from(s));
            let factor = ScalarExpr::literal(Datum::from(factor), ColumnType::new(to_scalar_type));
            expr.call_unary(CastDecimalToFloat32)
                .call_binary(factor, BinaryFunc::DivFloat32)
        }
        (Decimal(_, s), Float64) => {
            let factor = 10_f64.powi(i32::from(s));
            let factor = ScalarExpr::literal(Datum::from(factor), ColumnType::new(to_scalar_type));
            expr.call_unary(CastDecimalToFloat64)
                .call_binary(factor, BinaryFunc::DivFloat64)
        }
        (Decimal(_, s1), Decimal(_, s2)) => rescale_decimal(expr, s1, s2),
        (Date, Timestamp) => expr.call_unary(CastDateToTimestamp),
        (Date, TimestampTz) => expr.call_unary(CastDateToTimestampTz),
        (Timestamp, TimestampTz) => expr.call_unary(CastTimestampToTimestampTz),
        (Null, _) => {
            // assert_eq!(expr, ScalarExpr::Literal(Datum::Null, ColumnType::new(ScalarType::Null)));
            ScalarExpr::literal(Datum::Null, ColumnType::new(to_scalar_type).nullable(true))
        }
        (from, to) if from == to => expr,
        (from, to) => {
            bail!(
                "{} does not support casting from {:?} to {:?}",
                name,
                from,
                to
            );
        }
    };
    Ok(expr)
}

fn promote_int_int64<'a, S>(
    ecx: &ExprContext<'a>,
    name: S,
    expr: ScalarExpr,
) -> Result<ScalarExpr, failure::Error>
where
    S: fmt::Display + Copy,
{
    Ok(match ecx.column_type(&expr).scalar_type {
        ScalarType::Null | ScalarType::Int64 => expr,
        ScalarType::Int32 => plan_cast_internal(ecx, name, expr, ScalarType::Int64)?,
        other => bail!("{} has non-integer type {:?}", name, other,),
    })
}

fn rescale_decimal(expr: ScalarExpr, s1: u8, s2: u8) -> ScalarExpr {
    if s2 > s1 {
        let typ = ColumnType::new(ScalarType::Decimal(38, s2 - s1));
        let factor = 10_i128.pow(u32::from(s2 - s1));
        let factor = ScalarExpr::literal(Datum::from(factor), typ);
        expr.call_binary(factor, BinaryFunc::MulDecimal)
    } else if s1 > s2 {
        let typ = ColumnType::new(ScalarType::Decimal(38, s1 - s2));
        let factor = 10_i128.pow(u32::from(s1 - s2));
        let factor = ScalarExpr::literal(Datum::from(factor), typ);
        expr.call_binary(factor, BinaryFunc::DivDecimal)
    } else {
        expr
    }
}

pub fn scalar_type_from_sql(data_type: &DataType) -> Result<ScalarType, failure::Error> {
    // NOTE this needs to stay in sync with sqllogictest::postgres::get_column
    Ok(match data_type {
        DataType::Boolean => ScalarType::Bool,
        DataType::Custom(name) if QualName::name_equals(name.clone(), "bool") => ScalarType::Bool,
        DataType::Custom(name) if QualName::name_equals(name.clone(), "string") => {
            ScalarType::String
        }
        DataType::Char(_) | DataType::Varchar(_) | DataType::Text => ScalarType::String,
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
        DataType::TimestampTz => ScalarType::TimestampTz,
        DataType::Interval => ScalarType::Interval,
        DataType::Bytea => ScalarType::Bytes,
        other @ DataType::Array(_)
        | other @ DataType::Binary(..)
        | other @ DataType::Blob(_)
        | other @ DataType::Clob(_)
        | other @ DataType::Custom(_)
        | other @ DataType::Regclass
        | other @ DataType::Time
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
        if let Ok(name) = QualName::try_from(func.name.clone()) {
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

/// A bundle of unrelated things that we need for planning `Query`s.
#[derive(Debug)]
struct QueryContext {
    /// Always add the current timestamp of the query, available for now() function.
    current_timestamp: DateTime<Utc>,
    /// The scope of the outer relation expression.
    outer_scope: Scope,
    /// The type of the outer relation expression.
    outer_relation_type: RelationType,
    /// The types of the parameters in the query. This is filled in as planning
    /// occurs.
    param_types: Rc<RefCell<BTreeMap<usize, ScalarType>>>,
}

impl QueryContext {
    fn root() -> QueryContext {
        QueryContext {
            current_timestamp: Utc::now(),
            outer_scope: Scope::empty(None),
            outer_relation_type: RelationType::empty(),
            param_types: Rc::new(RefCell::new(BTreeMap::new())),
        }
    }

    fn unwrap_param_types(self) -> BTreeMap<usize, ScalarType> {
        Rc::try_unwrap(self.param_types).unwrap().into_inner()
    }

    fn relation_type(&self, expr: &RelationExpr) -> RelationType {
        expr.typ(&self.outer_relation_type, &self.param_types.borrow())
    }
}

/// A bundle of unrelated things that we need for planning `Expr`s.
#[derive(Debug)]
struct ExprContext<'a> {
    qcx: &'a QueryContext,
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
    fn column_type(&self, expr: &ScalarExpr) -> ColumnType {
        expr.typ(
            &self.qcx.outer_relation_type,
            &self.relation_type,
            &self.qcx.param_types.borrow(),
        )
    }

    fn derived_query_context(&self) -> QueryContext {
        QueryContext {
            current_timestamp: self.qcx.current_timestamp,
            outer_scope: self.scope.clone(),
            outer_relation_type: RelationType::new(
                self.qcx
                    .outer_relation_type
                    .column_types
                    .iter()
                    .cloned()
                    .chain(self.relation_type.column_types.iter().cloned())
                    .collect(),
            ),
            param_types: self.qcx.param_types.clone(),
        }
    }
}

fn is_aggregate_func(name: &QualName) -> bool {
    match name.as_ident_str() {
        // avg is handled by transform::AvgFuncRewriter.
        Ok("max") | Ok("min") | Ok("sum") | Ok("count") => true,
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
        ("max", ScalarType::Null) => AggregateFunc::MaxNull,
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
        ("min", ScalarType::Null) => AggregateFunc::MinNull,
        ("sum", ScalarType::Int32) => AggregateFunc::SumInt32,
        ("sum", ScalarType::Int64) => AggregateFunc::SumInt64,
        ("sum", ScalarType::Float32) => AggregateFunc::SumFloat32,
        ("sum", ScalarType::Float64) => AggregateFunc::SumFloat64,
        ("sum", ScalarType::Decimal(_, _)) => AggregateFunc::SumDecimal,
        ("sum", ScalarType::Null) => AggregateFunc::SumNull,
        ("count", _) => AggregateFunc::Count,
        other => bail!("Unimplemented function/type combo: {:?}", other),
    })
}
