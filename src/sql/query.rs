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

use super::expr::{
    AggregateExpr, AggregateFunc, BinaryFunc, ColumnOrder, ColumnRef, JoinKind, RelationExpr,
    ScalarExpr, UnaryFunc, VariadicFunc, LITERAL_NULL, LITERAL_TRUE,
};
use super::scope::{Scope, ScopeItem, ScopeItemName};
use super::statement::extract_sql_object_name;
use super::store::Catalog;
use dataflow_types::RowSetFinishing;
use failure::{bail, ensure, format_err, ResultExt};
use ore::iter::{FallibleIteratorExt, IteratorExt};
use repr::decimal::MAX_DECIMAL_PRECISION;
use repr::{ColumnType, Datum, RelationType, ScalarType};
use sqlparser::ast::visit::{self, Visit};
use sqlparser::ast::{
    BinaryOperator, DataType, DateTimeField, Expr, Function, Ident, JoinConstraint, JoinOperator,
    ObjectName, ParsedDate, ParsedTimestamp, Query, Select, SelectItem, SetExpr, SetOperator,
    TableAlias, TableFactor, TableWithJoins, UnaryOperator, Value, Values,
};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt;
use std::iter;
use std::mem;
use uuid::Uuid;

pub fn plan_query(
    catalog: &Catalog,
    q: &Query,
    outer_scope: &Scope,
    outer_relation_type: &RelationType,
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
    let (expr, scope) = plan_set_expr(catalog, &q.body, outer_scope, outer_relation_type)?;
    let output_typ = expr.typ(outer_relation_type);
    let mut order_by = vec![];
    let mut map_exprs = vec![];

    for obe in &q.order_by {
        match &obe.expr {
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
                order_by.push(ColumnOrder {
                    column: n - 1,
                    desc: match obe.asc {
                        None => false,
                        Some(asc) => !asc,
                    },
                });
            }
            other => {
                let ctx = &ExprContext {
                    name: "ORDER BY clause",
                    scope: &scope,
                    outer_relation_type,
                    inner_relation_type: &output_typ,
                    allow_aggregates: true,
                };
                let expr = plan_expr(catalog, ctx, other)?;
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
    }

    let finishing = RowSetFinishing {
        filter: vec![],
        order_by,
        limit,
        project: (0..output_typ.column_types.len()).collect(),
        offset,
    };
    Ok((expr.map(map_exprs), scope, finishing))
}

pub fn plan_subquery(
    catalog: &Catalog,
    q: &Query,
    outer_scope: &Scope,
    outer_relation_type: &RelationType,
) -> Result<(RelationExpr, Scope), failure::Error> {
    let (mut expr, scope, finishing) = plan_query(catalog, q, outer_scope, outer_relation_type)?;
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
    q: &SetExpr,
    outer_scope: &Scope,
    outer_relation_type: &RelationType,
) -> Result<(RelationExpr, Scope), failure::Error> {
    match q {
        SetExpr::Select(select) => {
            plan_view_select(catalog, select, outer_scope, outer_relation_type)
        }
        SetExpr::SetOperation {
            op,
            all,
            left,
            right,
        } => {
            let (left_expr, left_scope) =
                plan_set_expr(catalog, left, outer_scope, outer_relation_type)?;
            let (right_expr, _right_scope) =
                plan_set_expr(catalog, right, outer_scope, outer_relation_type)?;

            // TODO(jamii) this type-checking is redundant with RelationExpr::typ, but currently it seems that we need both because RelationExpr::typ is not allowed to return errors
            let left_types = &left_expr.typ(outer_relation_type).column_types;
            let right_types = &right_expr.typ(outer_relation_type).column_types;
            if left_types.len() != right_types.len() {
                bail!(
                    "set operation {:?} with {:?} and {:?} columns not supported",
                    op,
                    left_types.len(),
                    right_types.len(),
                );
            }
            for (left_col_type, right_col_type) in left_types.iter().zip(right_types.iter()) {
                left_col_type.union(*right_col_type)?;
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
                Some(outer_scope.clone()),
            );

            Ok((relation_expr, scope))
        }
        SetExpr::Values(Values(values)) => {
            ensure!(
                !values.is_empty(),
                "Can't infer a type for empty VALUES expression"
            );
            let ctx = &ExprContext {
                name: "values",
                scope: &Scope::empty(Some(outer_scope.clone())),
                outer_relation_type: &RelationType::empty(),
                inner_relation_type: &RelationType::empty(),
                allow_aggregates: false,
            };
            let mut expr: Option<RelationExpr> = None;
            let mut types: Option<Vec<ColumnType>> = None;
            for row in values {
                let mut value_exprs = vec![];
                let mut value_types = vec![];
                for value in row {
                    let expr = plan_expr(catalog, ctx, value)?;
                    value_types.push(ctx.column_type(&expr));
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

                let row_expr = RelationExpr::Constant {
                    rows: vec![vec![]],
                    typ: RelationType::new(vec![]),
                }
                .map(value_exprs);
                expr = if let Some(expr) = expr {
                    Some(expr.union(row_expr))
                } else {
                    Some(row_expr)
                };
            }
            let mut scope = Scope::empty(Some(outer_scope.clone()));
            for i in 0..types.unwrap().len() {
                let name = Some(format!("column{}", i + 1));
                scope.items.push(ScopeItem::from_column_name(name));
            }
            Ok((expr.unwrap(), scope))
        }
        SetExpr::Query(query) => {
            let (expr, scope) = plan_subquery(catalog, query, outer_scope, outer_relation_type)?;
            Ok((expr, scope))
        }
    }
}

fn plan_view_select(
    catalog: &Catalog,
    s: &Select,
    outer_scope: &Scope,
    outer_relation_type: &RelationType,
) -> Result<(RelationExpr, Scope), failure::Error> {
    // Step 1. Handle FROM clause, including joins.
    let (mut relation_expr, from_scope) = s
        .from
        .iter()
        .map(|twj| plan_table_with_joins(catalog, twj, outer_scope, outer_relation_type))
        .fallible()
        .fold1(|(left, left_scope), (right, right_scope)| {
            plan_join_operator(
                catalog,
                &JoinOperator::CrossJoin,
                left,
                left_scope,
                right,
                right_scope,
                outer_relation_type,
            )
        })
        .unwrap_or_else(|| {
            let typ = RelationType::new(vec![]);
            Ok((
                RelationExpr::Constant {
                    rows: vec![vec![]],
                    typ: typ.clone(),
                },
                Scope::from_source(
                    None,
                    iter::empty::<Option<String>>(),
                    Some(outer_scope.clone()),
                ),
            ))
        })?;

    // Step 2. Handle WHERE clause.
    if let Some(selection) = &s.selection {
        let ctx = &ExprContext {
            name: "WHERE clause",
            scope: &from_scope,
            outer_relation_type,
            inner_relation_type: &relation_expr.typ(outer_relation_type),
            allow_aggregates: false,
        };
        let expr = plan_expr(catalog, ctx, &selection)?;
        let typ = ctx.column_type(&expr);
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
        let ctx = &ExprContext {
            name: "GROUP BY clause",
            scope: &from_scope,
            outer_relation_type,
            inner_relation_type: &relation_expr.typ(outer_relation_type),
            allow_aggregates: false,
        };
        let mut group_key = vec![];
        let mut group_exprs = vec![];
        let mut group_scope = Scope::empty(Some(outer_scope.clone()));
        let mut select_all_mapping = HashMap::new();
        for group_expr in &s.group_by {
            let expr = plan_expr(catalog, ctx, group_expr)?;
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
                    ctx.scope.items[*old_column].clone()
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
        let ctx = &ExprContext {
            name: "aggregate function",
            scope: &from_scope,
            outer_relation_type,
            inner_relation_type: &relation_expr
                .clone()
                .map(group_exprs.clone())
                .typ(outer_relation_type),
            allow_aggregates: false,
        };
        let mut aggregates = vec![];
        for sql_function in aggregate_visitor.into_result()? {
            aggregates.push(plan_aggregate(catalog, ctx, sql_function)?);
            group_scope.items.push(ScopeItem {
                names: vec![ScopeItemName {
                    table_name: None,
                    column_name: Some(sql_function.name.to_string().to_lowercase()),
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
        let ctx = &ExprContext {
            name: "HAVING clause",
            scope: &group_scope,
            outer_relation_type,
            inner_relation_type: &relation_expr.typ(outer_relation_type),
            allow_aggregates: true,
        };
        let expr = plan_expr(catalog, ctx, having)?;
        let typ = ctx.column_type(&expr);
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
        let mut project_scope = Scope::empty(Some(outer_scope.clone()));
        for p in &s.projection {
            let ctx = &ExprContext {
                name: "SELECT clause",
                scope: &group_scope,
                outer_relation_type,
                inner_relation_type: &relation_expr.typ(outer_relation_type),
                allow_aggregates: true,
            };
            for (expr, scope_item) in
                plan_select_item(catalog, ctx, p, &from_scope, &select_all_mapping)?
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

fn plan_table_with_joins<'a>(
    catalog: &Catalog,
    table_with_joins: &'a TableWithJoins,
    outer_scope: &Scope,
    outer_relation_type: &RelationType,
) -> Result<(RelationExpr, Scope), failure::Error> {
    let (mut left, mut left_scope) =
        plan_table_factor(catalog, &table_with_joins.relation, outer_scope)?;
    for join in &table_with_joins.joins {
        let (right, right_scope) = plan_table_factor(catalog, &join.relation, outer_scope)?;
        let (new_left, new_left_scope) = plan_join_operator(
            catalog,
            &join.join_operator,
            left,
            left_scope,
            right,
            right_scope,
            outer_relation_type,
        )?;
        left = new_left;
        left_scope = new_left_scope;
    }
    Ok((left, left_scope))
}

fn plan_table_factor<'a>(
    catalog: &Catalog,
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
            let desc = catalog.get_desc(&name)?;
            let expr = RelationExpr::Get {
                name: name.clone(),
                typ: desc.typ().clone(),
            };
            let alias = if let Some(TableAlias { name, columns }) = alias {
                if !columns.is_empty() {
                    bail!("aliasing columns is not yet supported");
                }
                name.value.to_owned()
            } else {
                name
            };
            let scope =
                Scope::from_source(Some(&alias), desc.iter_names(), Some(outer_scope.clone()));
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
            let (expr, scope) = plan_subquery(
                catalog,
                &subquery,
                &Scope::empty(None),
                &RelationType::empty(),
            )?;
            let alias = if let Some(TableAlias { name, columns }) = alias {
                if !columns.is_empty() {
                    bail!("aliasing columns is not yet supported");
                }
                Some(name.value.as_str())
            } else {
                None
            };
            let scope = Scope::from_source(alias, scope.column_names(), Some(outer_scope.clone()));
            Ok((expr, scope))
        }
        TableFactor::NestedJoin(table_with_joins) => plan_table_with_joins(
            catalog,
            table_with_joins,
            outer_scope,
            &RelationType::empty(),
        ),
    }
}

fn plan_select_item<'a>(
    catalog: &Catalog,
    ctx: &ExprContext,
    s: &'a SelectItem,
    select_all_scope: &Scope,
    select_all_mapping: &HashMap<usize, usize>,
) -> Result<Vec<(ScalarExpr, ScopeItem)>, failure::Error> {
    match s {
        SelectItem::UnnamedExpr(sql_expr) => {
            let expr = plan_expr(catalog, ctx, sql_expr)?;
            let mut scope_item = if let ScalarExpr::Column(ColumnRef::Inner(i)) = &expr {
                ctx.scope.items[*i].clone()
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
            let expr = plan_expr(catalog, ctx, sql_expr)?;
            let mut scope_item = if let ScalarExpr::Column(ColumnRef::Inner(i)) = &expr {
                ctx.scope.items[*i].clone()
            } else {
                ScopeItem::from_column_name(None)
            };
            scope_item.names.insert(
                0,
                ScopeItemName {
                    table_name: None,
                    column_name: Some(alias.value.clone()),
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
    operator: &JoinOperator,
    left: RelationExpr,
    left_scope: Scope,
    right: RelationExpr,
    right_scope: Scope,
    outer_relation_type: &RelationType,
) -> Result<(RelationExpr, Scope), failure::Error> {
    match operator {
        JoinOperator::Inner(constraint) => plan_join_constraint(
            catalog,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            outer_relation_type,
            JoinKind::Inner,
        ),
        JoinOperator::LeftOuter(constraint) => plan_join_constraint(
            catalog,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            outer_relation_type,
            JoinKind::LeftOuter,
        ),
        JoinOperator::RightOuter(constraint) => plan_join_constraint(
            catalog,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            outer_relation_type,
            JoinKind::RightOuter,
        ),
        JoinOperator::FullOuter(constraint) => plan_join_constraint(
            catalog,
            &constraint,
            left,
            left_scope,
            right,
            right_scope,
            outer_relation_type,
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
    constraint: &'a JoinConstraint,
    left: RelationExpr,
    left_scope: Scope,
    right: RelationExpr,
    right_scope: Scope,
    outer_relation_type: &RelationType,
    kind: JoinKind,
) -> Result<(RelationExpr, Scope), failure::Error> {
    let (expr, scope) = match constraint {
        JoinConstraint::On(expr) => {
            let mut product_scope = left_scope.product(right_scope);
            let ctx = &ExprContext {
                name: "ON clause",
                scope: &product_scope,
                outer_relation_type,
                inner_relation_type: &RelationType::new(
                    left.typ(outer_relation_type)
                        .column_types
                        .into_iter()
                        .chain(right.typ(outer_relation_type).column_types)
                        .collect(),
                ),
                allow_aggregates: false,
            };
            let on = plan_expr(catalog, ctx, expr)?;
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
                .map(|ident| ident.value.to_owned())
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
            .fold(LITERAL_TRUE, |expr1, expr2| ScalarExpr::CallBinary {
                func: BinaryFunc::And,
                expr1: Box::new(expr1),
                expr2: Box::new(expr2),
            }),
        kind,
    }
    .map(map_exprs)
    .project(project_key);
    Ok((both, both_scope))
}

fn plan_expr<'a>(
    catalog: &Catalog,
    ctx: &ExprContext,
    e: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    if let Some((i, _)) = ctx.scope.resolve_expr(e) {
        // surprise - we already calculated this expr before
        Ok(ScalarExpr::Column(i))
    } else {
        match e {
            Expr::Identifier(name) => {
                let (i, _) = ctx.scope.resolve_column(&name.value)?;
                Ok(ScalarExpr::Column(i))
            }
            Expr::CompoundIdentifier(names) => {
                if names.len() == 2 {
                    let (i, _) = ctx
                        .scope
                        .resolve_table_column(&names[0].value, &names[1].value)?;
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
            Expr::Parameter(_) => bail!("query parameters are not yet supported"),
            // TODO(benesch): why isn't IS [NOT] NULL a unary op?
            Expr::IsNull(expr) => plan_is_null_expr(catalog, ctx, expr, false),
            Expr::IsNotNull(expr) => plan_is_null_expr(catalog, ctx, expr, true),
            Expr::UnaryOp { op, expr } => plan_unary_op(catalog, ctx, op, expr),
            Expr::BinaryOp { op, left, right } => plan_binary_op(catalog, ctx, op, left, right),
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => plan_between(catalog, ctx, expr, low, high, *negated),
            Expr::InList {
                expr,
                list,
                negated,
            } => plan_in_list(catalog, ctx, expr, list, *negated),
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => plan_case(catalog, ctx, operand, conditions, results, else_result),
            Expr::Nested(expr) => plan_expr(catalog, ctx, expr),
            Expr::Cast { expr, data_type } => plan_cast(catalog, ctx, expr, data_type),
            Expr::Function(func) => plan_function(catalog, ctx, func),
            Expr::Exists(query) => {
                let typ = RelationType::new(
                    ctx.outer_relation_type
                        .column_types
                        .iter()
                        .cloned()
                        .chain(ctx.inner_relation_type.column_types.iter().cloned())
                        .collect(),
                );
                let (expr, _scope) = plan_subquery(catalog, query, &ctx.scope, &typ)?;
                Ok(expr.exists())
            }
            Expr::Subquery(query) => {
                let typ = RelationType::new(
                    ctx.outer_relation_type
                        .column_types
                        .iter()
                        .cloned()
                        .chain(ctx.inner_relation_type.column_types.iter().cloned())
                        .collect(),
                );
                let (expr, _scope) = plan_subquery(catalog, query, &ctx.scope, &typ)?;
                let column_types = expr.typ(&typ).column_types;
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
            } => plan_any_or_all(catalog, ctx, left, op, right, AggregateFunc::Any),
            Expr::All { left, op, right } => {
                plan_any_or_all(catalog, ctx, left, op, right, AggregateFunc::All)
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
                    plan_any_or_all(catalog, ctx, expr, &NotEq, subquery, AggregateFunc::All)
                } else {
                    // `<expr> IN (<subquery>)` is equivalent to
                    // `<expr> = ANY (<subquery>)`.
                    plan_any_or_all(catalog, ctx, expr, &Eq, subquery, AggregateFunc::Any)
                }
            }
            Expr::Extract { field, expr } => {
                let mut expr = plan_expr(catalog, ctx, expr)?;
                let mut typ = expr.typ(&ctx.outer_relation_type, &ctx.inner_relation_type);
                if let ScalarType::Date = typ.scalar_type {
                    expr = plan_cast_internal(ctx, "EXTRACT", expr, ScalarType::Timestamp)?;
                    typ = ctx.column_type(&expr);
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

fn plan_any_or_all<'a>(
    catalog: &Catalog,
    ctx: &ExprContext,
    left: &'a Expr,
    op: &'a BinaryOperator,
    right: &'a Query,
    func: AggregateFunc,
) -> Result<ScalarExpr, failure::Error> {
    let typ = RelationType::new(
        ctx.outer_relation_type
            .column_types
            .iter()
            .cloned()
            .chain(ctx.inner_relation_type.column_types.iter().cloned())
            .collect(),
    );
    // plan right

    let (right, _scope) = plan_subquery(catalog, right, &ctx.scope, &typ)?;
    let column_types = right.typ(&typ).column_types;
    if column_types.len() != 1 {
        bail!(
            "Expected subquery of ANY to return 1 column, got {} columns",
            column_types.len()
        );
    }

    // plan left and op
    // this is a bit of a hack - we want to plan `op` as if the original expr was `(SELECT ANY/ALL(left op right[1]) FROM right)`
    let mut scope = Scope::empty(Some(ctx.scope.clone()));
    let right_name = format!("right_{}", Uuid::new_v4());
    scope.items.push(ScopeItem {
        names: vec![ScopeItemName {
            table_name: Some(right_name.clone()),
            column_name: Some(right_name.clone()),
        }],
        expr: None,
    });
    let any_ctx = ExprContext {
        name: "WHERE clause",
        scope: &scope,
        outer_relation_type: &typ,
        inner_relation_type: &right.typ(&typ),
        allow_aggregates: false,
    };
    let op_expr = plan_binary_op(
        catalog,
        &any_ctx,
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
    ctx: &ExprContext,
    expr: &'a Expr,
    data_type: &'a DataType,
) -> Result<ScalarExpr, failure::Error> {
    let to_scalar_type = scalar_type_from_sql(data_type)?;
    let expr = plan_expr(catalog, ctx, expr)?;
    plan_cast_internal(ctx, "CAST", expr, to_scalar_type)
}

fn plan_aggregate(
    catalog: &Catalog,
    ctx: &ExprContext,
    sql_func: &Function,
) -> Result<AggregateExpr, failure::Error> {
    let ident = sql_func.name.to_string().to_lowercase();
    assert!(is_aggregate_func(&ident));

    if sql_func.over.is_some() {
        bail!("window functions are not yet supported");
    }

    if sql_func.args.len() != 1 {
        bail!("{} function only takes one argument", ident);
    }

    let arg = &sql_func.args[0];
    let (expr, func) = match (&*ident, arg) {
        // COUNT(*) is a special case that doesn't compose well
        ("count", Expr::Wildcard) => (LITERAL_NULL, AggregateFunc::CountAll),
        _ => {
            let expr = plan_expr(catalog, ctx, arg)?;
            let typ = ctx.column_type(&expr);
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
    ctx: &ExprContext,
    sql_func: &'a Function,
) -> Result<ScalarExpr, failure::Error> {
    let ident = sql_func.name.to_string().to_lowercase();
    if is_aggregate_func(&ident) {
        if ctx.allow_aggregates {
            // should already have been caught by `scope.resolve_expr` in `plan_expr`
            bail!(
                "Internal error: encountered unplanned aggregate function: {:?}",
                sql_func,
            )
        } else {
            bail!("aggregate functions are not allowed in {}", ctx.name);
        }
    } else {
        match ident.as_str() {
            "abs" => {
                if sql_func.args.len() != 1 {
                    bail!("abs expects one argument, got {}", sql_func.args.len());
                }
                let expr = plan_expr(catalog, ctx, &sql_func.args[0])?;
                let typ = ctx.column_type(&expr);
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
                let expr = plan_expr(catalog, ctx, &sql_func.args[0])?;
                let typ = ctx.column_type(&expr);
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
                let mut exprs = Vec::new();
                for arg in &sql_func.args {
                    exprs.push(plan_expr(catalog, ctx, arg)?);
                }
                let exprs = try_coalesce_types(ctx, "coalesce", exprs)?;
                let expr = ScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs,
                };
                Ok(expr)
            }

            "mod" => {
                if sql_func.args.len() != 2 {
                    bail!("mod requires exactly two arguments");
                }
                plan_binary_op(
                    catalog,
                    ctx,
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
                let cond_expr = plan_expr(catalog, ctx, &cond)?;
                let else_expr = plan_expr(catalog, ctx, &sql_func.args[0])?;
                let expr = ScalarExpr::If {
                    cond: Box::new(cond_expr),
                    then: Box::new(LITERAL_NULL),
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
                plan_function(catalog, ctx, &func)
            }

            "substring" => {
                if sql_func.args.len() < 2 || sql_func.args.len() > 3 {
                    bail!(
                        "substring expects two or three arguments, got {:?}",
                        sql_func.args.len()
                    );
                }
                let mut exprs = Vec::new();
                let expr1 = plan_expr(catalog, ctx, &sql_func.args[0])?;
                let typ1 = ctx.column_type(&expr1);
                if typ1.scalar_type != ScalarType::String && typ1.scalar_type != ScalarType::Null {
                    bail!("substring first argument has non-string type {:?}", typ1);
                }
                exprs.push(expr1);

                let expr2 = plan_expr(catalog, ctx, &sql_func.args[1])?;
                let expr2 = promote_int_int64(ctx, "substring start", expr2)?;
                exprs.push(expr2);
                if sql_func.args.len() == 3 {
                    let expr3 = plan_expr(catalog, ctx, &sql_func.args[2])?;
                    let expr3 = promote_int_int64(ctx, "substring length", expr3)?;
                    exprs.push(expr3);
                }
                let expr = ScalarExpr::CallVariadic {
                    func: VariadicFunc::Substr,
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
                let expr = plan_expr(catalog, ctx, &sql_func.args[0])?;
                let typ = ctx.column_type(&expr);
                let output_type = match &typ.scalar_type {
                    ScalarType::Null => ScalarType::Null,
                    ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
                    ScalarType::Decimal(p, s) => ScalarType::Decimal(*p, *s),
                    ScalarType::Int32 => ScalarType::Decimal(10, 0),
                    ScalarType::Int64 => ScalarType::Decimal(19, 0),
                    _ => bail!("internal.avg_promotion called with unexpected argument"),
                };
                plan_cast_internal(ctx, "internal.avg_promotion", expr, output_type)
            }

            _ => bail!("unsupported function: {}", ident),
        }
    }
}

fn plan_is_null_expr<'a>(
    catalog: &Catalog,
    ctx: &ExprContext,
    inner: &'a Expr,
    not: bool,
) -> Result<ScalarExpr, failure::Error> {
    let expr = plan_expr(catalog, ctx, inner)?;
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
    ctx: &ExprContext,
    op: &'a UnaryOperator,
    expr: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let expr = plan_expr(catalog, ctx, expr)?;
    let typ = ctx.column_type(&expr);
    let func = match op {
        UnaryOperator::Not => UnaryFunc::Not,
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
    ctx: &ExprContext,
    op: &'a BinaryOperator,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    use BinaryOperator::*;
    match op {
        And => plan_boolean_op(catalog, ctx, BooleanOp::And, left, right),
        Or => plan_boolean_op(catalog, ctx, BooleanOp::Or, left, right),

        Plus => plan_arithmetic_op(catalog, ctx, ArithmeticOp::Plus, left, right),
        Minus => plan_arithmetic_op(catalog, ctx, ArithmeticOp::Minus, left, right),
        Multiply => plan_arithmetic_op(catalog, ctx, ArithmeticOp::Multiply, left, right),
        Divide => plan_arithmetic_op(catalog, ctx, ArithmeticOp::Divide, left, right),
        Modulus => plan_arithmetic_op(catalog, ctx, ArithmeticOp::Modulo, left, right),

        Lt => plan_comparison_op(catalog, ctx, ComparisonOp::Lt, left, right),
        LtEq => plan_comparison_op(catalog, ctx, ComparisonOp::LtEq, left, right),
        Gt => plan_comparison_op(catalog, ctx, ComparisonOp::Gt, left, right),
        GtEq => plan_comparison_op(catalog, ctx, ComparisonOp::GtEq, left, right),
        Eq => plan_comparison_op(catalog, ctx, ComparisonOp::Eq, left, right),
        NotEq => plan_comparison_op(catalog, ctx, ComparisonOp::NotEq, left, right),

        Like => plan_like(catalog, ctx, left, right, false),
        NotLike => plan_like(catalog, ctx, left, right, true),
    }
}

fn plan_boolean_op<'a>(
    catalog: &Catalog,
    ctx: &ExprContext,
    op: BooleanOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let lexpr = plan_expr(catalog, ctx, left)?;
    let rexpr = plan_expr(catalog, ctx, right)?;
    let ltype = ctx.column_type(&lexpr);
    let rtype = ctx.column_type(&rexpr);

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
    ctx: &ExprContext,
    op: ArithmeticOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    use ArithmeticOp::*;
    use BinaryFunc::*;
    use ScalarType::*;

    // Step 1. Plan inner expressions.
    let mut lexpr = plan_expr(catalog, ctx, left)?;
    let mut rexpr = plan_expr(catalog, ctx, right)?;
    let mut ltype = ctx.column_type(&lexpr);
    let mut rtype = ctx.column_type(&rexpr);

    // Step 2. Infer whether any implicit type coercions are required.
    let both_decimals = match (&ltype.scalar_type, &rtype.scalar_type) {
        (Decimal(_, _), Decimal(_, _)) => true,
        _ => false,
    };
    let timelike_and_interval = match (&ltype.scalar_type, &rtype.scalar_type) {
        (Date, Interval) | (Timestamp, Interval) | (Interval, Date) | (Interval, Timestamp) => true,
        _ => false,
    };
    if both_decimals {
        // When both inputs are already decimals, we skip coalescing, which
        // could result in a rescale if the decimals have different
        // precisions, because we tightly control the rescale when planning
        // the arithmetic operation (below). E.g., decimal multiplication
        // does not need to rescale its inputs, even when the inputs have
        // different scales.
    } else if timelike_and_interval {
        // If the inputs are a timelike and an interval, we skip coalescing,
        // because adding and subtracting intervals and timelikes is well
        // defined. We will, however, promote any date inputs to timestamps.
        // Adding an
        if let Date = &ltype.scalar_type {
            let expr = plan_cast_internal(ctx, &op, lexpr, Timestamp)?;
            ltype = ctx.column_type(&expr);
            lexpr = expr;
        }
        if let Date = &rtype.scalar_type {
            let expr = plan_cast_internal(ctx, &op, rexpr, Timestamp)?;
            rtype = ctx.column_type(&expr);
            rexpr = expr;
        }
    } else {
        // Otherwise, "coalesce" types by finding a common type that can
        // represent both inputs.
        let mut exprs = try_coalesce_types(ctx, &op, vec![lexpr, rexpr])?;
        assert_eq!(exprs.len(), 2);
        rexpr = exprs.pop().unwrap();
        lexpr = exprs.pop().unwrap();
        rtype = ctx.column_type(&rexpr);
        ltype = ctx.column_type(&lexpr);
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
            _ => bail!(
                "no overload for {:?} + {:?}",
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
            _ => bail!(
                "no overload for {:?} - {:?}",
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
                "no overload for {:?} * {:?}",
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
                "no overload for {:?} / {:?}",
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
                "no overload for {:?} % {:?}",
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
    ctx: &ExprContext,
    op: ComparisonOp,
    left: &'a Expr,
    right: &'a Expr,
) -> Result<ScalarExpr, failure::Error> {
    let mut lexpr = plan_expr(catalog, ctx, left)?;
    let mut rexpr = plan_expr(catalog, ctx, right)?;

    let mut exprs = try_coalesce_types(ctx, &op, vec![lexpr, rexpr])?;
    assert_eq!(exprs.len(), 2);
    rexpr = exprs.pop().unwrap();
    lexpr = exprs.pop().unwrap();
    let rtype = ctx.column_type(&rexpr);
    let ltype = ctx.column_type(&lexpr);

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
    ctx: &ExprContext,
    left: &'a Expr,
    right: &'a Expr,
    negate: bool,
) -> Result<ScalarExpr, failure::Error> {
    let lexpr = plan_expr(catalog, ctx, left)?;
    let ltype = ctx.column_type(&lexpr);
    let rexpr = plan_expr(catalog, ctx, right)?;
    let rtype = ctx.column_type(&rexpr);

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
        expr2: Box::new(ScalarExpr::CallUnary {
            func: UnaryFunc::BuildLikeRegex,
            expr: Box::new(rexpr),
        }),
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
    ctx: &ExprContext,
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
    plan_expr(catalog, ctx, &both)
}

fn plan_in_list<'a>(
    catalog: &Catalog,
    ctx: &ExprContext,
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
    plan_expr(catalog, ctx, &cond)
}

fn plan_case<'a>(
    catalog: &Catalog,
    ctx: &ExprContext,
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
        let cexpr = plan_expr(catalog, ctx, &c)?;
        let ctype = ctx.column_type(&cexpr);
        if ctype.scalar_type != ScalarType::Bool {
            bail!(
                "CASE expression has non-boolean type {:?}",
                ctype.scalar_type
            );
        }
        cond_exprs.push(cexpr);
        let rexpr = plan_expr(catalog, ctx, r)?;
        result_exprs.push(rexpr);
    }
    result_exprs.push(match else_result {
        Some(else_result) => plan_expr(catalog, ctx, else_result)?,
        None => LITERAL_NULL,
    });
    let mut result_exprs = try_coalesce_types(ctx, "CASE", result_exprs)?;
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
    let typ = ColumnType::new(scalar_type).nullable(nullable);
    let expr = ScalarExpr::Literal(datum, typ);
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

// Takes a list of (expression, type), where the type can be different for every
// expression, and attempts to find a uniform type to which all expressions can
// be cast. If successful, returns a new list of expressions in the same order
// as the input, where each expression has the appropriate casts, as well as the
// selected type for all the expressions.
//
// When types don't match exactly, SQL has some poorly-documented type promotion
// rules. For now, just promote integers into decimals or floats, decimals into
// floats, and small Xs into bigger Xs.
fn try_coalesce_types<'a, S>(
    ctx: &ExprContext<'a>,
    name: S,
    exprs: Vec<ScalarExpr>,
) -> Result<Vec<ScalarExpr>, failure::Error>
where
    S: fmt::Display + Copy,
{
    assert!(!exprs.is_empty());
    let types: Vec<_> = exprs.iter().map(|e| ctx.column_type(e)).collect();
    let scalar_type_prec = |scalar_type: &ScalarType| match scalar_type {
        ScalarType::Null => 0,
        ScalarType::Int32 => 1,
        ScalarType::Int64 => 2,
        ScalarType::Decimal(_, _) => 3,
        ScalarType::Float32 => 4,
        ScalarType::Float64 => 5,
        ScalarType::Date => 6,
        ScalarType::Timestamp => 7,
        _ => 8,
    };
    let max_scalar_type = types
        .iter()
        .map(|typ| typ.scalar_type)
        .max_by_key(|scalar_type| scalar_type_prec(scalar_type))
        .unwrap();
    let mut out = Vec::new();
    for (expr, typ) in exprs.into_iter().zip(types) {
        match plan_cast_internal(ctx, name, expr, max_scalar_type) {
            Ok(expr) => out.push(expr),
            Err(_) => bail!(
                "{} does not have uniform type: {:?} vs {:?}",
                name,
                typ.scalar_type,
                max_scalar_type,
            ),
        }
    }
    Ok(out)
}

/// Plans a cast between two `RelationExpr`s of different types. If it is
/// impossible to cast between the two types, an error is returned.
///
/// Note that `plan_cast_internal` only understands [`ScalarType`]s. If you need
/// to cast between SQL [`DataType`]s, see [`Planner::plan_cast`].
fn plan_cast_internal<'a, S>(
    ctx: &ExprContext<'a>,
    name: S,
    expr: ScalarExpr,
    to_scalar_type: ScalarType,
) -> Result<ScalarExpr, failure::Error>
where
    S: fmt::Display + Copy,
{
    use ScalarType::*;
    use UnaryFunc::*;
    let from_scalar_type = ctx.column_type(&expr).scalar_type;
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
            let factor = ScalarExpr::Literal(Datum::from(factor), ColumnType::new(to_scalar_type));
            expr.call_unary(CastDecimalToFloat32)
                .call_binary(factor, BinaryFunc::DivFloat32)
        }
        (Decimal(_, s), Float64) => {
            let factor = 10_f64.powi(i32::from(s));
            let factor = ScalarExpr::Literal(Datum::from(factor), ColumnType::new(to_scalar_type));
            expr.call_unary(CastDecimalToFloat64)
                .call_binary(factor, BinaryFunc::DivFloat64)
        }
        (Decimal(_, s1), Decimal(_, s2)) => rescale_decimal(expr, s1, s2),
        (Date, Timestamp) => expr.call_unary(CastDateToTimestamp),
        (Null, _) => {
            // assert_eq!(expr, ScalarExpr::Literal(Datum::Null, ColumnType::new(ScalarType::Null)));
            ScalarExpr::Literal(Datum::Null, ColumnType::new(to_scalar_type).nullable(true))
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
    ctx: &ExprContext<'a>,
    name: S,
    expr: ScalarExpr,
) -> Result<ScalarExpr, failure::Error>
where
    S: fmt::Display + Copy,
{
    Ok(match ctx.column_type(&expr).scalar_type {
        ScalarType::Null | ScalarType::Int64 => expr,
        ScalarType::Int32 => plan_cast_internal(ctx, name, expr, ScalarType::Int64)?,
        other => bail!("{} has non-integer type {:?}", name, other,),
    })
}

fn rescale_decimal(expr: ScalarExpr, s1: u8, s2: u8) -> ScalarExpr {
    if s2 > s1 {
        let typ = ColumnType::new(ScalarType::Decimal(38, s2 - s1));
        let factor = 10_i128.pow(u32::from(s2 - s1));
        let factor = ScalarExpr::Literal(Datum::from(factor), typ);
        expr.call_binary(factor, BinaryFunc::MulDecimal)
    } else if s1 > s2 {
        let typ = ColumnType::new(ScalarType::Decimal(38, s1 - s2));
        let factor = 10_i128.pow(u32::from(s1 - s2));
        let factor = ScalarExpr::Literal(Datum::from(factor), typ);
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
        | other @ DataType::TimeTz
        | other @ DataType::TimestampTz
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
/// A bundle of unrelated things that we need for planning `Expr`s
struct ExprContext<'a> {
    /// The name of this kind of expression eg "WHERE clause". Used only for error messages.
    name: &'static str,
    /// The current scope
    scope: &'a Scope,
    /// The type of the outer relation expression upon which this scalar
    /// expression will be evaluated.
    outer_relation_type: &'a RelationType,
    /// The type of the inner relation expression upon which this scalar
    /// expression will be evaluated.
    inner_relation_type: &'a RelationType,
    /// Are aggregate functions allowed in this context
    allow_aggregates: bool,
}

impl<'a> ExprContext<'a> {
    fn column_type(&self, expr: &ScalarExpr) -> ColumnType {
        expr.typ(&self.outer_relation_type, &self.inner_relation_type)
    }
}

fn is_aggregate_func(name: &str) -> bool {
    match name {
        // avg is handled by transform::AvgFuncRewriter.
        "max" | "min" | "sum" | "count" => true,
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
        ("max", ScalarType::Null) => AggregateFunc::MaxNull,
        ("min", ScalarType::Int32) => AggregateFunc::MinInt32,
        ("min", ScalarType::Int64) => AggregateFunc::MinInt64,
        ("min", ScalarType::Float32) => AggregateFunc::MinFloat32,
        ("min", ScalarType::Float64) => AggregateFunc::MinFloat64,
        ("min", ScalarType::Decimal(_, _)) => AggregateFunc::MinDecimal,
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
    })
}
