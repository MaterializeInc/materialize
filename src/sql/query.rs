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
    AggregateExpr, AggregateFunc, BinaryFunc, ColumnRef, JoinKind, RelationExpr, ScalarExpr,
    UnaryFunc, VariadicFunc,
};
use super::scope::{Scope, ScopeItem, ScopeItemName};
use super::{extract_sql_object_name, Planner};
use dataflow_types::{ColumnOrder, RowSetFinishing};
use failure::{bail, ensure, format_err, ResultExt};
use ore::collections::CollectionExt;
use ore::iter::{FallibleIteratorExt, IteratorExt};
use repr::decimal::MAX_DECIMAL_PRECISION;
use repr::{ColumnType, Datum, RelationType, ScalarType};
use sqlparser::ast::visit::{self, Visit};
use sqlparser::ast::{
    BinaryOperator, DataType, Expr, Function, JoinConstraint, JoinOperator, ObjectName, ParsedDate,
    ParsedTimestamp, Query, Select, SelectItem, SetExpr, SetOperator, TableAlias, TableFactor,
    TableWithJoins, UnaryOperator, Value, Values,
};
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fmt;
use std::mem;
use uuid::Uuid;

impl Planner {
    pub fn plan_query(
        &self,
        q: &Query,
        outer_scope: &Scope,
    ) -> Result<(RelationExpr, Scope, RowSetFinishing), failure::Error> {
        if !q.ctes.is_empty() {
            bail!("CTEs are not yet supported");
        }
        let limit = match &q.limit {
            None => None,
            Some(Expr::Value(Value::Number(x))) => Some(x.parse()?),
            _ => bail!("LIMIT must be an integer constant"),
        };
        let (expr, scope) = self.plan_set_expr(&q.body, outer_scope)?;
        let output_typ = expr.typ();
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
                        allow_aggregates: true,
                    };
                    let (expr, typ) = self.plan_expr(ctx, other)?;
                    let idx = output_typ.column_types.len() + map_exprs.len();
                    map_exprs.push((expr, typ));
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
        let transform = RowSetFinishing {
            order_by,
            limit,
            project: (0..output_typ.column_types.len()).collect(),
        };
        Ok((expr.map(map_exprs), scope, transform))
    }

    fn plan_set_expr(
        &self,
        q: &SetExpr,
        outer_scope: &Scope,
    ) -> Result<(RelationExpr, Scope), failure::Error> {
        match q {
            SetExpr::Select(select) => self.plan_view_select(select, outer_scope),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                let (left_expr, _left_scope) = self.plan_set_expr(left, outer_scope)?;
                let (right_expr, _right_scope) = self.plan_set_expr(right, outer_scope)?;

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

                let mut scope = Scope::empty(Some(outer_scope.clone()));
                for typ in relation_expr.typ().column_types {
                    scope.items.push(ScopeItem::from_column_type(typ));
                }

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
                    allow_aggregates: false,
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
                let mut scope = Scope::empty(Some(outer_scope.clone()));
                for typ in types.unwrap() {
                    scope.items.push(ScopeItem::from_column_type(typ));
                }
                Ok((expr.unwrap(), scope))
            }
            SetExpr::Query(query) => {
                let (expr, scope, transform) = self.plan_query(query, outer_scope)?;
                if !transform.is_trivial() {
                    bail!("ORDER BY and LIMIT are not yet supported in subqueries");
                }
                Ok((expr, scope))
            }
        }
    }

    fn plan_view_select(
        &self,
        s: &Select,
        outer_scope: &Scope,
    ) -> Result<(RelationExpr, Scope), failure::Error> {
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
                let typ = RelationType::new(vec![]);
                Ok((
                    RelationExpr::Constant {
                        rows: vec![vec![]],
                        typ: typ.clone(),
                    },
                    Scope::from_source(None, typ, Some(outer_scope.clone())),
                ))
            })?;

        // Step 2. Handle WHERE clause.
        if let Some(selection) = &s.selection {
            let ctx = &ExprContext {
                name: "WHERE clause",
                scope: &from_scope,
                allow_aggregates: false,
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
        let (group_scope, select_all_mapping) = {
            // gather group columns
            let ctx = &ExprContext {
                name: "GROUP BY clause",
                scope: &from_scope,
                allow_aggregates: false,
            };
            let mut group_key = vec![];
            let mut group_exprs = vec![];
            let mut group_scope = Scope::empty(Some(outer_scope.clone()));
            let mut select_all_mapping = HashMap::new();
            for group_expr in &s.group_by {
                let (expr, typ) = self.plan_expr(ctx, group_expr)?;
                let new_column = group_key.len();
                // repeated exprs in GROUP BY confuse name resolution later, and dropping them doesn't change the result
                if group_exprs
                    .iter()
                    .find(|(existing_expr, _)| *existing_expr == expr)
                    .is_none()
                {
                    let mut scope_item =
                        if let ScalarExpr::Column(ColumnRef::Inner(old_column)) = &expr {
                            // If we later have `SELECT foo.*` then we have to find all the `foo` items in `from_scope` and figure out where they ended up in `group_scope`.
                            // This is really hard to do right using SQL name resolution, so instead we just track the movement here.
                            select_all_mapping.insert(*old_column, new_column);
                            ctx.scope.items[*old_column].clone()
                        } else {
                            ScopeItem::from_column_type(typ.clone())
                        };
                    scope_item.expr = Some(group_expr.clone());

                    group_key.push(from_scope.len() + group_exprs.len());
                    group_exprs.push((expr, typ.clone()));
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
                allow_aggregates: false,
            };
            let mut aggregates = vec![];
            for sql_function in aggregate_visitor.into_result()? {
                let (expr, typ) = self.plan_aggregate(ctx, sql_function)?;
                aggregates.push((expr, typ.clone()));
                group_scope.items.push(ScopeItem {
                    names: vec![],
                    typ,
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
                allow_aggregates: true,
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
        let project_scope = {
            let mut project_exprs = vec![];
            let mut project_key = vec![];
            let mut project_scope = Scope::empty(Some(outer_scope.clone()));
            for p in &s.projection {
                let ctx = &ExprContext {
                    name: "SELECT clause",
                    scope: &group_scope,
                    allow_aggregates: true,
                };
                for (expr, scope_item) in
                    self.plan_select_item(ctx, p, &from_scope, &select_all_mapping)?
                {
                    project_key.push(group_scope.len() + project_exprs.len());
                    project_exprs.push((expr, scope_item.typ.clone()));
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
                // TODO(jamii) would be nice to use this scope instead of use expr.typ() below
                let (expr, _scope, finishing) = self.plan_query(&subquery, &Scope::empty(None))?;
                if !finishing.is_trivial() {
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
    ) -> Result<Vec<(ScalarExpr, ScopeItem)>, failure::Error> {
        match s {
            SelectItem::UnnamedExpr(sql_expr) => {
                let (expr, typ) = self.plan_expr(ctx, sql_expr)?;
                let mut scope_item = if let ScalarExpr::Column(ColumnRef::Inner(i)) = &expr {
                    ctx.scope.items[*i].clone()
                } else {
                    ScopeItem::from_column_type(typ)
                };
                scope_item.expr = Some(sql_expr.clone());
                Ok(vec![(expr, scope_item)])
            }
            SelectItem::ExprWithAlias {
                expr: sql_expr,
                alias,
            } => {
                let (expr, typ) = self.plan_expr(ctx, sql_expr)?;
                let mut scope_item = if let ScalarExpr::Column(ColumnRef::Inner(i)) = &expr {
                    ctx.scope.items[*i].clone()
                } else {
                    ScopeItem::from_column_type(typ)
                };
                scope_item.names.push(ScopeItemName {
                    table_name: None,
                    column_name: Some(alias.clone()),
                });
                scope_item.typ.name = Some(alias.clone());
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
                    allow_aggregates: false,
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
            new_items.push(ScopeItem {
                names,
                typ,
                expr: None,
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
        if let Some((i, item)) = ctx.scope.resolve_expr(e) {
            // surprise - we already calculated this expr before
            let expr = ScalarExpr::Column(i);
            Ok((expr, item.typ.clone()))
        } else {
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
                    let (expr, _scope, transform) = self.plan_query(query, &ctx.scope)?;
                    if !transform.is_trivial() {
                        bail!("ORDER BY and LIMIT are not yet supported in subqueries");
                    }
                    Ok((expr.exists(), ColumnType::new(ScalarType::Bool)))
                }
                Expr::Subquery(query) => {
                    let (expr, _scope, transform) = self.plan_query(query, &ctx.scope)?;
                    if !transform.is_trivial() {
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
        let (right, _scope, transform) = self.plan_query(right, &ctx.scope)?;
        if !transform.is_trivial() {
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
                table_name: Some(right_name.clone()),
                column_name: Some(right_name.clone()),
            }],
            typ: right_type,
            expr: None,
        });
        let any_ctx = ExprContext {
            name: "WHERE clause",
            scope: &scope,
            allow_aggregates: false,
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

                "ascii" => {
                    if sql_func.args.len() != 1 {
                        bail!("ascii expects one argument, got {}", sql_func.args.len());
                    }
                    let (expr, typ) = self.plan_expr(ctx, &sql_func.args[0])?;
                    if typ.scalar_type != ScalarType::String && typ.scalar_type != ScalarType::Null
                    {
                        bail!("ascii does not accept arguments of type {:?}", typ);
                    }
                    let expr = ScalarExpr::CallUnary {
                        func: UnaryFunc::Ascii,
                        expr: Box::new(expr),
                    };
                    Ok((
                        expr,
                        ColumnType::new(ScalarType::Int32).nullable(typ.nullable),
                    ))
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

                "mod" => {
                    if sql_func.args.len() != 2 {
                        bail!("mod requires exactly two arguments");
                    }
                    self.plan_binary_op(
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

                "substr" => {
                    let func = Function {
                        name: ObjectName(vec![String::from("substring")]),
                        args: sql_func.args.clone(),
                        over: sql_func.over.clone(),
                        distinct: sql_func.distinct,
                    };
                    self.plan_function(ctx, &func)
                }

                "substring" => {
                    if sql_func.args.len() < 2 || sql_func.args.len() > 3 {
                        bail!(
                            "substring expects two or three arguments, got {:?}",
                            sql_func.args.len()
                        );
                    }
                    let mut exprs = Vec::new();
                    let (expr1, typ1) = self.plan_expr(ctx, &sql_func.args[0])?;
                    if typ1.scalar_type != ScalarType::String
                        && typ1.scalar_type != ScalarType::Null
                    {
                        bail!("substring first argument has non-string type {:?}", typ1);
                    }
                    exprs.push(expr1);

                    let (expr2, typ2) = self.plan_expr(ctx, &sql_func.args[1])?;
                    let (verified_expr2, _) =
                        plan_promote_int_int64("substring", expr2, typ2, "start")?;
                    exprs.push(verified_expr2);
                    if sql_func.args.len() == 3 {
                        let (expr3, typ3) = self.plan_expr(ctx, &sql_func.args[2])?;
                        let (verified_expr3, _) =
                            plan_promote_int_int64("substring", expr3, typ3, "length")?;
                        exprs.push(verified_expr3);
                    }
                    let expr = ScalarExpr::CallVariadic {
                        func: VariadicFunc::Substr,
                        exprs,
                    };
                    Ok((expr, ColumnType::new(typ1.scalar_type).nullable(true)))
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
                    let (expr, typ) = self.plan_expr(ctx, &sql_func.args[0])?;
                    let output_type = match &typ.scalar_type {
                        ScalarType::Null => ScalarType::Null,
                        ScalarType::Float32 | ScalarType::Float64 => ScalarType::Float64,
                        ScalarType::Decimal(p, s) => ScalarType::Decimal(*p, *s),
                        ScalarType::Int32 => ScalarType::Decimal(10, 0),
                        ScalarType::Int64 => ScalarType::Decimal(19, 0),
                        _ => bail!("internal.avg_promotion called with unexpected argument"),
                    };
                    plan_cast_internal("internal.avg_promotion", expr, &typ, output_type)
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
                ScalarType::Decimal(p, s) => (UnaryFunc::NegDecimal, ScalarType::Decimal(p, s)),
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
        use BinaryOperator::*;
        match op {
            And => self.plan_boolean_op(ctx, BooleanOp::And, left, right),
            Or => self.plan_boolean_op(ctx, BooleanOp::Or, left, right),

            Plus => self.plan_arithmetic_op(ctx, ArithmeticOp::Plus, left, right),
            Minus => self.plan_arithmetic_op(ctx, ArithmeticOp::Minus, left, right),
            Multiply => self.plan_arithmetic_op(ctx, ArithmeticOp::Multiply, left, right),
            Divide => self.plan_arithmetic_op(ctx, ArithmeticOp::Divide, left, right),
            Modulus => self.plan_arithmetic_op(ctx, ArithmeticOp::Modulo, left, right),

            Lt => self.plan_comparison_op(ctx, ComparisonOp::Lt, left, right),
            LtEq => self.plan_comparison_op(ctx, ComparisonOp::LtEq, left, right),
            Gt => self.plan_comparison_op(ctx, ComparisonOp::Gt, left, right),
            GtEq => self.plan_comparison_op(ctx, ComparisonOp::GtEq, left, right),
            Eq => self.plan_comparison_op(ctx, ComparisonOp::Eq, left, right),
            NotEq => self.plan_comparison_op(ctx, ComparisonOp::NotEq, left, right),

            Like => self.plan_like(ctx, left, right, false),
            NotLike => self.plan_like(ctx, left, right, true),
        }
    }

    fn plan_boolean_op<'a>(
        &self,
        ctx: &ExprContext,
        op: BooleanOp,
        left: &'a Expr,
        right: &'a Expr,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (lexpr, ltype) = self.plan_expr(ctx, left)?;
        let (rexpr, rtype) = self.plan_expr(ctx, right)?;

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
        let typ = ColumnType::new(ScalarType::Bool).nullable(ltype.nullable || rtype.nullable);
        Ok((expr, typ))
    }

    fn plan_arithmetic_op<'a>(
        &self,
        ctx: &ExprContext,
        op: ArithmeticOp,
        left: &'a Expr,
        right: &'a Expr,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        use ArithmeticOp::*;
        use BinaryFunc::*;
        use ScalarType::*;

        // Step 1. Plan inner expressions.
        let (mut lexpr, mut ltype) = self.plan_expr(ctx, left)?;
        let (mut rexpr, mut rtype) = self.plan_expr(ctx, right)?;

        // Step 2. Infer whether any implicit type coercions are required.
        let both_decimals = match (&ltype.scalar_type, &rtype.scalar_type) {
            (Decimal(_, _), Decimal(_, _)) => true,
            _ => false,
        };
        let timelike_and_interval = match (&ltype.scalar_type, &rtype.scalar_type) {
            (Date, Interval) | (Timestamp, Interval) | (Interval, Date) | (Interval, Timestamp) => {
                true
            }
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
                let (expr, typ) = plan_cast_internal(&op, lexpr, &ltype, Timestamp)?;
                lexpr = expr;
                ltype = typ;
            }
            if let Date = &rtype.scalar_type {
                let (expr, typ) = plan_cast_internal(&op, rexpr, &rtype, Timestamp)?;
                rexpr = expr;
                rtype = typ;
            }
        } else {
            // Otherwise, "coalesce" types by finding a common type that can
            // represent both inputs.
            let (mut exprs, typ) = try_coalesce_types(vec![(lexpr, ltype), (rexpr, rtype)], &op)?;
            assert_eq!(exprs.len(), 2);
            rexpr = exprs.pop().unwrap();
            lexpr = exprs.pop().unwrap();
            rtype = typ.clone();
            ltype = typ;
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
            (Plus, Decimal(p1, s1), Decimal(p2, s2))
            | (Minus, Decimal(p1, s1), Decimal(p2, s2))
            | (Modulo, Decimal(p1, s1), Decimal(p2, s2)) => {
                let p = cmp::max(p1, p2) + 1;
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
                let is_nullable_op = op == Modulo;
                let typ = ColumnType::new(Decimal(p, *so))
                    .nullable(ltype.nullable || rtype.nullable || is_nullable_op);
                return Ok((expr, typ));
            }
            (Multiply, Decimal(p1, s1), Decimal(p2, s2)) => {
                let so = cmp::max(cmp::max(cmp::min(s1 + s2, 12), *s1), *s2);
                let si = s1 + s2;
                let expr = lexpr.call_binary(rexpr, MulDecimal);
                let expr = rescale_decimal(expr, si, so);
                let p = (p1 - s1) + (p2 - s2) + so;
                let typ =
                    ColumnType::new(Decimal(p, so)).nullable(ltype.nullable || rtype.nullable);
                return Ok((expr, typ));
            }
            (Divide, Decimal(p1, s1), Decimal(_, s2)) => {
                let s = cmp::max(cmp::min(12, s1 + 6), *s1);
                let si = cmp::max(s + 1, *s2);
                lexpr = rescale_decimal(lexpr, *s1, si);
                let expr = lexpr.call_binary(rexpr, DivDecimal);
                let expr = rescale_decimal(expr, si - *s2, s);
                let p = (p1 - s1) + s2 + s;
                let typ = ColumnType::new(Decimal(p, s)).nullable(true);
                return Ok((expr, typ));
            }
            _ => (),
        }

        // Step 3b. Plan the arithmetic operation for all other types.
        let (func, scalar_type) = match op {
            Plus => match (&ltype.scalar_type, &rtype.scalar_type) {
                (Int32, Int32) => (AddInt32, Int32),
                (Int64, Int64) => (AddInt64, Int64),
                (Float32, Float32) => (AddFloat32, Float32),
                (Float64, Float64) => (AddFloat64, Float64),
                (Timestamp, Interval) => (AddTimestampInterval, Timestamp),
                (Interval, Timestamp) => {
                    mem::swap(&mut lexpr, &mut rexpr);
                    mem::swap(&mut ltype, &mut rtype);
                    (AddTimestampInterval, Timestamp)
                }
                _ => bail!(
                    "no overload for {:?} + {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
            Minus => match (&ltype.scalar_type, &rtype.scalar_type) {
                (Int32, Int32) => (SubInt32, Int32),
                (Int64, Int64) => (SubInt64, Int64),
                (Float32, Float32) => (SubFloat32, Float32),
                (Float64, Float64) => (SubFloat64, Float64),
                (Timestamp, Interval) => (SubTimestampInterval, Timestamp),
                _ => bail!(
                    "no overload for {:?} - {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
            Multiply => match (&ltype.scalar_type, &rtype.scalar_type) {
                (Int32, Int32) => (MulInt32, Int32),
                (Int64, Int64) => (MulInt64, Int64),
                (Float32, Float32) => (MulFloat32, Float32),
                (Float64, Float64) => (MulFloat64, Float64),
                _ => bail!(
                    "no overload for {:?} * {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
            Divide => match (&ltype.scalar_type, &rtype.scalar_type) {
                (Int32, Int32) => (DivInt32, Int32),
                (Int64, Int64) => (DivInt64, Int64),
                (Float32, Float32) => (DivFloat32, Float32),
                (Float64, Float64) => (DivFloat64, Float64),
                _ => bail!(
                    "no overload for {:?} / {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
            Modulo => match (&ltype.scalar_type, &rtype.scalar_type) {
                (Int32, Int32) => (ModInt32, Int32),
                (Int64, Int64) => (ModInt64, Int64),
                (Float32, Float32) => (ModFloat32, Float32),
                (Float64, Float64) => (ModFloat64, Float64),
                _ => bail!(
                    "no overload for {:?} % {:?}",
                    ltype.scalar_type,
                    rtype.scalar_type
                ),
            },
        };
        let expr = lexpr.call_binary(rexpr, func);
        let is_nullable_op = match op {
            Divide | Modulo => true,
            _ => false,
        };
        let typ = ColumnType::new(scalar_type)
            .nullable(ltype.nullable || rtype.nullable || is_nullable_op);
        Ok((expr, typ))
    }

    fn plan_comparison_op<'a>(
        &self,
        ctx: &ExprContext,
        op: ComparisonOp,
        left: &'a Expr,
        right: &'a Expr,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (mut lexpr, mut ltype) = self.plan_expr(ctx, left)?;
        let (mut rexpr, mut rtype) = self.plan_expr(ctx, right)?;

        let (mut exprs, typ) = try_coalesce_types(vec![(lexpr, ltype), (rexpr, rtype)], &op)?;
        assert_eq!(exprs.len(), 2);
        rexpr = exprs.pop().unwrap();
        lexpr = exprs.pop().unwrap();
        rtype = typ.clone();
        ltype = typ;

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
        let expr = lexpr.call_binary(rexpr, func);
        let typ = ColumnType::new(ScalarType::Bool).nullable(ltype.nullable || rtype.nullable);
        Ok((expr, typ))
    }

    fn plan_like<'a>(
        &self,
        ctx: &ExprContext,
        left: &'a Expr,
        right: &'a Expr,
        negate: bool,
    ) -> Result<(ScalarExpr, ColumnType), failure::Error> {
        let (lexpr, ltype) = self.plan_expr(ctx, left)?;
        let (rexpr, rtype) = self.plan_expr(ctx, right)?;

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
        // `BinaryFunc::MatchRegexp` returns `NULL` if the like
        // pattern is invalid. Ideally this would return an error
        // instead, but we don't currently support runtime errors.
        let typ = ColumnType::new(ScalarType::Bool).nullable(true);
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
        ScalarType::Date => 6,
        ScalarType::Timestamp => 7,
        _ => 8,
    };
    let max_scalar_type = exprs
        .iter()
        .map(|(_expr, typ)| &typ.scalar_type)
        .max_by_key(|scalar_type| scalar_type_prec(scalar_type))
        .unwrap()
        .clone();
    let nullable = exprs.iter().any(|(_expr, typ)| typ.nullable);
    let out_typ = ColumnType::new(max_scalar_type).nullable(nullable);
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

/// Plans a cast between two `RelationExpr`s of different types. If it is
/// impossible to cast between the two types, an error is returned.
///
/// Note that `plan_cast_internal` only understands [`ScalarType`]s. If you need
/// to cast between SQL [`DataType`]s, see [`Planner::plan_cast`].
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
        (Date, Timestamp) => expr.call_unary(CastDateToTimestamp),
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

fn plan_promote_int_int64<C>(
    context: C,
    e: ScalarExpr,
    typ: ColumnType,
    argname: &str,
) -> Result<(ScalarExpr, ColumnType), failure::Error>
where
    C: fmt::Display + Copy,
{
    let (caste, casttyp) = match typ.scalar_type {
        ScalarType::Int32 => plan_cast_internal(context, e, &typ, ScalarType::Int64)?,
        _ => (e, typ),
    };
    if casttyp.scalar_type != ScalarType::Int64 && casttyp.scalar_type != ScalarType::Null {
        bail!(
            "{} {:?} argument has non-integer type {:?}",
            context,
            argname,
            casttyp
        );
    }
    Ok((caste, casttyp))
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
                let mut seen = HashSet::new();
                Ok(self
                    .aggs
                    .into_iter()
                    .filter(move |agg| seen.insert(agg.clone()))
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
    /// Are aggregate functions allowed in this context
    allow_aggregates: bool,
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

    fn int32() -> (ScalarExpr, ColumnType) {
        (ScalarExpr::Literal(Datum::Int32(0)), ct(ScalarType::Int32))
    }

    fn int64() -> (ScalarExpr, ColumnType) {
        (ScalarExpr::Literal(Datum::Int64(0)), ct(ScalarType::Int64))
    }

    fn float32() -> (ScalarExpr, ColumnType) {
        (
            ScalarExpr::Literal(Datum::Float32(0.0.into())),
            ct(ScalarType::Float32),
        )
    }

    fn float64() -> (ScalarExpr, ColumnType) {
        (
            ScalarExpr::Literal(Datum::Float64(0.0.into())),
            ct(ScalarType::Float64),
        )
    }

    fn decimal(precision: u8, scale: u8) -> (ScalarExpr, ColumnType) {
        (
            ScalarExpr::Literal(Datum::from(0 as i128)),
            ct(ScalarType::Decimal(precision, scale)),
        )
    }

    #[test]
    fn test_type_coalescing() -> Result<(), failure::Error> {
        use ScalarType::*;

        let test_cases = vec![
            (vec![int32(), int64()], ct(Int64)),
            (vec![int64(), int32()], ct(Int64)),
            (vec![int64(), decimal(10, 10)], ct(Decimal(10, 10))),
            (vec![int64(), float32()], ct(Float32)),
            (vec![float32(), float64()], ct(Float64)),
        ];

        for (exprs, expected) in test_cases {
            let (_exprs, col_type) = try_coalesce_types(exprs, "test_type_coalescing")?;
            assert_eq!(col_type, expected);
        }

        Ok(())
    }
}
