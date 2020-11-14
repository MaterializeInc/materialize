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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryInto;
use std::iter;
use std::mem;

use anyhow::{anyhow, bail, ensure, Context};
use itertools::Itertools;
use ore::iter::IteratorExt;
use sql_parser::ast::visit::{self, Visit};
use sql_parser::ast::{
    DataType, Distinct, Expr, Function, FunctionArgs, Ident, InsertSource, JoinConstraint,
    JoinOperator, Limit, ObjectName, OrderByExpr, Query, Select, SelectItem, SetExpr, SetOperator,
    TableAlias, TableFactor, TableWithJoins, Value, Values,
};

use ::expr::{GlobalId, Id, RowSetFinishing};
use repr::adt::decimal::{Decimal, MAX_DECIMAL_PRECISION};
use repr::{
    strconv, ColumnName, Datum, RelationDesc, RelationType, RowArena, ScalarType, Timestamp,
};

use crate::catalog::CatalogItemType;
use crate::names::PartialName;
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::expr::{
    AbstractColumnType, AbstractExpr, AggregateExpr, BinaryFunc, CoercibleScalarExpr, ColumnOrder,
    ColumnRef, JoinKind, RelationExpr, ScalarExpr, UnaryFunc, VariadicFunc,
};
use crate::plan::func::{self, Func, FuncSpec};
use crate::plan::scope::{Scope, ScopeItem, ScopeItemName};
use crate::plan::statement::StatementContext;
use crate::plan::transform_ast;
use crate::plan::typeconv::{self, CastContext};

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
) -> Result<(RelationExpr, RelationDesc, RowSetFinishing), anyhow::Error> {
    transform_ast::transform_query(scx, &mut query)?;
    let qcx = QueryContext::root(scx, lifetime);
    let (mut expr, scope, mut finishing) = plan_query(&qcx, &query)?;

    // Attempt to push the finishing's ordering past its projection. This allows
    // data to be projected down on the workers rather than the coordinator. It
    // also improves the optimizer's demand analysis, as the optimizer can only
    // reason about demand information in `expr` (i.e., it can't see
    // `finishing.project`).
    try_push_projection_order_by(&mut expr, &mut finishing.project, &mut finishing.order_by);

    let typ = qcx.relation_type(&expr);
    let typ = RelationType::new(
        finishing
            .project
            .iter()
            .map(|i| typ.column_types[*i].clone())
            .collect(),
    );
    let desc = RelationDesc::new(typ, scope.column_names());

    Ok((expr, desc, finishing))
}

/// Attempts to push a projection through an order by.
///
/// The returned bool indicates whether the pushdown was successful or not.
/// Successful pushdown requires that all the columns referenced in `order_by`
/// are included in `project`.
///
/// When successful, `expr` is wrapped in a projection node, `order_by` is
/// rewritten to account for the pushed-down projection, and `project` is
/// replaced with the trivial projection. When unsuccessful, no changes are made
/// to any of the inputs.
fn try_push_projection_order_by(
    expr: &mut RelationExpr,
    project: &mut Vec<usize>,
    order_by: &mut Vec<ColumnOrder>,
) -> bool {
    let mut unproject = vec![None; expr.arity()];
    for (out_i, in_i) in project.iter().copied().enumerate() {
        unproject[in_i] = Some(out_i);
    }
    if order_by.iter().all(|ob| unproject[ob.column].is_some()) {
        let trivial_project = (0..project.len()).collect();
        *expr = expr.take().project(mem::replace(project, trivial_project));
        for ob in order_by {
            ob.column = unproject[ob.column].unwrap();
        }
        true
    } else {
        false
    }
}

pub fn plan_insert_query(
    scx: &StatementContext,
    table_name: ObjectName,
    columns: Vec<Ident>,
    source: InsertSource,
) -> Result<(GlobalId, RelationExpr), anyhow::Error> {
    let qcx = QueryContext::root(scx, QueryLifetime::OneShot);
    let name = scx.resolve_item(table_name)?;
    let table = scx.catalog.get_item(&name);
    let desc = table.desc()?;

    // Validate the target of the insert.
    if table.item_type() != CatalogItemType::Table {
        bail!(
            "cannot insert into {} '{}'",
            table.item_type(),
            table.name()
        );
    }
    if table.id().is_system() {
        bail!("cannot insert into system table '{}'", table.name());
    }

    // Validate target column order.
    let ordering = if columns.is_empty() {
        (0..desc.arity()).collect()
    } else {
        let column_to_index: HashMap<&ColumnName, usize> = desc
            .iter_names()
            .filter_map(|x| x)
            .enumerate()
            .map(|(idx, name)| (name, idx))
            .collect();

        let mut ordering = Vec::with_capacity(columns.len());
        for c in columns {
            let c = normalize::column_name(c);
            if let Some(idx) = column_to_index.get(&c) {
                ordering.push(*idx);
            } else {
                bail!(
                    "INSERT statement specifies column {}, but it is not present in table",
                    c.as_str(),
                );
            }
        }
        ordering
    };
    if ordering.iter().has_duplicates() {
        bail!("INSERT statement specifies duplicate column");
    }
    if ordering.len() < desc.arity() {
        unsupported!("INSERT statement with incomplete column set");
    }

    // Plan the source.
    let expr = match source {
        InsertSource::Query(mut query) => {
            transform_ast::transform_query(scx, &mut query)?;

            match query {
                // Special-case simple VALUES clauses, as PostgreSQL does, so
                // we can pass in a type hint for literal coercions.
                // See: https://github.com/postgres/postgres/blob/ad77039fa/src/backend/parser/analyze.c#L504-L518
                Query {
                    body: SetExpr::Values(values),
                    ctes,
                    order_by,
                    limit: None,
                    offset: None,
                } if ctes.is_empty() && order_by.is_empty() => {
                    let col_types = &desc.typ().column_types;
                    let type_hints = ordering
                        .iter()
                        .map(|idx| &col_types[*idx].scalar_type)
                        .collect();
                    let (expr, _scope) = plan_values(&qcx, &values.0, Some(type_hints))?;
                    expr
                }
                _ => {
                    let (expr, _scope) = plan_subquery(&qcx, &query)?;
                    expr
                }
            }
        }
        InsertSource::DefaultValues => unsupported!("INSERT ... DEFAULT VALUES"),
    };

    // Validate that the arity of the source query matches the arity of the
    // target table.
    let typ = qcx.relation_type(&expr);
    if typ.arity() != desc.arity() {
        bail!(
            "INSERT statement specifies {} columns, but table has {} columns",
            typ.arity(),
            desc.arity(),
        );
    }

    // Ensure the types of the source query match the types of the target table,
    // installing assignment casts where necessary and possible.
    let expr = cast_relation(
        &qcx,
        CastContext::Assignment,
        expr.project(ordering),
        desc.iter_types().map(|ty| &ty.scalar_type),
    )
    .map_err(|e| {
        anyhow!(
            "column \"{}\" is of type {} but expression is of type {}",
            desc.get_name(e.column)
                .unwrap_or(&ColumnName::from("?column?")),
            pgrepr::Type::from(&e.target_type).name(),
            pgrepr::Type::from(&e.source_type).name(),
        )
    })?;

    Ok((table.id(), expr))
}

struct CastRelationError {
    column: usize,
    source_type: ScalarType,
    target_type: ScalarType,
}

/// Cast a relation from one type to another using the specified type of cast.
///
/// The length of `target_types` must match the arity of `expr`.
fn cast_relation<'a, I>(
    qcx: &QueryContext,
    ccx: CastContext,
    expr: RelationExpr,
    target_types: I,
) -> Result<RelationExpr, CastRelationError>
where
    I: IntoIterator<Item = &'a ScalarType>,
{
    let ecx = &ExprContext {
        qcx,
        name: "values",
        scope: &Scope::empty(Some(qcx.outer_scope.clone())),
        relation_type: &qcx.relation_type(&expr),
        allow_aggregates: false,
        allow_subqueries: true,
    };
    let source_types = ecx
        .relation_type
        .column_types
        .iter()
        .map(|typ| &typ.scalar_type);
    let mut map_exprs = vec![];
    let mut project_key = vec![];
    for (i, (typ, target_typ)) in source_types.zip_eq(target_types).enumerate() {
        if typ != target_typ {
            let expr = ScalarExpr::Column(ColumnRef {
                level: 0,
                column: i,
            });
            match typeconv::plan_cast("relation cast", ecx, ccx, expr, target_typ) {
                Ok(expr) => {
                    project_key.push(ecx.relation_type.arity() + map_exprs.len());
                    map_exprs.push(expr);
                }
                Err(_) => {
                    return Err(CastRelationError {
                        column: i,
                        source_type: typ.clone(),
                        target_type: target_typ.clone(),
                    });
                }
            }
        } else {
            project_key.push(i);
        }
    }
    Ok(expr.map(map_exprs).project(project_key))
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

    transform_ast::transform_expr(scx, &mut expr)?;
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
        transform_ast::transform_expr(scx, &mut expr)?;
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
        Some(Limit {
            quantity: Expr::Value(Value::Number(x)),
            with_ties: false,
        }) => Some(x.parse()?),
        Some(Limit {
            quantity: _,
            with_ties: true,
        }) => unsupported!("FETCH ... WITH TIES"),
        Some(Limit {
            quantity: _,
            with_ties: _,
        }) => bail!("LIMIT must be an integer constant"),
    };
    let offset = match &q.offset {
        None => 0,
        Some(Expr::Value(Value::Number(x))) => x.parse()?,
        _ => bail!("OFFSET must be an integer constant"),
    };
    match &q.body {
        SetExpr::Select(s) => {
            let plan = plan_view_select(qcx, s, &q.order_by)?;
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
            let ecx = &ExprContext {
                qcx,
                name: "ORDER BY clause",
                scope: &scope,
                relation_type: &qcx.relation_type(&expr),
                allow_aggregates: true,
                allow_subqueries: true,
            };
            let (order_by, map_exprs) = plan_order_by_exprs(ecx, &q.order_by)?;
            let finishing = RowSetFinishing {
                order_by,
                limit,
                project: (0..ecx.relation_type.arity()).collect(),
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
        SetExpr::Select(select) => {
            let order_by_exprs = &[];
            let plan = plan_view_select(qcx, select, order_by_exprs)?;
            // We didn't provide any `order_by_exprs`, so `plan_view_select`
            // should not have planned any ordering.
            assert!(plan.order_by.is_empty());
            Ok((plan.expr.project(plan.project), plan.scope))
        }
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
        SetExpr::Values(Values(values)) => plan_values(qcx, values, None),
        SetExpr::Query(query) => {
            let (expr, scope) = plan_subquery(qcx, query)?;
            Ok((expr, scope))
        }
    }
}

fn plan_values(
    qcx: &QueryContext,
    values: &[Vec<Expr>],
    type_hints: Option<Vec<&ScalarType>>,
) -> Result<(RelationExpr, Scope), anyhow::Error> {
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
    for (index, col) in cols.iter().enumerate() {
        let type_hint = type_hints
            .as_ref()
            .and_then(|type_hints| type_hints.get(index).copied());
        let col = coerce_homogeneous_exprs("VALUES", ecx, plan_exprs(ecx, col)?, type_hint)?;
        col_types.push(ecx.column_type(&col[0]));
        col_iters.push(col.into_iter());
    }

    // Build constant relation.
    let typ = RelationType::new(col_types);
    let mut rows = vec![];
    for _ in 0..nrows {
        let row: Vec<_> = (0..ncols).map(|i| col_iters[i].next().unwrap()).collect();
        let empty = RelationExpr::constant(vec![vec![]], RelationType::new(vec![]));
        rows.push(empty.map(row));
    }
    let out = RelationExpr::Union {
        base: Box::new(RelationExpr::constant(vec![], typ)),
        inputs: rows,
    };

    // Build column names.
    let mut scope = Scope::empty(Some(qcx.outer_scope.clone()));
    for i in 0..ncols {
        let name = Some(format!("column{}", i + 1).into());
        scope.items.push(ScopeItem::from_column_name(name));
    }

    Ok((out, scope))
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
fn plan_view_select(
    qcx: &QueryContext,
    s: &Select,
    order_by_exprs: &[OrderByExpr],
) -> Result<SelectPlan, anyhow::Error> {
    let Select {
        distinct,
        projection,
        from,
        selection,
        group_by,
        having,
    } = s;

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
        let expr = plan_expr(ecx, &selection)
            .map_err(|e| anyhow::anyhow!("WHERE clause error: {}", e))?
            .type_as(ecx, &ScalarType::Bool)?;
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 3. Gather aggregates.
    let aggregates = {
        let mut aggregate_visitor = AggregateFuncVisitor::new(&qcx.scx);
        aggregate_visitor.visit_select(&s);
        for o in order_by_exprs {
            aggregate_visitor.visit_order_by_expr(o);
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
            let (group_expr, expr) = plan_group_by_expr(ecx, group_expr, &projection)?;
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
                    scope_item.expr = group_expr.cloned();
                    scope_item
                } else {
                    ScopeItem {
                        names: vec![],
                        expr: group_expr.cloned(),
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
                names: vec![],
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
        let expr = plan_expr(ecx, having)?.type_as(ecx, &ScalarType::Bool)?;
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 7. Handle SELECT clause.
    let (mut project_key, map_scope) = {
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
        for (select_item, column_name) in &projection {
            let expr = match select_item {
                ExpandedSelectItem::InputOrdinal(i) => {
                    if let Some(column) = select_all_mapping.get(&i).copied() {
                        ScalarExpr::Column(ColumnRef { level: 0, column })
                    } else {
                        bail!("column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function", from_scope.items[*i].short_display_name());
                    }
                }
                ExpandedSelectItem::Expr(expr) => plan_expr(ecx, &expr)?.type_as_any(ecx)?,
            };
            if let ScalarExpr::Column(ColumnRef { level: 0, column }) = expr {
                project_key.push(column);
                // Mark the output name as prioritized, so that they shadow any
                // input columns of the same name.
                if let Some(column_name) = column_name {
                    map_scope.items[column].names.push(ScopeItemName {
                        table_name: None,
                        column_name: Some(column_name.clone()),
                        priority: true,
                    });
                }
            } else {
                project_key.push(group_scope.len() + new_exprs.len());
                new_exprs.push(expr);
                map_scope.items.push(ScopeItem {
                    names: column_name
                        .iter()
                        .map(|column_name| ScopeItemName {
                            table_name: None,
                            column_name: Some(column_name.clone()),
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

    // Step 8. Handle intrusive ORDER BY and DISTINCT.
    let order_by = {
        let (mut order_by, mut map_exprs) = plan_projected_order_by_exprs(
            &ExprContext {
                qcx,
                name: "ORDER BY clause",
                scope: &map_scope,
                relation_type: &qcx.relation_type(&relation_expr),
                allow_aggregates: true,
                allow_subqueries: true,
            },
            order_by_exprs,
            &project_key,
        )?;

        match distinct {
            None => relation_expr = relation_expr.map(map_exprs),
            Some(Distinct::EntireRow) => {
                // `SELECT DISTINCT` only distincts on the columns in the SELECT
                // list, so we can't proceed if `ORDER BY` has introduced any
                // columns for arbitrary expressions. This matches PostgreSQL.
                if !try_push_projection_order_by(
                    &mut relation_expr,
                    &mut project_key,
                    &mut order_by,
                ) {
                    bail!("for SELECT DISTINCT, ORDER BY expressions must appear in select list");
                }
                assert!(map_exprs.is_empty());
                relation_expr = relation_expr.distinct();
            }
            Some(Distinct::On(exprs)) => {
                let ecx = &ExprContext {
                    qcx,
                    name: "DISTINCT ON clause",
                    scope: &map_scope,
                    relation_type: &qcx.relation_type(&relation_expr),
                    allow_aggregates: true,
                    allow_subqueries: true,
                };

                let mut distinct_exprs = vec![];
                for expr in exprs {
                    let expr = plan_order_by_or_distinct_expr(ecx, expr, &project_key)?;
                    distinct_exprs.push(expr);
                }

                let mut distinct_key = vec![];

                // If both `DISTINCT ON` and `ORDER BY` are specified, then the
                // `DISTINCT ON` expressions must match the initial `ORDER BY`
                // expressions, though the order of `DISTINCT ON` expressions
                // does not matter. This matches PostgreSQL and leaves the door
                // open to a future optimization where the `DISTINCT ON` and
                // `ORDER BY` operations happen in one pass.
                //
                // On the bright side, any columns that have already been
                // computed by `ORDER BY` can be reused in the distinct key.
                for ord in order_by.iter().take(distinct_exprs.len()) {
                    // The unusual construction of `expr` here is to ensure the
                    // temporary column expression lives long enough.
                    let mut expr = &ScalarExpr::Column(ColumnRef {
                        level: 0,
                        column: ord.column,
                    });
                    if ord.column >= map_scope.len() {
                        expr = &map_exprs[ord.column - map_scope.len()];
                    };
                    match distinct_exprs.iter().position(move |e| e == expr) {
                        None => bail!("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
                        Some(pos) => {
                            distinct_exprs.remove(pos);
                        }
                    }
                    distinct_key.push(ord.column);
                }

                // Add any remaining `DISTINCT ON` expressions to the key.
                for expr in distinct_exprs {
                    // If the expression is a reference to an existing column,
                    // do not introduce a new column to support it.
                    let column = match expr {
                        ScalarExpr::Column(ColumnRef { level: 0, column }) => column,
                        _ => {
                            map_exprs.push(expr);
                            map_scope.len() + map_exprs.len() - 1
                        }
                    };
                    distinct_key.push(column);
                }

                // `DISTINCT ON` is semantically a TopK with limit 1. The
                // columns in `ORDER BY` that are not part of the distinct key,
                // if there are any, determine the ordering within each group,
                // per PostgreSQL semantics.
                relation_expr = RelationExpr::TopK {
                    input: Box::new(relation_expr.map(map_exprs)),
                    order_key: order_by.iter().skip(distinct_key.len()).cloned().collect(),
                    group_key: distinct_key,
                    limit: Some(1),
                    offset: 0,
                }
            }
        }

        order_by
    };

    // Construct a clean scope to expose outwards, where all of the state that
    // accumulated in the scope during planning of this SELECT is erased. The
    // clean scope has at most one name for each column, and the names are not
    // associated with any table.
    let scope = Scope::from_source(
        None,
        projection.into_iter().map(|(_expr, name)| name),
        Some(qcx.outer_scope.clone()),
    );

    Ok(SelectPlan {
        expr: relation_expr,
        scope,
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
fn plan_group_by_expr<'a>(
    ecx: &ExprContext,
    group_expr: &'a Expr,
    projection: &'a [(ExpandedSelectItem, Option<ColumnName>)],
) -> Result<(Option<&'a Expr>, ScalarExpr), anyhow::Error> {
    let plan_projection = |column: usize| match &projection[column].0 {
        ExpandedSelectItem::InputOrdinal(column) => Ok((
            None,
            ScalarExpr::Column(ColumnRef {
                level: 0,
                column: *column,
            }),
        )),
        ExpandedSelectItem::Expr(expr) => Ok((
            Some(expr.as_ref()),
            plan_expr(&ecx, expr)?.type_as_any(ecx)?,
        )),
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
            res => Ok((Some(group_expr), res?)),
        },
        _ => Ok((
            Some(group_expr),
            plan_expr(&ecx, group_expr)?.type_as_any(ecx)?,
        )),
    }
}

/// Plans a slice of `ORDER BY` expressions.
fn plan_order_by_exprs(
    ecx: &ExprContext,
    order_by_exprs: &[OrderByExpr],
) -> Result<(Vec<ColumnOrder>, Vec<ScalarExpr>), anyhow::Error> {
    let project_key: Vec<_> = (0..ecx.scope.len()).collect();
    plan_projected_order_by_exprs(ecx, order_by_exprs, &project_key)
}

/// Like `plan_order_by_exprs`, except that any column ordinal references are
/// projected via `project_key` rather than being accepted directly.
fn plan_projected_order_by_exprs(
    ecx: &ExprContext,
    order_by_exprs: &[OrderByExpr],
    project_key: &[usize],
) -> Result<(Vec<ColumnOrder>, Vec<ScalarExpr>), anyhow::Error> {
    let mut order_by = vec![];
    let mut map_exprs = vec![];
    for obe in order_by_exprs {
        let expr = plan_order_by_or_distinct_expr(ecx, &obe.expr, project_key)?;
        // If the expression is a reference to an existing column,
        // do not introduce a new column to support it.
        let column = match expr {
            ScalarExpr::Column(ColumnRef { level: 0, column }) => column,
            _ => {
                map_exprs.push(expr);
                ecx.relation_type.arity() + map_exprs.len() - 1
            }
        };
        order_by.push(ColumnOrder {
            column,
            desc: !obe.asc.unwrap_or(true),
        });
    }
    Ok((order_by, map_exprs))
}

/// Plans an expression that appears in an `ORDER BY` or `DISTINCT ON` clause.
///
/// This is like `plan_expr_or_col_index`, except that any column ordinal
/// references are projected via `project_key` rather than being accepted
/// directly.
fn plan_order_by_or_distinct_expr(
    ecx: &ExprContext,
    expr: &Expr,
    project_key: &[usize],
) -> Result<ScalarExpr, anyhow::Error> {
    match check_col_index(&ecx.name, expr, project_key.len())? {
        Some(i) => Ok(ScalarExpr::Column(ColumnRef {
            level: 0,
            column: project_key[i],
        })),
        None => plan_expr(ecx, expr)?.type_as_any(ecx),
    }
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

fn plan_table_factor(
    left_qcx: &QueryContext,
    left: RelationExpr,
    left_scope: Scope,
    join_operator: &JoinOperator,
    table_factor: &TableFactor,
) -> Result<(RelationExpr, Scope), anyhow::Error> {
    let lateral = matches!(
        table_factor,
        TableFactor::Function { .. } | TableFactor::Derived { lateral: true, .. }
    );

    let qcx = if lateral {
        Cow::Owned(left_qcx.derived_context(left_scope.clone(), &left_qcx.relation_type(&left)))
    } else {
        Cow::Borrowed(left_qcx)
    };

    let (expr, scope) = match table_factor {
        TableFactor::Table { name, alias } => {
            let name = qcx.scx.resolve_item(name.clone())?;
            let item = qcx.scx.catalog.get_item(&name);
            let expr = RelationExpr::Get {
                id: Id::Global(item.id()),
                typ: item.desc()?.typ().clone(),
            };
            let scope = Scope::from_source(
                Some(name.into()),
                item.desc()?.iter_names().map(|n| n.cloned()),
                Some(qcx.outer_scope.clone()),
            );
            let scope = plan_table_alias(scope, alias.as_ref())?;
            (expr, scope)
        }

        TableFactor::Function { name, args, alias } => {
            let ecx = &ExprContext {
                qcx: &qcx,
                name: "FROM table function",
                scope: &Scope::empty(Some(qcx.outer_scope.clone())),
                relation_type: &RelationType::empty(),
                allow_aggregates: false,
                allow_subqueries: true,
            };
            plan_table_function(ecx, &name, alias.as_ref(), args)?
        }

        TableFactor::Derived {
            lateral: _,
            subquery,
            alias,
        } => {
            let (expr, scope) = plan_subquery(&qcx, &subquery)?;
            let scope = plan_table_alias(scope, alias.as_ref())?;
            (expr, scope)
        }

        TableFactor::NestedJoin { join, alias } => {
            let (identity, identity_scope) = plan_join_identity(&qcx);
            let (expr, scope) = plan_table_with_joins(
                &qcx,
                identity,
                identity_scope,
                &JoinOperator::CrossJoin,
                join,
            )?;
            let scope = plan_table_alias(scope, alias.as_ref())?;
            (expr, scope)
        }
    };

    plan_join_operator(
        &join_operator,
        left_qcx,
        left,
        left_scope,
        &qcx,
        expr,
        scope,
        lateral,
    )
}

fn plan_table_function(
    ecx: &ExprContext,
    name: &ObjectName,
    alias: Option<&TableAlias>,
    args: &FunctionArgs,
) -> Result<(RelationExpr, Scope), anyhow::Error> {
    let name = normalize::object_name(name.clone())?;

    if name.database.is_none() && name.schema.is_none() && name.item == "values" {
        // Produce a nice error message for the common typo
        // `SELECT * FROM VALUES (1)`.
        bail!("VALUES expression in FROM clause must be surrounded by parentheses");
    }

    let impls = match func::resolve_func(&ecx.qcx.scx, &name)? {
        Func::Table(impls) => impls,
        _ => bail!("{} is not a table function", name),
    };
    let args = match args {
        FunctionArgs::Star => bail!("{} does not accept * as an argument", name),
        FunctionArgs::Args(args) => plan_exprs(ecx, args)?,
    };
    let tf = func::select_impl(ecx, FuncSpec::Func(&name), impls, args)?;
    let call = RelationExpr::CallTable {
        func: tf.func,
        exprs: tf.exprs,
    };
    let scope = Scope::from_source(
        Some(PartialName {
            database: None,
            schema: None,
            item: name.item,
        }),
        tf.column_names,
        Some(ecx.qcx.outer_scope.clone()),
    );
    let mut scope = plan_table_alias(scope, alias)?;
    if let Some(alias) = alias {
        if let [item] = &mut *scope.items {
            // Strange special case for table functions that ouput one column.
            // If a table alias is provided but not a column alias, the column
            // implicitly takes on the same alias in addition to its inherent
            // name.
            //
            // Concretely, this means `SELECT x FROM generate_series(1, 5) AS x`
            // returns a single column of type int, even though
            //
            //     CREATE TABLE t (a int)
            //     SELECT x FROM t AS x
            //
            // would return a single column of type record(int).
            item.names.push(ScopeItemName {
                table_name: None,
                column_name: Some(normalize::column_name(alias.name.clone())),
                priority: false,
            });
        }
    }
    Ok((call, scope))
}

fn plan_table_alias(mut scope: Scope, alias: Option<&TableAlias>) -> Result<Scope, anyhow::Error> {
    if let Some(TableAlias {
        name,
        columns,
        strict,
    }) = alias
    {
        if (columns.len() > scope.items.len()) || (*strict && columns.len() != scope.items.len()) {
            bail!(
                "{} has {} columns available but {} columns specified",
                name,
                scope.items.len(),
                columns.len()
            );
        }

        let table_name = normalize::ident(name.to_owned());
        for (i, item) in scope.items.iter_mut().enumerate() {
            let column_name = columns
                .get(i)
                .map(|a| normalize::column_name(a.clone()))
                .or_else(|| item.names.get(0).and_then(|name| name.column_name.clone()));
            item.names = vec![ScopeItemName {
                table_name: Some(PartialName {
                    database: None,
                    schema: None,
                    item: table_name.clone(),
                }),
                column_name,
                priority: false,
            }];
        }
    }
    Ok(scope)
}

fn invent_column_name(ecx: &ExprContext, expr: &Expr) -> Option<ScopeItemName> {
    let name = match expr {
        Expr::Identifier(names) => names.last().map(|n| normalize::column_name(n.clone())),
        Expr::Function(func) => {
            let name = normalize::object_name(func.name.clone()).ok()?;
            if name.schema.as_deref() == Some("mz_internal") {
                None
            } else {
                Some(name.item.into())
            }
        }
        Expr::Coalesce { .. } => Some("coalesce".into()),
        Expr::Array { .. } => Some("array".into()),
        Expr::List { .. } => Some("list".into()),
        Expr::Cast { expr, .. } => return invent_column_name(ecx, expr),
        Expr::FieldAccess { field, .. } => Some(normalize::column_name(field.clone())),
        Expr::Exists { .. } => Some("exists".into()),
        Expr::Subquery(query) => {
            // A bit silly to have to plan the query here just to get its column
            // name, since we throw away the planned expression, but fixing this
            // requires a separate semantic analysis phase.
            let (_expr, scope) = plan_subquery(&ecx.derived_query_context(), query).ok()?;
            scope
                .items
                .first()
                .and_then(|item| item.names.first())
                .and_then(|name| name.column_name.clone())
        }
        _ => None,
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
                .or_else(|| invent_column_name(ecx, &expr).and_then(|n| n.column_name));
            Ok(vec![(ExpandedSelectItem::Expr(Cow::Borrowed(expr)), name)])
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn plan_join_operator(
    operator: &JoinOperator,
    left_qcx: &QueryContext,
    left: RelationExpr,
    left_scope: Scope,
    right_qcx: &QueryContext,
    right: RelationExpr,
    right_scope: Scope,
    lateral: bool,
) -> Result<(RelationExpr, Scope), anyhow::Error> {
    match operator {
        JoinOperator::Inner(constraint) => plan_join_constraint(
            &constraint,
            left_qcx,
            left,
            left_scope,
            right_qcx,
            right,
            right_scope,
            JoinKind::Inner { lateral },
        ),
        JoinOperator::LeftOuter(constraint) => plan_join_constraint(
            &constraint,
            left_qcx,
            left,
            left_scope,
            right_qcx,
            right,
            right_scope,
            JoinKind::LeftOuter { lateral },
        ),
        JoinOperator::RightOuter(constraint) => {
            if lateral {
                bail!("the combining JOIN type must be INNER or LEFT for a LATERAL reference");
            }
            plan_join_constraint(
                &constraint,
                left_qcx,
                left,
                left_scope,
                right_qcx,
                right,
                right_scope,
                JoinKind::RightOuter,
            )
        }
        JoinOperator::FullOuter(constraint) => {
            if lateral {
                bail!("the combining JOIN type must be INNER or LEFT for a LATERAL reference");
            }
            plan_join_constraint(
                &constraint,
                left_qcx,
                left,
                left_scope,
                right_qcx,
                right,
                right_scope,
                JoinKind::FullOuter,
            )
        }
        JoinOperator::CrossJoin => {
            // Suppress no-op joins to keep raw query plans understandable. Note
            // that LATERAL joins introduce a new inner scope for `right`, so
            // they can't be trivially elided. (We *could* rewrite all the
            // column references in `right`, but it doesn't seem worth it just
            // to make the raw query plans nicer. The optimizer doesn't have
            // trouble with the extra join.)
            let join = if left.is_join_identity() && !lateral {
                right
            } else if right.is_join_identity() {
                left
            } else {
                RelationExpr::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on: ScalarExpr::literal_true(),
                    kind: JoinKind::Inner { lateral },
                }
            };
            Ok((join, left_scope.product(right_scope)))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn plan_join_constraint<'a>(
    constraint: &'a JoinConstraint,
    left_qcx: &QueryContext,
    left: RelationExpr,
    left_scope: Scope,
    right_qcx: &QueryContext,
    right: RelationExpr,
    right_scope: Scope,
    kind: JoinKind,
) -> Result<(RelationExpr, Scope), anyhow::Error> {
    let (expr, scope) = match constraint {
        JoinConstraint::On(expr) => {
            let mut product_scope = left_scope.product(right_scope);
            let ecx = &ExprContext {
                qcx: &right_qcx,
                name: "ON clause",
                scope: &product_scope,
                relation_type: &RelationType::new(
                    left_qcx
                        .relation_type(&left)
                        .column_types
                        .into_iter()
                        .chain(right_qcx.relation_type(&right).column_types)
                        .collect(),
                ),
                allow_aggregates: false,
                allow_subqueries: true,
            };
            let on = plan_expr(ecx, expr)?.type_as(ecx, &ScalarType::Bool)?;
            if let JoinKind::Inner { .. } = kind {
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
            right_qcx,
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
                right_qcx,
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
            if ecx.param_types().borrow().contains_key(n) {
                ScalarExpr::Parameter(*n).into()
            } else {
                CoercibleScalarExpr::Parameter(*n)
            }
        }
        Expr::Array(exprs) => plan_array(ecx, exprs, None)?,
        Expr::List(exprs) => plan_list(ecx, exprs, None)?,
        Expr::Row { exprs } => {
            let mut out = vec![];
            for e in exprs {
                out.push(plan_expr(ecx, e)?);
            }
            CoercibleScalarExpr::LiteralRecord(out)
        }

        // Generalized functions, operators, and casts.
        Expr::Op { op, expr1, expr2 } => plan_op(ecx, op, expr1, expr2.as_deref())?.into(),
        Expr::Cast { expr, data_type } => {
            let to_scalar_type = scalar_type_from_sql(data_type)?;
            let expr = match &**expr {
                // Special case a direct cast of an ARRAY or LIST expression so
                // we can pass in the target type as a type hint. This is
                // a limited form of the coercion that we do for string literals
                // via CoercibleScalarExpr. We used to let CoercibleScalarExpr
                // handle ARRAY/LIST coercion too, but doing so causes
                // PostgreSQL compatibility trouble.
                //
                // See: https://github.com/postgres/postgres/blob/31f403e95/src/backend/parser/parse_expr.c#L2762-L2768
                Expr::Array(exprs) => plan_array(ecx, exprs, Some(&to_scalar_type))?,
                Expr::List(exprs) => plan_list(ecx, exprs, Some(&to_scalar_type))?,
                _ => plan_expr(ecx, expr)?,
            };
            let expr = typeconv::plan_coerce(ecx, expr, &to_scalar_type)?;
            typeconv::plan_cast("CAST", ecx, CastContext::Explicit, expr, &to_scalar_type)?.into()
        }
        Expr::Function(func) => plan_function(ecx, func)?.into(),

        // Special functions and operators.
        Expr::Not { expr } => {
            let ecx = ecx.with_name("NOT argument");
            ScalarExpr::CallUnary {
                func: UnaryFunc::Not,
                expr: Box::new(plan_expr(&ecx, expr)?.type_as(&ecx, &ScalarType::Bool)?),
            }
            .into()
        }
        Expr::And { left, right } => {
            let ecx = ecx.with_name("AND argument");
            ScalarExpr::CallBinary {
                func: BinaryFunc::And,
                expr1: Box::new(plan_expr(&ecx, left)?.type_as(&ecx, &ScalarType::Bool)?),
                expr2: Box::new(plan_expr(&ecx, right)?.type_as(&ecx, &ScalarType::Bool)?),
            }
            .into()
        }
        Expr::Or { left, right } => {
            let ecx = ecx.with_name("OR argument");
            ScalarExpr::CallBinary {
                func: BinaryFunc::Or,
                expr1: Box::new(plan_expr(&ecx, left)?.type_as(&ecx, &ScalarType::Bool)?),
                expr2: Box::new(plan_expr(&ecx, right)?.type_as(&ecx, &ScalarType::Bool)?),
            }
            .into()
        }
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
                exprs: coerce_homogeneous_exprs("coalesce", ecx, plan_exprs(ecx, exprs)?, None)?,
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
            let func = match &ty {
                ScalarType::List(_) => BinaryFunc::ListIndex,
                ScalarType::Array(_) => BinaryFunc::ArrayIndex,
                ty => bail!("cannot subscript type {}", ty),
            };

            expr.call_binary(
                plan_expr(ecx, subscript)?.cast_to(
                    "subscript (indexing)",
                    ecx,
                    CastContext::Explicit,
                    &ScalarType::Int64,
                )?,
                func,
            )
            .into()
        }

        Expr::SubscriptSlice { expr, positions } => {
            assert_ne!(
                positions.len(),
                0,
                "subscript expression must contain at least one position"
            );
            if positions.len() > 1 {
                ecx.require_experimental_mode("multi-dimensional slicing")?;
            }
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
                    plan_expr(ecx, start)?.cast_to(
                        op_str,
                        ecx,
                        CastContext::Explicit,
                        &ScalarType::Int64,
                    )?
                } else {
                    ScalarExpr::literal(Datum::Int64(1), ScalarType::Int64)
                };

                let end = if let Some(end) = &p.end {
                    plan_expr(ecx, end)?.cast_to(
                        op_str,
                        ecx,
                        CastContext::Explicit,
                        &ScalarType::Int64,
                    )?
                } else {
                    ScalarExpr::literal(Datum::Int64(i64::MAX - 1), ScalarType::Int64)
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
        Expr::AnyExpr { left, op, right } => {
            let lhs = plan_expr(ecx, left)?;
            let rhs = plan_expr(ecx, right)?;

            let (lhs, rhs) = if let Some(typ) = ecx.scalar_type(&lhs) {
                (
                    lhs.type_as(ecx, &typ)?,
                    rhs.type_as(
                        &ecx.with_name("ANY operand array"),
                        &ScalarType::Array(Box::new(typ)),
                    )?,
                )
            } else if let Some(ScalarType::Array(array_type)) = ecx.scalar_type(&rhs) {
                (
                    lhs.type_as(&ecx.with_name("ANY operand"), &array_type)?,
                    rhs.type_as(ecx, &ScalarType::Array(array_type))?,
                )
            } else {
                (lhs.type_as_any(ecx)?, rhs.type_as_any(ecx)?)
            };

            match &ecx.scalar_type(&rhs) {
                ScalarType::Array(array_type) => {
                    let element_type = ecx.scalar_type(&lhs);
                    if element_type != **array_type {
                        bail!(
                            "cannot evaluate ANY for element type {} and array type {}",
                            element_type,
                            array_type
                        )
                    }
                }
                _ => unsupported!("op ANY requires array on right hand side"),
            }

            let array_contains = ScalarExpr::CallBinary {
                func: BinaryFunc::ArrayContains,
                expr1: Box::new(lhs),
                expr2: Box::new(rhs),
            };
            let scalar_expr = match op.as_str() {
                "=" => array_contains,
                "<>" => ScalarExpr::CallUnary {
                    func: UnaryFunc::Not,
                    expr: Box::new(array_contains),
                },
                op => unsupported!(format!("ANY comparison op {}", op)),
            };
            CoercibleScalarExpr::Coerced(scalar_expr)
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
        Expr::AnySubquery { .. } => unreachable!("Expr::AnySubquery not desugared"),
        Expr::All { .. } => unreachable!("Expr::All not desugared"),
        Expr::Between { .. } => unreachable!("Expr::Between not desugared"),
    })
}

/// Plans a slice of expressions.
///
/// This function is a simple convenience function for mapping [`plan_expr`]
/// over a slice of expressions. The planned expressions are returned in the
/// same order as the input. If any of the expressions fail to plan, returns an
/// error instead.
fn plan_exprs<E>(ecx: &ExprContext, exprs: &[E]) -> Result<Vec<CoercibleScalarExpr>, anyhow::Error>
where
    E: std::borrow::Borrow<Expr>,
{
    let mut out = vec![];
    for expr in exprs {
        out.push(plan_expr(ecx, expr.borrow())?);
    }
    Ok(out)
}

fn plan_array(
    ecx: &ExprContext,
    exprs: &[Expr],
    type_hint: Option<&ScalarType>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    ecx.qcx.scx.require_experimental_mode("ARRAY")?;
    let (elem_type, exprs) = if exprs.is_empty() {
        if let Some(ScalarType::Array(elem_type)) = type_hint {
            ((**elem_type).clone(), vec![])
        } else {
            bail!("cannot determine type of empty array");
        }
    } else {
        let mut out = vec![];
        for expr in exprs {
            out.push(match expr {
                // Special case nested ARRAY expressions so we can plumb
                // the type hint through.
                Expr::Array(exprs) => plan_array(ecx, exprs, type_hint.clone())?,
                _ => plan_expr(ecx, expr)?,
            });
        }
        let type_hint = match type_hint {
            Some(ScalarType::Array(elem_type)) => Some(&**elem_type),
            _ => None,
        };
        let out = coerce_homogeneous_exprs("ARRAY expression", ecx, out, type_hint)?;
        (ecx.scalar_type(&out[0]), out)
    };
    Ok(ScalarExpr::CallVariadic {
        func: VariadicFunc::ArrayCreate { elem_type },
        exprs,
    }
    .into())
}

fn plan_list(
    ecx: &ExprContext,
    exprs: &[Expr],
    type_hint: Option<&ScalarType>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    let (elem_type, exprs) = if exprs.is_empty() {
        if let Some(ScalarType::List(elem_type)) = type_hint {
            ((**elem_type).clone(), vec![])
        } else {
            bail!("cannot determine type of empty list");
        }
    } else {
        let type_hint = match type_hint {
            Some(ScalarType::List(elem_type)) => Some(&**elem_type),
            _ => None,
        };
        let mut out = vec![];
        for expr in exprs {
            out.push(match expr {
                // Special case nested LIST expressions so we can plumb
                // the type hint through.
                Expr::List(exprs) => plan_list(ecx, exprs, type_hint)?,
                _ => plan_expr(ecx, expr)?,
            });
        }
        let out = coerce_homogeneous_exprs("LIST expression", ecx, out, type_hint)?;
        (ecx.scalar_type(&out[0]), out)
    };
    Ok(ScalarExpr::CallVariadic {
        func: VariadicFunc::ListCreate { elem_type },
        exprs,
    }
    .into())
}

/// Coerces a list of expressions such that all input expressions will be cast
/// to the same type. If successful, returns a new list of expressions in the
/// same order as the input, where each expression has the appropriate casts to
/// make them all of a uniform type.
///
/// Note that this is our implementation of Postgres' type conversion for
/// ["`UNION`, `CASE`, and Related Constructs"][union-type-conv], though it
/// isn't yet used in all of those cases.
///
/// [union-type-conv]:
/// https://www.postgresql.org/docs/12/typeconv-union-case.html
pub fn coerce_homogeneous_exprs(
    name: &str,
    ecx: &ExprContext,
    exprs: Vec<CoercibleScalarExpr>,
    type_hint: Option<&ScalarType>,
) -> Result<Vec<ScalarExpr>, anyhow::Error> {
    assert!(!exprs.is_empty());

    let types: Vec<_> = exprs.iter().map(|e| ecx.scalar_type(e)).collect();

    let target = match typeconv::guess_best_common_type(&types, type_hint) {
        Some(t) => t,
        None => bail!("Cannot determine homogenous type for arguments to {}", name),
    };

    // Try to cast all expressions to `target`.
    let mut out = Vec::new();
    for expr in exprs {
        let arg = typeconv::plan_coerce(ecx, expr, &target)?;
        match typeconv::plan_cast(name, ecx, CastContext::Implicit, arg.clone(), &target) {
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
    let name = normalize::object_name(sql_func.name.clone())?;
    let impls = match func::resolve_func(&ecx.qcx.scx, &name)? {
        Func::Aggregate(impls) => impls,
        _ => unreachable!("plan_aggregate called on non-aggregate function,"),
    };

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
        FunctionArgs::Star => vec![],
        FunctionArgs::Args(args) if args.is_empty() => {
            bail!(
                "{}(*) must be used to call a parameterless aggregate function",
                name
            );
        }
        FunctionArgs::Args(args) => plan_exprs(ecx, args)?,
    };
    let (mut expr, func) = func::select_impl(ecx, FuncSpec::Func(&name), impls, args)?;
    if let Some(filter) = &sql_func.filter {
        // If a filter is present, as in
        //
        //     <agg>(<expr>) FILTER (WHERE <cond>)
        //
        // we plan it by essentially rewriting the expression to
        //
        //     <agg>(CASE WHEN <cond> THEN <expr> ELSE <identity>)
        //
        // where <identity> is the identity input for <agg>.
        let cond = plan_expr(&ecx.with_name("FILTER"), filter)?.type_as(ecx, &ScalarType::Bool)?;
        let expr_typ = ecx.scalar_type(&expr);
        expr = ScalarExpr::If {
            cond: Box::new(cond),
            then: Box::new(expr),
            els: Box::new(ScalarExpr::literal(func.identity_datum(), expr_typ)),
        };
    }

    let mut seen_outer = false;
    let mut seen_inner = false;
    expr.visit_columns(0, &mut |depth, col| {
        if depth == 0 && col.level == 0 {
            seen_inner = true;
        } else if col.level > depth {
            seen_outer = true;
        }
    });
    if seen_outer && !seen_inner {
        unsupported!(
            3720,
            "aggregate functions that refer exclusively to outer columns"
        );
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
        .all_items()
        .iter()
        .filter(|(_level, _column, item)| {
            item.is_from_table(&PartialName {
                database: None,
                schema: None,
                item: col_name.as_str().to_owned(),
            })
        })
        .map(|(level, column, item)| {
            let expr = ScalarExpr::Column(ColumnRef {
                level: *level,
                column: *column,
            });
            let name = item
                .names
                .first()
                .and_then(|n| n.column_name.clone())
                .unwrap_or_else(|| ColumnName::from(format!("f{}", column + 1)));
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

fn plan_op(
    ecx: &ExprContext,
    op: &str,
    expr1: &Expr,
    expr2: Option<&Expr>,
) -> Result<ScalarExpr, anyhow::Error> {
    let impls = func::resolve_op(op)?;
    let args = match expr2 {
        None => plan_exprs(ecx, &[expr1])?,
        Some(expr2) => plan_exprs(ecx, &[expr1, expr2])?,
    };
    func::select_impl(ecx, FuncSpec::Op(op), impls, args)
}

fn plan_function<'a>(
    ecx: &ExprContext,
    sql_func: &'a Function,
) -> Result<ScalarExpr, anyhow::Error> {
    let name = normalize::object_name(sql_func.name.clone())?;
    let impls = match func::resolve_func(&ecx.qcx.scx, &name)? {
        Func::Aggregate(_) if ecx.allow_aggregates => {
            // should already have been caught by `scope.resolve_expr` in `plan_expr`
            bail!(
                "Internal error: encountered unplanned aggregate function: {:?}",
                sql_func,
            )
        }
        Func::Aggregate(_) => {
            bail!("aggregate functions are not allowed in {}", ecx.name);
        }
        Func::Table(_) => {
            unsupported!(
                1546,
                format!("table function ({}) in scalar position", name)
            );
        }
        Func::Scalar(impls) => impls,
    };

    if sql_func.over.is_some() {
        unsupported!(213, "window functions");
    }
    if sql_func.filter.is_some() {
        bail!(
            "FILTER specified but {}() is not an aggregate function",
            name
        );
    }
    let args = match &sql_func.args {
        FunctionArgs::Star => bail!("* argument is invalid with non-aggregate function {}", name),
        FunctionArgs::Args(args) => plan_exprs(ecx, args)?,
    };

    func::select_impl(ecx, FuncSpec::Func(&name), impls, args)
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
            Some(operand) => operand.clone().equals(c.clone()),
            None => c.clone(),
        };
        let cexpr = plan_expr(ecx, &c)?.type_as(ecx, &ScalarType::Bool)?;
        cond_exprs.push(cexpr);
        result_exprs.push(r);
    }
    result_exprs.push(match else_result {
        Some(else_result) => else_result,
        None => &Expr::Value(Value::Null),
    });
    let mut result_exprs =
        coerce_homogeneous_exprs("CASE", ecx, plan_exprs(ecx, &result_exprs)?, None)?;
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
    let expr = ScalarExpr::literal(datum, scalar_type);
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
        DataType::Uuid => ScalarType::Uuid,
        DataType::Array(elem_type) => ScalarType::Array(Box::new(scalar_type_from_sql(elem_type)?)),
        DataType::List(elem_type) => ScalarType::List(Box::new(scalar_type_from_sql(elem_type)?)),
        DataType::Oid => ScalarType::Oid,
        DataType::Binary(..)
        | DataType::Blob(_)
        | DataType::Clob(_)
        | DataType::Regclass
        | DataType::TimeTz
        | DataType::Varbinary(_)
        | DataType::Custom(_) => bail!("Unexpected SQL type: {:?}", data_type),
    })
}

pub fn scalar_type_from_pg(ty: &pgrepr::Type) -> Result<ScalarType, anyhow::Error> {
    match ty {
        pgrepr::Type::Bool => Ok(ScalarType::Bool),
        pgrepr::Type::Int4 => Ok(ScalarType::Int32),
        pgrepr::Type::Int8 => Ok(ScalarType::Int64),
        pgrepr::Type::Float4 => Ok(ScalarType::Float32),
        pgrepr::Type::Float8 => Ok(ScalarType::Float64),
        pgrepr::Type::Numeric => Ok(ScalarType::Decimal(0, 0)),
        pgrepr::Type::Date => Ok(ScalarType::Date),
        pgrepr::Type::Time => Ok(ScalarType::Time),
        pgrepr::Type::Timestamp => Ok(ScalarType::Timestamp),
        pgrepr::Type::TimestampTz => Ok(ScalarType::TimestampTz),
        pgrepr::Type::Interval => Ok(ScalarType::Interval),
        pgrepr::Type::Bytea => Ok(ScalarType::Bytes),
        pgrepr::Type::Text => Ok(ScalarType::String),
        pgrepr::Type::Jsonb => Ok(ScalarType::Jsonb),
        pgrepr::Type::Uuid => Ok(ScalarType::Uuid),
        pgrepr::Type::Array(t) => Ok(ScalarType::Array(Box::new(scalar_type_from_pg(t)?))),
        pgrepr::Type::List(l) => Ok(ScalarType::List(Box::new(scalar_type_from_pg(l)?))),
        pgrepr::Type::Record(_) => {
            bail!("internal error: can't convert from pg record to materialize record")
        }
        pgrepr::Type::Oid => Ok(ScalarType::Oid),
    }
}

/// This is used to collect aggregates from within an `Expr`.
/// See the explanation of aggregate handling at the top of the file for more details.
struct AggregateFuncVisitor<'a, 'ast> {
    scx: &'a StatementContext<'a>,
    aggs: Vec<&'ast Function>,
    within_aggregate: bool,
    err: Option<anyhow::Error>,
}

impl<'a, 'ast> AggregateFuncVisitor<'a, 'ast> {
    fn new(scx: &'a StatementContext<'a>) -> AggregateFuncVisitor<'a, 'ast> {
        AggregateFuncVisitor {
            scx,
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

impl<'a, 'ast> Visit<'ast> for AggregateFuncVisitor<'a, 'ast> {
    fn visit_function(&mut self, func: &'ast Function) {
        if let Ok(name) = normalize::object_name(func.name.clone()) {
            if let Ok(Func::Aggregate(_)) = func::resolve_func(self.scx, &name) {
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
#[derive(Debug, Clone)]
pub struct QueryContext<'a> {
    /// The context for the containing `Statement`.
    pub scx: &'a StatementContext<'a>,
    /// The lifetime that the planned query will have.
    pub lifetime: QueryLifetime,
    /// The scope of the outer relation expression.
    pub outer_scope: Scope,
    /// The type of the outer relation expressions.
    pub outer_relation_types: Vec<RelationType>,
}

impl<'a> QueryContext<'a> {
    pub fn root(scx: &'a StatementContext, lifetime: QueryLifetime) -> QueryContext<'a> {
        QueryContext {
            scx,
            lifetime,
            outer_scope: Scope::empty(None),
            outer_relation_types: vec![],
        }
    }

    fn relation_type(&self, expr: &RelationExpr) -> RelationType {
        expr.typ(&self.outer_relation_types, &self.scx.param_types.borrow())
    }

    fn derived_context(&self, scope: Scope, relation_type: &RelationType) -> QueryContext<'a> {
        QueryContext {
            scx: self.scx,
            lifetime: self.lifetime,
            outer_scope: scope,
            outer_relation_types: self
                .outer_relation_types
                .iter()
                .chain(std::iter::once(relation_type))
                .cloned()
                .collect(),
        }
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
        E: AbstractExpr,
    {
        expr.typ(
            &self.qcx.outer_relation_types,
            &self.relation_type,
            &self.qcx.scx.param_types.borrow(),
        )
    }

    pub fn scalar_type<E>(&self, expr: &E) -> <E::Type as AbstractColumnType>::AbstractScalarType
    where
        E: AbstractExpr,
    {
        self.column_type(expr).scalar_type()
    }

    fn derived_query_context(&self) -> QueryContext {
        self.qcx
            .derived_context(self.scope.clone(), self.relation_type)
    }

    pub fn require_experimental_mode(&self, feature_name: &str) -> Result<(), anyhow::Error> {
        self.qcx.scx.require_experimental_mode(feature_name)
    }

    pub fn param_types(&self) -> &RefCell<BTreeMap<usize, ScalarType>> {
        &self.qcx.scx.param_types
    }
}
