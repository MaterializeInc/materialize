// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL `Query`s are the declarative, computational part of SQL.
//! This module turns `Query`s into `HirRelationExpr`s - a more explicit, algebraic way of describing computation.

//! Functions named plan_* are typically responsible for handling a single node of the SQL ast. Eg `plan_query` is responsible for handling `sqlparser::ast::Query`.
//! plan_* functions which correspond to operations on relations typically return a `HirRelationExpr`.
//! plan_* functions which correspond to operations on scalars typically return a `HirScalarExpr` and a `ScalarType`. (The latter is because it's not always possible to infer from a `HirScalarExpr` what the intended type is - notably in the case of decimals where the scale/precision are encoded only in the type).

//! Aggregates are particularly twisty.
//! In SQL, a GROUP BY turns any columns not in the group key into vectors of values. Then anywhere later in the scope, an aggregate function can be applied to that group. Inside the arguments of an aggregate function, other normal functions are applied element-wise over the vectors. Thus `SELECT sum(foo.x + foo.y) FROM foo GROUP BY x` means adding the scalar `x` to the vector `y` and summing the results.
//! In `HirRelationExpr`, aggregates can only be applied immediately at the time of grouping.
//! To deal with this, whenever we see a SQL GROUP BY we look ahead for aggregates and precompute them in the `HirRelationExpr::Reduce`. When we reach the same aggregates during normal planning later on, we look them up in an `ExprContext` to find the precomputed versions.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::{TryFrom, TryInto};

use std::iter;
use std::mem;

use itertools::Itertools;
use uuid::Uuid;

use mz_expr::{func as expr_func, GlobalId, Id, LocalId, RowSetFinishing};
use mz_ore::collections::CollectionExt;
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_ore::str::StrExt;
use mz_repr::adt::char::CharLength;
use mz_repr::adt::numeric::{NumericMaxScale, NUMERIC_DATUM_MAX_PRECISION};
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{
    strconv, ColumnName, ColumnType, Datum, RelationDesc, RelationType, Row, RowArena, ScalarType,
};

use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    Assignment, DeleteStatement, Distinct, Expr, Function, FunctionArgs, HomogenizingFunction,
    Ident, InsertSource, IsExprConstruct, Join, JoinConstraint, JoinOperator, Limit, OrderByExpr,
    Query, Select, SelectItem, SetExpr, SetOperator, SubscriptPosition, TableAlias, TableFactor,
    TableFunction, TableWithJoins, UnresolvedObjectName, UpdateStatement, Value, Values,
};

use crate::catalog::{CatalogItemType, CatalogType, SessionCatalog};
use crate::func::{self, Func, FuncSpec};
use crate::names::{Aug, PartialName, ResolvedDataType, ResolvedObjectName};
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::expr::{
    AbstractColumnType, AbstractExpr, AggregateExpr, AggregateFunc, BinaryFunc,
    CoercibleScalarExpr, ColumnOrder, ColumnRef, HirRelationExpr, HirScalarExpr, JoinKind,
    ScalarWindowExpr, ScalarWindowFunc, UnaryFunc, VariadicFunc, WindowExpr, WindowExprType,
};
use crate::plan::plan_utils::{self, JoinSide};
use crate::plan::scope::{Scope, ScopeItem};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::typeconv::{self, CastContext};
use crate::plan::{transform_ast, PlanContext};
use crate::plan::{Params, QueryWhen};

pub struct PlannedQuery<E> {
    pub expr: E,
    pub desc: RelationDesc,
    pub finishing: RowSetFinishing,
}

/// Plans a top-level query, returning the `HirRelationExpr` describing the query
/// plan, the `RelationDesc` describing the shape of the result set, a
/// `RowSetFinishing` describing post-processing that must occur before results
/// are sent to the client, and the types of the parameters in the query, if any
/// were present.
///
/// Note that the returned `RelationDesc` describes the expression after
/// applying the returned `RowSetFinishing`.
pub fn plan_root_query(
    scx: &StatementContext,
    mut query: Query<Aug>,
    lifetime: QueryLifetime,
) -> Result<PlannedQuery<HirRelationExpr>, PlanError> {
    transform_ast::transform_query(scx, &mut query)?;
    let mut qcx = QueryContext::root(scx, lifetime);
    let (mut expr, scope, mut finishing) = plan_query(&mut qcx, &query)?;

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

    Ok(PlannedQuery {
        expr,
        desc,
        finishing,
    })
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
    expr: &mut HirRelationExpr,
    project: &mut Vec<usize>,
    order_by: &mut Vec<ColumnOrder>,
) -> bool {
    let mut unproject = vec![None; expr.arity()];
    for (out_i, in_i) in project.iter().copied().enumerate() {
        unproject[in_i] = Some(out_i);
    }
    if order_by
        .iter()
        .all(|ob| ob.column < unproject.len() && unproject[ob.column].is_some())
    {
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
    table_name: ResolvedObjectName,
    columns: Vec<Ident>,
    source: InsertSource<Aug>,
) -> Result<(GlobalId, HirRelationExpr), PlanError> {
    let mut qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));
    let table = scx.get_item_by_name(&table_name)?;

    // Validate the target of the insert.
    if table.item_type() != CatalogItemType::Table {
        sql_bail!(
            "cannot insert into {} '{}'",
            table.item_type(),
            table.name()
        );
    }
    let desc = table.desc()?;
    let defaults = table
        .table_details()
        .expect("attempted to insert into non-table");

    if table.id().is_system() {
        sql_bail!("cannot insert into system table '{}'", table.name());
    }

    let columns: Vec<_> = columns.into_iter().map(normalize::column_name).collect();

    // Validate target column order.
    let mut source_types = Vec::with_capacity(columns.len());
    let mut ordering = Vec::with_capacity(columns.len());

    if columns.is_empty() {
        // Columns in source query must be in order. Let's guess the full shape and truncate to the
        // right size later after planning the source query
        source_types.extend(desc.iter_types().map(|x| &x.scalar_type));
        ordering.extend(0..desc.arity());
    } else {
        let column_by_name: HashMap<&ColumnName, (usize, &ColumnType)> = desc
            .iter()
            .enumerate()
            .map(|(idx, (name, typ))| (name, (idx, typ)))
            .collect();

        for c in &columns {
            if let Some((idx, typ)) = column_by_name.get(c) {
                ordering.push(*idx);
                source_types.push(&typ.scalar_type);
            } else {
                sql_bail!(
                    "column {} of relation {} does not exist",
                    c.as_str().quoted(),
                    table.name().to_string().quoted()
                );
            }
        }
        if let Some(dup) = columns.iter().duplicates().next() {
            sql_bail!("column {} specified more than once", dup.as_str().quoted());
        }
    };

    // Plan the source.
    let expr = match source {
        InsertSource::Query(mut query) => {
            transform_ast::transform_query(scx, &mut query)?;

            match query {
                // Special-case simple VALUES clauses as PostgreSQL does.
                Query {
                    body: SetExpr::Values(Values(values)),
                    ctes,
                    order_by,
                    limit: None,
                    offset: None,
                } if ctes.is_empty() && order_by.is_empty() => {
                    let names: Vec<_> = ordering.iter().map(|i| desc.get_name(*i)).collect();
                    plan_values_insert(&qcx, &names, &source_types, &values)?
                }
                _ => {
                    let (expr, _scope) = plan_nested_query(&mut qcx, &query)?;
                    expr
                }
            }
        }
        InsertSource::DefaultValues => {
            HirRelationExpr::constant(vec![vec![]], RelationType::empty())
        }
    };

    let typ = qcx.relation_type(&expr);

    // Validate that the arity of the source query is at most the size of declared columns or the
    // size of the table if none are declared
    let max_columns = if columns.is_empty() {
        desc.arity()
    } else {
        columns.len()
    };
    if typ.arity() > max_columns {
        sql_bail!("INSERT has more expressions than target columns");
    }
    // But it should never have less than the declared columns (or zero)
    if typ.arity() < columns.len() {
        sql_bail!("INSERT has more target columns than expressions");
    }

    // Trim now that we know for sure the correct arity of the source query
    source_types.truncate(typ.arity());
    ordering.truncate(typ.arity());

    // Ensure the types of the source query match the types of the target table,
    // installing assignment casts where necessary and possible.
    let expr = cast_relation(&qcx, CastContext::Assignment, expr, source_types).map_err(|e| {
        PlanError::Unstructured(format!(
            "column {} is of type {} but expression is of type {}",
            desc.get_name(ordering[e.column]).as_str().quoted(),
            qcx.humanize_scalar_type(&e.target_type),
            qcx.humanize_scalar_type(&e.source_type),
        ))
    })?;

    // Fill in any omitted columns and rearrange into correct order
    let mut map_exprs = vec![];
    let mut project_key = Vec::with_capacity(desc.arity());

    // Maps from table column index to position in the source query
    let col_to_source: HashMap<_, _> = ordering.iter().enumerate().map(|(a, b)| (b, a)).collect();

    let column_details = desc.iter_types().zip_eq(defaults).enumerate();
    for (col_idx, (col_typ, default)) in column_details {
        if let Some(src_idx) = col_to_source.get(&col_idx) {
            project_key.push(*src_idx);
        } else {
            let hir = plan_default_expr(scx, default, &col_typ.scalar_type)?;
            project_key.push(typ.arity() + map_exprs.len());
            map_exprs.push(hir);
        }
    }

    Ok((table.id(), expr.map(map_exprs).project(project_key)))
}

pub fn plan_copy_from(
    scx: &StatementContext,
    table_name: ResolvedObjectName,
    columns: Vec<Ident>,
) -> Result<(GlobalId, RelationDesc, Vec<usize>), PlanError> {
    let table = scx.get_item_by_name(&table_name)?;

    // Validate the target of the insert.
    if table.item_type() != CatalogItemType::Table {
        sql_bail!(
            "cannot insert into {} '{}'",
            table.item_type(),
            table.name()
        );
    }
    let mut desc = table.desc()?.clone();
    let _ = table
        .table_details()
        .expect("attempted to insert into non-table");

    if table.id().is_system() {
        sql_bail!("cannot insert into system table '{}'", table.name());
    }

    let mut ordering = Vec::with_capacity(columns.len());

    if columns.is_empty() {
        ordering.extend(0..desc.arity());
    } else {
        let columns: Vec<_> = columns.into_iter().map(normalize::column_name).collect();
        let column_by_name: HashMap<&ColumnName, (usize, &ColumnType)> = desc
            .iter()
            .enumerate()
            .map(|(idx, (name, typ))| (name, (idx, typ)))
            .collect();

        let mut names = Vec::with_capacity(columns.len());
        let mut source_types = Vec::with_capacity(columns.len());

        for c in &columns {
            if let Some((idx, typ)) = column_by_name.get(c) {
                ordering.push(*idx);
                source_types.push((*typ).clone());
                names.push(c.clone());
            } else {
                sql_bail!(
                    "column {} of relation {} does not exist",
                    c.as_str().quoted(),
                    table.name().to_string().quoted()
                );
            }
        }
        if let Some(dup) = columns.iter().duplicates().next() {
            sql_bail!("column {} specified more than once", dup.as_str().quoted());
        }

        desc = RelationDesc::new(RelationType::new(source_types), names);
    };

    Ok((table.id(), desc, ordering))
}

/// Builds a plan that adds the default values for the missing columns and re-orders
/// the datums in the given rows to match the order in the target table.
pub fn plan_copy_from_rows(
    pcx: &PlanContext,
    catalog: &dyn SessionCatalog,
    id: GlobalId,
    columns: Vec<usize>,
    rows: Vec<mz_repr::Row>,
) -> Result<HirRelationExpr, PlanError> {
    let table = catalog.get_item_by_id(&id);
    let desc = table.desc()?;

    let defaults = table
        .table_details()
        .expect("attempted to insert into non-table");

    let column_types = columns
        .iter()
        .map(|x| desc.typ().column_types[*x].clone())
        .map(|mut x| {
            // Null constraint is enforced later, when inserting the row in the table.
            // Without this, an assert is hit during lowering.
            x.nullable = true;
            x
        })
        .collect();
    let typ = RelationType::new(column_types);
    let expr = HirRelationExpr::Constant {
        rows,
        typ: typ.clone(),
    };

    // Exit early with just the raw constant if we know that all columns are present
    // and in the correct order. This lets us bypass expensive downstream optimizations
    // more easily, as at every stage we know this expression is nothing more than
    // a constant (as opposed to e.g. a constant with wiith an identity map and identity
    // projection).
    let default: Vec<_> = (0..desc.arity()).collect();
    if columns == default {
        return Ok(expr);
    }

    // Fill in any omitted columns and rearrange into correct order
    let mut map_exprs = vec![];
    let mut project_key = Vec::with_capacity(desc.arity());

    // Maps from table column index to position in the source query
    let col_to_source: HashMap<_, _> = columns.iter().enumerate().map(|(a, b)| (b, a)).collect();

    let scx = StatementContext::new(Some(pcx), catalog);

    let column_details = desc.iter_types().zip_eq(defaults).enumerate();
    for (col_idx, (col_typ, default)) in column_details {
        if let Some(src_idx) = col_to_source.get(&col_idx) {
            project_key.push(*src_idx);
        } else {
            let hir = plan_default_expr(&scx, default, &col_typ.scalar_type)?;
            project_key.push(typ.arity() + map_exprs.len());
            map_exprs.push(hir);
        }
    }

    Ok(expr.map(map_exprs).project(project_key))
}

/// Common information used for DELETE and UPDATE plans.
pub struct ReadThenWritePlan {
    pub id: GlobalId,
    /// WHERE filter.
    pub selection: HirRelationExpr,
    /// Map from column index to SET expression. Empty for DELETE statements.
    pub assignments: HashMap<usize, HirScalarExpr>,
    pub finishing: RowSetFinishing,
}

pub fn plan_delete_query(
    scx: &StatementContext,
    mut delete_stmt: DeleteStatement<Aug>,
) -> Result<ReadThenWritePlan, PlanError> {
    transform_ast::run_transforms(
        scx,
        |t, delete_stmt| t.visit_delete_statement_mut(delete_stmt),
        &mut delete_stmt,
    )?;

    let qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));
    plan_mutation_query_inner(
        qcx,
        delete_stmt.table_name,
        delete_stmt.alias,
        delete_stmt.using,
        vec![],
        delete_stmt.selection,
    )
}

pub fn plan_update_query(
    scx: &StatementContext,
    mut update_stmt: UpdateStatement<Aug>,
) -> Result<ReadThenWritePlan, PlanError> {
    transform_ast::run_transforms(
        scx,
        |t, update_stmt| t.visit_update_statement_mut(update_stmt),
        &mut update_stmt,
    )?;

    let qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));

    plan_mutation_query_inner(
        qcx,
        update_stmt.table_name,
        None,
        vec![],
        update_stmt.assignments,
        update_stmt.selection,
    )
}

pub fn plan_mutation_query_inner(
    qcx: QueryContext,
    table_name: ResolvedObjectName,
    alias: Option<TableAlias>,
    using: Vec<TableWithJoins<Aug>>,
    assignments: Vec<Assignment<Aug>>,
    selection: Option<Expr<Aug>>,
) -> Result<ReadThenWritePlan, PlanError> {
    // Get global ID.
    let id = match table_name.id {
        Id::Global(id) => id,
        _ => sql_bail!("cannot mutate non-user table"),
    };

    // Perform checks on item with given ID.
    let item = qcx.scx.get_item_by_id(&id);
    if item.item_type() != CatalogItemType::Table {
        sql_bail!("cannot mutate {} '{}'", item.item_type(), item.name());
    }
    if id.is_system() {
        sql_bail!("cannot mutate system table '{}'", item.name());
    }

    // Derive structs for operation from validated table
    let (mut get, scope) = qcx.resolve_table_name(table_name)?;
    let scope = plan_table_alias(scope, alias.as_ref())?;
    let desc = item.desc()?;
    let relation_type = qcx.relation_type(&get);

    if using.is_empty() {
        if let Some(expr) = selection {
            let ecx = &ExprContext {
                qcx: &qcx,
                name: "WHERE clause",
                scope: &scope,
                relation_type: &relation_type,
                allow_aggregates: false,
                allow_subqueries: true,
                allow_windows: false,
            };
            let expr = plan_expr(&ecx, &expr)?.type_as(&ecx, &ScalarType::Bool)?;
            get = get.filter(vec![expr]);
        }
    } else {
        get = handle_mutation_using_clause(&qcx, selection, using, get, scope.clone())?;
    }

    let mut sets = HashMap::new();
    for Assignment { id, value } in assignments {
        // Get the index and type of the column.
        let name = normalize::column_name(id);
        match desc.get_by_name(&name) {
            Some((idx, typ)) => {
                let ecx = &ExprContext {
                    qcx: &qcx,
                    name: "SET clause",
                    scope: &scope,
                    relation_type: &relation_type,
                    allow_aggregates: false,
                    allow_subqueries: false,
                    allow_windows: false,
                };
                let expr = plan_expr(&ecx, &value)?.cast_to(
                    ecx,
                    CastContext::Assignment,
                    &typ.scalar_type,
                )?;

                if sets.insert(idx, expr).is_some() {
                    sql_bail!("column {} set twice", name)
                }
            }
            None => sql_bail!("unknown column {}", name),
        };
    }

    let finishing = RowSetFinishing {
        order_by: vec![],
        limit: None,
        offset: 0,
        project: (0..desc.arity()).collect(),
    };

    Ok(ReadThenWritePlan {
        id,
        selection: get,
        finishing,
        assignments: sets,
    })
}

// Adjust `get` to perform an existential subquery on `using` accounting for
// `selection`.
//
// If `USING`, we essentially want to rewrite the query as a correlated
// existential subquery, i.e.
// ```
// ...WHERE EXISTS (SELECT 1 FROM <using> WHERE <selection>)
// ```
// However, we can't do that directly because of esoteric rules w/r/t `lateral`
// subqueries.
// https://github.com/postgres/postgres/commit/158b7fa6a34006bdc70b515e14e120d3e896589b
fn handle_mutation_using_clause(
    qcx: &QueryContext,
    selection: Option<Expr<Aug>>,
    using: Vec<TableWithJoins<Aug>>,
    get: HirRelationExpr,
    outer_scope: Scope,
) -> Result<HirRelationExpr, PlanError> {
    // Plan `USING` as a cross-joined `FROM` without knowledge of the
    // statement's `FROM` target. This prevents `lateral` subqueries from
    // "seeing" the `FROM` target.
    let (mut using_rel_expr, using_scope) =
        using.into_iter().fold(Ok(plan_join_identity()), |l, twj| {
            let (left, left_scope) = l?;
            plan_join(
                qcx,
                left,
                left_scope,
                &Join {
                    relation: TableFactor::NestedJoin {
                        join: Box::new(twj),
                        alias: None,
                    },
                    join_operator: JoinOperator::CrossJoin,
                },
            )
        })?;

    if let Some(expr) = selection {
        // Join `FROM` with `USING` tables, like `USING..., FROM`. This gives us
        // PG-like semantics e.g. expressing ambiguous column references. We put
        // `USING...` first for no real reason, but making a different decision
        // would require adjusting the column references on this relation
        // differently.
        let on = HirScalarExpr::literal_true();
        let joined = using_rel_expr
            .clone()
            .join(get.clone(), on, JoinKind::Inner);
        let joined_scope = using_scope.product(outer_scope)?;
        let joined_relation_type = qcx.relation_type(&joined);

        let ecx = &ExprContext {
            qcx: &qcx,
            name: "WHERE clause",
            scope: &joined_scope,
            relation_type: &joined_relation_type,
            allow_aggregates: false,
            allow_subqueries: true,
            allow_windows: false,
        };

        // Plan the filter expression on `FROM, USING...`.
        let mut expr = plan_expr(&ecx, &expr)?.type_as(&ecx, &ScalarType::Bool)?;

        // Rewrite all column referring to the `FROM` section of `joined` (i.e.
        // those to the right of `using_rel_expr`) to instead be correlated to
        // the outer relation, i.e. `get`.
        let using_rel_arity = qcx.relation_type(&using_rel_expr).arity();
        expr.visit_mut(&mut |e| {
            if let HirScalarExpr::Column(c) = e {
                if c.column >= using_rel_arity {
                    c.level += 1;
                    c.column -= using_rel_arity;
                };
            }
        });

        // Filter `USING` tables like `<using_rel_expr> WHERE <expr>`. Note that
        // this filters the `USING` tables, _not_ the joined `USING..., FROM`
        // relation.
        using_rel_expr = using_rel_expr.filter(vec![expr]);
    } else {
        // Check that scopes are at compatible (i.e. do not double-reference
        // same table), despite lack of selection
        let _joined_scope = using_scope.product(outer_scope)?;
    }
    // From pg: Since the result [of EXISTS (<subquery>)] depends only on
    // whether any rows are returned, and not on the contents of those rows,
    // the output list of the subquery is normally unimportant.
    //
    // This means we don't need to worry about projecting/mapping any
    // additional expressions here.
    //
    // https://www.postgresql.org/docs/14/functions-subquery.html

    // Filter `get` like `...WHERE EXISTS (<using_rel_expr>)`.
    Ok(get.filter(vec![using_rel_expr.exists()]))
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
    expr: HirRelationExpr,
    target_types: I,
) -> Result<HirRelationExpr, CastRelationError>
where
    I: IntoIterator<Item = &'a ScalarType>,
{
    let ecx = &ExprContext {
        qcx,
        name: "values",
        scope: &Scope::empty(),
        relation_type: &qcx.relation_type(&expr),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_windows: false,
    };
    let mut map_exprs = vec![];
    let mut project_key = vec![];
    for (i, target_typ) in target_types.into_iter().enumerate() {
        let expr = HirScalarExpr::column(i);
        // We plan every cast and check the evaluated expressions rather than
        // checking the types directly because of some complex casting rules
        // between types not expressed in `ScalarType` equality.
        match typeconv::plan_cast(ecx, ccx, expr.clone(), target_typ) {
            Ok(cast_expr) => {
                if expr == cast_expr {
                    // Cast between types was unnecessary
                    project_key.push(i);
                } else {
                    // Cast between types required
                    project_key.push(ecx.relation_type.arity() + map_exprs.len());
                    map_exprs.push(cast_expr);
                }
            }
            Err(_) => {
                return Err(CastRelationError {
                    column: i,
                    source_type: ecx.scalar_type(&expr),
                    target_type: target_typ.clone(),
                });
            }
        }
    }
    Ok(expr.map(map_exprs).project(project_key))
}

/// Plans an expression in the AS OF position of a `SELECT` or `TAIL` statement.
pub fn plan_as_of(scx: &StatementContext, expr: Option<Expr<Aug>>) -> Result<QueryWhen, PlanError> {
    let mut expr = match expr {
        None => return Ok(QueryWhen::Immediately),
        Some(expr) => expr,
    };

    let scope = Scope::empty();
    let desc = RelationDesc::empty();
    let qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));

    transform_ast::transform_expr(scx, &mut expr)?;

    let ecx = &ExprContext {
        qcx: &qcx,
        name: "AS OF",
        scope: &scope,
        relation_type: &desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_windows: false,
    };
    let expr = plan_expr(ecx, &expr)?
        .type_as_any(ecx)?
        .lower_uncorrelated()?;
    Ok(QueryWhen::AtTimestamp(expr))
}

pub fn plan_default_expr(
    scx: &StatementContext,
    expr: &Expr<Aug>,
    target_ty: &ScalarType,
) -> Result<HirScalarExpr, PlanError> {
    let qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));
    let ecx = &ExprContext {
        qcx: &qcx,
        name: "DEFAULT expression",
        scope: &Scope::empty(),
        relation_type: &RelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_windows: false,
    };
    let hir = plan_expr(ecx, &expr)?.cast_to(ecx, CastContext::Assignment, target_ty)?;
    Ok(hir)
}

pub fn plan_params<'a>(
    scx: &'a StatementContext,
    params: Vec<Expr<Aug>>,
    desc: &StatementDesc,
) -> Result<Params, PlanError> {
    if params.len() != desc.param_types.len() {
        sql_bail!(
            "expected {} params, got {}",
            desc.param_types.len(),
            params.len()
        );
    }

    let qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));
    let scope = Scope::empty();
    let rel_type = RelationType::empty();

    let mut datums = Row::default();
    let mut packer = datums.packer();
    let mut types = Vec::new();
    let temp_storage = &RowArena::new();
    for (mut expr, ty) in params.into_iter().zip(&desc.param_types) {
        transform_ast::transform_expr(scx, &mut expr)?;

        let ecx = &ExprContext {
            qcx: &qcx,
            name: "EXECUTE",
            scope: &scope,
            relation_type: &rel_type,
            allow_aggregates: false,
            allow_subqueries: false,
            allow_windows: false,
        };
        let ex = plan_expr(ecx, &expr)?.type_as_any(ecx)?;
        let st = ecx.scalar_type(&ex);
        if st != *ty {
            sql_bail!(
                "mismatched parameter type: expected {}, got {}",
                ecx.humanize_scalar_type(&ty),
                ecx.humanize_scalar_type(&st),
            );
        }
        let ex = ex.lower_uncorrelated()?;
        let evaled = ex.eval(&[], temp_storage)?;
        packer.push(evaled);
        types.push(st);
    }
    Ok(Params { datums, types })
}

pub fn plan_index_exprs<'a>(
    scx: &'a StatementContext,
    on_desc: &RelationDesc,
    exprs: Vec<Expr<Aug>>,
) -> Result<Vec<mz_expr::MirScalarExpr>, PlanError> {
    let scope = Scope::from_source(None, on_desc.iter_names());
    let qcx = QueryContext::root(scx, QueryLifetime::Static);

    let ecx = &ExprContext {
        qcx: &qcx,
        name: "CREATE INDEX",
        scope: &scope,
        relation_type: on_desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_windows: false,
    };
    let mut out = vec![];
    for mut expr in exprs {
        transform_ast::transform_expr(scx, &mut expr)?;
        let expr = plan_expr_or_col_index(ecx, &expr)?;
        out.push(expr.lower_uncorrelated()?);
    }
    Ok(out)
}

fn plan_expr_or_col_index(ecx: &ExprContext, e: &Expr<Aug>) -> Result<HirScalarExpr, PlanError> {
    match check_col_index(&ecx.name, e, ecx.relation_type.column_types.len())? {
        Some(column) => Ok(HirScalarExpr::column(column)),
        _ => plan_expr(ecx, e)?.type_as_any(ecx),
    }
}

fn check_col_index(name: &str, e: &Expr<Aug>, max: usize) -> Result<Option<usize>, PlanError> {
    match e {
        Expr::Value(Value::Number(n)) => {
            let n = n.parse::<usize>().map_err(|e| {
                PlanError::Unstructured(format!(
                    "unable to parse column reference in {}: {}: {}",
                    name, n, e
                ))
            })?;
            if n < 1 || n > max {
                sql_bail!(
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
    qcx: &mut QueryContext,
    q: &Query<Aug>,
) -> Result<(HirRelationExpr, Scope, RowSetFinishing), PlanError> {
    qcx.checked_recur_mut(|qcx| plan_query_inner(qcx, q))
}

fn plan_query_inner(
    qcx: &mut QueryContext,
    q: &Query<Aug>,
) -> Result<(HirRelationExpr, Scope, RowSetFinishing), PlanError> {
    // Retain the old values of various CTE names so that we can restore them
    // after we're done planning this SELECT.
    let mut old_cte_values = Vec::new();
    // A single WITH block cannot use the same name multiple times.
    let mut used_names = HashSet::new();
    for cte in &q.ctes {
        let cte_name = normalize::ident(cte.alias.name.clone());

        if used_names.contains(&cte_name) {
            sql_bail!(
                "WITH query name {} specified more than once",
                cte_name.quoted()
            )
        }
        used_names.insert(cte_name.clone());

        // Plan CTE.
        let (val, scope) = plan_nested_query(qcx, &cte.query)?;
        let typ = qcx.relation_type(&val);
        let mut val_desc = RelationDesc::new(typ, scope.column_names());
        val_desc = plan_utils::maybe_rename_columns(
            format!("CTE {}", cte.alias.name),
            val_desc,
            &cte.alias.columns,
        )?;

        match cte.id {
            Id::Local(id) => {
                let old_val = qcx.ctes.insert(
                    id,
                    CteDesc {
                        val,
                        name: cte_name,
                        val_desc,
                    },
                );
                old_cte_values.push((id, old_val));
            }
            _ => unreachable!(),
        }
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
        }) => bail_unsupported!("FETCH ... WITH TIES"),
        Some(Limit {
            quantity: _,
            with_ties: _,
        }) => sql_bail!("LIMIT must be an integer constant"),
    };
    let offset = match &q.offset {
        None => 0,
        Some(Expr::Value(Value::Number(x))) => x.parse()?,
        _ => sql_bail!("OFFSET must be an integer constant"),
    };

    let (mut result, scope, finishing) = match &q.body {
        SetExpr::Select(s) => {
            let plan = plan_view_select(qcx, *s.clone(), q.order_by.clone())?;
            let finishing = RowSetFinishing {
                order_by: plan.order_by,
                project: plan.project,
                limit,
                offset,
            };
            Ok::<_, PlanError>((plan.expr, plan.scope, finishing))
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
                allow_windows: true,
            };
            let output_columns: Vec<_> = scope.column_names().enumerate().collect();
            let (order_by, map_exprs) = plan_order_by_exprs(ecx, &q.order_by, &output_columns)?;
            let finishing = RowSetFinishing {
                order_by,
                limit,
                project: (0..ecx.relation_type.arity()).collect(),
                offset,
            };
            Ok((expr.map(map_exprs), scope, finishing))
        }
    }?;

    for (id, old_val) in old_cte_values.into_iter().rev() {
        if let Some(cte) = qcx.ctes.remove(&id) {
            result = HirRelationExpr::Let {
                name: cte.name,
                id: id.clone(),
                value: Box::new(cte.val),
                body: Box::new(result),
            };
        }
        if let Some(old_val) = old_val {
            qcx.ctes.insert(id, old_val);
        }
    }

    Ok((result, scope, finishing))
}

pub fn plan_nested_query(
    qcx: &mut QueryContext,
    q: &Query<Aug>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    let (mut expr, scope, finishing) = plan_query(qcx, q)?;
    if finishing.limit.is_some() || finishing.offset > 0 {
        expr = HirRelationExpr::TopK {
            input: Box::new(expr),
            group_key: vec![],
            order_key: finishing.order_by,
            limit: finishing.limit,
            offset: finishing.offset,
        };
    }
    Ok((expr.project(finishing.project), scope))
}

fn plan_set_expr(
    qcx: &mut QueryContext,
    q: &SetExpr<Aug>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    match q {
        SetExpr::Select(select) => {
            let order_by_exprs = Vec::new();
            let plan = plan_view_select(qcx, *select.clone(), order_by_exprs)?;
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
            // Plan the LHS and RHS.
            let (left_expr, left_scope) = plan_set_expr(qcx, left)?;
            let (right_expr, right_scope) = plan_set_expr(qcx, right)?;

            // Validate that the LHS and RHS are the same width.
            let left_type = qcx.relation_type(&left_expr);
            let right_type = qcx.relation_type(&right_expr);
            if left_type.arity() != right_type.arity() {
                sql_bail!(
                    "each {} query must have the same number of columns: {} vs {}",
                    op,
                    left_type.arity(),
                    right_type.arity(),
                );
            }

            // Match the types of the corresponding columns on the LHS and RHS
            // using the normal type coercion rules. This is equivalent to
            // `coerce_homogeneous_exprs`, but implemented in terms of
            // `HirRelationExpr` rather than `HirScalarExpr`.
            let left_ecx = &ExprContext {
                qcx: &qcx,
                name: &op.to_string(),
                scope: &left_scope,
                relation_type: &left_type,
                allow_aggregates: false,
                allow_subqueries: false,
                allow_windows: false,
            };
            let right_ecx = &ExprContext {
                qcx: &qcx,
                name: &op.to_string(),
                scope: &right_scope,
                relation_type: &right_type,
                allow_aggregates: false,
                allow_subqueries: false,
                allow_windows: false,
            };
            let mut left_casts = vec![];
            let mut right_casts = vec![];
            for (i, (left_type, right_type)) in left_type
                .column_types
                .iter()
                .zip(right_type.column_types.iter())
                .enumerate()
            {
                let types = &[
                    Some(left_type.scalar_type.clone()),
                    Some(right_type.scalar_type.clone()),
                ];
                let target =
                    typeconv::guess_best_common_type(&left_ecx.with_name(&op.to_string()), types)?;
                match typeconv::plan_cast(
                    left_ecx,
                    CastContext::Implicit,
                    HirScalarExpr::column(i),
                    &target,
                ) {
                    Ok(expr) => left_casts.push(expr),
                    Err(_) => sql_bail!(
                        "{} types {} and {} cannot be matched",
                        op,
                        qcx.humanize_scalar_type(&left_type.scalar_type),
                        qcx.humanize_scalar_type(&target),
                    ),
                }
                match typeconv::plan_cast(
                    right_ecx,
                    CastContext::Implicit,
                    HirScalarExpr::column(i),
                    &target,
                ) {
                    Ok(expr) => right_casts.push(expr),
                    Err(_) => sql_bail!(
                        "{} types {} and {} cannot be matched",
                        op,
                        qcx.humanize_scalar_type(&target),
                        qcx.humanize_scalar_type(&right_type.scalar_type),
                    ),
                }
            }
            let project_key: Vec<_> = (left_type.arity()..left_type.arity() * 2).collect();
            let left_expr = left_expr.map(left_casts).project(project_key.clone());
            let right_expr = right_expr.map(right_casts).project(project_key);

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
            );

            Ok((relation_expr, scope))
        }
        SetExpr::Values(Values(values)) => plan_values(qcx, values),
        SetExpr::Query(query) => {
            let (expr, scope) = plan_nested_query(qcx, query)?;
            Ok((expr, scope))
        }
    }
}

/// Plans a `VALUES` clause that appears in a `SELECT` statement.
fn plan_values(
    qcx: &QueryContext,
    values: &[Vec<Expr<Aug>>],
) -> Result<(HirRelationExpr, Scope), PlanError> {
    assert!(!values.is_empty());

    let ecx = &ExprContext {
        qcx,
        name: "VALUES",
        scope: &Scope::empty(),
        relation_type: &RelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_windows: false,
    };

    let ncols = values[0].len();
    let nrows = values.len();

    // Arrange input expressions by columns, not rows, so that we can
    // call `coerce_homogeneous_exprs` on each column.
    let mut cols = vec![vec![]; ncols];
    for row in values {
        if row.len() != ncols {
            sql_bail!(
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
    for col in &cols {
        let col = coerce_homogeneous_exprs(ecx, plan_exprs(ecx, col)?, None)?;
        let mut col_type = ecx.column_type(&col[0]);
        for val in &col[1..] {
            col_type = col_type.union(&ecx.column_type(val))?;
        }
        col_types.push(col_type);
        col_iters.push(col.into_iter());
    }

    // Build constant relation.
    let mut exprs = vec![];
    for _ in 0..nrows {
        for i in 0..ncols {
            exprs.push(col_iters[i].next().unwrap());
        }
    }
    let out = HirRelationExpr::CallTable {
        func: mz_expr::TableFunc::Wrap {
            width: ncols,
            types: col_types,
        },
        exprs,
    };

    // Build column names.
    let mut scope = Scope::empty();
    for i in 0..ncols {
        let name = format!("column{}", i + 1);
        scope.items.push(ScopeItem::from_column_name(name));
    }

    Ok((out, scope))
}

/// Plans a `VALUES` clause that appears at the top level of an `INSERT`
/// statement.
///
/// This is special-cased in PostgreSQL and different enough from `plan_values`
/// that it is easier to use a separate function entirely. Unlike a normal
/// `VALUES` clause, each value is coerced to the type of the target table
/// via an assignment cast.
///
/// See: <https://github.com/postgres/postgres/blob/ad77039fa/src/backend/parser/analyze.c#L504-L518>
fn plan_values_insert(
    qcx: &QueryContext,
    target_names: &[&ColumnName],
    target_types: &[&ScalarType],
    values: &[Vec<Expr<Aug>>],
) -> Result<HirRelationExpr, PlanError> {
    assert!(!values.is_empty());

    if !values.iter().map(|row| row.len()).all_equal() {
        sql_bail!("VALUES lists must all be the same length");
    }

    let ecx = &ExprContext {
        qcx,
        name: "VALUES",
        scope: &Scope::empty(),
        relation_type: &RelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_windows: false,
    };

    let mut exprs = vec![];
    let mut types = vec![];
    for row in values {
        if row.len() > target_names.len() {
            sql_bail!("INSERT has more expressions than target columns");
        }
        for (column, val) in row.into_iter().enumerate() {
            let target_type = &target_types[column];
            let val = plan_expr(ecx, val)?;
            let val = typeconv::plan_coerce(&ecx, val, &target_type)?;
            let source_type = &ecx.scalar_type(&val);
            let val = match typeconv::plan_cast(ecx, CastContext::Assignment, val, target_type) {
                Ok(val) => val,
                Err(_) => sql_bail!(
                    "column {} is of type {} but expression is of type {}",
                    target_names[column].as_str().quoted(),
                    qcx.humanize_scalar_type(target_type),
                    qcx.humanize_scalar_type(source_type),
                ),
            };
            if column >= types.len() {
                types.push(ecx.column_type(&val));
            } else {
                types[column] = types[column].union(&ecx.column_type(&val))?;
            }
            exprs.push(val);
        }
    }

    Ok(HirRelationExpr::CallTable {
        func: mz_expr::TableFunc::Wrap {
            width: values[0].len(),
            types,
        },
        exprs,
    })
}

fn plan_join_identity() -> (HirRelationExpr, Scope) {
    let typ = RelationType::new(vec![]);
    let expr = HirRelationExpr::constant(vec![vec![]], typ);
    let scope = Scope::empty();
    (expr, scope)
}

/// Describes how to execute a SELECT query.
///
/// `order_by` describes how to order the rows in `expr` *before* applying the
/// projection. The `scope` describes the columns in `expr` *after* the
/// projection has been applied.
#[derive(Debug)]
struct SelectPlan {
    expr: HirRelationExpr,
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
    mut s: Select<Aug>,
    mut order_by_exprs: Vec<OrderByExpr<Aug>>,
) -> Result<SelectPlan, PlanError> {
    // TODO: Both `s` and `order_by_exprs` are not references because the
    // AggregateTableFuncVisitor needs to be able to rewrite the expressions for
    // table function support (the UUID mapping). Attempt to change this so callers
    // don't need to clone the Select.

    // Extract hints about group size if there are any
    let mut options = crate::normalize::options(&s.options);

    let option = options.remove("expected_group_size");

    let expected_group_size = match option {
        Some(Value::Number(n)) => Some(n.parse::<usize>()?),
        Some(_) => sql_bail!("expected_group_size must be a number"),
        None => None,
    };

    // Step 1. Handle FROM clause, including joins.
    let (mut relation_expr, mut from_scope) =
        s.from.iter().fold(Ok(plan_join_identity()), |l, twj| {
            let (left, left_scope) = l?;
            plan_join(
                qcx,
                left,
                left_scope,
                &Join {
                    relation: TableFactor::NestedJoin {
                        join: Box::new(twj.clone()),
                        alias: None,
                    },
                    join_operator: JoinOperator::CrossJoin,
                },
            )
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
            allow_windows: false,
        };
        let expr = plan_expr(ecx, &selection)
            .map_err(|e| PlanError::Unstructured(format!("WHERE clause error: {}", e)))?
            .type_as(ecx, &ScalarType::Bool)?;
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 3. Gather aggregates and table functions.
    let (aggregates, table_funcs) = {
        let mut visitor = AggregateTableFuncVisitor::new(&qcx.scx);
        visitor.visit_select_mut(&mut s);
        for o in order_by_exprs.iter_mut() {
            visitor.visit_order_by_expr_mut(o);
        }
        visitor.into_result()?
    };
    let mut table_func_names: HashMap<String, Ident> = HashMap::new();
    if !table_funcs.is_empty() {
        let (expr, scope) = plan_scalar_table_funcs(
            &qcx,
            table_funcs,
            &mut table_func_names,
            &relation_expr,
            &from_scope,
        )?;
        relation_expr = relation_expr.join(expr, HirScalarExpr::literal_true(), JoinKind::Inner);
        from_scope = from_scope.product(scope)?;
    }

    // Step 4. Expand SELECT clause.
    let projection = {
        let ecx = &ExprContext {
            qcx,
            name: "SELECT clause",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: true,
            allow_subqueries: true,
            allow_windows: true,
        };
        let mut out = vec![];
        for si in &s.projection {
            if *si == SelectItem::Wildcard && s.from.is_empty() {
                sql_bail!("SELECT * with no tables specified is not valid");
            }
            out.extend(expand_select_item(&ecx, si, &table_func_names)?);
        }
        out
    };

    // Step 5. Handle GROUP BY clause.
    let (mut group_scope, select_all_mapping) = {
        // Compute GROUP BY expressions.
        let ecx = &ExprContext {
            qcx,
            name: "GROUP BY clause",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: false,
            allow_subqueries: true,
            allow_windows: false,
        };
        let mut group_key = vec![];
        let mut group_exprs: HashMap<HirScalarExpr, ScopeItem> = HashMap::new();
        let mut group_hir_exprs = vec![];
        let mut group_scope = Scope::empty();
        let mut select_all_mapping = BTreeMap::new();

        for group_expr in &s.group_by {
            let (group_expr, expr) = plan_group_by_expr(ecx, group_expr, &projection)?;
            let new_column = group_key.len();

            if let Some(group_expr) = group_expr {
                // Multiple AST expressions can map to the same HIR expression.
                // If we already have a ScopeItem for this HIR, we can add this
                // next AST expression to its set
                if let Some(existing_scope_item) = group_exprs.get_mut(&expr) {
                    existing_scope_item.exprs.insert(group_expr.clone());
                    continue;
                }
            }

            let mut scope_item = if let HirScalarExpr::Column(ColumnRef {
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
                let scope_item = ecx.scope.items[*old_column].clone();
                scope_item
            } else {
                ScopeItem::empty()
            };

            if let Some(group_expr) = group_expr.cloned() {
                scope_item.exprs.insert(group_expr);
            }

            group_key.push(from_scope.len() + group_exprs.len());
            group_hir_exprs.push(expr.clone());
            group_exprs.insert(expr, scope_item);
        }

        assert_eq!(group_hir_exprs.len(), group_exprs.len());
        for expr in &group_hir_exprs {
            if let Some(scope_item) = group_exprs.remove(expr) {
                group_scope.items.push(scope_item);
            }
        }

        // Plan aggregates.
        let ecx = &ExprContext {
            qcx,
            name: "aggregate function",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr.clone().map(group_hir_exprs.clone())),
            allow_aggregates: false,
            allow_subqueries: true,
            allow_windows: false,
        };
        let mut agg_exprs = vec![];
        for sql_function in aggregates {
            agg_exprs.push(plan_aggregate(ecx, &sql_function)?);
            group_scope
                .items
                .push(ScopeItem::from_expr(Expr::Function(sql_function.clone())));
        }
        if !agg_exprs.is_empty() || !group_key.is_empty() || s.having.is_some() {
            // apply GROUP BY / aggregates
            relation_expr = relation_expr.map(group_hir_exprs).reduce(
                group_key,
                agg_exprs,
                expected_group_size,
            );
            (group_scope, select_all_mapping)
        } else {
            // if no GROUP BY, aggregates or having then all columns remain in scope
            (
                from_scope.clone(),
                (0..from_scope.len()).map(|i| (i, i)).collect(),
            )
        }
    };

    // Checks if an unknown column error was the result of not including that
    // column in the GROUP BY clause and produces a friendlier error instead.
    let check_ungrouped_col = |e| match e {
        PlanError::UnknownColumn { table, column } => {
            match from_scope.resolve(&qcx.outer_scopes, table.as_ref(), &column) {
                Ok(ColumnRef { level: 0, column }) => {
                    PlanError::ungrouped_column(&from_scope.items[column])
                }
                _ => PlanError::UnknownColumn { table, column },
            }
        }
        e => e,
    };

    // Step 6. Handle HAVING clause.
    if let Some(having) = s.having {
        let ecx = &ExprContext {
            qcx,
            name: "HAVING clause",
            scope: &group_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: true,
            allow_subqueries: true,
            allow_windows: false,
        };
        let expr = plan_expr(ecx, &having)
            .map_err(check_ungrouped_col)?
            .type_as(ecx, &ScalarType::Bool)?;
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 7. Handle SELECT clause.
    let output_columns = {
        let mut new_exprs = vec![];
        let mut new_type = qcx.relation_type(&relation_expr);
        let mut output_columns = vec![];
        for (select_item, column_name) in &projection {
            let ecx = &ExprContext {
                qcx,
                name: "SELECT clause",
                scope: &group_scope,
                relation_type: &new_type,
                allow_aggregates: true,
                allow_subqueries: true,
                allow_windows: true,
            };
            let expr = match select_item {
                ExpandedSelectItem::InputOrdinal(i) => {
                    if let Some(column) = select_all_mapping.get(&i).copied() {
                        HirScalarExpr::column(column)
                    } else {
                        return Err(PlanError::ungrouped_column(&from_scope.items[*i]));
                    }
                }
                ExpandedSelectItem::Expr(expr) => plan_expr(ecx, &expr)
                    .map_err(check_ungrouped_col)?
                    .type_as_any(ecx)?,
            };
            if let HirScalarExpr::Column(ColumnRef { level: 0, column }) = expr {
                // Simple column reference; no need to map on a new expression.
                output_columns.push((column, column_name));
            } else {
                // Complicated expression that requires a map expression. We
                // update `group_scope` as we go so that future expressions that
                // are textually identical to this one can reuse it. This
                // duplicate detection is required for proper determination of
                // ambiguous column references with SQL92-style `ORDER BY`
                // items. See `plan_order_by_or_distinct_expr` for more.
                let typ = ecx.column_type(&expr);
                new_type.column_types.push(typ);
                new_exprs.push(expr);
                output_columns.push((group_scope.len(), column_name));
                group_scope
                    .items
                    .push(ScopeItem::from_expr(select_item.as_expr().cloned()));
            }
        }
        relation_expr = relation_expr.map(new_exprs);
        output_columns
    };
    let mut project_key: Vec<_> = output_columns.iter().map(|(i, _name)| *i).collect();

    // Step 8. Handle intrusive ORDER BY and DISTINCT.
    let order_by = {
        let relation_type = qcx.relation_type(&relation_expr);
        let (mut order_by, mut map_exprs) = plan_order_by_exprs(
            &ExprContext {
                qcx,
                name: "ORDER BY clause",
                scope: &group_scope,
                relation_type: &relation_type,
                allow_aggregates: true,
                allow_subqueries: true,
                allow_windows: true,
            },
            &order_by_exprs,
            &output_columns,
        )
        .map_err(check_ungrouped_col)?;

        match s.distinct {
            None => relation_expr = relation_expr.map(map_exprs),
            Some(Distinct::EntireRow) => {
                if relation_type.arity() == 0 {
                    sql_bail!("SELECT DISTINCT must have at least one column");
                }
                // `SELECT DISTINCT` only distincts on the columns in the SELECT
                // list, so we can't proceed if `ORDER BY` has introduced any
                // columns for arbitrary expressions. This matches PostgreSQL.
                if !try_push_projection_order_by(
                    &mut relation_expr,
                    &mut project_key,
                    &mut order_by,
                ) {
                    sql_bail!(
                        "for SELECT DISTINCT, ORDER BY expressions must appear in select list"
                    );
                }
                assert!(map_exprs.is_empty());
                relation_expr = relation_expr.distinct();
            }
            Some(Distinct::On(exprs)) => {
                let ecx = &ExprContext {
                    qcx,
                    name: "DISTINCT ON clause",
                    scope: &group_scope,
                    relation_type: &qcx.relation_type(&relation_expr),
                    allow_aggregates: true,
                    allow_subqueries: true,
                    allow_windows: true,
                };

                let mut distinct_exprs = vec![];
                for expr in &exprs {
                    let expr = plan_order_by_or_distinct_expr(ecx, expr, &output_columns)
                        .map_err(check_ungrouped_col)?;
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
                let arity = relation_type.arity();
                for ord in order_by.iter().take(distinct_exprs.len()) {
                    // The unusual construction of `expr` here is to ensure the
                    // temporary column expression lives long enough.
                    let mut expr = &HirScalarExpr::column(ord.column);
                    if ord.column >= arity {
                        expr = &map_exprs[ord.column - arity];
                    };
                    match distinct_exprs.iter().position(move |e| e == expr) {
                        None => sql_bail!("SELECT DISTINCT ON expressions must match initial ORDER BY expressions"),
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
                        HirScalarExpr::Column(ColumnRef { level: 0, column }) => column,
                        _ => {
                            map_exprs.push(expr);
                            arity + map_exprs.len() - 1
                        }
                    };
                    distinct_key.push(column);
                }

                // `DISTINCT ON` is semantically a TopK with limit 1. The
                // columns in `ORDER BY` that are not part of the distinct key,
                // if there are any, determine the ordering within each group,
                // per PostgreSQL semantics.
                relation_expr = HirRelationExpr::TopK {
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
    let scope = Scope::from_source(None, projection.into_iter().map(|(_expr, name)| name));

    Ok(SelectPlan {
        expr: relation_expr,
        scope,
        order_by,
        project: project_key,
    })
}

fn plan_scalar_table_funcs(
    qcx: &QueryContext,
    table_funcs: HashMap<TableFunction<Aug>, String>,
    table_func_names: &mut HashMap<String, Ident>,
    relation_expr: &HirRelationExpr,
    from_scope: &Scope,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    let rows_from_qcx = qcx.derived_context(from_scope.clone(), qcx.relation_type(&relation_expr));
    let (expr, mut scope, num_cols) =
        plan_rows_from_internal(&rows_from_qcx, table_funcs.keys(), None)?;
    for (table_func, id) in table_funcs.iter() {
        table_func_names.insert(id.clone(), table_func.name.0.last().unwrap().clone());
    }
    // Munge the scope so table names match with the generated ids.
    let mut i = 0;
    for (id, num_cols) in table_funcs.values().zip(num_cols) {
        for _ in 0..num_cols {
            scope.items[i].table_name = Some(PartialName {
                database: None,
                schema: None,
                item: id.clone(),
            });
            scope.items[i].from_single_column_function = num_cols == 1;
            scope.items[i].allow_unqualified_references = false;
            i += 1;
        }
        // Ordinality column. This doubles as the
        // `is_exists_column_for_a_table_function_that_was_in_the_target_list` later on
        // because it only needs to be NULL or not.
        scope.items[i].table_name = Some(PartialName {
            database: None,
            schema: None,
            item: id.clone(),
        });
        scope.items[i].is_exists_column_for_a_table_function_that_was_in_the_target_list = true;
        scope.items[i].allow_unqualified_references = false;
        i += 1;
    }
    // Coalesced ordinality column.
    scope.items[i].allow_unqualified_references = false;
    Ok((expr, scope))
}

/// Plans an expression in a `GROUP BY` clause.
///
/// For historical reasons, PostgreSQL allows `GROUP BY` expressions to refer to
/// names/expressions defined in the `SELECT` clause. These special cases are
/// handled by this function; see comments within the implementation for
/// details.
fn plan_group_by_expr<'a>(
    ecx: &ExprContext,
    group_expr: &'a Expr<Aug>,
    projection: &'a [(ExpandedSelectItem, ColumnName)],
) -> Result<(Option<&'a Expr<Aug>>, HirScalarExpr), PlanError> {
    let plan_projection = |column: usize| match &projection[column].0 {
        ExpandedSelectItem::InputOrdinal(column) => Ok((None, HirScalarExpr::column(*column))),
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
        Expr::Identifier(names) => match plan_identifier(ecx, names) {
            Err(PlanError::UnknownColumn {
                table: None,
                column,
            }) => {
                // The expression was a simple identifier that did not match an
                // input column. See if it matches an output column.
                let mut iter = projection.iter().map(|(_expr, name)| name);
                if let Some(i) = iter.position(|n| *n == column) {
                    if iter.any(|n| *n == column) {
                        Err(PlanError::AmbiguousColumn(column))
                    } else {
                        plan_projection(i)
                    }
                } else {
                    // The name didn't match an output column either. Return the
                    // "unknown column" error.
                    Err(PlanError::UnknownColumn {
                        table: None,
                        column,
                    })
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
///
/// See `plan_order_by_or_distinct_expr` for details on the `output_columns`
/// parameter.
///
/// Returns the determined column orderings and a list of scalar expressions
/// that must be mapped onto the underlying relation expression.
fn plan_order_by_exprs(
    ecx: &ExprContext,
    order_by_exprs: &[OrderByExpr<Aug>],
    output_columns: &[(usize, &ColumnName)],
) -> Result<(Vec<ColumnOrder>, Vec<HirScalarExpr>), PlanError> {
    let mut order_by = vec![];
    let mut map_exprs = vec![];
    for obe in order_by_exprs {
        let expr = plan_order_by_or_distinct_expr(ecx, &obe.expr, output_columns)?;
        // If the expression is a reference to an existing column,
        // do not introduce a new column to support it.
        let column = match expr {
            HirScalarExpr::Column(ColumnRef { level: 0, column }) => column,
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
/// The `output_columns` parameter describes, in order, the physical index and
/// name of each expression in the `SELECT` list. For example, `[(3, "a")]`
/// corresponds to a `SELECT` list with a single entry named "a" that can be
/// found at index 3 in the underlying relation expression.
///
/// There are three cases to handle.
///
///    1. A simple numeric literal, as in `ORDER BY 1`. This is an ordinal
///       reference to the specified output column.
///    2. An unqualified identifier, as in `ORDER BY a`. This is a reference to
///       an output column, if it exists; otherwise it is a reference to an
///       input column.
///    3. An arbitrary expression, as in `ORDER BY -a`. Column references in
///       arbitrary expressions exclusively refer to input columns, never output
///       columns.
fn plan_order_by_or_distinct_expr(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    output_columns: &[(usize, &ColumnName)],
) -> Result<HirScalarExpr, PlanError> {
    if let Some(i) = check_col_index(&ecx.name, expr, output_columns.len())? {
        return Ok(HirScalarExpr::column(output_columns[i].0));
    }

    if let Expr::Identifier(names) = expr {
        if let [name] = &names[..] {
            let name = normalize::column_name(name.clone());
            let mut iter = output_columns.iter().filter(|(_, n)| **n == name);
            if let Some((i, _)) = iter.next() {
                match iter.next() {
                    // Per SQL92, names are not considered ambiguous if they
                    // refer to identical target list expressions, as in
                    // `SELECT a + 1 AS foo, a + 1 AS foo ... ORDER BY foo`.
                    Some((i2, _)) if i != i2 => return Err(PlanError::AmbiguousColumn(name)),
                    _ => return Ok(HirScalarExpr::column(*i)),
                }
            }
        }
    }

    plan_expr(ecx, expr)?.type_as_any(ecx)
}

fn plan_table_with_joins(
    qcx: &QueryContext,
    table_with_joins: &TableWithJoins<Aug>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    let (mut expr, mut scope) = plan_table_factor(qcx, &table_with_joins.relation)?;
    for join in &table_with_joins.joins {
        let (new_expr, new_scope) = plan_join(qcx, expr, scope, &join)?;
        expr = new_expr;
        scope = new_scope;
    }
    Ok((expr, scope))
}

fn plan_table_factor(
    qcx: &QueryContext,
    table_factor: &TableFactor<Aug>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    match table_factor {
        TableFactor::Table { name, alias } => {
            let (expr, scope) = qcx.resolve_table_name(name.clone())?;
            let scope = plan_table_alias(scope, alias.as_ref())?;
            Ok((expr, scope))
        }

        TableFactor::Function {
            function,
            alias,
            with_ordinality,
        } => plan_solitary_table_function(qcx, function, alias.as_ref(), *with_ordinality),

        TableFactor::RowsFrom {
            functions,
            alias,
            with_ordinality,
        } => plan_rows_from(qcx, functions, alias.as_ref(), *with_ordinality),

        TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            let mut qcx = (*qcx).clone();
            if !lateral {
                // Since this derived table was not marked as `LATERAL`,
                // make elements in outer scopes invisible until we reach the
                // next lateral barrier.
                for scope in &mut qcx.outer_scopes {
                    if scope.lateral_barrier {
                        break;
                    }
                    scope.items.clear();
                }
            }
            qcx.outer_scopes[0].lateral_barrier = true;
            let (expr, scope) = plan_nested_query(&mut qcx, &subquery)?;
            let scope = plan_table_alias(scope, alias.as_ref())?;
            Ok((expr, scope))
        }

        TableFactor::NestedJoin { join, alias } => {
            let (expr, scope) = plan_table_with_joins(&qcx, join)?;
            let scope = plan_table_alias(scope, alias.as_ref())?;
            Ok((expr, scope))
        }
    }
}

/// Plans a `ROWS FROM` expression.
///
/// `ROWS FROM` concatenates table functions into a single table, filling in
/// `NULL`s in places where one table function has fewer rows than another. We
/// can achieve this by augmenting each table function with a row number, doing
/// a `FULL JOIN` between each table function on the row number and eventually
/// projecting away the row number columns. Concretely, the following query
/// using `ROWS FROM`
///
/// ```sql
/// SELECT
///     *
/// FROM
///     ROWS FROM (
///         generate_series(1, 2),
///         information_schema._pg_expandarray(ARRAY[9]),
///         generate_series(3, 6)
///     );
/// ```
///
/// is equivalent to the following query that does not use `ROWS FROM`:
///
/// ```sql
/// SELECT
///     gs1.generate_series, expand.x, expand.n, gs2.generate_series
/// FROM
///     generate_series(1, 2) WITH ORDINALITY AS gs1
///     FULL JOIN information_schema._pg_expandarray(ARRAY[9]) WITH ORDINALITY AS expand
///         ON gs1.ordinality = expand.ordinality
///     FULL JOIN generate_series(3, 6) WITH ORDINALITY AS gs3
///         ON coalesce(gs1.ordinality, expand.ordinality) = gs3.ordinality;
/// ```
///
/// Note the call to `coalesce` in the last join condition, which ensures that
/// `gs3` will align with whichever of `gs1` or `expand` has more rows.
///
/// This function creates a HirRelationExpr that follows the structure of the
/// latter query.
///
/// `with_ordinality` can be used to have the output expression contain a
/// single coalesced ordinality column at the end of the entire expression.
fn plan_rows_from(
    qcx: &QueryContext,
    functions: &[TableFunction<Aug>],
    alias: Option<&TableAlias>,
    with_ordinality: bool,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    // If there's only a single table function, planning proceeds as if `ROWS
    // FROM` hadn't been written at all.
    if let [function] = functions {
        return plan_solitary_table_function(qcx, function, alias, with_ordinality);
    }

    // Per PostgreSQL, all scope items take the name of the first function
    // (unless aliased).
    // See: https://github.com/postgres/postgres/blob/639a86e36/src/backend/parser/parse_relation.c#L1701-L1705
    let (expr, mut scope, num_cols) =
        plan_rows_from_internal(qcx, functions, Some(&functions[0].name))?;

    // Columns tracks the set of columns we will keep in the projection.
    let mut columns = Vec::new();
    let mut offset = 0;
    // Retain table function's non-ordinality columns.
    for (idx, cols) in num_cols.into_iter().enumerate() {
        for i in 0..cols {
            columns.push(offset + i);
        }
        offset += cols + 1;

        // Remove the ordinality column from the scope, accounting for previous scope
        // changes from this loop.
        scope.items.remove(offset - idx - 1);
    }

    // If `WITH ORDINALITY` was specified, include the coalesced ordinality
    // column. Otherwise remove it from the scope.
    if with_ordinality {
        columns.push(scope.items.len());
    } else {
        scope.items.pop();
    }

    let expr = expr.project(columns);

    let scope = plan_table_alias(scope, alias)?;
    Ok((expr, scope))
}

/// Plans an expression coalescing multiple table functions. Each table
/// function is followed by its row ordinality. The entire expression is
/// followed by the coalesced row ordinality.
///
/// The returned Scope will set all item's table_name's to the `table_name`
/// parameter if it is `Some`. If `None`, they will be the name of each table
/// function.
///
/// The returned `Vec<usize>` is the number of (non-ordinality) columns from
/// each table function.
///
/// For example, with table functions tf1 returning 1 column (a) and tf2
/// returning 2 columns (b, c), this function will return an expr 6 columns:
///
/// - tf1.a
/// - tf1.ordinality
/// - tf2.b
/// - tf2.c
/// - tf2.ordinality
/// - coalesced_ordinality
///
/// And a `Vec<usize>` of `[1, 2]`.
fn plan_rows_from_internal<'a>(
    qcx: &QueryContext,
    functions: impl IntoIterator<Item = &'a TableFunction<Aug>>,
    table_name: Option<&UnresolvedObjectName>,
) -> Result<(HirRelationExpr, Scope, Vec<usize>), PlanError> {
    let mut functions = functions.into_iter();
    let mut num_cols = Vec::new();

    // Join together each of the table functions in turn. The last column is
    // always the column to join against and is maintained to be the coalesence
    // of the row number column for all prior functions.
    let (mut left_expr, mut left_scope) =
        plan_table_function_internal(&qcx, functions.next().unwrap(), true, table_name)?;
    num_cols.push(left_scope.len() - 1);
    // Create the coalesced ordinality column.
    left_expr = left_expr.map(vec![HirScalarExpr::column(left_scope.len() - 1)]);
    left_scope
        .items
        .push(ScopeItem::from_column_name("ordinality"));

    for function in functions {
        // The right hand side of a join must be planned in a new scope.
        let qcx = qcx.empty_derived_context();
        let (right_expr, mut right_scope) =
            plan_table_function_internal(&qcx, function, true, table_name)?;
        num_cols.push(right_scope.len() - 1);
        let left_col = left_scope.len() - 1;
        let right_col = left_scope.len() + right_scope.len() - 1;
        let on = HirScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1: Box::new(HirScalarExpr::column(left_col)),
            expr2: Box::new(HirScalarExpr::column(right_col)),
        };
        left_expr = left_expr
            .join(right_expr, on, JoinKind::FullOuter)
            .map(vec![HirScalarExpr::CallVariadic {
                func: VariadicFunc::Coalesce,
                exprs: vec![
                    HirScalarExpr::column(left_col),
                    HirScalarExpr::column(right_col),
                ],
            }]);

        // Project off the previous iteration's coalesced column, but keep both of this
        // iteration's ordinality columns.
        left_expr = left_expr.project(
            (0..left_col) // non-coalesced ordinality columns from left function
                .chain(left_col + 1..right_col + 2) // non-ordinality columns from right function
                .collect(),
        );
        // Move the coalesced ordinality column.
        right_scope.items.push(left_scope.items.pop().unwrap());

        left_scope.items.extend(right_scope.items);
    }

    Ok((left_expr, left_scope, num_cols))
}

/// Plans a table function that appears alone, i.e., that is not part of a `ROWS
/// FROM` clause that contains other table functions. Special aliasing rules
/// apply.
fn plan_solitary_table_function(
    qcx: &QueryContext,
    function: &TableFunction<Aug>,
    alias: Option<&TableAlias>,
    with_ordinality: bool,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    let (expr, mut scope) = plan_table_function_internal(qcx, function, with_ordinality, None)?;

    let single_column_function = scope.len() == 1 + if with_ordinality { 1 } else { 0 };
    if single_column_function {
        let item = &mut scope.items[0];

        // Mark that the function only produced a single column. This impacts
        // whole-row references.
        item.from_single_column_function = true;

        // Strange special case for solitary table functions that ouput one
        // column whose name matches the name of the table function. If a table
        // alias is provided, the column name is changed to the table alias's
        // name. Concretely, the following query returns a column named `x`
        // rather than a column named `generate_series`:
        //
        //     SELECT * FROM generate_series(1, 5) AS x
        //
        // Note that this case does not apply to e.g. `jsonb_array_elements`,
        // since its output column is explicitly named `value`, not
        // `jsonb_array_elements`.
        //
        // Note also that we may (correctly) change the column name again when
        // we plan the table alias below if the `alias.columns` is non-empty.
        if let Some(alias) = alias {
            if let ScopeItem {
                table_name: Some(table_name),
                column_name,
                ..
            } = item
            {
                if table_name.item.as_str() == column_name.as_str() {
                    *column_name = normalize::column_name(alias.name.clone());
                }
            }
        }
    }

    let scope = plan_table_alias(scope, alias)?;
    Ok((expr, scope))
}

/// Plans a table function.
///
/// You generally should call `plan_rows_from` or `plan_solitary_table_function`
/// instead to get the appropriate aliasing behavior.
fn plan_table_function_internal(
    qcx: &QueryContext,
    TableFunction { name, args }: &TableFunction<Aug>,
    with_ordinality: bool,
    table_name: Option<&UnresolvedObjectName>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    if *name == UnresolvedObjectName::unqualified("values") {
        // Produce a nice error message for the common typo
        // `SELECT * FROM VALUES (1)`.
        sql_bail!("VALUES expression in FROM clause must be surrounded by parentheses");
    }

    let ecx = &ExprContext {
        qcx: &qcx,
        name: "table function arguments",
        scope: &Scope::empty(),
        relation_type: &RelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_windows: false,
    };

    let scalar_args = match args {
        FunctionArgs::Star => sql_bail!("{} does not accept * as an argument", name),
        FunctionArgs::Args { args, order_by } => {
            if !order_by.is_empty() {
                sql_bail!(
                    "ORDER BY specified, but {} is not an aggregate function",
                    name
                );
            }
            plan_exprs(ecx, args)?
        }
    };
    let resolved_name = normalize::unresolved_object_name(name.clone())?;
    let table_name = match table_name {
        Some(table_name) => normalize::unresolved_object_name(table_name.clone())?.item,
        None => resolved_name.item.clone(),
    };
    let scope_name = Some(PartialName {
        database: None,
        schema: None,
        item: table_name,
    });

    let (mut expr, mut scope) = match resolve_func(ecx, name, args)? {
        Func::Table(impls) => {
            let tf = func::select_impl(
                ecx,
                FuncSpec::Func(&resolved_name),
                impls,
                scalar_args,
                vec![],
            )?;
            let scope = Scope::from_source(scope_name.clone(), tf.column_names);
            (tf.expr, scope)
        }
        _ => sql_bail!("{} is not a table function", name),
    };

    if with_ordinality {
        expr = expr.map(vec![HirScalarExpr::Windowing(WindowExpr {
            func: WindowExprType::Scalar(ScalarWindowExpr {
                func: ScalarWindowFunc::RowNumber,
                order_by: vec![],
            }),
            partition: vec![],
            order_by: vec![],
        })]);
        scope
            .items
            .push(ScopeItem::from_name(scope_name, "ordinality"));
    }

    Ok((expr, scope))
}

fn plan_table_alias(mut scope: Scope, alias: Option<&TableAlias>) -> Result<Scope, PlanError> {
    if let Some(TableAlias {
        name,
        columns,
        strict,
    }) = alias
    {
        if (columns.len() > scope.items.len()) || (*strict && columns.len() != scope.items.len()) {
            sql_bail!(
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
                .unwrap_or_else(|| item.column_name.clone());
            item.table_name = Some(PartialName {
                database: None,
                schema: None,
                item: table_name.clone(),
            });
            item.column_name = column_name;
        }
    }
    Ok(scope)
}

// `table_func_names` is a mapping from a UUID to the original function
// name. The UUIDs are identifiers that have been rewritten from some table
// function expression, and this mapping restores the original names.
fn invent_column_name(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    table_func_names: &HashMap<String, Ident>,
) -> Option<ColumnName> {
    // We follow PostgreSQL exactly here, which has some complicated rules
    // around "high" and "low" quality names. Low quality names override other
    // low quality names but not high quality names.
    //
    // See: https://github.com/postgres/postgres/blob/1f655fdc3/src/backend/parser/parse_target.c#L1716-L1728

    #[derive(Debug)]
    enum NameQuality {
        Low,
        High,
    }

    fn invent(
        ecx: &ExprContext,
        expr: &Expr<Aug>,
        table_func_names: &HashMap<String, Ident>,
    ) -> Option<(ColumnName, NameQuality)> {
        match expr {
            Expr::Identifier(names) => {
                if let [name] = names.as_slice() {
                    if let Some(table_func_name) = table_func_names.get(name.as_str()) {
                        return Some((
                            normalize::column_name(table_func_name.clone()),
                            NameQuality::High,
                        ));
                    }
                }
                names
                    .last()
                    .map(|n| (normalize::column_name(n.clone()), NameQuality::High))
            }
            Expr::Value(v) => match v {
                // Per PostgreSQL, `bool` and `interval` literals take on the name
                // of their type, but not other literal types.
                Value::Boolean(_) => Some(("bool".into(), NameQuality::High)),
                Value::Interval(_) => Some(("interval".into(), NameQuality::High)),
                _ => None,
            },
            Expr::Function(func) => {
                let name = normalize::unresolved_object_name(func.name.clone()).ok()?;
                if name.schema.as_deref() == Some("mz_internal") {
                    None
                } else {
                    Some((name.item.into(), NameQuality::High))
                }
            }
            Expr::HomogenizingFunction { function, .. } => Some((
                function.to_string().to_lowercase().into(),
                NameQuality::High,
            )),
            Expr::NullIf { .. } => Some(("nullif".into(), NameQuality::High)),
            Expr::Array { .. } => Some(("array".into(), NameQuality::High)),
            Expr::List { .. } => Some(("list".into(), NameQuality::High)),
            Expr::Cast { expr, data_type } => match invent(ecx, expr, table_func_names) {
                Some((name, NameQuality::High)) => Some((name, NameQuality::High)),
                _ => {
                    let ty = scalar_type_from_sql(&ecx.qcx.scx, data_type).ok()?;
                    let pgrepr_type = mz_pgrepr::Type::from(&ty);
                    let entry = ecx.catalog().get_item_by_oid(&pgrepr_type.oid());
                    Some((entry.name().item.clone().into(), NameQuality::Low))
                }
            },
            Expr::Case { else_result, .. } => {
                match else_result
                    .as_ref()
                    .and_then(|else_result| invent(ecx, else_result, table_func_names))
                {
                    Some((name, NameQuality::High)) => Some((name, NameQuality::High)),
                    _ => Some(("case".into(), NameQuality::Low)),
                }
            }
            Expr::FieldAccess { field, .. } => {
                Some((normalize::column_name(field.clone()), NameQuality::High))
            }
            Expr::Exists { .. } => Some(("exists".into(), NameQuality::High)),
            Expr::Subscript { expr, .. } => invent(ecx, expr, table_func_names),
            Expr::Subquery(query) | Expr::ListSubquery(query) | Expr::ArraySubquery(query) => {
                // A bit silly to have to plan the query here just to get its column
                // name, since we throw away the planned expression, but fixing this
                // requires a separate semantic analysis phase.
                let (_expr, scope) =
                    plan_nested_query(&mut ecx.derived_query_context(), query).ok()?;
                scope
                    .items
                    .first()
                    .map(|name| (name.column_name.clone(), NameQuality::High))
            }
            _ => None,
        }
    }

    invent(ecx, expr, table_func_names).map(|(name, _quality)| name)
}

#[derive(Debug)]
enum ExpandedSelectItem<'a> {
    InputOrdinal(usize),
    Expr(Cow<'a, Expr<Aug>>),
}

impl ExpandedSelectItem<'_> {
    fn as_expr(&self) -> Option<&Expr<Aug>> {
        match self {
            ExpandedSelectItem::InputOrdinal(_) => None,
            ExpandedSelectItem::Expr(expr) => Some(expr),
        }
    }
}

fn expand_select_item<'a>(
    ecx: &ExprContext,
    s: &'a SelectItem<Aug>,
    table_func_names: &HashMap<String, Ident>,
) -> Result<Vec<(ExpandedSelectItem<'a>, ColumnName)>, PlanError> {
    match s {
        SelectItem::Expr {
            expr: Expr::QualifiedWildcard(table_name),
            alias: _,
        } => {
            let table_name =
                normalize::unresolved_object_name(UnresolvedObjectName(table_name.clone()))?;
            let out: Vec<_> = ecx
                .scope
                .items
                .iter()
                .enumerate()
                .filter(|(_i, item)| item.is_from_table(&table_name))
                .map(|(i, item)| {
                    let name = item.column_name.clone();
                    (ExpandedSelectItem::InputOrdinal(i), name)
                })
                .collect();
            if out.is_empty() {
                sql_bail!("no table named '{}' in scope", table_name);
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
                ty => sql_bail!("type {} is not composite", ecx.humanize_scalar_type(&ty)),
            };
            let mut skip_cols: HashSet<ColumnName> = HashSet::new();
            if let Expr::Identifier(ident) = sql_expr.as_ref() {
                if let [name] = ident.as_slice() {
                    if let Ok(items) = ecx.scope.items_from_table(
                        &[],
                        &PartialName {
                            database: None,
                            schema: None,
                            item: name.as_str().to_string(),
                        },
                    ) {
                        for (_, item) in items {
                            if item
                                .is_exists_column_for_a_table_function_that_was_in_the_target_list
                            {
                                skip_cols.insert(item.column_name.clone());
                            }
                        }
                    }
                }
            }
            let items = fields
                .iter()
                .filter_map(|(name, _ty)| {
                    if skip_cols.contains(name) {
                        None
                    } else {
                        let item = ExpandedSelectItem::Expr(Cow::Owned(Expr::FieldAccess {
                            expr: sql_expr.clone(),
                            field: Ident::new(name.as_str()),
                        }));
                        Some((item, name.clone()))
                    }
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
                .filter(|(_i, item)| item.allow_unqualified_references)
                .map(|(i, item)| {
                    let name = item.column_name.clone();
                    (ExpandedSelectItem::InputOrdinal(i), name)
                })
                .collect();

            Ok(items)
        }
        SelectItem::Expr { expr, alias } => {
            let name = alias
                .clone()
                .map(normalize::column_name)
                .or_else(|| invent_column_name(ecx, &expr, &table_func_names))
                .unwrap_or_else(|| "?column?".into());
            Ok(vec![(ExpandedSelectItem::Expr(Cow::Borrowed(expr)), name)])
        }
    }
}

fn plan_join(
    left_qcx: &QueryContext,
    left: HirRelationExpr,
    left_scope: Scope,
    join: &Join<Aug>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    const ON_TRUE: JoinConstraint<Aug> = JoinConstraint::On(Expr::Value(Value::Boolean(true)));
    let (kind, constraint) = match &join.join_operator {
        JoinOperator::CrossJoin => (JoinKind::Inner, &ON_TRUE),
        JoinOperator::Inner(constraint) => (JoinKind::Inner, constraint),
        JoinOperator::LeftOuter(constraint) => (JoinKind::LeftOuter, constraint),
        JoinOperator::RightOuter(constraint) => (JoinKind::RightOuter, constraint),
        JoinOperator::FullOuter(constraint) => (JoinKind::FullOuter, constraint),
    };

    let mut right_qcx = left_qcx.derived_context(left_scope.clone(), left_qcx.relation_type(&left));
    if !kind.can_be_correlated() {
        for item in &mut right_qcx.outer_scopes[0].items {
            item.lateral_error_if_referenced = true;
        }
    }
    let (right, right_scope) = plan_table_factor(&right_qcx, &join.relation)?;

    let (expr, scope) = match constraint {
        JoinConstraint::On(expr) => {
            let product_scope = left_scope.product(right_scope)?;
            let ecx = &ExprContext {
                qcx: &left_qcx,
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
                allow_windows: false,
            };
            let on = plan_expr(ecx, expr)?.type_as(ecx, &ScalarType::Bool)?;
            let joined = left.join(right, on, kind);
            (joined, product_scope)
        }
        JoinConstraint::Using(column_names) => plan_using_constraint(
            &column_names
                .iter()
                .map(|ident| normalize::column_name(ident.clone()))
                .collect::<Vec<_>>(),
            left_qcx,
            left,
            left_scope,
            &right_qcx,
            right,
            right_scope,
            kind,
        )?,
        JoinConstraint::Natural => {
            let left_column_names: HashSet<_> = left_scope.column_names().collect();
            let right_column_names: HashSet<_> = right_scope.column_names().collect();
            let column_names: Vec<_> = left_column_names
                .intersection(&right_column_names)
                .into_iter()
                .map(|n| (*n).clone())
                .collect();
            plan_using_constraint(
                &column_names,
                left_qcx,
                left,
                left_scope,
                &right_qcx,
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
    column_names: &[ColumnName],
    left_qcx: &QueryContext,
    left: HirRelationExpr,
    left_scope: Scope,
    right_qcx: &QueryContext,
    right: HirRelationExpr,
    right_scope: Scope,
    kind: JoinKind,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    let mut both_scope = left_scope.clone().product(right_scope.clone())?;

    // Cargo culting PG here; no discernable reason this must fail, but PG does
    // so we do, as well.
    let mut unique_column_names = HashSet::new();
    for c in column_names {
        if !unique_column_names.insert(c) {
            return Err(PlanError::Unsupported {
                feature: format!(
                    "column name {} appears more than once in USING clause",
                    c.as_str().quoted()
                ),
                issue_no: None,
            });
        }
    }

    let ecx = &ExprContext {
        qcx: &right_qcx,
        name: "USING clause",
        scope: &both_scope,
        relation_type: &RelationType::new(
            left_qcx
                .relation_type(&left)
                .column_types
                .into_iter()
                .chain(right_qcx.relation_type(&right).column_types)
                .collect(),
        ),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_windows: false,
    };

    let mut join_exprs = vec![];
    let mut map_exprs = vec![];
    let mut new_items = vec![];
    let mut join_cols = vec![];
    let mut hidden_cols = vec![];

    for column_name in column_names {
        let lhs = left_scope.resolve_using_column(column_name, JoinSide::Left)?;
        let mut rhs = right_scope.resolve_using_column(column_name, JoinSide::Right)?;

        // Adjust the RHS reference to its post-join location.
        rhs.column += left_scope.len();

        // Join keys must be resolved to same type.
        let mut exprs = coerce_homogeneous_exprs(
            &ecx.with_name(&format!(
                "NATURAL/USING join column {}",
                column_name.as_str().quoted()
            )),
            vec![
                CoercibleScalarExpr::Coerced(HirScalarExpr::Column(lhs)),
                CoercibleScalarExpr::Coerced(HirScalarExpr::Column(rhs)),
            ],
            None,
        )?;
        let (expr1, expr2) = (exprs.remove(0), exprs.remove(0));

        match kind {
            JoinKind::LeftOuter { .. } | JoinKind::Inner { .. } => {
                join_cols.push(lhs.column);
                hidden_cols.push(rhs.column);
            }
            JoinKind::RightOuter => {
                join_cols.push(rhs.column);
                hidden_cols.push(lhs.column);
            }
            JoinKind::FullOuter => {
                // Create a new column that will be the coalesced value of left
                // and right.
                join_cols.push(both_scope.items.len() + map_exprs.len());
                hidden_cols.push(lhs.column);
                hidden_cols.push(rhs.column);
                map_exprs.push(HirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![expr1.clone(), expr2.clone()],
                });
                new_items.push(ScopeItem::from_column_name(column_name));
            }
        }

        join_exprs.push(HirScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1: Box::new(expr1),
            expr2: Box::new(expr2),
        });
    }
    both_scope.items.extend(new_items);

    // The columns from the secondary side of the join remain accessible by
    // their table-qualified name, but not by their column name alone. They are
    // also excluded from `SELECT *`.
    for c in hidden_cols {
        both_scope.items[c].allow_unqualified_references = false;
    }

    // Reproject all returned elements to the front of the list.
    let project_key = join_cols
        .into_iter()
        .chain(0..both_scope.items.len())
        .unique()
        .collect::<Vec<_>>();

    both_scope = both_scope.project(&project_key);

    let on = join_exprs
        .into_iter()
        .fold(HirScalarExpr::literal_true(), |expr1, expr2| {
            HirScalarExpr::CallBinary {
                func: BinaryFunc::And,
                expr1: Box::new(expr1),
                expr2: Box::new(expr2),
            }
        });
    let both = left
        .join(right, on, kind)
        .map(map_exprs)
        .project(project_key);
    Ok((both, both_scope))
}

pub fn plan_expr<'a>(
    ecx: &'a ExprContext,
    e: &Expr<Aug>,
) -> Result<CoercibleScalarExpr, PlanError> {
    ecx.checked_recur(|ecx| plan_expr_inner(ecx, e))
}

fn plan_expr_inner<'a>(
    ecx: &'a ExprContext,
    e: &Expr<Aug>,
) -> Result<CoercibleScalarExpr, PlanError> {
    if let Some(i) = ecx.scope.resolve_expr(e) {
        // We've already calculated this expression.
        return Ok(HirScalarExpr::Column(i).into());
    }

    match e {
        // Names.
        Expr::Identifier(names) | Expr::QualifiedWildcard(names) => {
            Ok(plan_identifier(ecx, names)?.into())
        }

        // Literals.
        Expr::Value(val) => plan_literal(val),
        Expr::Parameter(n) => plan_parameter(ecx, *n),
        Expr::Array(exprs) => plan_array(ecx, exprs, None),
        Expr::List(exprs) => plan_list(ecx, exprs, None),
        Expr::Row { exprs } => plan_row(ecx, exprs),

        // Generalized functions, operators, and casts.
        Expr::Op { op, expr1, expr2 } => {
            Ok(plan_op(ecx, normalize::op(op)?, expr1, expr2.as_deref())?.into())
        }
        Expr::Cast { expr, data_type } => plan_cast(ecx, expr, data_type),
        Expr::Function(func) => Ok(plan_function(ecx, func)?.into()),

        // Special functions and operators.
        Expr::Not { expr } => plan_not(ecx, expr),
        Expr::And { left, right } => plan_and(ecx, left, right),
        Expr::Or { left, right } => plan_or(ecx, left, right),
        Expr::IsExpr {
            expr,
            construct,
            negated,
        } => Ok(plan_is_expr(ecx, expr, *construct, *negated)?.into()),
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
        } => Ok(plan_case(ecx, operand, conditions, results, else_result)?.into()),
        Expr::HomogenizingFunction { function, exprs } => {
            plan_homogenizing_function(ecx, function, exprs)
        }
        Expr::NullIf { l_expr, r_expr } => Ok(plan_case(
            ecx,
            &None,
            &[l_expr.clone().equals(*r_expr.clone())],
            &[Expr::null()],
            &Some(Box::new(*l_expr.clone())),
        )?
        .into()),
        Expr::FieldAccess { expr, field } => plan_field_access(ecx, expr, field),
        Expr::WildcardAccess(expr) => plan_expr(ecx, expr),
        Expr::Subscript { expr, positions } => plan_subscript(ecx, expr, positions),
        Expr::Like {
            expr,
            pattern,
            escape,
            case_insensitive,
            negated,
        } => Ok(plan_like(
            ecx,
            expr,
            pattern,
            escape.as_deref(),
            *case_insensitive,
            *negated,
        )?
        .into()),

        // Subqueries.
        Expr::Exists(query) => plan_exists(ecx, query),
        Expr::Subquery(query) => plan_subquery(ecx, query),
        Expr::ListSubquery(query) => plan_list_subquery(ecx, query),
        Expr::ArraySubquery(query) => plan_array_subquery(ecx, query),
        Expr::Collate { expr, collation } => plan_collate(ecx, expr, collation),
        Expr::Nested(_) => unreachable!("Expr::Nested not desugared"),
        Expr::InList { .. } => unreachable!("Expr::InList not desugared"),
        Expr::InSubquery { .. } => unreachable!("Expr::InSubquery not desugared"),
        Expr::AnyExpr { .. } => unreachable!("Expr::AnyExpr not desugared"),
        Expr::AllExpr { .. } => unreachable!("Expr::AllExpr not desugared"),
        Expr::AnySubquery { .. } => unreachable!("Expr::AnySubquery not desugared"),
        Expr::AllSubquery { .. } => unreachable!("Expr::AllSubquery not desugared"),
        Expr::Between { .. } => unreachable!("Expr::Between not desugared"),
    }
}

fn plan_parameter(ecx: &ExprContext, n: usize) -> Result<CoercibleScalarExpr, PlanError> {
    if !ecx.allow_subqueries {
        return Err(PlanError::SubqueriesDisallowed {
            context: ecx.name.into(),
        });
    }
    if n == 0 || n > 65536 {
        return Err(PlanError::UnknownParameter(n));
    }
    if ecx.param_types().borrow().contains_key(&n) {
        Ok(HirScalarExpr::Parameter(n).into())
    } else {
        Ok(CoercibleScalarExpr::Parameter(n))
    }
}

fn plan_row(ecx: &ExprContext, exprs: &[Expr<Aug>]) -> Result<CoercibleScalarExpr, PlanError> {
    let mut out = vec![];
    for e in exprs {
        out.push(plan_expr(ecx, e)?);
    }
    Ok(CoercibleScalarExpr::LiteralRecord(out))
}

fn plan_cast(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    data_type: &ResolvedDataType,
) -> Result<CoercibleScalarExpr, PlanError> {
    let to_scalar_type = scalar_type_from_sql(ecx.qcx.scx, data_type)?;
    let expr = match expr {
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
    let ecx = &ecx.with_name("CAST");
    let expr = typeconv::plan_coerce(ecx, expr, &to_scalar_type)?;
    let expr = typeconv::plan_cast(ecx, CastContext::Explicit, expr, &to_scalar_type)?;
    Ok(expr.into())
}

fn plan_not(ecx: &ExprContext, expr: &Expr<Aug>) -> Result<CoercibleScalarExpr, PlanError> {
    let ecx = ecx.with_name("NOT argument");
    Ok(HirScalarExpr::CallUnary {
        func: UnaryFunc::Not(expr_func::Not),
        expr: Box::new(plan_expr(&ecx, expr)?.type_as(&ecx, &ScalarType::Bool)?),
    }
    .into())
}

fn plan_and(
    ecx: &ExprContext,
    left: &Expr<Aug>,
    right: &Expr<Aug>,
) -> Result<CoercibleScalarExpr, PlanError> {
    let ecx = ecx.with_name("AND argument");
    Ok(HirScalarExpr::CallBinary {
        func: BinaryFunc::And,
        expr1: Box::new(plan_expr(&ecx, left)?.type_as(&ecx, &ScalarType::Bool)?),
        expr2: Box::new(plan_expr(&ecx, right)?.type_as(&ecx, &ScalarType::Bool)?),
    }
    .into())
}

fn plan_or(
    ecx: &ExprContext,
    left: &Expr<Aug>,
    right: &Expr<Aug>,
) -> Result<CoercibleScalarExpr, PlanError> {
    let ecx = ecx.with_name("OR argument");
    Ok(HirScalarExpr::CallBinary {
        func: BinaryFunc::Or,
        expr1: Box::new(plan_expr(&ecx, left)?.type_as(&ecx, &ScalarType::Bool)?),
        expr2: Box::new(plan_expr(&ecx, right)?.type_as(&ecx, &ScalarType::Bool)?),
    }
    .into())
}

fn plan_homogenizing_function(
    ecx: &ExprContext,
    function: &HomogenizingFunction,
    exprs: &[Expr<Aug>],
) -> Result<CoercibleScalarExpr, PlanError> {
    assert!(!exprs.is_empty()); // `COALESCE()` is a syntax error
    let expr = HirScalarExpr::CallVariadic {
        func: match function {
            HomogenizingFunction::Coalesce => VariadicFunc::Coalesce,
            HomogenizingFunction::Greatest => VariadicFunc::Greatest,
            HomogenizingFunction::Least => VariadicFunc::Least,
        },
        exprs: coerce_homogeneous_exprs(
            &ecx.with_name(&function.to_string().to_lowercase()),
            plan_exprs(ecx, exprs)?,
            None,
        )?,
    };
    Ok(expr.into())
}

fn plan_field_access(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    field: &Ident,
) -> Result<CoercibleScalarExpr, PlanError> {
    let field = normalize::column_name(field.clone());
    let expr = plan_expr(ecx, expr)?.type_as_any(ecx)?;
    let ty = ecx.scalar_type(&expr);
    let i = match &ty {
        ScalarType::Record { fields, .. } => fields.iter().position(|(name, _ty)| *name == field),
        ty => sql_bail!(
            "column notation applied to type {}, which is not a composite type",
            ecx.humanize_scalar_type(&ty)
        ),
    };
    match i {
        None => sql_bail!(
            "field {} not found in data type {}",
            field,
            ecx.humanize_scalar_type(&ty)
        ),
        Some(i) => Ok(expr.call_unary(UnaryFunc::RecordGet(i)).into()),
    }
}

fn plan_subscript(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    positions: &[SubscriptPosition<Aug>],
) -> Result<CoercibleScalarExpr, PlanError> {
    assert!(
        !positions.is_empty(),
        "subscript expression must contain at least one position"
    );

    let ecx = &ecx.with_name("subscripting");
    let expr = plan_expr(ecx, expr)?.type_as_any(ecx)?;
    let ty = ecx.scalar_type(&expr);
    match &ty {
        ScalarType::Array(..) | ScalarType::Int2Vector => plan_subscript_array(
            ecx,
            expr,
            positions,
            if matches!(ty, ScalarType::Array(..)) {
                1
            } else {
                0
            },
        ),
        ScalarType::Jsonb => plan_subscript_jsonb(ecx, expr, positions),
        ScalarType::List { element_type, .. } => {
            let elem_type_name = ecx.humanize_scalar_type(&element_type);
            let n_layers = ty.unwrap_list_n_layers();
            plan_subscript_list(ecx, expr, positions, n_layers, &elem_type_name)
        }
        ty => sql_bail!("cannot subscript type {}", ecx.humanize_scalar_type(&ty)),
    }
}

// All subscript positions are of the form [<expr>(:<expr>?)?]; extract all
// expressions from those that look like indexes (i.e. `[<expr>]`) or error if
// any were slices (i.e. included colon).
fn extract_scalar_subscript_from_positions<'a>(
    positions: &'a [SubscriptPosition<Aug>],
    expr_type_name: &str,
) -> Result<Vec<&'a Expr<Aug>>, PlanError> {
    let mut scalar_subscripts = Vec::with_capacity(positions.len());
    for p in positions {
        if p.explicit_slice {
            sql_bail!("{} subscript does not support slices", expr_type_name);
        }
        assert!(
            p.end.is_none(),
            "index-appearing subscripts cannot have end value"
        );
        scalar_subscripts.push(p.start.as_ref().expect("has start if not slice"));
    }
    Ok(scalar_subscripts)
}

fn plan_subscript_array(
    ecx: &ExprContext,
    expr: HirScalarExpr,
    positions: &[SubscriptPosition<Aug>],
    offset: usize,
) -> Result<CoercibleScalarExpr, PlanError> {
    let mut exprs = Vec::with_capacity(positions.len() + 1);
    exprs.push(expr);

    // Subscripting arrays doesn't yet support slicing, so we always want to
    // extract scalars or error.
    let indexes = extract_scalar_subscript_from_positions(positions, "array")?;

    for i in indexes {
        exprs.push(plan_expr(ecx, i)?.cast_to(ecx, CastContext::Explicit, &ScalarType::Int64)?);
    }

    Ok(HirScalarExpr::CallVariadic {
        func: VariadicFunc::ArrayIndex { offset },
        exprs,
    }
    .into())
}

fn plan_subscript_list(
    ecx: &ExprContext,
    mut expr: HirScalarExpr,
    positions: &[SubscriptPosition<Aug>],
    mut remaining_layers: usize,
    elem_type_name: &str,
) -> Result<CoercibleScalarExpr, PlanError> {
    let mut i = 0;

    while i < positions.len() {
        // Take all contiguous index operations, i.e. find next slice operation.
        let j = positions[i..]
            .iter()
            .position(|p| p.explicit_slice)
            .unwrap_or(positions.len() - i);
        if j != 0 {
            let indexes = extract_scalar_subscript_from_positions(&positions[i..i + j], "")?;
            let (n, e) = plan_index_list(
                ecx,
                expr,
                indexes.as_slice(),
                remaining_layers,
                &elem_type_name,
            )?;
            remaining_layers = n;
            expr = e;
            i += j;
        }

        // Take all contiguous slice operations, i.e. find next index operation.
        let j = positions[i..]
            .iter()
            .position(|p| !p.explicit_slice)
            .unwrap_or(positions.len() - i);
        if j != 0 {
            expr = plan_slice_list(
                ecx,
                expr,
                &positions[i..i + j],
                remaining_layers,
                &elem_type_name,
            )?;
            i += j;
        }
    }

    Ok(expr.into())
}

fn plan_index_list(
    ecx: &ExprContext,
    expr: HirScalarExpr,
    indexes: &[&Expr<Aug>],
    n_layers: usize,
    elem_type_name: &str,
) -> Result<(usize, HirScalarExpr), PlanError> {
    let depth = indexes.len();

    if depth > n_layers {
        if n_layers == 0 {
            sql_bail!("cannot subscript type {}", elem_type_name)
        } else {
            sql_bail!(
                "cannot index into {} layers; list only has {} layer{}",
                depth,
                n_layers,
                if n_layers == 1 { "" } else { "s" }
            )
        }
    }

    let mut exprs = Vec::with_capacity(depth + 1);
    exprs.push(expr);

    for i in indexes {
        exprs.push(plan_expr(ecx, i)?.cast_to(ecx, CastContext::Explicit, &ScalarType::Int64)?);
    }

    Ok((
        n_layers - depth,
        HirScalarExpr::CallVariadic {
            func: VariadicFunc::ListIndex,
            exprs,
        },
    ))
}

fn plan_slice_list(
    ecx: &ExprContext,
    expr: HirScalarExpr,
    slices: &[SubscriptPosition<Aug>],
    n_layers: usize,
    elem_type_name: &str,
) -> Result<HirScalarExpr, PlanError> {
    if n_layers == 0 {
        sql_bail!("cannot subscript type {}", elem_type_name)
    }

    // first arg will be list
    let mut exprs = Vec::with_capacity(slices.len() + 1);
    exprs.push(expr);
    // extract (start, end) parts from collected slices
    let extract_position_or_default = |position, default| -> Result<HirScalarExpr, PlanError> {
        Ok(match position {
            Some(p) => {
                plan_expr(ecx, p)?.cast_to(ecx, CastContext::Explicit, &ScalarType::Int64)?
            }
            None => HirScalarExpr::literal(Datum::Int64(default), ScalarType::Int64),
        })
    };
    for p in slices {
        let start = extract_position_or_default(p.start.as_ref(), 1)?;
        let end = extract_position_or_default(p.end.as_ref(), i64::MAX - 1)?;
        exprs.push(start);
        exprs.push(end);
    }

    Ok(HirScalarExpr::CallVariadic {
        func: VariadicFunc::ListSliceLinear,
        exprs,
    })
}

fn plan_like(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    pattern: &Expr<Aug>,
    escape: Option<&Expr<Aug>>,
    case_insensitive: bool,
    not: bool,
) -> Result<HirScalarExpr, PlanError> {
    use CastContext::Implicit;
    let ecx = ecx.with_name("LIKE argument");
    let expr = plan_expr(&ecx, expr)?;
    let haystack = match ecx.scalar_type(&expr) {
        Some(ref ty @ ScalarType::Char { length }) => expr
            .type_as(&ecx, ty)?
            .call_unary(UnaryFunc::PadChar(expr_func::PadChar { length })),
        _ => expr.cast_to(&ecx, Implicit, &ScalarType::String)?,
    };
    let mut pattern = plan_expr(&ecx, pattern)?.cast_to(&ecx, Implicit, &ScalarType::String)?;
    if let Some(escape) = escape {
        pattern = pattern.call_binary(
            plan_expr(&ecx, escape)?.cast_to(&ecx, Implicit, &ScalarType::String)?,
            BinaryFunc::LikeEscape,
        );
    }
    let like = haystack.call_binary(pattern, BinaryFunc::IsLikeMatch { case_insensitive });
    if not {
        Ok(like.call_unary(UnaryFunc::Not(expr_func::Not)))
    } else {
        Ok(like)
    }
}

fn plan_subscript_jsonb(
    ecx: &ExprContext,
    expr: HirScalarExpr,
    positions: &[SubscriptPosition<Aug>],
) -> Result<CoercibleScalarExpr, PlanError> {
    use CastContext::Implicit;
    use ScalarType::{Int64, String};

    // JSONB doesn't support the slicing syntax, so simply error if you
    // encounter any explicit slices.
    let subscripts = extract_scalar_subscript_from_positions(positions, "jsonb")?;

    let mut exprs = Vec::with_capacity(subscripts.len());
    for s in subscripts {
        let subscript = plan_expr(ecx, s)?;
        let subscript = if let Ok(subscript) = subscript.clone().cast_to(ecx, Implicit, &String) {
            subscript
        } else if let Ok(subscript) = subscript.cast_to(ecx, Implicit, &Int64) {
            // Integers are converted to a string here and then re-parsed as an
            // integer by `JsonbGetPath`. Weird, but this is how PostgreSQL says to
            // do it.
            typeconv::to_string(ecx, subscript)
        } else {
            sql_bail!("jsonb subscript type must be coercible to integer or text");
        };
        exprs.push(subscript);
    }

    // Subscripting works like `expr #> ARRAY[subscript]` rather than
    // `expr->subscript` as you might expect.
    let expr = expr.call_binary(
        HirScalarExpr::CallVariadic {
            func: VariadicFunc::ArrayCreate {
                elem_type: ScalarType::String,
            },
            exprs,
        },
        BinaryFunc::JsonbGetPath { stringify: false },
    );
    Ok(expr.into())
}

fn plan_exists(ecx: &ExprContext, query: &Query<Aug>) -> Result<CoercibleScalarExpr, PlanError> {
    if !ecx.allow_subqueries {
        sql_bail!("{} does not allow subqueries", ecx.name)
    }
    let mut qcx = ecx.derived_query_context();
    let (expr, _scope) = plan_nested_query(&mut qcx, query)?;
    Ok(expr.exists().into())
}

fn plan_subquery(ecx: &ExprContext, query: &Query<Aug>) -> Result<CoercibleScalarExpr, PlanError> {
    if !ecx.allow_subqueries {
        sql_bail!("{} does not allow subqueries", ecx.name)
    }
    let mut qcx = ecx.derived_query_context();
    let (expr, _scope) = plan_nested_query(&mut qcx, query)?;
    let column_types = qcx.relation_type(&expr).column_types;
    if column_types.len() != 1 {
        sql_bail!(
            "Expected subselect to return 1 column, got {} columns",
            column_types.len()
        );
    }
    Ok(expr.select().into())
}

fn plan_list_subquery(
    ecx: &ExprContext,
    query: &Query<Aug>,
) -> Result<CoercibleScalarExpr, PlanError> {
    plan_vector_like_subquery(
        ecx,
        query,
        |_| false,
        |elem_type| VariadicFunc::ListCreate { elem_type },
        |order_by| AggregateFunc::ListConcat { order_by },
        BinaryFunc::ListListConcat,
        |elem_type| {
            HirScalarExpr::literal(
                Datum::empty_list(),
                ScalarType::List {
                    element_type: Box::new(elem_type),
                    custom_oid: None,
                },
            )
        },
        "list",
    )
}

fn plan_array_subquery(
    ecx: &ExprContext,
    query: &Query<Aug>,
) -> Result<CoercibleScalarExpr, PlanError> {
    ecx.require_experimental_mode("array subquery")?;
    plan_vector_like_subquery(
        ecx,
        query,
        |elem_type| {
            matches!(
                elem_type,
                ScalarType::Char { .. }
                    | ScalarType::Array { .. }
                    | ScalarType::List { .. }
                    | ScalarType::Map { .. }
            )
        },
        |elem_type| VariadicFunc::ArrayCreate { elem_type },
        |order_by| AggregateFunc::ArrayConcat { order_by },
        BinaryFunc::ArrayArrayConcat,
        |elem_type| {
            HirScalarExpr::literal(Datum::empty_array(), ScalarType::Array(Box::new(elem_type)))
        },
        "[]",
    )
}

/// Generic function used to plan both array subqueries and list subqueries
fn plan_vector_like_subquery<F1, F2, F3, F4>(
    ecx: &ExprContext,
    query: &Query<Aug>,
    is_unsupported_type: F1,
    vector_create: F2,
    aggregate_concat: F3,
    binary_concat: BinaryFunc,
    empty_literal: F4,
    vector_type_string: &str,
) -> Result<CoercibleScalarExpr, PlanError>
where
    F1: Fn(&ScalarType) -> bool,
    F2: Fn(ScalarType) -> VariadicFunc,
    F3: Fn(Vec<ColumnOrder>) -> AggregateFunc,
    F4: Fn(ScalarType) -> HirScalarExpr,
{
    if !ecx.allow_subqueries {
        sql_bail!("{} does not allow subqueries", ecx.name)
    }

    let mut qcx = ecx.derived_query_context();
    let (mut expr, _scope, finishing) = plan_query(&mut qcx, query)?;
    if finishing.limit.is_some() || finishing.offset > 0 {
        expr = HirRelationExpr::TopK {
            input: Box::new(expr),
            group_key: vec![],
            order_key: finishing.order_by.clone(),
            limit: finishing.limit,
            offset: finishing.offset,
        };
    }

    if finishing.project.len() != 1 {
        sql_bail!(
            "Expected subselect to return 1 column, got {} columns",
            finishing.project.len()
        );
    }

    let project_column = *finishing.project.get(0).unwrap();
    let elem_type = qcx
        .relation_type(&expr)
        .column_types
        .get(project_column)
        .cloned()
        .unwrap()
        .scalar_type();

    if is_unsupported_type(&elem_type) {
        bail_unsupported!(format!(
            "cannot build array from subquery because return type {}{}",
            ecx.humanize_scalar_type(&elem_type),
            vector_type_string
        ));
    }

    // `ColumnRef`s in `aggregation_exprs` refers to the columns produced by planning the
    // subquery above.
    let aggregation_exprs: Vec<_> = iter::once(HirScalarExpr::CallVariadic {
        func: vector_create(elem_type.clone()),
        exprs: vec![HirScalarExpr::column(project_column)],
    })
    .chain(
        finishing
            .order_by
            .iter()
            .map(|co| HirScalarExpr::column(co.column)),
    )
    .collect();

    // However, column references for `aggregation_projection` and `aggregation_order_by`
    // are with reference to the `exprs` of the aggregation expression.  Here that is
    // `aggregation_exprs`.
    let aggregation_projection = vec![0];
    let aggregation_order_by = finishing
        .order_by
        .into_iter()
        .enumerate()
        .map(|(i, ColumnOrder { column: _, desc })| ColumnOrder { column: i, desc })
        .collect();

    let reduced_expr = expr
        .reduce(
            vec![],
            vec![AggregateExpr {
                func: aggregate_concat(aggregation_order_by),
                expr: Box::new(HirScalarExpr::CallVariadic {
                    func: VariadicFunc::RecordCreate {
                        field_names: iter::repeat(ColumnName::from(""))
                            .take(aggregation_exprs.len())
                            .collect(),
                    },
                    exprs: aggregation_exprs,
                }),
                distinct: false,
            }],
            None,
        )
        .project(aggregation_projection);

    // If `expr` has no rows, return an empty array/list rather than NULL.
    Ok(HirScalarExpr::CallBinary {
        func: binary_concat,
        expr1: Box::new(HirScalarExpr::Select(Box::new(reduced_expr))),
        expr2: Box::new(empty_literal(elem_type)),
    }
    .into())
}

fn plan_collate(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    collation: &UnresolvedObjectName,
) -> Result<CoercibleScalarExpr, PlanError> {
    if collation.0.len() == 2
        && collation.0[0] == Ident::new("pg_catalog")
        && collation.0[1] == Ident::new("default")
    {
        plan_expr(ecx, expr)
    } else {
        bail_unsupported!("COLLATE");
    }
}

/// Plans a slice of expressions.
///
/// This function is a simple convenience function for mapping [`plan_expr`]
/// over a slice of expressions. The planned expressions are returned in the
/// same order as the input. If any of the expressions fail to plan, returns an
/// error instead.
fn plan_exprs<E>(ecx: &ExprContext, exprs: &[E]) -> Result<Vec<CoercibleScalarExpr>, PlanError>
where
    E: std::borrow::Borrow<Expr<Aug>>,
{
    let mut out = vec![];
    for expr in exprs {
        out.push(plan_expr(ecx, expr.borrow())?);
    }
    Ok(out)
}

/// Plans an `ARRAY` expression.
fn plan_array(
    ecx: &ExprContext,
    exprs: &[Expr<Aug>],
    type_hint: Option<&ScalarType>,
) -> Result<CoercibleScalarExpr, PlanError> {
    // Plan each element expression.
    let mut out = vec![];
    for expr in exprs {
        out.push(match expr {
            // Special case nested ARRAY expressions so we can plumb
            // the type hint through.
            Expr::Array(exprs) => plan_array(ecx, exprs, type_hint.clone())?,
            _ => plan_expr(ecx, expr)?,
        });
    }

    // Attempt to make use of the type hint.
    let type_hint = match type_hint {
        // The user has provided an explicit cast to an array type. We know the
        // element type to coerce to. Need to be careful, though: if there's
        // evidence that any of the array elements are themselves arrays, we
        // want to coerce to the array type, not the element type.
        Some(ScalarType::Array(elem_type)) => {
            let multidimensional = out
                .iter()
                .any(|e| matches!(ecx.scalar_type(e), Some(ScalarType::Array(_))));
            if multidimensional {
                type_hint
            } else {
                Some(&**elem_type)
            }
        }
        // The user provided an explicit cast to a non-array type. We'll have to
        // guess what the correct type for the array. Our caller will then
        // handle converting that array type to the desired non-array type.
        Some(_) => None,
        // No type hint. We'll have to guess the correct type for the array.
        None => None,
    };

    // Coerce all elements to the same type.
    let (elem_type, exprs) = if exprs.is_empty() {
        if let Some(elem_type) = type_hint {
            (elem_type.clone(), vec![])
        } else {
            sql_bail!("cannot determine type of empty array");
        }
    } else {
        let out = coerce_homogeneous_exprs(&ecx.with_name("ARRAY"), out, type_hint)?;
        (ecx.scalar_type(&out[0]), out)
    };

    // Arrays of `char` type are disallowed due to a known limitation:
    // https://github.com/MaterializeInc/materialize/issues/7613.
    //
    // Arrays of `list` and `map` types are disallowed due to mind-bending
    // semantics.
    if matches!(
        elem_type,
        ScalarType::Char { .. } | ScalarType::List { .. } | ScalarType::Map { .. }
    ) {
        bail_unsupported!(format!("{}[]", ecx.humanize_scalar_type(&elem_type)));
    }

    Ok(HirScalarExpr::CallVariadic {
        func: VariadicFunc::ArrayCreate { elem_type },
        exprs,
    }
    .into())
}

fn plan_list(
    ecx: &ExprContext,
    exprs: &[Expr<Aug>],
    type_hint: Option<&ScalarType>,
) -> Result<CoercibleScalarExpr, PlanError> {
    let (elem_type, exprs) = if exprs.is_empty() {
        if let Some(ScalarType::List { element_type, .. }) = type_hint {
            (element_type.without_modifiers(), vec![])
        } else {
            sql_bail!("cannot determine type of empty list");
        }
    } else {
        let type_hint = match type_hint {
            Some(ScalarType::List { element_type, .. }) => Some(&**element_type),
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
        let out = coerce_homogeneous_exprs(&ecx.with_name("LIST"), out, type_hint)?;
        (ecx.scalar_type(&out[0]).without_modifiers(), out)
    };

    if matches!(elem_type, ScalarType::Char { .. }) {
        bail_unsupported!("char list");
    }

    Ok(HirScalarExpr::CallVariadic {
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
/// If `force_type` is `Some`, the expressions are forced to the specified type
/// via an explicit cast. Otherwise the best common type is guessed via
/// [`typeconv::guess_best_common_type`] and conversions are attempted via
/// implicit casts
///
/// Note that this is our implementation of Postgres' type conversion for
/// ["`UNION`, `CASE`, and Related Constructs"][union-type-conv], though it
/// isn't yet used in all of those cases.
///
/// [union-type-conv]:
/// https://www.postgresql.org/docs/12/typeconv-union-case.html
pub fn coerce_homogeneous_exprs(
    ecx: &ExprContext,
    exprs: Vec<CoercibleScalarExpr>,
    force_type: Option<&ScalarType>,
) -> Result<Vec<HirScalarExpr>, PlanError> {
    assert!(!exprs.is_empty());

    let target_holder;
    let target = match force_type {
        Some(t) => t,
        None => {
            let types: Vec<_> = exprs.iter().map(|e| ecx.scalar_type(e)).collect();
            target_holder = typeconv::guess_best_common_type(ecx, &types)?;
            &target_holder
        }
    };

    // Try to cast all expressions to `target`.
    let mut out = Vec::new();
    for expr in exprs {
        let arg = typeconv::plan_coerce(&ecx, expr, &target)?;
        let ccx = match force_type {
            None => CastContext::Implicit,
            Some(_) => CastContext::Explicit,
        };
        match typeconv::plan_cast(ecx, ccx, arg.clone(), &target) {
            Ok(expr) => out.push(expr),
            Err(_) => sql_bail!(
                "{} could not convert type {} to {}",
                ecx.name,
                ecx.humanize_scalar_type(&ecx.scalar_type(&arg)),
                ecx.humanize_scalar_type(&target),
            ),
        }
    }
    Ok(out)
}

fn plan_function_order_by(
    ecx: &ExprContext,
    order_by: &[OrderByExpr<Aug>],
) -> Result<(Vec<HirScalarExpr>, Vec<ColumnOrder>), PlanError> {
    let mut order_by_exprs = vec![];
    let mut col_orders = vec![];
    {
        for (i, obe) in order_by.iter().enumerate() {
            // Unlike `SELECT ... ORDER BY` clauses, function `ORDER BY` clauses
            // do not support ordinal references in PostgreSQL. So we use
            // `plan_expr` directly rather than `plan_order_by_or_distinct_expr`.
            let expr = plan_expr(ecx, &obe.expr)?.type_as_any(ecx)?;
            order_by_exprs.push(expr);
            col_orders.push(ColumnOrder {
                column: i,
                desc: !obe.asc.unwrap_or(true),
            });
        }
    }
    Ok((order_by_exprs, col_orders))
}

fn plan_aggregate(
    ecx: &ExprContext,
    Function::<Aug> {
        name,
        args,
        filter,
        over,
        distinct,
    }: &Function<Aug>,
) -> Result<AggregateExpr, PlanError> {
    // Normal aggregate functions, like `sum`, expect as input a single expression
    // which yields the datum to aggregate. Order sensitive aggregate functions,
    // like `jsonb_agg`, are special, and instead expect a Record whose first
    // element yields the datum to aggregate and whose successive elements yield
    // keys to order by. This expectation is hard coded within the implementation
    // of each of the order-sensitive aggregates. The specification of how many
    // order by keys to consider, and in what order, is passed via the `order_by`
    // field on the `AggregateFunc` variant.

    // While all aggregate functions support the ORDER BY syntax, it's a no-op for
    // most, so explicitly drop it if the function doesn't care about order. This
    // prevents the projection into Record below from triggering on unspported
    // functions.
    let impls = match resolve_func(ecx, &name, &args)? {
        Func::Aggregate(impls) => impls,
        _ => unreachable!("plan_aggregate called on non-aggregate function,"),
    };

    if over.is_some() {
        bail_unsupported!("aggregate window functions");
    }

    let name = normalize::unresolved_object_name(name.clone())?;

    // We follow PostgreSQL's rule here for mapping `count(*)` into the
    // generalized function selection framework. The rule is simple: the user
    // must type `count(*)`, but the function selection framework sees an empty
    // parameter list, as if the user had typed `count()`. But if the user types
    // `count()` directly, that is an error. Like PostgreSQL, we apply these
    // rules to all aggregates, not just `count`, since we may one day support
    // user-defined aggregates, including user-defined aggregates that take no
    // parameters.
    let (args, order_by) = match &args {
        FunctionArgs::Star => (vec![], vec![]),
        FunctionArgs::Args { args, order_by } => {
            if args.is_empty() {
                sql_bail!(
                    "{}(*) must be used to call a parameterless aggregate function",
                    name
                );
            }
            let args = plan_exprs(ecx, args)?;
            (args, order_by.clone())
        }
    };

    let (order_by_exprs, col_orders) = plan_function_order_by(ecx, &order_by)?;

    let (mut expr, func) = func::select_impl(ecx, FuncSpec::Func(&name), impls, args, col_orders)?;
    if let Some(filter) = &filter {
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
        expr = HirScalarExpr::If {
            cond: Box::new(cond),
            then: Box::new(expr),
            els: Box::new(HirScalarExpr::literal(func.identity_datum(), expr_typ)),
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
        bail_unsupported!(
            3720,
            "aggregate functions that refer exclusively to outer columns"
        );
    }

    // If a function supports ORDER BY (even if there was no ORDER BY specified),
    // map the needed expressions into the aggregate datum.
    if func.is_order_sensitive() {
        let field_names = iter::repeat(ColumnName::from(""))
            .take(1 + order_by_exprs.len())
            .collect();
        let mut exprs = vec![expr];
        exprs.extend(order_by_exprs);
        expr = HirScalarExpr::CallVariadic {
            func: VariadicFunc::RecordCreate { field_names },
            exprs,
        };
    }

    Ok(AggregateExpr {
        func,
        expr: Box::new(expr),
        distinct: *distinct,
    })
}

fn plan_identifier(ecx: &ExprContext, names: &[Ident]) -> Result<HirScalarExpr, PlanError> {
    let mut names = names.to_vec();
    let col_name = normalize::column_name(names.pop().unwrap());

    // If the name is qualified, it must refer to a column in a table.
    if !names.is_empty() {
        let table_name = normalize::unresolved_object_name(UnresolvedObjectName(names))?;
        let i = ecx
            .scope
            .resolve_table_column(&ecx.qcx.outer_scopes, &table_name, &col_name)?;
        return Ok(HirScalarExpr::Column(i));
    }

    // If the name is unqualified, first check if it refers to a column.
    match ecx.scope.resolve_column(&ecx.qcx.outer_scopes, &col_name) {
        Ok(i) => return Ok(HirScalarExpr::Column(i)),
        Err(PlanError::UnknownColumn { .. }) => (),
        Err(e) => return Err(e),
    }

    // The name doesn't refer to a column. Check if it is a whole-row reference
    // to a table.
    let items = ecx.scope.items_from_table(
        &ecx.qcx.outer_scopes,
        &PartialName {
            database: None,
            schema: None,
            item: col_name.as_str().to_owned(),
        },
    )?;
    match items.as_slice() {
        // The name doesn't refer to a table either. Return an error.
        [] => Err(PlanError::UnknownColumn {
            table: None,
            column: col_name,
        }),
        // The name refers to a table that is the result of a function that
        // returned a single column. Per PostgreSQL, this is a special case
        // that returns the value directly.
        // See: https://github.com/postgres/postgres/blob/22592e10b/src/backend/parser/parse_expr.c#L2519-L2524
        [(column, item)] if item.from_single_column_function => Ok(HirScalarExpr::Column(*column)),
        // The name refers to a normal table. Return a record containing all the
        // columns of the table.
        _ => {
            let mut has_exists_column = None;
            let (exprs, field_names): (Vec<_>, Vec<_>) = items
                .into_iter()
                .filter_map(|(column, item)| {
                    if item.is_exists_column_for_a_table_function_that_was_in_the_target_list {
                        has_exists_column = Some(column);
                        None
                    } else {
                        let expr = HirScalarExpr::Column(column);
                        let name = item.column_name.clone();
                        Some((expr, name))
                    }
                })
                .unzip();
            // For the special case of a table function with a single column, the single column is instead not wrapped.
            let expr = if exprs.len() == 1 && has_exists_column.is_some() {
                exprs.into_element()
            } else {
                HirScalarExpr::CallVariadic {
                    func: VariadicFunc::RecordCreate { field_names },
                    exprs,
                }
            };
            if let Some(has_exists_column) = has_exists_column {
                Ok(HirScalarExpr::If {
                    cond: Box::new(HirScalarExpr::CallUnary {
                        func: UnaryFunc::IsNull(mz_expr::func::IsNull),
                        expr: Box::new(HirScalarExpr::Column(has_exists_column)),
                    }),
                    then: Box::new(HirScalarExpr::literal_null(ecx.scalar_type(&expr))),
                    els: Box::new(expr),
                })
            } else {
                Ok(expr)
            }
        }
    }
}

fn plan_op(
    ecx: &ExprContext,
    op: &str,
    expr1: &Expr<Aug>,
    expr2: Option<&Expr<Aug>>,
) -> Result<HirScalarExpr, PlanError> {
    let impls = func::resolve_op(op)?;
    let args = match expr2 {
        None => plan_exprs(ecx, &[expr1])?,
        Some(expr2) => plan_exprs(ecx, &[expr1, expr2])?,
    };
    func::select_impl(ecx, FuncSpec::Op(op), impls, args, vec![])
}

fn plan_function<'a>(
    ecx: &ExprContext,
    Function {
        name,
        args,
        filter,
        over,
        distinct,
    }: &'a Function<Aug>,
) -> Result<HirScalarExpr, PlanError> {
    let unresolved_name = normalize::unresolved_object_name(name.clone())?;

    let impls = match resolve_func(ecx, name, args)? {
        Func::Aggregate(_) if ecx.allow_aggregates => {
            // should already have been caught by `scope.resolve_expr` in `plan_expr`
            sql_bail!(
                "Internal error: encountered unplanned aggregate function: {:?}",
                name,
            )
        }
        Func::Aggregate(_) => {
            sql_bail!("aggregate functions are not allowed in {}", ecx.name);
        }
        Func::Table(_) => {
            sql_bail!("table functions are not allowed in {}", ecx.name);
        }
        Func::Scalar(impls) => impls,
        Func::ScalarWindow(impls) => {
            if !ecx.allow_windows {
                sql_bail!("window functions are not allowed in {}", ecx.name);
            }

            // Various things are duplicated here and below, but done this way to improve
            // error messages.

            if *distinct {
                sql_bail!(
                    "DISTINCT specified, but {} is not an aggregate function",
                    name
                );
            }

            if filter.is_some() {
                bail_unsupported!("FILTER in window functions");
            }

            let window_spec = match over.as_ref() {
                Some(over) => over,
                None => sql_bail!("window function {} requires an OVER clause", name),
            };
            if window_spec.window_frame.is_some() {
                bail_unsupported!("window frames");
            }
            let mut partition = Vec::new();
            for expr in &window_spec.partition_by {
                partition.push(plan_expr(ecx, expr)?.type_as_any(ecx)?);
            }

            let scalar_args = match &args {
                FunctionArgs::Star => {
                    sql_bail!("* argument is invalid with non-aggregate function {}", name)
                }
                FunctionArgs::Args { args, order_by } => {
                    if !order_by.is_empty() {
                        sql_bail!(
                            "ORDER BY specified, but {} is not an aggregate function",
                            name
                        );
                    }
                    plan_exprs(ecx, args)?
                }
            };

            let func = func::select_impl(
                ecx,
                FuncSpec::Func(&unresolved_name),
                impls,
                scalar_args,
                vec![],
            )?;

            let (order_by, col_orders) = plan_function_order_by(ecx, &window_spec.order_by)?;

            return Ok(HirScalarExpr::Windowing(WindowExpr {
                func: WindowExprType::Scalar(ScalarWindowExpr {
                    func,
                    order_by: col_orders,
                }),
                partition,
                order_by,
            }));
        }
    };

    if over.is_some() {
        bail_unsupported!(213, "window functions");
    }

    if *distinct {
        sql_bail!(
            "DISTINCT specified, but {} is not an aggregate function",
            name
        );
    }
    if filter.is_some() {
        sql_bail!(
            "FILTER specified, but {} is not an aggregate function",
            name
        );
    }

    let scalar_args = match &args {
        FunctionArgs::Star => {
            sql_bail!("* argument is invalid with non-aggregate function {}", name)
        }
        FunctionArgs::Args { args, order_by } => {
            if !order_by.is_empty() {
                sql_bail!(
                    "ORDER BY specified, but {} is not an aggregate function",
                    name
                );
            }
            plan_exprs(ecx, args)?
        }
    };

    func::select_impl(
        ecx,
        FuncSpec::Func(&unresolved_name),
        impls,
        scalar_args,
        vec![],
    )
}

/// Resolves the name to a set of function implementations.
///
/// If the name does not specify a known built-in function, returns an error.
pub fn resolve_func(
    ecx: &ExprContext,
    name: &UnresolvedObjectName,
    args: &mz_sql_parser::ast::FunctionArgs<Aug>,
) -> Result<&'static Func, PlanError> {
    if let Ok(i) = ecx.qcx.scx.resolve_function(name.clone()) {
        if let Ok(f) = i.func() {
            return Ok(f);
        }
    }

    // Couldn't resolve function with this name, so generate verbose error
    // message.
    let cexprs = match args {
        mz_sql_parser::ast::FunctionArgs::Star => vec![],
        mz_sql_parser::ast::FunctionArgs::Args { args, order_by } => {
            if !order_by.is_empty() {
                sql_bail!(
                    "ORDER BY specified, but {} is not an aggregate function",
                    name
                );
            }
            plan_exprs(ecx, &args)?
        }
    };

    let types: Vec<_> = cexprs
        .iter()
        .map(|e| match ecx.scalar_type(e) {
            Some(ty) => ecx.humanize_scalar_type(&ty),
            None => "unknown".to_string(),
        })
        .collect();

    sql_bail!("function {}({}) does not exist", name, types.join(", "))
}

fn plan_is_expr<'a>(
    ecx: &ExprContext,
    inner: &'a Expr<Aug>,
    construct: IsExprConstruct,
    not: bool,
) -> Result<HirScalarExpr, PlanError> {
    let planned_expr = plan_expr(ecx, inner)?;
    let expr = if construct.requires_boolean_expr() {
        planned_expr.type_as(ecx, &ScalarType::Bool)?
    } else {
        // PostgreSQL can plan `NULL IS NULL` but not `$1 IS NULL`. This is at odds
        // with our type coercion rules, which treat `NULL` literals and
        // unconstrained parameters identically. Providing a type hint of string
        // means we wind up supporting both.
        planned_expr.type_as_any(ecx)?
    };
    let func = match construct {
        IsExprConstruct::Null | IsExprConstruct::Unknown => UnaryFunc::IsNull(expr_func::IsNull),
        IsExprConstruct::True => UnaryFunc::IsTrue(expr_func::IsTrue),
        IsExprConstruct::False => UnaryFunc::IsFalse(expr_func::IsFalse),
    };
    let expr = HirScalarExpr::CallUnary {
        func,
        expr: Box::new(expr),
    };

    if not {
        Ok(HirScalarExpr::CallUnary {
            func: UnaryFunc::Not(expr_func::Not),
            expr: Box::new(expr),
        })
    } else {
        Ok(expr)
    }
}

fn plan_case<'a>(
    ecx: &ExprContext,
    operand: &'a Option<Box<Expr<Aug>>>,
    conditions: &'a [Expr<Aug>],
    results: &'a [Expr<Aug>],
    else_result: &'a Option<Box<Expr<Aug>>>,
) -> Result<HirScalarExpr, PlanError> {
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
    let mut result_exprs = coerce_homogeneous_exprs(
        &ecx.with_name("CASE"),
        plan_exprs(ecx, &result_exprs)?,
        None,
    )?;
    let mut expr = result_exprs.pop().unwrap();
    assert_eq!(cond_exprs.len(), result_exprs.len());
    for (cexpr, rexpr) in cond_exprs.into_iter().zip(result_exprs).rev() {
        expr = HirScalarExpr::If {
            cond: Box::new(cexpr),
            then: Box::new(rexpr),
            els: Box::new(expr),
        }
    }
    Ok(expr)
}

fn plan_literal<'a>(l: &'a Value) -> Result<CoercibleScalarExpr, PlanError> {
    let (datum, scalar_type) = match l {
        Value::Number(s) => {
            let d = strconv::parse_numeric(s.as_str())?;
            if !s.contains(&['E', '.'][..]) {
                // Maybe representable as an int?
                if let Ok(n) = d.0.try_into() {
                    (Datum::Int32(n), ScalarType::Int32)
                } else if let Ok(n) = d.0.try_into() {
                    (Datum::Int64(n), ScalarType::Int64)
                } else {
                    (Datum::Numeric(d), ScalarType::Numeric { max_scale: None })
                }
            } else {
                (Datum::Numeric(d), ScalarType::Numeric { max_scale: None })
            }
        }
        Value::HexString(_) => bail_unsupported!(3114, "hex string literals"),
        Value::Boolean(b) => match b {
            false => (Datum::False, ScalarType::Bool),
            true => (Datum::True, ScalarType::Bool),
        },
        Value::Interval(iv) => {
            let leading_precision = parser_datetimefield_to_adt(iv.precision_high);
            let mut i = strconv::parse_interval_w_disambiguator(
                &iv.value,
                match leading_precision {
                    mz_repr::adt::datetime::DateTimeField::Hour
                    | mz_repr::adt::datetime::DateTimeField::Minute => Some(leading_precision),
                    _ => None,
                },
                parser_datetimefield_to_adt(iv.precision_low),
            )?;
            i.truncate_high_fields(parser_datetimefield_to_adt(iv.precision_high));
            i.truncate_low_fields(
                parser_datetimefield_to_adt(iv.precision_low),
                iv.fsec_max_precision,
            )?;
            (Datum::Interval(i), ScalarType::Interval)
        }
        Value::String(s) => return Ok(CoercibleScalarExpr::LiteralString(s.clone())),
        Value::Null => return Ok(CoercibleScalarExpr::LiteralNull),
        Value::Array(_) => {
            sql_bail!(
                "bare [] arrays are not supported in this context; use ARRAY[] or LIST[] instead"
            )
        }
    };
    let expr = HirScalarExpr::literal(datum, scalar_type);
    Ok(expr.into())
}

// Implement these as two identical enums without From/Into impls so that they
// have no cross-package dependencies, leaving that work up to this crate.
fn parser_datetimefield_to_adt(
    dtf: mz_sql_parser::ast::DateTimeField,
) -> mz_repr::adt::datetime::DateTimeField {
    use mz_sql_parser::ast::DateTimeField::*;
    match dtf {
        Millennium => mz_repr::adt::datetime::DateTimeField::Millennium,
        Century => mz_repr::adt::datetime::DateTimeField::Century,
        Decade => mz_repr::adt::datetime::DateTimeField::Decade,
        Year => mz_repr::adt::datetime::DateTimeField::Year,
        Month => mz_repr::adt::datetime::DateTimeField::Month,
        Day => mz_repr::adt::datetime::DateTimeField::Day,
        Hour => mz_repr::adt::datetime::DateTimeField::Hour,
        Minute => mz_repr::adt::datetime::DateTimeField::Minute,
        Second => mz_repr::adt::datetime::DateTimeField::Second,
        Milliseconds => mz_repr::adt::datetime::DateTimeField::Milliseconds,
        Microseconds => mz_repr::adt::datetime::DateTimeField::Microseconds,
    }
}

pub fn scalar_type_from_sql(
    scx: &StatementContext,
    data_type: &ResolvedDataType,
) -> Result<ScalarType, PlanError> {
    match data_type {
        ResolvedDataType::AnonymousList(elem_type) => {
            let elem_type = scalar_type_from_sql(scx, &elem_type)?;
            if matches!(elem_type, ScalarType::Char { .. }) {
                bail_unsupported!("char list");
            }
            Ok(ScalarType::List {
                element_type: Box::new(elem_type),
                custom_oid: None,
            })
        }
        ResolvedDataType::AnonymousMap {
            key_type,
            value_type,
        } => {
            match scalar_type_from_sql(scx, &key_type)? {
                ScalarType::String => {}
                other => sql_bail!(
                    "map key type must be {}, got {}",
                    scx.humanize_scalar_type(&ScalarType::String),
                    scx.humanize_scalar_type(&other)
                ),
            }
            Ok(ScalarType::Map {
                value_type: Box::new(scalar_type_from_sql(scx, &value_type)?),
                custom_oid: None,
            })
        }
        ResolvedDataType::Named {
            id,
            modifiers,
            name: _,
            print_id: _,
        } => scalar_type_from_catalog(scx, *id, modifiers),
    }
}

fn scalar_type_from_catalog(
    scx: &StatementContext,
    id: GlobalId,
    modifiers: &[i64],
) -> Result<ScalarType, PlanError> {
    let entry = scx.catalog.get_item_by_id(&id);
    let type_details = match entry.type_details() {
        Some(type_details) => type_details,
        None => sql_bail!(
            "{} does not refer to a type",
            entry.name().to_string().quoted()
        ),
    };
    match &type_details.typ {
        CatalogType::Numeric => {
            let mut modifiers = modifiers.iter().fuse();
            let precision = match modifiers.next() {
                Some(p) if *p < 1 || *p > i64::from(NUMERIC_DATUM_MAX_PRECISION) => {
                    sql_bail!(
                        "precision for type numeric must be between 1 and {}",
                        NUMERIC_DATUM_MAX_PRECISION,
                    );
                }
                Some(p) => Some(*p),
                None => None,
            };
            let scale = match modifiers.next() {
                Some(scale) => {
                    if let Some(precision) = precision {
                        if *scale > precision {
                            sql_bail!(
                                "scale for type numeric must be between 0 and precision {}",
                                precision
                            );
                        }
                    }
                    Some(NumericMaxScale::try_from(*scale)?)
                }
                None => None,
            };
            if modifiers.next().is_some() {
                sql_bail!("type numeric supports at most two type modifiers");
            }
            Ok(ScalarType::Numeric { max_scale: scale })
        }
        CatalogType::Char => {
            let mut modifiers = modifiers.iter().fuse();
            let length = match modifiers.next() {
                Some(l) => Some(CharLength::try_from(*l)?),
                None => Some(CharLength::ONE),
            };
            if modifiers.next().is_some() {
                sql_bail!("type character supports at most one type modifier");
            }
            Ok(ScalarType::Char { length })
        }
        CatalogType::VarChar => {
            let mut modifiers = modifiers.iter().fuse();
            let length = match modifiers.next() {
                Some(l) => Some(VarCharMaxLength::try_from(*l)?),
                None => None,
            };
            if modifiers.next().is_some() {
                sql_bail!("type character varying supports at most one type modifier");
            }
            Ok(ScalarType::VarChar { max_length: length })
        }
        t => {
            if !modifiers.is_empty() {
                sql_bail!(
                    "{} does not support type modifiers",
                    &entry.name().to_string()
                );
            }
            match t {
                CatalogType::Array { element_id } => Ok(ScalarType::Array(Box::new(
                    scalar_type_from_catalog(scx, *element_id, modifiers)?,
                ))),
                CatalogType::List { element_id } => Ok(ScalarType::List {
                    element_type: Box::new(scalar_type_from_catalog(scx, *element_id, &[])?),
                    custom_oid: Some(scx.catalog.get_item_by_id(&id).oid()),
                }),
                CatalogType::Map {
                    key_id: _,
                    value_id,
                } => Ok(ScalarType::Map {
                    value_type: Box::new(scalar_type_from_catalog(scx, *value_id, &[])?),
                    custom_oid: Some(scx.catalog.get_item_by_id(&id).oid()),
                }),
                CatalogType::Record { fields } => {
                    let scalars: Vec<(ColumnName, ColumnType)> = fields
                        .iter()
                        .map(|(column, id)| {
                            let scalar_type = scalar_type_from_catalog(scx, *id, &[])?;
                            Ok((
                                column.clone(),
                                ColumnType {
                                    scalar_type,
                                    nullable: true,
                                },
                            ))
                        })
                        .collect::<Result<Vec<_>, PlanError>>()?;
                    let catalog_item = scx.catalog.get_item_by_id(&id);
                    Ok(ScalarType::Record {
                        fields: scalars,
                        custom_name: None,
                        custom_oid: Some(catalog_item.oid()),
                    })
                }
                CatalogType::Bool => Ok(ScalarType::Bool),
                CatalogType::Bytes => Ok(ScalarType::Bytes),
                CatalogType::Date => Ok(ScalarType::Date),
                CatalogType::Float32 => Ok(ScalarType::Float32),
                CatalogType::Float64 => Ok(ScalarType::Float64),
                CatalogType::Int16 => Ok(ScalarType::Int16),
                CatalogType::Int32 => Ok(ScalarType::Int32),
                CatalogType::Int64 => Ok(ScalarType::Int64),
                CatalogType::Interval => Ok(ScalarType::Interval),
                CatalogType::Jsonb => Ok(ScalarType::Jsonb),
                CatalogType::Oid => Ok(ScalarType::Oid),
                CatalogType::PgLegacyChar => Ok(ScalarType::PgLegacyChar),
                CatalogType::Pseudo => {
                    sql_bail!("cannot reference pseudo type {}", entry.name().to_string())
                }
                CatalogType::RegClass => Ok(ScalarType::RegClass),
                CatalogType::RegProc => Ok(ScalarType::RegProc),
                CatalogType::RegType => Ok(ScalarType::RegType),
                CatalogType::String => Ok(ScalarType::String),
                CatalogType::Time => Ok(ScalarType::Time),
                CatalogType::Timestamp => Ok(ScalarType::Timestamp),
                CatalogType::TimestampTz => Ok(ScalarType::TimestampTz),
                CatalogType::Uuid => Ok(ScalarType::Uuid),
                CatalogType::Int2Vector => Ok(ScalarType::Int2Vector),
                CatalogType::Numeric => unreachable!("handled above"),
                CatalogType::Char => unreachable!("handled above"),
                CatalogType::VarChar => unreachable!("handled above"),
            }
        }
    }
}

/// This is used to collect aggregates and table functions from within an `Expr`.
/// See the explanation of aggregate handling at the top of the file for more details.
struct AggregateTableFuncVisitor<'a> {
    scx: &'a StatementContext<'a>,
    aggs: Vec<Function<Aug>>,
    within_aggregate: bool,
    tables: HashMap<TableFunction<Aug>, String>,
    table_disallowed_context: Vec<&'static str>,
    in_select_item: bool,
    err: Option<PlanError>,
}

impl<'a> AggregateTableFuncVisitor<'a> {
    fn new(scx: &'a StatementContext<'a>) -> AggregateTableFuncVisitor<'a> {
        AggregateTableFuncVisitor {
            scx,
            aggs: Vec::new(),
            within_aggregate: false,
            tables: HashMap::new(),
            table_disallowed_context: Vec::new(),
            in_select_item: false,
            err: None,
        }
    }

    fn into_result(
        self,
    ) -> Result<(Vec<Function<Aug>>, HashMap<TableFunction<Aug>, String>), PlanError> {
        match self.err {
            Some(err) => Err(err),
            None => {
                // Dedup while preserving the order. We don't care what the order is, but it
                // has to be reproducible so that EXPLAIN PLAN tests work.
                let mut seen = HashSet::new();
                let aggs = self
                    .aggs
                    .into_iter()
                    .filter(move |agg| seen.insert(agg.clone()))
                    .collect();
                Ok((aggs, self.tables))
            }
        }
    }
}

impl<'a> VisitMut<'_, Aug> for AggregateTableFuncVisitor<'a> {
    fn visit_function_mut(&mut self, func: &mut Function<Aug>) {
        let item = match self.scx.resolve_function(func.name.clone()) {
            Ok(i) => i,
            // Catching missing functions later in planning improves error messages.
            Err(_) => return,
        };

        match item.func() {
            Ok(Func::Aggregate { .. }) => {
                if self.within_aggregate {
                    self.err = Some(PlanError::Unstructured(
                        "nested aggregate functions are not allowed".into(),
                    ));
                    return;
                }
                self.aggs.push(func.clone());
                let Function {
                    name: _,
                    args,
                    filter,
                    over: _,
                    distinct: _,
                } = func;
                if let Some(filter) = filter {
                    self.visit_expr_mut(filter);
                }
                let old_within_aggregate = self.within_aggregate;
                self.within_aggregate = true;
                self.table_disallowed_context
                    .push("aggregate function calls");

                self.visit_function_args_mut(args);

                self.within_aggregate = old_within_aggregate;
                self.table_disallowed_context.pop();
            }
            Ok(Func::Table { .. }) => {
                self.table_disallowed_context.push("other table functions");
                visit_mut::visit_function_mut(self, func);
                self.table_disallowed_context.pop();
            }
            _ => visit_mut::visit_function_mut(self, func),
        }
    }

    fn visit_query_mut(&mut self, _query: &mut Query<Aug>) {
        // Don't go into subqueries.
    }

    fn visit_expr_mut(&mut self, expr: &mut Expr<Aug>) {
        let (disallowed_context, func) = match expr {
            Expr::Case { .. } => (Some("CASE"), None),
            Expr::HomogenizingFunction {
                function: HomogenizingFunction::Coalesce,
                ..
            } => (Some("COALESCE"), None),
            Expr::Function(func) if self.in_select_item => {
                // If we're in a SELECT list, replace table functions with a uuid identifier
                // and save the table func so it can be planned elsewhere.
                let mut table_func = None;
                if let Ok(item) = self.scx.resolve_function(func.name.clone()) {
                    if let Ok(Func::Table { .. }) = item.func() {
                        if let Some(context) = self.table_disallowed_context.last() {
                            self.err = Some(PlanError::Unstructured(format!(
                                "table functions are not allowed in {}",
                                context
                            )));
                            return;
                        }
                        table_func = Some(func.clone());
                    }
                }
                // Since we will descend into the table func below, don't add its own disallow
                // context here, instead use visit_function to set that.
                (None, table_func)
            }
            _ => (None, None),
        };
        if let Some(func) = func {
            // Since we are trading out expr, we need to visit the table func here.
            visit_mut::visit_expr_mut(self, expr);
            // Don't attempt to replace table functions with unsupported syntax.
            if let Function {
                name,
                args,
                filter: None,
                over: None,
                distinct: false,
            } = func
            {
                let func = TableFunction { name, args };
                // Identical table functions can be de-duplicated.
                let id = self
                    .tables
                    .entry(func)
                    .or_insert_with(|| format!("table_func_{}", Uuid::new_v4()));
                *expr = Expr::Identifier(vec![Ident::from(id.clone())]);
            }
        }
        if let Some(context) = disallowed_context {
            self.table_disallowed_context.push(context);
        }

        visit_mut::visit_expr_mut(self, expr);

        if disallowed_context.is_some() {
            self.table_disallowed_context.pop();
        }
    }

    fn visit_select_item_mut(&mut self, si: &mut SelectItem<Aug>) {
        let old = self.in_select_item;
        self.in_select_item = true;
        visit_mut::visit_select_item_mut(self, si);
        self.in_select_item = old;
    }
}

/// Specifies how long a query will live. This impacts whether the query is
/// allowed to reason about the time at which it is running, e.g., by calling
/// the `now()` function.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum QueryLifetime<'a> {
    /// The query's result will be computed at one point in time.
    OneShot(&'a PlanContext),
    /// The query's result will be maintained indefinitely.
    Static,
}

/// Stores planned CTEs for later use.
#[derive(Debug, Clone)]
pub struct CteDesc {
    /// The CTE's expression.
    val: HirRelationExpr,
    name: String,
    val_desc: RelationDesc,
}

/// The state required when planning a `Query`.
#[derive(Debug, Clone)]
pub struct QueryContext<'a> {
    /// The context for the containing `Statement`.
    pub scx: &'a StatementContext<'a>,
    /// The lifetime that the planned query will have.
    pub lifetime: QueryLifetime<'a>,
    /// The scopes of the outer relation expression.
    pub outer_scopes: Vec<Scope>,
    /// The type of the outer relation expressions.
    pub outer_relation_types: Vec<RelationType>,
    /// CTEs for this query, mapping their assigned LocalIds to their definition.
    pub ctes: HashMap<LocalId, CteDesc>,
    pub recursion_guard: RecursionGuard,
}

impl CheckedRecursion for QueryContext<'_> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl<'a> QueryContext<'a> {
    pub fn root(scx: &'a StatementContext, lifetime: QueryLifetime<'a>) -> QueryContext<'a> {
        QueryContext {
            scx,
            lifetime,
            outer_scopes: vec![],
            outer_relation_types: vec![],
            ctes: HashMap::new(),
            recursion_guard: RecursionGuard::with_limit(1024), // chosen arbitrarily
        }
    }

    fn relation_type(&self, expr: &HirRelationExpr) -> RelationType {
        expr.typ(&self.outer_relation_types, &self.scx.param_types.borrow())
    }

    /// Generate a new `QueryContext` appropriate to be used in subqueries of
    /// `self`.
    fn derived_context(&self, scope: Scope, relation_type: RelationType) -> QueryContext<'a> {
        let ctes = self.ctes.clone();
        let outer_scopes = iter::once(scope).chain(self.outer_scopes.clone()).collect();
        let outer_relation_types = iter::once(relation_type)
            .chain(self.outer_relation_types.clone())
            .collect();

        QueryContext {
            scx: self.scx,
            lifetime: self.lifetime,
            outer_scopes,
            outer_relation_types,
            ctes,
            recursion_guard: self.recursion_guard.clone(),
        }
    }

    /// Derives a `QueryContext` for a scope that contains no columns.
    fn empty_derived_context(&self) -> QueryContext<'a> {
        let scope = Scope::empty();
        let ty = RelationType::empty();
        self.derived_context(scope, ty)
    }

    /// Resolves `object` to a table expr, i.e. creating a `Get` or inlining a
    /// CTE.
    pub fn resolve_table_name(
        &self,
        object: ResolvedObjectName,
    ) -> Result<(HirRelationExpr, Scope), PlanError> {
        match object.id {
            Id::Local(id) => {
                let name = object.raw_name;
                let cte = self.ctes.get(&id).unwrap();
                let expr = HirRelationExpr::Get {
                    id: Id::Local(id),
                    typ: cte.val_desc.typ().clone(),
                };

                let scope = Scope::from_source(Some(name), cte.val_desc.iter_names());

                Ok((expr, scope))
            }
            Id::Global(id) => {
                let item = self.scx.get_item_by_id(&id);
                let desc = item.desc()?.clone();
                let expr = HirRelationExpr::Get {
                    id: Id::Global(item.id()),
                    typ: desc.typ().clone(),
                };

                let scope = Scope::from_source(Some(object.raw_name), desc.iter_names().cloned());

                Ok((expr, scope))
            }
            Id::LocalBareSource => {
                // This is never introduced except when planning source transformations.
                unreachable!()
            }
        }
    }

    pub fn humanize_scalar_type(&self, typ: &ScalarType) -> String {
        self.scx.humanize_scalar_type(typ)
    }
}

/// A bundle of unrelated things that we need for planning `Expr`s.
#[derive(Debug, Clone)]
pub struct ExprContext<'a> {
    pub qcx: &'a QueryContext<'a>,
    /// The name of this kind of expression eg "WHERE clause". Used only for error messages.
    pub name: &'a str,
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
    /// Are window functions allowed in this context
    pub allow_windows: bool,
}

impl CheckedRecursion for ExprContext<'_> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.qcx.recursion_guard
    }
}

impl<'a> ExprContext<'a> {
    pub fn catalog(&self) -> &dyn SessionCatalog {
        self.qcx.scx.catalog
    }

    pub fn with_name(&self, name: &'a str) -> ExprContext<'a> {
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
        let mut scope = self.scope.clone();
        scope.lateral_barrier = true;
        self.qcx.derived_context(scope, self.relation_type.clone())
    }

    pub fn require_experimental_mode(&self, feature_name: &str) -> Result<(), anyhow::Error> {
        self.qcx.scx.require_experimental_mode(feature_name)
    }

    pub fn param_types(&self) -> &RefCell<BTreeMap<usize, ScalarType>> {
        &self.qcx.scx.param_types
    }

    pub fn humanize_scalar_type(&self, typ: &ScalarType) -> String {
        self.qcx.scx.humanize_scalar_type(typ)
    }
}
