// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL `Query`s are the declarative, computational part of SQL.
//! This module turns `Query`s into `HirRelationExpr`s - a more explicit, algebraic way of
//! describing computation.

//! Functions named plan_* are typically responsible for handling a single node of the SQL ast.
//! E.g. `plan_query` is responsible for handling `sqlparser::ast::Query`.
//! plan_* functions which correspond to operations on relations typically return a `HirRelationExpr`.
//! plan_* functions which correspond to operations on scalars typically return a `HirScalarExpr`
//! and a `SqlScalarType`. (The latter is because it's not always possible to infer from a
//! `HirScalarExpr` what the intended type is - notably in the case of decimals where the
//! scale/precision are encoded only in the type).

//! Aggregates are particularly twisty.
//!
//! In SQL, a GROUP BY turns any columns not in the group key into vectors of
//! values. Then anywhere later in the scope, an aggregate function can be
//! applied to that group. Inside the arguments of an aggregate function, other
//! normal functions are applied element-wise over the vectors. Thus, `SELECT
//! sum(foo.x + foo.y) FROM foo GROUP BY x` means adding the scalar `x` to the
//! vector `y` and summing the results.
//!
//! In `HirRelationExpr`, aggregates can only be applied immediately at the time
//! of grouping.
//!
//! To deal with this, whenever we see a SQL GROUP BY we look ahead for
//! aggregates and precompute them in the `HirRelationExpr::Reduce`. When we
//! reach the same aggregates during normal planning later on, we look them up
//! in an `ExprContext` to find the precomputed versions.

use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::{TryFrom, TryInto};
use std::num::NonZeroU64;
use std::rc::Rc;
use std::sync::{Arc, LazyLock};
use std::{iter, mem};

use itertools::Itertools;
use mz_expr::virtual_syntax::AlgExcept;
use mz_expr::{
    Id, LetRecLimit, LocalId, MapFilterProject, MirScalarExpr, RowSetFinishing, TableFunc,
    func as expr_func,
};
use mz_ore::assert_none;
use mz_ore::collections::CollectionExt;
use mz_ore::error::ErrorExt;
use mz_ore::id_gen::IdGen;
use mz_ore::option::FallibleMapExt;
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_ore::str::StrExt;
use mz_repr::adt::char::CharLength;
use mz_repr::adt::numeric::{NUMERIC_DATUM_MAX_PRECISION, NumericMaxScale};
use mz_repr::adt::timestamp::TimestampPrecision;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::{
    CatalogItemId, ColumnIndex, ColumnName, Datum, RelationDesc, RelationVersionSelector, Row,
    RowArena, SqlColumnType, SqlRelationType, SqlScalarType, UNKNOWN_COLUMN_NAME, strconv,
};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::visit::Visit;
use mz_sql_parser::ast::visit_mut::{self, VisitMut};
use mz_sql_parser::ast::{
    AsOf, Assignment, AstInfo, CreateWebhookSourceBody, CreateWebhookSourceCheck,
    CreateWebhookSourceHeader, CreateWebhookSourceSecret, CteBlock, DeleteStatement, Distinct,
    Expr, Function, FunctionArgs, HomogenizingFunction, Ident, InsertSource, IsExprConstruct, Join,
    JoinConstraint, JoinOperator, Limit, MapEntry, MutRecBlock, MutRecBlockOption,
    MutRecBlockOptionName, OrderByExpr, Query, Select, SelectItem, SelectOption, SelectOptionName,
    SetExpr, SetOperator, ShowStatement, SubscriptPosition, TableAlias, TableFactor,
    TableWithJoins, UnresolvedItemName, UpdateStatement, Value, Values, WindowFrame,
    WindowFrameBound, WindowFrameUnits, WindowSpec, visit,
};
use mz_sql_parser::ident;

use crate::catalog::{CatalogItemType, CatalogType, SessionCatalog};
use crate::func::{self, Func, FuncSpec, TableFuncImpl};
use crate::names::{
    Aug, FullItemName, PartialItemName, ResolvedDataType, ResolvedItemName, SchemaSpecifier,
};
use crate::plan::PlanError::InvalidWmrRecursionLimit;
use crate::plan::error::PlanError;
use crate::plan::hir::{
    AbstractColumnType, AbstractExpr, AggregateExpr, AggregateFunc, AggregateWindowExpr,
    BinaryFunc, CoercibleScalarExpr, CoercibleScalarType, ColumnOrder, ColumnRef, Hir,
    HirRelationExpr, HirScalarExpr, JoinKind, ScalarWindowExpr, ScalarWindowFunc, UnaryFunc,
    ValueWindowExpr, ValueWindowFunc, VariadicFunc, WindowExpr, WindowExprType,
};
use crate::plan::plan_utils::{self, GroupSizeHints, JoinSide};
use crate::plan::scope::{Scope, ScopeItem, ScopeUngroupedColumn};
use crate::plan::statement::{StatementContext, StatementDesc, show};
use crate::plan::typeconv::{self, CastContext, plan_hypothetical_cast};
use crate::plan::{
    Params, PlanContext, QueryWhen, ShowCreatePlan, WebhookValidation, WebhookValidationSecret,
    literal, transform_ast,
};
use crate::session::vars::ENABLE_WITH_ORDINALITY_LEGACY_FALLBACK;
use crate::session::vars::{self, FeatureFlag};
use crate::{ORDINALITY_COL_NAME, normalize};

#[derive(Debug)]
pub struct PlannedRootQuery<E> {
    pub expr: E,
    pub desc: RelationDesc,
    pub finishing: RowSetFinishing<HirScalarExpr, HirScalarExpr>,
    pub scope: Scope,
}

/// Plans a top-level query, returning the `HirRelationExpr` describing the query
/// plan, the `RelationDesc` describing the shape of the result set, a
/// `RowSetFinishing` describing post-processing that must occur before results
/// are sent to the client, and the types of the parameters in the query, if any
/// were present.
///
/// Note that the returned `RelationDesc` describes the expression after
/// applying the returned `RowSetFinishing`.
#[mz_ore::instrument(target = "compiler", level = "trace", name = "ast_to_hir")]
pub fn plan_root_query(
    scx: &StatementContext,
    mut query: Query<Aug>,
    lifetime: QueryLifetime,
) -> Result<PlannedRootQuery<HirRelationExpr>, PlanError> {
    transform_ast::transform(scx, &mut query)?;
    let mut qcx = QueryContext::root(scx, lifetime);
    let PlannedQuery {
        mut expr,
        scope,
        order_by,
        limit,
        offset,
        project,
        group_size_hints,
    } = plan_query(&mut qcx, &query)?;

    let mut finishing = RowSetFinishing {
        limit,
        offset,
        project,
        order_by,
    };

    // Attempt to push the finishing's ordering past its projection. This allows
    // data to be projected down on the workers rather than the coordinator. It
    // also improves the optimizer's demand analysis, as the optimizer can only
    // reason about demand information in `expr` (i.e., it can't see
    // `finishing.project`).
    try_push_projection_order_by(&mut expr, &mut finishing.project, &mut finishing.order_by);

    if lifetime.is_maintained() {
        expr.finish_maintained(&mut finishing, group_size_hints);
    }

    let typ = qcx.relation_type(&expr);
    let typ = SqlRelationType::new(
        finishing
            .project
            .iter()
            .map(|i| typ.column_types[*i].clone())
            .collect(),
    );
    let desc = RelationDesc::new(typ, scope.column_names());

    Ok(PlannedRootQuery {
        expr,
        desc,
        finishing,
        scope,
    })
}

/// TODO(ct2): Dedup this with [plan_root_query].
#[mz_ore::instrument(target = "compiler", level = "trace", name = "ast_to_hir")]
pub fn plan_ct_query(
    qcx: &mut QueryContext,
    mut query: Query<Aug>,
) -> Result<PlannedRootQuery<HirRelationExpr>, PlanError> {
    transform_ast::transform(qcx.scx, &mut query)?;
    let PlannedQuery {
        mut expr,
        scope,
        order_by,
        limit,
        offset,
        project,
        group_size_hints,
    } = plan_query(qcx, &query)?;

    let mut finishing = RowSetFinishing {
        limit,
        offset,
        project,
        order_by,
    };

    // Attempt to push the finishing's ordering past its projection. This allows
    // data to be projected down on the workers rather than the coordinator. It
    // also improves the optimizer's demand analysis, as the optimizer can only
    // reason about demand information in `expr` (i.e., it can't see
    // `finishing.project`).
    try_push_projection_order_by(&mut expr, &mut finishing.project, &mut finishing.order_by);

    expr.finish_maintained(&mut finishing, group_size_hints);

    let typ = qcx.relation_type(&expr);
    let typ = SqlRelationType::new(
        finishing
            .project
            .iter()
            .map(|i| typ.column_types[*i].clone())
            .collect(),
    );
    let desc = RelationDesc::new(typ, scope.column_names());

    Ok(PlannedRootQuery {
        expr,
        desc,
        finishing,
        scope,
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
    table_name: ResolvedItemName,
    columns: Vec<Ident>,
    source: InsertSource<Aug>,
    returning: Vec<SelectItem<Aug>>,
) -> Result<
    (
        CatalogItemId,
        HirRelationExpr,
        PlannedRootQuery<Vec<HirScalarExpr>>,
    ),
    PlanError,
> {
    let mut qcx = QueryContext::root(scx, QueryLifetime::OneShot);
    let table = scx.get_item_by_resolved_name(&table_name)?;

    // Validate the target of the insert.
    if table.item_type() != CatalogItemType::Table {
        sql_bail!(
            "cannot insert into {} '{}'",
            table.item_type(),
            table_name.full_name_str()
        );
    }
    let desc = table.desc(&scx.catalog.resolve_full_name(table.name()))?;
    let mut defaults = table
        .writable_table_details()
        .ok_or_else(|| {
            sql_err!(
                "cannot insert into non-writeable table '{}'",
                table_name.full_name_str()
            )
        })?
        .to_vec();

    for default in &mut defaults {
        transform_ast::transform(scx, default)?;
    }

    if table.id().is_system() {
        sql_bail!(
            "cannot insert into system table '{}'",
            table_name.full_name_str()
        );
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
        let column_by_name: BTreeMap<&ColumnName, (usize, &SqlColumnType)> = desc
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
                    c.quoted(),
                    table_name.full_name_str().quoted()
                );
            }
        }
        if let Some(dup) = columns.iter().duplicates().next() {
            sql_bail!("column {} specified more than once", dup.quoted());
        }
    };

    // Plan the source.
    let expr = match source {
        InsertSource::Query(mut query) => {
            transform_ast::transform(scx, &mut query)?;

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
            HirRelationExpr::constant(vec![vec![]], SqlRelationType::empty())
        }
    };

    let expr_arity = expr.arity();

    // Validate that the arity of the source query is at most the size of declared columns or the
    // size of the table if none are declared
    let max_columns = if columns.is_empty() {
        desc.arity()
    } else {
        columns.len()
    };
    if expr_arity > max_columns {
        sql_bail!("INSERT has more expressions than target columns");
    }
    // But it should never have less than the declared columns (or zero)
    if expr_arity < columns.len() {
        sql_bail!("INSERT has more target columns than expressions");
    }

    // Trim now that we know for sure the correct arity of the source query
    source_types.truncate(expr_arity);
    ordering.truncate(expr_arity);

    // Ensure the types of the source query match the types of the target table,
    // installing assignment casts where necessary and possible.
    let expr = cast_relation(&qcx, CastContext::Assignment, expr, source_types).map_err(|e| {
        sql_err!(
            "column {} is of type {} but expression is of type {}",
            desc.get_name(ordering[e.column]).quoted(),
            qcx.humanize_scalar_type(&e.target_type, false),
            qcx.humanize_scalar_type(&e.source_type, false),
        )
    })?;

    // Fill in any omitted columns and rearrange into correct order
    let mut map_exprs = vec![];
    let mut project_key = Vec::with_capacity(desc.arity());

    // Maps from table column index to position in the source query
    let col_to_source: BTreeMap<_, _> = ordering.iter().enumerate().map(|(a, b)| (b, a)).collect();

    let column_details = desc.iter_types().zip_eq(defaults).enumerate();
    for (col_idx, (col_typ, default)) in column_details {
        if let Some(src_idx) = col_to_source.get(&col_idx) {
            project_key.push(*src_idx);
        } else {
            let hir = plan_default_expr(scx, &default, &col_typ.scalar_type)?;
            project_key.push(expr_arity + map_exprs.len());
            map_exprs.push(hir);
        }
    }

    let returning = {
        let (scope, typ) = if let ResolvedItemName::Item {
            full_name,
            version: _,
            ..
        } = table_name
        {
            let scope = Scope::from_source(Some(full_name.clone().into()), desc.iter_names());
            let typ = desc.typ().clone();
            (scope, typ)
        } else {
            (Scope::empty(), SqlRelationType::empty())
        };
        let ecx = &ExprContext {
            qcx: &qcx,
            name: "RETURNING clause",
            scope: &scope,
            relation_type: &typ,
            allow_aggregates: false,
            allow_subqueries: false,
            allow_parameters: true,
            allow_windows: false,
        };
        let table_func_names = BTreeMap::new();
        let mut output_columns = vec![];
        let mut new_exprs = vec![];
        let mut new_type = SqlRelationType::empty();
        for mut si in returning {
            transform_ast::transform(scx, &mut si)?;
            for (select_item, column_name) in expand_select_item(ecx, &si, &table_func_names)? {
                let expr = match &select_item {
                    ExpandedSelectItem::InputOrdinal(i) => HirScalarExpr::column(*i),
                    ExpandedSelectItem::Expr(expr) => plan_expr(ecx, expr)?.type_as_any(ecx)?,
                };
                output_columns.push(column_name);
                let typ = ecx.column_type(&expr);
                new_type.column_types.push(typ);
                new_exprs.push(expr);
            }
        }
        let desc = RelationDesc::new(new_type, output_columns);
        let desc_arity = desc.arity();
        PlannedRootQuery {
            expr: new_exprs,
            desc,
            finishing: HirRelationExpr::trivial_row_set_finishing_hir(desc_arity),
            scope,
        }
    };

    Ok((
        table.id(),
        expr.map(map_exprs).project(project_key),
        returning,
    ))
}

/// Determines the mapping between some external data and a Materialize relation.
///
/// Returns the following:
/// * [`CatalogItemId`] for the destination table.
/// * [`RelationDesc`] representing the shape of the __input__ data we are copying from.
/// * The [`ColumnIndex`]es that the source data maps to. TODO(cf2): We don't need this mapping
///   since we now return a [`MapFilterProject`].
/// * [`MapFilterProject`] which will map and project the input data to match the shape of the
///   destination table.
///
pub fn plan_copy_item(
    scx: &StatementContext,
    item_name: ResolvedItemName,
    columns: Vec<Ident>,
) -> Result<
    (
        CatalogItemId,
        RelationDesc,
        Vec<ColumnIndex>,
        Option<MapFilterProject>,
    ),
    PlanError,
> {
    let item = scx.get_item_by_resolved_name(&item_name)?;
    let fullname = scx.catalog.resolve_full_name(item.name());
    let table_desc = item.desc(&fullname)?.into_owned();
    let mut ordering = Vec::with_capacity(columns.len());

    // TODO(cf2): The logic here to create the `source_desc` and the MFP are a bit duplicated and
    // should be simplified. The reason they are currently separate code paths is so we can roll
    // out `COPY ... FROM <url>` without touching the current `COPY ... FROM ... STDIN` behavior.

    // If we're copying data into a table that users can write into (e.g. not a `CREATE TABLE ...
    // FROM SOURCE ...`), then we generate an MFP.
    //
    // Note: This method is called for both `COPY INTO <table> FROM` and `COPY <expr> TO <external>`
    // so it's not always guaranteed that our `item` is a table.
    let mfp = if let Some(table_defaults) = item.writable_table_details() {
        let mut table_defaults = table_defaults.to_vec();

        for default in &mut table_defaults {
            transform_ast::transform(scx, default)?;
        }

        // Fill in any omitted columns and rearrange into correct order
        let source_column_names: Vec<_> = columns
            .iter()
            .cloned()
            .map(normalize::column_name)
            .collect();

        let mut default_exprs = Vec::new();
        let mut project_keys = Vec::with_capacity(table_desc.arity());

        // For each column in the destination table, either project it from the source data, or provide
        // an expression to fill in a default value.
        let column_details = table_desc.iter().zip_eq(table_defaults);
        for ((col_name, col_type), col_default) in column_details {
            let maybe_src_idx = source_column_names.iter().position(|name| name == col_name);
            if let Some(src_idx) = maybe_src_idx {
                project_keys.push(src_idx);
            } else {
                // If one a column from the table does not exist in the source data, then a default
                // value will get appended to the end of the input Row from the source data.
                let hir = plan_default_expr(scx, &col_default, &col_type.scalar_type)?;
                let mir = hir.lower_uncorrelated()?;
                project_keys.push(source_column_names.len() + default_exprs.len());
                default_exprs.push(mir);
            }
        }

        let mfp = MapFilterProject::new(source_column_names.len())
            .map(default_exprs)
            .project(project_keys);
        Some(mfp)
    } else {
        None
    };

    // Create a mapping from input data to the table we're copying into.
    let source_desc = if columns.is_empty() {
        let indexes = (0..table_desc.arity()).map(ColumnIndex::from_raw);
        ordering.extend(indexes);

        // The source data should be in the same order as the table.
        table_desc
    } else {
        let columns: Vec<_> = columns.into_iter().map(normalize::column_name).collect();
        let column_by_name: BTreeMap<&ColumnName, (ColumnIndex, &SqlColumnType)> = table_desc
            .iter_all()
            .map(|(idx, name, typ)| (name, (*idx, typ)))
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
                    c.quoted(),
                    item_name.full_name_str().quoted()
                );
            }
        }
        if let Some(dup) = columns.iter().duplicates().next() {
            sql_bail!("column {} specified more than once", dup.quoted());
        }

        // The source data is a different shape than the destination table.
        RelationDesc::new(SqlRelationType::new(source_types), names)
    };

    Ok((item.id(), source_desc, ordering, mfp))
}

/// See the doc comment on [`plan_copy_item`] for the details of what this function returns.
///
/// TODO(cf3): Merge this method with [`plan_copy_item`].
pub fn plan_copy_from(
    scx: &StatementContext,
    table_name: ResolvedItemName,
    columns: Vec<Ident>,
) -> Result<
    (
        CatalogItemId,
        RelationDesc,
        Vec<ColumnIndex>,
        Option<MapFilterProject>,
    ),
    PlanError,
> {
    let table = scx.get_item_by_resolved_name(&table_name)?;

    // Validate the target of the insert.
    if table.item_type() != CatalogItemType::Table {
        sql_bail!(
            "cannot insert into {} '{}'",
            table.item_type(),
            table_name.full_name_str()
        );
    }

    let _ = table.writable_table_details().ok_or_else(|| {
        sql_err!(
            "cannot insert into non-writeable table '{}'",
            table_name.full_name_str()
        )
    })?;

    if table.id().is_system() {
        sql_bail!(
            "cannot insert into system table '{}'",
            table_name.full_name_str()
        );
    }
    let (id, desc, ordering, mfp) = plan_copy_item(scx, table_name, columns)?;

    Ok((id, desc, ordering, mfp))
}

/// Builds a plan that adds the default values for the missing columns and re-orders
/// the datums in the given rows to match the order in the target table.
pub fn plan_copy_from_rows(
    pcx: &PlanContext,
    catalog: &dyn SessionCatalog,
    target_id: CatalogItemId,
    target_name: String,
    columns: Vec<ColumnIndex>,
    rows: Vec<mz_repr::Row>,
) -> Result<HirRelationExpr, PlanError> {
    let scx = StatementContext::new(Some(pcx), catalog);

    // Always copy at the latest version of the table.
    let table = catalog
        .try_get_item(&target_id)
        .ok_or_else(|| PlanError::CopyFromTargetTableDropped { target_name })?
        .at_version(RelationVersionSelector::Latest);
    let desc = table.desc(&catalog.resolve_full_name(table.name()))?;

    let mut defaults = table
        .writable_table_details()
        .ok_or_else(|| sql_err!("cannot copy into non-writeable table"))?
        .to_vec();

    for default in &mut defaults {
        transform_ast::transform(&scx, default)?;
    }

    let column_types = columns
        .iter()
        .map(|x| desc.get_type(x).clone())
        .map(|mut x| {
            // Null constraint is enforced later, when inserting the row in the table.
            // Without this, an assert is hit during lowering.
            x.nullable = true;
            x
        })
        .collect();
    let typ = SqlRelationType::new(column_types);
    let expr = HirRelationExpr::Constant {
        rows,
        typ: typ.clone(),
    };

    // Exit early with just the raw constant if we know that all columns are present
    // and in the correct order. This lets us bypass expensive downstream optimizations
    // more easily, as at every stage we know this expression is nothing more than
    // a constant (as opposed to e.g. a constant with with an identity map and identity
    // projection).
    let default: Vec<_> = (0..desc.arity()).map(ColumnIndex::from_raw).collect();
    if columns == default {
        return Ok(expr);
    }

    // Fill in any omitted columns and rearrange into correct order
    let mut map_exprs = vec![];
    let mut project_key = Vec::with_capacity(desc.arity());

    // Maps from table column index to position in the source query
    let col_to_source: BTreeMap<_, _> = columns.iter().enumerate().map(|(a, b)| (b, a)).collect();

    let column_details = desc.iter_all().zip_eq(defaults);
    for ((col_idx, _col_name, col_typ), default) in column_details {
        if let Some(src_idx) = col_to_source.get(&col_idx) {
            project_key.push(*src_idx);
        } else {
            let hir = plan_default_expr(&scx, &default, &col_typ.scalar_type)?;
            project_key.push(typ.arity() + map_exprs.len());
            map_exprs.push(hir);
        }
    }

    Ok(expr.map(map_exprs).project(project_key))
}

/// Common information used for DELETE, UPDATE, and INSERT INTO ... SELECT plans.
pub struct ReadThenWritePlan {
    pub id: CatalogItemId,
    /// Read portion of query.
    ///
    /// NOTE: Even if the WHERE filter is left off, we still need to perform a read to generate
    /// retractions.
    pub selection: HirRelationExpr,
    /// Map from column index to SET expression. Empty for DELETE statements.
    pub assignments: BTreeMap<usize, HirScalarExpr>,
    pub finishing: RowSetFinishing,
}

pub fn plan_delete_query(
    scx: &StatementContext,
    mut delete_stmt: DeleteStatement<Aug>,
) -> Result<ReadThenWritePlan, PlanError> {
    transform_ast::transform(scx, &mut delete_stmt)?;

    let qcx = QueryContext::root(scx, QueryLifetime::OneShot);
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
    transform_ast::transform(scx, &mut update_stmt)?;

    let qcx = QueryContext::root(scx, QueryLifetime::OneShot);

    plan_mutation_query_inner(
        qcx,
        update_stmt.table_name,
        update_stmt.alias,
        vec![],
        update_stmt.assignments,
        update_stmt.selection,
    )
}

pub fn plan_mutation_query_inner(
    qcx: QueryContext,
    table_name: ResolvedItemName,
    alias: Option<TableAlias>,
    using: Vec<TableWithJoins<Aug>>,
    assignments: Vec<Assignment<Aug>>,
    selection: Option<Expr<Aug>>,
) -> Result<ReadThenWritePlan, PlanError> {
    // Get ID and version of the relation desc.
    let (id, version) = match table_name {
        ResolvedItemName::Item { id, version, .. } => (id, version),
        _ => sql_bail!("cannot mutate non-user table"),
    };

    // Perform checks on item with given ID.
    let item = qcx.scx.get_item(&id).at_version(version);
    if item.item_type() != CatalogItemType::Table {
        sql_bail!(
            "cannot mutate {} '{}'",
            item.item_type(),
            table_name.full_name_str()
        );
    }
    let _ = item.writable_table_details().ok_or_else(|| {
        sql_err!(
            "cannot mutate non-writeable table '{}'",
            table_name.full_name_str()
        )
    })?;
    if id.is_system() {
        sql_bail!(
            "cannot mutate system table '{}'",
            table_name.full_name_str()
        );
    }

    // Derive structs for operation from validated table
    let (mut get, scope) = qcx.resolve_table_name(table_name)?;
    let scope = plan_table_alias(scope, alias.as_ref())?;
    let desc = item.desc(&qcx.scx.catalog.resolve_full_name(item.name()))?;
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
                allow_parameters: true,
                allow_windows: false,
            };
            let expr = plan_expr(ecx, &expr)?.type_as(ecx, &SqlScalarType::Bool)?;
            get = get.filter(vec![expr]);
        }
    } else {
        get = handle_mutation_using_clause(&qcx, selection, using, get, scope.clone())?;
    }

    let mut sets = BTreeMap::new();
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
                    allow_parameters: true,
                    allow_windows: false,
                };
                let expr = plan_expr(ecx, &value)?.cast_to(
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
        using.into_iter().try_fold(plan_join_identity(), |l, twj| {
            let (left, left_scope) = l;
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
            qcx,
            name: "WHERE clause",
            scope: &joined_scope,
            relation_type: &joined_relation_type,
            allow_aggregates: false,
            allow_subqueries: true,
            allow_parameters: true,
            allow_windows: false,
        };

        // Plan the filter expression on `FROM, USING...`.
        let mut expr = plan_expr(ecx, &expr)?.type_as(ecx, &SqlScalarType::Bool)?;

        // Rewrite all column referring to the `FROM` section of `joined` (i.e.
        // those to the right of `using_rel_expr`) to instead be correlated to
        // the outer relation, i.e. `get`.
        let using_rel_arity = qcx.relation_type(&using_rel_expr).arity();
        // local import to not get confused with `mz_sql_parser::ast::visit::Visit`
        use mz_expr::visit::Visit;
        expr.visit_mut_post(&mut |e| {
            if let HirScalarExpr::Column(c, _name) = e {
                if c.column >= using_rel_arity {
                    c.level += 1;
                    c.column -= using_rel_arity;
                };
            }
        })?;

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

#[derive(Debug)]
pub(crate) struct CastRelationError {
    pub(crate) column: usize,
    pub(crate) source_type: SqlScalarType,
    pub(crate) target_type: SqlScalarType,
}

/// Cast a relation from one type to another using the specified type of cast.
///
/// The length of `target_types` must match the arity of `expr`.
pub(crate) fn cast_relation<'a, I>(
    qcx: &QueryContext,
    ccx: CastContext,
    expr: HirRelationExpr,
    target_types: I,
) -> Result<HirRelationExpr, CastRelationError>
where
    I: IntoIterator<Item = &'a SqlScalarType>,
{
    let ecx = &ExprContext {
        qcx,
        name: "values",
        scope: &Scope::empty(),
        relation_type: &qcx.relation_type(&expr),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_parameters: true,
        allow_windows: false,
    };
    let mut map_exprs = vec![];
    let mut project_key = vec![];
    for (i, target_typ) in target_types.into_iter().enumerate() {
        let expr = HirScalarExpr::column(i);
        // We plan every cast and check the evaluated expressions rather than
        // checking the types directly because of some complex casting rules
        // between types not expressed in `SqlScalarType` equality.
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

/// Plans an expression in the AS OF position of a `SELECT` or `SUBSCRIBE`, or `CREATE MATERIALIZED
/// VIEW` statement.
pub fn plan_as_of(
    scx: &StatementContext,
    as_of: Option<AsOf<Aug>>,
) -> Result<QueryWhen, PlanError> {
    match as_of {
        None => Ok(QueryWhen::Immediately),
        Some(as_of) => match as_of {
            AsOf::At(expr) => Ok(QueryWhen::AtTimestamp(plan_as_of_or_up_to(scx, expr)?)),
            AsOf::AtLeast(expr) => Ok(QueryWhen::AtLeastTimestamp(plan_as_of_or_up_to(scx, expr)?)),
        },
    }
}

/// Plans and evaluates a scalar expression in a OneShot context to a non-null MzTimestamp.
///
/// Produces [`PlanError::InvalidAsOfUpTo`] if the expression is
/// - not a constant,
/// - not castable to MzTimestamp,
/// - is null,
/// - contains an unmaterializable function,
/// - some other evaluation error occurs, e.g., a division by 0,
/// - contains aggregates, subqueries, parameters, or window function calls.
pub fn plan_as_of_or_up_to(
    scx: &StatementContext,
    mut expr: Expr<Aug>,
) -> Result<mz_repr::Timestamp, PlanError> {
    let scope = Scope::empty();
    let desc = RelationDesc::empty();
    // (Even for a SUBSCRIBE, we need QueryLifetime::OneShot, because the AS OF or UP TO is
    // evaluated only once.)
    let qcx = QueryContext::root(scx, QueryLifetime::OneShot);
    transform_ast::transform(scx, &mut expr)?;
    let ecx = &ExprContext {
        qcx: &qcx,
        name: "AS OF or UP TO",
        scope: &scope,
        relation_type: desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    };
    let hir = plan_expr(ecx, &expr)?.cast_to(
        ecx,
        CastContext::Assignment,
        &SqlScalarType::MzTimestamp,
    )?;
    if hir.contains_unmaterializable() {
        bail_unsupported!("calling an unmaterializable function in AS OF or UP TO");
    }
    // At this point, we definitely have a constant expression:
    // - it can't contain any unmaterializable functions;
    // - it can't refer to any columns.
    // But the following can still fail due to a variety of reasons: most commonly, the cast can
    // fail, but also a null might appear, or some other evaluation error can happen, e.g., a
    // division by 0.
    let timestamp = hir
        .into_literal_mz_timestamp()
        .ok_or_else(|| PlanError::InvalidAsOfUpTo)?;
    Ok(timestamp)
}

/// Plans an expression in the AS position of a `CREATE SECRET`.
pub fn plan_secret_as(
    scx: &StatementContext,
    mut expr: Expr<Aug>,
) -> Result<MirScalarExpr, PlanError> {
    let scope = Scope::empty();
    let desc = RelationDesc::empty();
    let qcx = QueryContext::root(scx, QueryLifetime::OneShot);

    transform_ast::transform(scx, &mut expr)?;

    let ecx = &ExprContext {
        qcx: &qcx,
        name: "AS",
        scope: &scope,
        relation_type: desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    };
    let expr = plan_expr(ecx, &expr)?
        .type_as(ecx, &SqlScalarType::Bytes)?
        .lower_uncorrelated()?;
    Ok(expr)
}

/// Plans an expression in the CHECK position of a `CREATE SOURCE ... FROM WEBHOOK`.
pub fn plan_webhook_validate_using(
    scx: &StatementContext,
    validate_using: CreateWebhookSourceCheck<Aug>,
) -> Result<WebhookValidation, PlanError> {
    let qcx = QueryContext::root(scx, QueryLifetime::Source);

    let CreateWebhookSourceCheck {
        options,
        using: mut expr,
    } = validate_using;

    let mut column_typs = vec![];
    let mut column_names = vec![];

    let (bodies, headers, secrets) = options
        .map(|o| (o.bodies, o.headers, o.secrets))
        .unwrap_or_default();

    // Append all of the bodies so they can be used in the expression.
    let mut body_tuples = vec![];
    for CreateWebhookSourceBody { alias, use_bytes } in bodies {
        let scalar_type = use_bytes
            .then_some(SqlScalarType::Bytes)
            .unwrap_or(SqlScalarType::String);
        let name = alias
            .map(|a| a.into_string())
            .unwrap_or_else(|| "body".to_string());

        column_typs.push(SqlColumnType {
            scalar_type,
            nullable: false,
        });
        column_names.push(name);

        // Store the column index so we can be sure to provide this body correctly.
        let column_idx = column_typs.len() - 1;
        // Double check we're consistent with column names.
        assert_eq!(
            column_idx,
            column_names.len() - 1,
            "body column names and types don't match"
        );
        body_tuples.push((column_idx, use_bytes));
    }

    // Append all of the headers so they can be used in the expression.
    let mut header_tuples = vec![];

    for CreateWebhookSourceHeader { alias, use_bytes } in headers {
        let value_type = use_bytes
            .then_some(SqlScalarType::Bytes)
            .unwrap_or(SqlScalarType::String);
        let name = alias
            .map(|a| a.into_string())
            .unwrap_or_else(|| "headers".to_string());

        column_typs.push(SqlColumnType {
            scalar_type: SqlScalarType::Map {
                value_type: Box::new(value_type),
                custom_id: None,
            },
            nullable: false,
        });
        column_names.push(name);

        // Store the column index so we can be sure to provide this body correctly.
        let column_idx = column_typs.len() - 1;
        // Double check we're consistent with column names.
        assert_eq!(
            column_idx,
            column_names.len() - 1,
            "header column names and types don't match"
        );
        header_tuples.push((column_idx, use_bytes));
    }

    // Append all secrets so they can be used in the expression.
    let mut validation_secrets = vec![];

    for CreateWebhookSourceSecret {
        secret,
        alias,
        use_bytes,
    } in secrets
    {
        // Either provide the secret to the validation expression as Bytes or a String.
        let scalar_type = use_bytes
            .then_some(SqlScalarType::Bytes)
            .unwrap_or(SqlScalarType::String);

        column_typs.push(SqlColumnType {
            scalar_type,
            nullable: false,
        });
        let ResolvedItemName::Item {
            id,
            full_name: FullItemName { item, .. },
            ..
        } = secret
        else {
            return Err(PlanError::InvalidSecret(Box::new(secret)));
        };

        // Plan the expression using the secret's alias, if one is provided.
        let name = if let Some(alias) = alias {
            alias.into_string()
        } else {
            item
        };
        column_names.push(name);

        // Get the column index that corresponds for this secret, so we can make sure to provide the
        // secrets in the correct order during evaluation.
        let column_idx = column_typs.len() - 1;
        // Double check that our column names and types match.
        assert_eq!(
            column_idx,
            column_names.len() - 1,
            "column names and types don't match"
        );

        validation_secrets.push(WebhookValidationSecret {
            id,
            column_idx,
            use_bytes,
        });
    }

    let relation_typ = SqlRelationType::new(column_typs);
    let desc = RelationDesc::new(relation_typ, column_names.clone());
    let scope = Scope::from_source(None, column_names);

    transform_ast::transform(scx, &mut expr)?;

    let ecx = &ExprContext {
        qcx: &qcx,
        name: "CHECK",
        scope: &scope,
        relation_type: desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    };
    let expr = plan_expr(ecx, &expr)?
        .type_as(ecx, &SqlScalarType::Bool)?
        .lower_uncorrelated()?;
    let validation = WebhookValidation {
        expression: expr,
        relation_desc: desc,
        bodies: body_tuples,
        headers: header_tuples,
        secrets: validation_secrets,
    };
    Ok(validation)
}

pub fn plan_default_expr(
    scx: &StatementContext,
    expr: &Expr<Aug>,
    target_ty: &SqlScalarType,
) -> Result<HirScalarExpr, PlanError> {
    let qcx = QueryContext::root(scx, QueryLifetime::OneShot);
    let ecx = &ExprContext {
        qcx: &qcx,
        name: "DEFAULT expression",
        scope: &Scope::empty(),
        relation_type: &SqlRelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    };
    let hir = plan_expr(ecx, expr)?.cast_to(ecx, CastContext::Assignment, target_ty)?;
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

    let qcx = QueryContext::root(scx, QueryLifetime::OneShot);

    let mut datums = Row::default();
    let mut packer = datums.packer();
    let mut actual_types = Vec::new();
    let temp_storage = &RowArena::new();
    for (i, (mut expr, expected_ty)) in params.into_iter().zip_eq(&desc.param_types).enumerate() {
        transform_ast::transform(scx, &mut expr)?;

        let ecx = execute_expr_context(&qcx);
        let ex = plan_expr(&ecx, &expr)?.type_as_any(&ecx)?;
        let actual_ty = ecx.scalar_type(&ex);
        if plan_hypothetical_cast(&ecx, *EXECUTE_CAST_CONTEXT, &actual_ty, expected_ty).is_none() {
            return Err(PlanError::WrongParameterType(
                i + 1,
                ecx.humanize_scalar_type(expected_ty, false),
                ecx.humanize_scalar_type(&actual_ty, false),
            ));
        }
        let ex = ex.lower_uncorrelated()?;
        let evaled = ex.eval(&[], temp_storage)?;
        packer.push(evaled);
        actual_types.push(actual_ty);
    }
    Ok(Params {
        datums,
        execute_types: actual_types,
        expected_types: desc.param_types.clone(),
    })
}

static EXECUTE_CONTEXT_SCOPE: LazyLock<Scope> = LazyLock::new(Scope::empty);
static EXECUTE_CONTEXT_REL_TYPE: LazyLock<SqlRelationType> = LazyLock::new(SqlRelationType::empty);

/// Returns an `ExprContext` for the expressions in the parameters of an EXECUTE statement.
pub(crate) fn execute_expr_context<'a>(qcx: &'a QueryContext<'a>) -> ExprContext<'a> {
    ExprContext {
        qcx,
        name: "EXECUTE",
        scope: &EXECUTE_CONTEXT_SCOPE,
        relation_type: &EXECUTE_CONTEXT_REL_TYPE,
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    }
}

/// The CastContext used when matching up the types of parameters passed to EXECUTE.
///
/// This is an assignment cast also in Postgres, see
/// <https://github.com/MaterializeInc/database-issues/issues/9266>
pub(crate) static EXECUTE_CAST_CONTEXT: LazyLock<CastContext> =
    LazyLock::new(|| CastContext::Assignment);

pub fn plan_index_exprs<'a>(
    scx: &'a StatementContext,
    on_desc: &RelationDesc,
    exprs: Vec<Expr<Aug>>,
) -> Result<Vec<mz_expr::MirScalarExpr>, PlanError> {
    let scope = Scope::from_source(None, on_desc.iter_names());
    let qcx = QueryContext::root(scx, QueryLifetime::Index);

    let ecx = &ExprContext {
        qcx: &qcx,
        name: "CREATE INDEX",
        scope: &scope,
        relation_type: on_desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    };
    let mut out = vec![];
    for mut expr in exprs {
        transform_ast::transform(scx, &mut expr)?;
        let expr = plan_expr_or_col_index(ecx, &expr)?;
        let mut expr = expr.lower_uncorrelated()?;
        expr.reduce(&on_desc.typ().column_types);
        out.push(expr);
    }
    Ok(out)
}

fn plan_expr_or_col_index(ecx: &ExprContext, e: &Expr<Aug>) -> Result<HirScalarExpr, PlanError> {
    match check_col_index(ecx.name, e, ecx.relation_type.column_types.len())? {
        Some(column) => Ok(HirScalarExpr::column(column)),
        _ => plan_expr(ecx, e)?.type_as_any(ecx),
    }
}

fn check_col_index(name: &str, e: &Expr<Aug>, max: usize) -> Result<Option<usize>, PlanError> {
    match e {
        Expr::Value(Value::Number(n)) => {
            let n = n.parse::<usize>().map_err(|e| {
                sql_err!("unable to parse column reference in {}: {}: {}", name, n, e)
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

struct PlannedQuery {
    expr: HirRelationExpr,
    scope: Scope,
    order_by: Vec<ColumnOrder>,
    limit: Option<HirScalarExpr>,
    /// `offset` is either
    /// - an Int64 literal
    /// - or contains parameters. (If it contains parameters, then after parameter substitution it
    ///   should also be `is_constant` and reduce to an Int64 literal, but we check this only
    ///   later.)
    offset: HirScalarExpr,
    project: Vec<usize>,
    group_size_hints: GroupSizeHints,
}

fn plan_query(qcx: &mut QueryContext, q: &Query<Aug>) -> Result<PlannedQuery, PlanError> {
    qcx.checked_recur_mut(|qcx| plan_query_inner(qcx, q))
}

fn plan_query_inner(qcx: &mut QueryContext, q: &Query<Aug>) -> Result<PlannedQuery, PlanError> {
    // Plan CTEs and introduce bindings to `qcx.ctes`. Returns shadowed bindings
    // for the identifiers, so that they can be re-installed before returning.
    let cte_bindings = plan_ctes(qcx, q)?;

    let limit = match &q.limit {
        None => None,
        Some(Limit {
            quantity,
            with_ties: false,
        }) => {
            let ecx = &ExprContext {
                qcx,
                name: "LIMIT",
                scope: &Scope::empty(),
                relation_type: &SqlRelationType::empty(),
                allow_aggregates: false,
                allow_subqueries: true,
                allow_parameters: true,
                allow_windows: false,
            };
            let limit = plan_expr(ecx, quantity)?;
            let limit = limit.cast_to(ecx, CastContext::Explicit, &SqlScalarType::Int64)?;

            let limit = if limit.is_constant() {
                let arena = RowArena::new();
                let limit = limit.lower_uncorrelated()?;

                // TODO: Don't use ? on eval, but instead wrap the error and add the information
                // that the error happened in a LIMIT clause, so that we have better error msg for
                // something like `SELECT 5 LIMIT 'aaa'`.
                match limit.eval(&[], &arena)? {
                    d @ Datum::Int64(v) if v >= 0 => {
                        HirScalarExpr::literal(d, SqlScalarType::Int64)
                    }
                    d @ Datum::Null => HirScalarExpr::literal(d, SqlScalarType::Int64),
                    Datum::Int64(_) => sql_bail!("LIMIT must not be negative"),
                    _ => sql_bail!("constant LIMIT expression must reduce to an INT or NULL value"),
                }
            } else {
                // Gate non-constant LIMIT expressions behind a feature flag
                qcx.scx
                    .require_feature_flag(&vars::ENABLE_EXPRESSIONS_IN_LIMIT_SYNTAX)?;
                limit
            };

            Some(limit)
        }
        Some(Limit {
            quantity: _,
            with_ties: true,
        }) => bail_unsupported!("FETCH ... WITH TIES"),
    };

    let offset = match &q.offset {
        None => HirScalarExpr::literal(Datum::Int64(0), SqlScalarType::Int64),
        Some(offset) => {
            let ecx = &ExprContext {
                qcx,
                name: "OFFSET",
                scope: &Scope::empty(),
                relation_type: &SqlRelationType::empty(),
                allow_aggregates: false,
                allow_subqueries: false,
                allow_parameters: true,
                allow_windows: false,
            };
            let offset = plan_expr(ecx, offset)?;
            let offset = offset.cast_to(ecx, CastContext::Explicit, &SqlScalarType::Int64)?;

            let offset = if offset.is_constant() {
                // Simplify it to a literal or error out. (E.g., the cast inserted above may fail.)
                let offset_value = offset_into_value(offset)?;
                HirScalarExpr::literal(Datum::Int64(offset_value), SqlScalarType::Int64)
            } else {
                // The only case when this is allowed to not be a constant is if it contains
                // parameters. (In which case, we'll later check that it's a constant after
                // parameter binding.)
                if !offset.contains_parameters() {
                    return Err(PlanError::InvalidOffset(format!(
                        "must be simplifiable to a constant, possibly after parameter binding, got {}",
                        offset
                    )));
                }
                offset
            };
            offset
        }
    };

    let mut planned_query = match &q.body {
        SetExpr::Select(s) => {
            // Extract query options.
            let select_option_extracted = SelectOptionExtracted::try_from(s.options.clone())?;
            let group_size_hints = GroupSizeHints::try_from(select_option_extracted)?;

            let plan = plan_select_from_where(qcx, *s.clone(), q.order_by.clone())?;
            PlannedQuery {
                expr: plan.expr,
                scope: plan.scope,
                order_by: plan.order_by,
                project: plan.project,
                limit,
                offset,
                group_size_hints,
            }
        }
        _ => {
            let (expr, scope) = plan_set_expr(qcx, &q.body)?;
            let ecx = &ExprContext {
                qcx,
                name: "ORDER BY clause of a set expression",
                scope: &scope,
                relation_type: &qcx.relation_type(&expr),
                allow_aggregates: false,
                allow_subqueries: true,
                allow_parameters: true,
                allow_windows: false,
            };
            let output_columns: Vec<_> = scope.column_names().enumerate().collect();
            let (order_by, map_exprs) = plan_order_by_exprs(ecx, &q.order_by, &output_columns)?;
            let project = (0..ecx.relation_type.arity()).collect();
            PlannedQuery {
                expr: expr.map(map_exprs),
                scope,
                order_by,
                limit,
                project,
                offset,
                group_size_hints: GroupSizeHints::default(),
            }
        }
    };

    // Both introduce `Let` bindings atop `result` and re-install shadowed bindings.
    match &q.ctes {
        CteBlock::Simple(_) => {
            for (id, value, shadowed_val) in cte_bindings.into_iter().rev() {
                if let Some(cte) = qcx.ctes.remove(&id) {
                    planned_query.expr = HirRelationExpr::Let {
                        name: cte.name,
                        id: id.clone(),
                        value: Box::new(value),
                        body: Box::new(planned_query.expr),
                    };
                }
                if let Some(shadowed_val) = shadowed_val {
                    qcx.ctes.insert(id, shadowed_val);
                }
            }
        }
        CteBlock::MutuallyRecursive(MutRecBlock { options, ctes: _ }) => {
            let MutRecBlockOptionExtracted {
                recursion_limit,
                return_at_recursion_limit,
                error_at_recursion_limit,
                seen: _,
            } = MutRecBlockOptionExtracted::try_from(options.clone())?;
            let limit = match (recursion_limit, return_at_recursion_limit, error_at_recursion_limit) {
                (None, None, None) => None,
                (Some(max_iters), None, None) => Some((max_iters, LetRecLimit::RETURN_AT_LIMIT_DEFAULT)),
                (None, Some(max_iters), None) => Some((max_iters, true)),
                (None, None, Some(max_iters)) => Some((max_iters, false)),
                _ => {
                    return Err(InvalidWmrRecursionLimit("More than one recursion limit given. Please give at most one of RECURSION LIMIT, ERROR AT RECURSION LIMIT, RETURN AT RECURSION LIMIT.".to_owned()));
                }
            }.try_map(|(max_iters, return_at_limit)| Ok::<LetRecLimit, PlanError>(LetRecLimit {
                max_iters: NonZeroU64::new(*max_iters).ok_or(InvalidWmrRecursionLimit("Recursion limit has to be greater than 0.".to_owned()))?,
                return_at_limit: *return_at_limit,
            }))?;

            let mut bindings = Vec::new();
            for (id, value, shadowed_val) in cte_bindings.into_iter() {
                if let Some(cte) = qcx.ctes.remove(&id) {
                    bindings.push((cte.name, id, value, cte.desc.into_typ()));
                }
                if let Some(shadowed_val) = shadowed_val {
                    qcx.ctes.insert(id, shadowed_val);
                }
            }
            if !bindings.is_empty() {
                planned_query.expr = HirRelationExpr::LetRec {
                    limit,
                    bindings,
                    body: Box::new(planned_query.expr),
                }
            }
        }
    }

    Ok(planned_query)
}

/// Converts an OFFSET expression into a value.
pub fn offset_into_value(offset: HirScalarExpr) -> Result<i64, PlanError> {
    let offset = offset
        .try_into_literal_int64()
        .map_err(|err| PlanError::InvalidOffset(err.to_string_with_causes()))?;
    if offset < 0 {
        return Err(PlanError::InvalidOffset(format!(
            "must not be negative, got {}",
            offset
        )));
    }
    Ok(offset)
}

generate_extracted_config!(
    MutRecBlockOption,
    (RecursionLimit, u64),
    (ReturnAtRecursionLimit, u64),
    (ErrorAtRecursionLimit, u64)
);

/// Creates plans for CTEs and introduces them to `qcx.ctes`.
///
/// Returns for each identifier a planned `HirRelationExpr` value, and an optional
/// shadowed value that can be reinstalled once the planning has completed.
pub fn plan_ctes(
    qcx: &mut QueryContext,
    q: &Query<Aug>,
) -> Result<Vec<(LocalId, HirRelationExpr, Option<CteDesc>)>, PlanError> {
    // Accumulate planned expressions and shadowed descriptions.
    let mut result = Vec::new();
    // Retain the old descriptions of CTE bindings so that we can restore them
    // after we're done planning this SELECT.
    let mut shadowed_descs = BTreeMap::new();

    // A reused identifier indicates a reused name.
    if let Some(ident) = q.ctes.bound_identifiers().duplicates().next() {
        sql_bail!(
            "WITH query name {} specified more than once",
            normalize::ident_ref(ident).quoted()
        )
    }

    match &q.ctes {
        CteBlock::Simple(ctes) => {
            // Plan all CTEs, introducing the types for non-recursive CTEs as we go.
            for cte in ctes.iter() {
                let cte_name = normalize::ident(cte.alias.name.clone());
                let (val, scope) = plan_nested_query(qcx, &cte.query)?;
                let typ = qcx.relation_type(&val);
                let mut desc = RelationDesc::new(typ, scope.column_names());
                plan_utils::maybe_rename_columns(
                    format!("CTE {}", cte.alias.name),
                    &mut desc,
                    &cte.alias.columns,
                )?;
                // Capture the prior value if it exists, so that it can be re-installed.
                let shadowed = qcx.ctes.insert(
                    cte.id,
                    CteDesc {
                        name: cte_name,
                        desc,
                    },
                );

                result.push((cte.id, val, shadowed));
            }
        }
        CteBlock::MutuallyRecursive(MutRecBlock { options: _, ctes }) => {
            // Insert column types into `qcx.ctes` first for recursive bindings.
            for cte in ctes.iter() {
                let cte_name = normalize::ident(cte.name.clone());
                let mut desc_columns = Vec::with_capacity(cte.columns.capacity());
                for column in cte.columns.iter() {
                    desc_columns.push((
                        normalize::column_name(column.name.clone()),
                        SqlColumnType {
                            scalar_type: scalar_type_from_sql(qcx.scx, &column.data_type)?,
                            nullable: true,
                        },
                    ));
                }
                let desc = RelationDesc::from_names_and_types(desc_columns);
                let shadowed = qcx.ctes.insert(
                    cte.id,
                    CteDesc {
                        name: cte_name,
                        desc,
                    },
                );
                // Capture the prior value if it exists, so that it can be re-installed.
                if let Some(shadowed) = shadowed {
                    shadowed_descs.insert(cte.id, shadowed);
                }
            }

            // Plan all CTEs and validate the proposed types.
            for cte in ctes.iter() {
                let (val, _scope) = plan_nested_query(qcx, &cte.query)?;

                let proposed_typ = qcx.ctes[&cte.id].desc.typ();

                if proposed_typ.column_types.iter().any(|c| !c.nullable) {
                    // Once WMR CTEs support NOT NULL constraints, check that
                    // nullability of derived column types are compatible.
                    sql_bail!(
                        "[internal error]: WMR CTEs do not support NOT NULL constraints on proposed column types"
                    );
                }

                if !proposed_typ.keys.is_empty() {
                    // Once WMR CTEs support keys, check that keys exactly
                    // overlap.
                    sql_bail!("[internal error]: WMR CTEs do not support keys");
                }

                // Validate that the derived and proposed types are the same.
                let derived_typ = qcx.relation_type(&val);

                let type_err = |proposed_typ: &SqlRelationType, derived_typ: SqlRelationType| {
                    let cte_name = normalize::ident(cte.name.clone());
                    let proposed_typ = proposed_typ
                        .column_types
                        .iter()
                        .map(|ty| qcx.humanize_scalar_type(&ty.scalar_type, false))
                        .collect::<Vec<_>>();
                    let inferred_typ = derived_typ
                        .column_types
                        .iter()
                        .map(|ty| qcx.humanize_scalar_type(&ty.scalar_type, false))
                        .collect::<Vec<_>>();
                    Err(PlanError::RecursiveTypeMismatch(
                        cte_name,
                        proposed_typ,
                        inferred_typ,
                    ))
                };

                if derived_typ.column_types.len() != proposed_typ.column_types.len() {
                    return type_err(proposed_typ, derived_typ);
                }

                // Cast derived types to proposed types or error.
                let val = match cast_relation(
                    qcx,
                    // Choose `CastContext::Assignment`` because the user has
                    // been explicit about the types they expect. Choosing
                    // `CastContext::Implicit` is not "strong" enough to impose
                    // typmods from proposed types onto values.
                    CastContext::Assignment,
                    val,
                    proposed_typ.column_types.iter().map(|c| &c.scalar_type),
                ) {
                    Ok(val) => val,
                    Err(_) => return type_err(proposed_typ, derived_typ),
                };

                result.push((cte.id, val, shadowed_descs.remove(&cte.id)));
            }
        }
    }

    Ok(result)
}

pub fn plan_nested_query(
    qcx: &mut QueryContext,
    q: &Query<Aug>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    let PlannedQuery {
        mut expr,
        scope,
        order_by,
        limit,
        offset,
        project,
        group_size_hints,
    } = qcx.checked_recur_mut(|qcx| plan_query(qcx, q))?;
    if limit.is_some()
        || !offset
            .clone()
            .try_into_literal_int64()
            .is_ok_and(|offset| offset == 0)
    {
        expr = HirRelationExpr::top_k(
            expr,
            vec![],
            order_by,
            limit,
            offset,
            group_size_hints.limit_input_group_size,
        );
    }
    Ok((expr.project(project), scope))
}

fn plan_set_expr(
    qcx: &mut QueryContext,
    q: &SetExpr<Aug>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    match q {
        SetExpr::Select(select) => {
            let order_by_exprs = Vec::new();
            let plan = plan_select_from_where(qcx, *select.clone(), order_by_exprs)?;
            // We didn't provide any `order_by_exprs`, so `plan_select_from_where`
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
            let (left_expr, left_scope) = qcx.checked_recur_mut(|qcx| plan_set_expr(qcx, left))?;
            let (right_expr, right_scope) =
                qcx.checked_recur_mut(|qcx| plan_set_expr(qcx, right))?;

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
                qcx,
                name: &op.to_string(),
                scope: &left_scope,
                relation_type: &left_type,
                allow_aggregates: false,
                allow_subqueries: false,
                allow_parameters: false,
                allow_windows: false,
            };
            let right_ecx = &ExprContext {
                qcx,
                name: &op.to_string(),
                scope: &right_scope,
                relation_type: &right_type,
                allow_aggregates: false,
                allow_subqueries: false,
                allow_parameters: false,
                allow_windows: false,
            };
            let mut left_casts = vec![];
            let mut right_casts = vec![];
            for (i, (left_type, right_type)) in left_type
                .column_types
                .iter()
                .zip_eq(right_type.column_types.iter())
                .enumerate()
            {
                let types = &[
                    CoercibleScalarType::Coerced(left_type.scalar_type.clone()),
                    CoercibleScalarType::Coerced(right_type.scalar_type.clone()),
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
                        qcx.humanize_scalar_type(&left_type.scalar_type, false),
                        qcx.humanize_scalar_type(&target, false),
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
                        qcx.humanize_scalar_type(&target, false),
                        qcx.humanize_scalar_type(&right_type.scalar_type, false),
                    ),
                }
            }
            let lhs = if left_casts
                .iter()
                .enumerate()
                .any(|(i, e)| e != &HirScalarExpr::column(i))
            {
                let project_key: Vec<_> = (left_type.arity()..left_type.arity() * 2).collect();
                left_expr.map(left_casts).project(project_key)
            } else {
                left_expr
            };
            let rhs = if right_casts
                .iter()
                .enumerate()
                .any(|(i, e)| e != &HirScalarExpr::column(i))
            {
                let project_key: Vec<_> = (right_type.arity()..right_type.arity() * 2).collect();
                right_expr.map(right_casts).project(project_key)
            } else {
                right_expr
            };

            let relation_expr = match op {
                SetOperator::Union => {
                    if *all {
                        lhs.union(rhs)
                    } else {
                        lhs.union(rhs).distinct()
                    }
                }
                SetOperator::Except => Hir::except(all, lhs, rhs),
                SetOperator::Intersect => {
                    // TODO: Let's not duplicate the left-hand expression into TWO dataflows!
                    // Though we believe that render() does The Right Thing (TM)
                    // Also note that we do *not* need another threshold() at the end of the method chain
                    // because the right-hand side of the outer union only produces existing records,
                    // i.e., the record counts for differential data flow definitely remain non-negative.
                    let left_clone = lhs.clone();
                    if *all {
                        lhs.union(left_clone.union(rhs.negate()).threshold().negate())
                    } else {
                        lhs.union(left_clone.union(rhs.negate()).threshold().negate())
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
        SetExpr::Table(name) => {
            let (expr, scope) = qcx.resolve_table_name(name.clone())?;
            Ok((expr, scope))
        }
        SetExpr::Query(query) => {
            let (expr, scope) = plan_nested_query(qcx, query)?;
            Ok((expr, scope))
        }
        SetExpr::Show(stmt) => {
            // The create SQL definition of involving this query, will have the explicit `SHOW`
            // command in it. Many `SHOW` commands will expand into a sub-query that involves the
            // current schema of the executing user. When Materialize restarts and tries to re-plan
            // these queries, it will only have access to the raw `SHOW` command and have no idea
            // what schema to use. As a result Materialize will fail to boot.
            //
            // Some `SHOW` commands are ok, like `SHOW CLUSTERS`, and there are probably other ways
            // around this issue. Such as expanding the `SHOW` command in the SQL definition.
            // However, banning show commands in views gives us more flexibility to change their
            // output.
            //
            // TODO(jkosh44) Add message to error that prints out an equivalent view definition
            // with all show commands expanded into their equivalent SELECT statements.
            if !qcx.lifetime.allow_show() {
                return Err(PlanError::ShowCommandInView);
            }

            // Some SHOW statements are a SELECT query. Others produces Rows
            // directly. Convert both of these to the needed Hir and Scope.
            fn to_hirscope(
                plan: ShowCreatePlan,
                desc: StatementDesc,
            ) -> Result<(HirRelationExpr, Scope), PlanError> {
                let rows = vec![plan.row.iter().collect::<Vec<_>>()];
                let desc = desc.relation_desc.expect("must exist");
                let scope = Scope::from_source(None, desc.iter_names());
                let expr = HirRelationExpr::constant(rows, desc.into_typ());
                Ok((expr, scope))
            }

            match stmt.clone() {
                ShowStatement::ShowColumns(stmt) => {
                    show::show_columns(qcx.scx, stmt)?.plan_hir(qcx)
                }
                ShowStatement::ShowCreateConnection(stmt) => to_hirscope(
                    show::plan_show_create_connection(qcx.scx, stmt.clone())?,
                    show::describe_show_create_connection(qcx.scx, stmt)?,
                ),
                ShowStatement::ShowCreateCluster(stmt) => to_hirscope(
                    show::plan_show_create_cluster(qcx.scx, stmt.clone())?,
                    show::describe_show_create_cluster(qcx.scx, stmt)?,
                ),
                ShowStatement::ShowCreateIndex(stmt) => to_hirscope(
                    show::plan_show_create_index(qcx.scx, stmt.clone())?,
                    show::describe_show_create_index(qcx.scx, stmt)?,
                ),
                ShowStatement::ShowCreateSink(stmt) => to_hirscope(
                    show::plan_show_create_sink(qcx.scx, stmt.clone())?,
                    show::describe_show_create_sink(qcx.scx, stmt)?,
                ),
                ShowStatement::ShowCreateSource(stmt) => to_hirscope(
                    show::plan_show_create_source(qcx.scx, stmt.clone())?,
                    show::describe_show_create_source(qcx.scx, stmt)?,
                ),
                ShowStatement::ShowCreateTable(stmt) => to_hirscope(
                    show::plan_show_create_table(qcx.scx, stmt.clone())?,
                    show::describe_show_create_table(qcx.scx, stmt)?,
                ),
                ShowStatement::ShowCreateView(stmt) => to_hirscope(
                    show::plan_show_create_view(qcx.scx, stmt.clone())?,
                    show::describe_show_create_view(qcx.scx, stmt)?,
                ),
                ShowStatement::ShowCreateMaterializedView(stmt) => to_hirscope(
                    show::plan_show_create_materialized_view(qcx.scx, stmt.clone())?,
                    show::describe_show_create_materialized_view(qcx.scx, stmt)?,
                ),
                ShowStatement::ShowCreateType(stmt) => to_hirscope(
                    show::plan_show_create_type(qcx.scx, stmt.clone())?,
                    show::describe_show_create_type(qcx.scx, stmt)?,
                ),
                ShowStatement::ShowObjects(stmt) => {
                    show::show_objects(qcx.scx, stmt)?.plan_hir(qcx)
                }
                ShowStatement::ShowVariable(_) => bail_unsupported!("SHOW variable in subqueries"),
                ShowStatement::InspectShard(_) => sql_bail!("unsupported INSPECT statement"),
            }
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
        relation_type: &SqlRelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_parameters: true,
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
        func: TableFunc::Wrap {
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
    target_types: &[&SqlScalarType],
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
        relation_type: &SqlRelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_parameters: true,
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
            let val = typeconv::plan_coerce(ecx, val, target_type)?;
            let source_type = &ecx.scalar_type(&val);
            let val = match typeconv::plan_cast(ecx, CastContext::Assignment, val, target_type) {
                Ok(val) => val,
                Err(_) => sql_bail!(
                    "column {} is of type {} but expression is of type {}",
                    target_names[column].quoted(),
                    qcx.humanize_scalar_type(target_type, false),
                    qcx.humanize_scalar_type(source_type, false),
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
        func: TableFunc::Wrap {
            width: values[0].len(),
            types,
        },
        exprs,
    })
}

fn plan_join_identity() -> (HirRelationExpr, Scope) {
    let typ = SqlRelationType::new(vec![]);
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

generate_extracted_config!(
    SelectOption,
    (ExpectedGroupSize, u64),
    (AggregateInputGroupSize, u64),
    (DistinctOnInputGroupSize, u64),
    (LimitInputGroupSize, u64)
);

/// Plans a SELECT query. The SELECT query may contain an intrusive ORDER BY clause.
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
fn plan_select_from_where(
    qcx: &QueryContext,
    mut s: Select<Aug>,
    mut order_by_exprs: Vec<OrderByExpr<Aug>>,
) -> Result<SelectPlan, PlanError> {
    // TODO: Both `s` and `order_by_exprs` are not references because the
    // AggregateTableFuncVisitor needs to be able to rewrite the expressions for
    // table function support (the UUID mapping). Attempt to change this so callers
    // don't need to clone the Select.

    // Extract query options.
    let select_option_extracted = SelectOptionExtracted::try_from(s.options.clone())?;
    let group_size_hints = GroupSizeHints::try_from(select_option_extracted)?;

    // Step 1. Handle FROM clause, including joins.
    let (mut relation_expr, mut from_scope) =
        s.from.iter().try_fold(plan_join_identity(), |l, twj| {
            let (left, left_scope) = l;
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
            allow_parameters: true,
            allow_windows: false,
        };
        let expr = plan_expr(ecx, selection)
            .map_err(|e| sql_err!("WHERE clause error: {}", e))?
            .type_as(ecx, &SqlScalarType::Bool)?;
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 3. Gather aggregates and table functions.
    // (But skip window aggregates.)
    let (aggregates, table_funcs) = {
        let mut visitor = AggregateTableFuncVisitor::new(qcx.scx);
        visitor.visit_select_mut(&mut s);
        for o in order_by_exprs.iter_mut() {
            visitor.visit_order_by_expr_mut(o);
        }
        visitor.into_result()?
    };
    let mut table_func_names: BTreeMap<String, Ident> = BTreeMap::new();
    if !table_funcs.is_empty() {
        let (expr, scope) = plan_scalar_table_funcs(
            qcx,
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
            allow_parameters: true,
            allow_windows: true,
        };
        let mut out = vec![];
        for si in &s.projection {
            if *si == SelectItem::Wildcard && s.from.is_empty() {
                sql_bail!("SELECT * with no tables specified is not valid");
            }
            out.extend(expand_select_item(ecx, si, &table_func_names)?);
        }
        out
    };

    // Step 5. Handle GROUP BY clause.
    // This will also plan the aggregates gathered in Step 3.
    // See an overview of how aggregates are planned in the doc comment at the top of the file.
    let (mut group_scope, select_all_mapping) = {
        // Compute GROUP BY expressions.
        let ecx = &ExprContext {
            qcx,
            name: "GROUP BY clause",
            scope: &from_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: false,
            allow_subqueries: true,
            allow_parameters: true,
            allow_windows: false,
        };
        let mut group_key = vec![];
        let mut group_exprs: BTreeMap<HirScalarExpr, ScopeItem> = BTreeMap::new();
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

            let mut scope_item = if let HirScalarExpr::Column(
                ColumnRef {
                    level: 0,
                    column: old_column,
                },
                _name,
            ) = &expr
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
            allow_parameters: true,
            allow_windows: false,
        };
        let mut agg_exprs = vec![];
        for sql_function in aggregates {
            if sql_function.over.is_some() {
                unreachable!(
                    "Window aggregate; AggregateTableFuncVisitor explicitly filters these out"
                );
            }
            agg_exprs.push(plan_aggregate_common(ecx, &sql_function)?);
            group_scope
                .items
                .push(ScopeItem::from_expr(Expr::Function(sql_function.clone())));
        }
        if !agg_exprs.is_empty() || !group_key.is_empty() || s.having.is_some() {
            // apply GROUP BY / aggregates
            relation_expr = relation_expr.map(group_hir_exprs).reduce(
                group_key,
                agg_exprs,
                group_size_hints.aggregate_input_group_size,
            );

            // For every old column that wasn't a group key, add a scope item
            // that errors when referenced. We can't simply drop these items
            // from scope. These items need to *exist* because they might shadow
            // variables in outer scopes that would otherwise be valid to
            // reference, but accessing them needs to produce an error.
            for i in 0..from_scope.len() {
                if !select_all_mapping.contains_key(&i) {
                    let scope_item = &ecx.scope.items[i];
                    group_scope.ungrouped_columns.push(ScopeUngroupedColumn {
                        table_name: scope_item.table_name.clone(),
                        column_name: scope_item.column_name.clone(),
                        allow_unqualified_references: scope_item.allow_unqualified_references,
                    });
                }
            }

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
    if let Some(ref having) = s.having {
        let ecx = &ExprContext {
            qcx,
            name: "HAVING clause",
            scope: &group_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: true,
            allow_subqueries: true,
            allow_parameters: true,
            allow_windows: false,
        };
        let expr = plan_expr(ecx, having)?.type_as(ecx, &SqlScalarType::Bool)?;
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 7. Gather window functions from SELECT, ORDER BY, and QUALIFY, and plan them.
    // (This includes window aggregations.)
    //
    // Note that window functions can be present only in SELECT, ORDER BY, or QUALIFY (including
    // DISTINCT ON), because they are executed after grouped aggregations and HAVING.
    //
    // Also note that window functions in the ORDER BY can't refer to columns introduced in the
    // SELECT. This is because when an output column appears in ORDER BY, it can only stand alone,
    // and can't be part of a bigger expression.
    // See https://www.postgresql.org/docs/current/queries-order.html:
    // "Note that an output column name has to stand alone, that is, it cannot be used in an
    // expression"
    let window_funcs = {
        let mut visitor = WindowFuncCollector::default();
        // The `visit_select` call visits both `SELECT` and `QUALIFY` (and many other things, but
        // window functions are excluded from other things by `allow_windows` being false when
        // planning those before this code).
        visitor.visit_select(&s);
        for o in order_by_exprs.iter() {
            visitor.visit_order_by_expr(o);
        }
        visitor.into_result()
    };
    for window_func in window_funcs {
        let ecx = &ExprContext {
            qcx,
            name: "window function",
            scope: &group_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: true,
            allow_subqueries: true,
            allow_parameters: true,
            allow_windows: true,
        };
        relation_expr = relation_expr.map(vec![plan_expr(ecx, &window_func)?.type_as_any(ecx)?]);
        group_scope.items.push(ScopeItem::from_expr(window_func));
    }
    // From this point on, we shouldn't encounter _valid_ window function calls, because those have
    // been already planned now. However, we should still set `allow_windows: true` for the
    // remaining planning of `QUALIFY`, `SELECT`, and `ORDER BY`, in order to have a correct error
    // msg if an OVER clause is missing from a window function.

    // Step 8. Handle QUALIFY clause. (very similar to HAVING)
    if let Some(ref qualify) = s.qualify {
        let ecx = &ExprContext {
            qcx,
            name: "QUALIFY clause",
            scope: &group_scope,
            relation_type: &qcx.relation_type(&relation_expr),
            allow_aggregates: true,
            allow_subqueries: true,
            allow_parameters: true,
            allow_windows: true,
        };
        let expr = plan_expr(ecx, qualify)?.type_as(ecx, &SqlScalarType::Bool)?;
        relation_expr = relation_expr.filter(vec![expr]);
    }

    // Step 9. Handle SELECT clause.
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
                allow_parameters: true,
                allow_windows: true,
            };
            let expr = match select_item {
                ExpandedSelectItem::InputOrdinal(i) => {
                    if let Some(column) = select_all_mapping.get(i).copied() {
                        HirScalarExpr::column(column)
                    } else {
                        return Err(PlanError::ungrouped_column(&from_scope.items[*i]));
                    }
                }
                ExpandedSelectItem::Expr(expr) => plan_expr(ecx, expr)?.type_as_any(ecx)?,
            };
            if let HirScalarExpr::Column(ColumnRef { level: 0, column }, _name) = expr {
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

    // Step 10. Handle intrusive ORDER BY and DISTINCT.
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
                allow_parameters: true,
                allow_windows: true,
            },
            &order_by_exprs,
            &output_columns,
        )?;

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
                    allow_parameters: true,
                    allow_windows: true,
                };

                let mut distinct_exprs = vec![];
                for expr in &exprs {
                    let expr = plan_order_by_or_distinct_expr(ecx, expr, &output_columns)?;
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
                        None => sql_bail!(
                            "SELECT DISTINCT ON expressions must match initial ORDER BY expressions"
                        ),
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
                        HirScalarExpr::Column(ColumnRef { level: 0, column }, _name) => column,
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
                let distinct_len = distinct_key.len();
                relation_expr = HirRelationExpr::top_k(
                    relation_expr.map(map_exprs),
                    distinct_key,
                    order_by.iter().skip(distinct_len).cloned().collect(),
                    Some(HirScalarExpr::literal(
                        Datum::Int64(1),
                        SqlScalarType::Int64,
                    )),
                    HirScalarExpr::literal(Datum::Int64(0), SqlScalarType::Int64),
                    group_size_hints.distinct_on_input_group_size,
                );
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
    table_funcs: BTreeMap<Function<Aug>, String>,
    table_func_names: &mut BTreeMap<String, Ident>,
    relation_expr: &HirRelationExpr,
    from_scope: &Scope,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    let rows_from_qcx = qcx.derived_context(from_scope.clone(), qcx.relation_type(relation_expr));

    for (table_func, id) in table_funcs.iter() {
        table_func_names.insert(
            id.clone(),
            // TODO(parkmycar): Re-visit after having `FullItemName` use `Ident`s.
            Ident::new_unchecked(table_func.name.full_item_name().item.clone()),
        );
    }
    // If there's only a single table function, we can skip generating
    // ordinality columns.
    if table_funcs.len() == 1 {
        let (table_func, id) = table_funcs.iter().next().unwrap();
        let (expr, mut scope) =
            plan_solitary_table_function(&rows_from_qcx, table_func, None, false)?;

        // A single table-function might return several columns as a record
        let num_cols = scope.len();
        for i in 0..scope.len() {
            scope.items[i].table_name = Some(PartialItemName {
                database: None,
                schema: None,
                item: id.clone(),
            });
            scope.items[i].from_single_column_function = num_cols == 1;
            scope.items[i].allow_unqualified_references = false;
        }
        return Ok((expr, scope));
    }
    // Otherwise, plan as usual, emulating the ROWS FROM behavior
    let (expr, mut scope, num_cols) =
        plan_rows_from_internal(&rows_from_qcx, table_funcs.keys(), None)?;

    // Munge the scope so table names match with the generated ids.
    let mut i = 0;
    for (id, num_cols) in table_funcs.values().zip_eq(num_cols) {
        for _ in 0..num_cols {
            scope.items[i].table_name = Some(PartialItemName {
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
        scope.items[i].table_name = Some(PartialItemName {
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
        ExpandedSelectItem::Expr(expr) => {
            Ok((Some(expr.as_ref()), plan_expr(ecx, expr)?.type_as_any(ecx)?))
        }
    };

    // Check if the expression is a numeric literal, as in `GROUP BY 1`. This is
    // a special case that means to use the ith item in the SELECT clause.
    if let Some(column) = check_col_index(ecx.name, group_expr, projection.len())? {
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
                similar,
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
                        similar,
                    })
                }
            }
            res => Ok((Some(group_expr), res?)),
        },
        _ => Ok((
            Some(group_expr),
            plan_expr(ecx, group_expr)?.type_as_any(ecx)?,
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
pub(crate) fn plan_order_by_exprs(
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
            HirScalarExpr::Column(ColumnRef { level: 0, column }, _name) => column,
            _ => {
                map_exprs.push(expr);
                ecx.relation_type.arity() + map_exprs.len() - 1
            }
        };
        order_by.push(resolve_desc_and_nulls_last(obe, column));
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
    if let Some(i) = check_col_index(ecx.name, expr, output_columns.len())? {
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
        let (new_expr, new_scope) = plan_join(qcx, expr, scope, join)?;
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
            let (expr, scope) = plan_nested_query(&mut qcx, subquery)?;
            let scope = plan_table_alias(scope, alias.as_ref())?;
            Ok((expr, scope))
        }

        TableFactor::NestedJoin { join, alias } => {
            let (expr, scope) = plan_table_with_joins(qcx, join)?;
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
    functions: &[Function<Aug>],
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
    let (expr, mut scope, num_cols) = plan_rows_from_internal(
        qcx,
        functions,
        Some(functions[0].name.full_item_name().clone()),
    )?;

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
        columns.push(offset);
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
    functions: impl IntoIterator<Item = &'a Function<Aug>>,
    table_name: Option<FullItemName>,
) -> Result<(HirRelationExpr, Scope, Vec<usize>), PlanError> {
    let mut functions = functions.into_iter();
    let mut num_cols = Vec::new();

    // Join together each of the table functions in turn. The last column is
    // always the column to join against and is maintained to be the coalescence
    // of the row number column for all prior functions.
    let (mut left_expr, mut left_scope) =
        plan_table_function_internal(qcx, functions.next().unwrap(), true, table_name.clone())?;
    num_cols.push(left_scope.len() - 1);
    // Create the coalesced ordinality column.
    left_expr = left_expr.map(vec![HirScalarExpr::column(left_scope.len() - 1)]);
    left_scope
        .items
        .push(ScopeItem::from_column_name(ORDINALITY_COL_NAME));

    for function in functions {
        // The right hand side of a join must be planned in a new scope.
        let qcx = qcx.empty_derived_context();
        let (right_expr, mut right_scope) =
            plan_table_function_internal(&qcx, function, true, table_name.clone())?;
        num_cols.push(right_scope.len() - 1);
        let left_col = left_scope.len() - 1;
        let right_col = left_scope.len() + right_scope.len() - 1;
        let on = HirScalarExpr::call_binary(
            HirScalarExpr::column(left_col),
            HirScalarExpr::column(right_col),
            expr_func::Eq,
        );
        left_expr = left_expr
            .join(right_expr, on, JoinKind::FullOuter)
            .map(vec![HirScalarExpr::call_variadic(
                VariadicFunc::Coalesce,
                vec![
                    HirScalarExpr::column(left_col),
                    HirScalarExpr::column(right_col),
                ],
            )]);

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
    function: &Function<Aug>,
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

        // Strange special case for solitary table functions that output one
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
    Function {
        name,
        args,
        filter,
        over,
        distinct,
    }: &Function<Aug>,
    with_ordinality: bool,
    table_name: Option<FullItemName>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    assert_none!(filter, "cannot parse table function with FILTER");
    assert_none!(over, "cannot parse table function with OVER");
    assert!(!*distinct, "cannot parse table function with DISTINCT");

    let ecx = &ExprContext {
        qcx,
        name: "table function arguments",
        scope: &Scope::empty(),
        relation_type: &SqlRelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: true,
        allow_parameters: true,
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

    let table_name = match table_name {
        Some(table_name) => table_name.item,
        None => name.full_item_name().item.clone(),
    };

    let scope_name = Some(PartialItemName {
        database: None,
        schema: None,
        item: table_name,
    });

    let (expr, mut scope) = match resolve_func(ecx, name, args)? {
        Func::Table(impls) => {
            let tf = func::select_impl(ecx, FuncSpec::Func(name), impls, scalar_args, vec![])?;
            let scope = Scope::from_source(scope_name.clone(), tf.column_names);
            let expr = match tf.imp {
                TableFuncImpl::CallTable { mut func, exprs } => {
                    if with_ordinality {
                        func = TableFunc::with_ordinality(func.clone()).ok_or(
                            PlanError::Unsupported {
                                feature: format!("WITH ORDINALITY on {}", func),
                                discussion_no: None,
                            },
                        )?;
                    }
                    HirRelationExpr::CallTable { func, exprs }
                }
                TableFuncImpl::Expr(expr) => {
                    if !with_ordinality {
                        expr
                    } else {
                        // The table function is defined by a SQL query (i.e., TableFuncImpl::Expr),
                        // so we can't use the new `WITH ORDINALITY` implementation. We can fall
                        // back to the legacy implementation or error out the query.
                        if qcx
                            .scx
                            .is_feature_flag_enabled(&ENABLE_WITH_ORDINALITY_LEGACY_FALLBACK)
                        {
                            // Note that this can give an incorrect ordering, and also has an extreme
                            // performance problem in some cases. See the doc comment of
                            // `TableFuncImpl`.
                            tracing::error!(
                                %name,
                                "Using the legacy WITH ORDINALITY / ROWS FROM implementation for a table function",
                            );
                            expr.map(vec![HirScalarExpr::windowing(WindowExpr {
                                func: WindowExprType::Scalar(ScalarWindowExpr {
                                    func: ScalarWindowFunc::RowNumber,
                                    order_by: vec![],
                                }),
                                partition_by: vec![],
                                order_by: vec![],
                            })])
                        } else {
                            bail_unsupported!(format!(
                                "WITH ORDINALITY or ROWS FROM with {}",
                                name
                            ));
                        }
                    }
                }
            };
            (expr, scope)
        }
        Func::Scalar(impls) => {
            let expr = func::select_impl(ecx, FuncSpec::Func(name), impls, scalar_args, vec![])?;
            let output = expr.typ(
                &qcx.outer_relation_types,
                &SqlRelationType::new(vec![]),
                &qcx.scx.param_types.borrow(),
            );

            let relation = SqlRelationType::new(vec![output]);

            let function_ident = Ident::new(name.full_item_name().item.clone())?;
            let column_name = normalize::column_name(function_ident);
            let name = column_name.to_string();

            let scope = Scope::from_source(scope_name.clone(), vec![column_name]);

            let mut func = TableFunc::TabletizedScalar { relation, name };
            if with_ordinality {
                func = TableFunc::with_ordinality(func.clone()).ok_or(PlanError::Unsupported {
                    feature: format!("WITH ORDINALITY on {}", func),
                    discussion_no: None,
                })?;
            }
            (
                HirRelationExpr::CallTable {
                    func,
                    exprs: vec![expr],
                },
                scope,
            )
        }
        o => sql_bail!(
            "{} functions are not supported in functions in FROM",
            o.class()
        ),
    };

    if with_ordinality {
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
            item.table_name = if item.allow_unqualified_references {
                Some(PartialItemName {
                    database: None,
                    schema: None,
                    item: table_name.clone(),
                })
            } else {
                // Columns that prohibit unqualified references are special
                // columns from the output of a NATURAL or USING join that can
                // only be referenced by their full, pre-join name. Applying an
                // alias to the output of that join renders those columns
                // inaccessible, which we accomplish here by setting the
                // table name to `None`.
                //
                // Concretely, consider:
                //
                //      CREATE TABLE t1 (a int);
                //      CREATE TABLE t2 (a int);
                //  (1) SELECT ... FROM (t1 NATURAL JOIN t2);
                //  (2) SELECT ... FROM (t1 NATURAL JOIN t2) AS t;
                //
                // In (1), the join has no alias. The underlying columns from
                // either side of the join can be referenced as `t1.a` and
                // `t2.a`, respectively, and the unqualified name `a` refers to
                // a column whose value is `coalesce(t1.a, t2.a)`.
                //
                // In (2), the join is aliased as `t`. The columns from either
                // side of the join (`t1.a` and `t2.a`) are inaccessible, and
                // the coalesced column can be named as either `a` or `t.a`.
                //
                // We previously had a bug [0] that mishandled this subtle
                // logic.
                //
                // NOTE(benesch): We could in theory choose to project away
                // those inaccessible columns and drop them from the scope
                // entirely, but that would require that this function also
                // take and return the `HirRelationExpr` that is being aliased,
                // which is a rather large refactor.
                //
                // [0]: https://github.com/MaterializeInc/database-issues/issues/4887
                None
            };
            item.column_name = columns
                .get(i)
                .map(|a| normalize::column_name(a.clone()))
                .unwrap_or_else(|| item.column_name.clone());
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
    table_func_names: &BTreeMap<String, Ident>,
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
        table_func_names: &BTreeMap<String, Ident>,
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
                let (schema, item) = match &func.name {
                    ResolvedItemName::Item {
                        qualifiers,
                        full_name,
                        ..
                    } => (&qualifiers.schema_spec, full_name.item.clone()),
                    _ => unreachable!(),
                };

                if schema == &SchemaSpecifier::from(ecx.qcx.scx.catalog.get_mz_internal_schema_id())
                    || schema
                        == &SchemaSpecifier::from(ecx.qcx.scx.catalog.get_mz_unsafe_schema_id())
                {
                    None
                } else {
                    Some((item.into(), NameQuality::High))
                }
            }
            Expr::HomogenizingFunction { function, .. } => Some((
                function.to_string().to_lowercase().into(),
                NameQuality::High,
            )),
            Expr::NullIf { .. } => Some(("nullif".into(), NameQuality::High)),
            Expr::Array { .. } => Some(("array".into(), NameQuality::High)),
            Expr::List { .. } => Some(("list".into(), NameQuality::High)),
            Expr::Map { .. } | Expr::MapSubquery(_) => Some(("map".into(), NameQuality::High)),
            Expr::Cast { expr, data_type } => match invent(ecx, expr, table_func_names) {
                Some((name, NameQuality::High)) => Some((name, NameQuality::High)),
                _ => Some((data_type.unqualified_item_name().into(), NameQuality::Low)),
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
            Expr::Row { .. } => Some(("row".into(), NameQuality::High)),
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
    table_func_names: &BTreeMap<String, Ident>,
) -> Result<Vec<(ExpandedSelectItem<'a>, ColumnName)>, PlanError> {
    match s {
        SelectItem::Expr {
            expr: Expr::QualifiedWildcard(table_name),
            alias: _,
        } => {
            *ecx.qcx.scx.ambiguous_columns.borrow_mut() = true;
            let table_name =
                normalize::unresolved_item_name(UnresolvedItemName(table_name.clone()))?;
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
            *ecx.qcx.scx.ambiguous_columns.borrow_mut() = true;
            // A bit silly to have to plan the expression here just to get its
            // type, since we throw away the planned expression, but fixing this
            // requires a separate semantic analysis phase. Luckily this is an
            // uncommon operation and the PostgreSQL docs have a warning that
            // this operation is slow in Postgres too.
            let expr = plan_expr(ecx, sql_expr)?.type_as_any(ecx)?;
            let fields = match ecx.scalar_type(&expr) {
                SqlScalarType::Record { fields, .. } => fields,
                ty => sql_bail!(
                    "type {} is not composite",
                    ecx.humanize_scalar_type(&ty, false)
                ),
            };
            let mut skip_cols: BTreeSet<ColumnName> = BTreeSet::new();
            if let Expr::Identifier(ident) = sql_expr.as_ref() {
                if let [name] = ident.as_slice() {
                    if let Ok(items) = ecx.scope.items_from_table(
                        &[],
                        &PartialItemName {
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
                            field: name.clone().into(),
                        }));
                        Some((item, name.clone()))
                    }
                })
                .collect();
            Ok(items)
        }
        SelectItem::Wildcard => {
            *ecx.qcx.scx.ambiguous_columns.borrow_mut() = true;
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
                .or_else(|| invent_column_name(ecx, expr, table_func_names))
                .unwrap_or_else(|| UNKNOWN_COLUMN_NAME.into());
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
            // Per PostgreSQL (and apparently SQL:2008), we can't simply remove
            // these items from scope. These items need to *exist* because they
            // might shadow variables in outer scopes that would otherwise be
            // valid to reference, but accessing them needs to produce an error.
            item.error_if_referenced =
                Some(|table, column| PlanError::WrongJoinTypeForLateralColumn {
                    table: table.cloned(),
                    column: column.clone(),
                });
        }
    }
    let (right, right_scope) = plan_table_factor(&right_qcx, &join.relation)?;

    let (expr, scope) = match constraint {
        JoinConstraint::On(expr) => {
            let product_scope = left_scope.product(right_scope)?;
            let ecx = &ExprContext {
                qcx: left_qcx,
                name: "ON clause",
                scope: &product_scope,
                relation_type: &SqlRelationType::new(
                    left_qcx
                        .relation_type(&left)
                        .column_types
                        .into_iter()
                        .chain(right_qcx.relation_type(&right).column_types)
                        .collect(),
                ),
                allow_aggregates: false,
                allow_subqueries: true,
                allow_parameters: true,
                allow_windows: false,
            };
            let on = plan_expr(ecx, expr)?.type_as(ecx, &SqlScalarType::Bool)?;
            let joined = left.join(right, on, kind);
            (joined, product_scope)
        }
        JoinConstraint::Using { columns, alias } => {
            let column_names = columns
                .iter()
                .map(|ident| normalize::column_name(ident.clone()))
                .collect::<Vec<_>>();

            plan_using_constraint(
                &column_names,
                left_qcx,
                left,
                left_scope,
                &right_qcx,
                right,
                right_scope,
                kind,
                alias.as_ref(),
            )?
        }
        JoinConstraint::Natural => {
            // We shouldn't need to set ambiguous_columns on both the right and left qcx since they
            // have the same scx. However, it doesn't hurt to be safe.
            *left_qcx.scx.ambiguous_columns.borrow_mut() = true;
            *right_qcx.scx.ambiguous_columns.borrow_mut() = true;
            let left_column_names = left_scope.column_names();
            let right_column_names: BTreeSet<_> = right_scope.column_names().collect();
            let column_names: Vec<_> = left_column_names
                .filter(|col| right_column_names.contains(col))
                .cloned()
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
                None,
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
    alias: Option<&Ident>,
) -> Result<(HirRelationExpr, Scope), PlanError> {
    let mut both_scope = left_scope.clone().product(right_scope.clone())?;

    // Cargo culting PG here; no discernable reason this must fail, but PG does
    // so we do, as well.
    let mut unique_column_names = BTreeSet::new();
    for c in column_names {
        if !unique_column_names.insert(c) {
            return Err(PlanError::Unsupported {
                feature: format!(
                    "column name {} appears more than once in USING clause",
                    c.quoted()
                ),
                discussion_no: None,
            });
        }
    }

    let alias_item_name = alias.map(|alias| PartialItemName {
        database: None,
        schema: None,
        item: alias.clone().to_string(),
    });

    if let Some(alias_item_name) = &alias_item_name {
        for partial_item_name in both_scope.table_names() {
            if partial_item_name.matches(alias_item_name) {
                sql_bail!(
                    "table name \"{}\" specified more than once",
                    alias_item_name
                )
            }
        }
    }

    let ecx = &ExprContext {
        qcx: right_qcx,
        name: "USING clause",
        scope: &both_scope,
        relation_type: &SqlRelationType::new(
            left_qcx
                .relation_type(&left)
                .column_types
                .into_iter()
                .chain(right_qcx.relation_type(&right).column_types)
                .collect(),
        ),
        allow_aggregates: false,
        allow_subqueries: false,
        allow_parameters: false,
        allow_windows: false,
    };

    let mut join_exprs = vec![];
    let mut map_exprs = vec![];
    let mut new_items = vec![];
    let mut join_cols = vec![];
    let mut hidden_cols = vec![];

    for column_name in column_names {
        // the two sides will have different names (e.g., `t1.a` and `t2.a`)
        let (lhs, lhs_name) = left_scope.resolve_using_column(
            column_name,
            JoinSide::Left,
            &mut left_qcx.name_manager.borrow_mut(),
        )?;
        let (mut rhs, rhs_name) = right_scope.resolve_using_column(
            column_name,
            JoinSide::Right,
            &mut right_qcx.name_manager.borrow_mut(),
        )?;

        // Adjust the RHS reference to its post-join location.
        rhs.column += left_scope.len();

        // Join keys must be resolved to same type.
        let mut exprs = coerce_homogeneous_exprs(
            &ecx.with_name(&format!(
                "NATURAL/USING join column {}",
                column_name.quoted()
            )),
            vec![
                CoercibleScalarExpr::Coerced(HirScalarExpr::named_column(
                    lhs,
                    Arc::clone(&lhs_name),
                )),
                CoercibleScalarExpr::Coerced(HirScalarExpr::named_column(
                    rhs,
                    Arc::clone(&rhs_name),
                )),
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
                map_exprs.push(HirScalarExpr::call_variadic(
                    VariadicFunc::Coalesce,
                    vec![expr1.clone(), expr2.clone()],
                ));
                new_items.push(ScopeItem::from_column_name(column_name));
            }
        }

        // If a `join_using_alias` is present, add a new scope item that accepts
        // only table-qualified references for each specified join column.
        // Unlike regular table aliases, a `join_using_alias` should not hide the
        // names of the joined relations.
        if alias_item_name.is_some() {
            let new_item_col = both_scope.items.len() + new_items.len();
            join_cols.push(new_item_col);
            hidden_cols.push(new_item_col);

            new_items.push(ScopeItem::from_name(
                alias_item_name.clone(),
                column_name.clone().to_string(),
            ));

            // Should be safe to use either `lhs` or `rhs` here since the column
            // is available in both scopes and must have the same type of the new item.
            // We (arbitrarily) choose the left name.
            map_exprs.push(HirScalarExpr::named_column(lhs, Arc::clone(&lhs_name)));
        }

        join_exprs.push(expr1.call_binary(expr2, expr_func::Eq));
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

    let on = HirScalarExpr::variadic_and(join_exprs);

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
    if let Some((i, item)) = ecx.scope.resolve_expr(e) {
        // We've already calculated this expression.
        return Ok(HirScalarExpr::named_column(
            i,
            ecx.qcx.name_manager.borrow_mut().intern_scope_item(item),
        )
        .into());
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
        Expr::Map(exprs) => plan_map(ecx, exprs, None),
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
        } => Ok(plan_is_expr(ecx, expr, construct, *negated)?.into()),
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

        Expr::InList {
            expr,
            list,
            negated,
        } => plan_in_list(ecx, expr, list, negated),

        // Subqueries.
        Expr::Exists(query) => plan_exists(ecx, query),
        Expr::Subquery(query) => plan_subquery(ecx, query),
        Expr::ListSubquery(query) => plan_list_subquery(ecx, query),
        Expr::MapSubquery(query) => plan_map_subquery(ecx, query),
        Expr::ArraySubquery(query) => plan_array_subquery(ecx, query),
        Expr::Collate { expr, collation } => plan_collate(ecx, expr, collation),
        Expr::Nested(_) => unreachable!("Expr::Nested not desugared"),
        Expr::InSubquery { .. } => unreachable!("Expr::InSubquery not desugared"),
        Expr::AnyExpr { .. } => unreachable!("Expr::AnyExpr not desugared"),
        Expr::AllExpr { .. } => unreachable!("Expr::AllExpr not desugared"),
        Expr::AnySubquery { .. } => unreachable!("Expr::AnySubquery not desugared"),
        Expr::AllSubquery { .. } => unreachable!("Expr::AllSubquery not desugared"),
        Expr::Between { .. } => unreachable!("Expr::Between not desugared"),
    }
}

fn plan_parameter(ecx: &ExprContext, n: usize) -> Result<CoercibleScalarExpr, PlanError> {
    if !ecx.allow_parameters {
        // It might be clearer to return an error like "cannot use parameter
        // here", but this is how PostgreSQL does it, and so for now we follow
        // PostgreSQL.
        return Err(PlanError::UnknownParameter(n));
    }
    if n == 0 || n > 65536 {
        return Err(PlanError::UnknownParameter(n));
    }
    if ecx.param_types().borrow().contains_key(&n) {
        Ok(HirScalarExpr::parameter(n).into())
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
        // Special case a direct cast of an ARRAY, LIST, or MAP expression so
        // we can pass in the target type as a type hint. This is
        // a limited form of the coercion that we do for string literals
        // via CoercibleScalarExpr. We used to let CoercibleScalarExpr
        // handle ARRAY/LIST/MAP coercion too, but doing so causes
        // PostgreSQL compatibility trouble.
        //
        // See: https://github.com/postgres/postgres/blob/31f403e95/src/backend/parser/parse_expr.c#L2762-L2768
        Expr::Array(exprs) => plan_array(ecx, exprs, Some(&to_scalar_type))?,
        Expr::List(exprs) => plan_list(ecx, exprs, Some(&to_scalar_type))?,
        Expr::Map(exprs) => plan_map(ecx, exprs, Some(&to_scalar_type))?,
        _ => plan_expr(ecx, expr)?,
    };
    let ecx = &ecx.with_name("CAST");
    let expr = typeconv::plan_coerce(ecx, expr, &to_scalar_type)?;
    let expr = typeconv::plan_cast(ecx, CastContext::Explicit, expr, &to_scalar_type)?;
    Ok(expr.into())
}

fn plan_not(ecx: &ExprContext, expr: &Expr<Aug>) -> Result<CoercibleScalarExpr, PlanError> {
    let ecx = ecx.with_name("NOT argument");
    Ok(plan_expr(&ecx, expr)?
        .type_as(&ecx, &SqlScalarType::Bool)?
        .call_unary(UnaryFunc::Not(expr_func::Not))
        .into())
}

fn plan_and(
    ecx: &ExprContext,
    left: &Expr<Aug>,
    right: &Expr<Aug>,
) -> Result<CoercibleScalarExpr, PlanError> {
    let ecx = ecx.with_name("AND argument");
    Ok(HirScalarExpr::variadic_and(vec![
        plan_expr(&ecx, left)?.type_as(&ecx, &SqlScalarType::Bool)?,
        plan_expr(&ecx, right)?.type_as(&ecx, &SqlScalarType::Bool)?,
    ])
    .into())
}

fn plan_or(
    ecx: &ExprContext,
    left: &Expr<Aug>,
    right: &Expr<Aug>,
) -> Result<CoercibleScalarExpr, PlanError> {
    let ecx = ecx.with_name("OR argument");
    Ok(HirScalarExpr::variadic_or(vec![
        plan_expr(&ecx, left)?.type_as(&ecx, &SqlScalarType::Bool)?,
        plan_expr(&ecx, right)?.type_as(&ecx, &SqlScalarType::Bool)?,
    ])
    .into())
}

fn plan_in_list(
    ecx: &ExprContext,
    lhs: &Expr<Aug>,
    list: &Vec<Expr<Aug>>,
    negated: &bool,
) -> Result<CoercibleScalarExpr, PlanError> {
    let ecx = ecx.with_name("IN list");
    let or = HirScalarExpr::variadic_or(
        list.into_iter()
            .map(|e| {
                let eq = lhs.clone().equals(e.clone());
                plan_expr(&ecx, &eq)?.type_as(&ecx, &SqlScalarType::Bool)
            })
            .collect::<Result<Vec<HirScalarExpr>, PlanError>>()?,
    );
    Ok(if *negated {
        or.call_unary(UnaryFunc::Not(expr_func::Not))
    } else {
        or
    }
    .into())
}

fn plan_homogenizing_function(
    ecx: &ExprContext,
    function: &HomogenizingFunction,
    exprs: &[Expr<Aug>],
) -> Result<CoercibleScalarExpr, PlanError> {
    assert!(!exprs.is_empty()); // `COALESCE()` is a syntax error
    let expr = HirScalarExpr::call_variadic(
        match function {
            HomogenizingFunction::Coalesce => VariadicFunc::Coalesce,
            HomogenizingFunction::Greatest => VariadicFunc::Greatest,
            HomogenizingFunction::Least => VariadicFunc::Least,
        },
        coerce_homogeneous_exprs(
            &ecx.with_name(&function.to_string().to_lowercase()),
            plan_exprs(ecx, exprs)?,
            None,
        )?,
    );
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
        SqlScalarType::Record { fields, .. } => {
            fields.iter().position(|(name, _ty)| *name == field)
        }
        ty => sql_bail!(
            "column notation applied to type {}, which is not a composite type",
            ecx.humanize_scalar_type(ty, false)
        ),
    };
    match i {
        None => sql_bail!(
            "field {} not found in data type {}",
            field,
            ecx.humanize_scalar_type(&ty, false)
        ),
        Some(i) => Ok(expr
            .call_unary(UnaryFunc::RecordGet(expr_func::RecordGet(i)))
            .into()),
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
        SqlScalarType::Array(..) | SqlScalarType::Int2Vector => plan_subscript_array(
            ecx,
            expr,
            positions,
            // Int2Vector uses 0-based indexing, while arrays use 1-based indexing, so we need to
            // adjust all Int2Vector subscript operations by 1 (both w/r/t input and the values we
            // track in its backing data).
            if ty == SqlScalarType::Int2Vector {
                1
            } else {
                0
            },
        ),
        SqlScalarType::Jsonb => plan_subscript_jsonb(ecx, expr, positions),
        SqlScalarType::List { element_type, .. } => {
            // `elem_type_name` is used only in error msgs, so we set `postgres_compat` to false.
            let elem_type_name = ecx.humanize_scalar_type(element_type, false);
            let n_layers = ty.unwrap_list_n_layers();
            plan_subscript_list(ecx, expr, positions, n_layers, &elem_type_name)
        }
        ty => sql_bail!(
            "cannot subscript type {}",
            ecx.humanize_scalar_type(ty, false)
        ),
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
    offset: i64,
) -> Result<CoercibleScalarExpr, PlanError> {
    let mut exprs = Vec::with_capacity(positions.len() + 1);
    exprs.push(expr);

    // Subscripting arrays doesn't yet support slicing, so we always want to
    // extract scalars or error.
    let indexes = extract_scalar_subscript_from_positions(positions, "array")?;

    for i in indexes {
        exprs.push(plan_expr(ecx, i)?.cast_to(
            ecx,
            CastContext::Explicit,
            &SqlScalarType::Int64,
        )?);
    }

    Ok(HirScalarExpr::call_variadic(VariadicFunc::ArrayIndex { offset }, exprs).into())
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
                elem_type_name,
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
                elem_type_name,
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
        exprs.push(plan_expr(ecx, i)?.cast_to(
            ecx,
            CastContext::Explicit,
            &SqlScalarType::Int64,
        )?);
    }

    Ok((
        n_layers - depth,
        HirScalarExpr::call_variadic(VariadicFunc::ListIndex, exprs),
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
                plan_expr(ecx, p)?.cast_to(ecx, CastContext::Explicit, &SqlScalarType::Int64)?
            }
            None => HirScalarExpr::literal(Datum::Int64(default), SqlScalarType::Int64),
        })
    };
    for p in slices {
        let start = extract_position_or_default(p.start.as_ref(), 1)?;
        let end = extract_position_or_default(p.end.as_ref(), i64::MAX - 1)?;
        exprs.push(start);
        exprs.push(end);
    }

    Ok(HirScalarExpr::call_variadic(
        VariadicFunc::ListSliceLinear,
        exprs,
    ))
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
        CoercibleScalarType::Coerced(ref ty @ SqlScalarType::Char { length }) => expr
            .type_as(&ecx, ty)?
            .call_unary(UnaryFunc::PadChar(expr_func::PadChar { length })),
        _ => expr.cast_to(&ecx, Implicit, &SqlScalarType::String)?,
    };
    let mut pattern = plan_expr(&ecx, pattern)?.cast_to(&ecx, Implicit, &SqlScalarType::String)?;
    if let Some(escape) = escape {
        pattern = pattern.call_binary(
            plan_expr(&ecx, escape)?.cast_to(&ecx, Implicit, &SqlScalarType::String)?,
            expr_func::LikeEscape,
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
    use SqlScalarType::{Int64, String};

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
        HirScalarExpr::call_variadic(
            VariadicFunc::ArrayCreate {
                elem_type: SqlScalarType::String,
            },
            exprs,
        ),
        BinaryFunc::JsonbGetPath,
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
        expr_func::ListListConcat.into(),
        |elem_type| {
            HirScalarExpr::literal(
                Datum::empty_list(),
                SqlScalarType::List {
                    element_type: Box::new(elem_type),
                    custom_id: None,
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
    plan_vector_like_subquery(
        ecx,
        query,
        |elem_type| {
            matches!(
                elem_type,
                SqlScalarType::Char { .. }
                    | SqlScalarType::Array { .. }
                    | SqlScalarType::List { .. }
                    | SqlScalarType::Map { .. }
            )
        },
        |elem_type| VariadicFunc::ArrayCreate { elem_type },
        |order_by| AggregateFunc::ArrayConcat { order_by },
        expr_func::ArrayArrayConcat.into(),
        |elem_type| {
            HirScalarExpr::literal(
                Datum::empty_array(),
                SqlScalarType::Array(Box::new(elem_type)),
            )
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
    F1: Fn(&SqlScalarType) -> bool,
    F2: Fn(SqlScalarType) -> VariadicFunc,
    F3: Fn(Vec<ColumnOrder>) -> AggregateFunc,
    F4: Fn(SqlScalarType) -> HirScalarExpr,
{
    if !ecx.allow_subqueries {
        sql_bail!("{} does not allow subqueries", ecx.name)
    }

    let mut qcx = ecx.derived_query_context();
    let mut planned_query = plan_query(&mut qcx, query)?;
    if planned_query.limit.is_some()
        || !planned_query
            .offset
            .clone()
            .try_into_literal_int64()
            .is_ok_and(|offset| offset == 0)
    {
        planned_query.expr = HirRelationExpr::top_k(
            planned_query.expr,
            vec![],
            planned_query.order_by.clone(),
            planned_query.limit,
            planned_query.offset,
            planned_query.group_size_hints.limit_input_group_size,
        );
    }

    if planned_query.project.len() != 1 {
        sql_bail!(
            "Expected subselect to return 1 column, got {} columns",
            planned_query.project.len()
        );
    }

    let project_column = *planned_query.project.get(0).unwrap();
    let elem_type = qcx
        .relation_type(&planned_query.expr)
        .column_types
        .get(project_column)
        .cloned()
        .unwrap()
        .scalar_type();

    if is_unsupported_type(&elem_type) {
        bail_unsupported!(format!(
            "cannot build array from subquery because return type {}{}",
            ecx.humanize_scalar_type(&elem_type, false),
            vector_type_string
        ));
    }

    // `ColumnRef`s in `aggregation_exprs` refers to the columns produced by planning the
    // subquery above.
    let aggregation_exprs: Vec<_> = iter::once(HirScalarExpr::call_variadic(
        vector_create(elem_type.clone()),
        vec![HirScalarExpr::column(project_column)],
    ))
    .chain(
        planned_query
            .order_by
            .iter()
            .map(|co| HirScalarExpr::column(co.column)),
    )
    .collect();

    // However, column references for `aggregation_projection` and `aggregation_order_by`
    // are with reference to the `exprs` of the aggregation expression.  Here that is
    // `aggregation_exprs`.
    let aggregation_projection = vec![0];
    let aggregation_order_by = planned_query
        .order_by
        .into_iter()
        .enumerate()
        .map(|(i, order)| ColumnOrder { column: i, ..order })
        .collect();

    let reduced_expr = planned_query
        .expr
        .reduce(
            vec![],
            vec![AggregateExpr {
                func: aggregate_concat(aggregation_order_by),
                expr: Box::new(HirScalarExpr::call_variadic(
                    VariadicFunc::RecordCreate {
                        field_names: iter::repeat(ColumnName::from(""))
                            .take(aggregation_exprs.len())
                            .collect(),
                    },
                    aggregation_exprs,
                )),
                distinct: false,
            }],
            None,
        )
        .project(aggregation_projection);

    // If `expr` has no rows, return an empty array/list rather than NULL.
    Ok(reduced_expr
        .select()
        .call_binary(empty_literal(elem_type), binary_concat)
        .into())
}

fn plan_map_subquery(
    ecx: &ExprContext,
    query: &Query<Aug>,
) -> Result<CoercibleScalarExpr, PlanError> {
    if !ecx.allow_subqueries {
        sql_bail!("{} does not allow subqueries", ecx.name)
    }

    let mut qcx = ecx.derived_query_context();
    let mut query = plan_query(&mut qcx, query)?;
    if query.limit.is_some()
        || !query
            .offset
            .clone()
            .try_into_literal_int64()
            .is_ok_and(|offset| offset == 0)
    {
        query.expr = HirRelationExpr::top_k(
            query.expr,
            vec![],
            query.order_by.clone(),
            query.limit,
            query.offset,
            query.group_size_hints.limit_input_group_size,
        );
    }
    if query.project.len() != 2 {
        sql_bail!(
            "expected map subquery to return 2 columns, got {} columns",
            query.project.len()
        );
    }

    let query_types = qcx.relation_type(&query.expr).column_types;
    let key_column = query.project[0];
    let key_type = query_types[key_column].clone().scalar_type();
    let value_column = query.project[1];
    let value_type = query_types[value_column].clone().scalar_type();

    if key_type != SqlScalarType::String {
        sql_bail!("cannot build map from subquery because first column is not of type text");
    }

    let aggregation_exprs: Vec<_> = iter::once(HirScalarExpr::call_variadic(
        VariadicFunc::RecordCreate {
            field_names: vec![ColumnName::from("key"), ColumnName::from("value")],
        },
        vec![
            HirScalarExpr::column(key_column),
            HirScalarExpr::column(value_column),
        ],
    ))
    .chain(
        query
            .order_by
            .iter()
            .map(|co| HirScalarExpr::column(co.column)),
    )
    .collect();

    let expr = query
        .expr
        .reduce(
            vec![],
            vec![AggregateExpr {
                func: AggregateFunc::MapAgg {
                    order_by: query
                        .order_by
                        .into_iter()
                        .enumerate()
                        .map(|(i, order)| ColumnOrder { column: i, ..order })
                        .collect(),
                    value_type: value_type.clone(),
                },
                expr: Box::new(HirScalarExpr::call_variadic(
                    VariadicFunc::RecordCreate {
                        field_names: iter::repeat(ColumnName::from(""))
                            .take(aggregation_exprs.len())
                            .collect(),
                    },
                    aggregation_exprs,
                )),
                distinct: false,
            }],
            None,
        )
        .project(vec![0]);

    // If `expr` has no rows, return an empty map rather than NULL.
    let expr = HirScalarExpr::call_variadic(
        VariadicFunc::Coalesce,
        vec![
            expr.select(),
            HirScalarExpr::literal(
                Datum::empty_map(),
                SqlScalarType::Map {
                    value_type: Box::new(value_type),
                    custom_id: None,
                },
            ),
        ],
    );

    Ok(expr.into())
}

fn plan_collate(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    collation: &UnresolvedItemName,
) -> Result<CoercibleScalarExpr, PlanError> {
    if collation.0.len() == 2
        && collation.0[0] == ident!(mz_repr::namespaces::PG_CATALOG_SCHEMA)
        && collation.0[1] == ident!("default")
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
    type_hint: Option<&SqlScalarType>,
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
        Some(SqlScalarType::Array(elem_type)) => {
            let multidimensional = out.iter().any(|e| {
                matches!(
                    ecx.scalar_type(e),
                    CoercibleScalarType::Coerced(SqlScalarType::Array(_))
                )
            });
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
    // https://github.com/MaterializeInc/database-issues/issues/2360.
    //
    // Arrays of `list` and `map` types are disallowed due to mind-bending
    // semantics.
    if matches!(
        elem_type,
        SqlScalarType::Char { .. } | SqlScalarType::List { .. } | SqlScalarType::Map { .. }
    ) {
        bail_unsupported!(format!("{}[]", ecx.humanize_scalar_type(&elem_type, false)));
    }

    Ok(HirScalarExpr::call_variadic(VariadicFunc::ArrayCreate { elem_type }, exprs).into())
}

fn plan_list(
    ecx: &ExprContext,
    exprs: &[Expr<Aug>],
    type_hint: Option<&SqlScalarType>,
) -> Result<CoercibleScalarExpr, PlanError> {
    let (elem_type, exprs) = if exprs.is_empty() {
        if let Some(SqlScalarType::List { element_type, .. }) = type_hint {
            (element_type.without_modifiers(), vec![])
        } else {
            sql_bail!("cannot determine type of empty list");
        }
    } else {
        let type_hint = match type_hint {
            Some(SqlScalarType::List { element_type, .. }) => Some(&**element_type),
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

    if matches!(elem_type, SqlScalarType::Char { .. }) {
        bail_unsupported!("char list");
    }

    Ok(HirScalarExpr::call_variadic(VariadicFunc::ListCreate { elem_type }, exprs).into())
}

fn plan_map(
    ecx: &ExprContext,
    entries: &[MapEntry<Aug>],
    type_hint: Option<&SqlScalarType>,
) -> Result<CoercibleScalarExpr, PlanError> {
    let (value_type, exprs) = if entries.is_empty() {
        if let Some(SqlScalarType::Map { value_type, .. }) = type_hint {
            (value_type.without_modifiers(), vec![])
        } else {
            sql_bail!("cannot determine type of empty map");
        }
    } else {
        let type_hint = match type_hint {
            Some(SqlScalarType::Map { value_type, .. }) => Some(&**value_type),
            _ => None,
        };

        let mut keys = vec![];
        let mut values = vec![];
        for MapEntry { key, value } in entries {
            let key = plan_expr(ecx, key)?.type_as(ecx, &SqlScalarType::String)?;
            let value = match value {
                // Special case nested MAP expressions so we can plumb
                // the type hint through.
                Expr::Map(entries) => plan_map(ecx, entries, type_hint)?,
                _ => plan_expr(ecx, value)?,
            };
            keys.push(key);
            values.push(value);
        }
        let values = coerce_homogeneous_exprs(&ecx.with_name("MAP"), values, type_hint)?;
        let value_type = ecx.scalar_type(&values[0]).without_modifiers();
        let out = itertools::interleave(keys, values).collect();
        (value_type, out)
    };

    if matches!(value_type, SqlScalarType::Char { .. }) {
        bail_unsupported!("char map");
    }

    let expr = HirScalarExpr::call_variadic(VariadicFunc::MapBuild { value_type }, exprs);
    Ok(expr.into())
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
    force_type: Option<&SqlScalarType>,
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
        let arg = typeconv::plan_coerce(ecx, expr, target)?;
        let ccx = match force_type {
            None => CastContext::Implicit,
            Some(_) => CastContext::Explicit,
        };
        match typeconv::plan_cast(ecx, ccx, arg.clone(), target) {
            Ok(expr) => out.push(expr),
            Err(_) => sql_bail!(
                "{} could not convert type {} to {}",
                ecx.name,
                ecx.humanize_scalar_type(&ecx.scalar_type(&arg), false),
                ecx.humanize_scalar_type(target, false),
            ),
        }
    }
    Ok(out)
}

/// Creates a `ColumnOrder` from an `OrderByExpr` and column index.
/// Column index is specified by the caller, but `desc` and `nulls_last` is figured out here.
pub(crate) fn resolve_desc_and_nulls_last<T: AstInfo>(
    obe: &OrderByExpr<T>,
    column: usize,
) -> ColumnOrder {
    let desc = !obe.asc.unwrap_or(true);
    ColumnOrder {
        column,
        desc,
        // https://www.postgresql.org/docs/14/queries-order.html
        //   "NULLS FIRST is the default for DESC order, and NULLS LAST otherwise"
        nulls_last: obe.nulls_last.unwrap_or(!desc),
    }
}

/// Plans the ORDER BY clause of a window function.
///
/// Unfortunately, we have to create two HIR structs from an AST OrderByExpr:
/// A ColumnOrder has asc/desc and nulls first/last, but can't represent an HirScalarExpr, just
/// a column reference by index. Therefore, we return both HirScalarExprs and ColumnOrders.
/// Note that the column references in the ColumnOrders point NOT to input columns, but into the
/// `Vec<HirScalarExpr>` that we return.
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
            col_orders.push(resolve_desc_and_nulls_last(obe, i));
        }
    }
    Ok((order_by_exprs, col_orders))
}

/// Common part of the planning of windowed and non-windowed aggregation functions.
fn plan_aggregate_common(
    ecx: &ExprContext,
    Function::<Aug> {
        name,
        args,
        filter,
        over: _,
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
    // prevents the projection into Record below from triggering on unsupported
    // functions.

    let impls = match resolve_func(ecx, name, args)? {
        Func::Aggregate(impls) => impls,
        _ => unreachable!("plan_aggregate_common called on non-aggregate function,"),
    };

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
                    ecx.qcx
                        .scx
                        .humanize_resolved_name(name)
                        .expect("name actually resolved")
                );
            }
            let args = plan_exprs(ecx, args)?;
            (args, order_by.clone())
        }
    };

    let (order_by_exprs, col_orders) = plan_function_order_by(ecx, &order_by)?;

    let (mut expr, func) = func::select_impl(ecx, FuncSpec::Func(name), impls, args, col_orders)?;
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
        let cond =
            plan_expr(&ecx.with_name("FILTER"), filter)?.type_as(ecx, &SqlScalarType::Bool)?;
        let expr_typ = ecx.scalar_type(&expr);
        expr = HirScalarExpr::if_then_else(
            cond,
            expr,
            HirScalarExpr::literal(func.identity_datum(), expr_typ),
        );
    }

    let mut seen_outer = false;
    let mut seen_inner = false;
    #[allow(deprecated)]
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
        expr = HirScalarExpr::call_variadic(VariadicFunc::RecordCreate { field_names }, exprs);
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
        let table_name = normalize::unresolved_item_name(UnresolvedItemName(names))?;
        let (i, i_name) = ecx.scope.resolve_table_column(
            &ecx.qcx.outer_scopes,
            &table_name,
            &col_name,
            &mut ecx.qcx.name_manager.borrow_mut(),
        )?;
        return Ok(HirScalarExpr::named_column(i, i_name));
    }

    // If the name is unqualified, first check if it refers to a column. Track any similar names
    // that might exist for a better error message.
    let similar_names = match ecx.scope.resolve_column(
        &ecx.qcx.outer_scopes,
        &col_name,
        &mut ecx.qcx.name_manager.borrow_mut(),
    ) {
        Ok((i, i_name)) => {
            return Ok(HirScalarExpr::named_column(i, i_name));
        }
        Err(PlanError::UnknownColumn { similar, .. }) => similar,
        Err(e) => return Err(e),
    };

    // The name doesn't refer to a column. Check if it is a whole-row reference
    // to a table.
    let items = ecx.scope.items_from_table(
        &ecx.qcx.outer_scopes,
        &PartialItemName {
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
            similar: similar_names,
        }),
        // The name refers to a table that is the result of a function that
        // returned a single column. Per PostgreSQL, this is a special case
        // that returns the value directly.
        // See: https://github.com/postgres/postgres/blob/22592e10b/src/backend/parser/parse_expr.c#L2519-L2524
        [(column, item)] if item.from_single_column_function => Ok(HirScalarExpr::named_column(
            *column,
            ecx.qcx.name_manager.borrow_mut().intern_scope_item(item),
        )),
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
                        let expr = HirScalarExpr::named_column(
                            column,
                            ecx.qcx.name_manager.borrow_mut().intern_scope_item(item),
                        );
                        let name = item.column_name.clone();
                        Some((expr, name))
                    }
                })
                .unzip();
            // For the special case of a table function with a single column, the single column is instead not wrapped.
            let expr = if exprs.len() == 1 && has_exists_column.is_some() {
                exprs.into_element()
            } else {
                HirScalarExpr::call_variadic(VariadicFunc::RecordCreate { field_names }, exprs)
            };
            if let Some(has_exists_column) = has_exists_column {
                Ok(HirScalarExpr::if_then_else(
                    HirScalarExpr::unnamed_column(has_exists_column)
                        .call_unary(UnaryFunc::IsNull(mz_expr::func::IsNull)),
                    HirScalarExpr::literal_null(ecx.scalar_type(&expr)),
                    expr,
                ))
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
    f @ Function {
        name,
        args,
        filter,
        over,
        distinct,
    }: &'a Function<Aug>,
) -> Result<HirScalarExpr, PlanError> {
    let impls = match resolve_func(ecx, name, args)? {
        Func::Table(_) => {
            sql_bail!(
                "table functions are not allowed in {} (function {})",
                ecx.name,
                name
            );
        }
        Func::Scalar(impls) => {
            if over.is_some() {
                sql_bail!(
                    "OVER clause not allowed on {name}. The OVER clause can only be used with window functions (including aggregations)."
                );
            }
            impls
        }
        Func::ScalarWindow(impls) => {
            let (
                ignore_nulls,
                order_by_exprs,
                col_orders,
                _window_frame,
                partition_by,
                scalar_args,
            ) = plan_window_function_non_aggr(ecx, f)?;

            // All scalar window functions have 0 parameters. Let's print a nice error msg if the
            // user gave some args. (The below `func::select_impl` would fail anyway, but the error
            // msg there is less informative.)
            if !scalar_args.is_empty() {
                if let ResolvedItemName::Item {
                    full_name: FullItemName { item, .. },
                    ..
                } = name
                {
                    sql_bail!(
                        "function {} has 0 parameters, but was called with {}",
                        item,
                        scalar_args.len()
                    );
                }
            }

            // Note: the window frame doesn't affect scalar window funcs, but, strangely, we should
            // accept a window frame here without an error msg. (Postgres also does this.)
            // TODO: maybe we should give a notice

            let func = func::select_impl(ecx, FuncSpec::Func(name), impls, scalar_args, vec![])?;

            if ignore_nulls {
                // If we ever add a scalar window function that supports ignore, then don't forget
                // to also update HIR EXPLAIN.
                bail_unsupported!(IGNORE_NULLS_ERROR_MSG);
            }

            return Ok(HirScalarExpr::windowing(WindowExpr {
                func: WindowExprType::Scalar(ScalarWindowExpr {
                    func,
                    order_by: col_orders,
                }),
                partition_by,
                order_by: order_by_exprs,
            }));
        }
        Func::ValueWindow(impls) => {
            let (ignore_nulls, order_by_exprs, col_orders, window_frame, partition_by, scalar_args) =
                plan_window_function_non_aggr(ecx, f)?;

            let (args_encoded, func) =
                func::select_impl(ecx, FuncSpec::Func(name), impls, scalar_args, vec![])?;

            if ignore_nulls {
                match func {
                    ValueWindowFunc::Lag | ValueWindowFunc::Lead => {}
                    _ => bail_unsupported!(IGNORE_NULLS_ERROR_MSG),
                }
            }

            return Ok(HirScalarExpr::windowing(WindowExpr {
                func: WindowExprType::Value(ValueWindowExpr {
                    func,
                    args: Box::new(args_encoded),
                    order_by: col_orders,
                    window_frame,
                    ignore_nulls, // (RESPECT NULLS is the default)
                }),
                partition_by,
                order_by: order_by_exprs,
            }));
        }
        Func::Aggregate(_) => {
            if f.over.is_none() {
                // Not a window aggregate. Something is wrong.
                if ecx.allow_aggregates {
                    // Should already have been caught by `scope.resolve_expr` in `plan_expr_inner`
                    // (after having been planned earlier in `Step 5` of `plan_select_from_where`).
                    sql_bail!(
                        "Internal error: encountered unplanned non-windowed aggregate function: {:?}",
                        name,
                    );
                } else {
                    // scope.resolve_expr didn't catch it because we have not yet planned it,
                    // because it was in an unsupported context.
                    sql_bail!(
                        "aggregate functions are not allowed in {} (function {})",
                        ecx.name,
                        name
                    );
                }
            } else {
                let (ignore_nulls, order_by_exprs, col_orders, window_frame, partition_by) =
                    plan_window_function_common(ecx, &f.name, &f.over)?;

                // https://github.com/MaterializeInc/database-issues/issues/6720
                match (&window_frame.start_bound, &window_frame.end_bound) {
                    (
                        mz_expr::WindowFrameBound::UnboundedPreceding,
                        mz_expr::WindowFrameBound::OffsetPreceding(..),
                    )
                    | (
                        mz_expr::WindowFrameBound::UnboundedPreceding,
                        mz_expr::WindowFrameBound::OffsetFollowing(..),
                    )
                    | (
                        mz_expr::WindowFrameBound::OffsetPreceding(..),
                        mz_expr::WindowFrameBound::UnboundedFollowing,
                    )
                    | (
                        mz_expr::WindowFrameBound::OffsetFollowing(..),
                        mz_expr::WindowFrameBound::UnboundedFollowing,
                    ) => bail_unsupported!("mixed unbounded - offset frames"),
                    (_, _) => {} // other cases are ok
                }

                if ignore_nulls {
                    // https://github.com/MaterializeInc/database-issues/issues/6722
                    // If we ever add support for ignore_nulls for a window aggregate, then don't
                    // forget to also update HIR EXPLAIN.
                    bail_unsupported!(IGNORE_NULLS_ERROR_MSG);
                }

                let aggregate_expr = plan_aggregate_common(ecx, f)?;

                if aggregate_expr.distinct {
                    // https://github.com/MaterializeInc/database-issues/issues/6626
                    bail_unsupported!("DISTINCT in window aggregates");
                }

                return Ok(HirScalarExpr::windowing(WindowExpr {
                    func: WindowExprType::Aggregate(AggregateWindowExpr {
                        aggregate_expr,
                        order_by: col_orders,
                        window_frame,
                    }),
                    partition_by,
                    order_by: order_by_exprs,
                }));
            }
        }
    };

    if over.is_some() {
        unreachable!("If there is an OVER clause, we should have returned already above.");
    }

    if *distinct {
        sql_bail!(
            "DISTINCT specified, but {} is not an aggregate function",
            ecx.qcx
                .scx
                .humanize_resolved_name(name)
                .expect("already resolved")
        );
    }
    if filter.is_some() {
        sql_bail!(
            "FILTER specified, but {} is not an aggregate function",
            ecx.qcx
                .scx
                .humanize_resolved_name(name)
                .expect("already resolved")
        );
    }

    let scalar_args = match &args {
        FunctionArgs::Star => {
            sql_bail!(
                "* argument is invalid with non-aggregate function {}",
                ecx.qcx
                    .scx
                    .humanize_resolved_name(name)
                    .expect("already resolved")
            )
        }
        FunctionArgs::Args { args, order_by } => {
            if !order_by.is_empty() {
                sql_bail!(
                    "ORDER BY specified, but {} is not an aggregate function",
                    ecx.qcx
                        .scx
                        .humanize_resolved_name(name)
                        .expect("already resolved")
                );
            }
            plan_exprs(ecx, args)?
        }
    };

    func::select_impl(ecx, FuncSpec::Func(name), impls, scalar_args, vec![])
}

pub const IGNORE_NULLS_ERROR_MSG: &str =
    "IGNORE NULLS and RESPECT NULLS options for functions other than LAG and LEAD";

/// Resolves the name to a set of function implementations.
///
/// If the name does not specify a known built-in function, returns an error.
pub fn resolve_func(
    ecx: &ExprContext,
    name: &ResolvedItemName,
    args: &mz_sql_parser::ast::FunctionArgs<Aug>,
) -> Result<&'static Func, PlanError> {
    if let Ok(i) = ecx.qcx.scx.get_item_by_resolved_name(name) {
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
            plan_exprs(ecx, args)?
        }
    };

    let arg_types: Vec<_> = cexprs
        .into_iter()
        .map(|ty| match ecx.scalar_type(&ty) {
            CoercibleScalarType::Coerced(ty) => ecx.humanize_scalar_type(&ty, false),
            CoercibleScalarType::Record(_) => "record".to_string(),
            CoercibleScalarType::Uncoerced => "unknown".to_string(),
        })
        .collect();

    Err(PlanError::UnknownFunction {
        name: name.to_string(),
        arg_types,
    })
}

fn plan_is_expr<'a>(
    ecx: &ExprContext,
    expr: &'a Expr<Aug>,
    construct: &IsExprConstruct<Aug>,
    not: bool,
) -> Result<HirScalarExpr, PlanError> {
    let expr = plan_expr(ecx, expr)?;
    let mut expr = match construct {
        IsExprConstruct::Null => {
            // PostgreSQL can plan `NULL IS NULL` but not `$1 IS NULL`. This is
            // at odds with our type coercion rules, which treat `NULL` literals
            // and unconstrained parameters identically. Providing a type hint
            // of string means we wind up supporting both.
            let expr = expr.type_as_any(ecx)?;
            expr.call_is_null()
        }
        IsExprConstruct::Unknown => {
            let expr = expr.type_as(ecx, &SqlScalarType::Bool)?;
            expr.call_is_null()
        }
        IsExprConstruct::True => {
            let expr = expr.type_as(ecx, &SqlScalarType::Bool)?;
            expr.call_unary(UnaryFunc::IsTrue(expr_func::IsTrue))
        }
        IsExprConstruct::False => {
            let expr = expr.type_as(ecx, &SqlScalarType::Bool)?;
            expr.call_unary(UnaryFunc::IsFalse(expr_func::IsFalse))
        }
        IsExprConstruct::DistinctFrom(expr2) => {
            let expr1 = expr.type_as_any(ecx)?;
            let expr2 = plan_expr(ecx, expr2)?.type_as_any(ecx)?;
            // There are three cases:
            // 1. Both terms are non-null, in which case the result should be `a != b`.
            // 2. Exactly one term is null, in which case the result should be true.
            // 3. Both terms are null, in which case the result should be false.
            //
            // (a != b OR a IS NULL OR b IS NULL) AND (a IS NOT NULL OR b IS NOT NULL)
            let term1 = HirScalarExpr::variadic_or(vec![
                expr1.clone().call_binary(expr2.clone(), expr_func::NotEq),
                expr1.clone().call_is_null(),
                expr2.clone().call_is_null(),
            ]);
            let term2 = HirScalarExpr::variadic_or(vec![
                expr1.call_is_null().not(),
                expr2.call_is_null().not(),
            ]);
            term1.and(term2)
        }
    };
    if not {
        expr = expr.not();
    }
    Ok(expr)
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
    for (c, r) in conditions.iter().zip_eq(results) {
        let c = match operand {
            Some(operand) => operand.clone().equals(c.clone()),
            None => c.clone(),
        };
        let cexpr = plan_expr(ecx, &c)?.type_as(ecx, &SqlScalarType::Bool)?;
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
    for (cexpr, rexpr) in cond_exprs
        .into_iter()
        .rev()
        .zip_eq(result_exprs.into_iter().rev())
    {
        expr = HirScalarExpr::if_then_else(cexpr, rexpr, expr);
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
                    (Datum::Int32(n), SqlScalarType::Int32)
                } else if let Ok(n) = d.0.try_into() {
                    (Datum::Int64(n), SqlScalarType::Int64)
                } else {
                    (
                        Datum::Numeric(d),
                        SqlScalarType::Numeric { max_scale: None },
                    )
                }
            } else {
                (
                    Datum::Numeric(d),
                    SqlScalarType::Numeric { max_scale: None },
                )
            }
        }
        Value::HexString(_) => bail_unsupported!("hex string literals"),
        Value::Boolean(b) => match b {
            false => (Datum::False, SqlScalarType::Bool),
            true => (Datum::True, SqlScalarType::Bool),
        },
        Value::Interval(i) => {
            let i = literal::plan_interval(i)?;
            (Datum::Interval(i), SqlScalarType::Interval)
        }
        Value::String(s) => return Ok(CoercibleScalarExpr::LiteralString(s.clone())),
        Value::Null => return Ok(CoercibleScalarExpr::LiteralNull),
    };
    let expr = HirScalarExpr::literal(datum, scalar_type);
    Ok(expr.into())
}

/// The common part of the planning of non-aggregate window functions, i.e.,
/// scalar window functions and value window functions.
fn plan_window_function_non_aggr<'a>(
    ecx: &ExprContext,
    Function {
        name,
        args,
        filter,
        over,
        distinct,
    }: &'a Function<Aug>,
) -> Result<
    (
        bool,
        Vec<HirScalarExpr>,
        Vec<ColumnOrder>,
        mz_expr::WindowFrame,
        Vec<HirScalarExpr>,
        Vec<CoercibleScalarExpr>,
    ),
    PlanError,
> {
    let (ignore_nulls, order_by_exprs, col_orders, window_frame, partition) =
        plan_window_function_common(ecx, name, over)?;

    if *distinct {
        sql_bail!(
            "DISTINCT specified, but {} is not an aggregate function",
            name
        );
    }

    if filter.is_some() {
        bail_unsupported!("FILTER in non-aggregate window functions");
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

    Ok((
        ignore_nulls,
        order_by_exprs,
        col_orders,
        window_frame,
        partition,
        scalar_args,
    ))
}

/// The common part of the planning of all window functions.
fn plan_window_function_common(
    ecx: &ExprContext,
    name: &<Aug as AstInfo>::ItemName,
    over: &Option<WindowSpec<Aug>>,
) -> Result<
    (
        bool,
        Vec<HirScalarExpr>,
        Vec<ColumnOrder>,
        mz_expr::WindowFrame,
        Vec<HirScalarExpr>,
    ),
    PlanError,
> {
    if !ecx.allow_windows {
        sql_bail!(
            "window functions are not allowed in {} (function {})",
            ecx.name,
            name
        );
    }

    let window_spec = match over.as_ref() {
        Some(over) => over,
        None => sql_bail!("window function {} requires an OVER clause", name),
    };
    if window_spec.ignore_nulls && window_spec.respect_nulls {
        sql_bail!("Both IGNORE NULLS and RESPECT NULLS were given.");
    }
    let window_frame = match window_spec.window_frame.as_ref() {
        Some(frame) => plan_window_frame(frame)?,
        None => mz_expr::WindowFrame::default(),
    };
    let mut partition = Vec::new();
    for expr in &window_spec.partition_by {
        partition.push(plan_expr(ecx, expr)?.type_as_any(ecx)?);
    }

    let (order_by_exprs, col_orders) = plan_function_order_by(ecx, &window_spec.order_by)?;

    Ok((
        window_spec.ignore_nulls,
        order_by_exprs,
        col_orders,
        window_frame,
        partition,
    ))
}

fn plan_window_frame(
    WindowFrame {
        units,
        start_bound,
        end_bound,
    }: &WindowFrame,
) -> Result<mz_expr::WindowFrame, PlanError> {
    use mz_expr::WindowFrameBound::*;
    let units = window_frame_unit_ast_to_expr(units)?;
    let start_bound = window_frame_bound_ast_to_expr(start_bound);
    let end_bound = end_bound
        .as_ref()
        .map(window_frame_bound_ast_to_expr)
        .unwrap_or(CurrentRow);

    // Validate bounds according to Postgres rules
    match (&start_bound, &end_bound) {
        // Start bound can't be UNBOUNDED FOLLOWING
        (UnboundedFollowing, _) => {
            sql_bail!("frame start cannot be UNBOUNDED FOLLOWING")
        }
        // End bound can't be UNBOUNDED PRECEDING
        (_, UnboundedPreceding) => {
            sql_bail!("frame end cannot be UNBOUNDED PRECEDING")
        }
        // Start bound should come before end bound in the list of bound definitions
        (CurrentRow, OffsetPreceding(_)) => {
            sql_bail!("frame starting from current row cannot have preceding rows")
        }
        (OffsetFollowing(_), OffsetPreceding(_) | CurrentRow) => {
            sql_bail!("frame starting from following row cannot have preceding rows")
        }
        // The above rules are adopted from Postgres.
        // The following rules are Materialize-specific.
        (OffsetPreceding(o1), OffsetFollowing(o2)) => {
            // Note that the only hard limit is that partition size + offset should fit in i64, so
            // in theory, we could support much larger offsets than this. But for our current
            // performance, even 1000000 is quite big.
            if *o1 > 1000000 || *o2 > 1000000 {
                sql_bail!("Window frame offsets greater than 1000000 are currently not supported")
            }
        }
        (OffsetPreceding(o1), OffsetPreceding(o2)) => {
            if *o1 > 1000000 || *o2 > 1000000 {
                sql_bail!("Window frame offsets greater than 1000000 are currently not supported")
            }
        }
        (OffsetFollowing(o1), OffsetFollowing(o2)) => {
            if *o1 > 1000000 || *o2 > 1000000 {
                sql_bail!("Window frame offsets greater than 1000000 are currently not supported")
            }
        }
        (OffsetPreceding(o), CurrentRow) => {
            if *o > 1000000 {
                sql_bail!("Window frame offsets greater than 1000000 are currently not supported")
            }
        }
        (CurrentRow, OffsetFollowing(o)) => {
            if *o > 1000000 {
                sql_bail!("Window frame offsets greater than 1000000 are currently not supported")
            }
        }
        // Other bounds are valid
        (_, _) => (),
    }

    // RANGE is only supported in the default frame
    // https://github.com/MaterializeInc/database-issues/issues/6585
    if units == mz_expr::WindowFrameUnits::Range
        && (start_bound != UnboundedPreceding || end_bound != CurrentRow)
    {
        bail_unsupported!("RANGE in non-default window frames")
    }

    let frame = mz_expr::WindowFrame {
        units,
        start_bound,
        end_bound,
    };
    Ok(frame)
}

fn window_frame_unit_ast_to_expr(
    unit: &WindowFrameUnits,
) -> Result<mz_expr::WindowFrameUnits, PlanError> {
    match unit {
        WindowFrameUnits::Rows => Ok(mz_expr::WindowFrameUnits::Rows),
        WindowFrameUnits::Range => Ok(mz_expr::WindowFrameUnits::Range),
        WindowFrameUnits::Groups => bail_unsupported!("GROUPS in window frames"),
    }
}

fn window_frame_bound_ast_to_expr(bound: &WindowFrameBound) -> mz_expr::WindowFrameBound {
    match bound {
        WindowFrameBound::CurrentRow => mz_expr::WindowFrameBound::CurrentRow,
        WindowFrameBound::Preceding(None) => mz_expr::WindowFrameBound::UnboundedPreceding,
        WindowFrameBound::Preceding(Some(offset)) => {
            mz_expr::WindowFrameBound::OffsetPreceding(*offset)
        }
        WindowFrameBound::Following(None) => mz_expr::WindowFrameBound::UnboundedFollowing,
        WindowFrameBound::Following(Some(offset)) => {
            mz_expr::WindowFrameBound::OffsetFollowing(*offset)
        }
    }
}

pub fn scalar_type_from_sql(
    scx: &StatementContext,
    data_type: &ResolvedDataType,
) -> Result<SqlScalarType, PlanError> {
    match data_type {
        ResolvedDataType::AnonymousList(elem_type) => {
            let elem_type = scalar_type_from_sql(scx, elem_type)?;
            if matches!(elem_type, SqlScalarType::Char { .. }) {
                bail_unsupported!("char list");
            }
            Ok(SqlScalarType::List {
                element_type: Box::new(elem_type),
                custom_id: None,
            })
        }
        ResolvedDataType::AnonymousMap {
            key_type,
            value_type,
        } => {
            match scalar_type_from_sql(scx, key_type)? {
                SqlScalarType::String => {}
                other => sql_bail!(
                    "map key type must be {}, got {}",
                    scx.humanize_scalar_type(&SqlScalarType::String, false),
                    scx.humanize_scalar_type(&other, false)
                ),
            }
            Ok(SqlScalarType::Map {
                value_type: Box::new(scalar_type_from_sql(scx, value_type)?),
                custom_id: None,
            })
        }
        ResolvedDataType::Named { id, modifiers, .. } => {
            scalar_type_from_catalog(scx.catalog, *id, modifiers)
        }
        ResolvedDataType::Error => unreachable!("should have been caught in name resolution"),
    }
}

pub fn scalar_type_from_catalog(
    catalog: &dyn SessionCatalog,
    id: CatalogItemId,
    modifiers: &[i64],
) -> Result<SqlScalarType, PlanError> {
    let entry = catalog.get_item(&id);
    let type_details = match entry.type_details() {
        Some(type_details) => type_details,
        None => {
            // Resolution should never produce a `ResolvedDataType::Named` with
            // an ID of a non-type, but we error gracefully just in case.
            sql_bail!(
                "internal error: {} does not refer to a type",
                catalog.resolve_full_name(entry.name()).to_string().quoted()
            );
        }
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
            Ok(SqlScalarType::Numeric { max_scale: scale })
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
            Ok(SqlScalarType::Char { length })
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
            Ok(SqlScalarType::VarChar { max_length: length })
        }
        CatalogType::Timestamp => {
            let mut modifiers = modifiers.iter().fuse();
            let precision = match modifiers.next() {
                Some(p) => Some(TimestampPrecision::try_from(*p)?),
                None => None,
            };
            if modifiers.next().is_some() {
                sql_bail!("type timestamp supports at most one type modifier");
            }
            Ok(SqlScalarType::Timestamp { precision })
        }
        CatalogType::TimestampTz => {
            let mut modifiers = modifiers.iter().fuse();
            let precision = match modifiers.next() {
                Some(p) => Some(TimestampPrecision::try_from(*p)?),
                None => None,
            };
            if modifiers.next().is_some() {
                sql_bail!("type timestamp with time zone supports at most one type modifier");
            }
            Ok(SqlScalarType::TimestampTz { precision })
        }
        t => {
            if !modifiers.is_empty() {
                sql_bail!(
                    "{} does not support type modifiers",
                    catalog.resolve_full_name(entry.name()).to_string()
                );
            }
            match t {
                CatalogType::Array {
                    element_reference: element_id,
                } => Ok(SqlScalarType::Array(Box::new(scalar_type_from_catalog(
                    catalog,
                    *element_id,
                    modifiers,
                )?))),
                CatalogType::List {
                    element_reference: element_id,
                    element_modifiers,
                } => Ok(SqlScalarType::List {
                    element_type: Box::new(scalar_type_from_catalog(
                        catalog,
                        *element_id,
                        element_modifiers,
                    )?),
                    custom_id: Some(id),
                }),
                CatalogType::Map {
                    key_reference: _,
                    key_modifiers: _,
                    value_reference: value_id,
                    value_modifiers,
                } => Ok(SqlScalarType::Map {
                    value_type: Box::new(scalar_type_from_catalog(
                        catalog,
                        *value_id,
                        value_modifiers,
                    )?),
                    custom_id: Some(id),
                }),
                CatalogType::Range {
                    element_reference: element_id,
                } => Ok(SqlScalarType::Range {
                    element_type: Box::new(scalar_type_from_catalog(catalog, *element_id, &[])?),
                }),
                CatalogType::Record { fields } => {
                    let scalars: Box<[(ColumnName, SqlColumnType)]> = fields
                        .iter()
                        .map(|f| {
                            let scalar_type = scalar_type_from_catalog(
                                catalog,
                                f.type_reference,
                                &f.type_modifiers,
                            )?;
                            Ok((
                                f.name.clone(),
                                SqlColumnType {
                                    scalar_type,
                                    nullable: true,
                                },
                            ))
                        })
                        .collect::<Result<Box<_>, PlanError>>()?;
                    Ok(SqlScalarType::Record {
                        fields: scalars,
                        custom_id: Some(id),
                    })
                }
                CatalogType::AclItem => Ok(SqlScalarType::AclItem),
                CatalogType::Bool => Ok(SqlScalarType::Bool),
                CatalogType::Bytes => Ok(SqlScalarType::Bytes),
                CatalogType::Date => Ok(SqlScalarType::Date),
                CatalogType::Float32 => Ok(SqlScalarType::Float32),
                CatalogType::Float64 => Ok(SqlScalarType::Float64),
                CatalogType::Int16 => Ok(SqlScalarType::Int16),
                CatalogType::Int32 => Ok(SqlScalarType::Int32),
                CatalogType::Int64 => Ok(SqlScalarType::Int64),
                CatalogType::UInt16 => Ok(SqlScalarType::UInt16),
                CatalogType::UInt32 => Ok(SqlScalarType::UInt32),
                CatalogType::UInt64 => Ok(SqlScalarType::UInt64),
                CatalogType::MzTimestamp => Ok(SqlScalarType::MzTimestamp),
                CatalogType::Interval => Ok(SqlScalarType::Interval),
                CatalogType::Jsonb => Ok(SqlScalarType::Jsonb),
                CatalogType::Oid => Ok(SqlScalarType::Oid),
                CatalogType::PgLegacyChar => Ok(SqlScalarType::PgLegacyChar),
                CatalogType::PgLegacyName => Ok(SqlScalarType::PgLegacyName),
                CatalogType::Pseudo => {
                    sql_bail!(
                        "cannot reference pseudo type {}",
                        catalog.resolve_full_name(entry.name()).to_string()
                    )
                }
                CatalogType::RegClass => Ok(SqlScalarType::RegClass),
                CatalogType::RegProc => Ok(SqlScalarType::RegProc),
                CatalogType::RegType => Ok(SqlScalarType::RegType),
                CatalogType::String => Ok(SqlScalarType::String),
                CatalogType::Time => Ok(SqlScalarType::Time),
                CatalogType::Uuid => Ok(SqlScalarType::Uuid),
                CatalogType::Int2Vector => Ok(SqlScalarType::Int2Vector),
                CatalogType::MzAclItem => Ok(SqlScalarType::MzAclItem),
                CatalogType::Numeric => unreachable!("handled above"),
                CatalogType::Char => unreachable!("handled above"),
                CatalogType::VarChar => unreachable!("handled above"),
                CatalogType::Timestamp => unreachable!("handled above"),
                CatalogType::TimestampTz => unreachable!("handled above"),
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
    tables: BTreeMap<Function<Aug>, String>,
    table_disallowed_context: Vec<&'static str>,
    in_select_item: bool,
    id_gen: IdGen,
    err: Option<PlanError>,
}

impl<'a> AggregateTableFuncVisitor<'a> {
    fn new(scx: &'a StatementContext<'a>) -> AggregateTableFuncVisitor<'a> {
        AggregateTableFuncVisitor {
            scx,
            aggs: Vec::new(),
            within_aggregate: false,
            tables: BTreeMap::new(),
            table_disallowed_context: Vec::new(),
            in_select_item: false,
            id_gen: Default::default(),
            err: None,
        }
    }

    fn into_result(
        self,
    ) -> Result<(Vec<Function<Aug>>, BTreeMap<Function<Aug>, String>), PlanError> {
        match self.err {
            Some(err) => Err(err),
            None => {
                // Dedup while preserving the order. We don't care what the order is, but it
                // has to be reproducible so that EXPLAIN PLAN tests work.
                let mut seen = BTreeSet::new();
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
        let item = match self.scx.get_item_by_resolved_name(&func.name) {
            Ok(i) => i,
            // Catching missing functions later in planning improves error messages.
            Err(_) => return,
        };

        match item.func() {
            // We don't want to collect window aggregations, because these will be handled not by
            // plan_aggregate, but by plan_function.
            Ok(Func::Aggregate { .. }) if func.over.is_none() => {
                if self.within_aggregate {
                    self.err = Some(sql_err!("nested aggregate functions are not allowed",));
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
                if let Ok(item) = self.scx.get_item_by_resolved_name(&func.name) {
                    if let Ok(Func::Table { .. }) = item.func() {
                        if let Some(context) = self.table_disallowed_context.last() {
                            self.err = Some(sql_err!(
                                "table functions are not allowed in {} (function {})",
                                context,
                                func.name
                            ));
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
                name: _,
                args: _,
                filter: None,
                over: None,
                distinct: false,
            } = &func
            {
                // Identical table functions can be de-duplicated.
                let unique_id = self.id_gen.allocate_id();
                let id = self
                    .tables
                    .entry(func)
                    .or_insert_with(|| format!("table_func_{unique_id}"));
                // We know this is okay because id is is 11 characters + <=20 characters, which is
                // less than our max length.
                *expr = Expr::Identifier(vec![Ident::new_unchecked(id.clone())]);
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

#[derive(Default)]
struct WindowFuncCollector {
    window_funcs: Vec<Expr<Aug>>,
}

impl WindowFuncCollector {
    fn into_result(self) -> Vec<Expr<Aug>> {
        // Dedup while preserving the order.
        let mut seen = BTreeSet::new();
        let window_funcs_dedupped = self
            .window_funcs
            .into_iter()
            .filter(move |expr| seen.insert(expr.clone()))
            // Reverse the order, so that in case of a nested window function call, the
            // inner one is evaluated first.
            .rev()
            .collect();
        window_funcs_dedupped
    }
}

impl Visit<'_, Aug> for WindowFuncCollector {
    fn visit_expr(&mut self, expr: &Expr<Aug>) {
        match expr {
            Expr::Function(func) => {
                if func.over.is_some() {
                    self.window_funcs.push(expr.clone());
                }
            }
            _ => (),
        }
        visit::visit_expr(self, expr);
    }

    fn visit_query(&mut self, _query: &Query<Aug>) {
        // Don't go into subqueries. Those will be handled by their own `plan_query`.
    }
}

/// Specifies how long a query will live.
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum QueryLifetime {
    /// The query's (or the expression's) result will be computed at one point in time.
    OneShot,
    /// The query (or expression) is used in a dataflow that maintains an index.
    Index,
    /// The query (or expression) is used in a dataflow that maintains a materialized view.
    MaterializedView,
    /// The query (or expression) is used in a dataflow that maintains a SUBSCRIBE.
    Subscribe,
    /// The query (or expression) is part of a (non-materialized) view.
    View,
    /// The expression is part of a source definition.
    Source,
}

impl QueryLifetime {
    /// (This used to impact whether the query is allowed to reason about the time at which it is
    /// running, e.g., by calling the `now()` function. Nowadays, this is decided by a different
    /// mechanism, see `ExprPrepStyle`.)
    pub fn is_one_shot(&self) -> bool {
        let result = match self {
            QueryLifetime::OneShot => true,
            QueryLifetime::Index => false,
            QueryLifetime::MaterializedView => false,
            QueryLifetime::Subscribe => false,
            QueryLifetime::View => false,
            QueryLifetime::Source => false,
        };
        assert_eq!(!result, self.is_maintained());
        result
    }

    /// Maintained dataflows can't have a finishing applied directly. Therefore, the finishing is
    /// turned into a `TopK`.
    pub fn is_maintained(&self) -> bool {
        match self {
            QueryLifetime::OneShot => false,
            QueryLifetime::Index => true,
            QueryLifetime::MaterializedView => true,
            QueryLifetime::Subscribe => true,
            QueryLifetime::View => true,
            QueryLifetime::Source => true,
        }
    }

    /// Most maintained dataflows don't allow SHOW commands currently. However, SUBSCRIBE does.
    pub fn allow_show(&self) -> bool {
        match self {
            QueryLifetime::OneShot => true,
            QueryLifetime::Index => false,
            QueryLifetime::MaterializedView => false,
            QueryLifetime::Subscribe => true, // SUBSCRIBE allows SHOW commands!
            QueryLifetime::View => false,
            QueryLifetime::Source => false,
        }
    }
}

/// Description of a CTE sufficient for query planning.
#[derive(Debug, Clone)]
pub struct CteDesc {
    pub name: String,
    pub desc: RelationDesc,
}

/// The state required when planning a `Query`.
#[derive(Debug, Clone)]
pub struct QueryContext<'a> {
    /// The context for the containing `Statement`.
    pub scx: &'a StatementContext<'a>,
    /// The lifetime that the planned query will have.
    pub lifetime: QueryLifetime,
    /// The scopes of the outer relation expression.
    pub outer_scopes: Vec<Scope>,
    /// The type of the outer relation expressions.
    pub outer_relation_types: Vec<SqlRelationType>,
    /// CTEs for this query, mapping their assigned LocalIds to their definition.
    pub ctes: BTreeMap<LocalId, CteDesc>,
    /// A name manager, for interning column names that will be stored in HIR and MIR.
    pub name_manager: Rc<RefCell<NameManager>>,
    pub recursion_guard: RecursionGuard,
}

impl CheckedRecursion for QueryContext<'_> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl<'a> QueryContext<'a> {
    pub fn root(scx: &'a StatementContext, lifetime: QueryLifetime) -> QueryContext<'a> {
        QueryContext {
            scx,
            lifetime,
            outer_scopes: vec![],
            outer_relation_types: vec![],
            ctes: BTreeMap::new(),
            name_manager: Rc::new(RefCell::new(NameManager::new())),
            recursion_guard: RecursionGuard::with_limit(1024), // chosen arbitrarily
        }
    }

    fn relation_type(&self, expr: &HirRelationExpr) -> SqlRelationType {
        expr.typ(&self.outer_relation_types, &self.scx.param_types.borrow())
    }

    /// Generate a new `QueryContext` appropriate to be used in subqueries of
    /// `self`.
    fn derived_context(&self, scope: Scope, relation_type: SqlRelationType) -> QueryContext<'a> {
        let ctes = self.ctes.clone();
        let outer_scopes = iter::once(scope).chain(self.outer_scopes.clone()).collect();
        let outer_relation_types = iter::once(relation_type)
            .chain(self.outer_relation_types.clone())
            .collect();
        // These shenanigans are simpler than adding `&mut NameManager` arguments everywhere.
        let name_manager = Rc::clone(&self.name_manager);

        QueryContext {
            scx: self.scx,
            lifetime: self.lifetime,
            outer_scopes,
            outer_relation_types,
            ctes,
            name_manager,
            recursion_guard: self.recursion_guard.clone(),
        }
    }

    /// Derives a `QueryContext` for a scope that contains no columns.
    fn empty_derived_context(&self) -> QueryContext<'a> {
        let scope = Scope::empty();
        let ty = SqlRelationType::empty();
        self.derived_context(scope, ty)
    }

    /// Resolves `object` to a table expr, i.e. creating a `Get` or inlining a
    /// CTE.
    pub fn resolve_table_name(
        &self,
        object: ResolvedItemName,
    ) -> Result<(HirRelationExpr, Scope), PlanError> {
        match object {
            ResolvedItemName::Item {
                id,
                full_name,
                version,
                ..
            } => {
                let name = full_name.into();
                let item = self.scx.get_item(&id).at_version(version);
                let desc = item
                    .desc(&self.scx.catalog.resolve_full_name(item.name()))?
                    .clone();
                let expr = HirRelationExpr::Get {
                    id: Id::Global(item.global_id()),
                    typ: desc.typ().clone(),
                };

                let scope = Scope::from_source(Some(name), desc.iter_names().cloned());

                Ok((expr, scope))
            }
            ResolvedItemName::Cte { id, name } => {
                let name = name.into();
                let cte = self.ctes.get(&id).unwrap();
                let expr = HirRelationExpr::Get {
                    id: Id::Local(id),
                    typ: cte.desc.typ().clone(),
                };

                let scope = Scope::from_source(Some(name), cte.desc.iter_names());

                Ok((expr, scope))
            }
            ResolvedItemName::ContinualTask { id, name } => {
                let cte = self.ctes.get(&id).unwrap();
                let expr = HirRelationExpr::Get {
                    id: Id::Local(id),
                    typ: cte.desc.typ().clone(),
                };

                let scope = Scope::from_source(Some(name), cte.desc.iter_names());

                Ok((expr, scope))
            }
            ResolvedItemName::Error => unreachable!("should have been caught in name resolution"),
        }
    }

    /// The returned String is more detailed when the `postgres_compat` flag is not set. However,
    /// the flag should be set in, e.g., the implementation of the `pg_typeof` function.
    pub fn humanize_scalar_type(&self, typ: &SqlScalarType, postgres_compat: bool) -> String {
        self.scx.humanize_scalar_type(typ, postgres_compat)
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
    pub relation_type: &'a SqlRelationType,
    /// Are aggregate functions allowed in this context
    pub allow_aggregates: bool,
    /// Are subqueries allowed in this context
    pub allow_subqueries: bool,
    /// Are parameters allowed in this context.
    pub allow_parameters: bool,
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
            self.relation_type,
            &self.qcx.scx.param_types.borrow(),
        )
    }

    pub fn scalar_type<E>(&self, expr: &E) -> <E::Type as AbstractColumnType>::AbstractScalarType
    where
        E: AbstractExpr,
    {
        self.column_type(expr).scalar_type()
    }

    fn derived_query_context(&self) -> QueryContext<'_> {
        let mut scope = self.scope.clone();
        scope.lateral_barrier = true;
        self.qcx.derived_context(scope, self.relation_type.clone())
    }

    pub fn require_feature_flag(&self, flag: &'static FeatureFlag) -> Result<(), PlanError> {
        self.qcx.scx.require_feature_flag(flag)
    }

    pub fn param_types(&self) -> &RefCell<BTreeMap<usize, SqlScalarType>> {
        &self.qcx.scx.param_types
    }

    /// The returned String is more detailed when the `postgres_compat` flag is not set. However,
    /// the flag should be set in, e.g., the implementation of the `pg_typeof` function.
    pub fn humanize_scalar_type(&self, typ: &SqlScalarType, postgres_compat: bool) -> String {
        self.qcx.scx.humanize_scalar_type(typ, postgres_compat)
    }

    pub fn intern(&self, item: &ScopeItem) -> Arc<str> {
        self.qcx.name_manager.borrow_mut().intern_scope_item(item)
    }
}

/// Manages column names, doing lightweight string internment.
///
/// Names are stored in `HirScalarExpr` and `MirScalarExpr` using
/// `Option<Arc<str>>`; we use the `NameManager` when lowering from SQL to HIR
/// to ensure maximal sharing.
#[derive(Debug, Clone)]
pub struct NameManager(BTreeSet<Arc<str>>);

impl NameManager {
    /// Creates a new `NameManager`, with no interned names
    pub fn new() -> Self {
        Self(BTreeSet::new())
    }

    /// Interns a string, returning a reference-counted pointer to the interned
    /// string.
    fn intern<S: AsRef<str>>(&mut self, s: S) -> Arc<str> {
        let s = s.as_ref();
        if let Some(interned) = self.0.get(s) {
            Arc::clone(interned)
        } else {
            let interned: Arc<str> = Arc::from(s);
            self.0.insert(Arc::clone(&interned));
            interned
        }
    }

    /// Interns a string representing a reference to a `ScopeItem`, returning a
    /// reference-counted pointer to the interned string.
    pub fn intern_scope_item(&mut self, item: &ScopeItem) -> Arc<str> {
        // TODO(mgree): extracting the table name from `item` leads to an issue with the catalog
        //
        // After an `ALTER ... RENAME` on a table, the catalog will have out-of-date
        // name information. Note that as of 2025-04-09, we don't support column
        // renames.
        //
        // A few bad alternatives:
        //
        // (1) Store it but don't write it down. This fails because the expression
        //     cache will erase our names on restart.
        // (2) When `ALTER ... RENAME` is run, re-optimize all downstream objects to
        //     get the right names. But the world now and the world when we made
        //     those objects may be different.
        // (3) Just don't write down the table name. Nothing fails... for now.

        self.intern(item.column_name.as_str())
    }
}

#[cfg(test)]
mod test {
    use super::*;

    /// Ensure that `NameManager`'s string interning works as expected.
    ///
    /// In particular, structurally but not referentially identical strings should
    /// be interned to the same `Arc`ed pointer.
    #[mz_ore::test]
    pub fn test_name_manager_string_interning() {
        let mut nm = NameManager::new();

        let orig_hi = "hi";
        let hi = nm.intern(orig_hi);
        let hello = nm.intern("hello");

        assert_ne!(hi.as_ptr(), hello.as_ptr());

        // this static string is _likely_ the same as `orig_hi``
        let hi2 = nm.intern("hi");
        assert_eq!(hi.as_ptr(), hi2.as_ptr());

        // generate a "hi" string that doesn't get optimized to the same static string
        let s = format!(
            "{}{}",
            hi.chars().nth(0).unwrap(),
            hi2.chars().nth(1).unwrap()
        );
        // make sure that we're testing with a fresh string!
        assert_ne!(orig_hi.as_ptr(), s.as_ptr());

        let hi3 = nm.intern(s);
        assert_eq!(hi.as_ptr(), hi3.as_ptr());
    }
}
