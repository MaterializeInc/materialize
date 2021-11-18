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
use std::fmt;
use std::iter;
use std::mem;

use anyhow::{anyhow, bail, ensure, Context};
use expr::{func as expr_func, LocalId};
use itertools::Itertools;
use ore::stack::{CheckedRecursion, RecursionGuard};
use ore::str::StrExt;
use sql_parser::ast::display::{AstDisplay, AstFormatter};
use sql_parser::ast::fold::Fold;
use sql_parser::ast::visit::{self, Visit};
use sql_parser::ast::{
    Assignment, AstInfo, Cte, DataType, DeleteStatement, Distinct, Expr, Function, FunctionArgs,
    Ident, InsertSource, IsExprConstruct, JoinConstraint, JoinOperator, Limit, OrderByExpr, Query,
    Raw, RawName, Select, SelectItem, SetExpr, SetOperator, Statement, SubscriptPosition,
    TableAlias, TableFactor, TableWithJoins, UnresolvedObjectName, UpdateStatement, Value, Values,
};

use ::expr::{GlobalId, Id, RowSetFinishing};
use repr::adt::numeric;
use repr::{
    strconv, ColumnName, ColumnType, Datum, RelationDesc, RelationType, Row, RowArena, ScalarType,
    Timestamp,
};

use crate::catalog::{CatalogItemType, SessionCatalog};
use crate::func::{self, Func, FuncSpec};
use crate::names::PartialName;
use crate::normalize;
use crate::plan::error::PlanError;
use crate::plan::expr::{
    AbstractColumnType, AbstractExpr, AggregateExpr, AggregateFunc, BinaryFunc,
    CoercibleScalarExpr, ColumnOrder, ColumnRef, HirRelationExpr, HirScalarExpr, JoinKind,
    ScalarWindowExpr, UnaryFunc, VariadicFunc, WindowExpr, WindowExprType,
};
use crate::plan::scope::{Scope, ScopeItem, ScopeItemName};
use crate::plan::statement::{StatementContext, StatementDesc};
use crate::plan::typeconv::{self, CastContext};
use crate::plan::{plan_utils, Params};
use crate::plan::{transform_ast, PlanContext};

// Aug is the type variable assigned to an AST that has already been
// name-resolved. An AST in this state has global IDs populated next to table
// names, and local IDs assigned to CTE definitions and references.
#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, Default)]
pub struct Aug;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ResolvedObjectName {
    pub id: Id,
    pub raw_name: PartialName,
    // Whether this object, when printed out, should use [id AS name] syntax. We
    // want this for things like tables and sources, but not for things like
    // types.
    pub print_id: bool,
}

impl AstDisplay for ResolvedObjectName {
    fn fmt<W: fmt::Write>(&self, f: &mut AstFormatter<W>) {
        if self.print_id {
            f.write_str(format!("[{} AS ", self.id));
        }
        let n = self.raw_name();
        if let Some(database) = n.database {
            f.write_node(&Ident::new(database));
            f.write_str(".");
        }
        if let Some(schema) = n.schema {
            f.write_node(&Ident::new(schema));
            f.write_str(".");
        }
        f.write_node(&Ident::new(n.item));
        if self.print_id {
            f.write_str("]");
        }
    }
}

impl std::fmt::Display for ResolvedObjectName {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(self.to_ast_string().as_str())
    }
}

impl ResolvedObjectName {
    pub fn raw_name(&self) -> PartialName {
        self.raw_name.clone()
    }
}

impl AstInfo for Aug {
    type ObjectName = ResolvedObjectName;
    type Id = Id;
}

#[derive(Debug)]
struct NameResolver<'a> {
    catalog: &'a dyn SessionCatalog,
    ctes: HashMap<String, LocalId>,
    status: Result<(), anyhow::Error>,
    ids: HashSet<GlobalId>,
}

impl<'a> NameResolver<'a> {
    pub fn new(catalog: &'a dyn SessionCatalog) -> NameResolver {
        NameResolver {
            catalog,
            ctes: HashMap::new(),
            status: Ok(()),
            ids: HashSet::new(),
        }
    }
}

impl<'a> Fold<Raw, Aug> for NameResolver<'a> {
    fn fold_query(&mut self, q: Query<Raw>) -> Query<Aug> {
        // Retain the old values of various CTE names so that we can restore them after we're done
        // planning this SELECT.
        let mut old_cte_values = Vec::new();
        // A single WITH block cannot use the same name multiple times.
        let mut used_names = HashSet::new();
        let mut ctes = Vec::new();
        for cte in q.ctes {
            let cte_name = normalize::ident(cte.alias.name.clone());

            if used_names.contains(&cte_name) {
                self.status = Err(anyhow!(
                    "WITH query name \"{}\" specified more than once",
                    cte_name
                ));
            }
            used_names.insert(cte_name.clone());

            let id = LocalId::new(self.ctes.len() as u64);
            ctes.push(Cte {
                alias: cte.alias,
                id: Id::Local(id),
                query: self.fold_query(cte.query),
            });
            let old_val = self.ctes.insert(cte_name.clone(), id);
            old_cte_values.push((cte_name, old_val));
        }
        let result = Query {
            ctes,
            body: self.fold_set_expr(q.body),
            limit: q.limit.map(|l| self.fold_limit(l)),
            offset: q.offset.map(|l| self.fold_expr(l)),
            order_by: q
                .order_by
                .into_iter()
                .map(|c| self.fold_order_by_expr(c))
                .collect(),
        };

        // Restore the old values of the CTEs.
        for (name, value) in old_cte_values.iter() {
            match value {
                Some(value) => {
                    self.ctes.insert(name.to_string(), value.clone());
                }
                None => {
                    self.ctes.remove(name);
                }
            };
        }

        result
    }

    fn fold_id(&mut self, _id: <Raw as AstInfo>::Id) -> <Aug as AstInfo>::Id {
        panic!("this should have been handled when walking the CTE");
    }

    fn fold_object_name(
        &mut self,
        object_name: <Raw as AstInfo>::ObjectName,
    ) -> <Aug as AstInfo>::ObjectName {
        match object_name {
            RawName::Name(raw_name) => {
                // Check if unqualified name refers to a CTE.
                if raw_name.0.len() == 1 {
                    let norm_name = normalize::ident(raw_name.0[0].clone());
                    if let Some(id) = self.ctes.get(&norm_name) {
                        return ResolvedObjectName {
                            id: Id::Local(*id),
                            raw_name: normalize::unresolved_object_name(raw_name).unwrap(),
                            print_id: false,
                        };
                    }
                }

                let name = normalize::unresolved_object_name(raw_name).unwrap();
                match self.catalog.resolve_item(&name) {
                    Ok(item) => {
                        self.ids.insert(item.id());
                        let print_id = !matches!(
                            item.item_type(),
                            CatalogItemType::Func | CatalogItemType::Type
                        );
                        ResolvedObjectName {
                            id: Id::Global(item.id()),
                            raw_name: item.name().clone().into(),
                            print_id,
                        }
                    }
                    Err(e) => {
                        if self.status.is_ok() {
                            self.status = Err(e.into());
                        }
                        ResolvedObjectName {
                            id: Id::Local(LocalId::new(0)),
                            raw_name: name,
                            print_id: false,
                        }
                    }
                }
            }
            RawName::Id(id, raw_name) => {
                let gid: GlobalId = match id.parse() {
                    Ok(id) => id,
                    Err(e) => {
                        self.status = Err(e);
                        GlobalId::User(0)
                    }
                };
                if self.status.is_ok() && self.catalog.try_get_item_by_id(&gid).is_none() {
                    self.status = Err(anyhow!("invalid id {}", &gid));
                }
                self.ids.insert(gid.clone());
                ResolvedObjectName {
                    id: Id::Global(gid),
                    raw_name: normalize::unresolved_object_name(raw_name).unwrap(),
                    print_id: true,
                }
            }
        }
    }
}

pub fn resolve_names_stmt(
    catalog: &dyn SessionCatalog,
    stmt: Statement<Raw>,
) -> Result<Statement<Aug>, anyhow::Error> {
    let mut n = NameResolver::new(catalog);
    let result = n.fold_statement(stmt);
    n.status?;
    Ok(result)
}

// Attaches additional information to a `Raw` AST, resulting in an `Aug` AST, by
// resolving names and (aspirationally) performing semantic analysis such as
// type-checking.
pub fn resolve_names(
    qcx: &mut QueryContext,
    query: Query<Raw>,
) -> Result<Query<Aug>, anyhow::Error> {
    let mut n = NameResolver::new(qcx.scx.catalog);
    let result = n.fold_query(query);
    n.status?;
    qcx.ids.extend(n.ids.iter());
    Ok(result)
}

pub fn resolve_names_expr(
    qcx: &mut QueryContext,
    expr: Expr<Raw>,
) -> Result<Expr<Aug>, anyhow::Error> {
    let mut n = NameResolver::new(qcx.scx.catalog);
    let result = n.fold_expr(expr);
    n.status?;
    qcx.ids.extend(n.ids.iter());
    Ok(result)
}

pub fn resolve_names_data_type(
    scx: &StatementContext,
    data_type: DataType<Raw>,
) -> Result<(DataType<Aug>, HashSet<GlobalId>), anyhow::Error> {
    let mut n = NameResolver::new(scx.catalog);
    let result = n.fold_data_type(data_type);
    n.status?;
    Ok((result, n.ids))
}

/// A general implementation for name resolution on AST elements.
///
/// This implementation is appropriate Whenever:
/// - You don't need to export the name resolution outside the `sql` crate and
///   the extra typing isn't too onerous.
/// - Discovered dependencies should extend `qcx.ids`.
fn resolve_names_extend_qcx_ids<F, T>(qcx: &mut QueryContext, f: F) -> Result<T, anyhow::Error>
where
    F: FnOnce(&mut NameResolver) -> T,
{
    let mut n = NameResolver::new(qcx.scx.catalog);
    let result = f(&mut n);
    n.status?;
    qcx.ids.extend(n.ids.iter());
    Ok(result)
}

pub struct PlannedQuery<E> {
    pub expr: E,
    pub desc: RelationDesc,
    pub finishing: RowSetFinishing,
    pub depends_on: Vec<GlobalId>,
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
    mut query: Query<Raw>,
    lifetime: QueryLifetime,
) -> Result<PlannedQuery<HirRelationExpr>, anyhow::Error> {
    transform_ast::transform_query(scx, &mut query)?;
    let mut qcx = QueryContext::root(scx, lifetime);
    let resolved_query = resolve_names(&mut qcx, query)?;
    let (mut expr, scope, mut finishing) = plan_query(&mut qcx, &resolved_query)?;

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
    let depends_on = qcx.ids.into_iter().collect();

    Ok(PlannedQuery {
        expr,
        desc,
        finishing,
        depends_on,
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
    table_name: UnresolvedObjectName,
    columns: Vec<Ident>,
    source: InsertSource<Raw>,
) -> Result<(GlobalId, HirRelationExpr), anyhow::Error> {
    let mut qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));
    let table = scx.resolve_item(table_name)?;

    // Validate the target of the insert.
    if table.item_type() != CatalogItemType::Table {
        bail!(
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
        bail!("cannot insert into system table '{}'", table.name());
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
            .filter_map(|(name, typ)| name.map(|n| (n, typ)))
            .enumerate()
            .map(|(idx, (name, typ))| (name, (idx, typ)))
            .collect();

        for c in &columns {
            if let Some((idx, typ)) = column_by_name.get(c) {
                ordering.push(*idx);
                source_types.push(&typ.scalar_type);
            } else {
                bail!(
                    "column {} of relation {} does not exist",
                    c.as_str().quoted(),
                    table.name().to_string().quoted()
                );
            }
        }
        if let Some(dup) = columns.iter().duplicates().next() {
            bail!("column {} specified more than once", dup.as_str().quoted());
        }
    };

    // Plan the source.
    let expr = match source {
        InsertSource::Query(mut query) => {
            transform_ast::transform_query(scx, &mut query)?;
            let query = resolve_names(&mut qcx, query)?;

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
                    let (expr, _scope) = plan_values(&qcx, &values.0, Some(source_types.clone()))?;
                    expr
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
        bail!("INSERT has more expressions than target columns");
    }
    // But it should never have less than the declared columns (or zero)
    if typ.arity() < columns.len() {
        bail!("INSERT has more target columns than expressions");
    }

    // Trim now that we know for sure the correct arity of the source query
    source_types.truncate(typ.arity());
    ordering.truncate(typ.arity());

    // Ensure the types of the source query match the types of the target table,
    // installing assignment casts where necessary and possible.
    let expr = cast_relation(&qcx, CastContext::Assignment, expr, source_types).map_err(|e| {
        anyhow!(
            "column {} is of type {} but expression is of type {}",
            desc.get_name(e.column)
                .unwrap_or(&ColumnName::from("?column?"))
                .as_str()
                .quoted(),
            pgrepr::Type::from(&e.target_type).name(),
            pgrepr::Type::from(&e.source_type).name(),
        )
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
            let (hir, _) = plan_default_expr(scx, default, &col_typ.scalar_type)?;
            project_key.push(typ.arity() + map_exprs.len());
            map_exprs.push(hir);
        }
    }

    Ok((table.id(), expr.map(map_exprs).project(project_key)))
}

pub fn plan_copy_from(
    scx: &StatementContext,
    table_name: UnresolvedObjectName,
    columns: Vec<Ident>,
) -> Result<(GlobalId, RelationDesc, Vec<usize>), anyhow::Error> {
    let table = scx.resolve_item(table_name)?;

    // Validate the target of the insert.
    if table.item_type() != CatalogItemType::Table {
        bail!(
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
        bail!("cannot insert into system table '{}'", table.name());
    }

    let mut ordering = Vec::with_capacity(columns.len());

    if columns.is_empty() {
        ordering.extend(0..desc.arity());
    } else {
        let columns: Vec<_> = columns.into_iter().map(normalize::column_name).collect();
        let column_by_name: HashMap<&ColumnName, (usize, &ColumnType)> = desc
            .iter()
            .filter_map(|(name, typ)| name.map(|n| (n, typ)))
            .enumerate()
            .map(|(idx, (name, typ))| (name, (idx, typ)))
            .collect();

        let mut names = Vec::with_capacity(columns.len());
        let mut source_types = Vec::with_capacity(columns.len());

        for c in &columns {
            if let Some((idx, typ)) = column_by_name.get(c) {
                ordering.push(*idx);
                source_types.push((*typ).clone());
                names.push(Some(c.clone()));
            } else {
                bail!(
                    "column {} of relation {} does not exist",
                    c.as_str().quoted(),
                    table.name().to_string().quoted()
                );
            }
        }
        if let Some(dup) = columns.iter().duplicates().next() {
            bail!("column {} specified more than once", dup.as_str().quoted());
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
    rows: Vec<repr::Row>,
) -> Result<HirRelationExpr, anyhow::Error> {
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

    let scx = StatementContext::new(
        Some(pcx),
        catalog,
        std::rc::Rc::new(RefCell::new(BTreeMap::new())),
    );

    let column_details = desc.iter_types().zip_eq(defaults).enumerate();
    for (col_idx, (col_typ, default)) in column_details {
        if let Some(src_idx) = col_to_source.get(&col_idx) {
            project_key.push(*src_idx);
        } else {
            let (hir, _) = plan_default_expr(&scx, default, &col_typ.scalar_type)?;
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
    mut delete_stmt: DeleteStatement<Raw>,
) -> Result<ReadThenWritePlan, anyhow::Error> {
    transform_ast::run_transforms(
        scx,
        |t, delete_stmt| t.visit_delete_statement_mut(delete_stmt),
        &mut delete_stmt,
    )?;

    let mut qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));
    let DeleteStatement {
        table_name,
        alias,
        using,
        selection,
    } = resolve_names_extend_qcx_ids(&mut qcx, move |n: &mut NameResolver| {
        n.fold_delete_statement(delete_stmt)
    })?;

    plan_mutation_query_inner(qcx, table_name, alias, using, vec![], selection)
}

pub fn plan_update_query(
    scx: &StatementContext,
    mut update_stmt: UpdateStatement<Raw>,
) -> Result<ReadThenWritePlan, anyhow::Error> {
    transform_ast::run_transforms(
        scx,
        |t, update_stmt| t.visit_update_statement_mut(update_stmt),
        &mut update_stmt,
    )?;

    let mut qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));
    let UpdateStatement {
        table_name,
        assignments,
        selection,
    } = resolve_names_extend_qcx_ids(&mut qcx, move |n: &mut NameResolver| {
        n.fold_update_statement(update_stmt)
    })?;

    plan_mutation_query_inner(qcx, table_name, None, vec![], assignments, selection)
}

pub fn plan_mutation_query_inner(
    qcx: QueryContext,
    table_name: ResolvedObjectName,
    alias: Option<TableAlias>,
    using: Vec<TableWithJoins<Aug>>,
    assignments: Vec<Assignment<Aug>>,
    selection: Option<Expr<Aug>>,
) -> Result<ReadThenWritePlan, anyhow::Error> {
    // Get global ID.
    let id = match table_name.id {
        Id::Global(id) => id,
        _ => bail!("cannot mutate non-user table"),
    };

    // Perform checks on item with given ID.
    let item = qcx.scx.get_item_by_id(&id);
    if item.item_type() != CatalogItemType::Table {
        bail!("cannot mutate {} '{}'", item.item_type(), item.name());
    }
    if id.is_system() {
        bail!("cannot mutate system table '{}'", item.name());
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
                };
                let expr = plan_expr(&ecx, &value)?.cast_to(
                    "SET clause",
                    ecx,
                    CastContext::Assignment,
                    &typ.scalar_type,
                )?;

                if sets.insert(idx, expr).is_some() {
                    bail!("column {} set twice", name)
                }
            }
            None => bail!("unknown column {}", name),
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
    mut using: Vec<TableWithJoins<Aug>>,
    get: HirRelationExpr,
    outer_scope: Scope,
) -> Result<HirRelationExpr, anyhow::Error> {
    // Plan `USING` as a cross-joined `FROM` without knowledge of the
    // statement's `FROM` target. This prevents `lateral` subqueries from
    // "seeing" the `FROM` target.
    let (mut using_rel_expr, using_scope) =
        using
            .drain(..)
            .fold(Ok(plan_join_identity(&qcx)), |l, twj| {
                let (left, left_scope) = l?;
                plan_table_with_joins(&qcx, left, left_scope, &twj)
            })?;

    if let Some(expr) = selection {
        // Join `FROM` with `USING` tables, like `USING..., FROM`. This gives us
        // PG-like semantics e.g. expressing ambiguous column references. We put
        // `USING...` first for no real reason, but making a different decision
        // would require adjusting the column references on this relation
        // differently.
        let (joined, joined_scope) = plan_join_operator(
            &JoinOperator::CrossJoin,
            qcx,
            using_rel_expr.clone(),
            using_scope,
            qcx,
            get.clone(),
            outer_scope,
            false,
        )?;

        // Treat any `WHERE` clause as being executed in a subquery.
        let joined_relation_type = qcx.relation_type(&joined);
        let subquery_qcx = qcx.derived_context(qcx.outer_scope.clone(), &joined_relation_type);

        let ecx = &ExprContext {
            qcx: &subquery_qcx,
            name: "WHERE clause",
            scope: &joined_scope,
            relation_type: &joined_relation_type,
            allow_aggregates: false,
            allow_subqueries: true,
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
        scope: &Scope::empty(Some(qcx.outer_scope.clone())),
        relation_type: &qcx.relation_type(&expr),
        allow_aggregates: false,
        allow_subqueries: true,
    };
    let mut map_exprs = vec![];
    let mut project_key = vec![];
    for (i, target_typ) in target_types.into_iter().enumerate() {
        let expr = HirScalarExpr::Column(ColumnRef {
            level: 0,
            column: i,
        });
        // We plan every cast and check the evaluated expressions rather than
        // checking the types directly because of some complex casting rules
        // between types not expressed in `ScalarType` equality.
        match typeconv::plan_cast("relation cast", ecx, ccx, expr.clone(), target_typ) {
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

/// Evaluates an expression in the AS OF position of a TAIL statement.
pub fn eval_as_of<'a>(
    scx: &'a StatementContext,
    mut expr: Expr<Raw>,
) -> Result<Timestamp, anyhow::Error> {
    let scope = Scope::from_source(
        None,
        iter::empty::<Option<ColumnName>>(),
        Some(Scope::empty(None)),
    );
    let desc = RelationDesc::empty();
    let mut qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));

    transform_ast::transform_expr(scx, &mut expr)?;

    let expr = resolve_names_expr(&mut qcx, expr)?;

    let ecx = &ExprContext {
        qcx: &qcx,
        name: "AS OF",
        scope: &scope,
        relation_type: &desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
    };

    let ex = plan_expr(ecx, &expr)?
        .type_as_any(ecx)?
        .lower_uncorrelated()?;
    let temp_storage = &RowArena::new();
    let evaled = ex.eval(&[], temp_storage)?;

    Ok(match ex.typ(desc.typ()).scalar_type {
        ScalarType::Numeric { .. } => {
            let n = evaled.unwrap_numeric().0;
            u64::try_from(n)?.try_into()?
        }
        ScalarType::Int16 => evaled.unwrap_int16().try_into()?,
        ScalarType::Int32 => evaled.unwrap_int32().try_into()?,
        ScalarType::Int64 => evaled.unwrap_int64().try_into()?,
        ScalarType::TimestampTz => evaled.unwrap_timestamptz().timestamp_millis().try_into()?,
        ScalarType::Timestamp => evaled.unwrap_timestamp().timestamp_millis().try_into()?,
        _ => bail!(
            "can't use {} as a timestamp for AS OF",
            scx.humanize_column_type(&ex.typ(desc.typ()))
        ),
    })
}

pub fn plan_default_expr(
    scx: &StatementContext,
    expr: &Expr<Raw>,
    target_ty: &ScalarType,
) -> Result<(HirScalarExpr, Vec<GlobalId>), anyhow::Error> {
    let mut qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));
    let expr = resolve_names_expr(&mut qcx, expr.clone())?;
    let ecx = &ExprContext {
        qcx: &qcx,
        name: "DEFAULT expression",
        scope: &Scope::empty(None),
        relation_type: &RelationType::empty(),
        allow_aggregates: false,
        allow_subqueries: false,
    };
    let hir = plan_expr(ecx, &expr)?.cast_to(ecx.name, ecx, CastContext::Assignment, target_ty)?;
    Ok((hir, qcx.ids.into_iter().collect()))
}

pub fn plan_params<'a>(
    scx: &'a StatementContext,
    params: Vec<Expr<Raw>>,
    desc: &StatementDesc,
) -> Result<Params, anyhow::Error> {
    if params.len() != desc.param_types.len() {
        bail!(
            "expected {} params, got {}",
            desc.param_types.len(),
            params.len()
        );
    }

    let mut qcx = QueryContext::root(scx, QueryLifetime::OneShot(scx.pcx()?));
    let scope = Scope::empty(None);
    let rel_type = RelationType::empty();

    let mut datums = Row::with_capacity(desc.param_types.len());
    let mut types = Vec::new();
    let temp_storage = &RowArena::new();
    for (mut param, ty) in params.into_iter().zip(&desc.param_types) {
        transform_ast::transform_expr(scx, &mut param)?;
        let expr = resolve_names_expr(&mut qcx, param)?;

        let ecx = &ExprContext {
            qcx: &qcx,
            name: "EXECUTE",
            scope: &scope,
            relation_type: &rel_type,
            allow_aggregates: false,
            allow_subqueries: false,
        };
        let ex = plan_expr(ecx, &expr)?.type_as_any(ecx)?;
        let st = ecx.scalar_type(&ex);
        if pgrepr::Type::from(&st) != *ty {
            bail!(
                "mismatched parameter type: expected {}, got {}",
                ty.name(),
                pgrepr::Type::from(&st).name()
            );
        }
        let ex = ex.lower_uncorrelated()?;
        let evaled = ex.eval(&[], temp_storage)?;
        datums.push(evaled);
        types.push(st);
    }
    Ok(Params { datums, types })
}

pub fn plan_index_exprs<'a>(
    scx: &'a StatementContext,
    on_desc: &RelationDesc,
    exprs: Vec<Expr<Raw>>,
) -> Result<(Vec<::expr::MirScalarExpr>, Vec<GlobalId>), anyhow::Error> {
    let scope = Scope::from_source(None, on_desc.iter_names(), Some(Scope::empty(None)));
    let mut qcx = QueryContext::root(scx, QueryLifetime::Static);

    let resolved_exprs = exprs
        .into_iter()
        .map(|mut e| {
            transform_ast::transform_expr(scx, &mut e)?;
            resolve_names_expr(&mut qcx, e)
        })
        .collect::<Result<Vec<Expr<Aug>>, _>>()?;

    let ecx = &ExprContext {
        qcx: &qcx,
        name: "CREATE INDEX",
        scope: &scope,
        relation_type: on_desc.typ(),
        allow_aggregates: false,
        allow_subqueries: false,
    };
    let mut out = vec![];
    for expr in resolved_exprs {
        let expr = plan_expr_or_col_index(ecx, &expr)?;
        out.push(expr.lower_uncorrelated()?);
    }
    Ok((out, qcx.ids.into_iter().collect()))
}

fn plan_expr_or_col_index(
    ecx: &ExprContext,
    e: &Expr<Aug>,
) -> Result<HirScalarExpr, anyhow::Error> {
    match check_col_index(&ecx.name, e, ecx.relation_type.column_types.len())? {
        Some(column) => Ok(HirScalarExpr::Column(ColumnRef { level: 0, column })),
        _ => plan_expr(ecx, e)?.type_as_any(ecx),
    }
}

fn check_col_index(name: &str, e: &Expr<Aug>, max: usize) -> Result<Option<usize>, anyhow::Error> {
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
    qcx: &mut QueryContext,
    q: &Query<Aug>,
) -> Result<(HirRelationExpr, Scope, RowSetFinishing), anyhow::Error> {
    qcx.checked_recur_mut(|qcx| plan_query_inner(qcx, q))
}

fn plan_query_inner(
    qcx: &mut QueryContext,
    q: &Query<Aug>,
) -> Result<(HirRelationExpr, Scope, RowSetFinishing), anyhow::Error> {
    // Retain the old values of various CTE names so that we can restore them
    // after we're done planning this SELECT.
    let mut old_cte_values = Vec::new();
    // A single WITH block cannot use the same name multiple times.
    let mut used_names = HashSet::new();
    for cte in &q.ctes {
        let cte_name = normalize::ident(cte.alias.name.clone());

        if used_names.contains(&cte_name) {
            bail!(
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
                        val_desc,
                        level_offset: 0,
                    },
                );
                old_cte_values.push((cte_name, old_val));
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
        }) => bail!("LIMIT must be an integer constant"),
    };
    let offset = match &q.offset {
        None => 0,
        Some(Expr::Value(Value::Number(x))) => x.parse()?,
        _ => bail!("OFFSET must be an integer constant"),
    };

    let result = match &q.body {
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
    };

    result
}

pub fn plan_nested_query(
    qcx: &mut QueryContext,
    q: &Query<Aug>,
) -> Result<(HirRelationExpr, Scope), anyhow::Error> {
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
) -> Result<(HirRelationExpr, Scope), anyhow::Error> {
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

            // TODO(jamii) this type-checking is redundant with
            // HirRelationExpr::typ, but currently it seems that we need both
            // because HirRelationExpr::typ is not allowed to return errors
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
                        qcx.humanize_scalar_type(&left_col_type.scalar_type),
                        qcx.humanize_scalar_type(&right_col_type.scalar_type)
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
            let (expr, scope) = plan_nested_query(qcx, query)?;
            Ok((expr, scope))
        }
    }
}

fn plan_values(
    qcx: &QueryContext,
    values: &[Vec<Expr<Aug>>],
    type_hints: Option<Vec<&ScalarType>>,
) -> Result<(HirRelationExpr, Scope), anyhow::Error> {
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
        func: expr::TableFunc::Wrap {
            width: ncols,
            types: col_types,
        },
        exprs,
    };

    // Build column names.
    let mut scope = Scope::empty(Some(qcx.outer_scope.clone()));
    for i in 0..ncols {
        let name = Some(format!("column{}", i + 1).into());
        scope.items.push(ScopeItem::from_column_name(name));
    }

    Ok((out, scope))
}

fn plan_join_identity(qcx: &QueryContext) -> (HirRelationExpr, Scope) {
    let typ = RelationType::new(vec![]);
    let expr = HirRelationExpr::constant(vec![vec![]], typ);
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
    s: &Select<Aug>,
    order_by_exprs: &[OrderByExpr<Aug>],
) -> Result<SelectPlan, anyhow::Error> {
    let Select {
        distinct,
        projection,
        from,
        selection,
        group_by,
        having,
        options,
    } = s;

    // Extract hints about group size if there are any
    let mut options = crate::normalize::options(options);

    let option = options.remove("expected_group_size");

    let expected_group_size = match option {
        Some(Value::Number(n)) => Some(n.parse::<usize>()?),
        Some(_) => bail!("expected_group_size must be a number"),
        None => None,
    };

    // Step 1. Handle FROM clause, including joins.
    let (mut relation_expr, from_scope) =
        from.iter().fold(Ok(plan_join_identity(qcx)), |l, twj| {
            let (left, left_scope) = l?;
            plan_table_with_joins(qcx, left, left_scope, twj)
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
                let scope_item = if let HirScalarExpr::Column(ColumnRef {
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
            relation_expr =
                relation_expr
                    .map(group_exprs)
                    .reduce(group_key, agg_exprs, expected_group_size);
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
                        HirScalarExpr::Column(ColumnRef { level: 0, column })
                    } else {
                        bail!("column {} must appear in the GROUP BY clause or be used in an aggregate function", from_scope.items[*i].short_display_name().quoted());
                    }
                }
                ExpandedSelectItem::Expr(expr) => plan_expr(ecx, &expr)?.type_as_any(ecx)?,
            };
            if let HirScalarExpr::Column(ColumnRef { level: 0, column }) = expr {
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
        let relation_type = qcx.relation_type(&relation_expr);
        let (mut order_by, mut map_exprs) = plan_projected_order_by_exprs(
            &ExprContext {
                qcx,
                name: "ORDER BY clause",
                scope: &map_scope,
                relation_type: &relation_type,
                allow_aggregates: true,
                allow_subqueries: true,
            },
            order_by_exprs,
            &project_key,
        )?;

        match distinct {
            None => relation_expr = relation_expr.map(map_exprs),
            Some(Distinct::EntireRow) => {
                if relation_type.arity() == 0 {
                    bail!("SELECT DISTINCT must have at least one column");
                }
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
                    let mut expr = &HirScalarExpr::Column(ColumnRef {
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
                        HirScalarExpr::Column(ColumnRef { level: 0, column }) => column,
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
    group_expr: &'a Expr<Aug>,
    projection: &'a [(ExpandedSelectItem, Option<ColumnName>)],
) -> Result<(Option<&'a Expr<Aug>>, HirScalarExpr), anyhow::Error> {
    let plan_projection = |column: usize| match &projection[column].0 {
        ExpandedSelectItem::InputOrdinal(column) => Ok((
            None,
            HirScalarExpr::Column(ColumnRef {
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
    order_by_exprs: &[OrderByExpr<Aug>],
) -> Result<(Vec<ColumnOrder>, Vec<HirScalarExpr>), anyhow::Error> {
    let project_key: Vec<_> = (0..ecx.scope.len()).collect();
    plan_projected_order_by_exprs(ecx, order_by_exprs, &project_key)
}

/// Like `plan_order_by_exprs`, except that any column ordinal references are
/// projected via `project_key` rather than being accepted directly.
fn plan_projected_order_by_exprs(
    ecx: &ExprContext,
    order_by_exprs: &[OrderByExpr<Aug>],
    project_key: &[usize],
) -> Result<(Vec<ColumnOrder>, Vec<HirScalarExpr>), anyhow::Error> {
    let mut order_by = vec![];
    let mut map_exprs = vec![];
    for obe in order_by_exprs {
        let expr = plan_order_by_or_distinct_expr(ecx, &obe.expr, project_key)?;
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
/// This is like `plan_expr_or_col_index`, except that any column ordinal
/// references are projected via `project_key` rather than being accepted
/// directly.
fn plan_order_by_or_distinct_expr(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    project_key: &[usize],
) -> Result<HirScalarExpr, anyhow::Error> {
    match check_col_index(&ecx.name, expr, project_key.len())? {
        Some(i) => Ok(HirScalarExpr::Column(ColumnRef {
            level: 0,
            column: project_key[i],
        })),
        None => plan_expr(ecx, expr)?.type_as_any(ecx),
    }
}

fn plan_table_with_joins<'a>(
    qcx: &QueryContext,
    left: HirRelationExpr,
    left_scope: Scope,
    table_with_joins: &'a TableWithJoins<Aug>,
) -> Result<(HirRelationExpr, Scope), anyhow::Error> {
    let pushed_left_was_join_identity = left.is_join_identity();
    let (mut left, mut left_scope) = plan_table_factor(
        qcx,
        left,
        left_scope,
        &JoinOperator::CrossJoin,
        &table_with_joins.relation,
    )?;
    for join in &table_with_joins.joins {
        if !pushed_left_was_join_identity
            && matches!(
                &join.join_operator,
                JoinOperator::FullOuter(..) | JoinOperator::RightOuter(..)
            )
        {
            bail_unsupported!(6875, "full/right outer joins in comma join");
        }
        let (new_left, new_left_scope) =
            plan_table_factor(qcx, left, left_scope, &join.join_operator, &join.relation)?;
        left = new_left;
        left_scope = new_left_scope;
    }
    Ok((left, left_scope))
}

fn plan_table_factor(
    left_qcx: &QueryContext,
    left: HirRelationExpr,
    left_scope: Scope,
    join_operator: &JoinOperator<Aug>,
    table_factor: &TableFactor<Aug>,
) -> Result<(HirRelationExpr, Scope), anyhow::Error> {
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
            let (expr, scope) = qcx.resolve_table_name(name.clone())?;
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
            let mut qcx = (*qcx).clone();
            let (expr, scope) = plan_nested_query(&mut qcx, &subquery)?;
            let scope = plan_table_alias(scope, alias.as_ref())?;
            (expr, scope)
        }

        TableFactor::NestedJoin { join, alias } => {
            let (identity, identity_scope) = plan_join_identity(&qcx);
            let (expr, scope) = plan_table_with_joins(&qcx, identity, identity_scope, join)?;
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
    name: &UnresolvedObjectName,
    alias: Option<&TableAlias>,
    args: &FunctionArgs<Aug>,
) -> Result<(HirRelationExpr, Scope), anyhow::Error> {
    if *name == UnresolvedObjectName::unqualified("values") {
        // Produce a nice error message for the common typo
        // `SELECT * FROM VALUES (1)`.
        bail!("VALUES expression in FROM clause must be surrounded by parentheses");
    }

    let scalar_args = match args {
        FunctionArgs::Star => bail!("{} does not accept * as an argument", name),
        FunctionArgs::Args { args, order_by } => {
            if !order_by.is_empty() {
                bail!(
                    "ORDER BY specified, but {} is not an aggregate function",
                    name
                );
            }
            plan_exprs(ecx, args)?
        }
    };
    let resolved_name = normalize::unresolved_object_name(name.clone())?;

    let (call, mut scope) = match resolve_func(ecx, name, args)? {
        Func::Table(impls) => {
            let tf = func::select_impl(
                ecx,
                FuncSpec::Func(&resolved_name),
                impls,
                scalar_args,
                vec![],
            )?;
            let call = HirRelationExpr::CallTable {
                func: tf.func,
                exprs: tf.exprs,
            };
            let scope = Scope::from_source(
                Some(PartialName {
                    database: None,
                    schema: None,
                    item: resolved_name.item.clone(),
                }),
                tf.column_names,
                Some(ecx.qcx.outer_scope.clone()),
            );
            (call, scope)
        }
        Func::Set(impls) => {
            let (call, scope) = func::select_impl(
                ecx,
                FuncSpec::Func(&resolved_name),
                impls,
                scalar_args,
                vec![],
            )?;
            let scope = Scope::from_source(
                Some(PartialName {
                    database: None,
                    schema: None,
                    item: resolved_name.item.clone(),
                }),
                scope.column_names(),
                Some(ecx.qcx.outer_scope.clone()),
            );
            (call, scope)
        }
        _ => bail!("{} is not a table function", name),
    };

    if let Some(alias) = alias {
        scope = if alias.columns.is_empty()
            && scope.len() == 1
            && scope
                .resolve_table_column(
                    &resolved_name,
                    &normalize::column_name(Ident::from(resolved_name.item.clone())),
                )
                .is_ok()
        {
            // Strange special case for table functions that ouput one column.
            // If a table alias is provided but not a column alias, the column
            // implicitly takes on the same alias in addition to its inherent
            // name unless the column has a given name different from the function's name
            // (like jsonb_array_elements, which has a single `value` column).
            //
            // Concretely, this means `SELECT x FROM generate_series(1, 5) AS x`
            // returns a single column of type int, even though
            //
            //     CREATE TABLE t (a int)
            //     SELECT x FROM t AS x
            //
            // would return a single column of type record(int).
            plan_table_alias(
                scope,
                Some(&TableAlias {
                    name: alias.name.clone(),
                    columns: vec![alias.name.clone()],
                    strict: true,
                }),
            )?
        } else {
            plan_table_alias(scope, Some(&alias))?
        };
        if let [item] = &mut *scope.items {
            // The single column case also has the property that you can select the table
            // name and get the column (not the full table row wrapped in a record), named
            // by the table:
            //
            //     SELECT g FROM generate_series(1, 1) AS g(a);
            //
            // Returns `1` (not `(1)`) with a column named `g` (not `a`).
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

fn invent_column_name(ecx: &ExprContext, expr: &Expr<Aug>) -> Option<ScopeItemName> {
    let name = match expr {
        Expr::Identifier(names) => names.last().map(|n| normalize::column_name(n.clone())),
        Expr::Function(func) => {
            let name = normalize::unresolved_object_name(func.name.clone()).ok()?;
            if name.schema.as_deref() == Some("mz_internal") {
                None
            } else {
                Some(name.item.into())
            }
        }
        Expr::Coalesce { .. } => Some("coalesce".into()),
        Expr::NullIf { .. } => Some("nullif".into()),
        Expr::Array { .. } => Some("array".into()),
        Expr::List { .. } => Some("list".into()),
        Expr::Cast { expr, .. } => return invent_column_name(ecx, expr),
        Expr::FieldAccess { field, .. } => Some(normalize::column_name(field.clone())),
        Expr::Exists { .. } => Some("exists".into()),
        Expr::Subquery(query) | Expr::ListSubquery(query) => {
            // A bit silly to have to plan the query here just to get its column
            // name, since we throw away the planned expression, but fixing this
            // requires a separate semantic analysis phase.
            let (_expr, scope) = plan_nested_query(&mut ecx.derived_query_context(), query).ok()?;
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

#[derive(Debug)]
enum ExpandedSelectItem<'a> {
    InputOrdinal(usize),
    Expr(Cow<'a, Expr<Aug>>),
}

fn expand_select_item<'a>(
    ecx: &ExprContext,
    s: &'a SelectItem<Aug>,
) -> Result<Vec<(ExpandedSelectItem<'a>, Option<ColumnName>)>, anyhow::Error> {
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
                ty => bail!("type {} is not composite", ecx.humanize_scalar_type(&ty)),
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
    operator: &JoinOperator<Aug>,
    left_qcx: &QueryContext,
    left: HirRelationExpr,
    left_scope: Scope,
    right_qcx: &QueryContext,
    right: HirRelationExpr,
    right_scope: Scope,
    lateral: bool,
) -> Result<(HirRelationExpr, Scope), anyhow::Error> {
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
                HirRelationExpr::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    on: HirScalarExpr::literal_true(),
                    kind: JoinKind::Inner { lateral },
                }
            };
            Ok((join, left_scope.product(right_scope)?))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn plan_join_constraint<'a>(
    constraint: &'a JoinConstraint<Aug>,
    left_qcx: &QueryContext,
    left: HirRelationExpr,
    left_scope: Scope,
    right_qcx: &QueryContext,
    right: HirRelationExpr,
    right_scope: Scope,
    kind: JoinKind,
) -> Result<(HirRelationExpr, Scope), anyhow::Error> {
    let (expr, scope) = match constraint {
        JoinConstraint::On(expr) => {
            let mut product_scope = left_scope.product(right_scope)?;
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
            let joined = HirRelationExpr::Join {
                left: Box::new(left),
                right: Box::new(right),
                on,
                kind,
            };
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
            right_qcx,
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
                &column_names,
                left_qcx,
                left,
                left_scope,
                right_qcx,
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
) -> Result<(HirRelationExpr, Scope), anyhow::Error> {
    let mut join_exprs = vec![];
    let mut map_exprs = vec![];
    let mut new_items = vec![];
    let mut dropped_columns = HashSet::new();

    let mut both_scope = left_scope.clone().product(right_scope.clone())?;

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
    };

    for column_name in column_names {
        let (lhs, _) = left_scope.resolve_column(column_name)?;
        let (mut rhs, _) = right_scope.resolve_column(column_name)?;
        if lhs.level != 0 || rhs.level != 0 {
            bail!(
                "Internal error: name {} in USING resolved to outer column",
                column_name
            )
        }

        // The new column can be named using aliases from either the right or
        // left.
        let mut names = left_scope.items[lhs.column].names.clone();
        names.extend(right_scope.items[rhs.column].names.clone());

        // Adjust the RHS reference to its post-join location.
        rhs.column += left_scope.len();

        // Join keys must be resolved to same type.
        let mut exprs = coerce_homogeneous_exprs(
            &format!(
                "NATURAL/USING join column {}",
                column_name.as_str().quoted()
            ),
            &ecx,
            vec![
                CoercibleScalarExpr::Coerced(HirScalarExpr::Column(lhs)),
                CoercibleScalarExpr::Coerced(HirScalarExpr::Column(rhs)),
            ],
            None,
        )?;
        let (expr1, expr2) = (exprs.remove(0), exprs.remove(0));

        join_exprs.push(HirScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1: Box::new(expr1.clone()),
            expr2: Box::new(expr2.clone()),
        });
        map_exprs.push(HirScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce,
            exprs: vec![expr1, expr2],
        });
        new_items.push(ScopeItem {
            names,
            expr: None,
            nameable: true,
        });
        dropped_columns.insert(lhs.column);
        dropped_columns.insert(rhs.column);
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
    both_scope.items.extend(new_items);
    let both_scope = both_scope.project(&project_key);
    let both = HirRelationExpr::Join {
        left: Box::new(left),
        right: Box::new(right),
        on: join_exprs
            .into_iter()
            .fold(HirScalarExpr::literal_true(), |expr1, expr2| {
                HirScalarExpr::CallBinary {
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
    ecx: &'a ExprContext,
    e: &Expr<Aug>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    ecx.checked_recur(|ecx| plan_expr_inner(ecx, e))
}

fn plan_expr_inner<'a>(
    ecx: &'a ExprContext,
    e: &Expr<Aug>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
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
        Expr::Op { op, expr1, expr2 } => Ok(plan_op(ecx, op, expr1, expr2.as_deref())?.into()),
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
        Expr::Coalesce { exprs } => plan_coalesce(ecx, exprs),
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
        Expr::SubscriptIndex { expr, subscript } => plan_subscript_index(ecx, expr, subscript),
        Expr::SubscriptSlice { expr, positions } => plan_subscript_slice(ecx, expr, positions),

        // Subqueries.
        Expr::Exists(query) => plan_exists(ecx, query),
        Expr::Subquery(query) => plan_subquery(ecx, query),
        Expr::ListSubquery(query) => plan_list_subquery(ecx, query),

        Expr::Collate { .. } => bail_unsupported!("COLLATE"),
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

fn plan_parameter(ecx: &ExprContext, n: usize) -> Result<CoercibleScalarExpr, anyhow::Error> {
    if !ecx.allow_subqueries {
        bail!("{} does not allow subqueries", ecx.name)
    }
    if n == 0 || n > 65536 {
        bail!("there is no parameter ${}", n);
    }
    if ecx.param_types().borrow().contains_key(&n) {
        Ok(HirScalarExpr::Parameter(n).into())
    } else {
        Ok(CoercibleScalarExpr::Parameter(n))
    }
}

fn plan_row(ecx: &ExprContext, exprs: &[Expr<Aug>]) -> Result<CoercibleScalarExpr, anyhow::Error> {
    let mut out = vec![];
    for e in exprs {
        out.push(plan_expr(ecx, e)?);
    }
    Ok(CoercibleScalarExpr::LiteralRecord(out))
}

fn plan_cast(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    data_type: &DataType<Aug>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
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

    let expr = match expr {
        // Maintain the stringness of literals strings to preserve any
        // side effects of Explicit casts (going through plan_coerce
        // uses Assignment casts).
        CoercibleScalarExpr::LiteralString(..) => expr.type_as(&ecx, &ScalarType::String)?,
        expr => typeconv::plan_coerce(ecx, expr, &to_scalar_type)?,
    };

    Ok(typeconv::plan_cast("CAST", ecx, CastContext::Explicit, expr, &to_scalar_type)?.into())
}

fn plan_not(ecx: &ExprContext, expr: &Expr<Aug>) -> Result<CoercibleScalarExpr, anyhow::Error> {
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
) -> Result<CoercibleScalarExpr, anyhow::Error> {
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
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    let ecx = ecx.with_name("OR argument");
    Ok(HirScalarExpr::CallBinary {
        func: BinaryFunc::Or,
        expr1: Box::new(plan_expr(&ecx, left)?.type_as(&ecx, &ScalarType::Bool)?),
        expr2: Box::new(plan_expr(&ecx, right)?.type_as(&ecx, &ScalarType::Bool)?),
    }
    .into())
}

fn plan_coalesce(
    ecx: &ExprContext,
    exprs: &[Expr<Aug>],
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    assert!(!exprs.is_empty()); // `COALESCE()` is a syntax error
    let expr = HirScalarExpr::CallVariadic {
        func: VariadicFunc::Coalesce,
        exprs: coerce_homogeneous_exprs("coalesce", ecx, plan_exprs(ecx, exprs)?, None)?,
    };
    Ok(expr.into())
}

fn plan_field_access(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    field: &Ident,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    let field = normalize::column_name(field.clone());
    let expr = plan_expr(ecx, expr)?.type_as_any(ecx)?;
    let ty = ecx.scalar_type(&expr);
    let i = match &ty {
        ScalarType::Record { fields, .. } => fields.iter().position(|(name, _ty)| *name == field),
        ty => bail!(
            "column notation applied to type {}, which is not a composite type",
            ecx.humanize_scalar_type(&ty)
        ),
    };
    match i {
        None => bail!(
            "field {} not found in data type {}",
            field,
            ecx.humanize_scalar_type(&ty)
        ),
        Some(i) => Ok(expr.call_unary(UnaryFunc::RecordGet(i)).into()),
    }
}

fn plan_subscript_index(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    subscript: &Expr<Aug>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    let expr = plan_expr(ecx, expr)?.type_as_any(ecx)?;
    let ty = ecx.scalar_type(&expr);
    let func = match &ty {
        ScalarType::List { .. } => BinaryFunc::ListIndex,
        ScalarType::Array(_) => BinaryFunc::ArrayIndex,
        ty => bail!("cannot subscript type {}", ecx.humanize_scalar_type(&ty)),
    };

    Ok(expr
        .call_binary(
            plan_expr(ecx, subscript)?.cast_to(
                "subscript (indexing)",
                ecx,
                CastContext::Explicit,
                &ScalarType::Int64,
            )?,
            func,
        )
        .into())
}

fn plan_subscript_slice(
    ecx: &ExprContext,
    expr: &Expr<Aug>,
    positions: &[SubscriptPosition<Aug>],
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    assert_ne!(
        positions.len(),
        0,
        "subscript expression must contain at least one position"
    );
    if positions.len() > 1 {
        ecx.require_experimental_mode("layered/multidimensional slicing")?;
    }
    let expr = plan_expr(ecx, expr)?.type_as_any(ecx)?;
    let ty = ecx.scalar_type(&expr);
    match &ty {
        ScalarType::List { .. } => {
            let pos_len = positions.len();
            let n_dims = ty.unwrap_list_n_dims();
            if pos_len > n_dims {
                bail!(
                    "cannot slice into {} layers; list only has {} layer{}",
                    pos_len,
                    n_dims,
                    if n_dims == 1 { "" } else { "s" }
                )
            }
        }
        ty => bail!("cannot subscript type {}", ecx.humanize_scalar_type(&ty)),
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
            HirScalarExpr::literal(Datum::Int64(1), ScalarType::Int64)
        };

        let end = if let Some(end) = &p.end {
            plan_expr(ecx, end)?.cast_to(op_str, ecx, CastContext::Explicit, &ScalarType::Int64)?
        } else {
            HirScalarExpr::literal(Datum::Int64(i64::MAX - 1), ScalarType::Int64)
        };

        exprs.push(start);
        exprs.push(end);
    }

    Ok(HirScalarExpr::CallVariadic {
        func: VariadicFunc::ListSlice,
        exprs,
    }
    .into())
}

fn plan_exists(
    ecx: &ExprContext,
    query: &Query<Aug>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    if !ecx.allow_subqueries {
        bail!("{} does not allow subqueries", ecx.name)
    }
    let mut qcx = ecx.derived_query_context();
    let (expr, _scope) = plan_nested_query(&mut qcx, query)?;
    Ok(expr.exists().into())
}

fn plan_subquery(
    ecx: &ExprContext,
    query: &Query<Aug>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    if !ecx.allow_subqueries {
        bail!("{} does not allow subqueries", ecx.name)
    }
    let mut qcx = ecx.derived_query_context();
    let (expr, _scope) = plan_nested_query(&mut qcx, query)?;
    let column_types = qcx.relation_type(&expr).column_types;
    if column_types.len() != 1 {
        bail!(
            "Expected subselect to return 1 column, got {} columns",
            column_types.len()
        );
    }
    Ok(expr.select().into())
}

fn plan_list_subquery(
    ecx: &ExprContext,
    query: &Query<Aug>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    if !ecx.allow_subqueries {
        bail!("{} does not allow subqueries", ecx.name)
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
        bail!(
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

    // `ColumnRef`s in `aggregation_exprs` refers to the columns produced by planning the
    // subquery above.
    let aggregation_exprs: Vec<_> = iter::once(HirScalarExpr::CallVariadic {
        func: VariadicFunc::ListCreate {
            elem_type: elem_type.clone(),
        },
        exprs: vec![HirScalarExpr::Column(ColumnRef {
            column: project_column,
            level: 0,
        })],
    })
    .chain(finishing.order_by.iter().map(|co| {
        HirScalarExpr::Column(ColumnRef {
            column: co.column,
            level: 0,
        })
    }))
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
                func: AggregateFunc::ListConcat {
                    order_by: aggregation_order_by,
                },
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

    // If `expr` has no rows, return an empty list rather than NULL.
    Ok(HirScalarExpr::CallBinary {
        func: BinaryFunc::ListListConcat,
        expr1: Box::new(HirScalarExpr::Select(Box::new(reduced_expr))),
        expr2: Box::new(HirScalarExpr::literal(
            Datum::empty_list(),
            ScalarType::List {
                element_type: Box::new(elem_type),
                custom_oid: None,
            },
        )),
    }
    .into())
}

/// Plans a slice of expressions.
///
/// This function is a simple convenience function for mapping [`plan_expr`]
/// over a slice of expressions. The planned expressions are returned in the
/// same order as the input. If any of the expressions fail to plan, returns an
/// error instead.
fn plan_exprs<E>(ecx: &ExprContext, exprs: &[E]) -> Result<Vec<CoercibleScalarExpr>, anyhow::Error>
where
    E: std::borrow::Borrow<Expr<Aug>>,
{
    let mut out = vec![];
    for expr in exprs {
        out.push(plan_expr(ecx, expr.borrow())?);
    }
    Ok(out)
}

fn plan_array(
    ecx: &ExprContext,
    exprs: &[Expr<Aug>],
    type_hint: Option<&ScalarType>,
) -> Result<CoercibleScalarExpr, anyhow::Error> {
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
) -> Result<CoercibleScalarExpr, anyhow::Error> {
    let (elem_type, exprs) = if exprs.is_empty() {
        if let Some(ScalarType::List { element_type, .. }) = type_hint {
            (element_type.default_embedded_value(), vec![])
        } else {
            bail!("cannot determine type of empty list");
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
        let out = coerce_homogeneous_exprs("LIST expression", ecx, out, type_hint)?;
        (ecx.scalar_type(&out[0]).default_embedded_value(), out)
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
) -> Result<Vec<HirScalarExpr>, anyhow::Error> {
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
                "{} cannot be cast to uniform type: {} vs {}",
                name,
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
) -> Result<(Vec<HirScalarExpr>, Vec<ColumnOrder>), anyhow::Error> {
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
) -> Result<AggregateExpr, anyhow::Error> {
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
                bail!(
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
        let (i, _name) = ecx.scope.resolve_table_column(&table_name, &col_name)?;
        return Ok(HirScalarExpr::Column(i));
    }

    // If the name is unqualified, first check if it refers to a column.
    match ecx.scope.resolve_column(&col_name) {
        Ok((i, _name)) => return Ok(HirScalarExpr::Column(i)),
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
            let expr = HirScalarExpr::Column(ColumnRef {
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
        return Ok(HirScalarExpr::CallVariadic {
            func: VariadicFunc::RecordCreate { field_names },
            exprs,
        });
    }

    Err(PlanError::UnknownColumn(col_name.to_string()))
}

fn plan_op(
    ecx: &ExprContext,
    op: &str,
    expr1: &Expr<Aug>,
    expr2: Option<&Expr<Aug>>,
) -> Result<HirScalarExpr, anyhow::Error> {
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
) -> Result<HirScalarExpr, anyhow::Error> {
    let unresolved_name = normalize::unresolved_object_name(name.clone())?;

    let impls = match resolve_func(ecx, name, args)? {
        Func::Aggregate(_) if ecx.allow_aggregates => {
            // should already have been caught by `scope.resolve_expr` in `plan_expr`
            bail!(
                "Internal error: encountered unplanned aggregate function: {:?}",
                name,
            )
        }
        Func::Aggregate(_) => {
            bail!("aggregate functions are not allowed in {}", ecx.name);
        }
        Func::Table(_) => {
            bail_unsupported!(
                1546,
                format!("table function ({}) in scalar position", name)
            );
        }
        Func::Set(_) => {
            bail_unsupported!(
                1546,
                format!("table function ({}) in scalar position", name)
            );
        }
        Func::Scalar(impls) => impls,
        Func::ScalarWindow(impls) => {
            // Various things are duplicated here and below, but done this way to improve
            // error messages.

            if *distinct {
                bail!(
                    "DISTINCT specified, but {} is not an aggregate function",
                    name
                );
            }

            if filter.is_some() {
                bail_unsupported!("FILTER in window functions");
            }

            let window_spec = match over.as_ref() {
                Some(over) => over,
                None => bail!("window function {} requires an OVER clause", name),
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
                    bail!("* argument is invalid with non-aggregate function {}", name)
                }
                FunctionArgs::Args { args, order_by } => {
                    if !order_by.is_empty() {
                        bail!(
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
        bail!(
            "DISTINCT specified, but {} is not an aggregate function",
            name
        );
    }
    if filter.is_some() {
        bail!(
            "FILTER specified, but {} is not an aggregate function",
            name
        );
    }

    let scalar_args = match &args {
        FunctionArgs::Star => bail!("* argument is invalid with non-aggregate function {}", name),
        FunctionArgs::Args { args, order_by } => {
            if !order_by.is_empty() {
                bail!(
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
    args: &sql_parser::ast::FunctionArgs<Aug>,
) -> Result<&'static Func, anyhow::Error> {
    if let Ok(i) = ecx.qcx.scx.resolve_function(name.clone()) {
        if let Ok(f) = i.func() {
            return Ok(f);
        }
    }

    // Couldn't resolve function with this name, so generate verbose error
    // message.
    let cexprs = match args {
        sql_parser::ast::FunctionArgs::Star => vec![],
        sql_parser::ast::FunctionArgs::Args { args, order_by } => {
            if !order_by.is_empty() {
                bail!(
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

    bail!("function {}({}) does not exist", name, types.join(", "))
}

fn plan_is_expr<'a>(
    ecx: &ExprContext,
    inner: &'a Expr<Aug>,
    construct: IsExprConstruct,
    not: bool,
) -> Result<HirScalarExpr, anyhow::Error> {
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
        func: func,
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
) -> Result<HirScalarExpr, anyhow::Error> {
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
        expr = HirScalarExpr::If {
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
            let d = strconv::parse_numeric(s.as_str())?;
            if !s.contains(&['E', '.'][..]) {
                // Maybe representable as an int?
                if let Ok(n) = d.0.try_into() {
                    (Datum::Int32(n), ScalarType::Int32)
                } else if let Ok(n) = d.0.try_into() {
                    (Datum::Int64(n), ScalarType::Int64)
                } else {
                    (Datum::Numeric(d), ScalarType::Numeric { scale: None })
                }
            } else {
                (Datum::Numeric(d), ScalarType::Numeric { scale: None })
            }
        }
        Value::HexString(_) => bail_unsupported!(3114, "hex string literals"),
        Value::Boolean(b) => match b {
            false => (Datum::False, ScalarType::Bool),
            true => (Datum::True, ScalarType::Bool),
        },
        Value::Interval(iv) => {
            let mut i = strconv::parse_interval_w_disambiguator(
                &iv.value,
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
            bail!("bare [] arrays are not supported in this context; use ARRAY[] or LIST[] instead")
        }
    };
    let expr = HirScalarExpr::literal(datum, scalar_type);
    Ok(expr.into())
}

// Implement these as two identical enums without From/Into impls so that they
// have no cross-package dependencies, leaving that work up to this crate.
fn parser_datetimefield_to_adt(
    dtf: sql_parser::ast::DateTimeField,
) -> repr::adt::datetime::DateTimeField {
    use sql_parser::ast::DateTimeField::*;
    match dtf {
        Year => repr::adt::datetime::DateTimeField::Year,
        Month => repr::adt::datetime::DateTimeField::Month,
        Day => repr::adt::datetime::DateTimeField::Day,
        Hour => repr::adt::datetime::DateTimeField::Hour,
        Minute => repr::adt::datetime::DateTimeField::Minute,
        Second => repr::adt::datetime::DateTimeField::Second,
    }
}

fn find_trivial_column_equivalences(expr: &HirScalarExpr) -> Vec<(usize, usize)> {
    use BinaryFunc::*;
    use HirScalarExpr::*;
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
                    if l != r {
                        equivalences.push((*l, *r));
                    }
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

pub fn scalar_type_from_sql(
    scx: &StatementContext,
    data_type: &DataType<Aug>,
) -> Result<ScalarType, anyhow::Error> {
    Ok(match data_type {
        DataType::Array(elem_type) => {
            let elem_type = scalar_type_from_sql(scx, &elem_type)?;
            if matches!(
                elem_type,
                ScalarType::Char { .. } | ScalarType::List { .. } | ScalarType::Map { .. }
            ) {
                bail_unsupported!(format!("{}[]", scx.humanize_scalar_type(&elem_type)));
            }

            ScalarType::Array(Box::new(elem_type))
        }
        DataType::List(elem_type) => {
            let elem_type = scalar_type_from_sql(scx, &elem_type)?;

            if matches!(elem_type, ScalarType::Char { .. }) {
                bail_unsupported!("char list");
            }

            ScalarType::List {
                element_type: Box::new(elem_type),
                custom_oid: None,
            }
        }
        DataType::Map {
            key_type,
            value_type,
        } => {
            match scalar_type_from_sql(scx, &key_type)? {
                ScalarType::String => {}
                other => bail!(
                    "map key type must be {}, got {}",
                    scx.humanize_scalar_type(&ScalarType::String),
                    scx.humanize_scalar_type(&other)
                ),
            }
            ScalarType::Map {
                value_type: Box::new(scalar_type_from_sql(scx, &value_type)?),
                custom_oid: None,
            }
        }
        DataType::Other { name, typ_mod } => {
            let item = match scx.catalog.resolve_item(&name.raw_name()) {
                Ok(i) => i,
                Err(_) => bail!(
                    "type {} does not exist",
                    name.raw_name().to_string().quoted()
                ),
            };
            match scx.catalog.try_get_lossy_scalar_type_by_id(&item.id()) {
                Some(t) => match t {
                    ScalarType::Numeric { .. } => {
                        let scale = numeric::extract_typ_mod(typ_mod)?;
                        ScalarType::Numeric { scale }
                    }
                    ScalarType::Char { .. } => {
                        let length = repr::adt::char::extract_typ_mod(&typ_mod)?;
                        ScalarType::Char { length }
                    }
                    ScalarType::VarChar { .. } => {
                        let length = repr::adt::varchar::extract_typ_mod(&typ_mod)?;
                        ScalarType::VarChar { length }
                    }
                    t => {
                        if !typ_mod.is_empty() {
                            bail!("{} does not support type modifiers", &name.to_string());
                        }
                        t
                    }
                },
                None => bail!(
                    "type {} does not exist",
                    name.raw_name().to_string().quoted()
                ),
            }
        }
    })
}

pub fn scalar_type_from_pg(ty: &pgrepr::Type) -> Result<ScalarType, anyhow::Error> {
    match ty {
        pgrepr::Type::Bool => Ok(ScalarType::Bool),
        pgrepr::Type::Int2 => Ok(ScalarType::Int16),
        pgrepr::Type::Int4 => Ok(ScalarType::Int32),
        pgrepr::Type::Int8 => Ok(ScalarType::Int64),
        pgrepr::Type::Float4 => Ok(ScalarType::Float32),
        pgrepr::Type::Float8 => Ok(ScalarType::Float64),
        pgrepr::Type::Numeric => Ok(ScalarType::Numeric { scale: None }),
        pgrepr::Type::Date => Ok(ScalarType::Date),
        pgrepr::Type::Time => Ok(ScalarType::Time),
        pgrepr::Type::Timestamp => Ok(ScalarType::Timestamp),
        pgrepr::Type::TimestampTz => Ok(ScalarType::TimestampTz),
        pgrepr::Type::Interval => Ok(ScalarType::Interval),
        pgrepr::Type::Bytea => Ok(ScalarType::Bytes),
        pgrepr::Type::Text => Ok(ScalarType::String),
        pgrepr::Type::Char => Ok(ScalarType::Char { length: None }),
        pgrepr::Type::VarChar => Ok(ScalarType::VarChar { length: None }),
        pgrepr::Type::Jsonb => Ok(ScalarType::Jsonb),
        pgrepr::Type::Uuid => Ok(ScalarType::Uuid),
        pgrepr::Type::Array(t) => Ok(ScalarType::Array(Box::new(scalar_type_from_pg(t)?))),
        pgrepr::Type::List(l) => Ok(ScalarType::List {
            element_type: Box::new(scalar_type_from_pg(l)?),
            custom_oid: None,
        }),
        pgrepr::Type::Record(_) => {
            bail!("internal error: can't convert from pg record to materialize record")
        }
        pgrepr::Type::Oid => Ok(ScalarType::Oid),
        pgrepr::Type::RegProc => Ok(ScalarType::RegProc),
        pgrepr::Type::Map { value_type } => Ok(ScalarType::Map {
            value_type: Box::new(scalar_type_from_pg(value_type)?),
            custom_oid: None,
        }),
    }
}

/// This is used to collect aggregates from within an `Expr`.
/// See the explanation of aggregate handling at the top of the file for more details.
struct AggregateFuncVisitor<'a, 'ast> {
    scx: &'a StatementContext<'a>,
    aggs: Vec<&'ast Function<Aug>>,
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

    fn into_result(self) -> Result<Vec<&'ast Function<Aug>>, anyhow::Error> {
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

impl<'a, 'ast> Visit<'ast, Aug> for AggregateFuncVisitor<'a, 'ast> {
    fn visit_function(&mut self, func: &'ast Function<Aug>) {
        let item = match self.scx.resolve_function(func.name.clone()) {
            Ok(i) => i,
            // Catching missing functions later in planning improves error messages.
            Err(_) => return,
        };

        if let Ok(Func::Aggregate { .. }) = item.func() {
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
        } else {
            visit::visit_function(self, func);
        }
    }

    fn visit_query(&mut self, _query: &'ast Query<Aug>) {
        // Don't go into subqueries.
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
    val_desc: RelationDesc,
    /// We evaluate CTEs when they're defined and not when they're referred to
    /// (i.e. it's like a pointer, not a macro). This means that we need to
    /// adjust the level of correlated columns to retain their original
    /// references.
    level_offset: usize,
}

/// The state required when planning a `Query`.
#[derive(Debug, Clone)]
pub struct QueryContext<'a> {
    /// The context for the containing `Statement`.
    pub scx: &'a StatementContext<'a>,
    /// The lifetime that the planned query will have.
    pub lifetime: QueryLifetime<'a>,
    /// The scope of the outer relation expression.
    pub outer_scope: Scope,
    /// The type of the outer relation expressions.
    pub outer_relation_types: Vec<RelationType>,
    /// CTEs for this query, mapping their assigned LocalIds to their definition.
    pub ctes: HashMap<LocalId, CteDesc>,
    /// The GlobalIds of the items the `Query` is dependent upon.
    pub ids: HashSet<GlobalId>,
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
            outer_scope: Scope::empty(None),
            outer_relation_types: vec![],
            ctes: HashMap::new(),
            ids: HashSet::new(),
            recursion_guard: RecursionGuard::with_limit(1024), // chosen arbitrarily
        }
    }

    fn relation_type(&self, expr: &HirRelationExpr) -> RelationType {
        expr.typ(&self.outer_relation_types, &self.scx.param_types.borrow())
    }

    /// Generate a new `QueryContext` appropriate to be used in subqueries of
    /// `self`.
    fn derived_context(&self, scope: Scope, relation_type: &RelationType) -> QueryContext<'a> {
        let mut ctes = self.ctes.clone();
        for (_, cte) in ctes.iter_mut() {
            cte.level_offset += 1;
        }

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
            ctes,
            ids: HashSet::new(),
            recursion_guard: self.recursion_guard.clone(),
        }
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
                let mut val = cte.val.clone();
                val.visit_columns(0, &mut |depth, col| {
                    if col.level > depth {
                        col.level += cte.level_offset;
                    }
                });

                let scope = Scope::from_source(
                    Some(name),
                    cte.val_desc.iter_names().map(|n| n.cloned()),
                    Some(self.outer_scope.clone()),
                );

                // Inline `val` where its name was referenced. In an ideal
                // world, multiple instances of this expression would be
                // de-duplicated.
                Ok((val, scope))
            }
            Id::Global(id) => {
                let item = self.scx.get_item_by_id(&id);
                let desc = item.desc()?.clone();
                let expr = HirRelationExpr::Get {
                    id: Id::Global(item.id()),
                    typ: desc.typ().clone(),
                };

                let scope = Scope::from_source(
                    Some(object.raw_name),
                    desc.iter_names().map(|n| n.cloned()),
                    Some(self.outer_scope.clone()),
                );

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

impl CheckedRecursion for ExprContext<'_> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.qcx.recursion_guard
    }
}

impl<'a> ExprContext<'a> {
    pub fn catalog(&self) -> &dyn SessionCatalog {
        self.qcx.scx.catalog
    }

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

    pub fn humanize_scalar_type(&self, typ: &ScalarType) -> String {
        self.qcx.scx.humanize_scalar_type(typ)
    }
}
