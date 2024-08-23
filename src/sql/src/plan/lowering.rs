// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lowering is the process of transforming a `HirRelationExpr`
//! into a `MirRelationExpr`.
//!
//! The most crucial part of lowering is decorrelation; i.e.: rewriting a
//! `HirScalarExpr` that may contain subqueries (e.g. `SELECT` or `EXISTS`)
//! with instances of `MirScalarExpr` that contain none of these.
//!
//! Informally, a subquery should be viewed as a query that is executed in
//! the context of some outer relation, for each row of that relation. The
//! subqueries often contain references to the columns of the outer
//! relation.
//!
//! The transformation we perform maintains an `outer` relation and then
//! traverses the relation expression that may contain references to those
//! outer columns. As subqueries are discovered, the current relation
//! expression is recast as the outer expression until such a point as the
//! scalar expression's evaluation can be determined and appended to each
//! row of the previously outer relation.
//!
//! It is important that the outer columns (the initial columns) act as keys
//! for all nested computation. When counts or other aggregations are
//! performed, they should include not only the indicated keys but also all
//! of the outer columns.
//!
//! The decorrelation transformation is initialized with an empty outer
//! relation, but it seems entirely appropriate to decorrelate queries that
//! contain "holes" from prepared statements, as if the query was a subquery
//! against a relation containing the assignments of values to those holes.

use std::collections::{BTreeMap, BTreeSet};
use std::iter::repeat;

use crate::optimizer_metrics::OptimizerMetrics;
use crate::plan::expr::{
    AggregateExpr, ColumnOrder, ColumnRef, HirRelationExpr, HirScalarExpr, JoinKind, WindowExprType,
};
use crate::plan::{transform_expr, PlanError};
use crate::session::vars::SystemVars;
use itertools::Itertools;
use mz_expr::{AccessStrategy, AggregateFunc, MirRelationExpr, MirScalarExpr};
use mz_ore::collections::CollectionExt;
use mz_ore::stack::maybe_grow;
use mz_repr::*;

mod variadic_left;

/// Maps a leveled column reference to a specific column.
///
/// Leveled column references are nested, so that larger levels are
/// found early in a record and level zero is found at the end.
///
/// The column map only stores references for levels greater than zero,
/// and column references at level zero simply start at the first column
/// after all prior references.
#[derive(Debug, Clone)]
struct ColumnMap {
    inner: BTreeMap<ColumnRef, usize>,
}

impl ColumnMap {
    fn empty() -> ColumnMap {
        Self::new(BTreeMap::new())
    }

    fn new(inner: BTreeMap<ColumnRef, usize>) -> ColumnMap {
        ColumnMap { inner }
    }

    fn get(&self, col_ref: &ColumnRef) -> usize {
        if col_ref.level == 0 {
            self.inner.len() + col_ref.column
        } else {
            self.inner[col_ref]
        }
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    /// Updates references in the `ColumnMap` for use in a nested scope. The
    /// provided `arity` must specify the arity of the current scope.
    fn enter_scope(&self, arity: usize) -> ColumnMap {
        // From the perspective of the nested scope, all existing column
        // references will be one level greater.
        let existing = self
            .inner
            .clone()
            .into_iter()
            .update(|(col, _i)| col.level += 1);

        // All columns in the current scope become explicit entries in the
        // immediate parent scope.
        let new = (0..arity).map(|i| {
            (
                ColumnRef {
                    level: 1,
                    column: i,
                },
                self.len() + i,
            )
        });

        ColumnMap::new(existing.chain(new).collect())
    }
}

/// Map with the CTEs currently in scope.
type CteMap = BTreeMap<mz_expr::LocalId, CteDesc>;

/// Information about needed when finding a reference to a CTE in scope.
#[derive(Clone)]
struct CteDesc {
    /// The new ID assigned to the lowered version of the CTE, which may not match
    /// the ID of the input CTE.
    new_id: mz_expr::LocalId,
    /// The relation type of the CTE including the columns from the outer
    /// context at the beginning.
    relation_type: RelationType,
    /// The outer relation the CTE was applied to.
    outer_relation: MirRelationExpr,
}

#[derive(Debug)]
pub struct Config {
    /// Enable outer join lowering implemented in #22343.
    pub enable_new_outer_join_lowering: bool,
    /// Enable outer join lowering implemented in #25340.
    pub enable_variadic_left_join_lowering: bool,
    /// Enable the extra null filter implemented in #28018.
    pub enable_outer_join_null_filter: bool,
}

impl From<&SystemVars> for Config {
    fn from(vars: &SystemVars) -> Self {
        Self {
            enable_new_outer_join_lowering: vars.enable_new_outer_join_lowering(),
            enable_variadic_left_join_lowering: vars.enable_variadic_left_join_lowering(),
            enable_outer_join_null_filter: vars.enable_outer_join_null_filter(),
        }
    }
}

/// Context passed to the lowering. This is wired to most parts of the lowering.
struct Context<'a> {
    /// Feature flags affecting the behavior of lowering.
    config: &'a Config,
    /// Optional, because some callers don't have an `OptimizerMetrics` handy. When it's None, we
    /// simply don't write metrics.
    metrics: Option<&'a OptimizerMetrics>,
}

impl HirRelationExpr {
    /// Rewrite `self` into a `MirRelationExpr`.
    /// This requires rewriting all correlated subqueries (nested `HirRelationExpr`s) into flat queries
    #[mz_ore::instrument(target = "optimizer", level = "trace", name = "hir_to_mir")]
    pub fn lower<C: Into<Config>>(
        self,
        config: C,
        metrics: Option<&OptimizerMetrics>,
    ) -> Result<MirRelationExpr, PlanError> {
        let context = Context {
            config: &config.into(),
            metrics,
        };
        let result =
            match self {
                // We directly rewrite a Constant into the corresponding `MirRelationExpr::Constant`
                // to ensure that the downstream optimizer can easily bypass most
                // irrelevant optimizations (e.g. reduce folding) for this expression
                // without having to re-learn the fact that it is just a constant,
                // as it would if the constant were wrapped in a Let-Get pair.
                HirRelationExpr::Constant { rows, typ } => {
                    let rows: Vec<_> = rows.into_iter().map(|row| (row, 1)).collect();
                    MirRelationExpr::Constant {
                        rows: Ok(rows),
                        typ,
                    }
                }
                mut other => {
                    let mut id_gen = mz_ore::id_gen::IdGen::default();
                    transform_expr::split_subquery_predicates(&mut other);
                    transform_expr::try_simplify_quantified_comparisons(&mut other);
                    transform_expr::fuse_window_functions(&mut other)?;
                    MirRelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
                        .let_in_fallible(&mut id_gen, |id_gen, get_outer| {
                            other.applied_to(
                                id_gen,
                                get_outer,
                                &ColumnMap::empty(),
                                &mut CteMap::new(),
                                &context,
                            )
                        })?
                }
            };

        mz_repr::explain::trace_plan(&result);

        Ok(result)
    }

    /// Return a `MirRelationExpr` which evaluates `self` once for each row of `get_outer`.
    ///
    /// For uncorrelated `self`, this should be the cross-product between `get_outer` and `self`.
    /// When `self` references columns of `get_outer`, much more work needs to occur.
    ///
    /// The `col_map` argument contains mappings to some of the columns of `get_outer`, though
    /// perhaps not all of them. It should be used as the basis of resolving column references,
    /// but care must be taken when adding new columns that `get_outer.arity()` is where they
    /// will start, rather than any function of `col_map`.
    ///
    /// The `get_outer` expression should be a `Get` with no duplicate rows, describing the distinct
    /// assignment of values to outer rows.
    fn applied_to(
        self,
        id_gen: &mut mz_ore::id_gen::IdGen,
        get_outer: MirRelationExpr,
        col_map: &ColumnMap,
        cte_map: &mut CteMap,
        context: &Context,
    ) -> Result<MirRelationExpr, PlanError> {
        maybe_grow(|| {
            use MirRelationExpr as SR;

            use HirRelationExpr::*;

            if let MirRelationExpr::Get { .. } = &get_outer {
            } else {
                panic!(
                    "get_outer: expected a MirRelationExpr::Get, found {:?}",
                    get_outer
                );
            }
            assert_eq!(col_map.len(), get_outer.arity());
            Ok(match self {
                Constant { rows, typ } => {
                    // Constant expressions are not correlated with `get_outer`, and should be cross-products.
                    get_outer.product(SR::Constant {
                        rows: Ok(rows.into_iter().map(|row| (row, 1)).collect()),
                        typ,
                    })
                }
                Get { id, typ } => match id {
                    mz_expr::Id::Local(local_id) => {
                        let cte_desc = cte_map.get(&local_id).unwrap();
                        let get_cte = SR::Get {
                            id: mz_expr::Id::Local(cte_desc.new_id.clone()),
                            typ: cte_desc.relation_type.clone(),
                            access_strategy: AccessStrategy::UnknownOrLocal,
                        };
                        if get_outer == cte_desc.outer_relation {
                            // If the CTE was applied to the same exact relation, we can safely
                            // return a `Get` relation.
                            get_cte
                        } else {
                            // Otherwise, the new outer relation may contain more columns from some
                            // intermediate scope placed between the definition of the CTE and this
                            // reference of the CTE and/or more operations applied on top of the
                            // outer relation.
                            //
                            // An example of the latter is the following query:
                            //
                            // SELECT *
                            // FROM x,
                            //      LATERAL(WITH a(m) as (SELECT max(y.a) FROM y WHERE y.a < x.a)
                            //              SELECT (SELECT m FROM a) FROM y) b;
                            //
                            // When the CTE is lowered, the outer relation is `Get x`. But then,
                            // the reference of the CTE is applied to `Distinct(Join(Get x, Get y), x.*)`
                            // which has the same cardinality as `Get x`.
                            //
                            // In any case, `get_outer` is guaranteed to contain the columns of the
                            // outer relation the CTE was applied to at its prefix. Since, we must
                            // return a relation containing `get_outer`'s column at the beginning,
                            // we must build a join between `get_outer` and `get_cte` on their common
                            // columns.
                            let oa = get_outer.arity();
                            let cte_outer_columns = cte_desc.relation_type.arity() - typ.arity();
                            let equivalences = (0..cte_outer_columns)
                                .map(|pos| {
                                    vec![
                                        MirScalarExpr::Column(pos),
                                        MirScalarExpr::Column(pos + oa),
                                    ]
                                })
                                .collect();

                            // Project out the second copy of the common between `get_outer` and
                            // `cte_desc.outer_relation`.
                            let projection = (0..oa)
                                .chain(oa + cte_outer_columns..oa + cte_outer_columns + typ.arity())
                                .collect_vec();
                            SR::join_scalars(vec![get_outer, get_cte], equivalences)
                                .project(projection)
                        }
                    }
                    _ => {
                        // Get statements are only to external sources, and are not correlated with `get_outer`.
                        get_outer.product(SR::Get {
                            id,
                            typ,
                            access_strategy: AccessStrategy::UnknownOrLocal,
                        })
                    }
                },
                Let {
                    name: _,
                    id,
                    value,
                    body,
                } => {
                    let value =
                        value.applied_to(id_gen, get_outer.clone(), col_map, cte_map, context)?;
                    value.let_in_fallible(id_gen, |id_gen, get_value| {
                        let (new_id, typ) = if let MirRelationExpr::Get {
                            id: mz_expr::Id::Local(id),
                            typ,
                            ..
                        } = get_value
                        {
                            (id, typ)
                        } else {
                            panic!(
                                "get_value: expected a MirRelationExpr::Get with local Id, found {:?}",
                                get_value
                            );
                        };
                        // Add the information about the CTE to the map and remove it when
                        // it goes out of scope.
                        let old_value = cte_map.insert(
                            id.clone(),
                            CteDesc {
                                new_id,
                                relation_type: typ,
                                outer_relation: get_outer.clone(),
                            },
                        );
                        let body = body.applied_to(id_gen, get_outer, col_map, cte_map, context);
                        if let Some(old_value) = old_value {
                            cte_map.insert(id, old_value);
                        } else {
                            cte_map.remove(&id);
                        }
                        body
                    })?
                }
                LetRec {
                    limit,
                    bindings,
                    body,
                } => {
                    let num_bindings = bindings.len();

                    // We use the outer type with the HIR types to form MIR CTE types.
                    let outer_column_types = get_outer.typ().column_types;

                    // Rename and introduce all bindings.
                    let mut shadowed_bindings = Vec::with_capacity(num_bindings);
                    let mut mir_ids = Vec::with_capacity(num_bindings);
                    for (_name, id, _value, typ) in bindings.iter() {
                        let mir_id = mz_expr::LocalId::new(id_gen.allocate_id());
                        mir_ids.push(mir_id);
                        let shadowed = cte_map.insert(
                            id.clone(),
                            CteDesc {
                                new_id: mir_id,
                                relation_type: RelationType::new(
                                    outer_column_types
                                        .iter()
                                        .cloned()
                                        .chain(typ.column_types.iter().cloned())
                                        .collect::<Vec<_>>(),
                                ),
                                outer_relation: get_outer.clone(),
                            },
                        );
                        shadowed_bindings.push((*id, shadowed));
                    }

                    let mut mir_values = Vec::with_capacity(num_bindings);
                    for (_name, _id, value, _typ) in bindings.into_iter() {
                        mir_values.push(value.applied_to(
                            id_gen,
                            get_outer.clone(),
                            col_map,
                            cte_map,
                            context,
                        )?);
                    }

                    let mir_body = body.applied_to(id_gen, get_outer, col_map, cte_map, context)?;

                    // Remove our bindings and reinstate any shadowed bindings.
                    for (id, shadowed) in shadowed_bindings {
                        if let Some(shadowed) = shadowed {
                            cte_map.insert(id, shadowed);
                        } else {
                            cte_map.remove(&id);
                        }
                    }

                    MirRelationExpr::LetRec {
                        ids: mir_ids,
                        values: mir_values,
                        // Copy the limit to each binding.
                        limits: repeat(limit).take(num_bindings).collect(),
                        body: Box::new(mir_body),
                    }
                }
                Project { input, outputs } => {
                    // Projections should be applied to the decorrelated `inner`, and to its columns,
                    // which means rebasing `outputs` to start `get_outer.arity()` columns later.
                    let input =
                        input.applied_to(id_gen, get_outer.clone(), col_map, cte_map, context)?;
                    let outputs = (0..get_outer.arity())
                        .chain(outputs.into_iter().map(|i| get_outer.arity() + i))
                        .collect::<Vec<_>>();
                    input.project(outputs)
                }
                Map { input, mut scalars } => {
                    // Scalar expressions may contain correlated subqueries. We must be cautious!

                    // We lower scalars in chunks, and must keep track of the
                    // arity of the HIR fragments lowered so far.
                    let mut lowered_arity = input.arity();

                    let mut input =
                        input.applied_to(id_gen, get_outer, col_map, cte_map, context)?;

                    // Lower subqueries in maximally sized batches, such as no subquery in the current
                    // batch depends on columns from the same batch.
                    // Note that subqueries in this projection may reference columns added by this
                    // Map operator, so we need to ensure these columns exist before lowering the
                    // subquery.
                    while !scalars.is_empty() {
                        let end_idx = scalars
                            .iter_mut()
                            .position(|s| {
                                let mut requires_nonexistent_column = false;
                                #[allow(deprecated)]
                                s.visit_columns(0, &mut |depth, col| {
                                    if col.level == depth {
                                        requires_nonexistent_column |= col.column >= lowered_arity
                                    }
                                });
                                requires_nonexistent_column
                            })
                            .unwrap_or(scalars.len());
                        assert!(end_idx > 0, "a Map expression references itself or a later column; lowered_arity: {}, expressions: {:?}", lowered_arity, scalars);

                        lowered_arity = lowered_arity + end_idx;
                        let scalars = scalars.drain(0..end_idx).collect_vec();

                        let old_arity = input.arity();
                        let (with_subqueries, subquery_map) = HirScalarExpr::lower_subqueries(
                            &scalars, id_gen, col_map, cte_map, input, context,
                        )?;
                        input = with_subqueries;

                        // We will proceed sequentially through the scalar expressions, for each transforming
                        // the decorrelated `input` into a relation with potentially more columns capable of
                        // addressing the needs of the scalar expression.
                        // Having done so, we add the scalar value of interest and trim off any other newly
                        // added columns.
                        //
                        // The sequential traversal is present as expressions are allowed to depend on the
                        // values of prior expressions.
                        let mut scalar_columns = Vec::new();
                        for scalar in scalars {
                            let scalar = scalar.applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                &mut input,
                                &Some(&subquery_map),
                                context,
                            )?;
                            input = input.map_one(scalar);
                            scalar_columns.push(input.arity() - 1);
                        }

                        // Discard any new columns added by the lowering of the scalar expressions
                        input = input.project((0..old_arity).chain(scalar_columns).collect());
                    }

                    input
                }
                CallTable { func, exprs } => {
                    // FlatMap expressions may contain correlated subqueries. Unlike Map they are not
                    // allowed to refer to the results of previous expressions, and we have a simpler
                    // implementation that appends all relevant columns first, then applies the flatmap
                    // operator to the result, then strips off any columns introduce by subqueries.

                    let mut input = get_outer;
                    let old_arity = input.arity();

                    let exprs = exprs
                        .into_iter()
                        .map(|e| e.applied_to(id_gen, col_map, cte_map, &mut input, &None, context))
                        .collect::<Result<Vec<_>, _>>()?;

                    let new_arity = input.arity();
                    let output_arity = func.output_arity();
                    input = input.flat_map(func, exprs);
                    if old_arity != new_arity {
                        // this means we added some columns to handle subqueries, and now we need to get rid of them
                        input = input.project(
                            (0..old_arity)
                                .chain(new_arity..new_arity + output_arity)
                                .collect(),
                        );
                    }
                    input
                }
                Filter { input, predicates } => {
                    // Filter expressions may contain correlated subqueries.
                    // We extend `get_outer` with sufficient values to determine the value of the predicate,
                    // then filter the results, then strip off any columns that were added for this purpose.
                    let mut input =
                        input.applied_to(id_gen, get_outer, col_map, cte_map, context)?;
                    for predicate in predicates {
                        let old_arity = input.arity();
                        let predicate = predicate
                            .applied_to(id_gen, col_map, cte_map, &mut input, &None, context)?;
                        let new_arity = input.arity();
                        input = input.filter(vec![predicate]);
                        if old_arity != new_arity {
                            // this means we added some columns to handle subqueries, and now we need to get rid of them
                            input = input.project((0..old_arity).collect());
                        }
                    }
                    input
                }
                Join {
                    left,
                    right,
                    on,
                    kind,
                } if right.is_correlated() => {
                    // A correlated join is a join in which the right expression has
                    // access to the columns in the left expression. It turns out
                    // this is *exactly* our branch operator, plus some additional
                    // null handling in the case of left joins. (Right and full
                    // lateral joins are not permitted.)
                    //
                    // As with normal joins, the `on` predicate may be correlated,
                    // and we treat it as a filter that follows the branch.

                    assert!(kind.can_be_correlated());

                    let left = left.applied_to(id_gen, get_outer, col_map, cte_map, context)?;
                    left.let_in_fallible(id_gen, |id_gen, get_left| {
                        let apply_requires_distinct_outer = false;
                        let mut join = branch(
                            id_gen,
                            get_left.clone(),
                            col_map,
                            cte_map,
                            *right,
                            apply_requires_distinct_outer,
                            context,
                            |id_gen, right, get_left, col_map, cte_map, context| {
                                right.applied_to(id_gen, get_left, col_map, cte_map, context)
                            },
                        )?;

                        // Plan the `on` predicate.
                        let old_arity = join.arity();
                        let on =
                            on.applied_to(id_gen, col_map, cte_map, &mut join, &None, context)?;
                        join = join.filter(vec![on]);
                        let new_arity = join.arity();
                        if old_arity != new_arity {
                            // This means we added some columns to handle
                            // subqueries, and now we need to get rid of them.
                            join = join.project((0..old_arity).collect());
                        }

                        // If a left join, reintroduce any rows from the left that
                        // are missing, with nulls filled in for the right columns.
                        if let JoinKind::LeftOuter { .. } = kind {
                            let default = join
                                .typ()
                                .column_types
                                .into_iter()
                                .skip(get_left.arity())
                                .map(|typ| (Datum::Null, typ.scalar_type))
                                .collect();
                            Ok::<_, PlanError>(get_left.lookup(id_gen, join, default))
                        } else {
                            Ok::<_, PlanError>(join)
                        }
                    })?
                }
                Join {
                    left,
                    right,
                    on,
                    kind,
                } => {
                    if context.config.enable_variadic_left_join_lowering {
                        // Attempt to extract a stack of left joins.
                        if let JoinKind::LeftOuter = kind {
                            let mut rights = vec![(&*right, &on)];
                            let mut left_test = &left;
                            while let Join {
                                left,
                                right,
                                on,
                                kind: JoinKind::LeftOuter,
                            } = &**left_test
                            {
                                rights.push((&**right, on));
                                left_test = left;
                            }
                            if rights.len() > 1 {
                                // Defensively clone `cte_map` as it may be mutated.
                                let cte_map_clone = cte_map.clone();
                                if let Ok(Some(magic)) = variadic_left::attempt_left_join_magic(
                                    left_test,
                                    rights,
                                    id_gen,
                                    get_outer.clone(),
                                    col_map,
                                    cte_map,
                                    context,
                                ) {
                                    return Ok(magic);
                                } else {
                                    cte_map.clone_from(&cte_map_clone);
                                }
                            }
                        }
                    }

                    // Both join expressions should be decorrelated, and then joined by their
                    // leading columns to form only those pairs corresponding to the same row
                    // of `get_outer`.
                    //
                    // The `on` predicate may contain correlated subqueries, and we treat it
                    // as though it was a filter, with the caveat that we also translate outer
                    // joins in this step. The post-filtration results need to be considered
                    // against the records present in the left and right (decorrelated) inputs,
                    // depending on the type of join.
                    let oa = get_outer.arity();
                    let left =
                        left.applied_to(id_gen, get_outer.clone(), col_map, cte_map, context)?;
                    let lt = left.typ().column_types.into_iter().skip(oa).collect_vec();
                    let la = lt.len();
                    left.let_in_fallible(id_gen, |id_gen, get_left| {
                        let right_col_map = col_map.enter_scope(0);
                        let right = right.applied_to(
                            id_gen,
                            get_outer.clone(),
                            &right_col_map,
                            cte_map,
                            context,
                        )?;
                        let rt = right.typ().column_types.into_iter().skip(oa).collect_vec();
                        let ra = rt.len();
                        right.let_in_fallible(id_gen, |id_gen, get_right| {
                            let mut product = SR::join(
                                vec![get_left.clone(), get_right.clone()],
                                (0..oa).map(|i| vec![(0, i), (1, i)]).collect(),
                            )
                            // Project away the repeated copy of get_outer's columns.
                            .project(
                                (0..(oa + la))
                                    .chain((oa + la + oa)..(oa + la + oa + ra))
                                    .collect(),
                            );

                            // Decorrelate and lower the `on` clause.
                            let on = on.applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                &mut product,
                                &None,
                                context,
                            )?;
                            // Collect the types of all subqueries appearing in
                            // the `on` clause. The subquery results were
                            // appended to `product` in the `on.applied_to(...)`
                            // call above.
                            let on_subquery_types = product
                                .typ()
                                .column_types
                                .drain(oa + la + ra..)
                                .collect_vec();
                            // Remember if `on` had any subqueries.
                            let on_has_subqueries = !on_subquery_types.is_empty();

                            // Attempt an efficient equijoin implementation, in which outer joins are
                            // more efficiently rendered than in general. This can return `None` if
                            // such a plan is not possible, for example if `on` does not describe an
                            // equijoin between columns of `left` and `right`.
                            if kind != JoinKind::Inner {
                                if let Some(joined) = attempt_outer_equijoin(
                                    get_left.clone(),
                                    get_right.clone(),
                                    on.clone(),
                                    on_subquery_types,
                                    kind.clone(),
                                    oa,
                                    id_gen,
                                    context,
                                )? {
                                    if let Some(metrics) = context.metrics {
                                        metrics.inc_outer_join_lowering("equi");
                                    }
                                    return Ok(joined);
                                }
                            }

                            // Otherwise, perform a more general join.
                            if let Some(metrics) = context.metrics {
                                metrics.inc_outer_join_lowering("general");
                            }
                            let mut join = product.filter(vec![on]);
                            if on_has_subqueries {
                                // This means that `on.applied_to(...)` appended
                                // some columns to handle subqueries, and now we
                                // need to get rid of them.
                                join = join.project((0..oa + la + ra).collect());
                            }
                            join.let_in_fallible(id_gen, |id_gen, get_join| {
                                let mut result = get_join.clone();
                                if let JoinKind::LeftOuter { .. } | JoinKind::FullOuter { .. } =
                                    kind
                                {
                                    let left_outer = get_left.clone().anti_lookup(
                                        id_gen,
                                        get_join.clone(),
                                        rt.into_iter()
                                            .map(|typ| (Datum::Null, typ.scalar_type))
                                            .collect(),
                                    );
                                    result = result.union(left_outer);
                                }
                                if let JoinKind::RightOuter | JoinKind::FullOuter = kind {
                                    let right_outer = get_right
                                        .clone()
                                        .anti_lookup(
                                            id_gen,
                                            get_join
                                                // need to swap left and right to make the anti_lookup work
                                                .project(
                                                    (0..oa)
                                                        .chain((oa + la)..(oa + la + ra))
                                                        .chain((oa)..(oa + la))
                                                        .collect(),
                                                ),
                                            lt.into_iter()
                                                .map(|typ| (Datum::Null, typ.scalar_type))
                                                .collect(),
                                        )
                                        // swap left and right back again
                                        .project(
                                            (0..oa)
                                                .chain((oa + ra)..(oa + ra + la))
                                                .chain((oa)..(oa + ra))
                                                .collect(),
                                        );
                                    result = result.union(right_outer);
                                }
                                Ok::<MirRelationExpr, PlanError>(result)
                            })
                        })
                    })?
                }
                Union { base, inputs } => {
                    // Union is uncomplicated.
                    SR::Union {
                        base: Box::new(base.applied_to(
                            id_gen,
                            get_outer.clone(),
                            col_map,
                            cte_map,
                            context,
                        )?),
                        inputs: inputs
                            .into_iter()
                            .map(|input| {
                                input.applied_to(
                                    id_gen,
                                    get_outer.clone(),
                                    col_map,
                                    cte_map,
                                    context,
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    }
                }
                Reduce {
                    input,
                    group_key,
                    aggregates,
                    expected_group_size,
                } => {
                    // Reduce may contain expressions with correlated subqueries.
                    // In addition, here an empty reduction key signifies that we need to supply default values
                    // in the case that there are no results (as in a SQL aggregation without an explicit GROUP BY).
                    let mut input =
                        input.applied_to(id_gen, get_outer.clone(), col_map, cte_map, context)?;
                    let applied_group_key = (0..get_outer.arity())
                        .chain(group_key.iter().map(|i| get_outer.arity() + i))
                        .collect();
                    let applied_aggregates = aggregates
                        .into_iter()
                        .map(|aggregate| {
                            aggregate.applied_to(id_gen, col_map, cte_map, &mut input, context)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let input_type = input.typ();
                    let default = applied_aggregates
                        .iter()
                        .map(|agg| {
                            (
                                agg.func.default(),
                                agg.typ(&input_type.column_types).scalar_type,
                            )
                        })
                        .collect();
                    // NOTE we don't need to remove any extra columns from aggregate.applied_to above because the reduce will do that anyway
                    let mut reduced =
                        input.reduce(applied_group_key, applied_aggregates, expected_group_size);

                    // Introduce default values in the case the group key is empty.
                    if group_key.is_empty() {
                        reduced = get_outer.lookup(id_gen, reduced, default);
                    }
                    reduced
                }
                Distinct { input } => {
                    // Distinct is uncomplicated.
                    input
                        .applied_to(id_gen, get_outer, col_map, cte_map, context)?
                        .distinct()
                }
                TopK {
                    input,
                    group_key,
                    order_key,
                    limit,
                    offset,
                    expected_group_size,
                } => {
                    // TopK is uncomplicated, except that we must group by the columns of `get_outer` as well.
                    let mut input =
                        input.applied_to(id_gen, get_outer.clone(), col_map, cte_map, context)?;
                    let mut applied_group_key: Vec<_> = (0..get_outer.arity())
                        .chain(group_key.iter().map(|i| get_outer.arity() + i))
                        .collect();
                    let applied_order_key = order_key
                        .iter()
                        .map(|column_order| ColumnOrder {
                            column: column_order.column + get_outer.arity(),
                            desc: column_order.desc,
                            nulls_last: column_order.nulls_last,
                        })
                        .collect();

                    let old_arity = input.arity();

                    // Lower `limit`, which may introduce new columns if is a correlated subquery.
                    let mut limit_mir = None;
                    if let Some(limit) = limit {
                        limit_mir = Some(
                            limit
                                .applied_to(id_gen, col_map, cte_map, &mut input, &None, context)?,
                        );
                    }

                    let new_arity = input.arity();
                    // Extend the key to contain any new columns.
                    applied_group_key.extend(old_arity..new_arity);

                    let mut result = input.top_k(
                        applied_group_key,
                        applied_order_key,
                        limit_mir,
                        offset,
                        expected_group_size,
                    );

                    // If new columns were added for `limit` we must remove them.
                    if old_arity != new_arity {
                        result = result.project((0..old_arity).collect());
                    }

                    result
                }
                Negate { input } => {
                    // Negate is uncomplicated.
                    input
                        .applied_to(id_gen, get_outer, col_map, cte_map, context)?
                        .negate()
                }
                Threshold { input } => {
                    // Threshold is uncomplicated.
                    input
                        .applied_to(id_gen, get_outer, col_map, cte_map, context)?
                        .threshold()
                }
            })
        })
    }
}

impl HirScalarExpr {
    /// Rewrite `self` into a `mz_expr::ScalarExpr` which can be applied to the modified `inner`.
    ///
    /// This method is responsible for decorrelating subqueries in `self` by introducing further columns
    /// to `inner`, and rewriting `self` to refer to its physical columns (specified by `usize` positions).
    /// The most complicated logic is for the scalar expressions that involve subqueries, each of which are
    /// documented in more detail closer to their logic.
    ///
    /// This process presumes that `inner` is the result of decorrelation, meaning its first several columns
    /// may be inherited from outer relations. The `col_map` column map should provide specific offsets where
    /// each of these references can be found.
    fn applied_to(
        self,
        id_gen: &mut mz_ore::id_gen::IdGen,
        col_map: &ColumnMap,
        cte_map: &mut CteMap,
        inner: &mut MirRelationExpr,
        subquery_map: &Option<&BTreeMap<HirScalarExpr, usize>>,
        context: &Context,
    ) -> Result<MirScalarExpr, PlanError> {
        maybe_grow(|| {
            use MirScalarExpr as SS;

            use HirScalarExpr::*;

            if let Some(subquery_map) = subquery_map {
                if let Some(col) = subquery_map.get(&self) {
                    return Ok(SS::Column(*col));
                }
            }

            Ok::<MirScalarExpr, PlanError>(match self {
                Column(col_ref) => SS::Column(col_map.get(&col_ref)),
                Literal(row, typ) => SS::Literal(Ok(row), typ),
                Parameter(_) => panic!("cannot decorrelate expression with unbound parameters"),
                CallUnmaterializable(func) => SS::CallUnmaterializable(func),
                CallUnary { func, expr } => SS::CallUnary {
                    func,
                    expr: Box::new(expr.applied_to(
                        id_gen,
                        col_map,
                        cte_map,
                        inner,
                        subquery_map,
                        context,
                    )?),
                },
                CallBinary { func, expr1, expr2 } => SS::CallBinary {
                    func,
                    expr1: Box::new(expr1.applied_to(
                        id_gen,
                        col_map,
                        cte_map,
                        inner,
                        subquery_map,
                        context,
                    )?),
                    expr2: Box::new(expr2.applied_to(
                        id_gen,
                        col_map,
                        cte_map,
                        inner,
                        subquery_map,
                        context,
                    )?),
                },
                CallVariadic { func, exprs } => SS::CallVariadic {
                    func,
                    exprs: exprs
                        .into_iter()
                        .map(|expr| {
                            expr.applied_to(id_gen, col_map, cte_map, inner, subquery_map, context)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                },
                If { cond, then, els } => {
                    // The `If` case is complicated by the fact that we do not want to
                    // apply the `then` or `else` logic to tuples that respectively do
                    // not or do pass the `cond` test. Our strategy is to independently
                    // decorrelate the `then` and `else` logic, and apply each to tuples
                    // that respectively pass and do not pass the `cond` logic (which is
                    // executed, and so decorrelated, for all tuples).
                    //
                    // Informally, we turn the `if` statement into:
                    //
                    //   let then_case = inner.filter(cond).map(then);
                    //   let else_case = inner.filter(!cond).map(else);
                    //   return then_case.concat(else_case);
                    //
                    // We only require this if either expression would result in any
                    // computation beyond the expr itself, which we will interpret as
                    // "introduces additional columns". In the absence of correlation,
                    // we should just retain a `ScalarExpr::If` expression; the inverse
                    // transformation as above is complicated to recover after the fact,
                    // and we would benefit from not introducing the complexity.

                    let inner_arity = inner.arity();
                    let cond_expr =
                        cond.applied_to(id_gen, col_map, cte_map, inner, subquery_map, context)?;

                    // Defensive copies, in case we mangle these in decorrelation.
                    let inner_clone = inner.clone();
                    let then_clone = then.clone();
                    let else_clone = els.clone();

                    let cond_arity = inner.arity();
                    let then_expr =
                        then.applied_to(id_gen, col_map, cte_map, inner, subquery_map, context)?;
                    let else_expr =
                        els.applied_to(id_gen, col_map, cte_map, inner, subquery_map, context)?;

                    if cond_arity == inner.arity() {
                        // If no additional columns were added, we simply return the
                        // `If` variant with the updated expressions.
                        SS::If {
                            cond: Box::new(cond_expr),
                            then: Box::new(then_expr),
                            els: Box::new(else_expr),
                        }
                    } else {
                        // If columns were added, we need a more careful approach, as
                        // described above. First, we need to de-correlate each of
                        // the two expressions independently, and apply their cases
                        // as `MirRelationExpr::Map` operations.

                        *inner = inner_clone.let_in_fallible(id_gen, |id_gen, get_inner| {
                            // Restrict to records satisfying `cond_expr` and apply `then` as a map.
                            let mut then_inner = get_inner.clone().filter(vec![cond_expr.clone()]);
                            let then_expr = then_clone.applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                &mut then_inner,
                                subquery_map,
                                context,
                            )?;
                            let then_arity = then_inner.arity();
                            then_inner = then_inner
                                .map_one(then_expr)
                                .project((0..inner_arity).chain(Some(then_arity)).collect());

                            // Restrict to records not satisfying `cond_expr` and apply `els` as a map.
                            let mut else_inner = get_inner.filter(vec![SS::CallVariadic {
                                func: mz_expr::VariadicFunc::Or,
                                exprs: vec![
                                    cond_expr
                                        .clone()
                                        .call_binary(SS::literal_false(), mz_expr::BinaryFunc::Eq),
                                    cond_expr.clone().call_is_null(),
                                ],
                            }]);
                            let else_expr = else_clone.applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                &mut else_inner,
                                subquery_map,
                                context,
                            )?;
                            let else_arity = else_inner.arity();
                            else_inner = else_inner
                                .map_one(else_expr)
                                .project((0..inner_arity).chain(Some(else_arity)).collect());

                            // concatenate the two results.
                            Ok::<MirRelationExpr, PlanError>(then_inner.union(else_inner))
                        })?;

                        SS::Column(inner_arity)
                    }
                }

                // Subqueries!
                // These are surprisingly subtle. Things to be careful of:

                // Anything in the subquery that cares about row counts (Reduce/Distinct/Negate/Threshold) must not:
                // * change the row counts of the outer query
                // * accidentally compute its own value using the row counts of the outer query
                // Use `branch` to calculate the subquery once for each __distinct__ key in the outer
                // query and then join the answers back on to the original rows of the outer query.

                // When the subquery would return 0 rows for some row in the outer query, `subquery.applied_to(get_inner)` will not have any corresponding row.
                // Use `lookup` if you need to add default values for cases when the subquery returns 0 rows.
                Exists(expr) => {
                    let apply_requires_distinct_outer = true;
                    *inner = apply_existential_subquery(
                        id_gen,
                        inner.take_dangerous(),
                        col_map,
                        cte_map,
                        *expr,
                        apply_requires_distinct_outer,
                        context,
                    )?;
                    SS::Column(inner.arity() - 1)
                }

                Select(expr) => {
                    let apply_requires_distinct_outer = true;
                    *inner = apply_scalar_subquery(
                        id_gen,
                        inner.take_dangerous(),
                        col_map,
                        cte_map,
                        *expr,
                        apply_requires_distinct_outer,
                        context,
                    )?;
                    SS::Column(inner.arity() - 1)
                }
                Windowing(expr) => {
                    let partition_by = expr.partition_by;
                    let order_by = expr.order_by;

                    // argument lowering for scalar window functions
                    // (We need to specify the & _ in the arguments because of this problem:
                    // https://users.rust-lang.org/t/the-implementation-of-fnonce-is-not-general-enough/72141/3 )
                    let scalar_lower_args =
                        |_id_gen: &mut _,
                         _col_map: &_,
                         _cte_map: &mut _,
                         _get_inner: &mut _,
                         _subquery_map: &Option<&_>,
                         order_by_mir: Vec<MirScalarExpr>,
                         original_row_record,
                         original_row_record_type: ScalarType| {
                            let agg_input = MirScalarExpr::CallVariadic {
                                func: mz_expr::VariadicFunc::ListCreate {
                                    elem_type: original_row_record_type.clone(),
                                },
                                exprs: vec![original_row_record],
                            };
                            let mut agg_input = vec![agg_input];
                            agg_input.extend(order_by_mir.clone());
                            let agg_input = MirScalarExpr::CallVariadic {
                                func: mz_expr::VariadicFunc::RecordCreate {
                                    field_names: (0..agg_input.len())
                                        .map(|_| ColumnName::from("?column?"))
                                        .collect_vec(),
                                },
                                exprs: agg_input,
                            };
                            let list_type = ScalarType::List {
                                element_type: Box::new(original_row_record_type),
                                custom_id: None,
                            };
                            let agg_input_type = ScalarType::Record {
                                fields: std::iter::once(&list_type)
                                    .map(|t| {
                                        (ColumnName::from("?column?"), t.clone().nullable(false))
                                    })
                                    .collect_vec(),
                                custom_id: None,
                            }
                            .nullable(false);

                            Ok((agg_input, agg_input_type))
                        };

                    // argument lowering for value window functions and aggregate window functions
                    let value_or_aggr_lower_args = |hir_encoded_args: Box<HirScalarExpr>| {
                        |id_gen: &mut _,
                         col_map: &_,
                         cte_map: &mut _,
                         get_inner: &mut _,
                         subquery_map: &Option<&_>,
                         order_by_mir: Vec<MirScalarExpr>,
                         original_row_record,
                         original_row_record_type| {
                            // Creates [((OriginalRow, EncodedArgs), OrderByExprs...)]

                            // Compute the encoded args for all rows
                            let mir_encoded_args = hir_encoded_args.applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                get_inner,
                                subquery_map,
                                context,
                            )?;
                            let mir_encoded_args_type = mir_encoded_args
                                .typ(&get_inner.typ().column_types)
                                .scalar_type;

                            // Build a new record that has two fields:
                            // 1. the original row in a record
                            // 2. the encoded args (which can be either a single value, or a record
                            //    if the window function has multiple arguments, such as `lag`)
                            let fn_input_record_fields =
                                [original_row_record_type, mir_encoded_args_type]
                                    .iter()
                                    .map(|t| {
                                        (ColumnName::from("?column?"), t.clone().nullable(false))
                                    })
                                    .collect_vec();
                            let fn_input_record = MirScalarExpr::CallVariadic {
                                func: mz_expr::VariadicFunc::RecordCreate {
                                    field_names: fn_input_record_fields
                                        .iter()
                                        .map(|(n, _)| n.clone())
                                        .collect_vec(),
                                },
                                exprs: vec![original_row_record, mir_encoded_args],
                            };
                            let fn_input_record_type = ScalarType::Record {
                                fields: fn_input_record_fields,
                                custom_id: None,
                            }
                            .nullable(false);

                            // Build a new record with the record above + the ORDER BY exprs
                            // This follows the standard encoding of ORDER BY exprs used by aggregate functions
                            let mut agg_input = vec![fn_input_record];
                            agg_input.extend(order_by_mir.clone());
                            let agg_input = MirScalarExpr::CallVariadic {
                                func: mz_expr::VariadicFunc::RecordCreate {
                                    field_names: (0..agg_input.len())
                                        .map(|_| ColumnName::from("?column?"))
                                        .collect_vec(),
                                },
                                exprs: agg_input,
                            };

                            let agg_input_type = ScalarType::Record {
                                fields: vec![(
                                    ColumnName::from("?column?"),
                                    fn_input_record_type.nullable(false),
                                )],
                                custom_id: None,
                            }
                            .nullable(false);

                            Ok((agg_input, agg_input_type))
                        }
                    };

                    match expr.func {
                        WindowExprType::Scalar(scalar_window_expr) => {
                            let mir_aggr_func = scalar_window_expr.into_expr();
                            Self::window_func_applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                inner,
                                subquery_map,
                                partition_by,
                                order_by,
                                mir_aggr_func,
                                scalar_lower_args,
                                context,
                            )?
                        }
                        WindowExprType::Value(value_window_expr) => {
                            let (hir_encoded_args, mir_aggr_func) = value_window_expr.into_expr();

                            Self::window_func_applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                inner,
                                subquery_map,
                                partition_by,
                                order_by,
                                mir_aggr_func,
                                value_or_aggr_lower_args(hir_encoded_args),
                                context,
                            )?
                        }
                        WindowExprType::Aggregate(aggr_window_expr) => {
                            let (hir_encoded_args, mir_aggr_func) = aggr_window_expr.into_expr();

                            Self::window_func_applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                inner,
                                subquery_map,
                                partition_by,
                                order_by,
                                mir_aggr_func,
                                value_or_aggr_lower_args(hir_encoded_args),
                                context,
                            )?
                        }
                    }
                }
            })
        })
    }

    fn window_func_applied_to<F>(
        id_gen: &mut mz_ore::id_gen::IdGen,
        col_map: &ColumnMap,
        cte_map: &mut CteMap,
        inner: &mut MirRelationExpr,
        subquery_map: &Option<&BTreeMap<HirScalarExpr, usize>>,
        partition_by: Vec<HirScalarExpr>,
        order_by: Vec<HirScalarExpr>,
        mir_aggr_func: AggregateFunc,
        lower_args: F,
        context: &Context,
    ) -> Result<MirScalarExpr, PlanError>
    where
        F: FnOnce(
            &mut mz_ore::id_gen::IdGen,
            &ColumnMap,
            &mut CteMap,
            &mut MirRelationExpr,
            &Option<&BTreeMap<HirScalarExpr, usize>>,
            Vec<MirScalarExpr>,
            MirScalarExpr,
            ScalarType,
        ) -> Result<(MirScalarExpr, ColumnType), PlanError>,
    {
        // Example MIRs for a window function (specifically, a window aggregation):
        //
        // CREATE TABLE t7(x INT, y INT);
        //
        // explain decorrelated plan for select sum(x*y) over (partition by x+y order by x-y, x/y) from t7;
        //
        // Decorrelated Plan
        // Project (#3)
        //   Map (#2)
        //     Project (#3..=#5)
        //       Map (record_get[0](record_get[1](#2)), record_get[1](record_get[1](#2)), record_get[0](#2))
        //         FlatMap unnest_list(#1)
        //           Reduce group_by=[#2] aggregates=[window_agg[sum order_by=[#0 asc nulls_last, #1 asc nulls_last]](row(row(row(#0, #1), (#0 * #1)), (#0 - #1), (#0 / #1)))]
        //             Map ((#0 + #1))
        //               CrossJoin
        //                 Constant
        //                   - ()
        //                 Get materialize.public.t7
        //
        // The same query after optimizations:
        //
        // explain select sum(x*y) over (partition by x+y order by x-y, x/y) from t7;
        //
        // Optimized Plan
        // Explained Query:
        //   Project (#2)
        //     Map (record_get[0](#1))
        //       FlatMap unnest_list(#0)
        //         Project (#1)
        //           Reduce group_by=[(#0 + #1)] aggregates=[window_agg[sum order_by=[#0 asc nulls_last, #1 asc nulls_last]](row(row(row(#0, #1), (#0 * #1)), (#0 - #1), (#0 / #1)))]
        //             ReadStorage materialize.public.t7
        //
        // The `row(row(row(...), ...), ...)` stuff means the following:
        // `row(row(row(<original row>), <arguments to window function>), <order by values>...)`
        //   - The <arguments to window function> can be either a single value or itself a
        //     `row` if there are multiple arguments.
        //   - The <order by values> are _not_ wrapped in a `row`, even if there are more than one
        //     ORDER BY columns.
        //   - The <original row> currently always captures the entire original row. This should
        //     improve when we make `ProjectionPushdown` smarter, see
        //     https://github.com/MaterializeInc/materialize/issues/17522
        //
        // TODO:
        // We should probably introduce some dedicated Datum constructor functions instead of `row`
        // to make MIR plans and MIR construction/manipulation code more readable. Additionally, we
        // might even introduce dedicated Datum enum variants, so that the rendering code also
        // becomes more readable (and possibly slightly more performant).

        *inner = inner
            .take_dangerous()
            .let_in_fallible(id_gen, |id_gen, mut get_inner| {
                let order_by_mir = order_by
                    .into_iter()
                    .map(|o| {
                        o.applied_to(
                            id_gen,
                            col_map,
                            cte_map,
                            &mut get_inner,
                            subquery_map,
                            context,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                // Record input arity here so that any group_keys that need to mutate get_inner
                // don't add those columns to the aggregate input.
                let input_arity = get_inner.typ().arity();
                // The reduction that computes the window function must be keyed on the columns
                // from the outer context, plus the expressions in the partition key. The current
                // subquery will be 'executed' for every distinct row from the outer context so
                // by putting the outer columns in the grouping key we isolate each re-execution.
                let mut group_key = col_map
                    .inner
                    .iter()
                    .map(|(_, outer_col)| *outer_col)
                    .sorted()
                    .collect_vec();
                for p in partition_by {
                    let key = p.applied_to(
                        id_gen,
                        col_map,
                        cte_map,
                        &mut get_inner,
                        subquery_map,
                        context,
                    )?;
                    if let MirScalarExpr::Column(c) = key {
                        group_key.push(c);
                    } else {
                        get_inner = get_inner.map_one(key);
                        group_key.push(get_inner.arity() - 1);
                    }
                }

                get_inner.let_in_fallible(id_gen, |id_gen, mut get_inner| {
                    let input_type = get_inner.typ();

                    // Original columns of the relation
                    let fields = input_type
                        .column_types
                        .iter()
                        .take(input_arity)
                        .map(|t| (ColumnName::from("?column?"), t.clone()))
                        .collect_vec();

                    // Original row made into a record
                    let original_row_record = MirScalarExpr::CallVariadic {
                        func: mz_expr::VariadicFunc::RecordCreate {
                            field_names: fields.iter().map(|(name, _)| name.clone()).collect_vec(),
                        },
                        exprs: (0..input_arity).map(MirScalarExpr::Column).collect_vec(),
                    };
                    let original_row_record_type = ScalarType::Record {
                        fields,
                        custom_id: None,
                    };

                    let (agg_input, agg_input_type) = lower_args(
                        id_gen,
                        col_map,
                        cte_map,
                        &mut get_inner,
                        subquery_map,
                        order_by_mir,
                        original_row_record,
                        original_row_record_type,
                    )?;

                    let aggregate = mz_expr::AggregateExpr {
                        func: mir_aggr_func,
                        expr: agg_input,
                        distinct: false,
                    };

                    // Actually call reduce with the window function
                    // The output of the aggregation function should be a list of tuples that has
                    // the result in the first position, and the original row in the second position
                    let mut reduce = get_inner
                        .reduce(group_key.clone(), vec![aggregate.clone()], None)
                        .flat_map(
                            mz_expr::TableFunc::UnnestList {
                                el_typ: aggregate
                                    .func
                                    .output_type(agg_input_type)
                                    .scalar_type
                                    .unwrap_list_element_type()
                                    .clone(),
                            },
                            vec![MirScalarExpr::Column(group_key.len())],
                        );
                    let record_col = reduce.arity() - 1;

                    // Unpack the record output by the window function
                    for c in 0..input_arity {
                        reduce = reduce.take_dangerous().map_one(MirScalarExpr::CallUnary {
                            func: mz_expr::UnaryFunc::RecordGet(mz_expr::func::RecordGet(c)),
                            expr: Box::new(MirScalarExpr::CallUnary {
                                func: mz_expr::UnaryFunc::RecordGet(mz_expr::func::RecordGet(1)),
                                expr: Box::new(MirScalarExpr::Column(record_col)),
                            }),
                        });
                    }

                    // Append the column with the result of the window function.
                    reduce = reduce.take_dangerous().map_one(MirScalarExpr::CallUnary {
                        func: mz_expr::UnaryFunc::RecordGet(mz_expr::func::RecordGet(0)),
                        expr: Box::new(MirScalarExpr::Column(record_col)),
                    });

                    let agg_col = record_col + 1 + input_arity;
                    Ok::<_, PlanError>(reduce.project((record_col + 1..agg_col + 1).collect_vec()))
                })
            })?;
        Ok(MirScalarExpr::Column(inner.arity() - 1))
    }

    /// Applies the subqueries in the given list of scalar expressions to every distinct
    /// value of the given relation and returns a join of the given relation with all
    /// the subqueries found, and the mapping of scalar expressions with columns projected
    /// by the returned join that will hold their results.
    fn lower_subqueries(
        exprs: &[Self],
        id_gen: &mut mz_ore::id_gen::IdGen,
        col_map: &ColumnMap,
        cte_map: &mut CteMap,
        inner: MirRelationExpr,
        context: &Context,
    ) -> Result<(MirRelationExpr, BTreeMap<HirScalarExpr, usize>), PlanError> {
        let mut subquery_map = BTreeMap::new();
        let output = inner.let_in_fallible(id_gen, |id_gen, get_inner| {
            let mut subqueries = Vec::new();
            let distinct_inner = get_inner.clone().distinct();
            for expr in exprs.iter() {
                #[allow(deprecated)]
                expr.visit_pre_post(
                    &mut |e| match e {
                        // For simplicity, subqueries within a conditional statement will be
                        // lowered when lowering the conditional expression.
                        HirScalarExpr::If { .. } => Some(vec![]),
                        _ => None,
                    },
                    &mut |e| match e {
                        HirScalarExpr::Select(expr) => {
                            let apply_requires_distinct_outer = false;
                            let subquery = apply_scalar_subquery(
                                id_gen,
                                distinct_inner.clone(),
                                col_map,
                                cte_map,
                                (**expr).clone(),
                                apply_requires_distinct_outer,
                                context,
                            )
                            .unwrap();

                            subqueries.push((e.clone(), subquery));
                        }
                        HirScalarExpr::Exists(expr) => {
                            let apply_requires_distinct_outer = false;
                            let subquery = apply_existential_subquery(
                                id_gen,
                                distinct_inner.clone(),
                                col_map,
                                cte_map,
                                (**expr).clone(),
                                apply_requires_distinct_outer,
                                context,
                            )
                            .unwrap();
                            subqueries.push((e.clone(), subquery));
                        }
                        _ => {}
                    },
                );
            }

            if subqueries.is_empty() {
                Ok::<MirRelationExpr, PlanError>(get_inner)
            } else {
                let inner_arity = get_inner.arity();
                let mut total_arity = inner_arity;
                let mut join_inputs = vec![get_inner];
                for (expr, subquery) in subqueries.into_iter() {
                    // Avoid lowering duplicated subqueries
                    if !subquery_map.contains_key(&expr) {
                        let subquery_arity = subquery.arity();
                        assert_eq!(subquery_arity, inner_arity + 1);
                        join_inputs.push(subquery);
                        total_arity += subquery_arity;

                        // Column with the value of the subquery
                        subquery_map.insert(expr, total_arity - 1);
                    }
                }
                // Each subquery projects all the columns of the outer context (distinct_inner)
                // plus 1 column, containing the result of the subquery. Those columns must be
                // joined with the outer/main relation (get_inner).
                let input_mapper = mz_expr::JoinInputMapper::new(&join_inputs);
                let equivalences = (0..inner_arity)
                    .map(|col| {
                        join_inputs
                            .iter()
                            .enumerate()
                            .map(|(input, _)| {
                                MirScalarExpr::Column(input_mapper.map_column_to_global(col, input))
                            })
                            .collect_vec()
                    })
                    .collect_vec();
                Ok(MirRelationExpr::join_scalars(join_inputs, equivalences))
            }
        })?;
        Ok((output, subquery_map))
    }

    /// Rewrites `self` into a `mz_expr::ScalarExpr`.
    pub fn lower_uncorrelated(self) -> Result<MirScalarExpr, PlanError> {
        use MirScalarExpr as SS;

        use HirScalarExpr::*;

        Ok(match self {
            Column(ColumnRef { level: 0, column }) => SS::Column(column),
            Literal(datum, typ) => SS::Literal(Ok(datum), typ),
            CallUnmaterializable(func) => SS::CallUnmaterializable(func),
            CallUnary { func, expr } => SS::CallUnary {
                func,
                expr: Box::new(expr.lower_uncorrelated()?),
            },
            CallBinary { func, expr1, expr2 } => SS::CallBinary {
                func,
                expr1: Box::new(expr1.lower_uncorrelated()?),
                expr2: Box::new(expr2.lower_uncorrelated()?),
            },
            CallVariadic { func, exprs } => SS::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| expr.lower_uncorrelated())
                    .collect::<Result<_, _>>()?,
            },
            If { cond, then, els } => SS::If {
                cond: Box::new(cond.lower_uncorrelated()?),
                then: Box::new(then.lower_uncorrelated()?),
                els: Box::new(els.lower_uncorrelated()?),
            },
            Select { .. } | Exists { .. } | Parameter(..) | Column(..) | Windowing(..) => {
                sql_bail!("unexpected ScalarExpr in uncorrelated plan: {:?}", self);
            }
        })
    }
}

/// Prepare to apply `inner` to `outer`. Note that `inner` is a correlated (SQL)
/// expression, while `outer` is a non-correlated (dataflow) expression. `inner`
/// will, in effect, be executed once for every distinct row in `outer`, and the
/// results will be joined with `outer`. Note that columns in `outer` that are
/// not depended upon by `inner` are thrown away before the distinct, so that we
/// don't perform needless computation of `inner`.
///
/// `branch` will inspect the contents of `inner` to determine whether `inner`
/// is not multiplicity sensitive (roughly, contains only maps, filters,
/// projections, and calls to table functions). If it is not multiplicity
/// sensitive, `branch` will *not* distinctify outer. If this is problematic,
/// e.g. because the `apply` callback itself introduces multiplicity-sensitive
/// operations that were not present in `inner`, then set
/// `apply_requires_distinct_outer` to ensure that `branch` chooses the plan
/// that distinctifies `outer`.
///
/// The caller must supply the `apply` function that applies the rewritten
/// `inner` to `outer`.
fn branch<F>(
    id_gen: &mut mz_ore::id_gen::IdGen,
    outer: MirRelationExpr,
    col_map: &ColumnMap,
    cte_map: &mut CteMap,
    inner: HirRelationExpr,
    apply_requires_distinct_outer: bool,
    context: &Context,
    apply: F,
) -> Result<MirRelationExpr, PlanError>
where
    F: FnOnce(
        &mut mz_ore::id_gen::IdGen,
        HirRelationExpr,
        MirRelationExpr,
        &ColumnMap,
        &mut CteMap,
        &Context,
    ) -> Result<MirRelationExpr, PlanError>,
{
    // TODO: It would be nice to have a version of this code w/o optimizations,
    // at the least for purposes of understanding. It was difficult for one reader
    // to understand the required properties of `outer` and `col_map`.

    // If the inner expression is sufficiently simple, it is safe to apply it
    // *directly* to outer, rather than applying it to the distinctified key
    // (see below).
    //
    // As an example, consider the following two queries:
    //
    //     CREATE TABLE t (a int, b int);
    //     SELECT a, series FROM t, generate_series(1, t.b) series;
    //
    // The "simple" path for the `SELECT` yields
    //
    //     %0 =
    //     | Get t
    //     | FlatMap generate_series(1, #1)
    //
    // while the non-simple path yields:
    //
    //    %0 =
    //    | Get t
    //
    //    %1 =
    //    | Get t
    //    | Distinct group=(#1)
    //    | FlatMap generate_series(1, #0)
    //
    //    %2 =
    //    | LeftJoin %1 %2 (= #1 #2)
    //
    // There is a tradeoff here: the simple plan is stateless, but the non-
    // simple plan may do (much) less computation if there are only a few
    // distinct values of `t.b`.
    //
    // We apply a very simple heuristic here and take the simple path if `inner`
    // contains only maps, filters, projections, and calls to table functions.
    // The intuition is that straightforward usage of table functions should
    // take the simple path, while everything else should not. (In theory we
    // think this transformation is valid as long as `inner` does not contain a
    // Reduce, Distinct, or TopK node, but it is not always an optimization in
    // the general case.)
    //
    // TODO(benesch): this should all be handled by a proper optimizer, but
    // detecting the moment of decorrelation in the optimizer right now is too
    // hard.
    let mut is_simple = true;
    #[allow(deprecated)]
    inner.visit(0, &mut |expr, _| match expr {
        HirRelationExpr::Constant { .. }
        | HirRelationExpr::Project { .. }
        | HirRelationExpr::Map { .. }
        | HirRelationExpr::Filter { .. }
        | HirRelationExpr::CallTable { .. } => (),
        _ => is_simple = false,
    });
    if is_simple && !apply_requires_distinct_outer {
        let new_col_map = col_map.enter_scope(outer.arity() - col_map.len());
        return outer.let_in_fallible(id_gen, |id_gen, get_outer| {
            apply(id_gen, inner, get_outer, &new_col_map, cte_map, context)
        });
    }

    // The key consists of the columns from the outer expression upon which the
    // inner relation depends. We discover these dependencies by walking the
    // inner relation expression and looking for column references whose level
    // escapes inner.
    //
    // At the end of this process, `key` contains the decorrelated position of
    // each outer column, according to the passed-in `col_map`, and
    // `new_col_map` maps each outer column to its new ordinal position in key.
    let mut outer_cols = BTreeSet::new();
    #[allow(deprecated)]
    inner.visit_columns(0, &mut |depth, col| {
        // Test if the column reference escapes the subquery.
        if col.level > depth {
            outer_cols.insert(ColumnRef {
                level: col.level - depth,
                column: col.column,
            });
        }
    });
    // Collect all the outer columns referenced by any CTE referenced by
    // the inner relation.
    #[allow(deprecated)]
    inner.visit(0, &mut |e, _| match e {
        HirRelationExpr::Get {
            id: mz_expr::Id::Local(id),
            ..
        } => {
            if let Some(cte_desc) = cte_map.get(id) {
                let cte_outer_arity = cte_desc.outer_relation.arity();
                outer_cols.extend(
                    col_map
                        .inner
                        .iter()
                        .filter(|(_, position)| **position < cte_outer_arity)
                        .map(|(c, _)| {
                            // `col_map` maps column references to column positions in
                            // `outer`'s projection.
                            // `outer_cols` is meant to contain the external column
                            // references in `inner`.
                            // Since `inner` defines a new scope, any column reference
                            // in `col_map` is one level deeper when seen from within
                            // `inner`, hence the +1.
                            ColumnRef {
                                level: c.level + 1,
                                column: c.column,
                            }
                        }),
                );
            }
        }
        HirRelationExpr::Let { id, .. } => {
            // Note: if ID uniqueness is not guaranteed, we can't use `visit` since
            // we would need to remove the old CTE with the same ID temporarily while
            // traversing the definition of the new CTE under the same ID.
            assert!(!cte_map.contains_key(id));
        }
        _ => {}
    });
    let mut new_col_map = BTreeMap::new();
    let mut key = vec![];
    for col in outer_cols {
        new_col_map.insert(col, key.len());
        key.push(col_map.get(&ColumnRef {
            // Note: `outer_cols` contains the external column references within `inner`.
            // We must compensate for `inner`'s scope when translating column references
            // as seen within `inner` to column references as seen from `outer`'s context,
            // hence the -1.
            level: col.level - 1,
            column: col.column,
        }));
    }
    let new_col_map = ColumnMap::new(new_col_map);
    outer.let_in_fallible(id_gen, |id_gen, get_outer| {
        let keyed_outer = if key.is_empty() {
            // Don't depend on outer at all if the branch is not correlated,
            // which yields vastly better query plans. Note that this is a bit
            // weird in that the branch will be computed even if outer has no
            // rows, whereas if it had been correlated it would not (and *could*
            // not) have been computed if outer had no rows, but the callers of
            // this function don't mind these somewhat-weird semantics.
            MirRelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
        } else {
            get_outer.clone().distinct_by(key.clone())
        };
        keyed_outer.let_in_fallible(id_gen, |id_gen, get_keyed_outer| {
            let oa = get_outer.arity();
            let branch = apply(
                id_gen,
                inner,
                get_keyed_outer,
                &new_col_map,
                cte_map,
                context,
            )?;
            let ba = branch.arity();
            let joined = MirRelationExpr::join(
                vec![get_outer.clone(), branch],
                key.iter()
                    .enumerate()
                    .map(|(i, &k)| vec![(0, k), (1, i)])
                    .collect(),
            )
            // throw away the right-hand copy of the key we just joined on
            .project((0..oa).chain((oa + key.len())..(oa + ba)).collect());
            Ok(joined)
        })
    })
}

fn apply_scalar_subquery(
    id_gen: &mut mz_ore::id_gen::IdGen,
    outer: MirRelationExpr,
    col_map: &ColumnMap,
    cte_map: &mut CteMap,
    scalar_subquery: HirRelationExpr,
    apply_requires_distinct_outer: bool,
    context: &Context,
) -> Result<MirRelationExpr, PlanError> {
    branch(
        id_gen,
        outer,
        col_map,
        cte_map,
        scalar_subquery,
        apply_requires_distinct_outer,
        context,
        |id_gen, expr, get_inner, col_map, cte_map, context| {
            // compute for every row in get_inner
            let select = expr.applied_to(id_gen, get_inner.clone(), col_map, cte_map, context)?;
            let col_type = select.typ().column_types.into_last();

            let inner_arity = get_inner.arity();
            // We must determine a count for each `get_inner` prefix,
            // and report an error if that count exceeds one.
            let guarded = select.let_in_fallible(id_gen, |_id_gen, get_select| {
                // Count for each `get_inner` prefix.
                let counts = get_select.clone().reduce(
                    (0..inner_arity).collect::<Vec<_>>(),
                    vec![mz_expr::AggregateExpr {
                        func: mz_expr::AggregateFunc::Count,
                        expr: MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool),
                        distinct: false,
                    }],
                    None,
                );
                // Errors should result from counts > 1.
                let errors = counts
                    .filter(vec![MirScalarExpr::Column(inner_arity).call_binary(
                        MirScalarExpr::literal_ok(Datum::Int64(1), ScalarType::Int64),
                        mz_expr::BinaryFunc::Gt,
                    )])
                    .project((0..inner_arity).collect::<Vec<_>>())
                    .map_one(MirScalarExpr::literal(
                        Err(mz_expr::EvalError::MultipleRowsFromSubquery),
                        col_type.clone().scalar_type,
                    ));
                // Return `get_select` and any errors added in.
                Ok::<_, PlanError>(get_select.union(errors))
            })?;
            // append Null to anything that didn't return any rows
            let default = vec![(Datum::Null, col_type.scalar_type)];
            Ok(get_inner.lookup(id_gen, guarded, default))
        },
    )
}

fn apply_existential_subquery(
    id_gen: &mut mz_ore::id_gen::IdGen,
    outer: MirRelationExpr,
    col_map: &ColumnMap,
    cte_map: &mut CteMap,
    subquery_expr: HirRelationExpr,
    apply_requires_distinct_outer: bool,
    context: &Context,
) -> Result<MirRelationExpr, PlanError> {
    branch(
        id_gen,
        outer,
        col_map,
        cte_map,
        subquery_expr,
        apply_requires_distinct_outer,
        context,
        |id_gen, expr, get_inner, col_map, cte_map, context| {
            let exists = expr
                // compute for every row in get_inner
                .applied_to(id_gen, get_inner.clone(), col_map, cte_map, context)?
                // throw away actual values and just remember whether or not there were __any__ rows
                .distinct_by((0..get_inner.arity()).collect())
                // Append true to anything that returned any rows.
                .map(vec![MirScalarExpr::literal_true()]);

            // append False to anything that didn't return any rows
            Ok(get_inner.lookup(id_gen, exists, vec![(Datum::False, ScalarType::Bool)]))
        },
    )
}

impl AggregateExpr {
    fn applied_to(
        self,
        id_gen: &mut mz_ore::id_gen::IdGen,
        col_map: &ColumnMap,
        cte_map: &mut CteMap,
        inner: &mut MirRelationExpr,
        context: &Context,
    ) -> Result<mz_expr::AggregateExpr, PlanError> {
        let AggregateExpr {
            func,
            expr,
            distinct,
        } = self;

        Ok(mz_expr::AggregateExpr {
            func: func.into_expr(),
            expr: expr.applied_to(id_gen, col_map, cte_map, inner, &None, context)?,
            distinct,
        })
    }
}

/// Attempts an efficient outer join, if `on` has equijoin structure.
///
/// Both `left` and `right` are decorrelated inputs.
///
/// The first `oa` columns correspond to an outer context: we should do the
/// outer join independently for each prefix. In the case that `on` contains
/// just some equality tests between columns of `left` and `right` and some
/// local predicates, we can employ a relatively simple plan.
///
/// The last `on_subquery_types.len()` columns correspond to results from
/// subqueries defined in the `on` clause - we treat those as theta-join
/// conditions that prohibit the use of the simple plan attempted here.
fn attempt_outer_equijoin(
    left: MirRelationExpr,
    right: MirRelationExpr,
    on: MirScalarExpr,
    on_subquery_types: Vec<ColumnType>,
    kind: JoinKind,
    oa: usize,
    id_gen: &mut mz_ore::id_gen::IdGen,
    context: &Context,
) -> Result<Option<MirRelationExpr>, PlanError> {
    // TODO(#22581): In theory, we can be smarter and also handle `on`
    // predicates that reference subqueries as long as these subqueries don't
    // reference `left` and `right` at the same time.
    //
    // TODO(#22582): This code can be improved as follows:
    //
    // 1. Move the `canonicalize_predicates(...)` call to `applied_to`.
    // 2. Use the canonicalized `on` predicate in the non-equijoin based
    //    lowering strategy.
    // 3. Move the `OnPredicates::new(...)` call to `applied_to`.
    // 4. Pass the classified `OnPredicates` as a parameter.
    // 5. Guard calls of this function with `on_predicates.is_equijoin()`.
    //
    // Steps (1 + 2) require further investigation because we might change the
    // error semantics in case the `on` predicate contains a literal error..

    let l_type = left.typ();
    let r_type = right.typ();
    let la = l_type.column_types.len() - oa;
    let ra = r_type.column_types.len() - oa;
    let sa = on_subquery_types.len();

    // The output type contains [outer, left, right, sa] attributes.
    let mut output_type = Vec::with_capacity(oa + la + ra + sa);
    output_type.extend(l_type.column_types);
    output_type.extend(r_type.column_types.into_iter().skip(oa));
    output_type.extend(on_subquery_types);

    // Generally healthy to do, but specifically `USING` conditions sometimes
    // put an `AND true` at the end of the `ON` condition.
    //
    // TODO(aalexandrov): maybe we should already be doing this in `applied_to`.
    // However, in that case it's not clear that we won't see regressions if
    // `on` simplifies to a literal error.
    let mut on = vec![on];
    mz_expr::canonicalize::canonicalize_predicates(&mut on, &output_type);

    // Form the left and right types without the outer attributes.
    output_type.drain(0..oa);
    let lt = output_type.drain(0..la).collect_vec();
    let rt = output_type.drain(0..ra).collect_vec();
    assert!(output_type.len() == sa);

    let on_predicates = OnPredicates::new(oa, la, ra, sa, on.clone(), context);
    if !on_predicates.is_equijoin(context) {
        return Ok(None);
    }

    // If we've gotten this far, we can do the clever thing.
    // We'll want to use left and right multiple times
    let result = left.let_in_fallible(id_gen, |id_gen, get_left| {
        right.let_in_fallible(id_gen, |id_gen, get_right| {
            // TODO: we know that we can re-use the arrangements of left and right
            // needed for the inner join with each of the conditional outer joins.
            // It is not clear whether we should hint that, or just let the planner
            // and optimizer run and see what happens.

            // We'll want the inner join (minus repeated columns)
            let join = MirRelationExpr::join(
                vec![get_left.clone(), get_right.clone()],
                (0..oa).map(|i| vec![(0, i), (1, i)]).collect(),
            )
            // remove those columns from `right` repeating the first `oa` columns.
            .project(
                (0..(oa + la))
                    .chain((oa + la + oa)..(oa + la + oa + ra))
                    .collect(),
            )
            // apply the filter constraints here, to ensure nulls are not matched.
            .filter(on);

            // We'll want to re-use the results of the join multiple times.
            join.let_in_fallible(id_gen, |id_gen, get_join| {
                let mut result = get_join.clone();

                // A collection of keys present in both left and right collections.
                let join_keys = on_predicates.join_keys();
                let both_keys_arity = join_keys.len();
                let both_keys = get_join.restrict(join_keys).distinct();

                // The plan is now to determine the left and right rows matched in the
                // inner join, subtract them from left and right respectively, pad what
                // remains with nulls, and fold them in to `result`.

                both_keys.let_in_fallible(id_gen, |_id_gen, get_both| {
                    if let JoinKind::LeftOuter { .. } | JoinKind::FullOuter = kind {
                        // Rows in `left` matched in the inner equijoin. This is
                        // a semi-join between `left` and `both_keys`.
                        let left_present = MirRelationExpr::join_scalars(
                            vec![
                                get_left
                                    .clone()
                                    // Push local predicates.
                                    .filter(on_predicates.lhs()),
                                get_both.clone(),
                            ],
                            itertools::zip_eq(
                                on_predicates.eq_lhs(),
                                (0..both_keys_arity).map(|k| MirScalarExpr::column(oa + la + k)),
                            )
                            .map(|(l_key, b_key)| [l_key, b_key].to_vec())
                            .collect(),
                        )
                        .project((0..(oa + la)).collect());

                        // Determine the types of nulls to use as filler.
                        let right_fill = rt
                            .into_iter()
                            .map(|typ| MirScalarExpr::literal_null(typ.scalar_type))
                            .collect();

                        // Add to `result` absent elements, filled with typed nulls.
                        result = left_present
                            .negate()
                            .union(get_left.clone())
                            .map(right_fill)
                            .union(result);
                    }

                    if let JoinKind::RightOuter | JoinKind::FullOuter = kind {
                        // Rows in `right` matched in the inner equijoin. This
                        // is a semi-join between `right` and `both_keys`.
                        let right_present = MirRelationExpr::join_scalars(
                            vec![
                                get_right
                                    .clone()
                                    // Push local predicates.
                                    .filter(on_predicates.rhs()),
                                get_both,
                            ],
                            itertools::zip_eq(
                                on_predicates.eq_rhs(),
                                (0..both_keys_arity).map(|k| MirScalarExpr::column(oa + ra + k)),
                            )
                            .map(|(r_key, b_key)| [r_key, b_key].to_vec())
                            .collect(),
                        )
                        .project((0..(oa + ra)).collect());

                        // Determine the types of nulls to use as filler.
                        let left_fill = lt
                            .into_iter()
                            .map(|typ| MirScalarExpr::literal_null(typ.scalar_type))
                            .collect();

                        // Add to `result` absent elements, prepended with typed nulls.
                        result = right_present
                            .negate()
                            .union(get_right.clone())
                            .map(left_fill)
                            // Permute left fill before right values.
                            .project(
                                itertools::chain!(
                                    0..oa,                 // Preserve `outer`.
                                    oa + ra..oa + la + ra, // Increment the next `la` cols by `ra`.
                                    oa..oa + ra            // Decrement the next `ra` cols by `la`.
                                )
                                .collect(),
                            )
                            .union(result)
                    }

                    Ok::<_, PlanError>(result)
                })
            })
        })
    })?;
    Ok(Some(result))
}

/// A struct that represents the predicates in the `on` clause in a form
/// suitable for efficient planning outer joins with equijoin predicates.
struct OnPredicates {
    /// A store for classified `ON` predicates.
    ///
    /// Predicates that reference a single side are adjusted to assume an
    /// `outer × <side>` schema.
    predicates: Vec<OnPredicate>,
    /// Number of outer context columns.
    oa: usize,
}

impl OnPredicates {
    const I_OUT: usize = 0; // outer context input position
    const I_LHS: usize = 1; // lhs input position
    const I_RHS: usize = 2; // rhs input position
    const I_SUB: usize = 3; // on subqueries input position

    /// Classify the predicates in the `on` clause of an outer join.
    ///
    /// The other parameters are arities of the input parts:
    ///
    /// - `oa` is the arity of the `outer` context.
    /// - `la` is the arity of the `left` input.
    /// - `ra` is the arity of the `right` input.
    /// - `sa` is the arity of the `on` subqueries.
    ///
    /// The constructor assumes that:
    ///
    /// 1. The `on` parameter will be applied on a result that has the following
    ///    schema `outer × left × right × on_subqueries`.
    /// 2. The `on` parameter is already adjusted to assume that schema.
    /// 3. The `on` parameter is obtained by canonicalizing the original `on:
    ///    MirScalarExpr` with `canonicalize_predicates`.
    fn new(
        oa: usize,
        la: usize,
        ra: usize,
        sa: usize,
        on: Vec<MirScalarExpr>,
        context: &Context,
    ) -> Self {
        use mz_expr::BinaryFunc::Eq;

        // Re-bind those locally for more compact pattern matching.
        const I_LHS: usize = OnPredicates::I_LHS;
        const I_RHS: usize = OnPredicates::I_RHS;

        // Self parameters.
        let mut predicates = Vec::with_capacity(on.len());

        // Helpers for populating `predicates`.
        let inner_join_mapper = mz_expr::JoinInputMapper::new_from_input_arities([oa, la, ra, sa]);
        let rhs_permutation = itertools::chain!(0..oa + la, oa..oa + ra).collect::<Vec<_>>();
        let lookup_inputs = |expr: &MirScalarExpr| -> Vec<usize> {
            inner_join_mapper
                .lookup_inputs(expr)
                .filter(|&i| i != Self::I_OUT)
                .collect()
        };
        let has_subquery_refs = |expr: &MirScalarExpr| -> bool {
            inner_join_mapper
                .lookup_inputs(expr)
                .any(|i| i == Self::I_SUB)
        };

        // Iterate over `on` elements and populate `predicates`.
        for mut predicate in on {
            if predicate.might_error() {
                tracing::debug!(case = "thetajoin (error)", "OnPredicates::new");
                // Treat predicates that can produce a literal error as Theta.
                predicates.push(OnPredicate::Theta(predicate));
            } else if has_subquery_refs(&predicate) {
                tracing::debug!(case = "thetajoin (subquery)", "OnPredicates::new");
                // Treat predicates referencing an `on` subquery as Theta.
                predicates.push(OnPredicate::Theta(predicate));
            } else if let MirScalarExpr::CallBinary {
                func: Eq,
                expr1,
                expr2,
            } = &mut predicate
            {
                // Obtain the non-outer inputs referenced by each side.
                let inputs1 = lookup_inputs(expr1);
                let inputs2 = lookup_inputs(expr2);

                match (&inputs1[..], &inputs2[..]) {
                    // Neither side references an input. This could be a
                    // constant expression or an expression that depends only on
                    // the outer context.
                    ([], []) => {
                        predicates.push(OnPredicate::Const(predicate));
                    }
                    // Both sides reference different inputs.
                    ([I_LHS], [I_RHS]) => {
                        let lhs = expr1.take();
                        let mut rhs = expr2.take();
                        rhs.permute(&rhs_permutation);
                        predicates.push(OnPredicate::Eq(lhs.clone(), rhs.clone()));
                        if context.config.enable_outer_join_null_filter {
                            predicates.push(OnPredicate::LhsConsequence(lhs.call_is_null().not()));
                            predicates.push(OnPredicate::RhsConsequence(rhs.call_is_null().not()));
                        }
                    }
                    // Both sides reference different inputs (swapped).
                    ([I_RHS], [I_LHS]) => {
                        let lhs = expr2.take();
                        let mut rhs = expr1.take();
                        rhs.permute(&rhs_permutation);
                        predicates.push(OnPredicate::Eq(lhs.clone(), rhs.clone()));
                        if context.config.enable_outer_join_null_filter {
                            predicates.push(OnPredicate::LhsConsequence(lhs.call_is_null().not()));
                            predicates.push(OnPredicate::RhsConsequence(rhs.call_is_null().not()));
                        }
                    }
                    // Both sides reference the left input or no input.
                    ([I_LHS], [I_LHS]) | ([I_LHS], []) | ([], [I_LHS]) => {
                        predicates.push(OnPredicate::Lhs(predicate));
                    }
                    // Both sides reference the right input or no input.
                    ([I_RHS], [I_RHS]) | ([I_RHS], []) | ([], [I_RHS]) => {
                        predicate.permute(&rhs_permutation);
                        predicates.push(OnPredicate::Rhs(predicate));
                    }
                    // At least one side references more than one input.
                    _ => {
                        tracing::debug!(case = "thetajoin (eq)", "OnPredicates::new");
                        predicates.push(OnPredicate::Theta(predicate));
                    }
                }
            } else {
                // Obtain the non-outer inputs referenced by this predicate.
                let inputs = lookup_inputs(&predicate);

                match &inputs[..] {
                    // The predicate references no inputs. This could be a
                    // constant expression or an expression that depends only on
                    // the outer context.
                    [] => {
                        predicates.push(OnPredicate::Const(predicate));
                    }
                    // The predicate references only the left input.
                    [I_LHS] => {
                        predicates.push(OnPredicate::Lhs(predicate));
                    }
                    // The predicate references only the right input.
                    [I_RHS] => {
                        predicate.permute(&rhs_permutation);
                        predicates.push(OnPredicate::Rhs(predicate));
                    }
                    // The predicate references both inputs.
                    _ => {
                        tracing::debug!(case = "thetajoin (non-eq)", "OnPredicates::new");
                        predicates.push(OnPredicate::Theta(predicate));
                    }
                }
            }
        }

        Self { predicates, oa }
    }

    /// Check if the predicates can be lowered with an equijoin-based strategy.
    fn is_equijoin(&self, context: &Context) -> bool {
        // Count each `OnPredicate` variant in `self.predicates`.
        let (const_cnt, lhs_cnt, rhs_cnt, eq_cnt, eq_cols, theta_cnt) =
            self.predicates.iter().fold(
                (0, 0, 0, 0, 0, 0),
                |(const_cnt, lhs_cnt, rhs_cnt, eq_cnt, eq_cols, theta_cnt), p| {
                    (
                        const_cnt + usize::from(matches!(p, OnPredicate::Const(..))),
                        lhs_cnt + usize::from(matches!(p, OnPredicate::Lhs(..))),
                        rhs_cnt + usize::from(matches!(p, OnPredicate::Rhs(..))),
                        eq_cnt + usize::from(matches!(p, OnPredicate::Eq(..))),
                        eq_cols + usize::from(matches!(p, OnPredicate::Eq(lhs, rhs) if lhs.is_column() && rhs.is_column())),
                        theta_cnt + usize::from(matches!(p, OnPredicate::Theta(..))),
                    )
                },
            );

        let is_equijion = if context.config.enable_new_outer_join_lowering {
            // New classifier.
            eq_cnt > 0 && theta_cnt == 0
        } else {
            // Old classifier.
            eq_cnt > 0 && eq_cnt == eq_cols && theta_cnt + const_cnt + lhs_cnt + rhs_cnt == 0
        };

        // Log an entry only if this is an equijoin according to the new classifier.
        if eq_cnt > 0 && theta_cnt == 0 {
            tracing::debug!(
                const_cnt,
                lhs_cnt,
                rhs_cnt,
                eq_cnt,
                eq_cols,
                theta_cnt,
                "OnPredicates::is_equijoin"
            );
        }

        is_equijion
    }

    /// Return an [`MirRelationExpr`] list that represents the keys for the
    /// equijoin. The list will contain the outer columns as a prefix.
    fn join_keys(&self) -> JoinKeys {
        // We could return either the `lhs` or the `rhs` of the keys used to
        // form the inner join as they are equated by the join condition.
        let join_keys = self.eq_lhs().collect::<Vec<_>>();

        if join_keys.iter().all(|k| k.is_column()) {
            tracing::debug!(case = "outputs", "OnPredicates::join_keys");
            JoinKeys::Outputs(join_keys.iter().flat_map(|k| k.as_column()).collect())
        } else {
            tracing::debug!(case = "scalars", "OnPredicates::join_keys");
            JoinKeys::Scalars(join_keys)
        }
    }

    /// Return an iterator over the left-hand sides of all [`OnPredicate::Eq`]
    /// conditions in the predicates list.
    ///
    /// The iterator will start with column references to the outer columns as a
    /// prefix.
    fn eq_lhs(&self) -> impl Iterator<Item = MirScalarExpr> + '_ {
        itertools::chain(
            (0..self.oa).map(MirScalarExpr::column),
            self.predicates.iter().filter_map(|e| match e {
                OnPredicate::Eq(lhs, _) => Some(lhs.clone()),
                _ => None,
            }),
        )
    }

    /// Return an iterator over the right-hand sides of all [`OnPredicate::Eq`]
    /// conditions in the predicates list.
    ///
    /// The iterator will start with column references to the outer columns as a
    /// prefix.
    fn eq_rhs(&self) -> impl Iterator<Item = MirScalarExpr> + '_ {
        itertools::chain(
            (0..self.oa).map(MirScalarExpr::column),
            self.predicates.iter().filter_map(|e| match e {
                OnPredicate::Eq(_, rhs) => Some(rhs.clone()),
                _ => None,
            }),
        )
    }

    /// Return an iterator over the [`OnPredicate::Lhs`], [`OnPredicate::LhsConsequence`] and
    /// [`OnPredicate::Const`] conditions in the predicates list.
    fn lhs(&self) -> impl Iterator<Item = MirScalarExpr> + '_ {
        self.predicates.iter().filter_map(|p| match p {
            // We treat Const predicates local to both inputs.
            OnPredicate::Const(p) => Some(p.clone()),
            OnPredicate::Lhs(p) => Some(p.clone()),
            OnPredicate::LhsConsequence(p) => Some(p.clone()),
            _ => None,
        })
    }

    /// Return an iterator over the [`OnPredicate::Rhs`], [`OnPredicate::RhsConsequence`] and
    /// [`OnPredicate::Const`] conditions in the predicates list.
    fn rhs(&self) -> impl Iterator<Item = MirScalarExpr> + '_ {
        self.predicates.iter().filter_map(|p| match p {
            // We treat Const predicates local to both inputs.
            OnPredicate::Const(p) => Some(p.clone()),
            OnPredicate::Rhs(p) => Some(p.clone()),
            OnPredicate::RhsConsequence(p) => Some(p.clone()),
            _ => None,
        })
    }
}

enum OnPredicate {
    // A predicate that is either constant or references only outer columns.
    Const(MirScalarExpr),
    // A local predicate on the left-hand side of the join, i.e., it references only the left input
    // and possibly outer columns.
    //
    // This is one of the original predicates from the ON clause.
    //
    // One _must_ apply this predicate.
    Lhs(MirScalarExpr),
    // A local predicate on the left-hand side of the join, i.e., it references only the left input
    // and possibly outer columns.
    //
    // This is not one of the original predicates from the ON clause, but is just a consequence
    // of an original predicate in the ON clause, where the original predicate references both
    // inputs, but the consequence references only the left input.
    //
    // For example, the original predicate `input1.x = input2.a` has the consequence
    // `input1.x IS NOT NULL`. Applying such a consequence before the input is fed into the join
    // prevents null skew, and also makes more CSE opportunities available when the left input's key
    // doesn't have a NOT NULL constraint, saving us an arrangement.
    //
    // Applying the predicate is optional, because the original predicate will be applied anyway.
    LhsConsequence(MirScalarExpr),
    // A local predicate on the right-hand side of the join.
    //
    // This is one of the original predicates from the ON clause.
    //
    // One _must_ apply this predicate.
    Rhs(MirScalarExpr),
    // A consequence of an original ON predicate, see above.
    RhsConsequence(MirScalarExpr),
    // An equality predicate between the two sides.
    Eq(MirScalarExpr, MirScalarExpr),
    // a non-equality predicate between the two sides.
    #[allow(dead_code)]
    Theta(MirScalarExpr),
}

/// A set of join keys referencing an input.
///
/// This is used in the [`MirRelationExpr::Join`] lowering code in order to
/// avoid changes (and thereby possible regressions) in plans that have equijoin
/// predicates consisting only of column refs.
///
/// If we were running `CanonicalizeMfp` as part of `NormalizeOps` we might be
/// able to get rid of this code, but as it stands `Map` simplification seems
/// more cumbersome than `Project` simplification, so do this just to be sure.
enum JoinKeys {
    // A predicate that is either constant or references only outer columns.
    Outputs(Vec<usize>),
    // A local predicate on the left-hand side of the join.
    Scalars(Vec<MirScalarExpr>),
}

impl JoinKeys {
    fn len(&self) -> usize {
        match self {
            JoinKeys::Outputs(outputs) => outputs.len(),
            JoinKeys::Scalars(scalars) => scalars.len(),
        }
    }
}

/// Extension methods for [`MirRelationExpr`] required in the HIR ⇒ MIR lowering
/// code.
trait LoweringExt {
    /// See [`MirRelationExpr::restrict`].
    fn restrict(self, join_keys: JoinKeys) -> Self;
}

impl LoweringExt for MirRelationExpr {
    /// Restrict the set of columns of an input to the sequence of [`JoinKeys`].
    fn restrict(self, join_keys: JoinKeys) -> Self {
        let num_keys = join_keys.len();
        match join_keys {
            JoinKeys::Outputs(outputs) => self.project(outputs),
            JoinKeys::Scalars(scalars) => {
                let input_arity = self.arity();
                let outputs = (input_arity..input_arity + num_keys).collect();
                self.map(scalars).project(outputs)
            }
        }
    }
}
