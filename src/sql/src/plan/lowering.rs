// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lowering is the process of transforming a `sql::expr::HirRelationExpr`
//! into a `mz_expr::MirRelationExpr`.
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

use std::collections::{BTreeSet, HashMap};

use anyhow::bail;
use itertools::Itertools;

use mz_ore::collections::CollectionExt;
use mz_ore::stack::maybe_grow;
use mz_repr::RelationType;
use mz_repr::*;

use crate::plan::expr::{
    AggregateExpr, ColumnOrder, ColumnRef, HirRelationExpr, HirScalarExpr, JoinKind, WindowExprType,
};
use crate::plan::transform_expr;

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
    inner: HashMap<ColumnRef, usize>,
}

impl ColumnMap {
    fn empty() -> ColumnMap {
        Self::new(HashMap::new())
    }

    fn new(inner: HashMap<ColumnRef, usize>) -> ColumnMap {
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
type CteMap = HashMap<mz_expr::LocalId, CteDesc>;

/// Information about needed when finding a reference to a CTE in scope.
struct CteDesc {
    /// The new ID assigned to the lowered version of the CTE, which may not match
    /// the ID of the input CTE.
    new_id: mz_expr::LocalId,
    /// The relation type of the CTE including the columns from the outer
    /// context at the beginning.
    relation_type: RelationType,
    /// The outer relation the CTE was applied to.
    outer_relation: mz_expr::MirRelationExpr,
}

impl HirRelationExpr {
    /// Rewrite `self` into a `mz_expr::MirRelationExpr`.
    /// This requires rewriting all correlated subqueries (nested `HirRelationExpr`s) into flat queries
    pub fn lower(self) -> mz_expr::MirRelationExpr {
        match self {
            // We directly rewrite a Constant into the corresponding `MirRelationExpr::Constant`
            // to ensure that the downstream optimizer can easily bypass most
            // irrelevant optimizations (e.g. reduce folding) for this expression
            // without having to re-learn the fact that it is just a constant,
            // as it would if the constant were wrapped in a Let-Get pair.
            HirRelationExpr::Constant { rows, typ } => {
                let rows: Vec<_> = rows.into_iter().map(|row| (row, 1)).collect();
                mz_expr::MirRelationExpr::Constant {
                    rows: Ok(rows),
                    typ,
                }
            }
            mut other => {
                let mut id_gen = mz_ore::id_gen::IdGen::default();
                transform_expr::split_subquery_predicates(&mut other);
                transform_expr::try_simplify_quantified_comparisons(&mut other);
                mz_expr::MirRelationExpr::constant(vec![vec![]], RelationType::new(vec![])).let_in(
                    &mut id_gen,
                    |id_gen, get_outer| {
                        other.applied_to(id_gen, get_outer, &ColumnMap::empty(), &mut CteMap::new())
                    },
                )
            }
        }
    }

    /// Return a `mz_expr::MirRelationExpr` which evaluates `self` once for each row of `get_outer`.
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
        get_outer: mz_expr::MirRelationExpr,
        col_map: &ColumnMap,
        cte_map: &mut CteMap,
    ) -> mz_expr::MirRelationExpr {
        maybe_grow(|| {
            use self::HirRelationExpr::*;
            use mz_expr::MirRelationExpr as SR;
            if let mz_expr::MirRelationExpr::Get { .. } = &get_outer {
            } else {
                panic!(
                    "get_outer: expected a MirRelationExpr::Get, found {:?}",
                    get_outer
                );
            }
            assert_eq!(col_map.len(), get_outer.arity());
            match self {
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
                                        mz_expr::MirScalarExpr::Column(pos),
                                        mz_expr::MirScalarExpr::Column(pos + oa),
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
                        get_outer.product(SR::Get { id, typ })
                    }
                },
                Let {
                    name: _,
                    id,
                    value,
                    body,
                } => {
                    let value = value.applied_to(id_gen, get_outer.clone(), col_map, cte_map);
                    value.let_in(id_gen, |id_gen, get_value| {
                        let (new_id, typ) = if let mz_expr::MirRelationExpr::Get {
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
                        let body = body.applied_to(id_gen, get_outer, col_map, cte_map);
                        if let Some(old_value) = old_value {
                            cte_map.insert(id, old_value);
                        } else {
                            cte_map.remove(&id);
                        }
                        body
                    })
                }
                Project { input, outputs } => {
                    // Projections should be applied to the decorrelated `inner`, and to its columns,
                    // which means rebasing `outputs` to start `get_outer.arity()` columns later.
                    let input = input.applied_to(id_gen, get_outer.clone(), col_map, cte_map);
                    let outputs = (0..get_outer.arity())
                        .chain(outputs.into_iter().map(|i| get_outer.arity() + i))
                        .collect::<Vec<_>>();
                    input.project(outputs)
                }
                Map { input, mut scalars } => {
                    // Scalar expressions may contain correlated subqueries. We must be cautious!
                    let mut input = input.applied_to(id_gen, get_outer, col_map, cte_map);

                    // Lower subqueries in maximally sized batches, such as no subquery in the current
                    // batch depends on columns from the same batch.
                    // Note that subqueries in this projection may reference columns added by this
                    // Map operator, so we need to ensure these columns exist before lowering the
                    // subquery.
                    while !scalars.is_empty() {
                        let old_arity = input.arity();

                        let end_idx = scalars
                            .iter_mut()
                            .position(|s| {
                                let mut requires_nonexistent_column = false;
                                s.visit_columns(0, &mut |depth, col| {
                                    if col.level == depth {
                                        requires_nonexistent_column |= (col.column + 1) > old_arity
                                    }
                                });
                                requires_nonexistent_column
                            })
                            .unwrap_or(scalars.len());

                        let scalars = scalars.drain(0..end_idx).collect_vec();
                        let (with_subqueries, subquery_map) = HirScalarExpr::lower_subqueries(
                            &scalars, id_gen, col_map, cte_map, input,
                        );
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
                            );
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
                        .map(|e| e.applied_to(id_gen, col_map, cte_map, &mut input, &None))
                        .collect::<Vec<_>>();

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
                    let mut input = input.applied_to(id_gen, get_outer, col_map, cte_map);
                    for predicate in predicates {
                        let old_arity = input.arity();
                        let predicate =
                            predicate.applied_to(id_gen, col_map, cte_map, &mut input, &None);
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

                    let left = left.applied_to(id_gen, get_outer, col_map, cte_map);
                    left.let_in(id_gen, |id_gen, get_left| {
                        let apply_requires_distinct_outer = false;
                        let mut join = branch(
                            id_gen,
                            get_left.clone(),
                            col_map,
                            cte_map,
                            *right,
                            apply_requires_distinct_outer,
                            |id_gen, right, get_left, col_map, cte_map| {
                                right.applied_to(id_gen, get_left, col_map, cte_map)
                            },
                        );

                        // Plan the `on` predicate.
                        let old_arity = join.arity();
                        let on = on.applied_to(id_gen, col_map, cte_map, &mut join, &None);
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
                            get_left.lookup(id_gen, join, default)
                        } else {
                            join
                        }
                    })
                }
                Join {
                    left,
                    right,
                    on,
                    kind,
                } => {
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
                    let left = left.applied_to(id_gen, get_outer.clone(), col_map, cte_map);
                    let lt = left.typ().column_types.into_iter().skip(oa).collect_vec();
                    let la = lt.len();
                    left.let_in(id_gen, |id_gen, get_left| {
                        let right_col_map = col_map.enter_scope(0);
                        let right =
                            right.applied_to(id_gen, get_outer.clone(), &right_col_map, cte_map);
                        let rt = right.typ().column_types.into_iter().skip(oa).collect_vec();
                        let ra = rt.len();
                        right.let_in(id_gen, |id_gen, get_right| {
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
                            let old_arity = product.arity();
                            let on = on.applied_to(id_gen, col_map, cte_map, &mut product, &None);

                            // Attempt an efficient equijoin implementation, in which outer joins are
                            // more efficiently rendered than in general. This can return `None` if
                            // such a plan is not possible, for example if `on` does not describe an
                            // equijoin between columns of `left` and `right`.
                            if kind != JoinKind::Inner {
                                if let Some(joined) = attempt_outer_join(
                                    get_left.clone(),
                                    get_right.clone(),
                                    on.clone(),
                                    kind.clone(),
                                    oa,
                                    id_gen,
                                ) {
                                    return joined;
                                }
                            }

                            // Otherwise, perform a more general join.
                            let mut join = product.filter(vec![on]);
                            let new_arity = join.arity();
                            if old_arity != new_arity {
                                // this means we added some columns to handle subqueries, and now we need to get rid of them
                                join = join.project((0..old_arity).collect());
                            }
                            join.let_in(id_gen, |id_gen, get_join| {
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
                                result
                            })
                        })
                    })
                }
                Union { base, inputs } => {
                    // Union is uncomplicated.
                    SR::Union {
                        base: Box::new(base.applied_to(
                            id_gen,
                            get_outer.clone(),
                            col_map,
                            cte_map,
                        )),
                        inputs: inputs
                            .into_iter()
                            .map(|input| {
                                input.applied_to(id_gen, get_outer.clone(), col_map, cte_map)
                            })
                            .collect(),
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
                    let mut input = input.applied_to(id_gen, get_outer.clone(), col_map, cte_map);
                    let applied_group_key = (0..get_outer.arity())
                        .chain(group_key.iter().map(|i| get_outer.arity() + i))
                        .collect();
                    let applied_aggregates = aggregates
                        .into_iter()
                        .map(|aggregate| aggregate.applied_to(id_gen, col_map, cte_map, &mut input))
                        .collect::<Vec<_>>();
                    let input_type = input.typ();
                    let default = applied_aggregates
                        .iter()
                        .map(|agg| (agg.func.default(), agg.typ(&input_type).scalar_type))
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
                        .applied_to(id_gen, get_outer, col_map, cte_map)
                        .distinct()
                }
                TopK {
                    input,
                    group_key,
                    order_key,
                    limit,
                    offset,
                } => {
                    // TopK is uncomplicated, except that we must group by the columns of `get_outer` as well.
                    let input = input.applied_to(id_gen, get_outer.clone(), col_map, cte_map);
                    let applied_group_key = (0..get_outer.arity())
                        .chain(group_key.iter().map(|i| get_outer.arity() + i))
                        .collect();
                    let applied_order_key = order_key
                        .iter()
                        .map(|column_order| ColumnOrder {
                            column: column_order.column + get_outer.arity(),
                            desc: column_order.desc,
                        })
                        .collect();
                    input.top_k(applied_group_key, applied_order_key, limit, offset)
                }
                Negate { input } => {
                    // Negate is uncomplicated.
                    input
                        .applied_to(id_gen, get_outer, col_map, cte_map)
                        .negate()
                }
                Threshold { input } => {
                    // Threshold is uncomplicated.
                    input
                        .applied_to(id_gen, get_outer, col_map, cte_map)
                        .threshold()
                }
                DeclareKeys { input, keys } => input
                    .applied_to(id_gen, get_outer, col_map, cte_map)
                    .declare_keys(keys),
            }
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
        inner: &mut mz_expr::MirRelationExpr,
        subquery_map: &Option<&HashMap<HirScalarExpr, usize>>,
    ) -> mz_expr::MirScalarExpr {
        maybe_grow(|| {
            use self::HirScalarExpr::*;
            use mz_expr::MirScalarExpr as SS;

            if let Some(subquery_map) = subquery_map {
                if let Some(col) = subquery_map.get(&self) {
                    return SS::Column(*col);
                }
            }

            match self {
                Column(col_ref) => SS::Column(col_map.get(&col_ref)),
                Literal(row, typ) => SS::Literal(Ok(row), typ),
                Parameter(_) => panic!("cannot decorrelate expression with unbound parameters"),
                CallNullary(func) => SS::CallNullary(func),
                CallUnary { func, expr } => SS::CallUnary {
                    func,
                    expr: Box::new(expr.applied_to(id_gen, col_map, cte_map, inner, subquery_map)),
                },
                CallBinary { func, expr1, expr2 } => SS::CallBinary {
                    func,
                    expr1: Box::new(expr1.applied_to(
                        id_gen,
                        col_map,
                        cte_map,
                        inner,
                        subquery_map,
                    )),
                    expr2: Box::new(expr2.applied_to(
                        id_gen,
                        col_map,
                        cte_map,
                        inner,
                        subquery_map,
                    )),
                },
                CallVariadic { func, exprs } => SS::CallVariadic {
                    func,
                    exprs: exprs
                        .into_iter()
                        .map(|expr| expr.applied_to(id_gen, col_map, cte_map, inner, subquery_map))
                        .collect::<Vec<_>>(),
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
                    let cond_expr = cond.applied_to(id_gen, col_map, cte_map, inner, subquery_map);

                    // Defensive copies, in case we mangle these in decorrelation.
                    let inner_clone = inner.clone();
                    let then_clone = then.clone();
                    let else_clone = els.clone();

                    let cond_arity = inner.arity();
                    let then_expr = then.applied_to(id_gen, col_map, cte_map, inner, subquery_map);
                    let else_expr = els.applied_to(id_gen, col_map, cte_map, inner, subquery_map);

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

                        *inner = inner_clone.let_in(id_gen, |id_gen, get_inner| {
                            // Restrict to records satisfying `cond_expr` and apply `then` as a map.
                            let mut then_inner = get_inner.clone().filter(vec![cond_expr.clone()]);
                            let then_expr = then_clone.applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                &mut then_inner,
                                subquery_map,
                            );
                            let then_arity = then_inner.arity();
                            then_inner = then_inner
                                .map_one(then_expr)
                                .project((0..inner_arity).chain(Some(then_arity)).collect());

                            // Restrict to records not satisfying `cond_expr` and apply `els` as a map.
                            let mut else_inner = get_inner.filter(vec![SS::CallBinary {
                                func: mz_expr::BinaryFunc::Or,
                                expr1: Box::new(SS::CallBinary {
                                    func: mz_expr::BinaryFunc::Eq,
                                    expr1: Box::new(cond_expr.clone()),
                                    expr2: Box::new(SS::literal_ok(Datum::False, ScalarType::Bool)),
                                }),
                                expr2: Box::new(SS::CallUnary {
                                    func: mz_expr::UnaryFunc::IsNull(mz_expr::func::IsNull),
                                    expr: Box::new(cond_expr.clone()),
                                }),
                            }]);
                            let else_expr = else_clone.applied_to(
                                id_gen,
                                col_map,
                                cte_map,
                                &mut else_inner,
                                subquery_map,
                            );
                            let else_arity = else_inner.arity();
                            else_inner = else_inner
                                .map_one(else_expr)
                                .project((0..inner_arity).chain(Some(else_arity)).collect());

                            // concatenate the two results.
                            then_inner.union(else_inner)
                        });

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
                    );
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
                    );
                    SS::Column(inner.arity() - 1)
                }
                Windowing(expr) => {
                    // - For Scalar window functions we need to put a FlatMap operator on top of inner

                    let partition = expr.partition;
                    let order_by = expr.order_by;

                    match expr.func {
                        WindowExprType::Scalar(func) => {
                            *inner =
                                inner
                                    .take_dangerous()
                                    .let_in(id_gen, |id_gen, mut get_inner| {
                                        let order_by = order_by
                                            .into_iter()
                                            .map(|o| {
                                                o.applied_to(
                                                    id_gen,
                                                    col_map,
                                                    cte_map,
                                                    &mut get_inner,
                                                    subquery_map,
                                                )
                                            })
                                            .collect_vec();

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
                                        for p in partition {
                                            let key = p.applied_to(
                                                id_gen,
                                                col_map,
                                                cte_map,
                                                &mut get_inner,
                                                subquery_map,
                                            );
                                            if let mz_expr::MirScalarExpr::Column(c) = key {
                                                group_key.push(c);
                                            } else {
                                                get_inner = get_inner.map_one(key);
                                                group_key.push(get_inner.arity() - 1);
                                            }
                                        }

                                        get_inner.let_in(id_gen, |_id_gen, get_inner| {
                                            let to_reduce = get_inner;
                                            let input_type = to_reduce.typ();
                                            let fields = input_type
                                                .column_types
                                                .iter()
                                                .take(input_arity)
                                                .map(|t| (ColumnName::from("?column?"), t.clone()))
                                                .collect_vec();
                                            let agg_input = mz_expr::MirScalarExpr::CallVariadic {
                                                func: mz_expr::VariadicFunc::RecordCreate {
                                                    field_names: fields
                                                        .iter()
                                                        .map(|(name, _)| name.clone())
                                                        .collect_vec(),
                                                },
                                                exprs: (0..input_arity)
                                                    .map(|column| {
                                                        mz_expr::MirScalarExpr::Column(column)
                                                    })
                                                    .collect_vec(),
                                            };
                                            let record_type = ScalarType::Record {
                                                fields,
                                                custom_oid: None,
                                                custom_name: None,
                                            };
                                            let agg_input = mz_expr::MirScalarExpr::CallVariadic {
                                                func: mz_expr::VariadicFunc::ListCreate {
                                                    elem_type: record_type.clone(),
                                                },
                                                exprs: vec![agg_input],
                                            };
                                            let mut agg_input = vec![agg_input];
                                            agg_input.extend(order_by.clone());
                                            let agg_input = mz_expr::MirScalarExpr::CallVariadic {
                                                func: mz_expr::VariadicFunc::RecordCreate {
                                                    field_names: (0..1)
                                                        .map(|_| ColumnName::from("?column?"))
                                                        .collect_vec(),
                                                },
                                                exprs: agg_input,
                                            };
                                            let list_type = ScalarType::List {
                                                element_type: Box::new(record_type),
                                                custom_oid: None,
                                            };
                                            let agg_input_type = ScalarType::Record {
                                                fields: std::iter::once(&list_type)
                                                    .map(|t| {
                                                        (
                                                            ColumnName::from("?column?"),
                                                            t.clone().nullable(false),
                                                        )
                                                    })
                                                    .collect_vec(),
                                                custom_oid: None,
                                                custom_name: None,
                                            }
                                            .nullable(false);
                                            let func = func.into_expr();
                                            let aggregate = mz_expr::AggregateExpr {
                                                func,
                                                expr: agg_input,
                                                distinct: false,
                                            };
                                            let mut reduce = to_reduce
                                                .reduce(
                                                    group_key.clone(),
                                                    vec![aggregate.clone()],
                                                    None,
                                                )
                                                .flat_map(
                                                    mz_expr::TableFunc::UnnestList {
                                                        el_typ: aggregate
                                                            .func
                                                            .output_type(agg_input_type)
                                                            .scalar_type
                                                            .unwrap_list_element_type()
                                                            .clone(),
                                                    },
                                                    vec![mz_expr::MirScalarExpr::Column(
                                                        group_key.len(),
                                                    )],
                                                );
                                            let record_col = reduce.arity() - 1;

                                            // Unpack the record
                                            for c in 0..input_arity {
                                                reduce =
                                                    reduce
                                                        .take_dangerous()
                                                        .map_one(mz_expr::MirScalarExpr::CallUnary {
                                                        func: mz_expr::UnaryFunc::RecordGet(c),
                                                        expr: Box::new(
                                                            mz_expr::MirScalarExpr::CallUnary {
                                                                func: mz_expr::UnaryFunc::RecordGet(
                                                                    1,
                                                                ),
                                                                expr: Box::new(
                                                                    mz_expr::MirScalarExpr::Column(
                                                                        record_col,
                                                                    ),
                                                                ),
                                                            },
                                                        ),
                                                    });
                                            }

                                            // Append the column with the result of the window function.
                                            reduce = reduce.take_dangerous().map_one(
                                                mz_expr::MirScalarExpr::CallUnary {
                                                    func: mz_expr::UnaryFunc::RecordGet(0),
                                                    expr: Box::new(mz_expr::MirScalarExpr::Column(
                                                        record_col,
                                                    )),
                                                },
                                            );

                                            let agg_col = record_col + 1 + input_arity;
                                            reduce.project(
                                                (record_col + 1..agg_col + 1).collect_vec(),
                                            )
                                        })
                                    });
                            SS::Column(inner.arity() - 1)
                        }
                    }
                }
            }
        })
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
        inner: mz_expr::MirRelationExpr,
    ) -> (mz_expr::MirRelationExpr, HashMap<HirScalarExpr, usize>) {
        let mut subquery_map = HashMap::new();
        let output = inner.let_in(id_gen, |id_gen, get_inner| {
            let mut subqueries = Vec::new();
            let distinct_inner = get_inner.clone().distinct();
            for expr in exprs.iter() {
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
                            );

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
                            );
                            subqueries.push((e.clone(), subquery));
                        }
                        _ => {}
                    },
                );
            }

            if subqueries.is_empty() {
                get_inner
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
                                mz_expr::MirScalarExpr::Column(
                                    input_mapper.map_column_to_global(col, input),
                                )
                            })
                            .collect_vec()
                    })
                    .collect_vec();
                mz_expr::MirRelationExpr::join_scalars(join_inputs, equivalences)
            }
        });
        (output, subquery_map)
    }

    /// Rewrites `self` into a `mz_expr::ScalarExpr`.
    pub fn lower_uncorrelated(self) -> Result<mz_expr::MirScalarExpr, anyhow::Error> {
        use self::HirScalarExpr::*;
        use mz_expr::MirScalarExpr as SS;

        Ok(match self {
            Column(ColumnRef { level: 0, column }) => SS::Column(column),
            Literal(datum, typ) => SS::Literal(Ok(datum), typ),
            CallNullary(func) => SS::CallNullary(func),
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
                bail!("unexpected ScalarExpr in uncorrelated plan: {:?}", self);
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
    outer: mz_expr::MirRelationExpr,
    col_map: &ColumnMap,
    cte_map: &mut CteMap,
    inner: HirRelationExpr,
    apply_requires_distinct_outer: bool,
    apply: F,
) -> mz_expr::MirRelationExpr
where
    F: FnOnce(
        &mut mz_ore::id_gen::IdGen,
        HirRelationExpr,
        mz_expr::MirRelationExpr,
        &ColumnMap,
        &mut CteMap,
    ) -> mz_expr::MirRelationExpr,
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
        return outer.let_in(id_gen, |id_gen, get_outer| {
            apply(id_gen, inner, get_outer, &new_col_map, cte_map)
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
    let mut new_col_map = HashMap::new();
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

    outer.let_in(id_gen, |id_gen, get_outer| {
        let keyed_outer = if key.is_empty() {
            // Don't depend on outer at all if the branch is not correlated,
            // which yields vastly better query plans. Note that this is a bit
            // weird in that the branch will be computed even if outer has no
            // rows, whereas if it had been correlated it would not (and *could*
            // not) have been computed if outer had no rows, but the callers of
            // this function don't mind these somewhat-weird semantics.
            mz_expr::MirRelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
        } else {
            get_outer.clone().distinct_by(key.clone())
        };
        keyed_outer.let_in(id_gen, |id_gen, get_keyed_outer| {
            let oa = get_outer.arity();
            let branch = apply(id_gen, inner, get_keyed_outer, &new_col_map, cte_map);
            let ba = branch.arity();
            let joined = mz_expr::MirRelationExpr::join(
                vec![get_outer.clone(), branch],
                key.iter()
                    .enumerate()
                    .map(|(i, &k)| vec![(0, k), (1, i)])
                    .collect(),
            )
            // throw away the right-hand copy of the key we just joined on
            .project((0..oa).chain((oa + key.len())..(oa + ba)).collect());
            joined
        })
    })
}

fn apply_scalar_subquery(
    id_gen: &mut mz_ore::id_gen::IdGen,
    outer: mz_expr::MirRelationExpr,
    col_map: &ColumnMap,
    cte_map: &mut CteMap,
    scalar_subquery: HirRelationExpr,
    apply_requires_distinct_outer: bool,
) -> mz_expr::MirRelationExpr {
    branch(
        id_gen,
        outer,
        col_map,
        cte_map,
        scalar_subquery,
        apply_requires_distinct_outer,
        |id_gen, expr, get_inner, col_map, cte_map| {
            let select = expr
                // compute for every row in get_inner
                .applied_to(id_gen, get_inner.clone(), col_map, cte_map);
            let col_type = select.typ().column_types.into_last();

            let inner_arity = get_inner.arity();
            // We must determine a count for each `get_inner` prefix,
            // and report an error if that count exceeds one.
            let guarded = select.let_in(id_gen, |_id_gen, get_select| {
                // Count for each `get_inner` prefix.
                let counts = get_select.clone().reduce(
                    (0..inner_arity).collect::<Vec<_>>(),
                    vec![mz_expr::AggregateExpr {
                        func: mz_expr::AggregateFunc::Count,
                        expr: mz_expr::MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool),
                        distinct: false,
                    }],
                    None,
                );
                // Errors should result from counts > 1.
                let errors = counts
                    .filter(vec![mz_expr::MirScalarExpr::Column(inner_arity)
                        .call_binary(
                            mz_expr::MirScalarExpr::literal_ok(Datum::Int64(1), ScalarType::Int64),
                            mz_expr::BinaryFunc::Gt,
                        )])
                    .project((0..inner_arity).collect::<Vec<_>>())
                    .map_one(mz_expr::MirScalarExpr::literal(
                        Err(mz_expr::EvalError::MultipleRowsFromSubquery),
                        col_type.clone().scalar_type,
                    ));
                // Return `get_select` and any errors added in.
                get_select.union(errors)
            });
            // append Null to anything that didn't return any rows
            let default = vec![(Datum::Null, col_type.scalar_type)];
            get_inner.lookup(id_gen, guarded, default)
        },
    )
}

fn apply_existential_subquery(
    id_gen: &mut mz_ore::id_gen::IdGen,
    outer: mz_expr::MirRelationExpr,
    col_map: &ColumnMap,
    cte_map: &mut CteMap,
    subquery_expr: HirRelationExpr,
    apply_requires_distinct_outer: bool,
) -> mz_expr::MirRelationExpr {
    branch(
        id_gen,
        outer,
        col_map,
        cte_map,
        subquery_expr,
        apply_requires_distinct_outer,
        |id_gen, expr, get_inner, col_map, cte_map| {
            let exists = expr
                // compute for every row in get_inner
                .applied_to(id_gen, get_inner.clone(), col_map, cte_map)
                // throw away actual values and just remember whether or not there were __any__ rows
                .distinct_by((0..get_inner.arity()).collect())
                // Append true to anything that returned any rows. This
                // join is logically equivalent to
                // `.map(vec![Datum::True])`, but using a join allows
                // for potential predicate pushdown and elision in the
                // optimizer.
                .product(mz_expr::MirRelationExpr::constant(
                    vec![vec![Datum::True]],
                    RelationType::new(vec![ScalarType::Bool.nullable(false)]),
                ));
            // append False to anything that didn't return any rows
            get_inner.lookup(id_gen, exists, vec![(Datum::False, ScalarType::Bool)])
        },
    )
}

impl AggregateExpr {
    fn applied_to(
        self,
        id_gen: &mut mz_ore::id_gen::IdGen,
        col_map: &ColumnMap,
        cte_map: &mut CteMap,
        inner: &mut mz_expr::MirRelationExpr,
    ) -> mz_expr::AggregateExpr {
        let AggregateExpr {
            func,
            expr,
            distinct,
        } = self;

        mz_expr::AggregateExpr {
            func: func.into_expr(),
            expr: expr.applied_to(id_gen, col_map, cte_map, inner, &None),
            distinct,
        }
    }
}

// TODO: move this to the `mz_expr` crate.
/// If the on clause of an outer join is an equijoin, figure out the join keys.
///
/// `oa`, `la`, and `ra` are the arities of `outer`, the lhs, and the rhs
/// respectively.
pub(crate) fn derive_equijoin_cols(
    oa: usize,
    la: usize,
    ra: usize,
    on: Vec<mz_expr::MirScalarExpr>,
) -> Option<(Vec<usize>, Vec<usize>)> {
    use mz_expr::BinaryFunc;
    // TODO: Replace this predicate deconstruction with
    // `mz_expr::canonicalize::canonicalize_predicates`, which will also enable
    // treating select * from lhs left join rhs on lhs.id = rhs.id and true as
    // an equijoin.
    // Deconstruct predicates that may be ands of multiple conditions.
    let mut predicates = Vec::new();
    let mut todo = on;
    while let Some(next) = todo.pop() {
        if let mz_expr::MirScalarExpr::CallBinary {
            expr1,
            expr2,
            func: BinaryFunc::And,
        } = next
        {
            todo.push(*expr1);
            todo.push(*expr2);
        } else {
            predicates.push(next)
        }
    }

    // We restrict ourselves to predicates that test column equality between left and right.
    let mut l_keys = Vec::new();
    let mut r_keys = Vec::new();
    for predicate in predicates.iter_mut() {
        if let mz_expr::MirScalarExpr::CallBinary {
            expr1,
            expr2,
            func: BinaryFunc::Eq,
        } = predicate
        {
            if let (mz_expr::MirScalarExpr::Column(c1), mz_expr::MirScalarExpr::Column(c2)) =
                (&mut **expr1, &mut **expr2)
            {
                if *c1 > *c2 {
                    std::mem::swap(c1, c2);
                }
                if (oa <= *c1 && *c1 < oa + la) && (oa + la <= *c2 && *c2 < oa + la + ra) {
                    l_keys.push(*c1);
                    r_keys.push(*c2 - la);
                }
            }
        }
    }
    // If any predicates were not column equivs, give up.
    if l_keys.len() < predicates.len() {
        None
    } else {
        Some((l_keys, r_keys))
    }
}

/// Attempts an efficient outer join, if `on` has equijoin structure.
fn attempt_outer_join(
    left: mz_expr::MirRelationExpr,
    right: mz_expr::MirRelationExpr,
    on: mz_expr::MirScalarExpr,
    kind: JoinKind,
    oa: usize,
    id_gen: &mut mz_ore::id_gen::IdGen,
) -> Option<mz_expr::MirRelationExpr> {
    // Both `left` and `right` are decorrelated inputs, whose first `oa` calumns
    // correspond to an outer context: we should do the outer join independently
    // for each prefix. In the case that `on` is just some equality tests between
    // columns of `left` and `right`, we can employ a relatively simple plan.

    let lt = left.typ().column_types.into_iter().skip(oa).collect_vec();
    let la = lt.len();
    let rt = right.typ().column_types.into_iter().skip(oa).collect_vec();
    let ra = rt.len();

    let equijoin_keys = derive_equijoin_cols(oa, la, ra, vec![on.clone()]);
    if equijoin_keys.is_none() {
        return None;
    }

    let (l_keys, r_keys) = equijoin_keys.unwrap();

    // If we've gotten this far, we can do the clever thing.
    // We'll want to use left and right multiple times
    let result = left.let_in(id_gen, |id_gen, get_left| {
        right.let_in(id_gen, |id_gen, get_right| {
            // TODO: we know that we can re-use the arrangements of left and right
            // needed for the inner join with each of the conditional outer joins.
            // It is not clear whether we should hint that, or just let the planner
            // and optimizer run and see what happens.

            // We'll want the inner join (minus repeated columns)
            let join = mz_expr::MirRelationExpr::join(
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
            .filter(vec![on]);

            // We'll want to re-use the results of the join multiple times.
            join.let_in(id_gen, |id_gen, get_join| {
                let mut result = get_join.clone();

                // A collection of keys present in both left and right collections.
                let both_keys = get_join
                    .project((0..oa).chain(l_keys.clone()).collect::<Vec<_>>())
                    .distinct();

                // The plan is now to determine the left and right rows matched in the
                // inner join, subtract them from left and right respectively, pad what
                // remains with nulls, and fold them in to `result`.

                both_keys.let_in(id_gen, |_id_gen, get_both| {
                    if let JoinKind::LeftOuter { .. } | JoinKind::FullOuter = kind {
                        // Rows in `left` that are matched in the inner equijoin.
                        let left_present = mz_expr::MirRelationExpr::join(
                            vec![get_left.clone(), get_both.clone()],
                            (0..oa)
                                .chain(l_keys.clone())
                                .enumerate()
                                .map(|(i, c)| vec![(0, c), (1, i)])
                                .collect::<Vec<_>>(),
                        )
                        .project((0..(oa + la)).collect());

                        // Determine the types of nulls to use as filler.
                        let right_fill = rt
                            .into_iter()
                            .map(|typ| mz_expr::MirScalarExpr::literal_null(typ.scalar_type))
                            .collect();

                        // Add to `result` absent elements, filled with typed nulls.
                        result = left_present
                            .negate()
                            .union(get_left.clone())
                            .map(right_fill)
                            .union(result);
                    }

                    if let JoinKind::RightOuter | JoinKind::FullOuter = kind {
                        // Rows in `right` that are matched in the inner equijoin.
                        let right_present = mz_expr::MirRelationExpr::join(
                            vec![get_right.clone(), get_both],
                            (0..oa)
                                .chain(r_keys.clone())
                                .enumerate()
                                .map(|(i, c)| vec![(0, c), (1, i)])
                                .collect::<Vec<_>>(),
                        )
                        .project((0..(oa + ra)).collect());

                        // Determine the types of nulls to use as filler.
                        let left_fill = lt
                            .into_iter()
                            .map(|typ| mz_expr::MirScalarExpr::literal_null(typ.scalar_type))
                            .collect();

                        // Add to `result` absent elemetns, prepended with typed nulls.
                        result = right_present
                            .negate()
                            .union(get_right.clone())
                            .map(left_fill)
                            // Permute left fill before right values.
                            .project(
                                (0..oa)
                                    .chain(oa + ra..oa + ra + la)
                                    .chain(oa..oa + ra)
                                    .collect(),
                            )
                            .union(result)
                    }

                    result
                })
            })
        })
    });
    Some(result)
}
