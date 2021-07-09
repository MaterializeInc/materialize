// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Lowering is the process of transforming a `sql::expr::HirRelationExpr`
//! into a `expr::MirRelationExpr`.
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

use ore::collections::CollectionExt;
use repr::RelationType;
use repr::*;

use crate::plan::expr::HirRelationExpr;
use crate::plan::expr::HirScalarExpr;
use crate::plan::expr::{AggregateExpr, ColumnOrder, ColumnRef, JoinKind};
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

impl HirRelationExpr {
    /// Rewrite `self` into a `expr::MirRelationExpr`.
    /// This requires rewriting all correlated subqueries (nested `HirRelationExpr`s) into flat queries
    pub fn lower(mut self) -> expr::MirRelationExpr {
        let mut id_gen = expr::IdGen::default();
        transform_expr::split_subquery_predicates(&mut self);
        transform_expr::try_simplify_quantified_comparisons(&mut self);
        expr::MirRelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
            .let_in(&mut id_gen, |id_gen, get_outer| {
                self.applied_to(id_gen, get_outer, &ColumnMap::empty())
            })
    }

    /// Return a `expr::MirRelationExpr` which evaluates `self` once for each row of `get_outer`.
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
        id_gen: &mut expr::IdGen,
        get_outer: expr::MirRelationExpr,
        col_map: &ColumnMap,
    ) -> expr::MirRelationExpr {
        use self::HirRelationExpr::*;
        use expr::MirRelationExpr as SR;
        if let expr::MirRelationExpr::Get { .. } = &get_outer {
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
            Get { id, typ } => {
                // Get statements are only to external sources, and are not correlated with `get_outer`.
                get_outer.product(SR::Get { id, typ })
            }
            Project { input, outputs } => {
                // Projections should be applied to the decorrelated `inner`, and to its columns,
                // which means rebasing `outputs` to start `get_outer.arity()` columns later.
                let input = input.applied_to(id_gen, get_outer.clone(), col_map);
                let outputs = (0..get_outer.arity())
                    .chain(outputs.into_iter().map(|i| get_outer.arity() + i))
                    .collect::<Vec<_>>();
                input.project(outputs)
            }
            Map { input, scalars } => {
                // Scalar expressions may contain correlated subqueries. We must be cautious!
                let mut input = input.applied_to(id_gen, get_outer, col_map);

                // We will proceed sequentially through the scalar expressions, for each transforming the decorrelated `input`
                // into a relation with potentially more columns capable of addressing the needs of the scalar expression.
                // Having done so, we add the scalar value of interest and trim off any other newly added columns.
                //
                // The sequential traversal is present as expressions are allowed to depend on the values of prior expressions.
                for scalar in scalars {
                    let old_arity = input.arity();
                    let scalar = scalar.applied_to(id_gen, col_map, &mut input);
                    let new_arity = input.arity();
                    input = input.map(vec![scalar]);
                    if old_arity != new_arity {
                        // this means we added some columns to handle subqueries, and now we need to get rid of them
                        input = input.project((0..old_arity).chain(vec![new_arity]).collect());
                    }
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
                    .map(|e| e.applied_to(id_gen, col_map, &mut input))
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
                let mut input = input.applied_to(id_gen, get_outer, col_map);
                for predicate in predicates {
                    let old_arity = input.arity();
                    let predicate = predicate.applied_to(id_gen, col_map, &mut input);
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
            } if kind.is_lateral() => {
                // A LATERAL join is a join in which the right expression has
                // access to the columns in the left expression. It turns out
                // this is *exactly* our branch operator, plus some additional
                // null handling in the case of left joins. (Right and full
                // lateral joins are not permitted.)
                //
                // As with normal joins, the `on` predicate may be correlated,
                // and we treat it as a filter that follows the branch.

                let left = left.applied_to(id_gen, get_outer, col_map);
                left.let_in(id_gen, |id_gen, get_left| {
                    let apply_requires_distinct_outer = false;
                    let mut join = branch(
                        id_gen,
                        get_left.clone(),
                        col_map,
                        *right,
                        apply_requires_distinct_outer,
                        |id_gen, right, get_left, col_map| {
                            right.applied_to(id_gen, get_left, col_map)
                        },
                    );

                    // Plan the `on` predicate.
                    let old_arity = join.arity();
                    let on = on.applied_to(id_gen, col_map, &mut join);
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
                            .map(|typ| (Datum::Null, typ.nullable(true)))
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
                let left = left.applied_to(id_gen, get_outer.clone(), col_map);
                let lt = left.typ();
                let la = left.arity() - oa;
                left.let_in(id_gen, |id_gen, get_left| {
                    let right = right.applied_to(id_gen, get_outer.clone(), col_map);
                    let rt = right.typ();
                    let ra = right.arity() - oa;
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
                        let on = on.applied_to(id_gen, col_map, &mut product);

                        // Attempt an efficient equijoin implementation, in which outer joins are
                        // more efficiently rendered than in general. This can return `None` if
                        // such a plan is not possible, for example if `on` does not describe an
                        // equijoin between columns of `left` and `right`.
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

                        // Otherwise, perform a more general join.
                        let mut join = product.filter(vec![on]);
                        let new_arity = join.arity();
                        if old_arity != new_arity {
                            // this means we added some columns to handle subqueries, and now we need to get rid of them
                            join = join.project((0..old_arity).collect());
                        }
                        join.let_in(id_gen, |id_gen, get_join| {
                            let mut result = get_join.clone();
                            if let JoinKind::LeftOuter { .. } | JoinKind::FullOuter { .. } = kind {
                                let left_outer = get_left.clone().anti_lookup(
                                    id_gen,
                                    get_join.clone(),
                                    rt.column_types
                                        .into_iter()
                                        .skip(oa)
                                        .map(|typ| (Datum::Null, typ.nullable(true)))
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
                                        lt.column_types
                                            .into_iter()
                                            .skip(oa)
                                            .map(|typ| (Datum::Null, typ.nullable(true)))
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
                    base: Box::new(base.applied_to(id_gen, get_outer.clone(), col_map)),
                    inputs: inputs
                        .into_iter()
                        .map(|input| input.applied_to(id_gen, get_outer.clone(), col_map))
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
                let mut input = input.applied_to(id_gen, get_outer.clone(), col_map);
                let applied_group_key = (0..get_outer.arity())
                    .chain(group_key.iter().map(|i| get_outer.arity() + i))
                    .collect();
                let applied_aggregates = aggregates
                    .into_iter()
                    .map(|aggregate| aggregate.applied_to(id_gen, col_map, &mut input))
                    .collect::<Vec<_>>();
                let input_type = input.typ();
                let default = applied_aggregates
                    .iter()
                    .map(|agg| {
                        (
                            agg.func.default(),
                            agg.typ(&input_type).nullable(agg.func.default().is_null()),
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
                input.applied_to(id_gen, get_outer, col_map).distinct()
            }
            TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
            } => {
                // TopK is uncomplicated, except that we must group by the columns of `get_outer` as well.
                let input = input.applied_to(id_gen, get_outer.clone(), col_map);
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
                input.applied_to(id_gen, get_outer, col_map).negate()
            }
            Threshold { input } => {
                // Threshold is uncomplicated.
                input.applied_to(id_gen, get_outer, col_map).threshold()
            }
            DeclareKeys { input, keys } => input
                .applied_to(id_gen, get_outer, col_map)
                .declare_keys(keys),
        }
    }
}

impl HirScalarExpr {
    /// Rewrite `self` into a `expr::ScalarExpr` which can be applied to the modified `inner`.
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
        id_gen: &mut expr::IdGen,
        col_map: &ColumnMap,
        inner: &mut expr::MirRelationExpr,
    ) -> expr::MirScalarExpr {
        use self::HirScalarExpr::*;
        use expr::MirScalarExpr as SS;

        match self {
            Column(col_ref) => SS::Column(col_map.get(&col_ref)),
            Literal(row, typ) => SS::Literal(Ok(row), typ),
            Parameter(_) => panic!("cannot decorrelate expression with unbound parameters"),
            CallNullary(func) => SS::CallNullary(func),
            CallUnary { func, expr } => SS::CallUnary {
                func,
                expr: Box::new(expr.applied_to(id_gen, col_map, inner)),
            },
            CallBinary { func, expr1, expr2 } => SS::CallBinary {
                func,
                expr1: Box::new(expr1.applied_to(id_gen, col_map, inner)),
                expr2: Box::new(expr2.applied_to(id_gen, col_map, inner)),
            },
            CallVariadic { func, exprs } => SS::CallVariadic {
                func,
                exprs: exprs
                    .into_iter()
                    .map(|expr| expr.applied_to(id_gen, col_map, inner))
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
                let cond_expr = cond.applied_to(id_gen, col_map, inner);

                // Defensive copies, in case we mangle these in decorrelation.
                let inner_clone = inner.clone();
                let then_clone = then.clone();
                let else_clone = els.clone();

                let cond_arity = inner.arity();
                let then_expr = then.applied_to(id_gen, col_map, inner);
                let else_expr = els.applied_to(id_gen, col_map, inner);

                if cond_arity == inner.arity() {
                    // If no additional columns were added, we simply return the
                    // `If` variant with the updated expressions.
                    SS::If {
                        cond: Box::new(cond_expr),
                        then: Box::new(then_expr),
                        els: Box::new(else_expr),
                    }
                } else {
                    // If columns were added, we need a more careful approch, as
                    // described above. First, we need to de-correlate each of
                    // the two expressions independently, and apply their cases
                    // as `MirRelationExpr::Map` operations.

                    *inner = inner_clone.let_in(id_gen, |id_gen, get_inner| {
                        // Restrict to records satisfying `cond_expr` and apply `then` as a map.
                        let mut then_inner = get_inner.clone().filter(vec![cond_expr.clone()]);
                        let then_expr = then_clone.applied_to(id_gen, col_map, &mut then_inner);
                        let then_arity = then_inner.arity();
                        then_inner = then_inner
                            .map(vec![then_expr])
                            .project((0..inner_arity).chain(Some(then_arity)).collect());

                        // Restrict to records not satisfying `cond_expr` and apply `els` as a map.
                        let mut else_inner = get_inner.filter(vec![SS::CallBinary {
                            func: expr::BinaryFunc::Or,
                            expr1: Box::new(SS::CallBinary {
                                func: expr::BinaryFunc::Eq,
                                expr1: Box::new(cond_expr.clone()),
                                expr2: Box::new(SS::literal_ok(Datum::False, ScalarType::Bool)),
                            }),
                            expr2: Box::new(SS::CallUnary {
                                func: expr::UnaryFunc::IsNull,
                                expr: Box::new(cond_expr.clone()),
                            }),
                        }]);
                        let else_expr = else_clone.applied_to(id_gen, col_map, &mut else_inner);
                        let else_arity = else_inner.arity();
                        else_inner = else_inner
                            .map(vec![else_expr])
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
                *inner = branch(
                    id_gen,
                    inner.take_dangerous(),
                    col_map,
                    *expr,
                    apply_requires_distinct_outer,
                    |id_gen, expr, get_inner, col_map| {
                        let exists = expr
                            // compute for every row in get_inner
                            .applied_to(id_gen, get_inner.clone(), col_map)
                            // throw away actual values and just remember whether or not there were __any__ rows
                            .distinct_by((0..get_inner.arity()).collect())
                            // Append true to anything that returned any rows. This
                            // join is logically equivalent to
                            // `.map(vec![Datum::True])`, but using a join allows
                            // for potential predicate pushdown and elision in the
                            // optimizer.
                            .product(expr::MirRelationExpr::constant(
                                vec![vec![Datum::True]],
                                RelationType::new(vec![ScalarType::Bool.nullable(false)]),
                            ));
                        // append False to anything that didn't return any rows
                        let default = vec![(Datum::False, ScalarType::Bool.nullable(false))];
                        get_inner.lookup(id_gen, exists, default)
                    },
                );
                SS::Column(inner.arity() - 1)
            }

            Select(expr) => {
                let apply_requires_distinct_outer = true;
                *inner = branch(
                    id_gen,
                    inner.take_dangerous(),
                    col_map,
                    *expr,
                    apply_requires_distinct_outer,
                    |id_gen, expr, get_inner, col_map| {
                        let select = expr
                            // compute for every row in get_inner
                            .applied_to(id_gen, get_inner.clone(), col_map);
                        let col_type = select.typ().column_types.into_last();

                        let inner_arity = get_inner.arity();
                        // We must determine a count for each `get_inner` prefix,
                        // and report an error if that count exceeds one.
                        let guarded = select.let_in(id_gen, |_id_gen, get_select| {
                            // Count for each `get_inner` prefix.
                            let counts = get_select.clone().reduce(
                                (0..inner_arity).collect::<Vec<_>>(),
                                vec![expr::AggregateExpr {
                                    func: expr::AggregateFunc::Count,
                                    expr: expr::MirScalarExpr::literal_ok(
                                        Datum::True,
                                        ScalarType::Bool,
                                    ),
                                    distinct: false,
                                }],
                                None,
                            );
                            // Errors should result from counts > 1.
                            let errors = counts
                                .filter(vec![expr::MirScalarExpr::Column(inner_arity).call_binary(
                                    expr::MirScalarExpr::literal_ok(
                                        Datum::Int64(1),
                                        ScalarType::Int64,
                                    ),
                                    expr::BinaryFunc::Gt,
                                )])
                                .project((0..inner_arity).collect::<Vec<_>>())
                                .map(vec![expr::MirScalarExpr::literal(
                                    Err(expr::EvalError::MultipleRowsFromSubquery),
                                    col_type.clone().scalar_type,
                                )]);
                            // Return `get_select` and any errors added in.
                            get_select.union(errors)
                        });
                        // append Null to anything that didn't return any rows
                        let default = vec![(Datum::Null, col_type.nullable(true))];
                        get_inner.lookup(id_gen, guarded, default)
                    },
                );
                SS::Column(inner.arity() - 1)
            }
        }
    }

    /// Rewrites `self` into a `expr::ScalarExpr`.
    pub fn lower_uncorrelated(self) -> Result<expr::MirScalarExpr, anyhow::Error> {
        use self::HirScalarExpr::*;
        use expr::MirScalarExpr as SS;

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
            Select { .. } | Exists { .. } | Parameter(..) | Column(..) => {
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
    id_gen: &mut expr::IdGen,
    outer: expr::MirRelationExpr,
    col_map: &ColumnMap,
    mut inner: HirRelationExpr,
    apply_requires_distinct_outer: bool,
    apply: F,
) -> expr::MirRelationExpr
where
    F: FnOnce(
        &mut expr::IdGen,
        HirRelationExpr,
        expr::MirRelationExpr,
        &ColumnMap,
    ) -> expr::MirRelationExpr,
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
    inner.visit(&mut |expr| match expr {
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
            apply(id_gen, inner, get_outer, &new_col_map)
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
    let mut new_col_map = HashMap::new();
    let mut key = vec![];
    for col in outer_cols {
        new_col_map.insert(col, key.len());
        key.push(col_map.get(&ColumnRef {
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
            expr::MirRelationExpr::constant(vec![vec![]], RelationType::new(vec![]))
        } else {
            get_outer.clone().distinct_by(key.clone())
        };
        keyed_outer.let_in(id_gen, |id_gen, get_keyed_outer| {
            let oa = get_outer.arity();
            let branch = apply(id_gen, inner, get_keyed_outer, &new_col_map);
            let ba = branch.arity();
            let joined = expr::MirRelationExpr::join(
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

impl AggregateExpr {
    fn applied_to(
        self,
        id_gen: &mut expr::IdGen,
        col_map: &ColumnMap,
        inner: &mut expr::MirRelationExpr,
    ) -> expr::AggregateExpr {
        let AggregateExpr {
            func,
            expr,
            distinct,
        } = self;

        expr::AggregateExpr {
            func: func.into_expr(),
            expr: expr.applied_to(id_gen, col_map, inner),
            distinct,
        }
    }
}

/// Attempts an efficient outer join, if `on` has equijoin structure.
fn attempt_outer_join(
    left: expr::MirRelationExpr,
    right: expr::MirRelationExpr,
    on: expr::MirScalarExpr,
    kind: JoinKind,
    oa: usize,
    id_gen: &mut expr::IdGen,
) -> Option<expr::MirRelationExpr> {
    use expr::BinaryFunc;

    // Both `left` and `right` are decorrelated inputs, whose first `oa` calumns
    // correspond to an outer context: we should do the outer join independently
    // for each prefix. In the case that `on` is just some equality tests between
    // columns of `left` and `right`, we can employ a relatively simple plan.

    let la = left.arity() - oa;
    let lt = left.typ();
    let ra = right.arity() - oa;
    let rt = right.typ();

    // Deconstruct predicates that may be ands of multiple conditions.
    let mut predicates = Vec::new();
    let mut todo = vec![on.clone()];
    while let Some(next) = todo.pop() {
        if let expr::MirScalarExpr::CallBinary {
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
        if let expr::MirScalarExpr::CallBinary {
            expr1,
            expr2,
            func: BinaryFunc::Eq,
        } = predicate
        {
            if let (expr::MirScalarExpr::Column(c1), expr::MirScalarExpr::Column(c2)) =
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
        return None;
    }

    // If we've gotten this far, we can do the clever thing.
    // We'll want to use left and right multiple times
    let result = left.let_in(id_gen, |id_gen, get_left| {
        right.let_in(id_gen, |id_gen, get_right| {
            // TODO: we know that we can re-use the arrangements of left and right
            // needed for the inner join with each of the conditional outer joins.
            // It is not clear whether we should hint that, or just let the planner
            // and optimizer run and see what happens.

            // We'll want the inner join (minus repeated columns)
            let join = expr::MirRelationExpr::join(
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
                        let left_present = expr::MirRelationExpr::join(
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
                            .column_types
                            .into_iter()
                            .skip(oa)
                            .map(|typ| expr::MirScalarExpr::literal_null(typ.scalar_type))
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
                        let right_present = expr::MirRelationExpr::join(
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
                            .column_types
                            .into_iter()
                            .skip(oa)
                            .map(|typ| expr::MirScalarExpr::literal_null(typ.scalar_type))
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
