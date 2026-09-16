// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations of SQL IR, before decorrelation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;
use std::{iter, mem};

use itertools::Itertools;
use mz_expr::WindowFrame;
use mz_expr::func::variadic::RecordCreate;
use mz_expr::visit::{Visit, VisitChildren};
use mz_expr::{ColumnOrder, UnaryFunc, VariadicFunc};
use mz_ore::stack::{RecursionLimitError, maybe_grow};
use mz_repr::{ColumnName, SqlColumnType, SqlRelationType, SqlScalarType};

use crate::plan::hir::{
    AbstractExpr, AggregateFunc, AggregateWindowExpr, ColumnRef, HirRelationExpr, HirScalarExpr,
    ValueWindowExpr, ValueWindowFunc, WindowExpr,
};
use crate::plan::{AggregateExpr, WindowExprType};

/// Rewrites predicates that contain subqueries so that the subqueries
/// appear in their own later predicate when possible.
///
/// For example, this function rewrites this expression
///
/// ```text
/// Filter {
///     predicates: [a = b AND EXISTS (<subquery 1>) AND c = d AND (<subquery 2>) = e]
/// }
/// ```
///
/// like so:
///
/// ```text
/// Filter {
///     predicates: [
///         a = b AND c = d,
///         EXISTS (<subquery>),
///         (<subquery 2>) = e,
///     ]
/// }
/// ```
///
/// The rewrite causes decorrelation to incorporate prior predicates into
/// the outer relation upon which the subquery is evaluated. In the above
/// rewritten example, the `EXISTS (<subquery>)` will only be evaluated for
/// outer rows where `a = b AND c = d`. The second subquery, `(<subquery 2>)
/// = e`, will be further restricted to outer rows that match `A = b AND c =
/// d AND EXISTS(<subquery>)`. This can vastly reduce the cost of the
/// subquery, especially when the original conjunction contains join keys.
pub fn split_subquery_predicates(expr: &mut HirRelationExpr) -> Result<(), RecursionLimitError> {
    fn walk_relation(expr: &mut HirRelationExpr) -> Result<(), RecursionLimitError> {
        #[allow(deprecated)]
        expr.visit_mut_fallible(0, &mut |expr, _| {
            match expr {
                HirRelationExpr::Map { scalars, .. } => {
                    for scalar in scalars {
                        walk_scalar(scalar)?;
                    }
                }
                HirRelationExpr::CallTable { exprs, .. } => {
                    for expr in exprs {
                        walk_scalar(expr)?;
                    }
                }
                HirRelationExpr::Filter { predicates, .. } => {
                    let mut subqueries = vec![];
                    for predicate in &mut *predicates {
                        walk_scalar(predicate)?;
                        extract_conjuncted_subqueries(predicate, &mut subqueries)?;
                    }
                    // TODO(benesch): we could be smarter about the order in which
                    // we emit subqueries. At the moment we just emit in the order
                    // we discovered them, but ideally we'd emit them in an order
                    // that accounted for their cost/selectivity. E.g., low-cost,
                    // high-selectivity subqueries should go first.
                    for subquery in subqueries {
                        predicates.push(subquery);
                    }
                }
                _ => (),
            }
            Ok(())
        })
    }

    fn walk_scalar(expr: &mut HirScalarExpr) -> Result<(), RecursionLimitError> {
        expr.try_visit_direct_subqueries_mut(&mut walk_relation)
    }

    fn contains_subquery(expr: &HirScalarExpr) -> Result<bool, RecursionLimitError> {
        let mut found = false;
        expr.try_visit_direct_subqueries(|_| {
            found = true;
            Ok(())
        })?;
        Ok(found)
    }

    /// Extracts subqueries from a conjunction into `out`.
    ///
    /// For example, given an expression like
    ///
    /// ```text
    /// a = b AND EXISTS (<subquery 1>) AND c = d AND (<subquery 2>) = e
    /// ```
    ///
    /// this function rewrites the expression to
    ///
    /// ```text
    /// a = b AND true AND c = d AND true
    /// ```
    ///
    /// and returns the expression fragments `EXISTS (<subquery 1>)` and
    /// `(<subquery 2>) = e` in the `out` vector.
    fn extract_conjuncted_subqueries(
        expr: &mut HirScalarExpr,
        out: &mut Vec<HirScalarExpr>,
    ) -> Result<(), RecursionLimitError> {
        match expr {
            HirScalarExpr::CallVariadic {
                func: VariadicFunc::And(_),
                exprs,
                name: _,
            } => {
                exprs
                    .into_iter()
                    .try_for_each(|e| extract_conjuncted_subqueries(e, out))?;
            }
            expr if contains_subquery(expr)? => {
                out.push(mem::replace(expr, HirScalarExpr::literal_true()))
            }
            _ => (),
        }
        Ok(())
    }

    walk_relation(expr)
}

/// Rewrites quantified comparisons into simpler EXISTS operators.
///
/// Note that this transformation is only valid when the expression is
/// used in a context where the distinction between `FALSE` and `NULL`
/// is immaterial, e.g., in a `WHERE` clause or a `CASE` condition, or
/// when the inputs to the comparison are non-nullable. This function is careful
/// to only apply the transformation when it is valid to do so.
///
/// ```ignore
/// WHERE (SELECT any(<pred>) FROM <rel>)
/// =>
/// WHERE EXISTS(SELECT * FROM <rel> WHERE <pred>)
///
/// WHERE (SELECT all(<pred>) FROM <rel>)
/// =>
/// WHERE NOT EXISTS(SELECT * FROM <rel> WHERE (NOT <pred>) OR <pred> IS NULL)
/// ```
///
/// See Section 3.5 of "Execution Strategies for SQL Subqueries" by
/// M. Elhemali, et al.
pub fn try_simplify_quantified_comparisons(
    expr: &mut HirRelationExpr,
    simplify_join_on: bool,
) -> Result<(), RecursionLimitError> {
    // There is nothing to simplify unless the query contains a subquery. Bail
    // early in that common case: `walk_relation` recomputes `input.typ()` at
    // every level, which is O(depth^2) over a deep relation tree (e.g. a long
    // JOIN or CTE chain) and would wedge the coordinator.
    if !relation_contains_subquery(expr) {
        return Ok(());
    }

    fn walk_relation(
        expr: &mut HirRelationExpr,
        outers: &[SqlRelationType],
        simplify_join_on: bool,
    ) -> Result<(), RecursionLimitError> {
        // Grow the stack: recurses over a user-controlled-depth relation tree.
        maybe_grow(|| {
            match expr {
                HirRelationExpr::Map { scalars, input } => {
                    walk_relation(input, outers, simplify_join_on)?;
                    let mut outers = outers.to_vec();
                    outers.insert(0, input.typ(&outers, &NO_PARAMS));
                    for scalar in scalars {
                        walk_scalar(scalar, &outers, false, simplify_join_on)?;
                        let (inner, outers) = outers
                            .split_first_mut()
                            .expect("outers known to have at least one element");
                        let scalar_type = scalar.typ(outers, inner, &NO_PARAMS);
                        inner.column_types.push(scalar_type);
                    }
                }
                HirRelationExpr::Filter { predicates, input } => {
                    walk_relation(input, outers, simplify_join_on)?;
                    let mut outers = outers.to_vec();
                    outers.insert(0, input.typ(&outers, &NO_PARAMS));
                    for pred in predicates {
                        walk_scalar(pred, &outers, true, simplify_join_on)?;
                    }
                }
                HirRelationExpr::CallTable { exprs, .. } => {
                    let mut outers = outers.to_vec();
                    outers.insert(0, SqlRelationType::empty());
                    for scalar in exprs {
                        walk_scalar(scalar, &outers, false, simplify_join_on)?;
                    }
                }
                HirRelationExpr::Join {
                    left, right, on, ..
                } => {
                    walk_relation(left, outers, simplify_join_on)?;
                    let left_type = left.typ(outers, &NO_PARAMS);
                    let mut outers = outers.to_vec();
                    outers.insert(0, left_type);
                    walk_relation(right, &outers, simplify_join_on)?;
                    if simplify_join_on {
                        // Build outers with the full join output type, since the
                        // ON clause can reference columns from both sides.
                        let right_type = right.typ(&outers, &NO_PARAMS);
                        let mut join_columns = outers[0].column_types.clone();
                        join_columns.extend(right_type.column_types);
                        outers[0] = SqlRelationType::new(join_columns);
                        walk_scalar(on, &outers, true, simplify_join_on)?;
                    }
                }
                expr => {
                    #[allow(deprecated)]
                    let _ = expr.visit1_mut(0, &mut |expr, _| -> Result<(), RecursionLimitError> {
                        walk_relation(expr, outers, simplify_join_on)
                    });
                }
            }
            Ok(())
        })
    }

    fn walk_scalar(
        expr: &mut HirScalarExpr,
        outers: &[SqlRelationType],
        mut in_filter: bool,
        simplify_join_on: bool,
    ) -> Result<(), RecursionLimitError> {
        expr.try_visit_mut_pre(&mut |e| {
            match e {
                HirScalarExpr::Exists(input, _name) => {
                    walk_relation(input, outers, simplify_join_on)?
                }
                HirScalarExpr::Select(input, _name) => {
                    walk_relation(input, outers, simplify_join_on)?;

                    // We're inside a `(SELECT ...)` subquery. Now let's see if
                    // it has the form `(SELECT <any|all>(...) FROM <input>)`.
                    // Ideally we could do this with one pattern, but Rust's pattern
                    // matching engine is not powerful enough, so we have to do this
                    // in stages; the early returns avoid brutal nesting.

                    let (func, expr, input) = match &mut **input {
                        HirRelationExpr::Reduce {
                            group_key,
                            aggregates,
                            input,
                            expected_group_size: _,
                        } if group_key.is_empty() && aggregates.len() == 1 => {
                            let agg = &mut aggregates[0];
                            (&agg.func, &mut agg.expr, input)
                        }
                        _ => return Ok(()),
                    };

                    if !in_filter && column_type(outers, input, expr).nullable {
                        // Unless we're directly inside a WHERE, this
                        // transformation is only valid if the expression involved
                        // is non-nullable.
                        return Ok(());
                    }

                    match func {
                        AggregateFunc::Any => {
                            // Found `(SELECT any(<expr>) FROM <input>)`. Rewrite to
                            // `EXISTS(SELECT 1 FROM <input> WHERE <expr>)`.
                            *e = input.take().filter(vec![expr.take()]).exists();
                        }
                        AggregateFunc::All => {
                            // Found `(SELECT all(<expr>) FROM <input>)`. Rewrite to
                            // `NOT EXISTS(SELECT 1 FROM <input> WHERE NOT <expr> OR <expr> IS NULL)`.
                            //
                            // Note that negation of <expr> alone is insufficient.
                            // Consider that `WHERE <pred>` filters out rows if
                            // `<pred>` is false *or* null. To invert the test, we
                            // need `NOT <pred> OR <pred> IS NULL`.
                            let expr = expr.take();
                            let filter = expr.clone().not().or(expr.call_is_null());
                            *e = input.take().filter(vec![filter]).exists().not();
                        }
                        _ => (),
                    }
                }
                _ => {
                    // As soon as we see *any* scalar expression, we are no longer
                    // directly inside a filter.
                    in_filter = false;
                }
            }
            Ok(())
        })
    }

    walk_relation(expr, &[], simplify_join_on)
}

/// Collapses `EXISTS` over a FROM-less subquery into an equivalent scalar
/// predicate on the outer row, so that decorrelation produces a plain `Filter`
/// rather than a semijoin (for `EXISTS`) or an antijoin (for `NOT EXISTS`).
///
/// A FROM-less subquery is a chain of `Map`, `Project`, and `Filter` nodes over
/// a single-row `Constant` (the join identity of a query with no `FROM`
/// clause). Such a subquery yields exactly one row when every `Filter`
/// predicate is `TRUE` and zero rows otherwise, so
///
/// ```text
/// EXISTS(<from-less subquery with predicates p1, p2, ...>) == (p1 AND p2 AND ...) IS TRUE
/// ```
///
/// evaluated on the outer row. The `IS TRUE` is mandatory for null safety. An
/// empty subquery (some predicate `FALSE` or `NULL`) must make `EXISTS` return
/// `FALSE`, which `IS TRUE` reproduces while a bare predicate would leak `NULL`.
/// `NOT EXISTS` then becomes `NOT ((...) IS TRUE)`, which is `... IS NOT TRUE`
/// and likewise null-safe.
///
/// The rewrite fires only on correlated subqueries, where the predicate
/// references at least one outer column. This keeps it to the pure existence
/// check that a genuine anti/semi-join would otherwise be lowered to, and it
/// avoids changing whether an uncorrelated erroring subquery is evaluated when
/// the outer relation is empty.
///
/// This closes database-issues#2613 (`x IN (SELECT ... WHERE p)`, which
/// [`try_simplify_quantified_comparisons`] has already turned into an `EXISTS`)
/// and database-issues#2969 (`NOT EXISTS (SELECT ... WHERE p)`). It must run
/// after [`try_simplify_quantified_comparisons`].
pub fn simplify_from_less_existence_subqueries(
    expr: &mut HirRelationExpr,
) -> Result<(), RecursionLimitError> {
    // `try_visit_mut_post` walks every relation node, and because
    // `VisitChildren<Self>` for `HirRelationExpr` descends into the bodies of
    // `Exists`/`Select` subqueries, it reaches existence checks at every nesting
    // level. Post-order guarantees a subquery body is simplified before the
    // `Exists` that encloses it.
    expr.try_visit_mut_post(&mut |rel| {
        rel.try_visit_mut_children(|scalar: &mut HirScalarExpr| {
            scalar.try_visit_mut_pre(&mut |e| {
                if let HirScalarExpr::Exists(input, _name) = e {
                    if let Some(pred) = from_less_existence_predicate(input) {
                        *e = pred.call_unary(UnaryFunc::IsTrue(mz_expr::func::IsTrue));
                    }
                }
                Ok(())
            })
        })
    })
}

/// If `sub` is a FROM-less subquery (see
/// [`simplify_from_less_existence_subqueries`]) whose existence check is
/// correlated on the outer row, returns the predicate `p1 AND p2 AND ...`
/// expressed in the outer row's frame. Returns `None` otherwise.
fn from_less_existence_predicate(sub: &HirRelationExpr) -> Option<HirScalarExpr> {
    // A FROM-less subquery is a linear Map/Project/Filter chain over a single-row
    // `Constant`. Both properties of the base are load-bearing for soundness: the
    // single row is what lets EXISTS reduce to "the predicate holds on that row",
    // and the constant is what lets its columns be inlined into the lifted
    // predicate below. A 0-row, multi-row, or non-constant base is a genuine
    // anti/semi-join and bails at the `_` arm.
    //
    // Record the chain top to bottom here; it is replayed bottom to top below.
    let mut chain = Vec::new();
    let mut cur = sub;
    let (row, typ) = loop {
        match cur {
            HirRelationExpr::Filter { input, .. }
            | HirRelationExpr::Map { input, .. }
            | HirRelationExpr::Project { input, .. } => {
                chain.push(cur);
                cur = input.as_ref();
            }
            HirRelationExpr::Constant { rows, typ } if rows.len() == 1 => break (&rows[0], typ),
            _ => return None,
        }
    };

    // `env` holds the value of each column of the current relation, expressed in
    // the subquery's own frame. Because level-0 references are resolved as we go,
    // env entries only ever contain constants and outer (level >= 1) references.
    let mut env: Vec<HirScalarExpr> = row
        .iter()
        .zip_eq(typ.column_types.iter())
        .map(|(datum, col_type)| HirScalarExpr::literal(datum, col_type.scalar_type.clone()))
        .collect();

    // Replay the chain bottom to top so each node sees the `env` built by the nodes
    // beneath it: `Map` extends `env`, `Filter` reads it, `Project` permutes it.
    let mut preds: Vec<HirScalarExpr> = Vec::new();
    for node in chain.iter().rev() {
        match node {
            HirRelationExpr::Filter { predicates, .. } => {
                for predicate in predicates {
                    preds.push(resolve_local_columns(predicate, &env)?);
                }
            }
            HirRelationExpr::Map { scalars, .. } => {
                for scalar in scalars {
                    let resolved = resolve_local_columns(scalar, &env)?;
                    env.push(resolved);
                }
            }
            HirRelationExpr::Project { outputs, .. } => {
                env = outputs
                    .iter()
                    .map(|i| env.get(*i).cloned())
                    .collect::<Option<Vec<_>>>()?;
            }
            _ => unreachable!("chain only contains Filter, Map, and Project nodes"),
        }
    }

    let mut pred = HirScalarExpr::variadic_and(preds);

    // `pred` is built only from predicates that `resolve_local_columns` accepted,
    // and that rejects any subquery, so `pred` contains no nested subqueries. Every
    // column reference is therefore in the subquery's own frame at nesting depth 0,
    // and an outer reference is exactly one with `level > 0`.

    // Require correlation: the predicate must reference an outer column. Without
    // correlation this is not the existence check a genuine anti/semi-join lowers
    // to, and firing would risk changing when a constant erroring predicate is
    // evaluated.
    let mut correlated = false;
    pred.visit_post(&mut |e| {
        if let HirScalarExpr::Column(col, _name) = e {
            if col.level > 0 {
                correlated = true;
            }
        }
    });
    if !correlated {
        return None;
    }

    // Lift the predicate out of the subquery: references to the immediately
    // enclosing (outer) scope move down one level.
    pred.visit_mut_post(&mut |e| {
        if let HirScalarExpr::Column(col, _name) = e {
            if col.level > 0 {
                col.level -= 1;
            }
        }
    });

    Some(pred)
}

/// Returns `expr` with every reference to the current scope (a [`ColumnRef`]
/// with `level == 0`) replaced by its value from `env`. Returns `None` if `expr`
/// cannot be soundly lifted into the outer scope, or references a column absent
/// from `env`.
fn resolve_local_columns(expr: &HirScalarExpr, env: &[HirScalarExpr]) -> Option<HirScalarExpr> {
    // Every scalar in the FROM-less body is substituted into the outer scope, so
    // reject any that cannot be evaluated equivalently there. The match is
    // exhaustive on purpose: a new `HirScalarExpr` variant must be classified here
    // rather than silently treated as liftable.
    let mut unliftable = false;
    expr.visit_post(&mut |e| {
        let liftable = match e {
            // Row-local: the value depends only on the row, so it is the same in
            // the subquery's frame and the outer frame.
            HirScalarExpr::Column(..)
            | HirScalarExpr::Parameter(..)
            | HirScalarExpr::Literal(..)
            | HirScalarExpr::CallUnmaterializable(..)
            | HirScalarExpr::CallUnary { .. }
            | HirScalarExpr::CallBinary { .. }
            | HirScalarExpr::CallVariadic { .. }
            | HirScalarExpr::If { .. } => true,
            // A subquery carries its own nested scopes that this flat substitution
            // does not handle. A window function over the single-row body (e.g.
            // `row_number() OVER ()` is always 1) is not the same function over the
            // multi-row outer relation. Neither may cross the subquery boundary.
            HirScalarExpr::Exists(..)
            | HirScalarExpr::Select(..)
            | HirScalarExpr::Windowing(..) => false,
        };
        unliftable |= !liftable;
    });
    if unliftable {
        return None;
    }

    let mut expr = expr.clone();
    let mut ok = true;
    expr.visit_mut_post(&mut |e| {
        if let HirScalarExpr::Column(ColumnRef { level: 0, column }, _name) = e {
            match env.get(*column) {
                Some(value) => *e = value.clone(),
                None => ok = false,
            }
        }
    });
    ok.then_some(expr)
}

/// Returns whether `expr` contains any subquery (`HirScalarExpr::Exists` or
/// `HirScalarExpr::Select`). Both the relation tree and the per-node scalars are
/// traversed iteratively, so this is stack-safe on deeply nested inputs: a long
/// JOIN/CTE chain grows the relation tree, and a flat `CASE` with many arms
/// lowers to a deep right-nested `If` chain in a single scalar. `visit_pre` on
/// `HirScalarExpr` stops at `Exists`/`Select` (they are scalar leaves), so the
/// scan never descends into subquery bodies. The relation walk already yields
/// those bodies as its own children.
fn relation_contains_subquery(expr: &HirRelationExpr) -> bool {
    let mut found = false;
    expr.visit_post(&mut |r: &HirRelationExpr| {
        if !found {
            VisitChildren::<HirScalarExpr>::visit_children(r, |s| {
                s.visit_pre(&mut |e: &HirScalarExpr| {
                    if matches!(e, HirScalarExpr::Exists(..) | HirScalarExpr::Select(..)) {
                        found = true;
                    }
                });
            });
        }
    });
    found
}

/// An empty parameter type map.
///
/// These transformations are expected to run after parameters are bound, so
/// there is no need to provide any parameter type information.
static NO_PARAMS: LazyLock<BTreeMap<usize, SqlScalarType>> = LazyLock::new(BTreeMap::new);

fn column_type(
    outers: &[SqlRelationType],
    inner: &HirRelationExpr,
    expr: &HirScalarExpr,
) -> SqlColumnType {
    let inner_type = inner.typ(outers, &NO_PARAMS);
    expr.typ(outers, &inner_type, &NO_PARAMS)
}

impl HirScalarExpr {
    /// Similar to `MirScalarExpr::support`, but adapted to `HirScalarExpr` in a special way: it
    /// considers column references that target the root level.
    /// (See `visit_columns_referring_to_root_level`.)
    fn support(&self) -> Vec<usize> {
        let mut result = Vec::new();
        self.visit_columns_referring_to_root_level(&mut |c| result.push(c));
        result
    }

    /// Changes column references in `self` by the given remapping.
    /// Panics if a referred column is not present in `idx_map`!
    fn remap(mut self, idx_map: &BTreeMap<usize, usize>) -> HirScalarExpr {
        self.visit_columns_referring_to_root_level_mut(&mut |c| {
            *c = idx_map[c];
        });
        self
    }
}

/// # Aims and scope
///
/// The aim here is to amortize the overhead of the MIR window function pattern
/// (see `window_func_applied_to`) by fusing groups of window function calls such
/// that each group can be performed by one instance of the window function MIR
/// pattern.
///
/// For now, we fuse only value window function calls and window aggregations.
/// (We probably won't need to fuse scalar window functions for a long time.)
///
/// For now, we can fuse value window function calls and window aggregations where the
/// A. partition by
/// B. order by
/// C. window frame
/// D. ignore nulls for value window functions and distinct for window aggregations
/// are all the same. (See `extract_options`.)
/// (Later, we could improve this to only need A. to be the same. This would require
/// much more code changes, because then we'd have to blow up `ValueWindowExpr`.
/// TODO: As a much simpler intermediate step, at least we should ignore options that
/// don't matter. For example, we should be able to fuse a `lag` that has a default
/// frame with a `first_value` that has some custom frame, because `lag` is not
/// affected by the frame.)
/// Note that we fuse value window function calls and window aggregations separately.
///
/// # Implementation
///
/// At a high level, what we are going to do is look for Maps with more than one window function
/// calls, and for each Map
/// - remove some groups of window function call expressions from the Map's `scalars`;
/// - insert a fused version of each group;
/// - insert some expressions that decompose the results of the fused calls;
/// - update some column references in `scalars`: those that refer to window function results that
///   participated in fusion, as well as those that refer to columns that moved around due to
///   removing and inserting expressions.
/// - insert a Project above the matched Map to permute columns back to their original places.
///
/// It would be tempting to find groups simply by taking a list of all window function calls
/// and calling `group_by` with a key function that extracts the above A. B. C. D. properties,
/// but a complication is that the possible groups that we could theoretically fuse overlap.
/// This is because when forming groups we need to also take into account column references
/// that point inside the same Map. For example, imagine a Map with the following scalar
/// expressions:
/// C1, E1, C2, C3, where
/// - E1 refers to C1
/// - C3 refers to E1.
/// In this situation, we could either
/// - fuse C1 and C2, and put the fused expression in the place of C1 (so that E1 can keep referring
///   to it);
/// - or fuse C2 and C3.
/// However, we can't fuse all of C1, C2, C3 into one call, because then there would be
/// no appropriate place for the fused expression: it would have to be both before and after E1.
///
/// So, how we actually form the groups is that, keeping track of a list of non-overlapping groups,
/// we go through `scalars`, try to put each expression into each of our groups, and the first of
/// these succeed. When trying to put an expression into a group, we need to be mindful about column
/// references inside the same Map, as noted above. A constraint that we impose on ourselves for
/// sanity is that the fused version of each group will be inserted at the place where the first
/// element of the group originally was. This means that the only condition that we need to check on
/// column references when adding an expression to a group is that all column references in a group
/// should be to columns that are earlier than the first element of the group. (No need to check
/// column references in the other direction, i.e., references in other expressions that refer to
/// columns in the group.)
pub fn fuse_window_functions(
    root: &mut HirRelationExpr,
    _context: &crate::plan::lowering::Context,
) -> Result<(), RecursionLimitError> {
    /// Those options of a window function call that are relevant for fusion.
    #[derive(PartialEq, Eq)]
    enum WindowFuncCallOptions {
        Value(ValueWindowFuncCallOptions),
        Agg(AggregateWindowFuncCallOptions),
    }
    #[derive(PartialEq, Eq)]
    struct ValueWindowFuncCallOptions {
        partition_by: Vec<HirScalarExpr>,
        outer_order_by: Vec<HirScalarExpr>,
        inner_order_by: Vec<ColumnOrder>,
        window_frame: WindowFrame,
        ignore_nulls: bool,
        bucket_key_range: Option<u64>,
    }
    #[derive(PartialEq, Eq)]
    struct AggregateWindowFuncCallOptions {
        partition_by: Vec<HirScalarExpr>,
        outer_order_by: Vec<HirScalarExpr>,
        inner_order_by: Vec<ColumnOrder>,
        window_frame: WindowFrame,
        distinct: bool,
        bucket_key_range: Option<u64>,
    }

    /// Helper function to extract the above options.
    fn extract_options(call: &HirScalarExpr) -> WindowFuncCallOptions {
        match call {
            HirScalarExpr::Windowing(
                WindowExpr {
                    func:
                        WindowExprType::Value(ValueWindowExpr {
                            order_by: inner_order_by,
                            window_frame,
                            ignore_nulls,
                            func: _,
                            args: _,
                        }),
                    partition_by,
                    order_by: outer_order_by,
                    bucket_key_range,
                },
                _name,
            ) => WindowFuncCallOptions::Value(ValueWindowFuncCallOptions {
                partition_by: partition_by.clone(),
                outer_order_by: outer_order_by.clone(),
                inner_order_by: inner_order_by.clone(),
                window_frame: window_frame.clone(),
                ignore_nulls: ignore_nulls.clone(),
                bucket_key_range: *bucket_key_range,
            }),
            HirScalarExpr::Windowing(
                WindowExpr {
                    func:
                        WindowExprType::Aggregate(AggregateWindowExpr {
                            aggregate_expr:
                                AggregateExpr {
                                    distinct,
                                    func: _,
                                    expr: _,
                                },
                            order_by: inner_order_by,
                            window_frame,
                        }),
                    partition_by,
                    order_by: outer_order_by,
                    bucket_key_range,
                },
                _name,
            ) => WindowFuncCallOptions::Agg(AggregateWindowFuncCallOptions {
                partition_by: partition_by.clone(),
                outer_order_by: outer_order_by.clone(),
                inner_order_by: inner_order_by.clone(),
                window_frame: window_frame.clone(),
                distinct: distinct.clone(),
                bucket_key_range: *bucket_key_range,
            }),
            _ => panic!(
                "extract_options should only be called on value window functions or window aggregations"
            ),
        }
    }

    struct FusionGroup {
        /// The original column index of the first element of the group. (This is an index into the
        /// Map's `scalars` plus the arity of the Map's input.)
        first_col: usize,
        /// The options of all the window function calls in the group. (Must be the same for all the
        /// calls.)
        options: WindowFuncCallOptions,
        /// The calls in the group, with their original column indexes.
        calls: Vec<(usize, HirScalarExpr)>,
    }

    impl FusionGroup {
        /// Creates a window function call that is a fused version of all the calls in the group.
        /// `new_col` is the column index where the fused call will be inserted at.
        fn fuse(self, new_col: usize) -> (HirScalarExpr, Vec<HirScalarExpr>) {
            let fused = match self.options {
                WindowFuncCallOptions::Value(options) => {
                    let (fused_funcs, fused_args): (Vec<_>, Vec<_>) = self
                        .calls
                        .iter()
                        .map(|(_idx, call)| {
                            if let HirScalarExpr::Windowing(
                                WindowExpr {
                                    func:
                                        WindowExprType::Value(ValueWindowExpr {
                                            func,
                                            args,
                                            order_by: _,
                                            window_frame: _,
                                            ignore_nulls: _,
                                        }),
                                    partition_by: _,
                                    order_by: _,
                                    bucket_key_range: _,
                                },
                                _name,
                            ) = call
                            {
                                (func.clone(), (**args).clone())
                            } else {
                                panic!("unknown window function in FusionGroup")
                            }
                        })
                        .unzip();
                    let fused_args = HirScalarExpr::call_variadic(
                        RecordCreate {
                            // These field names are not important, because this record will only be an
                            // intermediate expression, which we'll manipulate further before it ends up
                            // anywhere where a column name would be visible.
                            field_names: iter::repeat(ColumnName::from(""))
                                .take(fused_args.len())
                                .collect(),
                        },
                        fused_args,
                    );
                    HirScalarExpr::windowing(WindowExpr {
                        func: WindowExprType::Value(ValueWindowExpr {
                            func: ValueWindowFunc::Fused(fused_funcs),
                            args: Box::new(fused_args),
                            order_by: options.inner_order_by,
                            window_frame: options.window_frame,
                            ignore_nulls: options.ignore_nulls,
                        }),
                        partition_by: options.partition_by,
                        order_by: options.outer_order_by,
                        bucket_key_range: options.bucket_key_range,
                    })
                }
                WindowFuncCallOptions::Agg(options) => {
                    let (fused_funcs, fused_args): (Vec<_>, Vec<_>) = self
                        .calls
                        .iter()
                        .map(|(_idx, call)| {
                            if let HirScalarExpr::Windowing(
                                WindowExpr {
                                    func:
                                        WindowExprType::Aggregate(AggregateWindowExpr {
                                            aggregate_expr:
                                                AggregateExpr {
                                                    func,
                                                    expr,
                                                    distinct: _,
                                                },
                                            order_by: _,
                                            window_frame: _,
                                        }),
                                    partition_by: _,
                                    order_by: _,
                                    bucket_key_range: _,
                                },
                                _name,
                            ) = call
                            {
                                (func.clone(), (**expr).clone())
                            } else {
                                panic!("unknown window function in FusionGroup")
                            }
                        })
                        .unzip();
                    let fused_args = HirScalarExpr::call_variadic(
                        RecordCreate {
                            field_names: iter::repeat(ColumnName::from(""))
                                .take(fused_args.len())
                                .collect(),
                        },
                        fused_args,
                    );
                    HirScalarExpr::windowing(WindowExpr {
                        func: WindowExprType::Aggregate(AggregateWindowExpr {
                            aggregate_expr: AggregateExpr {
                                func: AggregateFunc::FusedWindowAgg { funcs: fused_funcs },
                                expr: Box::new(fused_args),
                                distinct: options.distinct,
                            },
                            order_by: options.inner_order_by,
                            window_frame: options.window_frame,
                        }),
                        partition_by: options.partition_by,
                        order_by: options.outer_order_by,
                        bucket_key_range: options.bucket_key_range,
                    })
                }
            };

            let decompositions = (0..self.calls.len())
                .map(|field| {
                    HirScalarExpr::column(new_col)
                        .call_unary(UnaryFunc::RecordGet(mz_expr::func::RecordGet(field)))
                })
                .collect();

            (fused, decompositions)
        }
    }

    let is_value_or_agg_window_func_call = |scalar_expr: &HirScalarExpr| -> bool {
        // Look for calls only at the root of scalar expressions. This is enough
        // because they are always there, see 72e84bb78.
        match scalar_expr {
            HirScalarExpr::Windowing(
                WindowExpr {
                    func: WindowExprType::Value(ValueWindowExpr { func, .. }),
                    ..
                },
                _name,
            ) => {
                // Exclude those calls that are already fused. (We shouldn't currently
                // encounter these, because we just do one pass, but it's better to be
                // robust against future code changes.)
                !matches!(func, ValueWindowFunc::Fused(..))
            }
            HirScalarExpr::Windowing(
                WindowExpr {
                    func:
                        WindowExprType::Aggregate(AggregateWindowExpr {
                            aggregate_expr: AggregateExpr { func, .. },
                            ..
                        }),
                    ..
                },
                _name,
            ) => !matches!(func, AggregateFunc::FusedWindowAgg { .. }),
            _ => false,
        }
    };

    root.try_visit_mut_post(&mut |rel_expr| {
        match rel_expr {
            HirRelationExpr::Map { input, scalars } => {
                // There will be various variable names involving `idx` or `col`:
                // - `idx` will always be an index into `scalars` or something similar,
                // - `col` will always be a column index,
                //   which is often `arity_before_map` + an index into `scalars`.
                let arity_before_map = input.arity();
                let orig_num_scalars = scalars.len();

                // Collect all value window function calls and window aggregations with their column
                // indexes.
                let value_or_agg_window_func_calls = scalars
                    .iter()
                    .enumerate()
                    .filter(|(_idx, scalar_expr)| is_value_or_agg_window_func_call(scalar_expr))
                    .map(|(idx, call)| (idx + arity_before_map, call.clone()))
                    .collect_vec();
                // Exit early if obviously no chance for fusion.
                if value_or_agg_window_func_calls.len() <= 1 {
                    // Note that we are doing this only for performance. All plans should be exactly
                    // the same even if we comment out the following line.
                    return Ok(());
                }

                // Determine the fusion groups. (Each group will later be fused into one window
                // function call.)
                // Note that this has a quadratic run time with value_or_agg_window_func_calls in
                // the worst case. However, this is fine even with 1000 window function calls.
                let mut groups: Vec<FusionGroup> = Vec::new();
                for (col, call) in value_or_agg_window_func_calls {
                    let options = extract_options(&call);
                    let support = call.support();
                    let to_fuse_with = groups
                        .iter_mut()
                        .filter(|group| {
                            group.options == options && support.iter().all(|c| *c < group.first_col)
                        })
                        .next();
                    if let Some(group) = to_fuse_with {
                        group.calls.push((col, call.clone()));
                    } else {
                        groups.push(FusionGroup {
                            first_col: col,
                            options,
                            calls: vec![(col, call.clone())],
                        });
                    }
                }

                // No fusion to do on groups of 1.
                groups.retain(|g| g.calls.len() > 1);

                let removals: BTreeSet<usize> = groups
                    .iter()
                    .flat_map(|g| g.calls.iter().map(|(col, _)| *col))
                    .collect();

                // Mutate `scalars`.
                // We do this by simultaneously iterating through `scalars` and `groups`. (Note that
                // `groups` is already sorted by `first_col` due to the way it was constructed.)
                // We also compute a remapping of old indexes to new indexes as we go.
                let mut groups_it = groups.drain(..).peekable();
                let mut group = groups_it.next();
                let mut remap = BTreeMap::new();
                remap.extend((0..arity_before_map).map(|col| (col, col)));
                let mut new_col: usize = arity_before_map;
                let mut new_scalars = Vec::new();
                for (old_col, e) in scalars
                    .drain(..)
                    .enumerate()
                    .map(|(idx, e)| (idx + arity_before_map, e))
                {
                    if group.as_ref().is_some_and(|g| g.first_col == old_col) {
                        // The current expression will be fused away, and a fused expression will
                        // appear in its place. Additionally, some new expressions will be inserted
                        // after the fused expression, to decompose the record that is the result of
                        // the fused call.
                        assert!(removals.contains(&old_col));
                        let group_unwrapped = group.expect("checked above");
                        let calls_cols = group_unwrapped
                            .calls
                            .iter()
                            .map(|(col, _call)| *col)
                            .collect_vec();
                        let (fused, decompositions) = group_unwrapped.fuse(new_col);
                        new_scalars.push(fused.remap(&remap));
                        new_scalars.extend(decompositions); // (no remapping needed)
                        new_col += 1;
                        for call_old_col in calls_cols {
                            let present = remap.insert(call_old_col, new_col);
                            assert!(present.is_none());
                            new_col += 1;
                        }
                        group = groups_it.next();
                    } else if removals.contains(&old_col) {
                        assert!(remap.contains_key(&old_col));
                    } else {
                        new_scalars.push(e.remap(&remap));
                        let present = remap.insert(old_col, new_col);
                        assert!(present.is_none());
                        new_col += 1;
                    }
                }
                *scalars = new_scalars;
                assert_eq!(remap.len(), arity_before_map + orig_num_scalars);

                // Add a project to permute columns back to their original places.
                *rel_expr = rel_expr.take().project(
                    (0..arity_before_map)
                        .chain((0..orig_num_scalars).map(|idx| {
                            *remap
                                .get(&(idx + arity_before_map))
                                .expect("all columns should be present by now")
                        }))
                        .collect(),
                );

                assert_eq!(rel_expr.arity(), arity_before_map + orig_num_scalars);
            }
            _ => {}
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deeply nested, subquery-free relation tree must plan without
    /// overflowing the stack. The pre-decorrelation HIR walks
    /// (`split_subquery_predicates`, `try_simplify_quantified_comparisons`)
    /// recurse over its full, user-controlled depth, and the latter would
    /// otherwise recompute `input.typ()` at every level (O(depth^2)).
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn deep_relation_chain_does_not_overflow() {
        const DEPTH: usize = 100_000;
        let mut expr = HirRelationExpr::constant(vec![], SqlRelationType::empty());
        for _ in 0..DEPTH {
            expr = HirRelationExpr::Filter {
                predicates: vec![],
                input: Box::new(expr),
            };
        }

        split_subquery_predicates(&mut expr).unwrap();
        try_simplify_quantified_comparisons(&mut expr, false).unwrap();

        // Dismantle iteratively: dropping the deep tree recursively would itself
        // overflow the stack.
        while let HirRelationExpr::Filter { input, .. } = expr {
            expr = *input;
        }
    }

    /// A shallow relation whose scalar is a deeply nested `If` chain must plan
    /// without overflowing the stack. A flat `CASE` with many arms consumes no
    /// per-arm parser recursion but lowers to a right-nested `If` chain of that
    /// depth, so the subquery scan in `try_simplify_quantified_comparisons` must
    /// scan the scalar iteratively, not once per `If` node.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn deep_scalar_if_chain_does_not_overflow() {
        const DEPTH: usize = 100_000;
        let mut scalar = HirScalarExpr::literal_true();
        for _ in 0..DEPTH {
            scalar = HirScalarExpr::if_then_else(
                HirScalarExpr::literal_true(),
                HirScalarExpr::literal_true(),
                scalar,
            );
        }
        let mut expr = HirRelationExpr::Map {
            input: Box::new(HirRelationExpr::constant(vec![], SqlRelationType::empty())),
            scalars: vec![scalar],
        };

        try_simplify_quantified_comparisons(&mut expr, false).unwrap();

        // Dismantle the `If` chain iteratively: dropping it recursively would
        // itself overflow the stack.
        let HirRelationExpr::Map { mut scalars, .. } = expr else {
            unreachable!()
        };
        let mut scalar = scalars.pop().unwrap();
        while let HirScalarExpr::If { els, .. } = scalar {
            scalar = *els;
        }
    }

    /// Once a subquery defeats the early bail in
    /// `try_simplify_quantified_comparisons`, `walk_relation` recurses over the
    /// full depth of the relation tree and must not overflow.
    ///
    /// The depth stays modest because `walk_relation` recomputes `input.typ()`
    /// at every level, which is O(depth^2). Running on a thread whose stack is
    /// smaller than `mz_ore::stack::STACK_RED_ZONE` is what makes the walk's
    /// `maybe_grow` load-bearing at that depth: without it, the walk overflows.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn deep_relation_chain_with_subquery_does_not_overflow() {
        const DEPTH: usize = 3_000;
        const THREAD_STACK_SIZE: usize = 256 << 10;

        std::thread::Builder::new()
            .stack_size(THREAD_STACK_SIZE)
            .spawn(|| {
                let mut expr = HirRelationExpr::constant(vec![], SqlRelationType::empty());
                for _ in 0..DEPTH {
                    expr = HirRelationExpr::Filter {
                        predicates: vec![],
                        input: Box::new(expr),
                    };
                }
                // A single subquery anywhere in the tree is enough to make the
                // full-depth walk run.
                expr = HirRelationExpr::Filter {
                    predicates: vec![
                        HirRelationExpr::constant(vec![], SqlRelationType::empty()).exists(),
                    ],
                    input: Box::new(expr),
                };

                try_simplify_quantified_comparisons(&mut expr, false).unwrap();

                // Dismantle iteratively: dropping the deep tree recursively
                // would itself overflow this thread's small stack.
                while let HirRelationExpr::Filter { input, .. } = expr {
                    expr = *input;
                }
            })
            .unwrap()
            .join()
            .unwrap();
    }
}

/// Width of one bucket when the leading `ORDER BY` key is an integer, in key
/// units.
///
/// NOTE: This and [`BUCKET_STRIDE_SECONDS`] are provisional, and are constants
/// rather than tunables because the per-update cost is tolerant of the choice
/// rather than sensitive to it. Over a simulated 8000-row partition, every
/// width landing between 200 and 400 rows per bucket came within a factor of
/// two of the best available, so being in the right neighbourhood is what
/// matters. Erring wide is much safer than erring narrow, because the boundary
/// level grows as buckets shrink and eventually dominates. Deriving the width
/// from statistics or from a user hint is the open question recorded in
/// `doc/developer/design/20260916_range_bucketed_window_functions.md`.
const BUCKET_WIDTH_INT: i64 = 4096;

/// Stride of one bucket when the leading `ORDER BY` key is a timestamp.
const BUCKET_STRIDE_SECONDS: i64 = 3600;

/// One `lag`/`lead` constituent of a window call that bucketing can handle.
struct BucketableCall {
    /// `Lag` or `Lead`.
    func: ValueWindowFunc,
    /// The original `row(value, offset, default)` argument record.
    args: HirScalarExpr,
    /// The `value` argument. Recognizing the rows that carry a bucket's
    /// trailing non-nulls under `IGNORE NULLS` needs it.
    value: HirScalarExpr,
    /// The `offset` argument, which must be a literal so that the width of the
    /// boundary region is known without looking at data.
    offset: HirScalarExpr,
    /// Whether `default` is the NULL literal. When it is, an `IGNORE NULLS`
    /// call's own result already distinguishes "the lookback resolved" from
    /// "it ran off the end of the bucket", and no separate marker is needed.
    default_is_null: bool,
}

impl BucketableCall {
    /// The direction a constituent's own lookback runs in.
    fn same_direction(&self) -> ValueWindowFunc {
        self.func.clone()
    }

    /// The opposite direction, which is where a bucket's summary rows sit: a
    /// `lag` is resolved by rows before it, so the rows other buckets need from
    /// this one are at its end, which is what a `lead` marker finds.
    fn opposite_direction(&self) -> ValueWindowFunc {
        match self.func {
            ValueWindowFunc::Lag => ValueWindowFunc::Lead,
            ValueWindowFunc::Lead => ValueWindowFunc::Lag,
            _ => unreachable!("BucketableCall is only built for Lag and Lead"),
        }
    }
}

/// Whether `expr` is the NULL literal.
fn is_null_literal(expr: &HirScalarExpr) -> bool {
    match expr {
        HirScalarExpr::Literal(row, _typ, _name) => row.unpack_first() == mz_repr::Datum::Null,
        _ => false,
    }
}

/// Whether any column reference in `expr` points outside it.
///
/// [`HirRelationExpr::is_correlated`] only reports references exactly one level
/// up, which is not enough to make typing against an empty outer context safe.
fn references_outer_columns(expr: &HirRelationExpr) -> bool {
    let mut found = false;
    #[allow(deprecated)]
    expr.visit_columns(0, &mut |depth, col| {
        if col.level > depth {
            found = true;
        }
    });
    found
}

/// Whether any column reference in `expr` points outside the relation it is
/// attached to. See [`references_outer_columns`].
fn scalar_references_outer_columns(expr: &HirScalarExpr) -> bool {
    let mut found = false;
    #[allow(deprecated)]
    expr.visit_columns(0, &mut |depth, col| {
        if col.level > depth {
            found = true;
        }
    });
    found
}

/// Builds `row(value, offset, NULL)`, the arguments of a marker call.
///
/// The default is always NULL so that an IS NULL test on the marker's result
/// means "the lookback ran out of rows", whatever default the user's own call
/// carries.
fn marker_args(
    value: HirScalarExpr,
    offset: HirScalarExpr,
    value_type: SqlScalarType,
) -> HirScalarExpr {
    HirScalarExpr::call_variadic(
        RecordCreate {
            field_names: iter::repeat(ColumnName::from("")).take(3).collect(),
        },
        vec![value, offset, HirScalarExpr::literal_null(value_type)],
    )
}

/// A monotone, non-decreasing coarsening of `key`, or `None` for key types with
/// no natural one.
///
/// Monotonicity is the whole contract: it is what makes each bucket a
/// contiguous run of the sort order, which is what lets a bucket-local `lag`
/// mean anything. Ties map to one bucket for free, since the coarsening is a
/// function of the key.
fn bucket_expr(
    key: &HirScalarExpr,
    key_type: &SqlScalarType,
    hint: Option<u64>,
) -> Option<HirScalarExpr> {
    use mz_expr::func::{DateBinTimestamp, DateBinTimestampTz, DivInt16, DivInt32, DivInt64};
    use mz_repr::Datum;
    use mz_repr::adt::interval::Interval;

    // Truncating division is monotone for a positive divisor. The bucket that
    // straddles zero ends up twice as wide as the others, which costs nothing.
    // A hint of zero would divide by zero, and a hint that does not fit the key
    // type cannot be honoured, so both fall back rather than erroring: this is a
    // hint, and ignoring an unusable one is better than failing the query.
    let width = hint.filter(|w| *w > 0);
    let int_width = i64::try_from(width.unwrap_or(0)).ok();
    let int_width = match int_width {
        Some(0) | None => BUCKET_WIDTH_INT,
        Some(w) => w,
    };
    let stride_secs = i64::try_from(width.unwrap_or(0))
        .ok()
        .filter(|w| *w > 0)
        .unwrap_or(BUCKET_STRIDE_SECONDS);

    match key_type {
        SqlScalarType::Int16 => Some(key.clone().call_binary(
            HirScalarExpr::literal(
                Datum::Int16(i16::try_from(int_width).ok()?),
                SqlScalarType::Int16,
            ),
            DivInt16,
        )),
        SqlScalarType::Int32 => Some(key.clone().call_binary(
            HirScalarExpr::literal(
                Datum::Int32(i32::try_from(int_width).ok()?),
                SqlScalarType::Int32,
            ),
            DivInt32,
        )),
        SqlScalarType::Int64 => Some(key.clone().call_binary(
            HirScalarExpr::literal(Datum::Int64(int_width), SqlScalarType::Int64),
            DivInt64,
        )),
        SqlScalarType::Timestamp { .. } | SqlScalarType::TimestampTz { .. } => {
            let stride = HirScalarExpr::literal(
                Datum::Interval(Interval::new(0, 0, stride_secs.saturating_mul(1_000_000))),
                SqlScalarType::Interval,
            );
            // `date_bin` takes the stride first and bins toward the origin, so
            // it is monotone in the source.
            if matches!(key_type, SqlScalarType::Timestamp { .. }) {
                Some(stride.call_binary(key.clone(), DateBinTimestamp))
            } else {
                Some(stride.call_binary(key.clone(), DateBinTimestampTz))
            }
        }
        _ => None,
    }
}

/// Splits `args` into one `lag`/`lead` constituent, or returns `None` if the
/// call is not one bucketing can handle.
fn bucketable_call(func: &ValueWindowFunc, args: &HirScalarExpr) -> Option<BucketableCall> {
    if !matches!(func, ValueWindowFunc::Lag | ValueWindowFunc::Lead) {
        return None;
    }
    let HirScalarExpr::CallVariadic {
        func: VariadicFunc::RecordCreate(_),
        exprs,
        ..
    } = args
    else {
        return None;
    };
    let [value, offset, default] = &exprs[..] else {
        return None;
    };
    // A non-literal offset leaves the width of the boundary region unknown, so
    // there is no way to say which rows a bucket owes its neighbours.
    if !matches!(offset, HirScalarExpr::Literal(..)) {
        return None;
    }
    Some(BucketableCall {
        func: func.clone(),
        args: args.clone(),
        value: value.clone(),
        offset: offset.clone(),
        default_is_null: is_null_literal(default),
    })
}

/// Splits eligible `lag`/`lead` windows into a level bucketed by a monotone
/// coarsening of the leading `ORDER BY` key, plus a level that resolves only the
/// rows whose lookback crosses a bucket boundary.
///
/// A changed row otherwise costs a re-sort and re-walk of its whole partition,
/// because the reduce closure is handed the partition's full contents on every
/// invocation. Bucketing makes that a function of the bucket instead.
///
/// The two levels emit results for disjoint sets of rows, in the same shape, so
/// they combine with a union rather than a join. See
/// `doc/developer/design/20260916_range_bucketed_window_functions.md` for the
/// argument that the boundary level sees every row it needs.
pub fn bucket_window_functions(
    root: &mut HirRelationExpr,
    context: &crate::plan::lowering::Context,
) -> Result<(), RecursionLimitError> {
    if !context.config.enable_window_bucketing {
        return Ok(());
    }
    root.try_visit_mut_post(&mut |rel_expr| {
        if let HirRelationExpr::Map { input, scalars } = rel_expr {
            if let Some(rewritten) = bucket_one_window(input, scalars) {
                *rel_expr = rewritten;
            }
        }
        Ok(())
    })
}

/// Rewrites the first bucketable window call in a `Map`, or returns `None`.
///
/// Only the first call is rewritten, because the replacement contains window
/// calls of its own and rewriting those in turn would not terminate. Fusion has
/// already run by this point, so a `Map` holds at most one call per window.
fn bucket_one_window(
    input: &HirRelationExpr,
    scalars: &[HirScalarExpr],
) -> Option<HirRelationExpr> {
    let n = input.arity();

    let (idx_w, window) = scalars.iter().enumerate().find_map(|(idx, s)| match s {
        HirScalarExpr::Windowing(w, _name) => Some((idx, w)),
        _ => None,
    })?;
    let WindowExpr {
        func: WindowExprType::Value(value_expr),
        partition_by,
        order_by,
        bucket_key_range,
    } = window
    else {
        return None;
    };
    let ValueWindowExpr {
        func,
        args,
        order_by: inner_order_by,
        window_frame,
        ignore_nulls,
    } = value_expr;

    // The rewrite reads the ORDER BY key and the arguments out of the `Map`'s
    // input, so the call must not reach into the `Map`'s own scalars.
    if scalars[idx_w].support().iter().any(|c| *c >= n) {
        return None;
    }
    // `lag`/`lead` ignore the frame, but a non-default one would be a sign that
    // something else is going on.
    if *window_frame != WindowFrame::default() {
        return None;
    }

    // Typing the ORDER BY key and the call arguments below is only sound against
    // an empty outer context, so both the call and the relation it reads have to
    // stay inside themselves. A window function in a correlated subquery whose
    // ORDER BY reaches the outer relation lands here.
    //
    // These two walk whole subtrees, so they come after the syntactic checks
    // above rather than before: a `Map` with no window call in it should cost
    // nothing.
    if scalar_references_outer_columns(&scalars[idx_w]) || references_outer_columns(input) {
        return None;
    }

    // Contiguity is decided by the primary sort key, which is whichever ORDER BY
    // expression the `ColumnOrder` list puts first rather than the first one
    // written. The planner happens to emit these in order, but reading the
    // order through `inner_order_by` is what the rewrite actually needs.
    let order_key = order_by.get(inner_order_by.first()?.column)?;
    let input_typ = input.typ(&[], &NO_PARAMS);
    let order_key_type = order_key.typ(&[], &input_typ, &NO_PARAMS).scalar_type;
    // Bucketing on the primary key alone is enough however many ORDER BY
    // expressions there are: the sort is lexicographic, so a coarsening of the
    // first key still cuts the partition into contiguous runs. Its direction
    // does not matter, since a monotone coarsening keeps runs contiguous
    // whether the sort ascends or descends.
    let bucket = bucket_expr(order_key, &order_key_type, *bucket_key_range)?;

    // Treat a single call as a fused group of one, so the two are handled
    // uniformly below.
    let fused = matches!(func, ValueWindowFunc::Fused(_));
    let (funcs, arg_exprs): (Vec<_>, Vec<_>) = match func {
        ValueWindowFunc::Fused(funcs) => {
            let HirScalarExpr::CallVariadic {
                func: VariadicFunc::RecordCreate(_),
                exprs,
                ..
            } = &**args
            else {
                return None;
            };
            if exprs.len() != funcs.len() {
                return None;
            }
            (funcs.clone(), exprs.clone())
        }
        single => (vec![single.clone()], vec![(**args).clone()]),
    };
    let calls = funcs
        .iter()
        .zip_eq(arg_exprs.iter())
        .map(|(f, a)| bucketable_call(f, a))
        .collect::<Option<Vec<_>>>()?;
    let m = calls.len();

    // Every constituent has to look the same way along the order. The split
    // relies on a bucket's unresolved rows forming a prefix and the rows it owes
    // its neighbours forming a suffix, which is only true of one direction at a
    // time. Mixing the two breaks it: a row can be a target because a `lag`
    // constituent ran off the front of the bucket, and the boundary level then
    // has to produce that row's `lead` as well, whose context is the rows
    // immediately after it. Those are neither targets nor summaries, so the
    // boundary level would not have them and would read past them to the next
    // bucket.
    if calls.iter().any(|c| c.func != calls[0].func) {
        return None;
    }

    // Level 0 computes every original constituent plus, per constituent, the
    // markers that say whether its lookback stayed inside the bucket and
    // whether it is one of the rows a neighbouring bucket will need.
    let mut l0_funcs: Vec<ValueWindowFunc> = calls.iter().map(|c| c.func.clone()).collect();
    let mut l0_args: Vec<HirScalarExpr> = calls.iter().map(|c| c.args.clone()).collect();
    let mut resolved_pos = Vec::with_capacity(m);
    let mut summary_pos = Vec::with_capacity(m);
    for (i, call) in calls.iter().enumerate() {
        // Under IGNORE NULLS a marker has to watch the value's nullness, so it
        // looks at the value itself. Under RESPECT NULLS resolution is purely
        // positional, so a non-null constant suffices.
        let (marker_value, marker_type) = if *ignore_nulls {
            let value_type = call.value.typ(&[], &input_typ, &NO_PARAMS).scalar_type;
            (call.value.clone(), value_type)
        } else {
            (
                HirScalarExpr::literal(mz_repr::Datum::Int32(1), SqlScalarType::Int32),
                SqlScalarType::Int32,
            )
        };
        if *ignore_nulls && call.default_is_null {
            // The call's own result is already an unambiguous marker.
            resolved_pos.push(i);
        } else {
            l0_funcs.push(call.same_direction());
            l0_args.push(marker_args(
                marker_value.clone(),
                call.offset.clone(),
                marker_type.clone(),
            ));
            resolved_pos.push(l0_funcs.len() - 1);
        }
        l0_funcs.push(call.opposite_direction());
        l0_args.push(marker_args(marker_value, call.offset.clone(), marker_type));
        summary_pos.push(l0_funcs.len() - 1);
    }

    let record_fields = |len: usize| RecordCreate {
        field_names: iter::repeat(ColumnName::from("")).take(len).collect(),
    };
    let l0_call = HirScalarExpr::windowing(WindowExpr {
        func: WindowExprType::Value(ValueWindowExpr {
            func: ValueWindowFunc::Fused(l0_funcs),
            args: Box::new(HirScalarExpr::call_variadic(
                record_fields(l0_args.len()),
                l0_args,
            )),
            order_by: inner_order_by.clone(),
            window_frame: window_frame.clone(),
            ignore_nulls: *ignore_nulls,
        }),
        partition_by: partition_by
            .iter()
            .cloned()
            .chain(iter::once(bucket))
            .collect(),
        order_by: order_by.clone(),
        bucket_key_range: *bucket_key_range,
    });

    // NOTE: The level 0 subtree is built twice, once per branch. `RelationCSE`
    // factors the duplicate out later; without that the bucketed reduce would
    // be maintained twice over.
    let l0 = input.clone().map(vec![l0_call]);
    let res = |pos: usize| {
        HirScalarExpr::column(n).call_unary(UnaryFunc::RecordGet(mz_expr::func::RecordGet(pos)))
    };

    let all_resolved = resolved_pos
        .iter()
        .map(|p| res(*p).call_is_null().not())
        .reduce(|a, b| a.and(b))
        .expect("a window call has at least one constituent");
    let any_summary = calls
        .iter()
        .zip_eq(summary_pos.iter())
        .map(|(call, p)| {
            let nothing_further = res(*p).call_is_null();
            if *ignore_nulls {
                // Only a bucket's trailing non-nulls are useful as context.
                call.value.clone().call_is_null().not().and(nothing_further)
            } else {
                nothing_further
            }
        })
        .reduce(|a, b| a.or(b))
        .expect("a window call has at least one constituent");

    // A row whose every constituent resolved inside its bucket is finished, and
    // level 0 reassembles the original call's result shape for it.
    let original_result = if fused {
        HirScalarExpr::call_variadic(record_fields(m), (0..m).map(res).collect())
    } else {
        res(0)
    };
    let resolved_branch = l0
        .clone()
        .filter(vec![all_resolved.clone()])
        .map(vec![original_result])
        .project((0..n).chain(iter::once(n + 1)).collect());

    // Everything else goes to level 0's boundary rows, which carry a flag
    // saying whether they are there to be resolved or only as context.
    let level1_input = l0
        .filter(vec![all_resolved.clone().not().or(any_summary)])
        .map(vec![all_resolved.not()])
        .project((0..n).chain(iter::once(n + 1)).collect());
    let target_branch = level1_input
        .map(vec![scalars[idx_w].clone()])
        .filter(vec![HirScalarExpr::column(n)])
        .project((0..n).chain(iter::once(n + 1)).collect());

    // Re-apply the `Map`'s own scalars on top of the union. Every column the
    // `Map` produced shifts by one, because the window result now occupies
    // column `n`, and the call itself becomes a reference to it.
    let union = resolved_branch.union(target_branch);
    let remap: BTreeMap<usize, usize> = (0..n)
        .map(|c| (c, c))
        .chain((0..scalars.len()).map(|j| (n + j, n + 1 + j)))
        .collect();
    let mut new_scalars: Vec<_> = scalars.iter().map(|s| s.clone().remap(&remap)).collect();
    new_scalars[idx_w] = HirScalarExpr::column(n);
    Some(
        union
            .map(new_scalars)
            .project((0..n).chain(n + 1..n + 1 + scalars.len()).collect()),
    )
}
