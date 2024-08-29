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
use mz_expr::visit::Visit;
use mz_expr::WindowFrame;
use mz_expr::{ColumnOrder, UnaryFunc, VariadicFunc};
use mz_ore::stack::RecursionLimitError;
use mz_repr::{ColumnName, ColumnType, RelationType, ScalarType};

use crate::plan::expr::{
    AbstractExpr, AggregateFunc, ColumnRef, HirRelationExpr, HirScalarExpr, ValueWindowExpr,
    ValueWindowFunc, WindowExpr,
};
use crate::plan::WindowExprType;

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
pub fn split_subquery_predicates(expr: &mut HirRelationExpr) {
    fn walk_relation(expr: &mut HirRelationExpr) {
        #[allow(deprecated)]
        expr.visit_mut(0, &mut |expr, _| match expr {
            HirRelationExpr::Map { scalars, .. } => {
                for scalar in scalars {
                    walk_scalar(scalar);
                }
            }
            HirRelationExpr::CallTable { exprs, .. } => {
                for expr in exprs {
                    walk_scalar(expr);
                }
            }
            HirRelationExpr::Filter { predicates, .. } => {
                let mut subqueries = vec![];
                for predicate in &mut *predicates {
                    walk_scalar(predicate);
                    extract_conjuncted_subqueries(predicate, &mut subqueries);
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
        });
    }

    fn walk_scalar(expr: &mut HirScalarExpr) {
        #[allow(deprecated)]
        expr.visit_mut(&mut |expr| match expr {
            HirScalarExpr::Exists(input) | HirScalarExpr::Select(input) => walk_relation(input),
            _ => (),
        })
    }

    fn contains_subquery(expr: &HirScalarExpr) -> bool {
        let mut found = false;
        expr.visit(&mut |expr| match expr {
            HirScalarExpr::Exists(_) | HirScalarExpr::Select(_) => found = true,
            _ => (),
        });
        found
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
    fn extract_conjuncted_subqueries(expr: &mut HirScalarExpr, out: &mut Vec<HirScalarExpr>) {
        match expr {
            HirScalarExpr::CallVariadic {
                func: VariadicFunc::And,
                exprs,
            } => {
                exprs
                    .into_iter()
                    .for_each(|e| extract_conjuncted_subqueries(e, out));
            }
            expr if contains_subquery(expr) => {
                out.push(mem::replace(expr, HirScalarExpr::literal_true()))
            }
            _ => (),
        }
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
pub fn try_simplify_quantified_comparisons(expr: &mut HirRelationExpr) {
    fn walk_relation(expr: &mut HirRelationExpr, outers: &[RelationType]) {
        match expr {
            HirRelationExpr::Map { scalars, input } => {
                walk_relation(input, outers);
                let mut outers = outers.to_vec();
                outers.insert(0, input.typ(&outers, &NO_PARAMS));
                for scalar in scalars {
                    walk_scalar(scalar, &outers, false);
                    let (inner, outers) = outers
                        .split_first_mut()
                        .expect("outers known to have at least one element");
                    let scalar_type = scalar.typ(outers, inner, &NO_PARAMS);
                    inner.column_types.push(scalar_type);
                }
            }
            HirRelationExpr::Filter { predicates, input } => {
                walk_relation(input, outers);
                let mut outers = outers.to_vec();
                outers.insert(0, input.typ(&outers, &NO_PARAMS));
                for pred in predicates {
                    walk_scalar(pred, &outers, true);
                }
            }
            HirRelationExpr::CallTable { exprs, .. } => {
                let mut outers = outers.to_vec();
                outers.insert(0, RelationType::empty());
                for scalar in exprs {
                    walk_scalar(scalar, &outers, false);
                }
            }
            HirRelationExpr::Join { left, right, .. } => {
                walk_relation(left, outers);
                let mut outers = outers.to_vec();
                outers.insert(0, left.typ(&outers, &NO_PARAMS));
                walk_relation(right, &outers);
            }
            expr => {
                #[allow(deprecated)]
                let _ = expr.visit1_mut(0, &mut |expr, _| -> Result<(), ()> {
                    walk_relation(expr, outers);
                    Ok(())
                });
            }
        }
    }

    fn walk_scalar(expr: &mut HirScalarExpr, outers: &[RelationType], mut in_filter: bool) {
        #[allow(deprecated)]
        expr.visit_mut_pre(&mut |e| match e {
            HirScalarExpr::Exists(input) => walk_relation(input, outers),
            HirScalarExpr::Select(input) => {
                walk_relation(input, outers);

                // We're inside of a `(SELECT ...)` subquery. Now let's see if
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
                    _ => return,
                };

                if !in_filter && column_type(outers, input, expr).nullable {
                    // Unless we're directly inside of a WHERE, this
                    // transformation is only valid if the expression involved
                    // is non-nullable.
                    return;
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
                // directly inside of a filter.
                in_filter = false;
            }
        })
    }

    walk_relation(expr, &[])
}

/// An empty parameter type map.
///
/// These transformations are expected to run after parameters are bound, so
/// there is no need to provide any parameter type information.
static NO_PARAMS: LazyLock<BTreeMap<usize, ScalarType>> = LazyLock::new(BTreeMap::new);

fn column_type(
    outers: &[RelationType],
    inner: &HirRelationExpr,
    expr: &HirScalarExpr,
) -> ColumnType {
    let inner_type = inner.typ(outers, &NO_PARAMS);
    expr.typ(outers, &inner_type, &NO_PARAMS)
}

/// # Aims and scope
///
/// The aim here is to amortize the overhead of the MIR window function pattern
/// (see `window_func_applied_to`) by fusing groups of window function calls such
/// that each group can be performed by one instance of the window function MIR
/// pattern.
///
/// For now, we fuse only value window function calls (`WindowExprType::Value`).
/// TODO: We should consider fusing the other types later:
/// - `WindowExprType::Aggregate`: Would be great to also fuse these soon, see e.g.
///   <https://github.com/MaterializeInc/materialize/issues/20741#issuecomment-2231723718>
/// - `WindowExprType::Scalar`: probably won't need to fuse these for a long time.)
///
/// For now, we can fuse value window function calls where the
/// A. partition by
/// B. order by
/// C. window frame
/// D. ignore nulls
/// are all the same. (See `extract_options`.)
/// (Later, we could improve this to only need A. to be the same. This would require
/// much more code changes, because then we'd have to blow up `ValueWindowExpr`.
/// TODO: As a much simpler intermediate step, at least we should ignore options that
/// don't matter. For example, we should be able to fuse a `lag` that has a default
/// frame with a `first_value` that has some custom frame, because `lag` is not
/// affected by the frame.)
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
/// It would be tempting to find groups simply by taking a list of all value window function calls
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
    context: &crate::plan::lowering::Context,
) -> Result<(), RecursionLimitError> {
    if !context.config.enable_value_window_function_fusion {
        return Ok(());
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

    /// Those options of a window function call that are relevant for fusion.
    #[derive(PartialEq, Eq)]
    struct ValueWindowFuncCallOptions {
        partition_by: Vec<HirScalarExpr>,
        outer_order_by: Vec<HirScalarExpr>,
        inner_order_by: Vec<ColumnOrder>,
        window_frame: WindowFrame,
        ignore_nulls: bool,
    }

    /// Helper function to extract the above options.
    fn extract_options(call: &HirScalarExpr) -> ValueWindowFuncCallOptions {
        match call {
            HirScalarExpr::Windowing(WindowExpr {
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
            }) => ValueWindowFuncCallOptions {
                partition_by: partition_by.clone(),
                outer_order_by: outer_order_by.clone(),
                inner_order_by: inner_order_by.clone(),
                window_frame: window_frame.clone(),
                ignore_nulls: ignore_nulls.clone(),
            },
            _ => panic!("extract_options should only be called on value window functions"),
        }
    }

    struct FusionGroup {
        /// The original column index of the first element of the group. (This is an index into the
        /// Map's `scalars` plus the arity of the Map's input.)
        first_col: usize,
        /// The options of all the window function calls in the group. (Must be the same for all the
        /// calls.)
        options: ValueWindowFuncCallOptions,
        /// The calls in the group, with their original column indexes.
        calls: Vec<(usize, HirScalarExpr)>,
    }

    impl FusionGroup {
        /// Creates a window function call that is a fused version of all the calls in the group.
        /// `new_col` is the column index where the fused call will be inserted at.
        fn fuse(self, new_col: usize) -> (HirScalarExpr, Vec<HirScalarExpr>) {
            let (fused_funcs, fused_args): (Vec<_>, Vec<_>) = self
                .calls
                .iter()
                .map(|(_idx, call)| {
                    if let HirScalarExpr::Windowing(WindowExpr {
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
                    }) = call
                    {
                        (func.clone(), (**args).clone())
                    } else {
                        panic!("unknown window function in FusionGroup")
                    }
                })
                .unzip();
            let fused_args = HirScalarExpr::CallVariadic {
                func: VariadicFunc::RecordCreate {
                    // These field names are not important, because this record will only be an
                    // intermediate expression, which we'll manipulate further before it ends up
                    // anywhere where a column name would be visible.
                    field_names: iter::repeat(ColumnName::from(""))
                        .take(fused_args.len())
                        .collect(),
                },
                exprs: fused_args,
            };
            let fused = HirScalarExpr::Windowing(WindowExpr {
                func: WindowExprType::Value(ValueWindowExpr {
                    func: ValueWindowFunc::Fused(fused_funcs),
                    args: Box::new(fused_args),
                    order_by: self.options.inner_order_by,
                    window_frame: self.options.window_frame,
                    ignore_nulls: self.options.ignore_nulls,
                }),
                partition_by: self.options.partition_by,
                order_by: self.options.outer_order_by,
            });

            let decompositions = (0..self.calls.len())
                .map(|field| HirScalarExpr::CallUnary {
                    func: UnaryFunc::RecordGet(mz_expr::func::RecordGet(field)),
                    expr: Box::new(HirScalarExpr::Column(ColumnRef {
                        level: 0,
                        column: new_col,
                    })),
                })
                .collect();

            (fused, decompositions)
        }
    }

    root.try_visit_mut_post(&mut |rel_expr| {
        match rel_expr {
            HirRelationExpr::Map { input, scalars } => {
                // There will be various variable names involving `idx` or `col`:
                // - `idx` will always be an index into `scalars` or something similar,
                // - `col` will always be a column index,
                //   which is often `arity_before_map` + an index into `scalars`.
                let arity_before_map = input.arity();
                let orig_num_scalars = scalars.len();

                // Collect all value window function calls with their column indexes.
                let value_window_function_calls = scalars
                    .iter()
                    .enumerate()
                    .filter(|(_idx, scalar_expr)| {
                        // Look for calls only at the root of scalar expressions. This is enough
                        // because they are always there, see 72e84bb78.
                        if let HirScalarExpr::Windowing(WindowExpr {
                            func: WindowExprType::Value(ValueWindowExpr { func, .. }),
                            ..
                        }) = scalar_expr
                        {
                            // Exclude those calls that are already fused. (We shouldn't currently
                            // encounter these, because we just do one pass, but it's better to be
                            // robust against future code changes.)
                            !matches!(func, ValueWindowFunc::Fused(..))
                        } else {
                            false
                        }
                    })
                    .map(|(idx, call)| (idx + arity_before_map, call.clone()))
                    .collect_vec();
                // Exit early if obviously no chance for fusion.
                if value_window_function_calls.len() <= 1 {
                    // Note that we are doing this only for performance. All plans should be exactly
                    // the same even if we comment out the following line.
                    return Ok(());
                }

                // Determine the fusion groups. (Each group will later be fused into one window
                // function call.)
                // Note that this has a quadratic run time in the number of value window function
                // calls in the worst case. However, this is fine even with 1000 window function
                // calls.
                let mut groups: Vec<FusionGroup> = Vec::new();
                for (col, call) in value_window_function_calls {
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
                // `groups` is already sorted by `first_col` due to way it was constructed.)
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
