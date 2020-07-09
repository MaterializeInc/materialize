// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Performs common sub-expression elimination.
//!
//! This transform identifies common `ScalarExpr` expressions across
//! and within expressions that are arguments to `Map` operators, and
//! reforms the operator to build each distinct expression at most once
//! and to re-use expressions instead of re-evaluating them.
//!
//! The re-use policy at the moment is severe and re-uses everything.
//! It may be worth considering relations of this if it results in more
//! busywork and less efficiency, but the wins can be substantial when
//! expressions re-use complex subexpressions.

use crate::TransformArgs;
use expr::{RelationExpr, ScalarExpr};

/// Performs common sub-expression elimination.
#[derive(Debug)]
pub struct Map;

impl crate::Transform for Map {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.visit_mut(&mut |e| {
            self.action(e);
        });
        Ok(())
    }
}

impl Map {
    /// Performs common sub-expression elimination.
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Map { input, scalars } = relation {
            let input_arity = input.arity();
            let scalars_len = scalars.len();
            let (scalars, projection) = memoize_and_reuse(scalars, input_arity);
            let mut expression = input.take_dangerous().map(scalars);
            if projection.len() != (input_arity + scalars_len)
                || projection.iter().enumerate().any(|(a, b)| a != *b)
            {
                expression = expression.project(projection);
            }
            *relation = expression;
        }
    }
}

/// Memoize expressions in `exprs` and re-use where possible.
///
/// This method extracts all expression AST nodes as individual expressions,
/// and builds others out of column references to them, avoiding any duplication.
/// Once complete, expressions which are referred to only once are re-inlined.
///
/// Some care is taken to ensure that `if` expressions only memoize expresions
/// they are certain to evaluate.
fn memoize_and_reuse(
    exprs: &mut [ScalarExpr],
    input_arity: usize,
) -> (Vec<ScalarExpr>, Vec<usize>) {
    let mut projection = (0..input_arity).collect::<Vec<_>>();
    let mut scalars = Vec::new();
    for expr in exprs.iter_mut() {
        // Carefully memoize expressions that will certainly be evaluated.
        memoize(expr, &mut scalars, &mut projection, input_arity);

        // At this point `expr` should be a column reference or a literal. It may not have
        // been added to `scalars` and we should do so if it is a literal to make sure we
        // have a column reference we can record (for the ultimate projection).
        let position = match expr {
            ScalarExpr::Column(index) => *index,
            ScalarExpr::Literal(_, _) => {
                scalars.push(expr.clone());
                input_arity + scalars.len() - 1
            }
            _ => unreachable!("Non column/literal expression found"),
        };

        projection.push(position);
    }

    inline_single_use(&mut scalars, &mut projection[..], input_arity);
    (scalars, projection)
}

/// Visit and memoize expression nodes.
///
/// Importantly, we should not memoize expressions that may not be excluded by virtue of
/// being guarded by `if` expressions.
fn memoize(
    expr: &mut ScalarExpr,
    scalars: &mut Vec<ScalarExpr>,
    projection: &[usize],
    input_arity: usize,
) {
    match expr {
        ScalarExpr::Column(index) => {
            // Column references need to be rewritten, but do not need to be memoized.
            *index = projection[*index];
        }
        ScalarExpr::Literal(_, _) => {
            // Literals do not need to be memoized.
        }
        _ => {
            // We should not eagerly memoize `if` branches that might not be taken.
            // TODO: Memoize expressions in the intersection of `then` and `els`.
            if let ScalarExpr::If { cond, then, els } = expr {
                memoize(cond, scalars, projection, input_arity);
                // Conditionally evaluated expressions still need to update their
                // column references.
                then.permute(projection);
                els.permute(projection);
            } else {
                expr.visit1_mut(|e| memoize(e, scalars, projection, input_arity));
            }
            if let Some(position) = scalars.iter().position(|e| e == expr) {
                // Any complex expression that already exists as a prior column can
                // be replaced by a reference to that column.
                *expr = ScalarExpr::Column(input_arity + position);
            } else {
                // A complex expression that does not exist should be memoized, and
                // replaced by a reference to the column.
                scalars.push(std::mem::replace(
                    expr,
                    ScalarExpr::Column(input_arity + scalars.len()),
                ));
            }
        }
    }
}

/// Replaces column references that occur once with the referenced expression.
///
/// This method in-lines expressions that are only referenced once, and then
/// collects expressions that are no longer referenced, updating column indexes.
fn inline_single_use(exprs: &mut Vec<ScalarExpr>, projection: &mut [usize], input_arity: usize) {
    // Determine which columns are referred to by which others.
    let mut referenced = vec![0; input_arity + exprs.len()];
    for column in projection.iter() {
        referenced[*column] += 1;
    }
    for expr in exprs.iter() {
        expr.visit(&mut |e| {
            if let ScalarExpr::Column(i) = e {
                referenced[*i] += 1;
            }
        })
    }

    // Any column with a single reference should be in-lined in
    // to its referrer. This process does not change the number
    // of references to a column, other than from one to zero,
    // and should be tractable with a single linear pass.
    for index in 0..exprs.len() {
        let (prior, expr) = exprs.split_at_mut(index);
        expr[0].visit_mut(&mut |e| {
            if let ScalarExpr::Column(i) = e {
                if *i >= input_arity && referenced[*i] == 1 {
                    referenced[*i] -= 1;
                    *e = prior[*i - input_arity].clone();
                }
            }
        });
    }

    // Remove unreferenced columns, update column references.
    let mut column_rename = std::collections::HashMap::new();
    for column in 0..input_arity {
        column_rename.insert(column, column);
    }
    let mut results = Vec::new();
    for (index, mut expr) in exprs.drain(..).enumerate() {
        if referenced[input_arity + index] > 0 {
            // keep the expression, but update column references.
            expr.visit_mut(&mut |e| {
                if let ScalarExpr::Column(i) = e {
                    *i = column_rename[i];
                }
            });
            results.push(expr);
            column_rename.insert(input_arity + index, input_arity + results.len() - 1);
        }
    }
    *exprs = results;
    for column in projection.iter_mut() {
        *column = column_rename[column];
    }
}
