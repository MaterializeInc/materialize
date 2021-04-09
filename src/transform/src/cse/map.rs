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
use expr::{MirRelationExpr, MirScalarExpr};

/// Performs common sub-expression elimination.
#[derive(Debug)]
pub struct Map;

impl crate::Transform for Map {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
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
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Map { input, scalars } = relation {
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
    exprs: &mut [MirScalarExpr],
    input_arity: usize,
) -> (Vec<MirScalarExpr>, Vec<usize>) {
    let mut projection = (0..input_arity).collect::<Vec<_>>();
    let mut scalars = Vec::new();
    for expr in exprs.iter_mut() {
        expr.permute(&projection);
        // Carefully memoize expressions that will certainly be evaluated.
        expr::memoize_expr(expr, &mut scalars, input_arity);

        // At this point `expr` should be a column reference or a literal. It may not have
        // been added to `scalars` and we should do so if it is a literal to make sure we
        // have a column reference we can record (for the ultimate projection).
        let position = match expr {
            MirScalarExpr::Column(index) => *index,
            MirScalarExpr::Literal(_, _) => {
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

/// Replaces column references that occur once with the referenced expression.
///
/// This method in-lines expressions that are only referenced once, and then
/// collects expressions that are no longer referenced, updating column indexes.
fn inline_single_use(exprs: &mut Vec<MirScalarExpr>, projection: &mut [usize], input_arity: usize) {
    // Determine which columns are referred to by which others.
    let mut referenced = vec![0; input_arity + exprs.len()];
    for column in projection.iter() {
        referenced[*column] += 1;
    }
    for expr in exprs.iter() {
        expr.visit(&mut |e| {
            if let MirScalarExpr::Column(i) = e {
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
            if let MirScalarExpr::Column(i) = e {
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
            expr.permute_map(&column_rename);
            results.push(expr);
            column_rename.insert(input_arity + index, input_arity + results.len() - 1);
        }
    }
    *exprs = results;
    for column in projection.iter_mut() {
        *column = column_rename[column];
    }
}
