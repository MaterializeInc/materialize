// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Re-orders Map expressions based on complexity, and performs common
//! sub-expression elimination.

use crate::TransformArgs;
use expr::{RelationExpr, ScalarExpr};

/// Re-orders Map expressions based on complexity, and performs common
/// sub-expression elimination.
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
    /// Re-orders Map expressions based on complexity, and performs common
    /// sub-expression elimination.
    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Map { input, scalars } = relation {
            let input_arity = input.arity();
            let (scalars, projection) = flatten_and_cse(scalars, input_arity);
            *relation = input.take_dangerous().map(scalars).project(projection);
        }
    }
}

fn flatten_and_cse(exprs: &mut [ScalarExpr], arity: usize) -> (Vec<ScalarExpr>, Vec<usize>) {
    let mut projection = (0..arity).collect::<Vec<_>>();
    let mut scalars = Vec::new();
    for expr in exprs.iter_mut() {
        expr.visit_mut(&mut |node| {
            match node {
                ScalarExpr::Column(index) => {
                    // Column references need to be rewritten, but no not
                    // need to be memoized.
                    *index = projection[*index];
                }
                ScalarExpr::Literal(_, _) => {
                    // Literals do not need to be memoized.
                }
                _ => {
                    if let Some(position) = scalars.iter().position(|e| e == node) {
                        // Any complex expression that already exists as a prior column can
                        // be replaced by a reference to that column.
                        *node = ScalarExpr::Column(arity + position);
                    } else {
                        // A complex expression that does not exist should be memoized, and
                        // replaced by a reference to the column.
                        scalars.push(std::mem::replace(
                            node,
                            ScalarExpr::Column(arity + scalars.len()),
                        ));
                    }
                }
            }
        });

        // At this point `expr` should be a column reference or a literal. It may not have
        // been added to `scalars` and we should do so if it is a literal to make sure we
        // have a column reference we can record (for the ultimate projection).
        let position = match expr {
            ScalarExpr::Column(index) => *index,
            ScalarExpr::Literal(_, _) => {
                scalars.push(expr.clone());
                arity + scalars.len() - 1
            }
            e => {
                arity
                    + scalars
                        .iter()
                        .position(|r| r == e)
                        .expect("Failed to find memoized complex expression")
            }
        };

        projection.push(position);
    }

    inline_single_use(&mut scalars, &mut projection[..], arity);
    (scalars, projection)
}

fn inline_single_use(exprs: &mut Vec<ScalarExpr>, projection: &mut [usize], arity: usize) {
    // Determine which columns are referred to by which others.
    let mut referenced = vec![0; arity + exprs.len()];
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
                if *i >= arity && referenced[*i] == 1 {
                    referenced[*i] -= 1;
                    *e = prior[*i - arity].clone();
                }
            }
        });
    }

    // Remove unreferenced columns, update column references.
    let mut results = Vec::new();
    for (index, mut expr) in exprs.drain(..).enumerate() {
        if referenced[arity + index] > 0 {
            // keep the expression, but update column references.
            expr.visit_mut(&mut |e| {
                if let ScalarExpr::Column(i) = e {
                    // Subtract from i the number of unreferenced columns before it
                    if *i >= arity {
                        *i -= referenced[arity..*i].iter().filter(|x| x == &&0).count();
                    }
                }
            });
            results.push(expr);
        }
    }
    *exprs = results;
    for column in projection.iter_mut() {
        if *column >= arity {
            *column -= referenced[arity..*column]
                .iter()
                .filter(|x| x == &&0)
                .count()
        }
    }
}
