// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Range;

use crate::RelationExpr;
use crate::ScalarExpr;

/// Any column in a join expression exists in two contexts:
/// 1) It has a position relative to the result of the join (global)
/// 2) It has a position relative to the specific input it came from (local)
/// This utility focuses on taking expressions that are in terms of
/// the local input and re-expressing them in global terms and vice versa.
#[derive(Debug)]
pub struct JoinUtil {
    /// The number of columns per input
    arities: Vec<usize>,
    /// Looks up which input each column belongs to
    input_relation: Vec<usize>,
    /// The sum of the arities of the previous inputs in the join
    prior_arities: Vec<usize>,
}

impl JoinUtil {
    /// Creates a new `JoinUtil` and calculates the mapping of global context
    /// columns to local context columns.
    pub fn new(inputs: &[RelationExpr]) -> Self {
        let types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
        let arities = types
            .iter()
            .map(|t| t.column_types.len())
            .collect::<Vec<_>>();

        let mut offset = 0;
        let mut prior_arities = Vec::new();
        for input in 0..inputs.len() {
            prior_arities.push(offset);
            offset += arities[input];
        }

        let input_relation = arities
            .iter()
            .enumerate()
            .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
            .collect::<Vec<_>>();

        JoinUtil {
            arities,
            input_relation,
            prior_arities,
        }
    }

    /// All column numbers in order for a particular input in the local context
    #[inline]
    pub fn local_columns(&self, index: usize) -> Range<usize> {
        0..self.arities[index]
    }

    /// All column numbers in order for a particular input in the global context
    #[inline]
    pub fn global_columns(&self, index: usize) -> Range<usize> {
        self.prior_arities[index]..(self.prior_arities[index] + self.arities[index])
    }

    /// Shift the column references in an expression from the context of the
    /// larger join to the context of the input that the expression refers to.
    pub fn localize_expression(&self, expr: &mut ScalarExpr) {
        expr.visit_mut(&mut |e| {
            if let ScalarExpr::Column(c) = e {
                *c -= self.prior_arities[self.input_relation[*c]];
            }
        });
    }

    /// Shift the column references in an expression from the context of the
    /// `index`th input to the context of the larger join.
    pub fn globalize_expression(&self, expr: &mut ScalarExpr, index: usize) {
        expr.visit_mut(&mut |e| {
            if let ScalarExpr::Column(c) = e {
                *c += self.prior_arities[index];
            }
        });
    }

    /// Convert column numbers to their corresponding values relative to the
    /// input it comes from
    pub fn localize_columns(&self, columns: &[usize]) -> Vec<usize> {
        columns
            .iter()
            .map(|c| *c - self.prior_arities[self.input_relation[*c]])
            .collect::<Vec<_>>()
    }

    /// Find the sorted, dedupped set of inputs an expression references
    pub fn lookup_inputs(&self, expr: &ScalarExpr) -> Vec<usize> {
        let mut result = expr
            .support()
            .iter()
            .map(|c| self.input_relation[*c])
            .collect::<Vec<_>>();
        result.sort();
        result.dedup();
        result
    }

    /// Takes an expression in the global context and looks in `equivalences`
    /// for an equivalent expression (also expressed in the global context) that
    /// belongs to one or more of the inputs in `bound_inputs`
    pub fn find_bound_expr(
        &self,
        expr: &ScalarExpr,
        bound_inputs: &[usize],
        equivalences: &[Vec<ScalarExpr>],
    ) -> Option<ScalarExpr> {
        if let Some(equivalence) = equivalences.iter().find(|equivs| equivs.contains(expr)) {
            if let Some(bound_expr) = equivalence.iter().find(|expr| {
                self.lookup_inputs(expr)
                    .into_iter()
                    .all(|i| bound_inputs.contains(&i))
            }) {
                return Some(bound_expr.clone());
            }
        }
        None
    }

    /// Try to rewrite an subexpression referencing the larger join so that all the
    /// column references point to the `index` input taking advantage of equivalences
    /// in the join, if necessary.
    /// Takes an expression in the global context and makes rewrites in the
    /// global context so we can identify using `lookup_inputs` whether if an expression
    /// was only partially rewritten.
    fn try_localize_subexpression(
        &self,
        expr: &mut ScalarExpr,
        index: usize,
        equivalences: &[Vec<ScalarExpr>],
    ) {
        let inputs = self.lookup_inputs(expr);
        if inputs.len() == 1 && *inputs.first().unwrap() == index {
            // we're good. do not continue the recursion
        } else if let Some(bound_expr) = self.find_bound_expr(expr, &[index], equivalences) {
            // replace the subexpression with the equivalent one from input `index`
            *expr = bound_expr;
        } else {
            // recurse to see if we can replace subexpressions further down
            expr.visit1_mut(|e| self.try_localize_subexpression(e, index, equivalences))
        }
    }

    /// Try to rewrite an expression referencing the larger join so that all the
    /// column references point to the `index` input taking advantage of equivalences
    /// in the join, if necessary.
    /// The return value, if not None, is in the context local to input `index`
    pub fn try_localize_expression(
        &self,
        expr: &ScalarExpr,
        index: usize,
        equivalences: &[Vec<ScalarExpr>],
    ) -> Option<ScalarExpr> {
        let mut expr = expr.clone();
        expr.visit1_mut(&mut |e| self.try_localize_subexpression(e, index, equivalences));
        // if the localization attempt is successful, all columns in `expr`
        // should only come from input `index`
        let inputs_after_localization = self.lookup_inputs(&expr);
        if inputs_after_localization.len() == 1 {
            if *inputs_after_localization.first().unwrap() == index {
                self.localize_expression(&mut expr);
                return Some(expr);
            }
        }
        None
    }
}
