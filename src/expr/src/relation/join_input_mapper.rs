// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Range;

use itertools::Itertools;

use crate::RelationExpr;
use crate::ScalarExpr;

/// Any column in a join expression exists in two contexts:
/// 1) It has a position relative to the result of the join (global)
/// 2) It has a position relative to the specific input it came from (local)
/// This utility focuses on taking expressions that are in terms of
/// the local input and re-expressing them in global terms and vice versa.
#[derive(Debug)]
pub struct JoinInputMapper {
    /// The number of columns per input. All other fields in this struct are
    /// derived using the information in this field.
    arities: Vec<usize>,
    /// Looks up which input each column belongs to. Derived from `arities`.
    /// Stored as a field to avoid recomputation.
    input_relation: Vec<usize>,
    /// The sum of the arities of the previous inputs in the join. Derived from
    /// `arities`. Stored as a field to avoid recomputation.
    prior_arities: Vec<usize>,
}

impl JoinInputMapper {
    /// Creates a new `JoinInputMapper` and calculates the mapping of global context
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

        JoinInputMapper {
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

    /// Takes an expression from the global context and creates a new version
    /// where column references have been remapped to the local context.
    /// Assumes that all columns in `expr` are from the same input.
    pub fn map_expr_to_local(&self, mut expr: ScalarExpr) -> ScalarExpr {
        expr.visit_mut(&mut |e| {
            if let ScalarExpr::Column(c) = e {
                *c -= self.prior_arities[self.input_relation[*c]];
            }
        });
        expr
    }

    /// Takes an expression from the local context of the `index`th input and
    /// creates a new version where column references have been remapped to the
    /// global context.
    pub fn map_expr_to_global(&self, mut expr: ScalarExpr, index: usize) -> ScalarExpr {
        expr.visit_mut(&mut |e| {
            if let ScalarExpr::Column(c) = e {
                *c += self.prior_arities[index];
            }
        });
        expr
    }

    /// Remap column numbers from the global to the local context.
    /// Assumes column numbers are from the same input.
    pub fn map_columns_to_local(&self, columns: &[usize]) -> Vec<usize> {
        columns
            .iter()
            .map(move |c| *c - self.prior_arities[self.input_relation[*c]])
            .collect()
    }

    /// Find the sorted, dedupped set of inputs an expression references
    pub fn lookup_inputs(&self, expr: &ScalarExpr) -> impl Iterator<Item = usize> {
        expr.support()
            .iter()
            .map(|c| self.input_relation[*c])
            .sorted()
            .dedup()
    }

    /// Takes an expression in the global context and looks in `equivalences`
    /// for an equivalent expression (also expressed in the global context) that
    /// belongs to one or more of the inputs in `bound_inputs`
    ///
    /// # Examples
    ///
    /// ```
    /// use repr::{Datum, ColumnType, RelationType, ScalarType};
    /// use expr::{JoinInputMapper, RelationExpr, ScalarExpr};
    ///
    /// // A two-column schema common to each of the three inputs
    /// let schema = RelationType::new(vec![
    ///   ScalarType::Int32.nullable(false),
    ///   ScalarType::Int32.nullable(false),
    /// ]);
    ///
    /// // the specific data are not important here.
    /// let data = vec![Datum::Int32(0), Datum::Int32(1)];
    /// let input0 = RelationExpr::constant(vec![data.clone()], schema.clone());
    /// let input1 = RelationExpr::constant(vec![data.clone()], schema.clone());
    /// let input2 = RelationExpr::constant(vec![data.clone()], schema.clone());
    ///
    /// // [input0(#0) = input2(#1)], [input0(#1) = input1(#0) = input2(#0)]
    /// let equivalences = vec![
    ///   vec![ScalarExpr::Column(0), ScalarExpr::Column(5)],
    ///   vec![ScalarExpr::Column(1), ScalarExpr::Column(2), ScalarExpr::Column(4)],
    /// ];
    ///
    /// let input_mapper = JoinInputMapper::new(&[input0, input1, input2]);
    /// assert_eq!(
    ///   Some(ScalarExpr::Column(4)),
    ///   input_mapper.find_bound_expr(&ScalarExpr::Column(2), &[2], &equivalences)
    /// );
    /// assert_eq!(
    ///   None,
    ///   input_mapper.find_bound_expr(&ScalarExpr::Column(0), &[1], &equivalences)
    /// );
    /// ```
    pub fn find_bound_expr(
        &self,
        expr: &ScalarExpr,
        bound_inputs: &[usize],
        equivalences: &[Vec<ScalarExpr>],
    ) -> Option<ScalarExpr> {
        if let Some(equivalence) = equivalences.iter().find(|equivs| equivs.contains(expr)) {
            if let Some(bound_expr) = equivalence
                .iter()
                .find(|expr| self.lookup_inputs(expr).all(|i| bound_inputs.contains(&i)))
            {
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
    fn try_map_to_input_with_bound_expr_sub(
        &self,
        expr: &mut ScalarExpr,
        index: usize,
        equivalences: &[Vec<ScalarExpr>],
    ) {
        {
            let mut inputs = self.lookup_inputs(expr);
            if let Some(first_input) = inputs.next() {
                if inputs.next().is_none() && first_input == index {
                    // there is only one input, and it is equal to index, so we're
                    // good. do not continue the recursion
                    return;
                }
            }
        }
        if let Some(bound_expr) = self.find_bound_expr(expr, &[index], equivalences) {
            // replace the subexpression with the equivalent one from input `index`
            *expr = bound_expr;
        } else {
            // recurse to see if we can replace subexpressions further down
            expr.visit1_mut(|e| self.try_map_to_input_with_bound_expr_sub(e, index, equivalences))
        }
    }

    /// Try to rewrite an expression from the global context so that all the
    /// columns point to the `index` input by replacing subexpressions with their
    /// bound equivalents in the `index`th input if necessary.
    /// The return value, if not None, is in the context of the `index`th input
    pub fn try_map_to_input_with_bound_expr(
        &self,
        mut expr: ScalarExpr,
        index: usize,
        equivalences: &[Vec<ScalarExpr>],
    ) -> Option<ScalarExpr> {
        // call the recursive submethod
        self.try_map_to_input_with_bound_expr_sub(&mut expr, index, equivalences);
        // if the localization attempt is successful, all columns in `expr`
        // should only come from input `index`
        let mut inputs_after_localization = self.lookup_inputs(&expr);
        if let Some(first_input) = inputs_after_localization.next() {
            if inputs_after_localization.next().is_none() && first_input == index {
                return Some(self.map_expr_to_local(expr));
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BinaryFunc, ScalarExpr, UnaryFunc};
    use repr::{Datum, ScalarType};

    #[test]
    fn try_map_to_input_with_bound_expr_test() {
        let input_mapper = JoinInputMapper {
            arities: vec![2, 3, 3],
            input_relation: vec![0, 0, 1, 1, 1, 2, 2, 2],
            prior_arities: vec![0, 2, 5],
        };

        // keys are numbered by (equivalence class #, input #)
        let key10 = ScalarExpr::Column(0);
        let key12 = ScalarExpr::Column(6);
        let localized_key12 = ScalarExpr::Column(1);

        let mut equivalences = vec![vec![key10.clone(), key12.clone()]];

        // when the column is already part of the target input, all that happens
        // is that it gets localized
        assert_eq!(
            Some(ScalarExpr::Column(1)),
            input_mapper.try_map_to_input_with_bound_expr(key12.clone(), 2, &equivalences)
        );

        // basic tests that we can find a column's corresponding column in a
        // different input
        assert_eq!(
            Some(key10.clone()),
            input_mapper.try_map_to_input_with_bound_expr(key12.clone(), 0, &equivalences)
        );
        assert_eq!(
            None,
            input_mapper.try_map_to_input_with_bound_expr(key12.clone(), 1, &equivalences)
        );

        let key20 = ScalarExpr::CallUnary {
            func: UnaryFunc::NegInt32,
            expr: Box::new(ScalarExpr::Column(1)),
        };
        let key21 = ScalarExpr::CallBinary {
            func: BinaryFunc::AddInt32,
            expr1: Box::new(ScalarExpr::Column(2)),
            expr2: Box::new(ScalarExpr::literal(
                Ok(Datum::Int32(4)),
                ScalarType::Int32.nullable(false),
            )),
        };
        let key22 = ScalarExpr::Column(5);
        let localized_key22 = ScalarExpr::Column(0);
        equivalences.push(vec![key22.clone(), key20.clone(), key21.clone()]);

        // basic tests that we can find an expression's corresponding expression in a
        // different input
        assert_eq!(
            Some(key20.clone()),
            input_mapper.try_map_to_input_with_bound_expr(key21.clone(), 0, &equivalences)
        );
        assert_eq!(
            Some(localized_key22.clone()),
            input_mapper.try_map_to_input_with_bound_expr(key21.clone(), 2, &equivalences)
        );

        // test that `try_map_to_input_with_bound_expr` will map multiple
        // subexpressions to the corresponding expressions bound to a different input
        let key_comp = ScalarExpr::CallBinary {
            func: BinaryFunc::MulInt32,
            expr1: Box::new(key12.clone()),
            expr2: Box::new(key22),
        };
        assert_eq!(
            Some(ScalarExpr::CallBinary {
                func: BinaryFunc::MulInt32,
                expr1: Box::new(key10.clone()),
                expr2: Box::new(key20.clone()),
            }),
            input_mapper.try_map_to_input_with_bound_expr(key_comp.clone(), 0, &equivalences)
        );

        // test that the function returns None when part
        // of the expression can be mapped to an input but the rest can't
        assert_eq!(
            None,
            input_mapper.try_map_to_input_with_bound_expr(key_comp.clone(), 1, &equivalences)
        );

        let key_comp_plus_non_key = ScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1: Box::new(key_comp),
            expr2: Box::new(ScalarExpr::Column(7)),
        };
        assert_eq!(
            None,
            input_mapper.try_map_to_input_with_bound_expr(key_comp_plus_non_key, 0, &equivalences)
        );

        let key_comp_multi_input = ScalarExpr::CallBinary {
            func: BinaryFunc::Eq,
            expr1: Box::new(key12),
            expr2: Box::new(key21),
        };
        // test that the function works when part of the expression is already
        // part of the target input
        assert_eq!(
            Some(ScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1: Box::new(localized_key12),
                expr2: Box::new(localized_key22),
            }),
            input_mapper.try_map_to_input_with_bound_expr(
                key_comp_multi_input.clone(),
                2,
                &equivalences
            )
        );
        // test that the function works when parts of the expression come from
        // multiple inputs
        assert_eq!(
            Some(ScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1: Box::new(key10),
                expr2: Box::new(key20),
            }),
            input_mapper.try_map_to_input_with_bound_expr(
                key_comp_multi_input.clone(),
                0,
                &equivalences
            )
        );
        assert_eq!(
            None,
            input_mapper.try_map_to_input_with_bound_expr(key_comp_multi_input, 1, &equivalences)
        )
    }
}
