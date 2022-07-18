// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses multiple `Join` operators into one `Join` operator.
//!
//! Multiway join planning relies on a broad view of the involved relations,
//! and chains of binary joins can make this challenging to reason about.
//! Collecting multiple joins together with their constraints improves
//! our ability to plan these joins, and reason about other operators' motion
//! around them.
//!
//! Also removes unit collections from joins, and joins with fewer than two inputs.
//!
//! Unit collections have no columns and a count of one, and a join with such
//! a collection act as the identity operator on collections. Once removed,
//! we may find joins with zero or one input, which can be further simplified.

use crate::{TransformArgs, TransformError};
use mz_expr::visit::Visit;
use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_repr::RelationType;

/// Fuses multiple `Join` operators into one `Join` operator.
///
/// Removes unit collections from joins, and joins with fewer than two inputs.
/// Filters on top of nested joins are lifted so the nested joins can be fused.
#[derive(Debug)]
pub struct Join;

impl crate::Transform for Join {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        relation.try_visit_mut_post(&mut |e| self.action(e))
    }
}

impl Join {
    /// Fuses multiple `Join` operators into one `Join` operator.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), TransformError> {
        if let MirRelationExpr::Join {
            inputs,
            equivalences,
            ..
        } = relation
        {
            let mut join_builder = JoinBuilder::new(equivalences);

            // We scan through each input, digesting any joins that we find and updating their equivalence classes.
            // We retain any existing equivalence classes, as they are already with respect to the cross product.
            for input in inputs.drain(..) {
                match input {
                    MirRelationExpr::Join {
                        inputs,
                        equivalences,
                        ..
                    } => {
                        // Merge the inputs into the new join being built.
                        join_builder.add_subjoin(inputs, equivalences, None)?;
                    }
                    MirRelationExpr::Filter { input, predicates } => {
                        if let MirRelationExpr::Join {
                            inputs,
                            equivalences,
                            ..
                        } = *input
                        {
                            // Merge the inputs and the predicates into the new join being built.
                            join_builder.add_subjoin(inputs, equivalences, Some(predicates))?;
                        } else {
                            // Retain the input.
                            let input = input.filter(predicates);
                            join_builder.add_input(input);
                        }
                    }
                    _ => {
                        // Retain the input.
                        join_builder.add_input(input);
                    }
                }
            }

            *relation = join_builder.build();
        }
        Ok(())
    }
}

/// Helper builder for fusing the inputs of nested joins into a single Join expression.
struct JoinBuilder {
    inputs: Vec<MirRelationExpr>,
    equivalences: Vec<Vec<MirScalarExpr>>,
    num_columns: usize,
    /// Predicates that will be evaluated on top of the join, if any.
    predicates: Vec<MirScalarExpr>,
}

impl JoinBuilder {
    fn new(equivalences: &mut Vec<Vec<MirScalarExpr>>) -> Self {
        Self {
            inputs: Vec::new(),
            equivalences: equivalences.drain(..).collect(),
            num_columns: 0,
            predicates: Vec::new(),
        }
    }

    fn add_input(&mut self, input: MirRelationExpr) {
        // Filter join identities out of the inputs.
        // The join identity is a single 0-ary row constant expression.
        let insert = {
            if let MirRelationExpr::Constant {
                rows: Ok(rows),
                typ,
            } = &input
            {
                !(rows.len() == 1 && typ.column_types.len() == 0 && rows[0].1 == 1)
            } else {
                true
            }
        };
        if insert {
            self.num_columns += input.arity();
            self.inputs.push(input);
        }
    }

    fn add_subjoin<I>(
        &mut self,
        inputs: I,
        mut equivalences: Vec<Vec<MirScalarExpr>>,
        predicates: Option<Vec<MirScalarExpr>>,
    ) -> Result<(), TransformError>
    where
        I: IntoIterator<Item = MirRelationExpr>,
    {
        // Update and push all of the variables.
        for mut equivalence in equivalences.drain(..) {
            for expr in equivalence.iter_mut() {
                expr.visit_mut_post(&mut |e| {
                    if let MirScalarExpr::Column(c) = e {
                        *c += self.num_columns;
                    }
                })?;
            }
            self.equivalences.push(equivalence);
        }

        if let Some(mut predicates) = predicates {
            for mut expr in predicates.drain(..) {
                expr.visit_mut_post(&mut |e| {
                    if let MirScalarExpr::Column(c) = e {
                        *c += self.num_columns;
                    }
                })?;
                self.predicates.push(expr);
            }
        }

        // Add all of the inputs.
        for input in inputs {
            self.add_input(input);
        }
        Ok(())
    }

    fn build(mut self) -> MirRelationExpr {
        mz_expr::canonicalize::canonicalize_equivalence_classes(&mut self.equivalences);

        // If `inputs` is now empty or a singleton (without constraints),
        // we can remove the join.
        let mut join = match self.inputs.len() {
            0 => {
                // The identity for join is the collection containing a single 0-ary row.
                MirRelationExpr::constant(vec![vec![]], RelationType::empty())
            }
            1 if self.equivalences.is_empty() => {
                // if there are constraints, they probably should have
                // been pushed down by predicate pushdown, but .. let's
                // not re-write that code here.
                self.inputs.pop().unwrap()
            }
            _ => MirRelationExpr::Join {
                inputs: self.inputs,
                equivalences: self.equivalences,
                implementation: mz_expr::JoinImplementation::Unimplemented,
            },
        };

        if !self.predicates.is_empty() {
            join = join.filter(self.predicates);
        }
        join
    }
}
