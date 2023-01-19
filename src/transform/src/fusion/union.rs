// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuses multiple `Union` operators into one.
//!
//! Nested negated unions are merged into the parent one by pushing
//! the Negate to all their branches.

use std::iter;

use crate::TransformArgs;
use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;
use mz_repr::RelationType;

/// Fuses multiple `Union` operators into one.
#[derive(Debug)]
pub struct Union;

impl crate::Transform for Union {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "union")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let result = relation.try_visit_mut_post(&mut |e| Ok(self.action(e)));
        mz_repr::explain_new::trace_plan(&*relation);
        result
    }
}

impl Union {
    /// Fuses multiple `Union` operators into one.
    ///
    /// The order among children is maintained, and the action should be idempotent.
    /// This action works best if other operators such as `Negate` and other linear
    /// operators are pushed down through other `Union` operators.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Union { inputs, .. } = relation {
            let mut list: Vec<MirRelationExpr> = Vec::with_capacity(1 + inputs.len());
            Self::unfold_unions_into(relation.take_dangerous(), &mut list);
            *relation = MirRelationExpr::Union {
                base: Box::new(list.remove(0)),
                inputs: list,
            }
        }
    }

    /// Unfolds `self` and children into a list of expressions that can be unioned.
    fn unfold_unions_into(expr: MirRelationExpr, list: &mut Vec<MirRelationExpr>) {
        let mut stack = vec![expr];
        while let Some(expr) = stack.pop() {
            if let MirRelationExpr::Union { base, inputs } = expr {
                stack.extend(inputs.into_iter().rev());
                stack.push(*base);
            } else {
                list.push(expr);
            }
        }
    }
}

/// Fuses `Union` and `Negate` operators into one `Union` and multiple `Negate` operators.
#[derive(Debug)]
pub struct UnionNegate;

impl crate::Transform for UnionNegate {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "union_negate")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let result = relation.try_visit_mut_post(&mut |e| Ok(self.action(e)));
        mz_repr::explain_new::trace_plan(&*relation);
        result
    }
}
impl UnionNegate {
    /// Fuses multiple `Union` operators into one.
    /// Nested negated unions are merged into the parent one by pushing
    /// the Negate to all their branches.
    pub fn action(&self, relation: &mut MirRelationExpr) {
        if let MirRelationExpr::Union { base, inputs } = relation {
            let can_fuse = iter::once(&**base).chain(&*inputs).any(|input| -> bool {
                match input {
                    MirRelationExpr::Union { .. } => true,
                    MirRelationExpr::Negate { input: inner_input } => {
                        if let MirRelationExpr::Union { .. } = **inner_input {
                            true
                        } else {
                            false
                        }
                    }
                    _ => false,
                }
            });
            if can_fuse {
                let mut new_inputs: Vec<MirRelationExpr> = vec![];
                for input in iter::once(&mut **base).chain(inputs) {
                    let outer_input = input.take_dangerous();
                    match outer_input {
                        MirRelationExpr::Union { base, inputs } => {
                            new_inputs.push(*base);
                            new_inputs.extend(inputs);
                        }
                        MirRelationExpr::Negate {
                            input: ref inner_input,
                        } => {
                            if let MirRelationExpr::Union { base, inputs } = &**inner_input {
                                new_inputs.push(base.to_owned().negate());
                                new_inputs.extend(inputs.into_iter().map(|x| x.clone().negate()));
                            } else {
                                new_inputs.push(outer_input);
                            }
                        }
                        _ => new_inputs.push(outer_input),
                    }
                }
                // A valid relation type is only needed for empty unions, but an existing union
                // is guaranteed to be non-empty given that it always has at least a base branch.
                assert!(!new_inputs.is_empty());
                *relation = MirRelationExpr::union_many(new_inputs, RelationType::empty());
            }
        }
    }
}
