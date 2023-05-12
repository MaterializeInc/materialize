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

use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;
use mz_repr::RelationType;

use crate::TransformArgs;

/// Fuses `Union` and `Negate` operators into one `Union` and multiple `Negate` operators.
#[derive(Debug)]
pub struct UnionNegateFusion;

impl crate::Transform for UnionNegateFusion {
    fn recursion_safe(&self) -> bool {
        true
    }

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
        relation.visit_mut_post(&mut Self::action)?;
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

impl UnionNegateFusion {
    /// Fuses multiple `Union` operators into one.
    /// Nested negated unions are merged into the parent one by pushing
    /// the Negate to all their inputs.
    pub fn action(relation: &mut MirRelationExpr) {
        use MirRelationExpr::*;
        if let Union { base, inputs } = relation {
            let can_fuse = iter::once(&**base).chain(&*inputs).any(|input| -> bool {
                match input {
                    Union { .. } => true,
                    Negate { input } => matches!(**input, Union { .. }),
                    _ => false,
                }
            });
            if can_fuse {
                let mut new_inputs: Vec<MirRelationExpr> = vec![];
                for input in iter::once(base.as_mut()).chain(inputs) {
                    let input = input.take_dangerous();
                    match input {
                        Union { base, inputs } => {
                            new_inputs.push(*base);
                            new_inputs.extend(inputs);
                        }
                        Negate { input } if matches!(*input, Union { .. }) => {
                            if let Union { base, inputs } = *input {
                                new_inputs.push(base.negate());
                                new_inputs.extend(inputs.into_iter().map(|x| x.negate()));
                            } else {
                                unreachable!()
                            }
                        }
                        _ => new_inputs.push(input),
                    }
                }

                // Pushing down negations might enable further Negate fusion.
                for new_input in new_inputs.iter_mut() {
                    crate::fusion::negate::Negate::action(new_input);
                }

                // A valid relation type is only needed for empty unions, but an existing union
                // is guaranteed to be non-empty given that it always has at least a base branch.
                assert!(!new_inputs.is_empty());
                *relation = MirRelationExpr::union_many(new_inputs, RelationType::empty());
            }
        }
    }
}
