// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Remove Threshold operators when we are certain no records have negative multiplicity.
//!
//! If we have Threshold(A - Subset(A)) and we believe that A has no negative multiplicities,
//! then we can replace this with A - Subset(A).
//!
//! The Subset(X) notation means that the collection is a multiset subset of X:
//! multiplicities of each record in Subset(X) are at most that of X.

use crate::attribute::{DerivedAttributes, RequiredAttributes};
use crate::attribute::{NonNegative, SubtreeSize};
use crate::TransformArgs;

use mz_expr::visit::{Visit, VisitorMut};
use mz_expr::MirRelationExpr;

/// Remove Threshold operators that have no effect.
#[derive(Debug)]
pub struct ThresholdElision;

impl crate::Transform for ThresholdElision {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let mut visitor = ThresholdElisionAction::default();
        relation.visit_mut(&mut visitor).map_err(From::from)
    }
}

struct ThresholdElisionAction {
    deriver: DerivedAttributes,
}

impl Default for ThresholdElisionAction {
    fn default() -> Self {
        let mut builder = RequiredAttributes::default();
        builder.require::<NonNegative>();
        builder.require::<SubtreeSize>();
        Self {
            deriver: builder.finish(),
        }
    }
}

impl VisitorMut<MirRelationExpr> for ThresholdElisionAction {
    fn pre_visit(&mut self, expr: &mut MirRelationExpr) {
        self.deriver.pre_visit(expr);
    }

    fn post_visit(&mut self, expr: &mut MirRelationExpr) {
        self.deriver.post_visit(expr);
        self.action(expr);
    }
}

impl ThresholdElisionAction {
    /// Remove Threshold operators with no effect.
    pub fn action(&mut self, expr: &mut MirRelationExpr) {
        // The results vectors or all attributes should be equal after each step.
        debug_assert_eq!(
            self.deriver.get_results::<NonNegative>().len(),
            self.deriver.get_results::<SubtreeSize>().len()
        );

        if let MirRelationExpr::Threshold { input } = expr {
            // We look for the pattern `Union { base, Negate(Subset(base)) }`.
            let mut should_replace = false;
            if let MirRelationExpr::Union { base, inputs } = &mut **input {
                if inputs.len() == 1 {
                    if let MirRelationExpr::Negate { input } = &inputs[0] {
                        // This is somewhat convoluted way to access the non_negative result for the base:
                        // - the Threshold is at position n - 1,
                        // - the Union (i.e., the Threshold input) is n - 2,
                        // - the Union input[0] is at position n - 3 and its subtree size is m,
                        // - the Union base therefore is at position n - m - 3
                        let n = self.deriver.get_results::<NonNegative>().len();
                        let m = self.deriver.get_results::<SubtreeSize>()[n - 3];
                        if self.deriver.get_results::<NonNegative>()[n - m - 3]
                            && is_superset_of(base, &*input)
                        {
                            should_replace = true;
                        }
                    }
                }
            }
            if should_replace {
                // Replace the root Threshold with its input.
                *expr = input.take_dangerous();
                // Trim the attribute result vectors inferred so far to adjust for the above change.
                self.deriver.trim();
                // We can be a bit smarter when adjusting the NonNegative result. Since the Threshold
                // at the root can only be safely elided iff its input is non-negative, we can overwrite
                // the new last value to be `true`.
                if let Some(result) = self.deriver.get_results_mut::<NonNegative>().last_mut() {
                    *result = true;
                }
            }
        }
    }
}

/// Returns true iff `rhs` is always a subset of `lhs`.
///
/// This method is a conservative approximation and is known to miss not-hard cases.
///
/// We iteratively descend `rhs` through a few operators, looking for `lhs`.
/// In addition, we descend simultaneously through `lhs` and `rhs` if the root node
/// on both sides is identical.
pub fn is_superset_of(mut lhs: &MirRelationExpr, mut rhs: &MirRelationExpr) -> bool {
    // This implementation is iterative.
    // Before converting this implementation to recursive (e.g. to improve its accuracy)
    // make sure to use the `CheckedRecursion` struct to avoid blowing the stack.
    while lhs != rhs {
        match rhs {
            MirRelationExpr::Filter { input, .. } => rhs = &**input,
            MirRelationExpr::TopK { input, .. } => rhs = &**input,
            // Descend in both sides if the current roots are
            // projections with the same `outputs` vector.
            MirRelationExpr::Project {
                input: rhs_input,
                outputs: rhs_outputs,
            } => match lhs {
                MirRelationExpr::Project {
                    input: lhs_input,
                    outputs: lhs_outputs,
                } if lhs_outputs == rhs_outputs => {
                    rhs = &**rhs_input;
                    lhs = &**lhs_input;
                }
                _ => return false,
            },
            // Descend in both sides if the current roots are reduces with empty aggregates
            // on the same set of keys (that is, a distinct operation on those keys).
            MirRelationExpr::Reduce {
                input: rhs_input,
                group_key: rhs_group_key,
                aggregates: rhs_aggregates,
                monotonic: _,
                expected_group_size: _,
            } if rhs_aggregates.is_empty() => match lhs {
                MirRelationExpr::Reduce {
                    input: lhs_input,
                    group_key: lhs_group_key,
                    aggregates: lhs_aggregates,
                    monotonic: _,
                    expected_group_size: _,
                } if lhs_aggregates.is_empty() && lhs_group_key == rhs_group_key => {
                    rhs = &**rhs_input;
                    lhs = &**lhs_input;
                }
                _ => return false,
            },
            _ => {
                // TODO: Imagine more complex reasoning here!
                return false;
            }
        }
    }
    return true;
}
