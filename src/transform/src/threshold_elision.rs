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

use mz_expr::visit::{Visit, VisitorMut};
use mz_expr::MirRelationExpr;

use crate::analysis::{Analysis, DerivedBuilder, NonNegative, SubtreeSize};
use crate::TransformCtx;

/// Remove Threshold operators that have no effect.
#[derive(Debug)]
pub struct ThresholdElision;

impl crate::Transform for ThresholdElision {
    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "threshold_elision")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), crate::TransformError> {
        let mut builder = DerivedBuilder::new(ctx.features);
        builder.require(NonNegative);
        builder.require(SubtreeSize);
        let mut derived = builder.visit(&*relation);
        let subtree_size = derived.take_results::<SubtreeSize>().unwrap();
        let non_negative = derived.take_results::<NonNegative>().unwrap();

        let mut visitor = Action::new(subtree_size, non_negative);
        let result = relation.visit_mut(&mut visitor).map_err(From::from);
        mz_repr::explain::trace_plan(&*relation);
        result
    }
}

struct Action {
    subtree_size: Vec<<SubtreeSize as Analysis>::Value>,
    non_negative: Vec<<NonNegative as Analysis>::Value>,
    index: usize,
}

impl VisitorMut<MirRelationExpr> for Action {
    fn pre_visit(&mut self, _expr: &mut MirRelationExpr) {}

    fn post_visit(&mut self, expr: &mut MirRelationExpr) {
        self.action(expr);
    }
}

impl Action {
    fn new(
        subtree_size: Vec<<SubtreeSize as Analysis>::Value>,
        non_negative: Vec<<NonNegative as Analysis>::Value>,
    ) -> Self {
        Self {
            subtree_size,
            non_negative,
            index: 0,
        }
    }

    /// Remove Threshold operators with no effect.
    pub fn action(&mut self, expr: &mut MirRelationExpr) {
        // The result result vectors should be equal after each step.
        debug_assert_eq!(self.non_negative.len(), self.subtree_size.len());

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
                        let n = self.index;
                        let m = self.subtree_size[n - 3];
                        if self.non_negative[n - m - 3] && is_superset_of(base, &*input) {
                            should_replace = true;
                        }
                    }
                }
            }
            if should_replace {
                // Trim the attribute result vectors inferred so far to adjust for the above change.
                self.subtree_size.remove(self.index);
                self.non_negative.remove(self.index);

                // We can be a bit smarter when adjusting the NonNegative result. Since the Threshold
                // at the root can only be safely elided iff its input is non-negative, we can overwrite
                // the new last value to be `true`.
                if let Some(result) = self.non_negative.get_mut(self.index - 1) {
                    *result = true;
                }

                // Replace the root Threshold with its input.
                *expr = input.take_dangerous();

                // Adjust index by -1 to account for removing the current root.
                self.index -= 1;
            }
        }

        // Advance the index to the next node in post-visit order.
        self.index += 1;
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
    true
}
