// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Detects an input being unioned with its negation and cancels them out

use crate::{TransformArgs, TransformError};
use itertools::Itertools;
use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;

/// Detects an input being unioned with its negation and cancels them out
#[derive(Debug)]
pub struct UnionBranchCancellation;

impl crate::Transform for UnionBranchCancellation {
    #[tracing::instrument(
        target = "optimizer"
        level = "trace",
        skip_all,
        fields(path.segment = "union_branch_cancellation")
    )]
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        let result = relation.try_visit_mut_post(&mut |e| self.action(e));
        mz_repr::explain_new::trace_plan(&*relation);
        result
    }
}

/// Result of the comparison of two branches of a union for cancellation
/// purposes.
enum BranchCmp {
    /// The two branches are equivalent in the sense the the produce the
    /// same exact results.
    Equivalent,
    /// The two branches are equivalent, but one of them produces negated
    /// row count values, and hence, they cancel each other.
    Inverse,
    /// The two branches are not equivalent in any way.
    Distinct,
}

impl BranchCmp {
    /// Modify the result of the comparison when a Negate operator is
    /// found at the top of one the branches just compared.
    fn inverse(self) -> Self {
        match self {
            BranchCmp::Equivalent => BranchCmp::Inverse,
            BranchCmp::Inverse => BranchCmp::Equivalent,
            BranchCmp::Distinct => BranchCmp::Distinct,
        }
    }
}

impl UnionBranchCancellation {
    /// Detects an input being unioned with its negation and cancels them out
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), TransformError> {
        if let MirRelationExpr::Union { base, inputs } = relation {
            // Compares a union branch against the remaining branches in the union
            // with opposite sign until it finds a branch that cancels the given one
            // and returns its position.
            let matching_negation = |input: &MirRelationExpr,
                                     sign: bool,
                                     inputs: &[MirRelationExpr],
                                     input_signs: &[bool],
                                     start_idx: usize|
             -> Option<usize> {
                for i in start_idx..inputs.len() {
                    // Only compare branches with opposite signs
                    if sign != input_signs[i] {
                        if let BranchCmp::Inverse = Self::compare_branches(input, &inputs[i]) {
                            return Some(i);
                        }
                    }
                }
                None
            };

            let base_sign = Self::branch_sign(base);
            let input_signs = inputs.iter().map(Self::branch_sign).collect_vec();

            // Compare branches if there is at least a negated branch
            if std::iter::once(&base_sign).chain(&input_signs).any(|x| *x) {
                if let Some(j) = matching_negation(&*base, base_sign, inputs, &input_signs, 0) {
                    let relation_typ = base.typ();
                    **base = MirRelationExpr::constant(vec![], relation_typ.clone());
                    inputs[j] = MirRelationExpr::constant(vec![], relation_typ);
                }

                for i in 0..inputs.len() {
                    if let Some(j) =
                        matching_negation(&inputs[i], input_signs[i], inputs, &input_signs, i + 1)
                    {
                        let relation_typ = inputs[i].typ();
                        inputs[i] = MirRelationExpr::constant(vec![], relation_typ.clone());
                        inputs[j] = MirRelationExpr::constant(vec![], relation_typ);
                    }
                }
            }
        }
        Ok(())
    }

    /// Returns the sign of a given union branch. The sign is `true` if the branch contains
    /// an odd number of Negate operators within a chain of Map, Filter and Project
    /// operators, and `false` otherwise.
    ///
    /// This sign is pre-computed for all union branches in order to avoid performing
    /// expensive comparisons of branches with the same sign since they can't possibly
    /// cancel each other.
    fn branch_sign(branch: &MirRelationExpr) -> bool {
        let mut relation = branch;
        let mut sign = false;
        loop {
            match relation {
                MirRelationExpr::Negate { input } => {
                    sign ^= true;
                    relation = &**input;
                }
                MirRelationExpr::Map { input, .. }
                | MirRelationExpr::Filter { input, .. }
                | MirRelationExpr::Project { input, .. } => {
                    relation = &**input;
                }
                _ => return sign,
            }
        }
    }

    /// Compares two branches to check whether they produce the same results but
    /// with negated row count values, ie. one of them contains an extra Negate operator.
    /// Negate operators may appear interleaved with Map, Filter and Project
    /// operators, but these operators must appear in the same order in both branches.
    fn compare_branches(relation: &MirRelationExpr, other: &MirRelationExpr) -> BranchCmp {
        match (relation, other) {
            (
                MirRelationExpr::Negate { input: input1 },
                MirRelationExpr::Negate { input: input2 },
            ) => Self::compare_branches(&*input1, &*input2),
            (r, MirRelationExpr::Negate { input }) | (MirRelationExpr::Negate { input }, r) => {
                Self::compare_branches(&*input, r).inverse()
            }
            (
                MirRelationExpr::Map {
                    input: input1,
                    scalars: scalars1,
                },
                MirRelationExpr::Map {
                    input: input2,
                    scalars: scalars2,
                },
            ) => {
                if scalars1 == scalars2 {
                    Self::compare_branches(&*input1, &*input2)
                } else {
                    BranchCmp::Distinct
                }
            }
            (
                MirRelationExpr::Filter {
                    input: input1,
                    predicates: predicates1,
                },
                MirRelationExpr::Filter {
                    input: input2,
                    predicates: predicates2,
                },
            ) => {
                if predicates1 == predicates2 {
                    Self::compare_branches(&*input1, &*input2)
                } else {
                    BranchCmp::Distinct
                }
            }
            (
                MirRelationExpr::Project {
                    input: input1,
                    outputs: outputs1,
                },
                MirRelationExpr::Project {
                    input: input2,
                    outputs: outputs2,
                },
            ) => {
                if outputs1 == outputs2 {
                    Self::compare_branches(&*input1, &*input2)
                } else {
                    BranchCmp::Distinct
                }
            }
            _ => {
                if relation == other {
                    BranchCmp::Equivalent
                } else {
                    BranchCmp::Distinct
                }
            }
        }
    }
}
