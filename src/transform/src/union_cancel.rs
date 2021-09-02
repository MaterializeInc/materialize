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
use expr::MirRelationExpr;

/// Detects an input being unioned with its negation and cancels them out
#[derive(Debug)]
pub struct UnionBranchCancellation;

impl crate::Transform for UnionBranchCancellation {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), TransformError> {
        relation.try_visit_mut(&mut |e| self.action(e))
    }
}

enum BranchCmp {
    Equivalent,
    Inverse,
    Distinct,
}

impl BranchCmp {
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
            let matching_negation = |input: &MirRelationExpr,
                                     inputs: &[MirRelationExpr],
                                     start_idx: usize|
             -> Option<usize> {
                for i in start_idx..inputs.len() {
                    if let BranchCmp::Inverse = Self::compare_branches(input, &inputs[i]) {
                        return Some(i);
                    }
                }
                None
            };

            if let Some(j) = matching_negation(&*base, inputs, 0) {
                let relation_typ = base.typ();
                **base = MirRelationExpr::constant(vec![], relation_typ.clone());
                inputs[j] = MirRelationExpr::constant(vec![], relation_typ);
            }

            for i in 0..inputs.len() {
                if let Some(j) = matching_negation(&inputs[i], inputs, i + 1) {
                    let relation_typ = inputs[i].typ();
                    inputs[i] = MirRelationExpr::constant(vec![], relation_typ.clone());
                    inputs[j] = MirRelationExpr::constant(vec![], relation_typ);
                }
            }
        }
        Ok(())
    }

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
