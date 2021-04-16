// Copyright Materialize, Inc. All rights reserved.
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

impl UnionBranchCancellation {
    /// Detects an input being unioned with its negation and cancels them out
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), TransformError> {
        if let MirRelationExpr::Union { base, inputs } = relation {
            let matching_negation = |input: &MirRelationExpr,
                                     inputs: &[MirRelationExpr],
                                     start_idx: usize|
             -> Option<usize> {
                match input {
                    MirRelationExpr::Negate { input: inner_input } => {
                        for i in start_idx..inputs.len() {
                            if inputs[i] == **inner_input {
                                return Some(i);
                            }
                        }
                    }
                    _ => {
                        for i in start_idx..inputs.len() {
                            if let MirRelationExpr::Negate { input: inner_input } = &inputs[i] {
                                if *input == **inner_input {
                                    return Some(i);
                                }
                            }
                        }
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
}
