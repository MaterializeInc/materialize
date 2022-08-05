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

use std::collections::HashSet;

use crate::TransformArgs;
use mz_expr::visit::Visit;
use mz_expr::MirRelationExpr;
use mz_expr::{Id, LocalId};

/// Remove Threshold operators that have no effect.
#[derive(Debug)]
pub struct ThresholdElision;

impl crate::Transform for ThresholdElision {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        relation.try_visit_mut_post(&mut |e| self.action(e))
    }
}

impl ThresholdElision {
    /// Remove Threshold operators with no effect.
    pub fn action(&self, relation: &mut MirRelationExpr) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::Threshold { input } = relation {
            // We look for the pattern `Union { base, Negate(Subset(base)) }`.
            let mut should_replace = false;
            if let MirRelationExpr::Union { base, inputs } = &mut **input {
                if inputs.len() == 1 {
                    if let MirRelationExpr::Negate { input } = &inputs[0] {
                        let mut safe_lets = HashSet::default();
                        if non_negative(base, &mut safe_lets) && lhs_superset_of_rhs(base, &*input)
                        {
                            should_replace = true;
                        }
                    }
                }
            }
            if should_replace {
                *relation = input.take_dangerous();
            }
        }
        Ok(())
    }
}

/// Return true if `relation` is believed to contain no negative multiplicities.
///
/// This method is a conservative approximation and is known to miss not-hard cases.
///
/// This assumes that all `Get` bindings correspond to collections without negative
/// multiplicities. Local let bindings present in `safe_lets` are relied on to have
/// no non-negative multiplicities.
pub fn non_negative(relation: &MirRelationExpr, safe_lets: &mut HashSet<LocalId>) -> bool {
    // This implementation is iterative.
    // Before converting this implementation to recursive (e.g. to improve its accuracy)
    // make sure to use the `CheckedRecursion` struct to avoid blowing the stack.
    let mut to_check = vec![relation];
    while let Some(expr) = to_check.pop() {
        match expr {
            MirRelationExpr::Constant { rows, .. } => {
                if let Ok(rows) = rows {
                    if rows.iter().any(|(_data, diff)| diff < &0) {
                        return false;
                    }
                }
            }
            MirRelationExpr::Negate { .. } => {
                return false;
            }
            MirRelationExpr::Get { id, .. } => {
                if let Id::Local(local_id) = id {
                    if !safe_lets.contains(local_id) {
                        return false;
                    }
                }
            }
            MirRelationExpr::Let { id, value, body } => {
                // We will check both `value` and `body`, with the latter
                // under the assumption that `value` works out. Of course,
                // if `value` doesn't work out we'll return `false`.
                if !safe_lets.insert(id.clone()) {
                    // Return false conservatively if we detect identifier re-use.
                    // Ideally this would be unreachable code.
                    return false;
                }
                to_check.push(value);
                to_check.push(body);
            }
            x => {
                to_check.extend(x.children());
            }
        }
    }
    return true;
}

/// Returns true iff `rhs` is always a subset of `lhs`.
///
/// This method is a conservative approximation and is known to miss not-hard cases.
///
/// We iteratively descend `rhs` through a few operators, looking for `lhs`.
pub fn lhs_superset_of_rhs(lhs: &MirRelationExpr, mut rhs: &MirRelationExpr) -> bool {
    // This implementation is iterative.
    // Before converting this implementation to recursive (e.g. to improve its accuracy)
    // make sure to use the `CheckedRecursion` struct to avoid blowing the stack.
    while lhs != rhs {
        match rhs {
            MirRelationExpr::Filter { input, .. } => rhs = &**input,
            MirRelationExpr::TopK { input, .. } => rhs = &**input,
            _ => {
                // TODO: Imagine more complex reasoning here!
                return false;
            }
        }
    }
    return true;
}
