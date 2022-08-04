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
                        if non_negative(base, &mut safe_lets) && subset_of(base, &*input) {
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

/// Return true iff `relation` is believed to contain no negative multiplicities.
///
/// This relies on all `Get` bindings corresponding to collections without negative
/// multiplicities. Local let bindings present in `safe_lets` are relied on to have
/// no non-negative multiplicities.
pub fn non_negative(relation: &MirRelationExpr, safe_lets: &mut HashSet<LocalId>) -> bool {
    match relation {
        MirRelationExpr::Constant { rows, .. } => {
            if let Ok(rows) = rows {
                rows.iter().all(|(_data, diff)| diff >= &0)
            } else {
                true
            }
        }
        MirRelationExpr::Negate { .. } => false,
        MirRelationExpr::Get { id, .. } => match id {
            Id::Global(_) => true,
            Id::Local(local_id) => safe_lets.contains(local_id),
        },
        MirRelationExpr::Let { id, value, body } => {
            let value_non_negative = non_negative(value, safe_lets);
            if value_non_negative {
                safe_lets.insert(id.clone());
            }
            let result = non_negative(body, safe_lets);
            safe_lets.remove(id);
            result
        }
        x => {
            use mz_expr::visit::VisitChildren;
            let mut all_non_negative = true;
            x.visit_children(|e| all_non_negative &= non_negative(e, safe_lets));
            all_non_negative
        }
    }
}

/// Returns true iff `b` is always a subset of `a`.
pub fn subset_of(a: &MirRelationExpr, b: &MirRelationExpr) -> bool {
    if a == b {
        true
    } else {
        match b {
            MirRelationExpr::Filter { input, .. } => subset_of(a, input),
            MirRelationExpr::TopK { input, .. } => subset_of(a, input),
            _ => {
                // TODO: Imagine more complex reasoning here!
                false
            }
        }
    }
}
