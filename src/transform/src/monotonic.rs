// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Analysis to identify monotonic collections, especially TopK inputs.
use mz_expr::visit::VisitChildren;
use mz_expr::{Id, LocalId, MirRelationExpr, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_repr::GlobalId;
use std::collections::HashSet;

/// A struct that holds a recursive function that determines if a
/// relation is monotonic, and applies any optimizations along the way.
#[derive(Debug)]
pub struct MonotonicFlag {
    recursion_guard: RecursionGuard,
}

impl Default for MonotonicFlag {
    fn default() -> MonotonicFlag {
        MonotonicFlag {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for MonotonicFlag {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl MonotonicFlag {
    /// Determines if a relation is monotonic, and applies any optimizations along the way.
    /// mon_ids should be the ids of monotonic sources and indexes involved in expr.
    pub fn apply(
        &self,
        expr: &mut MirRelationExpr,
        mon_ids: &HashSet<GlobalId>,
        locals: &mut HashSet<LocalId>,
    ) -> Result<bool, crate::RecursionLimitError> {
        self.checked_recur(|_| {
            let is_monotonic = match expr {
                MirRelationExpr::Get { id, .. } => match id {
                    Id::Global(id) => mon_ids.contains(id),
                    Id::Local(id) => locals.contains(id),
                },
                MirRelationExpr::Project { input, .. } => self.apply(input, mon_ids, locals)?,
                MirRelationExpr::Filter { input, predicates } => {
                    let is_monotonic = self.apply(input, mon_ids, locals)?;
                    // Non-temporal predicates can introduce non-monotonicity, as they
                    // can result in the future removal of records.
                    // TODO: this could be improved to only restrict if upper bounds
                    // are present, as temporal lower bounds only delay introduction.
                    is_monotonic && !predicates.iter().any(|p| p.contains_temporal())
                }
                MirRelationExpr::Map { input, .. } => self.apply(input, mon_ids, locals)?,
                MirRelationExpr::TopK {
                    input, monotonic, ..
                } => {
                    *monotonic = self.apply(input, mon_ids, locals)?;
                    false
                }
                MirRelationExpr::Reduce {
                    input,
                    aggregates,
                    monotonic,
                    ..
                } => {
                    *monotonic = self.apply(input, mon_ids, locals)?;
                    // Reduce is monotonic iff its input is and it is a "distinct",
                    // with no aggregate values; otherwise it may need to retract.
                    *monotonic && aggregates.is_empty()
                }
                MirRelationExpr::Union { base, inputs } => {
                    let mut monotonic = self.apply(base, mon_ids, locals)?;
                    for input in inputs.iter_mut() {
                        let monotonic_i = self.apply(input, mon_ids, locals)?;
                        monotonic = monotonic && monotonic_i;
                    }
                    monotonic
                }
                MirRelationExpr::ArrangeBy { input, .. } => self.apply(input, mon_ids, locals)?,
                MirRelationExpr::FlatMap { input, func, .. } => {
                    let is_monotonic = self.apply(input, mon_ids, locals)?;
                    is_monotonic && func.preserves_monotonicity()
                }
                MirRelationExpr::Join { inputs, .. } => {
                    // If all inputs to the join are monotonic then so is the join.
                    let mut monotonic = true;
                    for input in inputs.iter_mut() {
                        let monotonic_i = self.apply(input, mon_ids, locals)?;
                        monotonic = monotonic && monotonic_i;
                    }
                    monotonic
                }
                MirRelationExpr::Constant { rows: Ok(rows), .. } => {
                    rows.iter().all(|(_, diff)| diff > &0)
                }
                MirRelationExpr::Threshold { input } => self.apply(input, mon_ids, locals)?,
                MirRelationExpr::Let { id, value, body } => {
                    let prior = locals.remove(id);
                    if self.apply(value, mon_ids, locals)? {
                        locals.insert(*id);
                    }
                    let result = self.apply(body, mon_ids, locals)?;
                    if prior {
                        locals.insert(*id);
                    } else {
                        locals.remove(id);
                    }
                    result
                }
                // The default behavior.
                // TODO: check that this is the behavior we want.
                MirRelationExpr::Negate { .. } | MirRelationExpr::Constant { rows: Err(_), .. } => {
                    expr.try_visit_mut_children(|e| self.apply(e, mon_ids, locals).map(|_| ()))?;
                    false
                }
            };
            Ok(is_monotonic)
        })
    }
}
