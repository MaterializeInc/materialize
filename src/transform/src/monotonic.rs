// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Analysis to identify monotonic collections, especially TopK inputs.
use std::collections::BTreeSet;

use itertools::zip_eq;
use mz_expr::visit::VisitChildren;
use mz_expr::{Id, LetRecLimit, LocalId, MirRelationExpr, RECURSION_LIMIT};
use mz_ore::cast::CastFrom;
use mz_ore::soft_panic_or_log;
use mz_ore::stack::{CheckedRecursion, RecursionGuard};
use mz_repr::GlobalId;

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
        mon_ids: &BTreeSet<GlobalId>,
        locals: &mut BTreeSet<LocalId>,
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
                MirRelationExpr::LetRec {
                    ids,
                    values,
                    limits,
                    body,
                } => {
                    // Optimistically assume that all bindings were monotonic
                    // (it is safe to do this as we initialize each binding to
                    // the empty collection, which is trivially monotonic).
                    for id in ids.iter() {
                        let inserted = locals.insert(id.clone());
                        assert!(inserted, "Shadowing of identifier: {id:?}");
                    }

                    // The following is equivalent to a dataflow analysis on the
                    // following lattice:
                    // - The bottom element is `true` / contained in `locals`.
                    // - The top element is `false` / not present in `locals`.
                    // - The join operator is boolean `AND`.
                    // - The meet operator is boolean `OR` (but it's not used).
                    //
                    // Sequentially AND locals[id] with the result of descending
                    // into a clone of values[id]. Repeat until one of the
                    // following conditions is met:
                    //
                    // 1. The locals entries have stabilized at a fixpoint.
                    // 2. No fixpoint was found after `max_iterations`. If this
                    //    is the case we reset the locals entries for all
                    //    recursive CTEs to `false` by removing them from
                    //    `locals` (pessimistic approximation).
                    // 3. We reach the user-specified recursion limit of any of
                    //    the bindings. In this case, we also give up similarly
                    //    to (2), because we don't want to complicate things
                    //    with handling different limits per binding.
                    let min_max_iter = LetRecLimit::min_max_iter(limits);
                    let max_iterations = 100;
                    let mut curr_iteration = 0;
                    loop {
                        // Check for conditions (2) and (3).
                        if curr_iteration >= max_iterations
                            || min_max_iter
                                .map(|min_max_iter| curr_iteration >= min_max_iter)
                                .unwrap_or(false)
                        {
                            if curr_iteration > u64::cast_from(ids.len()) {
                                soft_panic_or_log!(
                                    "LetRec loop in MonotonicFlag has not converged in |{}|",
                                    ids.len()
                                );
                            }

                            for id in ids.iter() {
                                locals.remove(id);
                            }
                            break;
                        }

                        // Check for condition (1).
                        let mut change = false;
                        for (id, mut value) in zip_eq(ids.iter(), values.iter().cloned()) {
                            if !self.apply(&mut value, mon_ids, locals)? {
                                change |= locals.remove(id); // set only if `id` is still present
                            }
                        }
                        if !change {
                            break;
                        }

                        curr_iteration += 1;
                    }

                    // Descend into the values with the locals.
                    for value in values.iter_mut() {
                        self.apply(value, mon_ids, locals)?;
                    }

                    let is_monotonic = self.apply(body, mon_ids, locals)?;

                    // Remove shadowed bindings. This is good hygiene, as
                    // otherwise with nested LetRec blocks the `loop { ... }`
                    // above will carry inner LetRec IDs across outer LetRec
                    // iterations. As a consequence, the "no shadowing"
                    // assertion at the beginning of this block will fail at the
                    // inner LetRec for the second outer LetRec iteration.
                    for id in ids.iter() {
                        locals.remove(id);
                    }

                    is_monotonic
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
