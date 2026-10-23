// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Monotonicity rules.
//!
//! Monotonic `Reduce` and `TopK` variants keep only the current winners of each group, so
//! they cannot take a retraction back. Rendering turns each retraction that reaches them into
//! an error in the output (`ensure_monotonic`), so they require an input that never retracts
//! a row.
//!
//! The walk derives two judgements for every node. A collection is logically monotonic if it
//! never retracts a row. That holds for imports marked monotonic whose source operators have
//! no temporal predicate, which would retract rows as time passes, and passes through `Get`,
//! `Mfp` and `FlatMap` without temporal bounds or retracting table functions, `ArrangeBy`,
//! joins and unions of monotonic inputs, distinct `Reduce`s and `Threshold`s of a monotonic
//! input, and `Let` and `LetRec` bindings, and it holds for constants of positive diffs. It
//! holds for no other operator. This is the judgement of the optimizer's `Monotonic`
//! analysis, which decides where lowering picks monotonic variants, for every operator that
//! lowering emits, so optimizer plans meet it.
//!
//! A collection is physically monotonic if its update stream has no negative diffs, as
//! `SingleTimeMonotonic` judges it. Lowering uses that analysis in single-time dataflows to
//! drop the consolidation that monotonic variants otherwise apply to their input, and the
//! walk calls the same analysis. Outside single-time dataflows the judgement is false.

use std::collections::BTreeSet;

use mz_expr::Id;
use mz_repr::{Diff, GlobalId};

use crate::plan::interpret::{
    Context, ContextEntry, Interpreter, PhysicallyMonotonic, SingleTimeMonotonic,
};
use crate::plan::reduce::{HierarchicalPlan, ReducePlan};
use crate::plan::top_k::TopKPlan;
use crate::plan::{GetPlan, LirRelationNode};

use super::Rule;
use super::arity::Issues;

/// Whether a collection retracts rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Monotonicity {
    /// The collection never retracts a row.
    pub logical: bool,
    /// The collection's update stream has no negative diffs. Only ever true in single-time
    /// dataflows.
    pub physical: bool,
}

impl Monotonicity {
    /// The monotonicity of a collection without rows.
    pub const NO_ROWS: Monotonicity = Monotonicity {
        logical: true,
        physical: true,
    };
}

/// The monotonicity analysis of one dataflow.
#[derive(Debug, Default)]
pub(super) struct Analysis {
    /// Whether the dataflow is single-time.
    single_time: bool,
    /// The imported collections marked monotonic.
    monotonic_ids: BTreeSet<GlobalId>,
}

impl Analysis {
    pub(super) fn new(single_time: bool, monotonic_ids: BTreeSet<GlobalId>) -> Self {
        Analysis {
            single_time,
            monotonic_ids,
        }
    }

    pub(super) fn single_time(&self) -> bool {
        self.single_time
    }

    /// The monotonicity of the imported collection `id`, where `temporal` says whether the
    /// operators of one of its source imports have a temporal predicate.
    pub(super) fn import(&self, id: GlobalId, temporal: bool) -> Monotonicity {
        let monotonic = self.monotonic_ids.contains(&id);
        Monotonicity {
            logical: monotonic && !temporal,
            // `SingleTimeMonotonic` judges imports by their flag alone, and a single time
            // has no later time at which a temporal predicate retracts.
            physical: self.single_time && monotonic,
        }
    }

    /// The monotonicity of the output of `node`, which produces rows, given that of its
    /// inputs in order, or of its binding for a `Get`.
    ///
    /// Checks that monotonic variants receive a monotonic input. `in_rec` says whether `node`
    /// is inside the value of a `LetRec`. Returns `None` for `Let` and `LetRec`, whose output
    /// is their body's, with the body's monotonicity.
    pub(super) fn node(
        &self,
        issues: &mut Issues,
        node: &LirRelationNode,
        inputs: &[Monotonicity],
        in_rec: bool,
    ) -> Option<Monotonicity> {
        let interpreter = SingleTimeMonotonic::new(&self.monotonic_ids);
        let mut ctx = Context::default();
        ctx.is_rec = in_rec;
        let first = inputs.first().copied().unwrap_or(Monotonicity::NO_ROWS);
        let first_physical = PhysicallyMonotonic(first.physical);
        let all_physical = || {
            inputs
                .iter()
                .map(|m| PhysicallyMonotonic(m.physical))
                .collect()
        };
        let all_logical = inputs.iter().all(|m| m.logical);
        let (logical, PhysicallyMonotonic(physical)) = match node {
            LirRelationNode::Constant { rows } => (
                rows.as_ref()
                    .is_ok_and(|rows| rows.iter().all(|(_, _, diff)| *diff > Diff::ZERO)),
                interpreter.constant(&ctx, rows),
            ),
            LirRelationNode::Get { id, keys, plan } => {
                let temporal = match plan {
                    GetPlan::PassArrangements => false,
                    GetPlan::Arrangement(_, _, mfp) | GetPlan::Collection(mfp) => {
                        mfp.has_temporal_bounds()
                    }
                };
                if let Id::Local(local) = id {
                    // The walk states the physical monotonicity of recursive bindings as
                    // false, which is how the interpreter judges them.
                    ctx.bindings.insert(
                        *local,
                        ContextEntry {
                            is_rec: false,
                            value: first_physical,
                        },
                    );
                }
                (
                    first.logical && !temporal,
                    interpreter.get(&ctx, id, keys, plan),
                )
            }
            LirRelationNode::Let { .. } | LirRelationNode::LetRec { .. } => return None,
            LirRelationNode::Mfp {
                mfp, input_key_val, ..
            } => (
                first.logical && !mfp.has_temporal_bounds(),
                interpreter.mfp(&ctx, first_physical, mfp, input_key_val),
            ),
            LirRelationNode::FlatMap {
                input_key,
                exprs,
                func,
                mfp_after,
                ..
            } => (
                first.logical && func.preserves_monotonicity() && !mfp_after.has_temporal_bounds(),
                interpreter.flat_map(&ctx, input_key, first_physical, exprs, func, mfp_after),
            ),
            LirRelationNode::Join { plan, .. } => {
                (all_logical, interpreter.join(&ctx, all_physical(), plan))
            }
            LirRelationNode::Reduce {
                input_key,
                key_val_plan,
                plan,
                mfp_after,
                ..
            } => {
                if let ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(p)) = plan {
                    self.require(issues, "Reduce", p.must_consolidate, first, in_rec);
                }
                (
                    matches!(plan, ReducePlan::Distinct) && first.logical,
                    interpreter.reduce(
                        &ctx,
                        input_key,
                        first_physical,
                        key_val_plan,
                        plan,
                        mfp_after,
                    ),
                )
            }
            LirRelationNode::TopK { top_k_plan, .. } => {
                let must_consolidate = match top_k_plan {
                    TopKPlan::MonotonicTop1(p) => Some(p.must_consolidate),
                    TopKPlan::MonotonicTopK(p) => Some(p.must_consolidate),
                    TopKPlan::Basic(_) => None,
                };
                if let Some(must_consolidate) = must_consolidate {
                    self.require(issues, "TopK", must_consolidate, first, in_rec);
                }
                (false, interpreter.top_k(&ctx, first_physical, top_k_plan))
            }
            LirRelationNode::Negate { .. } => (false, interpreter.negate(&ctx, first_physical)),
            LirRelationNode::Threshold { threshold_plan, .. } => (
                first.logical,
                interpreter.threshold(&ctx, first_physical, threshold_plan),
            ),
            LirRelationNode::Union {
                consolidate_output, ..
            } => (
                all_logical,
                interpreter.union(&ctx, all_physical(), *consolidate_output),
            ),
            LirRelationNode::ArrangeBy {
                input_key,
                input_mfp,
                forms,
                ..
            } => (
                first.logical && !input_mfp.has_temporal_bounds(),
                interpreter.arrange_by(&ctx, first_physical, forms, input_key, input_mfp),
            ),
        };
        Some(Monotonicity {
            logical,
            physical: self.single_time && physical,
        })
    }

    /// Checks that the input of a monotonic variant of `operator` is monotonic.
    fn require(
        &self,
        issues: &mut Issues,
        operator: &str,
        must_consolidate: bool,
        input: Monotonicity,
        in_rec: bool,
    ) {
        // In a single-time dataflow, the input has a single time outside recursions, so
        // consolidating it leaves the accumulated collection, without retractions. Lowering
        // picks monotonic variants there with consolidation, and drops it where the input is
        // physically monotonic. In a recursion the input changes between iterations.
        let consolidated = self.single_time && must_consolidate && !in_rec;
        if !(input.logical || input.physical || consolidated) {
            issues.push((
                Rule::Monotonicity,
                format!(
                    "monotonic {operator} requires a monotonic input, but its input may retract rows"
                ),
            ));
        }
    }
}
