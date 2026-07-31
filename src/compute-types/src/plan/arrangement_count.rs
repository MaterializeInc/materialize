// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Static prediction of how many arrangements a plan will build.
//!
//! This is the physical-multiplier half of a static memory bound: an arrangement's byte cost is
//! `count * rows * width`, and only `count` is determined by the plan alone, with no statistics.
//! Predicting it is therefore separable from, and verifiable independently of, any cardinality
//! estimate. Validation compares [`Prediction::data`] against the operators in each node's
//! `mz_lir_mapping` range, where a mismatch is a bug in this module rather than an estimation
//! error.
//!
//! Error arrangements are counted separately, not folded into one total. They hold nothing in a
//! healthy dataflow, so charging them against a row bound would inflate it, and they surface in
//! `mz_arrangement_sizes` only when they happen to have allocated, which no plan property predicts.
//!
//! Three counts cannot be read off the plan alone and are reported via [`Prediction::caveat`]
//! rather than silently guessed. See [`Caveat`] for which and why.

use std::collections::BTreeMap;

use crate::plan::join::JoinPlan;
use crate::plan::reduce::{BasicPlan, HierarchicalPlan, ReducePlan};
use crate::plan::top_k::TopKPlan;
use crate::plan::{LirId, LirRelationExpr, LirRelationNode};

/// Why a predicted count may not match reality.
///
/// A prediction carrying a caveat is excluded from the strict-equality check during validation,
/// because the deviation is expected rather than a defect.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Caveat {
    /// `ensure_collections` skips a form whose key the bundle already holds, so counting the
    /// advertised forms is an upper bound. Well-formed plans do not hit this, and the renderer
    /// soft-asserts when they do, so a deviation here is worth investigating rather than ignoring.
    ArrangeByMayReuse,
    /// `Threshold` builds an extra error arrangement only when its input arrangement is an
    /// imported trace rather than a dataflow-local one. Which flavor arrives is a property of the
    /// rendered bundle, not of the plan.
    ThresholdFlavorUnknown,
    /// A linear join reuses the source arrangement when one is available and no initial closure
    /// intervenes, but whether the arrangement is genuinely present is settled during rendering.
    JoinSourceMayReuse,
}

/// A predicted arrangement count for one LIR node.
///
/// Data and error arrangements are separated because they behave differently in both directions
/// that matter. Only data arrangements scale with the collection, so only `data` may multiply a row
/// bound: charging a reduce for its error traces would inflate its byte bound by half. And error
/// arrangements are only observable when they happen to have allocated, which is an allocator
/// artifact rather than a plan property, so only `data` can be validated against
/// `mz_arrangement_sizes`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Prediction {
    /// Arrangements holding collection data. These carry the memory.
    pub data: usize,
    /// Arrangements holding errors. Empty in a healthy dataflow.
    pub error: usize,
    /// Set when the count is not decidable from the plan alone.
    pub caveat: Option<Caveat>,
}

impl Prediction {
    /// Total arrangements built, whether or not they hold anything.
    pub fn total(&self) -> usize {
        self.data + self.error
    }

    fn exact(data: usize, error: usize) -> Self {
        Prediction {
            data,
            error,
            caveat: None,
        }
    }

    fn caveated(data: usize, error: usize, caveat: Caveat) -> Self {
        Prediction {
            data,
            error,
            caveat: Some(caveat),
        }
    }
}

/// Predicts the arrangement count for every node in `expr`.
///
/// Nodes that build nothing are present with a count of zero, so a caller can distinguish "walked
/// and found none" from "never visited".
pub fn predict_arrangement_counts(expr: &LirRelationExpr) -> BTreeMap<LirId, Prediction> {
    let mut out = BTreeMap::new();
    // Iterative rather than recursive: LIR trees are as deep as the MIR they came from, and this
    // runs on plans that have already survived stack-safe lowering.
    let mut stack = vec![expr];
    while let Some(expr) = stack.pop() {
        out.insert(expr.lir_id, predict_node(&expr.node));
        stack.extend(expr.node.children());
    }
    out
}

/// Predicts the arrangement count for a single node, ignoring its children.
pub fn predict_node(node: &LirRelationNode) -> Prediction {
    match node {
        // Imports an arrangement, never builds one.
        LirRelationNode::Get { .. } => Prediction::default(),

        // Stateless, or state that does not scale with the collection. `Union`'s
        // `consolidate_output` installs a merge batcher rather than a trace, bounded by updates at
        // incomplete timestamps.
        LirRelationNode::Constant { .. }
        | LirRelationNode::Let { .. }
        | LirRelationNode::LetRec { .. }
        | LirRelationNode::Mfp { .. }
        | LirRelationNode::FlatMap { .. }
        | LirRelationNode::Negate { .. }
        | LirRelationNode::Union { .. } => Prediction::default(),

        LirRelationNode::ArrangeBy { forms, .. } => {
            let forms = forms.arranged.len();
            // Nothing to reuse when nothing is built, so a caveat there would be noise.
            if forms == 0 {
                Prediction::default()
            } else {
                Prediction::caveated(forms, forms, Caveat::ArrangeByMayReuse)
            }
        }

        LirRelationNode::Threshold { .. } => {
            Prediction::caveated(1, 1, Caveat::ThresholdFlavorUnknown)
        }

        LirRelationNode::Join { plan, .. } => match plan {
            // Every stage is a `half_join` over a pre-existing trace, and the output is assembled
            // from collections, so not even an error arrangement appears.
            JoinPlan::Delta(_) => Prediction::default(),
            JoinPlan::Linear(plan) => {
                // Each stage arranges its incoming stream. The seed is the exception: it is reused
                // when an arrangement was selected for it and no initial closure forces a rebuild.
                let reuses_source = plan.source_key.is_some() && plan.initial_closure.is_none();
                let data = plan
                    .stage_plans
                    .len()
                    .saturating_sub(usize::from(reuses_source));
                if reuses_source {
                    Prediction::caveated(data, 0, Caveat::JoinSourceMayReuse)
                } else {
                    Prediction::exact(data, 0)
                }
            }
        },

        LirRelationNode::Reduce {
            plan, mfp_after, ..
        } => {
            // The renderer discards an identity MFP before testing it, so an identity MFP that
            // reports `could_error` must not be charged for an error arrangement.
            let mfp_can_error = !mfp_after.is_identity() && mfp_after.could_error();
            let (data, error) = reduce_count(plan, mfp_can_error);
            // `render_reduce_plan` wraps every reduce in a bundle error arrangement. Empirically
            // this one never allocates, so it never appears in `mz_arrangement_sizes`.
            Prediction::exact(data, error + 1)
        }

        LirRelationNode::TopK { top_k_plan, .. } => {
            let (data, error) = top_k_count(top_k_plan);
            Prediction::exact(data, error)
        }
    }
}

/// Data and error arrangements built by a reduce plan, excluding the bundle error arrangement.
fn reduce_count(plan: &ReducePlan, mfp_can_error: bool) -> (usize, usize) {
    match plan {
        // Arrange and reduce, plus an unconditional error reduce.
        ReducePlan::Distinct => (2, 1),

        // As `Distinct`, plus an arrange/reduce pair to pre-distinct each `DISTINCT` aggregate.
        ReducePlan::Accumulable(plan) => (2 + 2 * plan.distinct_aggrs.len(), 1),

        ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(_)) => (2, usize::from(mfp_can_error)),

        ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(plan)) => {
            // Two per level, then a final arrange/reduce pair. The final error reduce fires when
            // no level validated, which happens only when there are no levels at all, or when the
            // trailing MFP can error.
            let validated_in_a_level = !plan.buckets.is_empty();
            let error = usize::from(!validated_in_a_level || mfp_can_error);
            (2 * plan.buckets.len() + 2, error)
        }

        ReducePlan::Basic(BasicPlan::Single(plan)) => {
            // Validation is skipped for a fused unnest-list because that path is already
            // CPU-bound, and a `DISTINCT` aggregate supplies errors from its own demux instead.
            let validating = !plan.fused_unnest_list;
            let distinct = plan.expr.distinct;
            let must_validate = validating && !distinct;
            let error = usize::from(must_validate || mfp_can_error);
            (2 + 2 * usize::from(distinct), error)
        }

        ReducePlan::Basic(BasicPlan::Multiple(aggrs)) => {
            // Only the first aggregate validates: it populates the shared error output, and every
            // later aggregate sees it as already present. Sub-aggregates are rendered with no MFP,
            // so only the collating stage can contribute an MFP error check.
            let mut data = 0;
            let mut error = 0;
            for (index, aggr) in aggrs.iter().enumerate() {
                data += 2 + 2 * usize::from(aggr.distinct);
                error += usize::from(index == 0 && !aggr.distinct);
            }
            (data + 2, error + usize::from(mfp_can_error))
        }
    }
}

/// Data and error arrangements built by a top-k plan.
fn top_k_count(plan: &TopKPlan) -> (usize, usize) {
    match plan {
        // Arrange and reduce, plus a bundle error arrangement that the other variants do not get,
        // because this is the only variant whose group-key arrangement is advertised upward.
        TopKPlan::MonotonicTop1(_) => (2, 1),

        // A single stage: the hierarchy is unnecessary when the input is monotonic.
        TopKPlan::MonotonicTopK(_) => (2, 0),

        TopKPlan::Basic(plan) => {
            // NOTE: the bucket hierarchy is gated on a limit being present. A pure-`OFFSET` top-k
            // renders only the final stage, so charging it for `buckets` would over-count eightfold
            // at the default group size.
            let levels = if plan.limit.is_some() {
                plan.buckets.len()
            } else {
                0
            };
            (2 * levels + 2, 0)
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::AggregateFunc;
    use mz_ore::treat_as_equal::TreatAsEqual;

    use super::*;
    use crate::plan::bucketing_of_expected_group_size;
    use crate::plan::join::JoinClosure;
    use crate::plan::join::delta_join::DeltaJoinPlan;
    use crate::plan::join::linear_join::{LinearJoinPlan, LinearStagePlan};
    use crate::plan::reduce::{
        AccumulablePlan, BucketedPlan, LirAggregateExpr, MonotonicPlan, SingleBasicPlan,
    };
    use crate::plan::scalar::LirScalarExpr;
    use crate::plan::top_k::{BasicTopKPlan, MonotonicTop1Plan, MonotonicTopKPlan};

    fn column() -> LirScalarExpr {
        LirScalarExpr::Column(0, TreatAsEqual(None))
    }

    fn aggregate(distinct: bool) -> LirAggregateExpr {
        LirAggregateExpr {
            func: AggregateFunc::Count,
            expr: column(),
            distinct,
        }
    }

    fn basic_top_k(limit: Option<LirScalarExpr>, buckets: Vec<u64>) -> TopKPlan {
        TopKPlan::Basic(BasicTopKPlan {
            group_key: vec![],
            order_key: vec![],
            limit,
            offset: 0,
            arity: 1,
            buckets,
        })
    }

    fn linear_stage() -> LinearStagePlan {
        // Only `stage_plans.len()` feeds the count, so an identity closure suffices.
        let before = mz_expr::MapFilterProject::<LirScalarExpr>::new(0)
            .into_plan()
            .expect("identity MFP is a valid plan")
            .into_nontemporal()
            .expect("identity MFP is nontemporal");
        LinearStagePlan {
            lookup_relation: 0,
            stream_key: vec![],
            stream_thinning: vec![],
            lookup_key: vec![],
            closure: JoinClosure {
                ready_equivalences: vec![],
                before,
            },
        }
    }

    /// The default expected group size yields seven levels, not eight: the loop stops once
    /// `16^8` exceeds the 4e9 limit. Hard-coding eight would inflate every hierarchical reduce.
    #[mz_ore::test]
    fn default_bucketing_has_seven_levels() {
        assert_eq!(bucketing_of_expected_group_size(None).len(), 7);
    }

    /// A pure-`OFFSET` top-k skips the whole hierarchy, because the renderer gates the bucket loop
    /// on a limit being present. Charging it for its buckets over-counts eightfold by default.
    #[mz_ore::test]
    fn basic_top_k_without_limit_skips_the_hierarchy() {
        let buckets = bucketing_of_expected_group_size(None);
        assert_eq!(top_k_count(&basic_top_k(None, buckets.clone())), (2, 0));
        assert_eq!(top_k_count(&basic_top_k(Some(column()), buckets)), (16, 0));
    }

    #[mz_ore::test]
    fn monotonic_top_k_variants() {
        // Only Top1 advertises its arrangement upward, so only Top1 gets a bundle error trace.
        let top1 = TopKPlan::MonotonicTop1(MonotonicTop1Plan {
            group_key: vec![],
            order_key: vec![],
            arity: 1,
            must_consolidate: false,
        });
        assert_eq!(top_k_count(&top1), (2, 1));

        let top_k = TopKPlan::MonotonicTopK(MonotonicTopKPlan {
            group_key: vec![],
            order_key: vec![],
            limit: Some(column()),
            arity: 1,
            must_consolidate: false,
        });
        assert_eq!(top_k_count(&top_k), (2, 0));
    }

    #[mz_ore::test]
    fn distinct_reduce() {
        assert_eq!(reduce_count(&ReducePlan::Distinct, false), (2, 1));
    }

    #[mz_ore::test]
    fn accumulable_charges_two_per_distinct_aggregate() {
        let plan = |distinct_count: usize| {
            ReducePlan::Accumulable(AccumulablePlan {
                full_aggrs: vec![],
                simple_aggrs: vec![],
                distinct_aggrs: (0..distinct_count).map(|i| (i, aggregate(true))).collect(),
            })
        };
        assert_eq!(reduce_count(&plan(0), false), (2, 1));
        assert_eq!(reduce_count(&plan(2), false), (6, 1));
    }

    #[mz_ore::test]
    fn monotonic_hierarchical_charges_for_a_fallible_mfp() {
        let plan = ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(MonotonicPlan {
            aggr_funcs: vec![],
            must_consolidate: false,
        }));
        assert_eq!(reduce_count(&plan, false), (2, 0));
        assert_eq!(reduce_count(&plan, true), (2, 1));
    }

    #[mz_ore::test]
    fn bucketed_hierarchical_counts_levels() {
        let plan = |buckets: Vec<u64>| {
            ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(BucketedPlan {
                aggr_funcs: vec![],
                buckets,
            }))
        };
        // Seven levels at two each, plus the final pair. No level-independent error trace, because
        // the first level already validated.
        assert_eq!(
            reduce_count(&plan(bucketing_of_expected_group_size(None)), false),
            (16, 0)
        );
        // With no levels nothing validated, so the final error reduce fires.
        assert_eq!(reduce_count(&plan(vec![]), false), (2, 1));
    }

    #[mz_ore::test]
    fn basic_single_validates_unless_distinct_or_fused() {
        let plan = |distinct, fused_unnest_list| {
            ReducePlan::Basic(BasicPlan::Single(SingleBasicPlan {
                expr: aggregate(distinct),
                fused_unnest_list,
            }))
        };
        // Plain: arrange, reduce, and an unconditional validation reduce.
        assert_eq!(reduce_count(&plan(false, false), false), (2, 1));
        // A fused unnest-list skips validation entirely.
        assert_eq!(reduce_count(&plan(false, true), false), (2, 0));
        // A distinct aggregate supplies errors from its own demux, so no validation reduce, but it
        // pays for the pre-distinct arrange/reduce pair.
        assert_eq!(reduce_count(&plan(true, false), false), (4, 0));
    }

    /// Only the first aggregate validates. Later ones see the shared error output already set.
    #[mz_ore::test]
    fn basic_multiple_validates_once() {
        let plan = |aggrs: Vec<LirAggregateExpr>| ReducePlan::Basic(BasicPlan::Multiple(aggrs));
        // Two plain aggregates: (2 + 1) + 2, plus the collating pair.
        assert_eq!(
            reduce_count(&plan(vec![aggregate(false), aggregate(false)]), false),
            (6, 1)
        );
        // A leading distinct aggregate never validates, so nothing else does either.
        assert_eq!(
            reduce_count(&plan(vec![aggregate(true), aggregate(false)]), false),
            (8, 0)
        );
    }

    #[mz_ore::test]
    fn delta_join_builds_nothing() {
        let node = LirRelationNode::Join {
            inputs: vec![],
            plan: JoinPlan::Delta(DeltaJoinPlan { path_plans: vec![] }),
        };
        assert_eq!(predict_node(&node), Prediction::default());
    }

    #[mz_ore::test]
    fn linear_join_charges_per_stage_less_seed_reuse() {
        let join = |source_key| LirRelationNode::Join {
            inputs: vec![],
            plan: JoinPlan::Linear(LinearJoinPlan {
                source_relation: 0,
                source_key,
                initial_closure: None,
                stage_plans: vec![linear_stage(), linear_stage()],
                final_closure: None,
            }),
        };
        // No source arrangement: every stage arranges its own input.
        assert_eq!(predict_node(&join(None)), Prediction::exact(2, 0));
        // A reusable source arrangement saves the seed, but only if it is really there.
        assert_eq!(
            predict_node(&join(Some(vec![column()]))),
            Prediction::caveated(1, 0, Caveat::JoinSourceMayReuse)
        );
    }
}
