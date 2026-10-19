// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Operator rules.
//!
//! Requirements of single operators that rendering asserts or relies on, beyond arities,
//! arrangements, and types. Each function pushes the violations it finds as
//! [`Rule::Operator`] issues.

use mz_expr::{AggregateFunc, LetRecLimit, LocalId};

use crate::plan::ArrangementStrategy;
use crate::plan::join::JoinPlan;
use crate::plan::reduce::{
    BasicPlan, HierarchicalPlan, KeyValPlan, ReducePlan, ReductionType, reduction_type,
};
use crate::plan::top_k::TopKPlan;

use super::Rule;
use super::arity::Issues;

fn push(issues: &mut Issues, message: String) {
    issues.push((Rule::Operator, message));
}

/// `Reduce`: plans without predicates, and a plan variant that fits its aggregates.
pub(super) fn reduce(issues: &mut Issues, key_val_plan: &KeyValPlan, plan: &ReducePlan) {
    // Rendering panics when a key or value plan filters a row out ("Row expected as no
    // predicate was used").
    for (what, mfp) in [
        ("key", &key_val_plan.key_plan),
        ("value", &key_val_plan.val_plan),
    ] {
        if !mfp.predicates.is_empty() {
            push(issues, format!("Reduce {what} plan has a predicate"));
        }
    }
    match plan {
        ReducePlan::Distinct => {}
        // The arity rules check that the simple and distinct lists copy `full_aggrs`.
        ReducePlan::Accumulable(p) => aggregates(
            issues,
            "accumulable",
            p.full_aggrs.iter().map(|a| &a.func),
            ReductionType::Accumulable,
        ),
        ReducePlan::Hierarchical(h) => {
            let kind = match h {
                HierarchicalPlan::Monotonic(_) => "monotonic",
                HierarchicalPlan::Bucketed(_) => "bucketed",
            };
            if h.aggr_funcs().is_empty() {
                push(issues, format!("{kind} Reduce plan has no aggregates"));
            }
            aggregates(
                issues,
                kind,
                h.aggr_funcs().iter(),
                ReductionType::Hierarchical,
            );
            if let HierarchicalPlan::Bucketed(b) = h {
                // Only the bucket stages validate their input, and rendering expects one to
                // have done so. Each stage takes a hash modulo its bucket.
                if b.buckets.is_empty() {
                    push(issues, "bucketed Reduce plan has no buckets".to_string());
                }
                if let Some(i) = b.buckets.iter().position(|b| *b == 0) {
                    push(issues, format!("Reduce bucket {i} is zero"));
                }
            }
        }
        ReducePlan::Basic(BasicPlan::Single(s)) => {
            aggregates(
                issues,
                "basic",
                std::iter::once(&s.expr.func),
                ReductionType::Basic,
            );
            // `AggregateFunc::eval_with_unnest_list` asserts that the aggregate can fuse.
            if s.fused_unnest_list && !s.expr.func.can_fuse_with_unnest_list() {
                push(
                    issues,
                    format!(
                        "Reduce aggregate {} cannot fuse with an unnested list",
                        s.expr.func.name()
                    ),
                );
            }
        }
        ReducePlan::Basic(BasicPlan::Multiple(list)) => {
            // Rendering soft-panics on fewer than two, and panics on none, because then no
            // aggregate validates the input.
            if list.len() < 2 {
                push(
                    issues,
                    format!(
                        "multiple basic Reduce plan has {} aggregates, but at least 2 are required",
                        list.len()
                    ),
                );
            }
            aggregates(
                issues,
                "basic",
                list.iter().map(|a| &a.func),
                ReductionType::Basic,
            );
        }
    }
}

/// Checks that every aggregate reduces as `expected`, the type its plan variant renders.
///
/// Rendering dispatches on the variant and panics on aggregates of another type, for example
/// in `finalize_accum` for an accumulable plan, and in `get_monoid` for a hierarchical one.
fn aggregates<'a>(
    issues: &mut Issues,
    kind: &str,
    funcs: impl Iterator<Item = &'a AggregateFunc>,
    expected: ReductionType,
) {
    for func in funcs {
        let actual = reduction_type(func);
        if actual != expected {
            // `get_monoid` has a monoid for exactly the hierarchical aggregates.
            let why = match expected {
                ReductionType::Hierarchical => "which has no monoid".to_string(),
                _ => format!("which reduces as {actual:?}"),
            };
            push(
                issues,
                format!("{kind} Reduce plan has aggregate {}, {why}", func.name()),
            );
        }
    }
}

/// `TopK`: positive buckets, and consolidation of temporally bucketed monotonic input.
pub(super) fn top_k(issues: &mut Issues, plan: &TopKPlan, strategy: &ArrangementStrategy) {
    let must_consolidate = match plan {
        TopKPlan::Basic(p) => {
            // Each stage takes a hash modulo its bucket.
            if let Some(i) = p.buckets.iter().position(|b| *b == 0) {
                push(issues, format!("TopK bucket {i} is zero"));
            }
            return;
        }
        TopKPlan::MonotonicTop1(p) => p.must_consolidate,
        TopKPlan::MonotonicTopK(p) => p.must_consolidate,
    };
    // A soft assertion in rendering, which panics where soft assertions are enabled.
    if matches!(strategy, ArrangementStrategy::TemporalBucketing) && !must_consolidate {
        push(
            issues,
            "monotonic TopK with temporal bucketing must consolidate its input".to_string(),
        );
    }
}

/// `Join`: a delta path reads its source raw only from relation 0.
pub(super) fn join(issues: &mut Issues, plan: &JoinPlan) {
    let JoinPlan::Delta(plan) = plan else {
        return;
    };
    // `build_update_stream_stream` asserts it, because only the first relation's path keeps
    // the updates at the as-of, which a raw collection has no times to filter by.
    for path in &plan.path_plans {
        if path.source_key.is_none() && path.source_relation != 0 {
            push(
                issues,
                format!(
                    "delta path from relation {} reads its source raw, which only the path from relation 0 may",
                    path.source_relation
                ),
            );
        }
    }
}

/// `LetRec`: every binding has an iteration limit.
///
/// Rendering iterates a binding without a limit until it converges, which a recursion that
/// does not converge never does, and its workers stay occupied.
pub(super) fn let_rec_limits(issues: &mut Issues, ids: &[LocalId], limits: &[Option<LetRecLimit>]) {
    // A length mismatch has been reported, so ids without a limit are skipped.
    for (i, id) in ids.iter().enumerate() {
        if matches!(limits.get(i), Some(None)) {
            push(
                issues,
                format!("recursive binding {id} has no iteration limit"),
            );
        }
    }
}
