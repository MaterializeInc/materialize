// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Static upper bound on the arrangement memory a plan will hold.
//!
//! The bound factors into three terms, `arrangements * rows * width`, which fail independently and
//! are therefore worth keeping apart:
//!
//! * `arrangements` comes from [`crate::plan::arrangement_count`] and is exact. It needs no
//!   statistics and has been validated against `mz_arrangement_sizes`.
//! * `width` comes from the per-node output type recorded during lowering, and is exact for
//!   fixed-width columns and unknown otherwise.
//! * `rows` is the only term needing statistics, and is supplied by the caller.
//!
//! This module owns the first two. Keeping `rows` out means the per-row part of the bound can be
//! measured and calibrated on its own, before any cardinality estimate is trusted.
//!
//! A batch stores its keys, then its values, then the `(time, diff)` updates, with an offset range
//! linking each layer to the next. So an update costs the row it holds plus a fixed overhead, and
//! the worst case for the ranges is one value per key and one update per value.
//!
//! The bound covers the logical contents of steady-state batches. It does not model the Spine
//! holding geometric levels during merge, uncompacted history within the compaction window, or the
//! allocator retaining capacity a consolidated arrangement no longer uses. Measurement shows the
//! last of these can exceed the bound for arrangements that ingested far more updates than they
//! now hold, so this is a bound on content and not on resident bytes.

use std::collections::BTreeMap;

use mz_repr::{ReprRelationType, max_datum_size};

use crate::plan::arrangement_count::{Caveat, predict_arrangement_counts};
use crate::plan::reduce::ReducePlan;
use crate::plan::{LirId, LirRelationExpr, LirRelationNode, NodeBounds};

/// Bytes a batch spends on the difference accumulated for one update.
pub const DIFF_BYTES: usize = 8;

/// Widest accumulator an accumulable reduce can carry in the difference position.
///
/// An accumulable reduce does not accumulate a plain difference. It threads one accumulator per
/// aggregate through the difference, and the widest of those holds an arbitrary-precision decimal.
/// The renderer static-asserts that its accumulator fits here, since this crate cannot name the
/// type.
pub const MAX_ACCUM_BYTES: usize = 112;

/// Bytes the accumulator vector's own allocation costs, once per update.
pub const ACCUM_VEC_HEADER_BYTES: usize = 3 * 8;

/// Bytes a batch spends on the timestamp of one update, outside a recursive scope.
pub const TIMESTAMP_BYTES: usize = 8;

/// Bytes the point stamp's own allocation costs, once per update inside a recursive scope.
///
/// Inside a `LetRec` the timestamp becomes a product of the outer timestamp and a point stamp.
/// The point stamp holds its coordinates in a heap allocation, so an update pays for the pointer,
/// length and capacity as well as the coordinates.
pub const POINT_STAMP_HEADER_BYTES: usize = 3 * 8;

/// Additional timestamp bytes per level of recursive nesting, one coordinate each.
pub const RECURSION_TIMESTAMP_BYTES_PER_LEVEL: usize = 8;

/// Bytes a batch spends on the offset delimiting one key's range of values.
pub const KEY_RANGE_BYTES: usize = 8;

/// Bytes a batch spends on the offset delimiting one value's range of updates.
pub const VALUE_RANGE_BYTES: usize = 8;

/// Per-update batch overhead outside the row itself, at recursion depth `depth`.
///
/// A batch stores keys, then values, then updates, with an offset range linking each layer to the
/// next. Charging a full range per update is the worst case, reached when every key has exactly
/// one value and every value exactly one update.
pub fn overhead_bytes(depth: usize) -> usize {
    overhead_bytes_with_diff(depth, DIFF_BYTES)
}

/// As [`overhead_bytes`], for a batch whose difference is wider than a plain `Diff`.
pub fn overhead_bytes_with_diff(depth: usize, diff_bytes: usize) -> usize {
    let timestamp = if depth == 0 {
        TIMESTAMP_BYTES
    } else {
        TIMESTAMP_BYTES + POINT_STAMP_HEADER_BYTES + depth * RECURSION_TIMESTAMP_BYTES_PER_LEVEL
    };
    diff_bytes + timestamp + KEY_RANGE_BYTES + VALUE_RANGE_BYTES
}

/// Width of the difference an update of `node` carries.
///
/// Only an accumulable reduce departs from a plain `Diff`. Its accumulators are charged to every
/// arrangement the node builds, which over-charges the output arrangement, whose difference is an
/// ordinary one. That is the safe direction.
fn diff_bytes(node: &LirRelationNode) -> usize {
    match node {
        LirRelationNode::Reduce {
            plan: ReducePlan::Accumulable(plan),
            ..
        } => ACCUM_VEC_HEADER_BYTES + plan.full_aggrs.len() * MAX_ACCUM_BYTES + DIFF_BYTES,
        _ => DIFF_BYTES,
    }
}

/// The per-row memory a node's arrangements occupy, where it can be determined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BytesPerRow {
    /// Data arrangements the node builds. Zero for a node that holds no state.
    pub arrangements: usize,
    /// Widest encoding of one row of this node's output, or `None` if any column is unbounded.
    ///
    /// A batch splits the row across its key and value layers, so the two together are never
    /// wider than the row.
    pub row_width: Option<usize>,
    /// Per-update batch overhead, which grows with recursive nesting.
    pub overhead: usize,
    /// `arrangements * (row_width + overhead)`, or `None` when the width is unknown.
    ///
    /// Zero is a real answer, meaning the node holds nothing. `None` means the node holds
    /// something whose size cannot be bounded, which is the case a caller must not round down.
    pub bytes_per_row: Option<usize>,
    /// Set when the arrangement count itself is not decidable from the plan.
    pub caveat: Option<Caveat>,
}

/// Widest encoding of one row of `typ`, or `None` if any column is unbounded.
///
/// A nullable column is not wider than a non-nullable one: `Datum::Null` occupies a single byte,
/// which every type's bound already exceeds.
pub fn max_row_width(typ: &ReprRelationType) -> Option<usize> {
    let mut total = 0;
    for column in &typ.column_types {
        total += max_datum_size(&column.scalar_type)?;
    }
    Some(total)
}

/// Computes the per-row memory bound for every node in `expr`.
///
/// `node_types` is the map returned by
/// [`crate::plan::LirRelationExpr::finalize_dataflow_with_node_types`]. A node missing from it
/// yields an unknown width rather than being skipped, so the result covers the whole plan and a
/// caller can see what it does not know.
pub fn bytes_per_row(
    expr: &LirRelationExpr,
    node_types: &BTreeMap<LirId, ReprRelationType>,
    node_bounds: &BTreeMap<LirId, NodeBounds>,
) -> BTreeMap<LirId, BytesPerRow> {
    let counts = predict_arrangement_counts(expr);
    let depths = recursion_depths(expr);
    let nodes = nodes_by_id(expr);

    counts
        .into_iter()
        .map(|(lir_id, prediction)| {
            // Caller-supplied widths first: they know the declared SQL types, which bound a
            // `char(n)` column that the repr type reports as an unbounded `String`. They are
            // never looser, since the analysis producing them also consults the repr type.
            let row_width = node_bounds
                .get(&lir_id)
                .and_then(|bounds| bounds.column_widths.as_ref())
                .map_or_else(
                    || node_types.get(&lir_id).and_then(max_row_width),
                    |widths| widths.iter().copied().sum::<Option<usize>>(),
                );
            let depth = depths.get(&lir_id).copied().unwrap_or(0);
            let overhead = overhead_bytes_with_diff(
                depth,
                nodes
                    .get(&lir_id)
                    .map_or(DIFF_BYTES, |node| diff_bytes(node)),
            );
            // A node that arranges nothing costs nothing, even where the width is unknown.
            let bytes_per_row = if prediction.data == 0 {
                Some(0)
            } else {
                row_width.map(|width| prediction.data * (width + overhead))
            };
            let entry = BytesPerRow {
                arrangements: prediction.data,
                row_width,
                overhead,
                bytes_per_row,
                caveat: prediction.caveat,
            };
            (lir_id, entry)
        })
        .collect()
}

/// A short name for a node, for reporting the bound.
///
/// Deliberately coarser than the `EXPLAIN PHYSICAL PLAN` rendering: this names the operator kind
/// that determines the arrangement count, not the expressions it evaluates.
pub fn node_label(node: &LirRelationNode) -> String {
    use crate::plan::join::JoinPlan;
    use crate::plan::reduce::{BasicPlan, HierarchicalPlan};
    use crate::plan::top_k::TopKPlan;

    match node {
        LirRelationNode::Constant { .. } => "Constant".into(),
        LirRelationNode::Get { .. } => "Get".into(),
        LirRelationNode::Let { .. } => "Let".into(),
        LirRelationNode::LetRec { .. } => "LetRec".into(),
        LirRelationNode::Mfp { .. } => "Map/Filter/Project".into(),
        LirRelationNode::FlatMap { .. } => "FlatMap".into(),
        LirRelationNode::Negate { .. } => "Negate".into(),
        LirRelationNode::Union { .. } => "Union".into(),
        LirRelationNode::Threshold { .. } => "Threshold".into(),
        LirRelationNode::ArrangeBy { forms, .. } => {
            format!("ArrangeBy ({} forms)", forms.arranged.len())
        }
        LirRelationNode::Join { plan, .. } => match plan {
            JoinPlan::Delta(_) => "Delta Join".into(),
            JoinPlan::Linear(plan) => {
                format!("Differential Join ({} stages)", plan.stage_plans.len())
            }
        },
        LirRelationNode::Reduce { plan, .. } => match plan {
            ReducePlan::Distinct => "Reduce (distinct)".into(),
            ReducePlan::Accumulable(plan) => {
                format!("Reduce (accumulable, {} aggregates)", plan.full_aggrs.len())
            }
            ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(_)) => {
                "Reduce (hierarchical, monotonic)".into()
            }
            ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(plan)) => {
                format!("Reduce (hierarchical, {} levels)", plan.buckets.len())
            }
            ReducePlan::Basic(BasicPlan::Single(_)) => "Reduce (basic)".into(),
            ReducePlan::Basic(BasicPlan::Multiple(aggrs)) => {
                format!("Reduce (basic, {} aggregates)", aggrs.len())
            }
        },
        LirRelationNode::TopK { top_k_plan, .. } => match top_k_plan {
            TopKPlan::MonotonicTop1(_) => "TopK (monotonic top1)".into(),
            TopKPlan::MonotonicTopK(_) => "TopK (monotonic)".into(),
            TopKPlan::Basic(plan) => {
                let levels = if plan.limit.is_some() {
                    plan.buckets.len()
                } else {
                    0
                };
                format!("TopK ({levels} levels)")
            }
        },
    }
}

/// Indexes the plan's nodes so per-node properties can be looked up alongside the counts.
pub fn nodes_by_id(expr: &LirRelationExpr) -> BTreeMap<LirId, &LirRelationNode> {
    let mut out = BTreeMap::new();
    let mut stack = vec![expr];
    while let Some(expr) = stack.pop() {
        out.insert(expr.lir_id, &expr.node);
        stack.extend(expr.node.children());
    }
    out
}

/// How many `LetRec` scopes enclose each node.
fn recursion_depths(expr: &LirRelationExpr) -> BTreeMap<LirId, usize> {
    let mut out = BTreeMap::new();
    let mut stack = vec![(expr, 0usize)];
    while let Some((expr, depth)) = stack.pop() {
        out.insert(expr.lir_id, depth);
        // Only a `LetRec` opens a new timestamp coordinate; its whole subtree sits inside it.
        let child_depth = match &expr.node {
            LirRelationNode::LetRec { .. } => depth + 1,
            _ => depth,
        };
        stack.extend(expr.node.children().map(|child| (child, child_depth)));
    }
    out
}

/// Sums the per-row bound over a whole plan.
///
/// Returns `None` if any node that holds state has an unbounded width, since a total that silently
/// omits such a node reads as a bound when it is not one.
pub fn total_bytes_per_row(
    expr: &LirRelationExpr,
    node_types: &BTreeMap<LirId, ReprRelationType>,
    node_bounds: &BTreeMap<LirId, NodeBounds>,
) -> Option<usize> {
    let mut total = 0;
    for entry in bytes_per_row(expr, node_types, node_bounds).values() {
        total += entry.bytes_per_row?;
    }
    Some(total)
}

#[cfg(test)]
mod tests {
    use mz_repr::{ReprColumnType, ReprScalarType};

    use super::*;

    fn typ(scalar_types: Vec<ReprScalarType>) -> ReprRelationType {
        ReprRelationType::new(
            scalar_types
                .into_iter()
                .map(|scalar_type| ReprColumnType {
                    scalar_type,
                    nullable: false,
                })
                .collect(),
        )
    }

    #[mz_ore::test]
    fn width_sums_bounded_columns() {
        // Int64 is 1 + 8, Bool is 1.
        let bounded = typ(vec![ReprScalarType::Int64, ReprScalarType::Bool]);
        assert_eq!(max_row_width(&bounded), Some(10));
    }

    /// One unbounded column makes the whole row unbounded. Rounding it down to the bounded
    /// columns' width would report a bound that does not hold.
    #[mz_ore::test]
    fn one_unbounded_column_makes_the_row_unbounded() {
        let unbounded = typ(vec![ReprScalarType::Int64, ReprScalarType::String]);
        assert_eq!(max_row_width(&unbounded), None);
    }

    #[mz_ore::test]
    fn an_empty_row_is_bounded_at_zero() {
        assert_eq!(max_row_width(&typ(vec![])), Some(0));
    }

    /// Outside recursion an update pays diff, timestamp, and one range per layer.
    #[mz_ore::test]
    fn overhead_outside_recursion() {
        assert_eq!(overhead_bytes(0), 8 + 8 + 8 + 8);
    }

    /// Inside a `LetRec` the timestamp gains a point stamp, which costs its own allocation plus
    /// one coordinate per enclosing level.
    #[mz_ore::test]
    fn overhead_grows_with_recursive_nesting() {
        assert_eq!(overhead_bytes(1), 8 + (8 + 24 + 8) + 8 + 8);
        assert_eq!(
            overhead_bytes(2) - overhead_bytes(1),
            RECURSION_TIMESTAMP_BYTES_PER_LEVEL
        );
        // The point stamp allocation is paid once, not once per level.
        assert!(overhead_bytes(1) - overhead_bytes(0) > RECURSION_TIMESTAMP_BYTES_PER_LEVEL);
    }

    /// An accumulable reduce threads accumulators through the difference, so its updates are far
    /// wider than a plain `Diff` suggests. Measured at 274 bytes per record for two aggregates.
    #[mz_ore::test]
    fn accumulable_charges_for_its_accumulators() {
        let two_aggregates = 24 + 2 * MAX_ACCUM_BYTES + 8;
        let bound = 5 + overhead_bytes_with_diff(0, two_aggregates);
        assert!(
            bound >= 274,
            "bound {bound} below the 274 bytes per record measured for a two-aggregate reduce"
        );
        // A plain difference would badly under-count it.
        assert!(5 + overhead_bytes(0) < 274);
    }

    /// Measured against a live insert-only workload: every bound here exceeded the observed
    /// bytes per record. Pinned so a change to the batch model has to confront the evidence.
    #[mz_ore::test]
    fn bound_exceeds_measured_bytes_per_record() {
        // (row width, recursion depth, observed bytes per record)
        let measured = [
            (10, 0, 10.3), // index on (int4, int4), keyed by the first
            (5, 0, 5.2),   // join arrangement whose value is empty after thinning
            (10, 0, 11.6), // join arrangement over two int4 columns
            (23, 0, 17.6), // accumulable reduce output, int4 key and two int8 aggregates
            (5, 1, 52.7),  // distinct inside a WITH MUTUALLY RECURSIVE
            (10, 1, 52.7), // arrangement on a computed key inside recursion
        ];
        for (row_width, depth, observed) in measured {
            let bound = row_width + overhead_bytes(depth);
            assert!(
                f64::from(u32::try_from(bound).unwrap()) >= observed,
                "bound {bound} below observed {observed} for width {row_width} at depth {depth}"
            );
        }
    }
}
