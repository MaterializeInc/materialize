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
//! The bound covers steady-state arrangement contents only. It does not model the Spine holding
//! geometric levels during merge, nor uncompacted history within the compaction window, both of
//! which scale the result by a factor this module does not attempt to predict.

use std::collections::BTreeMap;

use mz_repr::{ReprRelationType, max_datum_size};

use crate::plan::arrangement_count::{Caveat, predict_arrangement_counts};
use crate::plan::{LirId, LirRelationExpr};

/// The per-row memory a node's arrangements occupy, where it can be determined.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BytesPerRow {
    /// Data arrangements the node builds. Zero for a node that holds no state.
    pub arrangements: usize,
    /// Widest encoding of one row of this node's output, or `None` if any column is unbounded.
    pub row_width: Option<usize>,
    /// `arrangements * row_width`, or `None` when the width is unknown.
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
) -> BTreeMap<LirId, BytesPerRow> {
    predict_arrangement_counts(expr)
        .into_iter()
        .map(|(lir_id, prediction)| {
            let row_width = node_types.get(&lir_id).and_then(max_row_width);
            // A node that arranges nothing costs nothing, even where the width is unknown.
            let bytes_per_row = if prediction.data == 0 {
                Some(0)
            } else {
                row_width.map(|width| prediction.data * width)
            };
            let entry = BytesPerRow {
                arrangements: prediction.data,
                row_width,
                bytes_per_row,
                caveat: prediction.caveat,
            };
            (lir_id, entry)
        })
        .collect()
}

/// Sums the per-row bound over a whole plan.
///
/// Returns `None` if any node that holds state has an unbounded width, since a total that silently
/// omits such a node reads as a bound when it is not one.
pub fn total_bytes_per_row(
    expr: &LirRelationExpr,
    node_types: &BTreeMap<LirId, ReprRelationType>,
) -> Option<usize> {
    let mut total = 0;
    for entry in bytes_per_row(expr, node_types).values() {
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
}
