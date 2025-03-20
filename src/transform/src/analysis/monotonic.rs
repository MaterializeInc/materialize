// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Analysis that determines the logical monotonicity of an expression.
//!
//! Expressions are logically monotonic if their inputs are monotonic, and they
//! do not introduce any retractions.

use std::collections::BTreeSet;

use mz_expr::{Id, MirRelationExpr};
use mz_repr::GlobalId;

use crate::analysis::common_lattice::BoolLattice;
use crate::analysis::{Analysis, Derived, Lattice};

/// Determines the logical monotonicity of an expression.
#[derive(Debug)]
pub struct Monotonic {
    global_monotonic_ids: BTreeSet<GlobalId>,
}

impl Monotonic {
    /// A monotonicity analyser provided with the set of Global IDs that are known to be monotonic.
    pub fn new(global_monotonic_ids: BTreeSet<GlobalId>) -> Self {
        Self {
            global_monotonic_ids,
        }
    }

    #[inline]
    fn has_monotonic_children(
        expr: &MirRelationExpr,
        index: usize,
        results: &[bool],
        depends: &Derived,
    ) -> bool {
        depends
            .children_of_rev(index, expr.children().count())
            .fold(true, |acc, child| acc && results[child])
    }
}

impl Analysis for Monotonic {
    type Value = bool;

    fn derive(
        &self,
        expr: &MirRelationExpr,
        index: usize,
        results: &[Self::Value],
        depends: &Derived,
    ) -> Self::Value {
        use Id::*;
        match expr {
            MirRelationExpr::Get { id: Global(id), .. } => self.global_monotonic_ids.contains(id),
            MirRelationExpr::Get { id: Local(id), .. } => {
                let index = *depends
                    .bindings()
                    .get(id)
                    .expect("Dependency info not found");
                *results.get(index).unwrap_or(&false)
            }
            // Monotonic iff their input is.
            MirRelationExpr::Project { .. }
            | MirRelationExpr::Map { .. }
            | MirRelationExpr::ArrangeBy { .. }
            | MirRelationExpr::Threshold { .. }
            | MirRelationExpr::Let { .. }
            | MirRelationExpr::LetRec { .. } => results[index - 1],
            // Monotonic iff all inputs are.
            MirRelationExpr::Union { .. } | MirRelationExpr::Join { .. } => {
                Self::has_monotonic_children(expr, index, results, depends)
            }
            // Any set limit or offset can result in retractions when input data arrive.
            // If neither limit nor offset are set, the TopK stage will eventually be optimized out.
            MirRelationExpr::TopK { .. } => false,
            MirRelationExpr::Negate { .. } => false,
            MirRelationExpr::Filter { predicates, .. } => {
                let is_monotonic = results[index - 1];
                // Temporal predicates can introduce non-monotonicity, as they
                // can result in the future removal of records.
                // TODO: this could be improved to only restrict if upper bounds
                // are present, as temporal lower bounds only delay introduction.
                is_monotonic && !predicates.iter().any(|p| p.contains_temporal())
            }
            MirRelationExpr::Reduce { aggregates, .. } => {
                // Reduce is monotonic iff its input is, and it is a "distinct"
                // with no aggregate values; otherwise it may need to retract.
                results[index - 1] && aggregates.is_empty()
            }
            MirRelationExpr::FlatMap { func, .. } => {
                results[index - 1] && func.preserves_monotonicity()
            }
            MirRelationExpr::Constant { rows: Ok(rows), .. } => {
                rows.iter().all(|(_, diff)| diff > &0)
            }
            // TODO: database-issues#8337 (Investigate if constant expressions with error rows can be marked monotonic)
            MirRelationExpr::Constant { rows: Err(_), .. } => false,
        }
    }

    fn lattice() -> Option<Box<dyn Lattice<Self::Value>>> {
        Some(Box::new(BoolLattice))
    }
}
