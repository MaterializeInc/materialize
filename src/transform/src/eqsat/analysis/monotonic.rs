// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Monotonicity analysis: tracks whether a relation is *insert-only* (its
//! multiplicities never decrease as inputs grow).

use std::collections::BTreeMap;

use crate::eqsat::core::{Analysis, Id};
use crate::eqsat::egraph::{CNode, CombinedLang, ENode};

use super::RelCtx;

/// Monotonicity: a relation is *insert-only* — its multiplicities never
/// decrease as inputs grow (no retractions). Conservatively, it is built from
/// monotone leaves (base `Get`s and `Constant`s, assumed insert-only) using
/// operators that preserve monotonicity.
///
/// This is genuinely sharper *and* coarser than [`NonNeg`] in different places:
/// `Threshold` and `Negate` are handled the same way, but a `Reduce` breaks
/// monotonicity (a group's aggregate can move both up and down under updates)
/// while it preserves non-negativity. So `Monotonic` is a distinct analysis,
/// not a re-skin of `NonNeg` — even though, on this static relational subset,
/// `monotonic ⟹ non_negative`, so it does not by itself unlock a rewrite that
/// `non_negative` cannot (its real consumers are *physical* monotonic-rendering
/// passes such as `TopK`, which this IR does not model). It is wired in as a
/// condition so those passes are a drop-in away.
///
/// [`NonNeg`]: super::nonneg::NonNeg
#[derive(Default)]
pub struct Monotonic {
    pub locals: BTreeMap<usize, bool>,
}

impl Analysis<CombinedLang> for Monotonic {
    type Domain = bool;
    type Ctx<'a> = RelCtx<'a>;

    fn bottom(&self) -> bool {
        false
    }

    fn make(&self, node: &CNode, get: &dyn Fn(Id) -> bool, _ctx: Self::Ctx<'_>) -> bool {
        let CNode::Rel(node) = node else {
            return self.bottom();
        };
        match node {
            // A retraction (Negate), an aggregate that can move down (Reduce),
            // or a top-k whose output a higher-ranked row can evict (TopK) is
            // not insert-only.
            ENode::Negate { .. } | ENode::Reduce { .. } | ENode::TopK { .. } => false,
            // A recursive reference: use the Rel-level recursion fact if proven,
            // else conservative (the cross-binding fact comes from
            // `rel_monotonic`).
            ENode::LocalGet { id, .. } => self.locals.get(id).copied().unwrap_or(false),
            // Base collections and constants are taken to be insert-only.
            ENode::Constant { .. } | ENode::Get { .. } => true,
            // Map/Filter/Project/Threshold/Union/Join preserve monotonicity.
            // Only relational inputs matter (scalar children are inert).
            other => other.relational_children().iter().all(|c| get(*c)),
        }
    }

    fn merge(&self, a: bool, b: bool) -> bool {
        a || b
    }
}
