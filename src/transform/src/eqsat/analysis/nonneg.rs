// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Non-negativity analysis: tracks whether a relation's multiplicities are
//! everywhere non-negative (i.e., it has a `Negate`-free representative).

use std::collections::BTreeMap;

use crate::eqsat::core::{Analysis, Id};
use crate::eqsat::egraph::{CNode, CombinedLang, ENode};

use super::RelCtx;

/// Non-negativity: a relation has non-negative multiplicities everywhere.
/// Conservatively, it has *some* `Negate`-free representative.
///
/// `locals` carries facts for `LocalGet` references proven by the Rel-level
/// recursion fixpoint (see [`LocalFacts`]); within a fragment a recursive
/// reference is otherwise unknown.
///
/// [`LocalFacts`]: super::recursion::LocalFacts
#[derive(Default)]
pub struct NonNeg {
    pub locals: BTreeMap<usize, bool>,
}

impl Analysis<CombinedLang> for NonNeg {
    type Domain = bool;
    type Ctx<'a> = RelCtx<'a>;

    fn bottom(&self) -> bool {
        false
    }

    fn make(&self, node: &CNode, get: &dyn Fn(Id) -> bool, _ctx: Self::Ctx<'_>) -> bool {
        // Scalar classes are inert (no relational fact); they sit at bottom.
        let CNode::Rel(node) = node else {
            return self.bottom();
        };
        match node {
            ENode::Negate { .. } => false,
            // A recursive reference: use the Rel-level recursion fact if we have
            // one, else assume nothing (the authoritative fact is the
            // greatest-fixpoint `rel_non_negative`).
            ENode::LocalGet { id, .. } => self.locals.get(id).copied().unwrap_or(false),
            // Leaves (Get/Constant) have no relational children, so `all` is
            // vacuously true; every other operator preserves non-negativity.
            // Only relational inputs matter (scalar children are inert).
            other => other.relational_children().iter().all(|c| get(*c)),
        }
    }

    fn merge(&self, a: bool, b: bool) -> bool {
        a || b
    }
}
