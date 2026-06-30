// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

#![allow(dead_code)] // Trait methods are resolved to concrete types by generated rules; a few methods (e.g. analysis-gated conds on ColoredView) remain conditionally-used.

//! `MatchGraph` / `ApplyGraph` abstraction layer over the base e-graph.
//!
//! Defines the two traits that generated rule functions call, and implements
//! them for the base e-graph layer (`BaseView` / `EGraph`). The colored
//! implementation arrives in SP4d T5.

use mz_repr::ReprColumnType;

use crate::eqsat::core::Id;
use crate::eqsat::ir::EScalar;
use crate::eqsat::matcher::Payload;

use super::{EGraph, ENode, Index, Sym};

// Re-export Analyses so `use super::*` in the tests block can resolve it.
pub(crate) use super::Analyses;

/// Read/match surface the generated `find` functions call. Implemented
/// byte-identically by the base layer (`BaseView`) and color-aware by
/// `ColoredView` (SP4d T5).
pub(crate) trait MatchGraph {
    fn rel_class_ids(&self) -> Vec<Id>;
    fn rel_class_nodes(&self, id: Id) -> Vec<ENode>;
    fn nodes_by_sym(&self, sym: Sym) -> Vec<(Id, ENode)>;
    fn arity(&self, id: Id) -> usize;
    /// Resolve a scalar id to its `EScalar` (base cache or colored delta cache).
    fn escalar(&self, id: Id) -> EScalar;
    // Color-exact conditions (graph/payload/arity only):
    fn cond_uses_only_input(&self, p: &Payload, rel: Id) -> bool;
    fn cond_cols_in_range(&self, p: &Payload, lo: i64, hi: i64) -> bool;
    fn cond_all_columns(&self, p: &Payload) -> bool;
    fn cond_any_false(&self, p: &Payload) -> bool;
    fn cond_no_false(&self, p: &Payload) -> bool;
    fn cond_no_error(&self, p: &Payload) -> bool;
    fn cond_all_true(&self, p: &Payload) -> bool;
    fn cond_identity_projection(&self, p: &Payload, rel: Id) -> bool;
    fn cond_is_rel_empty(&self, id: Id) -> bool;
    fn cond_not_rel_empty(&self, id: Id) -> bool;
    fn cond_has_three_or_more_inputs(&self, root: Id) -> bool;
    fn cond_is_binary_join(&self, root: Id) -> bool;
    fn cond_join_is_cyclic(&self, root: Id) -> bool;
    fn cond_has_inner_equiv(&self, p: &Payload, boundary: i64) -> bool;
    fn cond_reads_indexed_global(&self, id: Id) -> bool;
    fn cond_produces_key(&self, rel: Id, key: &Payload) -> bool;
    // Analysis-gated conditions: present for uniform codegen, NEVER reached for
    // colored rules (enforced at build time, Task 4). Base delegates; colored
    // returns false (sound: the rule simply does not fire).
    fn cond_non_negative(&self, id: Id) -> bool;
    fn cond_monotonic(&self, id: Id) -> bool;
    fn cond_is_unique_key(&self, p: &Payload, id: Id) -> bool;
}

/// Write surface the generated `apply` functions call.
pub(crate) trait ApplyGraph {
    fn intern_scalar(&mut self, e: &EScalar) -> Id;
    fn arity_of_binding(&self, id: Id) -> usize;
    fn escalar(&self, id: Id) -> EScalar;
    fn column_types(&self, id: Id) -> Option<Vec<ReprColumnType>>;
}

/// The base read-side view: bundles the e-graph with its prebuilt sym index
/// and the per-round analysis results.
pub(crate) struct BaseView<'a> {
    pub eg: &'a EGraph,
    pub index: &'a Index,
    pub an: &'a Analyses,
}

impl<'a> MatchGraph for BaseView<'a> {
    fn rel_class_ids(&self) -> Vec<Id> {
        self.eg.rel_class_ids()
    }

    fn rel_class_nodes(&self, id: Id) -> Vec<ENode> {
        self.eg.rel_class_nodes(id).into_iter().cloned().collect()
    }

    fn nodes_by_sym(&self, sym: Sym) -> Vec<(Id, ENode)> {
        self.index.get(&sym).cloned().unwrap_or_default()
    }

    fn arity(&self, id: Id) -> usize {
        self.eg.arity(id)
    }

    fn escalar(&self, id: Id) -> EScalar {
        self.eg.data().escalar(id).clone()
    }

    fn cond_uses_only_input(&self, p: &Payload, rel: Id) -> bool {
        super::cond_uses_only_input(self.eg.data(), p, self.eg.arity(rel))
    }

    fn cond_cols_in_range(&self, p: &Payload, lo: i64, hi: i64) -> bool {
        super::cond_cols_in_range(self.eg.data(), p, lo, hi)
    }

    fn cond_all_columns(&self, p: &Payload) -> bool {
        super::cond_all_columns(self.eg.data(), p)
    }

    fn cond_any_false(&self, p: &Payload) -> bool {
        super::cond_any_false(self.eg.data(), p)
    }

    fn cond_no_false(&self, p: &Payload) -> bool {
        super::cond_no_false(self.eg.data(), p)
    }

    fn cond_no_error(&self, p: &Payload) -> bool {
        super::cond_no_error(self.eg.data(), p)
    }

    fn cond_all_true(&self, p: &Payload) -> bool {
        super::cond_all_true(self.eg.data(), p)
    }

    fn cond_identity_projection(&self, p: &Payload, rel: Id) -> bool {
        super::cond_identity_projection(p, self.eg.arity(rel))
    }

    fn cond_is_rel_empty(&self, id: Id) -> bool {
        self.eg.cond_is_rel_empty(id)
    }

    fn cond_not_rel_empty(&self, id: Id) -> bool {
        self.eg.cond_not_rel_empty(id)
    }

    fn cond_has_three_or_more_inputs(&self, root: Id) -> bool {
        self.eg.cond_has_three_or_more_inputs(root)
    }

    fn cond_is_binary_join(&self, root: Id) -> bool {
        self.eg.cond_is_binary_join(root)
    }

    fn cond_join_is_cyclic(&self, root: Id) -> bool {
        self.eg.cond_join_is_cyclic(root)
    }

    fn cond_has_inner_equiv(&self, p: &Payload, boundary: i64) -> bool {
        self.eg.cond_has_inner_equiv(p, boundary)
    }

    fn cond_reads_indexed_global(&self, id: Id) -> bool {
        self.eg.cond_reads_indexed_global(id)
    }

    fn cond_produces_key(&self, rel: Id, key: &Payload) -> bool {
        self.eg.cond_produces_key(self.an, rel, key)
    }

    fn cond_non_negative(&self, id: Id) -> bool {
        self.eg.cond_non_negative(self.an, id)
    }

    fn cond_monotonic(&self, id: Id) -> bool {
        self.eg.cond_monotonic(self.an, id)
    }

    fn cond_is_unique_key(&self, p: &Payload, id: Id) -> bool {
        self.eg.cond_is_unique_key(self.an, p, id)
    }
}

impl ApplyGraph for EGraph {
    fn intern_scalar(&mut self, e: &EScalar) -> Id {
        EGraph::intern_scalar(self, e)
    }

    fn arity_of_binding(&self, id: Id) -> usize {
        self.arity(id)
    }

    fn escalar(&self, id: Id) -> EScalar {
        self.data().escalar(id).clone()
    }

    fn column_types(&self, id: Id) -> Option<Vec<ReprColumnType>> {
        self.column_types(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::egraph::{CNode, EGraph, ENode};
    use crate::eqsat::ir::EScalar;
    use mz_expr::MirScalarExpr;

    #[mz_ore::test]
    fn base_view_matches_inherent_methods() {
        let mut eg = EGraph::new();
        // Build Filter(#0 = #0)(Get-like leaf) via a constant input.
        let leaf = eg.add(CNode::Rel(ENode::Constant {
            card: 1,
            arity: 1,
            col_types: None,
        }));
        let p = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
        let filt = eg.add(CNode::Rel(ENode::Filter {
            predicates: vec![p],
            input: leaf,
        }));
        eg.rebuild();
        let index = eg.rel_index();
        let an = Analyses::default();
        let view = BaseView {
            eg: &eg,
            index: &index,
            an: &an,
        };
        // The trait read methods agree with the inherent ones.
        assert_eq!(view.rel_class_ids(), eg.rel_class_ids());
        assert_eq!(view.arity(filt), eg.arity(filt));
        assert_eq!(
            view.rel_class_nodes(filt).len(),
            eg.rel_class_nodes(filt).len()
        );
    }
}
