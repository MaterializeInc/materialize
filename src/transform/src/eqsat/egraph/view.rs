// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

#![allow(dead_code)]
// Trait methods are resolved to concrete types by generated rules; a few methods (e.g. analysis-gated conds on ColoredView) remain conditionally-used.

//! `MatchGraph` / `ApplyGraph` abstraction layer over the base e-graph.
//!
//! Defines the two traits that generated rule functions call, and implements
//! them for the base e-graph layer (`BaseView` / `EGraph`). The colored
//! implementation arrives in SP4d T5.

use mz_repr::ReprColumnType;

use crate::eqsat::core::Id;
use crate::eqsat::egraph::combined::ScalarIndex;
use crate::eqsat::ir::EScalar;
use crate::eqsat::matcher::Payload;
use crate::eqsat::scalar::lang::ScalarSym;
use crate::eqsat::scalar::node::SNode;

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
    /// The scalar e-nodes of class `id` (empty if the class holds no scalar nodes).
    fn scalar_class_nodes(&self, id: Id) -> Vec<SNode>;
    /// Every scalar e-node whose operator symbol is `sym`, as `(class, node)`.
    fn nodes_by_scalar_sym(&self, sym: ScalarSym) -> Vec<(Id, SNode)>;
    /// Whether the scalar class `id` may produce a runtime error, per the scalar
    /// `could_error` analysis. `false` if the class carries no scalar analysis.
    fn scalar_could_error(&self, id: Id) -> bool;
    /// The boolean-or-null literal of scalar class `id`: `Some(Some(true/false))`
    /// for a bool literal, `Some(None)` for a null literal, `None` otherwise.
    fn scalar_lit_bool_or_null(&self, id: Id) -> Option<Option<bool>>;
    /// Whether scalar class `id`, raised to a `MirScalarExpr` and typed against
    /// the graph's `col_types`, is nullable. Computed on demand rather than
    /// stored in an analysis lattice.
    fn scalar_nullable(&self, id: Id) -> bool;
    /// Whether some id in `ids` is a scalar literal equal to `value`. The
    /// list-quantified backing for `Cond::AnyScalarLit`. Reuses the per-element
    /// `scalar_lit_bool_or_null` so no new analysis is needed.
    fn cond_any_scalar_lit(&self, ids: &[Id], value: bool) -> bool {
        ids.iter()
            .any(|&id| self.scalar_lit_bool_or_null(id) == Some(Some(value)))
    }
    /// Whether two or more of `ids` share a canonical e-class id. The backing
    /// for `Cond::HasDuplicateId`, the `and_or_dedup` fire-guard.
    fn cond_has_duplicate_id(&self, ids: &[Id]) -> bool;
    /// Whether some operand in `ids` is subsumption-droppable under dual
    /// connective `inner` with error-free dropped extras. Backs
    /// `Cond::AbsorbApplies`.
    fn cond_absorb_applies(&self, ids: &[Id], inner: &mz_expr::VariadicFunc) -> bool;
    /// Whether some operand in `ids` is a non-circular same-`func` variadic node,
    /// so flattening would change the operand list. Backs `Cond::FlattenApplies`.
    fn cond_flatten_applies(&self, ids: &[Id], func: &mz_expr::VariadicFunc) -> bool;
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
    pub scalar_index: &'a ScalarIndex,
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

    fn scalar_class_nodes(&self, id: Id) -> Vec<SNode> {
        self.eg
            .nodes(id)
            .into_iter()
            .filter_map(|n| match n {
                crate::eqsat::egraph::combined::CNode::Scalar(s) => Some(s),
                _ => None,
            })
            .collect()
    }

    fn nodes_by_scalar_sym(&self, sym: ScalarSym) -> Vec<(Id, SNode)> {
        self.scalar_index.get(&sym).cloned().unwrap_or_default()
    }

    fn scalar_could_error(&self, id: Id) -> bool {
        self.eg
            .data()
            .scalar
            .analysis
            .get(&self.eg.find(id))
            .map_or(false, |a| a.could_error)
    }

    fn scalar_lit_bool_or_null(&self, id: Id) -> Option<Option<bool>> {
        let (row, _ty) = self
            .eg
            .data()
            .scalar
            .analysis
            .get(&self.eg.find(id))?
            .literal
            .as_ref()?;
        let row = row.as_ref().ok()?;
        match row.unpack_first() {
            mz_repr::Datum::True => Some(Some(true)),
            mz_repr::Datum::False => Some(Some(false)),
            mz_repr::Datum::Null => Some(None),
            _ => None,
        }
    }

    fn scalar_nullable(&self, id: Id) -> bool {
        crate::eqsat::scalar_extract::raise(self.eg, id)
            .typ(&self.eg.data().scalar.col_types)
            .nullable
    }

    fn cond_has_duplicate_id(&self, ids: &[Id]) -> bool {
        let mut seen = std::collections::HashSet::new();
        !ids.iter().all(|&id| seen.insert(self.eg.find(id)))
    }

    fn cond_absorb_applies(&self, ids: &[Id], inner: &mz_expr::VariadicFunc) -> bool {
        crate::eqsat::rest_filters::absorb_drop_index(self.eg, ids, inner).is_some()
    }

    fn cond_flatten_applies(&self, ids: &[Id], func: &mz_expr::VariadicFunc) -> bool {
        crate::eqsat::rest_filters::flatten_applies(self.eg, ids, func)
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
        let scalar_index = eg.scalar_index();
        let an = Analyses::default();
        let view = BaseView {
            eg: &eg,
            index: &index,
            scalar_index: &scalar_index,
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

    #[mz_ore::test]
    fn scalar_could_error_and_lit_bool_or_null() {
        use crate::eqsat::scalar::analysis::ClassAnalysis;
        use mz_expr::BinaryFunc;
        use mz_repr::{Datum, ReprScalarType, Row};

        let mut eg = EGraph::new();
        let col = eg.add(CNode::Scalar(SNode::Column(
            0,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )));
        let lhs = eg.add(CNode::Scalar(SNode::Column(
            0,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )));
        let rhs = eg.add(CNode::Scalar(SNode::Column(
            1,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )));
        let div = eg.add(CNode::Scalar(SNode::CallBinary {
            func: BinaryFunc::DivInt64(mz_expr::func::DivInt64),
            expr1: lhs,
            expr2: rhs,
        }));
        let bool_ty = ReprColumnType {
            scalar_type: ReprScalarType::Bool,
            nullable: false,
        };
        let lit = eg.add(CNode::Scalar(SNode::Literal(
            Ok(Row::pack_slice(&[Datum::True])),
            bool_ty.clone(),
        )));
        eg.rebuild();

        // `scalar_saturate::recompute_analysis` is private to its module, so seed
        // the analysis map directly the way that pass would after a round: this
        // exercises the same `eg.data().scalar.analysis` lookup the view methods
        // use without depending on the (still Task-1-red) rest of the crate.
        // Canonicalize ids before the mutable `data_mut()` borrow so `eg.find`'s
        // immutable borrow does not overlap it.
        let (col_id, div_id, lit_id) = (eg.find(col), eg.find(div), eg.find(lit));
        eg.data_mut().scalar.analysis.insert(
            col_id,
            ClassAnalysis {
                could_error: false,
                literal: None,
            },
        );
        eg.data_mut().scalar.analysis.insert(
            div_id,
            ClassAnalysis {
                could_error: true,
                literal: None,
            },
        );
        eg.data_mut().scalar.analysis.insert(
            lit_id,
            ClassAnalysis {
                could_error: false,
                literal: Some((Ok(Row::pack_slice(&[Datum::True])), bool_ty)),
            },
        );

        let index = eg.rel_index();
        let scalar_index = eg.scalar_index();
        let an = Analyses::default();
        let view = BaseView {
            eg: &eg,
            index: &index,
            scalar_index: &scalar_index,
            an: &an,
        };

        assert!(
            !view.scalar_could_error(col),
            "bare column must not could_error"
        );
        assert!(view.scalar_could_error(div), "division must could_error");
        assert_eq!(
            view.scalar_lit_bool_or_null(lit),
            Some(Some(true)),
            "true literal must resolve to Some(Some(true))"
        );
        assert_eq!(
            view.scalar_lit_bool_or_null(col),
            None,
            "non-literal class must return None"
        );
    }

    #[mz_ore::test]
    fn cond_any_scalar_lit() {
        use crate::eqsat::scalar::analysis::ClassAnalysis;
        use mz_repr::{Datum, ReprScalarType, Row};

        let mut eg = EGraph::new();
        let col = eg.add(CNode::Scalar(SNode::Column(
            0,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )));
        let bool_ty = ReprColumnType {
            scalar_type: ReprScalarType::Bool,
            nullable: false,
        };
        let lit_false = eg.add(CNode::Scalar(SNode::Literal(
            Ok(Row::pack_slice(&[Datum::False])),
            bool_ty.clone(),
        )));
        eg.rebuild();

        let (col_id, lit_false_id) = (eg.find(col), eg.find(lit_false));
        eg.data_mut().scalar.analysis.insert(
            col_id,
            ClassAnalysis {
                could_error: false,
                literal: None,
            },
        );
        eg.data_mut().scalar.analysis.insert(
            lit_false_id,
            ClassAnalysis {
                could_error: false,
                literal: Some((Ok(Row::pack_slice(&[Datum::False])), bool_ty)),
            },
        );

        let index = eg.rel_index();
        let scalar_index = eg.scalar_index();
        let an = Analyses::default();
        let view = BaseView {
            eg: &eg,
            index: &index,
            scalar_index: &scalar_index,
            an: &an,
        };

        let mixed = [lit_false, col];
        assert!(
            view.cond_any_scalar_lit(&mixed, false),
            "list containing a literal-false id must match value:false"
        );
        assert!(
            !view.cond_any_scalar_lit(&mixed, true),
            "list with no literal-true id must not match value:true"
        );

        let only_columns = [col];
        assert!(
            !view.cond_any_scalar_lit(&only_columns, false),
            "list of only columns must never match"
        );
    }

    #[mz_ore::test]
    fn scalar_nullable_reads_col_types() {
        use mz_repr::ReprScalarType;

        let mut eg = EGraph::new();
        let nullable_col = eg.add(CNode::Scalar(SNode::Column(
            0,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )));
        let non_nullable_col = eg.add(CNode::Scalar(SNode::Column(
            1,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )));
        eg.rebuild();
        eg.data_mut().scalar.col_types = vec![
            ReprScalarType::Int64.nullable(true),
            ReprScalarType::Int64.nullable(false),
        ];

        let index = eg.rel_index();
        let scalar_index = eg.scalar_index();
        let an = Analyses::default();
        let view = BaseView {
            eg: &eg,
            index: &index,
            scalar_index: &scalar_index,
            an: &an,
        };

        assert!(
            view.scalar_nullable(nullable_col),
            "column typed nullable must report nullable"
        );
        assert!(
            !view.scalar_nullable(non_nullable_col),
            "column typed non-nullable must report non-nullable"
        );
    }
}

#[cfg(test)]
mod scalar_view_tests {
    use crate::eqsat::egraph::combined::{CNode, EGraph};
    use crate::eqsat::scalar::lang::ScalarSym;
    use crate::eqsat::scalar::node::SNode;
    use mz_expr::UnaryFunc;

    #[mz_ore::test]
    fn scalar_index_buckets_unary() {
        let mut eg = EGraph::new();
        let x = eg.add(CNode::Scalar(SNode::Column(
            0,
            mz_ore::treat_as_equal::TreatAsEqual(None),
        )));
        let not = eg.add(CNode::Scalar(SNode::CallUnary {
            func: UnaryFunc::Not(mz_expr::func::Not),
            expr: x,
        }));
        let idx = eg.scalar_index();
        let unary = idx.get(&ScalarSym::Unary).cloned().unwrap_or_default();
        assert_eq!(unary.len(), 1);
        assert_eq!(unary[0].0, eg.find(not));
    }
}
