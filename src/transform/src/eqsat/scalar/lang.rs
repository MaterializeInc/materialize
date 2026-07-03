// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The scalar instantiation of the generic e-graph [`Language`].
//!
//! `ScalarLang` makes the scalar engine a second user of `core::EGraph<L>`. The
//! per-class analysis ([`ClassAnalysis`]) is maintained incrementally through the
//! `on_add`/`on_union` hooks (the fixpoint recompute lives in the scalar
//! `saturate` driver). See `doc/developer/design/20260624_eqsat/20260627_eqsat_scalar_instance.md`.

use std::collections::HashMap;

use mz_repr::ReprColumnType;

use crate::eqsat::core::{Id, Language};
use crate::eqsat::scalar::analysis::{self, ClassAnalysis};
use crate::eqsat::scalar::node::SNode;

/// The operator symbol used to bucket scalar e-nodes in the matcher index.
///
/// SP2a does not run the matcher over scalar nodes (the scalar rules are
/// hand-written and matched against concrete `SNode`s), so a coarse discriminant
/// — no function identity — is sufficient to satisfy [`Language::symbol`]. A
/// future DSL matcher (SP2b) would refine this.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ScalarSym {
    Column,
    Literal,
    Unmaterializable,
    Unary,
    Binary,
    Variadic,
    If,
}

/// Language-owned state on the scalar e-graph: the relation's column types
/// (read by the typed-literal rules) and the per-class analyses (maintained by
/// the hooks).
#[derive(Debug, Default)]
pub struct ScalarGraphData {
    /// Column types of the relation the expression is evaluated against, indexed
    /// by column position. Empty when no rule needs a column type.
    pub(crate) col_types: Vec<ReprColumnType>,
    /// Per-class analyses, keyed by canonical class id.
    pub(crate) analysis: HashMap<Id, ClassAnalysis>,
}

/// The scalar node language.
pub struct ScalarLang;

impl Language for ScalarLang {
    type Node = SNode;
    type Sym = ScalarSym;
    type GraphData = ScalarGraphData;

    fn children(node: &SNode) -> Vec<Id> {
        node.children()
    }

    fn map_children(node: &SNode, f: impl Fn(Id) -> Id) -> SNode {
        node.map_children(f)
    }

    fn symbol(node: &SNode) -> ScalarSym {
        match node {
            SNode::Column(..) => ScalarSym::Column,
            SNode::Literal(..) => ScalarSym::Literal,
            SNode::CallUnmaterializable(_) => ScalarSym::Unmaterializable,
            SNode::CallUnary { .. } => ScalarSym::Unary,
            SNode::CallBinary { .. } => ScalarSym::Binary,
            SNode::CallVariadic { .. } => ScalarSym::Variadic,
            SNode::If { .. } => ScalarSym::If,
        }
    }

    fn on_add(data: &mut ScalarGraphData, id: Id, node: &SNode, get: &dyn Fn(Id) -> Id) {
        // The new class's analysis is the node's `make` contribution, reading
        // child analyses (already present: children were added bottom-up).
        let a = analysis::make(node, &data.analysis, get);
        data.analysis.insert(id, a);
    }

    fn on_union(data: &mut ScalarGraphData, winner: Id, loser: Id) {
        // Fold loser into winner. Both classes must already carry an analysis
        // (a missing one is a bug, not a recoverable state: the safe
        // `could_error` default would be `true`, and silently inventing it
        // would corrupt a rule guard).
        let lo = data
            .analysis
            .remove(&loser)
            .expect("class must have an analysis before union");
        let wi = data
            .analysis
            .remove(&winner)
            .expect("class must have an analysis before union");
        data.analysis.insert(winner, analysis::merge(wi, lo));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::EvalError;
    use mz_ore::treat_as_equal::TreatAsEqual;
    use mz_repr::{Datum, ReprScalarType};

    use crate::eqsat::core::EGraph;

    fn err_lit() -> SNode {
        // Build a typed error literal SNode directly.
        let mir =
            mz_expr::MirScalarExpr::literal(Err(EvalError::DivisionByZero), ReprScalarType::Int64);
        match mir {
            mz_expr::MirScalarExpr::Literal(row, typ) => SNode::Literal(row, typ),
            _ => unreachable!("literal builder returns a Literal"),
        }
    }

    fn ok_lit(v: i64) -> SNode {
        let mir = mz_expr::MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        match mir {
            mz_expr::MirScalarExpr::Literal(row, typ) => SNode::Literal(row, typ),
            _ => unreachable!("literal builder returns a Literal"),
        }
    }

    #[mz_ore::test]
    fn on_add_populates_analysis() {
        let mut eg = EGraph::<ScalarLang>::new();
        let col = eg.add(SNode::Column(0, TreatAsEqual(None)));
        assert!(
            eg.data().analysis.contains_key(&col),
            "on_add must create an analysis entry for the column"
        );
        assert!(
            !eg.data().analysis[&col].could_error,
            "a column must not could_error"
        );

        let err = eg.add(err_lit());
        assert!(
            eg.data().analysis[&err].could_error,
            "an error literal must could_error"
        );
        assert!(
            eg.data().analysis[&err].literal.is_some(),
            "a literal class must carry its literal"
        );
    }

    #[mz_ore::test]
    fn on_union_merges_analysis() {
        let mut eg = EGraph::<ScalarLang>::new();
        // ok literal (could_error = false, literal = Some) ∪ column (false, None)
        let lit = eg.add(ok_lit(7));
        let col = eg.add(SNode::Column(0, TreatAsEqual(None)));
        eg.union(lit, col);
        let merged = eg.find(lit);
        let a = &eg.data().analysis[&merged];
        assert!(!a.could_error, "merge of two safe classes stays safe");
        assert!(a.literal.is_some(), "literal survives the merge");
    }
}
