// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Lower bridge: intern a [`MirScalarExpr`] into the combined e-graph.
//!
//! Each `MirScalarExpr` variant maps to its [`SNode`] counterpart, with operand
//! subterms interned recursively.

use mz_expr::MirScalarExpr;
use mz_ore::treat_as_equal::TreatAsEqual;

use crate::eqsat::core::Id;
use crate::eqsat::egraph::{CNode, EGraph as CombinedEGraph};
use crate::eqsat::scalar::node::SNode;

/// Build the [`SNode`] for `expr`, interning each operand subterm via
/// `intern_child`.
///
/// `pub(crate)` so that `colored::view::lower_colored_impl` can call it with a
/// closure that targets `add_colored` instead of re-duplicating the match arms.
pub(crate) fn snode_of(
    expr: &MirScalarExpr,
    mut intern_child: impl FnMut(&MirScalarExpr) -> Id,
) -> SNode {
    match expr {
        MirScalarExpr::Column(index, name) => {
            // `name` is `TreatAsEqual<Option<Arc<str>>>`. Carry it through so
            // raise reconstructs the original column, even though it does not
            // participate in equality or hashing.
            SNode::Column(*index, TreatAsEqual(name.0.clone()))
        }
        MirScalarExpr::Literal(lit, typ) => SNode::Literal(lit.clone(), typ.clone()),
        MirScalarExpr::CallUnmaterializable(func) => SNode::CallUnmaterializable(func.clone()),
        MirScalarExpr::CallUnary { func, expr } => SNode::CallUnary {
            func: func.clone(),
            expr: intern_child(expr),
        },
        MirScalarExpr::CallBinary { func, expr1, expr2 } => SNode::CallBinary {
            func: func.clone(),
            expr1: intern_child(expr1),
            expr2: intern_child(expr2),
        },
        MirScalarExpr::CallVariadic { func, exprs } => SNode::CallVariadic {
            func: func.clone(),
            exprs: exprs.iter().map(&mut intern_child).collect(),
        },
        MirScalarExpr::If { cond, then, els } => SNode::If {
            cond: intern_child(cond),
            then: intern_child(then),
            els: intern_child(els),
        },
    }
}

/// Intern `expr` into the combined relational+scalar e-graph, returning the
/// scalar e-class of its root. Subterms are added bottom-up, so hash-consing
/// shares structurally equal operands. Wraps each [`SNode`] in [`CNode::Scalar`]
/// so it shares the combined graph's single `Id` space.
pub fn lower_into(egraph: &mut CombinedEGraph, expr: &MirScalarExpr) -> Id {
    let node = snode_of(expr, |child| lower_into(egraph, child));
    egraph.add(CNode::Scalar(node))
}
