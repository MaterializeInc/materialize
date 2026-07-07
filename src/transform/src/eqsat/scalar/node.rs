// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Decomposed scalar e-node.
//!
//! [`SNode`] is the unit the scalar e-graph interns. Each variant mirrors a
//! [`MirScalarExpr`] variant, but operator children are replaced by e-class
//! [`Id`]s rather than nested expressions. Leaves carry their payload directly.
//!
//! [`MirScalarExpr`]: mz_expr::MirScalarExpr

use mz_expr::{BinaryFunc, EvalError, UnaryFunc, UnmaterializableFunc, VariadicFunc};
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::{ReprColumnType, Row};

use crate::eqsat::core::Id;

/// A node in the scalar e-graph: a scalar operator whose operands are e-class
/// ids. Mirrors the variants of [`mz_expr::MirScalarExpr`].
///
/// Leaves (`Column`, `Literal`, `CallUnmaterializable`) hold no `Id` and carry
/// their full payload, so the bridge reconstructs them losslessly. Operators
/// hold `Id` children and carry the function as payload.
///
/// The `Ord` derive gives deterministic tie-breaks during extraction, matching
/// the relational `ENode`. It composes the component types' own `Ord`, which is
/// the same order used by `MirScalarExpr`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum SNode {
    /// A column reference. Carries the column index and the original name
    /// payload. The name uses [`TreatAsEqual`] so it is ignored for hashing and
    /// equality (two references to the same column with different names intern
    /// to one e-node), exactly as in `MirScalarExpr::Column`, while still being
    /// available for a faithful raise.
    Column(usize, TreatAsEqual<Option<std::sync::Arc<str>>>),
    /// A literal value or a literal evaluation error, with its column type.
    ///
    /// `MirScalarExpr::Literal` stores `Result<Row, EvalError>`, where the
    /// `Err` arm is an error that evaluation produces unconditionally. We keep
    /// the whole `Result` so error literals round-trip; storing only the `Row`
    /// would drop the error arm.
    Literal(Result<Row, EvalError>, ReprColumnType),
    /// A call to an unmaterializable function (a leaf, no operands).
    CallUnmaterializable(UnmaterializableFunc),
    /// A unary function applied to one operand class.
    CallUnary { func: UnaryFunc, expr: Id },
    /// A binary function applied to two operand classes.
    CallBinary {
        func: BinaryFunc,
        expr1: Id,
        expr2: Id,
    },
    /// A variadic function applied to a list of operand classes.
    CallVariadic { func: VariadicFunc, exprs: Vec<Id> },
    /// A conditional. `then` and `els` are only evaluated per `cond`, which the
    /// bridge preserves by keeping all three as distinct operand classes.
    If { cond: Id, then: Id, els: Id },
}

impl SNode {
    /// Apply `f` to every child `Id`, returning the rewritten node. Leaves are
    /// returned unchanged. Used to canonicalize children against the union-find.
    pub fn map_children<F: FnMut(Id) -> Id>(&self, mut f: F) -> SNode {
        match self {
            SNode::Column(..) | SNode::Literal(..) | SNode::CallUnmaterializable(_) => self.clone(),
            SNode::CallUnary { func, expr } => SNode::CallUnary {
                func: func.clone(),
                expr: f(*expr),
            },
            SNode::CallBinary { func, expr1, expr2 } => SNode::CallBinary {
                func: func.clone(),
                expr1: f(*expr1),
                expr2: f(*expr2),
            },
            SNode::CallVariadic { func, exprs } => SNode::CallVariadic {
                func: func.clone(),
                exprs: exprs.iter().map(|c| f(*c)).collect(),
            },
            SNode::If { cond, then, els } => SNode::If {
                cond: f(*cond),
                then: f(*then),
                els: f(*els),
            },
        }
    }

    /// The child classes of this node, in operand order. Empty for leaves.
    pub fn children(&self) -> Vec<Id> {
        match self {
            SNode::Column(..) | SNode::Literal(..) | SNode::CallUnmaterializable(_) => Vec::new(),
            SNode::CallUnary { expr, .. } => vec![*expr],
            SNode::CallBinary { expr1, expr2, .. } => vec![*expr1, *expr2],
            SNode::CallVariadic { exprs, .. } => exprs.clone(),
            SNode::If { cond, then, els } => vec![*cond, *then, *els],
        }
    }
}
