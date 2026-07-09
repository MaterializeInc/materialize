// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Term graphs, flat deduplicated representations of expression trees.
//!
//! A [`TermGraph`] assigns dense identifiers to terms, where a term is an
//! operator applied to identifiers of previously inserted terms. Structurally
//! equal subexpressions receive equal identifiers, so the graph is a DAG
//! rather than a tree. A term's children always have identifiers smaller than
//! the term's own, so iterating terms in identifier order visits children
//! before parents. That order lets consumers traverse expressions bottom-up
//! with a loop rather than recursion, avoiding the stack hazards that owning
//! expression trees carry.
//!
//! Expression types opt in by implementing [`TermRep`], which splits a node
//! into an operator (children elided) and child references, and reassembles
//! a node from an operator and owned children. [`TermGraph::ensure`] and
//! [`TermGraph::extract`] then convert in both directions, linearly and
//! iteratively.

use std::hash::Hash;
use std::sync::Arc;

use indexmap::IndexSet;
use mz_ore::collections::HashMap;
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::{ReprColumnType, Row};
use smallvec::SmallVec;

use crate::{BinaryFunc, EvalError, MirScalarExpr, UnaryFunc, UnmaterializableFunc, VariadicFunc};

/// Identifies a term within a [`TermGraph`].
pub type TermId = usize;

/// An operator applied to identifiers of previously inserted terms.
pub type Term<Op> = (Op, SmallVec<[TermId; 2]>);

/// Types interconvertible with the terms of a [`TermGraph`].
pub trait TermRep: Sized {
    /// The operator type heading each term.
    type Op: Clone + Eq + Hash;

    /// The operator at the root of `self`, children elided.
    fn to_op(&self) -> Self::Op;

    /// The direct children of `self`, in the order `rebuild` expects them.
    fn children(&self) -> impl Iterator<Item = &Self>;

    /// Reassembles a node from its operator and owned children.
    ///
    /// The children arrive in the order `children` produced them, and there
    /// are exactly as many as the original node had.
    fn rebuild(op: &Self::Op, children: Vec<Self>) -> Self;
}

/// A deduplicating map from terms to dense identifiers.
///
/// Invariants:
/// * Identifiers are dense, exactly `0 .. self.len()`.
/// * A term's children have identifiers strictly smaller than the term's own,
///   so identifier order is a topological (children first) order.
/// * The graph is insert-only. Identifiers are never invalidated or reused.
#[derive(Clone, Debug)]
pub struct TermGraph<Op> {
    terms: IndexSet<Term<Op>>,
}

impl<Op> Default for TermGraph<Op> {
    fn default() -> Self {
        Self {
            terms: IndexSet::default(),
        }
    }
}

impl<Op: Eq + Hash> TermGraph<Op> {
    /// Ensures the term is present and returns its identifier.
    ///
    /// The children in `term` must be identifiers previously returned by this
    /// graph, which maintains the topological ordering of identifiers.
    pub fn insert(&mut self, term: Term<Op>) -> TermId {
        debug_assert!(term.1.iter().all(|c| *c < self.terms.len()));
        self.terms.insert_full(term).0
    }

    /// The term bound to `id`.
    ///
    /// Panics if `id` was not returned by a prior insertion into this graph.
    pub fn term(&self, id: TermId) -> &Term<Op> {
        self.terms.get_index(id).expect("term id from this graph")
    }

    /// The number of distinct terms.
    pub fn len(&self) -> usize {
        self.terms.len()
    }

    /// True iff the graph contains no terms.
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    /// Terms in identifier order, so children before parents.
    pub fn iter(&self) -> impl Iterator<Item = (TermId, &Term<Op>)> {
        self.terms.iter().enumerate()
    }

    /// Introduces `root` and its distinct subexpressions, returning the
    /// identifier of `root`.
    ///
    /// Iterative, so arbitrarily deep expressions cannot exhaust the stack.
    /// Structurally equal subexpressions receive equal identifiers.
    pub fn ensure<T: TermRep<Op = Op>>(&mut self, root: &T) -> TermId {
        // The reverse of a depth-first pre-order lists children after their
        // parents, so the reversed list can be processed children first.
        let mut stack = vec![root];
        let mut rev_order = Vec::new();
        while let Some(expr) = stack.pop() {
            rev_order.push(expr);
            stack.extend(expr.children());
        }
        // Identifiers for processed nodes, keyed by node address. Comparing
        // the expressions themselves would recurse, reintroducing the stack
        // hazard this type exists to avoid.
        let mut ids: HashMap<*const T, TermId> = HashMap::default();
        let mut root_id = 0;
        for expr in rev_order.into_iter().rev() {
            let args: SmallVec<[TermId; 2]> = expr
                .children()
                .map(|child| ids[&std::ptr::from_ref(child)])
                .collect();
            root_id = self.insert((expr.to_op(), args));
            ids.insert(std::ptr::from_ref(expr), root_id);
        }
        // The root is first in `rev_order`, so it is processed last.
        root_id
    }

    /// Builds the owned expression rooted at `id`.
    ///
    /// Iterative, but the result is an owning tree. A deep result carries the
    /// usual expression tree stack hazards (drop, comparison), which the
    /// caller owns. Shared subterms are duplicated in the result, which can
    /// be much larger than the graph that describes it.
    pub fn extract<T: TermRep<Op = Op>>(&self, id: TermId) -> T {
        enum Step {
            Descend(TermId),
            Emit(TermId),
        }
        let mut todo = vec![Step::Descend(id)];
        // Completed subexpressions. When `Emit(id)` is popped, the results
        // for `id`'s children sit on top, first child topmost.
        let mut done: Vec<T> = Vec::new();
        while let Some(step) = todo.pop() {
            match step {
                Step::Descend(id) => {
                    todo.push(Step::Emit(id));
                    for child in self.term(id).1.iter() {
                        todo.push(Step::Descend(*child));
                    }
                }
                Step::Emit(id) => {
                    let (op, args) = self.term(id);
                    let children = (0..args.len())
                        .map(|_| done.pop().expect("child result present"))
                        .collect();
                    done.push(T::rebuild(op, children));
                }
            }
        }
        assert_eq!(done.len(), 1);
        done.pop().expect("exactly one result")
    }
}

/// The operator at the root of a [`MirScalarExpr`], children elided.
///
/// NOTE: `TreatAsEqual` compares equal regardless of its content, so
/// deduplication unifies columns that differ only in their name hint. The
/// first observed hint wins. This matches the `Eq` semantics of
/// [`MirScalarExpr`] itself.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MseOp {
    /// A column reference. See [`MirScalarExpr::Column`].
    Column(usize, TreatAsEqual<Option<Arc<str>>>),
    /// A literal value or error. See [`MirScalarExpr::Literal`].
    Literal(Result<Row, EvalError>, ReprColumnType),
    /// An unmaterializable function call. See [`MirScalarExpr::CallUnmaterializable`].
    CallUnmaterializable(UnmaterializableFunc),
    /// A unary function call, applied to one child.
    CallUnary(UnaryFunc),
    /// A binary function call, applied to two children.
    CallBinary(BinaryFunc),
    /// A variadic function call, applied to all children.
    CallVariadic(VariadicFunc),
    /// A conditional, applied to children `[cond, then, els]`.
    If,
}

impl TermRep for MirScalarExpr {
    type Op = MseOp;

    fn to_op(&self) -> MseOp {
        match self {
            MirScalarExpr::Column(col, name) => MseOp::Column(*col, name.clone()),
            MirScalarExpr::Literal(row, typ) => MseOp::Literal(row.clone(), typ.clone()),
            MirScalarExpr::CallUnmaterializable(func) => MseOp::CallUnmaterializable(func.clone()),
            MirScalarExpr::CallUnary { func, .. } => MseOp::CallUnary(func.clone()),
            MirScalarExpr::CallBinary { func, .. } => MseOp::CallBinary(func.clone()),
            MirScalarExpr::CallVariadic { func, .. } => MseOp::CallVariadic(func.clone()),
            MirScalarExpr::If { .. } => MseOp::If,
        }
    }

    fn children(&self) -> impl Iterator<Item = &Self> {
        MirScalarExpr::children(self)
    }

    fn rebuild(op: &MseOp, children: Vec<Self>) -> Self {
        let mut children = children.into_iter();
        let mut next = || children.next().expect("child present");
        match op {
            MseOp::Column(col, name) => MirScalarExpr::Column(*col, name.clone()),
            MseOp::Literal(row, typ) => MirScalarExpr::Literal(row.clone(), typ.clone()),
            MseOp::CallUnmaterializable(func) => MirScalarExpr::CallUnmaterializable(func.clone()),
            MseOp::CallUnary(func) => MirScalarExpr::CallUnary {
                func: func.clone(),
                expr: Box::new(next()),
            },
            MseOp::CallBinary(func) => MirScalarExpr::CallBinary {
                func: func.clone(),
                expr1: Box::new(next()),
                expr2: Box::new(next()),
            },
            MseOp::CallVariadic(func) => MirScalarExpr::CallVariadic {
                func: func.clone(),
                exprs: children.collect(),
            },
            MseOp::If => MirScalarExpr::If {
                cond: Box::new(next()),
                then: Box::new(next()),
                els: Box::new(next()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, ReprScalarType};

    use super::*;
    use crate::func;

    fn col(i: usize) -> MirScalarExpr {
        MirScalarExpr::column(i)
    }

    fn lit(i: i64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int64(i), ReprScalarType::Int64)
    }

    /// Drops an expression without recursing, since `MirScalarExpr` lacks a
    /// custom `Drop` and deep trees overflow the stack when dropped naively.
    fn dismantle(expr: MirScalarExpr) {
        let mut todo = vec![expr];
        while let Some(expr) = todo.pop() {
            match expr {
                MirScalarExpr::CallUnary { expr, .. } => todo.push(*expr),
                MirScalarExpr::CallBinary { expr1, expr2, .. } => {
                    todo.push(*expr1);
                    todo.push(*expr2);
                }
                MirScalarExpr::CallVariadic { exprs, .. } => todo.extend(exprs),
                MirScalarExpr::If { cond, then, els } => {
                    todo.push(*cond);
                    todo.push(*then);
                    todo.push(*els);
                }
                _ => {}
            }
        }
    }

    #[mz_ore::test]
    fn roundtrip_basic() {
        let exprs = vec![
            col(3),
            lit(7),
            col(0).call_binary(lit(1), func::AddInt64),
            col(0).call_unary(UnaryFunc::Not(func::Not)),
            MirScalarExpr::call_variadic(func::variadic::Coalesce, vec![lit(1), lit(2), lit(3)]),
            col(0).if_then_else(lit(1), lit(2)),
        ];
        let mut graph = TermGraph::default();
        for expr in exprs {
            let id = graph.ensure(&expr);
            let extracted: MirScalarExpr = graph.extract(id);
            assert_eq!(extracted, expr);
        }
    }

    #[mz_ore::test]
    fn sharing() {
        let sum = col(0).call_binary(col(1), func::AddInt64);
        let expr = sum.clone().call_binary(sum, func::AddInt64);
        let mut graph = TermGraph::default();
        let id = graph.ensure(&expr);
        // Terms: col 0, col 1, the inner sum (once), the outer sum.
        assert_eq!(graph.len(), 4);
        // Re-ensuring the extraction reproduces the identifier and adds nothing.
        let extracted: MirScalarExpr = graph.extract(id);
        assert_eq!(graph.ensure(&extracted), id);
        assert_eq!(graph.len(), 4);
    }

    #[mz_ore::test]
    fn dedup_across_ensures() {
        let mut graph = TermGraph::default();
        let a = graph.ensure(&col(0).call_binary(lit(1), func::AddInt64));
        let b = graph.ensure(&col(0).call_binary(lit(1), func::AddInt64));
        assert_eq!(a, b);
    }

    #[mz_ore::test]
    fn deep_expressions() {
        // Deep enough that recursive traversal, comparison, or drop would
        // overflow a default 2MiB test thread stack.
        const DEPTH: usize = 100_000;
        let mut expr = col(0);
        for _ in 0..DEPTH {
            expr = expr.call_unary(UnaryFunc::Not(func::Not));
        }
        let mut graph = TermGraph::default();
        let id = graph.ensure(&expr);
        assert_eq!(graph.len(), DEPTH + 1);
        // Equality on the deep trees would recurse, so round-trip through
        // `ensure` instead, which must reproduce the identifier exactly.
        let extracted: MirScalarExpr = graph.extract(id);
        assert_eq!(graph.ensure(&extracted), id);
        assert_eq!(graph.len(), DEPTH + 1);
        dismantle(expr);
        dismantle(extracted);
    }
}
