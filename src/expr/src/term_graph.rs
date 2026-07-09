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
use mz_repr::{Diff, ReprColumnType, ReprRelationType, Row};
use smallvec::SmallVec;

use crate::{
    AccessStrategy, AggregateExpr, BinaryFunc, ColumnOrder, EvalError, Id, JoinImplementation,
    LetRecLimit, LocalId, MirRelationExpr, MirScalarExpr, TableFunc, UnaryFunc,
    UnmaterializableFunc, VariadicFunc,
};

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

    /// Reassembles a node from its operator and owned children, the inverse
    /// of `into_parts`.
    ///
    /// The children arrive in the order `children` produced them, and there
    /// are exactly as many as the original node had.
    fn from_parts(op: Self::Op, children: Vec<Self>) -> Self;

    /// Decomposes `self` into its operator and owned children.
    fn into_parts(self) -> (Self::Op, Vec<Self>);

    /// Like `from_parts`, but cloning the operator's payloads.
    fn rebuild(op: &Self::Op, children: Vec<Self>) -> Self {
        Self::from_parts(op.clone(), children)
    }
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

    /// Like [`TermGraph::ensure`], but consumes the expression, moving its
    /// operator payloads into the graph rather than cloning them.
    ///
    /// Also iterative, including the teardown of `root`, so this doubles as
    /// a stack-safe way to dispose of arbitrarily deep expressions.
    pub fn ensure_owned<T: TermRep<Op = Op>>(&mut self, root: T) -> TermId {
        // Decompose nodes into an arena, recording child slots. Slots are
        // assigned before children are pushed, so a child's slot is always
        // larger than its parent's and a reverse sweep is children first.
        let mut arena: Vec<Option<(Op, Vec<usize>)>> = vec![None];
        let mut todo: Vec<(usize, T)> = vec![(0, root)];
        while let Some((slot, node)) = todo.pop() {
            let (op, children) = node.into_parts();
            let base = arena.len();
            let slots: Vec<usize> = (0..children.len()).map(|i| base + i).collect();
            for (i, child) in children.into_iter().enumerate() {
                arena.push(None);
                todo.push((base + i, child));
            }
            arena[slot] = Some((op, slots));
        }
        let mut ids = vec![0; arena.len()];
        for slot in (0..arena.len()).rev() {
            let (op, slots) = arena[slot].take().expect("slot populated");
            let args: SmallVec<[TermId; 2]> = slots.iter().map(|s| ids[*s]).collect();
            ids[slot] = self.insert((op, args));
        }
        ids[0]
    }

    /// Consumes the graph, yielding terms in identifier order, so children
    /// before parents.
    pub fn into_terms(self) -> impl Iterator<Item = Term<Op>> {
        self.terms.into_iter()
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

    fn from_parts(op: MseOp, children: Vec<Self>) -> Self {
        let mut children = children.into_iter();
        let mut next = || children.next().expect("child present");
        match op {
            MseOp::Column(col, name) => MirScalarExpr::Column(col, name),
            MseOp::Literal(row, typ) => MirScalarExpr::Literal(row, typ),
            MseOp::CallUnmaterializable(func) => MirScalarExpr::CallUnmaterializable(func),
            MseOp::CallUnary(func) => MirScalarExpr::CallUnary {
                func,
                expr: Box::new(next()),
            },
            MseOp::CallBinary(func) => MirScalarExpr::CallBinary {
                func,
                expr1: Box::new(next()),
                expr2: Box::new(next()),
            },
            MseOp::CallVariadic(func) => MirScalarExpr::CallVariadic {
                func,
                exprs: children.collect(),
            },
            MseOp::If => MirScalarExpr::If {
                cond: Box::new(next()),
                then: Box::new(next()),
                els: Box::new(next()),
            },
        }
    }

    fn into_parts(self) -> (MseOp, Vec<Self>) {
        match self {
            MirScalarExpr::Column(col, name) => (MseOp::Column(col, name), Vec::new()),
            MirScalarExpr::Literal(row, typ) => (MseOp::Literal(row, typ), Vec::new()),
            MirScalarExpr::CallUnmaterializable(func) => {
                (MseOp::CallUnmaterializable(func), Vec::new())
            }
            MirScalarExpr::CallUnary { func, expr } => (MseOp::CallUnary(func), vec![*expr]),
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                (MseOp::CallBinary(func), vec![*expr1, *expr2])
            }
            MirScalarExpr::CallVariadic { func, exprs } => (MseOp::CallVariadic(func), exprs),
            MirScalarExpr::If { cond, then, els } => (MseOp::If, vec![*cond, *then, *els]),
        }
    }
}

/// The operator at the root of a [`MirRelationExpr`], relation children
/// elided.
///
/// Scalar payloads (scalar expressions, aggregates, join metadata) remain
/// owned ASTs. Operations on a graph of these ops are immune to relation
/// nesting depth, but comparisons and hashes of the ops themselves still
/// recurse into the scalar payloads.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum MreOp {
    /// See [`MirRelationExpr::Constant`].
    Constant {
        /// The rows of the constant, or an error.
        rows: Result<Vec<(Row, Diff)>, EvalError>,
        /// Schema of the constant.
        typ: ReprRelationType,
    },
    /// See [`MirRelationExpr::Get`].
    Get {
        /// A global or local identifier of a collection.
        id: Id,
        /// Schema of the collection.
        typ: ReprRelationType,
        /// How the collection is accessed.
        access_strategy: AccessStrategy,
    },
    /// A binding, applied to children `[value, body]`. See
    /// [`MirRelationExpr::Let`].
    Let {
        /// The local identifier the value is bound to.
        id: LocalId,
    },
    /// Recursive bindings, applied to children `[values .., body]` where the
    /// values number `ids.len()`. See [`MirRelationExpr::LetRec`].
    LetRec {
        /// The local identifiers the values are bound to.
        ids: Vec<LocalId>,
        /// Recursion iteration limits, one per binding.
        limits: Vec<Option<LetRecLimit>>,
    },
    /// See [`MirRelationExpr::Project`].
    Project {
        /// The retained columns, in order.
        outputs: Vec<usize>,
    },
    /// See [`MirRelationExpr::Map`].
    Map {
        /// The appended scalar expressions.
        scalars: Vec<MirScalarExpr>,
    },
    /// See [`MirRelationExpr::FlatMap`].
    FlatMap {
        /// The table function.
        func: TableFunc,
        /// Arguments to the table function.
        exprs: Vec<MirScalarExpr>,
    },
    /// See [`MirRelationExpr::Filter`].
    Filter {
        /// The retention predicates.
        predicates: Vec<MirScalarExpr>,
    },
    /// A join, applied to its input children. See [`MirRelationExpr::Join`].
    Join {
        /// Classes of expressions that must evaluate to equal values.
        equivalences: Vec<Vec<MirScalarExpr>>,
        /// The join implementation strategy.
        implementation: JoinImplementation,
    },
    /// See [`MirRelationExpr::Reduce`].
    Reduce {
        /// Grouping key expressions.
        group_key: Vec<MirScalarExpr>,
        /// Aggregates to compute per group.
        aggregates: Vec<AggregateExpr>,
        /// Whether the input is monotonic.
        monotonic: bool,
        /// A user hint at the number of groups.
        expected_group_size: Option<u64>,
    },
    /// See [`MirRelationExpr::TopK`].
    TopK {
        /// Grouping key columns.
        group_key: Vec<usize>,
        /// Ordering within each group.
        order_key: Vec<ColumnOrder>,
        /// An optional limit on returned rows per group.
        limit: Option<MirScalarExpr>,
        /// Rows to skip per group.
        offset: usize,
        /// Whether the input is monotonic.
        monotonic: bool,
        /// A user hint at the number of groups.
        expected_group_size: Option<u64>,
    },
    /// See [`MirRelationExpr::Negate`].
    Negate,
    /// See [`MirRelationExpr::Threshold`].
    Threshold,
    /// A union, applied to children `[base, inputs ..]`. See
    /// [`MirRelationExpr::Union`].
    Union,
    /// See [`MirRelationExpr::ArrangeBy`].
    ArrangeBy {
        /// The key sets to arrange by.
        keys: Vec<Vec<MirScalarExpr>>,
    },
}

impl TermRep for MirRelationExpr {
    type Op = MreOp;

    fn to_op(&self) -> MreOp {
        match self {
            MirRelationExpr::Constant { rows, typ } => MreOp::Constant {
                rows: rows.clone(),
                typ: typ.clone(),
            },
            MirRelationExpr::Get {
                id,
                typ,
                access_strategy,
            } => MreOp::Get {
                id: *id,
                typ: typ.clone(),
                access_strategy: access_strategy.clone(),
            },
            MirRelationExpr::Let { id, .. } => MreOp::Let { id: *id },
            MirRelationExpr::LetRec { ids, limits, .. } => MreOp::LetRec {
                ids: ids.clone(),
                limits: limits.clone(),
            },
            MirRelationExpr::Project { outputs, .. } => MreOp::Project {
                outputs: outputs.clone(),
            },
            MirRelationExpr::Map { scalars, .. } => MreOp::Map {
                scalars: scalars.clone(),
            },
            MirRelationExpr::FlatMap { func, exprs, .. } => MreOp::FlatMap {
                func: func.clone(),
                exprs: exprs.clone(),
            },
            MirRelationExpr::Filter { predicates, .. } => MreOp::Filter {
                predicates: predicates.clone(),
            },
            MirRelationExpr::Join {
                equivalences,
                implementation,
                ..
            } => MreOp::Join {
                equivalences: equivalences.clone(),
                implementation: implementation.clone(),
            },
            MirRelationExpr::Reduce {
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
                ..
            } => MreOp::Reduce {
                group_key: group_key.clone(),
                aggregates: aggregates.clone(),
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
            MirRelationExpr::TopK {
                group_key,
                order_key,
                limit,
                offset,
                monotonic,
                expected_group_size,
                ..
            } => MreOp::TopK {
                group_key: group_key.clone(),
                order_key: order_key.clone(),
                limit: limit.clone(),
                offset: *offset,
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
            MirRelationExpr::Negate { .. } => MreOp::Negate,
            MirRelationExpr::Threshold { .. } => MreOp::Threshold,
            MirRelationExpr::Union { .. } => MreOp::Union,
            MirRelationExpr::ArrangeBy { keys, .. } => MreOp::ArrangeBy { keys: keys.clone() },
        }
    }

    fn children(&self) -> impl Iterator<Item = &Self> {
        MirRelationExpr::children(self)
    }

    fn from_parts(op: MreOp, children: Vec<Self>) -> Self {
        let mut children = children.into_iter();
        match op {
            MreOp::Constant { rows, typ } => MirRelationExpr::Constant { rows, typ },
            MreOp::Get {
                id,
                typ,
                access_strategy,
            } => MirRelationExpr::Get {
                id,
                typ,
                access_strategy,
            },
            MreOp::Let { id } => {
                let value = Box::new(children.next().expect("value present"));
                let body = Box::new(children.next().expect("body present"));
                MirRelationExpr::Let { id, value, body }
            }
            MreOp::LetRec { ids, limits } => {
                let mut values = Vec::with_capacity(ids.len());
                for _ in 0..ids.len() {
                    values.push(children.next().expect("value present"));
                }
                let body = Box::new(children.next().expect("body present"));
                MirRelationExpr::LetRec {
                    ids,
                    values,
                    limits,
                    body,
                }
            }
            MreOp::Project { outputs } => MirRelationExpr::Project {
                input: Box::new(children.next().expect("input present")),
                outputs,
            },
            MreOp::Map { scalars } => MirRelationExpr::Map {
                input: Box::new(children.next().expect("input present")),
                scalars,
            },
            MreOp::FlatMap { func, exprs } => MirRelationExpr::FlatMap {
                input: Box::new(children.next().expect("input present")),
                func,
                exprs,
            },
            MreOp::Filter { predicates } => MirRelationExpr::Filter {
                input: Box::new(children.next().expect("input present")),
                predicates,
            },
            MreOp::Join {
                equivalences,
                implementation,
            } => MirRelationExpr::Join {
                inputs: children.collect(),
                equivalences,
                implementation,
            },
            MreOp::Reduce {
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => MirRelationExpr::Reduce {
                input: Box::new(children.next().expect("input present")),
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            },
            MreOp::TopK {
                group_key,
                order_key,
                limit,
                offset,
                monotonic,
                expected_group_size,
            } => MirRelationExpr::TopK {
                input: Box::new(children.next().expect("input present")),
                group_key,
                order_key,
                limit,
                offset,
                monotonic,
                expected_group_size,
            },
            MreOp::Negate => MirRelationExpr::Negate {
                input: Box::new(children.next().expect("input present")),
            },
            MreOp::Threshold => MirRelationExpr::Threshold {
                input: Box::new(children.next().expect("input present")),
            },
            MreOp::Union => MirRelationExpr::Union {
                base: Box::new(children.next().expect("base present")),
                inputs: children.collect(),
            },
            MreOp::ArrangeBy { keys } => MirRelationExpr::ArrangeBy {
                input: Box::new(children.next().expect("input present")),
                keys,
            },
        }
    }

    fn into_parts(self) -> (MreOp, Vec<Self>) {
        match self {
            MirRelationExpr::Constant { rows, typ } => (MreOp::Constant { rows, typ }, Vec::new()),
            MirRelationExpr::Get {
                id,
                typ,
                access_strategy,
            } => (
                MreOp::Get {
                    id,
                    typ,
                    access_strategy,
                },
                Vec::new(),
            ),
            MirRelationExpr::Let { id, value, body } => (MreOp::Let { id }, vec![*value, *body]),
            MirRelationExpr::LetRec {
                ids,
                values,
                limits,
                body,
            } => {
                let mut children = values;
                children.push(*body);
                (MreOp::LetRec { ids, limits }, children)
            }
            MirRelationExpr::Project { input, outputs } => {
                (MreOp::Project { outputs }, vec![*input])
            }
            MirRelationExpr::Map { input, scalars } => (MreOp::Map { scalars }, vec![*input]),
            MirRelationExpr::FlatMap { input, func, exprs } => {
                (MreOp::FlatMap { func, exprs }, vec![*input])
            }
            MirRelationExpr::Filter { input, predicates } => {
                (MreOp::Filter { predicates }, vec![*input])
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                implementation,
            } => (
                MreOp::Join {
                    equivalences,
                    implementation,
                },
                inputs,
            ),
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
            } => (
                MreOp::Reduce {
                    group_key,
                    aggregates,
                    monotonic,
                    expected_group_size,
                },
                vec![*input],
            ),
            MirRelationExpr::TopK {
                input,
                group_key,
                order_key,
                limit,
                offset,
                monotonic,
                expected_group_size,
            } => (
                MreOp::TopK {
                    group_key,
                    order_key,
                    limit,
                    offset,
                    monotonic,
                    expected_group_size,
                },
                vec![*input],
            ),
            MirRelationExpr::Negate { input } => (MreOp::Negate, vec![*input]),
            MirRelationExpr::Threshold { input } => (MreOp::Threshold, vec![*input]),
            MirRelationExpr::Union { base, inputs } => {
                let mut children = vec![*base];
                children.extend(inputs);
                (MreOp::Union, children)
            }
            MirRelationExpr::ArrangeBy { input, keys } => (MreOp::ArrangeBy { keys }, vec![*input]),
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::{Datum, GlobalId, ReprScalarType, SqlScalarType};

    use super::*;
    use crate::func;

    fn typ1() -> ReprRelationType {
        ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)])
    }

    fn base() -> MirRelationExpr {
        MirRelationExpr::constant(vec![vec![Datum::Int64(1)]], typ1())
    }

    /// Drops a relation expression without recursing along the relation
    /// spine, for the same reason as `dismantle`.
    fn dismantle_relation(expr: MirRelationExpr) {
        use MirRelationExpr::*;
        let mut todo = vec![expr];
        while let Some(expr) = todo.pop() {
            match expr {
                Constant { .. } | Get { .. } => {}
                Let { value, body, .. } => {
                    todo.push(*value);
                    todo.push(*body);
                }
                LetRec { values, body, .. } => {
                    todo.extend(values);
                    todo.push(*body);
                }
                Project { input, .. }
                | Map { input, .. }
                | FlatMap { input, .. }
                | Filter { input, .. }
                | Reduce { input, .. }
                | TopK { input, .. }
                | Negate { input }
                | Threshold { input }
                | ArrangeBy { input, .. } => todo.push(*input),
                Join { inputs, .. } => todo.extend(inputs),
                Union { base, inputs } => {
                    todo.push(*base);
                    todo.extend(inputs);
                }
            }
        }
    }

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

    #[mz_ore::test]
    fn ensure_owned_matches_ensure() {
        let exprs = vec![
            lit(7),
            col(0).call_binary(lit(1), func::AddInt64),
            col(0).if_then_else(lit(1), lit(2)),
        ];
        let mut graph = TermGraph::default();
        for expr in exprs {
            let borrowed = graph.ensure(&expr);
            assert_eq!(graph.ensure_owned(expr), borrowed);
        }
    }

    #[mz_ore::test]
    fn deep_owned_teardown() {
        // `ensure_owned` consumes the expression iteratively, so it needs no
        // separate dismantling even at depths where drop would overflow.
        const DEPTH: usize = 100_000;
        let mut expr = col(0);
        for _ in 0..DEPTH {
            expr = expr.call_unary(UnaryFunc::Not(func::Not));
        }
        let mut graph = TermGraph::default();
        let id = graph.ensure_owned(expr);
        assert_eq!(id, DEPTH);
        assert_eq!(graph.len(), DEPTH + 1);
    }

    #[mz_ore::test]
    fn into_terms_order() {
        let mut graph = TermGraph::default();
        graph.ensure(&col(0).call_binary(lit(1), func::AddInt64));
        let len = graph.len();
        let terms: Vec<_> = graph.into_terms().collect();
        assert_eq!(terms.len(), len);
        for (id, (_op, args)) in terms.iter().enumerate() {
            assert!(args.iter().all(|a| *a < id));
        }
    }

    #[mz_ore::test]
    fn roundtrip_basic_relations() {
        let local = LocalId::new(1);
        let exprs = vec![
            base(),
            MirRelationExpr::global_get(GlobalId::User(1), typ1()),
            base().project(vec![0]),
            MirRelationExpr::Map {
                input: Box::new(base()),
                scalars: vec![lit(5)],
            },
            base().filter(vec![col(0).call_unary(UnaryFunc::Not(func::Not))]),
            MirRelationExpr::FlatMap {
                input: Box::new(base()),
                func: TableFunc::Wrap {
                    types: vec![SqlScalarType::Int64.nullable(false)],
                    width: 1,
                },
                exprs: vec![col(0)],
            },
            base().negate().threshold(),
            base().union(base().negate()),
            MirRelationExpr::join(
                vec![
                    MirRelationExpr::global_get(GlobalId::User(1), typ1()),
                    MirRelationExpr::global_get(GlobalId::User(2), typ1()),
                ],
                vec![vec![(0, 0), (1, 0)]],
            ),
            MirRelationExpr::Let {
                id: local,
                value: Box::new(base()),
                body: Box::new(MirRelationExpr::Get {
                    id: Id::Local(local),
                    typ: typ1(),
                    access_strategy: AccessStrategy::UnknownOrLocal,
                }),
            },
            MirRelationExpr::LetRec {
                ids: vec![local],
                values: vec![base()],
                limits: vec![None],
                body: Box::new(base()),
            },
            MirRelationExpr::Reduce {
                input: Box::new(base()),
                group_key: vec![col(0)],
                aggregates: vec![],
                monotonic: false,
                expected_group_size: None,
            },
            MirRelationExpr::TopK {
                input: Box::new(base()),
                group_key: vec![0],
                order_key: vec![],
                limit: None,
                offset: 0,
                monotonic: false,
                expected_group_size: None,
            },
            MirRelationExpr::ArrangeBy {
                input: Box::new(base()),
                keys: vec![vec![col(0)]],
            },
        ];
        let mut graph = TermGraph::default();
        for expr in exprs {
            let id = graph.ensure(&expr);
            let extracted: MirRelationExpr = graph.extract(id);
            assert_eq!(extracted, expr);
        }
    }

    #[mz_ore::test]
    fn sharing_relations() {
        let get = MirRelationExpr::global_get(GlobalId::User(1), typ1());
        let expr = get.clone().union(get);
        let mut graph = TermGraph::default();
        let id = graph.ensure(&expr);
        // Terms: the get (once) and the union.
        assert_eq!(graph.len(), 2);
        let extracted: MirRelationExpr = graph.extract(id);
        assert_eq!(graph.ensure(&extracted), id);
        assert_eq!(graph.len(), 2);
    }

    #[mz_ore::test]
    fn deep_relations() {
        // Deep enough that recursive traversal, comparison, or drop would
        // overflow a default 2MiB test thread stack.
        const DEPTH: usize = 100_000;
        let mut expr = base();
        for _ in 0..DEPTH {
            // Constructed directly since the `negate` builder folds negation
            // into constants, keeping the tree shallow.
            expr = MirRelationExpr::Negate {
                input: Box::new(expr),
            };
        }
        let mut graph = TermGraph::default();
        let id = graph.ensure(&expr);
        // Every level has a distinct child, so nothing dedups.
        assert_eq!(graph.len(), DEPTH + 1);
        let extracted: MirRelationExpr = graph.extract(id);
        assert_eq!(graph.ensure(&extracted), id);
        assert_eq!(graph.len(), DEPTH + 1);
        dismantle_relation(expr);
        dismantle_relation(extracted);
    }
}
