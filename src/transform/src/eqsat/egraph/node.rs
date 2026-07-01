// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! E-node and operator-symbol types for the relational e-graph.

use mz_expr::{AggregateExpr, MirRelationExpr, TableFunc};
use mz_repr::ReprColumnType;

use crate::eqsat::core::Id;
use crate::eqsat::ir::RecVersion;

/// A node in the e-graph: an operator whose children are e-class ids. Mirrors
/// [`Rel`], with `Union` flattened to a single non-empty input list.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ENode {
    Constant {
        card: u64,
        arity: usize,
        /// The real column types of the relation this node stands in for, when
        /// known. Saturation rules synthesize an `Empty(r)` as a
        /// `Constant { card: 0, .. }`; capturing `r`'s column types here lets
        /// raise emit an empty relation of the correct type. `None` marks a node
        /// with no captured types (raise falls back to an arity-only placeholder).
        col_types: Option<Vec<ReprColumnType>>,
    },
    Get {
        name: String,
        arity: usize,
    },
    Project {
        input: Id,
        outputs: Vec<usize>,
    },
    Map {
        input: Id,
        /// Scalar e-class ids of the appended scalars (resolve via the cache).
        scalars: Vec<Id>,
    },
    /// Apply `func` to each row of `input`, appending result columns (see
    /// [`Rel::FlatMap`]). `func` is an opaque payload; `exprs` are scalar e-class
    /// ids; only `input` is a relational child.
    FlatMap {
        input: Id,
        func: TableFunc,
        exprs: Vec<Id>,
    },
    Filter {
        input: Id,
        /// Scalar e-class ids of the predicates (resolve via the cache).
        predicates: Vec<Id>,
    },
    Reduce {
        input: Id,
        /// Scalar e-class ids of the group-key expressions (resolve via the cache).
        group_key: Vec<Id>,
        aggregates: Vec<AggregateExpr>,
        monotonic: bool,
        expected_group_size: Option<u64>,
    },
    TopK {
        input: Id,
        shape: crate::eqsat::ir::TopKShape,
    },
    Negate {
        input: Id,
    },
    Threshold {
        input: Id,
    },
    Join {
        inputs: Vec<Id>,
        /// Per equivalence class, the scalar e-class ids it equates (resolve via
        /// the cache).
        equivalences: Vec<Vec<Id>>,
    },
    WcoJoin {
        inputs: Vec<Id>,
        equivalences: Vec<Vec<Id>>,
    },
    /// Arrange `input` by `key`. A multiset identity (see [`Rel::ArrangeBy`]);
    /// `key` is part of the e-node identity so arrangements by different keys
    /// stay distinct e-nodes. `key` holds scalar e-class ids (resolve via cache).
    ArrangeBy {
        input: Id,
        key: Vec<Id>,
    },
    /// A multi-key arrangement (see [`Rel::ArrangeByMany`]). Each entry in
    /// `keys` is one maintained index; `keys` is payload (part of e-node
    /// identity). Single-key arrangements stay `ENode::ArrangeBy`. Each key holds
    /// scalar e-class ids (resolve via cache).
    ArrangeByMany {
        input: Id,
        keys: Vec<Vec<Id>>,
    },
    /// A literal-constraint filter committed to an index lookup (see
    /// [`Rel::IndexedFilter`]). Semantically a `Filter[predicates](input)`: it
    /// shares `input` and `predicates` with the `Filter` it is seeded alongside,
    /// so every analysis treats the two identically. `committed` is the
    /// production realization emitted verbatim by physical raise and is part of
    /// the e-node identity (it never unifies, like an opaque payload).
    IndexedFilter {
        input: Id,
        /// Scalar e-class ids of the predicates (resolve via the cache).
        predicates: Vec<Id>,
        committed: Box<MirRelationExpr>,
    },
    Union {
        inputs: Vec<Id>,
    },
    /// An unsupported subtree carried verbatim (see [`Rel::Opaque`]). An opaque
    /// leaf; hash-consing dedups identical subtrees.
    Opaque(MirRelationExpr),
    /// A reference to a `LetRec`/`Let`-bound local. An opaque leaf inside a
    /// Let-free fragment (the structural optimizer saturates fragments and
    /// peels the binding scopes around them). `get` carries the original node
    /// for raise (`None` for engine scope placeholders).
    ///
    /// `version` is part of the node identity. Two references to the same id
    /// that differ only in iteration version (`Prev(j)` vs `Cur(j)`) must NOT be
    /// congruent: they denote the previous and current value of the same
    /// recursive binding, which differ during iteration. Carrying it here (the
    /// e-graph interns `ENode`, not `Rel`) makes congruence keep them in
    /// distinct e-classes. `None` for ordinary `Let` locals and CSE
    /// placeholders, so with the lift flag off hashing is byte-identical to
    /// before.
    LocalGet {
        id: usize,
        arity: usize,
        get: Option<Box<MirRelationExpr>>,
        version: Option<RecVersion>,
    },
}

/// The operator symbol of an e-node (its relation in the relational view).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Sym {
    Constant,
    Get,
    Project,
    Map,
    FlatMap,
    Filter,
    Reduce,
    Negate,
    Threshold,
    Join,
    WcoJoin,
    ArrangeBy,
    ArrangeByMany,
    IndexedFilter,
    Union,
    TopK,
    Opaque,
    LocalGet,
}

impl ENode {
    // `pub(crate)` (not `pub(super)`) so the colored view (`eqsat::colored::view`,
    // which is not a descendant of `eqsat::egraph`) can bucket colored e-nodes by
    // operator symbol when building its per-color match index (SP4d T5).
    pub(crate) fn sym(&self) -> Sym {
        match self {
            ENode::Constant { .. } => Sym::Constant,
            ENode::Get { .. } => Sym::Get,
            ENode::Project { .. } => Sym::Project,
            ENode::Map { .. } => Sym::Map,
            ENode::FlatMap { .. } => Sym::FlatMap,
            ENode::Filter { .. } => Sym::Filter,
            ENode::Reduce { .. } => Sym::Reduce,
            ENode::Negate { .. } => Sym::Negate,
            ENode::Threshold { .. } => Sym::Threshold,
            ENode::Join { .. } => Sym::Join,
            ENode::WcoJoin { .. } => Sym::WcoJoin,
            ENode::ArrangeBy { .. } => Sym::ArrangeBy,
            ENode::ArrangeByMany { .. } => Sym::ArrangeByMany,
            ENode::IndexedFilter { .. } => Sym::IndexedFilter,
            ENode::Union { .. } => Sym::Union,
            ENode::TopK { .. } => Sym::TopK,
            ENode::Opaque(_) => Sym::Opaque,
            ENode::LocalGet { .. } => Sym::LocalGet,
        }
    }

    /// The relational input e-class ids in operand order (empty for leaves).
    /// Scalar references are returned separately by [`ENode::scalar_children`].
    pub fn relational_children(&self) -> Vec<Id> {
        match self {
            ENode::Constant { .. }
            | ENode::Get { .. }
            | ENode::Opaque(_)
            | ENode::LocalGet { .. } => vec![],
            ENode::Project { input, .. }
            | ENode::Map { input, .. }
            | ENode::FlatMap { input, .. }
            | ENode::Filter { input, .. }
            | ENode::Reduce { input, .. }
            | ENode::TopK { input, .. }
            | ENode::ArrangeBy { input, .. }
            | ENode::ArrangeByMany { input, .. }
            | ENode::IndexedFilter { input, .. }
            | ENode::Negate { input }
            | ENode::Threshold { input } => vec![*input],
            ENode::Join { inputs, .. }
            | ENode::WcoJoin { inputs, .. }
            | ENode::Union { inputs } => inputs.clone(),
        }
    }

    /// Scalar e-class references this node carries, in a fixed order:
    /// Map.scalars; Filter/IndexedFilter.predicates; FlatMap.exprs;
    /// Reduce.group_key; ArrangeBy.key; ArrangeByMany.keys (flattened);
    /// Join/WcoJoin.equivalences (flattened). Empty for nodes with none.
    pub fn scalar_children(&self) -> Vec<Id> {
        match self {
            ENode::Map { scalars, .. } => scalars.clone(),
            ENode::Filter { predicates, .. } | ENode::IndexedFilter { predicates, .. } => {
                predicates.clone()
            }
            ENode::FlatMap { exprs, .. } => exprs.clone(),
            ENode::Reduce { group_key, .. } => group_key.clone(),
            ENode::ArrangeBy { key, .. } => key.clone(),
            ENode::ArrangeByMany { keys, .. } => keys.iter().flatten().copied().collect(),
            ENode::Join { equivalences, .. } | ENode::WcoJoin { equivalences, .. } => {
                equivalences.iter().flatten().copied().collect()
            }
            _ => Vec::new(),
        }
    }

    /// Remap both the relational inputs and the scalar `Id`s through `f`,
    /// preserving order and grouping, so the core canonicalizes scalar
    /// references the same way it canonicalizes relational children.
    pub(super) fn map_children(&self, f: impl Fn(Id) -> Id) -> ENode {
        let mut n = self.clone();
        match &mut n {
            ENode::Constant { .. }
            | ENode::Get { .. }
            | ENode::Opaque(_)
            | ENode::LocalGet { .. } => {}
            ENode::Project { input, .. } => *input = f(*input),
            ENode::Negate { input } | ENode::Threshold { input } => *input = f(*input),
            ENode::TopK { input, .. } => *input = f(*input),
            ENode::Map { input, scalars } => {
                *input = f(*input);
                for s in scalars.iter_mut() {
                    *s = f(*s);
                }
            }
            ENode::FlatMap { input, exprs, .. } => {
                *input = f(*input);
                for s in exprs.iter_mut() {
                    *s = f(*s);
                }
            }
            ENode::Filter { input, predicates }
            | ENode::IndexedFilter {
                input, predicates, ..
            } => {
                *input = f(*input);
                for s in predicates.iter_mut() {
                    *s = f(*s);
                }
            }
            ENode::Reduce {
                input, group_key, ..
            } => {
                *input = f(*input);
                for s in group_key.iter_mut() {
                    *s = f(*s);
                }
            }
            ENode::ArrangeBy { input, key } => {
                *input = f(*input);
                for s in key.iter_mut() {
                    *s = f(*s);
                }
            }
            ENode::ArrangeByMany { input, keys } => {
                *input = f(*input);
                for key in keys.iter_mut() {
                    for s in key.iter_mut() {
                        *s = f(*s);
                    }
                }
            }
            ENode::Join {
                inputs,
                equivalences,
            }
            | ENode::WcoJoin {
                inputs,
                equivalences,
            } => {
                for i in inputs.iter_mut() {
                    *i = f(*i);
                }
                for class in equivalences.iter_mut() {
                    for s in class.iter_mut() {
                        *s = f(*s);
                    }
                }
            }
            ENode::Union { inputs } => {
                for i in inputs.iter_mut() {
                    *i = f(*i);
                }
            }
        }
        n
    }
}
