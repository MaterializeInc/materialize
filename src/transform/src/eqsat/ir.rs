// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A faithful *subset* of Materialize's MIR relational algebra.
//!
//! The goal of this crate is to reason about **relational** rewrites, so the
//! scalar payloads are the real [`mz_expr::MirScalarExpr`]s, wrapped in
//! [`EScalar`] alongside one precomputed fact (`lit`). The relational facts the
//! rewrite rules read (column support, whether the scalar is a bare column
//! reference) are computed live off the expression; column remapping is the
//! real `MirScalarExpr::permute`.
//!
//! The relational variants mirror [`mz_expr::MirRelationExpr`] one-to-one for
//! the operators we model, with one addition: [`Rel::WcoJoin`], a *physical*
//! marker for a worst-case-optimal (generic / leapfrog-triejoin) multiway join.
//! `Join` and `WcoJoin` are semantically identical; they differ only in cost.
//! Unsupported subtrees are carried verbatim in [`Rel::Opaque`].

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use mz_expr::{AggregateExpr, ColumnOrder, Columns, MirRelationExpr, MirScalarExpr, TableFunc};
use mz_ore::treat_as_equal::TreatAsEqual;
use mz_repr::ReprColumnType;

/// A column index, 0-based, just like MIR's `#n`.
pub type Col = usize;

/// The non-input payload of a [`Rel::TopK`], carried verbatim.
///
/// `TopK` is a structural passthrough: no rule rewrites it, so its payload is
/// opaque and rides along unchanged through lower and raise. Modeling it as a
/// node (rather than an opaque leaf) lets the optimizer rewrite the subtree
/// below it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TopKShape {
    pub group_key: Vec<Col>,
    pub order_key: Vec<ColumnOrder>,
    pub limit: Option<MirScalarExpr>,
    pub offset: usize,
    pub monotonic: bool,
    pub expected_group_size: Option<u64>,
}

/// A scalar payload: the reduced [`MirScalarExpr`] plus one precomputed fact.
///
/// `expr` is the authoritative value (raise returns it verbatim). It is the
/// `MirScalarExpr::reduce` canonical form computed once at lower time against
/// the column types of the relation the scalar is evaluated over, so the
/// e-graph carries `ReduceScalars`-equivalent payloads. The column support and
/// bare-column-reference facts the rewrite rules read are computed live from
/// `expr` (see [`EScalar::cols`], [`EScalar::is_col`]).
///
/// `lit` records whether `expr` folds to a literal `true`/`false` against the
/// column types of the relation it is evaluated over. The e-graph tracks only
/// arities, not column types, so this nullability-sensitive fact cannot be
/// recomputed there; it is set once at lower time and carried through column
/// permutation unchanged (permuting columns cannot change literal-ness).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EScalar {
    pub expr: MirScalarExpr,
    pub lit: Option<bool>,
}

impl EScalar {
    /// A scalar with a precomputed `lit` fact.
    pub fn new(expr: MirScalarExpr, lit: Option<bool>) -> Self {
        EScalar { expr, lit }
    }

    /// A scalar with no precomputed `lit` fact (join equivalences, group keys,
    /// and the engine's own unit tests).
    pub fn plain(expr: MirScalarExpr) -> Self {
        EScalar { expr, lit: None }
    }

    /// The canonical `lit` fact for `expr`: `Some(true)`/`Some(false)` iff `expr`
    /// is syntactically the literal `true`/`false`, else `None`.
    ///
    /// `lit` MUST be exactly this deterministic function of the expr wherever an
    /// `EScalar` is cached by scalar e-class (`eqsat::egraph::CombinedData`), so
    /// that re-interning the same class (hash-consing shares it) can never store
    /// a different `lit` depending on insertion order. The lower-time
    /// `reduced_escalar` computes `lit` this way (after reducing against the
    /// column types), and the combined-graph `intern_scalar` re-derives it here.
    pub fn lit_of(expr: &MirScalarExpr) -> Option<bool> {
        if expr.is_literal_true() {
            Some(true)
        } else if expr.is_literal_false() {
            Some(false)
        } else {
            None
        }
    }

    /// The columns the scalar references.
    pub fn cols(&self) -> BTreeSet<Col> {
        self.expr.support()
    }

    /// The column index if the scalar is a bare column reference `#k`.
    pub fn is_col(&self) -> Option<Col> {
        self.expr.as_column()
    }

    /// The largest column referenced, plus one (0 if none) — a lower bound on
    /// the arity of any relation this scalar can be evaluated against.
    pub fn min_arity(&self) -> usize {
        self.cols().iter().copied().max().map_or(0, |c| c + 1)
    }

    /// Apply a column-index function to every referenced column, returning a new
    /// scalar with the remapped expression. The `lit` fact is preserved.
    /// Errors if any column maps to a negative index.
    pub fn permute_cols(&self, mut f: impl FnMut(Col) -> i64) -> Result<EScalar, String> {
        let mut map: BTreeMap<usize, usize> = BTreeMap::new();
        for c in self.cols() {
            let nc = f(c);
            if nc < 0 {
                return Err(format!(
                    "column shift produced negative index for `{}`",
                    self.expr
                ));
            }
            map.insert(
                c,
                usize::try_from(nc).expect("non-negative index fits usize"),
            );
        }
        let mut expr = self.expr.clone();
        expr.permute_map(&map);
        Ok(EScalar {
            expr,
            lit: self.lit,
        })
    }

    /// A name-sensitive ordering key over the displayed column names, in a fixed
    /// pre-order traversal of the expression.
    ///
    /// `MirScalarExpr::Column` carries a `TreatAsEqual` display name that is inert
    /// for `EScalar`'s derived `Eq`/`Ord` (and for congruence). Two scalars that
    /// differ only in column-name display are therefore equal under `Ord`, which
    /// cannot break the tie. This key compares the names themselves, so the
    /// `escalar` cache can keep a deterministic representative (the minimum key)
    /// instead of an order-dependent last-write-wins value. Two `EScalar`s that
    /// are equal under the derived `Eq` have identical structure, so this pre-order
    /// visits their columns in aligned positions and the keys are directly
    /// comparable.
    ///
    /// Each element is `(name.is_none(), name)`. `Option`'s derived order is
    /// `None < Some(..)`, but we want the representative to PREFER a present name
    /// over an absent one (dropping names was empirically worse than keeping an
    /// arbitrary one), then the lexicographically-smallest present name. The
    /// `is_none()` flag (`false < true`) makes `Some` sort before `None`, so the
    /// minimum key keeps a name whenever any interned use of the class had one.
    pub fn name_key(&self) -> Vec<(bool, Option<Arc<str>>)> {
        let mut names = Vec::new();
        self.expr.visit_pre(|e| {
            if let MirScalarExpr::Column(_, TreatAsEqual(name)) = e {
                names.push((name.is_none(), name.clone()));
            }
        });
        names
    }
}

impl fmt::Display for EScalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)
    }
}

/// Which iteration of a `LetRec` binding a recursive reference reads.
///
/// Materialize WMR is sequential: within an iteration a binding sees the
/// current value of bindings ordered before it and the previous value of
/// itself and bindings ordered after it. The body reads the converged value.
/// Tagging the reference makes that distinction explicit in the e-node, so
/// congruence does not merge reads of different versions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RecVersion {
    /// Previous iteration (the differential feedback variable).
    Prev,
    /// Current iteration (an earlier binding, already bound this iteration).
    Cur,
    /// Converged fixpoint value, read from the `LetRec` body.
    Final,
}

/// A relational expression: a subset of `MirRelationExpr`.
///
/// `Ord` is derived so extraction can break cost ties deterministically (the
/// e-graph is stored in hash maps with randomized iteration order).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rel {
    /// A literal collection of `card` rows over `arity` columns.
    ///
    /// We do not track the actual row data (irrelevant to relational rewrites),
    /// only the cardinality and arity. `col_types`, when present, carries the
    /// real column types of the relation this node stands in for. Saturation
    /// rules (`empty_false_filter`, `union_cancel`) synthesize an `Empty(r)` as a
    /// `Constant { card: 0, .. }`, replacing a relation `r` that is gone by raise
    /// time. Capturing `r`'s column types here lets raise emit an empty relation
    /// of the correct type rather than defaulting every column. `None` marks a
    /// node with no captured types (engine unit tests build these directly); raise
    /// then falls back to an arity-only placeholder type.
    Constant {
        card: u64,
        arity: usize,
        col_types: Option<Vec<ReprColumnType>>,
    },
    /// A reference to a named base collection. Cardinality comes from the
    /// `crate::eqsat::cost::Stats` oracle.
    Get { name: String, arity: usize },
    /// Retain only `outputs` columns, in that order.
    Project { input: Box<Rel>, outputs: Vec<Col> },
    /// Append `scalars` as new columns. Columns of `input` keep their indices.
    Map {
        input: Box<Rel>,
        scalars: Vec<EScalar>,
    },
    /// Apply `func` to each row of `input`, appending the result columns. Each
    /// call to `func` may produce zero or more output rows; the output arity is
    /// `input.arity() + func.output_arity()`. The `exprs` are the arguments
    /// passed to `func`, evaluated against the input row.
    FlatMap {
        input: Box<Rel>,
        func: TableFunc,
        exprs: Vec<EScalar>,
    },
    /// Keep rows where every predicate holds.
    Filter {
        input: Box<Rel>,
        predicates: Vec<EScalar>,
    },
    /// An equi-join over the cross product of `inputs`, constrained by
    /// `equivalences` (each class is a list of scalars that must be equal).
    /// `WcoJoin` is the same operator with a different physical strategy.
    Join {
        inputs: Vec<Rel>,
        equivalences: Vec<Vec<EScalar>>,
    },
    /// A worst-case-optimal (generic / leapfrog-triejoin) join. Semantically
    /// identical to [`Rel::Join`]; only the cost model treats it differently.
    WcoJoin {
        inputs: Vec<Rel>,
        equivalences: Vec<Vec<EScalar>>,
    },
    /// Arrange `input` by `key`. A multiset identity: it produces exactly the
    /// rows of `input`, materialized as an arrangement keyed by `key`. The only
    /// effect is physical (a maintained index), so it carries a memory cost but
    /// changes no relational fact. A single key list per node; a MIR `ArrangeBy`
    /// with several keys lowers to one node per key (or stays `Opaque`).
    ArrangeBy { input: Box<Rel>, key: Vec<EScalar> },
    /// A multi-key arrangement: one MIR `ArrangeBy` with two or more key lists.
    /// Each key list is one maintained index; the node is a multiset identity
    /// just like [`Rel::ArrangeBy`]. Single-key arrangements stay `Rel::ArrangeBy`
    /// so the `arrange_idempotent` rule and DSL remain valid.
    ArrangeByMany {
        input: Box<Rel>,
        keys: Vec<Vec<EScalar>>,
    },
    /// A literal-constraint filter committed to an index lookup. Semantically
    /// identical to `Filter[predicates](input)` where `input` is a global `Get`:
    /// it carries the same `input` and `predicates`, so every relational fact and
    /// every analysis treats it exactly like that `Filter`. The only differences
    /// are physical: its cost is a cheap point lookup rather than a scan, and
    /// `committed` holds the production `LiteralConstraints` realization (the
    /// semi-join `Join { implementation: IndexedFilter(..) }` wrapped in its MFP),
    /// which physical raise emits verbatim. The node is only ever seeded in the
    /// physical pass. The logical pass never produces it.
    IndexedFilter {
        input: Box<Rel>,
        predicates: Vec<EScalar>,
        committed: Box<MirRelationExpr>,
    },
    /// Group by `group_key`, producing the `aggregates` aggregate columns.
    /// `monotonic` and `expected_group_size` are physical hints carried
    /// verbatim for a faithful round-trip; no rule reads them.
    Reduce {
        input: Box<Rel>,
        group_key: Vec<EScalar>,
        aggregates: Vec<AggregateExpr>,
        monotonic: bool,
        expected_group_size: Option<u64>,
    },
    /// Group, order within each group, and limit. A structural passthrough: its
    /// `shape` is opaque and no rule rewrites it, but its `input` is optimized.
    TopK { input: Box<Rel>, shape: TopKShape },
    /// An unsupported subtree, carried verbatim. The supported envelope around
    /// it still saturates; raise re-emits the stored `MirRelationExpr`.
    /// Hash-consing dedups identical opaque leaves.
    Opaque(Box<MirRelationExpr>),
    /// Add the multiplicities of `base` and every relation in `inputs`.
    Union { base: Box<Rel>, inputs: Vec<Rel> },
    /// Negate every multiplicity.
    Negate { input: Box<Rel> },
    /// Drop rows whose accumulated multiplicity is not positive.
    Threshold { input: Box<Rel> },
    /// Bind `value` to a local id, available as [`Rel::LocalGet`] in `body`.
    /// Introduced by extraction-time CSE (see [`crate::eqsat::cse`]); the rewrite
    /// rules never produce it.
    Let {
        id: usize,
        value: Box<Rel>,
        body: Box<Rel>,
    },
    /// Mutually-recursive bindings, each `(id, value)`, in scope in every
    /// `value` and in `body`. Evaluated as the least fixpoint from the empty
    /// collection (differential-dataflow `iterate`). The recursive references
    /// are [`Rel::LocalGet`]s of the bound ids; this is the only operator that
    /// closes a genuine cycle in the plan.
    LetRec {
        bindings: Vec<(usize, Rel)>,
        // limits[i] is the per-binding iteration limit for bindings[i], carried
        // verbatim so raise reconstructs the MIR LetRec faithfully.
        // It is payload, not a child.
        limits: Vec<Option<mz_expr::LetRecLimit>>,
        body: Box<Rel>,
    },
    /// A reference to a [`Rel::Let`]-bound local. Carries its `arity` so the
    /// tree remains context-free to traverse, and the original
    /// `MirRelationExpr::Get { Id::Local(..) }` node (`get`) so raise can return
    /// it verbatim, preserving its type. `get` is `None` for the engine's scope
    /// placeholders (substituted out before raise) and cse-introduced locals.
    LocalGet {
        id: usize,
        arity: usize,
        get: Option<Box<MirRelationExpr>>,
        /// `None` for ordinary `Let` locals and CSE placeholders. `Some(_)` for a
        /// reference bound by an enclosing `LetRec`, classified by iteration
        /// version. Only set when `enable_eqsat_wmr_lift` is on.
        version: Option<RecVersion>,
    },
}

impl Rel {
    /// The number of output columns.
    pub fn arity(&self) -> usize {
        match self {
            Rel::Constant { arity, .. } | Rel::Get { arity, .. } => *arity,
            Rel::Project { outputs, .. } => outputs.len(),
            Rel::Map { input, scalars } => input.arity() + scalars.len(),
            Rel::FlatMap { input, func, .. } => input.arity() + func.output_arity(),
            Rel::Filter { input, .. }
            | Rel::Negate { input }
            | Rel::Threshold { input }
            | Rel::ArrangeBy { input, .. }
            | Rel::ArrangeByMany { input, .. }
            | Rel::IndexedFilter { input, .. }
            | Rel::TopK { input, .. } => input.arity(),
            Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => {
                inputs.iter().map(Rel::arity).sum()
            }
            Rel::Reduce {
                group_key,
                aggregates,
                ..
            } => group_key.len() + aggregates.len(),
            Rel::Union { base, .. } => base.arity(),
            Rel::Opaque(m) => m.arity(),
            Rel::Let { body, .. } | Rel::LetRec { body, .. } => body.arity(),
            Rel::LocalGet { arity, .. } => *arity,
        }
    }

    /// The children of this node, for generic traversal.
    pub fn children(&self) -> Vec<&Rel> {
        match self {
            Rel::Constant { .. } | Rel::Get { .. } | Rel::Opaque(_) => vec![],
            Rel::Project { input, .. }
            | Rel::Map { input, .. }
            | Rel::FlatMap { input, .. }
            | Rel::Filter { input, .. }
            | Rel::Reduce { input, .. }
            | Rel::TopK { input, .. }
            | Rel::ArrangeBy { input, .. }
            | Rel::ArrangeByMany { input, .. }
            | Rel::IndexedFilter { input, .. }
            | Rel::Negate { input }
            | Rel::Threshold { input } => vec![input],
            Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => inputs.iter().collect(),
            Rel::Union { base, inputs } => {
                let mut v = vec![&**base];
                v.extend(inputs.iter());
                v
            }
            Rel::Let { value, body, .. } => vec![value, body],
            Rel::LetRec { bindings, body, .. } => {
                let mut v: Vec<&Rel> = bindings.iter().map(|(_, r)| r).collect();
                v.push(body);
                v
            }
            Rel::LocalGet { .. } => vec![],
        }
    }

    /// Replace the children of this node with `new`, preserving order. The
    /// length of `new` must match [`Rel::children`].
    pub fn with_children(&self, mut new: Vec<Rel>) -> Rel {
        let mut take = |i: usize| {
            std::mem::replace(
                &mut new[i],
                Rel::Constant {
                    card: 0,
                    arity: 0,
                    col_types: None,
                },
            )
        };
        match self {
            Rel::Constant { .. } | Rel::Get { .. } | Rel::Opaque(_) => {
                assert!(new.is_empty());
                self.clone()
            }
            Rel::Project { outputs, .. } => Rel::Project {
                input: Box::new(take(0)),
                outputs: outputs.clone(),
            },
            Rel::Map { scalars, .. } => Rel::Map {
                input: Box::new(take(0)),
                scalars: scalars.clone(),
            },
            Rel::FlatMap { func, exprs, .. } => Rel::FlatMap {
                input: Box::new(take(0)),
                func: func.clone(),
                exprs: exprs.clone(),
            },
            Rel::Filter { predicates, .. } => Rel::Filter {
                input: Box::new(take(0)),
                predicates: predicates.clone(),
            },
            Rel::Reduce {
                group_key,
                aggregates,
                monotonic,
                expected_group_size,
                ..
            } => Rel::Reduce {
                input: Box::new(take(0)),
                group_key: group_key.clone(),
                aggregates: aggregates.clone(),
                monotonic: *monotonic,
                expected_group_size: *expected_group_size,
            },
            Rel::TopK { shape, .. } => Rel::TopK {
                input: Box::new(take(0)),
                shape: shape.clone(),
            },
            Rel::Negate { .. } => Rel::Negate {
                input: Box::new(take(0)),
            },
            Rel::Threshold { .. } => Rel::Threshold {
                input: Box::new(take(0)),
            },
            Rel::ArrangeBy { key, .. } => Rel::ArrangeBy {
                input: Box::new(take(0)),
                key: key.clone(),
            },
            Rel::ArrangeByMany { keys, .. } => Rel::ArrangeByMany {
                input: Box::new(take(0)),
                keys: keys.clone(),
            },
            Rel::IndexedFilter {
                predicates,
                committed,
                ..
            } => Rel::IndexedFilter {
                input: Box::new(take(0)),
                predicates: predicates.clone(),
                committed: committed.clone(),
            },
            Rel::Join { equivalences, .. } => Rel::Join {
                inputs: new,
                equivalences: equivalences.clone(),
            },
            Rel::WcoJoin { equivalences, .. } => Rel::WcoJoin {
                inputs: new,
                equivalences: equivalences.clone(),
            },
            Rel::Union { .. } => {
                let base = Box::new(take(0));
                let inputs = new.split_off(1);
                Rel::Union { base, inputs }
            }
            Rel::Let { id, .. } => Rel::Let {
                id: *id,
                value: Box::new(take(0)),
                body: Box::new(take(1)),
            },
            Rel::LetRec {
                bindings, limits, ..
            } => {
                let ids: Vec<usize> = bindings.iter().map(|(id, _)| *id).collect();
                let limits = limits.clone();
                let body = Box::new(take(bindings.len()));
                let values = (0..bindings.len()).map(&mut take).collect::<Vec<_>>();
                Rel::LetRec {
                    bindings: ids.into_iter().zip(values).collect(),
                    limits,
                    body,
                }
            }
            Rel::LocalGet { .. } => {
                assert!(new.is_empty());
                self.clone()
            }
        }
    }

    /// Total node count (size of the tree). Used as a structural tie-breaker in
    /// the cost model so that simplifications which remove a node are always
    /// strictly cheaper.
    pub fn node_count(&self) -> usize {
        1 + self
            .children()
            .iter()
            .map(|c| c.node_count())
            .sum::<usize>()
    }

    /// Total scalar tree-size carried by this node and its relational subtree.
    ///
    /// Sums [`scalar_expr_cost`] over every scalar payload — `EScalar` payloads via
    /// `.expr`, and the one bare `MirScalarExpr` payload (`TopKShape.limit`)
    /// directly — then recurses over relational children. `Project.outputs` (`Col`),
    /// `Constant` rows (literals), and `TopKShape.group_key`/`order_key`
    /// (`Col`/`ColumnOrder`) carry no scalar trees and are not counted.
    ///
    /// Used only by [`crate::eqsat::cost::CostModel::cost`] to make the `nodes`
    /// tie-breaker scalar-aware. Deliberately NOT folded into [`Self::node_count`],
    /// which is the CSE ordering key (`cse.rs`) and must stay relational-only.
    pub fn scalar_node_count(&self) -> usize {
        let here: usize = match self {
            Rel::Map { scalars, .. } => scalars.iter().map(|s| scalar_expr_cost(&s.expr)).sum(),
            Rel::Filter { predicates, .. } | Rel::IndexedFilter { predicates, .. } => {
                predicates.iter().map(|s| scalar_expr_cost(&s.expr)).sum()
            }
            Rel::Join { equivalences, .. } | Rel::WcoJoin { equivalences, .. } => equivalences
                .iter()
                .flatten()
                .map(|s| scalar_expr_cost(&s.expr))
                .sum(),
            Rel::FlatMap { exprs, .. } => exprs.iter().map(|s| scalar_expr_cost(&s.expr)).sum(),
            Rel::ArrangeBy { key, .. } => key.iter().map(|s| scalar_expr_cost(&s.expr)).sum(),
            Rel::ArrangeByMany { keys, .. } => keys
                .iter()
                .flatten()
                .map(|s| scalar_expr_cost(&s.expr))
                .sum(),
            Rel::Reduce {
                group_key,
                aggregates,
                ..
            } => {
                group_key
                    .iter()
                    .map(|s| scalar_expr_cost(&s.expr))
                    .sum::<usize>()
                    + aggregates
                        .iter()
                        .map(|a| scalar_expr_cost(&a.expr))
                        .sum::<usize>()
            }
            Rel::TopK { shape, .. } => shape.limit.as_ref().map_or(0, scalar_expr_cost),
            _ => 0,
        };
        here + self
            .children()
            .iter()
            .map(|c| c.scalar_node_count())
            .sum::<usize>()
    }

    /// Pretty-print the plan as an indented tree.
    fn pretty(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let pad = "  ".repeat(indent);
        let scalars = |xs: &[EScalar]| {
            xs.iter()
                .map(|s| s.expr.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let aggs = |xs: &[AggregateExpr]| {
            xs.iter()
                .map(|a| a.expr.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self {
            Rel::Constant { card, arity, .. } => {
                writeln!(f, "{pad}Constant rows={card} arity={arity}")?;
            }
            Rel::Get { name, arity } => writeln!(f, "{pad}Get {name} arity={arity}")?,
            Rel::Opaque(m) => writeln!(f, "{pad}Opaque arity={}", m.arity())?,
            Rel::Project { input, outputs } => {
                writeln!(f, "{pad}Project {outputs:?}")?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Map { input, scalars: s } => {
                writeln!(f, "{pad}Map [{}]", scalars(s))?;
                input.pretty(f, indent + 1)?;
            }
            Rel::FlatMap {
                input,
                func,
                exprs: s,
            } => {
                writeln!(f, "{pad}FlatMap {func:?} [{}]", scalars(s))?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Filter { input, predicates } => {
                writeln!(f, "{pad}Filter [{}]", scalars(predicates))?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } => {
                writeln!(
                    f,
                    "{pad}Reduce group_by=[{}] aggregates=[{}]",
                    scalars(group_key),
                    aggs(aggregates)
                )?;
                input.pretty(f, indent + 1)?;
            }
            Rel::TopK { input, shape } => {
                writeln!(
                    f,
                    "{pad}TopK group_by={:?} limit={:?} offset={}",
                    shape.group_key, shape.limit, shape.offset
                )?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Negate { input } => {
                writeln!(f, "{pad}Negate")?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Threshold { input } => {
                writeln!(f, "{pad}Threshold")?;
                input.pretty(f, indent + 1)?;
            }
            Rel::ArrangeBy { input, key } => {
                writeln!(f, "{pad}ArrangeBy [{}]", scalars(key))?;
                input.pretty(f, indent + 1)?;
            }
            Rel::ArrangeByMany { input, keys } => {
                let key_strs: Vec<String> =
                    keys.iter().map(|k| format!("[{}]", scalars(k))).collect();
                writeln!(f, "{pad}ArrangeByMany {}", key_strs.join(", "))?;
                input.pretty(f, indent + 1)?;
            }
            Rel::IndexedFilter {
                input, predicates, ..
            } => {
                writeln!(f, "{pad}IndexedFilter [{}]", scalars(predicates))?;
                input.pretty(f, indent + 1)?;
            }
            Rel::Join {
                inputs,
                equivalences,
            }
            | Rel::WcoJoin {
                inputs,
                equivalences,
            } => {
                let kind = if matches!(self, Rel::WcoJoin { .. }) {
                    "WcoJoin"
                } else {
                    "Join"
                };
                let eqs = equivalences
                    .iter()
                    .map(|c| format!("({})", scalars(c)))
                    .collect::<Vec<_>>()
                    .join(" ");
                writeln!(f, "{pad}{kind} on={eqs}")?;
                for i in inputs {
                    i.pretty(f, indent + 1)?;
                }
            }
            Rel::Union { base, inputs } => {
                writeln!(f, "{pad}Union")?;
                base.pretty(f, indent + 1)?;
                for i in inputs {
                    i.pretty(f, indent + 1)?;
                }
            }
            Rel::Let { id, value, body } => {
                writeln!(f, "{pad}Let l{id} =")?;
                value.pretty(f, indent + 1)?;
                writeln!(f, "{pad}in")?;
                body.pretty(f, indent + 1)?;
            }
            Rel::LetRec { bindings, body, .. } => {
                writeln!(f, "{pad}LetRec")?;
                for (id, value) in bindings {
                    writeln!(f, "{pad}  l{id} =")?;
                    value.pretty(f, indent + 2)?;
                }
                writeln!(f, "{pad}in")?;
                body.pretty(f, indent + 1)?;
            }
            Rel::LocalGet { id, arity, .. } => writeln!(f, "{pad}Get l{id} arity={arity}")?,
        }
        Ok(())
    }
}

impl fmt::Display for Rel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.pretty(f, 0)
    }
}

/// Tree-size cost of a scalar expr (node count); lower is cheaper. The single
/// canonical scalar cost, shared by the cost model's `nodes` tie-breaker
/// ([`crate::eqsat::cost::CostModel::cost`]) and the colored substitution's
/// cheapest-spelling choice ([`crate::eqsat::colored_derive::resolve_scalar_colored`]).
pub(crate) fn scalar_expr_cost(expr: &MirScalarExpr) -> usize {
    let mut n = 0;
    expr.visit_pre(|_| n += 1);
    n
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn scalar_node_count_sums_payloads_and_recurses() {
        use mz_expr::{BinaryFunc, func};
        use mz_repr::{Datum, ReprScalarType};
        // `#1 + 1` is a 3-node scalar (Add, column, literal); `#0` is 1 node.
        let lit1 = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let compute =
            MirScalarExpr::column(1).call_binary(lit1, BinaryFunc::AddInt64(func::AddInt64));
        let get = Rel::Get {
            name: "r".to_string(),
            arity: 2,
        };
        let map_compute = Rel::Map {
            scalars: vec![EScalar::plain(compute.clone())],
            input: Box::new(get.clone()),
        };
        let map_col = Rel::Map {
            scalars: vec![EScalar::plain(MirScalarExpr::column(0))],
            input: Box::new(get.clone()),
        };
        // Get carries no scalar payload.
        assert_eq!(get.scalar_node_count(), 0);
        // Map[#1 + 1] = 3; Map[#0] = 1.
        assert_eq!(map_compute.scalar_node_count(), 3);
        assert_eq!(map_col.scalar_node_count(), 1);
        // Recursion: Filter over the computing Map sums both levels (filter pred #0 = 1 is 3).
        let pred = MirScalarExpr::column(0).call_binary(
            MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
            BinaryFunc::Eq(func::Eq),
        );
        let filt = Rel::Filter {
            predicates: vec![EScalar::plain(pred)],
            input: Box::new(map_compute.clone()),
        };
        assert_eq!(filt.scalar_node_count(), 3 + 3);
    }

    #[mz_ore::test]
    fn rec_version_distinguishes_localgets() {
        let prev = Rel::LocalGet {
            id: 0,
            arity: 1,
            get: None,
            version: Some(RecVersion::Prev),
        };
        let cur = Rel::LocalGet {
            id: 0,
            arity: 1,
            get: None,
            version: Some(RecVersion::Cur),
        };
        // Same id, different version: not equal, so congruence keeps them apart.
        assert_ne!(prev, cur);
        // Version None is the default, flag-off form.
        let plain = Rel::LocalGet {
            id: 0,
            arity: 1,
            get: None,
            version: None,
        };
        assert_ne!(plain, prev);
    }
}

#[cfg(test)]
mod escalar_tests {
    use super::*;

    fn col(c: usize) -> MirScalarExpr {
        MirScalarExpr::column(c)
    }

    #[mz_ore::test]
    fn cols_and_is_col_are_live() {
        let s = EScalar::plain(col(3));
        assert_eq!(s.cols().into_iter().collect::<Vec<_>>(), vec![3]);
        assert_eq!(s.is_col(), Some(3));
    }

    #[mz_ore::test]
    fn permute_cols_remaps_the_expression() {
        // #3 shifted by -2 becomes #1; the lit fact is preserved.
        let s = EScalar::new(col(3), Some(true));
        let out = s.permute_cols(|c| i64::try_from(c).unwrap() - 2).unwrap();
        assert_eq!(out.is_col(), Some(1));
        assert_eq!(out.lit, Some(true));
    }

    #[mz_ore::test]
    fn permute_cols_rejects_negative_target() {
        let s = EScalar::plain(col(0));
        assert!(s.permute_cols(|c| i64::try_from(c).unwrap() - 1).is_err());
    }
}
