// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Combined relational+scalar language types, the e-graph type alias, and
//! associated index/seed types.

use std::collections::{BTreeMap, HashMap};

use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_repr::GlobalId;

use crate::eqsat::core::{self, Id, Language};
use crate::eqsat::ir::EScalar;
use crate::eqsat::scalar::lang::{ScalarGraphData, ScalarLang, ScalarSym};
use crate::eqsat::scalar::node::SNode;

use super::node::{ENode, Sym};

/// Per-e-graph relational auxiliary state: the index-availability oracle read by
/// the indexed-filter pull-up condition.
#[derive(Default)]
pub struct RelGraphData {
    pub available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
}

/// A node in the combined relational+scalar e-graph: either a relational
/// [`ENode`] (whose scalar payload fields are scalar-class `Id`s) or a scalar
/// [`SNode`]. One [`core::EGraph`] holds both sorts in a single `Id` space.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CNode {
    Rel(ENode),
    Scalar(SNode),
}

/// The operator symbol of a [`CNode`]: a relational [`Sym`] or a scalar
/// [`ScalarSym`], kept in separate variants so the matcher index buckets the two
/// sorts apart.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum CSym {
    Rel(Sym),
    Scalar(ScalarSym),
}

/// The combined e-graph's language-owned state: the relational availability
/// oracle, the scalar per-class analysis, and the write-once `escalar` cache
/// mapping each scalar e-class to the `EScalar` fact relational code reads.
#[derive(Default)]
pub struct CombinedData {
    pub rel: RelGraphData,
    pub scalar: ScalarGraphData,
    /// The cached `EScalar` fact for each scalar e-class, populated by
    /// [`EGraph::intern_scalar`].
    ///
    /// NOT write-once: `intern_scalar` hash-conses, so the same scalar class can
    /// be interned more than once (a saturation rewrite that folds a predicate to
    /// the shared literal `true`/`false` re-interns that shared class). The cached
    /// `EScalar` MUST therefore be a deterministic function of the class's expr,
    /// independent of which intern call writes and of HashMap iteration order:
    /// - the `lit` fact is normalized via [`EScalar::lit_of`] (so a stray
    ///   `lit: None` re-intern cannot clobber a genuine `Some(true/false)` and
    ///   misfire the literal-predicate conditions on another node), and
    /// - the displayed column name (`MirScalarExpr::Column`'s `TreatAsEqual`
    ///   payload, inert for `EScalar` equality and for congruence) is resolved to
    ///   a deterministic representative in [`CombinedData::set_escalar`] — the
    ///   smallest [`EScalar::name_key`], which prefers a present name over `None`
    ///   then orders lexicographically — so EXPLAIN renders a stable name
    ///   regardless of the order classes were interned.
    escalar: HashMap<Id, EScalar>,
}

impl CombinedData {
    /// The cached `EScalar` fact for a scalar class. Panics if `id` is not a
    /// registered scalar class (a relational-code bug).
    pub fn escalar(&self, id: Id) -> &EScalar {
        self.escalar
            .get(&id)
            .expect("scalar class has a cached EScalar fact")
    }

    pub(crate) fn set_escalar(&mut self, id: Id, e: EScalar) {
        // The cached fact must be a deterministic function of the class's expr,
        // so re-interning an already-cached class cannot change the `lit`/`cols`/
        // `is_col` facts (all inert to the `TreatAsEqual` column name and so already
        // equal under `EScalar`'s derived `Eq`). Catch any future caller that
        // violates that invariant (the only caller today is `intern_scalar`, which
        // normalizes `lit` via `EScalar::lit_of`).
        debug_assert!(
            self.escalar.get(&id).is_none_or(|existing| existing == &e),
            "escalar cache overwrite must not change the cached fact for class {id}: \
             {:?} vs {:?}",
            self.escalar.get(&id),
            e,
        );
        // The displayed column name *is* inert for the assert above, so plain
        // last-write-wins would let the surviving name depend on intern/HashMap
        // order (per-process randomized) and flake EXPLAIN output. Keep a
        // deterministic representative instead: the smallest `EScalar::name_key`,
        // which prefers a present name over `None` (no silent name-stripping) and
        // then orders lexicographically. `min` is commutative and associative, so
        // the cached name is independent of intern order.
        match self.escalar.get(&id) {
            Some(existing) if existing.name_key() <= e.name_key() => {}
            _ => {
                self.escalar.insert(id, e);
            }
        }
    }
}

/// The combined relational+scalar node language.
pub struct CombinedLang;

impl Language for CombinedLang {
    type Node = CNode;
    type Sym = CSym;
    type GraphData = CombinedData;

    fn children(node: &CNode) -> Vec<Id> {
        match node {
            CNode::Rel(e) => {
                let mut c = e.relational_children();
                c.extend(e.scalar_children());
                c
            }
            CNode::Scalar(s) => s.children(),
        }
    }

    fn map_children(node: &CNode, f: impl Fn(Id) -> Id) -> CNode {
        match node {
            CNode::Rel(e) => CNode::Rel(e.map_children(&f)),
            CNode::Scalar(s) => CNode::Scalar(s.map_children(f)),
        }
    }

    fn symbol(node: &CNode) -> CSym {
        match node {
            CNode::Rel(e) => CSym::Rel(e.sym()),
            CNode::Scalar(s) => CSym::Scalar(ScalarLang::symbol(s)),
        }
    }

    fn on_add(data: &mut CombinedData, id: Id, node: &CNode, get: &dyn Fn(Id) -> Id) {
        match node {
            // Relational analysis is batch-driven (the `Analysis` framework), not
            // hook-driven, so the relational arm is a no-op. The `escalar` cache
            // is populated by `intern_scalar`, not here (the hook lacks the `lit`
            // fact).
            CNode::Rel(_) => {}
            CNode::Scalar(s) => ScalarLang::on_add(&mut data.scalar, id, s, get),
        }
    }

    fn on_union(data: &mut CombinedData, winner: Id, loser: Id) {
        // In SP4a scalar classes never merge, so a union is between two relational
        // classes (which carry no scalar analysis) almost always. Maintain the
        // scalar analysis only when the union is genuinely between scalar classes
        // — both sides carry a scalar analysis entry. Routing a relational union
        // into `ScalarLang::on_union` would panic (no analysis to merge). The
        // relational side is a no-op (batch-driven analysis).
        if data.scalar.analysis.contains_key(&winner) && data.scalar.analysis.contains_key(&loser) {
            ScalarLang::on_union(&mut data.scalar, winner, loser);
        }
    }
}

/// The combined e-graph: the generic core instantiated at [`CombinedLang`].
pub type EGraph = core::EGraph<CombinedLang>;

/// The relational matcher index: relational e-nodes grouped by their operator
/// [`Sym`], paired with their canonical class. Scalar classes are excluded (no
/// scalar rules run in SP4a), so the generated rule matchers continue to key on
/// `Sym` and bind `ENode`s directly. Built by [`EGraph::rel_index`].
pub(crate) type Index = HashMap<Sym, Vec<(Id, ENode)>>;

/// Match index for scalar e-nodes, bucketed by scalar operator symbol. Built
/// per saturation round by the scalar saturate driver, mirroring the relational
/// `Index`. Kept separate so the relational matcher never sees scalar nodes and
/// vice versa.
pub(crate) type ScalarIndex = std::collections::HashMap<ScalarSym, Vec<(Id, SNode)>>;

impl EGraph {
    /// Bucket every scalar e-node by its `ScalarSym`. One entry per (class, node).
    ///
    /// Currently unused outside tests: no scalar rule matcher runs yet. A later
    /// task wires this into the saturation driver alongside
    /// `MatchGraph::nodes_by_scalar_sym`.
    #[allow(dead_code)]
    pub(crate) fn scalar_index(&self) -> ScalarIndex {
        let mut idx: ScalarIndex = std::collections::HashMap::new();
        for id in self.class_ids() {
            for node in self.nodes(id) {
                if let CNode::Scalar(s) = node {
                    idx.entry(ScalarLang::symbol(&s)).or_default().push((id, s));
                }
            }
        }
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn lower_into_builds_scalar_nodes() {
        use mz_expr::{MirScalarExpr, UnaryFunc};
        let e = MirScalarExpr::column(0).call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let mut eg = EGraph::new();
        let root = crate::eqsat::scalar::lower::lower_into(&mut eg, &e);
        assert!(
            eg.nodes(root)
                .iter()
                .any(|n| matches!(n, CNode::Scalar(SNode::CallUnary { .. })))
        );
    }
}

/// A request to seed an [`ENode::IndexedFilter`] into the e-graph.
///
/// Computed by the physical pass from the production `LiteralConstraints`
/// detector (see `crate::eqsat::transform`): it identifies a
/// `Filter[predicates](Get get_id)` subtree that the detector can evaluate as an
/// index lookup, and carries `committed`, the exact realization to emit at raise.
/// The detector requires the index oracle and imperative MFP analysis, neither
/// of which fits a declarative rewrite rule, so the decision is computed once and
/// seeded rather than discovered during saturation.
#[derive(Clone, Debug)]
pub struct IndexedFilterSeed {
    /// The global id of the `Get` the filter sits on.
    pub get_id: GlobalId,
    /// The lowered filter predicates, used both to locate the matching `Filter`
    /// e-node and to populate the seeded node (so it derives the same analyses).
    pub predicates: Vec<EScalar>,
    /// The production `LiteralConstraints` realization (a semi-join
    /// `Join { implementation: IndexedFilter(..) }`), emitted verbatim by raise.
    pub committed: MirRelationExpr,
}
