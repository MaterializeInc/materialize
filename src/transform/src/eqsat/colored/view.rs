// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! `ColoredView`: a color-aware implementation of the [`MatchGraph`] /
//! [`ApplyGraph`] surfaces over a [`ColoredEGraph`] at a given color.
//!
//! The base layer serves the same two traits through `BaseView` / `EGraph`
//! (`eqsat::egraph::view`); this is the colored mirror so the compiled rule
//! functions (generated in `build.rs`) can run *contextually* — matching and
//! rewriting within one color's coarsened congruence rather than the base.
//!
//! ## The `&self` / `&mut self` tension and the snapshot
//!
//! [`MatchGraph`]'s read methods are `&self`, but [`ColoredEGraph`]'s
//! `find`/`canon`/`colored_class_members` are `&mut self` (they path-compress
//! the layered union-find). The resolution is a per-round **immutable
//! snapshot**: [`ColoredView::new`] borrows the `ColoredEGraph` `&mut` *briefly*
//! to precompute everything the read side needs (the per-color sym index, the
//! colored class → node map, per-class arities, and a base representative per
//! class), then serves all `MatchGraph` reads from that snapshot via `&self`.
//!
//! This is sound because saturation is two-phase: Phase 1 (matching) reads the
//! snapshot; Phase 2 (application) mutates the `ColoredEGraph` via
//! `add_colored`. The T6 driver rebuilds a fresh `ColoredView` (a new snapshot)
//! each round, so `apply_*` mutations never invalidate an in-flight match scan.
//! Hence `MatchGraph` is `&self` and `ApplyGraph::intern_scalar` is
//! `&mut self`, mirroring the base `&BaseView` / `&mut EGraph` split.
//!
//! ## The external `delta_escalar` cache (I1 fix)
//!
//! Colored-delta scalar ids (`>= base.uf_len()`) are minted by `intern_scalar`
//! as fresh `add_colored` results. They are not in `base.data()` (which panics
//! on unknown ids), so the only safe resolver is the per-call cache. If that
//! cache lived inside `ColoredView` it would be dropped with every view rebuild,
//! meaning a colored-delta scalar id minted in round N would panic in round N+1.
//!
//! The fix: `delta_escalar` is owned by the *caller* (driver or test) and passed
//! in as `&mut HashMap<Id, EScalar>`. `ColoredView::new` holds a mutable
//! reference to it and never clears it. The caller reuses the same `HashMap`
//! across rounds and across child colors — this is safe because colored ids come
//! from a monotonic allocator so there are no cross-color collisions.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use mz_expr::Columns;
use mz_repr::ReprColumnType;

use crate::eqsat::colored::{ColorId, ColoredEGraph, Id};
use crate::eqsat::egraph::view::{ApplyGraph, MatchGraph};
use crate::eqsat::egraph::{CNode, CombinedLang, EGraph, ENode, Sym};
use crate::eqsat::ir::EScalar;
use crate::eqsat::matcher::Payload;

/// A color-aware [`MatchGraph`] / [`ApplyGraph`] over a [`ColoredEGraph`] at one
/// color. Reads are served from an immutable per-round snapshot (see the module
/// docs); writes go through `add_colored` into the color's delta overlay.
///
/// The three lifetime parameters are:
/// - `'a`: the borrow of the `ColoredEGraph` (alive for this view's duration).
/// - `'b`: the borrow of the base `EGraph` that the `ColoredEGraph` wraps.
/// - `'v`: the borrow of the caller-owned `delta_escalar` cache, which outlives
///   any single view so colored-delta scalar ids persist across view rebuilds.
pub(crate) struct ColoredView<'a, 'b, 'v> {
    /// The colored e-graph. Held `&mut` so the apply path (`ApplyGraph`,
    /// `&mut self`) can `add_colored`, and so the snapshot can be precomputed in
    /// [`ColoredView::new`]. `MatchGraph` (`&self`) never touches it — it reads
    /// the snapshot fields below — but tests inspect `find` through it.
    pub ceg: &'a mut ColoredEGraph<'b, CombinedLang>,
    /// The color this view is contextualized at.
    pub color: ColorId,
    /// The shared, immutable base e-graph (schema/arity facts are color-invariant
    /// and resolved through it; the base scalar `escalar` cache lives here too).
    pub base: &'b EGraph,
    /// Caller-owned cache of `EScalar` for colored-delta scalar ids created
    /// during colored saturation (`intern_scalar`). Persists across view rebuilds
    /// and across colors — the caller must pass the *same* `HashMap` to every
    /// `ColoredView::new` call so that ids minted in round N are still readable
    /// in round N+1. Base scalar ids (`< base.uf_len()`) resolve via
    /// `base.data().escalar`; they are never inserted here (M1).
    pub delta_escalar: &'v mut HashMap<Id, EScalar>,
    /// Per-color sym index over `canon(color, ·)` of `visible_nodes(color)`,
    /// keyed by operator [`Sym`], paired with the node's colored-canonical class.
    /// Deduped: no two entries in a bucket are equal (M2).
    pub index: HashMap<Sym, Vec<(Id, ENode)>>,
    /// The distinct colored-canonical ids of classes holding a relational node.
    pub rel_class_ids: Vec<Id>,
    /// Colored-canonical class id → its relational e-nodes (colored-canonical).
    /// Deduped: no two entries per class are equal (M2).
    pub class_nodes: HashMap<Id, Vec<ENode>>,
    /// Colored-canonical class id → its arity (color-invariant).
    pub arity: HashMap<Id, usize>,
    /// Colored-canonical class id → a base member of the class, when one exists.
    /// Used to resolve color-invariant base facts (column types) for a class.
    pub base_rep: HashMap<Id, Id>,
}

impl<'a, 'b, 'v> ColoredView<'a, 'b, 'v> {
    /// Build the view, precomputing the immutable read snapshot. Borrows `ceg`
    /// `&mut` only for the duration of this constructor (to call the `&mut`
    /// `find`/`canon`/`colored_class_members`); thereafter all `MatchGraph`
    /// reads are served `&self` from the snapshot.
    ///
    /// `delta_escalar` must be owned by the caller and reused across all view
    /// rebuilds (every round) and across child colors, so that colored-delta
    /// scalar ids minted in earlier rounds remain resolvable. It is NOT cleared
    /// here — the caller is responsible for its lifetime.
    pub fn new(
        ceg: &'a mut ColoredEGraph<'b, CombinedLang>,
        color: ColorId,
        base: &'b EGraph,
        delta_escalar: &'v mut HashMap<Id, EScalar>,
    ) -> Self {
        // Build the per-color sym index and the colored class → node map from the
        // color's visible e-nodes, each canonicalized under the color. `visible`
        // is owned, so the `&mut ceg` reads below borrow nothing from it.
        let visible = ceg.visible_nodes(color);
        let mut index: HashMap<Sym, Vec<(Id, ENode)>> = HashMap::new();
        let mut class_nodes: HashMap<Id, Vec<ENode>> = HashMap::new();
        let mut rel_ids: BTreeSet<Id> = BTreeSet::new();
        for (id, node) in visible {
            if !matches!(node, CNode::Rel(_)) {
                continue;
            }
            let rep = ceg.find(color, id);
            let CNode::Rel(ce) = ceg.canon(color, &node) else {
                continue;
            };
            index.entry(ce.sym()).or_default().push((rep, ce.clone()));
            class_nodes.entry(rep).or_default().push(ce);
            rel_ids.insert(rep);
        }
        let rel_class_ids: Vec<Id> = rel_ids.into_iter().collect();

        // M2: dedup each bucket/class. Colored merges can cause two base classes
        // to canonicalize to the same rep and produce identical (rep, ENode)
        // pairs. Dedup keeps the snapshot clean and prevents MATCH_LIMIT
        // inflation from duplicate entries.
        for bucket in index.values_mut() {
            bucket.sort_unstable();
            bucket.dedup();
        }
        for nodes in class_nodes.values_mut() {
            nodes.sort_unstable();
            nodes.dedup();
        }

        // Per-class arity + a base representative. Arity is color-invariant (a
        // color only merges scalar equalities; it never changes a relation's
        // arity), so a base member's `base.try_arity` is authoritative. A
        // colored-only class (no base member) derives its arity structurally
        // from its colored e-nodes.
        let mut arity: HashMap<Id, usize> = HashMap::new();
        let mut base_rep: HashMap<Id, Id> = HashMap::new();
        for &r in &rel_class_ids {
            // Prefer a direct base lookup when `r` itself is a base id; else scan
            // the colored class for a base member (a base class merged into a
            // colored one keeps its base members visible here).
            let base_member = if r < base.uf_len() && base.try_arity(r).is_some() {
                Some(r)
            } else {
                ceg.colored_class_members(color, r)
                    .into_iter()
                    .find(|&m| base.try_arity(m).is_some())
            };
            match base_member {
                Some(m) => {
                    base_rep.insert(r, m);
                    if let Some(a) = base.try_arity(m) {
                        arity.insert(r, a);
                    }
                }
                None => {
                    if let Some(a) = structural_arity(&class_nodes, base, r, &mut HashSet::new()) {
                        arity.insert(r, a);
                    }
                }
            }
        }

        ColoredView {
            ceg,
            color,
            base,
            delta_escalar,
            index,
            rel_class_ids,
            class_nodes,
            arity,
            base_rep,
        }
    }

    /// Resolve a scalar id to its `EScalar`: the caller-owned delta cache first
    /// (spellings minted by `intern_scalar` for colored-delta ids), else the base
    /// scalar cache (for base ids). `base.data().escalar` panics on unknown ids,
    /// so this must never be called with a colored-delta id that was not
    /// previously interned.
    fn esc(&self, id: Id) -> EScalar {
        self.delta_escalar
            .get(&id)
            .cloned()
            .unwrap_or_else(|| self.base.data().escalar(id).clone())
    }

    /// The arity of a colored-canonical class from the snapshot, falling back to
    /// the base for an id not captured at snapshot time (e.g. a freshly added
    /// colored class). Panics like `EGraph::arity` if no arity is derivable.
    ///
    /// NOTE: a scalar class id has no arity and would hit this panic. Sound
    /// today because no colored rule binds a scalar metavariable to a relation
    /// arity query. A future slice introducing a `colored: true` scalar rule
    /// must convert this (and `binding_arities` below) to the non-panicking
    /// `try_arity` approach, mirroring `EGraph::binding_arities` (slice-1 Task 7).
    fn arity_of(&self, id: Id) -> usize {
        if let Some(&a) = self.arity.get(&id) {
            return a;
        }
        if id < self.base.uf_len() {
            if let Some(a) = self.base.try_arity(id) {
                return a;
            }
        }
        structural_arity(&self.class_nodes, self.base, id, &mut HashSet::new())
            .expect("colored class has a well-defined arity")
    }

    /// The relational e-nodes of a colored-canonical class (snapshot read).
    fn class_rel_nodes(&self, id: Id) -> &[ENode] {
        self.class_nodes.get(&id).map_or(&[], |v| v.as_slice())
    }

    // --- inherent methods the apply codegen calls directly (Task 3 deviation) -

    /// Add a combined node into this color's overlay, returning its (colored or
    /// base) class. The apply codegen emits `g.add(CNode::Rel(..))` directly.
    pub fn add(&mut self, node: CNode) -> Id {
        self.ceg.add_colored(self.color, node)
    }

    /// Inherent `escalar`, color-delta-aware (delegates to [`Self::esc`]).
    ///
    /// Disambiguates the generated rule bodies' `g.escalar(id)` calls: a
    /// `ColoredView` implements *both* [`MatchGraph`] and [`ApplyGraph`], each of
    /// which declares an `escalar`, so a bare `g.escalar(id)` would be an
    /// ambiguous trait call. An inherent method takes priority over trait methods
    /// in method resolution, so the same emitted body that compiles for the base
    /// graph (`&mut EGraph`, which implements only `ApplyGraph`) also compiles
    /// here — keeping codegen byte-identical across both graph types.
    pub fn escalar(&self, id: Id) -> EScalar {
        self.esc(id)
    }

    /// Arities of the bound relation metavariables (colored-canonical).
    ///
    /// NOTE: like `arity_of`, this panics via `arity_of` if a binding resolves
    /// to a scalar class id. See the forward-guard note on `arity_of` above:
    /// a future `colored: true` scalar rule must switch both to `try_arity`.
    pub fn binding_arities(&self, b: &crate::eqsat::egraph::EBindings) -> BTreeMap<String, usize> {
        b.rels
            .iter()
            .map(|(n, &id)| (n.clone(), self.arity_of(id)))
            .collect()
    }

    /// Column types of a class, resolved via a base member (schema is
    /// color-invariant). `None` for a colored-only class with no base member.
    pub fn column_types(&self, id: Id) -> Option<Vec<ReprColumnType>> {
        let rep = self.base_rep.get(&id).copied().unwrap_or(id);
        self.base.column_types(rep)
    }
}

/// Structural arity of a colored class over the snapshot, mirroring
/// `EGraph::arity_guarded` but resolving base children through `base.try_arity`
/// (arity is color-invariant) and colored children through the snapshot. Used
/// only for colored-only classes (no base member pins the arity).
fn structural_arity(
    class_nodes: &HashMap<Id, Vec<ENode>>,
    base: &EGraph,
    id: Id,
    visiting: &mut HashSet<Id>,
) -> Option<usize> {
    if id < base.uf_len() {
        return base.try_arity(id);
    }
    if !visiting.insert(id) {
        return None;
    }
    let mut result = None;
    if let Some(nodes) = class_nodes.get(&id) {
        for node in nodes {
            let a = match node {
                ENode::Constant { arity, .. }
                | ENode::Get { arity, .. }
                | ENode::LocalGet { arity, .. } => Some(*arity),
                ENode::Opaque(m) => Some(m.arity()),
                ENode::Project { outputs, .. } => Some(outputs.len()),
                ENode::Map { input, scalars } => {
                    structural_arity(class_nodes, base, *input, visiting).map(|a| a + scalars.len())
                }
                ENode::FlatMap { input, func, .. } => {
                    structural_arity(class_nodes, base, *input, visiting)
                        .map(|a| a + func.output_arity())
                }
                ENode::Filter { input, .. }
                | ENode::TopK { input, .. }
                | ENode::ArrangeBy { input, .. }
                | ENode::ArrangeByMany { input, .. }
                | ENode::IndexedFilter { input, .. }
                | ENode::Negate { input }
                | ENode::Threshold { input } => {
                    structural_arity(class_nodes, base, *input, visiting)
                }
                ENode::Reduce {
                    group_key,
                    aggregates,
                    ..
                } => Some(group_key.len() + aggregates.len()),
                ENode::Join { inputs, .. } | ENode::WcoJoin { inputs, .. } => inputs
                    .iter()
                    .map(|i| structural_arity(class_nodes, base, *i, visiting))
                    .sum::<Option<usize>>(),
                ENode::Union { inputs } => structural_arity(class_nodes, base, inputs[0], visiting),
            };
            if a.is_some() {
                result = a;
                break;
            }
        }
    }
    visiting.remove(&id);
    result
}

/// All columns referenced by a payload, resolving scalar ids through `esc`
/// (the colored-delta-aware resolver). Mirrors `Payload::columns`, which reads
/// the base `CombinedData` cache directly.
fn payload_columns(p: &Payload, esc: impl Fn(Id) -> EScalar) -> Vec<usize> {
    match p {
        Payload::Predicates(s) | Payload::Scalars(s) | Payload::GroupKey(s) => {
            s.iter().flat_map(|id| esc(*id).cols()).collect()
        }
        Payload::Aggregates(a) => a.iter().flat_map(|x| x.expr.support()).collect(),
        Payload::Outputs(o) => o.clone(),
        Payload::Equivalences(classes) => classes
            .iter()
            .flat_map(|c| c.iter().flat_map(|id| esc(*id).cols()))
            .collect(),
        Payload::FlatMapFunc(_) | Payload::FlatMapExprs(_) => Vec::new(),
    }
}

impl<'a, 'b, 'v> MatchGraph for ColoredView<'a, 'b, 'v> {
    fn rel_class_ids(&self) -> Vec<Id> {
        self.rel_class_ids.clone()
    }

    fn rel_class_nodes(&self, id: Id) -> Vec<ENode> {
        self.class_rel_nodes(id).to_vec()
    }

    fn nodes_by_sym(&self, sym: Sym) -> Vec<(Id, ENode)> {
        self.index.get(&sym).cloned().unwrap_or_default()
    }

    fn arity(&self, id: Id) -> usize {
        self.arity_of(id)
    }

    fn escalar(&self, id: Id) -> EScalar {
        self.esc(id)
    }

    // Colored scalar matching is not wired up yet (no colored rule matches
    // scalar patterns until a later task), so these are sound empty stubs: an
    // empty result means "no match", never a false positive.
    //
    // NOTE: a future slice introducing a `colored: true` scalar rule must make
    // these stubs return real results. Until then the emptiness is sound
    // (no match), but it would silently make such a rule never fire.
    fn scalar_class_nodes(&self, _id: Id) -> Vec<crate::eqsat::scalar::node::SNode> {
        Vec::new()
    }

    fn nodes_by_scalar_sym(
        &self,
        _sym: crate::eqsat::scalar::lang::ScalarSym,
    ) -> Vec<(Id, crate::eqsat::scalar::node::SNode)> {
        Vec::new()
    }

    // Scalar analysis is not surfaced in the colored view: scalar rules run only
    // in the scalar saturate pass, never colored relational saturation, so these
    // are never consulted here. The sound default matches the emptiness stubs
    // above (no analysis known -> no gate satisfied), covered by the same NOTE:
    // a future `colored: true` scalar rule must make them return real results.
    fn scalar_could_error(&self, _id: Id) -> bool {
        false
    }

    fn scalar_lit_bool_or_null(&self, _id: Id) -> Option<Option<bool>> {
        None
    }

    // `scalar_nullable` reads `raise`+`typ` rather than the stored analysis, but
    // is unreachable here for the same structural reason as the two methods
    // above (no colored scalar rule exists), so it gets the same inert stub.
    fn scalar_nullable(&self, _id: Id) -> bool {
        false
    }

    fn cond_has_duplicate_id(&self, _ids: &[Id]) -> bool {
        // Colored saturation never runs scalar rules (they are `colored: false`),
        // so this is unreachable in practice. Inert `false` keeps the rule from
        // firing if ever reached, matching the analysis-gated conds.
        false
    }

    fn cond_absorb_applies(&self, _ids: &[Id], _inner: &mz_expr::VariadicFunc) -> bool {
        // Same reasoning as `cond_has_duplicate_id` above: unreachable for a
        // `colored: false` scalar rule, inert `false` if ever reached.
        false
    }

    fn cond_uses_only_input(&self, p: &Payload, rel: Id) -> bool {
        let rel_arity = self.arity_of(rel);
        payload_columns(p, |id| self.esc(id))
            .into_iter()
            .all(|c| c < rel_arity)
    }

    fn cond_cols_in_range(&self, p: &Payload, lo: i64, hi: i64) -> bool {
        payload_columns(p, |id| self.esc(id))
            .into_iter()
            .all(|c| (c as i64) >= lo && (c as i64) < hi)
    }

    fn cond_all_columns(&self, p: &Payload) -> bool {
        p.scalar_ids()
            .is_some_and(|s| s.iter().all(|&x| self.esc(x).is_col().is_some()))
    }

    fn cond_any_false(&self, p: &Payload) -> bool {
        p.scalar_ids()
            .is_some_and(|s| s.iter().any(|&x| self.esc(x).lit == Some(false)))
    }

    fn cond_no_false(&self, p: &Payload) -> bool {
        p.scalar_ids()
            .is_some_and(|s| s.iter().all(|&x| self.esc(x).lit != Some(false)))
    }

    fn cond_no_error(&self, p: &Payload) -> bool {
        p.scalar_ids()
            .is_some_and(|s| s.iter().all(|&x| !self.esc(x).expr.might_error()))
    }

    fn cond_all_true(&self, p: &Payload) -> bool {
        p.scalar_ids()
            .is_some_and(|s| s.iter().all(|&x| self.esc(x).lit == Some(true)))
    }

    fn cond_identity_projection(&self, p: &Payload, rel: Id) -> bool {
        crate::eqsat::egraph::cond_identity_projection(p, self.arity_of(rel))
    }

    fn cond_is_rel_empty(&self, id: Id) -> bool {
        self.class_rel_nodes(id)
            .iter()
            .any(|n| matches!(n, ENode::Constant { card: 0, .. }))
    }

    fn cond_not_rel_empty(&self, id: Id) -> bool {
        !self
            .class_rel_nodes(id)
            .iter()
            .any(|n| matches!(n, ENode::Constant { card: 0, .. }))
    }

    fn cond_has_three_or_more_inputs(&self, root: Id) -> bool {
        self.class_rel_nodes(root).iter().any(|n| match n {
            ENode::Join { inputs, .. } => inputs.len() >= 3,
            _ => false,
        })
    }

    fn cond_is_binary_join(&self, root: Id) -> bool {
        self.class_rel_nodes(root).iter().any(|n| match n {
            ENode::Join { inputs, .. } => inputs.len() == 2,
            _ => false,
        })
    }

    fn cond_join_is_cyclic(&self, root: Id) -> bool {
        self.class_rel_nodes(root).iter().any(|n| match n {
            ENode::Join {
                inputs,
                equivalences,
            } => {
                let arities: Vec<usize> = inputs.iter().map(|&c| self.arity_of(c)).collect();
                let equivalences: Vec<Vec<EScalar>> = equivalences
                    .iter()
                    .map(|class| class.iter().map(|&s| self.esc(s)).collect())
                    .collect();
                crate::eqsat::cost::join_is_cyclic(&arities, &equivalences)
            }
            _ => false,
        })
    }

    fn cond_has_inner_equiv(&self, p: &Payload, boundary: i64) -> bool {
        let Payload::Equivalences(classes) = p else {
            return false;
        };
        let Ok(b) = usize::try_from(boundary) else {
            return false;
        };
        classes.iter().any(|class| {
            class
                .iter()
                .all(|&s| self.esc(s).cols().iter().all(|&c| c < b))
        })
    }

    fn cond_reads_indexed_global(&self, id: Id) -> bool {
        use mz_expr::MirRelationExpr;
        let mut stack = vec![id];
        let mut seen = HashSet::new();
        while let Some(cls) = stack.pop() {
            if !seen.insert(cls) {
                continue;
            }
            for n in self.class_rel_nodes(cls) {
                match n {
                    ENode::Opaque(m) => {
                        if let MirRelationExpr::Get {
                            id: mz_expr::Id::Global(gid),
                            ..
                        } = m
                        {
                            if self
                                .base
                                .data()
                                .rel
                                .available
                                .get(gid)
                                .is_some_and(|keys| !keys.is_empty())
                            {
                                return true;
                            }
                        }
                    }
                    ENode::Project { input, .. }
                    | ENode::Filter { input, .. }
                    | ENode::Map { input, .. }
                    | ENode::ArrangeBy { input, .. }
                    | ENode::ArrangeByMany { input, .. } => stack.push(*input),
                    _ => {}
                }
            }
        }
        false
    }

    fn cond_produces_key(&self, rel: Id, key: &Payload) -> bool {
        let Some(key) = key.scalar_ids() else {
            return false;
        };
        self.class_rel_nodes(rel).iter().any(|n| match n {
            ENode::ArrangeBy { key: k2, .. } => k2.as_slice() == key,
            ENode::ArrangeByMany { keys, .. } => keys.iter().any(|k2| k2.as_slice() == key),
            ENode::Reduce { group_key, .. } => {
                key.len() == group_key.len()
                    && key
                        .iter()
                        .enumerate()
                        .all(|(i, &s)| self.esc(s).is_col() == Some(i))
            }
            _ => false,
        })
    }

    // Analysis-gated conditions: NEVER reached for colored rules (the Task 4
    // build-time assertion guarantees a colored-tagged rule carries none of
    // these). Returning `false` is sound — the rule simply does not fire.
    fn cond_non_negative(&self, _id: Id) -> bool {
        false
    }

    fn cond_monotonic(&self, _id: Id) -> bool {
        false
    }

    fn cond_is_unique_key(&self, _p: &Payload, _id: Id) -> bool {
        false
    }
}

impl<'a, 'b, 'v> ApplyGraph for ColoredView<'a, 'b, 'v> {
    fn intern_scalar(&mut self, e: &EScalar) -> Id {
        let top = lower_colored_impl(self.ceg, self.color, &e.expr);
        // M1: only insert for colored-delta ids (`>= base.uf_len()`). If the
        // spelling hash-conses to a base id that already lives in
        // `base.data().escalar`, inserting a shadow entry here would be
        // redundant and could mask the base's deterministically-normalized fact.
        if top >= self.base.uf_len() {
            self.delta_escalar.insert(top, e.clone());
        }
        top
    }

    fn arity_of_binding(&self, id: Id) -> usize {
        self.arity_of(id)
    }

    fn escalar(&self, id: Id) -> EScalar {
        self.esc(id)
    }

    fn column_types(&self, id: Id) -> Option<Vec<ReprColumnType>> {
        ColoredView::column_types(self, id)
    }
}

/// Lower a `MirScalarExpr` into the color's overlay, returning the top scalar
/// id. Calls `crate::eqsat::scalar::lower::snode_of` with a closure that
/// recurses into `add_colored` — this is the colored mirror of `lower_into`,
/// reusing the same match logic without duplication.
///
/// Implemented as a free function (not a method) so the closure passed to
/// `snode_of` can capture only `ceg` and `color`, avoiding a borrow conflict
/// with `&mut self` inside the closure.
fn lower_colored_impl(
    ceg: &mut ColoredEGraph<'_, CombinedLang>,
    color: ColorId,
    expr: &mz_expr::MirScalarExpr,
) -> Id {
    let node =
        crate::eqsat::scalar::lower::snode_of(expr, |child| lower_colored_impl(ceg, color, child));
    ceg.add_colored(color, CNode::Scalar(node))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use mz_expr::{BinaryFunc, MirScalarExpr, func};

    use crate::eqsat::colored::ColoredEGraph;
    use crate::eqsat::colored::view::ColoredView;
    use crate::eqsat::egraph::view::{ApplyGraph, MatchGraph};
    use crate::eqsat::egraph::{CNode, EGraph, ENode, Sym};
    use crate::eqsat::ir::EScalar;

    #[mz_ore::test]
    fn colored_view_reads_canonicalize_under_color() {
        let mut eg = EGraph::new();
        let leaf = eg.add(CNode::Rel(ENode::Constant {
            card: 1,
            arity: 2,
            col_types: None,
        }));
        let c0 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
        let c1 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(1)));
        let _ = leaf;
        eg.rebuild();
        let mut ceg = ColoredEGraph::new(&eg);
        let color = ceg.new_color(None);
        ceg.union(color, eg.find(c0), eg.find(c1)); // #0 ≅_c #1
        let mut delta_escalar = HashMap::new();
        let view = ColoredView::new(&mut ceg, color, &eg, &mut delta_escalar);
        // Under the color, #0 and #1 canonicalize to the same id.
        assert_eq!(
            view.ceg.find(color, eg.find(c0)),
            view.ceg.find(color, eg.find(c1))
        );
    }

    /// `intern_scalar` lowers a new spelling into the color's overlay as a
    /// colored-delta id (above the base id space) and the external
    /// `delta_escalar` cache round-trips it back through `escalar`.
    #[mz_ore::test]
    fn intern_scalar_round_trips_through_delta_cache() {
        let mut eg = EGraph::new();
        // Base has the bare columns #0 and #1 as scalar classes, but NOT the
        // `#0 + #1` compound — so lowering it must mint a fresh colored id.
        let _c0 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
        let _c1 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(1)));
        eg.rebuild();
        let base_len = eg.uf_len();

        let mut ceg = ColoredEGraph::new(&eg);
        let color = ceg.new_color(None);
        let mut delta_escalar = HashMap::new();
        let mut view = ColoredView::new(&mut ceg, color, &eg, &mut delta_escalar);

        let sum = EScalar::plain(MirScalarExpr::column(0).call_binary(
            MirScalarExpr::column(1),
            BinaryFunc::AddInt64(func::AddInt64),
        ));
        let id = view.intern_scalar(&sum);
        // A brand-new spelling gets a colored-delta id, never a base id.
        assert!(
            id >= base_len,
            "interned spelling must be a colored-delta id (>= base.uf_len() = {base_len}), got {id}"
        );
        // The delta cache round-trips: `escalar(id)` returns exactly what we put in.
        assert_eq!(MatchGraph::escalar(&view, id), sum);
        assert_eq!(ApplyGraph::escalar(&view, id), sum);
    }

    /// `MatchGraph` reads (`rel_class_nodes`, `nodes_by_sym`, `arity`) return
    /// color-canonical results: under a color asserting `#0 ≅ #1`, the matched
    /// `Filter[#1]` node's predicate child canonicalizes to `#0`'s class, and
    /// arity passes through the input relation.
    #[mz_ore::test]
    fn match_reads_are_color_canonical() {
        let mut eg = EGraph::new();
        let leaf = eg.add(CNode::Rel(ENode::Constant {
            card: 1,
            arity: 2,
            col_types: None,
        }));
        let c0 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
        let c1 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(1)));
        let filt = eg.add(CNode::Rel(ENode::Filter {
            input: leaf,
            predicates: vec![c1],
        }));
        eg.rebuild();

        let mut ceg = ColoredEGraph::new(&eg);
        let color = ceg.new_color(None);
        ceg.union(color, eg.find(c0), eg.find(c1)); // #0 ≅_c #1
        let mut delta_escalar = HashMap::new();
        let view = ColoredView::new(&mut ceg, color, &eg, &mut delta_escalar);

        let filt_rep = view.ceg.find(color, eg.find(filt));
        let c0_rep = view.ceg.find(color, eg.find(c0));

        // Arity passes through the Filter to its arity-2 Constant input.
        assert_eq!(MatchGraph::arity(&view, filt_rep), 2);

        // The Filter class holds a Filter whose predicate is canonicalized under
        // the color: `#1`'s scalar child resolves to `#0`'s colored class.
        let nodes = MatchGraph::rel_class_nodes(&view, filt_rep);
        let filter_node = nodes
            .iter()
            .find(|n| matches!(n, ENode::Filter { .. }))
            .expect("Filter class holds a Filter e-node");
        let ENode::Filter { predicates, .. } = filter_node else {
            unreachable!()
        };
        assert_eq!(
            predicates.as_slice(),
            &[c0_rep],
            "predicate `#1` must canonicalize to `#0`'s class under the color"
        );

        // The sym index agrees with the class read.
        let by_sym = MatchGraph::nodes_by_sym(&view, Sym::Filter);
        assert!(
            by_sym
                .iter()
                .any(|(id, n)| *id == filt_rep && matches!(n, ENode::Filter { .. })),
            "nodes_by_sym(Filter) must surface the colored-canonical Filter"
        );
    }

    /// Regression test for I1: a colored-delta scalar id minted by `intern_scalar`
    /// in one `ColoredView` (round 1) must still be resolvable via `escalar` in a
    /// SECOND `ColoredView` built over the same `ColoredEGraph` (round 2) when the
    /// caller passes the same external `delta_escalar` cache to both calls. Before
    /// the fix, the cache lived inside `ColoredView` and was reset on each rebuild,
    /// causing `base.data().escalar(colored_id)` to panic on the stale id.
    #[mz_ore::test]
    fn intern_scalar_delta_escalar_persists_across_view_rebuilds() {
        let mut eg = EGraph::new();
        let _c0 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(0)));
        let _c1 = eg.intern_scalar(&EScalar::plain(MirScalarExpr::column(1)));
        eg.rebuild();
        let base_len = eg.uf_len();

        let mut ceg = ColoredEGraph::new(&eg);
        let color = ceg.new_color(None);

        // The external cache is owned here and threaded through both views.
        let mut delta_escalar: HashMap<_, _> = HashMap::new();

        let sum = EScalar::plain(MirScalarExpr::column(0).call_binary(
            MirScalarExpr::column(1),
            BinaryFunc::AddInt64(func::AddInt64),
        ));

        // Round 1: mint a colored-delta scalar id.
        let minted_id = {
            let mut view = ColoredView::new(&mut ceg, color, &eg, &mut delta_escalar);
            let id = view.intern_scalar(&sum);
            assert!(
                id >= base_len,
                "interned spelling must be a colored-delta id (>= {base_len}), got {id}"
            );
            id
        };
        // `view` is dropped here; in the old design the cache was lost with it.

        // Round 2: build a NEW view with the SAME external cache — must not panic.
        let view2 = ColoredView::new(&mut ceg, color, &eg, &mut delta_escalar);
        // `escalar` must find `minted_id` in the external cache, not fall through
        // to `base.data().escalar` (which would panic for a colored-delta id).
        assert_eq!(
            MatchGraph::escalar(&view2, minted_id),
            sum,
            "escalar(minted_id) must resolve without panic across view rebuilds"
        );
    }
}
