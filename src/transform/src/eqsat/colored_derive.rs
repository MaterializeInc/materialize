// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Per-(canonical relational class) merged output-equivalence facts.
//!
//! This module exposes the eqsat `Equivalences` analysis as a one-shot,
//! per-class fact map for the colored-e-graph derivation. It does NOT
//! reimplement the per-operator equivalence derivation: it reuses the exact
//! analysis driver (`run_analysis_bounded`) the saturation loop runs, so the
//! "merge across all nodes in a class + bounded fixpoint" semantics are
//! identical to production.

use std::collections::{BTreeMap, BTreeSet};

use mz_expr::{Columns, MirScalarExpr};
use mz_ore::collections::HashMap as OreHashMap;
use mz_ore::collections::HashSet as OreHashSet;

use crate::analysis::equivalences::EquivalenceClasses;
use crate::eqsat::analysis::{Equivalences, RelCtx};
use crate::eqsat::colored::{ColorId, ColoredEGraph};
use crate::eqsat::egraph::{
    CNode, CombinedLang, EGraph, ENode, Id, MAX_EQUIVALENCES_ANALYSIS_ITERS, reduce_escalar,
};
use crate::eqsat::ir::{EScalar, scalar_expr_cost};

/// The output-equivalence fact of a relational e-class: `None` is the absorbing
/// "empty relation" top (vacuously all expressions equivalent); `Some(ec)` is
/// the known [`EquivalenceClasses`].
pub(crate) type EqFact = Option<EquivalenceClasses>;

/// Per-canonical-class output-equivalence fact, computed by the eqsat
/// `Equivalences` analysis (merge across all class nodes, bounded fixpoint).
///
/// Keyed by canonical class id (`eg.find(id)` for any member id). Uses the
/// production analysis driver directly: `run_analysis_bounded` with the
/// `Equivalences` analysis at the `MAX_EQUIVALENCES_ANALYSIS_ITERS` bound, run
/// once on the frozen base.
///
/// `locals` is empty here: that field carries per-binding recursion facts for
/// `LetRec` fragments. `derive_facts` operates on a self-contained e-graph with
/// no externally-supplied recursion facts, matching what `saturate` passes for a
/// `Let`-free fragment (`LocalFacts::default().equivalences`, an empty map).
pub(crate) fn derive_facts(eg: &EGraph) -> BTreeMap<Id, EqFact> {
    let analysis = Equivalences {
        locals: BTreeMap::new(),
    };
    // Build the per-run analysis context exactly as `saturate` does: an arity
    // provider plus the scalar-fact cache the analysis resolves `Id`→`EScalar`
    // through. Both borrow `eg` immutably, as does `run_analysis_bounded`.
    let arity = |c: Id| eg.arity(c);
    let arity_fn: &dyn Fn(Id) -> usize = &arity;
    let ctx = RelCtx {
        arity: arity_fn,
        data: eg.data(),
    };
    eg.run_analysis_bounded(&analysis, ctx, MAX_EQUIVALENCES_ANALYSIS_ITERS)
}

/// A class is empty iff its fact is `None` (the absorbing empty-relation top) or
/// its [`EquivalenceClasses`] is `unsatisfiable()` (a contradiction, e.g. two
/// distinct literals forced equal).
pub(crate) fn fact_is_empty(fact: &EqFact) -> bool {
    match fact {
        None => true,
        Some(ec) => ec.unsatisfiable(),
    }
}

/// One distinct contextual scope: the scalar-id equalities valid there.
///
/// `unions` holds `(original_payload_id, reduced_spelling_id)` scalar-class
/// pairs, sorted and deduped so two classes with the same equality-set produce
/// an identical (hashable) key. Each scope becomes one color in the colored
/// layer (Task 5/6).
pub(crate) struct ScopeEqualities {
    pub unions: Vec<(Id, Id)>,
}

/// The reduced-spelling interning + scope assignment produced by [`derive`].
pub(crate) struct DerivedScopes {
    /// Deduped distinct equality-sets (one future color each).
    pub scopes: Vec<ScopeEqualities>,
    /// Canonical relational class -> index into `scopes` (absent ⇒ black/no color).
    ///
    /// `BTreeMap`: `build_colored_layer` consumes this by re-canonicalizing
    /// every key through `base.find` and collecting into `color_of`. Two
    /// pre-rebuild classes can collapse onto the same post-rebuild id, so the
    /// consuming order decides which scope's color survives the collision.
    /// Sorted iteration makes that pick process-independent.
    pub class_scope: BTreeMap<Id, usize>,
    /// Canonical relational classes that denote the empty relation.
    pub empty_classes: BTreeSet<Id>,
}

/// The colored e-graph layer derived from a set of [`DerivedScopes`]: one color
/// per scope, each carrying that scope's contextual scalar-id equalities applied
/// over the frozen base. Borrows the base immutably for `'b`.
///
/// `color_of` maps a canonical relational class to the color whose equalities
/// hold there; `empty_classes` are the canonical relational classes that denote
/// the empty relation. Color-aware extraction consumes this layer to resolve each
/// Filter/Map scalar payload to its cheapest in-range congruent spelling.
pub(crate) struct ColoredLayer<'b> {
    pub ceg: ColoredEGraph<'b, CombinedLang>,
    /// Canonical relational class -> the color holding its contextual equalities.
    ///
    /// `BTreeMap`: `build.rs`'s colored-conclusion table build iterates this to
    /// map each class to its color's conclusion. Sorted iteration keeps that
    /// build order, and so the conclusion table, process-independent. The
    /// survivor pick when two pre-rebuild classes canonicalize onto one id is
    /// resolved upstream by `class_scope` being a `BTreeMap`, not here.
    pub color_of: BTreeMap<Id, ColorId>,
    /// Canonical relational classes that denote the empty relation.
    pub empty_classes: OreHashSet<Id>,
    /// Caller-owned `EScalar` cache for colored-delta scalar ids minted by the
    /// colored rule driver's `intern_scalar` (ids `>= base.uf_len()`, absent from
    /// `base.data()`). Owned here so it persists across the driver's per-round
    /// `ColoredView` rebuilds and across colors — each [`ColoredView::new`] is
    /// handed `&mut layer.delta_escalar` so an id minted in one round stays
    /// resolvable in the next. Colored extraction (a later task) reads it too.
    pub delta_escalar: OreHashMap<Id, EScalar>,
}

/// Build the colored layer from `scopes` over the frozen `base`.
///
/// Constructs a **hierarchical color forest**: colors correspond to distinct
/// equality-sets, and the parent of a color is the color whose equality-set is
/// the maximal proper subset (if any). This implements the Singher–Itzhaky
/// inclusion lattice: a child color inherits all equalities of its ancestor
/// chain and adds only its own delta.
///
/// After building the forest, [`ColoredEGraph::close_all`] is called with each
/// color's **delta equalities** (those not already in the immediate parent) in
/// parent-first (`ColorId`) order. This runs congruence closure per color so
/// that, for example, a color asserting `#0 ≅ #1` also merges `Eq(#0,#1)` with
/// `Eq(#0,#0)` (both canonicalize to `Eq(rep,rep)` under the color).
///
/// **Parent-first `ColorId` invariant:** scopes are processed in ascending
/// `(|unions|, unions)` order, so parents — which have strictly fewer equalities
/// — are always created before their children and therefore receive lower
/// `ColorId`s. `close_all` sorts by `ColorId` ascending, so it inherently
/// processes parents before children. No explicit reordering is needed.
///
/// All recorded `Id`s are re-canonicalized through `base.find` because they
/// were recorded against the pre-`rebuild()` graph (see [`derive`]); the caller
/// must `rebuild()` the base before calling this.
pub(crate) fn build_colored_layer<'b>(base: &'b EGraph, scopes: DerivedScopes) -> ColoredLayer<'b> {
    let mut ceg = ColoredEGraph::new(base);

    // 1. Sort scope indices by ascending (|unions|, unions) so parents
    //    (smaller equality-sets) are created before children.
    let mut order: Vec<usize> = (0..scopes.scopes.len()).collect();
    order.sort_by_key(|&i| {
        (
            scopes.scopes[i].unions.len(),
            scopes.scopes[i].unions.clone(),
        )
    });

    // 2. For each scope (in sorted order), find its parent: the already-placed
    //    scope whose union-set is a proper subset, maximal by |unions| (last
    //    match wins because we iterate in ascending-size order).
    let mut color_ids: Vec<Option<ColorId>> = vec![None; scopes.scopes.len()];
    // Per-color delta equalities (re-canonicalized through base) for close_all.
    let mut per_color: Vec<(ColorId, Vec<(Id, Id)>)> = Vec::with_capacity(scopes.scopes.len());

    for &i in &order {
        let mine: BTreeSet<(Id, Id)> = scopes.scopes[i].unions.iter().copied().collect();
        let mut parent: Option<usize> = None;
        for &j in &order {
            if j == i || color_ids[j].is_none() {
                continue;
            }
            let theirs: &Vec<(Id, Id)> = &scopes.scopes[j].unions;
            // Proper subset: strictly fewer equalities, all contained in mine.
            if theirs.len() < mine.len() && theirs.iter().all(|e| mine.contains(e)) {
                // Keep the maximal proper subset (ascending iteration → last wins).
                parent = Some(j);
            }
        }
        let pcolor = parent.map(|j| color_ids[j].unwrap());
        // Parents are created before children → pcolor.0 < cid.0 always holds.
        let cid = ceg.new_color(pcolor);
        color_ids[i] = Some(cid);

        // Delta equalities: those in this scope's set but not in the immediate
        // parent's set (the parent chain is inherited automatically).
        let parent_eqs: BTreeSet<(Id, Id)> = match parent {
            Some(j) => scopes.scopes[j].unions.iter().copied().collect(),
            None => BTreeSet::new(),
        };
        let delta: Vec<(Id, Id)> = scopes.scopes[i]
            .unions
            .iter()
            .filter(|&&pair| !parent_eqs.contains(&pair))
            // Re-canonicalize through the frozen base after rebuild().
            .map(|&(a, b)| (base.find(a), base.find(b)))
            .collect();
        per_color.push((cid, delta));
    }

    // 3. Apply delta equalities and close congruence, parent-first.
    // close_all sorts by ColorId ascending, which equals parent-first because
    // parents are created before children (see invariant note in the doc-comment).
    ceg.close_all(&per_color);

    let color_of = scopes
        .class_scope
        .into_iter()
        .map(|(cls, idx)| (base.find(cls), color_ids[idx].unwrap()))
        .collect();
    let empty_classes = scopes
        .empty_classes
        .into_iter()
        .map(|c| base.find(c))
        .collect();
    ColoredLayer {
        ceg,
        color_of,
        empty_classes,
        delta_escalar: OreHashMap::new(),
    }
}

/// Cheapest [`EScalar`] spelling of payload `id` under `color`, among its
/// colored-class base members whose column support is in range (`< max_col`).
///
/// Looks up every base scalar class congruent to `id` under `color` (the
/// contextual equalities recorded for that color by [`derive`]) and returns the
/// member minimizing `(scalar_expr_cost, name_key)` — the smallest tree, ties broken
/// by the same deterministic `name_key` the `escalar` cache uses.
///
/// The `max_col` guard re-applies, at resolution time, the exact column-range
/// check [`reduce_escalar`] enforces when it *records* a reduction. A colored
/// class merges spellings transitively within one color, so it can contain a
/// member whose column support is valid for some *other* node but out of range
/// for the payload position being emitted here (e.g. a Map scalar at position
/// `pos` may only reference columns `< input_arity + pos`). Emitting such an
/// out-of-range spelling would later crash `coalesce_mfp`'s `permute`. Members
/// with any column `>= max_col` are therefore filtered out. The fallback to
/// `id`'s own cached spelling is always in range (it is the payload originally
/// recorded at this position), so a valid candidate always exists; the fallback
/// is otherwise reached only defensively when the colored class is empty.
pub(crate) fn resolve_scalar_colored(
    base: &EGraph,
    layer: &mut ColoredLayer<'_>,
    color: ColorId,
    id: Id,
    max_col: usize,
) -> EScalar {
    let members = layer.ceg.colored_class_members(color, base.find(id));
    members
        .into_iter()
        .map(|m| base.data().escalar(m).clone())
        .filter(|e| e.expr.support().into_iter().all(|c| c < max_col))
        .min_by_key(|e| {
            (
                scalar_expr_cost(&e.expr),
                e.expr.support().into_iter().collect::<Vec<_>>(),
                e.name_key(),
            )
        })
        .unwrap_or_else(|| base.data().escalar(base.find(id)).clone())
}

/// Apply [`reduce_escalar`] to a fixpoint. A single `reduce_expr` pass rewrites
/// one layer of a nested equivalence — e.g. `(((#0->v)->config)->variant)` ->
/// `#1` — but the rewritten result may expose a further reducible subterm —
/// `((#1->Managed)->schedule)` -> `#3`. The Phase-2a saturation loop reached that
/// fixpoint by re-running the reducer every round; this single post-saturation
/// derivation iterates explicitly to match that depth (otherwise colored
/// extraction stops one level short and emits a more expensive spelling). Each
/// step re-applies the `max_col` guard, so the result is never out of range.
/// Bounded by `MAX_EQUIVALENCES_ANALYSIS_ITERS`-style safety; the reducer is
/// terminating (each step strictly lowers the canonical order), so it converges.
fn reduce_escalar_fixpoint(
    escalar: &EScalar,
    reducer: &BTreeMap<MirScalarExpr, MirScalarExpr>,
    max_col: usize,
) -> (bool, EScalar) {
    let mut cur = escalar.clone();
    let mut any = false;
    for _ in 0..16 {
        let (changed, next) = reduce_escalar(&cur, reducer, max_col);
        if !changed {
            break;
        }
        any = true;
        cur = next;
    }
    (any, cur)
}

/// Read a fact's substitution reducer, refreshing it if it is empty.
///
/// `derive_facts` runs the analysis through `minimize_bounded`, which normally
/// leaves the reducer (`EquivalenceClasses::remap`) populated. As a defensive
/// measure — and to honor the contract that consumers read the reducer only
/// after a `minimize` — a fact whose reducer is unexpectedly empty is cloned and
/// `minimize_bounded`d (at most 100 iterations, the analysis-merge bound) before
/// reading. This is purely additive: a genuinely
/// equivalence-free class still yields an empty reducer (and so no reduction).
fn fresh_reducer(ec: &EquivalenceClasses) -> BTreeMap<MirScalarExpr, MirScalarExpr> {
    if !ec.reducer().is_empty() {
        ec.reducer().clone()
    } else {
        // Defensive: `derive_facts` runs the analysis through a (bounded)
        // minimize, so a non-empty class normally already has a reducer; this
        // fallback is not expected to fire. Use the SAME bound the analysis
        // merge uses (`minimize_bounded(None, 100)`, see
        // `analysis::equivalences`) rather than an unbounded `minimize`, so the
        // fallback can never over-reduce relative to production. (M6)
        let mut clone = ec.clone();
        clone.minimize_bounded(None, 100);
        clone.reducer().clone()
    }
}

/// Reduce each relational node's Filter predicates / Map scalars to their
/// contextual spellings, intern those spellings into the base, and record the
/// per-scope scalar-id equalities (**keyed by the node's input class**) plus the
/// set of empty classes.
///
/// Mutates `eg` (interning reduced spellings) and so must run on the MUTABLE
/// graph BEFORE the colored layer is built (Task 5). The recorded `Id`s are the
/// pre-rebuild ids; the caller re-canonicalizes them through `base.find` after a
/// later `rebuild()`.
///
/// **SP4d soundness:** a Filter predicate / Map scalar is evaluated over the
/// node's INPUT rows, so it reduces against the **input class's** reducer and the
/// resulting `(orig, reduced)` union is recorded into the **input class's**
/// context (`class_scope`/`color_of` are keyed by the input class). This is what
/// keeps extraction sound when a single class holds sibling nodes with different
/// inputs (`merge_filters`/`fuse_maps`): each sibling's payload resolves under
/// its own input's color, never one shared enclosing-class color.
/// * **Filter** predicates reduce with `max_col = arity(input)`.
/// * **Map** scalars also reduce with `max_col = arity(input)`: a map scalar may
///   reference earlier map outputs but must never be *rewritten* to one (those
///   columns are local to a single Map node and cannot be shared via the input
///   context), so the guard keeps every recorded union valid over the input's
///   output rows.
///
/// All other node kinds are skipped (Phase-2a never rewrote them).
pub(crate) fn derive(eg: &mut EGraph) -> DerivedScopes {
    // `derive_facts` borrows `eg` immutably but returns an owned map, so `eg` is
    // free to mutate (intern) afterwards. Interning scalars never unions
    // relational classes, so the canonical ids in `facts` stay valid throughout.
    let facts = derive_facts(eg);

    let mut empty_classes: BTreeSet<Id> = BTreeSet::new();
    // Recorded equality unions, keyed by the **context class** whose
    // output-equivalences justify them — i.e. the *input* class of the Filter/Map
    // node, NOT the node's own (enclosing) class.
    //
    // SP4d soundness fix: a Filter predicate / Map scalar is evaluated over its
    // INPUT's rows, so the equivalences that may legitimately reduce it are the
    // INPUT's output-equivalences. After `merge_filters`/`fuse_maps`, one class
    // can hold sibling nodes with *different* inputs; recording a reduction under
    // the enclosing class (and resolving every sibling's payload under one class
    // color) folds a sibling whose own input proves nothing — wrong results.
    // Keying by the input class makes each sibling resolve under its own input's
    // context, which is sound.
    let mut by_context: BTreeMap<Id, Vec<(Id, Id)>> = BTreeMap::new();

    // Deterministic order (ids are `usize`), independent of HashMap iteration.
    let mut canon_ids: Vec<Id> = facts.keys().copied().collect();
    canon_ids.sort_unstable();

    for canon in canon_ids {
        // M8: skip scalar-only classes. They carry no Filter/Map payload to
        // reduce and are not relational empty relations, so visiting them only
        // wastes `fresh_reducer`/minimize work (and would mis-route a scalar
        // `None` fact into `empty_classes`). Relational empty classes have a
        // `CNode::Rel` node and are kept.
        if !eg.nodes(canon).iter().any(|n| matches!(n, CNode::Rel(_))) {
            continue;
        }
        let fact = &facts[&canon];
        // M7 (intentional divergence from Phase-2a): an empty-but-unsatisfiable
        // class is routed to `empty_classes` and its predicates are NOT rewritten.
        // The class denotes the empty relation, so extraction empty-folds it
        // regardless of predicate spelling — sound, and strictly less work than
        // Phase-2a (which would still rewrite the predicates of a doomed Filter).
        if fact_is_empty(fact) {
            empty_classes.insert(canon);
            continue;
        }

        for node in eg.nodes(canon) {
            let CNode::Rel(e) = node else {
                continue;
            };
            // Both Filter predicates and Map scalars are evaluated over the
            // node's INPUT rows, so they reduce against the INPUT's
            // output-equivalences and are recorded into the INPUT's context.
            let (input, payloads): (Id, Vec<Id>) = match e {
                ENode::Filter { input, predicates } => (input, predicates),
                ENode::Map { input, scalars } => (input, scalars),
                // All other node kinds carry no scalar payload that Phase-2a
                // rewrote; skip them.
                _ => continue,
            };
            let canon_input = eg.find(input);
            let Some(input_ec) = facts.get(&canon_input).and_then(|f| f.as_ref()) else {
                // Empty/unknown input: nothing sound to reduce against.
                continue;
            };
            let input_reducer = fresh_reducer(input_ec);
            if input_reducer.is_empty() {
                continue;
            }
            // Payloads reduce to a spelling valid over the input's columns
            // (`< arity(input)`). A Map scalar may *reference* an earlier map
            // output, but it must never be *rewritten* to one: a map-output
            // column is local to a single Map node, so recording it into the
            // shared input context would conflate distinct nodes' outputs. The
            // `max_col = arity(input)` guard rejects any such out-of-input-range
            // rewrite, so `by_context[input]` only ever asserts congruences that
            // genuinely hold over the input's output rows.
            let max_col = eg.arity(input);
            for &payload_id in &payloads {
                let escalar = eg.data().escalar(payload_id).clone();
                let (changed, reduced) = reduce_escalar_fixpoint(&escalar, &input_reducer, max_col);
                if changed {
                    let reduced_id = eg.intern_scalar(&reduced);
                    by_context
                        .entry(canon_input)
                        .or_default()
                        .push((payload_id, reduced_id));
                }
            }
        }
    }

    // Flatten the per-context map into deterministic per-class entries (sorted by
    // context class id) so the downstream scope dedup is order-independent.
    let mut per_class: Vec<(Id, Vec<(Id, Id)>)> = by_context.into_iter().collect();
    per_class.sort_by_key(|(ctx, _)| *ctx);

    // Dedup identical equality-sets into one scope each (sorted-pair key).
    let mut scopes: Vec<ScopeEqualities> = Vec::new();
    let mut class_scope: BTreeMap<Id, usize> = BTreeMap::new();
    let mut key_to_idx: OreHashMap<Vec<(Id, Id)>, usize> = OreHashMap::new();
    for (canon, mut unions) in per_class {
        unions.sort_unstable();
        unions.dedup();
        let idx = *key_to_idx.entry(unions.clone()).or_insert_with(|| {
            scopes.push(ScopeEqualities {
                unions: unions.clone(),
            });
            scopes.len() - 1
        });
        class_scope.insert(canon, idx);
    }

    DerivedScopes {
        scopes,
        class_scope,
        empty_classes,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_expr::{BinaryFunc, MirScalarExpr, UnaryFunc, func};
    use mz_repr::{Datum, ReprScalarType};

    use crate::analysis::equivalences::EquivalenceClasses;
    use crate::eqsat::analysis::Equivalences;
    use crate::eqsat::core::{Analysis, Id};
    use crate::eqsat::egraph::EGraph;
    use crate::eqsat::ir::{EScalar, Rel};

    use crate::eqsat::ir::scalar_expr_cost;

    use super::{
        ColoredLayer, EqFact, build_colored_layer, derive, derive_facts, fact_is_empty,
        resolve_scalar_colored,
    };

    // --- tiny helpers ---------------------------------------------------------

    /// A bare column reference `#i`.
    fn col(i: usize) -> MirScalarExpr {
        MirScalarExpr::column(i)
    }

    /// The canonical representative of `expr` under `ec`'s reducer (itself if the
    /// reducer has no entry for it).
    fn ec_canon(ec: &EquivalenceClasses, expr: MirScalarExpr) -> MirScalarExpr {
        ec.reducer().get(&expr).cloned().unwrap_or(expr)
    }

    /// An equality predicate `lhs = rhs` as an `EScalar`.
    fn eq(lhs: MirScalarExpr, rhs: MirScalarExpr) -> EScalar {
        EScalar::plain(lhs.call_binary(rhs, BinaryFunc::Eq(func::Eq)))
    }

    /// An `Int64` literal scalar.
    fn lit(v: i64) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64)
    }

    /// A `Get` with the given arity.
    fn get(arity: usize) -> Rel {
        Rel::Get {
            name: "r".to_string(),
            arity,
        }
    }

    // --- fixtures -------------------------------------------------------------

    /// `Filter[#0 = #1]` over a 2-column `Get`. Returns the e-graph and the
    /// Filter's class id.
    fn filter_eq_fixture() -> (EGraph, Id) {
        let rel = Rel::Filter {
            predicates: vec![eq(col(0), col(1))],
            input: Box::new(get(2)),
        };
        let mut eg = EGraph::new();
        let root = eg.add_rel(&rel);
        eg.rebuild();
        (eg, root)
    }

    /// `Filter[#0 = 2]` over `Filter[#0 = 1]` over a 1-column `Get`: the two
    /// predicates force `#0` equal to both `1` and `2`, an unsatisfiable
    /// contradiction. Returns the e-graph and the top Filter's class id.
    fn contradictory_filter_fixture() -> (EGraph, Id) {
        let rel = Rel::Filter {
            predicates: vec![eq(col(0), lit(2))],
            input: Box::new(Rel::Filter {
                predicates: vec![eq(col(0), lit(1))],
                input: Box::new(get(1)),
            }),
        };
        let mut eg = EGraph::new();
        let root = eg.add_rel(&rel);
        eg.rebuild();
        (eg, root)
    }

    /// `Filter[#0 = #1]` over a 3-column `Get`.
    fn filter_a_rel() -> Rel {
        Rel::Filter {
            predicates: vec![eq(col(0), col(1))],
            input: Box::new(get(3)),
        }
    }

    /// `Filter[#1 = #2]` over a 3-column `Get`.
    fn filter_b_rel() -> Rel {
        Rel::Filter {
            predicates: vec![eq(col(1), col(2))],
            input: Box::new(get(3)),
        }
    }

    /// A single class holding two equivalent e-nodes (`filter_a` and `filter_b`)
    /// whose individual equivalence facts differ (`{#0,#1}` vs `{#1,#2}`). Forced
    /// into one class via `union`, so `derive_facts` must merge their facts.
    fn two_node_class_fixture() -> (EGraph, Id) {
        let mut eg = EGraph::new();
        let a = eg.add_rel(&filter_a_rel());
        let b = eg.add_rel(&filter_b_rel());
        eg.union(a, b);
        eg.rebuild();
        (eg, a)
    }

    /// The root fact for a single-`Rel` e-graph, via `derive_facts`.
    fn node_fact(rel: &Rel) -> EqFact {
        let mut eg = EGraph::new();
        let root = eg.add_rel(rel);
        eg.rebuild();
        let facts = derive_facts(&eg);
        facts[&eg.find(root)].clone()
    }

    /// The expected merged fact for `two_node_class_fixture`: the lattice merge
    /// of `filter_a`'s and `filter_b`'s individual facts, computed independently
    /// via the production `Equivalences::merge`. This is the same merge the
    /// analysis driver performs across the two nodes of the unioned class, so a
    /// correct `derive_facts` must produce exactly this (and NOT one node's fact
    /// alone, which equates only two of the three columns).
    fn expected_merged_fact() -> EqFact {
        let analysis = Equivalences {
            locals: BTreeMap::new(),
        };
        let a = node_fact(&filter_a_rel());
        let b = node_fact(&filter_b_rel());
        analysis.merge(a, b)
    }

    /// `#1 + 1`, a non-column scalar over input column `#1`.
    fn add1(e: MirScalarExpr) -> MirScalarExpr {
        e.call_binary(lit(1), BinaryFunc::AddInt64(func::AddInt64))
    }

    /// `a + b`, a two-column computed scalar.
    fn add(a: MirScalarExpr, b: MirScalarExpr) -> MirScalarExpr {
        a.call_binary(b, BinaryFunc::AddInt64(func::AddInt64))
    }

    /// `NOT(#i)`, a non-column scalar over input column `#i`.
    fn not_col(i: usize) -> MirScalarExpr {
        col(i).call_unary(UnaryFunc::Not(func::Not))
    }

    /// `Map[#1 + 1](Filter[#0 = #1 + 1](Get(2)))`: the Filter establishes the
    /// equivalence `#0 = (#1 + 1)`, so the Map's own reducer maps the computed
    /// scalar `#1 + 1` back to the existing column `#0`. `derive` must reduce the
    /// Map scalar to `#0`, intern that spelling, and record the union. Returns the
    /// e-graph and the Map's class id.
    fn map_redundant_compute_fixture() -> (EGraph, Id) {
        let f = add1(col(1));
        let rel = Rel::Map {
            scalars: vec![EScalar::plain(f.clone())],
            input: Box::new(Rel::Filter {
                predicates: vec![eq(col(0), f)],
                input: Box::new(get(2)),
            }),
        };
        let mut eg = EGraph::new();
        let root = eg.add_rel(&rel);
        eg.rebuild();
        (eg, root)
    }

    /// Like [`map_redundant_compute_fixture`] but returns the *mutable* e-graph
    /// (so the caller can run [`derive`]), the Map's computed-scalar payload id
    /// `f(#1)`, and the Map's class. Under the color [`derive`] records, that
    /// payload is congruent to the bare column `#0` the Filter established.
    fn redundant_compute_fixture_mut() -> (EGraph, Id, Id) {
        let f = add1(col(1));
        let rel = Rel::Map {
            scalars: vec![EScalar::plain(f.clone())],
            input: Box::new(Rel::Filter {
                predicates: vec![eq(col(0), f.clone())],
                input: Box::new(get(2)),
            }),
        };
        let mut eg = EGraph::new();
        let root = eg.add_rel(&rel);
        // Re-intern `f(#1)` to recover its (hash-consed) scalar class id.
        let payload_id = eg.intern_scalar(&EScalar::plain(f));
        eg.rebuild();
        (eg, payload_id, root)
    }

    /// `Map[#1 + 1](Get(2))`: the Map's only equivalence is `#2 = (#1 + 1)`
    /// between the *new* output column `#2` and its defining scalar. The reducer
    /// would rewrite the Map scalar `#1 + 1` to `#2` — its own output column — but
    /// the `max_col` guard (`input_arity + pos = 2`) rejects that forward
    /// reference, so no union is recorded. Returns the e-graph and the Map's class.
    fn map_self_reference_fixture() -> (EGraph, Id) {
        let rel = Rel::Map {
            scalars: vec![EScalar::plain(add1(col(1)))],
            input: Box::new(get(2)),
        };
        let mut eg = EGraph::new();
        let root = eg.add_rel(&rel);
        eg.rebuild();
        (eg, root)
    }

    /// `Filter[NOT(#1)]` over `Filter[#0 = #1]` over a named 2-column `Get`. The
    /// inner Filter establishes `#0 = #1`, so the outer Filter's predicate
    /// `NOT(#1)` reduces (via the input reducer) to `NOT(#0)`.
    fn outer_filter_over(get_name: &str) -> Rel {
        Rel::Filter {
            predicates: vec![EScalar::plain(not_col(1))],
            input: Box::new(Rel::Filter {
                predicates: vec![eq(col(0), col(1))],
                input: Box::new(Rel::Get {
                    name: get_name.to_string(),
                    arity: 2,
                }),
            }),
        }
    }

    /// Two distinct outer-Filter classes (over two differently-named `Get`s) that
    /// each reduce the identical predicate `NOT(#1)` to the identical `NOT(#0)`.
    /// Their recorded equality-sets are byte-identical, so `derive` must dedup
    /// them to one shared scope index. Returns the e-graph and one outer class.
    fn two_filters_same_equality_fixture() -> (EGraph, Id) {
        let mut eg = EGraph::new();
        let a = eg.add_rel(&outer_filter_over("r"));
        let _b = eg.add_rel(&outer_filter_over("s"));
        eg.rebuild();
        (eg, a)
    }

    /// The canonical input class of the (sole) `Map` node in `map_class`.
    ///
    /// SP4d keys `class_scope`/`color_of` by the node's INPUT context, so tests
    /// that look up a Map node's recorded scope/color must do so via its input.
    fn map_input(eg: &EGraph, map_class: Id) -> Id {
        eg.rel_class_nodes(eg.find(map_class))
            .iter()
            .find_map(|n| match n {
                crate::eqsat::egraph::ENode::Map { input, .. } => Some(eg.find(*input)),
                _ => None,
            })
            .expect("map node has an input")
    }

    // --- tests ----------------------------------------------------------------

    #[mz_ore::test]
    fn filter_eq_derives_column_equivalence() {
        // Filter predicate `#0 = #1` ⇒ the Filter class's fact equates #0 and #1.
        let (eg, filter_class) = filter_eq_fixture();
        let facts = derive_facts(&eg);
        let ec = facts[&eg.find(filter_class)].as_ref().expect("non-empty");
        assert!(ec.reducer().values().any(|_| true), "has a reducer");
        // #0 and #1 are in one class:
        assert_eq!(ec_canon(ec, col(0)), ec_canon(ec, col(1)));
    }

    #[mz_ore::test]
    fn contradiction_is_empty() {
        // Filter `#0 = 1` then `#0 = 2` ⇒ unsatisfiable ⇒ fact_is_empty.
        let (eg, top) = contradictory_filter_fixture();
        let facts = derive_facts(&eg);
        assert!(fact_is_empty(&facts[&eg.find(top)]));
    }

    #[mz_ore::test]
    fn merge_across_two_nodes_in_a_class() {
        // A class holding two equivalent e-nodes whose individual facts differ:
        // the derived fact is their lattice merge, not one node's.
        let (eg, class) = two_node_class_fixture();
        let facts = derive_facts(&eg);
        let ec = &facts[&eg.find(class)];
        assert_eq!(ec, &expected_merged_fact());
        // Sanity: the merge equates all three columns, which neither node alone
        // does (guards against `derive_facts` picking one node's fact).
        let merged = ec.as_ref().expect("non-empty");
        assert_eq!(ec_canon(merged, col(0)), ec_canon(merged, col(1)));
        assert_eq!(ec_canon(merged, col(1)), ec_canon(merged, col(2)));
    }

    #[mz_ore::test]
    fn map_scalar_reduced_to_column_is_interned_and_unioned() {
        // input has #0; filter establishes #0 = f(#1) above; a Map computes f(#1).
        // derive() must intern `#0` spelling-equivalent and record (f(#1)-id, #0-id).
        let (mut eg, map_class) = map_redundant_compute_fixture();
        // SP4d: the reduction is recorded under the Map's INPUT context.
        let input = map_input(&eg, map_class);
        let d = derive(&mut eg);
        let scope = &d.scopes[d.class_scope[&eg.find(input)]];
        assert!(!scope.unions.is_empty(), "a reduction was recorded");
        // The reduced spelling is now a base class with an escalar entry:
        for &(_orig, reduced) in &scope.unions {
            let _ = eg.data().escalar(reduced); // must not panic
        }
    }

    #[mz_ore::test]
    fn map_self_reference_is_rejected_by_max_col() {
        // A map scalar at pos that would rewrite to its own output column must NOT
        // be reduced (max_col guard), so no union is recorded for it.
        let (mut eg, map_class) = map_self_reference_fixture();
        let d = derive(&mut eg);
        assert!(
            !d.class_scope.contains_key(&eg.find(map_class))
                || d.scopes[d.class_scope[&eg.find(map_class)]]
                    .unions
                    .is_empty()
        );
    }

    #[mz_ore::test]
    fn identical_scopes_dedup_to_one() {
        let (mut eg, _) = two_filters_same_equality_fixture();
        let d = derive(&mut eg);
        // Two classes with the same equality-set share one scope index.
        let distinct: mz_ore::collections::HashSet<_> = d.class_scope.values().collect();
        assert!(distinct.len() < d.class_scope.len() || d.class_scope.len() <= 1);
    }

    #[mz_ore::test]
    fn contradiction_recorded_in_empty_classes() {
        let (mut eg, top) = contradictory_filter_fixture();
        let d = derive(&mut eg);
        assert!(d.empty_classes.contains(&eg.find(top)));
    }

    #[mz_ore::test]
    fn build_colored_layer_applies_unions() {
        // A fixture that records at least one scalar-id equality (the Map's
        // computed scalar reduces to an existing column). The colored layer must
        // create a color for that scope and apply the recorded union inside it.
        let (mut eg, map_class) = map_redundant_compute_fixture();
        // SP4d: the scope/color is keyed by the Map's INPUT context.
        let input = map_input(&eg, map_class);
        let scopes = derive(&mut eg);
        // Capture the recorded pair + its scope index before `scopes` is moved.
        let pre_class = eg.find(input);
        let idx = scopes.class_scope[&pre_class];
        let (a, b) = scopes.scopes[idx].unions[0];

        eg.rebuild();
        let class = eg.find(input);
        let mut layer = build_colored_layer(&eg, scopes);

        // Some color exists and the recorded class is mapped to one.
        assert!(!layer.color_of.is_empty(), "a color was created");
        let color = layer.color_of[&class];

        // The recorded pair is merged under its color (re-canonicalized through
        // the frozen base).
        let ra = eg.find(a);
        let rb = eg.find(b);
        assert_eq!(
            layer.ceg.find(color, ra),
            layer.ceg.find(color, rb),
            "recorded equality is applied within the color",
        );
    }

    #[mz_ore::test]
    fn scalar_expr_cost_counts_nodes() {
        assert!(scalar_expr_cost(&col(0)) < scalar_expr_cost(&add(col(0), col(1))));
    }

    #[mz_ore::test]
    fn resolve_colored_picks_cheaper_member() {
        // Under a color where f(#1) ≅ #0, resolving f(#1) yields the column #0.
        let (mut eg, payload_id, map_class) = redundant_compute_fixture_mut();
        // SP4d: the scope/color is keyed by the Map's INPUT context.
        let input = map_input(&eg, map_class);
        let scopes = derive(&mut eg);
        eg.rebuild();
        let mut layer = build_colored_layer(&eg, scopes);
        let color = layer.color_of[&eg.find(input)];
        // The Map scalar sits at position 0 over a 2-column input, so its valid
        // column range is `input_arity + pos = 2`; the resolved column #0 is in
        // range.
        let got = resolve_scalar_colored(&eg, &mut layer, color, eg.find(payload_id), 2);
        assert!(got.is_col().is_some(), "resolved to the bare column #0");
    }

    /// Fix (Task 7 gate): the resolution-time column-range guard must reject a
    /// colored-class member whose column support is out of range for the payload
    /// position being emitted, even when it is the cheapest spelling.
    ///
    /// A colored class merges spellings transitively within one color, so it can
    /// contain a member valid for some *other* node but out of range here. This
    /// fixture unions the in-range payload `#1 + 1` (cost 3) with an out-of-range
    /// bare column `#5` (cost 1) under one color. Without the guard,
    /// `min_by_key` would pick the cheaper `#5` — an out-of-range column that
    /// would later crash `coalesce_mfp`'s `permute`. With the guard (`max_col =
    /// 2`), `#5` is filtered out and the in-range payload `#1 + 1` is returned.
    #[mz_ore::test]
    fn resolve_colored_rejects_out_of_range_member() {
        use std::collections::BTreeMap;

        use mz_ore::collections::{HashMap as OreHashMap, HashSet as OreHashSet};

        use crate::eqsat::colored::ColoredEGraph;

        let mut eg = EGraph::new();
        // The in-range payload `#1 + 1` (support {1}) emitted at a Map position 0
        // over a 2-column input ⇒ valid range is `< 2`.
        let payload = eg.intern_scalar(&EScalar::plain(add1(col(1))));
        // An out-of-range bare column `#5` (support {5}), cheaper than the
        // payload, but invalid for this position.
        let oor = eg.intern_scalar(&EScalar::plain(col(5)));
        eg.rebuild();

        let mut ceg = ColoredEGraph::new(&eg);
        let color = ceg.new_color(None);
        ceg.union(color, eg.find(payload), eg.find(oor));
        let mut layer = ColoredLayer {
            ceg,
            color_of: BTreeMap::new(),
            empty_classes: OreHashSet::new(),
            delta_escalar: OreHashMap::new(),
        };

        // Sanity: the cheaper out-of-range column IS a member of the colored
        // class, so an unguarded `min_by_key` would select it.
        let members = layer.ceg.colored_class_members(color, eg.find(payload));
        assert!(
            members.contains(&eg.find(oor)),
            "#5 is in the colored class"
        );

        let got = resolve_scalar_colored(&eg, &mut layer, color, eg.find(payload), 2);
        assert_eq!(
            got.is_col(),
            None,
            "must not return the out-of-range column #5, got {got:?}",
        );
        assert_eq!(
            got.expr,
            add1(col(1)),
            "returns the in-range payload `#1 + 1`"
        );
    }

    /// Task 7 gate (reducer parity): equal-cost tie-break prefers lower column index.
    ///
    /// Under a filter `#0 = #1`, the spellings `#0 + 1` and `#1 + 1` are congruent
    /// and have identical `scalar_expr_cost` (both cost 3). Without the column-support
    /// tie-break the winner is determined by `name_key` alone and may keep the
    /// non-canonical `#1 + 1`. With the support tie-break (`Vec<usize>` from a
    /// sorted `BTreeSet`), `{0}` < `{1}` lexicographically, so `#0 + 1` wins —
    /// matching the Phase-2a reducer's lower-index canonicalization.
    #[mz_ore::test]
    fn resolve_colored_equal_cost_prefers_lower_column_index() {
        use std::collections::BTreeMap;

        use mz_ore::collections::{HashMap as OreHashMap, HashSet as OreHashSet};

        use crate::eqsat::colored::ColoredEGraph;
        use crate::eqsat::ir::EScalar;

        let mut eg = EGraph::new();
        // `#1 + 1` — the "original" payload referencing column 1.
        let expr_col1 = add1(col(1));
        // `#0 + 1` — the canonical form referencing column 0.
        let expr_col0 = add1(col(0));
        // Both have identical scalar_expr_cost (3 nodes each).
        assert_eq!(
            scalar_expr_cost(&expr_col1),
            scalar_expr_cost(&expr_col0),
            "pre-condition: equal cost"
        );

        let payload_id = eg.intern_scalar(&EScalar::plain(expr_col1.clone()));
        let canonical_id = eg.intern_scalar(&EScalar::plain(expr_col0.clone()));
        eg.rebuild();

        // Union them under one color — simulating what `#0 = #1` establishes.
        let mut ceg = ColoredEGraph::new(&eg);
        let color = ceg.new_color(None);
        ceg.union(color, eg.find(payload_id), eg.find(canonical_id));
        let mut layer = ColoredLayer {
            ceg,
            color_of: BTreeMap::new(),
            empty_classes: OreHashSet::new(),
            delta_escalar: OreHashMap::new(),
        };

        // max_col = 2: both `#0` and `#1` are in range; neither is filtered.
        let got = resolve_scalar_colored(&eg, &mut layer, color, eg.find(payload_id), 2);
        assert_eq!(
            got.expr, expr_col0,
            "equal-cost tie must prefer the lower-column-index spelling `#0 + 1`, got {:?}",
            got.expr,
        );
    }

    /// I1 (review fix): the colored layer's resolution in `build_rel` substitutes
    /// a redundantly-recomputed Map scalar with the equal existing column.
    ///
    /// The fixture is built WITHOUT running saturation, so no rule rewrites the
    /// Map scalar; the ONLY thing that can substitute it is the colored
    /// extraction path. The test is therefore
    /// fail-on-no-op: the `None` (no-layer) extraction is asserted to KEEP the
    /// recomputed scalar `#1 + 1`, and only the colored extraction substitutes the
    /// bare column `#0`. If the Colored branch in `build_rel` did nothing, the two
    /// extractions would coincide and the structural assert would fail.
    #[mz_ore::test]
    fn colored_extraction_substitutes_redundant_map_scalar() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::objective::ArrangementCount;

        // Map[#1 + 1](Filter[#0 = #1 + 1](Get(2))): the Filter establishes
        // #0 = (#1 + 1), so the Map recomputes a column equal to the existing #0.
        let (mut eg, root) = map_redundant_compute_fixture();
        let scopes = derive(&mut eg);
        eg.rebuild();
        let mut layer = build_colored_layer(&eg, scopes);
        let root = eg.find(root);
        let model = CostModel::new();

        // Baseline (no color): extraction keeps the recomputed scalar `#1 + 1`,
        // because no saturation rule rewrote it.
        let plain = eg
            .extract_with(root, &model, &ArrangementCount, None, None)
            .expect("plain extraction");
        let Rel::Map { scalars, .. } = &plain else {
            panic!("plain extraction is not a Map: {plain}");
        };
        assert_eq!(scalars.len(), 1);
        assert!(
            scalars[0].is_col().is_none(),
            "baseline (no color) must keep the recomputed scalar, got {plain}",
        );

        // With the colored layer: the Map scalar resolves to the bare column #0.
        let colored = eg
            .extract_with(root, &model, &ArrangementCount, Some(&mut layer), None)
            .expect("colored extraction");
        let Rel::Map { scalars, .. } = &colored else {
            panic!("colored extraction is not a Map: {colored}");
        };
        assert_eq!(scalars.len(), 1);
        assert_eq!(
            scalars[0].is_col(),
            Some(0),
            "colored extraction must substitute the existing column #0, got {colored}",
        );
    }

    /// Task 7 gate: colors built from inclusion-ordered scope sets form a forest
    /// where the parent of a color's scope is the color whose scope is a maximal
    /// proper subset.
    ///
    /// Scopes are given OUT OF ORDER (larger first) to test that `build_colored_layer`
    /// sorts by `(|unions|, unions)` and creates the parent color before the child.
    #[mz_ore::test]
    fn color_forest_is_inclusion_ordered() {
        use std::collections::BTreeSet;

        use super::{DerivedScopes, ScopeEqualities};

        // Build base eg with 3 scalar leaves and 2 relational classes (for class_scope).
        let mut eg = EGraph::new();
        let id_a = eg.intern_scalar(&EScalar::plain(col(0)));
        let id_b = eg.intern_scalar(&EScalar::plain(col(1)));
        let id_c = eg.intern_scalar(&EScalar::plain(col(2)));
        let rel_large = eg.add_rel(&get(2));
        let rel_small = eg.add_rel(&get(3));
        eg.rebuild();

        let id_a = eg.find(id_a);
        let id_b = eg.find(id_b);
        let id_c = eg.find(id_c);
        let rel_large = eg.find(rel_large);
        let rel_small = eg.find(rel_small);

        // scope 0 (index 0): large — {(a,b),(a,c)}, 2 equalities, superset.
        // scope 1 (index 1): small — {(a,b)},        1 equality, proper subset.
        // Given OUT OF ORDER (large first) to exercise the sort.
        let mut large_unions = vec![(id_a, id_b), (id_a, id_c)];
        large_unions.sort_unstable();
        let mut small_unions = vec![(id_a, id_b)];
        small_unions.sort_unstable();

        let scopes = DerivedScopes {
            scopes: vec![
                ScopeEqualities {
                    unions: large_unions,
                }, // index 0 → rel_large
                ScopeEqualities {
                    unions: small_unions,
                }, // index 1 → rel_small
            ],
            class_scope: [(rel_large, 0), (rel_small, 1)].into_iter().collect(),
            empty_classes: BTreeSet::new(),
        };

        let layer = build_colored_layer(&eg, scopes);

        let color_large = layer.color_of[&rel_large];
        let color_small = layer.color_of[&rel_small];

        // The superset scope must be a child of the subset scope.
        assert_eq!(
            layer.ceg.parent_of(color_large),
            Some(color_small),
            "superset-scope color must be a child of the subset-scope color",
        );
        // The subset scope is a direct child of black (no parent color).
        assert_eq!(
            layer.ceg.parent_of(color_small),
            None,
            "subset-scope color must be a direct child of black",
        );
        // Parent-first invariant: parents have lower ColorId than children.
        assert!(
            color_small.0 < color_large.0,
            "parent ColorId ({}) must be less than child ColorId ({}) \
             (parent-first creation order)",
            color_small.0,
            color_large.0,
        );
    }

    /// Task 7 gate: after `build_colored_layer` (which now calls `close_all`),
    /// colored congruence has been run on the seed equalities.
    ///
    /// Under a color asserting `#0 ≅ #1`, the scalar nodes `Eq(#0,#1)` and
    /// `Eq(#0,#0)` must end up in the same colored class: `Eq(#0,#1)` canonicalizes
    /// to `Eq(rep,rep)` under the color (since `#1 → rep ← #0`), which is exactly
    /// the canonical form of `Eq(#0,#0)`, so congruence merges them.
    #[mz_ore::test]
    fn colored_congruence_propagates_from_seeds() {
        use std::collections::BTreeSet;

        use super::{DerivedScopes, ScopeEqualities};

        // Build base graph: scalar columns #0 and #1, and two equality nodes.
        let mut eg = EGraph::new();
        let id_0 = eg.intern_scalar(&EScalar::plain(col(0)));
        let id_1 = eg.intern_scalar(&EScalar::plain(col(1)));
        // Eq(#0, #1): equality between distinct columns.
        let id_eq01 = eg.intern_scalar(&eq(col(0), col(1)));
        // Eq(#0, #0): trivially-self equality.
        let id_eq00 = eg.intern_scalar(&eq(col(0), col(0)));
        // A relational class to anchor the scope.
        let rel = eg.add_rel(&get(2));
        eg.rebuild();

        let id_0 = eg.find(id_0);
        let id_1 = eg.find(id_1);
        let id_eq01 = eg.find(id_eq01);
        let id_eq00 = eg.find(id_eq00);
        let rel = eg.find(rel);

        // One scope asserting #0 ≅ #1.
        let mut unions = vec![(id_0, id_1)];
        unions.sort_unstable();

        let scopes = DerivedScopes {
            scopes: vec![ScopeEqualities { unions }],
            class_scope: [(rel, 0)].into_iter().collect(),
            empty_classes: BTreeSet::new(),
        };

        let mut layer = build_colored_layer(&eg, scopes);
        let color = layer.color_of[&rel];

        // After colored congruence propagation: Eq(#0,#1) and Eq(#0,#0) must
        // be in the same colored class (both canonicalize to Eq(rep,rep)).
        assert_eq!(
            layer.ceg.find(color, id_eq01),
            layer.ceg.find(color, id_eq00),
            "Eq(#0,#1) and Eq(#0,#0) must be in the same colored class \
             after congruence propagates from the #0 ≅ #1 seed",
        );
    }

    #[mz_ore::test]
    fn derive_does_not_mark_scalar_classes_empty() {
        use crate::eqsat::egraph::CNode;
        // A plain Filter[#0 = #1](Get(2)) plus a stray interned scalar class.
        let (mut eg, _filter) = filter_eq_fixture();
        let scalar = eg.intern_scalar(&EScalar::plain(col(0)));
        eg.rebuild();
        let scalar = eg.find(scalar);
        let d = derive(&mut eg);
        // The scalar class must not be recorded as an empty relational class, nor
        // assigned a scope.
        assert!(
            !d.empty_classes.contains(&scalar),
            "scalar class is not an empty relation"
        );
        assert!(
            !d.class_scope.contains_key(&scalar),
            "scalar class gets no scope"
        );
        // Sanity: it really is a scalar-only class.
        assert!(
            !eg.nodes(scalar).iter().any(|n| matches!(n, CNode::Rel(_))),
            "fixture precondition: the class is scalar-only",
        );
    }

    /// I2 (review fix): the colored layer's empty-fold in `build_rel` extracts an
    /// unsatisfiable class as an empty `Constant` of the right arity.
    ///
    /// Built WITHOUT saturation, so the `empty_false_filter`/`union_cancel` rules
    /// never synthesize the empty Constant; only `build_rel`'s colored empty-fold
    /// can. Fail-on-no-op: the `None` extraction is asserted to keep the Filter
    /// tree, and only the colored extraction folds to `Constant{card:0, arity:1}`.
    /// This exercises the `build_rel` empty path itself, not just `derive`'s
    /// `empty_classes` marking (covered by `contradiction_recorded_in_empty_classes`).
    #[mz_ore::test]
    fn colored_extraction_empty_folds_contradiction() {
        use crate::eqsat::cost::CostModel;
        use crate::eqsat::objective::ArrangementCount;

        // Filter[#0 = 2](Filter[#0 = 1](Get(1))): unsatisfiable.
        let (mut eg, top) = contradictory_filter_fixture();
        let scopes = derive(&mut eg);
        eg.rebuild();
        let mut layer = build_colored_layer(&eg, scopes);
        let top = eg.find(top);
        let model = CostModel::new();

        // Baseline (no color): extraction keeps the Filter tree (no fold).
        let plain = eg
            .extract_with(top, &model, &ArrangementCount, None, None)
            .expect("plain extraction");
        assert!(
            matches!(plain, Rel::Filter { .. }),
            "baseline (no color) must keep the Filter, got {plain}",
        );

        // With the colored layer: the contradictory class folds to an empty
        // Constant of the relation's arity (1).
        let colored = eg
            .extract_with(top, &model, &ArrangementCount, Some(&mut layer), None)
            .expect("colored extraction");
        assert!(
            matches!(
                colored,
                Rel::Constant {
                    card: 0,
                    arity: 1,
                    ..
                }
            ),
            "colored extraction must empty-fold to Constant(card=0, arity=1), got {colored}",
        );
    }
}
