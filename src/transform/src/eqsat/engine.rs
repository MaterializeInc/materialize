// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The optimizer compiled from a rule set.
//!
//! [`Optimizer`] saturates an e-graph (exploring the transform graph in a
//! worst-case-optimal manner via [`crate::eqsat::egraph`]) and extracts the cheapest
//! plan, so it never gets stuck in a local minimum that a greedy
//! cost-monotone rewriter would. Conditions are evaluated by a single
//! evaluator, the e-class one in [`crate::eqsat::egraph`], over the saturated graph.

use std::collections::{BTreeMap, HashMap};

use crate::eqsat::analysis::{LocalFacts, letrec_local_facts};
use crate::eqsat::colored_derive::{DerivedScopes, build_colored_layer, derive};
use crate::eqsat::core::Id;
use crate::eqsat::cost::{Cost, CostModel};
use crate::eqsat::egraph::{EGraph, IndexedFilterSeed};
use crate::eqsat::ir::Rel;
use crate::eqsat::rules::CompiledRuleSet;

/// Bound on the scope-refinement loop that re-analyzes a binding scope after
/// rewriting it: a rewrite can prove a stronger recursive invariant that enables
/// another, strictly cost-lowering rewrite. The loop adopts round 0
/// unconditionally, then accepts a further round only when it strictly lowers
/// cost under the active objective. A cost-equal re-spelling is the extraction
/// flap, not refinement, so the loop rejects it and stops. Convergence is fast
/// because genuine improvements are rare, so a small cap suffices.
const SCOPE_REFINE_ROUNDS: usize = 4;

/// SP4d colored saturation: run `colored_saturate` after `build_colored_layer`
/// at every extraction site before extracting. Off by default until goldens are
/// regenerated in T12; flip to `true` locally to capture the corpus differential.
pub(crate) const COLORED_SATURATION: bool = true;

/// A faster-but-heavier alternative plan: switching to it would improve
/// time (CPU work) at the cost of more memory (arranged collections).
#[derive(Clone, Debug)]
pub struct Recommendation {
    /// The time-optimal alternative plan.
    pub plan: Rel,
    /// Cost of the alternative plan.
    pub cost: Cost,
}

/// The outcome of optimizing a plan.
#[derive(Clone, Debug)]
pub struct Outcome {
    pub plan: Rel,
    pub initial_cost: Cost,
    pub final_cost: Cost,
    /// Number of saturation (or descent) iterations performed.
    pub iterations: usize,
    /// A faster-but-heavier alternative, if the time-optimal plan differs
    /// from the memory-optimal default and offers a strict time improvement at
    /// the cost of more memory.
    ///
    /// `None` when both orderings agree, or when there is no strict time
    /// benefit.
    ///
    /// Note: recommendation is computed only for the top-level Let-free
    /// fragment; scoped/recursive fragments are not covered here.
    pub recommendation: Option<Recommendation>,
}

/// The saturating optimizer: equality saturation + cheapest-plan extraction.
#[derive(Clone, Debug)]
pub struct Optimizer {
    rules: CompiledRuleSet,
    model: CostModel,
    max_iters: usize,
    /// Whether to union a non-recursive `Let` definition into the body's e-graph
    /// so its analysis facts reach the body's `Get` references. On by default;
    /// the only reason to disable it is a control that proves a Let-crossing win
    /// requires the union.
    union_let_defs: bool,
    /// Indexed-filter seeds applied to every Let-free fragment's e-graph before
    /// saturation (see [`EGraph::seed_indexed_filters`]). Empty in the logical
    /// pass. The physical pass populates it from the production detector.
    seeds: Vec<IndexedFilterSeed>,
    /// Orders candidate costs during extraction. Defaults to arrangement count.
    objective: std::sync::Arc<dyn crate::eqsat::objective::Objective>,
    /// Reconstructs the chosen plan. Defaults to the greedy extractor.
    extractor: std::sync::Arc<dyn crate::eqsat::extract::Extractor>,
    /// Enable the delta-aware join-cost spelling selector at extraction.
    delta_join_cost: bool,
}

impl Optimizer {
    pub fn new(rules: CompiledRuleSet, model: CostModel) -> Self {
        Optimizer {
            rules,
            model,
            max_iters: 100,
            union_let_defs: true,
            seeds: Vec::new(),
            objective: std::sync::Arc::new(crate::eqsat::objective::ArrangementCount),
            extractor: std::sync::Arc::new(crate::eqsat::extract::GreedyExtractor),
            delta_join_cost: false,
        }
    }

    /// Run the colored derivation over a saturated Let-free fragment's e-graph,
    /// returning the derived scopes (from which the caller builds the
    /// [`ColoredLayer`] over the now-frozen graph).
    ///
    /// CRITICAL ordering (SP4b): `derive` mutates `eg` (it interns the reduced
    /// scalar spellings the colored layer unions over), so it MUST run after
    /// `seed_indexed_filters` and BEFORE the `rebuild()` that freezes the graph
    /// for extraction — hence the `rebuild()` here. Returning owned
    /// [`DerivedScopes`] (no borrow of `eg`) lets the caller take the shared
    /// borrows extraction needs while the layer is alive.
    ///
    /// [`ColoredLayer`]: crate::eqsat::colored_derive::ColoredLayer
    fn colored_scopes(&self, eg: &mut EGraph) -> DerivedScopes {
        let scopes = derive(eg);
        eg.rebuild();
        scopes
    }

    /// Override the extraction objective.
    pub fn with_objective(
        mut self,
        objective: std::sync::Arc<dyn crate::eqsat::objective::Objective>,
    ) -> Self {
        self.objective = objective;
        self
    }

    /// Override the extractor.
    // `Extractor` is crate-internal (`pub(crate)`), so this builder is too; the
    // only caller (`eqsat::optimize_inner`) is in-crate.
    pub(crate) fn with_extractor(
        mut self,
        extractor: std::sync::Arc<dyn crate::eqsat::extract::Extractor>,
    ) -> Self {
        self.extractor = extractor;
        self
    }

    /// Enable the delta-aware join-cost spelling selector at extraction.
    pub fn with_delta_join_cost(mut self, enabled: bool) -> Self {
        self.delta_join_cost = enabled;
        self
    }

    /// Name of the active objective, for diagnostics and tests.
    pub fn objective_name(&self) -> &'static str {
        self.objective.name()
    }

    /// Seed the given indexed-filter decisions into each fragment's e-graph
    /// before saturation. Used by the physical pass.
    pub fn with_seeds(mut self, seeds: Vec<IndexedFilterSeed>) -> Self {
        self.seeds = seeds;
        self
    }

    /// Disable unioning non-recursive `Let` definitions into the body e-graph.
    ///
    /// Used only by the control half of the Let-crossing-win test, to show that
    /// the win disappears when the union is off (the facts stay trapped behind
    /// the `Get`). Production always keeps the union on.
    pub fn without_let_union(mut self) -> Self {
        self.union_let_defs = false;
        self
    }

    /// Optimize `plan` by saturating an e-graph and extracting the cheapest
    /// equivalent plan.
    ///
    /// `Let`/`LetRec` are *binding scopes*, not relational operators, and a
    /// recursive `LetRec` closes a cycle that finite saturation cannot
    /// represent.  So they are handled **structurally**: the optimizer treats
    /// each scope as a boundary, recursively optimizing every maximal Let-free
    /// fragment (binding values and bodies) by equality saturation, then
    /// reassembling the scope.  Recursive references are opaque `LocalGet`
    /// leaves within a fragment.  Analyses still flow *through* the recursion
    /// via the recursion-aware fixpoint in [`crate::eqsat::analysis`].
    ///
    /// After saturation the memory-first plan (default) is returned.  A
    /// time-first alternative is also extracted; if it is strictly faster but
    /// uses more memory it is reported in [`Outcome::recommendation`].
    pub fn optimize(&self, plan: Rel) -> Outcome {
        let initial_cost = self.model.cost(&plan);
        // Scalars are opaque MirScalarExpr in-tree; scalar folding is left to
        // the existing FoldConstants transform.
        let (best, time_alt, iterations) =
            self.optimize_node_with_alt(plan, &LocalFacts::default());
        let final_cost = self.model.cost(&best);

        // Compute a recommendation if the time-first alternative is strictly
        // faster on time but uses more memory than the chosen plan.
        let recommendation = time_alt.and_then(|alt| {
            if alt == best {
                return None;
            }
            let alt_cost = self.model.cost(&alt);
            // Strictly faster on time?
            let faster = alt_cost.cmp_time_first(&final_cost) == std::cmp::Ordering::Less;
            // Uses strictly more memory?
            let heavier = alt_cost.cmp_memory_first(&final_cost) == std::cmp::Ordering::Greater;
            if faster && heavier {
                Some(Recommendation {
                    plan: alt,
                    cost: alt_cost,
                })
            } else {
                None
            }
        });

        Outcome {
            plan: best,
            initial_cost,
            final_cost,
            iterations,
            recommendation,
        }
    }

    /// Like `optimize_node` but also returns the time-first alternative
    /// extracted from the same saturated e-graph (for Let-free fragments only;
    /// scoped fragments return `None` for the alternative).
    ///
    /// The triple is `(memory_first_plan, time_first_plan, iterations)`.
    fn optimize_node_with_alt(&self, plan: Rel, facts: &LocalFacts) -> (Rel, Option<Rel>, usize) {
        let plan = normalize_push_into_scopes(plan);
        match &plan {
            Rel::Let { .. } | Rel::LetRec { .. } => {
                let (p, i) = self.optimize_scope(plan, facts);
                (p, None, i)
            }
            _ if contains_scope(&plan) => {
                let (p, i) = self.optimize_around_scopes(plan, facts);
                (p, None, i)
            }
            _ => {
                let mut eg = EGraph::new();
                eg.set_available(self.model.available().clone());
                let root = eg.add_rel(&plan);
                let iterations = eg.saturate(&self.rules, self.max_iters, facts);
                // Seed after saturation: a literal filter wrapped in a Project
                // (`Filter(Project(Get))`) only exposes the bare `Filter(Get)`
                // node the seeder matches once `push_filter_past_project` has
                // fired. Seeding pre-saturation would miss it.
                eg.seed_indexed_filters(&self.seeds);
                // Pre-run the structural Equivalences analysis when the delta-join-cost
                // flag is set: collect per-e-class equivalence facts into an owned map
                // (immutable borrows of `eg`) BEFORE `colored_scopes` takes `&mut eg`.
                let eq_facts = self.collect_eq_facts(&eg);
                // Build the colored layer (SP4b) over the frozen graph.
                let scopes = self.colored_scopes(&mut eg);
                let mut layer = build_colored_layer(&eg, scopes);
                if COLORED_SATURATION {
                    crate::eqsat::colored::saturate::colored_saturate(&mut layer, &eg);
                }
                // Extraction returns `None` when the root has no representative
                // satisfying the polarity constraints (only malformed input can
                // reach this). Fall back to the original, un-optimized fragment:
                // skipping optimization is always a sound no-op.
                let Some(mem_plan) = self.extractor.extract(
                    &eg,
                    root,
                    &self.model,
                    self.objective.as_ref(),
                    Some(&mut layer),
                    eq_facts.as_ref(),
                ) else {
                    return (plan.clone(), None, iterations);
                };
                // The time-first alternate is only a recommendation, and the
                // time objective is compositional, so it always uses the greedy
                // extractor regardless of the configured primary extractor. It
                // shares the same colored layer.
                // Spelling selector not applied to the time-first alternative:
                // it is a recommendation only and v1 scopes selector to the
                // main memory-first extraction path.
                let time_plan = eg.extract_with(
                    root,
                    &self.model,
                    &crate::eqsat::objective::TimeFirst,
                    Some(&mut layer),
                    None,
                );
                (mem_plan, time_plan, iterations)
            }
        }
    }

    /// Optimize one node under the recursion facts `facts` (the proven
    /// non-negativity / monotonicity / keys of every in-scope `LocalGet`).
    /// Saturate it if it is a Let-free fragment; otherwise walk past the binding
    /// scope.  Returns the optimized plan and the saturation iterations spent.
    fn optimize_node(&self, plan: Rel, facts: &LocalFacts) -> (Rel, usize) {
        // Normalize first: push any single-input operator sitting directly above
        // a binding scope *into* the scope's body. `O(LetRec x = b in B)` denotes
        // `O(B[x*])`, and so does `LetRec x = b in O(B)` — the bindings (hence
        // the fixpoint `x*`) are untouched — so this is unconditionally sound. It
        // moves the operator inside the scope, where the recursion facts can act
        // on it.  (It does *not* push into the recursive bindings; see
        // `COVERAGE.md` on why predicate pushdown *through* a recursion is
        // unsound without a commutation side-condition.)
        let plan = normalize_push_into_scopes(plan);
        match &plan {
            // A binding scope: analyze the recursion and optimize its fragments
            // with those facts injected.
            Rel::Let { .. } | Rel::LetRec { .. } => self.optimize_scope(plan, facts),
            // A node *above* a scope (necessarily multi-input, e.g. a Union/Join
            // with a scope argument — unary ones were pushed in above): rewrite
            // the region around the recursion, treating each maximal scope
            // subtree as an opaque value carrying its proven properties (A).
            _ if contains_scope(&plan) => self.optimize_around_scopes(plan, facts),
            // A maximal Let-free fragment: saturate and extract, seeding the
            // analyses with the recursion facts so analysis-gated rules can fire
            // on recursive references.
            _ => {
                let mut eg = EGraph::new();
                eg.set_available(self.model.available().clone());
                let root = eg.add_rel(&plan);
                let iterations = eg.saturate(&self.rules, self.max_iters, facts);
                // Seed after saturation (see `optimize_node_with_alt`): the bare
                // `Filter(Get)` node a Project-wrapped literal filter matches is
                // only exposed once `push_filter_past_project` has fired.
                eg.seed_indexed_filters(&self.seeds);
                // Apply the spelling selector when the delta-join-cost flag is
                // set: collect per-e-class equivalence facts before
                // `colored_scopes` takes &mut eg.
                let eq_facts = self.collect_eq_facts(&eg);
                // Build the colored layer (SP4b) over the frozen graph.
                let scopes = self.colored_scopes(&mut eg);
                let mut layer = build_colored_layer(&eg, scopes);
                if COLORED_SATURATION {
                    crate::eqsat::colored::saturate::colored_saturate(&mut layer, &eg);
                }
                // `None` when no representative satisfies the polarity
                // constraints; fall back to the un-optimized fragment, a sound
                // no-op.
                match self.extractor.extract(
                    &eg,
                    root,
                    &self.model,
                    self.objective.as_ref(),
                    Some(&mut layer),
                    eq_facts.as_ref(),
                ) {
                    Some(best) => (best, iterations),
                    None => (plan.clone(), iterations),
                }
            }
        }
    }

    /// Collect the structural Equivalences analysis facts for `eg` when the
    /// delta-join-cost selector is enabled; `None` otherwise.
    ///
    /// The returned map covers every e-class id for which the analysis produced a
    /// non-`None` result.  Callers must invoke this **before** `colored_scopes`
    /// takes `&mut eg`, because `run_analysis` needs a shared borrow of the graph.
    fn collect_eq_facts(
        &self,
        eg: &EGraph,
    ) -> Option<HashMap<Id, crate::analysis::equivalences::EquivalenceClasses>> {
        if !self.delta_join_cost {
            return None;
        }
        let arity = |c: Id| eg.arity(c);
        let arity_fn: &dyn Fn(Id) -> usize = &arity;
        let ctx = crate::eqsat::analysis::RelCtx {
            arity: arity_fn,
            data: eg.data(),
        };
        let facts = eg.run_analysis(
            &crate::eqsat::analysis::Equivalences {
                // Empty `locals`: the Let-body path relies on value-into-Get union
                // for fact propagation, not `locals`. Only makes the selector more
                // conservative (never unsound); revisit when the feature is activated.
                locals: std::collections::BTreeMap::new(),
            },
            ctx,
        );
        Some(
            facts
                .into_iter()
                .filter_map(|(id, opt)| opt.map(|ec| (id, ec)))
                .collect(),
        )
    }

    /// Optimize a `Let`/`LetRec` scope. We solve the recursion-aware analyses
    /// for the bound ids (extending `outer`), inject the resulting facts while
    /// optimizing each binding value and the body, and repeat: a rewrite can
    /// expose a stronger invariant that unlocks another. This is option B — the
    /// recursion fixpoint feeding the in-fragment rewriter — and it is sound
    /// because each fact is a greatest-/least-fixpoint certificate on the
    /// current syntactic form, and equality rewrites preserve the underlying
    /// property.
    fn optimize_scope(&self, plan: Rel, outer: &LocalFacts) -> (Rel, usize) {
        let (mut bindings, limits, mut body, recursive) = match plan {
            Rel::LetRec {
                bindings,
                limits,
                body,
            } => (bindings, limits, *body, true),
            Rel::Let { id, value, body } => (vec![(id, *value)], vec![], *body, false),
            _ => unreachable!("optimize_scope on a non-scope node"),
        };

        let mut total = 0;
        // The loop refines a scope by re-optimizing its bindings and body with the
        // facts proven of the previous round's bindings, which can be strictly
        // stronger and enable a cost-lowering rewrite. A round that only re-spells
        // at equal cost is the extraction flap, not refinement, so it is rejected:
        // we keep the incumbent and stop. Round 0 is always adopted because
        // extraction returns a plan no costlier than its input, so its result is
        // the optimization to emit, not the unoptimized input. Extraction is
        // deterministic per input, so a rejected round would only be re-derived,
        // hence break rather than continue.
        let mut incumbent_cost: Option<Cost> = None;
        for _ in 0..SCOPE_REFINE_ROUNDS {
            let facts = letrec_local_facts(&bindings, outer);

            let mut next = Vec::with_capacity(bindings.len());
            for (id, value) in &bindings {
                let (v, i) = self.optimize_node(value.clone(), &facts);
                total += i;
                next.push((*id, v));
            }
            // For a non-recursive `Let x = v in body`, optimize the body in an
            // e-graph that also contains the optimized definition `v`, with the
            // `Get x` (`LocalGet`) class unioned to `v`'s root. The union makes
            // `v`'s e-class analysis facts (e.g. constant columns) reach the
            // body's `Get x` via congruence, un-trapping them across the binding
            // boundary so an analysis-gated rule can fire on the reference. The
            // body still extracts to `LocalGet x` references (shared, not
            // inlined), so the reassembled scope below stays a correct, sharing
            // `Let`. Recursive `LetRec` bindings stay on the opaque
            // `optimize_node` path: unioning a recursive reference into its own
            // definition would close an e-graph cycle that breaks extraction.
            let (nb, i) = if recursive || !self.union_let_defs {
                self.optimize_node(body.clone(), &facts)
            } else {
                let (id, value) = &next[0];
                self.optimize_body_with_let_union(body.clone(), *id, value, &facts)
            };
            total += i;

            let cand_cost = self
                .model
                .cost(&assemble_scope(recursive, &next, &limits, &nb));
            if accept_round(self.objective.as_ref(), &cand_cost, incumbent_cost.as_ref()) {
                bindings = next;
                body = nb;
                incumbent_cost = Some(cand_cost);
            } else {
                break;
            }
        }

        let result = if recursive {
            Rel::LetRec {
                bindings,
                limits,
                body: Box::new(body),
            }
        } else {
            let (id, value) = bindings.into_iter().next().unwrap();
            Rel::Let {
                id,
                value: Box::new(value),
                body: Box::new(body),
            }
        };
        (result, total)
    }

    /// Optimize the body of a non-recursive `Let id = value in body`, unioning
    /// `value` into the body's e-graph so its e-class analysis facts reach the
    /// body's `Get id` (`LocalGet`) references via congruence.
    ///
    /// `value` is the already-optimized definition; the union is purely for fact
    /// propagation, the binding's emitted definition is `value` unchanged (the
    /// caller reassembles `Let id = value in <returned body>`). The body extracts
    /// to `LocalGet id` references, which the cost model keeps shared rather than
    /// inlining the definition (a `LocalGet` is a free leaf, cheaper than any
    /// re-materialized definition), so the result stays a correct sharing `Let`.
    ///
    /// When the body is not a single Let-free fragment (it nests further scopes),
    /// the union has no single body e-graph to attach to, so we fall back to the
    /// ordinary opaque path. This keeps the change localized to the common shape;
    /// nested-scope bodies are an optional refinement, not a correctness gap.
    fn optimize_body_with_let_union(
        &self,
        body: Rel,
        id: usize,
        value: &Rel,
        facts: &LocalFacts,
    ) -> (Rel, usize) {
        let body = normalize_push_into_scopes(body);
        // The union only makes sense for a body that is itself a saturable
        // Let-free fragment with a `Get id` reference. A body that is or contains
        // a binding scope has no single e-graph to union into; defer to the
        // opaque path, which is unchanged and always sound.
        if contains_scope(&body) {
            return self.optimize_node(body, facts);
        }
        let Some(local) = find_local_get(&body, id) else {
            // The body does not reference the binding: nothing to un-trap, so the
            // ordinary fragment path is equivalent.
            return self.optimize_node(body, facts);
        };

        let mut eg = EGraph::new();
        eg.set_available(self.model.available().clone());
        let root = eg.add_rel(&body);
        // The `LocalGet id` class as it appears in the body. Re-adding the exact
        // node (same arity and `get`) hash-conses to the existing class.
        let get_class = eg.add_rel(&local);
        // The optimized definition's root, added to the same e-graph.
        let value_class = eg.add_rel(value);
        // Equate the reference with the definition: they denote the same
        // relation, so every fact proven of `value` now holds of `Get id`.
        eg.union(get_class, value_class);
        eg.rebuild();
        let iterations = eg.saturate(&self.rules, self.max_iters, facts);
        // Seed after saturation, as on the Let-free path. Unioning the binding
        // definition into the `Get id` class lets `push_filter_past_project`
        // expose a bare `Filter(Get global)` for a literal filter over a
        // Project-bound `Get` (the shared-CSE shape in `relation_cse`), which the
        // seeder then attaches the index lookup to.
        eg.seed_indexed_filters(&self.seeds);
        // Apply the spelling selector to Joins in the Let body when the
        // delta-join-cost flag is set. Collect per-e-class equivalence facts
        // BEFORE `colored_scopes` takes &mut eg (same pattern as
        // `optimize_node_with_alt`). The binding value was unioned into the
        // `Get id` class above, so analysis facts proven of the value propagate
        // to the body's reference — the selector sees the same structural
        // equivalences as for a Let-free join.
        let eq_facts = self.collect_eq_facts(&eg);
        // Build the colored layer (SP4b) over the frozen graph.
        let scopes = self.colored_scopes(&mut eg);
        let mut layer = build_colored_layer(&eg, scopes);
        if COLORED_SATURATION {
            crate::eqsat::colored::saturate::colored_saturate(&mut layer, &eg);
        }
        // `None` when no representative satisfies the polarity constraints; fall
        // back to the un-optimized body, a sound no-op.
        match self.extractor.extract(
            &eg,
            root,
            &self.model,
            self.objective.as_ref(),
            Some(&mut layer),
            eq_facts.as_ref(),
        ) {
            Some(best) => (best, iterations),
            None => (body, iterations),
        }
    }

    /// Optimize a fragment that sits *above* one or more binding scopes (option
    /// A, sound core). We cannot e-match *through* the recursive back-edge
    /// soundly — the fixpoint equation `x = body(x)` holds only at the fixpoint,
    /// so rewriting with it can change the denoted least fixpoint (see
    /// `COVERAGE.md`). What *is* sound is to treat each maximal recursion as an
    /// opaque relation that carries the properties the recursion fixpoint
    /// proves (non-negative / monotone / keyed), then let the ordinary rules
    /// rewrite the surrounding fragment using those facts — e.g. eliding a
    /// `Threshold` wrapped around a provably non-negative recursion.
    fn optimize_around_scopes(&self, plan: Rel, facts: &LocalFacts) -> (Rel, usize) {
        // Keep the original fragment to fall back on if extraction fails.
        let original = plan.clone();
        // Replace each maximal scope subtree with a fresh opaque `LocalGet`
        // placeholder (ids chosen above any real bound id, so they cannot clash
        // with genuine recursive references).
        let mut next_id = max_local_id(&plan) + 1;
        let mut scopes: Vec<(usize, Rel)> = Vec::new();
        let placeholder_plan = hoist_scopes(plan, &mut next_id, &mut scopes);

        // Optimize each scope and record its proven properties under the
        // placeholder id, extending the incoming facts.
        let mut ext = facts.clone();
        let mut subst: BTreeMap<usize, Rel> = BTreeMap::new();
        let mut iters = 0;
        for (id, scope) in scopes {
            let (opt, i) = self.optimize_node(scope, facts);
            iters += i;
            ext.nonneg
                .insert(id, crate::eqsat::analysis::rel_non_negative(&opt));
            ext.monotonic
                .insert(id, crate::eqsat::analysis::rel_monotonic(&opt));
            ext.keys.insert(id, crate::eqsat::analysis::rel_keys(&opt));
            subst.insert(id, opt);
        }

        // Saturate the (now Let-free) surrounding fragment with those facts,
        // then splice the optimized scopes back in.
        let mut eg = EGraph::new();
        eg.set_available(self.model.available().clone());
        let root = eg.add_rel(&placeholder_plan);
        iters += eg.saturate(&self.rules, self.max_iters, &ext);
        // Build the colored layer (SP4b) over the frozen graph.
        let scopes = self.colored_scopes(&mut eg);
        let mut layer = build_colored_layer(&eg, scopes);
        if COLORED_SATURATION {
            crate::eqsat::colored::saturate::colored_saturate(&mut layer, &eg);
        }
        // `None` when no representative satisfies the polarity constraints; fall
        // back to the original, un-optimized fragment, a sound no-op.
        // Joins in fragments above scopes are out of v1 scope for the spelling
        // selector and fall back to the base spelling.
        match self.extractor.extract(
            &eg,
            root,
            &self.model,
            self.objective.as_ref(),
            Some(&mut layer),
            None,
        ) {
            Some(extracted) => (substitute_locals(extracted, &subst), iters),
            None => (original, iters),
        }
    }
}

/// Push every single-input operator that sits directly above a binding scope
/// *into* that scope's body, to a fixpoint: `O(LetRec x = b in B)` becomes
/// `LetRec x = b in O(B)` (and likewise for `Let`). Sound for any unary
/// operator because it is a function of its one input and the bindings are
/// untouched. Multi-input operators (`Union`/`Join`) are left in place (pushing
/// them in would pull their other arguments into the scope).
fn normalize_push_into_scopes(rel: Rel) -> Rel {
    if !contains_scope(&rel) {
        return rel;
    }
    // Normalize children first.
    let children: Vec<Rel> = rel
        .children()
        .into_iter()
        .cloned()
        .map(normalize_push_into_scopes)
        .collect();
    let rel = rel.with_children(children);

    // A single-input operator (not itself a scope) directly over a scope: push.
    let unary_over_scope = rel.children().len() == 1
        && !matches!(rel, Rel::Let { .. } | Rel::LetRec { .. })
        && matches!(rel.children()[0], Rel::Let { .. } | Rel::LetRec { .. });
    if !unary_over_scope {
        return rel;
    }
    let scope = rel.children()[0].clone();
    match scope {
        Rel::LetRec {
            bindings,
            limits,
            body,
        } => {
            let new_body = normalize_push_into_scopes(rel.with_children(vec![*body]));
            Rel::LetRec {
                bindings,
                limits,
                body: Box::new(new_body),
            }
        }
        Rel::Let { id, value, body } => {
            let new_body = normalize_push_into_scopes(rel.with_children(vec![*body]));
            Rel::Let {
                id,
                value,
                body: Box::new(new_body),
            }
        }
        _ => unreachable!("unary_over_scope guaranteed a scope child"),
    }
}

/// Replace every maximal `Let`/`LetRec` subtree of `rel` with a fresh opaque
/// `LocalGet` placeholder, collecting the `(id, subtree)` pairs in `out`.
fn hoist_scopes(rel: Rel, next_id: &mut usize, out: &mut Vec<(usize, Rel)>) -> Rel {
    if matches!(rel, Rel::Let { .. } | Rel::LetRec { .. }) {
        let id = *next_id;
        *next_id += 1;
        let arity = rel.arity();
        out.push((id, rel));
        return Rel::LocalGet {
            id,
            arity,
            get: None,
            version: None,
        };
    }
    let children: Vec<Rel> = rel
        .children()
        .into_iter()
        .cloned()
        .map(|c| hoist_scopes(c, next_id, out))
        .collect();
    rel.with_children(children)
}

/// Splice scope subtrees back in for their placeholder `LocalGet`s.
fn substitute_locals(rel: Rel, subst: &BTreeMap<usize, Rel>) -> Rel {
    if let Rel::LocalGet { id, .. } = &rel {
        if let Some(r) = subst.get(id) {
            return r.clone();
        }
    }
    let children: Vec<Rel> = rel
        .children()
        .into_iter()
        .cloned()
        .map(|c| substitute_locals(c, subst))
        .collect();
    rel.with_children(children)
}

/// The largest local id anywhere in `rel`, for choosing fresh, non-clashing
/// placeholder ids.
///
/// Like `cse::max_local_id`, this must look inside `Rel::Opaque` leaves: `lower`
/// bails unsupported nodes (notably `LetRec`) into an opaque `MirRelationExpr`
/// that can carry its own `LocalId`s, and an opaque leaf has no `Rel` children,
/// so a `Rel`-only walk would miss them. A placeholder id colliding with such an
/// id would shadow a genuine recursive reference.
fn max_local_id(rel: &Rel) -> usize {
    let here = match rel {
        Rel::LocalGet { id, .. } | Rel::Let { id, .. } => *id,
        Rel::LetRec { bindings, .. } => bindings.iter().map(|(id, _)| *id).max().unwrap_or(0),
        Rel::Opaque(mir) => crate::eqsat::cse::max_mir_local_id(mir),
        _ => 0,
    };
    rel.children()
        .iter()
        .map(|c| max_local_id(c))
        .fold(here, usize::max)
}

/// Find a `LocalGet` of `id` anywhere in `rel`, returning a clone of it (so the
/// caller can re-add the exact node, with its arity and `get`, to hit the body's
/// existing `Get id` e-class when unioning). Returns `None` if `rel` does not
/// reference `id`.
fn find_local_get(rel: &Rel, id: usize) -> Option<Rel> {
    if let Rel::LocalGet { id: gid, .. } = rel {
        if *gid == id {
            return Some(rel.clone());
        }
    }
    rel.children()
        .into_iter()
        .find_map(|c| find_local_get(c, id))
}

/// Whether `rel` is, or contains anywhere, a binding scope (`Let`/`LetRec`).
/// Such trees cannot be added to the e-graph wholesale; the structural
/// optimizer peels the scopes and saturates the Let-free fragments between.
fn contains_scope(rel: &Rel) -> bool {
    matches!(rel, Rel::Let { .. } | Rel::LetRec { .. })
        || rel.children().iter().any(|c| contains_scope(c))
}

/// Whether a refinement round's candidate scope cost is accepted. Round 0 (no
/// incumbent yet) is always accepted, because extraction returns a plan no
/// costlier than its input, so its result is the optimization to emit. A later
/// round is accepted only when strictly cheaper under the active objective. A
/// cost-equal re-spelling is the extraction flap, not refinement, so it is
/// rejected. The comparison MUST use the objective, not `Cost::cmp_memory_first`,
/// because the default objective (`ArrangementCount`) minimizes the
/// `arrangements` field that `cmp_memory_first` ignores.
fn accept_round(
    objective: &dyn crate::eqsat::objective::Objective,
    candidate: &Cost,
    incumbent: Option<&Cost>,
) -> bool {
    match incumbent {
        None => true,
        Some(inc) => objective.cmp(candidate, inc) == std::cmp::Ordering::Less,
    }
}

/// Build a scope `Rel` from its parts, for costing a candidate refinement round
/// without consuming the parts. `recursive` selects `LetRec` vs `Let`.
fn assemble_scope(
    recursive: bool,
    bindings: &[(usize, Rel)],
    limits: &[Option<mz_expr::LetRecLimit>],
    body: &Rel,
) -> Rel {
    if recursive {
        Rel::LetRec {
            bindings: bindings.to_vec(),
            limits: limits.to_vec(),
            body: Box::new(body.clone()),
        }
    } else {
        let (id, value) = &bindings[0];
        Rel::Let {
            id: *id,
            value: Box::new(value.clone()),
            body: Box::new(body.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::{BinaryFunc, MirScalarExpr, func};
    use mz_repr::{Datum, ReprScalarType};

    use crate::eqsat::default_ruleset;
    use crate::eqsat::ir::{EScalar, Rel};

    use super::*;

    /// Optimize `plan` with the eqsat engine (the default colored path).
    fn optimize(plan: Rel) -> Rel {
        Optimizer::new(default_ruleset(), CostModel::new())
            .optimize(plan)
            .plan
    }

    /// The redundantly-recomputed scalar `#1 + 1`.
    fn computed_scalar() -> MirScalarExpr {
        MirScalarExpr::column(1).call_binary(
            MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
            BinaryFunc::AddInt64(func::AddInt64),
        )
    }

    /// `Map[#1 + 1](Filter[#0 = #1 + 1](Get(2)))`: the Filter establishes
    /// `#0 = (#1 + 1)`, so the Map recomputes a column equal to the existing `#0`.
    fn redundant_map_plan() -> Rel {
        let f = computed_scalar();
        Rel::Map {
            scalars: vec![EScalar::plain(f.clone())],
            input: Box::new(Rel::Filter {
                predicates: vec![EScalar::plain(
                    MirScalarExpr::column(0).call_binary(f, BinaryFunc::Eq(func::Eq)),
                )],
                input: Box::new(Rel::Get {
                    name: "t".to_string(),
                    arity: 2,
                }),
            }),
        }
    }

    /// Whether `rel` anywhere contains a `Map` that recomputes `scalar`.
    fn maps_recompute(rel: &Rel, scalar: &MirScalarExpr) -> bool {
        let here = matches!(rel, Rel::Map { scalars, .. }
            if scalars.iter().any(|s| s.expr == *scalar));
        here || rel.children().iter().any(|c| maps_recompute(c, scalar))
    }

    #[mz_ore::test]
    fn colored_path_rewrites_redundant_map_compute() {
        // The colored extraction path replaces the redundant recomputation
        // `#1 + 1` with the equal column `#0` the Filter established, so the
        // optimized plan no longer recomputes that scalar in a Map.
        let out = optimize(redundant_map_plan());
        assert!(
            !maps_recompute(&out, &computed_scalar()),
            "colored extraction must eliminate the redundant `#1 + 1` recomputation; got {out:?}",
        );
    }

    /// The predicate `#0 = 1` over a `Get t0`.
    fn eq_col0_lit1() -> MirScalarExpr {
        MirScalarExpr::column(0).call_binary(
            MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
            BinaryFunc::Eq(func::Eq),
        )
    }

    /// `Filter[#0 = 1](Filter[#0 = 1](Get t0))`: the minimal trigger for the
    /// SP4d colored-scalar soundness bug. `merge_filters` fuses the two filters
    /// into a sibling node `Filter[#0=1, #0=1](Get)` that lives in the same class
    /// as `Filter[#0=1](Filter[#0=1](Get))`.
    fn nested_identical_filter_plan() -> Rel {
        let get = Rel::Get {
            name: "t0".to_string(),
            arity: 2,
        };
        Rel::Filter {
            predicates: vec![EScalar::plain(eq_col0_lit1())],
            input: Box::new(Rel::Filter {
                predicates: vec![EScalar::plain(eq_col0_lit1())],
                input: Box::new(get),
            }),
        }
    }

    /// Whether `rel` anywhere contains a `Filter` that retains `pred`.
    fn filters_on(rel: &Rel, pred: &MirScalarExpr) -> bool {
        let here = matches!(rel, Rel::Filter { predicates, .. }
            if predicates.iter().any(|p| p.expr == *pred));
        here || rel.children().iter().any(|c| filters_on(c, pred))
    }

    /// Whether `rel` is a bare `Get` (no enclosing Filter/operator).
    fn is_bare_get(rel: &Rel) -> bool {
        matches!(rel, Rel::Get { .. })
    }

    /// SP4d soundness regression: `Filter[#0=1](Filter[#0=1](Get t0))` must keep
    /// filtering `#0 = 1`. The colored scalar resolution must NOT collapse the
    /// fused sibling `Filter[#0=1, #0=1](Get)` down to a predicate-free `Get`
    /// (which would return ALL rows instead of the `#0 = 1` rows).
    ///
    /// RED before the input-context fix: `optimize` returns the bare `Get t0`.
    #[mz_ore::test]
    fn nested_identical_filters_retain_predicate() {
        let out = optimize(nested_identical_filter_plan());
        assert!(
            !is_bare_get(&out),
            "must not collapse to a predicate-free Get (soundness); got {out:?}",
        );
        assert!(
            filters_on(&out, &eq_col0_lit1()),
            "must retain the `#0 = 1` filter predicate (soundness); got {out:?}",
        );
    }

    use crate::eqsat::objective::ArrangementCount;

    fn cost_of(arrangements: usize, memory: Vec<f64>, time: Vec<f64>, nodes: usize) -> Cost {
        let memory_arity = vec![0; memory.len()];
        Cost {
            arrangements,
            memory,
            memory_arity,
            time,
            nodes,
        }
    }

    #[mz_ore::test]
    fn accept_round_zero_always_adopts() {
        // Incumbent None (round 0): adopt even a cost-equal-to-raw result.
        let c = cost_of(2, vec![1.0], vec![1.0], 5);
        assert!(accept_round(&ArrangementCount, &c, None));
    }

    #[mz_ore::test]
    fn accept_round_strict_improvement_adopts() {
        let inc = cost_of(3, vec![1.0], vec![1.0], 5);
        let better = cost_of(2, vec![1.0], vec![1.0], 5);
        assert!(accept_round(&ArrangementCount, &better, Some(&inc)));
    }

    #[mz_ore::test]
    fn accept_round_cost_equal_rejects() {
        let inc = cost_of(2, vec![1.0], vec![1.0], 5);
        let equal = cost_of(2, vec![1.0], vec![1.0], 5);
        assert!(!accept_round(&ArrangementCount, &equal, Some(&inc)));
    }

    #[mz_ore::test]
    fn accept_round_uses_objective_not_memory_first() {
        // The comparator trap: candidate has FEWER arrangements but identical
        // memory/time/nodes. The objective must see the arrangement win (accept),
        // while cmp_memory_first would tie (Equal) and wrongly reject.
        let inc = cost_of(3, vec![1.0], vec![1.0], 5);
        let cand = cost_of(2, vec![1.0], vec![1.0], 5);
        assert!(accept_round(&ArrangementCount, &cand, Some(&inc)));
        assert_eq!(cand.cmp_memory_first(&inc), std::cmp::Ordering::Equal);
    }

    /// Test-only extractor that returns a scripted sequence of plans, ignoring the
    /// e-graph. Models a deterministic cost-equal flap or a genuine improvement to
    /// exercise the refinement loop's convergence contract.
    #[derive(Debug)]
    struct ScriptedExtractor {
        calls: std::cell::Cell<usize>,
        seq: Vec<Rel>,
    }
    impl crate::eqsat::extract::Extractor for ScriptedExtractor {
        fn extract(
            &self,
            _eg: &crate::eqsat::egraph::EGraph,
            _root: crate::eqsat::egraph::Id,
            _model: &crate::eqsat::cost::CostModel,
            _objective: &dyn crate::eqsat::objective::Objective,
            _colored: Option<&mut crate::eqsat::colored_derive::ColoredLayer<'_>>,
            _spellings: Option<
                &std::collections::HashMap<
                    crate::eqsat::egraph::Id,
                    crate::analysis::equivalences::EquivalenceClasses,
                >,
            >,
        ) -> Option<Rel> {
            let i = self.calls.get();
            self.calls.set(i + 1);
            Some(self.seq[i.min(self.seq.len() - 1)].clone())
        }
    }

    #[mz_ore::test]
    fn refinement_loop_converges_under_cost_equal_flap() {
        // Scope: Let x = leaf in <body>. With without_let_union, each round runs
        // exactly two extract calls: optimize_node(value) then optimize_node(body).
        // Script a cost-equal flap on the body (A on round 0, B on round 1), binding
        // stable. A and B are two Constants of identical cost, different card.
        let constant = |card: u64| Rel::Constant {
            card,
            arity: 1,
            col_types: None,
        };
        let leaf = constant(1);
        let a = constant(2);
        let b = constant(3);
        let seq = vec![
            leaf.clone(),
            a.clone(), // round 0
            leaf.clone(),
            b.clone(), // round 1 (cost-equal, rejected)
        ];
        let ext = std::sync::Arc::new(ScriptedExtractor {
            calls: std::cell::Cell::new(0),
            seq,
        });
        let opt = Optimizer::new(default_ruleset(), CostModel::new())
            .with_extractor(ext.clone())
            .without_let_union();
        let scope = Rel::Let {
            id: 0,
            value: Box::new(leaf.clone()),
            body: Box::new(a.clone()), // any Let-free body; extractor overrides it
        };
        let (result, _) = opt.optimize_scope(scope, &LocalFacts::default());

        // The loop must break after the round-1 reject: exactly 4 extract calls
        // (2 rounds x 2), never 8 (4 rounds). This is the 4^d guard at the
        // phenomenon level.
        assert_eq!(
            ext.calls.get(),
            4,
            "loop must stop after the cost-equal round-1 reject"
        );
        // The emitted body is the round-0 representative A, not the round-1 flap B.
        match result {
            Rel::Let { body, .. } => assert_eq!(*body, a, "must emit the round-0 rep"),
            other => panic!("expected Let, got {other:?}"),
        }
    }
}
