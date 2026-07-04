# Eqsat research verdict: capability, maintainability, and where the leverage actually is

> **Status:** capstone verdict. Synthesizes the E0-E5 showcase arc, the physical
> join planning phases, the capability and maintainability probes, and an
> external literature scan. This is the durable answer to the research question,
> and it supersedes the mid-arc status in `20260701_eqsat_showcase_status.md`.
> The per-topic design docs in this directory remain the detailed record. This
> doc is the conclusion they add up to.

## The question

Should Materialize invest in an equality-saturation optimizer, on two axes.

* Capability: does eqsat produce better plans than the directional optimizer.
* Maintainability: is eqsat a better authoring and consolidation surface, at no
  worse plan quality.

## Capability verdict: no realized unique-eqsat win; the gaps are unbuilt levers

The verdict is about what is realized and what is unique to eqsat, not about what
is reachable in principle. No axis produced a plan eqsat realizes today that the
directional optimizer could not reach or be taught. Where eqsat falls short of an
achievable better plan, the gate is always unbuilt machinery (a missing rewrite,
a cost the model does not track, or absent constraint facts), not an architectural
impossibility. So "we have not built it" is the accurate framing, and most of
those unbuilt levers are portable to the directional optimizer, which is why eqsat
is not a unique-capability bet.

* Logical rewrite (E0-E5): every candidate win was portable. eqsat found no
  logical rewrite the directional fixpoint misses, because the logical rewrites
  are monotone and fixpointed.
* Arrangement sharing: rendering already builds each `(collection, key)`
  arrangement once and shares it, and CSE captures the sharing. Arrangement keys
  are predicate-determined by the equi-join equivalence, so join order cannot
  manufacture a sharing opportunity CSE misses. No order-times-sharing win exists.
* Cost-based and physical join selection: the greedy commit reaches exact
  JoinImplementation parity on join strategy (measured, 164-join arm). The ILP
  extractor models only an arrangement-count objective, with no join-order or
  strategy term, so it cannot select a better physical plan. Logical
  phase-ordering: the directional optimizer does have real local minima
  (documented non-confluence, removable semijoins and antijoins), but eqsat
  escapes none of them, because its rule set lacks the same rewrites the optimum
  needs. Saturation over a rule set that cannot produce the better form is stuck
  exactly where the directional passes are.

The mechanism, stated once: where the directional optimizer is suboptimal, the
fix is a new rewrite rule (coverage), a cardinality-aware or sharing-aware cost
(cost model), or absent constraint facts (keys, functional dependencies, foreign
keys). Each is buildable, and each is portable to the directional optimizer, so
none is a capability unique to equality saturation. The one place eqsat's
mechanism is genuinely cleaner is holding non-confluent alternatives and
cost-picking (#2409 filter-sharing below), and even there the missing piece is a
sharing-aware cost the directional optimizer would also need. The gap is unbuilt
machinery, not an eqsat-only capability and not an impossibility.

A final probe tested the one case that looked like eqsat's theoretical
wheelhouse: database-issues#3324, a syntactic-match failure where RedundantJoin
cannot see that `Project(#0); Distinct(#0)` equals `Distinct(#0)`. Congruence
matching by e-class equality is spelling-robust in principle, so this is where
eqsat should shine. It does not, twice over. The issue does not reproduce on
current main, because modern IN-decorrelation already emits the optimal single
semijoin. And even if it did, eqsat has no redundant-join rewrite (RedundantJoin
is a directional pass, empty under eqsat) and no Project-into-Reduce absorption
rewrite, so it could not apply congruence here. The spelling-robustness of
congruence matching is a theoretical property eqsat does not implement, and
realizing it would be more coverage and build work, not a free structural
advantage. The last intrinsic-robustness thread closes negative.

## The meta-finding: the levers are foundational, not architectural

Every lever found where the optimizer is suboptimal is one of three foundational
things, none of which is how the optimizer searches the plan space.

* Cardinality (the cost model): delta-versus-differential, join ordering,
  late-versus-early materialization, the decorrelation simple-versus-keyed
  choice.
* Coverage (missing rewrites): semijoin and antijoin collapse, and the HIR layer
  they naturally live at.
* Constraint infrastructure (keys, functional dependencies, foreign keys):
  late materialization soundness, redundant-join removal, delta-join viability,
  and the cardinality cost model itself, which needs key information.

This is why capability closed. Equality saturation is a search-architecture bet.
Materialize's optimizer levers are knowledge and infrastructure bets. Global
saturation buys nothing when the gate is that the optimizer does not know the
cardinalities or the keys, or lacks the rewrite, because that gate is identical
regardless of how the plan space is searched.

## Maintainability verdict: modest and bounded

* Rule authoring in the declarative DSL is feasible but carries friction. The
  SP2b scalar-rule port needed builtin escape hatches for rules that did not fit
  declaratively, a permanent-sorry taxonomy, and it exposed a nondeterminism
  pitfall (a builtin picking one class node by hash order).
* There is a hard layer ceiling, and it is two-sided. On the input side, eqsat
  operates on MIR only, so HIR rewrites, decorrelation and subquery
  simplification, are out of scope, and that is exactly where several real
  coverage gaps live. On the output side, eqsat's internal representation is
  richer than MIR can carry, so extraction to MIR strands the richness. The
  e-graph decomposes scalars finely and hash-conses identical subexpressions,
  but MIR has no scalar `Let`, and extraction re-expands the shared scalar
  inline. Note (correcting an earlier overstatement): MIR *can* represent a
  shared scalar, by materializing it as a column via a `Map` (splitting the MFP
  so an upstream `Map` computes it and downstream operators reference the
  column). So the barrier is not representability, it is cost, and the cost is
  not what an earlier draft claimed. `Map` is a streaming operator, it adds no
  arrangement, so computing a scalar once and carrying the column is free through
  streaming operators and costs incremental memory only where the column crosses
  an arrangement boundary (a `Join` or `Reduce`). Recompute pays the scalar's CPU
  at every consuming operator on the same rows, forever, in incremental dataflow.
  So compute-once is always weakly better on execution and strictly better for
  any non-trivial scalar. The only case recompute wins is a cheap scalar carried
  across a large arrangement, where the incremental column width outweighs the
  tiny CPU saving. Realizing eqsat's internal scalar sharing would need a
  factoring extractor to emit the hoisted-column form and a sharing-aware cost
  model to weigh that narrow tradeoff, defaulting to share. That cost model is
  the missing lever, and it does not exist in either surface today. The scalar
  output ceiling is a cost-model gap to build, not a representability wall and
  not a decided cost-negative. For database-issues#2409 (common
  sub-sub-expression elimination) this splits by kind of subexpression. SCALAR
  sub-sharing is output-stranded and cost-negative, as above. But RELATIONAL
  sub-expression sharing is a different, more promising, and still-OPEN case:
  sharing a filtered collection, `Filter[a](r)` between `Filter[a](r)` and
  `Filter[a and b](r) = Filter[b](Filter[a](r))`, is output-representable
  (`Rel::Let`), a real win (collections fan out for free, so sharing avoids
  recomputing the a-filter), and it is a fuse-versus-split non-confluence. The
  directional optimizer canonicalizes filters toward fused (`Filter[a and b]`),
  so whole-stage RelationCSE sees two different stages and misses the share,
  which is why #2409 is a hard open epic. An e-graph could hold both the fused
  and split forms and cost-pick the split when `Filter[a]` is shared, which the
  fixed fuse-canonicalization cannot. That is the classic
  hold-alternatives-and-cost-pick advantage, relational and output-representable,
  and it is the strongest genuine candidate the arc has surfaced. The probe ran
  (read-only, 2026-07-04) and the result is unbuilt, not unreachable, and not
  closed. Today eqsat and directional emit the identical no-share plan, gated on
  two build gaps. First, the filter-split rewrite does not exist and cannot be
  expressed in the current DSL, which deliberately treats scalar payload lists as
  opaque and never destructures them, so the split form never enters the e-graph.
  Adding it needs a destructuring builtin, buildable via the existing escape-hatch
  mechanism. Second, and independently, the greedy extractor costs each e-class in
  isolation with no credit for a subtree shared across consumers, and the ILP
  objective counts only arrangements, so no cost path values a shared collection.
  That second gap is the sharing-aware cost lever, the same foundational lever the
  scalar case needs, unbuilt in both surfaces. eqsat holds the fused and split
  alternatives natively, so once that cost exists eqsat's route (cost-pick among
  held alternatives) is cleaner than a bespoke directional don't-fuse-when-shared
  pass fighting its own canonicalization. The lever is worth building. The one
  honest caveat is value, not reachability: in the canonical shape source-filter
  pushdown and collection fan-out already share the scan and the common filter, so
  the residual win is a per-row re-check of the shared predicate, marginal unless
  the shared filter is expensive and not pushable. So this is a buildable
  capability with a natural eqsat mechanism and bounded common-case value, not a
  closed case.
* The MIR logical layer is already well-covered, so there is little new to add
  in eqsat's own layer.
* A recurring shape across the probes: eqsat has real theoretical properties,
  congruence (database-issues#3324) and native fine-grained scalar CSE, that are
  not realized, because MIR on input or output cannot carry them. The advantage
  is genuine inside the e-graph and stranded at the MIR boundary. The relational
  filter-sharing case above is the exception, MIR can carry it (`Rel::Let`), and
  it is reopened rather than stranded. Realizing any of these is build work, not
  a free structural win, and the scalar and congruence cases have bounded
  plan-cost payoff regardless because rendering already dedups the
  memory-relevant arrangements.

Net: eqsat consolidates a well-covered MIR layer feasibly but with friction, and
cannot reach the HIR layer where the real gaps are. The consolidation value is
real but bounded.

## What landed, and why it is the correct end-state

Two phases shipped on the research branch (not merged).

* Phase 1: extract the join commit out of the raise arm into a reusable
  `commit_join`, byte-identical.
* Phase 2: count the delta-versus-differential decision net of the arrangements
  a differential plan builds anyway. This reaches exact JoinImplementation parity
  and fixes the `outer_join.slt` differential-where-delta-is-better regression.

The reframe that came out of the physical-planning investigation is the durable
architectural point. Physical join planning is context-dependent: it needs the
extracted children and their arrangement availability. That is precisely why it
cannot be a pre-extraction cost-selected e-node, and why the raise boundary,
given the already-extracted children, is its correct home. The two-layer split,
logical choices cost-selected in the e-graph and physical join planning committed
at raise, is right, not an inconsistency to remove. Phases 1 and 2 are the
end-state for this work, not a stepping stone to a bigger e-graph.

## What was shelved, and why

Phase 3 would have made the physical plan a cost-selected property of the
e-graph: physical candidates as e-class members, the ILP extractor for joint
selection. It was shelved before building, for two independent reasons.

* The seeding wall. The ILP solve runs on the saturated graph before CSE and
  before raise, as a one-shot program over pre-existing nodes. It has no moment
  to construct a context-dependent physical candidate, so the physical
  candidates cannot be seeded for it.
* The structural no-win. Arrangement keys are predicate-determined, so there is
  no order-times-sharing plan a joint search would find that the greedy commit
  does not. The payoff is structurally zero.

Building it would have been the largest effort in the arc for a
structurally-negative result.

## External corroboration

A literature scan (web-verified) matches these conclusions.

* No published system runs equality saturation across genuinely distinct query
  IRs (a correlated high-level IR down to a physical IR) in one e-graph. The
  maximal "dissolve MIR into the e-graph" architecture is not how mature systems
  are built.
* Aurora (2024), an eqsat relational query-rewrite prototype, independently made
  the same three boundary choices this arc reached: ILP extraction over a
  cardinality cost, logical plans plus physical operators in one e-graph, and
  subquery decorrelation deferred out of the e-graph.
* Cranelift's acyclic e-graph is the production shape: single IR in and out,
  many mid-end passes consolidated, cost-based extraction. That is the
  "eqsat as single-IR consolidation" pattern, shipped.
* No formal unification of the Cascades cost-based memo with equality saturation
  exists. The relationship is only noted informally.
* Cross-abstraction-level eqsat exists only in compilers (over MLIR dialects),
  where the levels are uniform, not in databases where the levels are
  heterogeneous.

## Forward directions

All are foundational and directional. None requires equality saturation, though
one keeps eqsat's mechanism in frame contingently.

* Constraint infrastructure (keys, functional dependencies, foreign keys). The
  highest-leverage cross-cutting investment. It unlocks late materialization,
  redundant-join removal, delta-join viability, and feeds the cardinality cost
  model. Foreign keys specifically are the prerequisite that makes late
  materialization sound.
* Cardinality cost model. The recurring lever across delta-versus-differential,
  join order, and the decorrelation simple-versus-keyed choice. Cardinality
  estimation is currently reverted and off.
* Sharing-aware cost model. The lever that gates both #2409 cases (sharing a
  scalar and sharing a filtered collection). The cost model charges each e-class
  in isolation and counts arrangements, so it cannot value a subtree computed once
  and reused across consumers. Computing a shared scalar once is always weakly
  better on execution (`Map` is streaming, the only cost is carrying the column
  across an arrangement boundary), so the cost model should credit compute-once by
  default and charge carry only at arrangement crossings. This is a distinct lever
  from cardinality, it is what makes #2409 the strongest eqsat-mechanism candidate
  (hold both fused and split, cost-pick), and it is worth building.
* Cost-aware decorrelation. The one thread where eqsat's mechanism retains a
  contingent role. The `branch()` simple-versus-keyed choice (D1) is a syntactic
  heuristic today with an explicit code TODO saying it should be cost-based. Its
  cost need is narrow, the distinct-count of the correlation-key columns, a
  property of the outer relation estimable before decorrelation, so the pragmatic
  fix is to make that signal available early, which is directional. Only if that
  early-signal path is infeasible does holding decorrelation-shape alternatives
  and cost-picking late become the answer, and that is the one place a memo or
  e-graph would contribute something directional deferral cannot. The
  equijoin-versus-cross-join choice (D2) is coverage, portable, and where the
  reported cross-join regressions actually are.
* HIR subquery simplification. A directional good-citizenship fix for the
  removable-semijoin and collapsible-antijoin reports, at the HIR layer where the
  existence check is one clean node, with no new MIR nodes.

## Bottom line

Equality saturation for the Materialize optimizer is a consolidation bet, not a
unique-capability bet, and the consolidation value is bounded by a MIR-only layer
ceiling. That is not the same as "eqsat can do nothing more": every capability gap
found is unbuilt machinery, and most of it is portable to the directional
optimizer, so building it does not require eqsat. The one place eqsat's mechanism
is genuinely cleaner is holding non-confluent alternatives and cost-picking
(#2409), gated on a sharing-aware cost worth building. The concrete banked value
from the arc is the physical delta-parity fix (phases 1 and 2). The leverage for
the optimizer going forward is foundational, the cardinality and sharing-aware
cost model, keys and foreign keys, and rewrite coverage, and it is
architecture-independent.
