# eqsat pipeline subsumption: design and integration plan

Status: Draft (research design). Unbiased assessment, not an advocacy document.
The verdict from the prior research arc stands: eqsat is a consolidation and
maintainability play, not a plan-quality win. This design describes what it
would take to make eqsat the optimizer core, what is expressible, what is not,
and it states the case against as plainly as the case for.

## Summary

eqsat runs today as two appended passes: a logical pass at the end of
`logical_optimizer` and a physical pass in the middle of `physical_optimizer`,
each placed after the directional fixpoints it would eventually replace. The
goal of subsumption is to collapse those two touchpoints into one engine that is
the logical fixpoint and the physical planner, so the hand-ordered directional
pass pipeline can be retired.

The central finding of this design: with the colored e-graph extractor already
built and integrated, the extraction-model barrier that previously looked
structural (per-context output for column demand, per-consumer sharing, per-edge
spelling) is not a wall. Those become expressible. What remains genuinely
outside eqsat is a small, well-defined substrate: the fact-computing analyses,
side-effecting notice emission, and HIR-layer rewrites. Everything else is
reachable through three kinds of work, in order of difficulty: rule coverage,
new e-class analyses, and productionizing the colored extractor with a real
cost. The hard walls are not expressibility. They are the measured in-loop
placement regression, the reopening of the compile-time gate when the node cap
is removed, and the cost-model foundation.

## Motivation

### The case for

The directional pipeline encodes optimization as a hand-ordered sequence of
passes iterated inside `Fixpoint` loops. That ordering is fragile in documented
ways. The pipeline comments in `src/transform/src/lib.rs` call out two
pathologies directly. `FoldConstants` and `LiteralLifting` are non-confluent and
oscillate between `Constant(4)` and `Map(4)/Constant`. `JoinImplementation`
cannot share a fixpoint with `EquivalencePropagation`, `Demand`, `FoldConstants`,
or `LiteralLifting`, because either equivalence propagation invalidates a
committed join plan (database-issues#5260) or join implementation would have to
run an unbounded number of times (database-issues#4639). These are the exact
failure modes an e-graph removes by construction. It holds all equivalent forms
at once and commits once, at extraction, under a cost model. Bugs of the class
"the right rewrite existed but ran at the wrong place" (for example
database-issues#5225) cannot occur in a single saturated rule set.

The consolidation value is that one cost-driven engine replaces N hand-tuned
passes plus the human effort of keeping their order correct.

### The case against (stated up front, not buried)

Every capability probe in the prior arc came back portable. eqsat produced no
plan the directional pipeline could not reach. The measured attempts to move
eqsat in-loop made plans worse, not better (see Walls). Retiring even three
logical passes (union and threshold algebra) failed on plan parity, because
eqsat-after-the-fixpoint cannot reproduce the fixpoint's interleaving, and the
divergences were regressions, not cosmetic. The honest expected value of full
subsumption is therefore a maintainability gain with plans held at parity, paid
for with a large, multi-quarter program and a standing risk that per-rule cost
alignment becomes a maintenance burden that exceeds the saving of not calling
the passes. This design should be read as "here is the complete path if we
decide the consolidation is worth it," not "here is why we should."

## Current state

### The two eqsat touchpoints

`logical_optimizer` (src/transform/src/lib.rs:784): after `fixpoint_logical_01`
(PredicatePushdown, EquivalencePropagation, Demand, FuseAndCollapse) and
`fixpoint_logical_02` (SemijoinIdempotence, ReductionPushdown, ReduceElision,
ReduceReduction, LiteralLifting, RelationCSE, FuseAndCollapse), `EqSatTransform`
runs last and must leave joins `Unimplemented`.

`physical_optimizer` (src/transform/src/lib.rs:843): after `fixpoint_physical_01`
(EquivalencePropagation, FoldConstants, CoalesceCase, Demand, ProjectionPushdown,
LiteralLifting), `PhysicalEqSatTransform` runs, then LiteralConstraints,
`fixpoint_join_impl` (JoinImplementation), CanonicalizeMfp, RelationCSE,
ProjectionPushdown (skip_joins), CanonicalizeMfp, CaseLiteralTransform, and a
final constant-folding fixpoint.

### The substrate eqsat already has

The e-class analysis framework exists (`analysis/equivalences.rs`,
`analysis/monotonic.rs`), and the colored e-graph is built and default-on
(`COLORED_SATURATION = true`, engine.rs:40). The colored extractor
(`colored/extract.rs`) is a color-aware least-cost extractor: per colored class
it considers base nodes plus colored delta siblings, so a color-specific equality
can expose a different representative. It currently runs on a toy additive cost
(SP3b); the real relational and scalar cost is SP4 and is not yet on the
production extraction path.

## Expressibility classification

A transform is expressible in eqsat if it is a local pattern rewrite whose
guard is a fact available on the e-class (structurally or via an analysis), and
whose output is representable, including per-context output via colors. A
transform is not expressible if it computes a fact congruence cannot derive, if
it is a side effect, or if it belongs to a different IR layer.

### Tier 1: expressible as rules today

Local, monotone, MIR to MIR simplifications. Saturation is their fixpoint, and
eqsat already carries most.

- Fusion family: ProjectionExtraction, ProjectionLifting, Fusion,
  FlatMapElimination, Join-fusion, Reduce-fusion.
- Union algebra: UnionNegateFusion, UnionBranchCancellation, flatten. Blocked
  only on variadic matcher machinery, not architecture.
- Scalar: FoldConstants, ReduceScalars, CoalesceCase, CaseLiteralTransform. The
  eqsat scalar canonicalizer already does constant folding and CASE.
- Canonicalizations: LiteralLifting, NormalizeOps, CanonicalizeMfp,
  NormalizeLets. The raise already produces the canonical Map-Filter-Project and
  Let structure these enforce.
- PredicatePushdown (the pushdown rewrites).

The non-confluent pairs the pipeline comments flag are eqsat's strongest fit:
hold both forms, cost-pick once, no oscillation, no ordering fragility.

### Tier 2: expressible as a rule plus a new e-class analysis

The rewrite is a local pattern. The guard is a fact eqsat does not yet compute.
The analysis framework can host each as a fact attached to e-classes.

- ThresholdElision needs a containment or subset analysis (`is_superset_of`).
  All fourteen corpus cases are general containment, zero syntactic.
- RedundantJoin, SemijoinIdempotence, ReduceElision, ReduceReduction need key,
  functional-dependency, and uniqueness analyses.
- EquivalencePropagation, LiteralConstraints, NonNullRequirements need
  equivalences and non-null facts. eqsat already has the equivalences analysis,
  so these are the closest.
- JoinImplementation plan quality needs a cardinality analysis. eqsat already
  commits joins by delegating to `plan_join_min_arrangements`. To own the
  quality decision it needs cardinality, which is the unbuilt MIR cost model
  (`20230512_mir_cost_model.md`).

### Tier 3 (corrected): expressible via the colored extractor plus an analysis

This tier was previously classified as structurally not expressible. That was
wrong. The colored e-graph gives per-context representative selection, so
per-context output has a home.

- Demand and ProjectionPushdown. Column demand is "which columns does this
  context observe". Install a color asserting `r ≅_c Project[S](r)` valid under a
  context that observes only column-set `S`. The color-aware extractor then
  selects the pruned form for that consumer while other consumers keep theirs.
  The pruning is expressible. What remains is computing `S` (a backward demand
  analysis, a fact-provider) and wiring per-consumer colors.
- Per-context RelationCSE and per-edge join spelling. The same colored mechanism
  carries per-context sharing and spelling. E5 already concluded per-consumer
  spelling is representable this way.

The two caveats that keep this honest. First, the colored extractor is built and
integrated at the saturation layer but is not load-bearing for production
output. It runs a toy cost (SP3b), and production extraction still routes through
the ILP and scalar extractors. Routing production output through color-aware
extraction with a real cost (SP4) is unfinished. Second, the demand fact is a
genuine backward analysis over use-sites. Colored extraction gives per-context
selection but not the fact itself. The fact stays a separate provider.

### The irreducible remainder: what stays outside eqsat

Independent of any implementation effort:

- The fact-computing analyses: Typecheck, non-null, monotonic, equivalences,
  and the new containment, keys, cardinality, and demand analyses. They produce
  facts congruence cannot derive by rewriting. They are eqsat's substrate and
  feed the colors and rule guards. Not rewrites, never subsumed. This is
  expected and correct, not a shortfall.
- Side-effecting notice emission (CollectNotices). It becomes an extraction-time
  emission hung off the engine, not a rewrite.
- HIR-layer rewrites (for example database-issues#2613 and #2969 antijoin and
  semijoin collapse, whose clean form is pre-MIR). This is a layer boundary, not
  an expressibility limit. A MIR e-graph cannot express a rewrite that must
  happen before decorrelation.

So the reachable end-state is "eqsat is the optimizer core, all logical rewrites
plus join and physical planning by cost, fed by a fixed set of analyses", not
"eqsat is literally the whole file". The passes that survive are exactly the
analyses and the layer boundary.

## The missing bits to build

1. Analyses as e-class fact providers: containment or subset, keys and
   functional dependencies, cardinality, and demand. Cardinality and keys are
   the foundational levers the prior arc repeatedly identified as the real
   bottleneck.
2. Colored extractor productionization: give it the real relational and scalar
   cost (SP4) and route production extraction through it, replacing or
   subsuming the current ILP and scalar extractor paths. This is the enabling
   step for Tier 3.
3. Rule coverage: port the remaining Tier 1 rewrites and the Tier 2 rewrites
   whose analyses now exist. This is large but mechanical and low-risk per rule.
4. Architecture: remove the node cap and make eqsat the in-loop fixpoint. This
   is where the real walls are.

## The measured walls (unbiased)

These are results, not risks. They already happened.

- In-loop placement is worse. The foundational spike moved eqsat into the loop
  and measured 39 plan divergences versus 29 for the after-phase placement.
  Re-saturation perturbs the interleaved monotonic, non-null, and equivalence
  analyses that the fixpoint passes depend on. Moving eqsat in-loop fixed zero
  divergences and added ten. The placement hypothesis was refuted. This is the
  single most important wall, and it is directly corroborated by the pipeline
  comment that keeps JoinImplementation out of EquivalencePropagation's fixpoint
  for the same reason.
- Removing the node cap reopens E0. The 200-node cap made the compile-time gate
  optimistic, because expensive plans were skipped out of eqsat and never
  contributed to the measurement. Uncapped saturation was sub-second on user and
  BI plans in the spike, but the catalog-bootstrap tail (tens of seconds on
  builtin indexes) is unverified. Cap removal is remove-and-measure, not
  remove-and-ship.
- The cost model is the bottleneck for both quality and retirement. Plan-neutral
  retirement needs per-rule cost-model alignment to mimic the directional
  canonical forms. That alignment is a maintenance burden that may exceed the
  saving of not calling the passes. This tension is real and unresolved.
- Batch-1 retirement failed on parity. Retiring the three union and threshold
  passes kept rows and errors identical but regressed 29 of 1187 goldens
  (under-reduction, genuinely worse plans). The root cause was placement plus
  coverage, not a single fixable bug.

## Integration plan

Phased, each phase gated on a measurement that can stop the program. The gates
are hard. A red gate ends the phase, it does not get waived.

### Phase 0: cost model foundation (prerequisite, no pipeline change)

Build the cardinality analysis from `20230512_mir_cost_model.md` as an e-class
analysis, and a keys and functional-dependency analysis. Wire them into the
colored extractor's cost (SP4). Gate: on the showcase and LDBC corpus, the
colored extractor with the real cost reproduces the current production plans at
parity (no regressions), measured by the three-lens audit (arity, arrangement
count, join-type transitions). Red gate: if the real cost cannot hold parity,
subsumption is not viable and the program stops here. This phase is also
independently useful (it is the MIR cost model) even if subsumption never
proceeds.

### Phase 1: colored extractor on the production path

Route production relational and scalar extraction through the color-aware
extractor. No new rewrites, no placement change. Gate: byte-identical or
parity plans versus the current extractors on the full corpus. This de-risks the
enabling mechanism for Tier 3 before any capability is added.

### Phase 2: Tier 2 analyses and rules, still after-phase

Add containment, keys, and demand analyses and the rules they guard
(ThresholdElision, RedundantJoin, SemijoinIdempotence, ReduceElision). eqsat
still runs after the directional fixpoints. Gate: for each retired directional
pass, the eqsat rule reproduces its output at parity on the corpus. Retire
passes one at a time, never in a batch, and only when its parity gate is green.
This is the per-rule grind the prior arc identified as the actual unlock, now
with the analyses that were missing.

### Phase 3: Tier 3 demand and per-context output via colors

Install per-consumer demand colors and route Demand and ProjectionPushdown
through colored extraction. Gate: parity with the directional Demand and
ProjectionPushdown output, including the dummy-introduction and
projection-pushdown-after-RelationCSE interactions the pipeline comments flag.

### Phase 4: uncap and in-loop placement (the walls)

Remove the node cap and re-run E0 uncapped as a hard gate, including the
catalog-bootstrap tail. Only if E0 passes, attempt to move eqsat in-loop and
re-measure plan neutrality. Gate: in-loop placement must not regress plans
versus after-phase. Given the spike already refuted this once, the burden of
proof is high. If the analysis-perturbation problem is not solved (for example
by making the analyses saturation-stable, or by not interleaving them), this
phase fails and eqsat stays after-phase. That is an acceptable end-state: eqsat
can subsume most rewrites while remaining a post-fixpoint pass, it just cannot
then claim to have replaced the fixpoint itself.

### Phase 5: physical join planning ownership

Replace the delegation to `plan_join_min_arrangements` with eqsat's own
cardinality-aware physical join cost (the shelved phase-3 e-node plus ILP joint
selection). Gate: no worse than JoinImplementation on the corpus, measured
through the three-lens audit and paired execution, not rows-only.

## Risks and open questions

- The analysis-perturbation problem (Phase 4) may have no clean solution.
  E-graph saturation and iterative directional analyses have different fixpoint
  semantics, and reconciling them may be fundamental, not incidental. If so, the
  fixpoint itself is never subsumed and the end-state is "eqsat is the rewrite
  and physical engine, fed by a directional analysis loop".
- Cost-model canonical form drift. Each retired pass requires the eqsat cost to
  prefer the same canonical form the directional pass produced. Keeping these
  aligned across future changes is ongoing work, and it is not obviously cheaper
  than maintaining the passes.
- The consolidation may not be worth it. The prior verdict is that every
  capability is portable and the value is maintainability. A multi-quarter
  program for a maintainability gain, with plans held at parity and a standing
  cost-alignment burden, is a real ROI question the design does not pretend to
  resolve. The phase gates exist precisely so the program can stop cheaply if
  the value does not materialize.

## Decision gates summary

The program should proceed only while each holds. Any red gate stops it, and
several early gates deliver standalone value (the cost model, the colored
extractor on the production path) even if subsumption never completes.

1. Phase 0: real cost holds plan parity. Standalone value: the MIR cost model.
2. Phase 1: colored extractor holds parity on the production path.
3. Phase 2: each retired pass holds parity, one at a time.
4. Phase 3: demand-via-colors holds parity.
5. Phase 4: E0 passes uncapped, and in-loop placement does not regress plans.
6. Phase 5: eqsat physical join planning is no worse than JoinImplementation.

## What this design does not claim

It does not claim subsumption produces better plans. It does not claim in-loop
placement is achievable (the spike says otherwise, and Phase 4 may fail by
design). It does not claim the consolidation is worth the cost. It claims only
that, with the colored extractor already built, the set of transforms eqsat
cannot express shrinks to the analyses, notice emission, and HIR-layer rewrites,
and that the remaining path is engineering plus the named, measured walls, not a
representational impossibility.
