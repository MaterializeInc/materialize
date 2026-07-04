# Eqsat consolidation: optimizer retirement roadmap

> **Tempered by the verdict. See `20260704_eqsat_research_verdict.md`.**
> This roadmap plans subsuming directional passes into eqsat for consolidation.
> The final verdict bounds how far that pays: eqsat is MIR-only (a hard layer
> ceiling, so HIR passes like decorrelation cannot be subsumed), the MIR logical
> layer is already well-covered, and retirement of the remaining passes rides the
> same foundational cardinality, coverage, and key/FK spend as everything else,
> not a free consolidation. So this ledger is gated on those foundations. Retained
> as the retirement analysis, read against the verdict's bounded-value conclusion.
>
> **Original status (2026-07-01):** planning artifact for subsuming the directional
> optimizer into eqsat, consolidation and maintainability goal, capability closed.

## Purpose

Map which directional MRE and MSE transforms can be subsumed into eqsat, in what order, and what it takes to delete each directional pass.
The unit of progress is a directional pass removed, not an eqsat rule added.
Subsuming a rule while its directional transform still runs is duplication, which makes maintenance worse.
So every entry is judged by what it takes to turn the pass off, then delete it.

## The consolidation ceiling: eqsat depends on some transforms

The ledger exposes a hard floor.
Class E below (Demand, ProjectionPushdown, CanonicalizeMfp, ReduceReduction) cannot be retired by subsumption, because eqsat's own raise and commit paths call them.
Full optimizer replacement is therefore two phases.

* Phase 1, subsume the subsumable: retire classes A, B, C, D. Large, real, and bounded. This roadmap.
* Phase 2, make eqsat self-contained: reimplement the class E dependencies inside eqsat. A separate, larger effort, the standalone-engine project.

Phase 1 stops at the dependency floor.
That floor is a defensible stopping point for the consolidation bet.
Do not scope "retire the optimizer" as if phase 2 were free.

## Retirement ledger

### A. Retirable now (DSL-expressible, coverage present, parity-gate only)

| transform | eqsat rule(s) covering | payoff | retire-blocker |
|---|---|---|---|
| UnionBranchCancellation | union_cancel | HIGH (#5225, #9396) | cast-variant #5014 defers to scalar; cover all run-sites |
| ThresholdElision | threshold_elision + non_negative analysis | HIGH (#4044, #8771) | non_negative parity including #4044 gaps |
| UnionNegateFusion | distribute_negate_union, flatten_union, negate_negate, negate_empty | HIGH (#9396, #5903) | run-sites |
| RelationCSE | eqsat CSE step | MED | inline_mfp cleanup variant parity |
| ProjectionExtraction | map_columns_to_projection | LOW | glue |
| fusion::{filter,map,project,negate,union,join} | merge_filters, fuse_maps, fuse_projects, flatten_union, flatten_join | MED (#4958) | #4958 kept fusions deliberately; multi-site |

### B. Physical, largely landed (the showcase arc)

| transform | eqsat coverage | payoff | retire-blocker |
|---|---|---|---|
| JoinImplementation | native commit: commit_differential + commit_delta_query (Fix A/B/C, flag enable_eqsat_native_join_commit) | HIGH | full join-shape parity (E4 red goldens); delta-orderer still index-blind (B1 landmine); JI helpers stay as a library |
| LiteralConstraints | IndexedFilter fed by the production detector via seeds | MED | duplication trap: port the detector or the pass keeps feeding eqsat |

### C. Scalar-engine-gated (CLU-137)

Blocked until the scalar engine is wired live (today offline-only, flag default off).

* LiteralLifting (the #8772 marquee, LiteralLifting versus FoldConstants peace).
* FoldConstants (scalar half).
* ReduceScalars, CaseLiteralTransform, CoalesceCase.

### D. Analysis-gated (needs a new e-graph analysis, named)

The blocked long tail. Each needs an analysis eqsat does not yet have.

* PredicatePushdown (equivalence-class establishment).
* EquivalencePropagation (equivalence plus scalar).
* NonNullRequirements (non-null).
* ReductionPushdown (reduce functional dependency).
* RedundantJoin (key/FD).
* SemijoinIdempotence.
* WillDistinct (distinct-demand).
* ColumnKnowledge.

### E. Anti-priority, never retirable by subsumption

eqsat depends on these. Its own coalesce_mfp, demand_pushdown, and split_mixed_reductions reuse them.
Subsuming them is the standalone-engine project (phase 2), not consolidation.

* Demand.
* ProjectionPushdown.
* CanonicalizeMfp.
* ReduceReduction.

## Batches

Each batch ends in at least one retired pass.
The parity gate is full-corpus rows-identical AND errors-identical (the E-err lesson: the rows-only gate is blind to error divergence).
Parity must be corpus-wide, not unit-level, because these passes run at multiple fixpoint sites and a mid-fixpoint firing can enable other transforms downstream.

1. **Union/Negate/Threshold algebra.** Retire UnionBranchCancellation, UnionNegateFusion, ThresholdElision. Cheapest first: the ops are modeled, the rules exist, and fixpointing them kills the phase-placement bug class by construction (#5225, #9396, #5903, #4044). Blocker: cover all run-sites. The #5014 cast variant defers to batch 5.
2. **RelationCSE.** Retire it. eqsat CSE already claims subsumption. Parity on inline_mfp.
3. **Fusion plus ProjectionExtraction.** Caution: #4958 deliberately kept the fusions, and the multi-run-site glue makes this all-or-nothing or it is the duplication trap. May be net-zero.
4. **JoinImplementation (landed).** Flag-gate off. Blocker: the E4 red goldens and the delta-orderer B1 landmine.
5. **LiteralLifting plus FoldConstants peace (#8772 marquee).** The canonical "fixpoint kills phase-order" win. Gated on the scalar engine being live.
6. **Blocked tail.** The analysis-gated relational passes. Do not start until the analysis substrate exists, else partial-coverage duplication.

## Anti-priority and trap list

* Demand, ProjectionPushdown, CanonicalizeMfp, ReduceReduction are eqsat dependencies (class E).
* LiteralConstraints: the detector feeds eqsat seeds, so retiring it is duplication unless the detector is ported.
* Any single fusion site retired while others still run (batch 3 is all-or-nothing).
* #8771 (Negate/Threshold as Union flags) is a representation refactor, orthogonal.
* #6740 is a LIR heuristic, not MIR.
* #4958 looks like a free dead-pass retirement, but the issue records a decision to keep the fusions, so it is not free. No genuinely dead passes were found.

## Critical path

Batch 1 is the first real retirement.
Turn off UnionBranchCancellation, with UnionNegateFusion and ThresholdElision, behind a corpus-wide rows-and-errors gate.
Every operator is modeled and every rule exists, and fixpointing them kills the phase-placement bug class that is the whole reason to consolidate.
RelationCSE (batch 2) and the landed JoinImplementation (batch 4) follow as clean retirements.
The scalar engine (CLU-137) gates the rest: the #8772 marquee (batch 5) and the entire MSE tail.
A harder gate blocks the relational long tail, since each of PredicatePushdown equivalence establishment, RedundantJoin, SemijoinIdempotence, ReductionPushdown, NonNullRequirements, and WillDistinct needs a new e-graph analysis.
And the hard boundary: the four class E dependency passes can never be retired by subsumption, only by phase 2.

## Pointers

* `20260701_eqsat_showcase_status.md`: the capability verdict this consolidation bet rests on.
* Transforms and fixpoint structure: `src/transform/src/lib.rs` and `src/transform/src/*.rs`.
* eqsat rules, DSL, IR: `src/transform/src/eqsat/dsl.rs`, `src/transform/src/eqsat/ir.rs`, the relational rewrite rules.
