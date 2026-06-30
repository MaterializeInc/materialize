# E-graph Let inlining / extraction (future phase design)

Status: design note for a FUTURE phase, not yet planned for implementation.
Captured 2026-06-21 during the polarity-aware-extractor workstream.

## Motivation

The eqsat optimizer currently handles `Let` bindings asymmetrically, and that
asymmetry is the root of two separate pain points we hit:

1. No cost-driven INLINING. A `Let` in the input is peeled into separate e-graph
   fragments with each `Get x` an opaque `LocalGet` leaf; the definition's
   structure never enters the body's e-graph, so eqsat cannot inline. It relies
   on the surrounding pipeline (`NormalizeLets`) for inlining.
2. TRAPPED analysis facts. Because `Get x` is opaque in the body fragment, facts
   proven about the definition (e.g. constant columns from a `Filter(#0=123,
   #1=234)`) do not reach the use sites. This is the documented root cause of the
   `column_knowledge.slt:480` (ck480) fast-path loss: the constant fact is
   trapped behind the multi-use `Get l0`, and no MIR post-pass can cross the Let
   boundary (`NormalizeLets` will not inline a 3-use binding).

Production (non-eqsat) does both inlining (`NormalizeLets`) and extraction / CSE
(`RelationCSE`). A future phase could bring both INTO the e-graph.

## The two directions map very differently onto e-graphs

* Extraction / CSE (Let INTRODUCTION): finding a repeated subexpression and
  hoisting it into `let x = .. in .. x .. x ..`. E-graphs do this for free: the
  e-graph is a hash-consed DAG, so structurally-identical subterms ARE one
  e-class with multiple parents. Sharing is the native representation. Today this
  is purely an extraction-time concern, already implemented: extract the shared
  DAG, then `cse::eliminate_common_subexpressions` names multiply-referenced
  nodes as `Let`/`Get`. The e-graph produces sharing; CSE names it.
* Inlining (Let ELIMINATION): `let x = v in body` -> substitute `v` at each
  `Get x` when beneficial. NOT done by the current eqsat.

## Design options for inlining

### (a) Heuristic, pre-saturation
At lower time, substitute the definition for bindings obviously worth inlining
(single-use, or trivially small); keep multi-use / expensive ones as `LocalGet`.
Simple, but the inline-or-not decision happens before the cost model runs, so it
is not cost-optimal. Roughly mirrors `NormalizeLets`.

### (b) Native, cost-driven (the elegant target)
Add the equivalence `Get x` is-equal-to `definition` into the e-graph (union the
`LocalGet` class with the definition's class). The e-graph then holds BOTH the
reference form and the expanded form in one class, and inline-vs-share collapses
into a SINGLE extraction-time decision: an e-class referenced many times is
materialized as a `Let` if that is cheaper, or expanded inline if duplication is
cheaper. Inlining and CSE become the same cost-driven extraction choice.

Two payoffs from (b):
* It UN-TRAPS analysis facts. With `Get l0` equal to `Filter(123,234)(t4)` in one
  e-class, the constant-column fact computed on the `Filter` form lives on the
  SAME class as `Get l0`, with no cross-fragment seeding. This directly unblocks
  the ck480 constant-column collapse (see project memory for ck480 detail).
* It unifies inline + CSE under the cost model, which is the e-graph promise of
  dissolving staged-pass boundaries.

## Why (b) is not free (the hard parts)

1. E-graph blow-up. Unioning the definition into every use saturates and reasons
   about the definition's full structure at every use site, multiplying e-nodes.
   The opaque-`LocalGet` design exists precisely to avoid this. A real
   implementation needs growth control (node budget, or only unioning small /
   few-use definitions).
2. Cycles, for `LetRec`. For a NON-recursive `Let`, `v` does not mention `x`, so
   `Get x = v` is an ordinary acyclic union and extraction + CSE handle it. For a
   RECURSIVE binding, `v` references `x`, so the union makes the e-class CYCLIC
   (x's class contains a term pointing back at x's class). E-graphs can hold
   cycles, but EXTRACTION from a cyclic e-graph is the hard part: a naive
   extractor expands `x -> v -> x -> ..` forever, so it must be forced to pick the
   `Get x` reference at least once to break each cycle, which IS the
   materialize-as-`Let` decision. Cost-optimal extraction with this sharing is
   NP-hard in general (the egg/egglog community reaches for ILP / specialized
   cyclic extractors). The current eqsat sidesteps it by keeping `LetRec` opaque.

## Recommended scope for a future phase

Start with NON-recursive bindings only:
* Union `Get x` with its definition for non-recursive `Let`s (option b), gated by
  a size / use-count budget to bound e-graph growth.
* Make the extractor + `cse` sharing-aware so the Let-vs-inline choice is driven
  by the cost model (count a shared e-class once if materialized as a `Let`,
  N times if inlined).
* Keep `LetRec` on the opaque / heuristic path until there is a cyclic-extraction
  strategy worth the complexity.

This single mechanism would retire BOTH "facts trapped behind `Get`" (the ck480
class of problem, including the constant-column collapse the dedicated
`ConstantColumns` analysis was built for) AND "no cost-driven inlining", instead
of solving them with separate point fixes (cross-fragment fact seeding, bespoke
inlining heuristics).

## Relationship to work already landed

* The `ConstantColumns` e-class analysis (commit dc7e2ead06) is a building block;
  with (b) its facts would reach use sites natively (no `RecAnalysis<ConstCols>`
  seeding across `letrec_local_facts` needed).
* The polarity-aware extractor (commit df91893e64) and the sharing-aware extractor
  this phase needs are both EXTRACTION-time concerns; they would compose (the
  extractor would carry both a polarity demand and a share/inline cost decision).
* The `phase` annotation (commit 0aab159f81) is orthogonal but useful: a future
  inlining/union rule could itself be phase-scoped if it proves
  arrangement-sensitive.

## Spike validation (2026-06-21) and revised plan

A throwaway spike built the ck480 shape (`Let l0 = Filter(#0=123,#1=234)(Get u1)
in Union[Get l0, Get l0, Get l0]`) and prototyped approach (b): add the
definition into the body e-graph and `eg.union(getl0_id, def_id)`. Findings:

* **Prerequisite, now DONE:** the spike found `ConstantColumns` was NON-FUNCTIONAL
  as shipped (its `merge` used intersection, so the `run_analysis` fold from the
  empty `bottom` annihilated every fact, and an opaque `LocalGet` poisoned any
  unioned class). Fixed in commit c01f2bea78: `merge` is now a join
  (`union_const_cols`) with `bottom` as identity; intersection stays only for the
  `Union` operator arm. This was a real correctness bug and is the load-bearing
  prerequisite for any consumer of `an.cc`.
* **Un-trapping works (OBSERVED):** after the union, the `Get l0` class carries
  `{0:123, 1:234}` (with the fixed merge). The fact is no longer trapped.
* **No blow-up (OBSERVED):** the ck480 shape went 2 -> 4 e-nodes (definition added
  once, hash-consed across the 3 uses), saturation stable at 4 (iters=1). Growth
  is additive in definition size, not multiplicative in use count. `MAX_ENODES`
  already bounds the pathological case; a size/use budget is sufficient
  mitigation, not a blocker.
* **Extraction terminates + SHARES (OBSERVED):** non-recursive union is acyclic;
  the existing `extract_with` coped, and for the 3-use binding it extracted 3
  `LocalGet` references (shared, not inlined) which `cse` already names as a Let.
  CAVEAT: this is because the current cost model treats `LocalGet` as a free leaf,
  so it ALWAYS shares; for a single-use / tiny binding it would fail to inline
  when inlining is cheaper. So the inlining half of the payoff needs a
  sharing-aware extractor; the un-trapping half does not.

### Revised implementation plan (size M; M-L with sharing-aware extraction)

1. **DONE** - fix `ConstantColumns` merge polarity (c01f2bea78).
2. Union plumbing (S): in `optimize_scope` (engine.rs ~210), for a NON-recursive
   `Let x = v in body`, add the lowered `v` into the body's e-graph and
   `union(LocalGet x class, v root)` before saturating, instead of optimizing them
   in separate fragments. Keep `LetRec` on the opaque path (cycles break
   extraction). Gate by a definition size/use budget.
3. A CONSUMER to demonstrate value (the un-trapping is invisible without one): add
   a `Cond` reading `an.cc` for a class and a const-col rewrite rule (e.g. a
   redundant-`Filter`/`Map` simplification keyed on a now-reachable constant), with
   a test showing a plan improvement that was impossible before the union.
4. (Deferred, M) sharing-aware extraction for cost-driven inline-vs-share - needed
   for the inlining payoff, not for un-trapping. Composes with the polarity-aware
   extractor (a child carries both a polarity demand and a share/inline cost).

ck480's FULL collapse remains a further stack on top of this: a const-col
reconstruction rule that works through the `Negate` (sound now via the
polarity-aware extractor) plus an n-ary / order-insensitive `union_cancel` (a
matcher extension). Those are out of scope for the Let-union itself.
