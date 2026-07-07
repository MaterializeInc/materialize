# Eqsat optimizer: showcase status

> **Superseded. See `20260704_eqsat_research_verdict.md` for the complete verdict.**
> This 2026-07-01 status was correct but shallow. The arc continued past it: the
> physical join planning phases (1 and 2 landed, 3 shelved), a constructive
> phase-ordering hunt that briefly reopened then reclosed the logical capability
> axis, the maintainability layer-ceiling finding, the foundational-levers
> meta-finding, and an external literature scan. The capstone is the current,
> complete answer. This doc is retained as the mid-arc snapshot.
>
> **Original status (2026-07-01):** capability hunt complete, verdict eqsat is a
> consolidation and maintainability bet, not a capability win, engine adoption an
> open engineering-ROI decision.

## Purpose

This document records the state of the effort to demonstrate the greatest benefit of the MIR equality-saturation (eqsat) optimizer.
It captures the experiment results, the resulting verdict, what was built, and the outstanding threads.
It is a management rollup, not a design spec.
The design specs and findings it refers to live alongside it in `doc/developer/design/`.

## The brief

Devise experiments to show off the greatest benefit of eqsat over the directional transform pipeline plus `JoinImplementation` (JI).
Each experiment tests a falsifiable hypothesis with a pre-committed decision criterion.
The discriminating question throughout was intrinsic versus portable.
An intrinsic benefit is one the directional pipeline structurally cannot reach.
A portable benefit is one the directional pipeline could also reach with a bounded, data-independent change.
Only an intrinsic benefit justifies eqsat as a capability play.

## Experiment scorecard

| probe | door tested | verdict |
|---|---|---|
| E0 | compile-time viable | pass, 68x headroom under the 60s ceiling, 1.28x median vs directional |
| E1 | confluence | portable, non-confluence is structural canonicalization, not eqsat |
| E4 and Fix A/B/C and INV | physical plan quality | subsumption reached, one superiority instance, but that instance is portable |
| E2 | logical completeness | all portable, no data-dependent-order family exists |
| E-err | error-semantics advantage | no divergence across arms, no directional guard-drop to out-guard |
| E5 | scalar-opacity boundary | pending, predicted portable |

Every capability door checked so far closed portable.
E5 is the last unprobed axis, predicted portable, sizing pending.

## Final verdict

Eqsat matches and consolidates the directional pipeline.
It produces no plan the directional pipeline structurally cannot reach.
It enforces error-safety no better than the directional pipeline.
Every apparent benefit reduced to something portable to the directional pipeline.

The honest pitch is consolidation.
One fixpointed, cost-driven rule set replaces a hand-ordered pass pipeline.
That kills pass-placement bugs such as database-issues#5225 by construction.
It gives one cost-driven place to add optimizations rather than a phase-ordered sequence.
The payoff is future maintainability, not any single better plan.

## What was built

The physical-layer subsumption work landed as three fixes on branch `claude/mir-equality-optimizer-sodbej`.
Each closed one orderer-versus-commit disagreement.

* Fix A (`39fb12dae7`): index-aware differential join orderer.
  Feeds `available` arrangements into `best_left_deep_sequence` so the greedy prefers reuse.
  Closed C_some.
* Fix B (`d5c8723cdf`): see through a projection to reuse a hidden index.
  Closed C_skew.
* Fix C (`a2043e356f`, goldens at `a1e6643d05`): expression-level arranged check.
  Matches index-key expressions rather than column sets, reusing the commit's own predicate.
  Closed database-issues#2449.

The result is strict zero-worse on the showcase corpus.
There is one superiority instance, `join_fusion`, which the INV probe found portable to JI.
All 55 changed goldens are EXPLAIN re-spellings with zero result-row changes.

## Dispositions

* Fix A, B, C: ship.
  They are correct, row-verified and error-verified, reach JI parity plus one portable win, and fix real index-reuse bugs.
  They carry standalone value independent of the engine-adoption decision.
* Capability hunt: closed.
  No further probe beyond the pending E5.
  The only condition that could reopen it is a defined strict error semantics, which is a separate and larger project that must not be justified by eqsat.
* Spin-offs, landable independent of the eqsat decision:
  * The see-through-arranged orderer improvement ports to JI as a standalone plan-quality win.
  * database-issues#5404 error suppression and the spurious join-eliminated-row errors argue for a defined-error-semantics project on their own merits, not eqsat-motivated.
  * The delta-orderer index-blindness is a documented dependency for whenever the delta-commit path (B1) lands.

## Open decision: engine adoption

The technical question is answered.
What remains is a pure engineering-ROI call.

Adopt eqsat as a consolidation play, or ship the Fix A/B/C changes, port the see-through win to JI, and shelve the engine.

The case against adoption is the whole arc.
Every benefit was portable, so the directional alternative reaches the same plans at a fraction of the build and maintenance cost and with no compile-time tax.

The case for adoption is that consolidation compounds.
The payoff is not any single plan.
It is never fighting phase-ordering again, and one cost-driven surface for future optimizations.

This is a bet on future maintainability against present engineering cost.

## Outstanding threads

* E5 scalar-opacity sizing.
  Count corpus queries blocked by scalar-opacity and confirm the portable prediction on the cross-layer cases (database-issues#8732, ROW_NUMBER to TopK).
  Feeds the adoption ROI by sizing remaining scalar work.
* Merge-prep for Fix A/B/C.
  Resolve the remaining red goldens, untangle the pre-existing colname render drift, address the two orderer minors, and add an error regression test for the E-err placement shapes.
* Delta-orderer landmine.
  `delta_join_order` is still index-blind.
  B1's delta-commit path must fold the Fix A and Fix C logic into the delta orderer before shipping.

## Pointers

* `doc/developer/design/20260624_eqsat/20260629_eqsat_join_cost_findings.md`: the cost-model bottleneck findings.
* `doc/developer/design/20260624_eqsat/20260630_eqsat_acyclic_delta_and_hints.md`: the delta and hints design notes.
* `doc/developer/design/20260624_eqsat/20260630_joinimplementation_internals.md`: the JI subsume map.
* Cost model and orderer: `src/transform/src/eqsat/cost.rs` (`best_left_deep_sequence`, `binary_join_order`, `delta_join_order`).
* Commit path: `src/transform/src/eqsat/join_commit.rs`, `src/transform/src/eqsat/raise.rs`.
