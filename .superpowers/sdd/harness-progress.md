# Progress: eqsat marginal-value measurement harness
Plan: .claude/plans/eqsat-measurement-harness.md
Branch: claude/mir-equality-optimizer-sodbej
Goal: per-query final_off / final_on / intrinsic-snapshot / plan_changed + attribution (eqsat-wins/redundant/clobbered/neutral). Extends src/transform/tests/eqsat_arrangement_benchmark.rs.
## Tasks
- Task 1: done (df1e40e584) + join-aware metric fix (d3fc63c41b). FINDING: eqsat finishes with all joins
  JoinImplementation::Unimplemented -> makes ZERO join-arrangement decisions; production JoinImplementation
  owns 100%. final_on==final_off net=0 on all 9 fixtures (faithful headline). intrinsic dimension uninformative
  (eqsat defers the arrangement-creating step); 'Clobbered=6' are artifacts. Verdict logic to be corrected in T2.
- Task 2: done (acb03f8c81). Honest verdicts (EqsatWins/NetNeutral) + g1/g2 divergence fixtures.
## HARNESS COMPLETE. Result: 11 fixtures, EqsatWins=0 NetNeutral=11 net=0. eqsat defers all joins to production.

## Task 3 (user-directed): prove JI cross-join sharing OPPORTUNITY exists.
## Correction: JI does NOT use cardinality; it minimizes reuse-aware new-arrangement COUNT (same basis as eqsat).
## So eqsat's only edge = GLOBAL vs LOCAL scope. Test: does production arrange one collection >1 way when a
## single shared key would satisfy both join sites (a sharing opportunity greedy per-join misses)?

## Task 4 (user-directed): explore the JOIN ORDER lever.
## Key-choice ruled out (forced by equivalences). Order is FREE: multi-way join arrangement keys vary with
## order; eqsat enumerates, JI greedy. Q: is JI's order ever arrangement-suboptimal vs min-over-orders,
## with a single shared key valid? Method: brute-force per-order arrangement count vs JI (definitive), or
## offline eqsat (commits joins, enumerates orders) vs JI. Honest negative if JI already order-optimal.
