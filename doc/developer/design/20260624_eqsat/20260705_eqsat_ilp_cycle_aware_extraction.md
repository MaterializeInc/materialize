# Cycle-aware ILP extraction (Finding 2 fix, option A)

Status: spec, pending review (review gate 2). Resolves the Finding-2 filing
`20260705_eqsat_ilp_join_commute_cycle_bail.md`.

## Problem (from measurement, review gate 1)

`commute_binary_join` (relational.rewrite:370) rewrites `Join(a, b)` to
`Project[swap](Join(b, a))`, schema-identical, so it hash-conses into the join's
own e-class. The two join orders become two classes that cross-reference each
other through their `Project` alternatives: class A holds
`{Join(a, b), Project[swap](Join(b, a) @ B)}`, class B holds
`{Join(b, a), Project[swap](Join(a, b) @ A)}`. That is the e-graph correctly
expressing "either order is available." It is also a directed cycle A to B to A
in the candidate graph.

`IlpExtractor::solve` runs `reachable_has_cycle` over the candidate graph
(extract.rs:182) and, on any cycle, returns `None`, so `IlpExtractor::extract`
silently falls back to the DAG-blind `GreedyExtractor` (extract.rs:148).

Measurement (2026-07-05): 19,206 bail events across 41 corpus files, with a
~426-per-process floor from catalog bootstrap alone (builtin views are joins).
100% of bails carry a self-loop, but 99.3% ALSO retain a genuine multi-class
A to B to A cycle after self-loop pruning. So excluding self-referential nodes
clears only 0.7%. The dominant case is the commutativity cross-reference. Net
effect: the ILP has effectively never run on join queries on this branch. It
silently degrades to greedy on essentially all of them, which taints every prior
ILP-on-joins conclusion (E0 optimize-time gate, E4 join cost-quality,
delta-join-cost, native-join-commit). WS0/WS1 are clean (their acceptance shapes
are join-free).

## Why option A (and not B or C)

The cycle is in the CANDIDATE graph, not in any extractable PLAN. A valid acyclic
selection always exists: pick `Join(a, b)` for class A and `Join(b, a)` for class
B, never the `Project`-of-the-other in both. The greedy fallback already finds
such a plan (it can even pick the `Project` alternative when cheaper). So the fix
is to make the ILP encode acyclicity of the SELECTED subgraph, which is the
standard way ILP e-graph extractors handle cyclic candidate graphs. This is
finishing the extractor to textbook form, not speculative machinery.

Rejected. B (re-express commutativity so it does not self-hash-cons) is the wrong
layer: it churns every join plan to work around an extractor limitation, and
risks the join-order cost model. C (restrict the ILP to a canonical-order acyclic
subgraph) makes the ILP strictly less expressive than its own greedy fallback,
which can select the swapped `Project` alternative. That is incoherent.

## Mechanism: MTZ-style level variables

Add acyclicity constraints on the selected subgraph via per-class topological
level variables, the Miller-Tucker-Zemlin (MTZ) subtour-elimination formulation,
the same technique the egg LP extractor (`egg::LpExtractor`) and the
extraction-gym ILP extractors use to guarantee an acyclic extracted DAG. Cite:
egg's `lp_extract` feature and MTZ (Miller, Tucker, Zemlin 1960) as the standard
acyclic-ordering ILP encoding.

New variables (added to the existing `node_sel` / `arr_sel` / `class_used` in
`IlpExtractor::solve`, extract.rs):

* `level[c]` continuous, one per reachable class `c`, bounded
  `0 <= level[c] <= C - 1` where `C` is the reachable class count. A topological
  numbering of `C` classes needs only levels `0..C-1` (the longest path has at
  most `C-1` edges), so `level_max = C - 1`.

New constraint (added alongside constraints 1 to 5 in `solve`), with big-M
`M = C`:

* For every node `n` in class `p` and every child class `c` of `n`:

  ```
  level[c] + 1 <= level[p] + M * (1 - node_sel[(p, n_pos)])     with M = C
  ```

  When `n` is selected (`node_sel == 1`) the big-M term vanishes and the
  constraint is `level[c] + 1 <= level[p]`, forcing the child strictly below the
  parent. When `n` is not selected (`node_sel == 0`) the term is `M = C`, which
  relaxes the constraint, so an unselected commute `Project` imposes no ordering.
  The selected subgraph therefore admits a topological order (the `level`
  values), hence is acyclic. The candidate graph may remain cyclic.

The big-M invariant is `M >= level_max + 1`. With `level_max = C - 1` and
`M = C`, an unselected edge at the worst-case boundary (`level[c] = C - 1`,
`level[p] = 0`) gives `(C - 1) + 1 <= 0 + C`, i.e. `C <= C`, satisfied and NOT
binding. The earlier draft used `level <= C` with `M = C`, which is off by one
(`level[c] = C`, `level[p] = 0` gives `C + 1 <= C`, falsely binding an unselected
edge). The two safe choices are `level <= C - 1` with `M = C` (used here, tighter
big-M, which helps the LP relaxation) or `level <= C` with `M = C + 1`. The unit
test must include a worst-case-boundary level assignment so this off-by-one
cannot silently return.

Self-loops are handled by the SAME constraint, no special case. A self-referential
node has `c == p`, so selecting it gives `level[p] + 1 <= level[p]`, infeasible,
and the solver simply never selects it. Unselected it is trivially satisfied. So
the 0.7% self-loop-only bails need no separate treatment either. This is one more
reason option A is the complete fix, not a partial one. The unit test asserts the
self-loop `Project` is never selected.

Then `reachable_has_cycle` and its bail are REMOVED as a trigger: the ILP now
represents the cyclic-candidate case directly. The solver-failure fallback to
greedy stays (a genuinely infeasible or solver-erroring problem still falls back,
always sound), but it MUST be counted and logged, never silent (this is the
second half of the original bug, that a bail was invisible).

Implementation checkpoint: `node_sel` integrality. The MTZ acyclicity argument
assumes `node_sel` is binary (a fractional selection weakens the big-M
constraints and could leave a fractional cycle). The existing extractor builds
`node_sel` as `variable().binary()`, so integrality is declared, but the
guarantee now leans on it harder than the count objective did. Confirm as a
read-first step that `microlp` actually solves this as a mixed-integer program
(enforcing the binary declaration), not as a continuous LP relaxation whose
rounding could reintroduce a cycle. If microlp only relaxes, the acyclicity
holds only at integral solutions and the fix needs the solver to branch, verify
this before relying on the encoding.

## Requirement 1: solver-risk gates (measured, not assumed)

MTZ has a weak LP relaxation (the big-M constraints are loose), `microlp` is
early-stage, and after this change every process solves the ~426 real join ILPs
that used to bail (plus the join-heavy query ILPs). So solve time is a first-class
risk and must be measured, not assumed.

* Corpus solve-time measurement. Instrument total ILP solve time and solve count
  per optimization, run the corpus before and after, and report the distribution
  (median, p95, worst). This is the gate: if solve time regresses beyond a
  budget, tighten the size cap or add a per-solve timeout.
* E0 compile-time gate re-run. Re-run the E0 optimize-time methodology (measure
  `enable_eqsat_physical_optimizer` optimize time versus the directional baseline,
  median and p95 and worst, as the original E0 gate did). E0 was measured while
  the ILP bailed on all joins, so its numbers do not reflect real join ILP solve
  time. This re-run is the corrected E0.
* Size cap. Keep the existing `total_nodes > 600` bail (a class-count bound also
  bounds the MTZ variable and constraint count). A cap hit falls back to greedy,
  counted and logged.
* SCC-scoped MTZ (the first scaling lever, held in reserve). `level` variables
  and constraints are only needed inside non-trivial strongly-connected components
  of the candidate graph. An edge that crosses an SCC boundary can never lie on a
  cycle, so it needs no ordering constraint. The commute cycles are localized
  two-class pairs, so condensing the candidate graph into SCCs first and emitting
  MTZ constraints only within non-trivial SCCs would cut the added constraint
  count enormously (most classes are singleton SCCs with zero MTZ constraints).
  Do NOT build this up front. But if the solve-time gate strains, it is the FIRST
  lever to reach for, ahead of tightening the size cap: SCC-scoping preserves the
  ILP's expressiveness (it still solves the whole fragment), whereas lowering the
  cap concedes more queries to greedy. Name it here so the implementer reaches for
  it in the right order.
* Bail-to-greedy retained and OBSERVABLE. The fallback stays for solver failure,
  infeasibility, panic, and the size cap. Every fallback increments a counter and
  emits a debug log with the reason (solver-error, infeasible, size-cap, panic).
  No silent bail. The old `reachable_has_cycle` bail is gone.

## Requirement 2: JoinImplementation-parity re-verification (in scope)

Post-fix the ILP runs on join queries where greedy ran before, and it may pick
DIFFERENT join orders than greedy did, so it could regress the physical-join
phases-1-2 parity that was verified under greedy-in-fact. The moved-golden audit
on the join files IS the parity re-check. For every moved join golden, the ILP
plan must be equal-or-better than the JoinImplementation / greedy plan it
replaces: arrangement count equal or lower, join type and order sensible, no
new full scan where a lookup existed. A moved golden that is worse to execute is
a STOP-and-report, not a recert. This gate governs the landing.

## Requirement 3: staged landing

1. Unit test first. Construct a two-class commutativity-cycle e-graph by hand
   (class A = `{Join(a, b), Project[swap](Join(b, a) @ B)}`, class B symmetric),
   and assert `IlpExtractor::solve` extracts successfully (does not bail) and the
   extracted plan selects exactly one plain `Join` per class, never the
   `Project`-of-the-commuted-order in both classes (which would be the cyclic
   selection). The test must also cover: (a) a self-referential node (a `Project`
   whose input is its own class) is NEVER selected, and (b) a worst-case-boundary
   level assignment (a chain forcing `level` near `C - 1` against an unselected
   edge at the boundary) so the big-M off-by-one cannot silently return. Mirror
   the e-graph-build idiom of the existing
   `ilp_arrangement_count_not_worse_than_greedy` test.
2. File-by-file churn audit, join-heavy first. Land the change, then regenerate
   and audit goldens file by file in this order: `joins.slt`, `tpch_select.slt`,
   `ldbc_bi*.slt`, then the transform join files (`join_fusion.slt`,
   `relation_cse.slt`, `predicate_pushdown.slt`, `subquery.slt`), then the rest.
   Each moved golden audited per Requirement 2. This is NOT a byte-identical
   change on the ILP path, that is the point.
3. `enable_eqsat_ilp_extraction = false` control. With the ILP off (greedy), the
   corpus is byte-identical to before this change (the MTZ code path is not
   taken). Verify this as the safety control. No new flag is introduced, the
   existing ILP flag is the A/B control.

## Requirement 4: WS2 certification (last, the smallest consequence)

Only after the ILP works on joins and the join-golden audit is clean, re-run the
held WS2 Task-5 acceptance (the join-shaped jsonb subset-prefix query, repro in
the Finding-2 filing): under `enable_eqsat_scalar_sharing` the shared `Map`
prefix is computed once in a `Rel::Let`, inspect-first (diagnose if not: post-eqsat
re-fusion by `CanonicalizeMfp`, pruning by `ProjectionPushdown::skip_joins`, or
the tie arithmetic not holding on the real shape). Land the acceptance slt and the
anti-case (widening declined). Update the WS2 design-doc status from landed-inert
to certified, or report exactly what still blocks. This is now the smallest
consequence of the fix, not the point.

## Files

* `src/transform/src/eqsat/extract.rs`: add `level` variables and the MTZ
  constraint in `solve`, remove the `reachable_has_cycle` bail trigger (keep the
  function only if still used elsewhere, else delete), add the counted/logged
  fallback. The width-aware arity tier and the scalar-aware node tier are
  untouched.
* Tests: the two-class-cycle unit test (extract.rs), plus the golden churn.

## Deliverables

The MTZ cycle-aware ILP (spec'd here, reviewed, landed staged), the solve-time
and E0 re-run measurements, the counted/logged fallback, the join-golden parity
audit, the WS2 acceptance certified or a precise remaining blocker, and the
design docs updated (Finding-2 filing resolved, WS2 status). Stop for review
after this spec, before implementation.
