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

## Diagnostic result: the MTZ constraints are the cost, and SCC-scoping is the fix

Measurement (the A/B pass: per fragment, solve WITH constraint 6 versus the same
model WITHOUT it, timed, the second result discarded) over 93,645 `joins.slt`
fragments settled the cause decisively:

* WITHOUT MTZ (base ILP): median 90us, p99 884us, worst 3.8ms, even on a 110-node
  fragment.
* WITH MTZ: median 940us, p99 86ms, worst 89.9 SECONDS.
* Total WITH 1738.8s versus WITHOUT 23.0s, so MTZ is 99% of solve time.
* 75.6% of fragments are cyclic. The worst was 110 nodes, 145 MTZ constraints,
  0.68ms without MTZ and 89.9s with it.

So the base ILP is affordable even on the largest join fragments. The GLOBAL MTZ
constraints (one per child-edge across the whole fragment, each with big-M = C)
are what explode microlp's branch-and-bound. The global formulation of the
mechanism above is superseded by the SCC-scoped one below.

### SCC-scoped MTZ

A directed cycle lies entirely within one strongly-connected component of the
candidate graph. An edge that crosses an SCC boundary can never be on a cycle, so
it needs no ordering constraint. Compute the SCCs of the reachable candidate
graph (Tarjan), and emit level variables and constraint 6 ONLY inside NON-TRIVIAL
SCCs. The commute cycles are localized two-class pairs, so the vast majority of
classes are singleton trivial SCCs with zero level constraints, restoring the
base-ILP-only cost (and the old E0 baseline) on all acyclic structure by
construction.

Two things this gets right, and they compound:

* Per-SCC level variables AND per-SCC big-M, not global. Inside an SCC of size
  `k`, levels are bounded `[0, k-1]` and big-M is `M = k`. For the dominant
  two-class commute pair that is levels `[0, 1]` and `M = 2`, not `M = C`. Big-M
  tightness is exactly what governs the LP-relaxation weakness driving the
  branch-and-bound blowup, so SCC-scoping does not only cut the constraint count,
  it shrinks every surviving big-M from `~C` to `~k`. The off-by-one invariant is
  the same as before, now PER-SCC: `M = |SCC| = level_max + 1`.
* Non-trivial SCC means size `>= 2` OR a self-loop edge. A self-referential node
  (child class == its own class) is a length-1 cycle sitting in a size-1 SCC that
  Tarjan does NOT report as non-trivial by size. It must still get its level
  variable and constraint 6, else a selected self-loop becomes extractable (an
  infinite term). So the 0.7% self-loop-only case is covered by the size-1-with-
  self-loop rule. A size-1 SCC with a self-loop has `|SCC| = 1`, level `[0, 0]`,
  `M = 1`, and the self-edge constraint `level[p] + 1 <= level[p]` is infeasible
  when selected, so the solver never selects it. The unit test must cover this
  size-1-with-self-loop SCC explicitly.

The unit tests from the global formulation still apply (two-class commute cycle
extracts to one order, never both cross-Projects; the boundary assignment), now
with the per-SCC level bound and M, plus the explicit size-1-self-loop test.

### Re-measure: SCC-scoped MTZ helped but did not collapse, so use exact DFJ cuts

The SCC re-measure (110,708 `joins.slt` A/B samples) showed max SCC size = 2
corpus-wide (every non-trivial SCC is a two-class commute pair, no fat SCCs), and
the worst solve dropped from 89.9s to 15.5s. But WITH-MTZ was still median 702us
(8x the 87us base), p99 31ms, 24% of solves over 10ms, MTZ still 97% of solve
time. A big join fragment holds MANY two-class SCCs, so it still gets many
continuous `level` variables and big-M constraints, and the big-M CONTINUOUS
variables are what strain microlp's branch-and-bound even at M = 2.

The fix, given max SCC size = 2: for the cycle sizes that actually occur, use the
EXACT Dantzig-Fulkerson-Johnson (DFJ) subtour-elimination cut instead of the weak
big-M MTZ. MTZ is a compact approximation of the DFJ cuts, and DFJ specialized to
small components is a pure-binary constraint microlp handles far better:

* Size-1 SCC with a self-loop: the DFJ cut at `|S| = 1` forbids the self-loop
  node. Pick ONE mechanism and test it: either a `node_sel[self] = 0` constraint,
  or exclude the self-referential node from the candidate set at construction. Do
  not do both halfway.
* Size-2 SCC `{A, B}`: the DFJ cut at `|S| = 2` is mutual exclusion. With the
  existing exactly-one-node-per-used-class constraint, a selected 2-cycle exists
  iff A's selected node has a child in B AND B's selected node has a child in A.
  Forbid exactly that conjunction: `xA + xB <= 1`, where `xA` is the sum of
  `node_sel` over every A-node with any child class in B, and `xB` symmetric. This
  is sound (it forbids only genuinely cyclic selections) and complete (it forbids
  every 2-cycle). No continuous variables, no big-M.
* Size >= 3 SCC: keep the per-SCC MTZ as the GENERAL fallback. None occur in the
  corpus today, but the SCC-size report is today's corpus, not forever, and a
  future rule could build a bigger SCC.

Requirements on the implementation:

* Enumerate ALL cross-edges, not just the commute `Project`. `xA` must sum over
  every A-node with ANY child in B, whatever the node type. Writing the cut
  against the commute-Project specifically would let a future rule with a
  different cross-edge shape silently escape it.
* Keep the size >= 3 MTZ path tested SYNTHETICALLY. It is dormant corpus-wide, so
  golden churn will never exercise it. Add a hand-built 3-class-cycle unit test so
  the general-correctness fallback does not rot, and as insurance against a future
  bigger SCC.
* The re-measure after this change should show the WITH column collapse toward the
  87us base (no continuous vars, no big-M, pure binary), with only a residual
  0.02% pathological tail for the deterministic size gate (below).

## Requirement 1: solver-risk gates (measured, not assumed)

MTZ has a weak LP relaxation (the big-M constraints are loose), `microlp` is
early-stage, and after this change every process solves the ~426 real join ILPs
that used to bail (plus the join-heavy query ILPs). So solve time is a first-class
risk and must be measured, not assumed.

* Corpus solve-time measurement. Instrument total ILP solve time and solve count
  per optimization, run the corpus before and after, and report the distribution
  (median, p95, worst). This is the gate: if solve time regresses beyond a
  budget, tighten the size gate `K` below.

NOTE: no fallback in this module may key on wall-clock time. A time-based bail
makes the chosen plan a function of solver speed, so the same query yields an ILP
plan on a fast machine and a greedy plan on a slow one. That is machine-dependent
goldens, the exact cross-machine flake class the extraction-determinism work
(`20260705_eqsat_extraction_determinism.md`) eliminated structurally at real
cost. Every fallback keys on an input property (SCC count, node count), never on
elapsed time. This strikes the earlier "per-solve timeout backstop" from the
design permanently.
* E0 compile-time gate re-run. Re-run the E0 optimize-time methodology (measure
  `enable_eqsat_physical_optimizer` optimize time versus the directional baseline,
  median and p95 and worst, as the original E0 gate did). E0 was measured while
  the ILP bailed on all joins, so its numbers do not reflect real join ILP solve
  time. This re-run is the corrected E0.

  Result (corrected E0, HiGHS backend, 2026-07-06). Paired wall-clock on the
  worst optimize-dominated file, chbench (the 7-way join with the 5.73s ILP
  solve): `enable_eqsat_physical_optimizer` on 29.18s versus the directional
  baseline (flag off) 16.40s, a 1.78x ratio for the whole file. The full corpus
  golden regeneration completed with zero statement-timeout stalls, which the
  microlp+ungated configuration could not do (chbench did not finish in 10
  minutes). So the corrected E0 passes: the join ILP that now actually runs is
  survivable. The chosen costs, on record with magnitudes: a per-solve median
  tax from the C++ setup (microlp 201us versus HiGHS 702us), bought back by an
  ~80x better tail (microlp worst 10.8s versus HiGHS 130ms on the small-solve
  corpus), and a 5.73s worst single solve on the gate-lifted 7-way (versus
  microlp's 54s). The tail win is why the size gate could widen from 6 to 30.
* Size cap. Keep the existing `total_nodes > 600` bail (a class-count bound also
  bounds the MTZ variable and constraint count). A cap hit falls back to greedy,
  counted and logged.
* SCC-size distribution (the re-measure gate). SCC-scoping is total only if the
  SCCs stay small. The two-class-pair shape is measured so far, not guaranteed:
  some rule combination could build a larger SCC, and MTZ inside a big SCC is
  still the old cost (though with the tighter per-SCC `M = |SCC|`). So the
  re-measure MUST report the SCC-size distribution corpus-wide (max SCC size, the
  tail). If max SCC size is small everywhere, the DFJ fix is total up to the
  many-pairs tail below. If a fat SCC appears, the deterministic size gate covers
  it too, and its frequency decides how much the gate matters.
* Deterministic size gate (the tail's stopgap, keyed on input, never time). Even
  with size-2 DFJ cuts, a join fragment holding MANY two-class SCCs is a large
  binary program that microlp's bounds-as-constraints branch-and-bound solves
  slowly (a 7-way join is ~21 commute pairs and 54s, while 1-3 pairs are
  sub-second). The pathological cases are separable at INPUT time by their
  non-trivial-SCC count (equivalently binary-exclusion count), so `solve` bails
  to greedy when that count exceeds a tuning knob `K`, via
  `record_ilp_fallback("cyclic_join_size")`, counted and logged. `K` is picked
  from the measured knee of the solve-time-versus-SCC-count curve and documented
  with its origin, and it widens or is removed when the backend can afford the
  tail (see below). This is deterministic and machine-independent, so it never
  moves a golden across machines, unlike a wall-clock bail. It sheds only the
  tail and keeps the ILP on small and medium joins.

  Those kept ILP plans are a win only with the width-aware arity tier ON. The
  gate lets the ILP run on the small and medium joins, but the ILP is a win over
  greedy there only if its objective values arrangement width. Left width-blind,
  the ILP loses projection pushdowns greedy keeps (measured: 7 of 70 net-wider
  golden files), so the gate is net-NEGATIVE without the tier. The tier is
  therefore promoted to the production objective (always on for the ILP), which
  is the precondition that makes this gate net-positive. See
  `20260705_eqsat_scalar_sharing_width.md` and
  `20260705_eqsat_extraction_determinism.md`.
* The tail's real fix is the HiGHS backend, not the gate. 54s for a ~100-node
  MILP with ~21 binary exclusions is microlp's bounds-as-constraints B&B, not the
  problem's difficulty. A production MILP solver (HiGHS, single-thread for
  determinism) solves this sub-second and deterministically. The HiGHS spike is
  pre-authorized and queued immediately after the join-parity audit and the WS2
  certification. The size gate `K` is the honest stopgap that keeps the branch
  green until then, and `K` widens or is retired once HiGHS lands.

  Realized instance: chbench (7-way TPC-CH join, ~21 commute pairs) is the query
  that measured 54s ungated and shed to greedy under `K=6`, dropping its
  `PhysicalEqSatTransform` to 1.775s. Because it sheds to width-blind greedy it
  stays net-wider than merge-base (+66 arity), tracked as an accepted residual in
  `20260705_eqsat_scalar_sharing_width.md` (known-width-residuals section) with
  this HiGHS swap as its named recovery: a real solver makes the 7-way affordable,
  `K` rises or retires, and the join returns to width-aware ILP. NOTE: the greedy
  drift still owes a worse-to-execute check on the final regen before landing.

Validation sequence: SCC-scoped MTZ, then re-run the A/B (the WITH column should
collapse toward the WITHOUT column and the SCC-size distribution reported), then
the E0 gate re-run is the actual pass/fail, then calibrate the size gate `K` from
the new distribution, then resume the join-parity audit and the WS2
certification, then the HiGHS spike. The 93k-solves-per-file volume finding goes
to the roadmap, not this fix.
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

Fold in the CSE-width free experiment. The aggregation_nullability residual (a
shared `Let` representative that widened from arity 1 to arity 2 under the
determinism-canonical extraction, tracked in the width doc's known-residuals) is
governed by the `cost.rs` `(degree, arity)` pair, the still-gated half of WS2's
width work that feeds the greedy and CSE paths. When this certification flips
`enable_eqsat_scalar_sharing` on, check whether aggregation_nullability (and the
same-family all_parts_essential shape, to the extent the candidate exists)
narrows. If it narrows, the residual has a built fix awaiting only its promotion
decision. If it does not, it is a genuine CSE-ordering gap. Either way the answer
comes free from the certification run, with no new work now.

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
