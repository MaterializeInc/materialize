# Physical join planning in the e-graph: making order, strategy, and placement cost-selected

> **Status: CONCLUDED. See `20260704_eqsat_research_verdict.md` for the outcome.**
> Phases 1 and 2 landed (the `commit_join` extract and the net-of-shared delta
> count reaching JoinImplementation parity, fixing `outer_join.slt`). Phase 3
> (the physical e-node plus ILP joint selection) was SHELVED before building: a
> seeding wall (the ILP solve is pre-CSE, pre-raise, with no point to construct a
> context-dependent physical candidate) and a structural no-win (arrangement keys
> are predicate-determined, so no order-times-sharing plan exists that greedy
> misses). The durable reframe: physical join planning is context-dependent, so
> the raise boundary given the extracted children is its correct home, and the
> two-layer split (logical in the e-graph, physical at raise) is right, not an
> inconsistency. Phases 1 and 2 are the end-state. The design below is retained as
> the record of how that conclusion was reached.
>
> **Original status (superseded):** design thesis and layer decision. It records
> the architectural decision the capability arc forced, before the phase-3
> investigation shelved the e-graph-physical-planning path.

## Why this exists

The capability arc (E0-E5, arrangement sharing, cost-based selection) closed
negative across every axis for the engine as it stands.
The cost-selection probe found the reason at the architecture level.
eqsat globally cost-selects only logical and spelling choices during extraction.
The physical join plan, the join order and the delta-versus-differential
strategy, is committed greedily in `raise` (`raise.rs:181-256`), in the same
position and with the same greedy character as production's JoinImplementation.
There is no e-class that holds `{order A, order B, delta, differential}` as
costed alternatives, so the classic Cascades win, choosing a globally optimal
plan that no fixed pass order can reach, is not available.

This is an inconsistency, not a missing feature.
A cost-selecting optimizer that then commits its most important physical
decision greedily is half a design.
For a research project the honest resolution is to remove the inconsistency by
making the physical join plan a first-class, cost-selected part of the e-graph.
That resolution is also the only thing that actually tests the capability
hypothesis instead of assuming it.

## Risk, reframed: feasibility resolved, selection value is the open question

The combinatorial-explosion risk is resolved on paper (JI-order-bounding recon,
2026-07-04).
JoinImplementation does NOT enumerate n! orders.
`optimize_orders` (`join_implementation.rs:1115`) computes a bounded frontier of
exactly n greedy orders, one best-first Prim/Dijkstra-style order per starting
input, O(n squared times k) in inputs n and candidate arrangements k.
Real join arity is small (sqllogictest goldens max around 6 inputs, mostly 2 to
4), so the frontier is trivially within the E0 budget.
The design reuses this exact frontier: insert JI's n start-orders (or top-k) as
e-class alternatives and let the cost model pick, instead of differential's
hard-coded max-min discard (`join_implementation.rs:860-875`).
No new order search, no reintroduced factorial or exponential closure.
Feasibility is therefore not the risk.

The real open risk is selection VALUE, and the recon sharpened it.
JI's structural pruning tends to COLLAPSE the frontier toward agreement: when one
input is uniquely-keyed or the only arranged one, all n greedy starts converge on
the same effective order, which is also what differential's max-min picks.
So the frontier is cheap and bounded but often thin, it may hold only one
materially distinct order.
This is the same "greedy-in-both, little to select" the cost-selection probe
found, now confirmed from the order side.

The value hypothesis, stated honestly.
The win, if any, is not in holding MORE orders (the frontier is what it is) but
in RANKING the bounded frontier by a global cardinality-aware plan cost, where JI
ranks by a local structural max-min with cardinality inert by default
(`join_implementation.rs:282-294`, the cardinality field is None unless
`enable_cardinality_estimates`).
Two places this could bite: cardinality distinguishing orders JI's structural
heuristic sees as equivalent, and a global DAG plan cost diverging from JI's
per-order worst-element proxy.
Portability caveat: the first is portable, production can enable its cardinality
estimates and feed the same heuristic.
The intrinsic residue is the second, a global plan cost versus JI's local
max-min, which JI's per-order scoring cannot replicate by design.
Whether that ever changes the chosen plan on real queries is what the build
exists to answer, and the prior probes suggest the headroom is thin.

This does not change the decision to build.
It sharpens what the build proves.
Parity is the guaranteed payoff: reuse the frontier, at least match JI's pick,
retire the greedy raise commit, and fix `outer_join.slt` via the delta-versus-
differential cardinality cost.
Superiority is the thin, uncertain residue in global-versus-local cost, and the
build is the only way to test it rather than assume it.

## The problem restated

Three physical decisions currently sit outside the cost-selected term.

* Join order, the sequence in which inputs are joined.
* Strategy, differential (a linear chain of binary joins with a running
  intermediate) versus delta (independent per-input dataflows that avoid the
  intermediate).
* Arrangement placement, which `(collection, key)` arrangements are built and
  which are reused, the subject of the companion arrangement document.

These interact.
The best strategy depends on the order and on which arrangements are already
available, and the value of an arrangement depends on the order and strategy
that consume it.
Deciding them in sequence, greedily, is the phase-ordering trap.
Holding them as joint alternatives and costing them together is what an e-graph
is for.

## Is MIR standing in the way

The answer refines an earlier overstatement in an earlier draft of this
document, which claimed MIR was the obstacle and a new inter-IR layer was
needed.
It is not, and it is not.

MIR is an adequate OUTPUT format.
Its `Join` carries the committed physical plan in one
`implementation: JoinImplementation` field (`relation.rs:233-235`), which
encodes any join order, per-input arrangement keys, and differential-versus-delta
strategy.
That is fully expressive for a committed plan.
It is also the existing integration point: when eqsat sets `implementation`,
JoinImplementation defers.
JI matches only `Unimplemented | Differential` (`join_implementation.rs:156`),
so an eqsat-set `DeltaQuery` is skipped outright, and an eqsat-set `Differential`
is kept under eager mode (`join_implementation.rs:366-369`, `507`).
So the output channel already exists, and setting the field already makes JI
inert.

What MIR cannot do is hold ALTERNATIVES, many physical plans coexisting for
cost-selection.
But that was never MIR's job.
Holding alternatives is the e-graph's job.
The earlier framing conflated the two.
The physical alternatives live in the e-graph, and only the chosen plan is
written to MIR's `implementation` field at extraction.
So MIR is an adequate output and the search happens above it, in the e-graph.
The ArrangeBy overload is likewise not an output problem, the committed plan's
arrangements are already decided and render dedups them by `(collection, key)`
regardless (the companion document's D1 finding), so build-versus-assert is an
INTERNAL cost concern (placement e-nodes), not an output one.

The order-freedom of MIR's logical `Join` is therefore not an obstacle either.
Order is not in the logical term by design, and it does not need to be.
Order lives in the physical e-nodes inside the e-graph, and extraction writes the
chosen order into `implementation`.

## Is LIR the answer instead

No, and it does not need to be.
`LinearJoinPlan` and `DeltaJoinPlan` (`compute-types/src/plan/join/`) are
committed plans, one implementation each, an extraction target rather than a
saturation language.
Since MIR's `implementation` field is an adequate output and makes JI inert,
there is no reason to reposition the e-graph lower, at MIR-to-LIR, which would
inherit LIR's committed-plan shape and its expressiveness constraints for no
gain.

## The layer decision

The physical alternatives live in eqsat's OWN relational language and extract to
MIR's existing output.
No new IR, no MIR shape change, no LIR repositioning.

* Rejected, a standalone physical algebra as a new IR between MIR and LIR (this
  document's earlier recommendation). Over-escalation. The output channel already
  exists, MIR's `implementation` field with JI inert, so no new inter-IR layer is
  needed to carry the committed plan.
* Rejected, reposition the e-graph as MIR-to-LIR lowering. LIR's committed shape
  is wrong for saturation and buys nothing given the adequate MIR output.
* Adopted, add physical e-node TYPES to eqsat's existing relational language. The
  e-graph gains physical operators, a physical join node carrying its order and
  strategy, delta as an alternative class member, arrangement nodes with a
  build-or-assert marker, so that order, strategy, and placement coexist as
  alternatives in the e-class and are cost-selected by extraction. Extraction
  lowers the chosen physical plan into a MIR `Join` with `implementation`
  populated plus materialized ArrangeBy nodes, and JI goes inert.

The "physical algebra" is thus realized as e-node types in eqsat's language, not
a separate IR.
Logical MIR stays logical, LIR stays committed, and the e-graph, which is built
to hold alternatives, is the one place the physical alternatives coexist and get
costed.
This is strictly less invasive than a new inter-IR layer and reuses the existing
native-commit output boundary.

## Representation

In the physical algebra, a join is not a symmetric n-ary node with a hidden
implementation.
It is a term built from physical operators that name their own cost.

* A physical binary join node carrying its two inputs and its strategy, so that
  a linear chain is an explicit left-deep (or bushy) tree of such nodes and
  alternative orders are alternative trees in one e-class.
* Arrangement nodes keyed by `(collection e-class, key)` with an explicit
  build-or-assert marker, hash-consed so that a shared `(collection, key)`
  request is one node, as the companion document develops.
* A delta node for a join whose per-input arrangements are all available or
  cheap, as an alternative member of the same class as the differential tree.

Order becomes a first-class structural fact.
Strategy becomes a choice between class members.
Placement becomes the sharing of arrangement nodes across the term.
All three are then selected by the same extraction cost that already selects
logical spellings.

## Termination and blowup, resolved

Order is NOT introduced as a free commutativity/associativity rewrite, which
would reintroduce the factorial closure deliberately removed at
`relational.rewrite:341-342`.
Instead the physical e-nodes are populated from JoinImplementation's existing
bounded frontier: `optimize_orders` builds n greedy best-first orders in
O(n squared times k), applying the same availability, bound-support, and
connectivity pruning JI already uses (`join_implementation.rs:335-358`,
`:1376-1416`) so only viable, bound-supported order steps become e-nodes.
The e-graph holds JI's n start-orders (or top-k) as alternatives, not the
permutation closure.

The bound is therefore reuse, not invention, and it is polynomial.
At the observed arity (mostly 2 to 4 inputs, max around 6) the frontier is a
handful of alternatives, trivially within the E0 optimize-time budget.
The spec must still gate and measure the expansion against the E0 budget as the
variadic-set rules are, but the recon establishes there is a bounded, viable
frontier to insert rather than a search to tame.

## Cardinality cost

The current cost model is cardinality-free (`cost.rs:6`), an AGM worst-case
size-degree from arities and keys.
That is sufficient to compare spellings but not to choose a physical plan, where
the whole point is that a small input is cheap to arrange and a large
intermediate is expensive to maintain.

Add locally-known cardinalities, structural facts derivable without statistics.

* A `Reduce`, `distinct`, or monotonic top-1 grouped by `G` produces at most one
  row per distinct `G` and is keyed by `G`.
* A `TopK` with limit `k` produces at most `k` per group.
* `Threshold` and `Filter` do not increase cardinality, `Map` and `Project`
  preserve it, a `Constant` has an exact count, a join on a unique key is bounded
  by the other side.
* Keys and functional dependencies from `RelationType` propagate these bounds.

Reuse the existing mz cardinality and key analysis rather than writing a new
one.
Feed cardinality through a single interface so that real statistics can replace
the structural estimate later without changing the cost model, the door the
user asked to keep open.
The estimate is a bound today and a statistic tomorrow, the cost model does not
care which.

## Delta versus differential as costed alternatives

With cardinality in the cost and both strategies present as class members, the
strategy choice becomes an extraction decision.
The cost weighs delta's built input arrangements against differential's
unbounded running intermediate, now that cardinality can quantify both.
This subsumes the greedy count-based gate in `raise` and fixes the
`outer_join.slt` regression as a byproduct, because the small distinct TopK
arrangement delta must build is cheap and differential's intermediate is not,
which the cardinality cost sees and the count gate cannot.
The regression fix is therefore not a separate task, it falls out of the correct
cost model, and the parity it reaches is the floor, not the goal.

## Joint extraction and the colored layer

Extraction must select order, strategy, placement, and logical spelling
together, by cost, over the joint space.
The bottom-up extractor already selects the minimum-cost representative of a
class.
The open question is whether placement interactions that identity alone does not
capture, a consumer that could derive `(C, K')` cheaply from an existing
`(C, K)`, reintroduce context-dependence.
The companion document argues that base sharing is captured by hash-consing and
the ArrId dedup, and that cross-key derivation is the residual that needs the
colored, context-sensitive layer.
The colored layer is therefore the likely home for any residual availability
analysis, and the physical algebra must be designed so the colored layer can
attach to it.

## Integration: field-inert versus bypass

There are two ways to hand physical join planning to eqsat, and they differ in
robustness.

* Field-inert. eqsat sets `implementation` and JoinImplementation defers, and JI
  stays in the pipeline as a fallback for joins eqsat does not commit. Simple,
  but the deferral is only clean for `DeltaQuery`, which JI skips outright
  (`join_implementation.rs:156`). For an eqsat-set `Differential`, JI re-enters
  and only keeps the plan under eager mode (`:366-369`, `:507`), a fragile
  coupling to a flag.
* Bypass. In the eqsat optimization path, remove JoinImplementation from the
  pipeline entirely. eqsat owns all join implementation, JI runs only in the
  non-eqsat path, and the two pipelines separate cleanly by the flag.

Bypass is preferred, because it does not depend on JI correctly deferring to an
eqsat-set plan, and it is the honest end-state where eqsat owns physical join
planning outright.
The cost is a completeness burden, in the eqsat path eqsat must implement every
join or downstream breaks.
During the phased build, where coverage is partial, keep a fallback: for a join
eqsat's physical planning does not yet cover, delegate to JI's existing
implementation algorithm as a subroutine rather than leaving it `Unimplemented`.
Remove the fallback when coverage is complete, at which point JI is fully retired
from the eqsat path.
This makes retirement a property of coverage, not a separate deprecation.

## Retirement path

The payoff is measured against JoinImplementation in two stages, in order.

* Parity first. The physical selection must reach no-worse plans against
  JoinImplementation across the whole sqllogictest corpus, the maintainability
  bar the current engine violates on `outer_join.slt`. Only when parity holds may
  the greedy `raise` commit be retired in favor of physical extraction.
* Capability second. With parity established, find the phase-ordering cases where
  joint cost-selection beats JoinImplementation's greedy per-join choice, the
  research payoff. If those cases exist and are common, JoinImplementation itself
  becomes subsumable. If they do not, the honest result is that physical planning
  in the e-graph reaches parity but not superiority, which is still a
  consolidation win, and the capability question closes for the last time with
  the strongest possible evidence, a built joint optimizer that does not beat
  greedy.

Either outcome is a valid research result.
The build is justified because it is the only way to distinguish them, and
because leaving the physical commit greedy is the inconsistency the project
cannot ship.

## Phasing

Reconciled by the phase-1 redesign (2026-07-04). Two findings force it. First,
on the greedy extractor physical join planning is a given-the-children commit:
the DP fixes each child class to its LOCAL optimum before the join is planned,
and the commit reads post-CSE input shape (`per_input_available`,
`raise.rs:530-615`), so the natural home for phases 1 to 2 is the raise boundary
where children, scope, and availability are all present, not the extraction DP.
Second, the physical e-node exists to let EXTRACTION cost-select physical
candidates, which is real only under the non-greedy `IlpExtractor`
(`extract.rs:65-73`). So the e-node is a phase-3 construct, and its shape (order-
annotated candidates versus a committed blob) is dictated by the phase-3 ILP
consumer that does not exist yet. Building it earlier would guess that shape.

* Phase 0, this design and the spec. Done.
* Phase 1, a parity refactor at the raise boundary, no behavior change. Extract
  the monolithic raise Join-commit (`raise.rs:181-295`) into a reusable
  `commit_join(inputs, raised_inputs, equivalences, available, flags)` returning
  the committed `MirRelationExpr` or `None`. The raise arm calls it, byte-
  identical. This is the shared foundation both later phases call: phase 2 swaps
  the gate inside it, phase 3 calls it as the given-children subroutine from the
  ILP path.
* Phase 2, the local win at the raise boundary. Replace the count-based
  delta-versus-differential gate (`raise.rs:262-266`) with the structural
  cardinality cost (the D3 interface). Fixes `outer_join.slt`. Goldens move here,
  and because phase 1 was byte-identical the movement is unambiguously the
  cardinality change.
* Phase 3, the capability, in the e-graph. Physical candidates (the order
  frontier plus delta and differential) as e-class members, and the
  `IlpExtractor` for joint cost-selection. The physical e-node lives HERE, its
  shape driven by the ILP consumer, and the scope and placement questions are
  solved here because they are now genuinely needed. This is the capability test:
  whether joint cost-selection ever beats the greedy per-join choice.
* Phase 4, arrangement placement as e-nodes folded in from the companion
  document, and the colored layer for cross-key residuals.
* Retirement. Phase 2 IS the improved raise commit, not its retirement, so there
  is no "retire after phase 2" step. The greedy raise commit is subsumed only
  if and when phase 3's ILP path proves out and owns join planning outright.
  JoinImplementation retires only after that phase-3 capability evidence.

## Open questions

* What exactly is the physical algebra's join node, and how does a bushy plan
  differ from left-deep in the term.
* Can the order search be bounded to stay within the E0 budget, and what is the
  pruning rule.
* Does the existing mz cardinality analysis cover the operators above, or are
  there gaps to fill.
* Does the physical algebra extract cleanly to today's LIR, or does LIR itself
  need enrichment to express the plans the algebra can hold, the same LIR
  expressiveness constraint the companion document flagged.
* Where does the physical algebra live in the crate structure, and how does it
  relate to the existing `CombinedLang` and colored substrate.
