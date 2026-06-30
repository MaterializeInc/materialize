# Re-aiming the eqsat cost model at reuse-aware arrangement sharing

## Goal

Make the equality-saturation MIR optimizer (`mz_transform::eqsat`) minimize the
number of distinct maintained arrangements a plan builds, crediting arrangements
that already exist (indexes, materialized views) and arrangements shared within
the plan.
The objective is to produce plans that maintain strictly fewer arrangements than
the production transform pipeline, by choosing logical forms, join orders, and
keys so that consumers share arrangements that a fixed pass order decides
locally and cannot align.
Arrangement count is the quantity that drives steady-state memory in a
differential-dataflow system, so optimizing it directly targets the resource
that matters.

## Non-goals

* Cross-query co-optimization (choosing forms across multiple new dataflows so
  they share newly-built arrangements).
  The only cross-query input is the existing-arrangement oracle (indexes and
  materialized views), treated as free.
* Optimizer performance.
  This is a research prototype; the join-order enumeration this design adds to
  saturation is the canonical e-graph blowup case, and bounding it is deferred to
  a separate effort.
* Cardinality or statistics.
  The cost model stays cardinality-free; arrangement count is a structural
  quantity, not a size estimate.

## Background

The current cost model's memory axis measures worst-case output degree, the peak
arrangement size, compared lexicographically on the maximum term.
This ranks a worst-case-optimal (delta) join above a binary join for every
cyclic join, because a binary join has a quadratic intermediate and the peak-term
comparison is dominated by it, regardless of how many smaller arrangements the
delta plan maintains.
Production instead minimizes the count of newly built arrangements, reuse-aware,
and that objective is what avoids the memory regressions a peak-degree model
causes.
The investigation that established this is recorded in
`docs/superpowers/plans/2026-06-22-eqsat-delta-vs-differential-commit.md`; this
design supersedes the cost-accounting framing there.

## Architecture

### Pluggable objective and extractor

The e-graph machinery stays independent of what is minimized.
Two abstractions vary independently:

* An `Objective` exposes, for an e-node given its children's chosen classes and
  keys, the set of arrangements that e-node maintains, plus a scalar weight for
  secondary tie-breaking (time, node count).
* An `Extractor` consumes any `Objective` and returns the chosen `Rel`.

`ArrangementCount` is the new default `Objective`.
The existing AGM/peak-degree behavior is retained as an alternate `Objective` for
regression comparison and for the time axis.
The ILP extractor and the existing greedy extractor both consume any `Objective`.
Saturation and raise are unchanged by the objective swap; only what is handed to
the extractor changes.

### The metric

An arrangement is identified by `(canonical e-class id, key)`.
The memory cost of a plan is the count of distinct arrangements it maintains,
minus those covered by the oracle (an existing index or materialized view on the
same collection and key).
Sharing is automatic: two consumers that need the same `(class, key)` arrangement
contribute one to the count.
Time stays an AGM-derived secondary axis, so a plan that ties on arrangement
count is broken by worst-case work.

### Absorbing the join order and key decision

A join input arrangement is shared with another consumer only when both arrange
the input by the same key, which is an order and key choice.
Production decides each join's order locally and greedily, so it cannot align
keys across consumers.
This design moves join order and key selection into saturation: associativity and
commutativity rules enumerate join orders as e-nodes, so the cost model chooses
the order whose arrangement keys align with other consumers.
Once the order is fixed by the chosen e-node, the arrangement set is exact, so
the cost model needs no planner call and no structural estimate to count it.

The production join planner is split:

* The decision (order, strategy, key per input) is subsumed into the e-graph and
  driven by the arrangement-count objective.
* The mechanical realization (deriving arrangement keys for a fixed order,
  lifting MFPs, emitting the `DeltaQuery` or `Differential` tuples) is included
  as a deterministic step at raise.

The planner's heuristic ordering is replaced, not reimplemented; only its
mechanical realization is reused.

### ILP extraction

Extraction minimizes a set-cardinality objective, which is not compositional: a
plan's cost is the size of the union of its arrangements, and sharing makes the
union smaller than the sum, so bottom-up min-cost extraction cannot optimize it.
Extraction is encoded as a 0/1 program:

* A binary selector per e-node, with one selected node per reachable class.
* A binary indicator per `(class, key)` arrangement.
* Constraints: the root is selected; a selected node implies its children's
  classes are selected; a selected node implies the arrangement indicators it
  requires are set.
* Objective: minimize the sum of arrangement indicators not covered by the
  oracle, with the AGM scalar as a tie-break term.

The plan-size cap (`MAX_PLAN_SIZE = 200`) bounds the model.
This replaces greedy extraction for the arrangement objective; greedy extraction
remains available behind the `Extractor` abstraction.

## Subsume or include audit

Every production transform the eqsat pass touches is a deliberate decision.

* `CanonicalizeMfp`: subsume.
  The fusion rules already produce canonical Map-Filter-Project form; the
  post-raise coalesce crutch is removed once saturation covers it.
* `JoinImplementation`: subsume the decision, include the realization.
  Order, strategy, and key are chosen by the arrangement objective in the
  e-graph; key derivation and MFP lifting for the chosen order are reused at
  raise.
* `LiteralConstraints` (IndexedFilter): include.
  Detection needs the index oracle and imperative MFP analysis, so it is seeded
  by running the production detector on candidate subtrees.
* `Demand` and `ProjectionPushdown`: include, for now.
  Column liveness is orthogonal to arrangement counting and off the critical
  path; subsuming them via an e-graph liveness analysis is future work, gated on
  whether projection choices turn out to affect arrangement-key alignment.

## Correctness and soundness

The realizer always emits a correct join for the chosen order, so a wrong cost
estimate selects a suboptimal plan, never an incorrect one.
With the order fixed in the e-graph the arrangement count is exact, so the
divergence risk between an estimate and the realized plan does not arise.
The equivalence and type-preservation guard at the live boundary
(`adopt_if_type_preserving`) is unchanged and remains the final safety net.

## Validation

* Corpus invariant: for each plan in the eqsat corpus and the SLT goldens, the
  eqsat plan's maintained-arrangement count is less than or equal to the
  production plan's count.
  This is the improvement claim, measured rather than asserted.
* Correctness: the SLT differential gate (eqsat on versus off) stays green.
* A microbenchmark records extraction and saturation time at the size cap, for
  tracking, not gating, since performance is deferred.

## Relationship to the in-flight change

The committed-at-raise delta-versus-differential decision
(`plan_join_min_arrangements`, the change in flight when this design was written)
is a stepping stone.
It proves the planner-as-realizer delegation and stops the memory regression
today.
This design subsumes it: once order and key are chosen in the e-graph by the
arrangement objective, the raise-time decision becomes a pure realization of the
already-chosen form.

## Open questions

* The encoding of join order and key as e-nodes, and which subset of orders
  saturation enumerates, is the central performance lever and is specified in the
  implementation plan, not here.
* Whether projection placement affects arrangement-key alignment enough to
  justify subsuming `Demand`/`ProjectionPushdown` is left to measurement.
