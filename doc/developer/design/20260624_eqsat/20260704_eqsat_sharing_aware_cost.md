# Sharing-aware cost for the eqsat MIR optimizer (WS0 + WS1)

Status: design, pending implementation. Scope decision: build WS0 + WS1 now,
design WS2 and defer its implementation to a follow-up spec.

## Motivation

Two forms of database-issues#2409 ("common sub-sub-expression elimination")
reduce to one missing lever: the cost model cannot value a computation done
once and reused across consumers.

* Relational filter sharing. `SELECT * FROM r WHERE a>0 UNION ALL SELECT * FROM
  r WHERE a>0 AND b>0`. The optimum shares the filtered collection
  `Filter[a>0](r)`: consumer 1 uses it directly, consumer 2 applies `b>0` on
  top. The directional optimizer canonicalizes filters toward fused
  (`FuseAndCollapse`), so consumer 2 becomes a single `Filter[a AND b]` stage
  that whole-stage RelationCSE cannot match against consumer 1's `Filter[a]`.
  This is a fuse-versus-split non-confluence. Holding both alternatives and
  cost-picking the shared split is what an e-graph is built for.
* Scalar sharing. A scalar `f(x)` computed in two places. Computing it once
  (materialize as a column via an upstream `Map`, reference the column
  downstream) is always weakly better on execution and strictly better for a
  non-trivial scalar, because `Map` is a streaming operator that adds no
  arrangement.

This document specifies the relational case (WS1) plus the extraction
foundation it needs (WS0), and sketches the scalar case (WS2) for a later
spec.

## The precise gap (why WS0 is not a no-op)

The ILP extractor (`extract.rs`) is already DAG-aware: its objective charges
the time and node tiers per selected node (`node_sel`, extract.rs:277-278), so
a subtree selected once is charged once regardless of how many parents
reference it. It is tempting to conclude that WS1 needs only the split rewrite
plus running under the ILP. It does not.

On the motivating query the ILP objective ties the fused and split forms:

* Fused consumer 2 is `Filter[a,b](r)`: incremental nodes `{Filter_ab, Get_r}`,
  time term `deg(r)`.
* Split consumer 2 is `Filter[b](Filter[a](r))` with `Filter[a](r)` shared from
  consumer 1: incremental node `{Filter_b}`, time term `deg(Filter_a(r))`.

A filter does not reduce worst-case cardinality degree, so
`deg(Filter_a(r)) = deg(r)`. The time axis is asymptotic and both plans are
`O(N)`, so they tie on time. The ILP node tier charges a flat `w_nodes` per
selected node and ignores scalar payload, so `Filter_ab` (two predicates) and
`Filter_b` (one predicate) count the same and the node tier ties too.
Arrangements are zero for both (filters do not arrange). All three real tiers
tie in the ILP.

Note that `cost.rs` would not tie: its node count is `node_count +
scalar_node_count` (cost.rs:370), so the fused `Filter_ab` costs more than the
split `Filter_b`. The ILP objective simply does not mirror that scalar
awareness. The genuine win, evaluating `a` on `|r|` rows once instead of on
`2*|r|` rows across the two consumers, is exactly the constant factor the
degree-based time axis ignores, and it surfaces in the scalar node count.
Relying on the solver's arbitrary resolution of the tie would be
non-deterministic. WS1 therefore needs three pieces: the split rewrite, the ILP
route (already the default), and a scalar-aware ILP node tier that strictly
prefers the split when the other tiers tie.

## Current-state anchors (verified file:line, `src/transform/src/eqsat/`)

* Cost axes: `arrangements`, `memory: Vec<f64>`, `time: Vec<f64>`, `nodes`
  (cost.rs:85-107). Memory dedups arrangements by `ArrId` via `seen`
  (cost.rs:429-553). Filter and Map fall in `_ => {}` (cost.rs:546-547): no
  memory term, `time` only. Memory is cardinality-based `size_degree`,
  width-blind.
* Extractors: `GreedyExtractor` is the default (engine.rs:97). The ILP path is
  selected by `use_ilp = ctx.features.enable_eqsat_ilp_extraction`
  (transform.rs:167), passed into `optimize_with_availability`.
* ILP objective (extract.rs:241-278): PRIMARY distinct `(class,key)`
  arrangement count, then TIME `w_time * node_work[vi]` per selected node, then
  NODES `w_nodes` per selected node. DAG-aware for time and nodes.
* Relational CSE (cse.rs:6-14) runs after extraction and binds every subtree
  occurring at least twice with a `Rel::Let`. Relational only, no scalar Let.
  It cannot create sharing the extractor did not leave in the tree.
* Filter fusion: `merge_filters` fuses `Filter[p](Filter[q] r) =>
  Filter[concat(q,p)] r` (relational.rewrite:32-37). No split rule exists. The
  DSL note (relational.rewrite:18-20) states scalar payloads (predicates, map
  expressions, projections, equivalences) are moved as opaque whole lists,
  never destructured, tied to per-rule Lean theorem generation.
* Builtins are scalar today (`scalar_builtins.rs`). A relational,
  predicate-list-destructuring builtin is a new category.
* Scalars are first-class finely decomposed e-nodes but extraction re-expands
  them inline as `MirScalarExpr` (`scalar_extract.rs::build`/`reconstruct`,
  scalar_extract.rs:110-135). No output shape materializes a shared scalar as a
  column.

## Architecture

```
saturation:  filter-split builtin adds Filter[b](Filter[a](r)) into c2's class   (WS1 rewrite)
                    | gated: sharing flag ON, else rule inert (corpus byte-identical)
extraction:  ILP extractor + scalar-aware node tier selects the split node       (WS0)
                    | gated: scalar-aware weighting behind the sharing flag; ILP is default-on
build_rel:   emits a tree where Filter[a](r) appears in both c1 and c2 branches
cse.rs:      binds the duplicated Filter[a](r) subtree to a Rel::Let             (UNCHANGED)
```

A single feature flag `enable_eqsat_filter_sharing` (default off) enables both
the split rule during saturation and the scalar-aware node tier at extraction.
With the flag off, the split builtin never fires, the node tier is the flat
`w_nodes` of today, and every unflagged plan is byte-identical.

`enable_eqsat_ilp_extraction` defaults to ON, so the ILP is already the steady
state and the sharing flag does not need to switch extractors in the common
case. The flag still routes `use_ilp = enable_eqsat_ilp_extraction ||
enable_eqsat_filter_sharing`, which matters only in the edge case where an
operator has turned the ILP off as a safeguard: sharing requires the ILP (the
greedy extractor is DAG-blind), so the sharing flag forces it back on for that
run. The byte-identical invariant is preserved by gating both the split rule
and the scalar-aware node tier behind the sharing flag, not by the extractor
default.

## WS0: align the ILP node tier with the scalar-aware node count (extract.rs)

The fix is not a bespoke reuse reward. It is aligning the ILP node tier with
the node count `cost.rs` already uses. `cost.rs` counts `nodes = node_count +
scalar_node_count` (cost.rs:370), so a node carrying more scalar payload costs
more. The ILP node tier charges a flat `w_nodes` per selected node
(extract.rs:278) and ignores scalar payload. That flat charge is exactly why
the fused and split forms tie in the ILP but would not tie under `cost.rs`. WS0
makes the ILP node tier scalar-aware.

Term. Weight each selected node in the node tier by its direct scalar-payload
node count, mirroring cost.rs:370. Per selected relational node the node tier
becomes proportional to `1 + direct_scalar_node_count(node)` rather than a flat
`1`. `direct_scalar_node_count` is the size of the scalar subtrees the node
carries directly (its predicates, map expressions, or keys), not the recursive
relational subtree, so summing per selected node over the DAG reconstructs the
tree scalar count with each shared relational node counted once.

Worked on the acceptance query, DAG-counted per selected node:

* Fused: `Filter[a]` carries `a` (size `|a|`), `Filter[a,b]` carries `a` and
  `b` (size `|a| + |b|`). Node-tier scalar mass `= 2|a| + |b|`.
* Split: `Filter[a]` carries `a` (size `|a|`), `Filter[b]` carries `b` (size
  `|b|`). Node-tier scalar mass `= |a| + |b|`.

Split is strictly smaller by `|a|`, so the ILP picks the split deterministically.
The earlier draft rewarded reuse per shared input class, which is backwards: the
fused form shares `Get_r` under two filters and would collect more reward. The
scalar-aware node count is the correct and principled discriminator.

Rank. This modifies the existing node tier, which is already the lowest tier,
below arrangements and time. It only discriminates when arrangements and time
tie. The tier-domination scaling (extract.rs:251-261) must be recomputed: the
`w_nodes` denominator currently scales by node count, and it must instead scale
by total node-tier mass including scalar counts, so a genuine time difference
still strictly dominates any node-tier difference.

Flag gating (required, not optional). `enable_eqsat_ilp_extraction` defaults to
ON (definitions.rs, the ILP is the intended steady state). The ILP objective
therefore runs for the whole corpus, so an unconditional scalar-aware node tier
would change ILP tie resolution corpus-wide and break the byte-identical
invariant. The scalar-aware weighting is gated behind
`enable_eqsat_filter_sharing`. With the flag off the node tier is the flat
`w_nodes` of today and every plan is byte-identical. This differs from the
initial assumption that the ILP was off by default. It is not, so the gating is
what preserves the invariant.

Location. The change is in the ILP objective in `extract.rs`, never in
`cost.rs`. `cost.rs` stays untouched in WS0 and WS1. This keeps the greedy
extractor and every non-flagged plan byte-identical.

Soundness boundary. Preferring the smaller-scalar-mass form is unconditionally
sound for filters only because they stream and are memory-free: computing the
shared filter once is always weakly better, with no carry cost. This does not
hold for WS2, where a scalar shared across an arrangement boundary costs memory
(a wider arranged row). The node-tier weighting is scoped to the memory-free
filter case and must not be reused for WS2 without a width charge. Code comments
must state this boundary so WS2 does not inherit an unsound default.

Relation to WS2. This is the honest special case of the WS2 predicate-work
term. WS2 charges actual predicate and scalar evaluation work (predicate-list
length times input degree) in `cost.rs`, which makes the split win outright
rather than by node-tier alignment, and which the expensive-scalar case needs.
That belongs in `cost.rs` and carries corpus churn, so it is WS2 scope. WS0 is
the same shape restricted to the ILP node tier and the memory-free case, so WS2
subsumes it rather than fights it.

## WS1: the filter-split builtin

Mechanism. A relational builtin applier, not a DSL rule, because the split
must destructure the predicate list, which the DSL deliberately forbids. The
builtin peels one predicate outward per fire:

```
Filter[S](r)  =>  Filter[{p}](Filter[S \ {p}](r))     for a single p in S
```

Through saturation, repeated single peels expose every subset `Filter[T](r)`
for `T` a subset of `S`, so a consumer whose predicate set is any subset of a
larger consumer's set finds its filtered collection as a shared sub-node. One
peel already suffices for the two-predicate acceptance case:
`Filter[a AND b](r) => Filter[b](Filter[a](r))`.

The split cannot be avoided by disabling `merge_filters`. HIR lowers `WHERE a
AND b` to a single predicate `Vec`, so `Filter[a]` never exists as a sub-node
without a peel.

Blow-up bound (the crux). Split reverses `merge_filters`, so with both present
the e-graph explores nested filter forms. Unbounded, an n-predicate conjunction
yields exponentially many subset nestings and factorially many orderings. The
builtin is bounded on three axes:

1. Single-predicate peel only, never an arbitrary subset split in one fire.
   Each fire moves exactly one predicate outward.
2. Predicate-list length cap `K`: the rule is inert for `|S| > K`, so at most
   `2^K` distinct inner subset-nodes per filter. The acceptance query has
   `|S| = 2`; `K` is set small (recommended 3 to 4) and is itself a tuning
   knob. Filters wider than `K` do not share, which is acceptable: wide
   expensive shared filters are rare and the value is marginal in the common
   shape.
3. Canonical predicate ordering on emitted peels, so `Filter[T]` for a given
   subset `T` is one canonical node rather than one node per ordering. Without
   this, ordering variants multiply the subset count past `2^K`. This assumes a
   canonical predicate order exists that the builtin can emit into. That has not
   been confirmed by recon and is a plan checkpoint, not an assumption: the plan
   verifies whether the engine already canonicalizes Filter predicate order
   (and merge_filters uses `concat`, which preserves order, so it likely does
   not canonicalize). If no canonical order exists, the builtin must impose one
   (sort the predicate list by a stable key before emitting), or the `2^K`
   bound degrades to include orderings and must be re-derived. The `K` cap
   still bounds it, but the plan must resolve this before relying on the `2^K`
   figure.

When the builtin declines a filter because `|S| > K`, it records that it
declined (a debug log or a counter), so a "did not share" outcome is
distinguishable from "the cap was hit." Silent truncation would read as full
coverage when it is not.

Determinism. The applier is try-all/union-all: for a matched `Filter[S](r)` it
adds every single-predicate peel and unions each into the class, never picking
one peel via `find_map`. This follows the established determinism rule for
builtin appliers (a `find_map` pick that can fail while another fires is
nondeterministic under hash-order).

Lean. The split is the exact inverse of `merge_filters`, whose theorem
`rule_merge_filters` is discharged, not sorried (Generated.lean:16-18): it
proves `filterB p (filterB q r) = filterB (predAnd q p) r` by casing on both
predicate truth values, so it already covers the predicate reordering the
composition performs. The split theorem
`filterB (predAnd q p) r = filterB p (filterB q r)` is the symmetric statement
and is dischargeable by the identical tactic. Reference that shared content, do
not claim a fresh trivial proof.

The subtlety is error ordering, and it is handled outside Lean.
`Filter[b](Filter[a](r))` evaluates `b` only on rows where `a` held, while
`Filter[a,b](r)` may evaluate `b` on rows where `a` is false. Under strict
error semantics a `b`-error on an `a`-false row would differ. The Lean bag model
uses total `Row -> Bool` predicates (Semantics.lean), so it does not model
errors at all, which is why `rule_merge_filters` discharges cleanly. The
error-ordering equivalence is justified separately by Materialize's
error-as-data envelope (predicate evaluation order is free within the permitted
behaviors), the same justification the existing filter-movement rules rely on.
The doc states this explicitly rather than folding it into a "trivial" claim.

Codegen. `gen-lean` emits one theorem per DSL rule and existing builtins are
scalar, so a relational builtin-driven rule may be a new theorem category. If
`gen-lean` cannot emit the symmetric theorem for a relational builtin,
hand-author it (it is the one-line twin of `rule_merge_filters`) rather than
extending codegen for a single rule. The implementation confirms which path
`gen-lean` supports and records it, and verifies `rule_merge_filters` is still
discharged so the reference holds.

Pipeline completion. Once the ILP with the scalar-aware node tier selects
`Filter[b](Filter[a](r))` for consumer 2, `build_rel` reconstructs a tree in
which `Filter[a](r)` appears in both the consumer 1 branch and inside consumer
2. `cse.rs` then detects the duplicated subtree and binds it to a `Rel::Let`.
No change to `cse.rs` is needed: it already binds every subtree occurring at
least twice.

## WS2: scalar compute-once (designed, deferred)

WS2 is sketched here and deferred to a follow-up spec. It needs three pieces
WS1 does not:

* A factoring extractor output shape that materializes a shared scalar as an
  upstream `Map` column at the common ancestor, with downstream operators
  referencing the column. Scalars are inlined today
  (scalar_extract.rs:110-135), so this is a new output shape in
  `scalar_extract.rs` and `raise.rs`.
* A width dimension in `cost.rs` charging a column carried across a `Join` or
  `Reduce` boundary. The memory axis is width-blind, so a naive sharing credit
  would treat scalar materialization as pure win and over-share. Compute-once
  is credited by default, carry is charged only where the column crosses an
  arrangement boundary. The width charge and the predicate-work generalization
  of the WS0 tie-break are the same `cost.rs` investment.
* Reconciliation with `Demand` and `ProjectionPushdown`, which would drop a
  materialized shared column unless it is demanded. The sharing decision must
  be marked demanded or made where those passes will not undo it.

Acceptance for WS2 (later): an expensive shared scalar is computed once. A
cheap scalar across a large arrangement is not over-shared, the width charge
prevents it. No corpus regression.

## Testing and validation

* Acceptance (WS1): the UNION-ALL query shares `Filter[a>0](r)` under
  `enable_eqsat_filter_sharing`, and does not without it. EXPLAIN OPTIMIZED
  PLAN, flags explicit.
* Corpus no-regression with the flag off: full sqllogictest and the eqsat
  corpus must be byte-identical to pre-change. This is the primary safety gate
  and follows directly from the flag gating both the rule and the extractor
  route.
* Re-certification with the flag on for the flagged tests, with golden churn
  budgeted only there.
* Unit tests: the scalar-aware ILP node tier (a constructed fused-versus-split
  tie resolves to split because the fused node carries more scalar payload). Also
  the split builtin (single peel, cap `K` inert above the cap, canonical
  ordering, try-all determinism, declined-count recorded above the cap).
* Use the `mz-test` skill for canonical commands.

## Risks

* Solver scaling. The ILP runs for flagged cases; large flagged plans stress
  `microlp`. The greedy fallback on solver failure (extract.rs) stays sound but
  loses the share. Bound flagged usage and watch latency.
* Saturation loop. Split reverses `merge_filters`. Bounded by the length cap
  and canonical ordering. Verify saturation terminates within the existing node
  budget for wide predicate lists at the cap.
* Relational-builtin Lean category. Resolve during implementation, hand-author
  the one theorem if needed.
* Value is marginal in the source-pushable shape. Source-filter pushdown and
  collection fan-out already share the scan and the common filter, so the
  relational win is real only when the shared filter is expensive and not
  pushable. Design for decision-correctness (share when it wins, tie-break only
  on real ties), not a headline number.

## Scope boundaries

* IN: the scalar-aware ILP node tier (WS0), the filter-split builtin and its
  flag (WS1).
* OUT this spec: scalar compute-once (WS2, follow-up), the `cost.rs` width and
  predicate-work dimensions (WS2), arrangement sharing (already handled by the
  `ArrId` dedup and render dedup), HIR-layer rewrites, sub-join sharing (joins
  are flat n-ary), cardinality estimation.

## Decisions recorded

1. Filter-split via a scoped relational builtin escape hatch, not a DSL
   destructuring extension. Bounded by single-peel, length cap `K`, canonical
   ordering, try-all determinism.
2. Sharing runs under the ILP (default on). The byte-identical invariant is
   preserved by gating both the split rule and the scalar-aware node tier behind
   one flag `enable_eqsat_filter_sharing`, not by the extractor default.
3. WS0 aligns the ILP node tier with cost.rs's scalar-aware node count
   (`node_count + scalar_node_count`), weighting each selected node by its
   direct scalar payload. It lives in the ILP objective, stays the lowest tier,
   and is gated behind `enable_eqsat_filter_sharing` because the ILP is
   default-on and an unconditional change would churn the corpus. It is scoped
   to the memory-free filter case. WS2 ships separately behind the `cost.rs`
   width charge and predicate-work term.
4. WS2 scalar sharing, its width cost dimension, and Demand coordination are
   deferred to a follow-up spec.
