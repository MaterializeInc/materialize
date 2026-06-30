# WcoJoin experiment report (M3b)

## Setup

The experiment re-enables the `join_to_wcoj` rule in the e-graph (removed the
`# M1-disabled:` guard) and wires `Rel::WcoJoin` in `raise.rs` to emit a plain
`MirRelationExpr::join_scalars(...)` (identical to the `Rel::Join` arm), with a
comment explaining that delta wiring is deferred to M3b-deep.

The `no_disabled_rule_is_active` guard in `tests/roundtrip.rs` is updated to
drop `join_to_wcoj` from the banned list (it is now intentionally active); the
three column-arithmetic rules remain banned.

## Triangle join input

```
R(a,b) ⋈ S(b,c) ⋈ T(c,a)
```

Column layout: R=#0,#1  S=#2,#3  T=#4,#5.
Equivalences: `[[#0,#4],[#1,#2],[#3,#5]]` (a=a, b=b, c=c).
All three inputs are global Gets (transient ids), so both pipelines treat each
as an opaque leaf of size degree 1.

## E-graph decision

Top-level extracted node: **WcoJoin** (AGM).
Cost: `degrees=[1.5]  nodes=4`.

The e-graph lowers the triangle to `Rel::Join { inputs: [leaf0, leaf1, leaf2], .. }`,
adds it to the e-graph, saturates, and the `join_to_wcoj` rule fires to add
`Rel::WcoJoin` in the same e-class.
The cost model scores WcoJoin via the fractional-edge-cover LP:
the triangle hypergraph has one covering solution with weight 1/2 per edge,
giving AGM degree 1.5.
The binary-join terms are computed by the DP: the best intermediate is N^2.
The cost model prefers WcoJoin (degrees=[1.5]) over Join (degrees=[2.0, 1.0]).
Extraction picks WcoJoin.

## Materialize JoinImplementation decision

`JoinImplementation::transform` was run on the same triangle.
`JoinImplementation` required no prior normalization; it ran directly on the
bare `Join { implementation: Unimplemented }` and populated the implementation
field.

| `enable_eager_delta_joins` | Implementation chosen |
|---|----|
| `false` | **`Differential(...)`** |
| `true`  | **`Differential(...)`** |

No delta query was planned in either case.
The reason: a delta query requires an arrangement for every input on every
query path (except the starting input).
The bare triangle of global Gets has zero available arrangements.
With `eager_delta_joins=false`, the transform requires zero new arrangements
for delta to be picked — condition not met, so it falls back to Differential.
With `eager_delta_joins=true`, the transform picks delta only if
`delta_new_arrangements <= differential_new_arrangements`.
For three inputs: differential needs 1 new intermediate arrangement;
delta needs 2 new ArrangeBy nodes per path × 3 paths = 6 new arrangements.
6 > 1, so Differential wins even with eager on.

## Verdict

The decisions differ.

The e-graph's cardinality-free AGM cost model picks **WcoJoin (N^1.5)** for
the triangle join, correctly identifying it as cyclic and preferring the
worst-case-optimal strategy.
Materialize's `JoinImplementation` picks **Differential** in both flag settings,
because:
1. It has no AGM awareness — it does not model join cyclicity.
2. On a bare join with no available arrangements, delta is more expensive to
   bootstrap (6 new arrangements vs 1), so Differential wins on Materialize's
   own criterion.

This confirms that the e-graph's AGM model is adding genuine new information
that the downstream JoinImplementation does not currently capture.
The gap is the M3b-deep deliverable: wiring the WcoJoin extraction signal into
the physical planner so it synthesizes a delta-like plan without requiring
pre-existing arrangements.
