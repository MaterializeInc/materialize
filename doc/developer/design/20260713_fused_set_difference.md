# Fused set-difference operator over co-arranged inputs

- Associated: [CLU-168](https://linear.app/materializeinc/issue/CLU-168), PR #37595 (measurement harness), PR #37592 (surfaced the co-arrangement)

## The Problem

Outer-join decorrelation and `EXCEPT` both render the "rows of B absent from A" side as a set difference on the join key: `Threshold(Union(Negate(A), B))`.
When A and B are already arranged on that key, rendering still inserts an intermediate `ArrangeBy` (call it trace T) that feeds the `Threshold`, plus a `UnionConsolidation` over the concatenated inputs.

A `Threshold` is a reduce.
It needs an arranged input (trace T) and produces an arranged output.
So the anti-side holds two full arrangements for one logical step, on top of the two input arrangements that already exist.

Measured on a maintained dataflow (`SELECT k FROM a EXCEPT SELECT k FROM b`, a=1M keys, b=500K overlapping), per `mz_arrangement_sizes`:

| operator | records | size |
|---|---|---|
| `ArrangeBy[[Column(0)]]` (trace T) | 1.5M | 25.8 MB |
| `Threshold local` (output arrangement) | 1.5M | 24.8 MB |
| `Arranged DistinctBy` (a) | 1M | 4.9 MB |
| `DistinctBy` (a) | 1M | 3.9 MB |
| `Arranged DistinctBy` (b) | 500K | 2.5 MB |
| `DistinctBy` (b) | 500K | 2.0 MB |

Trace T is 25.8 MB of the dataflow's 64.2 MB of arrangement memory, about 40%.
Against the anti-side's own footprint (T plus output, 50.6 MB, with the input arrangements shared by other consumers in the outer-join case) trace T is about 51%.
CPU attributable to the eliminable operators (`mz_scheduling_elapsed`): intermediate `ArrangeBy` 1612 ms plus `UnionConsolidation` 1411 ms, roughly 3.0 s.

## Success Criteria

- A maintained anti-side dataflow over co-arranged inputs holds one fewer arrangement, cutting arrangement memory by the trace-T share (about 40% on the workload above).
- Query results are unchanged for outer joins, `EXCEPT`, and any other source of the recognized shape.
- The optimizer only rewrites when both difference inputs are genuinely arranged on the difference key, and is a safe no-op otherwise.
- The `SetDifference` operator reproduces `Threshold(Union(Negate(A), B))` semantics exactly, including residual positive multiplicity per key, so it is sound for arbitrary multisets, not only distinct sets.

## Out of Scope

- The redundant `consolidate_output=true` on the anti-side `Union` (set only because of the `Negate` child, but the downstream reduce re-consolidates by key). Dropping it is a separate CPU-only change that captures no memory.
- Unions with more than one negated arm or more than two inputs.
- Non-anti-side `Union` shapes.
- Changing when the two inputs become co-arranged. This design consumes co-arrangement where it already exists (`Reduce::Distinct` always advertises it; PR #37592 adds it for `MonotonicTop1`), it does not create it.

## Solution Proposal

Introduce a dedicated `SetDifference` LIR operator that reads two co-arranged inputs directly and produces one output arrangement, plus an LIR-layer transform that recognizes the co-arranged anti-side shape and rewrites it to the new operator.
Deliver in two phases so correctness and plumbing land before the hard operator.

### LIR node

New `LirRelationNode::SetDifference { base, subtract, key }`:

- `base`: the positive input (B), required available arranged by `key`.
- `subtract`: the negated input (A), required available arranged by `key`.
- `key`: the difference key, also the output arrangement key.

Semantics: for each key, emit the records whose net multiplicity `count(base) - count(subtract)` is positive, with that multiplicity.
This is exactly `Threshold(Union(Negate(subtract), base))`.
Wire the node into `children` / `children_mut`, into EXPLAIN rendering, and into `keys()` so it advertises its output arrangement on `key` (mirroring `ThresholdPlan::keys()`).

### Recognizer

A new LIR-layer refinement pass, run under a feature flag alongside the existing single-time refinements in `plan.rs` (it needs physical arrangement availability, which only exists after lowering).

Match: a `Threshold::Basic` on `key`, whose input is an `ArrangeBy(key, raw=false)` over a `Union` with `consolidate_output=true` and exactly two inputs, one of which is a `Negate`, where both the negated input and the positive input expose an arrangement on `key`.
Rewrite to `SetDifference { base: <positive arm>, subtract: <negated arm>, key }`.
The match is insensitive to which arm is negated.
If either input is not arranged on `key`, leave the plan unchanged.

### Render

- Phase 1 (PR1): render `SetDifference` by reconstructing the equivalent `Union` / `Negate` / `ArrangeBy` / `Threshold` dataflow. Semantically identical, no performance change. This lands the node, recognizer, EXPLAIN output, goldens, and the flag end to end, and lets result-equivalence tests run against the recognizer before any operator risk.
- Phase 2 (PR2): replace the render body with a fused binary operator. It consumes the two input `oks` arrangements via `binary_frontier` (modeled on `mz_join_core`: dual `cursor_through`, per-input acknowledged frontiers, matched-key `seek_key` / `step_key` co-iteration) and emits per key the thresholded net diff into an output `RowRowSpine` (modeled on the output-spine construction in DD's `reduce_trace` and `mz_reduce_abelian`). Errors from both inputs are converted to collections, concatenated, and arranged once (the reduce error pattern). Output is returned as `CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))`. This removes trace T, the memory win, as a render-internal change.

### Gating

A feature flag `enable_fused_set_difference`, default off in production, enabled in CI and test configuration (per the project convention for new optimizer flags).
The recognizer runs only when the flag is set.

## Minimal Viable Prototype

The measurement in "The Problem" is the prototype: it quantifies the win on a real maintained dataflow via `mz_arrangement_sizes` and `mz_scheduling_elapsed`, and PR #37595 adds a Feature Benchmark scenario (`SetDifferenceCoArranged`) that will measure the win as an A/B once Phase 2 lands.
Phase 1 itself is a plan-only prototype: it validates that the recognizer fires on the intended shapes (`EXPLAIN PHYSICAL PLAN`) and preserves results, with zero runtime risk.

## Alternatives

- Emit an unarranged stream from the co-iteration and re-arrange it with `mz_arrange`. Rejected: the re-arrange is trace T, so this captures no memory.
- Keep the `Threshold` reduce but drop the redundant `consolidate_output`. This is real but CPU-only (about 1.4 s) and captures no memory, so it does not address the problem. Tracked separately.
- Reuse DD's single-input `reduce`. Rejected: `reduce_core` is strictly `unary_frontier`, so it cannot read two arrangements. The only prior art for consuming two arrangements is the join core, which emits a stream rather than a trace, hence the hand-written operator.
- One-shot delivery (node, recognizer, and fused operator in one PR). Rejected in favor of phasing: it entangles the hard timely operator with the plumbing, making review and bisection harder.

## Open questions

- Exact flag name and where its default is registered.
- Whether the output arrangement's `thinning` should match the current `Threshold` output exactly (key-only value) so downstream consumers see an identical arrangement, which they must for the rewrite to be transparent.
- Whether Phase 1 should reconstruct the dataflow via the existing `Threshold` render path or inline the equivalent operators, whichever keeps EXPLAIN output stable across the two phases.
