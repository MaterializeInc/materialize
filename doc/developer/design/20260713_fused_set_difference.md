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
- Self-difference, where `base` and `subtract` resolve to the same collection. Benign (the result is always empty, and traces support concurrent read cursors), so the recognizer need not special-case it, but the operator must not assume the two input handles are distinct.
- Changing when the two inputs become co-arranged. This design consumes co-arrangement where it already exists (`Reduce::Distinct` always advertises it; PR #37592 adds it for `MonotonicTop1`), it does not create it.

## Solution Proposal

Introduce a dedicated `SetDifference` LIR operator that reads two co-arranged inputs directly and produces one output arrangement, plus an LIR-layer transform that recognizes the co-arranged anti-side shape and rewrites it to the new operator.
Deliver in two phases so correctness and plumbing land before the hard operator.

### LIR node

New `LirRelationNode::SetDifference { base, subtract, key }`:

- `base`: the positive input (B), required available arranged by `key`.
- `subtract`: the negated input (A), required available arranged by `key`.
- `key`: the difference key, also the output arrangement key.

Semantics: group both inputs by the `key` columns.
For each key, sum the diffs over all values within that key for each input separately, then emit the `key` row with net multiplicity `count(base) - count(subtract)` when that net is positive.
The output row is the `key` columns with empty value (`thinning=()`), matching the current `Threshold` output.

This is exactly `Threshold(Union(Negate(subtract), base))` because the anti-side `Threshold` arranges by the entire row (`ThresholdPlan::create_from` keys on `0..arity`), so `key` spans the whole difference row and per-key accounting equals per-row accounting.
Crucially, the operator sums diffs over each input's values per key and never inspects value contents, so the two input arrangements may carry different `thinning` (e.g. in the LEFT JOIN anti-branch of `outer_join.slt`, `l0` is arranged `thinning=(#1)` while `l1` is `thinning=()`).
The current `Threshold` render already treats arrangement values as opaque and looks only at counts, so this matches existing behavior.
Wire the node into `children` / `children_mut`, into EXPLAIN rendering, and into `keys()` so it advertises its output arrangement on `key` (mirroring `ThresholdPlan::keys()`).

`EXPLAIN PHYSICAL PLAN` must render `SetDifference` as a first-class node with its `base`, `subtract`, and `key`.
Because EXPLAIN shows the LIR plan and not the rendered dataflow, the new operator surfaces as soon as the recognizer inserts it, in both phases.
This also means EXPLAIN output is identical across Phase 1 and Phase 2: the phase-1 render internals (reconstructed `Union` / `Negate` / `Threshold`) never appear in EXPLAIN, only the dataflow changes between phases.

### Recognizer

A new LIR-layer refinement pass (it needs physical arrangement availability, which only exists after lowering).
Wiring a feature-flag-gated refine pass is itself new plumbing: the existing refine passes in `plan.rs` are either unconditional or gated on `dataflow.is_single_time()`, none on an `OptimizerFeatures` flag, so threading the flag into the refine call site is its own subtask, not a drop-in alongside the existing passes.

Match: a `Threshold::Basic` on `key`, whose input is an `ArrangeBy(key, raw=false)` over a `Union` with exactly two inputs, one of which is a `Negate`.
The `consolidate_output=true` flag need not be checked separately, it is implied: lowering sets it whenever a `Union` arm is a `Negate`.
Both arms must satisfy: after stripping an optional pure projection-to-`key` MFP (projection only, no filter or map), the underlying node advertises an arrangement whose key columns equal `key`.
The check is against the full `(key columns, permutation, thinning)` triple of the advertised arrangement, but only the key columns must match `key`; `permutation` and `thinning` may differ between arms because the operator ignores values (see the node semantics).

Rewrite to `SetDifference { base: <positive arm>, subtract: <negated arm>, key }`, where each arm is the node under its (stripped) projection MFP.
The match is insensitive to which arm is negated.

Decline (leave the plan unchanged) when:
- either arm is not arranged on `key` columns,
- the MFP between an arm and its arrangement does anything beyond projecting to the `key` columns,
- either `Union` arm carries a non-default `temporal_bucketing_strategy` (the `SetDifference` node has no slot for it, so a matched non-default strategy would be silently dropped),
- an arm is a node type whose arrangement advertisement is not directly queryable. Only `Get` (via `keys`) and `ArrangeBy` (via `forms`) advertise `AvailableCollections` at the node level; `Reduce` / `TopK` / `Threshold` keep arrangement info in their own plan types and are reached here only wrapped in a `Get` or `ArrangeBy`, so the recognizer inspects `Get` and `ArrangeBy` arms only.

### Render

- Phase 1 (PR1): render `SetDifference` by reconstructing the equivalent `Union` / `Negate` / `ArrangeBy` / `Threshold` dataflow. Semantically identical, no performance change. This lands the node, recognizer, EXPLAIN output, goldens, and the flag end to end, and lets result-equivalence tests run against the recognizer before any operator risk.
- Phase 2 (PR2): replace the render body with a fused binary operator. It consumes the two input `oks` arrangements via `binary_frontier` (modeled on `mz_join_core`: dual `cursor_through`, per-input acknowledged frontiers, matched-key `seek_key` / `step_key` co-iteration) and emits per key the thresholded net diff into an output `RowRowSpine` (modeled on the output-spine construction in DD's `reduce_trace` and `mz_reduce_abelian`). Errors from both inputs are converted to collections, concatenated, and arranged once (the reduce error pattern). Output is returned as `CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))`. This removes trace T, the memory win, as a render-internal change.

### Central technical risk (Phase 2)

The fused operator is genuinely new machinery, not a thin composition, and the two cited references each solve only half of it.
`mz_join_core` consumes two arrangements but emits an unarranged stream, so it never had to define an output trace's compaction.
`reduce_trace` / `mz_reduce_abelian` build one output trace but react to a single input frontier via `unary_frontier`.
Two hazards live in the gap:

- Output-trace compaction against two upstream frontiers. The operator sits between two input traces that each compact by the other input's frontier (`mz_join_core` maintains `physical_compaction <= acknowledged` per input). The fused operator must define what its output trace's `since` means relative to both input acknowledged frontiers. Advancing it wrong lets a downstream reader request a `cursor_through` the operator can no longer answer.
- Per-key time-revisit. "Net positive" is evaluated as of specific timestamps, so the operator needs a reduce-style per-key interesting-times revisit list. A pure per-batch stream diff, which is all the join co-iteration gives, is not sufficient.

The implementation plan must treat this subsection as the core of Phase 2. Getting the input co-iteration and output-batch building right is mechanical given the templates; getting the two-frontier compaction and time-revisit right is the novel work and the likeliest source of a correctness bug.

### Phase 2 operator design (resolved by spike)

The operator is a `binary_frontier` timely operator that forks the output-trace machinery of differential's `reduce_trace` (all reusable `pub` surfaces: `TraceAgent::new`, builder `done`, `output_writer.insert`/`seal`, `set_{logical,physical}_compaction`, `advance_upper`, `cursor_through`) and the two-input consumption skeleton of `mz_join_core`. The target reduction is a per-key net across all values, `net(key) = sum_v (count_base(key,v) - count_subtract(key,v))`, emitting `(key, (), net)` when `net > 0`. The empty output value collapses differential's per-value `HistoryReplayer` to a per-key time-indexed running count.

Output-trace compaction. Define the combined processed frontier `P = meet(upper_base, upper_subtract)`, where each `upper_i` is the antichain-join of that input's received batch uppers, `trace_i.advance_upper`, and the input frontier (exactly as `reduce_trace` computes `upper_limit` for its single input). Set logical and physical compaction of both input traces and the output trace to `P`, and `seal(P)`. `P` is the strongest frontier the output can be final on: a net at `t` cannot be finalized until both inputs are done at `t`. This keeps a downstream `cursor_through` always answerable (output physical is `P`, we sealed `P`) and never compacts away input history still needed (each round reads both inputs with `cursor_through(prev P)`, and physical was set to `prev P` last round). It satisfies the join's `physical <= acknowledged` invariant since `P <= upper_i`.

Both inputs must be pinned to the same `P`, unlike the join which reads each trace through its own acknowledged frontier. The net at a time depends on both inputs, so reading them at different frontiers would miscount in the gap. This is the load-bearing divergence from `mz_join_core`.

Time-revisit. Mirror `reduce_trace`'s persisted `pending_keys` / `pending_time` bookkeeping and a `CapabilitySet`, driven from the `binary_frontier`. Each activation: (1) collect dirty keys as the union of keys touched in either input's incoming batches merged with pending keys (the join processes the intersection, which would miss keys present in only one input and their retractions), (2) retire `[prev P, P)`, (3) per dirty key, merge a time-ordered delta stream from both inputs (base contributes `+diff`, subtract contributes `-diff`), walk times ascending maintaining the running net, evaluate `net.is_positive()` at each interesting time, diff against previously-produced output to emit retractions, (4) generate synthetic interesting times `t_base.join(t_subtract)` for changes that occur where neither input has a literal update, carrying out-of-window ones forward in `pending_time`, (5) downgrade the `CapabilitySet` to the frontier of remaining `pending_time`.

Correctness hazards the implementation must handle: keys present in only one input (union of dirty keys, not intersection); a net falling to zero needing a retraction of a previously-emitted row (falls out of the output diffing, and the key is dirty because its last update touches a batch); synthetic times (the most delicate reimplementation); premature capability release (retain via `CapabilitySet`, downgrade only to `frontier(pending_time)`); self-difference aliasing the same trace (acquire the two cursors in sequence, never hold both `RefCell` borrows, as `mz_join_core` does); empty-region stalls (call `advance_upper` on both traces before computing `P`). Frontier divergence between the two inputs pins compaction low and retains history, inherent and identical to the shipping join's retention hazard, not a blocker.

Verification. Test the operator against `Threshold(ArrangeBy(Union(Negate(subtract), base)))` as a differential oracle under interleaved and diverging input frontiers, plus the memory-reduction introspection assertion (trace T gone) and the Feature Benchmark A/B.

### Gating

A feature flag `enable_fused_set_difference`, default off in production, enabled in CI and test configuration (per the project convention for new optimizer flags).
The recognizer runs only when the flag is set.

## Minimal Viable Prototype

The measurement in "The Problem" is the prototype: it quantifies the win on a real maintained dataflow via `mz_arrangement_sizes` and `mz_scheduling_elapsed`, and PR #37595 adds a Feature Benchmark scenario (`SetDifferenceCoArranged`) that will measure the win as an A/B once Phase 2 lands.
Phase 1 itself is a plan-only prototype: it validates that the recognizer fires on the intended shapes (`EXPLAIN PHYSICAL PLAN`) and preserves results, with zero runtime risk.

Required test cases, chosen to hit the risky shapes surfaced in review:

- The LEFT JOIN anti-branch in `outer_join.slt`, where the two arms have heterogeneous `thinning` (`l0` `thinning=(#1)`, `l1` `thinning=()`) and a projection MFP sits between `l0` and its arrangement. This is the case most likely to expose a recognizer or value-handling bug and it already exists in-tree.
- `EXCEPT` over two arms arranged on the key.
- A negative case: an arm whose MFP does more than project to `key` (a filter or map), which the recognizer must decline to match.
- Result-equivalence for all of the above with the flag on vs off (Phase 1 already exercises this before the operator exists).

The 40% / 51% memory figures are a pre-implementation estimate from the single measured workload.
The fused operator carries its own per-key interesting-times state (see the Phase 2 risk), so Phase 2 must re-measure via the Feature Benchmark scenario rather than assume the estimate is a floor.

## Alternatives

- Emit an unarranged stream from the co-iteration and re-arrange it with `mz_arrange`. Rejected: the re-arrange is trace T, so this captures no memory.
- Keep the `Threshold` reduce but drop the redundant `consolidate_output`. This is real but CPU-only (about 1.4 s) and captures no memory, so it does not address the problem. Tracked separately.
- Reuse DD's single-input `reduce`. Rejected: `reduce_core` is strictly `unary_frontier`, so it cannot read two arrangements. The only prior art for consuming two arrangements is the join core, which emits a stream rather than a trace, hence the hand-written operator.
- One-shot delivery (node, recognizer, and fused operator in one PR). Rejected in favor of phasing: it entangles the hard timely operator with the plumbing, making review and bisection harder.

## Open questions

- Exact flag name and where its default is registered.
- The precise definition of the output trace's `since` relative to the two input acknowledged frontiers (the core Phase 2 risk). This must be resolved during Phase 2 design, before implementation, not left to the implementer.

Resolved during review:

- Output `thinning` is `()` (key-only value), matching the current `Threshold` output. Hard requirement for a transparent rewrite, not an option.
- Input arrangements may have any `thinning`; the operator sums over values per key and ignores value contents, so heterogeneous input thinning is fine.
- Phase 1's render internals (reuse the `Threshold` render path vs. inline the equivalent operators) are invisible to EXPLAIN and to consumers, so it is a free choice. Leaning toward reusing the `Threshold` render path to minimize new code.
