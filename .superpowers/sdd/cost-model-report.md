# Cost model report: two-axis (memory-primary) + tradeoff recommendation

## New `Cost` type

```rust
pub struct Cost {
    pub memory: Vec<f64>,  // arranged-collection size-degrees, sorted desc
    pub time:   Vec<f64>,  // work-term degrees, sorted desc
    pub nodes:  usize,
}
```

The old `degrees` field is renamed to `time`.
Entries ≤ EPS (1e-9) are dropped from both vecs before storage.

### Comparators

* `cmp_memory_first(&self, other)` — memory multiset lexicographic (largest first, missing = 0.0), break ties on time, then nodes.
  This is the **default**; `cmp` and `lt` delegate here.
* `cmp_time_first(&self, other)` — time first, then memory, then nodes.

## Memory-term rules per operator

| Operator     | Memory term(s)                                              |
|--------------|-------------------------------------------------------------|
| `Reduce`     | `size_degree(input)` (arranges input by group key)         |
| `Join`       | `binary_join_terms(inputs, equivalences)` — one term per pairwise intermediate, same as the time-axis terms for the best left-deep order |
| `WcoJoin`    | `size_degree(input_i)` for each input (leapfrog arranges every input) |
| All others   | No memory term; recurse into children only                  |

`ArrangeBy` and `TopK` are not `Rel` variants (they bail to opaque `LocalGet` leaves during lowering), so no memory terms are emitted for them.

## Triangle costs (proving WcoJoin dominates)

Triangle: R(a,b) ⋈ S(b,c) ⋈ T(c,a), all base relations at degree 1.0.

| Plan    | TIME            | MEMORY                  |
|---------|-----------------|-------------------------|
| Join    | [2.0, 1.5]      | [2.0, 1.5]              |
| WcoJoin | [1.5]           | [1.0, 1.0, 1.0]         |

WcoJoin is strictly better on both axes:
- Time max: 1.5 < 2.0
- Memory max: 1.0 < 2.0
- `cmp_memory_first`: memory [1.0,1.0,1.0] < [2.0,1.5] → WcoJoin Less.

## Recommendation logic

After saturation, the engine extracts both:
1. Memory-first plan (default, returned as `Outcome.plan`).
2. Time-first plan (via `eg.extract_with(root, model, false)`).

A `Recommendation` is set when:
- The two plans differ (by value equality on `Rel`).
- Time-first plan is strictly faster: `time_cost.cmp_time_first(mem_cost) == Less`.
- Time-first plan uses strictly more memory: `time_cost.cmp_memory_first(mem_cost) == Greater`.

**End-to-end reachability**: In the current rule set, for the triangle join, both orderings extract the same plan (WcoJoin dominates on both axes; no tradeoff exists). For the non-join cases, the plans are degenerate (all memory terms are empty), so both orderings agree. The recommendation is therefore not reachable end-to-end with the current rule set — there is no case in the existing corpus where one plan wins on time but loses on memory.

The unit test `recommendation_logic_direct` validates the predicate logic directly with synthetic `Cost` values, confirming the comparator and guard conditions work correctly.

An end-to-end tradeoff case would require a rule that produces a plan with lower time degree but higher memory degree (e.g. a WcoJoin variant that pre-sorts/caches intermediates). This is left as future work when richer rules are added.

## Test results

```
running 37 tests (unit) — all pass
running 1 test  (compare_real) — pass
running 13 tests (roundtrip) — all pass
running 1 test  (wcoj_decision) — pass
```

### wcoj_decision output

```
=== E-GRAPH DECISION ===
Top-level node: WcoJoin (AGM)
Cost time: [1.5]  memory: [1.0, 1.0, 1.0]  nodes: 4
```

WcoJoin is selected. Time ~1.5 (AGM bound), memory max 1.0 (each of 3 base relations arranged).

### compare_real harness counts

Old ordering (time-only): 3 wins / 4 losses / 13 ties / 0 skips.
New ordering (memory-primary): **3 wins / 4 losses / 13 ties / 0 skips**.

The counts did not change.
The `triangle_join` case still wins because eqsat produces 4 nodes vs real's 5 nodes at identical time and memory vectors.
The `four_cycle_join` still wins for the same reason (fewer nodes at equal cost).
The `join_with_reduce_branch` still wins (eqsat avoids a redundant time term).
The 4 losses are unchanged — they involve `map`/`project` interactions that the current rule set does not model well.

## Commit hash

See git log; committed as:
`transform-egraph: two-axis cost model (memory-primary, size-weighted) + tradeoff recommendation`

## Concerns

1. **Recommendation not reachable end-to-end.** With the current rule set there is no plan in the e-graph that wins on time but loses on memory; the recommendation is always `None`. The logic is unit-tested. An end-to-end tradeoff needs a rule that introduces a time-efficient but memory-heavy arrangement.

2. **WcoJoin triangle wins on both axes.** There is no tradeoff for the canonical example: WcoJoin is strictly cheaper on both memory and time. This is correct and expected — the AGM result is a win/win.

3. **Recommendation is top-level only.** The `optimize_node_with_alt` helper computes the time-first alternative only for Let-free fragments. Scoped/recursive fragments propagate `None` for the alternative. Extending recommendation to sub-scopes is future work.
