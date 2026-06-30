# Termination fix: eqsat optimizer non-termination on Union+Filter plans

## Root cause

The hang was NOT in `run_analysis` outer fixpoint as originally hypothesized.
Investigation via debug instrumentation revealed the actual location: **Phase 2b (DSL rule application) in `EGraph::saturate`**.

The e-graph exhibits exponential growth in pending bindings across saturation iterations:

| iter | n_nodes (after rebuild) | pending entries |
|------|------------------------|-----------------|
| 6    | 48                     | 1 052           |
| 7    | 82                     | 4 128           |
| 8    | 148                    | 16 420          |

`rebuild()` at the top of each iteration merges equivalent nodes via hash-consing, dramatically reducing the e-node count (e.g. 4181 → 82 between iters 6 and 7).
The existing `MAX_ENODES` guard fires **after** rebuild, so it sees the reduced count (148 << 600) and does not stop.
Phase 2b then processes all 16 420 pending bindings, each calling `instantiate()` which can add fresh e-nodes, growing the graph well past the budget before the next iteration's guard fires.
Next iteration: `run_analysis` with a huge e-graph takes extremely long (one `merge` call per e-node per analysis iteration = millions of calls).

## Loops bounded

### 1. Phase 2b mid-loop budget check (`egraph.rs`)

**Primary fix.** Added a `n_nodes > MAX_ENODES` check **inside** the Phase 2b `for` loop (after each `instantiate`+`union`).
When the count exceeds the budget, Phase 2b breaks early and exits to the next saturation iteration.
Soundness: the outer `MAX_ENODES` guard already provides this guarantee at iteration boundaries; the mid-loop check applies the identical logic within an iteration.
Skipped rewrites are conservatively omitted, the same semantics as the outer guard.

### 2. `run_analysis` iteration cap (`egraph.rs`, `MAX_ANALYSIS_ITERS = 100`)

Secondary safety net.
The three finite-height analyses (`NonNeg`, `Monotonic`, `Keys`) converge in a handful of rounds.
`Equivalences` operates over `Option<EquivalenceClasses>` which is not finite-height on Union+Filter plans and could oscillate.
Cap at 100 iterations; stop early with the current (partial) map.
Soundness: every value in the partial map was derived from real node structure, so it is a sound under-approximation.
Both consumers (canonicalization and `unsatisfiable`) are correct with fewer known equivalences.
100 is well above any finite-height analysis's convergence bound and provides a meaningful partial result for `Equivalences`.

### 3. `EquivalenceClasses::minimize` iteration cap (`equivalences.rs`)

Tertiary safety net for a separate potential non-termination.
`minimize` has an outer fixpoint loop (`while let Some(prev) = previous`) whose termination relies on the `expand()` step not adding endlessly novel equivalences.
For arbitrary `MirScalarExpr` inputs (e.g. from e-graph Union nodes), this is not formally guaranteed.
Capped at 100 outer-loop iterations with a `tracing::debug!` log on truncation.
Soundness: same as above -- fewer known equivalences, never incorrect ones.

## `compare_real` result

```
SUMMARY: 3 wins / 0 losses / 17 ties / 0 skips
```

Previously-hanging case (`filter_over_union_with_branch_filters`) now completes as TIE.
Test finished in 30.25 seconds total (20 cases).

## Test results

* `cargo check -p mz-transform --lib`: clean.
* `cargo test -p mz-transform --lib eqsat`: 44 passed, 0 failed.
* `timeout 300 cargo test -p mz-transform --test compare_real -- --nocapture`: TERMINATES, 1 passed, 0 failed, 30s.
* `cargo test -p mz-transform --test wcoj_decision`: 4 passed, 0 failed.

## Files modified

* `src/transform/src/eqsat/egraph.rs`: added `MAX_ANALYSIS_ITERS` constant, iteration cap in `run_analysis`, mid-loop budget guard in Phase 2b of `saturate`.
* `src/transform/src/analysis/equivalences.rs`: iteration cap in `EquivalenceClasses::minimize` outer loop.
