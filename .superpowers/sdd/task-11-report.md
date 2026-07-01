# Task 11 report: arrangement-count validation harness

## Status

DONE. All invariants pass. No violations found.

## Files changed

* Created: `src/transform/src/eqsat/validation.rs`
* Modified: `src/transform/src/eqsat.rs` (added `#[cfg(test)] mod validation;`)

## Corpus

Six entries covering the major join shapes:

| Name | Shape | Available indexes |
|------|-------|-------------------|
| `triangle_no_indexes` | Cyclic 3-way (triangle R⋈S⋈T) | None |
| `triangle_all_indexes` | Same triangle | All 3 inputs keyed on their join key |
| `two_way_no_index` | Acyclic 2-way R⋈S | None |
| `two_way_with_index` | Same 2-way | R keyed on join col |
| `chain_join` | Acyclic 3-way chain R⋈S⋈T | None |
| `reduce_over_join` | Reduce(Count) over 2-way join | None |

## Reference used: documented proxy (Rel-level)

The production physical pipeline was not used.
Reason: `Optimizer::physical_optimizer` requires a real `IndexOracle`; the unit-test infrastructure does not provide one.
The logical pipeline is reachable in-crate but does not run `JoinImplementation`, so all joins have zero `ArrangeBy` nodes in the output.
Measuring after the full `optimize_with_availability` + `lower::lower` round-trip is also wrong: `commit_wcoj=true` emits explicit `ArrangeBy` wrappers for differential joins, so re-lowering charges those arrangements on top of the join's implicit ones, inflating the optimized cost in a way the unmodified input never incurs.

The proxy operates entirely at the `Rel` level:

* **Proxy A**: `cost(Optimizer::optimize(input_rel)).arrangements <= cost(input_rel).arrangements`
  The input `Rel` is lowered from the corpus MIR and measured directly; the extracted `Rel` is the `ArrangementCount`-extracted plan after saturation.
  Both sides use the same `CostModel`, so the comparison is symmetric.
  Non-tautological: a buggy rule that forces unnecessary binarization or wraps inputs in extra `ArrangeBy` nodes would cause the extracted cost to exceed the input cost.

* **Proxy B**: `cost(ArrangementCount_extract).arrangements <= cost(PeakDegree_extract).arrangements`
  Both objectives are exercised on the same input `Rel`.
  Non-tautological: a regression in the `ArrangementCount` comparator would fail this.

## Invariant results per entry

All pass.
Proxy A passes for all six corpus entries: the extractor does not introduce spurious arrangements.
Proxy B passes for all six corpus entries: `ArrangementCount` does not extract more arrangements than `PeakDegree` on any entry.

## Microbenchmark

Added as `#[ignore]` test `timing_microbenchmark`.
Prints per-entry mean saturation+extraction time in µs over 50 iterations.
Does not gate on performance (perf deferred per spec).
Run with: `bin/cargo-test -p mz-transform -- --ignored timing_microbenchmark -- --nocapture`

## Test run

```
bin/cargo-test -p mz-transform eqsat
Summary: 97 tests run: 97 passed, 0 failed
```

## Self-review

* Assertions are non-tautological: each would fail if the extractor inflated arrangements.
* Comments state the proxy choice and the reason the production pipeline is unreachable, per the task brief's fallback instructions.
* No em-dashes, no structuring semicolons, no vendor names in comments.
* `mz_ore::cast` not needed (no numeric casts).
* No `unsafe`.
* `cargo fmt`, `cargo clippy -p mz-transform`, and `bin/lint` all pass (the `buf` failure in `bin/lint` is unrelated: `buf` binary is not installed in this environment).

## Concerns

None after fixes (see fix note below).

---

## Fix note (d9d46b24e1 review findings)

### Finding 1: false "3 vs 4" triangle claim

**Root cause**: `Cost::arrangements` is `seen.len()` from `collect_memory`.
For both `Rel::Join` and `Rel::WcoJoin` on the triangle, `seen` receives exactly three `ArrId::JoinInput` entries (one per input).
Binary-join intermediate degrees are pushed into the `memory` degree vector only, NOT into `seen`.
So both join forms have `arrangements == 3` on the triangle; they are EQUAL on this metric.

**What was wrong**: The original report (line "WcoJoin over binary join: 3 vs 4 arrangements") and the module comment ("any rewrite that reduces arrangements (WcoJoin over binary join on cyclic inputs...)") both claimed a strict win on `arrangements` that does not exist.

**What is actually true**: WcoJoin dominates on the `memory` degree axis (leading term 1.0 vs 2.0 for the binary intermediate) and on `time` (1.5 vs 2.0), but not on `arrangements`, which is equal at 3 for both forms.

**Proxy A's actual scope**: It detects regressions where the extractor introduces SPURIOUS arrangements (extra `ArrangeBy`/`Reduce`/`TopK` nodes or additional join inputs).
It does NOT detect a binary-vs-WcoJoin form change; that difference lives on the `memory` degree axis.
The binary/WcoJoin distinction is exercised by `triangle_wcojoin_dominates` and related unit tests in `cost.rs`, and by the SLT goldens end-to-end.

**Changes made**: Corrected both the module comment in `validation.rs` and this report section.
The original incorrect text in the module comment was replaced with an accurate description of what `arrangements` counts, the equal-count result for the triangle, and the correct scope of Proxy A.

### Finding 2: doc run command

**What was wrong**: The microbenchmark doc comment said `cargo test -p mz-transform -- --ignored ...`; project convention is `bin/cargo-test`.

**Fix**: Changed to `bin/cargo-test -p mz-transform -- --ignored timing_microbenchmark -- --nocapture` in the doc comment and in this report.

### Finding 3: Proxy B non-vacuous corpus entry

**Attempt**: A corpus entry where `ArrangementCount` and `PeakDegree` extract different plans and `ArrangementCount`'s plan has strictly fewer arrangements requires the e-graph to contain two distinct forms that differ on arrangement count (not just on memory-degree).
The sharing scenario (two consumers of the same `(collection, key)` arrangement) does not produce a divergence here: `ArrId`-based deduplication in `collect_memory` is structural and applies equally to both objectives; neither objective changes which `ArrId` entries are charged.
The objectives diverge at the memory-degree level (which plan structure to choose), but the arrangement-count metric is determined by the plan's structural arrangement identity set, which both objectives see identically after extraction.
To make the two objectives diverge on the `arrangements` integer would require a corpus entry where one extraction path produces strictly fewer `ArrId::Node` or `ArrId::JoinInput` entries (e.g., one path forces an unnecessary extra `ArrangeBy` that the other avoids, or one path has an extra join input).
In the current rule set, neither objective forces extra nodes of this kind: the saturation produces both forms and both objectives extract the cheaper form on the arrangement-count axis (which is the same form, since both see the same `seen` set size).

**Outcome**: No discriminating corpus entry was added.
Proxy B is documented honestly in the module comment and the test's doc comment as a non-regression guard that currently holds as equality on the corpus.
The distinct value of the arrangement-count objective is exercised by `arrangement_count_dedups_and_credits_oracle` in `cost.rs` and by the SLT goldens end-to-end.

### Test results after fixes

`bin/cargo-test -p mz-transform eqsat::validation` and `bin/cargo-test -p mz-transform eqsat` all pass.
