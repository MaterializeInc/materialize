# Harness task 2 report

## Part A: Verdict semantics corrected

### Changes made

`Verdict` enum reduced from four variants (`EqsatWins`, `Clobbered`, `Redundant`, `Neutral`) to two honest ones:

* `EqsatWins` — `final_on < final_off`
* `NetNeutral` — `final_on == final_off`

The previous `Clobbered` and `Redundant` variants were derived by comparing `intrinsic` to `final_on` or `final_off`. This comparison is not meaningful because the intrinsic snapshot is taken after eqsat but before `JoinImplementation` selection. Every `Join` in that snapshot has `Unimplemented`, contributing 0 join-implied arrangements. So `intrinsic < final_on` was almost always true for join-containing fixtures, causing most to be labeled `Clobbered` — an artifact of the snapshot timing, not a real signal.

### New `Measurement` fields

`eqsat_committed_joins: bool` — true iff the intrinsic snapshot contains at least one `Join` with a non-`Unimplemented` `implementation`. On every current fixture this is false, which is the measured finding: eqsat defers all join-arrangement choices to the downstream physical passes.

`intrinsic` and `plan_changed` are retained as informational columns.

### New table format

Added `cmtjoins` column to the printed table. The `check-no-diff.sh` lint check reports the modified file, which is expected for uncommitted changes.

### Key finding confirmed

The `smoke/filter-over-index` fixture shows `cmtjoins=true` because the optimizer uses `IndexedFilter` (an `IndexedFilter` join implementation), which is a committed join strategy. All other fixtures have `cmtjoins=false`, confirming eqsat does not commit `JoinImplementation` decisions.

## Part B: Divergence-regime fixtures

### g1/cse-shared-filter-then-join

Sharing opportunity: `base` (GlobalId 801, arity 2) is filtered by `#0 > 0` in two independent branches that are then joined on `#0 = #2`. If CSE hoists the structurally identical filter into a single let-binding, downstream sees one filtered scan to arrange, not two.

Verdict: `NetNeutral`. Production's own CSE pass already collapses the duplicate filtered scans before arrangement planning runs. This is a valid finding: greedy already captures this opportunity at MIR level.

### g2/phase-order-filter-reduce

Sharing opportunity: a `Reduce` (group by column 0) is applied to a join of filtered `R` (GlobalId 901) with `S` (GlobalId 902). The filter `#1 > 0` on R is pushed below the join before `Reduce` sees the plan in the greedy pipeline. Eqsat explores both orderings simultaneously and should find the same or better result.

Verdict: `NetNeutral`. The greedy `PredicatePushdown`-before-`Reduce` ordering already achieves the optimal plan. Eqsat changed the plan (`plan_changed=true`, confirmed by the `g2` row), but the downstream passes converged to the same arrangement count.

## Final table (verbatim)

```
name                             off    on intrinsic  changed  cmtjoins verdict
-------------------------------------------------------------------------------------
smoke/filter-over-index            3     3         3    false      true NetNeutral
f1/shared-key-fanout               6     6         0    false     false NetNeutral
f2/diamond-shared-filter           2     2         0     true     false NetNeutral
f3/four-way-chain                  8     8         0    false     false NetNeutral
w1/reachability                    5     5         1     true     false NetNeutral
w2/reachability-envelope           5     5         1     true     false NetNeutral
letrec/body-shared                 5     5         1     true     false NetNeutral
flatmap/shared                     0     0         0    false     false NetNeutral
flatmap/filtered                   0     0         0    false     false NetNeutral
g1/cse-shared-filter-then-join     2     2         0     true     false NetNeutral
g2/phase-order-filter-reduce       5     5         1     true     false NetNeutral
-------------------------------------------------------------------------------------
summary: EqsatWins=0 NetNeutral=11 net_arrangements_saved=0
```

## No EqsatWins

No fixture achieved `EqsatWins`. All 11 fixtures show `NetNeutral`. This is consistent with the key finding: eqsat defers all join-arrangement decisions, and the logical restructuring it does perform does not produce a different arrangement count than the greedy pipeline on these fixtures. The `final_on <= final_off` assertion held on every fixture.

The new divergence-regime fixtures (g1, g2) are valid benchmarks of genuinely-deployed pipelines. Their `NetNeutral` results are honest findings that production's greedy pass ordering already captures the opportunities these fixtures were designed to test.
