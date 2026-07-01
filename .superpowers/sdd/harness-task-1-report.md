# Harness Task 1 Report: Marginal-value measurement

## What was added

Added to `src/transform/tests/eqsat_arrangement_benchmark.rs`:

* `enum Verdict` — four cases: `EqsatWins`, `Clobbered`, `Redundant`, `Neutral`.
* `struct Measurement` — holds `final_off`, `final_on`, `intrinsic`, `plan_changed`, `verdict`.
* `fn run_with_intrinsic_snapshot(plan, oracle) -> (usize, MirRelationExpr)` — runs the full logical + cleanup + physical transform list with eqsat ON (greedy), snapshotting `count_arrangements` immediately after each transform whose `name()` contains `"EqSat"`. The last such snapshot is `intrinsic`.
* `fn measure_marginal(name, plan, oracle) -> Measurement` — runs `optimize_full` with `Eqsat::Off` for `final_off`, then `run_with_intrinsic_snapshot` for `intrinsic` and `final_on`, computes `plan_changed` via structural `!=`, classifies the verdict, and asserts `final_on <= final_off`.
* `fn all_fixtures() -> Vec<(&str, MirRelationExpr, Box<dyn IndexOracle>)>` — centralized list of all nine fixtures (smoke, f1–f3, w1–w3, flatmap/shared, flatmap/filtered), built with the same hand-coded MIR as the existing individual tests.
* `#[mz_ore::test] fn eqsat_marginal_value()` — runs every fixture through `measure_marginal`, prints a fixed-width table, prints a summary line, and asserts `final_on <= final_off` per fixture.

## How eqsat transforms are detected

The `Transform` trait exposes `fn name(&self) -> &'static str`.
`EqSatTransform::name()` returns `"EqSatTransform"` and `PhysicalEqSatTransform::name()` returns `"PhysicalEqSatTransform"`.
Both contain the string `"EqSat"`, so the detection condition is `t.name().contains("EqSat")`.
No downcasting or `type_name_of_val` is needed.

## Verbatim printed table and summary

```
name                              off    on intrinsic  changed verdict
------------------------------------------------------------------------
smoke/filter-over-index             2     2         2    false Neutral
f1/shared-key-fanout                3     3         0    false Clobbered
f2/diamond-shared-filter            1     1         0     true Clobbered
f3/four-way-chain                   4     4         0    false Clobbered
w1/reachability                     3     3         1     true Clobbered
w2/reachability-envelope            3     3         1     true Clobbered
letrec/body-shared                  3     3         1     true Clobbered
flatmap/shared                      0     0         0    false Neutral
flatmap/filtered                    0     0         0    false Neutral
------------------------------------------------------------------------
summary: EqsatWins=0 Clobbered=6 Redundant=0 Neutral=3 net_arrangements_saved=0
```

## Interpretation

Six of nine fixtures are `Clobbered`: eqsat reduced arrangements after its own pass (intrinsic is 0 or lower than final_off) but downstream physical passes built the arrangements back.
Zero fixtures are `EqsatWins` and zero achieve a net arrangement saving in the final plan.
This confirms that the current eqsat logical pass does reduce arrangements in its own output, but those reductions do not survive the subsequent physical optimizer.
