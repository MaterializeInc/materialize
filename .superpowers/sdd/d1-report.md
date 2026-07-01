# Task D1 report: physical eqsat placement (feasibility spike)

## What was done

Added `enable_eqsat_physical_optimizer` (default off) and `PhysicalEqSatTransform`, registered in `physical_optimizer` after `fixpoint_physical_01` and before `LiteralConstraints`/`JoinImplementation`.

Files modified:

* `src/repr/src/optimize.rs`: added `enable_eqsat_physical_optimizer: bool` to `OptimizerFeatures`.
* `src/sql/src/session/vars/definitions.rs`: added the system var (`default: false`), wired into `From<&SystemVars> for OptimizerFeatures`, added to the test destructure and `set_var!` block.
* `src/sql/src/plan/statement/dml.rs`: added `enable_eqsat_physical_optimizer: Default::default()` to the `OptimizerFeatureOverrides` literal.
* `src/sql/src/plan/statement/ddl.rs`: added `enable_eqsat_physical_optimizer: _` to the destructure pattern.
* `src/transform/src/eqsat/transform.rs`: added `PhysicalEqSatTransform` (mirrors `EqSatTransform` but calls `optimize`/commit_wcoj=true; same size cap and arity guard).
* `src/transform/src/eqsat.rs`: exported `PhysicalEqSatTransform` from the `eqsat` module.
* `src/transform/src/lib.rs`: registered `PhysicalEqSatTransform` in `physical_optimizer` at the correct placement point.
* `src/transform/tests/wcoj_decision.rs`: added `physical_eqsat_commits_delta_query_for_triangle` and `physical_eqsat_delta_query_survives_join_implementation` tests.

## Feasibility gate result

Gate: PASSED. The pipeline tolerates a committed `DeltaQuery` at the before-`LiteralConstraints` placement.

Verification: temporarily set `enable_eqsat_physical_optimizer` default to `true`, built with `--optimized`, ran:

```
bin/sqllogictest --optimized -- test/sqllogictest/arithmetic.slt
```

Result: `PASS: success=206 total=206`, exit 0.
Zero "Non-positive multiplicity" occurrences.
Zero "can't deal with filled in join implementations" panics.
No panics of any kind.

The `PhysicalEqSatTransform` transform name appears in the slow-optimization warning's `transform_times_µs` map, confirming the physical pass executed.

## Default confirmed false

`enable_eqsat_physical_optimizer` is committed with `default: false`.
The flag was set to `true` only temporarily for the feasibility gate run and reverted before commit.

## Test summary

* `cargo test -p mz-transform --lib eqsat`: 49 passed, 0 failed.
* `cargo test -p mz-transform --test wcoj_decision`: 6 passed (4 pre-existing + 2 new), 0 failed.
* New tests: `physical_eqsat_commits_delta_query_for_triangle` and `physical_eqsat_delta_query_survives_join_implementation`.

## Concerns

* `PhysicalEqSatTransform` is slow on large plans (the arithmetic.slt slow-optimization warning shows `6565779 µs` for one object).
  The `MAX_PLAN_SIZE = 200` cap should prevent the worst cases, but the cap may need tuning once the flag is on by default.
* The cost model is still index-blind (Task 2).
  With the flag on, the physical placement will commit `WcoJoin` to `DeltaQuery` without knowing whether join inputs are already arranged, which may produce suboptimal plans in some cases.
  This is acceptable for the feasibility spike; Task 2 fixes it.
