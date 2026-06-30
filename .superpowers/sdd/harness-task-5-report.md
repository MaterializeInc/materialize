# Harness task 5: scalar-complexity visibility

## What was done

Added two scalar-complexity metric functions to `src/transform/tests/eqsat_arrangement_benchmark.rs`:

* `count_scalar_ops(plan)` — total `MirScalarExpr` node count across every scalar-bearing relational operator (`Map`, `Filter`, `Join`, `Reduce`, `TopK`, `FlatMap`).
  Uses `MirScalarExpr::size()` which already exists in `src/expr/src/scalar.rs`.
* `count_distinct_scalar_subexprs(plan)` — distinct scalar subexpressions across the whole plan, deduped by debug-format string.
  CSE-opportunity proxy: `scalar_ops - scalar_distinct` = shareable redundancy.

Extended `Measurement` with four new fields: `scalar_ops_off`, `scalar_ops_on`, `scalar_distinct_off`, `scalar_distinct_on`.

Updated `measure_marginal` to populate those fields from the eqsat-off and eqsat-on final plans.

Updated `eqsat_marginal_value` to print four new columns (`sc_off`, `sc_on`, `sd_off`, `sd_on`) and an extended summary line that reports totals and flags any fixture where the two pipelines diverge on scalar metrics.

Updated the module doc comment to describe the scalar metrics and what passthrough confirmation means.

## Verbatim table + summary

```
name                             off    on intrinsic  changed  cmtjoins sc_off sc_on sd_off sd_on verdict
-------------------------------------------------------------------------------------------------------------------
smoke/filter-over-index            3     3         3    false      true      2     2      2     2 NetNeutral
f1/shared-key-fanout               6     6         0    false     false      3     3      3     3 NetNeutral
f2/diamond-shared-filter           2     2         0     true     false      5     5      5     5 NetNeutral
f3/four-way-chain                  8     8         0    false     false      6     6      6     6 NetNeutral
w1/reachability                    5     5         1     true     false      4     4      3     3 NetNeutral
w2/reachability-envelope           5     5         1     true     false      7     7      5     5 NetNeutral
letrec/body-shared                 5     5         1     true     false      4     4      3     3 NetNeutral
flatmap/shared                     0     0         0    false     false      3     3      3     3 NetNeutral
flatmap/filtered                   0     0         0    false     false      6     6      5     5 NetNeutral
g1/cse-shared-filter-then-join     2     2         0     true     false      5     5      4     4 NetNeutral
g2/phase-order-filter-reduce       5     5         1     true     false      7     7      6     6 NetNeutral
p1/same-key-two-sites              6     6         0    false     false      3     3      3     3 NetNeutral
p2/prefix-vs-superset              6     6         0    false     false      5     5      5     5 NetNeutral
p3/self-join-diamond               2     2         0    false     false      3     3      3     3 NetNeutral
p4/index-plus-two-sites            6     6         0    false     false      3     3      3     3 NetNeutral
-------------------------------------------------------------------------------------------------------------------
summary: EqsatWins=0 NetNeutral=15 net_arrangements_saved=0
scalar:  total_sc_ops_off=66 total_sc_ops_on=66 scalar_ops_diff_any=false scalar_distinct_diff_any=false
scalar passthrough confirmed: eqsat-on == eqsat-off on both scalar metrics for every fixture (eqsat treats scalars as opaque)
```

## Scalar finding

Passthrough confirmed: eqsat-on equals eqsat-off on both `scalar_ops` and `scalar_distinct` for every fixture.
Total scalar ops: 66 off == 66 on.
No fixture showed any divergence.
This is the expected baseline: eqsat rewrites relational structure but passes scalars through opaquely.
