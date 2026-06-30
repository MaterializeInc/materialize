# Harness task 3: JI cross-join arrangement-sharing probe

## Bottom line

CONCLUSIVE NEGATIVE.
Across all fixtures, including five purpose-built probes, production `JoinImplementation` (JI) never arranges a single collection by two or more distinct keys.
Every collection is arranged exactly once, on the key forced by its join equivalence class, and that single arrangement is reused at every site that needs it.
There is no reducible duplication for eqsat's global scope to collapse, so subsuming JI into eqsat has no measurable arrangement-count target on this corpus.

## What was added

* `multi_key_collections(plan)` and `report_multi_key(name, plan)` in `src/transform/tests/eqsat_arrangement_benchmark.rs`.
  The diagnostic walks the eqsat-OFF fully-optimized plan, groups every arrangement by the identity of the collection it arranges (debug fingerprint of the arranged input expression), and collects the set of distinct keys per collection.
  It walks `ArrangeBy` keys plus each `Join.implementation`'s per-input keys (`Differential` start and lookup, `DeltaQuery` path keys, `IndexedFilter` index key).
  It reports any collection arranged by `>= 2` distinct keys, the sharing-opportunity candidate signal.
  `report_multi_key` is called from `measure_marginal` on the eqsat-OFF plan.
* Five probe fixtures (`p1`..`p4` registered, `p2` doubles as the honesty-guard control).

## Per-fixture findings

The diagnostic flagged ZERO multi-key collections on every fixture (existing and new):

```
[smoke/filter-over-index] no collection arranged by >=2 distinct keys
[f1/shared-key-fanout] no collection arranged by >=2 distinct keys
[f2/diamond-shared-filter] no collection arranged by >=2 distinct keys
[f3/four-way-chain] no collection arranged by >=2 distinct keys
[w1/reachability] no collection arranged by >=2 distinct keys
[w2/reachability-envelope] no collection arranged by >=2 distinct keys
[letrec/body-shared] no collection arranged by >=2 distinct keys
[flatmap/shared] no collection arranged by >=2 distinct keys
[flatmap/filtered] no collection arranged by >=2 distinct keys
[g1/cse-shared-filter-then-join] no collection arranged by >=2 distinct keys
[g2/phase-order-filter-reduce] no collection arranged by >=2 distinct keys
[p1/same-key-two-sites] no collection arranged by >=2 distinct keys
[p2/prefix-vs-superset] no collection arranged by >=2 distinct keys
[p3/self-join-diamond] no collection arranged by >=2 distinct keys
[p4/index-plus-two-sites] no collection arranged by >=2 distinct keys
```

Marginal-value table (final pipeline, eqsat off vs on):

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
p1/same-key-two-sites              6     6         0    false     false NetNeutral
p2/prefix-vs-superset              6     6         0    false     false NetNeutral
p3/self-join-diamond               2     2         0    false     false NetNeutral
p4/index-plus-two-sites            6     6         0    false     false NetNeutral
-------------------------------------------------------------------------------------
summary: EqsatWins=0 NetNeutral=15 net_arrangements_saved=0
```

### p1: same collection, two join sites, same column

`R(#0,#1)` joined to `S` on `R.#0=S.#2` AND to `T` on `R.#0=T.#4`.
Intended shared key: R by `(#0)`.
JI produced a delta join. Each of R/S/T is arranged by `[#0]` in a single `ArrangeBy`, and all three delta paths reuse those same single arrangements:

```
Join on=(#0 = #2 = #4) type=delta
  implementation
    %0:t1101 » %1:t1102[#0]KA » %2:t1103[#0]KA
    %1:t1102 » %0:t1101[#0]KA » %2:t1103[#0]KA
    %2:t1103 » %0:t1101[#0]KA » %1:t1102[#0]KA
  ArrangeBy keys=[[#0]] (Get t1101)
  ArrangeBy keys=[[#0]] (Get t1102)
  ArrangeBy keys=[[#0]] (Get t1103)
```

Keys JI chose for R: `{[#0]}` (one key). A single key already suffices. No opportunity.

### p2: prefix vs superset keys (honesty-guard control)

`R` joined to `S` on `R.#0=S.#2` AND to `T` on `R.#0=T.#4 AND R.#1=T.#5`.
JI produced a differential join:

```
Join on=(#0 = #2 = #4 AND #1 = #5) type=differential
  implementation
    %0:t1201[#0, #1]KK » %2:t1203[#0, #1]KK » %1:t1202[#0]K
  ArrangeBy keys=[[#0, #1]] (Get t1201)   # R
  ArrangeBy keys=[[#0]]     (Get t1202)   # S
  ArrangeBy keys=[[#0, #1]] (Get t1203)   # T
```

R is arranged by `[#0,#1]` once. S by `[#0]`. T by `[#0,#1]`.
No collection is arranged two ways.
Crucially, R and T legitimately need `[#0,#1]` (they bind both equivalences `#0=#2=#4` and `#1=#5`), while S binds only `#0`. These are genuinely different keys on different collections, so even if a collection HAD appeared twice here it would not be reducible. This confirms the honesty distinction holds.

### p3: self-join diamond

`R` referenced three times, joined on `#0` across all copies.
CSE collapses the three structurally identical `Get` copies into a single CTE `l0`, arranged ONCE by `[#0]`, and the delta join reuses it on every path:

```
With cte l0 = ArrangeBy keys=[[#0]] (Get t1301)
Return
  Join on=(#0 = #2 = #4) type=delta
    implementation
      %0:l0 » %1:l0[#0]KA » %2:l0[#0]KA
      %1:l0 » %0:l0[#0]KA » %2:l0[#0]KA
      %2:l0 » %0:l0[#0]KA » %1:l0[#0]KA
    Get l0 / Get l0 / Get l0
```

off=2 (one ArrangeBy + one JI collection). CSE plus JI already achieve the optimal single shared arrangement.

### p4: index plus two sites

`R` indexed on `[#0]`, joined to `S` and `T` both on `#0`.
JI arranges R once by `[#0]` and reuses it for both delta-join sites. No duplication, no extra arrangement alongside the index.

## Why this is the conclusive negative, not a search gap

In MIR, a collection's arrangement key at a join site is FORCED by the equivalence class that join binds it on.
Two sites can share a single key only when they bind the collection on the SAME equivalence class.
But when two sites bind the same equivalence class, CSE keeps the collection a single node and JI arranges it once and reuses it (p1, p3, p4).
When two sites bind DIFFERENT equivalence classes, the two keys genuinely differ and both arrangements are needed (p2).
There is no slack between these cases: a single key is correctness-valid for two sites if and only if those sites share an equivalence, which is exactly the case JI already shares.

Equivalence: the honesty guard's "single key valid for ALL sites" condition coincides with "all sites in one equivalence class", which JI already collapses to one arrangement. So the multi-key-where-one-suffices opportunity does not arise from JI's join-arrangement choices on this class of plans.

The arrangement count is dominated by explicit `ArrangeBy` nodes (the children JI references), which CSE deduplicates by `(collection, key)`, not by JI re-keying. Eqsat's global scope has nothing to collapse here that production's CSE + local JI does not already collapse.

## Constraints honored

Additive only (existing fixtures, `count_arrangements`, `compare` unchanged). No `as` casts. No em-dashes or clause-joining semicolons in comments. `cargo fmt`, `bin/lint` (only `check-no-diff` flags the uncommitted change, all real checks pass), `cargo clippy -p mz-transform --tests` clean. The `final_on <= final_off` assertion holds on every fixture.
