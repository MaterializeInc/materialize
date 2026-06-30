# Harness task 1b: join-aware arrangement count

## JoinImplementation enum shape

```
pub enum JoinImplementation {
    Differential(
        (usize, Option<Vec<MirScalarExpr>>, Option<JoinInputCharacteristics>),
        Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>,
    ),
    DeltaQuery(Vec<Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>>),
    IndexedFilter(GlobalId, GlobalId, Vec<MirScalarExpr>, Vec<Row>),
    Unimplemented,
}
```

* `Differential((start_idx, start_key, _), order)`: the start input is arranged if `start_key` is `Some(key)`.
  Each `(lookup_idx, lookup_key, _)` in `order` arranges `inputs[lookup_idx]` by `lookup_key`.
* `DeltaQuery(paths)`: each path is a list of `(lookup_idx, lookup_key, _)` pairs.
  All such pairs across all paths denote required arrangements; deduplication removes cross-path duplicates.
* `IndexedFilter(coll_id, _idx_id, index_key, _)`: the collection `coll_id` is arranged by `index_key`
  via an existing index.
* `Unimplemented`: no plan selected, contributes 0 implied arrangements.

## New counter

`count_arrangements_with_joins` is placed in `src/transform/tests/eqsat_arrangement_benchmark.rs`
immediately after `count_arrangements`.
It deduplicates by a string key using the same pattern as `count_arrangements`:

* `ArrangeBy`: `AB|{input:?}|{key:?}`
* `Reduce`: `RD|{input:?}|{group_key:?}`
* `TopK`: `TK|{input:?}|{group_key:?}|{order_key:?}`
* join-implied `(input, key)` pairs: `JI|{input:?}|{key:?}`
* `IndexedFilter`: `JI|{coll_id:?}|{index_key:?}`

`count_arrangements` and the `compare()` test helper are unchanged.

## Updated harness paths

* `run_with_intrinsic_snapshot`: uses `count_arrangements_with_joins` for the snapshot.
* `measure_marginal`: uses `count_arrangements_with_joins` for `final_off` and `final_on`.
* Module doc comment updated to describe the two-counter design and `Unimplemented`-counts-as-0 semantics.

## New table (verbatim output)

```
name                             off    on intrinsic  changed verdict
------------------------------------------------------------------------
smoke/filter-over-index            3     3         3    false Neutral
f1/shared-key-fanout               6     6         0    false Clobbered
f2/diamond-shared-filter           2     2         0     true Clobbered
f3/four-way-chain                  8     8         0    false Clobbered
w1/reachability                    5     5         1     true Clobbered
w2/reachability-envelope           5     5         1     true Clobbered
letrec/body-shared                 5     5         1     true Clobbered
flatmap/shared                     0     0         0    false Neutral
flatmap/filtered                   0     0         0    false Neutral
------------------------------------------------------------------------
summary: EqsatWins=0 Clobbered=6 Redundant=0 Neutral=3 net_arrangements_saved=0
```

## Step-4 observation: are joins Unimplemented at intrinsic snapshot?

The join-heavy fixtures (f1, f2, f3) all show `intrinsic=0` while `final_off` and `final_on` are
6, 2, and 8 respectively.
This is because at the intrinsic snapshot — immediately after the last `EqSat`-named transform —
every `Join` node still carries `JoinImplementation::Unimplemented`.
`count_arrangements_with_joins` returns 0 for `Unimplemented` joins, so `intrinsic=0` for those
fixtures is correct and expected.

The join planner (which runs in the physical optimizer AFTER eqsat) then assigns
`Differential` or `DeltaQuery` implementations, producing all the counted arrangements in the
final plan.

The w1/w2/w3 fixtures show `intrinsic=1` (from the `Reduce::distinct()` inside the LetRec value),
which is the only non-join arrangement present at the snapshot.

Conclusion: **eqsat does not commit `JoinImplementation`** decisions.
The intrinsic snapshot for join-heavy fixtures reflects only the pre-join-planning state (0 for
pure join plans).
The intrinsic-vs-final delta for those fixtures (`0` vs `6/2/8`) measures what the downstream
join planner adds, not what eqsat clobbered.
The `Clobbered` verdict for f1/f2/f3 is therefore a labeling artifact: `intrinsic < final_on`
because `intrinsic=0` and `final_on>0`, not because eqsat reduced arrangements that were later
added back.
The `final_on == final_off` for all fixtures means eqsat produces the same final arrangement
count as production.

## All tests

All 10 tests in `eqsat_arrangement_benchmark` pass:
`smoke`, `f1_shared_key_fanout`, `f2_diamond_shared_filter`, `f3_four_way_chain`,
`w1_reachability`, `w2_reachability_with_envelope`, `w3_letrec_body_shared`,
`flatmap_shared`, `flatmap_filtered`, `eqsat_marginal_value`.
The `final_on <= final_off` assertion holds for every fixture.
