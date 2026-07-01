# CI fix: eqsat binarize_join_first cross-join guard

## Summary

Added `has_inner_equiv(payload, n)` DSL condition to gate `binarize_join_first`
so it only fires when the first two inputs share a fully-contained join
equivalence.
Without this guard, clique/star joins (where every equivalence class spans all
inputs) produced an empty `equivs_inner`, making the inner join a cross product.

## Guard condition and wiring

`has_inner_equiv(payload, boundary)` returns true iff the equivalences payload
contains at least one class all of whose columns are < `boundary`.
It is the existence test that mirrors what `equivs_inner` computes.

Wired in four places:

* `src/transform/src/eqsat/dsl.rs` — `Cond::HasInnerEquiv { payload, boundary }` variant
* `src/transform/build/grammar.rs` — parser for `has_inner_equiv(ident, ixexpr)`
* `src/transform/build/codegen.rs` — codegen emits `eg.cond_has_inner_equiv(&p, n)` (both find-guard and rules_ast paths)
* `src/transform/src/eqsat/egraph.rs` — `EGraph::cond_has_inner_equiv` implementation (mirrors the filter loop inside `equivs_inner`); added `mz_ore::cast::CastFrom` import

Rule change in `relational.rewrite`:

```
rule binarize_join_first {
    ...
    where has_three_or_more_inputs()
    where has_inner_equiv(e, arity(a) + arity(b))
}
```

## join_index.slt: cross join gone, indexes restored

The 11-way clique self-join (all columns equal) previously produced:

```
CrossJoin type=delta  // arity: 70
  implementation
    %0:l0 » %4:big[×]f » %1:l0[×] » %2:l1[×] » %3:l1[×]
    ...
```

After the fix it produces a proper delta join directly over all 11 inputs:

```
Join on=(#0{a} = #15{b} = #30{c} = ... = #150{k}) type=delta  // arity: 154
  implementation
    %0:big » %9:big[#9{j}]KAef » %8:big[#8{i}]KAlf » ... » %2:l0[#2{c}]K
    ...
  ArrangeBy keys=[[#0{a}]]
    ReadIndex on=big big_idx_a=[delta join 1st input (full scan)]
  ArrangeBy keys=[[#1{b}]]
    ReadIndex on=big big_idx_b=[delta join lookup]
  ...
  ArrangeBy keys=[[#10{k}]]
    ReadIndex on=big big_idx_k=[delta join lookup]

Used Indexes:
  - materialize.public.big_idx_a (*** full scan ***, delta join 1st input (full scan))
  - materialize.public.big_idx_b (delta join lookup)
  - materialize.public.big_idx_e (delta join lookup)
  - materialize.public.big_idx_f (delta join lookup)
  - materialize.public.big_idx_g (delta join lookup)
  - materialize.public.big_idx_h (delta join lookup)
  - materialize.public.big_idx_i (delta join lookup)
  - materialize.public.big_idx_j (delta join lookup)
  - materialize.public.big_idx_k (delta join lookup)
```

No CrossJoin, all indexed delta-join lookups restored.

## Test results

* `bin/sqllogictest --optimized -- test/sqllogictest/transform/join_index.slt`: PASS (72/72)
* `bin/sqllogictest --optimized -- test/sqllogictest/attributes/mir_arity.slt test/sqllogictest/materialized_views.slt test/sqllogictest/transform/dataflow.slt`: PASS (397/397, no changes needed)
* `bin/cargo-test -p mz-transform eqsat`: PASS (97/97 including `binarize_preserves_arrangement_count_optimality`)

## Goldens touched

Only `test/sqllogictest/transform/join_index.slt` required rewriting (2 tests).
The chain/triangle/binary-join cases that use `binarize_join_first` were
unaffected because those joins have at least one equivalence class fully within
the inner boundary.

## Commit

`f47f62c1b2` — transform: gate eqsat join binarization to avoid cross joins on clique joins
