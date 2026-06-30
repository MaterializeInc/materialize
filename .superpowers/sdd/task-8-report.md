# Task 8 report: Gate off column-remapping and no-MIR-target rules

## Rules disabled

Four rules were commented out with `# M1-disabled: <reason>` headers.

### push_filter_into_join_first

Although the rule does not use `shift` or `remap` on its RHS, the brief
classifies it as a column-remap rule: the predicate `p` is moved from the
join's output namespace into the first input's namespace.
The brief explicitly lists it as banned.
Disabled.

### push_filter_into_join_second

RHS uses `Filter[shift(p, -arity(a))]` — a `shift` combinator that rebases
the predicate columns from the join's global numbering into the second input's
local numbering.
Opaque scalars cannot be re-indexed.
Disabled.

### push_filter_past_project

RHS uses `Filter[remap(p, o)]` — a `remap` combinator that rewrites each
column reference `c` in `p` to `o[c]` (the underlying pre-projection column).
Opaque scalars cannot be re-indexed.
Disabled.

### join_to_wcoj

RHS produces `WcoJoin`, which has no corresponding `MirRelationExpr` variant.
The `raise` pass cannot encode it and would panic or bail.
Disabled.

## Special audit: flatten_join_first

Rule: `Join[e1](Join[e2](xs...), ys...) => Join[concat(e2, e1)](xs..., ys...)`

The inner join's inputs `xs...` occupy the leading columns in the outer join's
output.
After flattening, `xs...` still lead, followed by `ys...` — the same left-to-right
concatenation order.
No input is reordered; the global column numbering is identical before and after.
Equivalences from both joins are merged with `concat` (no offset arithmetic).
Column order is preserved.

**Determination: KEEP.**

## Shift/remap audit on remaining active rules

Scanned all non-disabled rules for `shift`/`remap` combinators in their RHS
`PExpr` payloads:

* `merge_filters` — `concat(q, p)`: concatenation only. Safe.
* `fuse_projects` — `compose(a, b)`: projection composition, not a column remap of scalars. Safe.
* `push_filter_through_map` — `Map[s] (Filter[p] r)`: payloads moved verbatim. Safe.
* `distribute_filter_union` — payloads moved verbatim. Safe.
* `flatten_union` — structural. Safe.
* `negate_negate` — no payload. Safe.
* `threshold_idempotent` — no payload. Safe.
* `fuse_maps` — `concat(s1, s2)`: concatenation only. Safe.
* `push_filter_through_negate` — payload moved verbatim. Safe.
* `push_filter_through_threshold` — payload moved verbatim. Safe.
* `distribute_negate_union` — structural. Safe.
* `distribute_filter_union_nary` — payload moved verbatim via list-map. Safe.
* `distribute_negate_union_nary` — structural via list-map. Safe.
* `flatten_union_nary` — structural. Safe.
* `flatten_join_first` — `concat(e2, e1)`: concatenation of equivalences, no shift/remap. Safe.
* `threshold_elision` — no RHS payload transform. Safe.
* `union_cancel` — `Empty(a)`: arity constructor, no remap. Safe.
* `reduce_elision` — `cols_of(gk)`: converts group-key column references to a projection; does not remap scalar columns. Safe.
* `drop_true_filter` — no payload transform. Safe.
* `empty_false_filter` — `Empty(r)`: arity constructor only. Safe.
* `map_columns_to_projection` — `concat(iota(arity(r)), cols_of(s))`: builds a projection from iota and column indices; no remap of scalars. Safe.

No additional rules were disabled beyond the four listed above.

## Tests deleted

None.
No existing test exercised a disabled rule directly.
The two pre-existing roundtrip tests (`merges_nested_filters`, `arity_is_preserved`) rely on `merge_filters` and the optimizer pipeline respectively — both unaffected.

## Final active rule list (18 rules)

1. merge_filters
2. fuse_projects
3. push_filter_through_map
4. distribute_filter_union
5. flatten_union
6. negate_negate
7. threshold_idempotent
8. fuse_maps
9. push_filter_through_negate
10. push_filter_through_threshold
11. distribute_negate_union
12. distribute_filter_union_nary
13. distribute_negate_union_nary
14. flatten_union_nary
15. flatten_join_first
16. threshold_elision
17. union_cancel
18. reduce_elision
19. drop_true_filter
20. empty_false_filter
21. map_columns_to_projection

## Test output

```
running 31 tests
... [all ok] ...
test result: ok. 31 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

running 3 tests
test no_disabled_rule_is_active ... ok
test merges_nested_filters ... ok
test arity_is_preserved ... ok
test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

34/34 tests pass.

## Commit hash

72e9e0a746

## Concerns

None.
The pre-existing `bin/lint` warnings (test-attribute, copyright) are all from
ported prototype code and existed before this task.
No new warnings introduced.
